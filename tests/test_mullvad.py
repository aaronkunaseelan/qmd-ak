"""Relay selection at public HTTP/proxy and configuration filesystem edges."""

import fcntl
import http.server
import json
import os
from pathlib import Path
import select
import ssl
import subprocess
import threading
import time

import pytest

from qmd2 import attachments, mullvad
from qmd2.attachments import Failed, Unavailable


def test_relay_selection_http_and_config(tmp_path, monkeypatch):
    calls = []
    relays = [
        dict(
            hostname=name,
            active=active,
            country_code=country,
            pubkey='synthetic',
            ipv4_addr_in='192.0.2.1',
        )
        for name, active, country in [
            ('us-bos-wg-102', True, 'us'),
            ('us-nyc-wg-001', True, 'us'),
            ('se-sto-wg-001', True, 'se'),
            ('de-fra-wg-001', False, 'de'),
            ('jp-tyo-wg-001', True, 'jp'),
        ]
    ]

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(
                json.dumps(
                    relays
                    if self.path == '/relays'
                    else dict(mullvad_exit_ip=True, mullvad_exit_ip_hostname='us-bos-wg-102')
                    if self.path == 'http://exit.invalid/json'
                    else {}
                ).encode()
            )

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{server.server_port}'
    monkeypatch.setattr(mullvad, 'RELAYS', base + '/relays')
    monkeypatch.setattr(mullvad, 'AM_I', 'http://exit.invalid/json')
    monkeypatch.setattr(mullvad, 'GLUETUN_PROXY', base)
    monkeypatch.setattr(mullvad, 'CONFIG', tmp_path / 'fetch.json')
    try:
        assert {r['hostname'] for r in mullvad.select_relays()} == {
            'us-nyc-wg-001',
            'se-sto-wg-001',
        }
        mullvad.CONFIG.write_text('{"mullvad_countries": ["se"]}')
        assert [r['hostname'] for r in mullvad.select_relays()] == ['se-sto-wg-001']
        mullvad.CONFIG.write_text('{"mullvad_countries": ["bad"]}')
        with pytest.raises(Failed, match='mullvad_config'):
            mullvad.select_relays()
        monkeypatch.setattr(mullvad, 'AM_I', 'http://unknown.invalid/json')
        with pytest.raises(Unavailable, match='shared_key_relay_unknown'):
            mullvad.shared_relay()
        assert 'http://exit.invalid/json' in calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_tunnel_lock_and_parent_death(tmp_path, exits):
    # CLI and service environments must use the same per-user lock.
    paths = [
        subprocess.check_output(
            [__import__('sys').executable, '-c', 'from qmd2.mullvad import LOCK; print(LOCK)'],
            env=dict(os.environ, XDG_RUNTIME_DIR=str(tmp_path / name)),
        )
        for name in ('cli-runtime', 'service-runtime')
    ]
    assert paths[0] == paths[1]
    settings = {
        name: str(getattr(mullvad, name))
        for name in ('RELAYS', 'AM_I', 'GLUETUN_PROXY', 'CONFIG', 'VPN_ENV', 'WIREPROXY', 'LOCK')
    }
    script = '''
import json, os, time
from pathlib import Path
from qmd2 import mullvad
for name, value in json.loads(os.environ['Q8_EDGE_SETTINGS']).items():
    setattr(mullvad, name, Path(value) if name in ('CONFIG', 'VPN_ENV', 'WIREPROXY', 'LOCK') else value)
with mullvad.tunnel(mullvad.select_relays()[0], mullvad.credentials()):
    print('ready', flush=True)
    time.sleep(30)
'''
    with mullvad.LOCK.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        process = subprocess.Popen(
            [__import__('sys').executable, '-c', script],
            env=dict(os.environ, Q8_EDGE_SETTINGS=json.dumps(settings)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            assert not select.select([process.stdout], [], [], 0.2)[0]
            assert not (tmp_path / 'tunnels.jsonl').exists()
            fcntl.flock(lock, fcntl.LOCK_UN)
            assert select.select([process.stdout], [], [], 10)[0]
            assert process.stdout.readline() == b'ready\n'
            tunnel_pid = json.loads((tmp_path / 'tunnels.jsonl').read_text())['pid']
            process.kill()
            process.wait(timeout=5)
            deadline = time.monotonic() + 5
            while Path(f'/proc/{tunnel_pid}').exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            assert not Path(f'/proc/{tunnel_pid}').exists()
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)


@pytest.mark.parametrize(
    'path,count,category',
    [
        ('/recover', 2, None),
        ('/302', 2, None),
        ('/403', 3, 'unavailable'),
        ('/451', 3, 'unavailable'),
        ('/429', 3, 'temporary'),
        ('/500', 3, 'temporary'),
        ('/503', 3, 'temporary'),
        ('/mixed', 3, 'unavailable'),
        ('/404', 1, 'unavailable'),
        ('/410', 1, 'unavailable'),
    ],
)
def test_fetch_tunnel_subprocess(tmp_path, exits, path, count, category):
    base, calls = exits
    row = dict(ref=dict(source='link', url=base + path))
    target = tmp_path / 'media'
    if category:
        with pytest.raises(Exception) as caught:
            attachments.fetch(tmp_path, row, target)
        assert (
            'unavailable'
            if isinstance(caught.value, Unavailable)
            else attachments.temporary_refusal(caught.value)[0]
        ) == category
    else:
        attachments.fetch(tmp_path, row, target)
        assert target.read_text() == 'Synthetic remote lighthouse harbor'
    attempts = row['fetched']['attempts']
    assert len(attempts) == count
    assert attempts[0]['via'] == 'direct'
    assert all(a['via'] == 'mullvad' and a['relay'] != 'us-bos-wg-102' for a in attempts[1:])
    assert len({a['relay'] for a in attempts[1:]}) == count - 1
    assert all(a['elapsed_s'] >= 0 for a in attempts)
    assert '/relays' in calls if count > 1 else '/relays' not in calls


def test_own_failures_and_unknown_shared_relay(tmp_path, exits, monkeypatch):
    base, _ = exits
    for failure in (
        'mullvad_key_missing',
        'shared_key_relay_unknown',
        'mullvad_tunnel_start_failed',
    ):
        row = dict(ref=dict(source='link', url=base + '/429'))
        with monkeypatch.context() as patch:
            if failure == 'mullvad_key_missing':
                patch.setattr(mullvad, 'VPN_ENV', tmp_path / 'missing')
            elif failure == 'shared_key_relay_unknown':
                patch.setattr(mullvad, 'GLUETUN_PROXY', 'http://127.0.0.1:1')
            else:
                patch.setattr(mullvad, 'WIREPROXY', tmp_path / 'missing')
            with pytest.raises(
                Unavailable if failure == 'shared_key_relay_unknown' else Failed, match=failure
            ):
                attachments.fetch(tmp_path, row, tmp_path / 'media')
        assert len(row['fetched']['attempts']) == (
            2 if failure == 'mullvad_tunnel_start_failed' else 1
        )


def test_connection_refusal_uses_socks(tmp_path, exits):
    row = dict(ref=dict(source='link', url='http://localhost:1/recover'))
    attachments.fetch(tmp_path, row, tmp_path / 'media')
    assert [a['status'] for a in row['fetched']['attempts']] == [None, 200]
    assert 'connection_refused' in row['fetched']['attempts'][0]['error']


def test_tls_verification_never_uses_relay(tmp_path, exits):
    subprocess.run(
        [
            'openssl',
            'req',
            '-x509',
            '-newkey',
            'rsa:2048',
            '-nodes',
            '-days',
            '1',
            '-subj',
            '/CN=localhost',
            '-keyout',
            str(tmp_path / 'key'),
            '-out',
            str(tmp_path / 'cert'),
        ],
        check=True,
        capture_output=True,
    )
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), http.server.BaseHTTPRequestHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tmp_path / 'cert', tmp_path / 'key')
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    row = dict(ref=dict(source='link', url=f'https://localhost:{server.server_port}/'))
    try:
        with pytest.raises(Unavailable, match='tls_error'):
            attachments.fetch(tmp_path, row, tmp_path / 'media')
        assert len(row['fetched']['attempts']) == 1
        assert '/relays' not in exits[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
