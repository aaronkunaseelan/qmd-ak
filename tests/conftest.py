"""Synthetic VPN boundaries shared by fetch and queued-capture edge tests."""

import base64
import http.server
import json
import os
import shutil
import threading
from pathlib import Path

import pytest

os.environ['QMD2_ACCOUNTS'] = str(Path(__file__).with_name('accounts.json'))

from qmd2 import mullvad


@pytest.fixture
def exits(tmp_path, monkeypatch):
    key = base64.b64encode(bytes(32)).decode()
    config = tmp_path / 'vpn.env'
    config.write_text(f'WIREGUARD_PRIVATE_KEY={key}\nWIREGUARD_ADDRESSES=10.0.0.2/32\n')
    binary = tmp_path / 'wireproxy'
    shutil.copy(Path(__file__).with_name('wireproxy_edge.py'), binary)
    binary.chmod(0o700)
    relays = [
        dict(
            hostname=f'se-sto-wg-00{i}',
            active=True,
            country_code='se',
            pubkey=base64.b64encode(bytes([i]) * 32).decode(),
            ipv4_addr_in=f'192.0.2.{i}',
        )
        for i in (1, 2)
    ]
    calls = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            code = (
                403
                if self.path in ('/recover', '/mixed', '/302')
                else int(self.path[1:])
                if self.path[1:].isdigit()
                else 200
            )
            self.send_response(code)
            self.send_header('Retry-After', '86400')
            self.end_headers()
            self.wfile.write(
                json.dumps(
                    relays
                    if self.path == '/relays'
                    else dict(mullvad_exit_ip=True, mullvad_exit_ip_hostname='us-bos-wg-102')
                ).encode()
            )

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f'http://localhost:{server.server_port}'
    monkeypatch.setattr(mullvad, 'RELAYS', base + '/relays')
    monkeypatch.setattr(mullvad, 'AM_I', 'http://exit.invalid/json')
    monkeypatch.setattr(mullvad, 'GLUETUN_PROXY', base)
    monkeypatch.setattr(mullvad, 'CONFIG', tmp_path / 'fetch.json')
    monkeypatch.setattr(mullvad, 'VPN_ENV', config)
    monkeypatch.setattr(mullvad, 'WIREPROXY', binary)
    monkeypatch.setattr(mullvad, 'LOCK', tmp_path / 'tunnel.lock')
    try:
        yield base, calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
        log = tmp_path / 'tunnels.jsonl'
        for started in map(json.loads, log.read_text().splitlines() if log.exists() else []):
            with pytest.raises(ProcessLookupError):
                os.kill(started['pid'], 0)
