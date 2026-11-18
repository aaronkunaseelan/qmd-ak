"""The address guard on fetched URLs: internal names and hops are refused before any connection."""

import http.server
import json
import socket
import subprocess
import sys
import threading
import urllib.request

import pytest

from qmd2 import attachments, mullvad

BLOCKED = [
    '0.0.0.0',
    '127.0.0.1',
    '10.0.0.1',
    '172.16.0.5',
    '192.168.1.7',
    '169.254.169.254',
    '100.64.0.1',
    '240.0.0.1',
    '::1',
    'fd7a:115c:a1e0::1',
    'fd12:3456::1',
    'fe80::1',
    '224.0.0.1',
    'ff02::1',
    '::ffff:127.0.0.1',
    '::ffff:10.0.0.1',
    '::ffff:100.64.0.1',
    '::ffff:192.168.0.1',
]
PUBLIC = ['8.8.8.8', '1.1.1.1', '93.184.215.14', '2606:4700:4700::1111', '::ffff:8.8.8.8']


def address_for(monkeypatch, mapping):
    """Names answer with the given address: the guard's one lookup runs offline."""

    def getaddrinfo(host, port, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', (mapping.get(host, host), port, 0, 0))]

    monkeypatch.setattr(socket, 'getaddrinfo', getaddrinfo)


@pytest.mark.parametrize('address', BLOCKED)
def test_internal_addresses_are_not_allowed(address):
    assert not attachments.address_allowed(address)


@pytest.mark.parametrize('address', PUBLIC)
def test_public_addresses_are_allowed(address):
    assert attachments.address_allowed(address)


@pytest.mark.parametrize('address', BLOCKED)
def test_a_name_that_resolves_inward_is_refused(monkeypatch, address):
    address_for(monkeypatch, {'intranet.example': address})
    with pytest.raises(attachments.Unavailable, match='blocked_address') as refusal:
        attachments.check_url('http://intranet.example/health')
    assert 'intranet' not in str(refusal.value) and address not in str(refusal.value)


def test_a_public_name_is_checked_and_allowed(monkeypatch):
    address_for(monkeypatch, {'public.example': '93.184.215.14'})
    assert attachments.check_url('https://public.example/')[0][4][0] == '93.184.215.14'


def test_non_http_schemes_are_refused():
    with pytest.raises(attachments.Unavailable, match='blocked_scheme'):
        attachments.check_url('file:///etc/passwd')


def test_a_fetch_refuses_before_it_connects(tmp_path, monkeypatch):
    address_for(monkeypatch, {'intranet.example': '100.64.0.1'})
    monkeypatch.setattr(socket, 'socket', lambda *args, **kwargs: pytest.fail('connected'))
    row = dict(ref=dict(source='link', url='http://intranet.example:8180/health'))
    with pytest.raises(attachments.Unavailable, match='blocked_address'):
        attachments.fetch_url(row, tmp_path / 'media')
    assert [a['error'] for a in row['fetched']['attempts']] == ['blocked_address']
    assert not (tmp_path / 'media').exists()


def test_a_blocked_address_never_escalates_to_a_relay(tmp_path, monkeypatch):
    address_for(monkeypatch, {'intranet.example': '127.0.0.1'})
    monkeypatch.setattr(mullvad, 'select_relays', lambda: pytest.fail('relayed'))
    row = dict(ref=dict(source='link', url='http://intranet.example/'), fetched={})
    with pytest.raises(attachments.Unavailable, match='blocked_address'):
        attachments.fetch(tmp_path, row, tmp_path / 'media')


def test_a_queued_link_to_an_internal_name_ends_unavailable(tmp_path, monkeypatch):
    address_for(monkeypatch, {'intranet.example': '127.0.0.1'})
    identifier, state = attachments.materialise(tmp_path, 'http://intranet.example:8384/')
    assert state == 'queued'
    row = attachments.process(tmp_path, {'id': identifier})
    assert (row['state'], row['reason']) == ('unavailable', 'blocked_address')


def test_extract_refuses_internal_urls(tmp_path):
    for url in ('http://127.0.0.1:8384/', 'http://100.64.0.1:8180/health'):
        result = subprocess.run(
            [
                sys.executable,
                '-m',
                'qmd2.cli',
                '--home',
                str(tmp_path / 'qmd'),
                'extract',
                url,
                '--json',
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 4, result.stderr
        payload = json.loads(result.stdout)
        assert payload['error'] == 'blocked_address' and payload['text'] is None


class RedirectHandler(http.server.BaseHTTPRequestHandler):
    paths = []

    def do_GET(self):
        self.paths.append(self.path)
        if self.path == '/start':
            self.send_response(302)
            self.send_header('Location', self.server.inward)
            self.end_headers()
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'<html><body>public</body></html>')

    def log_message(self, *args):
        pass


def test_a_redirect_from_public_to_private_is_refused(tmp_path, monkeypatch):
    RedirectHandler.paths = []
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), RedirectHandler)
    server.inward = 'http://10.0.0.1:8384/'
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    # The test's own server is the one public address; everything else is judged as the guard judges it.
    monkeypatch.setattr(
        attachments, 'address_allowed', lambda address: address.startswith('127.0.0.1')
    )
    try:
        row = dict(ref=dict(source='link', url=f'http://127.0.0.1:{server.server_port}/start'))
        with pytest.raises(attachments.Unavailable, match='blocked_address'):
            attachments.fetch_url(row, tmp_path / 'media')
        assert RedirectHandler.paths == ['/start']
        assert '10.0.0.1' not in str(row['fetched']['attempts'][0]['error'])
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_a_hop_is_checked_where_a_socks_relay_resolves_it():
    handler = attachments.CheckedRedirects()
    request_ = urllib.request.Request('https://public.example/article')
    with pytest.raises(attachments.Unavailable, match='blocked_address'):
        handler.redirect_request(request_, None, 302, 'Found', {}, 'http://127.0.0.1:8384/')
    with pytest.raises(attachments.Unavailable, match='blocked_scheme'):
        handler.redirect_request(request_, None, 302, 'Found', {}, 'ftp://public.example/')
    relay = attachments.opener(('127.0.0.1', 9)).handlers
    assert any(isinstance(h, attachments.CheckedRedirects) for h in relay)


@pytest.mark.parametrize(
    'address',
    ['64:ff9b::a9fe:a9fe', '64:ff9b::a00:1', '64:ff9b::7f00:1', '::7f00:1', '::a00:1', '::1', '::'],
)
def test_ipv6_forms_that_carry_an_internal_ipv4_address_are_refused(address):
    from qmd2.attachments import address_allowed

    assert not address_allowed(address)


def test_nat64_of_a_public_ipv4_address_is_allowed():
    from qmd2.attachments import address_allowed

    assert address_allowed('64:ff9b::808:808')  # 8.8.8.8


def test_guarded_connect_accepts_http_clients_default_timeout():
    import socket
    import threading
    from qmd2 import attachments as A

    server = socket.socket()
    server.bind(('127.0.0.1', 0))
    server.listen(1)
    port = server.getsockname()[1]
    threading.Thread(target=lambda: server.accept()[0].close(), daemon=True).start()
    original = A.address_allowed
    try:
        A.address_allowed = lambda address: True
        sock = A.guarded_connect(('127.0.0.1', port), socket._GLOBAL_DEFAULT_TIMEOUT)
        sock.close()
    finally:
        A.address_allowed = original
        server.close()
