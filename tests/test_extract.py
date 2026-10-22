"""Pure extraction at the filesystem and CLI edges; no Azure calls."""

import http.server
import json
import os
from pathlib import Path
import subprocess
import sys
import threading


def test_extract_filesystem_cli(tmp_path):
    home = tmp_path / 'home'
    home.mkdir()
    (home / 'sentinel').write_text('synthetic')
    source = home / 'input.txt'
    source.write_text('Synthetic lighthouse harbor')
    env = dict(os.environ, HOME=str(home), TMPDIR=str(home), PYTHONDONTWRITEBYTECODE='1')
    command = [sys.executable, '-m', 'qmd2.cli', '--home', str(home / 'qmd')]
    contract = tmp_path / 'extract-contract.json'
    contract.write_bytes(
        subprocess.run(
            command + ['contract', 'qmd.extract.v1'],
            env=env,
            capture_output=True,
            check=True,
        ).stdout
    )

    def snapshot():
        return {str(p.relative_to(home)): p.stat().st_mtime_ns for p in home.rglob('*')}

    before = snapshot()
    for args, body, expected_text, error in [
        ([str(source)], None, source.read_text(), None),
        (['-'], b'Synthetic text from stdin', 'Synthetic text from stdin', None),
        (['-'], b'<p>Synthetic harbor</p>', 'Synthetic harbor', None),
        (['-'], b'\x00\xff', None, 'unsupported_type'),
        (['-'], b'', None, 'unsupported_type'),
        (['-'], b'<html></html>', None, 'no_text'),
        (
            ['-'],
            b'<html><body><script>document.write("article")</script></body></html>',
            None,
            'no_text',
        ),
        (
            ['-'],
            b'<html><body><nav>Menu</nav><article><h1>Synthetic lighthouse</h1>'
            b'<p>Bright harbor <a href="https://example.test">map</a>.</p>'
            b'<img src="https://example.test/beacon.png" width="640" height="480">'
            b'</article><footer>Footer</footer></body></html>',
            'Bright harbor [map](https://example.test/).\n\n![](https://example.test/beacon.png)',
            None,
        ),
        ([str(home / 'missing')], None, None, 'FileNotFoundError'),
    ]:
        result = subprocess.run(
            command + ['extract', *args, '--json'], input=body, env=env, capture_output=True
        )
        assert result.returncode == (4 if error else 0), result.stderr
        payload = json.loads(result.stdout)
        assert payload['error'] == error
        assert payload['pages'] is None
        assert payload['fetched'] is None
        if body is None or b'Synthetic lighthouse' not in body:
            assert {key: payload[key] for key in ('title', 'author', 'published')} == dict(
                title=None, author=None, published=None
            )
        if expected_text is not None:
            assert payload['text'].strip() == expected_text
            assert payload['kind'] == 'text'
        validated = subprocess.run(
            [sys.executable, '-I', str(Path(__file__).with_name('validate.py')), str(contract)],
            input=result.stdout,
            capture_output=True,
        )
        assert validated.returncode == 0, validated.stdout
        assert snapshot() == before


def test_extract_metadata_subprocess(tmp_path):
    source = tmp_path / 'article.html'
    source.write_text(
        '<html><head><title>Synthetic lighthouse</title>'
        '<meta name="author" content="Ada Example">'
        '<meta property="article:published_time" content="2026-01-02">'
        '</head><body><article><p>Bright harbor and lighthouse.</p></article></body></html>'
    )
    result = subprocess.run(
        [sys.executable, '-m', 'qmd2.cli', 'extract', '--json', str(source)],
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload['title'] == 'Synthetic lighthouse'
    assert payload['author'] == 'Ada Example'
    assert payload['published'] == '2026-01-02'


class ExtractHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        assert self.headers['User-Agent'] == 'Mozilla/5.0'
        if self.path == '/redirect':
            self.send_response(302)
            self.send_header('Location', '/article')
            self.end_headers()
            return
        if self.path[1:].isdigit():
            self.send_error(int(self.path[1:]))
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(
            b'<html><head><title>Synthetic lighthouse</title></head>'
            b'<body><p>Bright harbor and lighthouse.</p></body></html>'
            if self.path == '/article'
            else b'<html><body><script>document.write("article")</script></body></html>'
        )

    def log_message(self, *args):
        pass


def test_extract_url_http_subprocess(tmp_path):
    home = tmp_path / 'home'
    home.mkdir()
    env = dict(os.environ, HOME=str(home), TMPDIR=str(home), PYTHONDONTWRITEBYTECODE='1')
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), ExtractHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f'http://localhost:{server.server_port}'
        for path, status, final, error in [
            ('/article', 200, '/article', None),
            ('/redirect', 200, '/article', None),
            ('/empty', 200, '/empty', 'no_text'),
            ('/403', 403, '/403', 'mullvad_key_missing'),
            ('/451', 451, '/451', 'mullvad_key_missing'),
            ('/404', 404, '/404', 'media_expired (HTTP 404)'),
            ('/410', 410, '/410', 'media_expired (HTTP 410)'),
            ('/429', 429, '/429', 'mullvad_key_missing'),
            ('/503', 503, '/503', 'mullvad_key_missing'),
        ]:
            result = subprocess.run(
                [
                    sys.executable,
                    '-m',
                    'qmd2.cli',
                    '--home',
                    str(home / 'qmd'),
                    'extract',
                    '--json',
                    url + path,
                ],
                env=env,
                capture_output=True,
            )
            payload = json.loads(result.stdout)
            assert result.returncode == (4 if error else 0), payload
            assert payload['error'] == error
            fetched = payload['fetched']
            assert {k: fetched[k] for k in ('via', 'status', 'final_url')} == dict(
                via='direct', status=status, final_url=url + final
            )
            assert len(fetched['attempts']) == 1
            attempt = fetched['attempts'][0]
            assert (attempt['via'], attempt['relay'], attempt['status']) == ('direct', None, status)
            assert attempt['elapsed_s'] >= 0
            if not error:
                assert payload['text'] == 'Bright harbor and lighthouse.'
                assert payload['title'] == 'Synthetic lighthouse'
            assert not list(home.iterdir())
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
