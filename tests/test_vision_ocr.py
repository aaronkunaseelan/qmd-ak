"""An image or a PDF scan goes to m3's Apple Vision first; Azure reads what Vision cannot (X-99)."""

import http.server
import json
import threading
import time

import pytest

from qmd2 import attachments
from qmd2.doc import render

IMAGE = b'\x89PNG\r\n\x1a\n' + b'synthetic pixels' * 64
PDF = b'%PDF-1.4\nsynthetic scan\n'
TEXT = 'Synthetic lighthouse harbor'


def pages(text=TEXT, latin=0.99, greek=0.0, cyrillic=0.0):
    return [
        dict(
            index=0,
            text=text,
            chars=len(text),
            script=dict(latin=latin, greek=greek, cyrillic=cyrillic, other=0.01),
        )
    ]


class Ocr:
    """A fake m3-ocr: one file part, a bearer token, and whatever answer the test sets."""

    status = 200
    answer = dict(engine='apple-vision@27.2/rev3', secs=0.2, pages=pages())

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers['Content-Length']))
            assert (self.path, self.headers['Authorization']) == (
                '/v1/ocr',
                'Bearer synthetic-token',
            )
            assert b'Content-Disposition: form-data; name="file"' in body
            assert body.endswith(b'--qmd2vision--\r\n')
            self.send_response(Ocr.status)
            self.end_headers()
            self.wfile.write(json.dumps(Ocr.answer).encode())

        def log_message(self, *args):
            pass


class Slow(http.server.BaseHTTPRequestHandler):
    """Accepts the request and holds the answer past the client's read timeout."""

    def do_POST(self):
        self.rfile.read(int(self.headers['Content-Length']))
        time.sleep(2)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


class Broken(http.server.BaseHTTPRequestHandler):
    """Answers 200, opens a chunk it never finishes, then closes: the client sees IncompleteRead."""

    def do_POST(self):
        self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(200)
        self.send_header('Transfer-Encoding', 'chunked')
        self.end_headers()
        self.wfile.write(b'ff\r\n{"eng')
        self.wfile.flush()
        self.close_connection = True

    def log_message(self, *args):
        pass


@pytest.fixture
def vision(tmp_path, monkeypatch):
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Ocr.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(attachments, 'OCR_CONFIG', client(tmp_path, server.server_port))
    yield server
    server.shutdown()
    server.server_close()
    thread.join()


def client(tmp_path, port):
    config = tmp_path / 'client.json'
    config.write_text(json.dumps(dict(url=f'http://127.0.0.1:{port}', token='synthetic-token')))
    return config


def azure(calls, monkeypatch, text=TEXT):
    """Azure's answer without Azure: the model it was asked for is what the test reads back."""

    def fake(path, model, pages=None):
        calls.append((model, pages))
        return text, f'azure-document-intelligence:{model}@2024-11-30', 1

    monkeypatch.setattr(attachments, 'azure_di', fake)


def image(tmp_path):
    path = tmp_path / 'photo.png'
    path.write_bytes(IMAGE)
    return path


def test_a_latin_image_is_read_by_vision_and_labelled(vision, tmp_path, monkeypatch):
    azure_calls = []
    azure(azure_calls, monkeypatch)
    row = dict(ref={})
    text, model, tag = attachments.extract(row, image(tmp_path))
    assert (text, model, tag) == (TEXT, 'apple-vision@27.2/rev3', 'ocr')
    assert row['model'] == 'apple-vision@27.2/rev3'
    assert row['azure_pages'] == 0
    assert 'vision' not in row
    assert azure_calls == []


def test_a_cyrillic_signature_sends_the_image_to_azure(vision, tmp_path, monkeypatch):
    monkeypatch.setattr(
        Ocr, 'answer', dict(engine='apple-vision@27.2/rev3', secs=0.2, pages=pages(cyrillic=0.42))
    )
    azure_calls = []
    azure(azure_calls, monkeypatch)
    row = dict(ref={})
    text, model, tag = attachments.extract(row, image(tmp_path))
    assert (model, tag) == ('azure-document-intelligence:prebuilt-read@2024-11-30', 'ocr')
    assert row['vision'] == 'script_cyrillic'
    assert azure_calls == [('prebuilt-read', None)]


def test_an_unreachable_m3_falls_back_to_azure(tmp_path, monkeypatch):
    monkeypatch.setattr(attachments, 'OCR_CONFIG', client(tmp_path, 1))
    azure_calls = []
    azure(azure_calls, monkeypatch)
    row = dict(ref={})
    text, model, _ = attachments.extract(row, image(tmp_path))
    assert (text, model) == (TEXT, 'azure-document-intelligence:prebuilt-read@2024-11-30')
    assert row['vision'] == 'unreachable'
    assert azure_calls == [('prebuilt-read', None)]


def test_a_m3_that_does_not_answer_in_time_falls_back_to_azure(tmp_path, monkeypatch):
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Slow)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setattr(attachments, 'OCR_CONFIG', client(tmp_path, server.server_port))
        monkeypatch.setattr(attachments, 'OCR_READ_SECONDS', 0.2)
        azure_calls = []
        azure(azure_calls, monkeypatch)
        row = dict(ref={})
        _, model, _ = attachments.extract(row, image(tmp_path))
        assert model == 'azure-document-intelligence:prebuilt-read@2024-11-30'
        assert row['vision'] == 'timeout'
        assert azure_calls == [('prebuilt-read', None)]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_a_non_200_answer_falls_back_to_azure(vision, tmp_path, monkeypatch):
    monkeypatch.setattr(Ocr, 'status', 503)
    azure_calls = []
    azure(azure_calls, monkeypatch)
    row = dict(ref={})
    _, model, _ = attachments.extract(row, image(tmp_path))
    assert model == 'azure-document-intelligence:prebuilt-read@2024-11-30'
    assert row['vision'] == 'http_503'
    assert azure_calls == [('prebuilt-read', None)]


def test_a_missing_client_config_keeps_azure(tmp_path, monkeypatch):
    monkeypatch.setattr(attachments, 'OCR_CONFIG', tmp_path / 'absent.json')
    azure_calls = []
    azure(azure_calls, monkeypatch)
    row = dict(ref={})
    _, model, _ = attachments.extract(row, image(tmp_path))
    assert model == 'azure-document-intelligence:prebuilt-read@2024-11-30'
    assert row['vision'] == 'no_config'
    assert azure_calls == [('prebuilt-read', None)]


def test_an_empty_vision_read_is_no_text_and_azure_is_not_paid(vision, tmp_path, monkeypatch):
    monkeypatch.setattr(
        Ocr, 'answer', dict(engine='apple-vision@27.2/rev3', secs=0.2, pages=pages(text=''))
    )
    azure_calls = []
    azure(azure_calls, monkeypatch)
    row = dict(ref={})
    with pytest.raises(attachments.Unavailable, match='no_text'):
        attachments.extract(row, image(tmp_path))
    assert row['model'] == 'apple-vision@27.2/rev3'
    assert row['azure_pages'] == 0
    assert 'vision' not in row
    assert azure_calls == []


def test_a_broken_http_answer_is_unreachable_not_a_crash(tmp_path, monkeypatch):
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Broken)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setattr(attachments, 'OCR_CONFIG', client(tmp_path, server.server_port))
        azure_calls = []
        azure(azure_calls, monkeypatch)
        row = dict(ref={})
        text, model, _ = attachments.extract(row, image(tmp_path))
        assert (text, model) == (TEXT, 'azure-document-intelligence:prebuilt-read@2024-11-30')
        assert row['vision'] == 'unreachable'
        assert azure_calls == [('prebuilt-read', None)]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_a_scan_pdf_goes_to_vision_before_azure(vision, tmp_path, monkeypatch):
    azure_calls = []
    azure(azure_calls, monkeypatch)
    source = tmp_path / 'scan.pdf'
    source.write_bytes(PDF)
    row = dict(ref={})
    text, model, tag = attachments.extract(row, source)
    assert (text, model, tag) == (TEXT, 'apple-vision@27.2/rev3', 'text')
    assert row['text_layer'] == 'pdftotext_failed'
    assert row['azure_pages'] == 0
    assert 'vision' not in row
    assert azure_calls == []


def test_a_vision_image_doc_covers_its_one_page(vision, tmp_path, monkeypatch):
    azure_calls = []
    azure(azure_calls, monkeypatch)
    row = dict(
        id='wa:synthetic:chat:1/att/0',
        parent=dict(
            id='wa:synthetic:chat:1',
            source='whatsapp',
            group='mail',
            time='2026-10-13T00:00:00Z',
        ),
        ref={'source': 'whatsapp', 'type': 'image', 'fileName': 'synthetic.png'},
        media_sha256='synthetic',
    )
    text, model, kind = attachments.extract(row, image(tmp_path))
    doc = attachments.output_doc(row, text, model, kind)
    assert doc['pages'] == dict(covered=1, total=1, requested=None, azure=0)
    assert 'Extracted' not in render(doc)
    assert row['azure_pages'] == 0 and row['pages_read'] == 1
