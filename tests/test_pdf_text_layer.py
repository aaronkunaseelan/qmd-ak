"""A PDF's own text layer is read locally and free; only a scan goes on to OCR (D-181)."""

import zipfile

import pytest

from qmd2 import attachments
from qmd2.doc import render

# A real PDF, because the rule scores real prose and pdfinfo reads a real file.
PROSE = (
    'Synthetic lighthouse harbor keeps the beacon bright through every long winter night '
    'while the keeper walks the north pier and counts the ships beyond the harbor wall'
)


def make_pdf(path, *pages):
    """A born-digital PDF, one line of Helvetica per page; a page of None draws a line and no text.

    That last page is what a scan looks like to pdftotext: pixels and an empty text layer.
    """
    count = len(pages)
    font = 2 + 2 * count + 1
    kids = ' '.join(f'{3 + 2 * index} 0 R' for index in range(count))
    objects = [
        '<< /Type /Catalog /Pages 2 0 R >>',
        f'<< /Type /Pages /Kids [{kids}] /Count {count} >>',
    ]
    for index, line in enumerate(pages):
        body = '20 20 m 400 400 l S' if line is None else f'BT /F1 12 Tf 20 780 Td ({line}) Tj ET'
        objects.append(
            f'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] '
            f'/Resources << /Font << /F1 {font} 0 R >> >> /Contents {4 + 2 * index} 0 R >>'
        )
        objects.append(f'<< /Length {len(body)} >>\nstream\n{body}\nendstream')
    objects.append('<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>')
    out, offsets = '%PDF-1.4\n', []
    for number, text in enumerate(objects, 1):
        offsets.append(len(out))
        out += f'{number} 0 obj\n{text}\nendobj\n'
    start = len(out)
    out += f'xref\n0 {len(objects) + 1}\n0000000000 65535 f \n'
    out += ''.join(f'{offset:010d} 00000 n \n' for offset in offsets)
    out += f'trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{start}\n%%EOF\n'
    path.write_bytes(out.encode('latin-1'))
    return path


def azure(calls, monkeypatch, text='Synthetic lighthouse harbor'):
    """Azure without Azure: the test reads back the model and the pages it was asked for."""

    def fake(path, model, pages=None):
        calls.append((model, pages))
        return text, f'azure-document-intelligence:{model}@2024-11-30', 1

    monkeypatch.setattr(attachments, 'azure_di', fake)


def no_m3(monkeypatch, consulted):
    """An m3-ocr with no client config: consulted, and answering that it has none."""

    def fake(path):
        consulted.append(path)
        return None, 'no_config', None

    monkeypatch.setattr(attachments, 'vision_ocr', fake)
    return consulted


def doc_row():
    """A parent and a media hash, so an extracted row can become its output doc."""
    return dict(
        id='wa:synthetic:chat:1/att/0',
        parent=dict(
            id='wa:synthetic:chat:1',
            source='whatsapp',
            group='mail',
            time='2026-10-13T00:00:00Z',
        ),
        ref={'source': 'whatsapp', 'type': 'document', 'fileName': 'synthetic.pdf'},
        media_sha256='synthetic',
    )


def doc_of(row, path):
    text, model, kind = attachments.extract(row, path)
    return attachments.output_doc(row, text, model, kind)


def test_a_born_digital_pdf_is_read_from_its_own_text_layer(tmp_path, monkeypatch):
    azure_calls, consulted = [], []
    azure(azure_calls, monkeypatch)
    no_m3(monkeypatch, consulted)
    row = dict(ref={})
    text, model, tag = attachments.extract(row, make_pdf(tmp_path / 'born.pdf', PROSE))
    assert tag == 'text'
    assert 'Synthetic lighthouse harbor' in text
    assert model == 'pdftotext@' + attachments.pdftotext_version()
    assert row['model'] == model
    assert row['azure_pages'] == 0
    assert row['text_layer'] == 'ok'
    assert row['total_pages'] == 1
    assert (consulted, azure_calls) == ([], [])


def test_an_image_only_pdf_is_a_scan_and_goes_on_to_ocr(tmp_path, monkeypatch):
    azure_calls, consulted = [], []
    azure(azure_calls, monkeypatch)
    no_m3(monkeypatch, consulted)
    row = dict(ref={})
    text, model, tag = attachments.extract(row, make_pdf(tmp_path / 'scan.pdf', None))
    assert len(consulted) == 1, 'a scan goes to Vision first'
    assert row['text_layer'].startswith('chars/page')
    assert row['vision'] == 'no_config'
    assert model == 'azure-document-intelligence:prebuilt-layout@2024-11-30'
    assert tag == 'text'
    assert text == 'Synthetic lighthouse harbor'


def test_a_page_selection_is_pdftotext_own_span(tmp_path, monkeypatch):
    asked, azure_calls, consulted = [], [], []
    asked_run = attachments.run

    def record(argv, **options):
        asked.append(argv)
        return asked_run(argv, **options)

    monkeypatch.setattr(attachments, 'run', record)
    azure(azure_calls, monkeypatch)
    no_m3(monkeypatch, consulted)
    source = make_pdf(
        tmp_path / 'three.pdf',
        'OneBeacon ' + PROSE,
        'TwoMarker ' + PROSE,
        'ThreeMarker ' + PROSE,
    )
    row = dict(ref={'pages': '2-3'})
    text, model, _ = attachments.extract(row, source)
    span = [argv for argv in asked if attachments.PDFTOTEXT in argv][0]
    assert span[1:6] == ['-q', '-f', '2', '-l', '3']
    assert 'TwoMarker' in text and 'ThreeMarker' in text and 'OneBeacon' not in text
    assert model.startswith('pdftotext@')
    assert row['pages_requested'] == 2
    assert row['text_layer'] == 'ok'
    assert (consulted, azure_calls) == ([], [])


def test_a_pdftotext_timeout_goes_to_ocr_not_a_failed_row(tmp_path, monkeypatch):
    def late(argv, **options):
        raise attachments.ConverterTimeout('converter deadline')

    monkeypatch.setattr(attachments, 'run', late)
    azure_calls = []
    azure(azure_calls, monkeypatch)
    monkeypatch.setattr(
        attachments,
        'vision_ocr',
        lambda path: ('Synthetic lighthouse harbor read by vision', 'apple-vision@27.2/rev3', 1),
    )
    row = dict(ref={})
    text, model, tag = attachments.extract(row, make_pdf(tmp_path / 'scan.pdf', PROSE))
    assert row['text_layer'] == 'pdftotext_timeout'
    assert model == 'apple-vision@27.2/rev3'
    assert row['azure_pages'] == 0
    assert tag == 'text'
    assert azure_calls == []


def test_a_docx_still_goes_to_azure_layout(tmp_path, monkeypatch):
    azure_calls = []
    azure(azure_calls, monkeypatch)
    monkeypatch.setattr(attachments, 'vision_ocr', lambda path: pytest.fail('a DOCX is not a page'))
    source = tmp_path / 'synthetic.docx'
    with zipfile.ZipFile(source, 'w') as archive:
        archive.writestr('word/document.xml', '<w:document/>')
    row = dict(ref={})
    _, model, tag = attachments.extract(row, source)
    assert (model, tag) == ('azure-document-intelligence:prebuilt-layout@2024-11-30', 'text')
    assert 'text_layer' not in row
    assert azure_calls == [('prebuilt-layout', None)]


def test_a_three_page_text_layer_doc_covers_all_its_pages(tmp_path, monkeypatch):
    azure_calls, consulted = [], []
    azure(azure_calls, monkeypatch)
    no_m3(monkeypatch, consulted)
    doc = doc_of(doc_row(), make_pdf(tmp_path / 'three.pdf', PROSE, PROSE, PROSE))
    assert doc['pages'] == dict(covered=3, total=3, requested=None)
    assert 'Extracted' not in render(doc)
    assert (consulted, azure_calls) == ([], [])


def test_a_span_read_from_the_text_layer_covers_only_that_span(tmp_path, monkeypatch):
    azure_calls, consulted = [], []
    azure(azure_calls, monkeypatch)
    no_m3(monkeypatch, consulted)
    row = doc_row()
    row['ref'] = dict(row['ref'], pages='2-3')
    doc = doc_of(row, make_pdf(tmp_path / 'three.pdf', PROSE, PROSE, PROSE))
    assert doc['pages'] == dict(covered=2, total=3, requested=2)
    assert 'Extracted 2 of 3 pages' in render(doc)
    assert (consulted, azure_calls) == ([], [])
