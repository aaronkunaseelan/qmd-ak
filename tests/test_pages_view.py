from qmd2.attachments import output_doc, pages_view


def doc_of(row):
    parent = dict(id='manual:synthetic', source='manual', group='files', time='2026-10-13')
    return output_doc(
        dict(
            row,
            id='manual:synthetic',
            parent=parent,
            ref={'source': 'manual'},
            media_sha256='synthetic',
        ),
        'Lighthouse harbor',
        'synthetic-model',
        'ocr',
    )


def test_pages_view_azure_count():
    row = dict(pages_read=4, total_pages=4, pages_requested=None, azure_pages=4)
    assert pages_view(row) == dict(covered=4, total=4, requested=None, azure=4)
    assert doc_of(row)['pages'] == pages_view(row)


def test_pages_view_text_layer_zero():
    row = dict(pages_read=3, total_pages=3, pages_requested=2, azure_pages=0)
    assert pages_view(row) == dict(covered=3, total=3, requested=2, azure=0)
    assert doc_of(row)['pages']['azure'] == 0


def test_pages_view_unknown_omits_azure():
    row = dict(pages_read=2, total_pages=3, pages_requested=2)
    assert pages_view(row) == dict(covered=2, total=3, requested=2)
    assert 'azure' not in doc_of(row)['pages']
