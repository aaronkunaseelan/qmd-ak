import hashlib
import json
import pytest
from qmd2.sources.gsd_events import parse


@pytest.mark.parametrize(
    'row,failed',
    [
        (
            {
                'change_set_id': 'one',
                'time': '2026-10-16T00:00:00Z',
                'title': 'Example',
                'actor': 'owner',
                'text': 'upsert item: Lighthouse\n- state: done',
            },
            False,
        ),
        ({}, True),
        (None, True),
    ],
)
def test_parser_table(row, failed):
    doc = parse(json.dumps(row).encode(), {'key': 'one', 'time': '2026-10-16T00:00:00Z'})[0]
    assert bool(doc.get('parse_failed')) == failed
    assert doc['id'] == 'gsd-events:one'


def test_spool_chain(tmp_path):
    from qmd2.ingestd import Ingestd
    from qmd2.doc import render

    spool = tmp_path / 'spool/gsd-events'
    spool.mkdir(parents=True)
    raw = json.dumps(
        {
            'change_set_id': 'one',
            'time': '2026-10-16T00:00:00Z',
            'title': 'Example',
            'actor': 'owner',
            'text': 'upsert item: Lighthouse\n- state: done',
        }
    ).encode()
    (spool / 'payload').write_bytes(raw)
    entry = dict(
        schema='qmd.spool-entry.v1',
        source='gsd-events',
        account='',
        key='one',
        path='payload',
        bytes=len(raw),
        sha256=hashlib.sha256(raw).hexdigest(),
        captured_at='2026-10-16T00:00:00Z',
        state={'labels': {}},
    )
    (spool / 'index.jsonl').write_text(json.dumps(entry) + '\n')
    daemon = Ingestd(tmp_path, remote=False)
    daemon.spool()
    daemon.commit()
    hit = daemon.index.search('lighthouse')[0]
    assert hit['id'] == 'gsd-events:one'
    doc = json.loads(daemon.packs.read(hit['pointer'])[1])
    from qmd2.reads import Reader

    reader = Reader(tmp_path)
    try:
        fetched = reader.get(doc['id'])[0]
        assert fetched['id'] == doc['id'] and fetched['parts'] == doc['parts']
    finally:
        reader.close()
    assert 'state: done' in render(doc)
    assert daemon.packs.read(doc['raw'])[1] == raw
