import hashlib
import json

import pytest

from qmd2.sources.fathom import parse


def meeting():
    return {
        'recording_id': 123,
        'title': 'Synthetic rendezvous',
        'recording_start_time': '2026-10-13T00:00:00Z',
        'recording_end_time': '2026-10-13T01:00:00Z',
        'recorded_by': {'email': 'recorder@example.test'},
        'calendar_invitees': [{'email': 'guest@example.test'}],
        'share_url': 'https://example.test/share',
        'url': 'https://example.test/video',
        'default_summary': {'markdown_formatted': 'Synthetic summary'},
        'action_items': [{'description': 'Synthetic action', 'assignee': {'name': 'Guest'}}],
        'transcript': [
            {
                'timestamp': '01:02',
                'speaker': {'display_name': 'Guest'},
                'text': 'lighthouse &amp; agenda',
            }
        ],
    }


@pytest.mark.parametrize(
    'row,kind,failed',
    [
        (meeting(), 'meeting', False),
        ({'recording_id': '123', 'state': 'deleted'}, 'tombstone', False),
        ({'recording_id': 123, 'transcript': None, 'default_summary': None}, 'meeting', False),
        ({}, 'meeting', True),
        (None, 'meeting', True),
        ({'recording_id': 123, 'transcript': [{}]}, 'meeting', False),
        ({'recording_id': 123, 'default_summary': 'invalid'}, 'meeting', True),
    ],
)
def test_parser_table(row, kind, failed):
    doc = parse(
        json.dumps(row).encode(),
        {'account': 'synthetic', 'key': '123', 'time': '2026-10-13T02:00:00Z'},
    )[0]
    assert doc['id'] == 'fathom:synthetic:123'
    assert doc['kind'] == kind
    assert bool(doc.get('parse_failed')) == failed
    assert doc['edited'] == '2026-10-13T02:00:00Z'


def test_fathom_spool_chain(tmp_path):
    from qmd2.doc import render
    from qmd2.ingestd import Ingestd

    spool = tmp_path / 'spool/fathom'
    spool.mkdir(parents=True)
    raw = json.dumps(meeting()).encode()
    (spool / 'payload').write_bytes(raw)
    entry = {
        'schema': 'qmd.spool-entry.v1',
        'source': 'fathom',
        'account': 'synthetic',
        'key': '123',
        'path': 'payload',
        'bytes': len(raw),
        'sha256': hashlib.sha256(raw).hexdigest(),
        'captured_at': '2026-10-13T02:00:00Z',
        'state': {'labels': {'state': 'meeting'}},
    }
    (spool / 'index.jsonl').write_text(json.dumps(entry) + '\n')
    daemon = Ingestd(tmp_path, remote=False)
    daemon.spool()
    daemon.commit()
    hit = daemon.index.search('lighthouse')[0]
    assert hit['id'] == 'fathom:synthetic:123'
    doc = json.loads(daemon.packs.read(hit['pointer'])[1])
    from qmd2.reads import Reader

    reader = Reader(tmp_path)
    try:
        fetched = reader.get(doc['id'])[0]
        assert fetched['id'] == doc['id'] and fetched['parts'] == doc['parts']
    finally:
        reader.close()
    text = render(doc)
    assert text.index('# Summary') < text.index('# Action items') < text.index('# Transcript')
    assert '[00:01:02] Guest: lighthouse & agenda' in text
    assert 'From: recorder@example.test' in text and 'To: guest@example.test' in text
    assert 'Start: 2026-10-13T00:00:00Z; End: 2026-10-13T01:00:00Z' in text
    assert 'Link: https://example.test/share' in text
    assert daemon.packs.read(doc['raw'])[1] == raw
    changed = meeting()
    changed['default_summary']['markdown_formatted'] = 'Later beacon'
    daemon.record(
        'fathom',
        json.dumps(changed).encode(),
        {'account': 'synthetic', 'key': '123', 'time': '2026-10-14T02:00:00Z'},
    )
    daemon.commit()
    assert daemon.index.search('beacon')[0]['id'] == hit['id']
    daemon.record(
        'fathom',
        b'{"recording_id":123,"state":"deleted"}',
        {'account': 'synthetic', 'key': '123', 'time': '2026-10-15T02:00:00Z'},
    )
    daemon.commit()
    assert not daemon.index.search('beacon')
