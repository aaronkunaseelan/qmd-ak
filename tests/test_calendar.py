import json

import pytest

from qmd2.sources.calendar import parse


@pytest.mark.parametrize(
    'row,kind,failed',
    [
        (
            {
                'id': 'event',
                'summary': 'Synthetic meeting',
                'start': {'dateTime': '2026-10-13T00:00:00Z'},
                'end': {'dateTime': '2026-10-13T01:00:00Z'},
                'description': 'Synthetic agenda',
            },
            'event',
            False,
        ),
        (
            {'id': 'event', 'status': 'cancelled', 'updated': '2026-10-13T01:00:00Z'},
            'tombstone',
            False,
        ),
        ({'id': 'all-day', 'start': {'date': '2026-10-13'}}, 'event', False),
        ({}, 'event', True),
        (None, 'event', True),
    ],
)
def test_parser_table(row, kind, failed):
    doc = parse(
        json.dumps(row).encode(),
        {'calendar_id': 'synthetic', 'key': 'event', 'time': '2026-10-13T00:00:00Z'},
    )[0]
    assert doc['kind'] == kind
    assert bool(doc.get('parse_failed')) == failed
    assert doc['id'].startswith('calendar:synthetic:')


@pytest.mark.parametrize(
    'local,zone,expected',
    [
        ('2026-10-24T09:00:00', 'Europe/Athens', '2026-10-24T06:00:00+00:00'),
        ('2026-10-25T09:00:00', 'Europe/Athens', '2026-10-25T07:00:00+00:00'),
        ('2026-10-25T03:30:00+03:00', 'Europe/Athens', '2026-10-25T00:30:00+00:00'),
        ('2026-10-25T03:30:00+02:00', 'Europe/Athens', '2026-10-25T01:30:00+00:00'),
        ('2026-10-25T09:00:00', 'America/New_York', '2026-10-25T13:00:00+00:00'),
    ],
)
def test_calendar_named_zone_filesystem(tmp_path, local, zone, expected):
    from qmd2.doc import date
    from qmd2.ingestd import Ingestd

    daemon = Ingestd(tmp_path, remote=False)
    try:
        event = {
            'id': 'event',
            'start': {'dateTime': local, 'timeZone': zone},
            'end': {'dateTime': local, 'timeZone': zone},
        }
        raw = json.dumps(event).encode()
        ctx = {'key': 'synthetic:event', 'time': '2026-10-20T00:00:00Z'}
        # Before calendar_id was retained, the capture key still held the native calendar id.
        daemon.record('calendar', raw, ctx, parse(raw, dict(ctx, calendar_id='synthetic')))
        daemon.commit()
        for reparse in (False, True):
            if reparse:
                while not daemon.reindex('calendar', seconds=60):
                    pass
            hit = daemon.index.lookup('calendar:synthetic:event')
            assert hit
            doc = json.loads(daemon.packs.read(hit['pointer'])[1])
            assert date(doc['time']).isoformat() == expected
            assert date(doc['context']['end']).isoformat() == expected
    finally:
        daemon.upload_pool.shutdown()
        daemon.index.writer.wait_merging_threads()
        daemon.state.db.close()
        daemon.lock.close()


@pytest.mark.parametrize(
    'end,where,expected',
    [
        ({'date': '2026-10-14'}, 'Synthetic Hall', '2026-10-14'),
        ({'dateTime': '2026-10-14T12:00:00+03:00'}, 'Synthetic Hall', '2026-10-14T09:00:00+00:00'),
        ({}, '', None),
    ],
)
def test_calendar_browse_subprocess_pack_edges(tmp_path, end, where, expected):
    import subprocess
    import sys
    from qmd2.ingestd import Ingestd

    raw = json.dumps(
        dict(id='event', start={'date': '2026-10-13'}, end=end, location=where)
    ).encode()
    daemon = Ingestd(tmp_path, remote=False)
    try:
        daemon.record('calendar', raw, {'key': 'synthetic:event', 'time': '2026-10-13'})
        daemon.commit()
        result = subprocess.run(
            [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path), 'browse', '--json'],
            check=True,
            capture_output=True,
        )
        row = json.loads(result.stdout)['results'][0]
        assert row['where'] == (where or None) and row['end'] == expected
        assert 'pointer' not in row and 'version' not in row
        assert 'context' in row and 'item' in row
        assert daemon.index.lookup(row['id'])['pointer']
    finally:
        daemon.upload_pool.shutdown()
        daemon.index.writer.wait_merging_threads()
        daemon.state.db.close()
        daemon.lock.close()


def test_calendar_spool_chain(tmp_path):
    import hashlib
    from qmd2.ingestd import Ingestd
    from qmd2.doc import render

    spool = tmp_path / 'spool' / 'calendar'
    spool.mkdir(parents=True)
    from qmd2.doc import date
    import time

    updated = '2026-10-20T10:00:00Z'
    row = {
        'id': 'event',
        'summary': 'Synthetic rendezvous',
        'description': '<p>lighthouse &amp; agenda</p>',
        'start': {'dateTime': '2027-10-20T10:00:00+03:00'},
        'updated': updated,
    }
    raw = json.dumps(row).encode()
    (spool / 'payload').write_bytes(raw)
    entry = {
        'schema': 'qmd.spool-entry.v1',
        'source': 'calendar',
        'account': 'synthetic',
        'key': 'synthetic:event',
        'path': 'payload',
        'bytes': len(raw),
        'sha256': hashlib.sha256(raw).hexdigest(),
        'captured_at': '2026-10-20T10:01:00Z',
        'item_time': updated,
        'state': {'labels': {'calendar_id': 'synthetic'}},
    }
    (spool / 'index.jsonl').write_text(json.dumps(entry) + '\n')
    daemon = Ingestd(tmp_path, remote=False)
    daemon.spool()
    daemon.commit()
    hit = daemon.index.search('lighthouse')[0]
    assert hit['id'] == 'calendar:synthetic:event'
    doc = json.loads(daemon.packs.read(hit['pointer'])[1])
    from qmd2.reads import Reader

    reader = Reader(tmp_path)
    try:
        fetched = reader.get(doc['id'])[0]
        assert fetched['id'] == doc['id'] and fetched['parts'] == doc['parts']
    finally:
        reader.close()
    assert 'lighthouse & agenda' in render(doc)
    assert daemon.packs.read(doc['raw'])[1] == raw
    expected = date(updated).timestamp()
    status = daemon.source_status('calendar', 900, daemon.index, time.time())
    assert status['processed_through'] == expected
    assert status['searchable_through'] == expected
    # Scoped maintenance must lower a persisted, poisoned maximum, not only stop new poison.
    daemon.queue_checkpoint(
        'indexed:calendar',
        dict(
            daemon.checkpoint('indexed:calendar'),
            processed_through=1823668200,
            searchable_through=1823668200,
        ),
    )
    daemon.queue_checkpoint('receipt:calendar', {'searchable_through': 1823668200})
    daemon.commit()
    while not daemon.reindex('calendar', seconds=60):
        pass
    status = daemon.source_status('calendar', 900, daemon.index, time.time())
    assert status['processed_through'] == status['searchable_through'] == expected
    progress = expected
    cancelled = dict(row, status='cancelled', updated='2026-10-20T11:00:00Z')
    daemon.record(
        'calendar', json.dumps(cancelled).encode(), {'calendar_id': 'synthetic', 'key': 'event'}
    )
    daemon.commit()
    assert daemon.index.search('lighthouse') == []
    assert daemon.checkpoint('indexed:calendar')['searchable_through'] == progress
    from qmd2.sources import SOURCES

    lane = next(row for row in SOURCES if row[0] == 'calendar')
    assert lane[2][1] == 'calendar' and lane[3] == 900
