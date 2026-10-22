import hashlib
import json
import resource
import pytest

from qmd2.ingestd import Ingestd


@pytest.mark.parametrize('source', ['files', 'legacy'])
def test_archive_only_100mb_filesystem_stream(tmp_path, source, monkeypatch):
    monkeypatch.setattr("qmd2.packs.FRAME_MAX", 16 * 1024 * 1024)
    spool = tmp_path / 'spool/import-files'
    spool.mkdir(parents=True)
    block = b'synthetic original bytes\n' * (1024 * 1024 // 25)
    size = 100 * 1024 * 1024
    digest = hashlib.sha256()
    with (spool / 'payload').open('wb') as f:
        remaining = size
        while remaining:
            chunk = block[: min(remaining, len(block))]
            f.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
    entry = {
        'schema': 'qmd.spool-entry.v1',
        'source': source,
        'account': 'synthetic',
        'key': 'archive',
        'path': 'payload',
        'bytes': size,
        'sha256': digest.hexdigest(),
        'captured_at': '2026-10-13T00:00:00Z',
        'state': {
            'version': 'original-v1',
            'labels': {
                'archive_only': 'true',
                'object_hash': digest.hexdigest(),
                'custody_sha256': digest.hexdigest(),
                'old_catalog_location': 'synthetic:original',
                'size': str(size),
                'source': source,
            },
        },
    }
    (spool / 'index.jsonl').write_text(json.dumps(entry) + '\n')
    daemon = Ingestd(tmp_path, remote=False)
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    daemon.spool(seconds=30)
    daemon.commit()
    row = daemon.state.packs('raw')[0]
    assert row['sealed']
    frames = daemon.packs.frame_map({'stream': 'raw', 'source': source, 'seq': row['seq']})
    daemon.state.db.execute('UPDATE packs SET frames=NULL')
    daemon.state.commit()
    assert daemon.packs.frame_map({'stream': 'raw', 'source': source, 'seq': row['seq']}) == frames
    assert len(frames) == 7 and all(f[1] <= 16 * 1024 * 1024 for f in frames)
    [(loc, head, body)] = list(daemon.packs.iter_pack(row))
    with (tmp_path / "copied").open("wb") as target:
        daemon.packs.write_raw(loc, target)
    with (tmp_path / "copied").open("rb") as target:
        assert hashlib.file_digest(target, "sha256").hexdigest() == digest.hexdigest()
    assert head['sha256'] == digest.hexdigest() and head['len'] == size and body is None
    assert head['labels']['custody_sha256'] == digest.hexdigest()
    assert head['labels']['old_catalog_location'] == 'synthetic:original'
    if source == 'legacy':
        assert head['version'] == 'custody-unversioned-v1'
        assert 'legacy' not in json.loads((tmp_path / 'status.json').read_text())['sources']
    assert not daemon.index.search('synthetic')
    assert (
        daemon.state.get('spool:' + str(spool / 'index.jsonl'))['offset']
        == (spool / 'index.jsonl').stat().st_size
    )
    assert resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - before < 50 * 1024


def test_receipt_time_boundary(tmp_path):
    from qmd2.verify import evaluate

    daemon = Ingestd(tmp_path, remote=False)
    receipt = {
        'source': 'gmail',
        'ok': True,
        'enumerated': 0,
        'written': 0,
        'unchanged': 0,
        'excluded': 0,
        'failed': 0,
        'duration_s': 1,
        'source_head': '2026-10-13T14:00:00+02:00',
        'connection_head': '2026-10-13T12:00:00Z',
        'last_complete': '2026-10-13T12:00:00Z',
        'oldest_failed_at': None,
        'searchable_through': 1791892800,
        'captured_at': '2026-10-13T12:00:00Z',
    }
    import base64

    payload = {
        'id': 'abc123',
        'internalDate': 1791892800000,
        'raw': base64.urlsafe_b64encode(
            b'Subject: Synthetic boundary\nFrom: fixture@example.test\n\nSynthetic lighthouse'
        ).decode(),
    }
    daemon.record(
        'gmail',
        json.dumps(payload).encode(),
        {'account': 'synthetic', 'key': 'abc123', 'time': '2026-10-13T12:00:00Z'},
    )
    receipt['newest_id'] = 'abc123'
    daemon.queue_checkpoint('receipt:gmail', receipt)
    daemon.commit()
    status = json.loads((tmp_path / 'status.json').read_text())
    normalized = status['sources']['gmail']['receipt']
    assert normalized['newest_id'] == 'abc123'
    assert status['sources']['gmail']['newest_id'] == 'gmail:synthetic:abc123'
    assert normalized['source_head'] == 1791892800
    assert normalized['connection_head'] == 1791892800
    assert normalized['captured_at'] == 1791892800
    assert normalized['oldest_failed_at'] is None
    assert not any(check.startswith('gmail:') for check in evaluate(status, 1791892800))
