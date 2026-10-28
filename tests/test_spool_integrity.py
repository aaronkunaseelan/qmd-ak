"""Spool filesystem integrity across raw ingestion routes."""

import hashlib
import json
import time

import pytest

from qmd2.ingestd import Ingestd, replace_receipt
from qmd2.verify import evaluate

STAMP = '2026-10-28T12:00:00Z'


@pytest.fixture
def daemon(tmp_path):
    writer = Ingestd(tmp_path, remote=False)
    yield writer
    writer.upload_pool.shutdown()
    for stream in writer.packs.files.values():
        stream.close()
    writer.state.db.close()
    writer.lock.close()


def spool_entry(root, source='x', labels=None):
    payload = (
        json.dumps(
            {'tweet': {'id': 'proof', 'text': 'orchid', 'createdAt': STAMP}}
            if source == 'x'
            else {
                'type': 'message',
                'id': 'proof',
                'timestamp': STAMP,
                'message': {'role': 'user', 'content': 'orchid'},
            }
        ).encode()
        + b'\n'
    )
    root.mkdir(parents=True)
    (root / 'payload').write_bytes(payload)
    return {
        'schema': 'qmd.spool-entry.v1',
        'source': source,
        'account': 'synthetic',
        'key': 'proof',
        'path': 'payload',
        'bytes': len(payload),
        'sha256': hashlib.sha256(payload).hexdigest(),
        'captured_at': STAMP,
        'state': {'labels': labels or {}},
    }


@pytest.mark.parametrize('route', ['parsed', 'archive-only', 'reset-history', 'session'])
@pytest.mark.parametrize('damage', ['hash', 'size', None])
def test_spool_rejects_damaged_payload_filesystem(daemon, route, damage):
    root = daemon.home / 'spool/proof'
    entry = spool_entry(
        root,
        source='pi' if route == 'session' else 'x',
        labels={'archive_only': 'true'} if route == 'archive-only' else {},
    )
    if route == 'reset-history':
        daemon.queue_checkpoint('reset:x', 2_000_000_000_000_000_000)
    if damage == 'hash':
        path = root / entry['path']
        path.write_bytes(path.read_bytes().replace(b'orchid', b'violet'))
    elif damage == 'size':
        entry['bytes'] -= 1
    index = root / 'index.jsonl'
    index.write_text(json.dumps(entry) + '\n')
    daemon.spool(seconds=2)
    daemon.commit()
    offset = (daemon.state.get('spool:' + str(index)) or {'offset': 0})['offset']
    raw = [
        header
        for pack in daemon.state.packs('raw')
        for _, header, _ in daemon.packs.iter_pack(pack)
    ]
    accepted = any(
        header['sha256'] != entry['sha256'] or header['len'] != entry['bytes'] for header in raw
    )
    assert offset == (index.stat().st_size if damage is None else 0) and not accepted, (
        f'{route}/{damage}: acknowledged={offset == index.stat().st_size}, '
        f'archived_mismatch={accepted}'
    )


def test_malformed_spool_blocks_visibly_filesystem(daemon):
    now = time.time()
    anchor_stamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(now - 60))
    root = daemon.home / 'spool/x'
    entry = spool_entry(root)
    index = root / 'index.jsonl'
    index.write_text('{broken\n' + json.dumps(entry) + '\n')
    receipt = replace_receipt(
        {},
        {
            'ok': True,
            'enumerated': 1,
            'written': 1,
            'unchanged': 0,
            'excluded': 0,
            'failed': 0,
            'source_head': now,
        },
        now,
    )
    daemon.record(
        'x',
        json.dumps(
            {'tweet': {'id': 'anchor', 'text': 'existing', 'createdAt': anchor_stamp}}
        ).encode(),
        {'account': 'synthetic', 'key': 'anchor', 'time': anchor_stamp},
    )
    daemon.queue_checkpoint('receipt:x', receipt)
    daemon.spool(seconds=2)
    daemon.commit()
    status = json.loads((daemon.home / 'status.json').read_text())
    status['sources'] = {'x': status['sources']['x']}
    status.pop('attachments')
    checks = []
    red = evaluate(status, now, checks)
    assert 'spool:blocked' in red
    assert 'JSONDecodeError' in next(c['reason'] for c in checks if c['name'] == 'spool:blocked')
    offset = (daemon.state.get('spool:' + str(index)) or {'offset': 0})['offset']
    assert offset == 0 and not daemon.index.search('orchid')
    assert red, (
        f'valid row blocked; offset={offset}, '
        f'oldest_pending={status["sources"]["x"].get("oldest_pending")}, red_checks={red}'
    )
    index.write_text(json.dumps(entry) + '\n')
    daemon.spool(seconds=2)
    daemon.commit()
    daemon.last_status = float('-inf')
    daemon.publish_status()
    status = json.loads((daemon.home / 'status.json').read_text())
    assert 'spool:blocked' not in evaluate(status, time.time())
    assert daemon.index.search('orchid')
