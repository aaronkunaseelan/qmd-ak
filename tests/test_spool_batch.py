import hashlib
import json
import time

from qmd2.ingestd import Ingestd


def test_10k_context_spool_filesystem_tantivy(tmp_path):
    spool = tmp_path / 'spool/whatsapp'
    spool.mkdir(parents=True)
    aliases = {f'{n}@lid': f'{n}@s.whatsapp.net' for n in range(32)}
    update = {
        'aliases': aliases,
        'contacts': {v: f'Synthetic sender {n}' for n, v in enumerate(aliases.values())},
        'groups': {f'{n}@g.us': f'Synthetic group {n}' for n in range(8)},
    }
    stamp = '2026-10-13T00:00:00Z'
    with (spool / 'index.jsonl').open('w') as index:
        for n in range(10000):
            raw = json.dumps(
                {
                    'ctx_update': update,
                    'decoded': {
                        'kind': 'message',
                        'id': str(n),
                        'chat': '1@lid',
                        'sender': '1@lid',
                        'timestamp': stamp,
                        'text': 'Synthetic lighthouse',
                    },
                }
            ).encode()
            path = spool / str(n)
            path.write_bytes(raw)
            entry = {
                'schema': 'qmd.spool-entry.v1',
                'source': 'whatsapp',
                'account': 'synthetic',
                'key': str(n),
                'path': path.name,
                'bytes': len(raw),
                'sha256': hashlib.sha256(raw).hexdigest(),
                'captured_at': stamp,
                'state': {'labels': {}},
            }
            index.write(json.dumps(entry) + '\n')
    daemon = Ingestd(tmp_path, remote=False)
    statements = []
    daemon.state.db.set_trace_callback(statements.append)
    started = time.monotonic()
    daemon.spool_file(spool / 'index.jsonl', 120)
    daemon.commit()
    elapsed = time.monotonic() - started
    assert daemon.stats['records'] == 10000
    assert (
        daemon.state.get('spool:' + str(spool / 'index.jsonl'))['offset']
        == (spool / 'index.jsonl').stat().st_size
    )
    assert daemon.state.get('ctx:whatsapp')['aliases'] == aliases
    assert sum(sql == 'COMMIT' for sql in statements) <= int(elapsed / 10) + 2
    assert (
        sum(
            sql.startswith('INSERT OR REPLACE INTO checkpoints') and "'ctx:whatsapp'" in sql
            for sql in statements
        )
        == 1
    )
    for row in daemon.state.packs('raw'):
        for _, header, body in daemon.packs.iter_pack(row):
            assert set(header) == {
                'arrived',
                'id',
                'source',
                'stream',
                'version',
                'time',
                'kind',
                'len',
                'sha256',
            }
            assert 'aliases' not in header['version'] and 'contacts' not in header['version']
            assert len(json.dumps(header)) < 600
            assert hashlib.sha256(body).hexdigest() == header['sha256']
    hit = daemon.index.search('lighthouse')[0]
    assert hit['id'].startswith('wa:synthetic:1@s.whatsapp.net:')
