"""Current-document and uploaded-pack filesystem checks for cached status pointers."""

import json
import time

import pytest

from qmd2.ingestd import Ingestd


@pytest.fixture
def daemon(tmp_path):
    daemon = Ingestd(tmp_path, remote=False)
    for key in ('one', 'two'):
        raw = json.dumps(
            {
                'bookmark': {'bookmark_id': key, 'time': 1793059200, 'title': 'Synthetic'},
                'html': '<p>lighthouse</p>',
            }
        ).encode()
        daemon.record(
            'instapaper',
            raw,
            {
                'account': 'synthetic',
                'key': key,
                'time': '2026-10-27T00:00:00Z',
            },
        )
    daemon.commit()
    yield daemon
    daemon.upload_pool.shutdown()
    for stream in daemon.packs.files.values():
        stream.close()
    daemon.state.db.close()
    daemon.lock.close()


def remove(daemon, key):
    old = daemon.index.lookup('instapaper:synthetic:' + key)
    daemon.documents(
        'instapaper', [daemon.tombstone('instapaper', old)], old['version']['raw'], time.time_ns()
    )
    daemon.commit()


def test_empty_source_clears_newest_pointer_filesystem(daemon):
    for key in ('one', 'two'):
        remove(daemon, key)
    row = daemon.source_status('instapaper', 720, daemon.index, time.time())
    assert row['newest_id'] is None
    daemon.commit()
    assert daemon.state.get('indexed:instapaper').get('newest_id') is None


@pytest.mark.parametrize('damage', ['deleted', 'not-uploaded', 'moved'])
def test_cold_sample_requires_current_uploaded_pointer_filesystem(daemon, damage):
    packs = daemon.packs
    pack = daemon.state.packs('doc')[0]
    packs.seal('doc', 'instapaper', pack['seq'])
    daemon.state.db.execute('UPDATE packs SET uploaded=1 WHERE stream=\'doc\'')
    sample = 'instapaper:synthetic:one'
    daemon.state.set('cold_sample_id', sample)
    daemon.state.commit()
    assert daemon.status_cold_sample(daemon.index) == sample
    if damage == 'deleted':
        remove(daemon, 'one')
    elif damage == 'not-uploaded':
        daemon.state.db.execute('UPDATE packs SET uploaded=0 WHERE stream=\'doc\'')
    else:
        raw = json.dumps(
            {
                'bookmark': {'bookmark_id': 'one', 'time': 1793145600, 'title': 'Updated'},
                'html': '<p>new version</p>',
            }
        ).encode()
        daemon.record(
            'instapaper',
            raw,
            {
                'account': 'synthetic',
                'key': 'one',
                'time': '2026-10-28T00:00:00Z',
            },
        )
        daemon.commit()
    daemon.state.commit()
    chosen = daemon.status_cold_sample(daemon.index)
    if damage == 'not-uploaded':
        assert chosen is None
    else:
        assert chosen == 'instapaper:synthetic:two'
    daemon.commit()
    assert daemon.state.get('cold_sample_id') == chosen
