"""Real filesystem/Tantivy edge: legacy history drains while new capture continues."""

import gc
import json
import time

from qmd2.attachments import process, queue_path, save, snapshot
from qmd2.cli import get_docs
from qmd2.doc import dumps
from qmd2.ingestd import Ingestd


def legacy_rows(home, daemon, done=977, unavailable=36894, queued=7):
    root = home / 'attachments/queue'
    root.mkdir(parents=True)
    now = time.time()
    save(root / 'bad-row.json', dict(state='done'))
    for n in range(done + unavailable + queued):
        if n < 3:
            parent = dict(
                id=f'gmail:synthetic:legacy-{n}',
                item=f'gmail:synthetic:legacy-{n}',
                source='gmail',
                group='mail',
                kind='email',
                time=now,
                title='Synthetic parent',
                parts=[dict(kind='authored', text='Synthetic parent remains live')],
            )
            daemon.record('attachment', dumps(parent).encode(), {'key': parent['id']})
            identifier = parent['id'] + '/att/4'
            row = dict(
                id=identifier,
                output_id=identifier,
                parent=parent,
                ref=dict(source='gmail', position=4, type='application/pdf'),
                state='done',
                automatic=False,
                attempts=1,
                queued_at=now - 60,
                done_at=now,
                conversion_started=now - 5,
                conversion_s=5,
                fetched_at=now - 10,
                media_path='attachments/media/legacy',
                media_sha256='synthetic',
                model='synthetic@1',
                azure_pages=1,
            )
            if n != 1:
                doc = dict(
                    id=identifier,
                    item=identifier,
                    source='gmail',
                    group='mail',
                    kind='text',
                    time=now,
                    parser='attachment@1',
                    pipeline_revision=1,
                    model=row['model'],
                    media_sha256='synthetic',
                    context=dict(parent=parent['id']),
                    parts=[dict(kind='attachment_text', text='Synthetic luminous harbor')],
                )
                daemon.record('attachment', dumps(doc).encode(), {'key': identifier})
            daemon.commit()
            for id in [identifier] if n == 0 else [identifier, parent['id']] if n == 2 else []:
                old = daemon.index.lookup(id)
                daemon.documents(
                    'gmail', [daemon.tombstone('gmail', old)], old['version']['raw'], time.time_ns()
                )
            daemon.commit()
            save(root / queue_path(home, identifier).name, row)
            continue
        identifier = f'wa:synthetic:chat:{n}/att/0'
        state = 'done' if n < done else 'unavailable' if n < done + unavailable else 'queued'
        row = dict(
            id=identifier,
            state=state,
            parent=dict(
                id=identifier.rsplit('/att/', 1)[0], source='whatsapp', group='mail', time=now
            ),
            ref=dict(source='whatsapp', type='image'),
            automatic=False,
            backfill=True,
            captured_at=now - n,
            enqueued_at=now,
            ended_at=now,
            pipeline_revision=2,
            model=None,
            reason='no_direct_path',
            attempts=1,
            last_attempt_at=now + 86400,
        )
        if state == 'done':
            doc = dict(
                id=identifier + '/ocr',
                item=identifier,
                source='whatsapp',
                group='mail',
                kind='ocr',
                time=now,
                parts=[dict(kind='attachment_text', text='Synthetic luminous harbor')],
                context=dict(parent=row['parent']['id']),
                pipeline_revision=2,
                model='synthetic@1',
            )
            row.update(output_id=doc['id'], output=doc, azure_pages=1)
            row.pop('reason')
            daemon.record('attachment', dumps(doc).encode(), {'key': doc['id']})
            if n % 2:
                row.pop('output')
        path = root / queue_path(home, identifier).name
        path.write_text(dumps(row))
    daemon.commit()
    return root


def test_attachment_history_migration_filesystem_tantivy(tmp_path):
    daemon = Ingestd(tmp_path, remote=False)
    root = legacy_rows(tmp_path, daemon)
    daemon.attachment_outcomes(seconds=0.2)
    first = daemon.state.get('attachment-migration')
    assert not first['complete'] and 0 < first['migrated'] < 37871
    live = dict(
        id='synthetic:live',
        item='synthetic:live',
        source='attachment',
        group='files',
        kind='text',
        time=time.time(),
        parts=[dict(kind='authored', text='Fresh zephyr capture')],
    )
    daemon.record('attachment', dumps(live).encode(), {'key': live['id']})
    daemon.commit()
    assert daemon.index.search('zephyr')[0]['id'] == live['id']
    daemon.upload_pool.shutdown()
    daemon.state.db.close()
    daemon.lock.close()
    del daemon
    gc.collect()
    daemon = Ingestd(tmp_path, remote=False)
    assert daemon.state.get('attachment-migration')['migrated'] == first['migrated']
    slices = []
    while daemon.state.get('attachment-migration').get('remaining') != 1:
        start = time.monotonic()
        daemon.attachment_outcomes()
        slices.append(time.monotonic() - start)
        assert daemon.state.get('attachment-migration').get('error') in (
            None,
            'KeyError',
            'TimeoutError',
        )
    daemon.attachment_outcomes()
    progress = daemon.state.get('attachment-migration')
    assert progress['row_errors'] == 1 and progress['error'] == 'KeyError'
    assert not progress['complete']
    assert json.loads((root / 'bad-row.json').read_text())['migration_error'] == 'KeyError'
    from qmd2.verify import evaluate

    assert 'attachments:migration' in evaluate(
        json.loads((tmp_path / 'status.json').read_text()), time.time()
    )
    # Repairing the retained bad row lets the same migration finish without a reset.
    save(
        root / 'bad-row.json',
        dict(
            id='synthetic:repaired/att/0',
            state='failed',
            reason='synthetic_error',
            pipeline_revision=2,
            ref={},
            ended_at=time.time(),
        ),
    )
    daemon.attachment_outcomes()
    result = snapshot(tmp_path)
    assert result['migration']['row_errors'] == 0 and not result['migration'].get('error')
    assert result['migration']['lost_outputs'] == 1
    assert (result['done'], result['unavailable'], result['failed'], result['queued']) == (
        975,
        36895,
        2,
        7,
    )
    restored = get_docs(tmp_path, 'gmail:synthetic:legacy-0/att/4')[0]
    assert restored['kind'] == 'text' and restored['pipeline_revision'] == 1
    assert restored['outcome']['ended_at'] == restored['time']
    assert get_docs(tmp_path, 'gmail:synthetic:legacy-1/att/4')[0]['reason'] == 'output_missing'
    assert get_docs(tmp_path, 'gmail:synthetic:legacy-2/att/4')[0]['reason'] == 'parent_deleted'
    daemon.reindex('gmail')
    assert get_docs(tmp_path, restored['id'])[0]['kind'] == 'text'
    assert result['azure_pages'] == 977
    assert result['migration']['remaining'] == 0 and result['migration']['moved'] == 7
    assert not list(root.glob('*.json'))
    assert (
        get_docs(tmp_path, 'wa:synthetic:chat:3/att/0')[0]['context']['parent']
        == 'wa:synthetic:chat:3'
    )
    assert daemon.index.search('luminous')
    assert not daemon.index.search('no_direct_path')
    assert not any(r['kind'] == 'attachment_outcome' for r in daemon.index.browse(limit=100))
    assert daemon.index.search('zephyr')[0]['id'] == live['id']
    # A committed output with a surviving queued row is acknowledged without a fetch.
    identifier = 'wa:synthetic:chat:1000/att/0'
    path = queue_path(tmp_path, identifier)
    row = dict(id=identifier, state='queued', pipeline_revision=2, ref=dict(source='invalid'))
    save(path, row)
    before = dict(daemon.stats)
    assert process(tmp_path, row)['state'] == 'unavailable'
    assert not path.exists() and daemon.stats == before
    # Replay a legacy terminal row after the write/delete crash boundary.
    save(root / path.name, dict(row, state='unavailable', reason='no_direct_path'))
    progress = dict(daemon.state.get('attachment-migration'), complete=False)
    daemon.queue_checkpoint('attachment-migration', progress)
    daemon.commit()
    daemon.attachment_outcomes()
    assert daemon.stats == before and not list(root.glob('*.json'))
    daemon.attachment_outcomes()
    assert daemon.stats == before
    print('migration slices wall_s:', slices)
