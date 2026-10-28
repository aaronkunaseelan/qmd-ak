import hashlib
import json
import os
import subprocess
import sys
import time


def close_daemon(daemon):
    daemon.upload_pool.shutdown()
    daemon.state.db.close()
    daemon.lock.close()


def test_unscheduled_parse_status_filesystem(tmp_path):
    from qmd2.ingestd import Ingestd
    from qmd2.state import State
    from qmd2.status import view

    state = State(tmp_path, True)
    state.set('totals', {'docs': 0, 'records': 0, 'bytes': 0, 'parse_failed': 813})
    state.commit()
    state.db.close()
    daemon = Ingestd(tmp_path, remote=False)
    try:
        daemon.publish_status()
        status = json.loads((tmp_path / 'status.json').read_text())
        assert 'parse_failed' not in status['totals']
        for source in ('attachment', 'manual', 'gsd-events'):
            row = status['sources'][source]
            assert row['parse_failures'] == 0
            assert row['last_complete'] is None and row['fresh_through'] is None
            assert row['ok'] is None and row['lag_s'] is None
            daemon.record(source, b'null', {'key': source, 'time': '2026-10-19'})
        daemon.last_status -= 60
        daemon.commit()
        status = view(tmp_path, json.loads((tmp_path / 'status.json').read_text()))
        for source in ('attachment', 'manual', 'gsd-events'):
            assert status['sources'][source]['parse_failures'] == 1
            assert status['sources'][source]['cadence_s'] is None
            assert status['sources'][source]['lag_s'] is None
            assert status['sources'][source]['current'] is None
        assert 'parse_failed' not in status['totals']
        assert 'parse_failed' not in daemon.state.get('totals')
        assert 'parse_failed' not in json.loads((tmp_path / 'heartbeat.json').read_text())
    finally:
        close_daemon(daemon)


def test_upload_catalog_sqlite_and_b2_edge(tmp_path, monkeypatch):
    import threading

    import boto3

    from qmd2.b2 import B2
    from qmd2.ingestd import Ingestd

    release = threading.Event()
    release_first = threading.Event()
    uploaded = []

    class S3:
        def put_object(self, **kwargs):
            ready = release_first if '/first/' in kwargs['Key'] else release
            assert ready.wait(10)
            body = kwargs['Body'].read()
            uploaded.append(kwargs['Key'])
            return {'ETag': hashlib.md5(body).hexdigest()}

    monkeypatch.setattr(boto3, 'client', lambda *args, **kwargs: S3())
    for key in ('QMD_B2_REGION', 'QMD_B2_KEY_ID', 'QMD_B2_APPLICATION_KEY', 'QMD_B2_PREFIX'):
        monkeypatch.setenv(key, 'synthetic')
    daemon = Ingestd(tmp_path, remote=False)
    daemon.b2 = B2(bucket='synthetic')
    statements = []
    daemon.state.db.set_trace_callback(statements.append)
    try:
        # Large historical catalog and an older open pack must not be candidates.
        daemon.state.db.executemany(
            'INSERT INTO packs(stream,source,seq,created,sealed,uploaded) VALUES (?,?,?,?,?,?)',
            [('raw', 'history', n, n, 1, 1) for n in range(1000)] + [('raw', 'open', 1, 0, 0, 0)],
        )
        for source, created in [('third', 1003), ('first', 1001), ('second', 1002)]:
            loc = daemon.packs.append('raw', source, {'id': source}, source.encode())
            daemon.packs.seal('raw', source, loc['seq'])
            daemon.state.db.execute('UPDATE packs SET created=? WHERE source=?', (created, source))
        query = 'SELECT * FROM packs WHERE sealed=1 AND uploaded=0 ORDER BY created LIMIT 2'
        plan = daemon.state.db.execute('EXPLAIN QUERY PLAN ' + query).fetchall()
        assert any('pending_uploads' in row['detail'] for row in plan)
        assert not any('TEMP B-TREE' in row['detail'] for row in plan)
        assert [row['source'] for row in daemon.state.db.execute(query)] == ['first', 'second']
        daemon.upload()
        first = dict(daemon.uploads)
        assert {key[1] for key in first} == {'first', 'second'}
        statements.clear()
        daemon.upload()
        assert daemon.uploads == first
        assert not statements  # Both slots busy: not even an empty catalog query.
        # One completion frees a slot; keep the in-flight second and start third.
        release_first.set()
        first_key = next(key for key in first if key[1] == 'first')
        second_key = next(key for key in first if key[1] == 'second')
        first[first_key].result(timeout=10)
        daemon.upload()
        assert {key[1] for key in daemon.uploads} == {'second', 'third'}
        assert daemon.uploads[second_key] is first[second_key]
        release.set()
        for future in daemon.uploads.values():
            future.result(timeout=10)
        daemon.upload()
        assert not daemon.uploads
        assert len(uploaded) == 3
        assert not daemon.state.db.execute(query).fetchall()
    finally:
        release_first.set()
        release.set()
        close_daemon(daemon)


def test_pending_arrival_filesystem(tmp_path):
    from qmd2.ingestd import Ingestd

    daemon = Ingestd(tmp_path, remote=False)
    spool = tmp_path / 'spool' / 'import'
    spool.mkdir(parents=True)
    payload = spool / 'payload'
    payload.write_bytes(b'synthetic pending record')
    captured = time.time() - 30 * 86400
    (spool / 'index.jsonl').write_text(
        json.dumps({'source': 'session-pi', 'path': 'payload', 'captured_at': captured}) + '\n'
    )
    try:
        daemon.publish_status()
        row = json.loads((tmp_path / 'status.json').read_text())['sources']['pi']
        assert row['oldest_pending_at'] == payload.stat().st_mtime
        assert 0 <= time.time() - row['oldest_pending_at'] < 5
        # Commits stay durable while the unchanged status waits for its next minute.
        published = (tmp_path / 'status.json').read_bytes()
        daemon.queue_checkpoint('synthetic-durable', 1)
        daemon.commit()
        assert daemon.state.get('synthetic-durable') == 1
        assert (tmp_path / 'status.json').read_bytes() == published
        payload.unlink()
        daemon.publish_status()
        assert (tmp_path / 'status.json').read_bytes() == published
        daemon.last_status -= 60
        daemon.publish_status()
        row = json.loads((tmp_path / 'status.json').read_text())['sources']['pi']
        assert abs(row['oldest_pending_at'] - captured) < 0.001
    finally:
        close_daemon(daemon)


def test_status_times_filesystem(tmp_path):
    from qmd2.ingestd import Ingestd

    daemon = Ingestd(tmp_path, remote=False)
    for value, expected in [
        ('0001-01-01T00:00:00Z', None),
        ('', None),
        (None, None),
        ('2026-10-13T02:00:00Z', 1791856800.0),
    ]:
        daemon.state.set('receipt:x', {'ok': True, 'failed': 0, 'oldest_failed': value})
        daemon.last_status -= 60
        daemon.publish_status()
        row = json.loads((tmp_path / 'status.json').read_text())['sources']['x']
        assert row['oldest_failed_at'] == expected
        assert row['receipt'] == {
            'source': 'x',
            'ok': True,
            'enumerated': 0,
            'written': 0,
            'unchanged': 0,
            'excluded': 0,
            'failed': 0,
            'duration_s': 0,
            'source_head': 0,
            'oldest_failed': expected,
        }
    from qmd2.verify import evaluate

    loc = daemon.packs.append('raw', 'pi', {'id': 'synthetic'}, b'old open pack')
    created = time.time() - 86400
    daemon.state.db.execute('UPDATE packs SET created=?', (created,))
    for sealed_age, late in ((None, False), (0, False), (7201, True)):
        if sealed_age is not None:
            daemon.packs.seal('raw', 'pi', loc['seq'])
            path = daemon.packs.path('raw', 'pi', loc['seq'], True)
            stamp = time.time() - sealed_age
            os.utime(path, (stamp, stamp))
        daemon.last_status -= 60
        daemon.publish_status()
        status = json.loads((tmp_path / 'status.json').read_text())
        row = status['backlog'][0]
        assert row['created_at'] == created
        assert row['sealed'] == (sealed_age is not None)
        expected = status['written_at'] - (created if sealed_age is None else stamp)
        assert abs(row['age_s'] - expected) < 0.001
        assert ('upload:late' in evaluate(status, status['written_at'])) == late
    daemon.state.db.execute('UPDATE packs SET uploaded=1')
    daemon.last_status -= 60
    daemon.publish_status()
    assert not json.loads((tmp_path / 'status.json').read_text())['backlog']
    close_daemon(daemon)


def test_indexed_time_tantivy_high_water(tmp_path):
    from qmd2.ingestd import Ingestd
    from qmd2.doc import date

    daemon = Ingestd(tmp_path, remote=False)
    for entry, stamp in [('new', '2026-10-13T12:00:00Z'), ('old', '2026-09-13T12:00:00Z')]:
        raw = json.dumps(
            {
                'type': 'message',
                'id': entry,
                'timestamp': stamp,
                'message': {'role': 'user', 'content': 'synthetic lighthouse'},
            }
        ).encode()
        daemon.record('pi', raw, {'runtime': 'pi', 'session': 'synthetic', 'item_time': stamp})
        daemon.commit()
        row = json.loads((tmp_path / 'status.json').read_text())['sources']['pi']
        assert row['newest_indexed_time'] == date('2026-10-13T12:00:00Z').timestamp()
        assert row['newest_id'] == 'pi:synthetic#new'
    daemon.state.set('indexed:pi', {'newest_id': 'pi:synthetic#old', 'searchable_through': 1})
    daemon.state.commit()
    daemon.last_status -= 60
    daemon.publish_status()
    row = json.loads((tmp_path / 'status.json').read_text())['sources']['pi']
    assert row['newest_id'] == 'pi:synthetic#new'
    # Index display times cannot reconstruct a lost processing checkpoint.
    assert row['newest_indexed_time'] == 1
    daemon.commit()
    assert daemon.state.get('indexed:pi')['searchable_through'] == 1
    while not daemon.reindex('pi', seconds=60):
        pass
    assert (
        daemon.state.get('indexed:pi')['searchable_through']
        == date('2026-10-13T12:00:00Z').timestamp()
    )
    close_daemon(daemon)


def test_fetch_receipt_filesystem_subprocess(tmp_path):
    import time
    from qmd2.ingestd import Ingestd
    from qmd2.sources import SOURCES

    daemon = Ingestd(tmp_path, remote=False)
    blocker = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
    output = tmp_path / 'receipt.json'
    base = dict(source='x', enumerated=0, written=0, unchanged=0, excluded=0, failed=0)
    cases = [
        (dict(base, ok=False, rate_limited=True), 1, True, 0),
        (dict(base, ok=True, rate_limited=False, head_complete=True), 0, False, 0),
        (dict(base, ok=True, head_complete=False), 0, False, 0),
        (dict(base, ok=True, error='provider HTTP 429', has_more=True), 0, False, 0),
        (dict(base, ok=True, head_complete=True), 0, False, 0),
        (dict(base, ok=True, head_complete=True, backfill_error='allowance'), 0, False, 0),
        (
            dict(
                base,
                ok=True,
                head_complete=True,
                enumerated=1,
                excluded=1,
                exclusion_reasons={'provider_deleted': 1},
            ),
            0,
            False,
            0,
        ),
        (dict(base, ok=True, head_complete=True), 0, False, 0),
        (None, 1, False, 1),
        (dict(source='x', ok=True, enumerated=0), 0, False, 1),
        (dict(base, ok=True, written=-1, unchanged=1), 0, False, 1),
        (dict(base, ok=True, enumerated=True, written=True), 0, False, 1),
        (dict(base, ok='false'), 0, False, 1),
        (dict(base, ok=True, rate_limited=True), 1, False, 1),
    ]
    try:
        daemon.queue_checkpoint(
            'receipt:x', dict(base, last_complete=123, oldest_failed=456, rate_limited=True)
        )
        for payload, exit_code, limited, failures in cases:
            previous_complete = daemon.checkpoint('receipt:x')['last_complete']
            previous_fresh = daemon.checkpoint('receipt:x').get('fresh_through')
            with output.open('wb') as f:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        '-c',
                        'import sys; print(sys.argv[1]); sys.exit(int(sys.argv[2]))',
                        json.dumps(payload),
                        str(exit_code),
                    ],
                    stdout=f,
                )
                process.wait(timeout=10)
            daemon.fetches = {
                name: {'process': blocker, 'started': time.monotonic()}
                for name, _, argv, cadence, *rest in SOURCES
                if argv and cadence
            }
            daemon.fetches['x'] = {
                'process': process,
                'started': time.monotonic(),
                'output': output,
            }
            daemon.fetch()
            receipt = daemon.checkpoint('receipt:x')
            advances = (
                isinstance(payload, dict)
                and payload.get('ok')
                and not failures
                and not exit_code
                and not payload.get('error')
                and not payload.get('backfill_error')
                and not payload.get('rate_limited')
                and payload.get('head_complete', True)
            )
            if advances:
                assert receipt['fresh_through'] > previous_complete
            else:
                assert receipt['fresh_through'] == previous_fresh
            daemon.last_status -= 60
            daemon.commit()
            status = json.loads((tmp_path / 'status.json').read_text())['sources']['x']
            for field in ('error', 'backfill_error', 'has_more', 'head_complete'):
                assert receipt.get(field) == (payload or {}).get(field)
                assert status.get(field) == receipt.get(field)
            assert receipt['rate_limited'] is limited
            assert receipt['failed'] == failures
            assert bool(receipt['oldest_failed']) == bool(failures)
            if failures or not isinstance(payload, dict) or not payload.get('ok') or exit_code:
                assert receipt['last_complete'] == previous_complete
            else:
                assert receipt['last_complete'] > previous_complete
    finally:
        blocker.terminate()
        blocker.wait(timeout=10)
        close_daemon(daemon)


def test_filesystem_subprocess_tail_spool_and_cli(tmp_path):
    lease = tmp_path / 'heavy.lock'
    env = dict(os.environ, HOME=str(tmp_path), QMD2_HEAVY_LEASE=str(lease))

    def run(*args):
        return subprocess.run(
            [sys.executable, '-m', *args], env=env, capture_output=True, text=True, check=True
        )

    roots = [
        'agents/pi/sessions',
        '.codex/sessions',
        '.claude/projects',
        'agents/codex/sessions',
        'agents/claude-code/projects',
    ]
    store = tmp_path / '.local/share/qmd2'
    run('qmd2.ingestd', '--home', str(store), '--no-b2', '--once')
    snapshot = json.loads((store / 'status.json').read_text())
    inventory_errors = {}
    for runtime in ('claude', 'codex', 'pi'):
        row = snapshot['sources'][runtime]
        assert row['receipt']['ok'] is False and row['receipt']['failed'] == 1
        assert row['fresh_through'] is None
        inventory_errors[runtime] = row['receipt'].get('error')
    for root in roots:
        (tmp_path / root).mkdir(parents=True)
    session = tmp_path / roots[0] / 's.jsonl'
    run('qmd2.ingestd', '--home', str(store), '--no-b2', '--once')
    snapshot = json.loads((store / 'status.json').read_text())
    for runtime in ('claude', 'codex', 'pi'):
        row = snapshot['sources'][runtime]
        assert row['receipt']['ok'] is True and row['receipt']['enumerated'] == 0
        assert row['fresh_through'] is not None
    rows = [
        {'type': 'session', 'id': 's', 'cwd': '/synthetic'},
        {
            'type': 'message',
            'id': 'u',
            'message': {
                'role': 'user',
                'content': [{'type': 'text', 'text': 'lighthouse synthetic'}],
            },
        },
        {
            'type': 'message',
            'id': 'a',
            'message': {
                'role': 'assistant',
                'content': [
                    {
                        'type': 'toolCall',
                        'id': 'call',
                        'name': 'bash',
                        'arguments': {'command': 'qmd search secretneedle'},
                    }
                ],
            },
        },
    ]
    session.write_bytes(b''.join(json.dumps(r).encode() + b'\n' for r in rows) + b'{"incomplete":')

    run('qmd2.ingestd', '--home', str(store), '--no-b2', '--once')
    result = json.loads(
        run('qmd2.cli', '--home', str(store), 'search', 'lighthouse', '--json').stdout
    )
    assert result[0]['id'] == 'pi:s#u'
    assert 'lighthouse synthetic' in run('qmd2.cli', '--home', str(store), 'get', 'pi:s').stdout
    raw = session.read_bytes().split(b'{"incomplete":')[0]
    session.write_bytes(
        raw
        + json.dumps(
            {
                'type': 'message',
                'id': 'r',
                'message': {
                    'role': 'toolResult',
                    'toolCallId': 'call',
                    'toolName': 'bash',
                    'content': [{'type': 'text', 'text': 'secretneedle'}],
                },
            }
        ).encode()
        + b'\n'
    )
    spool = store / 'spool/import'
    spool.mkdir(parents=True)
    payload = (
        json.dumps(
            {
                'type': 'message',
                'id': 'history',
                'message': {'role': 'user', 'content': 'archived synthetic'},
            }
        ).encode()
        + b'\n'
    )
    envelope = (
        json.dumps(
            {
                'schema': 'qmd.session-envelope.v1',
                'source': {'tool': 'pi', 'path_key': 'old.jsonl'},
                'start_offset': 0,
            }
        ).encode()
        + b'\n'
        + payload
    )
    (spool / 'payload').write_bytes(envelope)
    entry = {
        'schema': 'qmd.spool-entry.v1',
        'source': 'pi',
        'account': 'history',
        'key': 'old',
        'path': 'payload',
        'bytes': len(envelope),
        'sha256': hashlib.sha256(envelope).hexdigest(),
        'captured_at': '2026-10-13T02:00:00Z',
    }
    (spool / 'index.jsonl').write_text(json.dumps(entry) + '\n')
    run('qmd2.ingestd', '--home', str(store), '--no-b2', '--once')
    assert (
        json.loads(run('qmd2.cli', '--home', str(store), 'search', 'secretneedle', '--json').stdout)
        == []
    )
    assert 'secretneedle' in run('qmd2.cli', '--home', str(store), 'get', 'pi:s#r').stdout
    assert (
        json.loads(run('qmd2.cli', '--home', str(store), 'search', 'archived', '--json').stdout)[0][
            'id'
        ]
        == 'pi:old#history'
    )
    from qmd2.packs import Packs
    from qmd2.state import State

    state = State(store)
    packs = Packs(state)
    hit = json.loads(run('qmd2.cli', '--home', str(store), 'search', 'archived', '--json').stdout)[
        0
    ]
    assert 'pointer' not in hit and 'version' not in hit
    doc = json.loads(run('qmd2.cli', '--home', str(store), 'get', hit['id'], '--json').stdout)[
        'docs'
    ][0]
    assert packs.read(doc['raw'])[1] == envelope

    from qmd2.ingestd import Ingestd
    from qmd2.sources.sessions import ROOTS, context

    original_roots = ROOTS[:]
    ROOTS[:] = [(runtime, tmp_path / root) for (runtime, _), root in zip(ROOTS, roots)]
    daemon = Ingestd(store, remote=False)
    try:
        daemon.sessions()
        assert daemon.position == 0
        completed_walk = daemon.last_walk
        calls = []

        def count_context(frame, event, arg):
            if event == 'call' and frame.f_code is context.__code__:
                calls.append(frame)

        appended = {
            'type': 'message',
            'id': 'cadence',
            'message': {'role': 'user', 'content': 'cadenceappend synthetic'},
        }
        with session.open('ab') as stream:
            stream.write(json.dumps(appended).encode() + b'\n')
        new_session = session.with_name('new.jsonl')
        new_session.write_text(
            json.dumps(dict(appended, id='new', message={'role': 'user', 'content': 'cadencenew'}))
            + '\n'
        )
        previous_profile = sys.getprofile()
        sys.setprofile(count_context)
        try:
            daemon.sessions()
        finally:
            sys.setprofile(previous_profile)
        assert calls == []
        assert daemon.last_walk == completed_walk
        assert daemon.index.lookup('pi:s#cadence') is None
        assert daemon.index.lookup('pi:new#new') is None
        # Advance the walk's elapsed time, without mocking the clock or filesystem.
        daemon.last_walk -= 15
        daemon.sessions()
        daemon.commit()
        for term, item in [('cadenceappend', 'pi:s#cadence'), ('cadencenew', 'pi:new#new')]:
            hit = json.loads(
                run('qmd2.cli', '--home', str(store), 'search', term, '--json').stdout
            )[0]
            assert hit['id'] == item
            assert term in run('qmd2.cli', '--home', str(store), 'get', item).stdout
            doc = json.loads(run('qmd2.cli', '--home', str(store), 'get', item, '--json').stdout)[
                'docs'
            ][0]
            assert term.encode() in packs.read(doc['raw'])[1]

        # A bounded walk publishes the last completed run, not a failed partial run.
        from qmd2.verify import evaluate

        previous = {
            runtime: daemon.status_receipt(runtime, time.time())
            for runtime in ('claude', 'codex', 'pi')
        }
        first = session.with_name('slice-first.jsonl')
        first.write_bytes(b'')
        bulk = session.with_name('slice-bulk.jsonl')
        bulk.write_bytes(b'{"type":"progress"}\n' * 100000)
        os.utime(first, (2100000001, 2100000001))
        os.utime(bulk, (2100000000, 2100000000))
        daemon.last_walk -= 15
        daemon.sessions(seconds=0.01)
        assert daemon.position > 0
        daemon.last_status -= 60
        daemon.publish_status()
        snapshot = json.loads((store / 'status.json').read_text())
        for runtime, receipt in previous.items():
            assert snapshot['sources'][runtime]['receipt'] == receipt
        for age in (0, 3601):
            red = evaluate(snapshot, time.time() + age)
            for runtime in previous:
                assert runtime + ':capture' not in red
                assert (runtime + ':discovery' in red) == bool(age)
        # A file disappearing after inventory is a real failure, scoped to its lane.
        bulk.unlink()
        daemon.sessions(seconds=30)
        assert daemon.position == 0
        daemon.commit()
        failed = daemon.status_receipt('pi', time.time())
        assert failed['ok'] is False and failed['failed'] == 1
        assert failed['error'] == 'session stat: FileNotFoundError'
        for clock in ('fresh_through', 'last_complete'):
            assert failed[clock] == previous['pi'][clock]
        for runtime in ('claude', 'codex'):
            assert daemon.status_receipt(runtime, time.time())['ok'] is True
        assert set(inventory_errors.values()) == {'inventory root: FileNotFoundError'}

        # Inventory stat errors also carry a safe reason, without the target path.
        broken = tmp_path / roots[1] / 'broken.jsonl'
        broken.symlink_to('synthetic-secret-must-not-appear')
        daemon.last_walk -= 15
        daemon.sessions()
        daemon.commit()
        failed = daemon.status_receipt('codex', time.time())
        assert failed['ok'] is False and failed['failed'] == 1
        assert failed['error'] == 'inventory stat: FileNotFoundError'
        broken.unlink()
        daemon.last_walk -= 15
        daemon.sessions()
        daemon.commit()
        for runtime in previous:
            receipt = daemon.status_receipt(runtime, time.time())
            assert receipt['ok'] is True and not receipt.get('error')
        daemon.reset_source('codex')
        reset = daemon.status_receipt('codex', time.time())
        assert reset['ok'] is False
        assert reset['error'] == 'source reset: awaiting capture'
    finally:
        ROOTS[:] = original_roots
        close_daemon(daemon)
        del daemon
    assert run('qmd2.cli', '--help').returncode == 0
    wa = store / 'spool/whatsapp'
    wa.mkdir(parents=True, exist_ok=True)
    decoded = {
        'decoded': {
            'id': 'one',
            'chat': '7@lid',
            'kind': 'message',
            'text': 'reset lighthouse',
            'timestamp': '2026-10-13T02:00:00Z',
        },
        'ctx_update': {'aliases': {'7@lid': '8@s.whatsapp.net'}},
    }
    payload = json.dumps(decoded).encode()
    (wa / 'payload').write_bytes(payload)
    entry = dict(
        entry,
        source='whatsapp',
        account='synthetic',
        key='one',
        path='payload',
        bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    (wa / 'index.jsonl').write_text(json.dumps(entry) + '\n')
    run('qmd2.ingestd', '--home', str(store), '--no-b2', '--once')
    assert (
        json.loads(run('qmd2.cli', '--home', str(store), 'search', 'reset', '--json').stdout)[0][
            'id'
        ]
        == 'wa:synthetic:8@s.whatsapp.net:one'
    )
    cursor = store / 'capture-state/whatsapp'
    cursor.mkdir(parents=True)
    (cursor / 'cursor.json').write_text('{}')
    (store / 'reindex.request').write_text(
        json.dumps({'id': 'reset-test', 'source': 'whatsapp', 'reset': True})
    )
    run('qmd2.ingestd', '--home', str(store), '--no-b2', '--once')
    assert json.loads((store / 'reindex.result').read_text())['tombstoned'] == 1
    assert not cursor.exists()
    assert State(store).get('ctx:whatsapp') == {}
    assert (
        json.loads(run('qmd2.cli', '--home', str(store), 'search', 'reset', '--json').stdout) == []
    )

    (store / 'reindex.request').write_text(json.dumps({'id': 'lock-test', 'source': 'pi'}))
    run('qmd2.ingestd', '--home', str(store), '--no-b2', '--once')
    assert json.loads((store / 'reindex.result').read_text()) == {'id': 'lock-test', 'ok': True}
    assert lease.read_bytes() == b''
    writable = State(store, True)
    corrupt = Packs(writable, writable=True)
    corrupt.append('doc', 'pi', {'id': 'pi:synthetic#broken'}, b'{"id":"pi:synthetic#broken"}')
    corrupt.sync()
    writable.commit()
    writable.db.close()
    (store / 'reindex.request').write_text(json.dumps({'id': 'failure-test'}))
    run('qmd2.ingestd', '--home', str(store), '--no-b2', '--once')
    assert json.loads((store / 'reindex.result').read_text()) == {
        'id': 'failure-test',
        'ok': False,
        'exception_class': 'KeyError',
        'doc_id': 'pi:synthetic#broken',
    }
    assert lease.read_bytes() == b''
    import fcntl

    with lease.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
