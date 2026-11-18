import pytest

from qmd2.doc import version
from qmd2.index import Index
from qmd2.state import State


def test_real_tantivy_versions_filters_and_groups(tmp_path, monkeypatch):
    state = State(tmp_path, True)
    index = Index(tmp_path, True, state)

    def add(id, item, body, clock=1, capture=1, kind='authored', parser='pi@1'):
        doc = {
            'id': id,
            'item': item,
            'source': 'pi',
            'group': 'sessions',
            'kind': kind,
            'time': clock,
            'title': '',
            'context': {},
            'parts': [{'kind': 'authored', 'text': body}],
            'parser': parser,
            'raw': {
                'stream': 'raw',
                'source': 'pi',
                'seq': 0,
                'rec_off': sum(id.encode()),
                'rec_len': 10,
            },
        }
        doc['version'] = version(doc, capture)
        return index.upsert(
            doc, {'stream': 'doc', 'source': 'pi', 'seq': 1, 'rec_off': capture, 'rec_len': 1}
        )

    assert add('pi:clock#1', 'pi:clock', 'clock correction', 10, 1)
    assert add('pi:clock#1', 'pi:clock', 'clock correction', 5, 2, parser='pi@2')
    assert not add('pi:clock#1', 'pi:clock', 'clock correction', 10, 3, parser='pi@1')
    assert add('pi:s#1', 'pi:s', 'alpha beta', 2, 2)
    assert not add('pi:s#1', 'pi:s', 'older', 1, 3)
    assert add('pi:s#1', 'pi:s', 'alpha beta enriched', 2, 4)
    assert not add('pi:s#1', 'pi:s', 'alpha beta enriched', 2, 5)
    for n in range(210):
        add(f'pi:dominating#{n}', 'pi:dominating', 'alpha beta', 1, n)
    add('pi:other#1', 'pi:other', 'alpha beta', 1, 300)
    add('pi:partial#1', 'pi:partial', 'alpha', 3, 400)
    doc = {
        'id': 'claude:caller#1',
        'item': 'claude:caller',
        'source': 'claude',
        'group': 'sessions',
        'kind': 'authored',
        'time': 1,
        'title': '',
        'context': {},
        'parts': [{'kind': 'authored', 'text': 'alpha beta'}],
    }
    doc['version'] = version(doc, 1)
    index.upsert(doc, {'stream': 'doc', 'source': 'claude', 'seq': 1, 'rec_off': 1, 'rec_len': 1})
    monkeypatch.setenv('CLAUDE_CODE_SESSION_ID', 'caller')
    index.commit()
    state.commit()
    assert all(r['item'] != 'claude:caller' for r in index.search('alpha'))
    hits = index.search('alpha beta', limit=4)
    assert len({r['item'] for r in hits}) == 4
    assert hits[-1]['item'] == 'pi:partial'
    monkeypatch.setenv('PI_SESSION_ID', 'dominating')
    assert all(r['item'] != 'pi:dominating' for r in index.search('alpha'))
    assert add('pi:s#1', 'pi:s', '', 3, 5, 'tombstone')
    index.commit()
    state.commit()
    assert index.lookup('pi:s#1') is None
    assert not add('pi:s#1', 'pi:s', 'resurrection', 2, 600)
    del index
    index = Index(tmp_path, True, state)
    assert not add('pi:s#1', 'pi:s', 'resurrection', 2, 700)


def test_origin_filters_tantivy(tmp_path):
    from qmd2.state import State

    state = State(tmp_path, True)
    index = Index(tmp_path, True, state)

    def add(id, group, origin=None):
        doc = {
            'id': id,
            'item': id.rsplit('#', 1)[0],
            'source': 'pi' if group == 'sessions' else 'gmail',
            'group': group,
            'kind': 'authored',
            'time': '2026-10-13T01:00:00Z',
            'title': '',
            'context': {},
            'parts': [{'kind': 'authored', 'text': 'origin synthetic'}],
            'parser': 'pi@4',
        }
        if origin:
            doc['origin'] = origin
        doc['version'] = version(doc, 1)
        return index.upsert(
            doc,
            {
                'stream': 'doc',
                'source': doc['source'],
                'seq': 0,
                'rec_off': len(id),
                'rec_len': 1,
            },
        )

    add('pi:steward#1', 'sessions', 'steward')
    add('pi:gsd#1', 'sessions', 'gsd')
    add('pi:own#1', 'sessions', 'interactive')
    add('pi:old#1', 'sessions')
    add('gmail:a:1', 'mail')
    index.commit()

    def items(**filters):
        return sorted(row['item'] for row in index.search('origin', limit=20, **filters))

    # A doc indexed without an origin is an interactive session; a non-session doc has none at all.
    assert items() == ['gmail:a:1', 'pi:gsd', 'pi:old', 'pi:own', 'pi:steward']
    assert items(origins=['steward']) == ['pi:steward']
    assert items(origins=['gsd', 'interactive']) == ['pi:gsd', 'pi:old', 'pi:own']
    assert items(not_origins=['steward', 'gsd']) == ['gmail:a:1', 'pi:old', 'pi:own']
    assert items(origins=['interactive'], not_origins=['gsd']) == ['pi:old', 'pi:own']
    browsed = index.browse(limit=20, origins=['gsd'])
    assert [row['item'] for row in browsed] == ['pi:gsd']
    assert {row['origin'] for row in index.browse(limit=20, origins=['steward', 'gsd'])} == {
        'steward',
        'gsd',
    }


def old_index(home, rows):
    """An index directory with ff09cbe's schema (no origin field) holding these rows."""
    import datetime
    import json

    import tantivy

    (home / 'index').mkdir(parents=True)
    builder = tantivy.SchemaBuilder()
    for name in ('id', 'item', 'source', 'group', 'kind'):
        builder.add_text_field(name, stored=True, tokenizer_name='raw')
    builder.add_date_field('time', stored=True, fast=True)
    for name in ('title', 'from', 'context', 'body'):
        builder.add_text_field(name, stored=True)
    for name in ('pointer', 'version'):
        builder.add_text_field(name, stored=True, index_option='basic', tokenizer_name='raw')
    index = tantivy.Index(builder.build(), path=str(home / 'index'))
    writer = index.writer(heap_size=50_000_000, num_threads=1)
    for row in rows:
        writer.add_document(
            tantivy.Document.from_dict(
                {
                    'title': 'old',
                    'from': 'user',
                    'context': '',
                    'pointer': json.dumps(
                        dict(
                            stream='doc',
                            source=row['source'],
                            seq=0,
                            rec_off=0,
                            rec_len=1,
                        )
                    ),
                    'version': json.dumps(
                        {'capture': 1, 'time': '2026-10-13T01:00:00Z', 'hash': 'x'}
                    ),
                    **row,
                    'time': datetime.datetime(2026, 10, 13, 1, 0, 0),
                },
                index.schema,
            )
        )
    writer.commit()
    del writer


def test_an_index_built_without_origin(tmp_path):
    """Opening one is what a released ingestd used to do; recorded here is what it now does."""
    home = tmp_path / 'home'
    old_index(
        home,
        [
            dict(
                id='pi:s#1',
                item='pi:s',
                source='pi',
                group='sessions',
                kind='authored',
                body='old schema synthetic',
            )
        ],
    )

    read = Index(home)
    assert [row['id'] for row in read.search('synthetic')] == ['pi:s#1']
    # An index without the field holds no steward or gsd session, so excluding them
    # adds no clause and naming them matches nothing rather than raising.
    assert [row['id'] for row in read.search('synthetic', not_origins=['steward', 'gsd'])] == [
        'pi:s#1'
    ]
    assert [row['id'] for row in read.search('synthetic', origins=['interactive'])] == ['pi:s#1']
    assert read.search('synthetic', origins=['steward']) == []
    assert read.origin_field is False
    with pytest.raises(ValueError, match='does not match'):
        Index(home, True)
    assert Index(tmp_path / 'fresh', True).origin_field is True


def close(daemon):
    daemon.upload_pool.shutdown()
    daemon.index.writer.wait_merging_threads()
    del daemon.index
    daemon.state.db.close()
    daemon.lock.close()


def schema_mismatch_store(tmp_path):
    """A store as a released v1 left it: docs in packs, index in ff09cbe's schema."""
    import json
    import shutil

    from qmd2.ingestd import Ingestd
    from qmd2.sources.sessions import ROOTS

    def line(n):
        return json.dumps(
            {
                'type': 'user',
                'uuid': f'u{n}',
                'sessionId': 'c',
                'cwd': '/synthetic',
                'timestamp': '2026-10-13T01:00:00Z',
                'message': {'role': 'user', 'content': 'origin synthetic ' + str(n)},
            }
        ).encode() + b'\n'

    root = tmp_path / 'sessions'
    root.mkdir()
    store = tmp_path / 'store'
    (root / 'c.jsonl').write_bytes(line(1) + line(2))
    original = ROOTS[:]
    ROOTS[:] = [('claude', root)]
    try:
        daemon = Ingestd(store, remote=False)
        try:
            daemon.last_walk -= 15
            daemon.sessions(seconds=30)
            daemon.commit()
        finally:
            close(daemon)
    finally:
        ROOTS[:] = original
    # The store as it stands after the previous release: docs in packs, index in the old schema.
    shutil.rmtree(store / 'index')
    old_index(
        store,
        [
            dict(
                id='claude:c#u1',
                item='claude:c',
                source='claude',
                group='sessions',
                kind='authored',
                body='origin synthetic 1',
            )
        ],
    )
    return store


def test_ingestd_rebuilds_a_schema_mismatch_on_start(tmp_path):
    """A released schema change: ingestd rebuilds the index itself instead of crash-looping."""
    from qmd2.ingestd import Ingestd

    store = schema_mismatch_store(tmp_path)
    daemon = Ingestd(store, remote=False)
    try:
        assert daemon.visible_index is not None
        assert daemon.maintenance['stage'] == 'read'
        assert daemon.index.path == store / 'index-rebuild'
        # Reads keep working off the old index while the rebuild runs.
        assert [row['id'] for row in Index(store).search('origin synthetic')] == ['claude:c#u1']
        while not daemon.reindex(seconds=60):
            pass
        assert daemon.maintenance is None and daemon.visible_index is None
        swapped = Index(store)
        hits = swapped.search('origin synthetic', origins=['interactive'])
        assert [row['item'] for row in hits] == ['claude:c']
        assert {row['id'] for row in swapped.item('claude:c')} == {'claude:c#u1', 'claude:c#u2'}
    finally:
        close(daemon)
    daemon = Ingestd(store, remote=False)
    try:
        assert daemon.maintenance is None and daemon.visible_index is None
        assert not (store / 'index-rebuild').exists()
        assert [row['item'] for row in Index(store).search('origin synthetic')] == ['claude:c']
    finally:
        close(daemon)


def test_schema_rebuild_advances_under_turn(tmp_path, monkeypatch):
    """Under the daemon loop the start-time rebuild advances, so deferred docs are indexed."""
    import json

    from qmd2.ingestd import Ingestd

    monkeypatch.setattr('qmd2.ingestd.LEASE', tmp_path / 'heavy.lock')
    store = schema_mismatch_store(tmp_path)
    daemon = Ingestd(store, remote=False)
    try:
        assert daemon.maintenance['stage'] == 'read'
        # turn() only ever calls request(); the rebuild must advance from there.
        for _ in range(50):
            daemon.request(seconds=60)
            if daemon.maintenance is None:
                break
        assert daemon.maintenance is None and daemon.visible_index is None
        assert not (store / 'reindex.request').exists()
        result = json.loads((store / 'reindex.result').read_text())
        assert result['ok'] is True and result['id'].startswith('schema-rebuild-')
        hits = Index(store).search('origin synthetic', origins=['interactive'])
        assert [row['item'] for row in hits] == ['claude:c']
    finally:
        close(daemon)
