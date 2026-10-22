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
