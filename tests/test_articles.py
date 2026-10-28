import json

import pytest

from qmd2.doc import Exclusion, date, render, version
from qmd2.ingestd import Ingestd
from qmd2.sources.x import parse as x
from qmd2.sources.instapaper import parse as instapaper

TWEET = {
    'id': '42',
    'text': 'lighthouse https://t.test/a',
    'createdAt': 'Tue Oct 13 00:00:00 +0000 2026',
    'author': {'username': 'synthetic'},
    'entities': {'urls': [{'url': 'https://t.test/a', 'expanded_url': 'https://example.test'}]},
    'quotedTweet': {'text': 'beacon'},
    'media': [{'type': 'photo', 'altText': 'harbor'}],
}
SNAPSHOT = {
    'bookmark': {
        'bookmark_id': 42,
        'title': 'Synthetic article',
        'time': 1791849600,
        'url': 'https://example.test',
        'author': 'Synthetic',
    },
    'html': '<h1>lighthouse</h1><p>harbor &amp; <a href="https://example.test">beacon</a></p><script>hidden</script>',
}
CTX = {'account': 'synthetic', 'key': '42', 'time': '2026-10-13T00:00:00Z'}


@pytest.mark.parametrize(
    'row,kind,failed',
    [
        (TWEET, 'post', False),
        ({'tweet': TWEET}, 'post', False),
        ({'like': TWEET}, 'like', False),
        ({'bookmark': {'tweet': TWEET}}, 'bookmark', False),
        ({'like': {'tweetId': '42', 'fullText': 'archive lighthouse'}}, 'like', False),
        ({'tweet': dict(TWEET, favorited=True)}, 'like', False),
        ({'tweet': dict(TWEET, bookmarked=True, favorited=True)}, 'bookmark', False),
        (dict(TWEET, _raw={'legacy': {'bookmarked': True}}), 'bookmark', False),
        (dict(TWEET, _raw={'legacy': {'favorited': True}}), 'like', False),
        (dict(TWEET, _raw={'legacy': {'bookmarked': True, 'favorited': True}}), 'bookmark', False),
        (dict(TWEET, _raw={'legacy': {'bookmarked': False, 'favorited': False}}), 'post', False),
        (
            dict(TWEET, bookmarked=False, favorited=True, _raw={'legacy': {'bookmarked': True}}),
            'like',
            False,
        ),
        (None, 'post', True),
        ({}, 'post', True),
    ],
)
def test_x_table(row, kind, failed):
    doc = x(json.dumps(row).encode(), CTX)[0]
    assert doc['id'] == doc['item'] == 'x:synthetic:42'
    assert bool(doc.get('parse_failed')) == failed
    assert doc['kind'] == kind
    assert doc['parser'] == 'x@6'
    if not failed:
        assert 'lighthouse' in render(doc)
        changed = dict(row, favorite_count=99, views=100)
        updated = x(json.dumps(changed).encode(), CTX)[0]
        assert not updated.get('parse_failed')
        assert [p for p in doc['parts'] if p['kind'] == 'authored'] == [
            p for p in updated['parts'] if p['kind'] == 'authored'
        ]


@pytest.mark.parametrize(
    'row,count,kind,failed',
    [
        (SNAPSHOT, 1, 'article', False),
        ({'bookmark': SNAPSHOT['bookmark']}, 0, '', False),
        (dict(SNAPSHOT, html=''), 0, '', False),
        (dict(SNAPSHOT, html='\n'), 0, '', False),
        (dict(SNAPSHOT, html='<script>hidden</script>'), 0, '', False),
        (dict(SNAPSHOT, html='', text_unavailable=True), 1, 'article', False),
        ({'bookmark_id': 42, 'state': 'deleted'}, 1, 'tombstone', False),
        (None, 1, 'article', True),
    ],
)
def test_instapaper_table(row, count, kind, failed):
    docs = instapaper(json.dumps(row).encode(), CTX)
    if count == 0:
        assert docs == [Exclusion('empty_article')]
        return
    assert len(docs) == count
    if docs:
        assert docs[0]['id'] == docs[0]['item'] == 'instapaper:synthetic:42'
        assert docs[0]['kind'] == kind
        assert bool(docs[0].get('parse_failed')) == failed
        if row and row.get('text_unavailable'):
            assert docs[0]['parts'] == [{'kind': 'boilerplate', 'text': 'Article text unavailable'}]


@pytest.mark.parametrize(
    'source,row',
    [
        (
            'x',
            {
                'tweet': TWEET,
                'x_context': {
                    'posts': [
                        {
                            'tweet': {'id': '43', 'text': 'semaphore'},
                            'authored': True,
                            'role': 'Author reply',
                        },
                        {
                            'tweet': {'id': '41', 'text': 'zebracrossing'},
                            'authored': False,
                            'role': 'Parent',
                        },
                    ],
                    'notices': ['3-page cap hit'],
                },
            },
        ),
        ('instapaper', SNAPSHOT),
    ],
)
def test_article_chain(tmp_path, source, row):
    import hashlib

    spool = tmp_path / 'spool' / source
    spool.mkdir(parents=True)
    raw = json.dumps(row).encode()
    (spool / 'payload').write_bytes(raw)
    entry = dict(
        schema='qmd.spool-entry.v1',
        source=source,
        account='synthetic',
        key='42',
        path='payload',
        bytes=len(raw),
        sha256=hashlib.sha256(raw).hexdigest(),
        captured_at=CTX['time'],
        state={'labels': {}},
    )
    (spool / 'index.jsonl').write_text(json.dumps(entry) + '\n')
    daemon = Ingestd(tmp_path, remote=False)
    daemon.spool()
    daemon.commit()
    hits = daemon.index.search('lighthouse')
    assert len(hits) == 1
    doc = json.loads(daemon.packs.read(hits[0]['pointer'])[1])
    from qmd2.reads import Reader

    reader = Reader(tmp_path)
    try:
        fetched = reader.get(doc['id'])[0]
        assert fetched['id'] == doc['id'] and fetched['parts'] == doc['parts']
    finally:
        reader.close()
    capture = int(date(CTX['time']).timestamp() * 1e9)
    assert doc['version']['capture'] == capture
    assert daemon.packs.read(doc['raw'])[0]['version']['capture'] == capture
    text = render(doc)
    assert 'https://example.test' in text and 'harbor' in text and 'beacon' in text
    assert '<p>' not in text and 'hidden' not in text
    assert daemon.packs.read(doc['raw'])[1] == raw
    if source == 'x':
        assert daemon.index.search('semaphore')[0]['id'] == doc['id']
        assert not daemon.index.search('zebracrossing')
        assert 'zebracrossing' in text and '3-page cap hit' in text
        daemon.record(source, json.dumps({'bookmark': TWEET}).encode(), CTX)
    else:
        daemon.record(source, json.dumps({'bookmark': SNAPSHOT['bookmark']}).encode(), CTX)
    daemon.commit()
    assert len(daemon.index.search('lighthouse')) == 1
    assert 'harbor' in render(
        json.loads(daemon.packs.read(daemon.index.search('lighthouse')[0]['pointer'])[1])
    )
    if source == 'instapaper':
        before = daemon.checkpoint('indexed:instapaper')['processed_through']
        empty = json.dumps({'bookmark': SNAPSHOT['bookmark']}).encode()
        daemon.record(source, empty, CTX)
        assert daemon.checkpoint('indexed:instapaper')['processed_through'] == before
        daemon.record(source, empty, dict(CTX, item_time=before + 60))
        daemon.commit()
        assert daemon.checkpoint('parser_exclusions:instapaper') == {'empty_article': 3}
        assert daemon.checkpoint('indexed:instapaper')['processed_through'] == before + 60
        while not daemon.reindex(source, seconds=60):
            pass
        assert daemon.checkpoint('indexed:instapaper')['processed_through'] == before + 60
        deletion = json.dumps({'bookmark_id': '42', 'state': 'deleted'}).encode()
        for commit_between in (True, False):
            daemon.record(source, deletion, CTX, capture=10)
            if commit_between:
                daemon.commit()
                assert daemon.index.lookup(doc['id']) is None
                tombstones = [
                    json.loads(body)
                    for pack in daemon.state.packs('doc')
                    for _, head, body in daemon.packs.iter_pack(pack)
                    if head.get('kind') == 'tombstone'
                ]
                assert tombstones
                while not daemon.reindex(seconds=60):
                    pass
                assert daemon.index.lookup(doc['id']) is None
            daemon.record(source, json.dumps(SNAPSHOT).encode(), CTX, capture=9)
            daemon.commit()
            assert daemon.index.lookup(doc['id']) is not None
            while not daemon.reindex(seconds=60):
                pass
            assert daemon.index.lookup(doc['id']) is not None
        while not daemon.reindex(source, seconds=60):
            pass
        assert daemon.index.lookup(doc['id']) is not None
