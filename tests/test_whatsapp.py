import hashlib
import json

import pytest

from qmd2.doc import Exclusion, render, version
from qmd2.sources.whatsapp import parse

CTX = {
    'account': 'synthetic',
    'key': 'chat:message',
    'time': '2026-10-13T00:00:00Z',
    'aliases': {'7@lid': '8@s.whatsapp.net'},
    'contacts': {'8@s.whatsapp.net': 'Synthetic Sender'},
    'groups': {'9@g.us': 'Synthetic Group'},
}
BASE = {
    'id': 'message',
    'chat': '7@lid',
    'sender': '7@lid',
    'timestamp': '2026-10-12T00:00:00Z',
    'kind': 'message',
    'text': 'Synthetic lighthouse',
}


@pytest.mark.parametrize(
    'changes,count,failed',
    [
        ({}, 1, False),
        (
            {'chat': '9@g.us', 'push_name': 'Synthetic Push', 'quoted_message_id': 'quoted'},
            1,
            False,
        ),
        ({'kind': 'edit', 'target': 'original', 'edited': '2026-10-13T00:00:00Z'}, 1, False),
        (
            {
                'text': '',
                'media': [
                    {'type': 'audio', 'PTT': True, 'mimetype': 'audio/ogg', 'fileLength': '12'}
                ],
            },
            1,
            False,
        ),
        (
            {'media': [{'type': 'image'}, {'type': 'document', 'fileName': 'synthetic.pdf'}]},
            1,
            False,
        ),
        ({'kind': 'reaction'}, 0, False),
        ({'kind': 'protocol', 'target': 'original'}, 0, False),
        ({'kind': 'history_source'}, 0, False),
        ({'kind': 'context'}, 0, False),
        ({'chat': 'status@broadcast'}, 0, False),
        ({'status_broadcast': True}, 0, False),
        ({'kind': 'binary', 'text': ''}, 1, False),
        ({'id': ''}, 1, True),
        ({'text': {}}, 1, True),
        (None, 1, True),
    ],
)
def test_parser_table(changes, count, failed):
    decoded = dict(BASE, **changes) if changes is not None else None
    docs = parse(json.dumps({'raw': {'private': 'ignored'}, 'decoded': decoded}).encode(), CTX)
    if decoded and (decoded.get('status_broadcast') or decoded.get('chat') == 'status@broadcast'):
        assert docs == [Exclusion('status_broadcast')]
        return
    if decoded and decoded.get('kind') in ('reaction', 'protocol'):
        assert docs == [Exclusion(decoded['kind'])]
        return
    assert len(docs) == count
    if not docs:
        return
    doc = docs[0]
    assert bool(doc.get('parse_failed')) == failed
    if failed:
        assert doc['parts'][0]['kind'] == 'binary'
        return
    assert (
        doc['id']
        == f"wa:synthetic:{'9@g.us' if decoded['chat'] == '9@g.us' else '8@s.whatsapp.net'}:{decoded.get('target', 'message')}"
    )
    assert doc['item'] == doc['id']
    assert doc['from'] == decoded.get('push_name', 'Synthetic Sender')
    assert doc['to'] == ('Synthetic Group' if decoded['chat'] == '9@g.us' else 'Synthetic Sender')
    assert doc['time'] == BASE['timestamp']
    if decoded.get('quoted_message_id'):
        assert doc['reply_to'] == 'wa:synthetic:9@g.us:quoted'
    if decoded.get('edited'):
        assert version(doc)['time'] > version(dict(doc, edited=BASE['timestamp']))['time']
    assert len(doc['attachments']) == len(decoded.get('media', []))
    for i, attachment in enumerate(doc['attachments']):
        assert attachment['id'] == f"{doc['id']}/att/{i}"


def test_whatsapp_spool_chain(tmp_path):
    from qmd2.ingestd import Ingestd

    spool = tmp_path / 'spool/whatsapp'
    spool.mkdir(parents=True)
    decoded = dict(
        BASE,
        chat='9@g.us',
        push_name='Synthetic Sender',
        quoted_message_id='quoted',
        media=[{'type': 'document', 'fileName': 'synthetic.pdf', 'fileLength': '12'}],
    )
    raw = json.dumps({'raw': {'journal': 'synthetic'}, 'decoded': decoded}).encode()
    (spool / 'payload').write_bytes(raw)
    entry = {
        'schema': 'qmd.spool-entry.v1',
        'source': 'whatsapp',
        'account': 'synthetic',
        'key': '9@g.us:message',
        'path': 'payload',
        'bytes': len(raw),
        'sha256': hashlib.sha256(raw).hexdigest(),
        'captured_at': CTX['time'],
        'state': {'labels': {'groups': CTX['groups']}},
    }
    (spool / 'index.jsonl').write_text(json.dumps(entry) + '\n')
    daemon = Ingestd(tmp_path, remote=False)
    daemon.spool()
    daemon.commit()
    hit = daemon.index.search('lighthouse')[0]
    assert hit['id'] == 'wa:synthetic:9@g.us:message'
    doc = json.loads(daemon.packs.read(hit['pointer'])[1])
    from qmd2.reads import Reader

    reader = Reader(tmp_path)
    try:
        fetched = reader.get(doc['id'])[0]
        assert fetched['id'] == doc['id'] and fetched['parts'] == doc['parts']
    finally:
        reader.close()
    assert daemon.packs.read(doc['raw'])[1] == raw
    text = render(doc)
    assert 'From: Synthetic Sender' in text
    assert 'To: Synthetic Group' in text
    assert 'Sent: 2026-10-12T00:00:00Z' in text
    assert 'Reply-To: wa:synthetic:9@g.us:quoted' in text
    assert '/att/0 (synthetic.pdf' in text
    edited = json.dumps(
        {
            'decoded': dict(
                decoded,
                id='carrier',
                kind='edit',
                target='message',
                edited='2026-10-13T00:00:00Z',
                text='Synthetic beacon',
            )
        }
    ).encode()
    daemon.record('whatsapp', edited, CTX)
    daemon.commit()
    assert not daemon.index.search('lighthouse')
    assert daemon.index.search('beacon')[0]['id'] == hit['id']
    before = daemon.checkpoint('indexed:whatsapp')['processed_through']
    excluded = json.dumps({'decoded': dict(BASE, status_broadcast=True)}).encode()
    daemon.record('whatsapp', excluded, CTX)
    assert daemon.checkpoint('indexed:whatsapp')['processed_through'] == before
    daemon.record('whatsapp', excluded, dict(CTX, item_time=before + 60))
    assert daemon.checkpoint('indexed:whatsapp')['processed_through'] == before + 60
    control = json.dumps({'decoded': {'kind': 'context'}}).encode()
    daemon.record('whatsapp', control, dict(CTX, item_time=before + 120))
    daemon.commit()
    for offset, kind in enumerate(('reaction', 'protocol'), start=2):
        raw = json.dumps({'decoded': dict(BASE, kind=kind)}).encode()
        daemon.record('whatsapp', raw, dict(CTX, item_time=before + offset * 60))
        assert daemon.checkpoint('indexed:whatsapp')['processed_through'] == before + offset * 60
    daemon.commit()
    assert daemon.checkpoint('parser_exclusions:whatsapp') == {
        'status_broadcast': 2,
        'reaction': 1,
        'protocol': 1,
    }
    while not daemon.reindex('whatsapp', seconds=60):
        pass
    assert daemon.checkpoint('indexed:whatsapp')['processed_through'] == before + 180
