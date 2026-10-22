import json

from qmd2.ingestd import Ingestd
from qmd2.state import State


def test_source_context_and_reparse_filesystem_tantivy(tmp_path):
    daemon = Ingestd(tmp_path, remote=False)
    ctx = {'account': 'synthetic', 'key': '7@lid:one', 'time': '2026-10-13T00:00:00Z'}
    raw = json.dumps(
        {
            'decoded': {
                'id': 'one',
                'chat': '7@lid',
                'sender': '7@lid',
                'kind': 'message',
                'text': 'synthetic lighthouse',
                'timestamp': ctx['time'],
            }
        }
    ).encode()
    daemon.record('whatsapp', raw, ctx)
    daemon.commit()
    assert daemon.index.search('lighthouse')[0]['id'] == 'wa:synthetic:7@lid:one'
    update = json.dumps(
        {
            'ctx_update': {
                'aliases': {'7@lid': '8@s.whatsapp.net'},
                'contacts': {'8@s.whatsapp.net': 'Synthetic Sender'},
            },
            'decoded': {'kind': 'context'},
        }
    ).encode()
    daemon.record('whatsapp', update, ctx)
    daemon.commit()
    assert State(tmp_path).get('ctx:whatsapp')['aliases']['7@lid'] == '8@s.whatsapp.net'
    daemon.reindex('whatsapp')
    hits = daemon.index.search('lighthouse')
    assert len(hits) == 1
    assert hits[0]['id'] == 'wa:synthetic:8@s.whatsapp.net:one'
    assert daemon.index.lookup('wa:synthetic:7@lid:one') is None
    assert hits[0]['from'] == 'Synthetic Sender'
    daemon.reindex()
    assert daemon.index.lookup('wa:synthetic:7@lid:one') is None
    assert daemon.index.search('lighthouse')[0]['id'] == 'wa:synthetic:8@s.whatsapp.net:one'


def test_session_coordinates_reconstruct_context(tmp_path):
    import io
    from qmd2.sources.sessions import lines

    daemon = Ingestd(tmp_path, remote=False)
    rows = [
        {'type': 'session', 'id': 'synthetic', 'cwd': '/synthetic'},
        {
            'type': 'message',
            'id': 'user',
            'message': {
                'role': 'user',
                'content': [{'type': 'text', 'text': 'Synthetic lighthouse'}],
            },
        },
        {
            'type': 'message',
            'id': 'call',
            'message': {
                'role': 'assistant',
                'content': [
                    {
                        'type': 'toolCall',
                        'id': 'c',
                        'name': 'bash',
                        'arguments': {'command': 'qmd search echo_canary'},
                    }
                ],
            },
        },
        {
            'type': 'message',
            'id': 'result',
            'message': {
                'role': 'toolResult',
                'toolCallId': 'c',
                'content': [{'type': 'text', 'text': 'echo_canary'}],
            },
        },
    ]
    stream = io.BytesIO(b''.join(json.dumps(row).encode() + b'\n' for row in rows))
    for raw, before, docs, _after in lines(stream, {'runtime': 'pi', 'session': 'synthetic'}):
        daemon.record('pi', raw, before, docs)
    daemon.commit()
    assert not daemon.index.search('echo_canary')
    daemon.reindex('pi')
    assert not daemon.index.search('echo_canary')
    assert daemon.index.search('lighthouse')[0]['item'] == 'pi:synthetic'
    for row in daemon.state.packs('raw'):
        for _, header, _ in daemon.packs.iter_pack(row):
            assert 'ctx' not in header and 'calls' not in header['version']


def test_source_reparse_refreshes_failure_counts(tmp_path):
    import base64
    from qmd2.sources.gmail import PARSER_VERSION, parse

    daemon = Ingestd(tmp_path, remote=False)
    ctx = {'account': 'synthetic', 'key': 'abc123', 'time': '2026-10-13T00:00:00Z'}
    mime = b'Subject: Synthetic lighthouse\nContent-Type: text/html\n\n<p><span class>harbor</span></p>'
    raw = json.dumps(
        {
            'id': 'abc123',
            'internalDate': 1791849600000,
            'raw': base64.urlsafe_b64encode(mime).decode(),
        }
    ).encode()
    older = parse(raw, ctx)[0]
    older.update(
        parser='gmail@1', parse_failed=True, parts=[{'kind': 'boilerplate', 'text': 'Unreadable'}]
    )
    daemon.record('gmail', raw, ctx, [older])
    daemon.commit()
    assert daemon.state.get('parse_failures:gmail') == 1
    daemon.reindex('gmail')
    assert daemon.state.get('parse_failures:gmail') == 0
    assert daemon.state.get('parse_failed_id:gmail') is None
    hit = daemon.index.search('harbor')[0]
    doc = json.loads(daemon.packs.read(hit['pointer'])[1])
    assert doc['parser'] == f'gmail@{PARSER_VERSION}' and not doc.get('parse_failed')
    assert not (tmp_path / 'index-rebuild').exists()


def test_files_policy_reparse_preserves_raw_clock(tmp_path):
    import tantivy

    daemon = Ingestd(tmp_path, remote=False)
    raw = b'harbor ' * 12000 + b' lighthouse ' + b'port ' * 30000
    ctx = {
        'key': 'file:synthetic:article.txt',
        'root': 'synthetic',
        'relative': 'article.txt',
        'time': 1791849600,
    }
    daemon.record('files', raw, ctx)
    daemon.commit()
    current = daemon.index.lookup(ctx['key'])
    values = daemon.index.searcher.doc(
        daemon.index.searcher.search(daemon.index.term('id', ctx['key']), limit=1).hits[0][1]
    ).to_dict()
    values['body'] = [raw[:65536].decode()]
    daemon.index.writer.delete_documents_by_term('id', ctx['key'])
    daemon.index.writer.add_document(tantivy.Document.from_dict(values, daemon.index.schema))
    daemon.index.commit()
    assert not daemon.index.search('lighthouse')
    daemon.reindex('files')
    hit = daemon.index.search('lighthouse')[0]
    assert len(daemon.index.lookup(hit['id'])['body'].encode()) > 65536
    assert hit['version']['raw'] == current['version']['raw']
    assert hit['version']['capture'] == current['version']['capture']
    assert hit['version']['epoch'] == 1
    assert hit['pointer'] != current['pointer']


def test_sliced_reparse_restart_and_live_spool(tmp_path):
    import os
    import subprocess
    import sys
    from qmd2.index import Index

    script = '''
import hashlib,json,sys
from pathlib import Path
from qmd2.ingestd import Ingestd
home=Path(sys.argv[1]); phase=sys.argv[2]
d=Ingestd(home,remote=False)
ctx={'account':'synthetic','key':'one','time':'2026-10-13T00:00:00Z'}
def payload(id,chat='7@lid'):
 return json.dumps({'decoded':{'id':id,'chat':chat,'kind':'message','text':'synthetic lighthouse','timestamp':ctx['time']}}).encode()
if phase=='start':
 for id in ('one','two'):
  d.record('whatsapp',payload(id),dict(ctx,key=id))
 d.record('whatsapp',payload('vanished','9@s.whatsapp.net'),dict(ctx,key='vanished'))
 olddoc={'id':'wa:synthetic:9@s.whatsapp.net:vanished','item':'wa:synthetic:9@s.whatsapp.net:vanished','source':'whatsapp','group':'mail','kind':'message','time':ctx['time'],'parser':'whatsapp@0','parts':[{'kind':'authored','text':'older synthetic text'}]}
 d.record('whatsapp',json.dumps({'decoded':{'kind':'context'}}).encode(),dict(ctx,key='vanished'),[olddoc])
 d.commit()
 for pack in d.state.packs('raw'):d.packs.seal(pack['stream'],pack['source'],pack['seq'])
 d.commit()
 d.queue_checkpoint('ctx:whatsapp',{'aliases':{'7@lid':'8@s.whatsapp.net'}});d.commit()
 (home/'reindex.request').write_text(json.dumps({'id':'sliced','source':'whatsapp','lease_held':True}))
 d.request(seconds=.000001)
 assert (home/'reindex.request').exists()
 d.last_status=0;d.publish_status()
 status=json.loads((home/'status.json').read_text())['maintenance']
 assert status['source']=='whatsapp' and status['stage'] in ('prepare','read','tombstones')
 assert status['started_at'] <= status['last_slice_at']
 assert set(status)=={'source','stage','started_at','last_slice_at'}
 assert d.index.lookup('wa:synthetic:7@lid:one')
 root=home/'spool/whatsapp';root.mkdir(parents=True)
 raw=payload('live');(root/'payload').write_bytes(raw)
 entry={'schema':'qmd.spool-entry.v1','source':'whatsapp','account':'synthetic','key':'live','path':'payload','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'captured_at':'2026-10-14T00:00:00Z'}
 (root/'index.jsonl').write_text(json.dumps(entry)+'\\n')
 d.spool();d.commit()
 assert d.index.lookup('wa:synthetic:8@s.whatsapp.net:live')['version']['epoch']==1
else:
 assert d.maintenance and d.maintenance['cursor']
 assert d.state.get('epoch:whatsapp')==1
 for _ in range(30):
  d.request(seconds=.000001)
  if not (home/'reindex.request').exists():break
 assert json.loads((home/'reindex.result').read_text())=={'id':'sliced','ok':True}
 d.last_status=0;d.publish_status()
 assert json.loads((home/'status.json').read_text())['maintenance'] is None
 assert not d.index.lookup('wa:synthetic:7@lid:one')
 assert not d.index.lookup('wa:synthetic:7@lid:two')
 assert d.index.lookup('wa:synthetic:9@s.whatsapp.net:vanished')
 assert d.index.lookup('wa:synthetic:8@s.whatsapp.net:live')
'''
    env = dict(os.environ, HOME=str(tmp_path))
    lease = tmp_path / 'workbench/tmp/qmd-target/heavy.lease'
    lease.mkdir(parents=True)
    (lease / 'owner').write_text('synthetic owner\n')
    store = tmp_path / 'store'
    for phase in ('start', 'resume'):
        subprocess.run([sys.executable, '-c', script, str(store), phase], env=env, check=True)
    assert (lease / 'owner').read_text() == 'synthetic owner\n'
    assert len(Index(store).search('lighthouse')) == 4


def test_sliced_full_rebuild_restart_reaches_live_tail(tmp_path):
    import subprocess
    import sys
    from qmd2.index import Index

    script = '''
import sys
from pathlib import Path
from qmd2.ingestd import Ingestd
from qmd2.index import Index
home=Path(sys.argv[1]); phase=sys.argv[2]
d=Ingestd(home,remote=False)
def write(source,key):
 identifier = 'file:synthetic:'+key if source == 'files' else source+':'+key
 doc={'id':identifier,'item':identifier,'source':source,'group':'mail','kind':'message','time':'2026-10-13T00:00:00Z','parser':source+'@1','parts':[{'kind':'authored','text':'synthetic lighthouse'}]}
 d.record(source,b'{}',{'key':key},[doc])
if phase=='start':
 write('gmail','one');write('whatsapp','two');d.commit()
 assert not d.reindex(seconds=.000001)
 assert d.visible_index.writer is None
 d.last_status=0;d.publish_status()
 status=__import__('json').loads((home/'status.json').read_text())['maintenance']
 assert status['source'] is None and status['stage']=='read'
 assert status['started_at'] <= status['last_slice_at']
 write('gmail','appended');write('files','earlier');d.commit()
 assert Index(home).lookup('gmail:one')
 assert not Index(home).lookup('gmail:appended')
 assert not d.index.lookup('gmail:appended')
else:
 assert d.visible_index.writer is None
 for _ in range(30):
  if d.reindex(seconds=.000001):break
 else:raise AssertionError('rebuild did not reach tail')
 assert d.visible_index is None
 assert d.index.lookup('gmail:appended')
 assert d.index.lookup('file:synthetic:earlier')
 assert d.state.get('maintenance') is None
 d.last_status=0;d.publish_status()
 assert __import__('json').loads((home/'status.json').read_text())['maintenance'] is None
'''
    store = tmp_path / 'store'
    for phase in ('start', 'resume'):
        subprocess.run([sys.executable, '-c', script, str(store), phase], check=True)
    assert len(Index(store).search('lighthouse')) == 4


def test_sliced_session_import_context_and_payload_offsets(tmp_path):
    import subprocess
    import sys
    from qmd2.index import Index

    script = '''
import json,sys
from pathlib import Path
from qmd2.ingestd import Ingestd
home=Path(sys.argv[1]); phase=sys.argv[2]
d=Ingestd(home,remote=False)
if phase=='start':
 rows=[{'type':'session','id':'synthetic'}, {'type':'message','id':'call','message':{'role':'assistant','content':[{'type':'toolCall','id':'c','name':'bash','arguments':{'command':'qmd search echo_canary'}}]}}, {'type':'message','id':'result','message':{'role':'toolResult','toolCallId':'c','content':[{'type':'text','text':'echo_canary'}]}}, {'type':'message','id':'user','message':{'role':'user','content':'synthetic lighthouse'}}]
 raw=b''.join(json.dumps(row).encode()+b'\\n' for row in rows)
 d.packs.append('raw','pi',d.raw_header({'session':'synthetic','runtime':'pi','origin_start':0,'payload_start':0},1,'session-import'),raw)
 d.commit()
 assert not d.reindex('pi',seconds=.000001)
 assert d.maintenance['payload_off']>0
else:
 for _ in range(30):
  if d.reindex('pi',seconds=.000001):break
 else:raise AssertionError('reparse did not finish')
 assert not d.index.search('echo_canary')
 hit=d.index.search('lighthouse')[0]
 doc=json.loads(d.packs.read(hit['pointer'])[1])
 rawdoc=d.packs.read(doc['raw'])[1]
 pointer=doc['raw'];line=rawdoc[pointer['payload_off']:pointer['payload_off']+pointer['payload_len']]
 assert json.loads(line)['id']=='user'
'''
    store = tmp_path / 'store'
    for phase in ('start', 'resume'):
        subprocess.run([sys.executable, '-c', script, str(store), phase], check=True)
    assert Index(store).lookup('pi:synthetic#result')


def test_reparse_preserves_capture_order_across_later_custody_raw(tmp_path):
    daemon = Ingestd(tmp_path, remote=False)
    ctx = {'account': 'synthetic', 'key': 'one', 'time': '2026-10-13T00:00:00Z'}

    def raw(text):
        return json.dumps(
            {
                'decoded': {
                    'id': 'one',
                    'chat': '9@s.whatsapp.net',
                    'kind': 'message',
                    'text': text,
                    'timestamp': ctx['time'],
                }
            }
        ).encode()

    latest = daemon.record('whatsapp', raw('newer lighthouse'), ctx, capture=200)
    daemon.record('whatsapp', raw('older harbor'), ctx, capture=100)
    daemon.commit()
    id = 'wa:synthetic:9@s.whatsapp.net:one'
    before = daemon.index.lookup(id)
    assert before['version']['capture'] == 200 and before['version']['raw'] == latest
    while not daemon.reindex('whatsapp', seconds=0.000001):
        pass
    after = daemon.index.lookup(id)
    assert after['version'] == dict(before['version'], epoch=1)
    assert after['pointer'] != before['pointer']
    assert daemon.index.search('lighthouse') and not daemon.index.search('harbor')
    while not daemon.reindex(seconds=0.000001):
        pass
    assert daemon.index.lookup(id)['version'] == after['version']
    assert daemon.index.search('lighthouse') and not daemon.index.search('harbor')


def test_reparse_raw_only_winners_and_any_raw_keeps_id(tmp_path):
    import base64

    for order in ('mime-first', 'metadata-first'):
        daemon = Ingestd(tmp_path / order, remote=False)
        ctx = {'account': 'synthetic', 'key': 'recovered', 'time': '2026-10-14T00:00:00Z'}
        metadata = json.dumps({'message': {'id': 'recovered'}}).encode()
        mime = json.dumps(
            {
                'id': 'recovered',
                'internalDate': 1791849600000,
                'raw': base64.urlsafe_b64encode(
                    b'Subject: Synthetic recovery\nContent-Type: text/plain\n\nlighthouse'
                ).decode(),
            }
        ).encode()
        stale = {
            'id': 'gmail:synthetic:recovered',
            'item': 'gmail:synthetic:recovered',
            'source': 'gmail',
            'group': 'mail',
            'kind': 'email',
            'time': ctx['time'],
            'parser': 'gmail@3',
            'parse_failed': True,
            'parts': [{'kind': 'binary', 'text': 'Unreadable message'}],
        }
        if order == 'mime-first':
            daemon.record('gmail', mime, ctx, capture=100)
        daemon.record('gmail', metadata, ctx, [dict(stale)], capture=900)
        if order == 'metadata-first':
            daemon.record('gmail', mime, ctx, capture=100)
        empty = dict(stale, id='gmail:synthetic:empty', item='gmail:synthetic:empty')
        daemon.record(
            'gmail',
            json.dumps({'message': {'id': 'empty'}}).encode(),
            dict(ctx, key='empty'),
            [empty],
            capture=1000,
        )
        daemon.commit()
        assert daemon.index.lookup(stale['id'])['version']['capture'] == 900
        assert not daemon.index.search('lighthouse')
        while not daemon.reindex('gmail', seconds=0.000001):
            pass
        hit = daemon.index.search('lighthouse')[0]
        assert hit['id'] == stale['id']
        assert hit['version']['capture'] == 100
        assert hit['version']['time'] == 1791849600
        assert daemon.index.lookup('gmail:synthetic:empty') is None
        assert daemon.state.get('parse_failures:gmail') == 0
        assert hit['version']['epoch'] == 1
        tombstones = [
            json.loads(body)
            for row in daemon.state.packs('doc')
            for _, head, body in daemon.packs.iter_pack(row)
            if head['kind'] == 'tombstone'
        ]
        assert tombstones and all(doc['version']['epoch'] == 1 for doc in tombstones)
        daemon.queue_checkpoint('epoch:gmail', 0)
        daemon.commit()
        while not daemon.reindex(seconds=0.000001):
            pass
        assert daemon.index.search('lighthouse')[0]['version'] == hit['version']
        assert daemon.index.lookup('gmail:synthetic:empty') is None
        assert daemon.state.get('epoch:gmail') == 1
