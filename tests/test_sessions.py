import io
import json
import os
import re
import subprocess
import sys

from pathlib import Path

import pytest

from qmd2.doc import render, version
from qmd2.index import Index
from qmd2.packs import Packs
from qmd2.policy import text
from qmd2.sources.sessions import context, import_context, lines, parse, prepare
from qmd2.state import State


ROWS = [
    (
        'pi',
        {
            'type': 'message',
            'id': 'native',
            'message': {
                'role': 'toolResult',
                'toolCallId': 'other',
                'toolName': 'bash',
                'content': [{'type': 'text', 'text': 'synthetic \ud800 lighthouse'}],
            },
        },
        'tool_output',
        'synthetic \ufffd lighthouse',
    ),
    (
        'pi',
        {
            'type': 'message',
            'id': 'native',
            'timestamp': '2026-10-13T01:00:00Z',
            'message': {
                'role': 'user',
                'content': [{'type': 'text', 'text': 'synthetic lighthouse'}],
            },
        },
        'authored',
        'synthetic lighthouse',
    ),
    (
        'claude',
        {
            'type': 'user',
            'uuid': 'native',
            'message': {'role': 'user', 'content': 'synthetic lighthouse'},
        },
        'authored',
        'synthetic lighthouse',
    ),
    (
        'codex',
        {
            'type': 'response_item',
            'payload': {
                'type': 'message',
                'role': 'assistant',
                'content': [{'type': 'output_text', 'text': 'synthetic lighthouse'}],
            },
        },
        'authored',
        'synthetic lighthouse',
    ),
    (
        'codex',
        {
            'type': 'response_item',
            'payload': {
                'type': 'function_call',
                'name': 'exec_command',
                'call_id': 'c',
                'arguments': '{"cmd":"qmd search secretneedle"}',
            },
        },
        'tool_output',
        '',
    ),
    (
        'pi',
        {
            'type': 'message',
            'id': 'native',
            'message': {
                'role': 'assistant',
                'content': [
                    {
                        'type': 'toolCall',
                        'id': 'c',
                        'name': 'bash',
                        'arguments': {'command': '~/.local/bin/qmd2 search secretneedle'},
                    }
                ],
            },
        },
        'tool_output',
        '',
    ),
    (
        'claude',
        {
            'type': 'assistant',
            'uuid': 'native',
            'message': {
                'role': 'assistant',
                'content': [
                    {
                        'type': 'tool_use',
                        'id': 'c',
                        'name': 'Bash',
                        'input': {'command': 'qmd get secretneedle'},
                    }
                ],
            },
        },
        'tool_output',
        '',
    ),
    (
        'claude',
        {
            'type': 'user',
            'uuid': 'native',
            'message': {
                'role': 'user',
                'content': [{'type': 'tool_result', 'tool_use_id': 'c', 'content': 'secretneedle'}],
            },
        },
        'tool_output',
        '',
    ),
    (
        'pi',
        {
            'type': 'message',
            'id': 'native',
            'message': {
                'role': 'toolResult',
                'toolCallId': 'c',
                'toolName': 'bash',
                'content': [{'type': 'text', 'text': 'secretneedle'}],
            },
        },
        'tool_output',
        '',
    ),
    (
        'codex',
        {
            'type': 'response_item',
            'payload': {'type': 'function_call_output', 'call_id': 'c', 'output': 'secretneedle'},
        },
        'tool_output',
        '',
    ),
    (
        'codex',
        {'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'mirror'}},
        None,
        '',
    ),
    ('pi', {'type': 'session', 'id': 'native', 'cwd': '/synthetic'}, None, ''),
    ('claude', {'type': 'metadata', 'title': 'synthetic'}, None, ''),
    (
        'claude',
        {
            '_path': '/synthetic/projects/-home-ubuntu--local-state-steward-backups/s.jsonl',
            'message': {'role': 'user', 'content': 'synthetic lighthouse'},
        },
        'authored',
        'synthetic lighthouse',
    ),
] + [
    (
        runtime,
        {'cwd': project, 'message': {'role': 'user', 'content': 'synthetic lighthouse'}},
        None if blocked else 'authored',
        '' if blocked else 'synthetic lighthouse',
    )
    for runtime in ('claude', 'codex', 'pi')
    for project, blocked in (
        (str(Path.home()) + '/.local/state/steward', True),
        (str(Path.home()) + '/.local/state/steward/wakes/job', True),
        (str(Path.home()) + '/.local/share/steward/agents/job', True),
        (str(Path.home()) + '/.local/share/gsd/steps/job', True),
        (str(Path.home()) + '/.local/state/stewardship', False),
        (str(Path.home()) + '/.local/share/steward-other', False),
        (str(Path.home()) + '/.local/share/gsd-other', False),
        (None, False),
    )
]


@pytest.mark.parametrize('runtime,row,kind,body', ROWS)
def test_parser_table(runtime, row, kind, body):
    ctx = {'runtime': runtime, 'session': 's', 'offset': 17, 'calls': {'c': 'qmd'}}
    if row.get('_path'):
        ctx.update(context(row['_path'], runtime))
        ctx['offset'] = 17
    raw = json.dumps(row).encode()
    prepared = prepare(row, ctx)
    docs = parse(raw, prepared)
    assert ctx['offset'] == 17
    if kind is None:
        assert docs == []
    else:
        assert docs[0]['kind'] == kind
        assert text(docs[0]) == body
        native = row.get('uuid') or (row.get('id') if runtime == 'pi' else None)
        assert docs[0]['id'].endswith('#' + str(native or 17))


@pytest.mark.parametrize('runtime', ['pi', 'claude', 'codex'])
def test_source_chain(tmp_path, runtime):
    state = State(tmp_path, True)
    packs = Packs(state, writable=True)
    index = Index(tmp_path, True, state)
    row = next(r for rt, r, k, b in ROWS if rt == runtime and k == 'authored')
    raw = json.dumps(row).encode() + b'\n'
    f = io.BytesIO(raw + b'{"incomplete":')
    ctx = {'runtime': runtime, 'session': 's', 'offset': 0, 'calls': {}}
    for record, before, docs, after in lines(f, ctx):
        origin = packs.append('raw', runtime, {'id': 's', 'ctx': before}, record)
        for doc in docs:
            doc['raw'] = origin
            doc['version'] = version(doc, 1)
            pointer = packs.append('doc', runtime, {'id': doc['id']}, json.dumps(doc).encode())
            index.upsert(doc, pointer)
    packs.sync()
    index.commit()
    state.set('reader', after)
    state.commit()
    assert state.get('reader')['offset'] == len(raw)
    hit = index.search('synthetic lighthouse')[0]
    doc = json.loads(packs.read(hit['pointer'])[1])
    assert 'synthetic lighthouse' in render(doc)
    assert packs.read(doc['raw'])[1] == raw
    packs.seal('doc', runtime, pointer['seq'])
    assert json.loads(packs.read(pointer)[1]) == doc
    envelope = {
        'schema': 'qmd.session-envelope.v1',
        'start_offset': 0,
        'source': {'tool': runtime, 'path_key': 's.jsonl'},
    }
    imported = io.BytesIO(json.dumps(envelope).encode() + b'\n' + raw)
    imported_ctx = import_context(imported, runtime, 's')
    payload = imported.read()
    imported_ctx.pop('payload_start', None)
    assert parse(payload, prepare(row, imported_ctx))[0]['id'] == doc['id']

    home = tmp_path / 'home'
    store = home / '.local/share/qmd2'
    env = dict(os.environ, HOME=str(home))
    roots = {
        'pi': ['agents/pi/sessions'],
        'codex': ['.codex/sessions', 'agents/codex/sessions'],
        'claude': ['.claude/projects', 'agents/claude-code/projects'],
    }
    for names in roots.values():
        for name in names:
            (home / name).mkdir(parents=True)
    prefixes = (
        str(home) + '/.local/state/steward/',
        str(home) + '/.local/share/steward/',
        str(home) + '/.local/share/gsd/',
    )
    originals, seeded, excluded_ids, kept_ids = {}, [], set(), set()

    def session(root, name, project, body, encoded=False):
        path = home / root
        if runtime == 'claude':
            directory = re.sub(r'[^a-zA-Z0-9]', '-', project or '/synthetic')
            path /= directory
        path /= name + '.jsonl'
        path.parent.mkdir(parents=True, exist_ok=True)
        message = dict(row)
        if runtime == 'claude':
            message.update(sessionId=name, message={'role': 'user', 'content': body})
            if project and not encoded:
                message['cwd'] = project
            records = [message]
        else:
            header = {'type': 'session', 'id': name}
            if project:
                header['cwd'] = project
            if runtime == 'codex':
                header = {'type': 'session_meta', 'payload': header}
                message['payload'] = {'role': 'user', 'content': body}
            else:
                message['message'] = {'role': 'user', 'content': body}
            records = [header, message]
        data = b''.join(json.dumps(r).encode() + b'\n' for r in records)
        path.write_bytes(data)
        originals[path] = data
        return path

    for n, prefix in enumerate(prefixes):
        for r, root in enumerate(roots[runtime]):
            name = f'excluded-{n}-{r}'
            path = session(root, name, prefix + 'jobs/run', 'excludedbeacon')
            excluded_ids.add(f'{runtime}:{name}')
            if r == 0:
                seeded.append({'name': name, 'project': prefix + 'jobs/run', 'path': str(path)})
            kept = f'kept-{n}-{r}'
            session(root, kept, prefix.rstrip('/') + '-kept/run', 'keptbeacon', encoded=True)
            kept_ids.add(f'{runtime}:{kept}')
        legacy = f'legacy-{n}'
        seeded.append({'name': legacy, 'project': prefix + 'old/run'})
        excluded_ids.add(f'{runtime}:{legacy}')
    session(roots[runtime][0], 'unknown', None, 'unknownbeacon')
    session(roots[runtime][0], 'mention', '/synthetic', 'keptbeacon ' + prefixes[0])
    kept_ids.update((f'{runtime}:mention', f'{runtime}:kept-indexed'))
    kept_mtime = 1791849600
    for path in originals:
        stamp = kept_mtime + 7200 if path.stem.startswith('excluded-') else kept_mtime
        os.utime(path, (stamp, stamp))

    # Seed pre-D-170 docs, including files already checkpointed at EOF and legacy-only items.
    seed_script = '''
import json
from pathlib import Path
import sys

from qmd2.ingestd import Ingestd

runtime, home, rows = sys.argv[1:]
daemon = Ingestd(home, remote=False)
for row in json.loads(rows):
    item = runtime + ':' + row['name']
    doc = {
        'id': item + '#old',
        'item': item,
        'source': runtime,
        'group': 'sessions',
        'kind': 'authored',
        'time': '2026-10-13T01:00:00Z',
        'context': {'project': row['project']},
        'parts': [{'kind': 'authored', 'text': 'excludedbeacon'}],
        'parser': runtime + '@2',
    }
    earlier = dict(doc, id=item + '#early', context={})
    daemon.record(runtime, b'{}', {}, [earlier, doc])
    if row.get('path'):
        path = Path(row['path'])
        st = path.stat()
        ctx = dict(runtime=runtime, session=row['name'], offset=st.st_size)
        ctx['project'] = row['project']
        daemon.queue_checkpoint(
            'session:' + str(path), dict(inode=st.st_ino, offset=st.st_size, ctx=ctx)
        )
doc = dict(
    doc,
    id=runtime + ':kept-indexed#old',
    item=runtime + ':kept-indexed',
    context={'project': '/synthetic'},
    parts=[{'kind': 'authored', 'text': 'keptbeacon'}],
)
daemon.record(runtime, b'{}', {}, [doc])
daemon.commit()
'''
    subprocess.run(
        [sys.executable, '-c', seed_script, runtime, str(store), json.dumps(seeded)],
        env=env,
        check=True,
        capture_output=True,
    )

    def cli(*args, check=True):
        return subprocess.run(
            [sys.executable, '-m', 'qmd2.cli', '--home', str(store), *args],
            env=env,
            capture_output=True,
            text=True,
            check=check,
        )

    assert len(json.loads(cli('search', 'excludedbeacon', '--json').stdout)) == 6
    from qmd2.feed import Feed

    feed = Feed(store, runtime)
    before = feed.head()
    feed.close()
    for walk in range(2):
        if walk:
            for path in originals:
                if path.stem.startswith('excluded-'):
                    late = {'message': {'role': 'user', 'content': 'excludedbeacon later'}}
                    originals[path] += json.dumps(late).encode() + b'\n'
                    path.write_bytes(originals[path])
                    os.utime(path, (kept_mtime + 14400, kept_mtime + 14400))
        subprocess.run(
            [sys.executable, '-m', 'qmd2.ingestd', '--home', str(store), '--no-b2', '--once'],
            env=env,
            check=True,
            capture_output=True,
        )
        walked = State(store)
        assert walked.get('receipt:' + runtime)['source_head'] == kept_mtime + (
            14400 if walk else 7200
        )
        assert walked.get('indexed:' + runtime)['processed_through'] >= kept_mtime + (
            14400 if walk else 7200
        )
        walked.db.close()
        assert json.loads(cli('search', 'excludedbeacon', '--json').stdout) == []
        hits = json.loads(cli('search', 'keptbeacon', '--limit', '100', '--json').stdout)
        assert {h['item'] for h in hits} == kept_ids
        assert json.loads(cli('search', 'unknownbeacon', '--json').stdout)[0]['item'] == (
            runtime + ':unknown'
        )
        browsed = json.loads(cli('browse', '--source', runtime, '--limit', '100', '--json').stdout)
        assert {h['item'] for h in browsed['results']} == kept_ids | {runtime + ':unknown'}
        for item in excluded_ids:
            assert cli('get', item, check=False).returncode == 3
        for item in kept_ids:
            assert 'keptbeacon' in cli('get', item).stdout
        deletion_feed = json.loads(
            cli('feed', '--source', runtime, '--after', before, '--json').stdout
        )
        tombstones = [r for r in deletion_feed['items'] if r['tombstone']]
        assert {r['item'] for r in tombstones} == {runtime + ':' + r['name'] for r in seeded}
        assert len(tombstones) == 12
        assert all(p.read_bytes() == data for p, data in originals.items())
    current = State(store)
    receipt = current.get('receipt:' + runtime)
    assert receipt['excluded'] == 3 * len(roots[runtime])
    assert receipt['written'] == 0 and receipt['failed'] == 0
    captured = Packs(current)
    for pack in current.packs('raw'):
        assert all(b'excludedbeacon' not in body for _, _, body in captured.iter_pack(pack))
    rebuilt = Index(tmp_path / 'rebuilt', True)
    for pack in current.packs('doc'):
        for pointer, _, body in captured.iter_pack(pack):
            rebuilt.upsert(json.loads(body), pointer)
    rebuilt.commit()
    assert rebuilt.search('excludedbeacon') == []
    assert {r['item'] for r in rebuilt.search('keptbeacon', limit=100)} == kept_ids
    current.db.close()
