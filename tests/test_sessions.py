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
from qmd2.sources.sessions import context, import_context, lines, origin, parse, prepare
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
]

ORIGIN_PROJECTS = [
    (str(Path.home()) + '/.local/state/steward', 'steward'),
    (str(Path.home()) + '/.local/state/steward/wakes/165', 'steward'),
    (str(Path.home()) + '/.local/share/steward/agents/job', 'steward'),
    (str(Path.home()) + '/.local/share/gsd', 'gsd'),
    (str(Path.home()) + '/.local/share/gsd/runs/exp1-0813-0821', 'gsd'),
    (str(Path.home()) + '/.local/state/stewardship', 'interactive'),
    (str(Path.home()) + '/.local/share/steward-other', 'interactive'),
    (str(Path.home()) + '/.local/share/gsd-other', 'interactive'),
    (str(Path.home()) + '/.claude/projects/-home-ubuntu--local-state-steward', 'interactive'),
    ('/synthetic', 'interactive'),
    (None, 'interactive'),
]


@pytest.mark.parametrize('project,label', ORIGIN_PROJECTS)
def test_origin_table(project, label):
    assert origin(project) == label


@pytest.mark.parametrize('runtime', ['claude', 'codex', 'pi'])
@pytest.mark.parametrize('project,label', ORIGIN_PROJECTS)
def test_parser_origin(runtime, project, label):
    row = {'cwd': project, 'message': {'role': 'user', 'content': 'synthetic lighthouse'}}
    ctx = {'runtime': runtime, 'session': 's', 'offset': 17, 'calls': {}}
    docs = parse(json.dumps(row).encode(), prepare(row, ctx))
    assert docs[0]['origin'] == label


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


@pytest.mark.parametrize('runtime', ['claude'])
def test_origin_end_to_end(tmp_path, runtime):
    home = tmp_path / 'home'
    store = home / '.local/share/qmd2'
    env = dict(os.environ, HOME=str(home))
    for name in (
        '.claude/projects',
        'agents/claude-code/projects',
        '.codex/sessions',
        'agents/codex/sessions',
        'agents/pi/sessions',
    ):
        (home / name).mkdir(parents=True)
    # Working directories copied from real session records: steward wakes, a gsd run, and work.
    projects = {
        'steward': str(home) + '/.local/state/steward/wakes/165',
        'gsd': str(home) + '/.local/share/gsd/runs/exp1-0813-0821',
        'interactive': '/synthetic/work',
    }
    for label, project in projects.items():
        directory = home / '.claude/projects' / re.sub(r'[^a-zA-Z0-9]', '-', project)
        directory.mkdir(parents=True)
        rows = [
            {
                'type': 'user',
                'uuid': 'u1',
                'sessionId': label,
                'cwd': project,
                'timestamp': '2026-10-13T01:00:00Z',
                'message': {'role': 'user', 'content': 'origin synthetic ' + label + 'beacon'},
            },
            {
                'type': 'assistant',
                'uuid': 'a1',
                'sessionId': label,
                'cwd': project,
                'timestamp': '2026-10-13T01:01:00Z',
                'message': {
                    'role': 'assistant',
                    'content': [{'type': 'text', 'text': label + 'beacon reply only'}],
                },
            },
        ]
        (directory / (label + '.jsonl')).write_bytes(
            b''.join(json.dumps(r).encode() + b'\n' for r in rows)
        )

    def cli(*args, check=True, extra=None):
        return subprocess.run(
            [sys.executable, '-m', 'qmd2.cli', '--home', str(store), *args],
            env=dict(env, **(extra or {})),
            capture_output=True,
            text=True,
            check=check,
        )

    subprocess.run(
        [sys.executable, '-m', 'qmd2.ingestd', '--home', str(store), '--no-b2', '--once'],
        env=env,
        check=True,
        capture_output=True,
    )

    def origins(*args, extra=None):
        found = cli('search', 'origin synthetic', '--limit', '100', *args, '--json', extra=extra)
        return {row['origin']: row['item'] for row in json.loads(found.stdout)}

    assert origins() == {
        'steward': runtime + ':steward',
        'gsd': runtime + ':gsd',
        'interactive': runtime + ':interactive',
    }
    assert [
        row['item'] for row in json.loads(cli('search', 'stewardbeacon', '--json').stdout)
    ] == [runtime + ':steward']
    assert origins('--not-origin', 'steward,gsd') == {'interactive': runtime + ':interactive'}
    assert origins('--origin', 'gsd') == {'gsd': runtime + ':gsd'}
    assert origins('--origin', 'steward,interactive') == {
        'steward': runtime + ':steward',
        'interactive': runtime + ':interactive',
    }
    browsed = json.loads(
        cli(
            'browse',
            '--source',
            runtime,
            '--limit',
            '100',
            '--origin',
            'interactive',
            '--json',
        ).stdout
    )
    assert {row['item'] for row in browsed['results']} == {runtime + ':interactive'}
    assert {row['origin'] for row in browsed['results']} == {'interactive'}
    grepped = json.loads(
        cli(
            'grep',
            'beacon',
            '--since',
            '2000-01-01',
            '--until',
            '2100-01-01',
            '--not-origin',
            'steward,gsd',
            '--json',
        ).stdout
    )
    assert {row['id'].rsplit('#', 1)[0] for row in grepped} == {runtime + ':interactive'}
    fed = json.loads(
        cli(
            'feed',
            '--source',
            runtime,
            '--after',
            'qmd1.' + runtime + '.' + '0' * 20 + '.' + '0' * 20,
            '--origin',
            'gsd',
            '--json',
        ).stdout
    )
    assert {row['item'] for row in fed['items']} == {runtime + ':gsd'}
    unfiltered = json.loads(
        cli(
            'feed',
            '--source',
            runtime,
            '--after',
            'qmd1.' + runtime + '.' + '0' * 20 + '.' + '0' * 20,
            '--json',
        ).stdout
    )
    assert len(unfiltered['items']) == 6
    for item in (runtime + ':steward', runtime + ':gsd', runtime + ':interactive'):
        assert 'beacon' in cli('get', item).stdout
    bogus = cli('search', 'origin synthetic', '--not-origin', 'interactive', check=False)
    assert bogus.returncode == 2 and '--not-origin interactive' in bogus.stderr
    assert cli('search', 'origin synthetic', '--origin', 'own', check=False).returncode == 2

    # QMD_NOT_ORIGIN: the caller cannot read its own runs back, whatever it asks for.
    scoped = {'QMD_NOT_ORIGIN': 'steward,gsd'}
    assert origins(extra=scoped) == {'interactive': runtime + ':interactive'}
    assert origins('--origin', 'steward', extra=scoped) == {}
    assert origins('--not-origin', 'gsd', extra={'QMD_NOT_ORIGIN': 'steward'}) == {
        'interactive': runtime + ':interactive'
    }
    assert origins(extra={'QMD_NOT_ORIGIN': ''}) == {
        'steward': runtime + ':steward',
        'gsd': runtime + ':gsd',
        'interactive': runtime + ':interactive',
    }
    grepped = json.loads(
        cli(
            'grep',
            'beacon',
            '--since',
            '2000-01-01',
            '--until',
            '2100-01-01',
            '--json',
            extra=scoped,
        ).stdout
    )
    assert {row['id'].rsplit('#', 1)[0] for row in grepped} == {runtime + ':interactive'}
    browsed = json.loads(
        cli('browse', '--source', runtime, '--limit', '100', '--json', extra=scoped).stdout
    )
    assert {row['item'] for row in browsed['results']} == {runtime + ':interactive'}
    fed = json.loads(
        cli(
            'feed',
            '--source',
            runtime,
            '--after',
            'qmd1.' + runtime + '.' + '0' * 20 + '.' + '0' * 20,
            '--json',
            extra=scoped,
        ).stdout
    )
    assert {row['item'] for row in fed['items']} == {runtime + ':interactive'}
    assert 'beacon' in cli('get', runtime + ':steward', extra=scoped).stdout
    assert cli('status', '--json', extra=scoped).returncode == 0
    hidden = cli('search', 'origin synthetic', extra={'QMD_NOT_ORIGIN': 'interactive'}, check=False)
    assert hidden.returncode == 2 and 'QMD_NOT_ORIGIN' in hidden.stderr
    unknown = cli(
        'search', 'origin synthetic', extra={'QMD_NOT_ORIGIN': 'steward,own'}, check=False
    )
    assert unknown.returncode == 2 and 'QMD_NOT_ORIGIN' in unknown.stderr
    assert cli('status', '--json', extra={'QMD_NOT_ORIGIN': 'interactive'}).returncode == 0
