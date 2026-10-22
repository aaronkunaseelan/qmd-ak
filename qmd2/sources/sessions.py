"""Native session turns and complete-line readers with checkpointed call context."""

import copy
import json
import re
from pathlib import Path

PARSER_VERSION = 3
EXCLUDED_PROJECTS = (
    str(Path.home() / '.local/state/steward'),
    str(Path.home() / '.local/share/steward'),
    str(Path.home() / '.local/share/gsd'),
)
EXCLUDED_PROJECT_PATHS = tuple(Path(p) for p in EXCLUDED_PROJECTS)
ROOTS = [
    ('pi', Path.home() / 'agents/pi/sessions'),
    ('codex', Path.home() / '.codex/sessions'),
    ('claude', Path.home() / '.claude/projects'),
    ('codex', Path.home() / 'agents/codex/sessions'),
    ('claude', Path.home() / 'agents/claude-code/projects'),
]
QMD = re.compile(r'(?<![\w-])(?:~?/[^\s"\x27;]*)?qmd2?(?=\s|["\x27;]|$)')


def excluded(project):
    return (
        isinstance(project, str)
        and bool(project)
        and any(Path(project).is_relative_to(p) for p in EXCLUDED_PROJECT_PATHS)
    )


def tool(name, arguments):
    if name == 'qmd':
        return 'qmd'
    if isinstance(arguments, dict):
        command = arguments.get('command', arguments.get('cmd', arguments.get('code', '')))
    else:
        command = str(arguments)
    return 'qmd' if QMD.search(str(command)) else name


def clean(value):
    if isinstance(value, str):
        value = re.sub(r'[\ud800-\udfff]', '\ufffd', value)
        if value.lstrip().startswith(('{', '[')):
            try:
                return clean(json.loads(value))
            except ValueError:
                pass
        return value
    if isinstance(value, list):
        return '\n'.join(
            clean(p.get('text', p.get('content', ''))) for p in value if isinstance(p, dict)
        )
    if isinstance(value, dict):
        return '\n'.join(f'{k}: {clean(v)}' for k, v in value.items())
    return str(value) if value is not None else ''


def prepare(row, ctx):
    ctx = copy.deepcopy(ctx)
    m = row.get('message', row.get('payload', row))
    if not isinstance(m, dict):
        return ctx
    typ = row.get('type')
    if typ in ('session', 'session_meta'):
        ctx['session'] = str(m.get('id', m.get('session_id', ctx.get('session', 'unknown'))))
    if row.get('sessionId'):
        ctx['session'] = row['sessionId']
    project = row.get('cwd', m.get('cwd'))
    if project:
        ctx['project'] = project
    if excluded(ctx.get('project')):
        return {k: v for k, v in ctx.items() if k not in ('first_user', 'title', 'calls')}
    if typ in ('custom-title', 'session_info'):
        ctx['title'] = row.get(
            'customTitle', row.get('title', row.get('name', ctx.get('title', '')))
        )
    calls = ctx.setdefault('calls', {})
    if m.get('type') in ('function_call', 'custom_tool_call'):
        calls[m.get('call_id', m.get('id', ''))] = tool(
            m.get('name', ''), m.get('arguments', m.get('input', ''))
        )
    content = m.get('content', [])
    if isinstance(content, list):
        for part in content:
            if isinstance(part, dict) and part.get('type') in ('tool_use', 'toolCall'):
                calls[part.get('id', '')] = tool(
                    part.get('name', ''), part.get('input', part.get('arguments', {}))
                )
    if m.get('role') == 'user' and not ctx.get('first_user'):
        first = clean(
            [p for p in content if isinstance(p, dict) and p.get('type') in ('text', 'input_text')]
            if isinstance(content, list)
            else content
        )
        if first:
            ctx['first_user'] = first[:500]
    # Checkpoint keeps only open tool calls; completed() removes the rest.
    return ctx


def completed(row, ctx):
    m = row.get('message', row.get('payload', row))
    if not isinstance(m, dict):
        return ctx
    calls = ctx.get('calls', {})
    if m.get('type') in ('function_call_output', 'custom_tool_call_output'):
        calls.pop(m.get('call_id'), None)
    if m.get('role') in ('toolResult', 'tool'):
        calls.pop(m.get('toolCallId', m.get('tool_call_id')), None)
    if isinstance(m.get('content'), list):
        for p in m['content']:
            if isinstance(p, dict) and p.get('type') == 'tool_result':
                calls.pop(p.get('tool_use_id'), None)
    return ctx


def parse(raw, ctx):
    if excluded(ctx.get('project')):
        return []
    runtime = ctx['runtime']
    session = ctx.get('session', 'unknown')
    offset = ctx.get('offset', 0)
    try:
        row = json.loads(raw)
        if not isinstance(row, dict):
            raise ValueError()
        m = row.get('message', row.get('payload', row))
        if not isinstance(m, dict):
            return []
        if row.get('type') == 'event_msg':
            return []
        mt, role = m.get('type', ''), m.get('role', '')
        if role in ('system', 'developer') or mt in ('reasoning', 'agent_message'):
            return []
        parts = _session_parts(m, role, mt, ctx.get('calls', {}))
        if not parts:
            return []
        entry = row.get('uuid') or (row.get('id') if runtime == 'pi' else None) or offset
        item = f'{runtime}:{session}'
        context = {k: ctx[k] for k in ('title', 'project', 'first_user', 'parent') if ctx.get(k)}
        if ctx.get('agent'):
            context['parent'] = item
            item += ':' + ctx['agent']
        doc = {
            'id': f'{item}#{entry}',
            'item': item,
            'source': runtime,
            'group': 'sessions',
            'kind': 'authored' if any(p['kind'] == 'authored' for p in parts) else 'tool_output',
            'time': row.get(
                'timestamp', m.get('timestamp', ctx.get('time', '1970-01-01T00:00:00Z'))
            ),
            'title': ctx.get('title') or ctx.get('first_user', '')[:160] or session,
            'from': role or ('assistant' if 'call' in mt else ''),
            'where': ctx.get('project', ''),
            'context': context,
            'parts': parts,
            'parser': f'{runtime}@{PARSER_VERSION}',
        }
        return [doc]
    except (ValueError, TypeError, KeyError, AttributeError):
        return [
            {
                'id': f'{runtime}:{session}#{offset}',
                'item': f'{runtime}:{session}',
                'source': runtime,
                'group': 'sessions',
                'kind': 'binary',
                'time': ctx.get('time', '1970-01-01T00:00:00Z'),
                'context': {},
                'parts': [
                    {'kind': 'binary', 'text': 'Unreadable session record', 'bytes': len(raw)}
                ],
                'parser': f'{runtime}@{PARSER_VERSION}',
                'parse_failed': True,
            }
        ]


def _part(parts, kind, value, name=None):
    txt = clean(value)
    if txt or name:
        p = {'kind': kind, 'text': txt, 'bytes': len(txt.encode())}
        if name:
            p['tool'] = name
        parts.append(p)


def _session_parts(m, role, mt, calls):
    parts = []

    if mt in ('function_call', 'custom_tool_call'):
        args = m.get('arguments', m.get('input', ''))
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except ValueError:
                pass
        _part(parts, 'tool_output', args, tool(m.get('name', ''), args))
    elif mt in ('function_call_output', 'custom_tool_call_output'):
        _part(parts, 'tool_output', m.get('output', ''), calls.get(m.get('call_id'), 'unknown'))
    elif role in ('user', 'assistant', 'toolResult', 'tool'):
        content = m.get('content', [])
        if isinstance(content, str):
            content = [{'type': 'text', 'text': content}]
        for p in content:
            if not isinstance(p, dict):
                continue
            pt = p.get('type')
            if pt in ('text', 'input_text', 'output_text'):
                is_tool = role in ('toolResult', 'tool')
                name = (
                    calls.get(
                        m.get('toolCallId', m.get('tool_call_id')), m.get('toolName', 'unknown')
                    )
                    if is_tool
                    else None
                )
                _part(parts, 'tool_output' if is_tool else 'authored', p.get('text', ''), name)
            elif pt in ('tool_use', 'toolCall'):
                args = p.get('input', p.get('arguments', {}))
                _part(parts, 'tool_output', args, tool(p.get('name', ''), args))
            elif pt == 'tool_result':
                _part(
                    parts,
                    'tool_output',
                    p.get('content', ''),
                    calls.get(p.get('tool_use_id'), 'unknown'),
                )
            elif pt in ('image', 'image_url', 'input_image'):
                _part(parts, 'binary', 'Image attachment')
    return parts


def context(path, runtime):
    path = Path(path)
    session = path.stem
    ctx = {'runtime': runtime, 'session': session, 'offset': 0, 'calls': {}}
    if path.parent.name == 'subagents' or path.stem.startswith('agent-'):
        ctx['agent'] = path.stem.removeprefix('agent-')
        if path.parent.name == 'subagents':
            ctx['session'] = path.parent.parent.name
    return ctx


def lines(f, ctx, budget=4 * 1024 * 1024):
    used = 0
    while used < budget:
        off = f.tell()
        raw = f.readline()
        if not raw or not raw.endswith(b'\n'):
            f.seek(off)
            return
        used += len(raw)
        try:
            row = json.loads(raw)
        except ValueError:
            row = {}
        before = prepare(row, dict(ctx, offset=off))
        docs = parse(raw, before)
        after = completed(row, copy.deepcopy(before))
        after['offset'] = f.tell()
        yield raw, before, docs, after
        ctx = after


def inventory(roots=ROOTS):
    out = []
    failed = {}
    for runtime, root in roots:
        operation = 'inventory root'
        try:
            root.stat()
            if not root.is_dir():
                raise NotADirectoryError()
            operation = 'inventory list'
            for path in root.rglob('*.jsonl'):
                operation = 'inventory stat'
                st = path.stat()
                out.append((runtime, path, st))
                operation = 'inventory list'
        except OSError as error:
            failed.setdefault(runtime, set()).add(f'{operation}: {type(error).__name__}')
    return sorted(out, key=lambda r: (-r[2].st_mtime, str(r[1]))), failed


def import_context(f, runtime, key):
    ctx = {'runtime': runtime, 'session': key.rsplit(':', 1)[-1], 'offset': 0, 'calls': {}}
    pos = f.tell()
    first = f.readline()
    try:
        row = json.loads(first)
    except ValueError:
        row = {}
    if row.get('schema') == 'qmd.session-envelope.v1':
        ctx['offset'] = row.get('start_offset', 0)
        ctx['payload_start'] = f.tell()
        source = row.get('source', {})
        ctx['runtime'] = source.get('tool', runtime)
        ctx['session'] = Path(source.get('path_key', key)).stem
    else:
        f.seek(pos)
    return ctx
