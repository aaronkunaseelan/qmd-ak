"""Installed-CLI replay; inputs and query text stay outside git, reports contain ids/counts."""

import argparse
import json
import os
import math
import statistics
import random
import re
import subprocess
import time
from pathlib import Path

MARK = re.compile(
    r'<(?:div|p|span|table|td|a|html|style|br|img)\b|\{"[a-z_]+": ?|[A-Za-z0-9+/=]{80,}', re.I
)
ECHO = re.compile(
    r'tool_call qmd|qmd (find|search|lookup|recall)|"action": ?"(find|recall|lookup)"|action: (find|recall|lookup|search)',
    re.I,
)
UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.I)
MAP = {
    'sessions': 'sessions',
    'files': 'files',
    'cot': None,
    'email': 'mail',
    'evidence': 'files',
    'gsd': 'files',
}


def sample(calls):
    unique = []
    seen = set()
    for i, call in enumerate(calls):
        args = call['args']
        if (args.get('action') or args.get('verb')) not in ('find', 'recall', 'lookup', 'search'):
            continue
        q = args.get('query')
        if not isinstance(q, str) or not q.strip() or q.strip() in seen:
            continue
        seen.add(q.strip())
        follow = None
        for next_call in calls[i + 1 : i + 15]:
            if (
                next_call['proj'] != call['proj']
                or not 0 <= next_call['at'] - call['at'] <= 30 * 60 * 1000
            ):
                break
            act = next_call['args'].get('action') or next_call['args'].get('verb')
            if act in ('find', 'recall', 'lookup', 'search'):
                break
            if act == 'get' and not next_call.get('err') and next_call.get('res_len', 0):
                follow = next_call
                break
        unique.append((call, follow))
    return random.Random(11).sample(unique, 60)


def known_item(follow):
    if not follow:
        return None
    args = follow['args']
    target = str(args.get('target', args.get('locator', args.get('ref', ''))))
    text = target + '\n' + follow.get('res_head', '')
    matches = UUID.findall(text)
    if not matches:
        return None
    if any(s in text for s in ('claude', 'mac-cc', 'oci-cc')):
        runtime = 'claude'
    elif any(s in text for s in ('codex', 'mac-cx', 'oci-cx')):
        runtime = 'codex'
    elif any(s in text for s in ('session', 'oci:', 'mac:', 'pi:')):
        runtime = 'pi'
    else:
        return None
    return runtime + ':' + matches[-1]


def query(call):
    q = call['args']['query'].strip()
    legs = {}
    for line in q.splitlines():
        key, _, value = line.partition(':')
        if key.strip() in ('lex', 'vec', 'hyde', 'intent') and value.strip():
            legs.setdefault(key.strip(), value.strip())
    return legs.get('lex') or (q if not legs else legs.get('vec') or legs.get('hyde') or q)


def run(cli, args):
    start = time.monotonic()
    env = {
        k: v
        for k, v in os.environ.items()
        if k
        not in (
            'PI_SESSION_ID',
            'CLAUDE_SESSION_ID',
            'CLAUDE_CODE_SESSION_ID',
            'CODEX_THREAD_ID',
            'CODEX_SESSION_ID',
        )
    }
    try:
        result = subprocess.run([cli, *args], capture_output=True, text=True, timeout=120, env=env)
    except subprocess.TimeoutExpired:
        return time.monotonic() - start, 4, None
    elapsed = time.monotonic() - start
    try:
        data = json.loads(result.stdout)
    except ValueError:
        data = None
    return elapsed, result.returncode, data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--calls',
        type=Path,
        default=Path('calls.json'),
    )
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cli', default=str(Path.home() / '.local/bin/qmd2'))
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--known-items', type=Path)
    parser.add_argument('--private-output', type=Path)
    args = parser.parse_args()
    calls = json.loads(args.calls.read_text())
    resolved = json.loads(args.known_items.read_text()) if args.known_items else {}
    private = []
    rows = []
    for n, (call, follow) in enumerate(sample(calls)):
        known = resolved.get(str(n)) or known_item(follow)
        sources = [MAP.get(s) for s in call['args'].get('sources', [])]
        applicable = bool(known) or 'sessions' in sources
        if not args.all and not applicable:
            rows.append({'query_number': n, 'applicable': False})
            continue
        source = None if args.all else 'sessions'
        dt, code, hits = run(
            args.cli,
            [
                'search',
                query(call),
                '--json',
                '--limit',
                '10',
                *(['--source', source] if source else []),
            ],
        )
        hits = hits if isinstance(hits, list) else []
        echo = markup = fetched = failures = 0
        get_times = []
        review = []
        for rank, hit in enumerate(hits):
            latency, get_code, doc = run(args.cli, ['get', hit['id'], '--json'])
            get_times.append(latency)
            okay = get_code == 0 and isinstance(doc, dict) and bool(doc.get('docs'))
            fetched += int(okay)
            failures += int(not okay)
            if rank < 5:
                review.append(
                    {
                        'id': hit['id'],
                        'item': hit['item'],
                        'snippet': hit['snippet'],
                        'text': doc.get('text', '')[:6000] if okay else '',
                    }
                )
                snippet = hit['snippet']
                markup += bool(MARK.search(snippet))
                if ECHO.search(snippet) and okay:
                    parts = [p for d in doc['docs'] for p in d.get('parts', [])]
                    echo += any(
                        p.get('tool') == 'qmd' and snippet.strip() in p.get('text', '')
                        for p in parts
                    )
        private.append(
            {'query_number': n, 'query': query(call), 'original_followup': follow, 'top5': review}
        )
        if args.private_output:
            args.private_output.write_text(json.dumps(private, indent=2))
        rank = next((i + 1 for i, h in enumerate(hits) if h['item'] == known), None)
        rows.append(
            {
                'query_number': n,
                'applicable': applicable,
                'dt': dt,
                'exit_code': code,
                'n': len(hits),
                'duplicate_items': len(hits) - len({h['item'] for h in hits}),
                'followup_present': bool(follow),
                'n_top5': min(5, len(hits)),
                'echo_top5': echo,
                'markup_top5': markup,
                'fetched': fetched,
                'get_failures': failures,
                'get_times': get_times,
                'known_item': known,
                'known_rank': rank,
                'known_top5': rank is not None and rank <= 5,
                'items': [h['item'] for h in hits],
                'ids': [h['id'] for h in hits],
            }
        )
        args.output.write_text(json.dumps({'rows': rows}, indent=2))
        print(
            json.dumps({'query_number': n, 'results': len(hits), 'seconds': round(dt, 3)}),
            flush=True,
        )
    applicable = [r for r in rows if 'dt' in r]
    times = sorted(r['dt'] for r in applicable)
    known = [r for r in applicable if r['known_item']]
    summary = {
        'queries': len(rows),
        'applicable': len(applicable),
        'zero_hits': sum(r['n'] == 0 for r in applicable),
        'p50': statistics.median(times) if times else None,
        'p90': times[math.ceil(len(times) * 0.9) - 1] if times else None,
        'echo_top5': sum(r['echo_top5'] for r in applicable),
        'markup_top5': sum(r['markup_top5'] for r in applicable),
        'hits_fetched': sum(r['fetched'] for r in applicable),
        'get_failures': sum(r['get_failures'] for r in applicable),
        'duplicate_items': sum(r['duplicate_items'] for r in applicable),
        'followups': sum(r['followup_present'] for r in applicable),
        'unresolved_followups': sum(
            r['followup_present'] and not r['known_item'] for r in applicable
        ),
        'known_queries': len(known),
        'known_top5': sum(r['known_top5'] for r in known),
    }
    args.output.write_text(json.dumps({'summary': summary, 'rows': rows}, indent=2))
    print(json.dumps(summary))


if __name__ == '__main__':
    main()
