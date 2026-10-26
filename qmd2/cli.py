"""K6 process-per-query CLI; get always reads and renders the doc pack."""

import argparse
from contextlib import nullcontext
import json
import os
import re
import sys
import time
import tempfile
import uuid
from pathlib import Path

from .doc import ceiling, dumps, render, strict_date, visible
from .index import Index
from .packs import Packs
from .state import DEFAULT_HOME, State
from .reads import Reader
from .exits import EXIT_CODES, LEGEND


def get_docs(home, id, cold=False):
    reader = Reader(home, cold)
    try:
        return reader.get(id)
    finally:
        reader.close()


def status(home):
    return json.loads((Path(home) / 'status.json').read_text())


def read_command(home, args):
    reader = Reader(home, getattr(args, 'cold', False))
    try:
        if args.command == 'grep':
            return grep_command(reader, args)
        identifiers = args.ids if args.command == 'get' else [args.id]
        results, missing, failed = [], False, False
        for identifier in identifiers:
            try:
                if getattr(args, 'materialise', False):
                    from .attachments import demand

                    row = demand(
                        home,
                        identifier,
                        lambda message: print(message, file=sys.stderr, flush=True),
                        args.pages,
                    )
                    if row['state'] != 'done':
                        results.append(
                            dict(id=row['id'], state=row['state'], reason=row.get('reason'))
                        )
                        failed |= row['state'] == 'failed'
                        continue
                    docs = [row['output']]
                else:
                    docs = (
                        reader.thread(identifier, args.around)
                        if args.command == 'thread'
                        else reader.get(identifier)
                    )
                results.append(read_result(reader, args, identifier, docs))
            except LookupError:
                from .reads import read_identifier

                identifier = read_identifier(identifier)
                print(
                    'qmd2: not found: ' + json.dumps(identifier, ensure_ascii=False),
                    file=sys.stderr,
                )
                missing = True
        if args.json:
            print(dumps(results[0] if len(identifiers) == 1 and results else results))
        else:
            for result in results:
                if len(identifiers) > 1:
                    print(f"=== {result['id']} ===")
                print(result.get('text', dumps(result)))
        return EXIT_CODES['missing' if missing else 'failed' if failed else 'ok'][0]
    finally:
        reader.close()


def grep_command(reader, args):
    rows = list(reader.grep(args.pattern, args.since, args.until, args.source, args.limit))
    print(
        dumps(rows)
        if args.json
        else '\n'.join(
            f"{r['id']}  {r['time']}  {r['sender']}/{r['chat']}  {r['line']}" for r in rows
        )
    )
    return EXIT_CODES['ok'][0]


def read_result(reader, args, identifier, docs):
    heads = int(args.attachments.split(':')[1]) if getattr(args, 'attachments', None) else None
    output = '\n\n'.join(
        (
            f"=== {d['id']}{' [asked]' if d['id'] == identifier else ''} ===\n"
            if args.command == 'thread'
            else ''
        )
        + reader.view(d, args.no_quoted, heads)
        for d in docs
    )
    maximum = getattr(args, 'max_chars', None)
    truncated = maximum is not None and len(output) > maximum
    if maximum:
        output = output[:maximum]
    if args.no_quoted:
        docs = [
            dict(d, parts=[p for p in d.get('parts', []) if p['kind'] != 'quoted']) for d in docs
        ]
    return dict(
        id=identifier,
        text=output,
        truncated=truncated,
        docs=docs if not truncated else None,
    )


DATE_HELP = 'ISO date/time (UTC if no offset); invalid dates are errors.'


def flag_date(value):
    try:
        return strict_date(value).isoformat()
    except (ValueError, TypeError, OverflowError):
        raise argparse.ArgumentTypeError('invalid ISO date/time') from None


def add_read_parsers(commands):
    get = commands.add_parser(
        'get',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            'Fetch exact native ids; a session item id returns its entries.\n'
            'gmail:<account>:<message>; wa:<account>:<canonical chat>:<message>\n'
            'x:<account>:<tweet>; instapaper:<account>:<bookmark>\n'
            'fathom:<account>:<recording>; calendar:<calendar id>:<event>\n'
            'file:<root>:<relative path>; <claude|codex|pi>:<session>#<entry>\n'
            '<parent>/att/<position>[/<transcript|ocr|summary>]\n'
            'manual:<sha256>; link:<normalized URL sha256>; gsd-events:<change_set_id>'
        ),
    )
    get.add_argument('ids', nargs='+')
    get.add_argument(
        '--max-chars', type=int, help='Cap rendered text, not raw bytes; JSON marks truncation.'
    )
    get.add_argument('--json', action='store_true')
    get.add_argument(
        '--raw', action='store_true', help='Original bytes for one id; no rendered options.'
    )
    get.add_argument(
        '--out', type=Path, help='Write --raw bytes to this file (required for credentials).'
    )
    get.add_argument(
        '--materialise',
        action='store_true',
        help='Capture/extract and store attachment or URL; cached terminal results are reused. Fetches may use network; Azure audio/OCR is paid; HTML/text decode is local.',
    )
    get.add_argument('--pages', type=int)
    get.add_argument('--no-quoted', action='store_true')
    get.add_argument('--attachments', metavar='head:N')
    thread = commands.add_parser(
        'thread',
        description='Gmail thread or WhatsApp chat around a message only; use get for sessions.',
    )
    thread.add_argument('id')
    thread.add_argument('--around', type=int, default=8)
    thread.add_argument('--no-quoted', action='store_true')
    thread.add_argument('--json', action='store_true')
    grep = commands.add_parser(
        'grep', description='Regex over complete doc parts in an inclusive item-time window.'
    )
    grep.add_argument('regex')
    grep.add_argument('--since', required=True, type=flag_date, help=DATE_HELP)
    grep.add_argument('--until', required=True, type=flag_date, help=DATE_HELP)
    grep.add_argument('--source', action='append')
    grep.add_argument('--limit', type=int, default=100)
    grep.add_argument('--json', action='store_true')
    get.add_argument('--cold', action='store_true', help=argparse.SUPPRESS)


def build_parser():
    parser = argparse.ArgumentParser(
        prog='qmd2',
        epilog=LEGEND,
        description='Personal archive. QMD_UNTIL is an inclusive ISO replay ceiling for reads; explicit --until can only narrow it.',
    )
    parser.add_argument(
        '--home', type=Path, default=Path(os.environ.get('QMD2_HOME', DEFAULT_HOME))
    )
    commands = parser.add_subparsers(dest='command', required=True)
    search = commands.add_parser(
        'search',
        description='Lexical title/context/body matching, one result per item; ids are not searched: use get. QMD_UNTIL and --until are inclusive item-time ceilings.',
    )
    search.add_argument('query', nargs='?')
    search.add_argument('--lex', nargs='?', const=True)
    search.add_argument('--source')
    search.add_argument('--since', type=flag_date, help=DATE_HELP)
    search.add_argument('--until', type=flag_date, help=DATE_HELP)
    search.add_argument('--limit', type=int, default=10)
    search.add_argument('--json', action='store_true')
    add_read_parsers(commands)
    attachments = commands.add_parser('attachments').add_subparsers(
        dest='attachment_command', required=True
    )
    backfill = attachments.add_parser(
        'backfill',
        description='Operator verb: enqueue historical attachments, newest first; extraction may incur Azure cost.',
    )
    backfill.add_argument('source', choices=['x', 'whatsapp'])
    add = commands.add_parser(
        'add',
        description='Agents may add freely: stores bytes/extracted output in qmd manual source, group files, id manual:<sha256>. Uses the attachment pipeline (Azure extraction may be paid).',
    )
    add.add_argument('path')
    add.add_argument('--name')
    add.add_argument('--title')
    extract = commands.add_parser(
        'extract', help='Extract a path, stdin (-) or HTTP(S) URL without archiving or indexing.'
    )
    extract.add_argument('path', metavar='PATH|-|URL')
    extract.add_argument('--json', action='store_true', required=True)
    browse = commands.add_parser(
        'browse',
        description='Current docs by item time, newest first; inclusive date filters. Use feed to enumerate arrivals, including late items.',
    )
    browse.add_argument('--source')
    browse.add_argument('--since', type=flag_date, help=DATE_HELP)
    browse.add_argument('--until', type=flag_date, help=DATE_HELP)
    browse.add_argument('--limit', type=int, default=10)
    browse.add_argument('--cursor', type=int, default=0)
    browse.add_argument('--json', action='store_true')
    from .sources import SOURCES

    feed = commands.add_parser(
        'feed',
        description='Arrival order, not item time: includes late arrivals and stored versions. Response items carry changes; resume with next via --after, even when items is empty. QMD_UNTIL filters item time, not arrival.',
    )
    feed.add_argument('--source', required=True, choices=[name for name, *_ in SOURCES])
    start = feed.add_mutually_exclusive_group(required=True)
    start.add_argument('--after', help='Opaque next token from the preceding response.')
    start.add_argument('--since-arrival', type=flag_date, help=DATE_HELP)
    feed.add_argument('--limit', type=int, default=500)
    feed.add_argument('--json', action='store_true', required=True)
    contract = commands.add_parser('contract')
    contract.add_argument('id', choices=['qmd.status.v1', 'qmd.feed.v1', 'qmd.extract.v1'])
    stat = commands.add_parser(
        'status',
        description='fresh_through: every item published before this time is captured or explicitly excluded; null means unknown. Not an item-time enumeration boundary; use feed for late arrivals.',
    )
    stat.add_argument('--json', action='store_true')
    reindex = commands.add_parser(
        'reindex',
        description='Operator verb: request an ingestd rebuild/reparse; --reset-source tombstones current source docs and resets capture. Requires coordinated maintenance.',
    )
    mode = reindex.add_mutually_exclusive_group()
    mode.add_argument('--reparse', dest='source')
    mode.add_argument('--reset-source', dest='reset_source')
    verify = commands.add_parser(
        'verify',
        description='Operator verb: evaluates health and writes verify result/telemetry; --notify also appends local alerts on state change or daily red digest. No external messages.',
    )
    verify.add_argument('--notify', action='store_true')
    for verb in [*commands.choices.values(), backfill]:
        verb.epilog = LEGEND
    return parser


def parse_args(parser):
    args = parser.parse_args()
    if hasattr(args, 'limit') and (
        args.limit < 1 or (args.command in ('search', 'browse') and args.limit > 100)
    ):
        parser.error('limit must be positive (at most 100 for search/browse)')
    if args.command == 'search' and not (args.query or isinstance(args.lex, str)):
        parser.error('search requires a query')
    if getattr(args, 'max_chars', None) is not None and args.max_chars < 1:
        parser.error('max-chars must be positive')
    if args.command == 'get':
        if (args.out and not args.raw) or (
            args.raw
            and (
                args.json
                or args.max_chars
                or args.materialise
                or args.no_quoted
                or args.attachments
                or len(args.ids) != 1
            )
        ):
            parser.error(
                '--out requires --raw; --raw requires one id and no rendered-document options'
            )
        if args.pages is not None and (args.pages < 1 or not args.materialise):
            parser.error('--pages requires --materialise and a positive number')
        if args.attachments and not re.fullmatch(r'head:\d+', args.attachments):
            parser.error('--attachments must be head:N')
        args.id = args.ids[0]
    if args.command == 'thread' and args.around < 0:
        parser.error('--around must be nonnegative')
    try:
        if args.command in ('search', 'get', 'thread', 'grep', 'browse', 'feed'):
            ceiling()
        if args.command == 'feed':
            from .feed import position

            if args.after is not None:
                position(args.after, args.source)
            if args.since_arrival is not None:
                args.since_arrival = strict_date(args.since_arrival).timestamp()
        if args.command == 'grep':
            args.pattern = re.compile(args.regex)
    except (ValueError, re.error):
        parser.error('invalid QMD_UNTIL, feed start or regular expression')
    return args


def search_command(home, args):
    rows = Index(home).search(
        args.query or args.lex, args.source, args.since, args.limit, args.until
    )
    if args.json:
        print(dumps(rows))
    else:
        for row in rows:
            print(
                f"{row['id']}  {row['title']}{' [gone]' if row.get('gone') else ''}\n{row['snippet']}\n"
            )


def add_command(home, args, parser):
    from .attachments import add

    if args.path == '-' and not args.name:
        parser.error('stdin requires --name')
    with (
        nullcontext(sys.stdin.buffer) if args.path == '-' else Path(args.path).open('rb') as source
    ):
        row = add(
            home,
            source,
            args.name or Path(args.path).name,
            args.title,
            lambda message: print(message, file=sys.stderr, flush=True),
        )
    print(row['id'])
    print(
        render(row['output'])
        if row['state'] == 'done'
        else dumps({'id': row['id'], 'state': row['state'], 'reason': row.get('reason')})
    )
    return EXIT_CODES['failed' if row['state'] == 'failed' else 'ok'][0]


def extract_command(args):
    from .attachments import extract, fetch, stream, temporary_refusal, Unavailable

    result = dict(
        schema='qmd.extract.v1',
        kind=None,
        text=None,
        model=None,
        pages=None,
        error=None,
        title=None,
        author=None,
        published=None,
        fetched=None,
    )
    is_url = args.path.lower().startswith(('http://', 'https://'))
    row = {'ref': dict(source='link', url=args.path) if is_url else {}}
    try:
        with tempfile.TemporaryDirectory(prefix='qmd-extract-', dir='/tmp') as directory:
            path = Path(directory) / 'input'
            if is_url:
                fetch(Path(directory), row, path)
            else:
                with (
                    nullcontext(sys.stdin.buffer)
                    if args.path == '-'
                    else Path(args.path).open('rb') as source
                ):
                    stream(source, path)
            text, model, kind = extract(
                row, path, lambda message: print(message, file=sys.stderr, flush=True)
            )
            if not text.strip():
                raise Unavailable('no_text')
            result.update(kind=kind, text=text, model=model)
    except Exception as error:
        result['error'] = temporary_refusal(error)[1]
    result.update(row.get('metadata', {}))
    result['fetched'] = row.get('fetched')
    if 'azure_pages' in row:
        result['pages'] = dict(
            covered=row['azure_pages'],
            total=row.get('total_pages'),
            requested=row.get('pages_requested'),
        )
    print(dumps(result))
    return EXIT_CODES['failed' if result['error'] else 'ok'][0]


def raw_command(home, args, parser):
    entry = None
    if args.id.startswith('file:'):
        _, root, relative = args.id.split(':', 2)
        entry = State(home).get('files:' + root, {}).get(relative)
    if isinstance(entry, dict) and entry.get('raw'):
        doc = dict(entry['ctx'], source='files', where=root + '/' + relative, raw=entry['raw'])
        if not visible(doc):
            raise LookupError()
        docs = [doc]
    else:
        docs = get_docs(home, args.id)
    from .sources.files import credential_name

    if len(docs) != 1:
        parser.error('--raw requires one document id')
    doc = docs[0]
    if (
        doc['source'] == 'files'
        and (doc.get('credential', True) or credential_name(doc['where']))
        and not args.out
    ):
        parser.error('credential raw bytes require --out')
    packs = Packs(State(home))
    loc = doc['raw']
    if args.cold or not any(
        packs.path(loc['stream'], loc['source'], loc['seq'], sealed).exists()
        for sealed in (False, True)
    ):
        from .b2 import B2

        packs.b2 = B2()
    if args.out:
        with tempfile.TemporaryDirectory(prefix='.qmd-raw-', dir=args.out.parent) as directory:
            temporary = Path(directory) / 'raw'
            with temporary.open('wb') as target:
                os.fchmod(target.fileno(), 0o600)
                packs.write_raw(loc, target, args.cold)
            os.replace(temporary, args.out)
    else:
        packs.write_raw(loc, sys.stdout.buffer, args.cold)
    return EXIT_CODES['ok'][0]


def browse_command(home, args):
    rows = Index(home).browse(args.source, args.since, args.until, args.limit, args.cursor)
    print(
        dumps({'results': rows, 'cursor': args.cursor + len(rows)})
        if args.json
        else '\n'.join(f"{r['id']}  {r['title']}" for r in rows)
    )


def status_command(home, args):
    result = status(home)
    if args.json:
        from .status import view

        result = view(home, result)
    print(
        dumps(result)
        if args.json
        else '\n'.join(f"{s}: lag {r['lag_s']} s" for s, r in result['sources'].items())
    )


def reindex_command(home, args, parser):
    from .ingestd import atomic
    from .sources import SOURCES

    source = args.source or args.reset_source
    if source and source not in [s for s, _, _, _, *rest in SOURCES]:
        parser.error('unknown reparse source')
    heartbeat = json.loads((home / 'heartbeat.json').read_text())
    if time.time() - heartbeat['time'] > 300:
        raise OSError()
    request = home / 'reindex.request'
    if request.exists():
        raise OSError()
    id = uuid.uuid4().hex
    atomic(
        request,
        {
            'id': id,
            'source': source,
            'reset': bool(args.reset_source),
        },
    )
    reported_wait = False
    while request.exists():
        try:
            maintenance = json.loads((home / 'status.json').read_text()).get('maintenance')
        except (OSError, ValueError):
            maintenance = None
        if maintenance and maintenance.get('stage') == 'waiting_for_lease' and not reported_wait:
            owner = maintenance.get('lease_owner') or 'owner unknown'
            print('reindex: waiting for heavy lease: ' + owner, file=sys.stderr, flush=True)
            reported_wait = True
        time.sleep(1)
    result = json.loads((home / 'reindex.result').read_text())
    if result.get('id') != id or not result.get('ok'):
        raise OSError()
    print(dumps(result) if args.reset_source else 'reindex complete')


def main():
    parser = build_parser()
    args = parse_args(parser)
    try:
        home = args.home
        if args.command == 'contract':
            from importlib.resources import files

            print(files('qmd2').joinpath('contracts', args.id + '.json').read_text(), end='')
            return EXIT_CODES['ok'][0]
        if args.command == 'verify':
            os.environ.pop('QMD_UNTIL', None)
            from .verify import main as verify_main

            return verify_main(['--home', str(home)] + (['--notify'] if args.notify else []))
        if args.command == 'attachments':
            from .attachments import backfill

            print(dumps(backfill(home, args.source)))
            return EXIT_CODES['ok'][0]
        if args.command == 'add':
            return add_command(home, args, parser)
        if args.command == 'extract':
            return extract_command(args)
        if args.command == 'search':
            search_command(home, args)
        elif args.command in ('thread', 'grep') or (args.command == 'get' and not args.raw):
            return read_command(home, args)
        elif args.command == 'get':
            return raw_command(home, args, parser)
        elif args.command == 'browse':
            browse_command(home, args)
        elif args.command == 'feed':
            from .feed import Feed

            feed = Feed(home, args.source)
            try:
                print(dumps(feed.read(args.after, args.since_arrival, args.limit)))
            except ValueError as exc:
                parser.error(str(exc))
            finally:
                feed.close()
        elif args.command == 'status':
            status_command(home, args)
        elif args.command == 'reindex':
            reindex_command(home, args, parser)
        return EXIT_CODES['ok'][0]
    except LookupError:
        print('qmd2: not found', file=sys.stderr)
        return EXIT_CODES['missing'][0]
    except Exception as e:
        print(f'qmd2: unavailable ({type(e).__name__})', file=sys.stderr)
        return EXIT_CODES['failed'][0]


if __name__ == '__main__':
    sys.exit(main())
