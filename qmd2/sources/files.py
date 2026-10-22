"""General publication rules and bounded walks; archived paths survive deletion."""

from contextlib import closing
import errno
import hashlib
import json
import os
import re
import stat
import sqlite3
import tempfile
import struct
import time
from datetime import datetime, timezone
from pathlib import Path

from qmd2.doc import html_text
from qmd2.accounts import file_roots
from qmd2.packs import room

PARSER_VERSION = 4
ROOTS = file_roots()
MAX_BYTES = 2 * 1024**3
MEMORY_BYTES = 32 * 1024 * 1024
ARCHIVE_CADENCE = 86400
TEXT_BYTES = 256 * 1024
CADENCE = 24 * 60
QUIET_SECONDS = 60
DIRECTORIES = {
    'node_modules',
    '.venv',
    'venv',
    'site-packages',
    'vendor',
    '__pycache__',
    'build',
    'dist',
    'target',
    '.cache',
    'cache',
    'caches',
    '.mypy_cache',
    '.pytest_cache',
    '.ruff_cache',
    '.tox',
    '.turbo',
    '.next',
    'Cache',
    'Code Cache',
    'GPUCache',
    'DawnCache',
    'ShaderCache',
}


CREDENTIAL_KEYS = rb'(?:refresh_token|client_secret|aws_secret_access_key|private_key)'
CREDENTIAL_MARKER = re.compile(
    rb'-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----|"' + CREDENTIAL_KEYS + rb'"\s*:'
    rb'|^\s*(?:export\s+)?' + CREDENTIAL_KEYS + rb'\s*=',
    re.I | re.M,
)


def credential_name(path):
    name = re.sub(r'-(?:wal|shm|journal)$', '', Path(path).name.lower())
    return (
        name.endswith(('.env', '.pem', '.key', '.p12'))
        or (name.startswith('.env.') and not name.endswith(('.example', '.sample')))
        or name.startswith(('id_rsa', 'id_dsa', 'id_ecdsa', 'id_ed25519'))
        or name
        in ('.netrc', '.git-credentials', 'login data', 'logins.json', 'key4.db', 'signons.sqlite')
        or (name.startswith(('credentials', 'client_secret')) and name.endswith('.json'))
        or name in ('cookies', 'cookies.sqlite', 'cookies.sqlite3', 'cookies.db', 'cookiejar')
        or (name.startswith(('cookies', 'cookiejar')) and name.endswith(('.txt', '.json')))
    )


def excluded_path(path):
    parts, name = Path(path).parts, Path(path).name.lower()
    if (
        name == 'core'
        or name.endswith('.core')
        or re.fullmatch(
            r'core\.\d+(?:\.(?:gz|xz|bz2|zst))?|core(?:\..*)?\.(?:gz|xz|bz2|zst)|.+\.core\.(?:gz|xz|bz2|zst)',
            name,
        )
    ):
        return 'core_dump'
    if name.endswith(('-wal', '-shm', '-journal')):
        return 'sqlite_auxiliary'
    if any(p in DIRECTORIES or p.startswith('.venv') for p in parts[:-1]):
        return 'reconstructable'
    return None


def publication(path, size, head):
    if reason := excluded_path(path):
        return reason
    if head.startswith(b'\x7fELF') and len(head) >= 18 and head[5] in (1, 2):
        if struct.unpack_from(('<' if head[5] == 1 else '>') + 'H', head, 16)[0] == 4:
            return 'core_dump'
    return 'oversize' if size > MAX_BYTES else None


def facts(st):
    return [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns]


def file_facts(path, st):
    try:
        return facts(st) + facts(Path(str(path) + '-wal').stat())
    except FileNotFoundError:
        return facts(st)


def read_state(before, after, size):
    if facts(before) != facts(after) or size != before.st_size:
        return 'moving'
    return 'settled' if time.time() - before.st_mtime >= QUIET_SECONDS else 'stable'


def backup_status(code, *_):
    if code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
        raise sqlite3.OperationalError('busy snapshot')


def capture_file(path, stream, before, target):
    head = stream.read(min(64, before.st_size))
    stream.seek(0)
    state = None
    if head.startswith(b'SQLite format 3\x00'):
        try:
            if facts(path.lstat())[:2] != facts(before)[:2]:
                raise sqlite3.OperationalError('snapshot path changed')
            with (
                closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=0)) as src,
                closing(sqlite3.connect(target)) as dst,
            ):
                estimate = (
                    src.execute('PRAGMA page_count').fetchone()[0]
                    * src.execute('PRAGMA page_size').fetchone()[0]
                )
                if estimate > MAX_BYTES:
                    return None, 'snapshot', estimate, '', False
                room(target.parent, estimate)
                src.backup(dst, pages=-1, progress=backup_status, sleep=0)
            if facts(path.lstat())[:2] != facts(before)[:2]:
                raise sqlite3.OperationalError('snapshot path changed')
            state = 'snapshot'
        except (sqlite3.Error, FileNotFoundError, NotADirectoryError):
            state = 'moving'
    if state != 'snapshot':
        room(target.parent, before.st_size)
        with target.open('wb') as output:
            remaining = before.st_size
            while remaining and (chunk := stream.read(min(remaining, 1024 * 1024))):
                output.write(chunk)
                remaining -= len(chunk)
        state = state or read_state(before, os.fstat(stream.fileno()), target.stat().st_size)
    size = target.stat().st_size
    if size > MAX_BYTES:
        return None, state, size, '', False
    if size <= MEMORY_BYTES:
        raw = target.read_bytes()
        return (
            raw,
            state,
            size,
            hashlib.sha256(raw).hexdigest(),
            bool(CREDENTIAL_MARKER.search(raw)),
        )
    digest, credential, tail = hashlib.sha256(), False, b''
    with target.open('rb') as body:
        while chunk := body.read(1024 * 1024):
            digest.update(chunk)
            probe = re.sub(rb'\s+', b'\n', tail + chunk)
            credential |= bool(
                CREDENTIAL_MARKER.search(probe) or re.search(rb'private\s+key-----', probe, re.I)
            )
            tail = probe[-256:]
    return None, state, size, digest.hexdigest(), credential


PROSE = {'.md', '.txt', '.rst', '.org', '.adoc', '.markdown', '.html'}
MACHINE = re.compile(r'\.(stdout|stderr|log|out|err|pid)(?:\.txt)?$', re.I)
DUMPS = {
    '.json',
    '.jsonl',
    '.ndjson',
    '.csv',
    '.tsv',
    '.dump',
    '.db',
    '.sqlite',
    '.sqlite3',
    '.ipynb',
}
JSON_CONFIG = re.compile(
    r'^(.*schema|package(?:-lock)?|tsconfig(?:\..+)?|jsconfig|(?:.*[.-])?(?:config|settings|manifest|schema)(?:[.-].*)?)\.json$',
    re.I,
)


def config_text(value, prefix=''):
    if isinstance(value, dict):
        return '\n'.join(config_text(v, (prefix + '.' + k).lstrip('.')) for k, v in value.items())
    if isinstance(value, list):
        return '\n'.join(config_text(v, f'{prefix}[{n}]') for n, v in enumerate(value))
    return f'{prefix}: {value}'


def parse(raw, ctx):
    if ctx.get('deleted') or '.git' in Path(ctx['relative']).parts:
        return []
    identifier = 'file:' + ctx['root'] + ':' + ctx['relative']
    path = Path(ctx['relative'])
    credential = (
        ctx.get('credential', False) or credential_name(path) or bool(CREDENTIAL_MARKER.search(raw))
    )
    doc = {
        'id': identifier,
        'item': identifier,
        'source': 'files',
        'group': 'files',
        'kind': 'file',
        'time': ctx['time'],
        'title': ctx['relative'],
        'where': ctx['root'] + '/' + ctx['relative'],
        'context': {'root': ctx['root']},
        'state': ctx.get('state'),
        'raw_sha256': ctx.get('raw_sha256') or hashlib.sha256(raw).hexdigest(),
        'credential': credential,
        'parts': [],
        'parser': f'files@{PARSER_VERSION}',
    }
    _file_parts(doc, raw, ctx, path, credential)
    return [doc]


def _file_parts(doc, raw, ctx, path, credential):
    if (
        not credential
        and not path.name.endswith(('-wal', '-shm', '-journal'))
        and not MACHINE.search(path.name)
        and (path.suffix.lower() not in DUMPS or JSON_CONFIG.fullmatch(path.name))
    ):
        try:
            if ctx.get('size', len(raw)) > TEXT_BYTES or b'\x00' in raw:
                raise UnicodeError()
            text = raw.decode('utf-8')
            if Path(ctx['relative']).suffix.lower() in ('.html', '.htm', '.xhtml'):
                text = html_text(text)
            if path.suffix.lower() == '.json':
                text = config_text(json.loads(text))
            doc['parts'] = [{'kind': 'authored', 'text': text}]
        except (UnicodeError, ValueError, RecursionError):
            doc['parts'] = [{'kind': 'binary', 'text': f"File: {ctx.get('size', len(raw))} bytes"}]


class Walker:
    def __init__(self, roots=None):
        self.roots = ROOTS if roots is None else roots
        self.active = None

    def turn(self, daemon, seconds=2):
        if self.active is None:
            if time.time() < min(
                daemon.checkpoint('files-next-due', 0),
                daemon.checkpoint('files-archive-next-due', 0),
            ):
                return
            self.active = self.walk(daemon)
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                next(self.active)
            except StopIteration:
                self.active = None
                return

    def walk(self, daemon):
        started = time.time()
        archive_due = started >= daemon.checkpoint('files-archive-next-due', 0)
        totals = dict(daemon.checkpoint('files-archive-totals', {}) or {})
        moving = 0
        lanes = {
            name: dict(written=0, unchanged=0, excluded=0, failed=0, bytes=0, moving=0)
            for name in ('files', 'files-archive')
        }
        for root_name, root in self.roots:
            moving += yield from self.walk_root(
                daemon, root_name, root, lanes, archive_due, started, totals
            )
        for name, cadence in (('files', CADENCE), ('files-archive', ARCHIVE_CADENCE)):
            if name == 'files-archive' and not archive_due:
                continue
            counts = lanes[name]
            counts['enumerated'] = sum(
                counts[k] for k in ('written', 'unchanged', 'excluded', 'failed')
            )
            from ..ingestd import replace_receipt

            previous = daemon.checkpoint('receipt:' + name, {}) or {}
            receipt = replace_receipt(
                previous,
                dict(
                    counts,
                    source=name,
                    ok=counts['failed'] == 0,
                    duration_s=time.time() - started,
                    source_head=previous.get('searchable_through', 0),
                ),
                started,
            )
            daemon.queue_checkpoint('receipt:' + name, receipt)
            daemon.queue_checkpoint(name + '-next-due', time.time() + cadence)
        daemon.queue_checkpoint(
            'files-archive-totals',
            {
                d: n
                for d, n in totals.items()
                if d >= time.strftime('%Y-%m-%d', time.gmtime(time.time() - 7 * 86400))
            },
        )
        daemon.queue_checkpoint('files-moving', moving)

    def walk_root(self, daemon, root_name, root, lanes, archive_due, started, totals):
        key = 'files:' + root_name
        old = dict(daemon.checkpoint(key, {}) or {})
        errors = []
        try:
            if not root.is_dir():
                raise OSError()
            for directory, dirs, names in os.walk(
                root, onerror=lambda e: errors.append(type(e).__name__), followlinks=False
            ):
                dirs[:] = [
                    d
                    for d in dirs
                    if not excluded_path(str(Path(directory).relative_to(root) / d / '_'))
                    and not (Path(directory) / d).is_symlink()
                ]
                for name in names:
                    path = Path(directory) / name
                    yield from self.walk_file(
                        daemon, root_name, root, path, old, lanes, archive_due, started, totals
                    )
            if not errors:
                daemon.queue_checkpoint('files-complete:' + root_name, time.time())
        except OSError:
            errors.append('root')
        moving = sum(
            isinstance(entry, dict) and entry.get('ctx', {}).get('state') == 'moving'
            for entry in old.values()
        )
        for counts in lanes.values():
            counts['failed'] += len(errors)
        return moving

    def walk_file(self, daemon, root_name, root, path, old, lanes, archive_due, started, totals):
        key = 'files:' + root_name
        counts = lanes['files']
        relative = path.relative_to(root).as_posix()
        previous = old.get(relative, {})
        previous = {'facts': previous} if isinstance(previous, list) else previous
        try:
            st = path.lstat()
            prose = path.suffix.lower() in PROSE and not MACHINE.search(path.name)
            heavy = not prose and (
                st.st_size > MEMORY_BYTES or previous.get('ctx', {}).get('state') == 'moving'
            )
            if heavy:
                counts['excluded'] += 1
                if not archive_due:
                    yield
                    return
                counts = lanes['files-archive']
            current = file_facts(path, st)
            if not stat.S_ISREG(st.st_mode) or excluded_path(relative) or st.st_size > MAX_BYTES:
                counts['excluded'] += 1
            elif (
                previous.get('facts') == current
                and (not credential_name(path) or previous.get('ctx', {}).get('credential'))
            ) or (heavy and started - previous.get('at', 0) < ARCHIVE_CADENCE):
                counts['unchanged'] += 1
            else:
                captured = yield from self.capture(daemon, path, root_name, relative, counts)
                if captured is None:
                    return
                entry, size, state = captured
                old[relative] = entry
                daemon.queue_checkpoint(key, old)
                counts['written'] += 1
                counts['bytes'] += size
                counts['moving'] += state == 'moving'
                day = time.strftime('%Y-%m-%d', time.gmtime())
                totals[day] = totals.get(day, 0) + size
                daemon.queue_checkpoint('files-archive-totals', totals)
        except (FileNotFoundError, NotADirectoryError):
            counts['excluded'] += 1
        except OSError as error:
            counts['excluded' if error.errno == errno.ELOOP else 'failed'] += 1
        yield

    def capture(self, daemon, path, root_name, relative, counts):
        with (
            os.fdopen(
                os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK),
                'rb',
            ) as f,
            tempfile.TemporaryDirectory(prefix='file-read-', dir=daemon.packs.home) as scratch,
        ):
            before = os.fstat(f.fileno())
            head = b''
            if stat.S_ISREG(before.st_mode):
                head = f.read(min(64, before.st_size))
                f.seek(0)
            if not stat.S_ISREG(before.st_mode) or publication(relative, before.st_size, head):
                counts['excluded'] += 1
                yield
                return None
            observed = file_facts(path, before)
            target = Path(scratch) / 'bytes'
            raw, state, size, sha, credential = capture_file(path, f, before, target)
            with target.open('rb') as body:
                reason = publication(relative, size, body.read(64))
            if reason:
                counts['excluded'] += 1
                yield
                return None
            ctx = dict(
                key='file:' + root_name + ':' + relative,
                root=root_name,
                relative=relative,
                size=size,
                state=state,
                raw_sha256=sha,
                credential=credential or credential_name(path),
                time=datetime.fromtimestamp(before.st_mtime_ns / 1e9, timezone.utc).isoformat(),
            )
            if raw is not None:
                origin = daemon.record('files', raw, ctx, parse(raw, ctx))
            else:
                capture = time.time_ns()
                with target.open('rb') as body:
                    origin = daemon.packs.append(
                        'raw',
                        'files',
                        daemon.raw_header(ctx, capture, 'archive-only'),
                        body,
                    )
                daemon.documents('files', parse(b'', ctx), origin, capture)
                daemon.stats['records'] += 1
                daemon.stats['bytes'] += size
        return dict(facts=observed, raw=origin, ctx=ctx, at=time.time()), size, state
