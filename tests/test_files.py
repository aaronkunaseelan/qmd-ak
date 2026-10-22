import json
import os
import subprocess
import sqlite3
import hashlib
import sys
import time

import pytest

from qmd2.sources.files import Walker, parse, publication, capture_file


@pytest.mark.parametrize(
    'name,raw,size,excluded,kind,text',
    [
        ('note.md', b'# lighthouse', 12, None, 'authored', None),
        ('blob.bin', b'\x00\xff', 2, None, 'binary', None),
        ('long.txt', b'x' * (256 * 1024 + 1), 256 * 1024 + 1, None, 'binary', None),
        ('.env', b'synthetic', 9, None, None, None),
        ('node_modules/a.js', b'code', 4, 'reconstructable', 'authored', None),
        ('snapshot.db', b'bytes', 5, None, None, None),
        ('large', b'', 33 * 1024 * 1024, None, 'binary', None),
        ('core', b'', 0, 'core_dump', 'authored', None),
        ('unknown', b'\x7fELF', 4, None, 'authored', None),
        ('assets/a-BafhMyp1.js', b'code', 4, None, 'authored', None),
        ('too-big.bin', b'', 2 * 1024**3 + 1, 'oversize', 'binary', None),
        ('within-cap.bin', b'', 2 * 1024**3, None, 'binary', None),
        ('core.123.gz', b'', 0, 'core_dump', 'authored', None),
        ('core.worker.100.zst', b'', 0, 'core_dump', 'authored', None),
        ('state.db-wal', b'', 0, 'sqlite_auxiliary', None, None),
        *[
            (name, b'synthetic', 9, None, None, None)
            for name in ('id_ed25519', '.git-credentials', 'Login Data', 'logins.json', 'key4.db')
        ],
        *[
            (name, b'machine noise', 13, None, None, None)
            for name in (
                'run.stdout',
                'run.stderr.txt',
                'run.log',
                'run.log.txt',
                'run.out',
                'run.err.txt',
                'run.pid.txt',
                'records.json',
                'records.jsonl',
                'records.ndjson',
                'records.csv',
                'records.tsv',
                'notes.ipynb',
            )
        ],
        *[
            (name, b'lighthouse code', 15, None, 'authored', None)
            for name in (
                'main.py',
                'main.go',
                'main.ts',
                'settings.toml',
                'settings.yaml',
                'Makefile',
                'Dockerfile',
                '.gitignore',
            )
        ],
        (
            'config.json',
            b'{"nested":{"name":"lighthouse"},"ports":[80,443]}',
            47,
            None,
            'authored',
            'nested.name: lighthouse\nports[0]: 80\nports[1]: 443',
        ),
        ('.git/config', b'lighthouse', 10, None, 'no_row', None),
        *[
            (name, b'synthetic', 9, excluded, None, None)
            for name, excluded in (
                ('service.env', None),
                ('.env.production', None),
                ('server.pem', None),
                ('server.key', None),
                ('server.p12', None),
                ('id_rsa.pub', None),
                ('.netrc', None),
                ('credentials-local.json', None),
                ('client_secret-local.json', None),
                ('cookies', None),
                ('cookies.sqlite', None),
                ('cookies.sqlite3', None),
                ('cookies.db', None),
                ('cookiejar', None),
                ('cookies-local.txt', None),
                ('cookies-local.json', None),
                ('cookiejar-local.txt', None),
                ('cookiejar-local.json', None),
            )
        ],
        *[
            ('settings.txt', raw, len(raw), None, None, None)
            for raw in (
                b'-----BEGIN PRIVATE KEY-----\nsynthetic',
                b'-----BEGIN RSA PRIVATE KEY-----\nsynthetic',
                *[
                    b'{"' + key + b'": "synthetic"}'
                    for key in (
                        b'refresh_token',
                        b'client_secret',
                        b'aws_secret_access_key',
                        b'private_key',
                    )
                ],
                *[
                    b'export ' + key + b'=synthetic'
                    for key in (
                        b'REFRESH_TOKEN',
                        b'CLIENT_SECRET',
                        b'AWS_SECRET_ACCESS_KEY',
                        b'PRIVATE_KEY',
                    )
                ],
            )
        ],
        ('.env.example', b'EXAMPLE=synthetic', 17, None, 'authored', None),
        ('.env.sample', b'EXAMPLE=synthetic', 17, None, 'authored', None),
        (
            'schema.json',
            b'{"type": "object", "description": "refresh_token"}',
            47,
            None,
            'authored',
            None,
        ),
        *[
            (
                name,
                b'<h2>Beacon &amp; harbor</h2><p>Visit <a href="https://example.test/?a=1&amp;b=2">guide</a>.</p><script>hidden script</script><style>hidden style</style>',
                160,
                None,
                'authored',
                '## Beacon & harbor\nVisit guide <https://example.test/?a=1&b=2>.',
            )
            for name in ('page.html', 'page.htm', 'page.xhtml', 'page.HTML')
        ],
    ],
)
def test_parser_table(name, raw, size, excluded, kind, text):
    assert publication(name, size, raw[:64]) == excluded
    docs = parse(
        raw, {'root': 'synthetic', 'relative': name, 'time': '2026-10-13T00:00:00Z', 'size': size}
    )
    if kind == 'no_row':
        assert docs == []
        return
    doc = docs[0]
    assert doc['id'] == 'file:synthetic:' + name
    assert doc['kind'] == 'file'
    assert doc['title'] == name
    assert doc['where'] == 'synthetic/' + name
    if kind is None:
        assert doc['parts'] == []
    else:
        assert doc['parts'][0]['kind'] == kind
    if text is not None:
        assert doc['parts'][0]['text'] == text
    assert (
        parse(
            b'',
            {
                'root': 'synthetic',
                'relative': name,
                'time': '2026-10-13T00:00:00Z',
                'deleted': True,
            },
        )
        == []
    )


def test_files_chain_and_failed_root(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    root = tmp_path / 'workbench' / 'synthetic'
    root.mkdir(parents=True)
    (root / 'note.md').write_text('synthetic lighthouse current')
    (root / '.env').write_text('synthetic excluded')
    _seed_files_walk(tmp_path, root)
    daemon = _resume_files_walk(tmp_path, root)
    _files_captured_doc(daemon, root)
    _files_presence(daemon, tmp_path, root)
    moved, hits = _files_duplicates(daemon, root)
    _files_raw_reads(daemon, tmp_path, root, moved, hits)
    _files_sqlite(daemon, tmp_path, root)
    archived, large = _files_archive_capture(daemon, root)
    _files_archive_get(daemon, tmp_path, root, archived, large)
    _files_archive_transitions(daemon, root, large)


def _walk_files(daemon, root):
    for _ in Walker([('synthetic', root)]).walk(daemon):
        pass
    daemon.commit()


def _seed_files_walk(tmp_path, root):
    subprocess.run(
        [
            sys.executable,
            '-c',
            '''
import sys
from pathlib import Path
from qmd2.ingestd import Ingestd
from qmd2.sources.files import Walker
d = Ingestd(Path(sys.argv[1]), remote=False)
w = Walker([('synthetic', Path(sys.argv[2]))]).walk(d)
while not all(d.checkpoint('files:synthetic', {}).get(name) for name in ('note.md', '.env')):
    next(w)
d.commit()
assert d.state.get('files-complete:synthetic') is None
''',
            str(tmp_path / 'store'),
            str(root),
        ],
        check=True,
    )


def _resume_files_walk(tmp_path, root):
    from qmd2.ingestd import Ingestd
    from qmd2.state import State

    state = State(tmp_path / 'store', writable=True)
    state.set('deleted:file:synthetic:note.md', {'synthetic': True})
    state.commit()
    state.db.close()
    script = """
import sys
from pathlib import Path
from qmd2.ingestd import Ingestd
from qmd2.sources.files import Walker
home, root = Path(sys.argv[1]), Path(sys.argv[2])
d = Ingestd(home, remote=False)
assert d.state.get('deleted:file:synthetic:note.md') is None
assert d.state.get('files-complete:synthetic') is None
assert d.state.get('files:synthetic')['note.md']
opens = []
def audit(event, args):
    if event == 'open' and args[0] == str(root / 'note.md'):
        opens.append(args)
sys.addaudithook(audit)
for _ in Walker([('synthetic', root)]).walk(d):
    pass
d.commit()
assert not opens
"""
    subprocess.run([sys.executable, '-c', script, str(tmp_path / 'store'), str(root)], check=True)
    return Ingestd(tmp_path / 'store', remote=False)


def _files_captured_doc(daemon, root):
    from qmd2.doc import render

    path = root / 'note.md'
    assert daemon.checkpoint('receipt:files')['written'] == 0
    assert daemon.checkpoint('receipt:files')['unchanged'] == 2
    complete = daemon.state.get('files-complete:synthetic')
    assert complete
    hit = daemon.index.search('lighthouse')[0]
    doc = json.loads(daemon.packs.read(hit['pointer'])[1])
    header, raw = daemon.packs.read(doc['raw'])
    assert raw == path.read_bytes()
    assert header['version']['state'] == doc['state'] == 'stable'
    assert 'lighthouse current' in render(doc)
    assert daemon.checkpoint('receipt:files')['excluded'] == 0


def _files_presence(daemon, tmp_path, root):
    path = root / 'note.md'
    path.write_text('synthetic beacon updated')
    _walk_files(daemon, root)
    assert daemon.index.search('lighthouse') == []
    assert daemon.index.search('beacon')
    complete = daemon.state.get('files-complete:synthetic')
    hidden = tmp_path / 'hidden'
    root.rename(hidden)
    _walk_files(daemon, root)
    assert daemon.index.search('beacon')
    assert not daemon.checkpoint('receipt:files')['ok']
    assert daemon.state.get('files-complete:synthetic') == complete
    hidden.rename(root)
    path.unlink()
    _walk_files(daemon, root)
    assert daemon.index.search('beacon')
    assert daemon.checkpoint('receipt:files')['ok']
    due = daemon.checkpoint('files-next-due')
    assert 1430 < due - time.time() <= 1440
    Walker([('synthetic', root)]).turn(daemon)
    assert daemon.checkpoint('files-next-due') == due
    tombstones = [
        json.loads(raw)
        for row in daemon.state.packs('doc')
        for _, _, raw in daemon.packs.iter_pack(row)
    ]
    assert all(doc['kind'] != 'tombstone' for doc in tombstones)
    assert daemon.index.search('beacon')[0]['gone']
    path.write_text('synthetic beacon updated')
    _walk_files(daemon, root)


def _files_duplicates(daemon, root):
    path = root / 'note.md'
    moved = root / 'renamed.md'
    path.rename(moved)
    _walk_files(daemon, root)
    hits = daemon.index.search('beacon')
    assert len(hits) == 1 and hits[0]['id'] == 'file:synthetic:renamed.md'
    assert not hits[0]['gone'] and hits[0]['also_at'] == ['synthetic/note.md']
    lost = root / 'lost.md'
    lost.write_text('beacon ' * 100)
    _walk_files(daemon, root)
    lost.unlink()
    copy = root / 'copy.md'
    copy.write_bytes(moved.read_bytes())
    _walk_files(daemon, root)
    hits = daemon.index.search('beacon', limit=10)
    assert len(hits) == 2
    assert not hits[0]['gone'] and hits[1]['gone']
    assert len(hits[0]['also_at']) == 2
    assert len({r['version']['raw_sha256'] for r in hits}) == 2
    assert len(daemon.index.search('beacon unmatched')) == 2
    return moved, hits


def _files_raw_reads(daemon, tmp_path, root, moved, hits):
    lost = root / 'lost.md'
    from qmd2.cli import get_docs

    chosen = get_docs(tmp_path / 'store', hits[0]['id'])[0]
    assert daemon.packs.read(chosen['raw'])[1] == moved.read_bytes()
    argv = [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path / 'store'), 'get']
    result = subprocess.run([*argv, chosen['id'], '--raw'], capture_output=True)
    assert result.returncode == 0 and result.stdout == moved.read_bytes()
    protected_raw = b'CLIENT_SECRET=synthetic-secret\n'
    identifier = 'file:synthetic:credentials.txt'
    daemon.record(
        'files',
        protected_raw,
        dict(key=identifier, root='synthetic', relative='credentials.txt', time=time.time()),
    )
    daemon.commit()
    denied = subprocess.run([*argv, identifier, '--raw'], capture_output=True)
    assert denied.returncode == 2 and denied.stdout == b''
    out = tmp_path / 'raw.out'
    out.write_text('old synthetic output')
    out.chmod(0o644)
    result = subprocess.run([*argv, identifier, '--raw', '--out', str(out)], capture_output=True)
    assert result.returncode == 0 and result.stdout == b''
    assert out.read_bytes() == protected_raw and out.stat().st_mode & 0o777 == 0o600
    lost.write_text('synthetic beacon unique')
    _walk_files(daemon, root)
    lost.unlink()
    hits = daemon.index.search('beacon unique')
    assert len(hits) == 2 and not hits[0]['gone'] and hits[1]['gone']


def _files_sqlite(daemon, tmp_path, root):
    argv = [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path / 'store'), 'get']
    config = root / 'config.json'
    config.write_text('{"ctx_update":{"root":"wrong-root"},"note":"synthetic configuration"}')
    _walk_files(daemon, root)
    assert daemon.index.lookup('file:synthetic:config.json')
    assert daemon.checkpoint('ctx:files', {}) == {}
    git = root / '.git'
    git.mkdir()
    (git / 'config').write_text('synthetic git bytes')
    database = root / 'database.data'
    connection = sqlite3.connect(database)
    connection.execute('PRAGMA journal_mode=WAL')
    connection.execute('CREATE TABLE notes (value TEXT)')
    connection.execute("INSERT INTO notes VALUES ('synthetic')")
    connection.commit()
    _walk_files(daemon, root)
    assert not daemon.index.lookup('file:synthetic:.git/config')
    result = subprocess.run([*argv, 'file:synthetic:.git/config', '--raw'], capture_output=True)
    assert result.returncode == 0 and result.stdout == b'synthetic git bytes'
    before = database.stat().st_mtime_ns
    connection.execute("INSERT INTO notes VALUES ('synthetic second')")
    connection.commit()
    assert database.stat().st_mtime_ns == before
    _walk_files(daemon, root)
    record = daemon.checkpoint('files:synthetic')['database.data']
    assert record['ctx']['state'] == 'snapshot'
    out = tmp_path / 'snapshot'
    result = subprocess.run(
        [*argv, 'file:synthetic:database.data', '--raw', '--out', str(out)], capture_output=True
    )
    assert result.returncode == 0
    with sqlite3.connect(out) as restored:
        assert restored.execute('SELECT count(*) FROM notes').fetchone()[0] == 2
    connection.close()
    locked = sqlite3.connect(database)
    locked.execute('PRAGMA journal_mode=DELETE')
    locked.execute('BEGIN EXCLUSIVE')
    os.utime(database, None)
    _walk_files(daemon, root)
    assert daemon.checkpoint('files:synthetic')['database.data']['ctx']['state'] == 'moving'
    assert daemon.checkpoint('receipt:files')['failed'] == 0
    locked.rollback()
    locked.close()


def _files_archive_capture(daemon, root):
    large = root / 'large.jsonl'
    with large.open('wb') as body:
        body.write(b'"refresh_token"' + b' ' * (2 * 1024 * 1024) + b': "synthetic"')
        body.truncate(33 * 1024 * 1024)
    too_large = root / 'oversize.bin'
    with too_large.open('wb') as body:
        body.truncate(2 * 1024**3 + 1)
    daemon.queue_checkpoint('files-archive-next-due', 0)
    _walk_files(daemon, root)
    archived = daemon.checkpoint('files:synthetic')['large.jsonl']
    assert archived['ctx']['size'] == 33 * 1024 * 1024
    assert archived['ctx']['credential']
    return archived, large


def _files_archive_get(daemon, tmp_path, root, archived, large):
    argv = [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path / 'store'), 'get']
    with (tmp_path / 'streamed-record').open('wb') as output:
        header = daemon.packs.write_raw(archived['raw'], output)
    assert header['kind'] == 'archive-only'
    result = subprocess.run([*argv, 'file:synthetic:large.jsonl', '--raw'], capture_output=True)
    assert result.returncode == 2 and result.stdout == b''
    copied = tmp_path / 'copied-large'
    result = subprocess.run(
        [*argv, 'file:synthetic:large.jsonl', '--raw', '--out', str(copied)], capture_output=True
    )
    assert result.returncode == 0
    with copied.open('rb') as body:
        assert hashlib.file_digest(body, 'sha256').hexdigest() == archived['ctx']['raw_sha256']
    assert copied.stat().st_size == large.stat().st_size
    assert not daemon.index.lookup('file:synthetic:oversize.bin')
    daemon.reindex('files')
    assert (
        daemon.index.lookup('file:synthetic:large.jsonl')['version']['raw_sha256']
        == archived['ctx']['raw_sha256']
    )


def _files_archive_transitions(daemon, root, large):
    large.write_text('synthetic replacement')
    daemon.queue_checkpoint('files-archive-next-due', 0)
    # Small and stable: the ordinary lane captures it.
    _walk_files(daemon, root)
    archived = daemon.checkpoint('files:synthetic')['large.jsonl']
    large.touch()
    # Large again: the daily archive lane must keep the latest capture within 24 hours.
    with large.open('ab') as body:
        body.truncate(33 * 1024 * 1024)
    daemon.queue_checkpoint('files-archive-next-due', 0)
    _walk_files(daemon, root)
    assert daemon.checkpoint('files:synthetic')['large.jsonl']['raw'] == archived['raw']
    assert daemon.checkpoint('receipt:files-archive')['failed'] == 0
    assert daemon.state.get('files-archive-next-due') - time.time() > 86390
    daemon.reindex('files')
    assert daemon.index.lookup('file:synthetic:config.json')
    assert (
        daemon.index.lookup('file:synthetic:large.jsonl')['version']['raw_sha256']
        == archived['ctx']['raw_sha256']
    )
    assert daemon.state.get('files-archive-totals')
    assert daemon.state.get('files-moving') >= 1


@pytest.mark.parametrize(
    'case,expected',
    [
        ('quiet', 'settled'),
        ('recent', 'stable'),
        ('append', 'moving'),
        ('truncate', 'moving'),
        ('replace', 'moving'),
        ('unlink', 'moving'),
    ],
)
def test_read_filesystem(tmp_path, case, expected):
    path = tmp_path / 'read.txt'
    path.write_bytes(b'synthetic')
    if case == 'quiet':
        os.utime(path, (time.time() - 120, time.time() - 120))
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
        before = os.fstat(stream.fileno())
        if case == 'append':
            with path.open('ab') as writer:
                writer.write(b' appended')
        elif case == 'truncate':
            path.write_bytes(b'short')
        elif case == 'replace':
            other = tmp_path / 'new.txt'
            other.write_bytes(b'replacement')
            other.replace(path)
        elif case == 'unlink':
            path.unlink()
        raw, state, size, sha, credential = capture_file(
            path, stream, before, tmp_path / 'captured'
        )
    assert state == expected
    assert raw == (b'short' if case == 'truncate' else b'synthetic')


@pytest.mark.parametrize(
    'case,failed', [('vanished', 0), ('symlink', 0), ('fifo', 0), ('permission', 1)]
)
def test_walk_read_errors_subprocess(tmp_path, case, failed):
    root = tmp_path / 'root'
    root.mkdir()
    (root / 'note.txt').write_text('synthetic')
    script = """
import os, sys
from pathlib import Path
from qmd2.ingestd import Ingestd
from qmd2.sources.files import Walker
root, case = Path(sys.argv[2]), sys.argv[3]
path = root / 'note.txt'
def audit(event, args):
    if event == 'open' and args[0] == str(path):
        if case == 'vanished':
            path.unlink(missing_ok=True)
        elif case == 'symlink' and not path.is_symlink():
            path.unlink()
            path.symlink_to(root / 'missing')
        elif case == 'fifo':
            path.unlink()
            os.mkfifo(path)
sys.addaudithook(audit)
if case == 'permission':
    path.chmod(0)
d = Ingestd(Path(sys.argv[1]), remote=False)
for _ in Walker([('synthetic', root)]).walk(d):
    pass
d.commit()
assert d.checkpoint('receipt:files')['failed'] == int(sys.argv[4])
"""
    subprocess.run(
        [sys.executable, '-c', script, str(tmp_path / 'store'), str(root), case, str(failed)],
        check=True,
    )
