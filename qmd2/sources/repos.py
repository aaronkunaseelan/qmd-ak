"""The daily git-bundle lane: one record per repo per run, and only when its tips moved."""

import hashlib
import json
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from qmd2.accounts import file_roots
from qmd2.packs import room

from .files import CREDENTIAL_MARKER, MAX_BYTES, excluded_path

PARSER_VERSION = 1
CADENCE = 86400
ROOTS = file_roots()
GIT_TIMEOUT = 600
REFS = '%(objectname) %(refname) %(committerdate:unix)'


def git(args, repo, stdout=subprocess.PIPE, input=None):
    return subprocess.run(
        ['git', *args],
        cwd=repo,
        input=input,
        stdout=stdout,
        stderr=subprocess.PIPE,
        timeout=GIT_TIMEOUT,
    )


def label(root, relative):
    """A repo as a person names it: root, or root/relative for a directory below it."""
    return root if relative == '.' else root + '/' + relative


def find(roots):
    """(root name, relative path) for every repo under the roots; a repo ends the descent."""
    found, errors = [], []
    for name, root in roots:
        try:
            if not root.is_dir():
                raise OSError()
            for directory, dirs, _ in os.walk(
                root, onerror=lambda e: errors.append(type(e).__name__), followlinks=False
            ):
                path = Path(directory)
                relative = path.relative_to(root).as_posix()
                if (path / '.git').exists():
                    found.append([name, relative])
                    dirs[:] = []
                    continue
                dirs[:] = sorted(
                    d
                    for d in dirs
                    if not excluded_path(relative + '/' + d + '/_')
                    and not (path / d).is_symlink()
                )
        except OSError:
            errors.append('root')
    return found, errors


def tips(repo):
    """[(objectname, refname)] as git lists them, and the newest commit time among them."""
    result = git(['for-each-ref', '--format=' + REFS], repo)
    if result.returncode:
        raise OSError('git for-each-ref failed')
    pairs, newest = [], 0
    for line in result.stdout.decode('utf-8', 'replace').splitlines():
        sha, _, rest = line.partition(' ')
        ref, _, stamp = rest.rpartition(' ')
        if not sha or not ref:
            continue
        pairs.append([sha, ref])
        newest = max(newest, int(stamp or 0))
    return pairs, newest


def config(repo):
    """The repository's config text; a worktree keeps it in the common git directory."""
    result = git(['rev-parse', '--git-common-dir'], repo)
    if result.returncode:
        return ''
    try:
        return (repo / result.stdout.decode('utf-8', 'replace').strip() / 'config').read_text(
            errors='replace'
        )
    except OSError:
        return ''


def present(repo, shas):
    """The shas the repository still has; a stored tip can vanish with its branch and gc.

    `git bundle create --not <gone>` answers "bad object" (exit 128), which would fail that repo
    on every run, so one batch-check drops the gone ones and an empty answer means `--all`.
    """
    if not shas:
        return []
    result = git(
        ['cat-file', '--batch-check'], repo, input=''.join(f'{sha}\n' for sha in shas).encode()
    )
    if result.returncode:
        raise OSError('git cat-file failed')
    return [
        line.split()[0]
        for line in result.stdout.decode('utf-8', 'replace').splitlines()
        if not line.endswith(' missing')
    ]


def empty_bundle(result):
    return result.returncode != 0 and b'empty bundle' in result.stderr


def parse(raw, ctx):
    """One document per record: the repo path as title, the ref list as body."""
    meta = ctx.get('meta') or json.loads(raw.split(b'\n', 1)[0].decode('utf-8'))
    identifier = 'repo:' + meta['root'] + ':' + meta['relative']
    return [
        {
            'id': identifier,
            'item': identifier,
            'source': 'repos',
            'group': 'repos',
            'kind': 'git-bundle',
            'time': meta['time'],
            'title': label(meta['root'], meta['relative']),
            'where': label(meta['root'], meta['relative']),
            'context': {
                'root': meta['root'],
                'relative': meta['relative'],
                'bundle_bytes': meta['bundle_bytes'],
                'bundle_sha256': meta['bundle_sha256'],
            },
            'credential': meta['credential'],
            'parts': [
                {'kind': 'authored', 'text': '\n'.join(f'{sha} {ref}' for sha, ref in meta['tips'])}
            ],
            'parser': f'repos@{PARSER_VERSION}',
        }
    ]


class Repos:
    """At most one run per 24 h; each repo's new tips become the next run's `--not`."""

    def __init__(self, roots=None):
        self.roots = ROOTS if roots is None else roots

    def turn(self, daemon, seconds=8):
        run = daemon.checkpoint('repo-run')
        if run is None:
            if time.time() < (daemon.checkpoint('repos-next-due', 0) or 0):
                return
            found, errors = find(self.roots)
            run = {
                'started': time.time(),
                'repos': found,
                'at': 0,
                'head': 0,
                'written': 0,
                'unchanged': 0,
                'excluded': 0,
                'failed': len(errors),
                'errors': errors,
            }
            daemon.queue_checkpoint('repo-run', run)
        deadline = time.monotonic() + seconds
        while run['at'] < len(run['repos']) and time.monotonic() < deadline:
            root, relative = run['repos'][run['at']]
            try:
                self.capture(daemon, root, relative, run)
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                run['failed'] += 1
                if type(error).__name__ not in run['errors']:
                    run['errors'].append(type(error).__name__)
            run['at'] += 1
            daemon.queue_checkpoint('repo-run', run)
        if run['at'] >= len(run['repos']):
            self.finish(daemon, run)

    def capture(self, daemon, root, relative, run):
        repo = dict(self.roots)[root] / relative
        identifier = 'repo:' + root + ':' + relative
        key = 'repo:' + identifier
        pairs, newest = tips(repo)
        run['head'] = max(run['head'], newest)
        stored = (daemon.checkpoint(key, {}) or {}).get('tips') or []
        if pairs == stored:
            run['unchanged'] += 1
            return
        exclude = present(repo, sorted({sha for sha, _ in stored}))
        with tempfile.TemporaryDirectory(prefix='repo-bundle-', dir=daemon.packs.home) as scratch:
            bundle = Path(scratch) / 'bundle'
            argv = ['bundle', 'create', '-', '--all']
            if exclude:
                argv += ['--not', *exclude]
            with bundle.open('wb') as output:
                result = git(argv, repo, stdout=output)
            # A ref deleted or rewound since the last record leaves nothing to send: git refuses
            # with "empty bundle" while the tips moved, so the new ref list is the whole record.
            removal = empty_bundle(result)
            if result.returncode and not removal:
                raise OSError('git bundle failed')
            if removal:
                # git's refusal still writes a header; the record's bundle body is empty.
                bundle.write_bytes(b'')
            size = bundle.stat().st_size
            if size > MAX_BYTES:
                run['excluded'] += 1
                return
            digest = hashlib.sha256()
            with bundle.open('rb') as body:
                while chunk := body.read(1024 * 1024):
                    digest.update(chunk)
            text = config(repo)
            meta = {
                'kind': 'git-bundle',
                'id': identifier,
                'root': root,
                'relative': relative,
                'time': datetime.now(timezone.utc).isoformat(),
                'tips': pairs,
                'config': text,
                'bundle_bytes': size,
                'bundle_sha256': digest.hexdigest(),
                'credential': bool(CREDENTIAL_MARKER.search(text.encode('utf-8', 'replace'))),
            }
            room(daemon.packs.home, size)
            raw = Path(scratch) / 'raw'
            with raw.open('wb') as output:
                output.write(json.dumps(meta, sort_keys=True).encode() + b'\n')
                with bundle.open('rb') as body:
                    while chunk := body.read(1024 * 1024):
                        output.write(chunk)
            capture = time.time_ns()
            envelope = raw.stat().st_size
            ctx = {
                'key': identifier,
                'root': root,
                'relative': relative,
                'time': meta['time'],
                'size': envelope,
                'credential': meta['credential'],
                'meta': meta,
            }
            with raw.open('rb') as body:
                origin = daemon.packs.append('raw', 'repos', daemon.raw_header(ctx, capture), body)
            daemon.documents('repos', parse(b'', ctx), origin, capture)
            daemon.stats['records'] += 1
            daemon.stats['bytes'] += envelope
        daemon.queue_checkpoint(key, {'tips': pairs, 'at': time.time()})
        run['written'] += 1

    def finish(self, daemon, run):
        from ..ingestd import replace_receipt

        counts = {name: run[name] for name in ('written', 'unchanged', 'excluded', 'failed')}
        receipt = replace_receipt(
            daemon.checkpoint('receipt:repos', {}) or {},
            dict(
                counts,
                enumerated=sum(counts.values()),
                source='repos',
                ok=counts['failed'] == 0,
                error='; '.join(run['errors']) or None,
                duration_s=time.time() - run['started'],
                source_head=run['head'],
            ),
            run['started'],
        )
        daemon.queue_checkpoint('receipt:repos', receipt)
        daemon.queue_checkpoint('repos-next-due', time.time() + CADENCE)
        daemon.queue_checkpoint('repo-run', None)
