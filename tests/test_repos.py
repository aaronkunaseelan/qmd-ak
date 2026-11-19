import json
import subprocess
import sys
import time

from qmd2.ingestd import Ingestd
from qmd2.sources.repos import Repos, find, parse


def git(repo, *args, check=True):
    return subprocess.run(['git', '-C', str(repo), *args], capture_output=True, check=check)


def init(repo):
    repo.mkdir(parents=True)
    git(repo, 'init', '-q', '-b', 'main')
    git(repo, 'config', 'user.name', 'synthetic')
    git(repo, 'config', 'user.email', 'synthetic@example.test')


def commit(repo, text):
    (repo / 'file.txt').write_text(text)
    git(repo, 'add', '-A')
    git(repo, 'commit', '-qm', text)


def records(daemon):
    """The lane's raw records, in the order the packs hold them."""
    return [
        raw
        for row in daemon.state.packs('raw')
        if row['source'] == 'repos'
        for _, _, raw in daemon.packs.iter_pack(row)
    ]


def split(raw):
    meta, _, bundle = raw.partition(b'\n')
    return json.loads(meta), bundle


def heads(bundle, scratch):
    path = scratch / 'heads.bundle'
    path.write_bytes(bundle)
    result = git(scratch, 'bundle', 'list-heads', str(path))
    return {line.split(' ', 1)[1] for line in result.stdout.decode().splitlines()}


def run(lane, daemon):
    daemon.queue_checkpoint('repos-next-due', 0)
    daemon.commit()
    lane.turn(daemon, seconds=120)
    daemon.commit()


def refs(repo):
    """{refname: objectname} as git lists them."""
    result = git(repo, 'for-each-ref', '--format=%(objectname) %(refname)')
    return dict(reversed(line.split(' ', 1)) for line in result.stdout.decode().splitlines())


def restore(records, scratch):
    """Fetch the chain in order, then set the refs to the last record's tips."""
    target = scratch / 'restore'
    subprocess.run(['git', 'init', '-q', '--bare', str(target)], check=True)
    for meta, bundle in records:
        if not bundle:
            continue
        path = scratch / 'bundle'
        path.write_bytes(bundle)
        subprocess.run(
            ['git', '-C', str(target), 'fetch', '-q', str(path), '+refs/*:refs/*'], check=True
        )
    wanted = {ref: sha for sha, ref in records[-1][0]['tips']}
    for ref in refs(target):
        if ref not in wanted:
            git(target, 'update-ref', '-d', ref)
    for ref, sha in wanted.items():
        git(target, 'update-ref', ref, sha)
    return refs(target)


def test_find_stops_at_a_repo(tmp_path):
    root = tmp_path / 'root'
    init(root / 'outer')
    init(root / 'outer' / 'inner')
    (root / 'plain' / '.git').mkdir(parents=True)
    (root / 'node_modules' / 'vendored' / '.git').mkdir(parents=True)
    found, errors = find([('synthetic', root)])
    assert found == [['synthetic', 'outer'], ['synthetic', 'plain']]
    assert errors == []


def test_bundle_chain_restores_the_source(tmp_path):
    """First run full, a quiet run writes nothing, one commit adds one small record."""
    root = tmp_path / 'workbench' / 'synthetic'
    repo = root / 'repo'
    init(repo)
    commit(repo, 'one')
    git(repo, 'branch', 'side')

    daemon = Ingestd(tmp_path / 'store', remote=False)
    lane = Repos([('synthetic', root)])
    run(lane, daemon)

    stored = records(daemon)
    assert len(stored) == 1
    first_meta, first = split(stored[0])
    assert first_meta['id'] == 'repo:synthetic:repo'
    assert first_meta['kind'] == 'git-bundle'
    assert first_meta['bundle_bytes'] == len(first) and first
    assert [ref for _, ref in first_meta['tips']] == [
        'refs/heads/main',
        'refs/heads/side',
    ]
    assert '[core]' in first_meta['config']
    receipt = daemon.checkpoint('receipt:repos')
    assert receipt['enumerated'] == 1 and receipt['written'] == 1
    assert receipt['ok'] and receipt['failed'] == 0
    assert set(heads(first, tmp_path)) == {'refs/heads/main', 'refs/heads/side', 'HEAD'}

    # No commit since the last record: the run happens, the lane captures nothing.
    lane.turn(daemon, seconds=120)
    assert len(records(daemon)) == 1
    run(lane, daemon)
    assert len(records(daemon)) == 1
    assert daemon.checkpoint('receipt:repos')['unchanged'] == 1
    assert daemon.checkpoint('repo:repo:synthetic:repo')['tips'] == first_meta['tips']

    # One commit: one incremental record, the refs left unexcluded.
    commit(repo, 'two')
    run(lane, daemon)
    stored = records(daemon)
    assert len(stored) == 2
    second_meta, second = split(stored[1])
    assert second.startswith(b'# v2 git bundle\n-' + first_meta['tips'][0][0].encode())
    assert set(heads(second, tmp_path)) == {'refs/heads/main', 'HEAD'}
    assert [sha for sha, _ in second_meta['tips']] != [sha for sha, _ in first_meta['tips']]
    assert daemon.checkpoint('repo:repo:synthetic:repo')['tips'] == second_meta['tips']

    # The chain of bundles restores the source repository's refs exactly.
    assert restore([split(raw) for raw in stored], tmp_path) == refs(repo)

    # One document per record: the repo path as title, the ref list as body.
    doc = parse(stored[1], {})[0]
    assert doc['id'] == 'repo:synthetic:repo'
    assert doc['title'] == 'synthetic/repo'
    assert doc['kind'] == 'git-bundle'
    assert doc['parts'][0]['text'] == '\n'.join(
        sha + ' ' + ref for sha, ref in second_meta['tips']
    )
    hit = daemon.index.search('refs/heads/main')[0]
    assert hit['id'] == 'repo:synthetic:repo' and hit['title'] == 'synthetic/repo'

    # The tips are the durable record the next run reads back from the state store.
    assert daemon.state.get('repo:repo:synthetic:repo')['tips'] == second_meta['tips']

    # A ref deleted with no new commit: an empty bundle, the new ref list as the record.
    git(repo, 'branch', '-D', 'side')
    run(lane, daemon)
    stored = records(daemon)
    assert len(stored) == 3
    third_meta, third = split(stored[2])
    assert third == b'' and third_meta['bundle_bytes'] == 0
    assert [ref for _, ref in third_meta['tips']] == ['refs/heads/main']
    assert daemon.checkpoint('receipt:repos')['written'] == 1
    # Identical tips still write nothing.
    run(lane, daemon)
    assert len(records(daemon)) == 3
    # Restoring sets the refs to the last record's tips, so the deletion replays.
    assert restore([split(raw) for raw in stored], tmp_path) == refs(repo)
    assert 'refs/heads/side' not in refs(repo)


def test_a_vanished_tip_does_not_fail_the_lane(tmp_path):
    """A stored tip pruned from the repo leaves `--not`, so the repo records again."""
    root = tmp_path / 'workbench' / 'synthetic'
    repo = root / 'repo'
    init(repo)
    commit(repo, 'one')
    git(repo, 'checkout', '-q', '-b', 'side')
    commit(repo, 'two')
    git(repo, 'checkout', '-q', 'main')

    daemon = Ingestd(tmp_path / 'store', remote=False)
    lane = Repos([('synthetic', root)])
    run(lane, daemon)
    stored = records(daemon)
    assert len(stored) == 1
    first_meta, first = split(stored[0])
    assert len(first_meta['tips']) == 2

    # The branch goes, then git prunes the commit only it held: the stored tip is gone.
    git(repo, 'branch', '-D', 'side')
    git(repo, 'reflog', 'expire', '--expire=now', '--all')
    git(repo, 'gc', '--prune=now', '-q')
    assert git(repo, 'cat-file', '-e', first_meta['tips'][1][0], check=False).returncode
    commit(repo, 'three')
    run(lane, daemon)

    receipt = daemon.checkpoint('receipt:repos')
    assert receipt['failed'] == 0 and receipt['ok']
    stored = records(daemon)
    assert len(stored) == 2
    second_meta, second = split(stored[1])
    assert [ref for _, ref in second_meta['tips']] == ['refs/heads/main']

    # The chain still restores every ref the source has now.
    restored = restore([split(raw) for raw in stored], tmp_path)
    source = refs(repo)
    assert restored == source


def test_repos_row_in_status_and_verify(tmp_path):
    """The registered row carries the lane's receipt into status, and verify knows the lane."""
    from qmd2.status import view
    from qmd2.verify import evaluate

    root = tmp_path / 'workbench' / 'synthetic'
    repo = root / 'repo'
    init(repo)
    commit(repo, 'one')
    store = tmp_path / 'store'
    daemon = Ingestd(store, remote=False)
    lane = Repos([('synthetic', root)])
    run(lane, daemon)
    daemon.last_status -= 60
    daemon.publish_status()

    snapshot = json.loads((store / 'status.json').read_text())
    row = view(store, snapshot)['sources']['repos']
    assert row['receipt']['written'] == 1 and row['receipt']['ok'] is True
    assert row['cadence_s'] == 86400
    assert row['current'] is True
    assert [name for name in evaluate(snapshot, time.time()) if name.startswith('repos')] == []

    # A reparse of the lane rebuilds the same document from the pack.
    while not daemon.reindex('repos', seconds=60):
        pass
    assert daemon.index.search('refs/heads/main')[0]['id'] == 'repo:synthetic:repo'


def test_repo_credential_raw_needs_out(tmp_path):
    """A .git/config holding a secret marks the record; its raw bytes need --out."""
    root = tmp_path / 'workbench' / 'synthetic'
    repo = root / 'repo'
    init(repo)
    commit(repo, 'one')
    config = repo / '.git' / 'config'
    # A tool config pasted into .git/config, secret and all: git accepts it, the lane marks it.
    config.write_text(
        config.read_text() + '[synthetic-tool]\n\tkey = {"client_secret": "synthetic"}\n'
    )
    store = tmp_path / 'store'
    daemon = Ingestd(store, remote=False)
    lane = Repos([('synthetic', root)])
    run(lane, daemon)
    meta, _ = split(records(daemon)[0])
    assert meta['credential'] is True

    argv = [sys.executable, '-m', 'qmd2.cli', '--home', str(store), 'get']
    denied = subprocess.run([*argv, 'repo:synthetic:repo', '--raw'], capture_output=True)
    assert denied.returncode == 2 and denied.stdout == b''
    out = tmp_path / 'raw.out'
    result = subprocess.run(
        [*argv, 'repo:synthetic:repo', '--raw', '--out', str(out)], capture_output=True
    )
    assert result.returncode == 0 and out.read_bytes() == records(daemon)[0]
