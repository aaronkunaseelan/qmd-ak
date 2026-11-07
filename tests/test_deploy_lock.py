"""Real deploy subprocess edge with a synthetic committed build that cannot deploy."""

import fcntl
import os
from pathlib import Path
import shutil
import subprocess


def test_deploy_heavy_lock_busy_free_and_build_failure(tmp_path):
    repo = tmp_path / 'repo'
    repo.mkdir()
    shutil.copyfile(Path(__file__).parents[1] / 'deploy.sh', repo / 'deploy.sh')
    (repo / 'qmd2').mkdir()
    (repo / 'qmd2/__init__.py').touch()
    (repo / 'capture').mkdir()
    (repo / 'capture/go.mod').write_text('module synthetic.test/capture\n')
    wireproxy = repo / 'vendor/wireproxy'
    wireproxy.mkdir(parents=True)
    # Real Go fails here, before any install or systemctl command can run.
    (wireproxy / 'go.mod').write_text('synthetic invalid go.mod\n')
    for args in (
        ['init', '-q'],
        ['add', '.'],
        [
            '-c',
            'user.name=Synthetic',
            '-c',
            'user.email=test@example.test',
            'commit',
            '-qm',
            'fixture',
        ],
    ):
        subprocess.run(['git', '-C', str(repo), *args], check=True)
    # A stub release-guard first on PATH: the real one admits only the supervisor's agent.
    guard = tmp_path / 'guard'
    guard.mkdir()
    (guard / 'release-guard').write_text('#!/bin/sh\nexit 0\n')
    (guard / 'release-guard').chmod(0o755)
    lease = tmp_path / 'heavy.lock'
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    env = dict(
        os.environ,
        HOME=str(tmp_path / 'home'),
        TMPDIR=str(scratch),
        QMD2_HEAVY_LEASE=str(lease),
        PATH=f'{guard}:{os.environ["PATH"]}',
    )

    def deploy():
        return subprocess.run(
            ['bash', str(repo / 'deploy.sh'), 'HEAD'], env=env, capture_output=True, text=True
        )

    with lease.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        busy = deploy()
        assert busy.returncode == 4
        assert busy.stderr == 'deploy: heavy lock held\n'
    free = deploy()
    assert free.returncode == 1
    assert 'go.mod' in free.stderr and 'heavy lock held' not in free.stderr
    assert lease.read_bytes() == b''
    assert list(scratch.iterdir()) == []
    with lease.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_deploy_stops_when_release_guard_refuses(tmp_path):
    repo = tmp_path / 'repo'
    repo.mkdir()
    shutil.copyfile(Path(__file__).parents[1] / 'deploy.sh', repo / 'deploy.sh')
    (repo / 'file').touch()
    for args in (['init', '-q'], ['add', '.'], ['-c', 'user.name=Synthetic', '-c', 'user.email=test@example.test', 'commit', '-qm', 'fixture']):
        subprocess.run(['git', '-C', str(repo), *args], check=True)
    guard = tmp_path / 'guard'
    guard.mkdir()
    (guard / 'release-guard').write_text('#!/bin/sh\nexit 3\n')
    (guard / 'release-guard').chmod(0o755)
    home = tmp_path / 'home'
    env = dict(os.environ, HOME=str(home), PATH=f'{guard}:{os.environ["PATH"]}')
    refused = subprocess.run(['bash', str(repo / 'deploy.sh'), 'HEAD'], env=env, capture_output=True, text=True)
    assert refused.returncode == 3
    assert not home.exists()
