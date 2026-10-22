"""Real CLI/ingestd/filesystem edge: a busy lease is visible without losing the request."""

import json
import os
import subprocess
import sys
import time

import pytest


@pytest.mark.parametrize('source', [None, 'pi'])
def test_reindex_waits_visibly_then_completes(tmp_path, source):
    home = tmp_path / 'store'
    env = dict(os.environ, HOME=str(tmp_path))
    script = '''
import sys
from pathlib import Path
from qmd2.ingestd import Ingestd
d = Ingestd(Path(sys.argv[1]), remote=False)
d.commit()
if sys.argv[2] == 'request':
 d.request()
 d.request()
 d.last_status = 0
 d.publish_status()
'''

    def daemon(phase):
        subprocess.run([sys.executable, '-c', script, str(home), phase], env=env, check=True)

    daemon('start')
    lease = tmp_path / 'workbench/tmp/qmd-target/heavy.lease'
    lease.mkdir(parents=True)
    (lease / 'owner').write_text('synthetic busy owner\nsecond line must not escape\n')
    process = subprocess.Popen(
        [
            sys.executable,
            '-m',
            'qmd2.cli',
            '--home',
            str(home),
            'reindex',
            *(['--reparse', source] if source else []),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not (home / 'reindex.request').exists():
            assert process.poll() is None and time.monotonic() < deadline
            time.sleep(0.02)
        daemon('request')
        status = json.loads((home / 'status.json').read_text())['maintenance']
        assert status == dict(
            source=source,
            stage='waiting_for_lease',
            started_at=status['started_at'],
            last_slice_at=None,
            lease_owner='synthetic busy owner',
        )
        assert status['started_at'] > 0
        assert (home / 'reindex.request').exists()
        time.sleep(2.2)  # More than two CLI polls must still print only once.
        (lease / 'owner').unlink()
        lease.rmdir()
        daemon('request')
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0 and stdout == 'reindex complete\n'
        assert stderr == 'reindex: waiting for heavy lease: synthetic busy owner\n'
        assert json.loads((home / 'status.json').read_text())['maintenance'] is None
        assert not lease.exists()
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
