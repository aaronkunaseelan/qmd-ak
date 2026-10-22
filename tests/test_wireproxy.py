"""The pinned tunnel builds without network or a pre-populated module cache."""

import os
from pathlib import Path
import subprocess


def test_wireproxy_offline_build(tmp_path):
    root = Path(__file__).resolve().parents[1]
    assert 'vendor/wireproxy' in (root / 'deploy.sh').read_text()
    binary = tmp_path / 'wireproxy'
    env = dict(
        os.environ,
        GOPROXY='off',
        GOSUMDB='off',
        GOMODCACHE=str(tmp_path / 'modules'),
        GOTOOLCHAIN='local',
        CGO_ENABLED='0',
    )
    subprocess.run(
        [
            'go',
            'build',
            '-mod=vendor',
            '-trimpath',
            '-ldflags=-s -w -X main.version=v1.0.9',
            '-o',
            str(binary),
            './cmd/wireproxy',
        ],
        cwd=root / 'vendor/wireproxy',
        env=env,
        check=True,
        capture_output=True,
    )
    version = subprocess.run([binary, '--version'], capture_output=True, check=True)
    assert version.stdout.strip() == b'wireproxy, version v1.0.9'
