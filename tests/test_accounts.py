"""Synthetic account configuration and capture argv contracts."""

import json
import os
import subprocess
import sys

import pytest

from qmd2.accounts import account, accounts, file_roots
from qmd2.sources import SOURCES


@pytest.mark.parametrize(
    'source,key',
    [
        (source, source + '_account')
        for source in ('x', 'instapaper', 'whatsapp', 'gmail', 'fathom', 'calendar')
    ],
)
def test_capture_account_table(source, key):
    argv = next(row[2] for row in SOURCES if row[0] == source)
    assert argv[argv.index('--account') + 1] == account(key)
    if source == 'x':
        assert argv[argv.index('--x-username') + 1] == account('x_username')


@pytest.mark.parametrize(
    'body,key,error',
    [
        ('{}', 'gmail_account', 'gmail_account'),
        ('{"owner_domain": ""}', 'owner_domain', 'owner_domain'),
        ('[]', 'gmail_account', 'QMD2_ACCOUNTS'),
        ('{', 'gmail_account', 'QMD2_ACCOUNTS'),
        (None, 'gmail_account', 'QMD2_ACCOUNTS'),
    ],
)
def test_accounts_filesystem_table(tmp_path, monkeypatch, body, key, error):
    path = tmp_path / 'accounts.json'
    if body is not None:
        path.write_text(body)
    monkeypatch.setenv('QMD2_ACCOUNTS', str(path))
    accounts.cache_clear()
    try:
        with pytest.raises(RuntimeError, match=error):
            account(key)
    finally:
        accounts.cache_clear()


def test_configured_file_root_filesystem(tmp_path, monkeypatch):
    from qmd2.index import rank_row

    path = tmp_path / 'accounts.json'
    path.write_text(json.dumps(dict(file_roots=dict(notes=str(tmp_path / 'notes')))))
    monkeypatch.setenv('QMD2_ACCOUNTS', str(path))
    accounts.cache_clear()
    try:
        root = dict(file_roots())['notes']
        root.mkdir()
        (root / 'note.md').write_text('synthetic')
        row = dict(
            id='file:notes:note.md', source='files', kind='file', time='2026-01-01T00:00:00Z'
        )
        rank_row(row, 1, row['id'], {})
        assert not row['gone']
        (root / 'note.md').unlink()
        rank_row(row, 1, row['id'], {})
        assert row['gone']
    finally:
        accounts.cache_clear()


def test_missing_accounts_subprocess(tmp_path):
    result = subprocess.run(
        [sys.executable, '-c', 'import qmd2.sources'],
        env=dict(os.environ, QMD2_ACCOUNTS=str(tmp_path / 'absent.json')),
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert 'qmd2 accounts config unavailable: QMD2_ACCOUNTS' in result.stderr
