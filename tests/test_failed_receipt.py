"""The newest failed run's reason survives the next ok run (owner ruling A-8)."""

import json

from qmd2.ingestd import replace_receipt
from qmd2.status import view

FAILED = dict(
    ok=False,
    error='provider refused',
    failed=2,
    enumerated=2,
    written=0,
    unchanged=0,
    excluded=0,
)
UNREACHABLE = dict(
    ok=False,
    error='socket closed',
    failed=0,
    enumerated=0,
    written=0,
    unchanged=0,
    excluded=0,
)
OK = dict(
    ok=True,
    error=None,
    failed=0,
    enumerated=1,
    written=1,
    unchanged=0,
    excluded=0,
)


def test_failed_run_kept_after_ok_run():
    tabulated = (
        (
            'failed then ok',
            [FAILED, OK],
            dict(ok=False, error='provider refused', failed=2),
        ),
        ('ok then ok', [OK, OK], None),
        (
            'failed then failed',
            [FAILED, UNREACHABLE],
            dict(ok=False, error='socket closed', failed=0),
        ),
        (
            'no nesting',
            [FAILED, dict(UNREACHABLE, last_failed={'at': 1, 'started': 2})],
            dict(ok=False, error='socket closed', failed=0),
        ),
    )
    for case, runs, expected in tabulated:
        receipt = {}
        for run in runs:
            receipt = replace_receipt(receipt, run, 1000.0)
        kept = receipt.get('last_failed')
        if expected is None:
            assert not kept, case
            continue
        assert set(kept) == {'at', 'started', *expected}, case
        for key, value in expected.items():
            assert kept[key] == value, (case, key)
        assert kept['started'] == 1000.0, case
        assert kept['at'] > kept['started'], case


def test_status_view_shows_last_failed_filesystem(tmp_path):
    kept = {'at': 2, 'started': 1, 'ok': False, 'error': 'refused'}
    (tmp_path / 'status.json').write_text(
        json.dumps({'sources': {'x': {'last_failed': kept}, 'gmail': {}}})
    )
    sources = view(tmp_path, json.loads((tmp_path / 'status.json').read_text()))['sources']
    assert sources['x']['last_failed'] == kept
    assert sources['gmail']['last_failed'] is None


def test_failed_runs_counts_consecutive_failures():
    receipt = {}
    for run, runs in ((FAILED, 1), (UNREACHABLE, 2), (OK, 0), (OK, 0), (FAILED, 1)):
        receipt = replace_receipt(receipt, run, 1000.0)
        assert receipt['failed_runs'] == runs
