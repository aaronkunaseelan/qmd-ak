"""Real filesystem/subprocess edge: delivery failure does not consume a transition."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time
import pytest

from qmd2.ingestd import replace_receipt
from qmd2.status import view


@pytest.fixture(autouse=True)
def systemctl_edge(tmp_path, monkeypatch):
    command = tmp_path / 'systemctl'
    command.write_text(
        '#!/bin/sh\nif [ "$2" = set-property ]; then\n'
        '  echo "$*" >> "$PRIORITY_CALLS"\n'
        '  test ! -f "$PRIORITY_FAIL"\nfi\n'
    )
    command.chmod(0o755)
    monkeypatch.setenv('PATH', str(tmp_path) + os.pathsep + os.environ['PATH'])
    monkeypatch.setenv('PRIORITY_CALLS', str(tmp_path / 'calls'))
    monkeypatch.setenv('PRIORITY_FAIL', str(tmp_path / 'fail'))


def run_verify(home, notify=False):
    return subprocess.run(
        [
            sys.executable,
            '-m',
            'qmd2.verify',
            '--home',
            str(home),
            *(['--notify'] if notify else []),
        ],
        capture_output=True,
        text=True,
        env={**os.environ, 'XDG_STATE_HOME': str(home / 'state')},
    )


def test_delivery(tmp_path):
    now = time.time()
    receipt = dict(ok=True, enumerated=0, written=0, unchanged=0, excluded=0, failed=0)
    status = dict(
        written_at=now,
        heartbeat_at=now,
        backlog=[],
        totals={},
        attachments=dict(
            last_failure_at=now,
            last_failure=dict(id='synthetic/att/1', reason='HTTPError', at=now),
            worker=dict(heartbeat_at=now),
        ),
        sources={
            'pi': dict(
                receipt=receipt,
                cadence_s=15,
                last_complete=now - 7200,
                fresh_through=now - 7200,
                last_complete_at=now - 7200,
                error='provider HTTP 503',
            ),
            'healthy': dict(
                receipt=receipt,
                cadence_s=15,
                last_complete=now,
                fresh_through=now,
                last_complete_at=now,
                error='not red',
            ),
            'no_error': dict(
                receipt=receipt, cadence_s=15, last_complete_at=now - 7200, error=None
            ),
        },
    )
    (tmp_path / 'status.json').write_text(json.dumps(status))
    alerts = tmp_path / 'alerts.jsonl'
    alerts.mkdir()

    assert run_verify(tmp_path, notify=True).returncode == 4
    assert json.loads((tmp_path / 'verify.json').read_text())['emitted'] is None
    alerts.rmdir()
    assert run_verify(tmp_path, notify=True).returncode == 0
    assert json.loads((tmp_path / 'verify.json').read_text())['state'] == 'red'
    assert 'pi:discovery' in json.loads(alerts.read_text())['red_checks']
    reasons = json.loads(alerts.read_text())['reasons']
    assert set(reasons) == set(json.loads(alerts.read_text())['red_checks'])
    assert 'provider HTTP 503' in reasons['pi:discovery']
    assert 'synthetic/att/1' in reasons['attachments:failure']
    assert 'HTTPError' in reasons['attachments:failure']
    telemetry = json.loads((tmp_path / 'state/telemetry/qmd2.json').read_text())
    assert telemetry['schema'] == 'telemetry/1'
    checks = {c['name']: c for c in telemetry['checks']}
    assert checks['status:stale']['ok']
    assert not checks['pi:discovery']['ok']
    assert not any(name.startswith('disk:') for name in checks)
    assert telemetry['state'] == 'red'
    assert run_verify(tmp_path, notify=True).returncode == 0
    assert len(alerts.read_text().splitlines()) == 1
    for epoch, count, legacy, expected in (
        (0, 1, 1, True),
        (0, 1, 1, False),
        (0, 2, 2, True),
        (1, 0, 0, False),
        (1, 1, 1, True),
        (1, 1, 1, False),
        (2, 1, 1, True),
        (2, 1, 1, False),
    ):
        status['sources']['healthy'].update(
            parse_failures=count,
            legacy=legacy,
            parser_epoch=epoch,
        )
        (tmp_path / 'status.json').write_text(json.dumps(status))
        result = run_verify(tmp_path)
        assert result.returncode == 0
        red = json.loads(result.stdout)['red_checks']
        assert ('healthy:parse' in red) == expected
        assert ('healthy:legacy' in red) == expected
    status['sources']['healthy']['parser_reset_at'] = now
    (tmp_path / 'status.json').write_text(json.dumps(status))
    result = run_verify(tmp_path)
    assert 'healthy:parse' in json.loads(result.stdout)['red_checks']
    assert 'healthy:parse' not in json.loads(run_verify(tmp_path).stdout)['red_checks']
    (tmp_path / 'status.json').write_text('{')
    assert run_verify(tmp_path, notify=True).returncode == 4


def test_capture_limits_filesystem(tmp_path):
    now = time.time()
    for ok, limited, cadence, age, capture, discovery in (
        (False, True, 720, 300, False, False),
        (False, True, 720, 3601, False, True),
        (False, False, 720, 300, True, False),
        (True, False, 86400, 25 * 3600, False, False),
        (True, False, 86400, 49 * 3600, False, True),
        (True, False, 720, None, False, True),
        (True, False, 0, None, False, False),
        (True, False, None, None, False, False),
    ):
        receipt = dict(
            ok=ok, enumerated=int(not ok), written=0, unchanged=0, excluded=0, failed=int(not ok)
        )
        status = dict(
            written_at=now,
            heartbeat_at=now,
            backlog=[],
            totals={},
            sources={
                'x': dict(
                    receipt={**receipt, 'rate_limited': limited, 'error': 'FileNotFoundError'},
                    cadence_s=cadence,
                    last_complete=now - age if age is not None else None,
                    fresh_through=now - age if age is not None else None,
                    last_complete_at=now - age if age is not None else None,
                    source_head=None,
                    processed_through=None,
                )
            },
        )
        (tmp_path / 'status.json').write_text(json.dumps(status))
        (tmp_path / 'verify.json').unlink(missing_ok=True)
        (tmp_path / 'alerts.jsonl').unlink(missing_ok=True)
        result = run_verify(tmp_path, notify=True)
        assert result.returncode == 0
        red = json.loads(result.stdout)['red_checks']
        if capture:
            detail = json.loads((tmp_path / 'alerts.jsonl').read_text())['reasons']['x:capture']
            assert 'FileNotFoundError' in detail and 'failed=1' in detail
        assert ('x:capture' in red) == capture
        assert ('x:discovery' in red) == discovery
        telemetry = json.loads((tmp_path / 'state/telemetry/qmd2.json').read_text())
        checks = {c['name']: c for c in telemetry['checks']}
        if not cadence:
            assert 'x:discovery' not in checks and 'x:head' not in checks
        else:
            assert checks['x:head']['ok'] is False
            assert 'unknown' in checks['x:head']['reason'].lower()
            if age is None:
                assert 'unknown' in checks['x:discovery']['reason'].lower()


@pytest.mark.parametrize('source', ['attachment', 'manual', 'gsd-events'])
def test_unscheduled_integrity_and_newest_get(tmp_path, monkeypatch, source):
    from qmd2.ingestd import Ingestd
    from qmd2.verify import evaluate, check_newest

    monkeypatch.setenv('QMD2_VERIFY_CLI', str(Path(sys.executable).parent / 'qmd2'))
    daemon = Ingestd(tmp_path, remote=False)
    try:
        daemon.record(source, b'null', {'key': 'synthetic', 'time': '2026-10-20'})
        if source == 'manual':
            # Malformed manual input yields an attachment-source error doc, not a manual row.
            doc = dict(
                id='manual:synthetic',
                item='manual:synthetic',
                source='manual',
                group='files',
                kind='text',
                time='2026-10-20',
                parts=[dict(kind='authored', text='Synthetic manual body')],
            )
            daemon.record('manual', json.dumps(doc).encode(), {'key': doc['id']})
        daemon.commit()
        now = time.time()
        row = daemon.source_status(source, 0, daemon.index, now)
        assert row['ok'] is None and row['receipt']['ok'] is None
        assert row['parse_failures'] == 1
        row.update(legacy=1, oldest_pending=now - 3601)
        status = dict(written_at=now, heartbeat_at=now, backlog=[], sources={source: row})
        red = evaluate(status, now)
        assert set(red) == {source + ':' + check for check in ('parse', 'legacy', 'ingest')}
        assert row['newest_id']
        checks = []
        check_newest(tmp_path, status, checks)
        assert checks and checks[0]['ok']
        for pack in daemon.state.packs('doc'):
            daemon.packs.path('doc', source, pack['seq']).unlink()
        checks = []
        check_newest(tmp_path, status, checks)
        assert [c['name'] for c in checks if not c['ok']] == [source + ':get']
    finally:
        daemon.upload_pool.shutdown()
        daemon.index.writer.wait_merging_threads()
        daemon.state.db.close()
        daemon.lock.close()


@pytest.mark.parametrize('blocked', ['verify.tmp', 'state/telemetry', 'alerts.jsonl'])
def test_final_write_checks_filesystem(tmp_path, blocked):
    now = time.time()
    (tmp_path / 'status.json').write_text(
        json.dumps(
            dict(
                written_at=now,
                heartbeat_at=now,
                backlog=[],
                sources={'manual': dict(cadence_s=0)},
            )
        )
    )
    (tmp_path / 'verify.json').write_text(
        json.dumps(
            dict(
                cold_at=now,
                cold_ok=True,
                raw_ok=True,
                priority_level=0,
            )
        )
    )
    target = tmp_path / blocked
    target.parent.mkdir(parents=True, exist_ok=True)
    if blocked == 'state/telemetry':
        target.write_text('blocked')
    else:
        target.mkdir()
    result = run_verify(tmp_path, notify=True)
    assert result.returncode == 4
    assert json.loads(result.stdout)['state'] == 'red'
    if blocked != 'verify.tmp':
        assert json.loads((tmp_path / 'verify.json').read_text())['state'] == 'red'
    if blocked != 'state/telemetry':
        telemetry = json.loads((tmp_path / 'state/telemetry/qmd2.json').read_text())
        assert telemetry['state'] == 'red'
        assert any(not check['ok'] for check in telemetry['checks'])


def test_archive_head_applicability_filesystem(tmp_path, monkeypatch):
    from qmd2.ingestd import Ingestd
    from qmd2.verify import evaluate, check_newest

    daemon = Ingestd(tmp_path, remote=False)
    try:
        now = time.time()
        receipt = dict(
            ok=True,
            enumerated=0,
            written=0,
            unchanged=0,
            excluded=0,
            failed=0,
            source_head=now,
            last_complete=now,
            fresh_through=now,
        )
        for source, cadence, applicable in (
            ('files-archive', 86400, False),
            ('files', 1440, True),
            ('legacy', 86400, False),
            ('synthetic', 900, True),
        ):
            daemon.state.set('receipt:' + source, receipt)
            row = daemon.source_status(source, cadence, daemon.index, now)
            assert row['processed_through'] is None
            status = dict(written_at=now, heartbeat_at=now, backlog=[], sources={source: row})
            path = tmp_path / 'status.json'
            path.write_text(json.dumps(status))
            checks = []
            red = evaluate(json.loads(path.read_text()), now, checks)
            assert red == ([source + ':head'] if applicable else [])
            heads = [c for c in checks if c['name'] == source + ':head']
            if applicable:
                assert heads[0]['reason'] == 'Unknown timestamp'
            else:
                assert not heads
                # Even an obsolete indexed pointer cannot trigger an archive-only get.
                row['newest_id'] = 'synthetic:missing'
                checks = []
                monkeypatch.setenv('QMD2_VERIFY_CLI', str(tmp_path / 'missing-cli'))
                check_newest(tmp_path, status, checks)
                assert not checks
            row['fresh_through'] = None
            assert source + ':discovery' in evaluate(status, now)
    finally:
        daemon.upload_pool.shutdown()
        daemon.index.writer.wait_merging_threads()
        daemon.state.db.close()
        daemon.lock.close()


def test_event_counts_tantivy(tmp_path):
    from qmd2.index import Index
    from qmd2.doc import version
    from qmd2.verify import metrics

    index = Index(tmp_path, writable=True)
    from datetime import datetime, timezone

    now = 1791892800  # 2026-10-13 12:00 UTC
    midnight = 1791849600
    stamps = (midnight - 6 * 86400 - 1, midnight - 6 * 86400, midnight - 1, midnight, now)
    rows = [('pi', stamp) for stamp in stamps] + [
        (source, midnight) for source in ('gsd-events', 'attachment', 'manual')
    ]
    for i, (source, stamp) in enumerate(rows):
        doc = dict(
            id=str(i),
            item=str(i),
            source=source,
            group='sessions',
            kind='message',
            time=stamp,
            parts=[dict(kind='authored', text='Synthetic counts')],
        )
        doc['version'] = version(doc, 0)
        index.upsert(doc, {})
    index.commit()
    result = metrics(tmp_path, {'sources': {'pi': {}}}, now)
    points = [m for m in result if m['name'] == 'docs_by_day' and m['labels']['source'] == 'pi']
    assert [p['value'] for p in points] == [1, 0, 0, 0, 0, 1, 2]
    assert [p['labels']['day'] for p in points] == [
        datetime.fromtimestamp(midnight - day * 86400, timezone.utc)
        .isoformat()
        .replace('+00:00', 'Z')
        for day in range(6, -1, -1)
    ]
    assert all(p['labels']['source'] == 'pi' and p['unit'] == 'docs' for p in points)
    for source in ('gsd-events', 'attachment', 'manual'):
        producer = [
            m for m in result if m['name'] == 'docs_by_day' and m['labels']['source'] == source
        ]
        assert len(producer) == 7
        assert [m['value'] for m in producer] == [0, 0, 0, 0, 0, 0, 1]
    later = metrics(tmp_path, {'sources': {'pi': {}}}, now + 3600)
    assert [m for m in later if m['name'] == 'docs_by_day'] == [
        m for m in result if m['name'] == 'docs_by_day'
    ]


def test_priority_subprocess(tmp_path):
    receipt = dict(ok=True, enumerated=0, written=0, unchanged=0, excluded=0, failed=0)
    calls = tmp_path / 'calls'
    for ratio, level, count in ((5.9, 0, 1), (6, 1, 2), (6, 1, 2), (12, 2, 3), (0, 0, 4)):
        now = time.time()
        status = dict(
            written_at=now,
            heartbeat_at=now,
            backlog=[],
            sources={
                'synthetic': dict(
                    receipt=receipt,
                    cadence_s=1000,
                    last_complete_at=now,
                    oldest_pending_at=now - ratio * 1000,
                ),
                'disabled': dict(cadence_s=0, last_complete_at=0),
            },
        )
        (tmp_path / 'status.json').write_text(json.dumps(status))
        result = run_verify(tmp_path)
        assert result.returncode == 0
        assert json.loads((tmp_path / 'verify.json').read_text())['priority_level'] == level
        assert len(calls.read_text().splitlines()) == count
        assert calls.read_text().splitlines()[-1] == (
            f'--user set-property --runtime qmd2.slice CPUWeight={(10, 50, 90)[level]}'
        )
        telemetry = json.loads((tmp_path / 'state/telemetry/qmd2.json').read_text())
        values = {m['name']: m['value'] for m in telemetry['metrics']}
        assert values['priority_level'] == level
        assert ratio <= values['max_lateness_ratio'] < ratio + 0.1
    (tmp_path / 'fail').touch()
    status['sources']['synthetic']['last_complete_at'] = time.time() - 12000
    (tmp_path / 'status.json').write_text(json.dumps(status))
    for count in (5, 6):
        result = run_verify(tmp_path)
        assert result.returncode == 4
        assert 'priority:apply' in json.loads(result.stdout)['red_checks']
        assert json.loads((tmp_path / 'verify.json').read_text())['priority_level'] == 0
        assert len(calls.read_text().splitlines()) == count


def healthy_status(now, receipt):
    return {
        'written_at': now,
        'heartbeat_at': now,
        'backlog': [],
        'sources': {
            'x': {
                **receipt,
                'receipt': receipt,
                'cadence_s': 7200,
                'processed_through': now,
                'source_head': now,
            }
        },
    }


@pytest.mark.parametrize('freshness', ['expired', 'unknown', 'current'])
def test_discovery_requires_current_completeness_subprocess(tmp_path, freshness):
    now = time.time()
    boundary = (
        None if freshness == 'unknown' else now - (10 * 3600 if freshness == 'expired' else 60)
    )
    receipt = replace_receipt(
        {'fresh_through': boundary},
        {
            'ok': True,
            'head_complete': True,
            'rate_limited': True,
            'backfill_error': 'bird request allowance exhausted',
            'enumerated': 1,
            'written': 0,
            'unchanged': 1,
            'excluded': 0,
            'failed': 0,
            'source_head': now,
        },
        now,
    )
    status = healthy_status(now, receipt)
    contract = view(tmp_path, status)['sources']['x']
    assert (contract['current'] is True) == (freshness == 'current')
    (tmp_path / 'status.json').write_text(json.dumps(status))
    (tmp_path / 'verify.json').write_text(
        json.dumps(
            {
                'priority_level': 0,
                'cold_at': now,
                'cold_ok': True,
                'raw_ok': True,
            }
        )
    )
    systemctl = tmp_path / 'systemctl'
    systemctl.write_text('#!/bin/sh\nexit 0\n')
    systemctl.chmod(0o700)
    result = subprocess.run(
        [sys.executable, '-m', 'qmd2.verify', '--home', str(tmp_path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        env={
            **os.environ,
            'PATH': str(tmp_path) + os.pathsep + os.environ['PATH'],
            'XDG_STATE_HOME': str(tmp_path / 'telemetry-home'),
        },
    )
    assert result.returncode == 0, result.stderr
    outcome = json.loads(result.stdout)
    assert ('x:discovery' in outcome['red_checks']) == (freshness != 'current'), (
        f'{freshness}: contract_current={contract["current"]}, '
        f'verify={outcome["state"]}, red_checks={outcome["red_checks"]}'
    )
