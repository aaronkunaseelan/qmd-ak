"""K7 freshness checks and telemetry.
Verify owns verify.json, telemetry/qmd2.json and state-change alerts.jsonl.
All timestamps are epoch seconds; document counts use event time.
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import tempfile
from pathlib import Path

from .state import DEFAULT_HOME
from .exits import EXIT_CODES
from .sources import head_kind
from .status import currency, lateness_limit
from .packs import SEAL_AGE


REASONS = {
    'stale': 'Status older than five minutes',
    'heartbeat': 'Ingest heartbeat older than five minutes',
    'late': 'Work waiting beyond its deadline',
    'failure': 'Recent attachment failure',
    'worker': 'Attachment worker heartbeat missing or late',
    'migration': 'Attachment outcome migration is incomplete or has failures',
    'missing': 'No source receipts',
    'receipt_missing': 'Capture receipt missing or incomplete',
    'receipt': 'Capture counts do not add up',
    'capture': 'Capture run failed',
    'discovery': 'No complete capture within lane deadline',
    'unresolved': 'Failed item unresolved beyond lane deadline',
    'ingest': 'Ingest waiting beyond lane deadline',
    'head': 'Searchable content beyond lane deadline',
    'parse': 'Parser failures recorded',
    'legacy': 'Legacy fallbacks recorded',
    'attachment': 'Attachment failures recorded',
    'attachment_late': 'Attachment waiting beyond lane deadline',
    'get': 'Newest document could not be read',
    'cold_get': 'Cold archive read failed',
    'raw_get': 'Cold file raw hash check failed',
    'state': 'Verify state could not be read',
    'apply': 'Could not apply qmd2 slice CPU weight',
    'blocked': 'Spool ingestion blocked',
    'seal': 'Pack sealing failed',
}


def reason(name, failed, status=None):
    if not failed:
        return 'OK'
    message = REASONS.get(name.split(':')[-1], 'Verify evaluation failed')
    source, _, check = name.partition(':')
    row = (status or {}).get('sources', {}).get(source, {})
    if check == 'capture':
        receipt = row.get('receipt') or {}
        return f"{message}: error={receipt.get('error') or row.get('error') or 'unknown'}; failed={receipt.get('failed')}"
    if name == 'seal:late':
        return 'Open pack overdue for sealing'
    if name == 'spool:blocked':
        return message + ': ' + json.dumps((status or {}).get('spool_errors', {}), sort_keys=True)
    if name == 'packs:seal':
        return message + ': ' + json.dumps((status or {}).get('seal_errors', []), sort_keys=True)
    if name == 'attachments:failure':
        failure = ((status or {}).get('attachments') or {}).get('last_failure') or {}
        return f"{message}: id={failure.get('id') or 'unknown'}; reason={failure.get('reason') or 'unknown'}"
    return message + (': ' + row['error'] if row.get('error') else '')


def add_document_metrics(home, now, out):
    from datetime import datetime, timezone
    from .index import Index
    from .sources import SOURCES
    import tantivy

    try:
        idx = Index(home)
        at = lambda t: datetime.fromtimestamp(t, timezone.utc).replace(tzinfo=None)
        midnight = int(now) // 86400 * 86400
        for source, *_ in SOURCES:
            for day in range(7):
                start = midnight - (6 - day) * 86400
                end = start + 86400
                query = tantivy.Query.boolean_query(
                    [
                        (tantivy.Occur.Must, idx.term('source', source)),
                        (
                            tantivy.Occur.Must,
                            tantivy.Query.range_query(
                                idx.schema,
                                'time',
                                tantivy.FieldType.Date,
                                at(start),
                                at(end),
                                include_upper=False,
                            ),
                        ),
                    ]
                )
                query = idx.filters(query, exclude=False)
                out.append(
                    dict(
                        name='docs_by_day',
                        labels=dict(source=source, day=at(start).isoformat() + 'Z'),
                        value=idx.searcher.search(query, limit=1).count,
                        unit='docs',
                    )
                )
    except Exception:
        pass


def add_process_metrics(out):
    try:
        props = subprocess.run(
            ['systemctl', '--user', 'show', 'qmd2-ingestd', '-p', 'MainPID', '-p', 'MemoryMax'],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
        props = dict(line.split('=', 1) for line in props.splitlines() if '=' in line)
        pid = int(props.get('MainPID', 0))
        if pid:
            rss = next(
                line.split()[1]
                for line in Path(f'/proc/{pid}/status').read_text().splitlines()
                if line.startswith('VmRSS:')
            )
            out.append(dict(name='ingestd_rss', labels={}, value=int(rss) * 1024, unit='bytes'))
        if props.get('MemoryMax', '').isdigit():
            out.append(
                dict(
                    name='ingestd_memory_max',
                    labels={},
                    value=int(props['MemoryMax']),
                    unit='bytes',
                )
            )
    except Exception:
        pass


def metrics(home, status, now):
    out = []

    def metric(name, value, unit, **labels):
        out.append(dict(name=name, labels=labels, value=value, unit=unit))

    add_document_metrics(home, now, out)
    add_process_metrics(out)
    for day, value in status.get('files_archive_bytes', {}).items():
        metric('archive_bytes_by_day', value, 'bytes', source='files', day=day)
    metric('moving', status.get('files_moving', 0), 'count', source='files')
    metric(
        'upload_backlog_h',
        max([r['age_s'] for r in status.get('backlog', []) if r.get('sealed')] or [0]) / 3600,
        'h',
    )
    for state in ('done', 'failed', 'queued', 'unavailable'):
        metric('attachments', (status.get('attachments') or {}).get(state, 0), 'count', state=state)
    attachments = status.get('attachments') or {}
    for name in ('backfill', 'azure_pages', 'audio_minutes'):
        metric('attachments_' + name, attachments.get(name, 0), 'count')
    for reason, count in attachments.get('unavailable_by_reason', {}).items():
        metric('attachments_unavailable', count, 'count', reason=reason)
    return out


def evaluate(status, now, checks=None, observed=None):
    checks = checks if checks is not None else []

    def check(condition, name, detail=None):
        checks.append(
            dict(
                name=name, ok=not bool(condition), reason=detail or reason(name, condition, status)
            )
        )

    def age_check(stamp, limit, name, reference=now):
        unknown = stamp is None or reference is None
        check(
            unknown or reference - stamp > limit,
            name,
            'Unknown timestamp' if unknown else None,
        )

    age_check(status.get('written_at'), 300, 'status:stale')
    age_check(status.get('heartbeat_at'), 300, 'ingestd:heartbeat')
    check(
        any(r.get('sealed', False) and r['age_s'] > 7200 for r in status['backlog']), 'upload:late'
    )
    check(bool(status.get('spool_errors')), 'spool:blocked')
    check(bool(status.get('seal_errors')), 'packs:seal')
    check(
        any(not r.get('sealed', False) and r['age_s'] > SEAL_AGE for r in status['backlog']),
        'seal:late',
    )
    attachments = status.get('attachments')
    if attachments is not None:
        check(attachments.get('queue_errors', 0), 'attachments:queue')
        migration = attachments.get('migration', {})
        check(
            migration.get('error')
            or migration.get('row_errors')
            or not migration.get('complete', True)
            or migration.get('last_failure_at') is not None
            and now - migration['last_failure_at'] < 86400,
            'attachments:migration',
        )
        failed_at = attachments.get('last_failure_at')
        check(failed_at is not None and now - failed_at < 86400, 'attachments:failure')
        stamp = attachments.get('oldest_queued_at')
        if stamp is not None:
            age_check(stamp, 600, 'attachments:late')
        worker = attachments.get('worker') or {}
        age_check(worker.get('heartbeat_at'), 600, 'attachments:worker')
    check(not status['sources'], 'sources:missing')
    for source, row in status['sources'].items():
        limit = lateness_limit(row.get('cadence_s'))
        stamp = row.get('oldest_pending')
        if stamp is not None:
            age_check(stamp, limit, source + ':ingest')
        before = (observed or {}).get(source, {})
        if any(
            before.get(key, 0) != row.get(key, 0) for key in ('parser_epoch', 'parser_reset_at')
        ):
            before = {}
        for field, label in (('parse_failures', 'parse'), ('legacy', 'legacy')):
            count = row.get(field, 0)
            prior = before.get(field, 0)
            check(count > (prior if count >= prior else 0), source + ':' + label)
        check(row.get('attachment_failures', 0) > 0, source + ':attachment')
        stamp = row.get('oldest_attachment')
        if stamp is not None:
            age_check(stamp, limit, source + ':attachment_late')
        if not row.get('cadence_s'):
            continue
        receipt = row.get('receipt')
        required = ('ok', 'enumerated', 'written', 'unchanged', 'excluded', 'failed')
        if not isinstance(receipt, dict) or any(k not in receipt for k in required):
            check(True, source + ':receipt_missing')
            continue
        check(False, source + ':receipt_missing')
        counts = [receipt[k] for k in ('written', 'unchanged', 'excluded', 'failed')]
        check(receipt['enumerated'] != sum(counts), source + ':receipt')
        check(
            (not receipt['ok'] or receipt['failed'] > 0) and not receipt.get('rate_limited', False),
            source + ':capture',
        )
        freshness = currency(row, now)
        check(
            freshness['current'] is not True,
            source + ':discovery',
            'Unknown timestamp' if freshness['valid_until'] is None else None,
        )
        stamp = row.get('oldest_failed')
        if stamp is not None:
            age_check(stamp, limit, source + ':unresolved')
        if head_kind(source) != 'archive-only':
            age_check(
                row.get('processed_through'), limit, source + ':head', receipt.get('source_head')
            )
    return [c['name'] for c in checks if not c['ok']]


def save(path, value):
    temp = path.with_suffix('.tmp')
    with temp.open('w') as f:
        json.dump(value, f)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def priority(status, now):
    ratios = []
    for row in status.get('sources', {}).values():
        cadence = row.get('cadence_s') or 0
        if cadence > 0:
            stamps = [row.get('last_complete_at'), row.get('oldest_pending_at')]
            ratios.append(
                max([0] + [now - stamp for stamp in stamps if stamp is not None]) / cadence
            )
    ratio = max(ratios, default=0)
    return (2 if ratio >= 12 else 1 if ratio >= 6 else 0), ratio


def apply_priority(level):
    try:
        result = subprocess.run(
            [
                'systemctl',
                '--user',
                'set-property',
                '--runtime',
                'qmd2.slice',
                f'CPUWeight={(10, 50, 90)[level]}',
            ],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def get(home, id, cold=False):
    cli = os.environ.get('QMD2_VERIFY_CLI', str(Path.home() / '.local/bin/qmd2'))
    try:
        result = subprocess.run(
            [cli, '--home', str(home), 'get', id, '--json', *(['--cold'] if cold else [])],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode:
            return False
        payload = json.loads(result.stdout)
        return bool(payload.get('docs')) and bool(payload.get('text'))
    except Exception:
        return False


def raw_get(home):
    from .index import Index
    from .state import State

    try:
        state, index = State(home), Index(home)
        sample = next(
            (
                id
                for id, loc in index.bindings('files')
                if loc['rec_len'] <= 32 * 1024 * 1024
                and index.lookup(id)['version'].get('raw_sha256') != hashlib.sha256(b'').hexdigest()
                and state.db.execute(
                    'SELECT uploaded FROM packs WHERE stream=? AND source=? AND seq=?',
                    (loc['stream'], loc['source'], loc['seq']),
                ).fetchone()[0]
            ),
            None,
        )
        if not sample:
            return False
        expected = index.lookup(sample)['version'].get('raw_sha256')
        cli = os.environ.get('QMD2_VERIFY_CLI', str(Path.home() / '.local/bin/qmd2'))
        with tempfile.TemporaryDirectory(prefix='verify-raw-', dir=home) as directory:
            target = Path(directory) / 'raw'
            result = subprocess.run(
                [cli, '--home', str(home), 'get', sample, '--raw', '--cold', '--out', str(target)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
            )
            if result.returncode:
                return False
            with target.open('rb') as stream:
                return hashlib.file_digest(stream, 'sha256').hexdigest() == expected
    except Exception:
        return False


def check_newest(home, status, checks):
    for source, row in status['sources'].items():
        if head_kind(source) != 'archive-only' and row.get('newest_id'):
            ok = get(home, row['newest_id'])
            checks.append(dict(name=source + ':get', ok=ok, reason=reason(source + ':get', not ok)))


def check_archives(home, status, previous, now, checks):
    cold_at = previous.get('cold_at', 0)
    cold_ok = previous.get('cold_ok', False)
    raw_ok = previous.get('raw_ok', False)
    if isinstance(status, dict) and (now - cold_at >= 3600 or 'raw_ok' not in previous):
        sample = status.get('cold_sample_id')
        cold_ok = bool(sample) and get(home, sample, cold=True)
        raw_ok = raw_get(home)
        cold_at = now
    checks.append(
        dict(name='b2:cold_get', ok=bool(cold_ok), reason=reason('b2:cold_get', not cold_ok))
    )
    checks.append(dict(name='b2:raw_get', ok=bool(raw_ok), reason=reason('b2:raw_get', not raw_ok)))
    return cold_at, cold_ok, raw_ok


def publish_telemetry(home, status, now, state, checks, level, ratio):
    directory = Path(os.environ.get('XDG_STATE_HOME', Path.home() / '.local/state')) / 'telemetry'
    directory.mkdir(parents=True, exist_ok=True)
    save(
        directory / 'qmd2.json',
        dict(
            schema='telemetry/1',
            system='qmd2',
            at=now,
            state=state,
            checks=checks,
            metrics=metrics(home, status or {}, now)
            + [
                dict(name='priority_level', labels={}, value=level, unit='level'),
                dict(name='max_lateness_ratio', labels={}, value=ratio, unit='ratio'),
            ],
        ),
    )


def check_priority(status, previous, now, checks):
    failed = False
    applied_level = previous.get('priority_level')
    level, ratio = priority(status or {}, now)
    if isinstance(status, dict):
        ok = applied_level == level or apply_priority(level)
        checks.append(dict(name='priority:apply', ok=ok, reason=reason('priority:apply', not ok)))
        if ok:
            applied_level = level
        else:
            failed = True
    return applied_level, level, ratio, failed


def deliver_alert(home, previous, now, state, red, notify, status):
    failed = False
    emitted = previous.get('emitted')
    emitted_at = previous.get('emitted_at', 0)
    if notify and (state != emitted or (state == 'red' and now - emitted_at >= 86400)):
        try:
            with (home / 'alerts.jsonl').open('a') as f:
                f.write(
                    json.dumps(
                        {
                            'time': now,
                            'state': state,
                            'previous': emitted,
                            'red_checks': red,
                            'reasons': {name: reason(name, True, status) for name in red},
                        }
                    )
                    + '\n'
                )
                f.flush()
                os.fsync(f.fileno())
            emitted, emitted_at = state, now
        except Exception:
            failed = True
    return emitted, emitted_at, failed


def run(home=DEFAULT_HOME, notify=False):
    home, now = Path(home), time.time()
    failed = False
    checks = []
    status = None
    path = home / 'verify.json'

    def failure(name, detail=None):
        nonlocal failed
        failed = True
        checks.append(dict(name=name, ok=False, reason=detail or reason(name, True)))

    def outcome():
        red = [c['name'] for c in checks if not c['ok']]
        return dict(state='red' if red else 'green', red_checks=red)

    try:
        previous = json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        previous = {}
        failure('verify:state')
    try:
        status = json.loads((home / 'status.json').read_text())
        evaluate(status, now, checks, previous.get('parser_observed'))
        check_newest(home, status, checks)
    except Exception as exc:
        failure('verify:evaluation:' + type(exc).__name__)
    applied_level, level, ratio, priority_failed = check_priority(status, previous, now, checks)
    failed |= priority_failed
    cold_at, cold_ok, raw_ok = check_archives(home, status, previous, now, checks)
    result = outcome()
    emitted, emitted_at, delivery_failed = deliver_alert(
        home, previous, now, result['state'], result['red_checks'], notify, status
    )
    if delivery_failed:
        failure('verify:alert', 'Could not append state-change alert')

    def save_result():
        save(
            path,
            dict(
                time=now,
                **outcome(),
                emitted=emitted,
                emitted_at=emitted_at,
                priority_level=applied_level,
                cold_at=cold_at,
                cold_ok=cold_ok,
                raw_ok=raw_ok,
                parser_observed={
                    source: {
                        key: row.get(key, 0)
                        for key in ('parser_epoch', 'parser_reset_at', 'parse_failures', 'legacy')
                    }
                    for source, row in status['sources'].items()
                }
                if status is not None and not failed
                else previous.get('parser_observed', {}),
            ),
        )

    try:
        save_result()
    except Exception:
        failure('verify:write', 'Could not write verify result')
    try:
        publish_telemetry(home, status, now, outcome()['state'], checks, level, ratio)
    except Exception:
        failure('verify:telemetry', 'Could not publish telemetry')
        try:
            save_result()
        except Exception:
            failure('verify:write', 'Could not write verify result')
    print(json.dumps(outcome()))
    return EXIT_CODES['failed' if failed else 'ok'][0]


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--home', type=Path, default=Path(os.environ.get('QMD2_HOME', DEFAULT_HOME))
    )
    parser.add_argument('--notify', action='store_true')
    args = parser.parse_args(argv)
    return run(args.home, args.notify)


if __name__ == '__main__':
    sys.exit(main())
