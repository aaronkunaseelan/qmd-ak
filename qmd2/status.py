"""The JSON contract adds health without changing ingestd's status file or text view."""

import json
import time
from pathlib import Path

from . import installed_git_sha
from .sources import SOURCES
from .verify import lateness_limit


def head_lag(row):
    head, processed = row.get('source_head'), row.get('processed_through')
    if row.get('last_complete') is None or head is None or processed is None:
        return None
    return max(0, head - processed)


def view(home, status):
    result = dict(status)
    now = time.time()
    heartbeat = result.get('heartbeat_at')
    verify = dict(state=None, at=None, red_checks=[])
    try:
        saved = json.loads((Path(home) / 'verify.json').read_text())
        if (
            saved['state'] in ('green', 'red')
            and type(saved['time']) in (int, float)
            and isinstance(saved['red_checks'], list)
            and all(isinstance(name, str) for name in saved['red_checks'])
        ):
            verify = dict(state=saved['state'], at=saved['time'], red_checks=saved['red_checks'])
    except (OSError, ValueError, KeyError, TypeError):
        pass
    sources = {name: {} for name, *_ in SOURCES}
    sources.update(result.get('sources', {}))
    cadences = {name: cadence or None for name, _, _, cadence, *_ in SOURCES}
    for name, original in sources.items():
        row = dict(original)
        cadence = row.get('cadence_s', cadences.get(name)) or None
        limit = lateness_limit(cadence)
        fresh = row.get('fresh_through')
        valid_until = fresh + limit if cadence and fresh is not None else None
        row.update(
            lag_s=head_lag(row),
            parser_exclusions=row.get('parser_exclusions', {}),
            fresh_through=fresh,
            valid_until=valid_until,
            current=now <= valid_until if valid_until is not None else None,
            limit_s=limit,
            cadence_s=cadence,
            error=row.get('error') or None,
            rate_limited=bool(row.get('rate_limited', False)),
        )
        sources[name] = row
    attachments = result.get('attachments')
    if attachments is not None:
        attachments = dict(attachments)
        attachments.setdefault('last_failure', None)
        attachments.setdefault('last_failure_at', None)
    result.update(
        attachments=attachments,
        maintenance=result.get('maintenance'),
        schema='qmd.status.v1',
        installed_git_sha=installed_git_sha,
        at=now,
        alive=heartbeat is not None and now - heartbeat <= 300,
        heartbeat_at=heartbeat,
        verify=verify,
        sources=sources,
    )
    return result
