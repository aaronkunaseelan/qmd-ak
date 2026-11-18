"""The head check ages the newest head's wait, not the gap between item times (F-27)."""

import qmd2.ingestd as ingestd
from qmd2.ingestd import replace_receipt
from qmd2.verify import evaluate

COUNTED = dict(ok=True, enumerated=0, written=0, unchanged=0, excluded=0, failed=0)


def status_of(now, row):
    return dict(written_at=now, heartbeat_at=now, backlog=[], sources={'x': row})


def row_of(now, head, head_at, processed, cadence=3600):
    receipt = dict(COUNTED, source_head=head, rate_limited=False, last_complete=now - 60)
    if head_at is not None:
        receipt['head_at'] = head_at
    return dict(
        receipt=receipt,
        cadence_s=cadence,
        source_head=head,
        processed_through=processed,
        last_complete=now - 60,
        last_complete_at=now - 60,
        fresh_through=now - 60,
    )


def head_checks(now, row):
    checks = []
    red = evaluate(status_of(now, row), now, checks)
    return red, [c for c in checks if c['name'] == 'x:head']


def test_head_not_red_during_searchable_race():
    now = 1_800_000_000.0
    head = now - 60
    red, checks = head_checks(
        now, row_of(now, head, now - 3, head - (2 * 3600 + 43 * 60))
    )
    assert red == []
    assert checks[0]['ok'] is True


def test_head_red_when_unprocessed_past_limit():
    now = 1_800_000_000.0
    head = now - 60
    red, checks = head_checks(now, row_of(now, head, now - 7801, head - 100))
    assert red == ['x:head']
    assert checks[0]['reason'] == 'Searchable content beyond lane deadline'


def test_head_at_kept_while_head_unchanged(monkeypatch):
    class Clock:
        value = 1000.0

        def time(self):
            return self.value

    clock = Clock()
    monkeypatch.setattr(ingestd, 'time', clock)
    receipt = replace_receipt({}, dict(COUNTED, source_head=500.0), 900.0)
    assert receipt['head_at'] == 1000.0
    clock.value = 1200.0
    receipt = replace_receipt(receipt, dict(COUNTED, source_head=500.0), 1100.0)
    assert receipt['head_at'] == 1000.0
    clock.value = 1400.0
    receipt = replace_receipt(receipt, dict(COUNTED, source_head=900.0), 1300.0)
    assert receipt['head_at'] == 1400.0
    clock.value = 1600.0
    receipt = replace_receipt(receipt, dict(COUNTED, source_head=900.0), 1500.0)
    assert receipt['head_at'] == 1400.0
    receipt = replace_receipt(receipt, dict(COUNTED), 1700.0)
    assert 'head_at' not in receipt
