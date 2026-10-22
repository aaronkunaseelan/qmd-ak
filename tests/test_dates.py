from datetime import UTC

import pytest

from qmd2.cli import flag_date
from qmd2.doc import date, strict_date


@pytest.mark.parametrize(
    'value,expected',
    [
        ('2026-10-19', '2026-10-19T00:00:00+00:00'),
        ('2026-10-19T12:34:56Z', '2026-10-19T12:34:56+00:00'),
        ('2026-10-19T12:34:56+03:00', '2026-10-19T09:34:56+00:00'),
        ('1970-01-01', '1970-01-01T00:00:00+00:00'),
        ('2024-02-29', '2024-02-29T00:00:00+00:00'),
        ('garbage', None),
        ('2026-02-29', None),
        ('2026-13-01', None),
        ('2026-10-19T25:00:00Z', None),
        ('', None),
    ],
)
def test_date_parser_table(value, expected):
    import argparse

    if expected is None:
        with pytest.raises(ValueError):
            strict_date(value)
        with pytest.raises(argparse.ArgumentTypeError, match='invalid ISO date/time'):
            flag_date(value)
        assert date(value).year == 1970  # Source records retain their fallback.
    else:
        assert strict_date(value).tzinfo == UTC
        assert strict_date(value).isoformat() == flag_date(value) == expected
