"""Fixed source contracts: name, parser, fetch argv, cadence_s (0 disables scheduled checks),
optional kill timeout_s (default 300).
"""

from pathlib import Path
from ..accounts import account as configured
from ..attachments import parse as parse_attachment
from .gsd_events import parse as parse_gsd_events
from .gmail import parse as parse_gmail
from .sessions import parse
from .files import parse as parse_files
from .calendar import parse as parse_calendar
from .fathom import parse as parse_fathom
from .whatsapp import parse as parse_whatsapp
from .x import parse as parse_x
from .instapaper import parse as parse_instapaper

_HOME = Path.home() / '.local/share/qmd2'
ARCHIVE_ONLY_SOURCES = {'legacy': 'custody-unversioned-v1'}


def head_kind(source):
    """Lane head kind from its parser declaration, not individual raw-record kinds."""
    for name, parser, *_ in SOURCES:
        if name == source:
            return 'archive-only' if parser is None else 'raw'
    return 'archive-only' if source in ARCHIVE_ONLY_SOURCES else 'raw'


def fetch(source, account, *flags):
    if source == 'x':
        flags = ('--x-username', configured('x_username'), *flags)
    return [
        str(_HOME / 'bin/qmd2-fetch'),
        source,
        '--account',
        account,
        *flags,
        '--spool',
        str(_HOME / 'spool' / source),
        '--state',
        str(_HOME / 'capture-state' / source),
        '--timeout',
        '10m',
    ]


SOURCES = [
    ('gsd-events', parse_gsd_events, None, 0),
    ('attachment', parse_attachment, None, 0),
    ('manual', parse_attachment, None, 0),
    (
        'x',
        parse_x,
        fetch('x', configured('x_account')),
        7200,
        600,
    ),
    (
        'instapaper',
        parse_instapaper,
        fetch('instapaper', configured('instapaper_account')),
        720,
        600,
    ),
    (
        'whatsapp',
        parse_whatsapp,
        fetch(
            'whatsapp',
            configured('whatsapp_account'),
            '--journal',
            str(Path.home() / '.local/share/qmd/wa-meow/journal'),
        ),
        60,
        600,
    ),
    ('gmail', parse_gmail, fetch('gmail', configured('gmail_account')), 720, 600),
    ('claude', parse, None, 15),
    ('codex', parse, None, 15),
    ('pi', parse, None, 15),
    ('files', parse_files, None, 24 * 60),
    ('files-archive', None, None, 86400),
    ('fathom', parse_fathom, fetch('fathom', configured('fathom_account')), 720, 600),
    ('calendar', parse_calendar, fetch('calendar', configured('calendar_account')), 900, 600),
]
