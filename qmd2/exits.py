"""CLI outcomes and their help legend; argparse supplies usage/help exits."""

EXIT_CODES = {
    'ok': (0, 'ok/help; queued/unavailable materialisation outcomes carry their state/reason'),
    'usage': (2, 'usage'),
    'missing': (3, 'not found'),
    'failed': (4, 'could not get it (fetch/extraction, storage or verify operation failed)'),
}
LEGEND = 'Exit: ' + '; '.join(f'{code} {text}' for code, text in EXIT_CODES.values()) + '.'
