"""Host account identities, shared by parsers and capture commands."""

import json
import os
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def accounts():
    path = Path(os.environ.get('QMD2_ACCOUNTS', '~/.config/qmd2/accounts.json')).expanduser()
    try:
        value = json.loads(path.read_text())
        if not isinstance(value, dict):
            raise TypeError()
        return value
    except (OSError, ValueError, TypeError):
        raise RuntimeError('qmd2 accounts config unavailable: QMD2_ACCOUNTS') from None


def account(key):
    value = accounts().get(key)
    if not value:
        raise RuntimeError(f'qmd2 accounts config requires {key}')
    return value


def file_roots():
    return [(name, Path(path).expanduser()) for name, path in account('file_roots').items()]
