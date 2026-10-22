"""The ingestd-owned pack catalog and checkpoints; readers use read-only SQLite."""

import json
import sqlite3
from pathlib import Path

DEFAULT_HOME = Path.home() / '.local/share/qmd2'


class State:
    def __init__(self, home=DEFAULT_HOME, writable=False):
        self.home = Path(home)
        if writable:
            self.home.mkdir(parents=True, exist_ok=True)
        path = self.home / 'state.sqlite'
        self.db = sqlite3.connect(
            str(path) if writable else f'file:{path}?mode=ro', uri=not writable
        )
        self.db.row_factory = sqlite3.Row
        if writable:
            self.db.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS packs (
                    stream TEXT, source TEXT, seq INTEGER, size INTEGER DEFAULT 0,
                    created REAL, sealed INTEGER DEFAULT 0, uploaded INTEGER DEFAULT 0,
                    frames TEXT, PRIMARY KEY(stream,source,seq));
                CREATE TABLE IF NOT EXISTS checkpoints (key TEXT PRIMARY KEY, value TEXT);
                CREATE INDEX IF NOT EXISTS pending_uploads ON packs(created)
                    WHERE sealed=1 AND uploaded=0;
            ''')
            self.db.commit()

    def get(self, key, default=None):
        row = self.db.execute('SELECT value FROM checkpoints WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO checkpoints VALUES (?,?)', (key, json.dumps(value)))

    def packs(self, stream=None):
        sql = 'SELECT * FROM packs'
        args = ()
        if stream:
            sql += ' WHERE stream=?'
            args = (stream,)
        return self.db.execute(sql + ' ORDER BY source,seq', args).fetchall()

    def commit(self):
        self.db.commit()
