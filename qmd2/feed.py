"""Arrival reads use existing doc-pack coordinates, never the current search index."""

import json

from .doc import ceiling, date, dumps, visible
from .packs import Packs
from .state import State


def token(source, seq, end):
    return f'qmd1.{source}.{seq:020d}.{end:020d}'


def position(value, source):
    try:
        prefix, name, seq, end = value.split('.')
        if prefix != 'qmd1' or name != source or len(seq) != 20 or len(end) != 20:
            raise ValueError()
        if not seq.isascii() or not end.isascii() or not seq.isdigit() or not end.isdigit():
            raise ValueError()
        return int(seq), int(end)
    except (ValueError, AttributeError):
        raise ValueError('invalid feed token') from None


def item(source, pointer, header, doc):
    identity = doc['id'].split(':', 3)
    chat = identity[2] if source == 'whatsapp' and len(identity) == 4 else None
    return dict(
        token=token(source, pointer['seq'], pointer['rec_off'] + pointer['rec_len']),
        id=doc['id'],
        item=doc['item'],
        source=source,
        kind=doc.get('kind'),
        time=date(doc['time']).timestamp() if doc.get('time') not in (None, '') else None,
        arrived=header.get('arrived'),
        title=doc.get('title'),
        **{'from': doc.get('from')},
        thread=doc.get('thread'),
        chat=chat,
        version=dumps(doc['version']),
        tombstone=doc.get('kind') == 'tombstone',
        reparse=bool(header.get('reparse', False)),
    )


class Feed:
    def __init__(self, home, source):
        self.state = State(home)
        self.packs = Packs(self.state)
        self.source = source
        self.rows = self.state.db.execute(
            "SELECT * FROM packs WHERE stream='doc' AND source=? ORDER BY seq", (source,)
        ).fetchall()

    def close(self):
        self.state.db.close()

    def local_or_remote(self, row):
        args = 'doc', self.source, row['seq']
        if not any(self.packs.path(*args, sealed).exists() for sealed in (False, True)):
            if self.packs.b2 is None:
                from .b2 import B2

                self.packs.b2 = B2()

    def end(self, row):
        if not row['sealed']:
            return row['size']
        self.local_or_remote(row)
        frames = self.packs.frame_map(dict(stream='doc', source=self.source, seq=row['seq']))
        return frames[-1][0] + frames[-1][1] if frames else 0

    def entries(self, row, start=0):
        end = self.end(row)
        if start >= end:
            return
        self.local_or_remote(row)
        for pointer, header, body in self.packs.iter_range(row, start):
            if pointer['rec_off'] + pointer['rec_len'] > end:
                break
            doc = json.loads(body)
            if visible(doc):
                yield item(self.source, pointer, header, doc)

    def versions(self, after):
        for row in self.rows:
            if row['seq'] >= after[0]:
                yield from self.entries(row, after[1] if row['seq'] == after[0] else 0)

    def head(self):
        for row in reversed(self.rows):
            if not ceiling():
                end = self.end(row)
                if end:
                    return token(self.source, row['seq'], end)
            else:
                last = None
                for entry in self.entries(row):
                    last = entry['token']
                if last is not None:
                    return last
        return None

    def read(self, after=None, since=None, limit=500):
        if since is not None:
            epoch = self.state.get('feed_epoch')
            if epoch is None:
                raise ValueError('feed arrival epoch is not initialized')
            if since < epoch['at']:
                from datetime import datetime, timezone

                stamp = datetime.fromtimestamp(epoch['at'], timezone.utc).isoformat()
                raise ValueError(f'since-arrival precedes arrival epoch {stamp}')
            beginning = tuple(epoch['starts'].get(self.source, [0, 0]))
        else:
            beginning = position(after, self.source)
        head = self.head()
        items = []
        for entry in self.versions(beginning):
            if (
                since is not None
                and not items
                and (entry['arrived'] is None or entry['arrived'] < since)
            ):
                continue
            items.append(entry)
            if len(items) == limit:
                break
        if items:
            next_token = items[-1]['token']
        elif since is not None:
            next_token = head
        else:
            next_token = head if head is None or after >= head else after
        return dict(
            schema='qmd.feed.v1',
            source=self.source,
            items=items,
            next=next_token,
            head=head,
            complete=next_token == head,
        )
