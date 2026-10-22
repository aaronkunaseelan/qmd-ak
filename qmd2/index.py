"""K5 search store: one current version per id; the newest version wins within a pending batch."""

import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import tantivy

from .doc import ceiling, date, dumps, newer, strict_date
from .policy import KIND_WEIGHT, text


def schema():
    b = tantivy.SchemaBuilder()
    for name in ('id', 'item', 'source', 'group', 'kind'):
        b.add_text_field(name, stored=True, tokenizer_name='raw')
    b.add_date_field('time', stored=True, fast=True)
    for name in ('title', 'from', 'context', 'body'):
        b.add_text_field(name, stored=True)
    for name in ('pointer', 'version'):
        b.add_text_field(name, stored=True, index_option='basic', tokenizer_name='raw')
    return b.build()


class Index:
    def __init__(self, home, writable=False, state=None, path=None):
        self.path = Path(path) if path else Path(home) / 'index'
        self.state = state
        if writable:
            self.path.mkdir(parents=True, exist_ok=True)
            self.index = tantivy.Index(schema(), path=str(self.path))
            self.writer = self.index.writer(heap_size=128_000_000, num_threads=1)
        else:
            self.index = tantivy.Index.open(str(self.path))
            self.writer = None
        self.schema = self.index.schema
        self.pending = {}
        self.index.reload()
        self.searcher = self.index.searcher()

    def term(self, field, value):
        return tantivy.Query.term_query(self.schema, field, value, index_option='basic')

    def row(self, address):
        values = self.searcher.doc(address).to_dict()
        row = {k: v[0] for k, v in values.items() if v}
        for k in ('pointer', 'version'):
            row[k] = json.loads(row[k])
        if isinstance(row.get('time'), datetime):
            row['time'] = row['time'].replace(tzinfo=timezone.utc).isoformat()
        return row

    def lookup(self, id):
        hits = self.searcher.search(self.term('id', id), limit=1).hits
        return self.row(hits[0][1]) if hits else None

    def upsert(self, doc, pointer, refresh=False):
        if doc['source'] == 'files' and doc['kind'] == 'tombstone':
            return False
        id = doc['id']
        if id in self.pending:
            old = self.pending[id]
        else:
            row = self.lookup(id)
            old = (
                row['version'] if row else (self.state.get('deleted:' + id) if self.state else None)
            )
        enriched = (
            old
            and 'outcome' in doc
            and not old.get('attachment')
            and doc['version']['hash'] == old['hash']
            and doc['version'].get('revision') == old.get('revision')
        )
        if not newer(doc['version'], old) and not enriched:
            if not (
                refresh
                and id not in self.pending
                and row
                and doc['version']['hash'] == old['hash']
                and doc['version'].get('raw') == old.get('raw')
            ):
                return False
            # Policy refresh preserves the chosen K2 version and its doc-pack pointer.
            doc = dict(doc, version=old)
            pointer = row['pointer']
        if 'outcome' in doc:
            siblings = {r['id']: r['version'] for r in self.item(doc['item'])}
            siblings.update(
                (key, value)
                for key, value in self.pending.items()
                if value.get('attachment', {}).get('id') == doc['item']
            )
            for key, value in siblings.items():
                if key != id and not newer(doc['version'], value):
                    return False
            for key in siblings:
                if key != id:
                    self.writer.delete_documents_by_term('id', key)
                    self.pending.pop(key, None)
        self.writer.delete_documents_by_term('id', id)
        self.pending[id] = doc['version']
        if doc['kind'] == 'tombstone':
            if self.state:
                self.state.set('deleted:' + id, doc['version'])
            return True
        if self.state:
            self.state.db.execute('DELETE FROM checkpoints WHERE key=?', ('deleted:' + id,))
        self.write_document(doc, pointer)
        return True

    def write_document(self, doc, pointer):
        context = doc.get('context', {})
        context = (
            ' '.join(str(v) for v in context.values() if v)
            if isinstance(context, dict)
            else str(context)
        )
        values = {
            'id': doc['id'],
            'item': doc['item'],
            'source': doc['source'],
            'group': doc['group'],
            'kind': doc['kind'],
            'time': date(doc.get('time')).replace(tzinfo=None),
            'title': doc.get('title', ''),
            'from': str(doc.get('from', '')),
            'context': context,
            'body': text(doc),
            'pointer': dumps(pointer),
            'version': dumps(doc['version']),
        }
        self.writer.add_document(tantivy.Document.from_dict(values, self.schema))

    def commit(self):
        self.writer.commit()
        self.index.reload()
        self.searcher = self.index.searcher()
        self.pending.clear()

    def filters(
        self, query, source=None, since=None, until=None, exclude=True, include_outcomes=False
    ):
        clauses = [(tantivy.Occur.Must, query)]
        if not include_outcomes:
            clauses.append((tantivy.Occur.MustNot, self.term('kind', 'attachment_outcome')))
        if source:
            sources = source if isinstance(source, list) else [source]
            clauses.append(
                (
                    tantivy.Occur.Must,
                    tantivy.Query.boolean_query(
                        [
                            (
                                tantivy.Occur.Should,
                                self.term(
                                    'group'
                                    if s in ('mail', 'articles', 'files', 'sessions', 'meetings')
                                    else 'source',
                                    s,
                                ),
                            )
                            for s in sources
                        ]
                    ),
                )
            )
        until = ceiling(until)
        if since or until:
            clauses.append(
                (
                    tantivy.Occur.Must,
                    tantivy.Query.range_query(
                        self.schema,
                        'time',
                        tantivy.FieldType.Date,
                        strict_date(since).replace(tzinfo=None) if since else None,
                        strict_date(until).replace(tzinfo=None) if until else None,
                    ),
                )
            )
        if exclude:
            for var, runtime in (
                ('PI_SESSION_ID', 'pi'),
                ('CLAUDE_SESSION_ID', 'claude'),
                ('CLAUDE_CODE_SESSION_ID', 'claude'),
                ('CODEX_THREAD_ID', 'codex'),
            ):
                if os.environ.get(var):
                    value = os.environ[var]
                    item = value if value.startswith(runtime + ':') else runtime + ':' + value
                    clauses.append((tantivy.Occur.MustNot, self.term('item', item)))
        return tantivy.Query.boolean_query(clauses)

    def scan(self, source=None, since=None, until=None, query=None):
        query = self.filters(
            query or tantivy.Query.all_query(),
            source,
            since,
            until,
            exclude=False,
            include_outcomes=True,
        )
        count = self.searcher.search(query, limit=1).count
        for offset in range(0, count, 500):
            for _, address in self.searcher.search(
                query, limit=500, offset=offset, count=False
            ).hits:
                yield self.row(address)

    def search(self, query, source=None, since=None, limit=10, until=None):
        words = re.findall(r'\w+', query)
        if not words:
            return []
        escaped = ' '.join('"' + word + '"' for word in words)
        result, seen, present = [], set(), {}
        for conjunction in (True, False):
            parsed = self.index.parse_query(
                escaped, ['title', 'context', 'body'], conjunction_by_default=conjunction
            )
            parsed = self.filters(parsed, source, since, until)
            grouped = {}
            for page in range(3):
                hits = self.searcher.search(parsed, limit=200, offset=page * 200, count=False).hits
                for score, addr in hits:
                    row = self.row(addr)
                    identity = (
                        ('files', row['version']['raw_sha256'])
                        if row['source'] == 'files' and row['version'].get('raw_sha256')
                        else row['item']
                    )
                    if identity in seen:
                        continue
                    rank_row(row, score, identity, present)
                    old = grouped.get(identity)
                    if not old or (row.get('gone', False), -row['score'], row['id']) < (
                        old.get('gone', False),
                        -old['score'],
                        old['id'],
                    ):
                        grouped[identity] = row
                    else:
                        row, old = old, row
                    if old and row['source'] == 'files':
                        row['also_at'] = sorted(
                            set(
                                row['also_at']
                                + old['also_at']
                                + [old['id'].split(':', 1)[1].replace(':', '/', 1)]
                            )
                        )
                if len(grouped) >= limit - len(result) or len(hits) < 200:
                    break
            rows = sorted(
                grouped.values(), key=lambda x: (x.get('gone', False), -x['score'], x['id'])
            )
            for row in rows:
                row['snippet'] = snippet(row['body'], words) if row['body'] else row['title']
                row.pop('body', None)
                result.append(row)
                seen.add(row.pop('_identity'))
                if len(result) >= limit:
                    return sorted(result, key=lambda r: r.get('gone', False))
        return sorted(result, key=lambda r: r.get('gone', False))

    def bindings(self, source):
        query = self.term('source', source)
        count = self.searcher.search(query, limit=1).count
        for off in range(0, count, 500):
            for _, addr in self.searcher.search(query, limit=500, offset=off, count=False).hits:
                doc = self.searcher.doc(addr)
                yield doc.get_first('id'), json.loads(doc.get_first('version'))['raw']

    def item(self, id):
        query = self.term('item', id)
        count = self.searcher.search(query, limit=1).count
        rows = [self.row(a) for _, a in self.searcher.search(query, limit=max(1, count)).hits]
        return sorted(
            rows, key=lambda r: (date(r['time']), r['pointer']['seq'], r['pointer']['rec_off'])
        )

    def browse(self, source=None, since=None, until=None, limit=10, cursor=0):
        query = self.filters(tantivy.Query.all_query(), source, since, until)
        hits = self.searcher.search(query, limit=limit, offset=cursor, order_by_field='time').hits
        rows = [self.row(a) for _, a in hits]
        for row in rows:
            row['snippet'] = row.pop('body')[:240] or row['title']
        return rows


def rank_row(row, score, identity, present):
    age = max(0, (datetime.now(timezone.utc) - date(row['time'])).total_seconds() / 86400)
    recency = 0.5 + 0.5 * math.exp(-age / 365)
    row['score'] = score * recency * KIND_WEIGHT.get(row['kind'], 1)
    row['_identity'] = identity
    if row['source'] == 'files':
        _, root, relative = row['id'].split(':', 2)
        where = root + '/' + relative
        if where not in present:
            try:
                from .accounts import file_roots

                present[where] = (dict(file_roots())[root] / relative).exists()
            except OSError:
                present[where] = True
        row['gone'], row['also_at'] = not present[where], []


def snippet(body, words):
    lower = body.lower()
    positions = [lower.find(w.lower()) for w in words]
    pos = min((p for p in positions if p >= 0), default=0)
    start = max(0, pos - 70)
    return (
        ('…' if start else '')
        + body[start : start + 300]
        + ('…' if start + 300 < len(body) else '')
    )
