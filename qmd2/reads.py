"""Pack-backed read views; a replay ceiling never reads a later document."""

import hashlib
import json
import re

import tantivy
from pathlib import Path

from .doc import date, render, visible
from .index import Index
from .packs import Packs
from .state import State


def read_identifier(identifier):
    if identifier.lower().startswith(('http://', 'https://')):
        from .attachments import normalized_url

        return 'link:' + hashlib.sha256(normalized_url(identifier).encode()).hexdigest()
    return identifier


class Reader:
    def __init__(self, home, cold=False):
        self.home, self.cold = Path(home), cold
        self._index = None
        self._state, self._packs = None, None

    @property
    def index(self):
        if self._index is None:
            self._index = Index(self.home)
        return self._index

    @property
    def state(self):
        if self._state is None:
            self._state = State(self.home)
        return self._state

    @property
    def packs(self):
        if self._packs is None:
            self._packs = Packs(self.state)
            if self.cold:
                from .b2 import B2

                self._packs.b2 = B2()
        return self._packs

    def close(self):
        if self._state is not None:
            self._state.db.close()

    def read(self, row):
        if not visible(row):
            raise LookupError()
        try:
            _, body = self.packs.read(row['pointer'], cold=self.cold)
        except FileNotFoundError:
            from .b2 import B2

            self.packs.b2 = B2()
            _, body = self.packs.read(row['pointer'], cold=self.cold)
        doc = json.loads(body)
        if not visible(doc):
            raise LookupError()
        from .links import references

        refs = references(doc)
        if refs:
            existing = doc.get('attachments', [])
            doc = dict(
                doc,
                attachments=existing
                + [a for a in refs if a['id'] not in {x['id'] for x in existing}],
            )
        return doc

    def get(self, identifier):
        identifier = read_identifier(identifier)
        if '/att/link-' in identifier:
            parent_id = identifier.rsplit('/att/', 1)[0]
            parent = self.get(parent_id)[0]
            ref = next((a for a in parent.get('attachments', []) if a['id'] == identifier), None)
            if not ref:
                raise LookupError()
            return self.get(ref['capture_id'])
        row = self.index.lookup(identifier)
        rows = [row] if row else self.index.item(identifier)
        docs = [self.read(r) for r in rows if visible(r)]
        if not docs:
            raise LookupError()
        return docs

    def scan(self, source=None, since=None, until=None, query=None):
        for row in self.index.scan(source, since, until, query):
            yield self.read(row)

    def source_docs(self, source):
        # Whole-source reads stream each pack once, avoiding repeated frame decompression.
        current = {}
        for row in self.index.scan(source):
            pointer = row['pointer']
            current.setdefault((pointer['source'], pointer['seq']), {})[pointer['rec_off']] = row[
                'id'
            ]
        for pack in self.state.packs('doc'):
            key = (pack['source'], pack['seq'])
            if key not in current:
                continue
            path = self.packs.path('doc', *key, bool(pack['sealed']))
            if not path.exists():
                from .b2 import B2

                self.packs.b2 = B2()
            for loc, _, body in self.packs.iter_pack(pack):
                if loc['rec_off'] in current[key]:
                    doc = json.loads(body)
                    if visible(doc):
                        yield doc

    def thread(self, identifier, around):
        asked = self.get(identifier)[0]
        source = asked['source']
        if source == 'gmail':
            account = identifier.rsplit(':', 1)[0] + ':'
            related = lambda d: (
                d['id'].startswith(account) and d.get('thread') == asked.get('thread')
            )
            candidates = self.source_docs(source)
            if not asked.get('thread'):
                return [asked]
        elif source == 'whatsapp':
            return self.chat(asked, around)
        else:
            raise ValueError('thread requires Gmail or WhatsApp')
        return sorted(
            (d for d in candidates if related(d) and d['kind'] == asked['kind']),
            key=lambda d: (date(d['time']), d['id']),
        )

    def chat(self, asked, around):
        if around == 0:
            return [asked]
        chat = asked['id'].rsplit(':', 1)[0] + ':'
        query = tantivy.Query.boolean_query(
            [
                (
                    tantivy.Occur.Must,
                    tantivy.Query.regex_query(self.index.schema, 'id', re.escape(chat) + '.*'),
                ),
                (tantivy.Occur.Must, self.index.term('kind', asked['kind'])),
            ]
        )
        clock = date(asked['time']).replace(tzinfo=None)
        rows, ties = {}, {asked['time']}
        for before in (True, False):
            window = tantivy.Query.range_query(
                self.index.schema,
                'time',
                tantivy.FieldType.Date,
                None if before else clock,
                clock if before else None,
                include_lower=before,
                include_upper=not before,
            )
            bounded = tantivy.Query.boolean_query(
                [(tantivy.Occur.Must, query), (tantivy.Occur.Must, window)]
            )
            bounded = self.index.filters(bounded, 'whatsapp', exclude=False)
            hits = self.index.searcher.search(
                bounded,
                limit=around,
                count=False,
                order_by_field='time',
                order=tantivy.Order.Desc if before else tantivy.Order.Asc,
            ).hits
            side = [self.index.row(address) for _, address in hits]
            rows.update((r['id'], r) for r in side)
            if side:
                ties.add(side[-1]['time'])
        # Complete boundary timestamps before the id tie-break; only the final window reads packs.
        for clock in ties:
            rows.update((r['id'], r) for r in self.index.scan('whatsapp', clock, clock, query))
        ordered = sorted(rows.values(), key=lambda r: (date(r['time']), r['id']))
        position = next(i for i, row in enumerate(ordered) if row['id'] == asked['id'])
        return [self.read(r) for r in ordered[max(0, position - around) : position + around + 1]]

    def grep(self, pattern, since, until, sources, limit):
        count = 0
        for doc in self.scan(sources, since, until):
            for line in '\n'.join(p.get('text', '') for p in doc.get('parts', [])).splitlines():
                if pattern.search(line):
                    yield dict(
                        id=doc['id'],
                        time=doc['time'],
                        sender=doc.get('from', ''),
                        chat=doc.get('context', {}).get('chat', doc.get('to', '')),
                        line=line,
                    )
                    count += 1
                    if count >= limit:
                        return

    def attachment_head(self, attachment, lines):
        state = 'unmaterialised'
        try:
            docs = self.get(attachment['id'])
            state = docs[0].get('state', 'done')
        except LookupError:
            docs = []
        calendar = (
            attachment.get('name', '').lower().endswith('.ics')
            or attachment.get('type') == 'text/calendar'
        )
        if calendar and not docs:
            docs = self.calendar_attachment(attachment)
        out = [
            f"  {attachment['id']} ({attachment.get('name', '')}, {attachment.get('type', '')}, {state})"
        ]
        if attachment.get('type', '').startswith('image/'):
            return out
        text = '\n'.join(
            p.get('text', '') for d in docs for p in d.get('parts', []) if p['kind'] != 'header'
        )
        if docs and calendar:
            out.append('    ' + calendar_line(text))
        elif text:
            out.extend('    ' + line for line in text.splitlines()[:lines])
        return out

    def calendar_attachment(self, attachment):
        parent_id, _, position = attachment['id'].rpartition('/att/')
        parent = self.get(parent_id)[0]
        if parent['source'] == 'gmail':
            import base64
            from email import policy
            from email.parser import BytesParser
            from .attachments import raw_parent

            raw = json.loads(raw_parent(self.home, parent))
            encoded = raw['raw']
            message = BytesParser(policy=policy.default).parsebytes(
                base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4))
            )
            part = list(message.walk())[int(position)]
            return [
                dict(
                    parts=[
                        dict(
                            kind='attachment_text',
                            text=(part.get_payload(decode=True) or b'').decode(
                                part.get_content_charset() or 'utf-8', errors='replace'
                            ),
                        )
                    ]
                )
            ]
        return []

    def view(self, doc, no_quoted=False, heads=None):
        return render(
            doc,
            no_quoted,
            (lambda a: self.attachment_head(a, heads)) if heads is not None else None,
        )


def calendar_line(text):
    """SUMMARY, DTSTART, DTEND and LOCATION from the first VEVENT, so a VTIMEZONE DTSTART is
    ignored; with no VEVENT, the whole text is read.
    """
    unfolded = re.sub(r'\r?\n[ \t]', '', text)
    lines = unfolded.splitlines()
    begin = next(
        (i for i, line in enumerate(lines) if line.strip().upper() == 'BEGIN:VEVENT'), None
    )
    if begin is not None:
        end = next(
            (i for i in range(begin, len(lines)) if lines[i].strip().upper() == 'END:VEVENT'),
            len(lines),
        )
        lines = lines[begin + 1 : end]
    values = {}
    for line in lines:
        key, _, value = line.partition(':')
        name, *params = key.split(';')
        name = name.upper()
        if name in ('SUMMARY', 'DTSTART', 'DTEND', 'LOCATION'):
            value = value.replace('\\n', ' ').replace('\\,', ',').replace('\\;', ';')
            tzid = next((p.partition('=')[2] for p in params if p.upper().startswith('TZID=')), '')
            values.setdefault(name, value + (f' ({tzid})' if tzid else ''))
    return '; '.join(
        label + ': ' + values.get(key, '?')
        for key, label in [
            ('SUMMARY', 'summary'),
            ('DTSTART', 'start'),
            ('DTEND', 'end'),
            ('LOCATION', 'location'),
        ]
    )
