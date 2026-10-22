"""Gmail API raw envelopes become clean MIME documents; attachments remain refs."""

import base64
import binascii
import json
import re
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from html.parser import HTMLParser

from qmd2.doc import Exclusion
from qmd2.accounts import account

PARSER_VERSION = 6

OWNER = set(account('owner_addresses'))
HIDE = {
    name + '@' + account('owner_domain')
    for name in (
        'recruitment',
        'volunteers',
        'toggl',
        'toggle',
        'holidays',
        'interviews',
        'travels',
        'donations',
        'supporters',
        'registration',
    )
}
KEEP = {
    name + '@' + account('owner_domain')
    for name in ('core', 'star', 'grants', 'admin', 'procurements', 'inloco', 'coord.inloco')
}


def shared_alias(message):
    addresses = {}
    for header in ('From', 'To', 'Cc'):
        addresses[header] = {
            address.lower()
            for _, address in getaddresses([str(v) for v in message.get_all(header, [])])
        }
    for address in set.union(*addresses.values()):
        local, sep, domain = address.rpartition('@')
        if sep and local.split('+', 1)[0] + '@' + domain in OWNER:
            return False
    recipients = addresses['To'] | addresses['Cc']
    return bool(recipients.intersection(HIDE)) and not recipients.intersection(KEEP)


class BodyHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.chunks = []
        self.stack = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        attrs = {name: value or '' for name, value in attrs}
        quoted = tag == 'blockquote' or any(
            x in attrs.get('class', '').split() for x in ('gmail_quote', 'yahoo_quoted')
        )
        self.stack.append((tag, quoted, attrs.get('href') if tag == 'a' else None))
        if tag in ('script', 'style', 'head'):
            self.hidden += 1
        if tag in ('br', 'p', 'div', 'li', 'blockquote'):
            self.add('\n')
        if tag in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6'):
            self.add('\n' + '#' * int(tag[1]) + ' ')
        if tag in ('br', 'img', 'hr', 'meta', 'link', 'input'):
            self.stack.pop()

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i][0] == tag:
                _, _, link = self.stack[i]
                if link:
                    self.add(' <' + link + '>')
                if tag in ('p', 'div', 'li', 'blockquote') or re.fullmatch('h[1-6]', tag):
                    self.add('\n')
                del self.stack[i:]
                break
        if tag in ('script', 'style', 'head'):
            self.hidden = max(0, self.hidden - 1)

    def add(self, text):
        if not self.hidden:
            self.chunks.append(('quoted' if any(x[1] for x in self.stack) else 'authored', text))

    def handle_data(self, data):
        self.add(data)


def parts(chunks):
    result = []
    for kind, text in chunks:
        if result and result[-1]['kind'] == kind:
            result[-1]['text'] += text
        else:
            result.append({'kind': kind, 'text': text})
    return [dict(p, text=p['text'].strip()) for p in result if p['text'].strip()]


def plain(text):
    chunks = []
    history = False
    signature = False
    for line in text.splitlines(keepends=True):
        if re.match(r'^\s*(On .+wrote:|-----Original Message-----)', line):
            history = True
        if line.rstrip('\r\n') == '-- ' and not history:
            signature = True
        kind = (
            'quoted'
            if history or line.lstrip().startswith('>')
            else 'boilerplate'
            if signature
            else 'authored'
        )
        chunks.append((kind, line))
    return parts(chunks)


def parse(raw, ctx):
    account = ctx.get('account', '')
    key = ctx.get('key', '').rsplit(':', 1)[-1]
    identifier = f'gmail:{account}:{key}'
    doc = {
        'id': identifier,
        'item': identifier,
        'source': 'gmail',
        'group': 'mail',
        'kind': 'email',
        'time': ctx.get('time'),
        'parts': [],
        'attachments': [],
        'parser': f'gmail@{PARSER_VERSION}',
    }
    try:
        row = json.loads(raw)
        return _envelope(doc, row, ctx, account)
    except (
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        LookupError,
        binascii.Error,
        OverflowError,
    ):
        doc.update(
            parse_failed=True, parts=[{'kind': 'binary', 'text': 'Unreadable Gmail envelope'}]
        )
    return [doc]


def _envelope(doc, row, ctx, account):
    marked = isinstance(row, dict) and row.get('metadata_only') is True
    if isinstance(row, dict) and isinstance(row.get('message'), dict):
        row = row['message']
    if not isinstance(row, dict) or not isinstance(row.get('id'), str) or not row['id']:
        raise ValueError()
    identifier = f"gmail:{account}:{row['id']}"
    doc.update(id=identifier, item=identifier)
    if row.get('state') == 'deleted' or ctx.get('state') == 'deleted':
        doc['kind'] = 'tombstone'
        return [doc]
    doc.update(thread=row.get('threadId'), where=row.get('labelIds', []))
    if 'raw' not in row:
        if not marked:
            return []
        doc['parts'] = [{'kind': 'boilerplate', 'text': 'No text supplied by Gmail record'}]
        return [doc]
    encoded = row['raw']
    mime = base64.b64decode(encoded + '=' * (-len(encoded) % 4), altchars=b'-_', validate=True)
    message = BytesParser(policy=policy.default).parsebytes(mime)
    if not message.keys():
        raise ValueError()
    for header, field in [
        ('Subject', 'title'),
        ('From', 'from'),
        ('To', 'to'),
        ('Cc', 'cc'),
        ('Reply-To', 'reply_to'),
    ]:
        if message.get(header):
            doc[field] = str(message[header])
    if shared_alias(message):
        return [Exclusion('shared_alias', identifier)]
    if row.get('internalDate'):
        doc['time'] = datetime.fromtimestamp(
            int(row['internalDate']) / 1000, timezone.utc
        ).isoformat()
    elif message.get('Date'):
        doc['time'] = parsedate_to_datetime(str(message['Date'])).isoformat()
    _mime_parts(doc, message, identifier)
    return [doc]


def _mime_parts(doc, message, identifier):
    plain_bodies, html_bodies = [], []
    for position, part in enumerate(message.walk()):
        if part.is_multipart():
            continue
        body = part.get_payload(decode=True) or b''
        if (
            part.get_filename()
            or part.get_content_disposition() == 'attachment'
            or part.get_content_maintype() != 'text'
        ):
            doc['attachments'].append(
                {
                    'id': f'{identifier}/att/{position}',
                    'name': part.get_filename() or '',
                    'type': part.get_content_type(),
                    'size': len(body),
                }
            )
            continue
        charset = part.get_content_charset() or 'utf-8'
        try:
            text = body.decode(charset, errors='replace')
        except LookupError:
            try:
                text = body.decode(re.sub(r'^windows-(\d+)$', r'cp\1', charset), errors='replace')
            except LookupError:
                text = body.decode('utf-8', errors='replace')
        if part.get_content_type() == 'text/plain':
            plain_bodies.extend(plain(text))
        elif part.get_content_type() == 'text/html':
            html = BodyHTML()
            html.feed(text)
            html_bodies.extend(parts(html.chunks))
    doc['parts'] = (
        plain_bodies
        or html_bodies
        or [{'kind': 'boilerplate', 'text': 'No text supplied by Gmail record'}]
    )
