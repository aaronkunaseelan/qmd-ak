"""K2 docs: written once, ordered by source clock, rendered whole."""

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser


@dataclass(frozen=True)
class Exclusion:
    reason: str
    item: str | None = None


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def strict_date(value):
    if isinstance(value, (float, int)):
        return datetime.fromtimestamp(value / 1000 if value > 1e11 else value, timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def date(value):
    try:
        return strict_date(value)
    except (ValueError, TypeError, OverflowError):
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def ceiling(until=None):
    value = os.environ.get('QMD_UNTIL')
    if not value:
        return until
    value = strict_date(value).isoformat()
    return min((value, until), key=strict_date) if until else value


def visible(doc):
    until = ceiling()
    return not until or date(doc.get('time')) <= date(until)


def version(doc, capture=0, epoch=0):
    content = {k: v for k, v in doc.items() if k not in ('raw', 'version', 'outcome')}
    return {
        'time': (
            None
            if doc.get('kind') == 'tombstone' and not doc.get('edited', doc.get('time'))
            else date(doc.get('edited', doc.get('time'))).timestamp()
        ),
        'epoch': epoch,
        'capture': capture,
        'hash': hashlib.sha256(dumps(content).encode()).hexdigest(),
        'parser': doc.get('parser', ''),
        'raw': doc.get('raw'),
        'raw_sha256': doc.get('raw_sha256'),
        'revision': doc.get('pipeline_revision'),
        **({'attachment': doc['outcome']} if 'outcome' in doc else {}),
        'deleted': doc.get('kind') == 'tombstone',
    }


def newer(candidate, current):
    if current is None:
        return True
    if candidate.get('epoch', 0) != current.get('epoch', 0):
        return candidate.get('epoch', 0) > current.get('epoch', 0)
    if candidate.get('revision') is not None:
        if candidate['revision'] != (current.get('revision') or 0):
            return candidate['revision'] > (current.get('revision') or 0)
        # An explicit retry replaces its named no-text result, not a later result.
        return (
            current.get('attachment', {}).get('state') in ('failed', 'unavailable')
            and candidate.get('attachment', {}).get('retry_of') == current['hash']
        )
    if (
        candidate.get('raw') is not None
        and candidate.get('raw') == current.get('raw')
        and candidate.get('parser') != current.get('parser')
    ):

        def rev(v):
            try:
                return int(v.rsplit('@', 1)[1])
            except (ValueError, IndexError):
                return 0

        return rev(candidate.get('parser', '')) > rev(current.get('parser', ''))
    # A clockless deletion is a capture-order fence, including a later restore.
    # Zero also recognises clockless tombstones written before unknown stayed null.
    if any(v.get('deleted') and not v.get('time') for v in (candidate, current)):
        left, right = candidate.get('raw'), current.get('raw')
        if left and right and left.get('source') == right.get('source'):
            return (left['seq'], left['rec_off']) > (right['seq'], right['rec_off'])
        return candidate['capture'] > current['capture']
    if candidate['time'] != current['time']:
        return candidate['time'] > current['time']
    if candidate['hash'] == current['hash']:
        return False
    return candidate['capture'] > current['capture']


def render(doc, no_quoted=False, attachments=None):
    if doc.get('kind') == 'attachment_outcome':
        return dumps(
            {k: doc.get(k) for k in ('id', 'state', 'reason', 'model', 'pipeline_revision', 'time')}
        )
    fields = [
        ('kind', 'Kind'),
        ('title', 'Title'),
        ('from', 'From'),
        ('to', 'To'),
        ('cc', 'Cc'),
        ('time', 'Sent'),
        ('edited', 'Edited'),
        ('thread', 'Thread'),
        ('reply_to', 'Reply-To'),
        ('where', 'Where'),
        ('link', 'Link'),
        ('attachments', 'Attachments'),
    ]
    out = []
    for key, label in fields:
        value = doc.get(key)
        if not value:
            continue
        if key == 'attachments':
            if attachments is not None:
                out.append('Attachments:')
                for attachment in value:
                    out.extend(attachments(attachment))
                continue
            value = '; '.join(
                f"{a['id']} ({a.get('name', '')}, {a.get('type', '')}, {a.get('size', 0)} bytes)"
                for a in value
            )
        elif isinstance(value, list):
            value = ', '.join(str(x) for x in value)
        out.append(f'{label}: {value}')
    out.append('---')
    quoted = [p for p in doc.get('parts', []) if p['kind'] == 'quoted'] if no_quoted else []
    if quoted:
        out.append(
            f"[quoted omitted: {sum(len(p.get('text', '').splitlines()) for p in quoted)} lines, {sum(len(p.get('text', '')) for p in quoted)} chars]"
        )
    for part in doc.get('parts', []):
        if no_quoted and part['kind'] == 'quoted':
            continue
        if part['kind'] != 'authored':
            out.append(f"[{part['kind']}{': ' + part['tool'] if part.get('tool') else ''}]")
        out.append(part.get('text', ''))
    return '\n'.join(out)


class _HTMLText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out, self.links, self.hidden = [], [], 0

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style'):
            self.hidden += 1
        if self.hidden:
            return
        if tag in ('p', 'div', 'br', 'li', 'blockquote'):
            self.out.append('\n')
        if tag in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6'):
            self.out.append('\n' + '#' * int(tag[1]) + ' ')
        if tag == 'a':
            self.links.append(dict(attrs).get('href', ''))

    def handle_endtag(self, tag):
        if tag in ('script', 'style'):
            self.hidden = max(0, self.hidden - 1)
            return
        if self.hidden:
            return
        if tag == 'a' and self.links:
            url = self.links.pop()
            if url:
                self.out.append(' <' + url + '>')
        if tag in ('p', 'div', 'li', 'blockquote', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'):
            self.out.append('\n')

    def handle_data(self, data):
        if not self.hidden:
            self.out.append(data)


def html_text(value):
    """C7 text: headings and links retained, entities decoded, script/style omitted."""
    parser = _HTMLText()
    parser.feed(value)
    parser.close()
    return '\n'.join(line.strip() for line in ''.join(parser.out).splitlines() if line.strip())
