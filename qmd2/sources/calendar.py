"""Calendar K2 parsing for read-only capture every 900 seconds (D-168)."""

import json
from datetime import datetime, timezone
from html.parser import HTMLParser
from zoneinfo import ZoneInfo

PARSER_VERSION = 2


class Description(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.text = []
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag in ('br', 'p', 'div', 'li'):
            self.text.append('\n')
        if tag == 'a':
            self.links.append(dict(attrs).get('href', ''))

    def handle_endtag(self, tag):
        if tag == 'a' and self.links:
            self.text.append(' <' + self.links.pop() + '>')
        if tag in ('p', 'div', 'li'):
            self.text.append('\n')

    def handle_data(self, data):
        self.text.append(data)


def description(value):
    parser = Description()
    parser.feed(value)
    return ''.join(parser.text).strip()


def event_time(value, default=None):
    if not value.get('dateTime'):
        return value.get('date', default)
    stamp = datetime.fromisoformat(value['dateTime'].replace('Z', '+00:00'))
    if stamp.tzinfo is None and value.get('timeZone'):
        stamp = stamp.replace(tzinfo=ZoneInfo(value['timeZone']))
    if stamp.tzinfo is not None:
        return stamp.astimezone(timezone.utc).isoformat()
    return stamp.isoformat()


def parse(raw, ctx):
    key = ctx.get('key', '')
    calendar = ctx.get('calendar_id') or key.rpartition(':')[0]
    identifier = 'calendar:' + calendar + ':' + key.rsplit(':', 1)[-1]
    doc = {
        'id': identifier,
        'item': identifier,
        'source': 'calendar',
        'group': 'mail',
        'kind': 'event',
        'time': ctx.get('time'),
        'parts': [],
        'parser': f'calendar@{PARSER_VERSION}',
    }
    try:
        row = json.loads(raw)
        if not calendar:
            raise ValueError('calendar id missing')
        identifier = 'calendar:' + calendar + ':' + row['id']
        doc.update(
            id=identifier,
            item=identifier,
            title=row.get('summary', ''),
            time=event_time(row.get('start', {}), ctx.get('time')),
            where=row.get('location', ''),
            link=row.get('htmlLink', ''),
            context={'end': event_time(row.get('end', {}))},
            **{
                'from': row.get('organizer', {}).get('email', ''),
                'to': [a.get('email', '') for a in row.get('attendees', [])],
            },
        )
        if row.get('updated'):
            doc['edited'] = row['updated']
        if row.get('status') == 'cancelled' or ctx.get('state') == 'deleted':
            doc['kind'] = 'tombstone'
        else:
            doc['parts'] = [{'kind': 'authored', 'text': description(row.get('description', ''))}]
    except (ValueError, KeyError, TypeError, AttributeError):
        doc.update(
            parse_failed=True, parts=[{'kind': 'binary', 'text': 'Unreadable calendar event'}]
        )
    return [doc]
