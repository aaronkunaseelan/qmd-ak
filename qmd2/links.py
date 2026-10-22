"""Fathom share-page extractor (design.md 'Fathom share extraction', owner D10); other HTML uses
doc.html_text.
"""

import json
import hashlib
import re
import urllib.parse
from html.parser import HTMLParser

from .doc import html_text


def references(doc):
    from .attachments import normalized_url

    refs, seen = [], set()
    if doc['id'].startswith(('link:', 'manual:')) or '/att/' in doc['id']:
        return refs
    for part in doc.get('parts', []):
        if part['kind'] != 'authored':
            continue
        for value in re.findall(r'https?://[^\s<>\"\x27]+', part.get('text', '')):
            value = value.rstrip('.,;!?)')
            try:
                url = normalized_url(value)
            except ValueError:
                continue
            digest = hashlib.sha256(url.encode()).hexdigest()
            if digest in seen or url == doc.get('link'):
                continue
            seen.add(digest)
            refs.append(
                dict(
                    id=doc['id'] + '/att/link-' + digest,
                    capture_id='link:' + digest,
                    url=url,
                    name=urllib.parse.urlsplit(url).hostname,
                    type='text/uri-list',
                    size=0,
                )
            )
    return refs


class SharePage(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.payload = None

    def handle_starttag(self, tag, attrs):
        value = dict(attrs).get('data-page')
        if value:
            self.payload = json.loads(value)


def share_call(url, html):
    parsed = urllib.parse.urlsplit(url)
    if parsed.hostname == 'fathom.video' and parsed.path.startswith('/calls/'):
        from .attachments import Unavailable

        raise Unavailable('account_only_link')
    if parsed.hostname != 'fathom.video' or not re.fullmatch(r'/share/[\w-]+', parsed.path):
        return None
    page = SharePage()
    page.feed(html)
    props = (page.payload or {}).get('props', {})
    call = props.get('call')
    endpoint = props.get('copyTranscriptUrl') or (call or {}).get('copyTranscriptUrl')
    if not isinstance(call, dict) or not endpoint:
        raise ValueError('share transcript unavailable')
    endpoint = urllib.parse.urljoin(url, endpoint)
    target = urllib.parse.urlsplit(endpoint)
    if (
        target.scheme != 'https'
        or target.netloc != 'fathom.video'
        or target.path != f"/calls/{call['id']}/copy_transcript"
        or urllib.parse.parse_qs(target.query).get('token') != [parsed.path.rsplit('/', 1)[1]]
    ):
        raise ValueError('share transcript endpoint invalid')
    return call, props, endpoint


def meeting_text(call, props, transcript):
    def person(value):
        if isinstance(value, dict):
            return ' '.join(str(value[k]) for k in ('name', 'email') if value.get(k))
        return str(value)

    host = person(call.get('host', ''))
    attendees = call.get('attendees') or call.get('participants') or props.get('attendees') or []
    summary = call.get('summary') or props.get('summary')
    if isinstance(summary, dict):
        summary = summary.get('markdown') or summary.get('text') or summary.get('content')
    out = [
        f"Title: {call.get('title') or call.get('topic') or 'Shared meeting'}",
        f"Start: {call.get('started_at') or call.get('recording', {}).get('started_at', '')}",
        f'Host: {host}',
    ]
    if call.get('byline'):
        out.append('Byline: ' + str(call['byline']))
    if attendees:
        out.append('Attendees: ' + ', '.join(person(p) for p in attendees))
    if isinstance(summary, str) and summary.strip():
        out.append('Summary:\n' + (html_text(summary) if '<' in summary else summary))
    out.append('Transcript:\n' + transcript.strip())
    return '\n'.join(out)
