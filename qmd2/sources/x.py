"""Posts and saves share native tweet identity; counters never enter documents."""

import json
import re
from datetime import datetime, timezone
from html import unescape

PARSER_VERSION = 4


def parse(raw, ctx):
    identifier = 'x:' + ctx.get('account', '') + ':' + ctx.get('key', '').rsplit(':', 1)[-1]
    doc = dict(
        id=identifier,
        item=identifier,
        source='x',
        group='articles',
        kind='post',
        time=None,
        parts=[],
        parser=f'x@{PARSER_VERSION}',
    )
    try:
        row = json.loads(raw)
        _post(doc, row, ctx)
    except (ValueError, KeyError, TypeError, AttributeError):
        doc.update(parse_failed=True, parts=[{'kind': 'binary', 'text': 'Unreadable X post'}])
    return [doc]


def expand(text, body):
    provider = body.get('_raw') or body
    legacy = body.get('legacy') or provider.get('legacy') or {}
    note = (provider.get('note_tweet') or {}).get('note_tweet_results', {}).get('result') or {}
    sources = (
        legacy.get('entities') or {},
        body.get('entities') or {},
        note.get('entity_set') or {},
    )
    replacements = {}
    for source in sources:
        for link in source.get('urls', []):
            target = link.get('expanded_url') or link.get('expandedUrl')
            if target and link.get('url'):
                replacements[link['url']] = (
                    (link.get('display_url') or target) + ' <' + target + '>'
                )
    for source in sources + (
        legacy.get('extended_entities') or {},
        body.get('extended_entities') or {},
    ):
        for media in source.get('media', []):
            label = {'photo': '[photo]', 'video': '[video]', 'animated_gif': '[video]'}.get(
                media.get('type')
            )
            if label and media.get('url'):
                replacements[media['url']] = label
    for token, replacement in replacements.items():
        text = text.replace(token, replacement)
    return text


def _post(doc, row, ctx):
    saved = 'post'
    for wrapper in ('tweet', 'bookmark', 'like'):
        if wrapper in row:
            saved = wrapper
            row = row[wrapper]
            break
    if isinstance(row.get('tweet'), dict):
        row = row['tweet']
    viewer = (row.get('_raw') or {}).get('legacy') or {}
    if row.get('bookmarked', viewer.get('bookmarked')) or saved == 'bookmark':
        doc['kind'] = 'bookmark'
    elif row.get('favorited', viewer.get('favorited')) or saved == 'like':
        doc['kind'] = 'like'
    native = str(row.get('id') or row.get('id_str') or row['tweetId'])
    identifier = 'x:' + ctx['account'] + ':' + native
    doc.update(id=identifier, item=identifier, link='https://x.com/i/status/' + native)
    provider = row.get('_raw') or {}
    legacy = row.get('legacy') or provider.get('legacy') or {}
    text = (
        row.get('text')
        or row.get('fullText')
        or row.get('full_text')
        or legacy.get('full_text', '')
    )
    entities = row.get('entities') or legacy.get('entities') or {}
    text = expand(text, row)
    author = row.get('author') or row.get('user') or {}
    doc['from'] = author.get('username') or author.get('screen_name') or author.get('name', '')
    stamp = row.get('createdAt') or row.get('created_at') or legacy.get('created_at')
    if stamp:
        try:
            stamp = (
                datetime.strptime(stamp, '%a %b %d %H:%M:%S %z %Y')
                .astimezone(timezone.utc)
                .isoformat()
            )
        except ValueError:
            pass
    doc['time'] = stamp
    edited = row.get('editedAt') or row.get('edited_at')
    if edited:
        doc['edited'] = edited
    _post_parts(doc, row, legacy, entities, text, identifier)
    doc['context'] = {
        'unexpanded_links': sum(
            len(re.findall(r'https?://t\.co/[^\s<>]+', part['text']))
            for part in doc['parts']
            if part['kind'] == 'authored'
        )
    }


def _post_parts(doc, row, legacy, entities, text, identifier):
    doc['parts'] = [{'kind': 'authored', 'text': unescape(text)}]
    provider = row.get('_raw') or {}
    quoted = (
        row.get('quotedTweet')
        or row.get('quoted_status')
        or (row.get('quoted_status_result') or provider.get('quoted_status_result') or {}).get(
            'result'
        )
    )
    if quoted:
        quoted_legacy = quoted.get('legacy') or (quoted.get('_raw') or {}).get('legacy') or {}
        quoted_text = (
            quoted.get('text')
            or quoted.get('fullText')
            or quoted.get('full_text')
            or quoted_legacy.get('full_text', '')
        )
        doc['parts'].append(
            {'kind': 'authored', 'text': 'Quoted post: ' + unescape(expand(quoted_text, quoted))}
        )
    media = row.get('media') or (
        row.get('extended_entities') or legacy.get('extended_entities') or entities
    ).get('media', [])
    attachments = []
    for pos, photo in enumerate(media):
        if photo.get('type') != 'photo':
            continue
        attachments.append(
            {
                'id': identifier + '/att/' + str(pos),
                'name': photo.get('url') or photo.get('media_url_https', ''),
                'type': 'image',
                'size': 0,
            }
        )
        alt = photo.get('altText') or photo.get('ext_alt_text')
        if alt:
            doc['parts'].append({'kind': 'authored', 'text': unescape(alt)})
    if attachments:
        doc['attachments'] = attachments
