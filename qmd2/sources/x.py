"""X saves render response fields and scoped author context from archived bytes."""

import json
import re
from datetime import datetime, timezone
from html import unescape

PARSER_VERSION = 6


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


def expand(text, body, fields=None):
    provider = body.get('_raw') or body
    legacy = body.get('legacy') or provider.get('legacy') or {}
    note = (provider.get('note_tweet') or {}).get('note_tweet_results', {}).get('result') or {}
    sources = (
        legacy.get('entities') or {},
        body.get('entities') or {},
        note.get('entity_set') or {},
    )
    text = note.get('text') or text
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
    fields = fields if fields is not None else []
    for label, value in (
        ('Conversation', body.get('conversationId') or legacy.get('conversation_id_str')),
        ('Reply to', body.get('inReplyToStatusId') or legacy.get('in_reply_to_status_id_str')),
    ):
        if value is not None:
            fields.append(f'{label}: {value}')
    card = (provider.get('card') or {}).get('legacy') or provider.get('card') or {}
    bindings = card.get('binding_values') or []
    if isinstance(bindings, dict):
        bindings = [{'key': k, 'value': v} for k, v in bindings.items()]
    values = {v['key']: v.get('value', {}).get('string_value') for v in bindings}
    for key in ('title', 'description', 'card_url', 'vanity_url'):
        if values.get(key):
            fields.append(f'Link card {key}: {values[key]}')
    if card.get('url') and card['url'] != values.get('card_url'):
        fields.append('Link card URL: ' + card['url'])
    media_urls = set()
    for source in sources + (legacy.get('extended_entities') or {},):
        for media in source.get('media', []):
            for value in [media.get('media_url_https')] + [
                v.get('url') for v in (media.get('video_info') or {}).get('variants', [])
            ]:
                if value:
                    media_urls.add(value)
    for media in body.get('media') or []:
        media_urls.update(v for k in ('url', 'videoUrl', 'previewUrl') if (v := media.get(k)))
    fields.extend('Media: ' + url for url in sorted(media_urls))
    return text


def identity(body):
    provider = body.get('_raw') or body
    user = ((provider.get('core') or {}).get('user_results') or {}).get('result') or {}
    core, legacy = user.get('core') or {}, user.get('legacy') or {}
    author = body.get('author') or body.get('user') or {}
    name = core.get('name') or legacy.get('name') or author.get('name')
    handle = (
        core.get('screen_name')
        or legacy.get('screen_name')
        or author.get('username')
        or author.get('screen_name')
    )
    handle = '@' + handle.lstrip('@') if handle else ''
    return ' '.join(str(value) for value in (name, handle) if value)


def _post(doc, row, ctx):
    context = row.get('x_context') or {}
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
    fields = []
    text = expand(text, row, fields)
    doc['from'] = identity(row)
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
    _post_parts(doc, row, legacy, entities, text, identifier, fields)
    for post in context.get('posts', []):
        body = post['tweet']
        fields = []
        text = expand(body.get('text', ''), body, fields)
        author = identity(body)
        doc['parts'].append(
            {
                'kind': 'authored' if post['authored'] else 'quoted',
                'text': f"{post['role']} {body['id']}"
                + (f' by {author}' if author else '')
                + '\n'
                + unescape(text),
            }
        )
        if fields:
            doc['parts'].append({'kind': 'boilerplate', 'text': '\n'.join(fields)})
    for reason in context.get('notices', []):
        doc['parts'].append({'kind': 'boilerplate', 'text': 'X context: ' + reason})
    doc['context'] = {
        'unexpanded_links': sum(
            len(re.findall(r'https?://t\.co/[^\s<>]+', part['text']))
            for part in doc['parts']
            if part['kind'] == 'authored'
        )
    }


def _post_parts(doc, row, legacy, entities, text, identifier, fields):
    doc['parts'] = [{'kind': 'authored', 'text': unescape(text)}]
    if fields:
        doc['parts'].append({'kind': 'boilerplate', 'text': '\n'.join(fields)})
    provider = row.get('_raw') or {}
    quoted = (
        row.get('quotedTweet')
        or row.get('quoted_status')
        or (row.get('quoted_status_result') or provider.get('quoted_status_result') or {}).get(
            'result'
        )
    )
    if quoted:
        quoted = quoted.get('tweet') or quoted
        quoted_legacy = quoted.get('legacy') or (quoted.get('_raw') or {}).get('legacy') or {}
        quoted_text = (
            quoted.get('text')
            or quoted.get('fullText')
            or quoted.get('full_text')
            or quoted_legacy.get('full_text', '')
        )
        fields = []
        text = expand(quoted_text, quoted, fields)
        author = identity(quoted)
        attribution = 'Quoted post' + (f' by {author}' if author else '')
        if text.strip():
            doc['parts'].append({'kind': 'authored', 'text': attribution + ': ' + unescape(text)})
        else:
            reason = quoted.get('reason') or (quoted.get('tombstone') or {}).get('text')
            if isinstance(reason, dict):
                reason = reason.get('text')
            reason = (
                reason
                or quoted.get('__typename')
                or 'provider omitted post text; reason not supplied'
            )
            doc['parts'].append(
                {'kind': 'boilerplate', 'text': attribution + ' unavailable: ' + str(reason)}
            )
        if fields:
            doc['parts'].append({'kind': 'boilerplate', 'text': '\n'.join(fields)})
    elif legacy.get('quoted_status_id_str'):
        doc['parts'].append(
            {
                'kind': 'boilerplate',
                'text': 'Quoted post '
                + legacy['quoted_status_id_str']
                + ' unavailable: provider omitted post; reason not supplied',
            }
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


def demand(home, identifier):
    """Publish one capped refresh to ingestd and return its stored document."""
    import hashlib
    import subprocess
    import sys
    import time
    from pathlib import Path
    from ..accounts import account
    from ..reads import Reader

    home = Path(home)
    _, owner, native = identifier.split(':')
    configured = account('x_account')
    if (
        not native.isascii()
        or not native.isdigit()
        or owner != hashlib.sha256(configured.encode()).hexdigest()
    ):
        raise ValueError('X id does not match the configured account')
    started = time.time_ns()
    result = subprocess.run(
        [
            str(home / 'bin/qmd2-fetch'),
            'x',
            '--account',
            configured,
            '--x-id',
            native,
            '--spool',
            str(home / 'spool/x'),
            '--state',
            str(home / 'capture-state/x'),
            '--timeout',
            '8m',
        ],
        capture_output=True,
        timeout=500,
    )
    if result.returncode:
        receipt = json.loads(result.stdout)
        print(
            'qmd2: X refresh unavailable: ' + receipt.get('error', 'capture failed'),
            file=sys.stderr,
        )
        raise OSError('X refresh failed')
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        reader = Reader(home)
        try:
            doc = reader.get(identifier)[0]
            if doc['version']['capture'] >= started:
                return doc
        except LookupError:
            pass
        finally:
            reader.close()
        time.sleep(1)
    print('qmd2: X refresh spooled; ingest has not stored it within 90 seconds', file=sys.stderr)
    raise OSError('X refresh pending ingestion')
