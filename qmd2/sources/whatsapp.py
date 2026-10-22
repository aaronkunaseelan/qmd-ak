"""Decoded WhatsApp messages use explicit aliases; control records produce no doc."""

import json
from qmd2.doc import Exclusion

PARSER_VERSION = 4


def canonical(jid, aliases):
    return aliases.get(jid, jid)


def parse(raw, ctx):
    account = ctx.get('account', '')
    identifier = f"wa:{account}:{ctx.get('key', '')}"
    doc = {
        'id': identifier,
        'item': identifier,
        'source': 'whatsapp',
        'group': 'mail',
        'kind': 'message',
        'time': ctx.get('time'),
        'parts': [],
        'attachments': [],
        'parser': f'whatsapp@{PARSER_VERSION}',
    }
    try:
        decoded = json.loads(raw)['decoded']
        return _message(doc, decoded, ctx, account)
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        doc.update(
            parse_failed=True, parts=[{'kind': 'binary', 'text': 'Unreadable WhatsApp envelope'}]
        )
    return [doc]


def _message(doc, decoded, ctx, account):
    if not isinstance(decoded, dict):
        raise ValueError()
    if decoded.get('kind') in ('history_source', 'context'):
        return []
    if decoded.get('kind') in ('reaction', 'protocol'):
        return [Exclusion(decoded['kind'])]
    if decoded.get('status_broadcast') or decoded.get('chat') == 'status@broadcast':
        return [Exclusion('status_broadcast')]
    aliases = ctx.get('aliases', {})
    contacts, groups = ctx.get('contacts', {}), ctx.get('groups', {})
    chat = canonical(decoded['chat'], aliases)
    message = decoded['target'] if decoded.get('kind') == 'edit' else decoded['id']
    if not chat or not message:
        raise ValueError()
    identifier = f'wa:{account}:{chat}:{message}'
    sender = decoded.get('sender') or (account if decoded.get('from_me') else chat)
    sender = canonical(sender, aliases)
    name = decoded.get('push_name') or contacts.get(sender) or sender
    chat_name = groups.get(chat) or contacts.get(chat) or chat
    doc.update(
        id=identifier,
        item=identifier,
        time=decoded.get('timestamp') or doc['time'],
        thread=chat,
        to=chat_name,
        context={'chat': chat_name},
    )
    doc['from'] = name
    if decoded.get('edited'):
        doc['edited'] = decoded['edited']
    if decoded.get('quoted_message_id'):
        doc['reply_to'] = f"wa:{account}:{chat}:{decoded['quoted_message_id']}"
    text = decoded.get('text', '')
    if not isinstance(text, str):
        raise ValueError()
    if text:
        doc['parts'].append({'kind': 'authored', 'text': text})
    for position, media in enumerate(decoded.get('media') or []):
        attachment = {
            'id': f'{identifier}/att/{position}',
            'name': media.get('fileName', ''),
            'type': media.get('mimetype') or media.get('type', ''),
            'size': int(media.get('fileLength') or 0),
        }
        if media.get('fileSHA256'):
            attachment['sha256'] = media['fileSHA256']
        doc['attachments'].append(attachment)
    if decoded.get('kind') == 'binary':
        doc['parts'] = [{'kind': 'binary', 'text': 'WhatsApp message has no decoded body'}]
    return [doc]
