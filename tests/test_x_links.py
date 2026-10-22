"""Provider body/entity variants are one pure parser table, without network expansion."""

import json

import pytest

from qmd2.links import references
from qmd2.sources.x import parse


@pytest.mark.parametrize('body_location', ['root', 'quotedTweet', 'quoted_status_result'])
@pytest.mark.parametrize(
    'location',
    [
        'entities',
        'legacy',
        'provider',
        'note',
        'missing',
        'photo',
        'video',
        'animated_gif',
        'legacy_photo',
        'extended_video',
    ],
)
def test_x_link_table(location, body_location):
    entity = {
        'urls': [
            {
                'url': 'https://t.co/short',
                'display_url': 'example.test/article',
                'expanded_url': 'https://example.test/article',
            }
        ]
    }
    body = {'text': 'Synthetic lighthouse https://t.co/short'}
    media_type = {
        'photo': 'photo',
        'video': 'video',
        'animated_gif': 'animated_gif',
        'legacy_photo': 'photo',
        'extended_video': 'video',
    }.get(location)
    if location == 'entities':
        body['entities'] = entity
    elif location == 'legacy':
        body['legacy'] = {'entities': entity}
    elif location == 'provider':
        body['_raw'] = {'legacy': {'entities': entity}}
    elif location == 'note':
        body['_raw'] = {
            'legacy': {
                'entities': {
                    'urls': [{'url': 'https://t.co/other', 'expanded_url': 'https://wrong.test/'}]
                }
            },
            'note_tweet': {'note_tweet_results': {'result': {'entity_set': entity}}},
        }
    elif media_type:
        media = {
            'media': [
                {
                    'type': media_type,
                    'url': 'https://t.co/short',
                    'expanded_url': 'https://x.com/synthetic/status/42/'
                    + ('photo/1' if media_type == 'photo' else 'video/1'),
                    'media_url_https': 'https://pbs.twimg.com/media/synthetic.jpg',
                }
            ]
        }
        body['_raw'] = {'legacy': {'entities': media, 'extended_entities': media}}
        if location == 'legacy_photo':
            body['_raw']['legacy']['full_text'] = body.pop('text')
        elif location == 'extended_video':
            body['_raw']['legacy'].pop('entities')
    if body_location == 'root':
        row = dict(body, id='42')
    elif body_location == 'quotedTweet':
        row = {'id': '42', 'text': 'Synthetic root', 'quotedTweet': body}
    else:
        result = dict(body.get('_raw') or body)
        result.setdefault('legacy', {})['full_text'] = (
            body.get('text') or result['legacy']['full_text']
        )
        result.pop('text', None)
        row = {
            'id': '42',
            'text': 'Synthetic root',
            '_raw': {'quoted_status_result': {'result': result}},
        }
    raw = json.dumps({'tweet': row}).encode()
    doc = parse(raw, {'account': 'synthetic', 'key': '42'})[0]
    assert doc['id'] == doc['item'] == 'x:synthetic:42'
    assert not doc.get('parse_failed')
    refs = references(doc)
    target = 'https://t.co/short' if location == 'missing' else 'https://example.test/article'
    assert [ref['url'] for ref in refs] == ([] if media_type else [target])
    assert doc['context']['unexpanded_links'] == int(location == 'missing')
    replacement = (
        ('[photo]' if media_type == 'photo' else '[video]')
        if media_type
        else ('https://t.co/short' if location == 'missing' else f'example.test/article <{target}>')
    )
    text = 'Synthetic lighthouse ' + replacement
    assert doc['parts'][-1]['text'] == (text if body_location == 'root' else 'Quoted post: ' + text)
    assert doc.get('attachments', []) == (
        [
            {
                'id': 'x:synthetic:42/att/0',
                'name': 'https://t.co/short',
                'type': 'image',
                'size': 0,
            }
        ]
        if body_location == 'root' and media_type == 'photo'
        else []
    )
    assert raw == json.dumps({'tweet': row}).encode()
    if refs and location != 'missing':
        # A second post with another wrapper for the same article shares its capture.
        other = json.loads(json.dumps(row).replace('/short', '/another'))
        other['id'] = '43'
        other_doc = parse(
            json.dumps({'tweet': other}).encode(), {'account': 'synthetic', 'key': '43'}
        )[0]
        assert references(other_doc)[0]['capture_id'] == refs[0]['capture_id']
