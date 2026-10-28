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
    authored = [p['text'] for p in doc['parts'] if p['kind'] == 'authored']
    assert authored[-1] == (text if body_location == 'root' else 'Quoted post: ' + text)
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


@pytest.mark.parametrize('quoted', [False, True])
def test_x_response_fields_table(quoted):
    from qmd2.doc import render
    from qmd2.policy import text

    body = {
        'id': '42',
        'text': 'truncated',
        '_raw': {
            'note_tweet': {'note_tweet_results': {'result': {'text': 'Full lighthouse note'}}},
            'legacy': {
                'conversation_id_str': '10',
                'in_reply_to_status_id_str': '41',
                'lang': 'en',
                'favorite_count': 0,
                'reply_count': 2,
                'retweet_count': 3,
                'quote_count': 4,
                'bookmark_count': 5,
                'extended_entities': {
                    'media': [
                        {
                            'media_url_https': 'https://media.test/photo',
                            'video_info': {'variants': [{'url': 'https://media.test/video'}]},
                        }
                    ]
                },
            },
            'edit_control': {'edit_tweet_ids': ['40', '42']},
            'views': {'count': '123'},
            'card': {
                'legacy': {
                    'binding_values': [
                        {'key': k, 'value': {'string_value': v}}
                        for k, v in {
                            'title': 'Card lighthouse',
                            'description': 'Card description',
                            'card_url': 'https://card.test/',
                        }.items()
                    ]
                }
            },
            'core': {
                'user_results': {
                    'result': {
                        'core': {'name': 'Author Name', 'screen_name': 'synthetic'},
                        'legacy': {'description': 'Profile bio', 'followers_count': 9},
                        'is_blue_verified': False,
                    }
                }
            },
        },
    }
    row = {'id': '99', 'text': 'Root', 'quotedTweet': body} if quoted else body
    doc = parse(
        json.dumps({'tweet': row}).encode(),
        {
            'account': 'synthetic',
            'key': row['id'],
            'time': '2026-10-28T10:00:00Z',
        },
    )[0]
    output = render(doc)
    for expected in (
        'Full lighthouse note',
        'Author Name @synthetic',
        'Conversation: 10',
        'Reply to: 41',
        'Card description',
        'https://card.test/',
        'https://media.test/photo',
        'https://media.test/video',
    ):
        assert expected in output
    # Engagement, author profile, language and edit ids stay in the raw archive only.
    for absent in (
        'Language:',
        "['40', '42']",
        'likes=',
        'views=',
        'Engagement',
        'Profile bio',
        'followers',
        'verified',
    ):
        assert absent not in output
    assert 'Full lighthouse note' in text(doc)
    assert 'https://media.test/' not in text(doc)
    assert not references(doc)


@pytest.mark.parametrize('placement', ['saved', 'quoted', 'context'])
@pytest.mark.parametrize(
    'profile,expected',
    [
        ({'author': {'name': 'Author Name', 'username': 'writer'}}, 'Author Name @writer'),
        ({'user': {'name': 'Author Name', 'screen_name': 'writer'}}, 'Author Name @writer'),
        ({'author': {'username': 'writer'}}, '@writer'),
        ({'user': {'name': 'Author Name'}}, 'Author Name'),
        (
            {
                '_raw': {
                    'core': {
                        'user_results': {
                            'result': {
                                'legacy': {'name': 'Legacy Name', 'screen_name': 'legacy'},
                            }
                        }
                    }
                }
            },
            'Legacy Name @legacy',
        ),
        (
            {
                'core': {
                    'user_results': {
                        'result': {
                            'core': {'name': 'Core Name', 'screen_name': 'core'},
                            'legacy': {'name': 'Legacy Name', 'screen_name': 'legacy'},
                        }
                    }
                }
            },
            'Core Name @core',
        ),
    ],
)
def test_x_identity_table(profile, expected, placement):
    from qmd2.doc import render

    body = dict(profile, id='42', text='Lighthouse')
    row = body if placement == 'saved' else {'id': '99', 'text': 'Root'}
    if placement == 'quoted':
        row['quotedTweet'] = body
    elif placement == 'context':
        row['x_context'] = {'posts': [{'tweet': body, 'role': 'Parent', 'authored': False}]}
    doc = parse(json.dumps(row).encode(), {'account': 'synthetic', 'key': row['id']})[0]
    assert not doc.get('parse_failed')
    output = render(doc)
    assert output.count(expected) == 1
    if placement == 'saved':
        assert doc['from'] == expected
    else:
        attribution = 'Quoted post' if placement == 'quoted' else 'Parent 42'
        assert attribution + ' by ' + expected in output


@pytest.mark.parametrize(
    'result,reason',
    [
        ({'__typename': 'TweetTombstone', 'tombstone': {'text': {'text': 'Deleted'}}}, 'Deleted'),
        ({'__typename': 'TweetUnavailable', 'reason': 'Protected'}, 'Protected'),
        ({'__typename': 'TweetUnavailable', 'reason': 'Suspended'}, 'Suspended'),
        (None, 'provider omitted post'),
    ],
)
def test_x_unavailable_quote_table(result, reason):
    from qmd2.doc import render

    raw = {
        'tweet': {
            'id': '42',
            'text': 'lighthouse',
            '_raw': {
                'legacy': {'quoted_status_id_str': '41'},
                'quoted_status_result': {'result': result},
            },
        }
    }
    doc = parse(json.dumps(raw).encode(), {'account': 'synthetic', 'key': '42'})[0]
    assert not doc.get('parse_failed')
    assert 'unavailable' in render(doc) and reason in render(doc)
    assert not any(p['text'] == 'Quoted post: ' for p in doc['parts'])
