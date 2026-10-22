"""Article docs come from HTML snapshots or text_unavailable rows; deleted bookmarks become
tombstones.
"""

import json
from qmd2.doc import Exclusion, html_text

PARSER_VERSION = 2


def parse(raw, ctx):
    identifier = (
        'instapaper:' + ctx.get('account', '') + ':' + ctx.get('key', '').rsplit(':', 1)[-1]
    )
    doc = dict(
        id=identifier,
        item=identifier,
        source='instapaper',
        group='articles',
        kind='article',
        time=None,
        parts=[],
        parser=f'instapaper@{PARSER_VERSION}',
    )
    try:
        row = json.loads(raw)
        bookmark = row.get('bookmark', row)
        identifier = 'instapaper:' + ctx['account'] + ':' + str(bookmark['bookmark_id'])
        doc.update(
            id=identifier,
            item=identifier,
            title=bookmark.get('title', ''),
            link=bookmark.get('url', ''),
            time=bookmark.get('time'),
            **{'from': bookmark.get('author', '')},
        )
        if row.get('state') == 'deleted' or ctx.get('state') == 'deleted':
            doc['kind'] = 'tombstone'
        elif row.get('text_unavailable'):
            doc['parts'] = [{'kind': 'boilerplate', 'text': 'Article text unavailable'}]
        elif not isinstance(row.get('html'), str) or not row['html'].strip():
            return [Exclusion('empty_article')]
        else:
            text = html_text(row['html'])
            if not text:
                return [Exclusion('empty_article')]
            doc['parts'] = [{'kind': 'authored', 'text': text}]
    except (ValueError, KeyError, TypeError, AttributeError):
        doc.update(
            parse_failed=True, parts=[{'kind': 'binary', 'text': 'Unreadable Instapaper snapshot'}]
        )
    return [doc]
