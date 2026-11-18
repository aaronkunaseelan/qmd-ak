"""Article docs come from HTML snapshots or text_unavailable rows. Folders are the `where` labels,
and a delete notice keeps the document instead of tombstoning it.
"""

import json
from qmd2.doc import Exclusion, html_text

PARSER_VERSION = 3


def parse(raw, ctx):
    if ctx.get('state') == 'deleted':
        return []
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
        if isinstance(row, dict) and row.get('state') == 'deleted':
            return []
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
        where = folder_where(ctx, bookmark)
        if where:
            doc['where'] = where
        if row.get('text_unavailable'):
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


def folder_where(ctx, bookmark):
    # A snapshot's folder is a label; the starred listing contributes only `starred`.
    where = {ctx['folder']} if ctx.get('folder') in ('unread', 'archive') else set()
    if bookmark.get('starred') in ('1', 1, True) or ctx.get('folder') == 'starred':
        where.add('starred')
    if ctx.get('archive_only') == 'true':
        where.add('archive')
    return sorted(where)
