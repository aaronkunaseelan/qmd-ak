"""gsd D5 landed change history; native change-set ids, no pipeline feedback."""

import json

PARSER_VERSION = 1


def parse(raw, ctx):
    identifier = 'gsd-events:' + ctx.get('key', '')
    doc = dict(
        id=identifier,
        item=identifier,
        source='gsd-events',
        group='gsd-events',
        kind='change-set',
        time=ctx.get('time'),
        parser=f'gsd-events@{PARSER_VERSION}',
        parts=[],
    )
    try:
        row = json.loads(raw)
        identifier = 'gsd-events:' + row['change_set_id']
        doc.update(
            id=identifier,
            item=identifier,
            time=row['time'],
            title=row['title'],
            context={'actor': row['actor']},
            parts=[{'kind': 'authored', 'text': row['text']}],
        )
    except (ValueError, KeyError, TypeError):
        doc.update(
            parse_failed=True, parts=[{'kind': 'binary', 'text': 'Unreadable gsd change set'}]
        )
    return [doc]
