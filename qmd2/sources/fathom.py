"""Fathom snapshots render summaries before transcripts and order by capture time."""

import json

from .calendar import description

PARSER_VERSION = 1


def parse(raw, ctx):
    identifier = 'fathom:' + ctx.get('account', '') + ':' + ctx.get('key', '').rsplit(':', 1)[-1]
    doc = {
        'id': identifier,
        'item': identifier,
        'source': 'fathom',
        'group': 'meetings',
        'kind': 'meeting',
        'time': ctx.get('time'),
        'edited': ctx.get('time'),
        'parts': [],
        'parser': f'fathom@{PARSER_VERSION}',
    }
    try:
        row = json.loads(raw)
        identifier = 'fathom:' + ctx['account'] + ':' + str(row['recording_id'])
        doc.update(id=identifier, item=identifier)
        if ctx.get('state') == 'deleted' or row.get('state') == 'deleted':
            doc['kind'] = 'tombstone'
            return [doc]
        recorder = row.get('recorded_by') or {}
        invitees = row.get('calendar_invitees') or []
        start, end = row.get('recording_start_time', ''), row.get('recording_end_time', '')
        doc.update(
            title=row.get('meeting_title') or row.get('title', ''),
            time=start or ctx.get('time'),
            link=row.get('share_url') or row.get('url', ''),
            where=f'Start: {start}; End: {end}',
            context={'recorder': recorder.get('name', ''), 'start': start, 'end': end},
            **{
                'from': recorder.get('email') or recorder.get('name', ''),
                'to': [a.get('email') or a.get('name', '') for a in invitees],
            },
        )
        _meeting_parts(doc, row)
    except (ValueError, KeyError, TypeError, AttributeError):
        doc.update(
            parse_failed=True, parts=[{'kind': 'binary', 'text': 'Unreadable Fathom meeting'}]
        )
    return [doc]


def _meeting_parts(doc, row):
    parts = []
    summary = row.get('default_summary') or {}
    if summary.get('markdown_formatted'):
        parts.append({'kind': 'authored', 'text': '# Summary\n' + summary['markdown_formatted']})
    actions = row.get('action_items') or []
    if actions:
        lines = []
        for action in actions:
            assignee = action.get('assignee') or {}
            who = assignee.get('name') or assignee.get('email', '')
            lines.append('- ' + action.get('description', '') + (' (' + who + ')' if who else ''))
        parts.append({'kind': 'authored', 'text': '# Action items\n' + '\n'.join(lines)})
    transcript = []
    for turn in row.get('transcript') or []:
        speaker = turn.get('speaker') or {}
        stamp = str(turn.get('timestamp', '00:00:00'))
        if len(stamp.split(':')) == 2:
            stamp = '00:' + stamp
        transcript.append(
            f"[{stamp}] {speaker.get('display_name') or speaker.get('name', '')}: {description(turn.get('text', ''))}"
        )
    if transcript:
        parts.append({'kind': 'authored', 'text': '# Transcript\n' + '\n'.join(transcript)})
    if row.get('url'):
        parts.append({'kind': 'boilerplate', 'text': 'Video: ' + row['url']})
    doc['parts'] = parts
