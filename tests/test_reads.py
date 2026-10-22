import base64
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from email.message import EmailMessage

import pytest

from qmd2.attachments import Unavailable, enqueue, output_doc, queue_path
from qmd2.doc import dumps
from qmd2.ingestd import Ingestd
from qmd2.links import meeting_text, share_call
from qmd2.reads import calendar_line


@pytest.mark.parametrize('command', [[], ['get'], ['add'], ['extract'], ['verify']])
def test_exit_legend_subprocess(command):
    result = subprocess.run(
        [sys.executable, '-m', 'qmd2.cli', *command, '--help'], capture_output=True, text=True
    )
    assert result.returncode == 0
    help_text = ' '.join(result.stdout.split())
    assert '3 not found' in help_text
    assert '4 could not get it' in help_text
    assert 'queued/unavailable' in help_text


def test_read_subprocess_pack_edges(tmp_path):
    daemon = Ingestd(tmp_path, remote=False)
    for day in (13, 15, 17, 19):
        msg = EmailMessage()
        msg['Subject'], msg['From'] = 'Synthetic harbor', 'sender@example.test'
        msg.set_content(f'harbor line {day}\n> quoted secret {day}\nhttps://example.test/article')
        msg.add_attachment(
            b'first line\nsecond line\nthird line',
            maintype='text',
            subtype='plain',
            filename='note.txt',
        )
        msg.add_attachment(
            b'BEGIN:VCALENDAR\nBEGIN:VEVENT\nSUMMARY:Harbor meeting\nDTSTART:20260815T120000Z\nDTEND:20260815T130000Z\nLOCATION:Dock\nEND:VEVENT\nEND:VCALENDAR',
            maintype='text',
            subtype='calendar',
            filename='meeting.ics',
        )
        raw = dumps(
            dict(
                id=str(day),
                threadId='shared',
                internalDate=str(
                    int(datetime(2026, 8, day, tzinfo=timezone.utc).timestamp() * 1000)
                ),
                raw=base64.urlsafe_b64encode(msg.as_bytes()).decode(),
            )
        ).encode()
        daemon.record('gmail', raw, dict(account='synthetic', key=str(day)))
        daemon.record(
            'whatsapp',
            dumps(
                dict(
                    decoded=dict(
                        id=str(day),
                        chat='chat@g.us',
                        sender='a',
                        timestamp=f'2026-08-{day}T12:00:00Z',
                        kind='message',
                        text=f'harbor chat {day}',
                    )
                )
            ).encode(),
            dict(account='synthetic', key=str(day)),
        )
    for suffix, day in [('a', 13), ('b', 13), ('c', 13), ('asked', 15), ('future', 19)]:
        daemon.record(
            'whatsapp',
            dumps(
                dict(
                    decoded=dict(
                        id=suffix,
                        chat='ties@g.us',
                        sender='a',
                        timestamp=f'2026-08-{day}T12:00:00Z',
                        kind='message',
                        text='same instant',
                    )
                )
            ).encode(),
            dict(account='synthetic', key=suffix),
        )
    daemon.record(
        'pi',
        b'{"id":"later","type":"message","timestamp":"2026-08-19T12:00:00Z","message":{"role":"user","content":[{"type":"text","text":"harbor future"}]}}\n',
        dict(runtime='pi', session='future', key='future', offset=0),
    )
    daemon.record(
        'pi',
        b'{"id":"earlier","type":"message","timestamp":"2026-08-15T12:00:00Z","message":{"role":"user","content":[{"type":"text","text":"harbor past"}]}}\n',
        dict(runtime='pi', session='future', key='future', offset=100),
    )
    daemon.commit()
    from qmd2.cli import get_docs

    parent = get_docs(tmp_path, 'gmail:synthetic:15')[0]
    attachment = parent['attachments'][0]['id']
    enqueue(tmp_path, attachment, parent, dict(source='gmail'))
    row = json.loads(queue_path(tmp_path, attachment).read_text())
    row['media_sha256'] = 'synthetic'
    doc = output_doc(row, 'first line\nsecond line\nthird line', 'synthetic@1', 'text')
    daemon.record('attachment', dumps(doc).encode(), dict(key=doc['id']))
    daemon.commit()
    env = dict(os.environ, QMD_UNTIL='2026-08-17T00:00:00Z')
    command = [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path)]

    def run(*args, ceiling=True):
        return subprocess.run(
            command + list(args),
            capture_output=True,
            env=env if ceiling else {k: v for k, v in os.environ.items() if k != 'QMD_UNTIL'},
        )

    batch = run(
        'get',
        'gmail:synthetic:15',
        'missing',
        'gmail:synthetic:19',
        '--no-quoted',
        '--attachments',
        'head:2',
    )
    assert (
        batch.returncode == 3 and b'first line' in batch.stdout and b'second line' in batch.stdout
    )
    assert b'third line' not in batch.stdout and b'quoted secret' not in batch.stdout
    assert b'quoted omitted:' in batch.stdout and b'summary: Harbor meeting; start:' in batch.stdout
    assert b'=== gmail:synthetic:15 ===' in batch.stdout
    assert b'missing' in batch.stderr and b'gmail:synthetic:19' in batch.stderr
    found = run('get', 'gmail:synthetic:15', '--no-quoted', '--json')
    assert found.returncode == 0 and b'quoted secret' not in found.stdout
    assert run('get', 'gmail:synthetic:19').returncode == 3
    assert run('get', 'gmail:synthetic:19', '--materialise').returncode == 3
    assert run('get', 'gmail:synthetic:19', '--raw').returncode == 3
    assert run('get', 'pi:future#later').returncode == 3
    session = run('get', 'pi:future', '--json')
    assert session.returncode == 0 and [d['id'] for d in json.loads(session.stdout)['docs']] == [
        'pi:future#earlier'
    ]
    assert len(parent['attachments']) == 3 and parent['attachments'][-1]['capture_id'].startswith(
        'link:'
    )
    assert run('get', attachment).returncode == 0
    hidden = run('thread', 'gmail:synthetic:15', '--no-quoted', '--json')
    assert hidden.returncode == 0, hidden.stderr
    assert [d['id'] for d in json.loads(hidden.stdout)['docs']] == [
        'gmail:synthetic:13',
        'gmail:synthetic:15',
        'gmail:synthetic:17',
    ]
    whole = run('thread', 'gmail:synthetic:15', '--json', ceiling=False)
    assert len(json.loads(whole.stdout)['docs']) == 4
    context = run('thread', 'wa:synthetic:chat@g.us:15', '--around', '1', '--json')
    assert context.returncode == 0, context.stderr
    assert [d['id'].rsplit(':', 1)[1] for d in json.loads(context.stdout)['docs']] == ['13', '15']
    assert '[asked]' in json.loads(context.stdout)['text']
    for id, n, expected in [
        ('asked', '2', ['b', 'c', 'asked']),
        ('b', '1', ['a', 'b', 'c']),
        ('b', '0', ['b']),
    ]:
        tied = run('thread', 'wa:synthetic:ties@g.us:' + id, '--around', n, '--json')
        assert tied.returncode == 0, tied.stderr
        assert [d['id'].rsplit(':', 1)[1] for d in json.loads(tied.stdout)['docs']] == expected
    grep = run(
        'grep',
        'harbor (line|chat)',
        '--since',
        '2026-08-13',
        '--until',
        '2026-08-22',
        '--source',
        'gmail',
        '--source',
        'whatsapp',
        '--json',
    )
    assert grep.returncode == 0, grep.stderr
    assert len(json.loads(grep.stdout)) == 5
    assert (
        len(
            json.loads(
                run(
                    'grep',
                    'harbor',
                    '--since',
                    '2026-08-13',
                    '--until',
                    '2026-08-22',
                    '--limit',
                    '1',
                    '--json',
                ).stdout
            )
        )
        == 1
    )
    for verb in [('browse', '--until', '2026-08-22'), ('search', 'harbor')]:
        result = run(*verb, '--json')
        assert result.returncode == 0, result.stderr
        data = json.loads(result.stdout)
        rows = data['results'] if isinstance(data, dict) else data
        assert all(r['time'][:10] <= '2026-08-17' for r in rows)
    bad = subprocess.run(
        command + ['search', 'harbor'], capture_output=True, env=dict(env, QMD_UNTIL='broken')
    )
    assert bad.returncode == 2
    for arguments in [
        ['search', 'harbor', '--since', 'garbage'],
        ['search', 'harbor', '--until', '2026-02-29'],
        ['browse', '--since', ''],
        ['browse', '--until', 'garbage'],
        ['grep', 'harbor', '--since', 'garbage', '--until', '2026-08-22'],
        ['grep', 'harbor', '--since', '2026-08-13', '--until', 'garbage'],
        ['feed', '--source', 'gmail', '--since-arrival', 'garbage', '--json'],
    ]:
        invalid = run(*arguments, ceiling=False)
        assert invalid.returncode == 2 and b'invalid ISO date/time' in invalid.stderr
        assert not invalid.stdout
    narrowed = run('search', 'harbor', '--until', '2026-08-14', '--json')
    assert narrowed.returncode == 0
    assert {row['id'] for row in json.loads(narrowed.stdout)} == {
        'gmail:synthetic:13',
        'wa:synthetic:chat@g.us:13',
    }
    bounded = run(
        'browse',
        '--since',
        '2026-08-15T15:00:00+03:00',
        '--until',
        '2026-08-15T12:00:00Z',
        '--json',
    )
    assert bounded.returncode == 0
    assert all(
        row['time'].startswith('2026-08-15T12:00:00')
        for row in json.loads(bounded.stdout)['results']
    )


@pytest.mark.parametrize(
    'text,expected',
    [
        (
            'SUMMARY:One\\, two\nDTSTART;TZID=Europe/Athens:20260815T120000\nDTEND:20260815T130000\nLOCATION:Dock\n yard',
            'summary: One, two; start: 20260815T120000 (Europe/Athens); end: 20260815T130000; location: Dockyard',
        ),
        (
            'BEGIN:VCALENDAR\nBEGIN:VTIMEZONE\nBEGIN:STANDARD\nDTSTART:19701025T040000\nEND:STANDARD\nEND:VTIMEZONE\nBEGIN:VEVENT\nSUMMARY:Call\nDTSTART;TZID=Europe/Athens:20260813T144500\nDTEND;TZID=Europe/Athens:20260813T145000\nEND:VEVENT\nEND:VCALENDAR',
            'summary: Call; start: 20260813T144500 (Europe/Athens); end: 20260813T145000 (Europe/Athens); location: ?',
        ),
        ('', 'summary: ?; start: ?; end: ?; location: ?'),
    ],
)
def test_calendar_table(text, expected):
    assert calendar_line(text) == expected


@pytest.mark.parametrize(
    'url,payload,valid',
    [
        (
            'https://fathom.video/share/synthetic',
            dict(
                call=dict(id=123),
                copyTranscriptUrl='https://fathom.video/calls/123/copy_transcript?token=synthetic',
            ),
            True,
        ),
        (
            'https://fathom.video/share/synthetic',
            dict(call=dict(id=123, copyTranscriptUrl='/calls/123/copy_transcript?token=synthetic')),
            True,
        ),
        ('https://fathom.video/calls/123', {}, 'account'),
        ('https://example.test/share/synthetic', {}, None),
        ('https://fathom.video/share/synthetic', {}, False),
        (
            'https://fathom.video/share/synthetic',
            dict(
                call=dict(id=123),
                copyTranscriptUrl='https://evil.test/calls/123/copy_transcript?token=synthetic',
            ),
            False,
        ),
        (
            'https://fathom.video/share/synthetic',
            dict(
                call=dict(id=123),
                copyTranscriptUrl='https://fathom.video/calls/123/copy_transcript?token=other',
            ),
            False,
        ),
    ],
)
def test_share_parser_table(url, payload, valid):
    import html

    page = '<div data-page="' + html.escape(dumps(dict(props=payload)), quote=True) + '"></div>'
    if valid == 'account':
        with pytest.raises(Unavailable, match='account_only_link'):
            share_call(url, page)
    elif valid is False:
        with pytest.raises(ValueError):
            share_call(url, page)
    elif valid is None:
        assert share_call(url, page) is None
    else:
        assert share_call(url, page)[0]['id'] == 123
        text = meeting_text(
            dict(
                title='Harbor',
                started_at='2026-08-26',
                host=dict(email='host@example.test'),
                attendees=[dict(name='Guest')],
                summary='<p>Decided</p>',
            ),
            {},
            'Full transcript',
        )
        assert all(
            s in text
            for s in [
                'Harbor',
                '2026-08-26',
                'host@example.test',
                'Guest',
                'Decided',
                'Full transcript',
            ]
        )
