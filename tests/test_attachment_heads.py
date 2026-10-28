"""Structured attachment heads and human rendering share one pack-backed projection."""

import base64
import json
import subprocess
import sys
from email.message import EmailMessage

from qmd2.doc import dumps
from qmd2.ingestd import Ingestd
from qmd2.reads import Reader


def test_structured_attachment_heads_match_rendered_subprocess(tmp_path):
    daemon = Ingestd(tmp_path, remote=False)
    try:
        msg = EmailMessage()
        msg['Subject'] = 'Synthetic attachment heads'
        msg.set_content('Body is preserved')
        for name, media, content in (
            ('note.txt', 'text/plain', b'first line\nsecond line\nthird line'),
            (
                'meeting.ics',
                'text/calendar',
                b'BEGIN:VCALENDAR\nBEGIN:VEVENT\nSUMMARY:Harbor\nDTSTART:20261028T120000Z\nLOCATION:Dock\nEND:VEVENT\nEND:VCALENDAR',
            ),
            ('image.png', 'image/png', b'synthetic image'),
            ('missing.pdf', 'application/pdf', b'synthetic pdf'),
            ('failed.txt', 'text/plain', b'synthetic failure'),
        ):
            main, sub = media.split('/')
            msg.add_attachment(content, maintype=main, subtype=sub, filename=name)
        daemon.record(
            'gmail',
            dumps(
                {
                    'id': 'one',
                    'internalDate': '1793188800000',
                    'raw': base64.urlsafe_b64encode(msg.as_bytes()).decode(),
                }
            ).encode(),
            {'account': 'synthetic', 'key': 'one'},
        )
        daemon.commit()
        reader = Reader(tmp_path)
        parent = reader.get('gmail:synthetic:one')[0]
        reader.close()
        for ref in parent['attachments']:
            if ref['name'] not in ('note.txt', 'image.png', 'failed.txt'):
                continue
            failed = ref['name'] == 'failed.txt'
            doc = dict(
                id=ref['id'],
                item=ref['id'],
                source='attachment',
                group='mail',
                kind='attachment_outcome' if failed else 'text',
                time=parent['time'],
                state='failed' if failed else 'done',
                reason='synthetic failure' if failed else None,
                parts=[]
                if failed
                else [
                    {'kind': 'header', 'text': 'not head content'},
                    {'kind': 'attachment_text', 'text': 'first line\nsecond line\nthird line'},
                ],
            )
            daemon.record('attachment', dumps(doc).encode(), {'key': doc['id']})
        daemon.commit()
        command = [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path), 'get', parent['id']]

        def get(*args):
            result = subprocess.run(command + list(args), capture_output=True, text=True)
            assert result.returncode == 0, result.stderr
            return result.stdout

        projected = json.loads(get('--json', '--attachments', 'head:2'))
        doc = projected['docs'][0]
        refs = {ref['name']: ref for ref in doc['attachments']}
        assert refs['note.txt']['head'] == {
            'state': 'done',
            'text': 'first line\nsecond line',
            'calendar_summary': None,
        }
        assert refs['image.png']['head'] == {'state': 'done', 'text': '', 'calendar_summary': None}
        assert refs['missing.pdf']['head']['state'] == 'unmaterialised'
        assert refs['failed.txt']['head']['state'] == 'failed'
        calendar = refs['meeting.ics']['head']
        assert calendar['state'] == 'unmaterialised'
        assert calendar['text'] == ''
        assert (
            calendar['calendar_summary']
            == 'summary: Harbor; start: 20261028T120000Z; end: ?; location: Dock'
        )
        for ref in refs.values():
            head = ref['head']
            block = f"  {ref['id']} ({ref['name']}, {ref['type']}, {head['state']})"
            body = head['calendar_summary'] or head['text']
            if body:
                block += '\n' + '\n'.join('    ' + line for line in body.splitlines())
            assert block in projected['text']
        assert get('--attachments', 'head:2').rstrip('\n') == projected['text']
        assert all(doc[key] == value for key, value in parent.items() if key != 'attachments')
        for original, enriched in zip(parent['attachments'], doc['attachments']):
            assert all(enriched[key] == value for key, value in original.items())
        assert 'Body is preserved' in projected['text']
        assert json.loads(get('--json'))['docs'][0]['attachments'][0]['head']['text'].endswith(
            'third line'
        )
        assert json.loads(get('--json', '--max-chars', '10'))['docs'] is None
    finally:
        daemon.upload_pool.shutdown()
        for stream in daemon.packs.files.values():
            stream.close()
        daemon.state.db.close()
        daemon.lock.close()
