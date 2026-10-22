import base64
import hashlib
import json
from email.message import EmailMessage

import pytest

from qmd2.sources.gmail import parse


def envelope(body='Synthetic lighthouse\n> private quotation', html=False, attachment=False):
    message = EmailMessage()
    for name, value in [
        ('Subject', 'Synthetic rendezvous'),
        ('From', 'sender@example.test'),
        ('To', 'recipient@example.test'),
        ('Cc', 'copy@example.test'),
    ]:
        message[name] = value
    message.set_content(body, subtype='html' if html else 'plain')
    if attachment:
        message.add_attachment(
            b'abc', maintype='application', subtype='pdf', filename='synthetic.pdf'
        )
    return {
        'id': 'message',
        'threadId': 'thread',
        'internalDate': '1791849600000',
        'labelIds': ['INBOX'],
        'raw': base64.urlsafe_b64encode(message.as_bytes()).decode().rstrip('='),
    }


def windows_envelope(alternative=False, charset='windows-874'):
    message = EmailMessage()
    message['Subject'] = 'Synthetic charset'
    message.set_content('ภาษาไทย', charset='cp874')
    if alternative:
        message.add_alternative('<p>ภาษาไทย</p>', subtype='html', charset='cp874')
    mime = message.as_bytes().replace(b'charset="cp874"', f'charset="{charset}"'.encode())
    return {'id': 'message', 'raw': base64.urlsafe_b64encode(mime).decode().rstrip('=')}


def alias_rows():
    from qmd2.doc import Exclusion

    cases = [
        dict(name=name, **{'from': sender, 'to': to, 'cc': cc, 'excluded': excluded})
        for name, sender, to, cc, excluded in [
            (
                'to owner',
                'sender@example.test',
                'pat@example.test, recruitment@org.example.test',
                '',
                False,
            ),
            ('alias only', 'sender@example.test', 'recruitment@org.example.test', '', True),
            (
                'alias plus core',
                'sender@example.test',
                'recruitment@org.example.test, core@org.example.test',
                '',
                False,
            ),
            ('from owner', 'pat@org.example.test', 'recruitment@org.example.test', '', False),
            (
                'owner plus',
                'sender@example.test',
                'recruitment@org.example.test',
                'pat+tag@example.test',
                False,
            ),
            (
                'alias cc colleague to',
                'sender@example.test',
                'colleague@org.example.test',
                'recruitment@org.example.test',
                True,
            ),
            (
                'keep cc',
                'sender@example.test',
                'recruitment@org.example.test',
                'grants@org.example.test',
                False,
            ),
            ('training only', 'sender@example.test', 'training@org.example.test', '', False),
            (
                'display names mixed case',
                'Applicant <sender@example.test>',
                'Recruitment Team <RECRUITMENT@Org.Example.TEST>',
                '',
                True,
            ),
            ('no org recipient', 'sender@example.test', 'reader@example.test', '', False),
            (
                'owner plus from mixed case',
                'Owner <PAT+tag@Org.Example.TEST>',
                'recruitment@org.example.test',
                '',
                False,
            ),
            (
                'owner display cc',
                'sender@example.test',
                'recruitment@org.example.test',
                'Owner <PAT@EXAMPLE.TEST>',
                False,
            ),
            (
                'alias only from',
                'recruitment@org.example.test',
                'colleague@org.example.test',
                '',
                False,
            ),
        ]
    ]
    rows = []
    for case in cases:
        for attachment in (False, True):
            message = EmailMessage()
            for header in ('From', 'To', 'Cc'):
                if case[header.lower()]:
                    message[header] = case[header.lower()]
            message.set_content('Synthetic shared alias body')
            if attachment:
                message.add_attachment(
                    b'abc', maintype='application', subtype='pdf', filename='synthetic.pdf'
                )
            row = {
                'id': 'message',
                'raw': base64.urlsafe_b64encode(message.as_bytes()).decode(),
                '_attachment_count': int(attachment),
            }
            kinds = (
                Exclusion('shared_alias', 'gmail:synthetic:message')
                if case['excluded']
                else ['authored']
            )
            rows.append((row, kinds, False, None))
    return rows


@pytest.mark.parametrize(
    'row,kinds,failed,body',
    [
        (envelope(), ['authored', 'quoted'], False, None),
        (envelope('hello\n-- \nsignature'), ['authored', 'boilerplate'], False, None),
        (
            envelope(
                '<h1>Heading</h1><p>A &amp; B <a href="https://example.test">link</a></p><blockquote>quoted</blockquote><script>hidden</script>',
                html=True,
            ),
            ['authored', 'quoted'],
            False,
            ('# Heading', 'link <https://example.test>'),
        ),
        (
            envelope(
                '<span class><a href>synthetic body</a></span><blockquote class>quoted</blockquote>',
                html=True,
            ),
            ['authored', 'quoted'],
            False,
            None,
        ),
        (envelope(attachment=True), ['authored', 'quoted'], False, None),
        (windows_envelope(), ['authored'], False, ('ภาษาไทย',)),
        (windows_envelope(alternative=True), ['authored'], False, ('ภาษาไทย',)),
        (
            windows_envelope(charset='windows-99999'),
            ['authored'],
            False,
            ('ภาษาไทย'.encode('cp874').decode('utf-8', errors='replace'),),
        ),
        (
            {'message': {'id': 'message', 'threadId': 'metadata-thread', 'labelIds': ['INBOX']}},
            None,
            False,
            None,
        ),
        (
            {
                'metadata_only': True,
                'message': {'id': 'message', 'threadId': 'metadata-thread', 'labelIds': ['INBOX']},
            },
            ['boilerplate'],
            False,
            ('No text supplied by Gmail record',),
        ),
        (envelope(body=''), ['boilerplate'], False, None),
        ({'id': 'message', 'state': 'deleted'}, [], False, None),
        ({'id': 'message', 'raw': '!!!'}, ['binary'], True, None),
        ({}, ['binary'], True, None),
        (None, ['binary'], True, None),
    ]
    + alias_rows(),
)
def test_parser_table(row, kinds, failed, body):
    docs = parse(
        json.dumps(row).encode(),
        {'account': 'synthetic', 'key': 'message', 'time': '2026-10-13T00:00:00Z'},
    )
    from qmd2.doc import Exclusion

    if isinstance(kinds, Exclusion):
        assert docs == [kinds]
        return
    if kinds is None:
        assert docs == []
        return
    doc = docs[0]
    assert doc['id'] == 'gmail:synthetic:message'
    assert [p['kind'] for p in doc['parts']] == kinds
    assert bool(doc.get('parse_failed')) == failed
    if row and row.get('metadata_only'):
        assert doc['thread'] == 'metadata-thread' and doc['where'] == ['INBOX']
        assert doc['parts'][0]['text'] == 'No text supplied by Gmail record'
    if row and row.get('state') == 'deleted':
        assert doc['kind'] == 'tombstone'
    if row and row.get('labelIds'):
        assert doc['title'] == 'Synthetic rendezvous'
        assert doc['where'] == ['INBOX']
        assert doc['thread'] == 'thread'
        assert doc['cc'] == 'copy@example.test'
    if row and '_attachment_count' in row:
        assert len(doc['attachments']) == row['_attachment_count']
    if doc['attachments']:
        assert doc['attachments'][0] == {
            'id': 'gmail:synthetic:message/att/2',
            'name': 'synthetic.pdf',
            'type': 'application/pdf',
            'size': 3,
        }
    text = '\n'.join(p['text'] for p in doc['parts'])
    if body is not None:
        if len(body) == 1:
            assert text == body[0]
        else:
            assert all(fragment in text for fragment in body)
    assert 'hidden' not in text


def test_gmail_spool_chain(tmp_path):
    from qmd2.ingestd import Ingestd
    from qmd2.doc import render

    spool = tmp_path / 'spool/gmail'
    spool.mkdir(parents=True)
    raw = json.dumps(envelope(attachment=True)).encode()
    (spool / 'payload').write_bytes(raw)
    entry = {
        'schema': 'qmd.spool-entry.v1',
        'source': 'gmail',
        'account': 'synthetic',
        'key': 'message',
        'path': 'payload',
        'bytes': len(raw),
        'sha256': hashlib.sha256(raw).hexdigest(),
        'captured_at': '2026-10-13T01:00:00Z',
        'state': {'labels': {'state': 'message'}},
    }
    (spool / 'index.jsonl').write_text(json.dumps(entry) + '\n')
    daemon = Ingestd(tmp_path, remote=False)
    daemon.spool()
    daemon.commit()
    hit = daemon.index.search('lighthouse')[0]
    assert hit['id'] == 'gmail:synthetic:message'
    assert not daemon.index.search('quotation')
    doc = json.loads(daemon.packs.read(hit['pointer'])[1])
    from qmd2.reads import Reader

    reader = Reader(tmp_path)
    try:
        fetched = reader.get(doc['id'])[0]
        assert fetched['id'] == doc['id'] and fetched['parts'] == doc['parts']
    finally:
        reader.close()
    text = render(doc)
    assert 'From: sender@example.test' in text
    assert '[quoted]\n> private quotation' in text
    assert 'synthetic.pdf' in text
    assert daemon.packs.read(doc['raw'])[1] == raw
    deletion = json.dumps({'id': 'message', 'state': 'deleted'}).encode()
    daemon.record(
        'gmail',
        deletion,
        {'account': 'synthetic', 'key': 'message', 'time': '2026-10-14T00:00:00Z'},
    )
    daemon.commit()
    assert not daemon.index.search('lighthouse')

    # Late captured exclusions advance processing, not searchable content or capture counts.
    import time
    from qmd2.verify import evaluate

    message = EmailMessage()
    message['From'] = 'sender@example.test'
    message['To'] = 'recruitment@org.example.test'
    message.set_content('Synthetic alias-only application')
    alias_raw = json.dumps(
        {
            'id': 'alias',
            'raw': base64.urlsafe_b64encode(message.as_bytes()).decode(),
        }
    ).encode()
    late = round(time.time(), 6)
    alias_path = spool / 'alias-payload'
    alias_path.write_bytes(alias_raw)
    alias_entry = dict(
        entry,
        key='alias',
        path=alias_path.name,
        bytes=len(alias_raw),
        sha256=hashlib.sha256(alias_raw).hexdigest(),
        item_time=late,
    )
    with (spool / 'index.jsonl').open('a') as f:
        f.write(json.dumps(alias_entry) + '\n')
    capture_receipt = dict(
        ok=True,
        enumerated=1,
        written=1,
        unchanged=0,
        excluded=0,
        failed=0,
        source_head=late,
        last_complete=late,
    )
    # Extracted outputs have their own raw lane, but retain the Gmail source and parent.
    attachment = dict(
        id='gmail:synthetic:alias/att/2',
        item='gmail:synthetic:alias/att/2',
        source='gmail',
        group='mail',
        kind='text',
        time='2026-10-13T00:00:00Z',
        parser='attachment@1',
        context={'parent': 'gmail:synthetic:alias'},
        parts=[{'kind': 'attachment_text', 'text': 'Synthetic application attachment'}],
    )
    daemon.queue_checkpoint('epoch:attachment', 2)
    daemon.record('attachment', json.dumps(attachment).encode(), {'key': attachment['id']})
    daemon.commit()
    assert daemon.index.search('application')
    ocr = dict(attachment, id=attachment['id'] + '/ocr')
    daemon.record('attachment', json.dumps(ocr).encode(), {'key': ocr['id']})
    before = daemon.checkpoint('indexed:gmail').copy()
    daemon.queue_checkpoint('receipt:gmail', capture_receipt.copy())
    daemon.spool()
    daemon.commit()
    status = daemon.source_status('gmail', 720, daemon.index, late)
    assert status['processed_through'] == late
    assert status['searchable_through'] == before['searchable_through']
    assert status['parser_exclusions'] == {'shared_alias': 1}
    assert status['receipt']['written'] == 1 and status['receipt']['excluded'] == 0
    assert not daemon.index.search('application')
    assert daemon.index.lookup(attachment['id']) is None
    assert daemon.index.lookup(ocr['id']) is None
    daemon.record('attachment', json.dumps(attachment).encode(), {'key': attachment['id']})
    daemon.commit()
    assert daemon.index.lookup(attachment['id']) is None
    daemon.last_status = 0
    daemon.publish_status()
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path), 'status', '--json'],
        capture_output=True,
        text=True,
        check=True,
    )
    gmail_status = json.loads(result.stdout)['sources']['gmail']
    assert gmail_status['parser_exclusions'] == {'shared_alias': 1}
    assert gmail_status['lag_s'] == 0
    missing = subprocess.run(
        [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path), 'get', attachment['id']],
        capture_output=True,
        text=True,
    )
    assert missing.returncode == 3
    checks = dict(written_at=late, heartbeat_at=late, backlog=[], sources={'gmail': status})
    assert 'gmail:head' not in evaluate(checks, late)
    checks['sources']['gmail'] = dict(status, processed_through=late - 7200)
    assert 'gmail:head' in evaluate(checks, late)
    # Legacy raw with no native item clock must not claim progress from capture wall-time.
    daemon.record('gmail', alias_raw, {'account': 'synthetic', 'key': 'alias', 'time': late + 9999})
    daemon.commit()
    assert daemon.checkpoint('indexed:gmail')['processed_through'] == late
    # A reparse removes an old alias document, and the exclusion never becomes a doc pack row.
    old_doc = parse(json.dumps(envelope()).encode(), {'account': 'synthetic'})[0]
    old_doc.update(id='gmail:synthetic:alias', item='gmail:synthetic:alias', parser='gmail@4')
    daemon.record('gmail', alias_raw, {'account': 'synthetic', 'key': 'alias'}, docs=[old_doc])
    # Seed a pre-fix output whose raw is outside the Gmail reparse bounds.
    daemon.record(
        'attachment',
        json.dumps(dict(attachment, context={})).encode(),
        {'key': attachment['id']},
    )
    daemon.commit()
    assert daemon.index.lookup(attachment['id']) is not None
    while not daemon.reindex('gmail', seconds=60):
        pass
    assert daemon.index.lookup('gmail:synthetic:alias') is None
    assert daemon.index.lookup(attachment['id']) is None
    while not daemon.reindex(seconds=60):
        pass
    assert daemon.index.lookup(attachment['id']) is None
    assert daemon.checkpoint('indexed:gmail')['processed_through'] == late
