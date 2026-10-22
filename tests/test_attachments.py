import hashlib
import fcntl
import base64
import hmac
import http.server
import json
import os
import subprocess
import sys
import threading
import time

import pytest

from qmd2.attachments import (
    CUTOFF,
    PIPELINE_REVISION,
    Worker,
    decrypt,
    enqueue,
    enqueue_auto,
    materialise,
    parse,
    run,
    snapshot,
    detect,
    stream,
    Unavailable,
    queue_path,
    lock_path,
)
from qmd2.cli import get_docs
from qmd2.doc import date, dumps
from qmd2.ingestd import Ingestd
from qmd2.verify import evaluate


@pytest.mark.parametrize(
    'raw,failed',
    [
        (dumps({'id': 'synthetic/att/0', 'parts': []}).encode(), False),
        (b'null', True),
        (b'{', True),
        (b'{}', True),
    ],
)
def test_attachment_parser_table(raw, failed):
    assert bool(parse(raw, {'key': 'synthetic/att/0', 'time': 1})[0].get('parse_failed')) == failed


def test_ptt_queue_filesystem_boundaries(tmp_path):
    cutoff = int(date(CUTOFF).timestamp() * 1e9)
    parent = {
        'id': 'wa:synthetic:chat:message',
        'source': 'whatsapp',
        'group': 'mail',
        'time': '2020-01-01T00:00:00Z',
        'kind': 'message',
        'context': {'chat': 'Synthetic chat'},
    }
    for offset, type_, ptt, mime, expected in [
        (-1, 'audio', True, 'audio/ogg', False),
        (0, 'audio', True, 'audio/ogg', False),
        (1000000, 'audio', False, 'audio/ogg', True),
        (1000000, 'audio', None, 'audio/ogg', True),
        (1000000, 'image', True, 'audio/ogg', True),
        (1000000, 'audio', True, 'application/octet-stream', True),
    ]:
        raw = dumps(
            {'decoded': {'media': [{'type': type_, 'PTT': ptt, 'mimetype': mime}]}}
        ).encode()
        enqueue_auto(tmp_path, raw, [parent], cutoff + offset)
        assert (snapshot(tmp_path)['queued'] == 1) == expected
    for provenance in (
        {'history_sha256': 'synthetic'},
        {'legacy_db_sha256': 'synthetic'},
        {'received_at': '2020-01-01T00:00:00Z'},
    ):
        history_home = tmp_path / 'history'
        history_raw = dumps(
            {'raw': provenance, 'decoded': {'media': [{'type': 'audio', 'PTT': True}]}}
        ).encode()
        enqueue_auto(history_home, history_raw, [parent], cutoff + 1000000)
        assert snapshot(history_home)['queued'] == 0
    repeat = dumps(
        {
            'decoded': {
                'media': [{'type': 'audio', 'PTT': True, 'mimetype': 'application/octet-stream'}]
            }
        }
    ).encode()
    enqueue_auto(tmp_path, repeat, [parent], cutoff + 1000000)
    assert len(list((tmp_path / 'attachments/pending').glob('*.json'))) == 1
    path = next((tmp_path / 'attachments/pending').glob('*.json'))
    assert path.stat().st_mode & 0o077 == 0
    assert snapshot(tmp_path)['cutoff'] == CUTOFF


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.headers.get('User-Agent') != 'Mozilla/5.0':
            self.send_error(403)
            return
        if self.path == '/missing':
            self.send_error(404)
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(
            b'<h1>Synthetic lighthouse</h1><p>Bright harbor <a href="https://example.test">map</a></p>'
        )

    def log_message(self, *args):
        pass


def test_link_spool_chain_and_worker_end_states(tmp_path):
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f'http://localhost:{server.server_port}/article#ignored'
        identifier, state = materialise(tmp_path, url)
        assert state == 'queued'
        assert materialise(tmp_path, url.removesuffix('#ignored'))[0] == identifier
        worker = Worker(tmp_path)
        with lock_path(queue_path(tmp_path, identifier)).open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            assert not worker.cycle()
        inline = subprocess.run(
            [
                sys.executable,
                '-m',
                'qmd2.cli',
                '--home',
                str(tmp_path),
                'get',
                url,
                '--materialise',
                '--json',
            ],
            capture_output=True,
            check=True,
        )
        assert json.loads(inline.stdout)['docs'][0]['id'] == identifier
        assert inline.stderr.decode().splitlines() == [
            'fetch',
            'archive',
            'type',
            'extract',
            'spool',
            'done',
        ]
        assert snapshot(tmp_path)['queued'] == 1
        daemon = Ingestd(tmp_path, remote=False)
        daemon.spool(10)
        daemon.commit()
        daemon.attachment_outcomes()
        assert snapshot(tmp_path)['done'] == 1
        hit = daemon.index.search('harbor')[0]
        assert hit['id'] == identifier
        doc = get_docs(tmp_path, identifier)[0]
        assert doc['title'] == 'Synthetic lighthouse'
        assert daemon.index.search('lighthouse')[0]['id'] == identifier
        assert '[map](https://example.test/)' in doc['parts'][1]['text']
        assert doc['pipeline_revision'] == PIPELINE_REVISION
        archived = [r for r in daemon.state.packs('raw')]
        originals = [
            body
            for pack in archived
            for _, head, body in daemon.packs.iter_pack(pack)
            if head['kind'] == 'archive-only'
        ]
        assert originals == [
            b'<h1>Synthetic lighthouse</h1><p>Bright harbor <a href="https://example.test">map</a></p>'
        ]
        assert not queue_path(tmp_path, identifier).exists()
        assert not list((tmp_path / 'attachments/pending').iterdir())
        assert not worker.cycle()
        assert not worker.cycle()
        assert not list((tmp_path / 'attachments/media').iterdir())
        assert materialise(tmp_path, url)[1] == 'done'
        again = subprocess.run(
            [
                sys.executable,
                '-m',
                'qmd2.cli',
                '--home',
                str(tmp_path),
                'get',
                url,
                '--materialise',
                '--json',
            ],
            capture_output=True,
            check=True,
        )
        assert again.stderr.decode().splitlines() == ['done']
        for value in (
            identifier,
            url.removesuffix('#ignored'),
            url,
            url.replace('http://localhost', 'HTTP://LOCALHOST'),
        ):
            ordinary = subprocess.run(
                [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path), 'get', value, '--json'],
                capture_output=True,
            )
            assert ordinary.returncode == 0, ordinary.stderr
            assert json.loads(ordinary.stdout)['docs'][0]['id'] == identifier
        parent = {'id': 'synthetic', 'source': 'x', 'group': 'articles', 'time': time.time()}
        enqueue(
            tmp_path,
            'synthetic/att/1',
            parent,
            {'source': 'x', 'url': url.replace('/article#ignored', '/missing'), 'type': 'image'},
        )
        worker.cycle()
        daemon.spool(10)
        daemon.commit()
        daemon.attachment_outcomes()
        assert snapshot(tmp_path)['unavailable'] == 1
        assert snapshot(tmp_path)['worker']['receipt']['state'] == 'unavailable'
        expired = 'synthetic/att/1'
        # A browser fetch that still fails is terminal across later backfills.
        enqueue(tmp_path, expired, parent, {'source': 'x'}, backfill=True)
        assert materialise(tmp_path, expired)[1] == 'unavailable'
        assert not queue_path(tmp_path, expired).exists()
        outcome = get_docs(tmp_path, expired)[0]
        assert outcome['kind'] == 'attachment_outcome' and outcome['parts'] == []
        assert not any(k in outcome for k in ('title', 'from', 'to', 'context', 'thread'))
        assert not daemon.index.search('media_expired')
        assert outcome['reason'] == 'media_expired (HTTP 404)'
        fetched = subprocess.run(
            [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path), 'get', expired],
            capture_output=True,
            check=True,
        )
        rendered = json.loads(fetched.stdout)
        assert rendered['state'] == 'unavailable'
        assert set(rendered) == {'id', 'state', 'reason', 'model', 'pipeline_revision', 'time'}
        assert expired not in {r['id'] for r in daemon.index.browse(source='attachment')}
        enqueue(tmp_path, 'synthetic/att/2', parent, {'source': 'x', 'url': 'bad', 'type': 'image'})
        for _ in range(2):
            path = queue_path(tmp_path, 'synthetic/att/2')
            retry = json.loads(path.read_text())
            retry['first_attempt_at'] = retry['last_attempt_at'] = time.time() - 181
            from qmd2.attachments import save

            save(path, retry)
            worker.cycle()
        assert json.loads(path.read_text())['attempts'] == 2
        daemon.spool(10)
        daemon.commit()
        daemon.attachment_outcomes()
        assert snapshot(tmp_path)['failed'] == 1
        assert not path.exists()
        dated_url = url.replace('/article#ignored', '/dated')
        parent = dict(
            id='gmail:synthetic:dated',
            item='gmail:synthetic:dated',
            source='gmail',
            group='mail',
            kind='email',
            time='2026-08-15T00:00:00Z',
            parts=[dict(kind='authored', text=dated_url)],
        )
        daemon.record('attachment', dumps(parent).encode(), dict(key=parent['id']))
        daemon.commit()
        parent = get_docs(tmp_path, parent['id'])[0]
        alias = parent['attachments'][0]['id']
        ceiling = dict(os.environ, QMD_UNTIL='2026-08-17T00:00:00Z')
        cmd = [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path)]
        inline = subprocess.run(
            cmd + ['get', alias, '--materialise', '--json'], env=ceiling, capture_output=True
        )
        assert inline.returncode == 0, inline.stderr
        output = json.loads(inline.stdout)['docs'][0]
        assert (
            output['time'] == parent['time']
            and output['id'] == parent['attachments'][0]['capture_id']
        )
        daemon.spool(10)
        daemon.commit()
        assert (
            subprocess.run(cmd + ['get', alias], env=ceiling, capture_output=True).returncode == 0
        )
        hidden = dict(ceiling, QMD_UNTIL='2026-08-14T00:00:00Z')
        assert (
            subprocess.run(
                cmd + ['get', alias, '--materialise'], env=hidden, capture_output=True
            ).returncode
            == 3
        )
        status = json.loads((tmp_path / 'status.json').read_text())
        status['attachments'] = snapshot(tmp_path)
        assert 'attachments:failure' in evaluate(status, time.time())
    finally:
        server.shutdown()
        server.server_close()


def test_converter_subprocess_limits_and_environment():
    payload = run(
        [
            sys.executable,
            '-c',
            'import json,os,resource; print(json.dumps([os.environ.get("HTTP_PROXY"),resource.getrlimit(resource.RLIMIT_CPU),resource.getrlimit(resource.RLIMIT_AS)]))',
        ]
    )
    assert json.loads(payload) == [None, [120, 120], [1024**3, 1024**3]]
    with pytest.raises(TimeoutError) as timeout:
        run([sys.executable, '-c', 'import time; time.sleep(5)'], timeout=0.1)
    from qmd2.attachments import temporary_refusal

    assert temporary_refusal(timeout.value) == ('permanent', 'converter deadline')


def test_authenticated_whatsapp_media_subprocess_edge():
    plaintext = b'Synthetic media original bytes'
    key = bytes(range(32))
    prk = hmac.digest(bytes(32), key, 'sha256')
    expanded, block = b'', b''
    for i in range(1, 5):
        block = hmac.digest(prk, block + b'WhatsApp Audio Keys' + bytes([i]), 'sha256')
        expanded += block
    iv, aes, mac = expanded[:16], expanded[16:48], expanded[48:80]
    cipher = run(
        ['openssl', 'enc', '-aes-256-cbc', '-K', aes.hex(), '-iv', iv.hex()], data=plaintext
    )
    encrypted = cipher + hmac.digest(mac, iv + cipher, 'sha256')[:10]

    def encode(body):
        return base64.b64encode(body).decode()

    ref = dict(
        type='audio',
        mediaKey=encode(key),
        fileLength=len(plaintext),
        fileSHA256=encode(hashlib.sha256(plaintext).digest()),
        fileEncSHA256=encode(hashlib.sha256(encrypted).digest()),
    )
    assert decrypt(ref, encrypted) == plaintext
    bad_tag = encrypted[:-1] + bytes([encrypted[-1] ^ 1])
    ref['fileEncSHA256'] = encode(hashlib.sha256(bad_tag).digest())
    with pytest.raises(ValueError):
        decrypt(ref, bad_tag)
    ref['fileEncSHA256'] = encode(hashlib.sha256(encrypted).digest())
    ref['fileSHA256'] = encode(bytes(32))
    with pytest.raises(ValueError):
        decrypt(ref, encrypted)


def test_terminal_alert_age_filesystem_edge(tmp_path):
    from qmd2.attachments import outcome_record, save

    now = time.time()
    for n, (state, automatic, age, backfill, type_, reason, expected) in enumerate(
        [
            ('unavailable', False, 1, False, 'audio', 'HTTPError', False),
            ('unavailable', True, 1, False, 'audio', 'HTTPError', True),
            ('failed', False, 86399, False, 'audio', 'HTTPError', True),
            ('failed', True, 86400, False, 'audio', 'HTTPError', False),
            ('unavailable', True, 86401, False, 'audio', 'HTTPError', False),
            ('done', True, 1, False, 'audio', 'HTTPError', False),
            ('failed', False, 1, True, 'image', 'HTTPError', False),
            ('unavailable', True, 1, False, 'image', 'HTTPError', False),
            ('unavailable', True, 1, False, 'audio', 'rate_limited', False),
            ('unavailable', True, 1, False, 'audio', 'remote_unavailable', False),
            ('unavailable', True, 1, False, 'audio', 'media_expired', False),
            ('failed', True, 1, False, 'audio', 'azure_auth', True),
            ('failed', True, 1, False, 'audio', 'OSError', True),
        ]
    ):
        home = tmp_path / str(n)
        daemon = Ingestd(home, remote=False)
        save(home / 'attachments/worker.json', {'heartbeat_at': now})
        row = dict(
            id='synthetic/att/0',
            state=state,
            automatic=automatic,
            backfill=backfill,
            ref={'type': type_},
            captured_at=now,
            enqueued_at=now - age - 10,
            ended_at=now - age,
            reason=reason,
            pipeline_revision=2,
            azure_pages=3,
            audio_seconds=120,
        )
        if state == 'done':
            row['output'] = dict(
                id=row['id'],
                item=row['id'],
                source='whatsapp',
                group='mail',
                kind='transcript',
                parts=[dict(kind='authored', text='Synthetic harbor')],
                time=now,
                pipeline_revision=2,
            )
        doc = outcome_record(row)
        daemon.record('attachment', dumps(doc).encode(), {'key': doc['id']})
        daemon.commit()
        status = json.loads((home / 'status.json').read_text())
        assert ('attachments:failure' in evaluate(status, now)) == expected
        assert status['attachments'][state] == 1
        failure = status['attachments']['last_failure']
        if status['attachments']['last_failure_at'] is not None:
            assert failure == dict(id=row['id'], reason=reason, at=row['ended_at'])
        else:
            assert failure is None
        assert status['attachments']['azure_pages'] == 3
        assert status['attachments']['audio_minutes'] == 2
        if state != 'done':
            assert daemon.source_status('attachment', None, daemon.index, now)['newest_id'] is None
            assert daemon.stats['docs'] == 0
            from qmd2.verify import add_document_metrics

            metrics = []
            add_document_metrics(home, now, metrics)
            assert all(m['value'] == 0 for m in metrics)
        assert not list((home / 'attachments/pending').glob('*.json'))
    for n, (raw, age) in enumerate(
        [(b'{', 1), (b'null', 1), (b'{"state":"unknown"}', 1), (b'{', 86401)]
    ):
        home = tmp_path / ('corrupt-' + str(n))
        daemon = Ingestd(home, remote=False)
        path = home / 'attachments/pending/synthetic.json'
        path.parent.mkdir(parents=True)
        path.write_bytes(raw)
        os.utime(path, (now - age, now - age))
        daemon.commit()
        status = json.loads((home / 'status.json').read_text())
        assert status['attachments']['failed'] == 0
        assert status['attachments']['queue_errors'] == 1
        assert 'attachments:queue' in evaluate(status, now)


@pytest.mark.parametrize(
    'body,kind',
    [
        (b'%PDF-1.7', 'pdf'),
        (b'\x89PNG\r\n\x1a\n', 'png'),
        (b'\xff\xd8\xff', 'jpeg'),
        (b'II*\0', 'tiff'),
        (b'MM\0*', 'tiff'),
        (b'GIF89a', 'gif'),
        (b'RIFF1234WEBP', 'webp'),
        (b'RIFF1234WAVE', 'wav'),
        (b'0000ftypheic', 'heif'),
        (b'0000ftypM4A ', 'm4a'),
        (b'0000ftypisom', 'mp4'),
        (b'OggS', 'ogg'),
        (b'ID3', 'mp3'),
        (b'\xff\xfb', 'mp3'),
        (b'PK\x03\x04', 'zip'),
        (b'<!DOCTYPE html>', 'html'),
        (b'\xef\xbb\xbf<html>body</html>', 'html'),
        (b'<!-- comment --><html>', 'html'),
        (b'<?xml version="1.0"?><html>', 'html'),
        (b'<meta charset="utf-8"><head>', 'html'),
        (b'<script>code</script><body>', 'html'),
        (b'<title>title</title><html>', 'html'),
        (b'<div>lead</div><!doctype html>', 'html'),
        (b'x' * 4096 + b'<html>', 'text'),
        (b'synthetic harbor text', 'text'),
        (b'\0binary', 'unknown'),
        (b'', 'unknown'),
    ],
)
def test_signature_table(body, kind):
    assert detect(body) == kind


def test_stream_filesystem_caps(tmp_path):
    import io

    target = tmp_path / 'media'
    assert stream(io.BytesIO(b'Synthetic harbor text'), target) == 'text'
    assert target.read_bytes() == b'Synthetic harbor text'
    import zipfile

    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as z:
        z.writestr('word/document.xml', '<document>Synthetic harbor</document>')
    assert stream(io.BytesIO(archive.getvalue()), target) == 'docx'
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as z:
        z.writestr('other.xml', '<document>Not DOCX</document>')
    for body in (archive.getvalue(), b'PK\x03\x04' + b'x' * 10000, b'\0binary'):
        with pytest.raises(Unavailable, match='unsupported_type'):
            stream(io.BytesIO(body), target)

    class LargeImage:
        remaining = 21 * 1024**2

        def read(self, size):
            size = min(size, self.remaining)
            self.remaining -= size
            return b'\x89PNG\r\n\x1a\n' + bytes(size - 8) if size >= 8 else bytes(size)

    with pytest.raises(Unavailable, match='too_large'):
        stream(LargeImage(), target)
    assert target.stat().st_size <= 20 * 1024**2


def test_audio_slicing_subprocess_edge(tmp_path):
    from qmd2.attachments import audio_slices

    tone = tmp_path / 'tone.ogg'
    run(
        [
            'ffmpeg',
            '-v',
            'error',
            '-f',
            'lavfi',
            '-i',
            'sine=frequency=440:sample_rate=16000',
            '-t',
            '601',
            '-c:a',
            'libopus',
            str(tone),
        ]
    )
    offsets = []
    for offset, duration, sliced in audio_slices(tone):
        offsets.append(offset)
        assert duration > 600 and sliced.read_bytes().startswith(b'OggS')
        info = json.loads(
            run(
                [
                    'ffprobe',
                    '-v',
                    'error',
                    '-show_streams',
                    '-show_format',
                    '-of',
                    'json',
                    str(sliced),
                ]
            )
        )
        assert info['streams'][0]['codec_name'] == 'opus'
        assert info['streams'][0]['channels'] == 1
        assert float(info['format']['duration']) <= 600.1
    assert offsets == [0, 600]


def test_manual_chain(tmp_path):
    from qmd2.attachments import outcome_record

    body = b'Synthetic zephyr lighthouse archive'
    identifier = 'manual:' + hashlib.sha256(body).hexdigest()
    daemon = Ingestd(tmp_path, remote=False)
    failed = outcome_record(
        dict(
            id=identifier,
            state='failed',
            reason='FileNotFoundError',
            pipeline_revision=2,
            ended_at=time.time(),
        )
    )
    daemon.record('manual', dumps(failed).encode(), {'key': identifier})
    daemon.commit()
    command = [
        sys.executable,
        '-m',
        'qmd2.cli',
        '--home',
        str(tmp_path),
        'add',
        '-',
        '--name',
        'sample.txt',
    ]
    result = subprocess.run(command, input=body, capture_output=True, check=True)
    assert identifier in result.stdout.decode()
    assert (
        json.loads(queue_path(tmp_path, identifier).read_text())['pipeline_revision']
        == PIPELINE_REVISION
    )
    daemon.spool(10)
    daemon.commit()
    assert daemon.index.search('zephyr', source='manual')[0]['id'] == identifier
    assert get_docs(tmp_path, identifier)[0]['source'] == 'manual'
    assert get_docs(tmp_path, identifier)[0]['pipeline_revision'] == PIPELINE_REVISION
    packs = list(daemon.state.packs('raw'))
    assert any(
        head['kind'] == 'archive-only' and raw == body
        for pack in packs
        for _, head, raw in daemon.packs.iter_pack(pack)
    )
    again = subprocess.run(command, input=body, capture_output=True, check=True)
    assert again.stderr.decode().splitlines() == ['done']
    daemon.attachment_outcomes()
    assert not queue_path(tmp_path, identifier).exists()
    daemon.reindex()
    assert get_docs(tmp_path, identifier)[0]['kind'] == 'text'
    assert snapshot(tmp_path)['done'] == 1 and snapshot(tmp_path)['failed'] == 0


def test_automatic_x_queue_filesystem_edge(tmp_path):
    parent = dict(id='x:synthetic:1', source='x', group='articles', kind='post')
    raw = dumps(
        {
            'bookmark': {
                'tweet': {
                    'media': [
                        {'type': 'photo', 'media_url_https': 'https://example.test/image'},
                        {'type': 'video', 'url': 'https://example.test/video'},
                    ]
                }
            }
        }
    ).encode()
    capture = int(time.time() * 1e9)
    assert enqueue_auto(tmp_path, raw, [parent], capture) == ['x:synthetic:1/att/0']
    row = json.loads(queue_path(tmp_path, 'x:synthetic:1/att/0').read_text())
    assert row['automatic'] and row['captured_at'] == capture / 1e9
    assert row['enqueued_at'] >= capture / 1e9
    assert snapshot(tmp_path)['oldest_queued_at'] == row['enqueued_at']


def test_fetch_redo_queue_filesystem_edge(tmp_path):
    from qmd2.attachments import outcome_record

    daemon = Ingestd(tmp_path, remote=False)
    parent = dict(id='synthetic', source='x', group='articles')
    for source in ('x', 'link', 'whatsapp'):
        identifier = 'synthetic/' + source
        row = dict(
            id=identifier,
            ref=dict(source=source),
            state='unavailable',
            reason='media_expired',
            pipeline_revision=PIPELINE_REVISION,
            ended_at=time.time(),
        )
        doc = outcome_record(row)
        daemon.record('attachment', dumps(doc).encode(), {'key': identifier})
        daemon.commit()
        state = enqueue(tmp_path, identifier, parent, row['ref'], backfill=True)
        path = queue_path(tmp_path, identifier)
        assert state == ('unavailable' if source == 'whatsapp' else 'queued')
        if source != 'whatsapp':
            pending = json.loads(path.read_text())
            assert pending['attempts'] == 0 and pending['pipeline_revision'] == PIPELINE_REVISION
            row.update(browser_fetch=True, retry_of=pending['retry_of'], ended_at=time.time())
            doc = outcome_record(row)
            daemon.record('attachment', dumps(doc).encode(), {'key': identifier})
            daemon.commit()
            path.unlink()
            assert enqueue(tmp_path, identifier, parent, row['ref'], backfill=True) == 'unavailable'
            assert not path.exists()
    # A new pipeline revision re-enumerates an older store outcome, not queue history.
    row.update(id='synthetic/old/att/0', ref=dict(source='whatsapp'), pipeline_revision=1)
    doc = outcome_record(row)
    daemon.record('attachment', dumps(doc).encode(), {'key': doc['id']})
    daemon.commit()
    assert enqueue(tmp_path, row['id'], parent, row['ref'], backfill=True) == 'queued'


def test_enqueue_during_extraction_subprocess_edge(tmp_path):
    from qmd2.attachments import save

    parent = dict(id='synthetic', source='x', group='articles')
    identifier = 'synthetic/locked'
    enqueue(tmp_path, identifier, parent, {'source': 'x'})
    path = queue_path(tmp_path, identifier)
    row = json.loads(path.read_text())
    row['pipeline_revision'] = 0
    save(path, row)
    with lock_path(path).open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        result = subprocess.run(
            [
                sys.executable,
                '-c',
                "from qmd2.attachments import enqueue; import sys; "
                "enqueue(sys.argv[1], 'synthetic/locked', {}, {'source':'x'}); "
                "enqueue(sys.argv[1], 'synthetic/locked', {}, {'source':'x'}, backfill=True)",
                str(tmp_path),
            ],
            capture_output=True,
            timeout=3,
        )
        assert result.returncode == 0
        assert json.loads(path.read_text()) == row


def test_interrupted_manual_filesystem_subprocess_edge(tmp_path):
    import io
    from qmd2.attachments import add, process, save

    body = b'Synthetic interrupted durable lighthouse'
    identifier = 'manual:' + hashlib.sha256(body).hexdigest()

    def interrupt(message):
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        add(tmp_path, io.BytesIO(body), 'sample.txt', None, interrupt)
    path = queue_path(tmp_path, identifier)
    row = json.loads(path.read_text())
    assert __import__('pathlib').Path(row['ref']['path']).read_bytes() == body
    row['first_attempt_at'] = row['last_attempt_at'] = time.time() - 181
    save(path, row)
    assert process(tmp_path, row)['state'] == 'done'
    row.update(state='failed', reason='FileNotFoundError', attempts=3)
    save(path, row)
    assert add(tmp_path, io.BytesIO(body), 'sample.txt', None, lambda _: None)['state'] == 'done'


def test_retry_delay_filesystem_edge(tmp_path):
    from qmd2.attachments import finish, save

    identifier = 'manual:retry'
    enqueue(tmp_path, identifier, {}, {'source': 'manual', 'path': str(tmp_path / 'missing')})
    path = queue_path(tmp_path, identifier)
    row = finish(tmp_path, {'id': identifier})
    assert row['state'] == 'queued' and row['attempts'] == 1
    assert finish(tmp_path, row)['attempts'] == 1
    assert not Worker(tmp_path).cycle()
    row['first_attempt_at'] -= 181
    row['last_attempt_at'] -= 181
    save(path, row)
    row = finish(tmp_path, row)
    assert row['state'] == 'failed' and row['attempts'] == 2
