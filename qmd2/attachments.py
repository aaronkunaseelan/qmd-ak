"""K8: open work in the queue; pack-backed outcomes through the spool."""

import argparse
import base64
import codecs
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import hmac
import io
import json
import math
import os
import resource
import re
import shutil
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from email import policy
from email.parser import BytesParser
from pathlib import Path

from .accounts import account
from .doc import Exclusion, date, dumps, visible
from .state import DEFAULT_HOME

CUTOFF = '2026-10-13T23:06:38Z'
PIPELINE_REVISION = 3
# One initial try and one retry at least three minutes later.
RETRY_SCHEDULE = (180,)
REMOTE_REFUSALS = frozenset(
    {
        'rate_limited',
        'remote_unavailable',
        'remote_blocked',
        'media_expired',
        'azure_rejected',
        'tls_error',
    }
)
LIMIT = 50 * 1024 * 1024
CAPS = dict(
    audio=512 * 1024**2,
    pdf=100 * 1024**2,
    image=20 * 1024**2,
    html=LIMIT,
    text=LIMIT,
    zip=100 * 1024**2,
)
SIGNATURES = (
    (b'%PDF-', 'pdf'),
    (b'\x89PNG\r\n\x1a\n', 'png'),
    (b'\xff\xd8\xff', 'jpeg'),
    (b'II*\0', 'tiff'),
    (b'MM\0*', 'tiff'),
    (b'GIF8', 'gif'),
    (b'OggS', 'ogg'),
    (b'ID3', 'mp3'),
    (b'PK\x03\x04', 'zip'),
)
TOOL = Path(account('transcriber_tool')).expanduser()


class Unavailable(Exception):
    pass


class Failed(Exception):
    pass


class ConverterTimeout(TimeoutError, Failed):
    pass


def detect(body):
    for signature, kind in SIGNATURES:
        if body.startswith(signature):
            return kind
    if body[:4] == b'RIFF':
        return {b'WEBP': 'webp', b'WAVE': 'wav'}.get(body[8:12], 'unknown')
    if body[4:8] == b'ftyp':
        return (
            'heif'
            if body[8:12] in (b'heic', b'heix', b'hevc', b'mif1', b'avif')
            else 'm4a'
            if body[8:12] in (b'M4A ', b'M4B ')
            else 'mp4'
        )
    if len(body) > 1 and body[0] == 255 and body[1] & 0xE0 == 0xE0:
        return 'mp3'
    prefix = body[:4096].removeprefix(codecs.BOM_UTF8).lstrip().lower()
    if any(
        tag in prefix for tag in (b'<html', b'<head', b'<body', b'<!doctype')
    ) or prefix.startswith((b'<h1', b'<p>')):
        return 'html'
    try:
        text = codecs.getincrementaldecoder('utf-8-sig')().decode(body, final=False)
        return 'text' if text and all(c.isprintable() or c in '\r\n\t' for c in text) else 'unknown'
    except UnicodeError:
        return 'unknown'


def route(kind):
    return (
        'image'
        if kind in ('png', 'jpeg', 'tiff', 'heif', 'gif', 'webp')
        else 'audio'
        if kind in ('ogg', 'mp3', 'm4a', 'wav')
        else kind
    )


def stream(reader, target, encrypted=False):
    first = reader.read(4096)
    kind = detect(first) if not encrypted else 'audio'
    cap = CAPS.get(route(kind))
    if cap is None:
        raise Unavailable('unsupported_type')
    if shutil.disk_usage(target.parent).free < 8 * 1024**3 + 2 * cap:
        raise OSError('disk floor')
    size = 0
    with target.open('wb') as output:
        chunk = first
        while chunk:
            size += len(chunk)
            if size > cap + (32 if encrypted else 0):
                raise Unavailable('too_large')
            output.write(chunk)
            chunk = reader.read(min(1024**2, cap + 33 - size))
    if kind == 'zip':
        try:
            with zipfile.ZipFile(target) as archive:
                if 'word/document.xml' not in archive.namelist():
                    raise Unavailable('unsupported_type')
        except zipfile.BadZipFile:
            raise Unavailable('unsupported_type') from None
        return 'docx'
    return kind


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as f:
        temp = Path(f.name)
        f.write((dumps(value) + '\n').encode())
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def publish_queue(path, row):
    root = path.parent
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(dir=root, delete=False) as f:
        temp = Path(f.name)
        f.write((dumps(row) + '\n').encode())
        f.flush()
        os.fsync(f.fileno())
    try:
        os.link(temp, path)
        directory = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except FileExistsError:
        pass
    finally:
        temp.unlink()


def queued_row(path):
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return None


def enqueue(home, identifier, parent, ref, automatic=False, captured_at=None, backfill=False):
    root = Path(home) / 'attachments/pending'
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = queue_path(home, identifier)
    stored = stored_outcome(home, identifier, output=False)
    redo_fetch = (
        stored
        and ref['source'] in ('x', 'link')
        and stored.get('reason') == 'media_expired'
        and not stored.get('browser_fetch')
    )
    if stored and (
        not backfill or (stored['pipeline_revision'] >= PIPELINE_REVISION and not redo_fetch)
    ):
        return stored['state']
    if row := queued_row(path):
        return row['state']
    row = dict(
        id=identifier,
        parent={
            k: parent[k]
            for k in (
                'id',
                'source',
                'group',
                'time',
                'from',
                'to',
                'context',
                'thread',
                'raw',
                'title',
            )
            if k in parent
        },
        ref=ref,
        automatic=automatic,
        backfill=backfill,
        enqueued_at=time.time(),
        captured_at=time.time() if captured_at is None else captured_at,
        attempts=0,
        state='queued',
        pipeline_revision=max(PIPELINE_REVISION, stored['pipeline_revision'] if stored else 0),
        **({'retry_of': stored['outcome_hash']} if stored else {}),
    )
    publish_queue(path, row)
    return 'queued'


def cutoff(home):
    path = Path(home) / 'attachments/cutoff-v2'
    return path.read_text().strip() if path.exists() else CUTOFF


def media_refs(source, raw):
    envelope = json.loads(raw)
    if source == 'whatsapp':
        return envelope.get('decoded', {}).get('media') or []
    for wrapper in ('tweet', 'bookmark', 'like'):
        if isinstance(envelope.get(wrapper), dict):
            envelope = envelope[wrapper]
            break
    if isinstance(envelope.get('tweet'), dict):
        envelope = envelope['tweet']
    legacy = envelope.get('legacy') or {}
    return envelope.get('media') or (
        envelope.get('extended_entities')
        or legacy.get('extended_entities')
        or envelope.get('entities')
        or {}
    ).get('media', [])


def enqueue_auto(home, raw, docs, capture, automatic=True):
    if not docs:
        return []
    source = docs[0]['source']
    envelope = json.loads(raw)
    original = envelope.get('raw') or {}
    captured = capture / 1e9
    if source == 'whatsapp' and isinstance(original, dict):
        if automatic and ('history_sha256' in original or 'legacy_db_sha256' in original):
            return []
        if original.get('received_at'):
            captured = date(original['received_at']).timestamp()
    if automatic and captured <= date(cutoff(home)).timestamp():
        return []
    identifiers = []
    for doc in docs:
        if doc.get('parse_failed'):
            continue
        for pos, ref in enumerate(media_refs(source, raw)):
            if not (
                source == 'whatsapp'
                and ref.get('type') in ('audio', 'image')
                and ref.get('mimetype') != 'image/gif'
                or source == 'x'
                and ref.get('type') == 'photo'
            ):
                continue
            identifier = f"{doc['id']}/att/{pos}"
            if source == 'x':
                ref = dict(ref, url=ref.get('media_url_https') or ref.get('url'))
            enqueue(
                home, identifier, doc, dict(ref, source=source), automatic, captured, not automatic
            )
            identifiers.append(identifier)
    return identifiers


def outcome_record(row):
    """No-text records have no parent headers or content; metrics live with the outcome."""
    meta = {
        k: row[k]
        for k in (
            'id',
            'state',
            'reason',
            'model',
            'pipeline_revision',
            'ended_at',
            'attempts',
            'automatic',
            'backfill',
            'captured_at',
            'browser_fetch',
            'fetched',
            'azure_pages',
            'audio_seconds',
            'retry_of',
        )
        if k in row
    }
    meta.setdefault('pipeline_revision', 1)
    meta.setdefault('ended_at', row.get('enqueued_at', row.get('queued_at', 0)))
    meta['voice_note'] = row.get('ref', {}).get('type') == 'audio'
    if row['state'] == 'done':
        doc = dict(row['output'])
    else:
        doc = dict(
            id=row['id'],
            item=row['id'],
            source='manual' if row['id'].startswith('manual:') else 'attachment',
            group='files',
            kind='attachment_outcome',
            parts=[],
            parser='attachment@1',
            time=meta['ended_at'],
            state=row['state'],
            reason=row.get('reason'),
            model=row.get('model'),
            pipeline_revision=meta['pipeline_revision'],
        )
    doc['outcome'] = meta
    return doc


def stored_outcome(home, identifier, output=True):
    from .reads import Reader

    if not (Path(home) / 'index').exists():
        return None
    reader = Reader(home)
    try:
        stored = reader.index.lookup(identifier)
        rows = [stored] if stored else reader.index.item(identifier)
        if not rows:
            return None
        stored = max(rows, key=lambda r: r['version'].get('revision') or 0)
        if not visible(stored):
            raise LookupError()
        revision = stored['version'].get('revision')
        if revision is None:
            return None
        meta = dict(stored['version'].get('attachment', {}))
        meta.update(
            id=stored['item'],
            state=meta.get('state', 'done'),
            pipeline_revision=revision,
            outcome_hash=stored['version']['hash'],
        )
        doc = reader.read(stored) if output else None
        if meta['state'] == 'done':
            meta['output_id'] = stored['id']
            if output:
                meta['output'] = doc
        return meta
    finally:
        reader.close()


def matches_outcome(stored, row):
    return (
        stored.get('id') == row['id']
        and stored.get('pipeline_revision', 0) >= row.get('pipeline_revision', 1)
        and (
            not row.get('retry_of')
            or stored.get('outcome_hash') != row['retry_of']
            or stored.get('retry_of') == row['retry_of']
        )
    )


def remove_queue(path):
    path.unlink(missing_ok=True)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def snapshot(home, index=None):
    from .index import Index
    from .state import State
    import tantivy

    result = dict(
        cutoff=cutoff(home),
        pipeline_revision=PIPELINE_REVISION,
        queued=0,
        done=0,
        failed=0,
        unavailable=0,
        oldest_queued_at=None,
        last_failure_at=None,
        last_failure=None,
        unavailable_by_reason={},
        backfill=0,
        azure_pages=0,
        audio_minutes=0,
        queue_errors=0,
    )
    if index is None and (Path(home) / 'index').exists():
        index = Index(home)
    if index is not None:
        query = tantivy.Query.boolean_query(
            [
                (tantivy.Occur.Should, index.term('kind', kind))
                for kind in ('attachment_outcome', 'text', 'ocr', 'transcript', 'meeting')
            ]
        )
        for stored in index.scan(query=query):
            row = stored['version'].get('attachment')
            if not row:
                continue
            state = row['state']
            result[state] += 1
            result['backfill'] += bool(row.get('backfill'))
            result['azure_pages'] += row.get('azure_pages', 0)
            result['audio_minutes'] += row.get('audio_seconds', 0) / 60
            if state == 'unavailable':
                reason = row.get('reason', 'unknown')
                result['unavailable_by_reason'][reason] = (
                    result['unavailable_by_reason'].get(reason, 0) + 1
                )
            live = (
                row.get('automatic') and row.get('captured_at', 0) > date(cutoff(home)).timestamp()
            )
            if not row.get('backfill') and (
                state == 'failed'
                or state == 'unavailable'
                and not remote_refusal(row.get('reason'))
                and live
                and row.get('voice_note')
            ):
                if result['last_failure_at'] is None or row['ended_at'] > result['last_failure_at']:
                    result['last_failure_at'] = row['ended_at']
                    result['last_failure'] = dict(
                        id=row['id'], reason=row.get('reason'), at=row['ended_at']
                    )
    # Pending rows, including outputs awaiting ingest acknowledgement, are open work.
    for path in (Path(home) / 'attachments/pending').glob('*.json'):
        try:
            row = json.loads(path.read_text())
            if row['state'] not in ('queued', 'done', 'failed', 'unavailable') or not row['id']:
                raise ValueError('invalid pending row')
            result['queued'] += 1
            if row.get('automatic') and row.get('captured_at', 0) > date(cutoff(home)).timestamp():
                stamp = row.get('enqueued_at', row.get('queued_at', 0))
                if row['state'] == 'queued' and remote_refusal(row.get('reason')):
                    stamp = max(stamp, next_attempt_at(row) or stamp)
                result['oldest_queued_at'] = min(result['oldest_queued_at'] or stamp, stamp)
        except FileNotFoundError:
            continue
        except (ValueError, KeyError, TypeError):
            result['queue_errors'] += 1
    result['migration'] = {'complete': False, 'migrated': 0}
    if (Path(home) / 'state.sqlite').exists():
        state = State(home)
        try:
            result['migration'] = state.get('attachment-migration', result['migration'])
        finally:
            state.db.close()
    try:
        result['worker'] = json.loads((Path(home) / 'attachments/worker.json').read_text())
    except (OSError, ValueError):
        result['worker'] = None
    return result


def parse(raw, ctx):
    try:
        doc = json.loads(raw)
        if not isinstance(doc, dict) or not doc.get('id') or not isinstance(doc.get('parts'), list):
            raise ValueError()
        return [doc]
    except (ValueError, TypeError):
        return [
            dict(
                id=ctx['key'],
                item=ctx['key'],
                source='attachment',
                group='files',
                kind='binary',
                time=ctx.get('time'),
                parser='attachment@1',
                parse_failed=True,
                parts=[{'kind': 'binary', 'text': 'Unreadable attachment output'}],
            )
        ]


def spool(home, identifier, body, archive=False):
    lane = 'manual' if identifier.startswith('manual:') else 'attachment'
    root = Path(home) / 'spool' / lane
    objects = root / 'objects'
    objects.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(dir=objects, delete=False) as f:
        path = Path(f.name)
        if isinstance(body, Path):
            with body.open('rb') as source:
                shutil.copyfileobj(source, f, 1024**2)
        else:
            f.write(body)
        f.flush()
        os.fsync(f.fileno())
    with path.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    entry = dict(
        schema='qmd.spool-entry.v1',
        source=lane,
        account='',
        key=identifier,
        sha256=digest,
        bytes=path.stat().st_size,
        path=str(path.relative_to(root)),
        captured_at=time.time(),
        state={'labels': {'archive_only': 'true'} if archive else {}},
    )
    with (root / 'index.jsonl').open('a') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(dumps(entry) + '\n')
        f.flush()
        os.fsync(f.fileno())
    return path


def limits():
    resource.setrlimit(resource.RLIMIT_AS, (1024**3, 1024**3))
    resource.setrlimit(resource.RLIMIT_CPU, (120, 120))
    resource.setrlimit(resource.RLIMIT_FSIZE, (LIMIT, LIMIT))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def run(argv, timeout=120, data=None, azure=False):
    env = {
        'HOME': str(Path.home()),
        'PATH': '/usr/local/bin:/usr/bin:/bin',
        'LANG': 'C.UTF-8',
        'OMP_THREAD_LIMIT': '1',
        'OPENBLAS_NUM_THREADS': '1',
    }
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        import signal

        p = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
            stdout=output,
            stderr=errors,
            env=env,
            preexec_fn=limits,
            start_new_session=True,
        )
        try:
            p.communicate(data, timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait()
            raise ConverterTimeout('converter deadline')
        if p.returncode:
            if azure:
                errors.seek(0)
                detail = errors.read(1024**2).decode(errors='replace')
                if 'Missing Azure' in detail or re.search(r'(?:error|HTTP) (?:401|403)\b', detail):
                    raise Failed('azure_auth_or_config')
                if re.search(r'(?:error|HTTP) 4(?:00|04|10|13|15|22)\b', detail):
                    raise Unavailable('azure_rejected')
                status = re.search(r'\b(?:error|HTTP)\s+(429|5\d{2})\b', detail, re.IGNORECASE)
                if status:
                    raise urllib.error.HTTPError(
                        '', int(status[1]), 'Azure temporary refusal', None, None
                    )
            raise OSError('converter failed')
        output.seek(0)
        body = output.read(LIMIT + 1)
        if len(body) > LIMIT:
            raise Unavailable('too_large')
        return body


def request(url, body=None, headers=None, timeout=60):
    req = urllib.request.Request(url, data=body, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        result = response.read(LIMIT + 1)
        if len(result) > LIMIT:
            raise Unavailable('too_large')
        return result, response.headers


def b64(value):
    return base64.b64decode(value, validate=True)


def media_keys(media):
    info = {
        'audio': b'WhatsApp Audio Keys',
        'image': b'WhatsApp Image Keys',
        'document': b'WhatsApp Document Keys',
        'sticker': b'WhatsApp Image Keys',
    }[media['type']]
    key = b64(media['mediaKey'])
    if len(key) != 32:
        raise ValueError('media key invalid')
    prk, expanded, block = hmac.digest(bytes(32), key, 'sha256'), b'', b''
    for counter in range(1, 5):
        block = hmac.digest(prk, block + info + bytes([counter]), 'sha256')
        expanded += block
    iv, aes, mac = expanded[:16], expanded[16:48], expanded[48:80]
    return iv, aes, mac


def decrypt(media, encrypted, target=None):
    source = io.BytesIO(encrypted) if isinstance(encrypted, bytes) else encrypted.open('rb')
    with source:
        source.seek(0, 2)
        size = source.tell()
        if size < 26 or size > CAPS['audio'] + 32:
            raise ValueError('media size invalid')
        iv, aes, mac = media_keys(media)
        source.seek(0)
        digest, auth = hashlib.sha256(), hmac.new(mac, iv, 'sha256')
        with tempfile.TemporaryFile() as cipher:
            remaining = size - 10
            while remaining:
                chunk = source.read(min(1024**2, remaining))
                remaining -= len(chunk)
                digest.update(chunk)
                auth.update(chunk)
                cipher.write(chunk)
            tag = source.read(10)
            digest.update(tag)
            if digest.digest() != b64(media['fileEncSHA256']) or not hmac.compare_digest(
                auth.digest()[:10], tag
            ):
                raise ValueError('media authentication')
            cipher.seek(0)
            argv = ['openssl', 'enc', '-d', '-aes-256-cbc', '-K', aes.hex(), '-iv', iv.hex()]
            if target is None:
                body = run(argv, data=cipher.read())
                digest, length = hashlib.sha256(body).digest(), len(body)
            else:
                with subprocess.Popen(
                    argv,
                    stdin=cipher,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    preexec_fn=limits,
                ) as child:
                    try:
                        stream(child.stdout, target)
                    finally:
                        child.stdout.close()
                    if child.wait(timeout=120):
                        raise OSError('media decryption')
                with target.open('rb') as plaintext:
                    digest = hashlib.file_digest(plaintext, 'sha256').digest()
                length = target.stat().st_size
            if digest != b64(media['fileSHA256']) or (
                media.get('fileLength') and length != int(media['fileLength'])
            ):
                raise ValueError('media plaintext checksum')
            return body if target is None else target


def raw_parent(home, parent):
    from .packs import Packs
    from .state import State
    from .b2 import B2

    state = State(home)
    packs = Packs(state)
    try:
        try:
            return packs.read(parent['raw'])[1]
        except FileNotFoundError:
            packs.b2 = B2()
            return packs.read(parent['raw'])[1]
    finally:
        state.db.close()


def normalized_url(url):
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme.lower() not in ('http', 'https')
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        raise ValueError('link requires HTTP URL')
    host = parsed.hostname.lower()
    if ':' in host:
        host = '[' + host + ']'
    if parsed.port and parsed.port != (443 if parsed.scheme.lower() == 'https' else 80):
        host += ':' + str(parsed.port)
    return urllib.parse.urlunsplit(
        (parsed.scheme.lower(), host, parsed.path or '/', parsed.query, '')
    )


def materialise_link(home, url, pages, parent):
    url = normalized_url(url)
    identifier = 'link:' + hashlib.sha256(url.encode()).hexdigest()
    stored = stored_outcome(home, identifier, output=False)
    if stored:
        return identifier, stored['state']
    queued = queue_path(home, identifier)
    if row := queued_row(queued):
        if not visible(row['parent']):
            raise LookupError()
        return identifier, row['state']
    parent = (
        parent
        or link_parent(home, url)
        or dict(id=identifier, source='attachment', group='files', time=time.time(), title=url)
    )
    if not visible(parent):
        raise LookupError()
    return identifier, enqueue(home, identifier, parent, dict(source='link', url=url, pages=pages))


def materialise(home, identifier, pages=None, parent=None):
    from .cli import get_docs

    if '/att/link-' in identifier:
        parent_id = identifier.rsplit('/att/', 1)[0]
        parent = get_docs(home, parent_id)[0]
        ref = next(
            (a for a in parent.get('attachments', []) if a['id'] == identifier and a.get('url')),
            None,
        )
        if not ref:
            raise LookupError()
        return materialise(home, ref['url'], pages, parent)
    stored = stored_outcome(home, identifier, output=False)
    if stored:
        return identifier, stored['state']
    queued = queue_path(home, identifier)
    if row := queued_row(queued):
        if not visible(row['parent']):
            raise LookupError()
        return identifier, row['state']
    if identifier.startswith(('http://', 'https://')):
        return materialise_link(home, identifier, pages, parent)
    if '/att/' not in identifier:
        get_docs(home, identifier)
        raise LookupError()
    parent_id, position = identifier.rsplit('/att/', 1)
    parent = get_docs(home, parent_id)[0]
    attachment = next((a for a in parent.get('attachments', []) if a['id'] == identifier), None)
    if not attachment:
        raise LookupError()
    source = parent['source']
    if source == 'whatsapp':
        ref = json.loads(raw_parent(home, parent))['decoded']['media'][int(position)]
    elif source == 'gmail':
        ref = dict(position=int(position), type=attachment['type'])
    elif source == 'x':
        photo = media_refs('x', raw_parent(home, parent))[int(position)]
        ref = dict(
            url=photo.get('media_url_https') or photo.get('url') or attachment['name'], type='photo'
        )
    else:
        raise Unavailable('unsupported_source')
    return identifier, enqueue(home, identifier, parent, dict(ref, source=source, pages=pages))


def link_parent(home, url):
    from .reads import Reader

    if not (Path(home) / 'index').exists():
        return None
    reader = Reader(home)
    try:
        # Match the complete URL in packs, not just lexical candidates or snippets.
        words = re.findall(r'\w+', url)
        query = reader.index.index.parse_query(
            ' '.join('"' + w + '"' for w in words), ['body'], conjunction_by_default=True
        )
        parents = [
            d
            for d in reader.scan(query=query)
            if url in '\n'.join(p.get('text', '') for p in d.get('parts', []))
            and d['source'] not in ('attachment', 'manual')
            and not d['id'].startswith('link:')
        ]
        return min(parents, key=lambda d: (date(d['time']), d['id'])) if parents else None
    finally:
        reader.close()


def fetch_url(row, target, open_=None, relay=None, started=None):
    started = time.monotonic() if started is None else started
    via = 'mullvad' if relay else 'direct'
    attempt = dict(via=via, relay=relay, status=None, error=None, elapsed_s=0)
    attempts = row.get('fetched', {}).get('attempts', [])
    attempts.append(attempt)
    url = normalized_url(row['ref']['url'])
    row['fetched'] = dict(via=via, status=None, final_url=url, attempts=attempts)
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        open_ = open_ or urllib.request.build_opener(urllib.request.ProxyHandler({})).open
        with open_(req, timeout=60) as response:
            attempt['status'] = response.status
            row['fetched'].update(status=response.status, final_url=response.geturl())
            stream(response, target)
    except Exception as error:
        if isinstance(error, urllib.error.HTTPError):
            attempt['status'] = error.code
            row['fetched'].update(status=error.code, final_url=error.geturl())
        attempt['error'] = temporary_refusal(error)[1]
        raise
    finally:
        attempt['elapsed_s'] = round(time.monotonic() - started, 3)
    return target


def fetch_media(home, row, target):
    ref = row['ref']
    if ref['source'] == 'whatsapp':
        path = ref.get('directPath', '')
        if not path.startswith('/') or path.startswith('//'):
            raise Unavailable('no_direct_path')
        query = urllib.parse.urlencode(
            {
                'hash': base64.urlsafe_b64encode(b64(ref['fileEncSHA256'])).decode(),
                'mms-type': ref['type'],
                '__wa-mms': '',
            }
        )
        url = 'https://mmg.whatsapp.net' + path + ('&' if '?' in path else '?') + query
        request_ = urllib.request.Request(
            url,
            headers={'Origin': 'https://web.whatsapp.com', 'Referer': 'https://web.whatsapp.com/'},
        )
        encrypted = target.with_suffix('.encrypted')
        try:
            with urllib.request.urlopen(request_, timeout=60) as response:
                stream(response, encrypted, encrypted=True)
            decrypt(ref, encrypted, target)
        finally:
            encrypted.unlink(missing_ok=True)
    elif ref['source'] == 'gmail':
        raw = json.loads(raw_parent(home, row['parent']))
        encoded = raw['raw']
        mime = base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4))
        part = list(BytesParser(policy=policy.default).parsebytes(mime).walk())[ref['position']]
        stream(io.BytesIO(part.get_payload(decode=True) or b''), target)
    elif ref['source'] in ('x', 'link'):
        row['browser_fetch'] = True
        fetch_url(row, target)
    elif ref['source'] == 'manual':
        with Path(ref['path']).open('rb') as source:
            stream(source, target)
    else:
        raise Unavailable('unsupported_source')
    return target


def fetch(home, row, target):
    try:
        try:
            return fetch_media(home, row, target)
        except Exception as error:
            category, _ = temporary_refusal(error)
            if row['ref']['source'] not in ('x', 'link') or category not in (
                'blocked',
                'temporary',
            ):
                raise
            first_category, last_error = category, error
        from . import mullvad

        secret = mullvad.credentials()
        for relay in mullvad.select_relays()[:2]:
            started = time.monotonic()
            attempts = row['fetched']['attempts']
            before = len(attempts)
            try:
                with mullvad.tunnel(relay, secret) as open_:
                    return fetch_url(row, target, open_, relay['hostname'], started)
            except Exception as error:
                category, reason = temporary_refusal(error)
                if isinstance(error, Unavailable) and reason.startswith('shared_key_relay_'):
                    raise
                if len(attempts) == before:
                    attempts.append(
                        dict(
                            via='mullvad',
                            relay=relay['hostname'],
                            status=None,
                            error=reason,
                            elapsed_s=round(time.monotonic() - started, 3),
                        )
                    )
                    row['fetched'].update(via='mullvad', status=None)
                if category not in ('blocked', 'temporary'):
                    raise
                last_error = error
        if first_category == 'blocked':
            raise Unavailable(temporary_refusal(last_error)[1])
        raise last_error
    except Exception as error:
        category, reason = temporary_refusal(error)
        if category != 'temporary' and remote_refusal(reason):
            raise Unavailable(reason) from None
        raise


def env_file(path):
    result = {}
    if Path(path).exists():
        for line in Path(path).read_text().splitlines():
            key, sep, value = line.removeprefix('export ').partition('=')
            if sep and not key.lstrip().startswith('#'):
                result[key.strip()] = value.strip().strip('"\'')
    return result


def audio_slices(path, step=lambda message: None):
    duration = float(
        run(
            [
                'ffprobe',
                '-v',
                'error',
                '-show_entries',
                'format=duration',
                '-of',
                'default=noprint_wrappers=1:nokey=1',
                str(path),
            ]
        )
        .decode()
        .strip()
    )
    if not math.isfinite(duration) or duration <= 0:
        raise Unavailable('invalid_duration')
    count = math.ceil(duration / 600)
    with tempfile.TemporaryDirectory(dir=path.parent) as temporary:
        for index in range(count):
            step(f'slice {index + 1}/{count}')
            sliced = Path(temporary) / 'slice.ogg'
            run(
                [
                    'ffmpeg',
                    '-v',
                    'error',
                    '-y',
                    '-ss',
                    str(index * 600),
                    '-i',
                    str(path),
                    '-t',
                    '600',
                    '-vn',
                    '-ac',
                    '1',
                    '-ar',
                    '16000',
                    '-c:a',
                    'libopus',
                    str(sliced),
                ]
            )
            yield index * 600, duration, sliced


def transcribe(path):
    output = run(
        [sys.executable, str(TOOL), str(path), '--raw', '--model', 'mini', '--jobs', '1'],
        timeout=360,
        azure=True,
    )
    text = output.decode().strip()
    if not text:
        raise OSError('empty transcription')
    revision = run(['git', '-C', str(TOOL.parent), 'rev-parse', 'HEAD']).decode().strip()
    settings = {}
    for p in (
        TOOL.with_name('.env'),
        Path(account('transcription_env')).expanduser(),
        Path.home() / '.config/agent-secrets/azure-openai-transcription.env',
    ):
        for key, value in env_file(p).items():
            settings.setdefault(key, value)
    deployment = settings.get('TRANSCRIBE_AUDIO_MODEL_MINI') or settings.get(
        'AZURE_OPENAI_TRANSCRIPTION_MINI_DEPLOYMENT'
    )
    if not deployment:
        raise OSError('deployment label missing')
    return text, deployment + '@' + revision


def azure_di(path, model, pages=None):
    env = env_file('/etc/qmd-agent/qmd-agent.env')
    env.update(env_file(Path.home() / '.config/agent-secrets/credentials.env'))
    endpoint = env.get('QMD_DOCUMENT_INTELLIGENCE_ENDPOINT')
    try:
        key = (
            (Path.home() / '.config/agent-secrets/azure-document-intelligence.key')
            .read_text()
            .strip()
        )
    except OSError:
        raise Failed('azure_config') from None
    if not endpoint or not key:
        raise Failed('azure_config')
    headers = {'Ocp-Apim-Subscription-Key': key, 'Content-Type': 'application/octet-stream'}
    url = (
        endpoint.rstrip('/')
        + '/documentintelligence/documentModels/'
        + model
        + ':analyze?api-version=2024-11-30'
    )
    if pages is not None:
        url += '&' + urllib.parse.urlencode({'pages': f'1-{pages}'})
    try:
        _, response = request(url, path.read_bytes(), headers)
        operation = response['Operation-Location']
        if (
            urllib.parse.urlsplit(operation).netloc != urllib.parse.urlsplit(endpoint).netloc
            or urllib.parse.urlsplit(operation).scheme != 'https'
        ):
            raise Failed('azure_operation_host')
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            payload, _ = request(operation, headers=headers, timeout=30)
            result = json.loads(payload)
            if result['status'] == 'succeeded':
                analysis = result['analyzeResult']
                return (
                    analysis.get('content', ''),
                    'azure-document-intelligence:' + model + '@2024-11-30',
                    len(analysis.get('pages', [])),
                )
            if result['status'] == 'failed':
                raise Unavailable('azure_rejected')
            time.sleep(1)
        raise TimeoutError('Azure deadline')
    except urllib.error.HTTPError as error:
        if error.code in (401, 403):
            raise Failed('azure_auth') from None
        if 400 <= error.code < 500 and error.code != 429:
            if pages is not None and error.code == 400:
                detail = error.read(LIMIT).decode(errors='replace')
                if 'page range is unsupported' in detail.lower():
                    raise Unavailable('pages_unsupported') from None
            raise Unavailable('azure_rejected') from None
        raise


def extract(row, path, step=lambda message: None):
    with path.open('rb') as source:
        kind = route(detect(source.read(4096)))
    if kind == 'audio':
        pieces = []
        for offset, row['audio_seconds'], sliced in audio_slices(path, step):
            step('extract')
            text, model = transcribe(sliced)
            pieces.append(f'[{offset // 60:02d}:{offset % 60:02d}] {text}')
        return '\n\n'.join(pieces), model, 'transcript'
    step('extract')
    if kind == 'zip':
        with zipfile.ZipFile(path) as archive:
            if 'word/document.xml' not in archive.namelist():
                raise Unavailable('unsupported_type')
        kind = 'docx'
    if kind in ('pdf', 'image', 'docx'):
        pages = row['ref'].get('pages') if kind in ('pdf', 'docx') else None
        row['total_pages'] = page_count(path, kind)
        text, model, row['azure_pages'] = azure_di(
            path, 'prebuilt-layout' if kind in ('pdf', 'docx') else 'prebuilt-read', pages
        )
        if pages is None:
            row['total_pages'] = row['azure_pages']
        row['pages_requested'] = pages
        row['model'] = model
        if len(re.findall(r'[^\W\d_]{3,}', text)) < 3:
            raise Unavailable('no_text')
        return text, model, 'ocr' if kind == 'image' else 'text'
    text = path.read_bytes().decode('utf-8-sig', errors='replace')
    if kind == 'html':
        from .links import share_call, meeting_text

        share = share_call(row['ref'].get('url', ''), text)
        if share:
            call, props, endpoint = share
            transcript, _ = request(endpoint, headers={'User-Agent': 'Mozilla/5.0'})
            transcript = transcript.decode('utf-8-sig')
            if not transcript.strip() or transcript.lstrip().startswith('<'):
                raise Unavailable('transcript_unavailable')
            if 'id' in row:
                spool(
                    Path(path).parents[2], row['id'] + '/transcript-raw', transcript.encode(), True
                )
            row['meeting'] = dict(
                title=call.get('title') or call.get('topic'),
                started_at=call.get('started_at') or call.get('recording', {}).get('started_at'),
                host=call.get('host'),
                attendees=call.get('attendees') or props.get('attendees'),
            )
            return meeting_text(call, props, transcript), 'fathom-share@1', 'meeting'
        if row['ref'].get('source') != 'gmail':
            bundle = Path(__file__).with_name('defuddle.cjs')
            if not bundle.exists():
                bundle = Path(__file__).parents[1] / 'vendor/defuddle.cjs'
            article = json.loads(
                run(
                    ['node', '--v8-pool-size=1', '--max-old-space-size=256', str(bundle)],
                    timeout=15,
                    data=text.encode(),
                )
            )
            row['metadata'] = {key: article[key] for key in ('title', 'author', 'published')}
            if not article['text'].strip():
                raise Unavailable('no_text')
            return article['text'], 'defuddle@0.19.5', 'text'
        from .sources.gmail import BodyHTML

        html = BodyHTML()
        html.feed(text)
        text = ''.join(value for _, value in html.chunks)
    return text, kind, 'text'


def page_count(path, kind):
    if kind == 'pdf':
        try:
            match = re.search(rb'^Pages:\s+(\d+)', run(['pdfinfo', str(path)]), re.M)
            return int(match[1]) if match else None
        except (OSError, TimeoutError):
            return None
    return None


def attachment_title(row, kind):
    parent = row['parent']
    return (
        (
            'voice note from '
            + str(parent.get('from', ''))
            + ' in '
            + str(parent.get('context', {}).get('chat', parent.get('to', '')))
        )
        if kind == 'transcript' and parent['source'] == 'whatsapp'
        else parent.get('title') or row['ref'].get('fileName') or row['id']
    )


def output_doc(row, text, model, kind):
    parent = row['parent']
    identifier = row['id'] + (
        ''
        if parent['source'] == 'manual'
        else '/transcript'
        if kind == 'transcript'
        else '/ocr'
        if kind == 'ocr'
        else ''
    )
    title = attachment_title(row, kind)
    doc = {
        k: parent[k]
        for k in ('source', 'group', 'time', 'from', 'to', 'context', 'thread')
        if k in parent
    }
    doc.update(
        id=identifier,
        item=row['id'],
        kind=kind,
        title=title,
        parser='attachment@1',
        pipeline_revision=row.get('pipeline_revision', PIPELINE_REVISION),
        model=model,
        parts=[
            {
                'kind': 'header',
                'text': f"Model: {model}; pipeline revision: {row.get('pipeline_revision', PIPELINE_REVISION)}",
            },
            {'kind': 'authored' if kind == 'transcript' else 'attachment_text', 'text': text},
        ],
        context={
            **parent.get('context', {}),
            'parent': parent['id'],
            'sender': parent.get('from', ''),
        },
        media_sha256=row['media_sha256'],
    )
    if 'azure_pages' in row:
        doc['pages'] = dict(
            covered=row['azure_pages'],
            total=row.get('total_pages'),
            requested=row.get('pages_requested'),
        )
        doc['parts'][0]['text'] += (
            f"; Pages: {row['azure_pages']} of {row.get('total_pages') or 'unknown'}"
        )
    if row['ref']['source'] == 'link':
        doc['link'] = row['ref']['url']
        doc['title'] = row.get('metadata', {}).get('title') or title
    if row.get('meeting'):
        doc['meeting'] = row['meeting']
        doc['title'] = row['meeting'].get('title') or title
        doc['from'] = (row['meeting'].get('host') or {}).get('email', '')
    return doc


def queue_path(home, identifier):
    return (
        Path(home)
        / 'attachments/pending'
        / (hashlib.sha256(identifier.encode()).hexdigest() + '.json')
    )


def lock_path(path):
    root = path.parent.parent / 'locks'
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    return root / (path.stem + '.lock')


def extract_media(home, row, media, path, step):
    if not row.get('media_path'):
        step('fetch')
        fetch(home, row, media)
        with media.open('rb') as source:
            row['media_sha256'] = hashlib.file_digest(source, 'sha256').hexdigest()
        step('archive')
        if row['ref']['source'] != 'manual':
            spool(home, row['id'], media, True)
        row.update(media_path=str(media.relative_to(home)), fetched_at=time.time())
        save(path, row)
    step('type')
    with media.open('rb') as source:
        row['type'] = detect(source.read(4096))
    started = time.monotonic()
    text, model, kind = extract(row, media, step)
    row['conversion_s'] = time.monotonic() - started
    row['model'] = model
    if not text.strip():
        raise Unavailable('no_text')
    doc = output_doc(row, text, model, kind)
    return doc, model


def remote_refusal(reason):
    return (reason or '').split(' (', 1)[0] in REMOTE_REFUSALS


def temporary_refusal(error):
    if isinstance(error, (Failed, Unavailable)):
        return 'permanent', str(error)
    if isinstance(error, urllib.error.HTTPError):
        if error.code in (403, 451):
            return 'blocked', f'remote_blocked (HTTP {error.code})'
        if error.code == 429:
            return 'temporary', f'rate_limited (HTTP {error.code})'
        if 500 <= error.code < 600:
            return 'temporary', f'remote_unavailable (HTTP {error.code})'
        reason = 'media_expired' if error.code in (404, 410) else 'remote_unavailable'
        return 'permanent', f'{reason} (HTTP {error.code})'
    if isinstance(error, urllib.error.URLError):
        error = error.reason
    if isinstance(error, ssl.SSLError):
        return 'permanent', 'tls_error'
    for type_, reason in (
        (TimeoutError, 'timeout'),
        (ConnectionResetError, 'connection_reset'),
        (ConnectionRefusedError, 'connection_refused'),
    ):
        if isinstance(error, type_):
            return 'temporary', f'remote_unavailable ({reason})'
    return 'permanent', type(error).__name__


def next_attempt_at(row):
    attempts = row.get('attempts', 0)
    if not attempts or attempts > len(RETRY_SCHEDULE):
        return None
    return row.get('first_attempt_at', row.get('last_attempt_at', 0)) + RETRY_SCHEDULE[attempts - 1]


def process(home, row, step=lambda message: None, block=True):
    home = Path(home)
    path = queue_path(home, row['id'])
    with lock_path(path).open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | (0 if block else fcntl.LOCK_NB))
        except BlockingIOError:
            return None
        stored = stored_outcome(home, row['id'])
        if not path.exists():
            return stored
        row = json.loads(path.read_text())
        if stored and matches_outcome(stored, row):
            remove_queue(path)
            step(stored['state'])
            return stored
        if row['state'] != 'queued':
            step(row['state'])
            return row
        due = next_attempt_at(row)
        if due is not None and time.time() < due:
            step('queued')
            return row if block else None
        media = home / 'attachments/media' / path.stem
        media.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            row['attempts'] += 1
            row['last_attempt_at'] = time.time()
            row.setdefault('first_attempt_at', row['last_attempt_at'])
            row.pop('next_attempt_at', None)
            row['pipeline_revision'] = max(PIPELINE_REVISION, row.get('pipeline_revision', 0))
            save(path, row)
            doc, model = extract_media(home, row, media, path, step)
            row.update(
                pipeline_revision=doc['pipeline_revision'],
                state='done',
                ended_at=time.time(),
                output_id=doc['id'],
                model=model,
                output=doc,
            )
            row.pop('reason', None)
        except Exception as error:
            category, row['reason'] = temporary_refusal(error)
            if category == 'temporary':
                if row['attempts'] > len(RETRY_SCHEDULE):
                    row['state'] = 'failed'
            elif (
                isinstance(error, Unavailable)
                or category == 'blocked'
                or isinstance(error, urllib.error.HTTPError)
            ):
                row['state'] = 'unavailable'
            elif isinstance(error, Failed) or row['attempts'] > len(RETRY_SCHEDULE):
                row['state'] = 'failed'
            if row['state'] != 'queued':
                row['ended_at'] = time.time()
        if row['state'] != 'queued':
            doc = outcome_record(row)
            if row['state'] == 'done':
                row['output'] = doc
            step('spool')
            spool(home, doc['id'], dumps(doc).encode())
        save(path, row)
        if row['state'] != 'queued':
            media.unlink(missing_ok=True)
            if row['ref']['source'] == 'manual':
                Path(row['ref']['path']).unlink(missing_ok=True)
        step(row['state'])
        return row


def finish(home, row, step=lambda message: None):
    return process(home, row, step)


def demand(home, identifier, step, pages=None):
    identifier, _ = materialise(home, identifier, pages)
    stored = stored_outcome(home, identifier)
    if stored and not queue_path(home, identifier).exists():
        step(stored['state'])
        return stored
    row = finish(home, {'id': identifier}, step)
    if row['state'] == 'done' and 'output' not in row:
        from .cli import get_docs

        row['output'] = get_docs(home, row['output_id'])[0]
    return row


def add(home, input_, name, title, step):
    root = Path(home) / 'attachments'
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(dir=root, delete=False) as temporary:
        path = Path(temporary.name)
    try:
        stream(input_, path)
        with path.open('rb') as source:
            identifier = 'manual:' + hashlib.file_digest(source, 'sha256').hexdigest()
        queued = queue_path(home, identifier)
        stored = stored_outcome(home, identifier)
        if stored and stored['state'] in ('done', 'unavailable'):
            step(stored['state'])
            return stored
        parent = dict(
            id=identifier, source='manual', group='files', time=time.time(), title=title or name
        )
        durable = root / 'manual' / identifier.removeprefix('manual:')
        durable.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not durable.exists():
            archived = spool(home, identifier, path, True)
            try:
                os.link(archived, durable)
                directory = os.open(durable.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            except FileExistsError:
                pass
        ref = dict(source='manual', path=str(durable))
        if stored and stored['state'] == 'failed':
            # A supplied manual payload explicitly retries a failed extraction.
            row = dict(
                id=identifier,
                parent=parent,
                ref=ref,
                state='queued',
                attempts=0,
                automatic=False,
                backfill=False,
                captured_at=time.time(),
                enqueued_at=time.time(),
                pipeline_revision=max(PIPELINE_REVISION, stored['pipeline_revision']),
                retry_of=stored['outcome_hash'],
            )
            publish_queue(queued, row)
        else:
            enqueue(home, identifier, parent, ref)
        queued = queue_path(home, identifier)
        with lock_path(queued).open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return queued_row(queued) or stored_outcome(home, identifier)
            row = queued_row(queued)
            if row is None:
                return stored_outcome(home, identifier)
            if row.get('reason') == 'FileNotFoundError':
                row.update(ref=ref, state='queued', attempts=0, enqueued_at=time.time())
                row.pop('media_path', None)
                row.pop('reason', None)
                save(queued, row)
        return demand(home, identifier, step)
    finally:
        path.unlink(missing_ok=True)


def enqueue_backfill(home, source, state, packs, parser):
    for pack in sorted(
        (p for p in state.packs('raw') if p['source'] == source),
        key=lambda p: p['seq'],
        reverse=True,
    ):
        for origin, head, raw in packs.iter_pack(pack):
            if head.get('kind') in ('archive-only', 'reset-history'):
                continue
            native = head.get('version') or {}
            capture = native.get('capture', head.get('capture', 0))
            ctx = {
                **state.get('ctx:' + source, {}),
                **native,
                'time': head.get('time'),
                'key': native.get('key', head['id']),
            }
            docs = [doc for doc in parser(raw, ctx) if not isinstance(doc, Exclusion)]
            for doc in docs:
                doc['raw'] = origin
            enqueue_auto(home, raw, docs, capture, automatic=False)


def backfill(home, source):
    from .b2 import B2
    from .packs import Packs
    from .state import State
    from .sources import SOURCES

    home = Path(home)
    state = State(home)
    packs = Packs(state, B2())
    parser = next(parser for name, parser, *_ in SOURCES if name == source)
    started = time.time()
    try:
        enqueue_backfill(home, source, state, packs, parser)
    finally:
        state.db.close()
    rows = [
        row for path in (home / 'attachments/pending').glob('*.json') if (row := queued_row(path))
    ]
    rows = sorted(
        (r for r in rows if r.get('backfill') and r['parent']['source'] == source),
        key=lambda r: -r['captured_at'],
    )

    with ThreadPoolExecutor(max_workers=4) as pool:
        for offset in range(0, len(rows), 20):
            if shutil.disk_usage(home).free < 8 * 1024**3:
                raise OSError('disk floor')
            batch = list(pool.map(lambda row: finish(home, row), rows[offset : offset + 20]))
            save(
                home / 'attachments' / ('backfill-' + source + '.json'),
                dict(
                    source=source,
                    started_at=started,
                    updated_at=time.time(),
                    total=len(rows),
                    processed=offset + len(batch),
                    wall_s=time.time() - started,
                ),
            )
    return dict(source=source, total=len(rows), wall_s=time.time() - started)


class Worker:
    def __init__(self, home=DEFAULT_HOME):
        self.home = Path(home)
        self.root = self.home / 'attachments'
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = (self.root / 'worker.lock').open('a')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (self.root / 'pending').mkdir(exist_ok=True, mode=0o700)
        self.receipt = (queued_row(self.root / 'worker.json') or {}).get('receipt')

    def heartbeat(self, message=None):
        save(
            self.root / 'worker.json',
            {'heartbeat_at': time.time(), 'cutoff': cutoff(self.home), 'receipt': self.receipt},
        )

    def cycle(self):
        self.heartbeat()
        queue = self.root / 'pending'
        rows = [row for path in queue.glob('*.json') if (row := queued_row(path))]
        rows = sorted(
            (r for r in rows if r['state'] == 'queued'),
            key=lambda r: (not r['automatic'], -r.get('captured_at', r.get('queued_at', 0))),
        )
        for row in rows[:20]:
            result = process(self.home, row, self.heartbeat, block=False)
            if result is not None:
                self.receipt = {
                    k: result.get(k)
                    for k in (
                        'id',
                        'state',
                        'reason',
                        'model',
                        'pipeline_revision',
                        'attempts',
                        'ended_at',
                    )
                }
                self.receipt['at'] = time.time()
                self.heartbeat()
                return True
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--home', type=Path, default=DEFAULT_HOME)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--html', type=Path)
    args = parser.parse_args()
    if args.html:
        from .sources.gmail import BodyHTML

        html = BodyHTML()
        html.feed(args.html.read_bytes().decode('utf-8', errors='replace'))
        print(''.join(text for _, text in html.chunks))
        return 0
    try:
        os.umask(0o077)
        worker = Worker(args.home)
        while True:
            worker.cycle()
            if args.once:
                return 0
            time.sleep(2)
    except Exception as error:
        print('attachments: unavailable (' + type(error).__name__ + ')', file=sys.stderr)
        return 4


if __name__ == '__main__':
    sys.exit(main())
