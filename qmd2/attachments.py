"""K8: open work in the queue; pack-backed outcomes through the spool."""

import argparse
import base64
import codecs
from concurrent.futures import ThreadPoolExecutor
import errno
import fcntl
import hashlib
import hmac
import http.client
import io
import ipaddress
import json
import math
import os
import resource
import re
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import unicodedata
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from pathlib import Path

from .accounts import account
from .doc import Exclusion, ceiling, date, dumps, selected_pages, visible
from .state import DEFAULT_HOME

CUTOFF = '2026-10-13T23:06:38Z'
PIPELINE_REVISION = 3
PARSER_VERSION = 2
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
# WhatsApp media links expire, so bytes are fetched at arrival; one fetch stays bounded, and
# the rescue verb pauses between fetches to stay polite.
ARCHIVE_LIMIT = 100 * 1024**2
RESCUE_PACE = 1.0
# Names the one-time lane backfill: its presence says every archived record's locator is recorded.
MEDIA_INDEX = 'media-index'
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
# m3's Apple Vision OCR, the same client shape as m3-fetch's: {"url", "token"}. Absent means Azure only.
OCR_CONFIG = Path.home() / '.config/m3-ocr/client.json'
OCR_CONNECT_SECONDS = 5
# The service bounds one document to ~15 s and holds a single slot, so a queue wait is the long tail.
OCR_READ_SECONDS = 120
# A PDF's own text layer is read locally and free; only a scan goes to OCR (D-181).
PDFTOTEXT = '/usr/bin/pdftotext'
PDFTOTEXT_TIMEOUT = 60
PDFTOTEXT_VERSION = None
# A fetched URL arrives in a message or a document, so it must never reach an internal service. is_global already
# excludes loopback, RFC 1918, link-local, ULA and reserved space; the networks are named too so the rule reads
# plainly and survives a change in the ipaddress module's tables. 100.64/10 and fd7a:115c:a1e0::/48 are the tailnet.
BLOCKED_NETWORKS = [
    ipaddress.ip_network(network)
    for network in (
        '0.0.0.0/8',
        '10.0.0.0/8',
        '100.64.0.0/10',
        '127.0.0.0/8',
        '169.254.0.0/16',
        '172.16.0.0/12',
        '192.168.0.0/16',
        '::1/128',
        'fc00::/7',
        'fe80::/10',
        'fd7a:115c:a1e0::/48',
    )
]


class Unavailable(Exception):
    pass


class Failed(Exception):
    pass


class ConverterTimeout(TimeoutError, Failed):
    pass


class StageFailed(OSError):
    """A stage refused: `stage` is this module's literal, `detail` its child's last stderr line.

    `temporary_refusal` answers `stage_failed (<stage>: <detail>)`, temporary for a disk floor only.
    `code` is the exit status when a child failed, so a caller can tell one exit from another.
    """

    def __init__(self, stage, detail='', code=None):
        super().__init__(stage)
        self.stage = stage
        self.detail = detail
        self.code = code


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


def extraction_type(kind):
    """Refuse a kind extraction has no branch for: fetch, restore and extract share this one gate."""
    if route(kind) not in CAPS:
        raise Unavailable('unsupported_type')
    return kind


def stream(reader, target, encrypted=False):
    first = reader.read(4096)
    kind = extraction_type(detect(first) if not encrypted else 'audio')
    cap = CAPS[route(kind)]
    if shutil.disk_usage(target.parent).free < 8 * 1024**3 + 2 * cap:
        raise StageFailed('disk floor')
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


def stream_original(reader, target, encrypted=False):
    """Copy original bytes with no type routing, capped at ARCHIVE_LIMIT as they are read."""
    cap = ARCHIVE_LIMIT + (32 if encrypted else 0)
    if shutil.disk_usage(target.parent).free < 8 * 1024**3 + 2 * cap:
        raise StageFailed('disk floor')
    size = 0
    with target.open('wb') as output:
        while chunk := reader.read(1024**2):
            size += len(chunk)
            if size > cap:
                raise Unavailable('too_large')
            output.write(chunk)
    return target


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


def enqueue(
    home,
    identifier,
    parent,
    ref,
    automatic=False,
    captured_at=None,
    backfill=False,
    media_only=False,
    retry_failed=False,
):
    root = Path(home) / 'attachments/pending'
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = queue_path(home, identifier)
    queued = queued_row(path)
    stored = stored_outcome(home, identifier, output=False)
    # rescue retries what qmd itself broke (state failed); a source refusal (unavailable) stands.
    retry = retry_failed and (stored or queued or {}).get('state') == 'failed'
    redo_fetch = (
        stored
        and ref['source'] in ('x', 'link')
        and stored.get('reason') == 'media_expired'
        and not stored.get('browser_fetch')
    )
    if (
        stored
        and (not backfill or (stored['pipeline_revision'] >= PIPELINE_REVISION and not redo_fetch))
        and not retry
    ):
        return stored['state']
    if queued and not retry:
        return queued['state']
    if queued:
        # A retry replaces the failed queue row it read; publish_queue never overwrites one.
        remove_queue(path)
    ref = dict(ref)
    if prior_text(home, identifier):
        ref['prior_text'] = True
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
        media_only=media_only,
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


def auto_extracted(source, ref):
    """Whether enqueue_auto extracts these bytes, or only archives them (a doc's flattened type)."""
    kind = str(ref.get('type') or '')
    if '/' in kind:
        kind = kind.split('/', 1)[0]
    if ref.get('mimetype') == 'image/gif' or ref.get('type') == 'image/gif':
        return False
    return (source == 'whatsapp' and kind in ('audio', 'image')) or (
        source == 'x' and kind in ('photo', 'image')
    )


def prior_text(home, identifier):
    """Whether this attachment's OCR output was tombstoned, so Vision's empty read is a loss."""
    from .state import State

    if not (Path(home) / 'state.sqlite').exists():
        return False
    state = State(home)
    try:
        return state.get('deleted:' + identifier + '/ocr') is not None
    finally:
        state.db.close()


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
            identifier = f"{doc['id']}/att/{pos}"
            extracts = (source == 'whatsapp' and ref.get('type') in ('audio', 'image')) or (
                source == 'x' and ref.get('type') == 'photo'
            )
            extracts = extracts and ref.get('mimetype') != 'image/gif'
            # WhatsApp links expire, so every arriving attachment is fetched; only these extract.
            if not extracts and not (source == 'whatsapp' and automatic):
                continue
            if source == 'x':
                ref = dict(ref, url=ref.get('media_url_https') or ref.get('url'))
            enqueue(
                home,
                identifier,
                doc,
                dict(ref, source=source),
                automatic,
                captured,
                not automatic,
                media_only=not extracts,
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
            'pages_read',
            'vision',
            'text_layer',
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
            parser=f'attachment@{PARSER_VERSION}',
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
        return [parsed_output(doc)]
    except (ValueError, TypeError):
        return [
            dict(
                id=ctx['key'],
                item=ctx['key'],
                source='attachment',
                group='files',
                kind='binary',
                time=ctx.get('time'),
                parser=f'attachment@{PARSER_VERSION}',
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


def converter_failure(code):
    """The converter's label: its exit code, or the signal that ended it."""
    if code >= 0:
        return f'converter failed (exit {code})'
    number = -code
    try:
        ended = 'signal ' + signal.Signals(number).name
    except ValueError:
        ended = f'signal {number}'
    return f'converter failed ({ended})'


def stderr_tail(detail):
    """The child's last non-blank stderr line, bounded at 200: a cause, not a transcript."""
    lines = [line.strip() for line in detail.splitlines() if line.strip()]
    return lines[-1][:200] if lines else ''


def run(argv, timeout=120, data=None, azure=False):
    env = {
        'HOME': str(Path.home()),
        'PATH': '/usr/local/bin:/usr/bin:/bin',
        'LANG': 'C.UTF-8',
        'OMP_THREAD_LIMIT': '1',
        'OPENBLAS_NUM_THREADS': '1',
    }
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
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
            errors.seek(0)
            detail = errors.read(1024**2).decode(errors='replace')
            if azure:
                if 'Missing Azure' in detail or re.search(r'(?:error|HTTP) (?:401|403)\b', detail):
                    raise Failed('azure_auth_or_config')
                if re.search(r'(?:error|HTTP) 4(?:00|04|10|13|15|22)\b', detail):
                    raise Unavailable('azure_rejected')
                status = re.search(r'\b(?:error|HTTP)\s+(429|5\d{2})\b', detail, re.IGNORECASE)
                if status:
                    raise urllib.error.HTTPError(
                        '', int(status[1]), 'Azure temporary refusal', None, None
                    )
            # The child's last stderr line names its cause; this process's environment never does.
            raise StageFailed(converter_failure(p.returncode), stderr_tail(detail), p.returncode)
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
        'video': b'WhatsApp Video Keys',
        'ptv': b'WhatsApp Video Keys',  # a round video note
    }.get(media['type'])
    if info is None:
        raise Unavailable('unsupported_type')
    key = b64(media['mediaKey'])
    if len(key) != 32:
        raise ValueError('media key invalid')
    prk, expanded, block = hmac.digest(bytes(32), key, 'sha256'), b'', b''
    for counter in range(1, 5):
        block = hmac.digest(prk, block + info + bytes([counter]), 'sha256')
        expanded += block
    iv, aes, mac = expanded[:16], expanded[16:48], expanded[48:80]
    return iv, aes, mac


def decrypt(media, encrypted, target=None, copy=None):
    """Verify and decrypt WhatsApp media; `copy` writes the plaintext (default: extraction's typed `stream`)."""
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
                        (copy or stream)(child.stdout, target)
                    finally:
                        child.stdout.close()
                    if child.wait(timeout=120):
                        raise StageFailed('media decryption')
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


NAT64 = ipaddress.ip_network('64:ff9b::/96')
V4_COMPATIBLE = ipaddress.ip_network('::/96')


def address_allowed(address):
    """Public and not multicast; an IPv4-mapped IPv6 address is judged as the IPv4 one it carries."""
    ip = ipaddress.ip_address(address.split('%', 1)[0])
    if ip.version == 6:
        # An IPv6 form that carries an IPv4 address is judged as that address: mapped (::ffff:a.b.c.d),
        # NAT64 (64:ff9b::/96, reaches a.b.c.d through a DNS64 gateway) and the old compatible form (::a.b.c.d).
        embedded = ip.ipv4_mapped
        if embedded is None and (ip in NAT64 or ip in V4_COMPATIBLE):
            embedded = ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
        ip = embedded or ip
    return (
        ip.is_global
        and not ip.is_multicast
        and not any(
            ip in network for network in BLOCKED_NETWORKS if network.version == ip.version
        )
    )


def checked_addresses(host, port):
    """The addresses one name resolves to, every one public; a refusal never names the host."""
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    if not infos or not all(address_allowed(info[4][0]) for info in infos):
        raise Unavailable('blocked_address')
    return infos


def guarded_connect(address, timeout=None, source_address=None):
    """socket.create_connection, but the address that was checked is the one connected to (no second lookup)."""
    error = None
    for family, kind, proto, _, sockaddr in checked_addresses(*address):
        sock = socket.socket(family, kind, proto)
        try:
            if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:  # http.client's 'no timeout given'
                sock.settimeout(timeout)
            if source_address:
                sock.bind(source_address)
            sock.connect(sockaddr)
            return sock
        except OSError as failure:
            sock.close()
            error = failure
    raise error or ConnectionRefusedError('connect failed')


def guarded_connection(factory):
    def open_(host, **kwargs):
        connection = factory(host, **kwargs)
        connection._create_connection = guarded_connect
        return connection

    return open_


class GuardedHandlers(urllib.request.HTTPHandler, urllib.request.HTTPSHandler):
    """http and https on connections whose connect() refuses a non-public answer."""

    def http_open(self, req):
        return self.do_open(guarded_connection(http.client.HTTPConnection), req)

    def https_open(self, req):
        return self.do_open(guarded_connection(http.client.HTTPSConnection), req)


class CheckedRedirects(urllib.request.HTTPRedirectHandler):
    """Each hop is checked by name, so a relay's own resolver cannot aim the next request inward."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        following = super().redirect_request(req, fp, code, msg, headers, newurl)
        if following is not None:
            check_url(following.full_url)
        return following


def check_url(url):
    """An http(s) URL whose name resolves to public addresses only."""
    parts = urllib.parse.urlsplit(url)
    if parts.scheme.lower() not in ('http', 'https'):
        raise Unavailable('blocked_scheme')
    try:
        port = parts.port or (443 if parts.scheme.lower() == 'https' else 80)
    except ValueError:
        raise Unavailable('blocked_address') from None
    if not parts.hostname:
        raise Unavailable('blocked_address')
    try:
        return checked_addresses(parts.hostname, port)
    except OSError as error:  # classified as the connect failure it stands for
        raise urllib.error.URLError(error) from None


def opener(relay=None):
    """One attempt's connections: no proxy, public addresses only, and every redirect hop checked again.

    A relay is a SOCKS handler, which resolves the name at the exit, so the name is what gets checked there.
    """
    handlers = [urllib.request.ProxyHandler({}), CheckedRedirects()]
    if relay is None:
        handlers.append(GuardedHandlers())
    else:
        from . import mullvad

        handlers.append(mullvad.SocksHandler(relay))
    return urllib.request.build_opener(*handlers)


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
    parent, _, ref = attachment_ref(home, identifier)
    return identifier, enqueue(home, identifier, parent, dict(ref, pages=pages))


def attachment_ref(home, identifier, parent=None):
    """The parent document, its attachment reference and the fetch ref for one attachment id."""
    from .cli import get_docs

    parent_id, position = identifier.rsplit('/att/', 1)
    parent = parent or get_docs(home, parent_id)[0]
    attachment = next((a for a in parent.get('attachments', []) if a['id'] == identifier), None)
    if not attachment:
        raise LookupError()
    if attachment.get('type') == 'text/uri-list':
        # A link entry is a URL the parent mentions, not bytes to fetch: only media has a fetch ref.
        raise Unavailable('not_media')
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
    return parent, attachment, dict(ref, source=source, size=attachment.get('size'))


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


def fetch_url(row, target, open_=None, relay=None, started=None, original=False):
    started = time.monotonic() if started is None else started
    copy = stream_original if original else stream
    via = 'mullvad' if relay else 'direct'
    attempt = dict(via=via, relay=relay, status=None, error=None, elapsed_s=0)
    attempts = row.get('fetched', {}).get('attempts', [])
    attempts.append(attempt)
    url = normalized_url(row['ref']['url'])
    row['fetched'] = dict(via=via, status=None, final_url=url, attempts=attempts)
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        check_url(url)
        open_ = open_ or opener().open
        with open_(req, timeout=60) as response:
            attempt['status'] = response.status
            row['fetched'].update(status=response.status, final_url=response.geturl())
            copy(response, target)
    except Exception as error:
        if isinstance(error, urllib.error.HTTPError):
            attempt['status'] = error.code
            row['fetched'].update(status=error.code, final_url=error.geturl())
        attempt['error'] = temporary_refusal(error)[1]
        raise
    finally:
        attempt['elapsed_s'] = round(time.monotonic() - started, 3)
    return target


def fetch_media(home, row, target, original=False):
    """Fetch one attachment's bytes; original copies verbatim for archive, else extraction routes by type."""
    copy = stream_original if original else stream
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
            with opener().open(request_, timeout=60) as response:
                copy(response, encrypted, encrypted=True)
            decrypt(ref, encrypted, target, copy)
        finally:
            encrypted.unlink(missing_ok=True)
    elif ref['source'] == 'gmail':
        raw = json.loads(raw_parent(home, row['parent']))
        encoded = raw['raw']
        mime = base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4))
        part = list(BytesParser(policy=policy.default).parsebytes(mime).walk())[ref['position']]
        copy(io.BytesIO(part.get_payload(decode=True) or b''), target)
    elif ref['source'] in ('x', 'link'):
        row['browser_fetch'] = True
        fetch_url(row, target, original=original)
    elif ref['source'] == 'manual':
        with Path(ref['path']).open('rb') as source:
            copy(source, target)
    else:
        raise Unavailable('unsupported_source')
    return target


def fetch(home, row, target, original=False):
    try:
        try:
            return fetch_media(home, row, target, original)
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
                with mullvad.tunnel(relay, secret) as proxy:
                    open_ = opener(proxy).open
                    return fetch_url(row, target, open_, relay['hostname'], started, original)
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


def transcriber_release(tool=None):
    """The tg-tools release that ran the transcriber: `current` names releases/<sha>, no git read."""
    return Path(tool or TOOL).resolve().parents[1].name


def transcribe(path):
    """Transcribe one audio file and label the engine that ran: provider:model+release.

    The transcriber reads its own credentials and reports the engine it used as JSON: a self-hosted
    run names model and engine build (parakeet-ultra@fluidaudio-...), an Azure fallback says azure.
    """
    handle, engine = tempfile.mkstemp(prefix='transcribe-engine-', suffix='.json')
    os.close(handle)
    try:
        try:
            output = run(
                [
                    sys.executable,
                    str(TOOL),
                    str(path),
                    '--raw',
                    '--model',
                    'mini',
                    '--jobs',
                    '1',
                    '--engine-file',
                    engine,
                ],
                timeout=360,
                azure=True,
            )
        except StageFailed as error:
            # tg-tools exits 3 when no engine heard speech: the audio's answer, not a failure.
            if error.code != 3:
                raise
            raise Unavailable('no_text') from None
        try:
            reported = json.loads(Path(engine).read_text())
            label = f'{reported["provider"]}:{reported["model"]}'
        except (OSError, ValueError, KeyError, TypeError):
            raise StageFailed('engine label missing') from None
    finally:
        Path(engine).unlink(missing_ok=True)
    text = output.decode().strip()
    if not text:
        raise StageFailed('empty transcription')
    return text, label + '+' + transcriber_release()


def vision_ocr(path):
    """Apple Vision on m3 reads one document: (text, engine, pages), or (None, the reason, None).

    The endpoint is the operator's own tailnet service, reached like Azure's, through no URL guard. An image
    Vision reads as empty is ('', engine, 1): its answer, not a reason for Azure to read it again.
    """
    try:
        config = json.loads(OCR_CONFIG.read_text())
        url, token = config['url'].rstrip('/'), config['token']
        parts = urllib.parse.urlsplit(url)
        if not parts.hostname or not isinstance(token, str) or not token:
            raise ValueError()
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None, 'no_config', None
    boundary = 'qmd2vision'
    name = path.name.replace('"', '')
    head = (
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\n'
        'Content-Type: application/octet-stream\r\n\r\n'
    ).encode()
    body = head + path.read_bytes() + f'\r\n--{boundary}--\r\n'.encode()
    connection_type = (
        http.client.HTTPSConnection if parts.scheme == 'https' else http.client.HTTPConnection
    )
    connection = connection_type(parts.hostname, parts.port, timeout=OCR_CONNECT_SECONDS)
    try:
        connection.connect()
        connection.sock.settimeout(OCR_READ_SECONDS)
        connection.request(
            'POST',
            parts.path + '/v1/ocr',
            body=body,
            headers={
                'Authorization': 'Bearer ' + token,
                'Content-Type': 'multipart/form-data; boundary=' + boundary,
            },
        )
        response = connection.getresponse()
        answer = json.loads(response.read(LIMIT + 1)) if response.status == 200 else {}
    except TimeoutError:
        return None, 'timeout', None
    except (OSError, http.client.HTTPException):
        # A broken answer (BadStatusLine, IncompleteRead) is the endpoint being unreachable, not a crash.
        return None, 'unreachable', None
    except ValueError:
        return None, 'bad_answer', None
    finally:
        connection.close()
    if response.status != 200:
        return None, f'http_{response.status}', None
    scripts = [page.get('script') or {} for page in answer.get('pages') or []]
    # Vision has no Greek model: Greek arrives as Cyrillic look-alikes, so a Latin-free Cyrillic page means it
    # guessed the wrong script and Azure reads the image instead.
    if any(s.get('greek', 1) == 0 and s.get('cyrillic', 0) > 0.05 for s in scripts):
        return None, 'script_cyrillic', None
    text = '\n\n'.join(page.get('text') or '' for page in answer.get('pages') or [])
    if not answer.get('engine'):
        return None, 'no_text', None
    # An empty read is Vision's answer, not a reason for Azure: ('', engine) labels the row and extract()'s
    # 3-word check raises Unavailable('no_text'), as for any image it cannot read.
    return text, answer['engine'], len(answer.get('pages') or [])


def pdftotext_version():
    """The tool's own version, read once: `pdftotext -v` prints it to stderr and exits 0."""
    global PDFTOTEXT_VERSION
    if PDFTOTEXT_VERSION is None:
        try:
            printed = subprocess.run([PDFTOTEXT, '-v'], capture_output=True, text=True, timeout=10)
            found = re.search(r'pdftotext version (\S+)', printed.stdout + printed.stderr)
            PDFTOTEXT_VERSION = found[1] if found else 'unknown'
        except (OSError, subprocess.SubprocessError):
            PDFTOTEXT_VERSION = 'unknown'
    return PDFTOTEXT_VERSION


def text_quality(text, pages):
    """The measured rule (26/26 PDFs on the test corpus): why a text layer is unreadable, or None.

    A scan has no text layer or a garbage one: too few characters per page, not letters, control or
    replacement characters, or words that are not words.
    """
    if not pages:
        return 'no pages'
    chars = len(text)
    per_page = chars / pages
    if per_page < 100:
        return f'chars/page {per_page:.0f} < 100'
    spaced = [c for c in text if not c.isspace()]
    share = sum(c.isalnum() for c in spaced) / max(1, len(spaced))
    if share < 0.70:
        return f'alnum share {share:.2f} < 0.70'
    control = sum(unicodedata.category(c) in ('Cc', 'Co', 'Cn') and c not in '\n\r\t' for c in text)
    if control / chars > 0.01 or '\ufffd' in text or '\ue000' in text:
        return 'control/replacement chars'
    words = re.findall(r'[^\W\d_]{2,}', text)
    mean = sum(len(w) for w in words) / max(1, len(words))
    if not 2.0 <= mean <= 14:
        return f'mean word length {mean:.1f}'
    return None


def text_layer(path, pages, total_pages):
    """A PDF's own text when it passes the rule: (text, None), else (None, why the page needs OCR).

    A pdftotext failure or timeout is a failed check, not a failed row: its reason goes to the row
    and the page goes to Vision. A page selection becomes pdftotext's own -f/-l span.
    """
    argv = [PDFTOTEXT, '-q']
    if pages:
        first, last = pages.split('-')
        argv += ['-f', first, '-l', last]
    try:
        text = run(argv + [str(path), '-'], timeout=PDFTOTEXT_TIMEOUT).decode('utf-8', 'replace')
    except TimeoutError:
        return None, 'pdftotext_timeout'
    except OSError:
        return None, 'pdftotext_failed'
    if pages:
        pages = selected_pages(pages)
    reason = text_quality(text, pages or total_pages)
    return (text, None) if reason is None else (None, reason)


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
        url += '&' + urllib.parse.urlencode({'pages': pages})
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
        kind = route(extraction_type(detect(source.read(4096))))
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
        # A selection is an A-B span; a queued ref may hold a page count instead.
        pages = f'1-{pages}' if isinstance(pages, int) else pages
        row['total_pages'] = page_count(path, kind)
        text = model = None
        if kind == 'pdf':
            text, reason = text_layer(path, pages, row['total_pages'])
            row['text_layer'] = reason or 'ok'
            if text is not None:
                model = 'pdftotext@' + pdftotext_version()
                row['azure_pages'] = 0
                row['pages_read'] = selected_pages(pages) if pages else row['total_pages']
        if text is None:
            # m3-ocr reads a PDF whole, so a page span stays on Azure's layout, which honours it.
            read = (None, None, None)
            if kind in ('pdf', 'image'):
                read = (None, 'pages_selected', None)
                if kind == 'image' or not pages:
                    read = vision_ocr(path)
            if read[0] is not None and not (read[0] == '' and row['ref'].get('prior_text')):
                text, model, row['pages_read'] = read
                row['azure_pages'] = 0
                # m3 reads the whole document (an image is one page) and answers how many it read.
                row['total_pages'] = row['pages_read']
            else:
                if read[1] is not None and read[0] is None:
                    row['vision'] = read[1]
                if read[0] == '':
                    # This attachment had text before: an empty read is a loss, not its answer.
                    row['vision'] = 'empty'
                text, model, row['azure_pages'] = azure_di(
                    path, 'prebuilt-layout' if kind in ('pdf', 'docx') else 'prebuilt-read', pages
                )
                if pages is None:
                    row['total_pages'] = row['azure_pages']
                row['pages_read'] = row['azure_pages']
        row['pages_requested'] = selected_pages(pages) if pages else None
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


def parsed_output(doc):
    doc['parser'] = f'attachment@{PARSER_VERSION}'
    doc['parts'] = [
        part
        for part in doc['parts']
        if not (
            part['kind'] == 'header'
            and part['text'].startswith('Model: ')
            and '; pipeline revision: ' in part['text']
        )
    ]
    pages = doc.get('pages') or {}
    covered, total = pages.get('covered'), pages.get('total')
    if covered is not None and (total is None or covered < total):
        coverage = {'kind': 'header', 'text': f"Extracted {covered} of {total or 'unknown'} pages"}
        if coverage not in doc['parts']:
            doc['parts'].insert(0, coverage)
    return doc


def pages_view(row):
    """Page coverage for a document or extract payload; azure is absent when unknown."""
    if 'pages_read' not in row:
        return None
    pages = dict(
        covered=row['pages_read'],
        total=row.get('total_pages'),
        requested=row.get('pages_requested'),
    )
    if 'azure_pages' in row:
        pages['azure'] = row['azure_pages']
    return pages


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
        parser=f'attachment@{PARSER_VERSION}',
        pipeline_revision=row.get('pipeline_revision', PIPELINE_REVISION),
        model=model,
        parts=[{'kind': 'authored' if kind == 'transcript' else 'attachment_text', 'text': text}],
        context={
            **parent.get('context', {}),
            'parent': parent['id'],
            'sender': parent.get('from', ''),
        },
        media_sha256=row['media_sha256'],
    )
    pages = pages_view(row)
    if pages is not None:
        doc['pages'] = pages
    if row['ref']['source'] == 'link':
        doc['link'] = row['ref']['url']
        doc['title'] = row.get('metadata', {}).get('title') or title
    if row.get('meeting'):
        doc['meeting'] = row['meeting']
        doc['title'] = row['meeting'].get('title') or title
        doc['from'] = (row['meeting'].get('host') or {}).get('email', '')
    return parsed_output(doc)


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


def attachment_identifier(identifier):
    """The attachment an id names, or None: an extracted output id names its own document."""
    if re.fullmatch(r'manual:[0-9a-f]{64}', identifier):
        return identifier
    return identifier if re.fullmatch(r'.*/att/\d+', identifier) else None


def lane_locators(home, identifier=None):
    """Archived attachment ids and their raw locators, newest lane pack first; one id stops at its match."""
    from .b2 import B2
    from .packs import Packs
    from .state import State

    if not (Path(home) / 'state.sqlite').exists():
        return {}
    state = State(home)
    packs = Packs(state)
    found = {}
    try:
        lanes = sorted(
            (p for p in state.packs('raw') if p['source'] in ('attachment', 'manual')),
            key=lambda p: -p['seq'],
        )
        for pack in lanes:
            for attempt in (0, 1):
                try:
                    for loc, head, _ in packs.iter_pack(pack):
                        if head.get('kind') == 'archive-only' and head.get('id'):
                            found.setdefault(head['id'], loc)
                            if identifier == head['id']:
                                return found
                    break
                except FileNotFoundError:
                    if attempt:
                        raise
                    packs.b2 = B2()
    finally:
        state.db.close()
    return found


def archived_locator(home, identifier):
    """Raw locator recorded when this attachment's original bytes were archived, or None."""
    from .state import State

    if not (Path(home) / 'state.sqlite').exists():
        return None
    state = State(home)
    try:
        return state.get('media:' + identifier)
    finally:
        state.db.close()


def checkpoint_ids(home):
    """Every attachment id whose original bytes are archived, from the checkpoints alone."""
    from .state import State

    if not (Path(home) / 'state.sqlite').exists():
        return set()
    state = State(home)
    try:
        return {
            key.removeprefix('media:')
            for (key,) in state.db.execute("SELECT key FROM checkpoints WHERE key LIKE 'media:%'")
        }
    finally:
        state.db.close()


def record_locator(home, identifier, loc):
    """Record an archived attachment's locator, so no later read has to walk the lanes."""
    from .state import State

    state = State(home, writable=True)
    try:
        state.set('media:' + identifier, loc)
        state.commit()
    finally:
        state.db.close()


def parent_document(home, identifier):
    """The parent document of an attachment id: hidden or unknown under a ceiling is a refusal."""
    from .reads import Reader

    if not (Path(home) / 'index').exists():
        return None
    parent_id = identifier.rsplit('/att/', 1)[0]
    reader = Reader(home)
    try:
        row = reader.index.lookup(parent_id) or next(iter(reader.index.item(parent_id)), None)
        if row is None:
            if ceiling():
                raise LookupError()
            return None
        return reader.read(row)
    finally:
        reader.close()


def too_large(ref):
    """A declared size over the cap refuses early; the read itself is capped by stream_original."""
    return int(ref.get('fileLength') or 0) > ARCHIVE_LIMIT


def raw_bytes(home, loc, target):
    """Write one archived record's bytes, from a local pack or cold from B2."""
    from .b2 import B2
    from .packs import Packs
    from .state import State

    state = State(home)
    packs = Packs(state)
    try:
        try:
            packs.write_raw(loc, target)
        except FileNotFoundError:
            packs.b2 = B2()
            packs.write_raw(loc, target)
    finally:
        state.db.close()


def media_source(home, identifier, step=lambda message: None):
    """An attachment's original bytes: its recorded locator, else a fetched temp file spooled for archive."""
    identifier = attachment_identifier(identifier)
    manual = identifier.startswith('manual:')
    # A manual id is its own document, so the ceiling applies to its bytes like any other id's.
    parent = parent_document(home, identifier)
    loc = archived_locator(home, identifier)
    if loc:
        return loc
    if manual:
        return None
    listed = parent is not None and any(
        a['id'] == identifier for a in parent.get('attachments', [])
    )
    if parent is not None and not listed:
        raise LookupError()
    # Only a read may walk the lanes: for an id its parent lists, and only before the backfill has run.
    if listed and backfill_pending(home):
        loc = lane_locators(home, identifier).get(identifier)
        if loc:
            record_locator(home, identifier, loc)
            return loc
    parent, _, ref = attachment_ref(home, identifier, parent)
    if too_large(ref):
        raise Unavailable('too_large')
    root = Path(home) / 'attachments/media'
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = Path(tempfile.mkstemp(dir=root)[1])
    try:
        step('fetch')
        fetch(home, dict(id=identifier, parent=parent, ref=ref), target, original=True)
        step('archive')
        spool(home, identifier, target, True)
    except Exception:
        target.unlink(missing_ok=True)
        raise
    return target


def archive_media(home, row, media, step):
    """Fetch and archive one attachment's original bytes only; extraction stays on demand."""
    if archived_locator(home, row['id']):
        row['state'] = 'archived'
        return
    if too_large(row['ref']):
        raise Unavailable('too_large')
    step('fetch')
    fetch(home, row, media, original=True)
    step('archive')
    spool(home, row['id'], media, True)
    row['state'] = 'archived'


def extract_media(home, row, media, path, step):
    if not row.get('media_path'):
        loc = archived_locator(home, row['id'])
        if loc:
            step('restore')
            with media.open('wb') as target:
                raw_bytes(home, loc, target)
            # An archived original may be of any type: refuse one extraction has no branch for.
            with media.open('rb') as source:
                extraction_type(detect(source.read(4096)))
        else:
            step('fetch')
            fetch(home, row, media)
        with media.open('rb') as source:
            row['media_sha256'] = hashlib.file_digest(source, 'sha256').hexdigest()
        if not loc and row['ref']['source'] != 'manual':
            step('archive')
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


# Errnos worth another attempt; everything else keeps the failure permanent.
TRANSIENT_ERRNOS = frozenset(
    name
    for name in (
        'EAGAIN',
        'EINTR',
        'ETIMEDOUT',
        'ECONNRESET',
        'ECONNREFUSED',
        'EHOSTUNREACH',
        'ENETUNREACH',
        'ENOSPC',
        'EIO',
    )
    if hasattr(errno, name)
)


def temporary_refusal(error):
    if isinstance(error, (Failed, Unavailable)):
        return 'permanent', str(error)
    if isinstance(error, StageFailed):
        # A disk floor is a condition that passes; every other stage is this pipeline's own refusal.
        kind = 'temporary' if error.stage == 'disk floor' else 'permanent'
        cause = f': {error.detail}' if error.detail else ''
        return kind, f'stage_failed ({error.stage}{cause})'
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
    if isinstance(error, OSError) and error.errno is not None:
        name = errno.errorcode.get(error.errno, f'errno {error.errno}')
        short = error.strerror or os.strerror(error.errno)
        kind = 'temporary' if name in TRANSIENT_ERRNOS else 'permanent'
        return kind, f'{type(error).__name__} {name} ({short})'
    return 'permanent', type(error).__name__


def failure_line(row, kind):
    """One journal line per failed attempt: which item, why, how it is treated, which try."""
    return f'attachments: {row["id"]} {row["reason"]} {kind} attempt {row["attempts"]}'


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
            if row.get('media_only'):
                archive_media(home, row, media, step)
            else:
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
            print(failure_line(row, category), file=sys.stderr)
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
            if row['state'] == 'archived':
                media.unlink(missing_ok=True)
                remove_queue(path)
                step('archived')
                return row
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
    path = queue_path(home, identifier)
    if (queued := queued_row(path)) and queued.get('media_only'):
        # An explicit materialise extracts; the archived bytes are its input.
        save(path, dict(queued, media_only=False, state='queued'))
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
                raise StageFailed('disk floor')
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


def backfill_pending(home):
    """Whether archive lanes hold records whose locators were never recorded (the backfill has not run)."""
    from .state import State

    if not (Path(home) / 'state.sqlite').exists():
        return False
    state = State(home)
    try:
        lanes = any(p['source'] in ('attachment', 'manual') for p in state.packs('raw'))
        return lanes and state.get(MEDIA_INDEX) is None
    finally:
        state.db.close()


def index_locators(home, dry_run=False):
    """One-time operator verb: record the archived lane records' locators by one walk of both lanes."""
    from .packs import Packs
    from .state import State

    home = Path(home)
    known = checkpoint_ids(home)
    state = State(home)
    packs = Packs(state)
    try:
        lanes = [p for p in state.packs('raw') if p['source'] in ('attachment', 'manual')]
        local = sum(
            1
            for p in lanes
            if packs.path('raw', p['source'], p['seq'], bool(p['sealed'])).exists()
        )
    finally:
        state.db.close()
    started = time.time()
    found = lane_locators(home)
    missing = {identifier: loc for identifier, loc in found.items() if identifier not in known}
    written = 0
    if not dry_run:
        state = State(home, writable=True)
        try:
            for identifier, loc in missing.items():
                state.set('media:' + identifier, loc)
                written += 1
                if not written % 200:
                    state.commit()
            state.set(MEDIA_INDEX, dict(records=len(found), at=time.time()))
            state.commit()
        finally:
            state.db.close()
    return dict(
        dry_run=dry_run,
        records=len(found),
        known=len(known),
        written=len(missing) if dry_run else written,
        packs=len(lanes),
        local=local,
        b2=len(lanes) - local,
        bytes=sum(p['size'] or 0 for p in lanes),
        wall_s=round(time.time() - started, 1),
    )


def rescue(home, days=45, dry_run=False, step=lambda message: None):
    """Archive the original bytes of WhatsApp media captured in the last N days, oldest first.

    Link entries are skipped (not media), and an outcome qmd itself failed is tried again, while a
    refusal the source gave (unavailable) stands.
    """
    from .reads import Reader

    home = Path(home)
    since = datetime.fromtimestamp(time.time() - days * 86400, timezone.utc).isoformat()
    reader = Reader(home)
    try:
        parents = sorted(
            reader.scan('whatsapp', since=since), key=lambda doc: (date(doc['time']), doc['id'])
        )
    finally:
        reader.close()
    archived = checkpoint_ids(home)
    unarchived = [
        attachment
        for parent in parents
        for attachment in parent.get('attachments', [])
        if attachment['id'] not in archived
    ]
    # A parent's link entries are URLs it mentions, not media: they have no bytes to archive.
    pending = [a['id'] for a in unarchived if a.get('type') != 'text/uri-list']
    started = time.time()
    result = dict(
        days=days,
        dry_run=dry_run,
        archived_index=len(archived),
        total=len(pending),
        skipped=len(unarchived) - len(pending),
        archived=0,
        unavailable={},
        failed={},
        queued=0,
    )
    if backfill_pending(home):
        # Without recorded locators every already-archived attachment would be fetched again.
        result['reason'] = 'run attachments index once: no archived-locator index in this store yet'
        return result
    for identifier in pending if not dry_run else []:
        try:
            if shutil.disk_usage(home).free < 8 * 1024**3:
                raise StageFailed('disk floor')
            parent, _, ref = attachment_ref(home, identifier)
            enqueue(home, identifier, parent, ref, media_only=True, retry_failed=True)
            row = finish(home, {'id': identifier}, step) or {}
        except Exception as error:
            # One bad item never ends the run: it is counted with its reason, and the counts add up.
            row = dict(state='failed', reason=temporary_refusal(error)[1])
        state, reason = row.get('state'), row.get('reason', 'unknown')
        if state == 'archived':
            result['archived'] += 1
        elif state in ('failed', 'unavailable'):
            counts = result['failed' if state == 'failed' else 'unavailable']
            counts[reason] = counts.get(reason, 0) + 1
        else:
            result['queued'] += 1
        time.sleep(RESCUE_PACE)
    result['wall_s'] = round(time.time() - started, 1)
    return result


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
