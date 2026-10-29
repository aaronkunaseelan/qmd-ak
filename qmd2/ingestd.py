"""The sole store writer; bounded source slices, durable packs before checkpoints."""

import argparse
import concurrent.futures
import contextlib
import fcntl
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from .attachments import (
    enqueue_auto,
    snapshot as attachment_status,
    outcome_record,
    queue_path,
    publish_queue,
    remove_queue,
    lock_path,
    matches_outcome,
)
from .b2 import B2
from .doc import Exclusion, date, dumps, version, visible
from .index import Index
from .packs import MAINTENANCE_INTERVAL, Packs, payload_integrity, room
from .sources import SOURCES, ARCHIVE_ONLY_SOURCES
from .sources.files import Walker
from .sources.sessions import (
    EXCLUDED_PROJECTS,
    completed,
    context,
    excluded,
    import_context,
    inventory,
    lines,
    parse,
    prepare,
)
from .state import DEFAULT_HOME, State
from .status import head_lag

LEASE = Path(os.environ.get('QMD2_HEAVY_LEASE', '/run/lock/qmd-batch-heavy.lock')).expanduser()
STATUS_INTERVAL = 60


def atomic(path, value):
    tmp = path.with_suffix('.tmp')
    with tmp.open('w') as f:
        f.write(dumps(value) + '\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def fresh_through(receipt, started):
    if (
        receipt.get('ok')
        and not receipt.get('failed')
        and not receipt.get('error')
        and not receipt.get('backfill_error')
        and not receipt.get('rate_limited')
        and receipt.get('head_complete', True)
    ):
        return started
    return receipt.get('fresh_through')


def replace_receipt(previous, latest, started):
    now = time.time()
    receipt = {
        key: previous[key]
        for key in ('fresh_through', 'last_complete', 'searchable_through')
        if key in previous
    }
    receipt.update(latest)
    receipt.setdefault('rate_limited', False)
    receipt['last_run'] = now
    receipt['fresh_through'] = fresh_through(receipt, started)
    if receipt['ok'] and not receipt.get('failed'):
        receipt['last_complete'] = now
    receipt['oldest_failed'] = (
        (previous.get('oldest_failed') or started) if receipt.get('failed') else None
    )
    return receipt


class Ingestd:
    def __init__(self, home=DEFAULT_HOME, remote=True):
        self.home = Path(home)
        self.home.mkdir(parents=True, exist_ok=True)
        self.lock = (self.home / 'ingestd.lock').open('a')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.state = State(self.home, True)
        self.state.db.execute("DELETE FROM checkpoints WHERE key GLOB 'deleted:file:*'")
        self.b2 = B2() if remote else None
        self.packs = Packs(self.state, self.b2, True)
        if self.state.get('feed_epoch') is None:
            starts = {}
            for row in self.state.packs('doc'):
                starts[row['source']] = (
                    [row['seq'] + 1, 0] if row['sealed'] else [row['seq'], row['size']]
                )
            self.state.set(
                'feed_epoch', {'at': time.time_ns() // 1000 / 1_000_000, 'starts': starts}
            )
            self.state.commit()
        self.maintenance = self.state.get('maintenance')
        self.waiting_for_lease = None
        self.heavy_lock = None
        self.visible_index = None
        if self.maintenance and not self.maintenance['source']:
            if self.maintenance['stage'] == 'swap':
                self.swap_rebuild()
            else:
                self.visible_index = Index(self.home)
                self.index = Index(self.home, True, self.state, self.home / 'index-rebuild')
        else:
            self.index = Index(self.home, True, self.state)
        self.reparse_seen = set((self.maintenance or {}).get('seen', []))
        self.checkpoints = {}
        self.pending = {}
        self.context_dirty = set()
        self.context_hashes = {}
        self.last_commit = time.monotonic()
        self.last_status = float('-inf')
        self.publish_on_commit = True
        self.last_maintenance = 0
        self.last_walk = 0
        self.session_cleanup_done = False
        self.files = []
        self.fetches = {}
        self.spool_errors = {}
        self.uploads = {}
        self.upload_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        self.stats = self.state.get('totals', {'docs': 0, 'records': 0, 'bytes': 0})
        self.stats.pop('parse_failed', None)
        self.started = time.time()
        self.position = 0
        self.file_walker = Walker()
        self.reindex_doc_id = None

    def checkpoint(self, key, default=None):
        if key not in self.checkpoints:
            self.checkpoints[key] = self.state.get(key, default)
        return self.checkpoints[key]

    def queue_checkpoint(self, key, value):
        self.pending[key] = value
        self.checkpoints[key] = value

    def commit(self):
        self.packs.sync()
        self.index.commit()
        if self.maintenance:
            if self.maintenance['source']:
                self.maintenance['seen'] = sorted(self.reparse_seen)
            self.queue_checkpoint('maintenance', self.maintenance)
        for source in self.context_dirty:
            key = 'ctx:' + source
            value = self.checkpoint(key, {})
            digest = hashlib.sha256(dumps(value).encode()).digest()
            if digest != self.context_hashes[source]:
                self.pending[key] = value
                self.context_hashes[source] = digest
        self.context_dirty.clear()
        for key, value in self.pending.items():
            self.state.set(key, value)
        self.state.set('totals', self.stats)
        self.state.commit()
        self.pending.clear()
        self.last_commit = time.monotonic()
        atomic(
            self.home / 'heartbeat.json',
            {'time': time.time(), 'started': self.started, **self.stats},
        )
        if self.publish_on_commit:
            self.publish_status()

    def raw_header(self, ctx, capture, kind='raw', metadata=None):
        # Native coordinates belong to the source version; parser context stays in checkpoints.
        fields = (
            'account',
            'calendar_id',
            'key',
            'runtime',
            'session',
            'agent',
            'offset',
            'root',
            'relative',
            'deleted',
            'size',
            'state',
            'raw_sha256',
            'item_time',
            'credential',
            'origin_start',
            'payload_start',
        )
        native = {key: ctx[key] for key in fields if key in ctx}
        native['capture'] = capture
        imported = (metadata or {}).get('imported_from', ctx.get('imported_from'))
        if imported:
            native['imported_from'] = imported
        return {
            'id': ctx.get('key', ctx.get('session', '')),
            'time': ctx.get('time', ''),
            'kind': kind,
            'version': native,
        }

    def record(self, source, raw, ctx, docs=None, capture=None, metadata=None, expected=None):
        capture = capture if capture is not None else time.time_ns()
        persisted = self.checkpoint('ctx:' + source, {})
        if persisted is None:
            persisted = {}
            self.checkpoints['ctx:' + source] = persisted
        if source not in self.context_hashes:
            self.context_hashes[source] = hashlib.sha256(dumps(persisted).encode()).digest()
        update = None
        if docs is None:
            try:
                update = json.loads(raw).get('ctx_update')
            except (ValueError, AttributeError):
                pass
        if isinstance(update, dict):
            for key, value in update.items():
                if isinstance(value, dict):
                    current = persisted.setdefault(key, {})
                    for name, item in value.items():
                        if current.get(name) != item:
                            current[name] = item
                            self.context_dirty.add(source)
                elif persisted.get(key) != value:
                    persisted[key] = value
                    self.context_dirty.add(source)
        origin = self.packs.append(
            'raw', source, self.raw_header(ctx, capture, metadata=metadata), raw, expected=expected
        )
        if docs is None:
            parser = next((p for name, p, _, _, *rest in SOURCES if name == source), None)
            if parser is None:
                raise ValueError('source parser unavailable')
            docs = parser(raw, {**ctx, **persisted})
        docs = list(docs)
        if source in ('whatsapp', 'x'):
            enqueue_auto(
                self.home, raw, [doc for doc in docs if not isinstance(doc, Exclusion)], capture
            )
        self.documents(
            source,
            docs,
            origin,
            capture,
            item_time=ctx.get('item_time') or ctx.get('time'),
        )
        if docs:
            self.processed(source, ctx.get('item_time'))
        self.stats['records'] += 1
        self.stats['bytes'] += len(raw)
        return origin

    def exclude_item(self, source, item, origin, capture, reparse):
        # One parser outcome removes the item and its independently stored outputs.
        ids = {item}
        ids.update(id for id, _ in self.index.searcher.terms_with_prefix('id', item + '/att/'))
        ids.update(id for id in self.index.pending if id.startswith(item + '/att/'))
        for id in ids:
            old = self.index.lookup(id)
            current = self.index.pending.get(id) or (old['version'] if old else None)
            doc = dict(
                id=id,
                item=old['item'] if old else id,
                source=source,
                group=old['group'] if old else 'mail',
                kind='tombstone',
                time=current['time'] if current else None,
                parts=[],
                parser=current.get('parser', '') if current else '',
            )
            deleted_at = max(capture, current['capture'] + 1) if current else capture
            pointer = self.append_doc(
                source,
                doc,
                origin,
                deleted_at,
                reparse=reparse,
                epoch=current.get('epoch', 0) if current else None,
            )
            self.index.upsert(doc, pointer, refresh=reparse)

    def append_doc(self, source, doc, origin, capture, reparse=False, epoch=None):
        doc['raw'] = origin
        doc['version'] = version(
            doc, capture, self.checkpoint('epoch:' + source, 0) if epoch is None else epoch
        )
        return self.packs.append(
            'doc',
            source,
            {
                'id': doc['id'],
                'time': doc.get('time'),
                'version': doc['version'],
                'kind': doc['kind'],
                'reparse': reparse,
            },
            dumps(doc).encode(),
        )

    def processed(self, source, item_time):
        if item_time is None:
            return
        stamp = date(item_time).timestamp()
        indexed = self.checkpoint('indexed:' + source, {}) or {}
        if stamp > indexed.get('processed_through', 0):
            indexed['processed_through'] = stamp
            self.queue_checkpoint('indexed:' + source, indexed)

    def documents(
        self, source, docs, origin, capture, refresh=False, reparse=False, item_time=None
    ):
        for doc in docs:
            if isinstance(doc, Exclusion):
                if not refresh and not reparse:
                    reasons = dict(self.checkpoint('parser_exclusions:' + source, {}) or {})
                    reasons[doc.reason] = reasons.get(doc.reason, 0) + 1
                    self.queue_checkpoint('parser_exclusions:' + source, reasons)
                if doc.item:
                    self.exclude_item(source, doc.item, origin, capture, refresh or reparse)
                continue
            parent = doc.get('context', {}).get('parent')
            if parent and self.state.get('deleted:' + parent):
                continue
            outcome_time = item_time if item_time is not None else capture / 1_000_000_000
            self.processed(source, outcome_time)
            if source == 'files' and doc['kind'] == 'tombstone':
                continue
            self.reindex_doc_id = doc['id']
            pointer = self.append_doc(source, doc, origin, capture, reparse=refresh or reparse)
            deferred = self.visible_index is not None
            scoped = self.maintenance and self.maintenance['source'] == source
            if scoped:
                self.reparse_seen.add(doc['id'])
            if not deferred and self.index.upsert(doc, pointer, refresh=refresh):
                self.stats['docs'] += doc['kind'] != 'attachment_outcome'
            if doc.get('parse_failed'):
                failures = self.checkpoint('parse_failures:' + source, 0)
                self.queue_checkpoint('parse_failures:' + source, failures + 1)
                self.queue_checkpoint('parse_failed_id:' + source, doc['id'])
                if self.maintenance and self.maintenance['source'] == source and not refresh:
                    self.maintenance['live_failures'] = self.maintenance.get('live_failures', 0) + 1
                    self.maintenance['live_failed_id'] = doc['id']
            if not deferred and doc['kind'] not in ('tombstone', 'attachment_outcome'):
                indexed = self.checkpoint('indexed:' + source, {}) or {}
                receipt = self.checkpoint('receipt:' + source, {}) or {}
                stamp = date(outcome_time).timestamp()
                if stamp >= indexed.get('searchable_through', 0):
                    indexed['newest_id'] = doc['id']
                indexed['searchable_through'] = max(
                    indexed.get('searchable_through', 0),
                    date(receipt.get('searchable_through')).timestamp(),
                    stamp,
                )
                self.queue_checkpoint('indexed:' + source, indexed)
                receipt['searchable_through'] = indexed['searchable_through']
                self.queue_checkpoint('receipt:' + source, receipt)

    def sessions(self, seconds=8):
        now = time.monotonic()
        if self.position == 0:
            if now - self.last_walk < 15:
                return
            self.start_session_walk(now)
            if not self.files:
                self.finish_session_walk()
        if not self.files:
            return
        deadline = now + seconds
        while time.monotonic() < deadline:
            source, path, _ = self.files[self.position]
            key = 'session:' + str(path)
            cp = self.checkpoint(key, {}) or {}
            operation = 'session stat'
            try:
                st = path.stat()
                operation = 'session read'
                ctx = {**context(path, source), **cp.get('ctx', {})}
                off = cp.get('offset', 0)
                if cp.get('inode') != st.st_ino or st.st_size < off:
                    ctx, off = context(path, source), 0
                if st.st_size > off:
                    with path.open('rb') as f:
                        f.seek(off)
                        for raw, before, docs, after in lines(f, ctx, 4 * 1024 * 1024):
                            if not excluded(before.get('project')):
                                self.record(source, raw, before, docs)
                            cp = {'inode': st.st_ino, 'offset': after['offset'], 'ctx': after}
                            self.queue_checkpoint(key, cp)
                            if time.monotonic() - self.last_commit >= 10:
                                self.commit()
                            if time.monotonic() >= deadline:
                                break
                    if not excluded(cp.get('ctx', ctx).get('project')):
                        self.walk_counts[source]['written'] += int(cp.get('offset', off) > off)
                    if (
                        cp.get('offset', off) < st.st_size
                        and st.st_size > cp.get('offset', off) + 1
                    ):
                        # Partial final lines wait for another pass; a bounded slice continues next turn.
                        with path.open('rb') as f:
                            f.seek(cp.get('offset', off))
                            if f.readline().endswith(b'\n'):
                                if time.monotonic() >= deadline:
                                    return
                                continue
                elif not excluded(ctx.get('project')):
                    self.walk_counts[source]['unchanged'] += 1
                ctx.update(cp.get('ctx', {}))
                if excluded(ctx.get('project')):
                    item = f"{source}:{ctx['session']}"
                    if ctx.get('agent'):
                        item += ':' + ctx['agent']
                    self.excluded_sessions.add((source, item))
                    self.walk_counts[source]['excluded'] += 1
                self.walk_counts[source]['enumerated'] += 1
                if cp.get('offset', off) >= st.st_size:
                    self.processed(source, st.st_mtime)
            except OSError as error:
                self.walk_counts[source]['enumerated'] += 1
                self.walk_counts[source]['failed'] += 1
                self.walk_errors[source].add(f'{operation}: {type(error).__name__}')
            self.position += 1
            if self.position >= len(self.files):
                self.position = 0
                self.finish_session_walk()
                return

    def start_session_walk(self, now):
        self.files, failed = inventory()
        self.last_walk = now
        self.walk_started = time.time()
        self.excluded_sessions = set()
        self.walk_errors = {s: set(failed.get(s, ())) for s in ('claude', 'codex', 'pi')}
        self.walk_counts = {
            s: {
                'enumerated': 0,
                'written': 0,
                'unchanged': 0,
                'excluded': 0,
                'failed': int(s in failed),
            }
            for s in ('claude', 'codex', 'pi')
        }

    def finish_session_walk(self):
        self.commit()
        # Sweep legacy-only items once per process; later walks use each file's context.
        if not self.session_cleanup_done:
            query = self.index.index.parse_query(
                ' OR '.join('"' + p + '"' for p in EXCLUDED_PROJECTS), ['context']
            )
            for old in self.index.scan(source='sessions', query=query):
                _, raw = self.packs.read(old['pointer'])
                doc = json.loads(raw)
                if excluded(doc.get('context', {}).get('project')):
                    self.excluded_sessions.add((old['source'], old['item']))
        for source, item in sorted(self.excluded_sessions):
            for old in self.index.item(item):
                self.documents(
                    source, [self.tombstone(source, old)], old['version']['raw'], time.time_ns()
                )
            if time.monotonic() - self.last_commit >= 10:
                self.commit()
        self.session_cleanup_done = True
        for source, counts in self.walk_counts.items():
            receipt = self.checkpoint('receipt:' + source, {}) or {}
            counts['enumerated'] = sum(
                counts[k] for k in ('written', 'unchanged', 'excluded', 'failed')
            )
            receipt = replace_receipt(
                receipt,
                dict(
                    counts,
                    source=source,
                    ok=counts['failed'] == 0,
                    error='; '.join(sorted(self.walk_errors[source])) or None,
                    duration_s=time.time() - self.walk_started,
                    source_head=max(
                        (st.st_mtime for runtime, _, st in self.files if runtime == source),
                        default=0,
                    ),
                ),
                self.walk_started,
            )
            self.queue_checkpoint('receipt:' + source, receipt)

    def spool(self, seconds=2):
        paths = sorted(
            p
            for p in (self.home / 'spool').glob('*/index.jsonl')
            if p.stat().st_size
            > (self.checkpoint('spool:' + str(p), {'offset': 0}) or {'offset': 0})['offset']
        )
        for path in paths:
            try:
                self.spool_file(path, seconds / max(1, len(paths)))
                self.spool_errors.pop(str(path), None)
            except Exception as e:
                self.spool_errors[str(path)] = type(e).__name__
                print(f'spool: {path.parent.name}: {type(e).__name__}', file=sys.stderr, flush=True)

    def spool_file(self, path, seconds):
        deadline = time.monotonic() + seconds
        root = path.parent
        key = 'spool:' + str(path)
        cp = self.checkpoint(key, {'offset': 0})
        with path.open('rb') as f:
            f.seek(cp['offset'])
            while time.monotonic() < deadline:
                if time.monotonic() - self.last_commit >= 10:
                    self.commit()
                off = f.tell()
                line = f.readline()
                if not line or not line.endswith(b'\n'):
                    break
                entry = json.loads(line)
                if entry.get('schema') != 'qmd.spool-entry.v1':
                    raise ValueError('foreign spool entry')
                source = entry['source'].removeprefix('session-')
                labels = entry.get('state', {}).get('labels', {})
                archive_only = labels.get('archive_only') == 'true'
                if source not in [s for s, _, _, _, *rest in SOURCES] and not (
                    archive_only and source in ARCHIVE_ONLY_SOURCES
                ):
                    f.seek(off)
                    break
                payload = root / entry['path']
                if not payload.resolve().is_relative_to(root.resolve()):
                    raise ValueError('unsafe spool path')
                capture = int(date(entry['captured_at']).timestamp() * 1e9)
                if archive_only:
                    origin = self.archive_spool_payload(source, payload, entry, labels, capture)
                    self.queue_checkpoint('consumed:' + str(payload), [origin])
                    cp = {'offset': f.tell()}
                    self.queue_checkpoint(key, cp)
                    continue
                if capture < (self.checkpoint('reset:' + source, 0) or 0):
                    origin = self.archive_reset_payload(source, payload, entry, capture)
                    self.queue_checkpoint('consumed:' + str(payload), [origin])
                    cp = {'offset': f.tell()}
                    self.queue_checkpoint(key, cp)
                    continue
                if source in ('claude', 'codex', 'pi'):
                    origin = self.import_session_payload(
                        source, payload, entry, labels, cp, key, off, capture, deadline
                    )
                    if origin is None:
                        return
                else:
                    origin = self.parse_spool_payload(source, payload, entry, labels, capture)
                consumed = self.checkpoint('consumed:' + str(payload), [])
                if origin not in consumed:
                    consumed.append(origin)
                self.queue_checkpoint('consumed:' + str(payload), consumed)
                cp = {'offset': f.tell()}
                self.queue_checkpoint(key, cp)

    def archive_spool_payload(self, source, payload, entry, labels, capture):
        header = self.raw_header(
            {'key': entry['key'], 'time': entry['captured_at']},
            capture,
            'archive-only',
            {'imported_from': labels.get('object_hash', entry['sha256'])},
        )
        header['version'] = ARCHIVE_ONLY_SOURCES.get(source, header['version'])
        header['labels'] = labels
        with payload.open('rb') as body:
            origin = self.packs.append('raw', source, header, body, expected=entry)
        self.stats['records'] += 1
        self.stats['bytes'] += entry['bytes']
        return origin

    def archive_reset_payload(self, source, payload, entry, capture):
        with payload.open('rb') as body:
            origin = self.packs.append(
                'raw',
                source,
                self.raw_header(
                    {'key': entry['key'], 'time': entry['captured_at']},
                    capture,
                    'reset-history',
                ),
                body,
                expected=entry,
            )
        return origin

    def import_session_payload(
        self, source, payload, entry, labels, cp, key, off, capture, deadline
    ):
        progress = cp.get('payload', {}) if cp.get('entry') == off else {}
        with payload.open('rb') as body:
            ctx = progress.get('ctx') or (
                json.loads(labels['ctx'])
                if labels.get('ctx')
                else import_context(body, source, entry['key'])
            )
            start = ctx.get('payload_start', 0)
            base = ctx.get('origin_start', ctx.get('offset', 0))
            ctx['origin_start'] = base
            origin = progress.get('origin')
            if origin is None:
                body.seek(0)
                origin = self.packs.append(
                    'raw',
                    source,
                    self.raw_header(dict(ctx, key=entry['key']), capture, 'session-import', labels),
                    body,
                    expected=entry,
                )
                self.stats['records'] += 1
                self.stats['bytes'] += entry['bytes']
            else:
                body.seek(0)
                payload_integrity(body, entry)
            body.seek(progress.get('offset', start))
            while time.monotonic() < deadline:
                pos = body.tell()
                raw = body.readline()
                if not raw:
                    break
                try:
                    row = json.loads(raw)
                except ValueError:
                    row = {}
                before = prepare(row, dict(ctx, offset=base + pos - start))
                docs = parse(raw, before)
                raw_pointer = dict(origin, payload_off=pos, payload_len=len(raw))
                self.documents(source, docs, raw_pointer, capture)
                ctx = completed(row, before)
                progress = {'offset': body.tell(), 'ctx': ctx, 'origin': origin}
                self.queue_checkpoint(key, {'offset': off, 'entry': off, 'payload': progress})
                if time.monotonic() - self.last_commit >= 10:
                    self.commit()
            if body.tell() < entry['bytes']:
                return None
        return origin

    def parse_spool_payload(self, source, payload, entry, labels, capture):
        if entry['bytes'] >= 50 * 1024 * 1024:
            raise ValueError('source payload exceeds 50 MB')
        raw = payload.read_bytes()
        ctx = {
            'source': source,
            'account': entry['account'],
            'key': entry['key'],
            'time': entry['captured_at'],
            'item_time': entry.get('item_time'),
            **labels,
        }
        origin = self.record(
            source, raw, ctx, capture=capture, metadata={'spool': str(payload)}, expected=entry
        )
        return origin

    def fetch(self):
        for source, _, argv, cadence, *options in SOURCES:
            if not argv or not cadence:
                continue
            job = self.fetches.get(source)
            if job and job.get('process') and job['process'].poll() is None:
                if time.monotonic() - job['started'] > (options[0] if options else 300):
                    job['process'].kill()
                continue
            if job and not job.get('received'):
                self.receive_capture(source, job)
            if job and time.monotonic() < job['started'] + cadence:
                continue
            self.start_capture(source, argv)

    def receive_capture(self, source, job):
        previous = self.checkpoint('receipt:' + source, {}) or {}
        try:
            data = json.loads(job['output'].read_text())
            counts = [data[k] for k in ('enumerated', 'written', 'unchanged', 'excluded', 'failed')]
            if (
                data.get('source') != source
                or type(data['ok']) is not bool
                or any(type(count) is not int or count < 0 for count in counts)
                or counts[0] != sum(counts[1:])
            ):
                raise ValueError()
            if data['ok'] and job['process'].returncode != 0:
                raise ValueError()
            receipt = data
        except (OSError, ValueError, TypeError, AttributeError, KeyError):
            receipt = dict(
                source=source,
                ok=False,
                enumerated=1,
                written=0,
                unchanged=0,
                excluded=0,
                failed=1,
                rate_limited=False,
            )
        receipt = replace_receipt(
            previous,
            receipt,
            job.get('started_at', time.time() - receipt.get('duration_s', 0)),
        )
        self.queue_checkpoint('receipt:' + source, receipt)
        job['received'] = True

    def start_capture(self, source, argv):
        output = self.home / 'spool' / source / 'receipt.json'
        output.parent.mkdir(parents=True, exist_ok=True)
        try:
            with output.open('wb') as f:
                process = subprocess.Popen(argv, stdout=f, stderr=subprocess.DEVNULL)
            self.fetches[source] = {
                'process': process,
                'started_at': time.time(),
                'started': time.monotonic(),
                'output': output,
            }
        except OSError:
            receipt = replace_receipt(
                self.checkpoint('receipt:' + source, {}) or {},
                dict(
                    source=source,
                    ok=False,
                    enumerated=1,
                    written=0,
                    unchanged=0,
                    excluded=0,
                    failed=1,
                ),
                time.time(),
            )
            self.queue_checkpoint('receipt:' + source, receipt)
            self.fetches[source] = {
                'process': None,
                'started': time.monotonic(),
                'received': True,
            }
            print(f'capture: {source}: unavailable', file=sys.stderr, flush=True)

    def publish_status(self):
        started = time.monotonic()
        if started - self.last_status < STATUS_INTERVAL:
            return
        now = time.time()
        sources = {}
        visible = self.visible_index or self.index
        for source, _, _, cadence, *_ in SOURCES:
            sources[source] = self.source_status(source, cadence, visible, now)
        self.status_pending(sources)
        # A sealed file's mtime is its seal time; open packs age from creation.
        backlog = [
            {
                'source': row['source'],
                'stream': row['stream'],
                'bytes': row['size'],
                'sealed': bool(row['sealed']),
                'age_s': now
                - (
                    self.packs.path(row['stream'], row['source'], row['seq'], True).stat().st_mtime
                    if row['sealed']
                    else row['created']
                ),
                'created_at': row['created'],
            }
            for row in self.state.packs()
            if not row['uploaded']
        ]
        cold_id = self.status_cold_sample(visible)
        for row in sources.values():
            row['last_complete_at'] = row['last_complete']
            row['oldest_failed_at'] = row['oldest_failed']
            row['oldest_pending_at'] = row['oldest_pending']
            row['newest_indexed_time'] = row['searchable_through']
            row['lag_s'] = head_lag(row)
        atomic(
            self.home / 'status.json',
            {
                'written_at': now,
                'heartbeat_at': now,
                'cold_sample_id': cold_id,
                'free_bytes': shutil.disk_usage(self.home).free,
                'backlog': backlog,
                'spool_errors': dict(self.spool_errors),
                'seal_errors': [
                    dict(stream=stream, source=source, seq=seq, error=error)
                    for (stream, source, seq), error in self.packs.seal_errors.items()
                ],
                'totals': self.stats,
                'sources': sources,
                'files_archive_bytes': self.state.get('files-archive-totals', {}),
                'files_moving': self.state.get('files-moving', 0),
                'attachments': attachment_status(self.home, self.visible_index or self.index),
                'maintenance': self.waiting_for_lease
                or (
                    {
                        **{
                            key: self.maintenance.get(key)
                            for key in ('source', 'stage', 'started_at', 'last_slice_at')
                        },
                        'lease_owner': 'ingestd reindex ' + self.reindex_id
                        if self.heavy_lock is not None
                        else None,
                    }
                    if self.maintenance
                    else None
                ),
            },
        )

        self.last_status = started

    def status_receipt(self, source, now):
        receipt = self.state.get('receipt:' + source, {}) or {}
        receipt = {
            'source': source,
            'ok': None,
            'enumerated': 0,
            'written': 0,
            'unchanged': 0,
            'excluded': 0,
            'failed': 0,
            'duration_s': 0,
            'source_head': 0,
            **receipt,
        }
        for key, value in list(receipt.items()):
            if value is not None and (
                key
                in (
                    'source_head',
                    'connection_head',
                    'last_complete',
                    'last_run',
                    'oldest_failed',
                    'oldest_pending',
                    'searchable_through',
                )
                or key.endswith('_at')
            ):
                stamp = date(value) if value != '' else None
                receipt[key] = None if stamp is None or stamp.year == 1 else stamp.timestamp()
        if receipt.get('failed') and not receipt.get('oldest_failed'):
            receipt['oldest_failed'] = receipt.get('last_run', self.started)
        return receipt

    def source_status(self, source, cadence, visible, now):
        receipt = self.status_receipt(source, now)
        indexed = self.state.get('indexed:' + source, {}) or {}
        hits = visible.searcher.search(
            visible.filters(visible.term('source', source), exclude=False),
            limit=1,
            order_by_field='time',
        ).hits
        recovered = {
            **indexed,
            'newest_id': visible.row(hits[0][1])['id'] if hits else None,
        }
        if recovered != indexed:
            self.queue_checkpoint('indexed:' + source, recovered)
        indexed = recovered
        return {
            **receipt,
            'receipt': receipt,
            'cadence_s': cadence,
            'last_complete': receipt.get('last_complete'),
            'fresh_through': receipt.get('fresh_through'),
            'oldest_failed': receipt.get('oldest_failed'),
            'oldest_pending': receipt.get('oldest_pending'),
            'searchable_through': indexed.get('searchable_through'),
            'processed_through': indexed.get('processed_through'),
            'parser_exclusions': self.state.get('parser_exclusions:' + source, {}) or {},
            'parser_epoch': self.state.get('epoch:' + source, 0),
            'parser_reset_at': self.state.get('reset:' + source, 0),
            'newest_id': indexed.get('newest_id'),
            'parse_failures': self.state.get('parse_failures:' + source, 0),
            'legacy': self.state.get('legacy:' + source, 0),
        }

    def status_pending(self, sources):
        for runtime, path, st in self.files:
            cp = self.state.get('session:' + str(path), {})
            if st.st_size > cp.get('offset', 0) and runtime in sources:
                old = sources[runtime]['oldest_pending']
                sources[runtime]['oldest_pending'] = (
                    min(old, self.walk_started) if old else self.walk_started
                )
        for path in (self.home / 'spool').glob('*/index.jsonl'):
            cp = self.state.get('spool:' + str(path), {'offset': 0})
            with path.open('rb') as f:
                f.seek(cp['offset'])
                line = f.readline()
            if not line:
                self.spool_errors.pop(str(path), None)
                continue
            if not line.endswith(b'\n'):
                continue
            try:
                entry = json.loads(line)
                source = entry['source'].removeprefix('session-')
                if source in sources:
                    try:
                        pending = (path.parent / entry['path']).stat().st_mtime
                    except FileNotFoundError:
                        pending = date(entry['captured_at']).timestamp()
                    old = sources[source]['oldest_pending']
                    sources[source]['oldest_pending'] = min(old, pending) if old else pending
            except (ValueError, KeyError, TypeError, AttributeError) as error:
                self.spool_errors[str(path)] = type(error).__name__

    def status_cold_sample(self, visible):
        def valid(candidate):
            row = visible.lookup(candidate) if candidate else None
            if not row:
                return False
            pointer = row['pointer']
            return (
                self.state.db.execute(
                    'SELECT 1 FROM packs WHERE stream=? AND source=? AND seq=? AND uploaded=1',
                    (pointer['stream'], pointer['source'], pointer['seq']),
                ).fetchone()
                is not None
            )

        cold_id = self.state.get('cold_sample_id')
        if valid(cold_id):
            return cold_id
        uploaded = {
            (pack['source'], pack['seq']) for pack in self.state.packs('doc') if pack['uploaded']
        }
        cold_id = next(
            (
                row['id']
                for source in sorted({source for source, _ in uploaded})
                for row in visible.scan(source=source)
                if (row['pointer']['source'], row['pointer']['seq']) in uploaded
            ),
            None,
        )
        self.queue_checkpoint('cold_sample_id', cold_id)
        return cold_id

    def upload(self):
        for key, future in list(self.uploads.items()):
            if not future.done():
                continue
            try:
                future.result()
                self.state.db.execute(
                    'UPDATE packs SET uploaded=1 WHERE stream=? AND source=? AND seq=?', key
                )
            except Exception as e:
                print(f'upload: {key[1]}: {type(e).__name__}', file=sys.stderr, flush=True)
            del self.uploads[key]
        if not self.b2 or len(self.uploads) >= 2:
            return
        for row in self.state.db.execute(
            'SELECT * FROM packs WHERE sealed=1 AND uploaded=0 ORDER BY created LIMIT 2'
        ).fetchall():
            key = row['stream'], row['source'], row['seq']
            if key not in self.uploads and len(self.uploads) < 2:
                self.uploads[key] = self.upload_pool.submit(
                    self.b2.put, self.b2.key(*key), self.packs.path(*key, True)
                )

    def cleanup_spool(self):
        referenced = set()
        for index in (self.home / 'spool').glob('*/index.jsonl'):
            cp = self.state.get('spool:' + str(index), {'offset': 0})
            with index.open('rb') as f:
                f.seek(cp['offset'])
                for line in f:
                    try:
                        entry = json.loads(line)
                        referenced.add(str(index.parent / entry['path']))
                    except (ValueError, KeyError):
                        continue
        for row in self.state.db.execute(
            "SELECT key,value FROM checkpoints WHERE key LIKE 'consumed:%'"
        ).fetchall():
            locs = json.loads(row['value'])
            archived = all(
                self.state.db.execute(
                    'SELECT uploaded FROM packs WHERE stream=? AND source=? AND seq=?',
                    (p['stream'], p['source'], p['seq']),
                ).fetchone()[0]
                for p in locs
            )
            path = Path(row['key'][9:])
            root = path.relative_to(self.home / 'spool').parts[0]
            job = self.fetches.get(root)
            producing = job and job.get('process') and job['process'].poll() is None
            if archived and str(path) not in referenced and not producing:
                path.unlink(missing_ok=True)
                self.state.db.execute('DELETE FROM checkpoints WHERE key=?', (row['key'],))

    def tombstone(self, source, old):
        return {
            'id': old['id'],
            'item': old['item'],
            'source': source,
            'group': old['group'],
            'kind': 'tombstone',
            'time': old['version']['time'],
            'parts': [],
            'parser': old['version'].get('parser', ''),
        }

    def reset_source(self, source):
        self.commit()
        job = self.fetches.pop(source, None)
        if job and job.get('process') and job['process'].poll() is None:
            job['process'].kill()
            job['process'].wait(timeout=5)
        ids = [id for id, _ in self.index.bindings(source)]
        for id in ids:
            old = self.index.lookup(id)
            tombstone = self.tombstone(source, old)
            self.documents(source, [tombstone], old['version']['raw'], time.time_ns())
        self.context_hashes.pop(source, None)
        self.context_dirty.discard(source)
        self.queue_checkpoint('ctx:' + source, {})
        self.queue_checkpoint('indexed:' + source, {})
        self.queue_checkpoint('reset:' + source, time.time_ns())
        self.queue_checkpoint(
            'receipt:' + source,
            {
                'source': source,
                'ok': False,
                'error': 'source reset: awaiting capture',
                'enumerated': 0,
                'written': 0,
                'unchanged': 0,
                'excluded': 0,
                'failed': 0,
                'duration_s': 0,
                'source_head': 0,
            },
        )
        self.queue_checkpoint('parse_failures:' + source, 0)
        self.queue_checkpoint('legacy:' + source, 0)
        argv = next(argv for name, _, argv, _, *rest in SOURCES if name == source)
        if argv and '--state' in argv:
            path = Path(argv[argv.index('--state') + 1])
            if not path.is_relative_to(self.home / 'capture-state'):
                raise ValueError('capture state outside qmd2 home')
            shutil.rmtree(path, ignore_errors=True)
        self.commit()
        return len(ids)

    def swap_rebuild(self):
        target, old = self.home / 'index-rebuild', self.home / 'index-old'
        if target.exists():
            if (self.home / 'index').exists():
                shutil.rmtree(old, ignore_errors=True)
                os.rename(self.home / 'index', old)
            os.rename(target, self.home / 'index')
        self.visible_index = None
        self.index = Index(self.home, True, self.state)
        shutil.rmtree(old, ignore_errors=True)

    def start_reindex(self, source, request=None):
        self.commit()
        bounds = {}
        if source:
            self.queue_checkpoint('indexed:' + source, {})
            receipt = dict(self.checkpoint('receipt:' + source, {}) or {})
            receipt.pop('searchable_through', None)
            self.queue_checkpoint('receipt:' + source, receipt)
            self.queue_checkpoint('epoch:' + source, self.checkpoint('epoch:' + source, 0) + 1)
            for row in self.state.packs('raw'):
                if row['source'] == source:
                    frames = json.loads(row['frames']) if row['frames'] else None
                    bounds[str(row['seq'])] = (
                        frames[-1][0] + frames[-1][1]
                        if frames
                        else None
                        if row['sealed']
                        else row['size']
                    )
        self.maintenance = {
            'source': source,
            'request': request,
            'stage': 'prepare' if source else 'read',
            'started_at': time.time(),
            'last_slice_at': None,
            'cursor': {},
            'sealed_done': [],
            'bounds': bounds,
            'contexts': {},
            'failures': 0,
            'failed_id': None,
        }
        self.reparse_seen = set()
        if not source:
            target = self.home / 'index-rebuild'
            shutil.rmtree(target, ignore_errors=True)
            self.visible_index = Index(self.home)
            self.index.writer.wait_merging_threads()
            del self.index
            self.index = Index(self.home, True, self.state, target)
        self.commit()

    def reparse_entry(self, source, doc, origin, capture, item_time=None):
        self.documents(source, [doc], origin, capture, refresh=True, item_time=item_time)
        if not isinstance(doc, Exclusion) and doc.get('parse_failed'):
            self.maintenance['failures'] += 1
            self.maintenance['failed_id'] = doc['id']

    def reparse_raw(self, origin, head, raw, deadline):
        job, source = self.maintenance, self.maintenance['source']
        native = head.get('version', {})
        native = native if isinstance(native, dict) else {}
        capture = native.get('capture', head.get('capture', 0))
        if head.get('kind') == 'archive-only' and source == 'files' and native.get('root'):
            raw = b''
        elif head.get('kind') in ('archive-only', 'reset-history'):
            return True
        if capture < (self.checkpoint('reset:' + source, 0) or 0):
            return True
        if head.get('kind') == 'session-import':
            return self.reparse_session_import(origin, head, raw, deadline, native, capture)
        parser = next(p for s, p, _, _, *rest in SOURCES if s == source)
        identity = dumps([native.get('session', head['id']), native.get('agent')])
        contexts = job['contexts']
        if source in ('claude', 'codex', 'pi'):
            ctx = {**contexts.get(identity, {}), **head.get('ctx', {}), **native, 'runtime': source}
            try:
                record = json.loads(raw)
            except ValueError:
                record = {}
            ctx = prepare(record, ctx)
            docs = list(parser(raw, ctx))
            for doc in docs:
                self.reparse_entry(
                    source,
                    doc,
                    origin,
                    capture,
                    item_time=native.get('item_time') or head.get('time'),
                )
            contexts[identity] = completed(record, ctx)
        else:
            ctx = {
                **head.get('ctx', {}),
                **native,
                'key': native.get('key', head['id']),
                'time': head['time'],
                **(self.checkpoint('ctx:' + source, {}) or {}),
            }
            if source == 'files':
                ctx = {**head.get('ctx', {}), **native, 'time': head['time']}
            docs = list(parser(raw, ctx))
            for doc in docs:
                self.reparse_entry(
                    source,
                    doc,
                    origin,
                    capture,
                    item_time=native.get('item_time') or head.get('time'),
                )
        if docs:
            self.processed(source, native.get('item_time'))
        return True

    def reparse_session_import(self, origin, head, raw, deadline, native, capture):
        job, source = self.maintenance, self.maintenance['source']
        parser = next(p for s, p, _, _, *rest in SOURCES if s == source)
        identity = dumps([native.get('session', head['id']), native.get('agent')])
        contexts = job['contexts']
        body = io.BytesIO(raw)
        initial = import_context(body, source, head['id'])
        ctx = {**initial, **head.get('ctx', {}), **contexts.get(identity, {}), **native}
        base = native.get('origin_start', ctx['offset'])
        start = native.get('payload_start', body.tell())
        body.seek(job.get('payload_off', start))
        while line := body.readline():
            pos = body.tell() - len(line)
            try:
                record = json.loads(line)
            except ValueError:
                record = {}
            ctx = prepare(record, dict(ctx, offset=base + pos - start))
            for doc in parser(line, ctx):
                self.reparse_entry(
                    source, doc, dict(origin, payload_off=pos, payload_len=len(line)), capture
                )
            ctx = completed(record, ctx)
            contexts[identity] = ctx
            job['payload_off'] = body.tell()
            if time.monotonic() >= deadline:
                return False
            if time.monotonic() - self.last_commit >= 10:
                self.commit()
        job.pop('payload_off', None)
        return True

    def reindex(self, source=None, seconds=60, request=None):
        # Each slice stops at its deadline (at most 60 s); per-pack bounds persist with the
        # maintenance job so restarts resume there.
        deadline = time.monotonic() + min(seconds, 60)
        if not self.maintenance:
            self.start_reindex(source, request)
        job = self.maintenance
        job['last_slice_at'] = time.time()
        source = job['source']
        worked = False
        rows = self.state.packs('raw' if source else 'doc')
        if job['stage'] == 'prepare':
            for row in rows:
                key = str(row['seq'])
                if row['source'] == source and key in job['bounds'] and job['bounds'][key] is None:
                    frames = self.packs.frame_map(dict(stream='raw', source=source, seq=row['seq']))
                    worked = True
                    job['bounds'][key] = frames[-1][0] + frames[-1][1]
                    if time.monotonic() >= deadline:
                        self.commit()
                        return False
            job['stage'] = 'read'
        if job['stage'] == 'read':
            if not self.read_reindex_packs(job, rows, deadline, worked):
                return False
        if job['stage'] == 'tombstones':
            if not self.reindex_tombstones(job, deadline):
                return False
        if job['stage'] == 'swap' and self.visible_index is not None:
            self.index.writer.wait_merging_threads()
            del self.index
            self.swap_rebuild()
        self.maintenance = None
        self.reparse_seen.clear()
        self.queue_checkpoint('maintenance', None)
        self.commit()
        return True

    def read_reindex_packs(self, job, rows, deadline, worked):
        source = job['source']
        sealed_done = set(job['sealed_done'])
        for row in rows:
            key = str(row['seq']) if source else row['source'] + ':' + str(row['seq'])
            if source and (row['source'] != source or key not in job['bounds']):
                continue
            start = job['cursor'].get(key, 0)
            if key in sealed_done or (source and start >= job['bounds'][key]):
                continue
            if not source and not row['sealed'] and start >= row['size']:
                continue
            if time.monotonic() >= deadline and worked:
                self.commit()
                return False
            worked = True
            self.reindex_doc_id = None
            for pointer, head, body in self.packs.iter_pack(row, start):
                self.reindex_doc_id = None
                if source:
                    if pointer['rec_off'] >= job['bounds'][key]:
                        break
                    if not self.reparse_raw(pointer, head, body, deadline):
                        self.commit()
                        return False
                else:
                    doc = json.loads(body)
                    self.reindex_doc_id = doc['id']
                    self.index.upsert(doc, pointer)
                    key_epoch = 'epoch:' + doc['source']
                    self.queue_checkpoint(
                        key_epoch,
                        max(self.checkpoint(key_epoch, 0), doc['version'].get('epoch', 0)),
                    )
                job['cursor'][key] = pointer['rec_off'] + pointer['rec_len']
                if time.monotonic() >= deadline:
                    self.commit()
                    return False
                if time.monotonic() - self.last_commit >= 10:
                    self.commit()
            if row['sealed'] and key not in sealed_done:
                job['sealed_done'].append(key)
                sealed_done.add(key)
        self.commit()
        job['stage'] = 'tombstones' if source else 'swap'
        if not source:
            self.commit()
        if time.monotonic() >= deadline and worked:
            self.commit()
            return False
        return True

    def reindex_tombstones(self, job, deadline):
        source = job['source']
        if 'candidates' not in job:
            job['candidates'] = [
                id
                for id, _ in self.index.searcher.terms_with_prefix(
                    'id', '', self.index.term('source', source)
                )
                if id not in self.reparse_seen
            ]
            job['position'] = 0
            self.commit()
        while job['position'] < len(job['candidates']):
            id = job['candidates'][job['position']]
            self.reindex_doc_id = id
            old = self.index.lookup(id)
            raw = old['version'].get('raw', {}) if old else {}
            bound = job['bounds'].get(str(raw.get('seq')), 0)
            if old and raw.get('source') == source and raw.get('rec_off', 0) < bound:
                doc = self.tombstone(source, old)
                self.documents(source, [doc], raw, time.time_ns(), reparse=True)
            job['position'] += 1
            if time.monotonic() >= deadline:
                self.commit()
                return False
            if time.monotonic() - self.last_commit >= 10:
                self.commit()
        self.queue_checkpoint(
            'parse_failures:' + source, job['failures'] + job.get('live_failures', 0)
        )
        self.queue_checkpoint(
            'parse_failed_id:' + source, job.get('live_failed_id') or job['failed_id']
        )
        return True

    def request(self, seconds=60):
        path = self.home / 'reindex.request'
        if not path.exists():
            self.waiting_for_lease = None
            return
        request = json.loads(path.read_text())
        if self.heavy_lock is None:
            lock = LEASE.open('a')
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                lock.close()
                waiting = dict(
                    source=request.get('source'),
                    stage='waiting_for_lease',
                    started_at=path.stat().st_mtime,
                    last_slice_at=None,
                    lease_owner=None,
                )
                if waiting != self.waiting_for_lease:
                    self.waiting_for_lease = waiting
                    self.last_status = 0
                    self.publish_status()
                return
            self.heavy_lock = lock
            self.reindex_id = request['id']
        self.waiting_for_lease = None
        try:
            self.reindex_doc_id = None
            result = {'id': request['id'], 'ok': True}
            if request.get('reset'):
                result['tombstoned'] = self.reset_source(request['source'])
            else:
                if not self.reindex(request.get('source'), seconds, request):
                    return
            atomic(self.home / 'reindex.result', result)
        except Exception as e:
            atomic(
                self.home / 'reindex.result',
                {
                    'id': request['id'],
                    'ok': False,
                    'exception_class': type(e).__name__,
                    'doc_id': self.reindex_doc_id,
                },
            )
            if self.visible_index is not None:
                self.index.writer.wait_merging_threads()
                del self.index
                self.visible_index = None
                self.index = Index(self.home, True, self.state)
            self.maintenance = None
            self.reparse_seen.clear()
            self.queue_checkpoint('maintenance', None)
            self.commit()
        path.unlink(missing_ok=True)
        self.heavy_lock.close()
        self.heavy_lock = None

    def recover_attachment_output(self, row, deadline):
        parent = row['parent']['id']
        if parent != row['id']:
            current = self.index.lookup(parent)
            if (
                current is None
                or not visible(current)
                or not any(current.get(k) for k in ('body', 'title', 'from', 'context'))
            ):
                row.update(
                    state='unavailable',
                    reason='parent_deleted'
                    if self.state.get('deleted:' + parent)
                    else 'parent_unavailable',
                    ended_at=time.time(),
                )
                return
        lane = 'manual' if row['id'].startswith('manual:') else 'attachment'
        for pack in sorted(
            (p for p in self.state.packs('doc') if p['source'] == lane), key=lambda p: p['seq']
        ):
            for _, head, body in self.packs.iter_pack(pack):
                if time.monotonic() >= deadline:
                    raise TimeoutError('attachment archive lookup slice')
                if head['id'] != row.get('output_id', row['id']) or head.get('kind') == 'tombstone':
                    continue
                doc = json.loads(body)
                if doc.get('pipeline_revision') != row.get('pipeline_revision', 1):
                    continue
                if row.get('media_sha256') and doc.get('media_sha256') != row['media_sha256']:
                    continue
                if any(
                    p.get('text', '').strip() for p in doc.get('parts', []) if p['kind'] != 'header'
                ):
                    row.update(
                        output=doc,
                        pipeline_revision=doc['pipeline_revision'],
                        ended_at=row.get('done_at', row.get('ended_at', row.get('queued_at', 0))),
                    )
                    return
        row.update(state='failed', reason='output_missing', ended_at=time.time())

    def attachment_outcomes(self, seconds=55):
        """Migrate legacy history and acknowledge pending outputs in bounded, durable batches."""
        if self.maintenance:
            return
        deadline = time.monotonic() + min(seconds, 55)
        progress = dict(
            self.state.get('attachment-migration', {'migrated': 0, 'moved': 0, 'complete': False})
        )
        if progress['complete'] and not any((self.home / 'attachments/pending').glob('*.json')):
            return
        progress.pop('error', None)
        progress['row_errors'] = 0
        batch = []

        def row_error(path, row, error):
            progress['row_errors'] += 1
            progress['error'] = type(error).__name__
            if isinstance(row, dict) and row.get('migration_error') != progress['error']:
                atomic(path, dict(row, migration_error=progress['error']))

        def flush():
            if not batch:
                return
            self.commit()
            for path, row, lock, legacy in batch:
                try:
                    stored = self.index.lookup(row.get('output_id', row['id']))
                    if stored is None:
                        matches = self.index.item(row['id'])
                        stored = matches[-1] if matches else None
                    if not stored:
                        raise LookupError('attachment outcome readback missing')
                    doc = json.loads(self.packs.read(stored['pointer'])[1])
                    meta = dict(doc.get('outcome', {}), outcome_hash=doc['version']['hash'])
                    if not matches_outcome(meta, row):
                        raise LookupError('attachment outcome readback mismatch')
                    remove_queue(path)
                    if legacy:
                        progress['migrated'] += 1
                    if row.get('reason') == 'output_missing':
                        progress['lost_outputs'] = progress.get('lost_outputs', 0) + 1
                        progress['last_failure_at'] = row['ended_at']
                except Exception as error:
                    row_error(path, row, error)
                finally:
                    lock.close()
            batch.clear()

        try:
            for root in (self.home / 'attachments/pending', self.home / 'attachments/queue'):
                legacy = root.name == 'queue'
                if legacy and progress['complete']:
                    continue
                if not root.exists():
                    continue
                with os.scandir(root) as entries:
                    for entry in entries:
                        if time.monotonic() >= deadline:
                            break
                        if not entry.name.endswith('.json'):
                            continue
                        path = Path(entry.path)
                        lock = lock_path(path).open('a')
                        try:
                            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            lock.close()
                            continue
                        row = None
                        try:
                            row = json.loads(path.read_text())
                            if row['state'] == 'queued':
                                if legacy:
                                    target = queue_path(self.home, row['id'])
                                    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                                    publish_queue(target, row)
                                    remove_queue(path)
                                    progress['moved'] += 1
                                lock.close()
                                continue
                            existing = self.index.lookup(row.get('output_id', row['id']))
                            if existing is None:
                                matches = self.index.item(row['id'])
                                existing = matches[-1] if matches else None
                            old_version = (existing or {}).get('version', {})
                            meta = dict(
                                old_version.get('attachment', {}),
                                outcome_hash=old_version.get('hash'),
                            )
                            if not matches_outcome(meta, row):
                                if row['state'] == 'done' and 'output' not in row:
                                    if existing is None:
                                        self.recover_attachment_output(row, deadline)
                                    else:
                                        row['output'] = json.loads(
                                            self.packs.read(existing['pointer'])[1]
                                        )
                                doc = outcome_record(row)
                                lane = 'manual' if row['id'].startswith('manual:') else 'attachment'
                                self.record(lane, dumps(doc).encode(), {'key': doc['id']})
                            batch.append((path, row, lock, legacy))
                        except FileNotFoundError as error:
                            if path.exists():
                                row_error(path, row, error)
                            lock.close()
                        except Exception as error:
                            row_error(path, row, error)
                            lock.close()
                        if len(batch) >= 100:
                            flush()
                flush()
                if time.monotonic() >= deadline:
                    break
        except Exception as error:
            progress['error'] = type(error).__name__
        finally:
            flush()
        legacy = self.home / 'attachments/queue'
        with os.scandir(legacy) if legacy.exists() else contextlib.nullcontext(()) as entries:
            progress['remaining'] = sum(entry.name.endswith('.json') for entry in entries)
        progress['complete'] = progress['remaining'] == 0
        progress['updated_at'] = time.time()
        self.queue_checkpoint('attachment-migration', progress)
        self.commit()

    def turn(self):
        self.request()
        self.fetch()
        for name, action in (
            ('sessions', self.sessions),
            ('files', lambda: self.file_walker.turn(self)),
            ('spool', self.spool),
        ):
            try:
                room(self.home)
                action()
            except Exception as e:
                # Log type and lane only; source errors can contain credentials or private payloads.
                print(f'{name}: {type(e).__name__}', file=sys.stderr, flush=True)
        self.attachment_outcomes()
        self.upload()
        self.commit()
        backlog = self.state.db.execute(
            'SELECT 1 FROM packs WHERE sealed=1 AND uploaded=0 LIMIT 1'
        ).fetchone()
        if backlog or time.monotonic() - self.last_maintenance >= MAINTENANCE_INTERVAL:
            try:
                self.packs.maintenance(max_uploads=0, sync=False)
                self.cleanup_spool()
            except Exception as e:
                print(f'packs: {type(e).__name__}', file=sys.stderr, flush=True)
            self.last_maintenance = time.monotonic()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--home', type=Path, default=DEFAULT_HOME)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--no-b2', action='store_true')
    args = parser.parse_args()
    try:
        daemon = Ingestd(args.home, not args.no_b2)
        daemon.publish_on_commit = not args.once
        while True:
            daemon.turn()
            if args.once:
                daemon.publish_status()
                return 0
            time.sleep(1)
    except Exception as e:
        print(f'ingestd unavailable: {type(e).__name__}', file=sys.stderr)
        return 4


if __name__ == '__main__':
    sys.exit(main())
