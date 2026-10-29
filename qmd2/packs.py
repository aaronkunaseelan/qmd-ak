"""K4 record packs; stable uncompressed locators and seekable zstd frames."""

import hashlib
from contextlib import ExitStack
import io
import json
import os
import shutil
import struct
import tempfile
import time
from pathlib import Path

import zstandard as zstd

from .doc import dumps

TARGET = 128 * 1024
FRAME_MAX = 256 * 1024 * 1024
LARGE = 8 * 1024 * 1024
SEAL = 64 * 1024 * 1024
SEAL_AGE = 24 * 3600
MAINTENANCE_INTERVAL = 300
FLOOR = 8 * 1024**3


def room(path, size=0):
    if shutil.disk_usage(path).free - size < FLOOR:
        raise OSError('disk below 8 GB write floor')


def payload_integrity(body, expected=None):
    if isinstance(body, bytes):
        size, sha = len(body), hashlib.sha256(body).hexdigest()
    else:
        start = body.tell()
        digest, size = hashlib.sha256(), 0
        while chunk := body.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
        sha = digest.hexdigest()
        body.seek(start)
    if expected is not None and (size != expected['bytes'] or sha != expected['sha256']):
        raise ValueError('spool payload size or SHA256 mismatch')
    return size, sha


def records(f):
    while True:
        off = f.tell()
        line = f.readline()
        if not line:
            return
        try:
            head = json.loads(line)
            n = head['len']
            if not isinstance(n, int) or n < 0:
                raise ValueError()
            body = f.read(n)
            if len(body) != n or f.read(1) != b'\n':
                raise ValueError()
            if hashlib.sha256(body).hexdigest() != head['sha256']:
                raise ValueError()
        except (ValueError, KeyError, TypeError) as exc:
            raise ValueError(f'invalid pack record at {off}') from exc
        yield off, f.tell() - off, head, body


class Packs:
    def __init__(self, state, b2=None, writable=False):
        self.state, self.home, self.b2 = state, state.home / 'packs', b2
        self.active, self.files = {}, {}
        self.seal_errors = {}
        self.remote = {}
        if writable:
            self.home.mkdir(parents=True, exist_ok=True)
            if b2:
                self.catalog_remote()
            self.catalog_sealed()
            for path in self.home.glob('open/*/*/*.rec'):
                self.recover_open(path)
            rows = state.packs()
            newest = {(row['stream'], row['source']): row['seq'] for row in rows}
            for row in rows:
                key = row['stream'], row['source']
                if not row['sealed'] and row['seq'] < newest[key]:
                    try:
                        self.seal(*key, row['seq'])
                    except Exception:
                        pass  # seal retains the failure for health; independent packs continue.
            state.commit()

    def catalog_remote(self):
        for obj in self.b2.list():
            bits = obj['Key'].split('/')
            try:
                stream, source, seq = bits[-3], bits[-2], int(bits[-1].split('.')[0])
            except ValueError:
                continue
            if stream not in ('raw', 'doc'):
                continue
            self.remote[stream, source] = max(seq, self.remote.get((stream, source), -1))
            self.state.db.execute(
                'INSERT OR IGNORE INTO packs VALUES (?,?,?,?,?,1,1,NULL)',
                (stream, source, seq, obj['Size'], obj['LastModified'].timestamp()),
            )
            self.state.db.execute(
                'UPDATE packs SET uploaded=1 WHERE stream=? AND source=? AND seq=?',
                (stream, source, seq),
            )

    def catalog_sealed(self):
        for path in self.home.glob('sealed/*/*/*.zst'):
            stream, source, seq = path.parts[-3], path.parts[-2], int(path.stem)
            self.state.db.execute(
                'INSERT OR IGNORE INTO packs VALUES (?,?,?,?,?,1,0,NULL)',
                (stream, source, seq, path.stat().st_size, path.stat().st_mtime),
            )

    def recover_open(self, path):
        stream, source, seq = path.parts[-3], path.parts[-2], int(path.stem)
        catalog = self.state.db.execute(
            'SELECT sealed FROM packs WHERE stream=? AND source=? AND seq=?',
            (stream, source, seq),
        ).fetchone()
        sealed_path = self.path(stream, source, seq, True)
        if (catalog and catalog['sealed']) or sealed_path.exists():
            if not sealed_path.exists() and seq > self.remote.get((stream, source), -1):
                raise OSError('sealed pack unavailable')
            self.state.db.execute(
                'UPDATE packs SET sealed=1 WHERE stream=? AND source=? AND seq=?',
                (stream, source, seq),
            )
            path.unlink()
            return
        with path.open('r+b') as f:
            end = 0
            while line := f.readline():
                if not line.endswith(b'\n'):
                    break
                head = json.loads(line)
                remaining = head['len']
                if not isinstance(remaining, int) or remaining < 0:
                    raise ValueError('invalid pack record length')
                digest = hashlib.sha256()
                while remaining > 0:
                    block = f.read(min(remaining, 1024 * 1024))
                    if not block:
                        break
                    digest.update(block)
                    remaining -= len(block)
                terminator = f.read(1)
                if remaining or not terminator:
                    break
                if terminator != b'\n' or digest.hexdigest() != head['sha256']:
                    raise ValueError('invalid pack record checksum or terminator')
                end = f.tell()
            f.truncate(end)
            f.flush()
            os.fsync(f.fileno())
        if end == 0:
            self.seal(stream, source, seq)
            return
        self.state.db.execute(
            'INSERT OR IGNORE INTO packs VALUES (?,?,?,?,?,0,0,NULL)',
            (stream, source, seq, end, path.stat().st_mtime),
        )
        self.state.db.execute(
            'UPDATE packs SET size=? WHERE stream=? AND source=? AND seq=?',
            (end, stream, source, seq),
        )
        self.active[stream, source] = max(seq, self.active.get((stream, source), -1))

    def path(self, stream, source, seq, sealed=False):
        return (
            self.home
            / ('sealed' if sealed else 'open')
            / stream
            / source
            / f'{seq:010d}.{"zst" if sealed else "rec"}'
        )

    def allocate(self, stream, source):
        row = self.state.db.execute(
            'SELECT MAX(seq) FROM packs WHERE stream=? AND source=?', (stream, source)
        ).fetchone()
        local = [
            int(p.stem)
            for sub in ('open', 'sealed')
            for p in (self.home / sub / stream / source).glob('*')
            if p.suffix in ('.rec', '.zst')
        ]
        seq = (
            max(
                [row[0] if row[0] is not None else -1, self.remote.get((stream, source), -1)]
                + local
            )
            + 1
        )
        path = self.path(stream, source, seq)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=False)
        self.state.db.execute(
            'INSERT INTO packs VALUES (?,?,?,?,?,0,0,NULL)', (stream, source, seq, 0, time.time())
        )
        self.active[stream, source] = seq
        return seq

    def append(self, stream, source, header, body, expected=None):
        size, sha = payload_integrity(body, expected)
        if isinstance(body, bytes):
            body = io.BytesIO(body)
        room(self.home, size * 2 + 4096)
        key = stream, source
        if size >= LARGE:
            if key in self.active:
                self.seal(stream, source, self.active[key])
            seq = self.allocate(stream, source)
        else:
            seq = self.active.get(key)
            if seq is None:
                seq = self.allocate(stream, source)
        path = self.path(stream, source, seq)
        head = dict(header, source=source, stream=stream, len=size, sha256=sha)
        head.setdefault('arrived', time.time_ns() // 1000 / 1_000_000)
        f = self.files.get(key)
        if f is None or Path(f.name) != path:
            if f:
                f.close()
            f = self.files[key] = path.open('ab')
        off = f.tell()
        f.write(dumps(head).encode() + b'\n')
        shutil.copyfileobj(body, f, 1024 * 1024)
        f.write(b'\n')
        f.flush()
        n = f.tell() - off
        self.state.db.execute(
            'UPDATE packs SET size=? WHERE stream=? AND source=? AND seq=?',
            (f.tell(), stream, source, seq),
        )
        loc = dict(stream=stream, source=source, seq=seq, rec_off=off, rec_len=n)
        if size >= LARGE or f.tell() >= SEAL:
            self.seal(stream, source, seq)
        return loc

    def sync(self):
        for f in self.files.values():
            f.flush()
            os.fsync(f.fileno())

    def seal(self, stream, source, seq):
        self.seal_errors.pop((stream, source, seq), None)
        try:
            key = stream, source
            if key in self.files:
                f = self.files.pop(key)
                f.flush()
                os.fsync(f.fileno())
                f.close()
            src, dst = self.path(stream, source, seq), self.path(stream, source, seq, True)
            if not src.exists():
                return
            if src.stat().st_size == 0:
                src.unlink()
                self.state.db.execute(
                    'DELETE FROM packs WHERE stream=? AND source=? AND seq=?', (stream, source, seq)
                )
                if self.active.get(key) == seq:
                    self.active.pop(key)
                return
            room(self.home, src.stat().st_size + 4096)
            dst.parent.mkdir(parents=True, exist_ok=True)
            with src.open('rb') as f, dst.with_suffix('.tmp').open('wb') as out:
                frames = self.write_pack_frames(src, f, out)
                table = b''.join(struct.pack('<II', f[3], f[1]) for f in frames)
                table += struct.pack('<IBI', len(frames), 0, 0x8F92EAB1)
                out.write(struct.pack('<II', 0x184D2A5E, len(table)) + table)
                out.flush()
                os.fsync(out.fileno())
            os.replace(dst.with_suffix('.tmp'), dst)
            self.state.db.execute(
                'UPDATE packs SET sealed=1,frames=? WHERE stream=? AND source=? AND seq=?',
                (dumps(frames), stream, source, seq),
            )
            self.state.commit()
            src.unlink()
            if self.active.get(key) == seq:
                self.active.pop(key)
        except Exception as error:
            self.seal_errors[stream, source, seq] = type(error).__name__
            raise

    def write_pack_frames(self, src, f, out):
        frames, buffer = [], bytearray()
        uoff = 0
        compressor = zstd.ZstdCompressor(level=3, write_checksum=True)

        def flush():
            nonlocal buffer, uoff
            if not buffer:
                return
            compressed = compressor.compress(buffer)
            frames.append([uoff, len(buffer), out.tell(), len(compressed)])
            out.write(compressed)
            uoff += len(buffer)
            buffer = bytearray()

        first = json.loads(f.readline())
        f.seek(0)
        if first['len'] >= LARGE:
            loc = dict(
                stream=first['stream'],
                source=first['source'],
                seq=int(src.stem),
                rec_off=0,
                rec_len=src.stat().st_size,
            )
            with open(os.devnull, 'wb') as sink:
                self.write_raw(loc, sink)
            remaining = src.stat().st_size
            while remaining:
                size = min(remaining, FRAME_MAX)
                coff = out.tell()
                with compressor.stream_writer(out, size=size, closefd=False) as writer:
                    left = size
                    while left:
                        chunk = f.read(min(left, 1024 * 1024))
                        if not chunk:
                            raise OSError('short open pack')
                        writer.write(chunk)
                        left -= len(chunk)
                frames.append([uoff, size, coff, out.tell() - coff])
                uoff += size
                remaining -= size
        else:
            for _, _, head, body in records(f):
                record = dumps(head).encode() + b'\n' + body + b'\n'
                if buffer and len(buffer) + len(record) > TARGET:
                    flush()
                buffer.extend(record)
                if len(buffer) >= TARGET:
                    flush()
            flush()
        return frames

    def frame_map(self, loc, path=None):
        stream, source, seq = loc['stream'], loc['source'], loc['seq']
        row = self.state.db.execute(
            'SELECT frames FROM packs WHERE stream=? AND source=? AND seq=?', (stream, source, seq)
        ).fetchone()
        if row and row[0]:
            return json.loads(row[0])
        path = Path(path) if path is not None else self.path(stream, source, seq, True)
        key = self.b2.key(stream, source, seq) if self.b2 else None
        if path.exists():
            with path.open('rb') as f:
                f.seek(-9, 2)
                footer = f.read(9)
                count, descriptor, magic = struct.unpack('<IBI', footer)
                entry_size = 12 if descriptor & 128 else 8
                f.seek(-(9 + count * entry_size), 2)
                table = f.read(count * entry_size)
        elif self.b2:
            obj = self.b2.s3.head_object(Bucket=self.b2.bucket, Key=key)
            size = obj['ContentLength']
            count, descriptor, magic = struct.unpack('<IBI', self.b2.read(key, size - 9, 9))
            entry_size = 12 if descriptor & 128 else 8
            table = self.b2.read(key, size - 9 - count * entry_size, count * entry_size)
        else:
            raise FileNotFoundError('pack unavailable')
        if magic != 0x8F92EAB1:
            raise OSError('invalid seek table')
        frames, uoff, coff = [], 0, 0
        for i in range(count):
            clen, ulen = struct.unpack_from('<II', table, i * entry_size)
            frames.append([uoff, ulen, coff, clen])
            uoff += ulen
            coff += clen
        return frames

    def read(self, loc, cold=False):
        target = io.BytesIO()
        head = self.write_raw(loc, target, cold)
        return head, target.getvalue()

    def write_raw(self, loc, target, cold=False):
        args = loc['stream'], loc['source'], loc['seq']
        sealed, opened = self.path(*args, True), self.path(*args)
        with ExitStack() as stack:
            if not cold and not sealed.exists() and opened.exists():
                reader = stack.enter_context(opened.open('rb'))
                reader.seek(loc['rec_off'])
            else:
                frames = [
                    f
                    for f in self.frame_map(loc)
                    if f[0] < loc['rec_off'] + loc['rec_len'] and f[0] + f[1] > loc['rec_off']
                ]
                frame = frames[0]
                compressed_len = frames[-1][2] + frames[-1][3] - frame[2]
                if sealed.exists() and not cold:
                    compressed = stack.enter_context(sealed.open('rb'))
                    compressed.seek(frame[2])
                elif self.b2:
                    compressed = stack.enter_context(
                        self.b2.open_range(self.b2.key(*args), frame[2], compressed_len)
                    )
                else:
                    raise FileNotFoundError('pack unavailable')
                reader = stack.enter_context(
                    io.BufferedReader(
                        zstd.ZstdDecompressor().stream_reader(
                            compressed, closefd=False, read_across_frames=True
                        )
                    )
                )
                skip = loc['rec_off'] - frame[0]
                while skip:
                    chunk = reader.read(min(skip, 1024 * 1024))
                    if not chunk:
                        raise OSError('short raw pack')
                    skip -= len(chunk)
            line = reader.readline(1024 * 1024)
            head = json.loads(line)
            if head['len'] < 0 or len(line) + head['len'] + 1 != loc['rec_len']:
                raise OSError('invalid raw locator')
            digest, remaining = hashlib.sha256(), head['len']
            while remaining:
                chunk = reader.read(min(remaining, 1024 * 1024))
                if not chunk:
                    raise OSError('short raw record')
                digest.update(chunk)
                target.write(chunk)
                remaining -= len(chunk)
            if reader.read(1) != b'\n' or digest.hexdigest() != head['sha256']:
                raise OSError('raw checksum mismatch')
            return head

    def maintenance(self, cache_bytes=8 * 1024**3, max_uploads=2, sync=True):
        if sync:
            self.sync()
        for row in list(self.state.packs()):
            if not row['sealed'] and (
                row['size'] >= SEAL or time.time() - row['created'] >= SEAL_AGE
            ):
                key = row['stream'], row['source'], row['seq']
                try:
                    self.seal(*key)
                except Exception:
                    pass  # seal retains the failure for health; independent packs continue.
        uploaded = 0
        for row in list(self.state.packs()):
            if not row['sealed'] or row['uploaded'] or not self.b2 or uploaded >= max_uploads:
                continue
            path = self.path(row['stream'], row['source'], row['seq'], True)
            self.b2.put(self.b2.key(row['stream'], row['source'], row['seq']), path)
            self.state.db.execute(
                'UPDATE packs SET uploaded=1 WHERE stream=? AND source=? AND seq=?',
                (row['stream'], row['source'], row['seq']),
            )
            self.state.commit()
            uploaded += 1
        rows = list(self.state.packs())
        docs = sorted((r for r in rows if r['stream'] == 'doc'), key=lambda r: r['created'])
        total = sum(
            self.path(r['stream'], r['source'], r['seq'], bool(r['sealed'])).stat().st_size
            for r in docs
            if self.path(r['stream'], r['source'], r['seq'], bool(r['sealed'])).exists()
        )
        for r in [r for r in rows if r['stream'] == 'raw'] + docs:
            path = self.path(r['stream'], r['source'], r['seq'], True)
            if r['uploaded'] and path.exists() and (r['stream'] == 'raw' or total > cache_bytes):
                if r['stream'] == 'doc':
                    total -= path.stat().st_size
                path.unlink()

    def iter_pack(self, row, start=0):
        args = row['stream'], row['source'], row['seq']
        opened = self.path(*args)
        if not row['sealed'] and opened.exists():
            with opened.open('rb') as f:
                f.seek(start)
                for off, n, head, body in records(f):
                    yield (
                        dict(stream=args[0], source=args[1], seq=args[2], rec_off=off, rec_len=n),
                        head,
                        body,
                    )
            return
        sealed = self.path(*args, True)
        if sealed.exists():
            yield from self.iter_sealed(row, sealed, start)
        elif self.b2:
            frames = json.loads(row['frames']) if row['frames'] else None
            # Sealed size = frame bytes + 8-byte seek header + 8 bytes per frame + 9-byte footer.
            size = (
                sum(frame[3] for frame in frames) + 17 + 8 * len(frames) if frames else row['size']
            )
            room(self.home, size)
            with tempfile.TemporaryDirectory(prefix='read-pack-', dir=self.home) as temporary:
                local = Path(temporary) / 'pack.zst'
                self.b2.download(self.b2.bucket, self.b2.key(*args), local)
                yield from self.iter_sealed(row, local, start)
        else:
            raise FileNotFoundError('pack unavailable')

    def iter_range(self, row, start=0):
        """Read from a record boundary, using only the remaining compressed range on B2."""
        args = row['stream'], row['source'], row['seq']
        opened, sealed = self.path(*args), self.path(*args, True)
        with ExitStack() as stack:
            if not row['sealed'] and opened.exists() and not sealed.exists():
                reader = stack.enter_context(opened.open('rb'))
                reader.seek(start)
            else:
                loc = dict(stream=args[0], source=args[1], seq=args[2])
                frames = self.frame_map(loc)
                frames = [f for f in frames if f[0] + f[1] > start]
                if not frames:
                    return
                first, last = frames[0], frames[-1]
                if sealed.exists():
                    compressed = stack.enter_context(sealed.open('rb'))
                    compressed.seek(first[2])
                elif self.b2:
                    compressed = stack.enter_context(
                        self.b2.open_range(
                            self.b2.key(*args), first[2], last[2] + last[3] - first[2]
                        )
                    )
                else:
                    raise FileNotFoundError('pack unavailable')
                reader = stack.enter_context(
                    io.BufferedReader(
                        zstd.ZstdDecompressor().stream_reader(
                            compressed, closefd=False, read_across_frames=True
                        )
                    )
                )
                remaining = start - first[0]
                while remaining:
                    block = reader.read(min(remaining, 1024 * 1024))
                    if not block:
                        raise OSError('short pack')
                    remaining -= len(block)
            base = start - reader.tell()
            for off, n, head, body in records(reader):
                yield (
                    dict(
                        stream=args[0], source=args[1], seq=args[2], rec_off=base + off, rec_len=n
                    ),
                    head,
                    body,
                )

    def iter_sealed(self, row, sealed, start):
        args = row['stream'], row['source'], row['seq']
        frames = self.frame_map(dict(stream=args[0], source=args[1], seq=args[2]), sealed)
        if not frames:
            return
        with sealed.open('rb') as stream:
            with io.BufferedReader(
                zstd.ZstdDecompressor().stream_reader(
                    stream, closefd=False, read_across_frames=True
                )
            ) as reader:
                line = reader.readline(1024 * 1024)
                head = json.loads(line)
                if head['len'] >= LARGE:
                    if start:
                        return
                    n = len(line) + head['len'] + 1
                    body = (
                        None
                        if head.get('kind') in ('archive-only', 'reset-history')
                        else bytearray()
                    )
                    digest, remaining = hashlib.sha256(), head['len']
                    while remaining:
                        chunk = reader.read(min(remaining, 1024 * 1024))
                        if not chunk:
                            raise OSError('short pack record')
                        digest.update(chunk)
                        if body is not None:
                            body.extend(chunk)
                        remaining -= len(chunk)
                    if (
                        reader.read(1) != b'\n'
                        or digest.hexdigest() != head['sha256']
                        or reader.read(1)
                    ):
                        raise OSError('invalid pack record checksum or terminator')
                    if body is not None:
                        body = bytes(body)
                    yield (
                        dict(stream=args[0], source=args[1], seq=args[2], rec_off=0, rec_len=n),
                        head,
                        body,
                    )
                    return
        for uoff, ulen, coff, clen in frames:
            if uoff + ulen <= start:
                continue
            with sealed.open('rb') as f:
                f.seek(coff)
                compressed = f.read(clen)
            data = zstd.ZstdDecompressor().decompress(compressed)
            for off, n, head, body in records(io.BytesIO(data)):
                if uoff + off < start:
                    continue
                yield (
                    dict(
                        stream=args[0], source=args[1], seq=args[2], rec_off=uoff + off, rec_len=n
                    ),
                    head,
                    body,
                )
