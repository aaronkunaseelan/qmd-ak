import hashlib
import io
import os
import time
import uuid

import pytest

from qmd2.b2 import B2
from qmd2.packs import Packs, SEAL_AGE
from qmd2.state import State


def synthetic_b2(monkeypatch, client):
    import boto3

    monkeypatch.setattr(boto3, 'client', lambda *args, **kwargs: client)
    for key in ('QMD_B2_REGION', 'QMD_B2_KEY_ID', 'QMD_B2_APPLICATION_KEY', 'QMD_B2_PREFIX'):
        monkeypatch.setenv(key, 'synthetic')
    return B2(bucket='synthetic')


def test_pack_filesystem_and_b2(tmp_path):
    remote = (
        B2(prefix='packs/v1-test/' + uuid.uuid4().hex) if os.environ.get('QMD2_TEST_B2') else None
    )
    if remote:
        config = remote.s3.meta.config
        assert config.retries == {'mode': 'standard', 'total_max_attempts': 3}
        assert (config.connect_timeout, config.read_timeout, config.max_pool_connections) == (
            5,
            20,
            16,
        )
    state = State(tmp_path, True)
    packs = Packs(state, remote, True)
    blobs = [b'first record', b'oversize ' * 30000, b'last record']
    locs = [packs.append('doc', 'pi', {'id': str(n)}, body) for n, body in enumerate(blobs)]
    packs.sync()
    for loc, body in zip(locs, blobs):
        assert hashlib.sha256(packs.read(loc)[1]).digest() == hashlib.sha256(body).digest()
    with packs.path('doc', 'pi', locs[0]['seq']).open('ab') as f:
        f.write(b'{"len":999}\npartial')
    packs.files.pop(('doc', 'pi')).close()
    packs = Packs(state, remote, True)
    original = packs.path('doc', 'pi', locs[0]['seq']).read_bytes()
    packs.seal('doc', 'pi', locs[0]['seq'])
    packs.path('doc', 'pi', locs[0]['seq']).write_bytes(original)
    packs = Packs(state, remote, True)
    assert not packs.path('doc', 'pi', locs[0]['seq']).exists()
    next_loc = packs.append('doc', 'pi', {'id': 'next'}, b'new pack')
    assert next_loc['seq'] > locs[0]['seq']
    young_loc = packs.append('doc', 'synthetic', {'id': 'young'}, b'young pack')
    now = time.time()
    for loc, age in ((next_loc, SEAL_AGE + 60), (young_loc, SEAL_AGE - 60)):
        state.db.execute(
            'UPDATE packs SET created=? WHERE stream=? AND source=? AND seq=?',
            (now - age, loc['stream'], loc['source'], loc['seq']),
        )
    state.commit()
    packs.maintenance(max_uploads=0)
    rows = {(r['source'], r['seq']): r for r in state.packs('doc')}
    assert rows[('pi', next_loc['seq'])]['sealed'] == 1
    assert rows[('synthetic', young_loc['seq'])]['sealed'] == 0
    assert packs.path('doc', 'pi', next_loc['seq'], True).exists()
    assert packs.path('doc', 'synthetic', young_loc['seq']).exists()
    for loc, body in zip(locs, blobs):
        assert packs.read(loc)[1] == body
    assert len(packs.frame_map(locs[0])) == 3
    raw_file = tmp_path / 'large.bin'
    with raw_file.open('wb') as f:
        f.truncate(9 * 1024 * 1024)
    with raw_file.open('rb') as f:
        raw_loc = packs.append('raw', 'files', {'id': 'file:synthetic:large.bin'}, f)
    copied = tmp_path / 'copied.bin'
    with copied.open('wb') as target:
        head = packs.write_raw(raw_loc, target)
    with copied.open('rb') as f:
        assert hashlib.file_digest(f, 'sha256').hexdigest() == head['sha256']
    assert copied.stat().st_size == raw_file.stat().st_size
    lower = packs.append('doc', 'orphan', {'id': 'lower'}, b'earlier')
    packs.allocate('doc', 'orphan')
    higher = packs.append('doc', 'orphan', {'id': 'higher'}, b'later')
    packs.sync()
    packs.files.pop(('doc', 'orphan')).close()
    packs = Packs(state, remote, True)
    assert not packs.path('doc', 'orphan', lower['seq']).exists()
    resumed = packs.append('doc', 'orphan', {'id': 'resumed'}, b'latest')
    assert resumed['seq'] == higher['seq'] and resumed['rec_off'] > higher['rec_off']
    packs.allocate('doc', 'orphan')
    newest = packs.append('doc', 'orphan', {'id': 'newest'}, b'last')
    packs.seal('doc', 'orphan', newest['seq'])
    packs = Packs(state, remote, True)
    assert not packs.path('doc', 'orphan', resumed['seq']).exists()
    assert packs.append('doc', 'orphan', {'id': 'tail'}, b'tail')['seq'] > newest['seq']
    if remote:
        key = remote.key('doc', 'pi', locs[0]['seq'])
        try:
            remote.put(key, packs.path('doc', 'pi', locs[0]['seq'], True))
            for loc, body in zip(locs, blobs):
                assert packs.read(loc)[1] == body
            packs.path('doc', 'pi', locs[0]['seq'], True).unlink()
            for loc, body in zip(locs, blobs):
                assert packs.read(loc)[1] == body
        finally:
            remote.s3.delete_object(Bucket=remote.bucket, Key=key)
        key = remote.key('raw', 'files', raw_loc['seq'])
        try:
            remote.put(key, packs.path('raw', 'files', raw_loc['seq'], True))
            with copied.open('wb') as target:
                head = packs.write_raw(raw_loc, target, cold=True)
            with copied.open('rb') as f:
                assert hashlib.file_digest(f, 'sha256').hexdigest() == head['sha256']
        finally:
            remote.s3.delete_object(Bucket=remote.bucket, Key=key)


@pytest.mark.parametrize('shape', ['ordinary', 'large'])
@pytest.mark.parametrize('damage', ['checksum', 'header', 'body', 'terminator'])
def test_corrupt_record_reads_and_sealing_keep_original(tmp_path, monkeypatch, damage, shape):
    state = State(tmp_path, True)
    packs = Packs(state, writable=True)
    if shape == 'ordinary':
        packs.append('raw', 'synthetic', {'id': 'first'}, b'first original')
    loc = packs.append('raw', 'synthetic', {'id': 'second'}, b'second original')
    if shape == 'large':
        monkeypatch.setattr('qmd2.packs.LARGE', 10)
    packs.sync()
    state.commit()
    opened = packs.path('raw', 'synthetic', loc['seq'])
    original = opened.read_bytes()
    start = loc['rec_off']
    body = original.index(b'\n', start) + 1
    damaged = {
        'checksum': original[:body] + b'X' + original[body + 1 :],
        'header': original[:start] + b'not json\n',
        'body': original[:-4],
        'terminator': original[:-1] + b'X',
    }[damage]
    opened.write_bytes(damaged)
    row = state.packs('raw')[0]
    for read in (packs.iter_pack, packs.iter_range):
        with pytest.raises((OSError, ValueError)):
            list(read(row))
    with pytest.raises((OSError, ValueError)):
        packs.seal('raw', 'synthetic', loc['seq'])
    assert opened.read_bytes() == damaged
    assert not packs.path('raw', 'synthetic', loc['seq'], True).exists()
    assert not state.packs('raw')[0]['sealed']
    if damage in ('checksum', 'header', 'terminator'):
        with pytest.raises((OSError, ValueError)):
            packs.recover_open(opened)
        assert opened.read_bytes() == damaged
    else:
        packs.recover_open(opened)
        assert opened.read_bytes() == original[:start]
    state.db.close()


@pytest.mark.parametrize('kind', ['raw', 'archive-only'])
def test_sealed_large_integrity_streaming(tmp_path, monkeypatch, kind):
    import zstandard as zstd

    state = State(tmp_path, True)
    packs = Packs(state, writable=True)
    loc = packs.append('raw', 'synthetic', {'id': 'large', 'kind': kind}, b'original ' * 20)
    packs.sync()
    original = packs.path('raw', 'synthetic', loc['seq']).read_bytes()
    monkeypatch.setattr('qmd2.packs.LARGE', 10)
    packs.seal('raw', 'synthetic', loc['seq'])
    row = state.packs('raw')[0]
    assert list(packs.iter_pack(row))[0][2] == (
        None if kind == 'archive-only' else b'original ' * 20
    )
    body = original.index(b'\n') + 1
    damaged = original[:body] + b'X' + original[body + 1 :]
    packs.path('raw', 'synthetic', loc['seq'], True).write_bytes(
        zstd.ZstdCompressor().compress(damaged)
    )
    with pytest.raises((OSError, ValueError)):
        list(packs.iter_pack(row))
    state.db.close()


def test_doc_cache_eviction_filesystem(tmp_path):
    state = State(tmp_path, True)
    packs = Packs(state, writable=True)
    paths = {}
    for source, created, uploaded in (('a-new', 20, 1), ('z-old', 10, 1), ('unuploaded', 1, 0)):
        loc = packs.append('doc', source, {'id': source}, b'synthetic cache content')
        packs.seal('doc', source, loc['seq'])
        paths[source] = packs.path('doc', source, loc['seq'], True)
        state.db.execute(
            'UPDATE packs SET created=?,uploaded=? WHERE source=?', (created, uploaded, source)
        )
    raw = packs.append('raw', 'synthetic', {'id': 'raw'}, b'synthetic raw')
    packs.seal('raw', 'synthetic', raw['seq'])
    raw_path = packs.path('raw', 'synthetic', raw['seq'], True)
    state.db.execute("UPDATE packs SET uploaded=1 WHERE stream='raw'")
    state.commit()
    budget = paths['a-new'].stat().st_size + paths['unuploaded'].stat().st_size
    packs.maintenance(cache_bytes=budget, max_uploads=0)
    assert paths['a-new'].exists()
    assert not paths['z-old'].exists()
    assert paths['unuploaded'].exists()
    assert not raw_path.exists()
    packs.maintenance(cache_bytes=0, max_uploads=0)
    assert not paths['a-new'].exists()
    assert paths['unuploaded'].exists()


def test_cold_whole_pack_temp_fetch_and_single_doc_ranges(tmp_path, monkeypatch):
    monkeypatch.setattr("qmd2.packs.FRAME_MAX", 1024 * 1024)
    import shutil

    state = State(tmp_path, True)
    writer = Packs(state, writable=True)
    blobs = [os.urandom(140000) for _ in range(6)]
    locs = [writer.append('doc', 'synthetic', {'id': str(n)}, body) for n, body in enumerate(blobs)]
    writer.seal('doc', 'synthetic', locs[0]['seq'])
    row = state.packs('doc')[0]
    local = writer.path('doc', 'synthetic', row['seq'], True)
    remote_object = tmp_path / 'remote.zst'
    shutil.copyfile(local, remote_object)
    local.unlink()
    original_frames = row['frames']
    state.db.execute('UPDATE packs SET frames=NULL,size=?', (remote_object.stat().st_size,))
    state.commit()
    row = state.packs('doc')[0]

    class S3:
        fail = False

        def __init__(self):
            self.downloads = []
            self.ranges = []

        def get_object(self, *, Bucket, Key, Range=None):
            assert Bucket == 'synthetic'
            if Range is None:
                self.downloads.append(Key)
                if self.fail:
                    raise ConnectionError('synthetic download failure')
                return {'Body': remote_object.open('rb')}
            offset, end = map(int, Range.removeprefix('bytes=').split('-'))
            length = end - offset + 1
            self.ranges.append((Key, offset, length))
            with remote_object.open('rb') as f:
                f.seek(offset)
                return {'Body': io.BytesIO(f.read(length))}

    remote = S3()
    cold = Packs(state, synthetic_b2(monkeypatch, remote))
    result = list(cold.iter_pack(row))
    assert [loc for loc, _, _ in result] == locs
    assert [body for _, _, body in result] == blobs
    assert len(remote.downloads) == 1 and not remote.ranges
    assert not local.exists() and not list(writer.home.glob('read-pack-*'))
    partial = cold.iter_pack(row, start=locs[2]['rec_off'])
    assert next(partial)[0] == locs[2]
    assert list(writer.home.glob('read-pack-*'))
    partial.close()
    assert not list(writer.home.glob('read-pack-*'))
    remote.fail = True
    with pytest.raises(ConnectionError):
        list(cold.iter_pack(row))
    assert not list(writer.home.glob('read-pack-*'))
    remote.fail = False
    downloads = len(remote.downloads)
    with pytest.raises(OSError):
        list(cold.iter_pack(dict(row, size=shutil.disk_usage(tmp_path).free + 1)))
    assert len(remote.downloads) == downloads
    state.db.execute('UPDATE packs SET frames=?', (original_frames,))
    state.commit()
    assert [body for _, _, body in cold.iter_pack(state.packs('doc')[0])] == blobs
    downloads += 1
    assert len(remote.downloads) == downloads and not remote.ranges
    assert cold.read(locs[0])[1] == blobs[0]
    assert len(remote.downloads) == downloads and len(remote.ranges) == 1
    ranged = list(cold.iter_range(state.packs('doc')[0], locs[2]['rec_off']))
    assert [pointer for pointer, _, _ in ranged] == locs[2:]
    assert [body for _, _, body in ranged] == blobs[2:]
    assert len(remote.downloads) == downloads and len(remote.ranges) == 2
    assert remote.ranges[-1][1] > 0

    body = os.urandom(9 * 1024 * 1024)
    loc = writer.append('raw', 'synthetic', {'id': 'archive', 'kind': 'archive-only'}, body)
    shutil.copyfile(writer.path('raw', 'synthetic', loc['seq'], True), remote_object)
    assert len(writer.frame_map(loc)) == 10
    assert list(writer.iter_pack(state.packs('raw')[0]))[0][2] is None
    assert cold.read(loc, cold=True)[1] == body
    assert len(remote.ranges) == 3
    writer.path('raw', 'synthetic', loc['seq'], True).unlink()
    ranged = list(cold.iter_range(state.packs('raw')[0]))
    assert ranged[0][0] == loc and ranged[0][2] == body
    assert len(remote.downloads) == downloads and len(remote.ranges) == 4


def test_b2_upload_checksums_and_abort(tmp_path, monkeypatch):
    import base64

    class S3:
        fail = None

        def __init__(self):
            self.parts = []
            self.completed = False
            self.aborted = False
            self.single = False

        def put_object(self, **args):
            self.single = True
            body = args['Body'].read()
            assert args['ContentMD5'] == base64.b64encode(hashlib.md5(body).digest()).decode()
            return {'ETag': '"' + hashlib.md5(body).hexdigest() + '"'}

        def create_multipart_upload(self, **args):
            return {'UploadId': 'synthetic'}

        def upload_part(self, **args):
            body = args['Body']
            assert len(body) <= 8
            assert args['ContentMD5'] == base64.b64encode(hashlib.md5(body).digest()).decode()
            if self.fail == 'network':
                raise ConnectionError('synthetic')
            tag = 'bad' if self.fail == 'checksum' else hashlib.md5(body).hexdigest()
            self.parts.append(body)
            return {'ETag': '"' + tag + '"'}

        def complete_multipart_upload(self, **args):
            assert args['MultipartUpload']['Parts'] == [
                {'PartNumber': i + 1, 'ETag': '"' + hashlib.md5(body).hexdigest() + '"'}
                for i, body in enumerate(self.parts)
            ]
            if self.fail == 'complete':
                raise ConnectionError('synthetic')
            self.completed = True

        def abort_multipart_upload(self, **args):
            self.aborted = True

    monkeypatch.setattr('qmd2.b2.SINGLE_MAX', 16)
    monkeypatch.setattr('qmd2.b2.PART_SIZE', 8)
    remote = synthetic_b2(monkeypatch, S3())
    path = tmp_path / 'upload'
    for size in (16, 19):
        path.write_bytes(bytes(range(size)))
        remote.s3 = S3()
        remote.put('key', path)
        assert remote.s3.single == (size == 16)
        assert remote.s3.completed == (size == 19)
        if size == 19:
            assert b''.join(remote.s3.parts) == path.read_bytes()
    for failure in ('network', 'checksum', 'complete'):
        remote.s3 = S3()
        remote.s3.fail = failure
        with pytest.raises((ConnectionError, RuntimeError)):
            remote.put('key', path)
        assert remote.s3.aborted and not remote.s3.completed
