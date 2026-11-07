"""An attachment id gives back its original bytes, on fixtures only."""

import hashlib
import json
import os
import subprocess
import sys
import urllib.error

import pytest

from qmd2.doc import dumps

PAYLOAD = b'%PDF-1.4 synthetic invoice bytes\n'
PARENT = 'wa:synthetic:9@g.us:message'
WHATSAPP = {
    'id': 'message',
    'chat': '9@g.us',
    'sender': '8@s.whatsapp.net',
    'timestamp': '2026-10-13T00:00:00Z',
    'kind': 'message',
    'text': 'Synthetic harbor',
    'media': [dict(type='document', fileName='invoice.pdf', fileLength=str(len(PAYLOAD)))],
}


def entry(source, key, body, archive=True, captured_at='2026-10-13T00:00:00Z'):
    return {
        'schema': 'qmd.spool-entry.v1',
        'source': source,
        'account': 'synthetic',
        'key': key,
        'sha256': hashlib.sha256(body).hexdigest(),
        'bytes': len(body),
        'path': '',
        'captured_at': captured_at,
        'state': {'labels': {'archive_only': 'true'} if archive else {}},
    }


def spool(home, lane, rows):
    root = home / 'spool' / lane
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'index.jsonl').open('a') as index:
        for name, row, body in rows:
            (root / name).write_bytes(body)
            index.write(json.dumps(dict(row, path=name)) + '\n')


def ingest(home):
    from qmd2.ingestd import Ingestd

    daemon = Ingestd(home, remote=False)
    daemon.spool()
    daemon.commit()
    daemon.state.db.close()


def archived(tmp_path):
    home = tmp_path / 'qmd'
    home.mkdir()
    other = b'synthetic harbour attachment\n'
    spool(
        home,
        'attachment',
        [
            ('payload', entry('attachment', 'synthetic:parent/att/0', PAYLOAD), PAYLOAD),
            ('payload2', entry('attachment', 'synthetic:parent/att/1', other), other),
        ],
    )
    ingest(home)
    return home


def cli(home, *args):
    env = dict(os.environ, HOME=str(home), TMPDIR=str(home), PYTHONDONTWRITEBYTECODE='1')
    return subprocess.run(
        [sys.executable, '-m', 'qmd2.cli', '--home', str(home), *args],
        env=env,
        capture_output=True,
    )


def test_attachment_raw_filesystem_chain(tmp_path):
    home = archived(tmp_path)
    out = tmp_path / 'out'
    result = cli(home, 'get', 'synthetic:parent/att/0', '--raw', '--out', str(out))
    assert (result.returncode, out.read_bytes()) == (0, PAYLOAD)
    assert cli(home, 'get', 'synthetic:parent/att/0', '--raw').stdout == PAYLOAD
    # An output id keeps serving its own document, never the original bytes.
    assert cli(home, 'get', 'synthetic:parent/att/0/ocr', '--raw').returncode == 3


def lane_walks(monkeypatch):
    """Every lane pack read, so a test can assert that a path does not walk them."""
    from qmd2 import packs

    walked = []
    original = packs.Packs.iter_pack

    def spy(self, row, start=0):
        walked.append((row['source'], row['seq']))
        return original(self, row, start)

    monkeypatch.setattr(packs.Packs, 'iter_pack', spy)
    return walked


def test_attachment_raw_legacy_lane_lookup(tmp_path, monkeypatch):
    from qmd2 import attachments
    from qmd2.state import State

    home = whatsapp_home(tmp_path, archived_attachment=True)
    identifier = PARENT + '/att/0'
    state = State(home, writable=True)
    state.db.execute('DELETE FROM checkpoints WHERE key=?', ('media:' + identifier,))
    state.commit()
    state.db.close()
    walked = lane_walks(monkeypatch)
    # Before the backfill, a listed id with no recorded locator is found by one walk.
    loc = attachments.media_source(home, identifier)
    assert (loc is not None, walked != []) == (True, True)
    # The find is recorded, so the read and the CLI never walk again.
    assert attachments.archived_locator(home, identifier) is not None
    walked.clear()
    result = cli(home, 'get', identifier, '--raw')
    assert (result.returncode, result.stdout, walked) == (0, PAYLOAD, [])


def test_attachment_raw_fetch_only_and_refusals(tmp_path, monkeypatch):
    from qmd2 import attachments

    home = tmp_path / 'qmd'
    home.mkdir()
    raw = dumps({'raw': {'journal': 'synthetic'}, 'decoded': WHATSAPP}).encode()
    spool(home, 'whatsapp', [('payload', entry('whatsapp', '9@g.us:message', raw, False), raw)])
    ingest(home)
    identifier = PARENT + '/att/0'

    def fetch(home, row, target, original=False):
        assert original, 'the archive path copies original bytes, never type-routed ones'
        target.write_bytes(PAYLOAD)
        return target

    monkeypatch.setattr(attachments, 'fetch', fetch)
    fetched = attachments.media_source(home, identifier)
    assert fetched.read_bytes() == PAYLOAD
    fetched.unlink()
    archived_entry = json.loads(
        (home / 'spool/attachment/index.jsonl').read_text().splitlines()[-1]
    )
    assert (archived_entry['key'], archived_entry['state']) == (
        identifier,
        {'labels': {'archive_only': 'true'}},
    )

    def expired(home, row, target, original=False):
        raise urllib.error.HTTPError('/v/synthetic', 410, 'Gone', None, None)

    monkeypatch.setattr(attachments, 'fetch', expired)
    with pytest.raises(urllib.error.HTTPError) as error:
        attachments.media_source(home, identifier)
    assert attachments.temporary_refusal(error.value)[1] == 'media_expired (HTTP 410)'


def test_whatsapp_arrival_archives_every_attachment(tmp_path):
    from qmd2.attachments import ARCHIVE_LIMIT, enqueue_auto, queue_path, queued_row, too_large

    parent = {'id': PARENT, 'source': 'whatsapp', 'group': 'mail', 'time': '2026-11-05T00:00:00Z'}
    media = [
        {'type': 'audio', 'mimetype': 'audio/ogg', 'PTT': True, 'fileLength': '10'},
        {'type': 'image', 'mimetype': 'image/jpeg', 'fileLength': '10'},
        {'type': 'document', 'mimetype': 'application/pdf', 'fileLength': '10'},
        {'type': 'video', 'mimetype': 'video/mp4', 'fileLength': str(ARCHIVE_LIMIT + 1)},
    ]
    enqueue_auto(
        tmp_path, dumps({'decoded': {'media': media}}).encode(), [parent], 1793900000 * 10**9
    )
    rows = [queued_row(queue_path(tmp_path, f'{PARENT}/att/{n}')) for n in range(len(media))]
    assert [bool(row['media_only']) for row in rows] == [False, False, True, True]
    assert too_large(rows[3]['ref']) and not too_large(rows[2]['ref'])


def test_rescue_dry_run_counts_unarchived(tmp_path):
    from qmd2.attachments import rescue

    home = tmp_path / 'qmd'
    home.mkdir()
    raw = dumps({'raw': {'journal': 'synthetic'}, 'decoded': WHATSAPP}).encode()
    spool(
        home,
        'whatsapp',
        [('payload', entry('whatsapp', '9@g.us:message', raw, False, '2026-10-13T00:00:00Z'), raw)],
    )
    ingest(home)
    result = rescue(home, days=3650, dry_run=True)
    assert (result['total'], result['archived'], result['dry_run']) == (1, 0, True)


def test_verify_samples_an_archived_attachment(tmp_path, monkeypatch):
    from qmd2 import verify
    from qmd2.packs import Packs
    from qmd2.state import State

    wrapper = tmp_path / 'qmd2'
    wrapper.write_text(f'#!/bin/sh\nexec {sys.executable} -m qmd2.cli "$@"\n')
    wrapper.chmod(0o700)
    monkeypatch.setenv('QMD2_VERIFY_CLI', str(wrapper))
    home = archived(tmp_path)
    assert verify.raw_attachment(home, State(home)) is True

    # A locator that answers with another attachment's bytes fails the sample: no silent pass.
    state = State(home, writable=True)
    pack = next(p for p in state.packs('raw'))
    other = next(
        loc for loc, head, _ in Packs(state).iter_pack(pack) if head['id'].endswith('/att/1')
    )
    state.set('media:synthetic:parent/att/0', other)
    state.commit()
    state.db.close()
    assert verify.raw_attachment(home, State(home)) is False


def whatsapp_home(tmp_path, archived_attachment=False, parents=(WHATSAPP,)):
    """A WhatsApp parent (or two) and, optionally, its attachment's archived bytes."""
    home = tmp_path / 'qmd'
    home.mkdir()
    for index, parent in enumerate(parents):
        raw = dumps({'raw': {'journal': 'synthetic'}, 'decoded': parent}).encode()
        name = 'payload' if index == 0 else f'payload{index}'
        row = entry('whatsapp', f'9@g.us:{parent["id"]}', raw, False)
        spool(home, 'whatsapp', [(name, row, raw)])
    if archived_attachment:
        archived_row = entry('attachment', PARENT + '/att/0', PAYLOAD)
        spool(home, 'attachment', [('payload', archived_row, PAYLOAD)])
    ingest(home)
    return home


def test_worker_paths_never_walk_the_lanes(tmp_path, monkeypatch):
    from qmd2 import attachments, packs

    home = archived(tmp_path)  # the lanes hold two archived records; the arrival is neither
    walked = []
    original = packs.Packs.iter_pack

    def spy(self, row, start=0):
        walked.append((row['source'], row['seq']))
        return original(self, row, start)

    monkeypatch.setattr(packs.Packs, 'iter_pack', spy)
    monkeypatch.setattr(
        attachments, 'fetch', lambda home, row, target, original=False: target.write_bytes(PAYLOAD)
    )
    row = dict(
        id='wa:x:g@g.us:new/att/0',
        ref=dict(source='whatsapp', type='document', fileLength=str(len(PAYLOAD))),
        state='queued',
    )
    attachments.archive_media(home, row, tmp_path / 'media', lambda message: None)
    assert row['state'] == 'archived'
    assert attachments.archived_locator(home, 'wa:x:g@g.us:new/att/1') is None
    assert walked == []


def test_rescue_needs_recorded_locators_and_never_walks(tmp_path, monkeypatch):
    from qmd2 import attachments, packs

    home = whatsapp_home(tmp_path, archived_attachment=True)
    walked = []
    original = packs.Packs.iter_pack
    monkeypatch.setattr(
        packs.Packs,
        'iter_pack',
        lambda self, row, start=0: walked.append(row['seq']) or original(self, row, start),
    )
    # Archived by the old code: no locator is recorded, so rescue must not guess by walking.
    result = attachments.rescue(home, days=3650, dry_run=True)
    assert 'attachments index' in result['reason'] and walked == []
    assert cli(home, 'attachments', 'rescue', '--dry-run').returncode == 4
    # The verb is the one walk; ingestd recorded this locator already, so it only writes the index.
    assert attachments.index_locators(home)['written'] == 0
    walked.clear()
    result = attachments.rescue(home, days=3650, dry_run=True)
    assert (result['total'], result['archived_index'], walked) == (0, 1, [])
    assert cli(home, 'attachments', 'index', '--dry-run').returncode == 0
    assert cli(home, 'attachments', 'rescue', '--dry-run').returncode == 0


def test_index_locators_backfills_legacy_records_once(tmp_path):
    from qmd2 import attachments
    from qmd2.state import State

    home = archived(tmp_path)
    state = State(home, writable=True)
    state.db.execute("DELETE FROM checkpoints WHERE key LIKE 'media:%'")
    state.commit()
    state.db.close()

    dry = attachments.index_locators(home, dry_run=True)
    assert (dry['records'], dry['written'], dry['dry_run'], dry['local'], dry['b2']) == (
        2,
        2,
        True,
        1,
        0,
    )
    assert dry['bytes'] > 0 and attachments.checkpoint_ids(home) == set()

    done = attachments.index_locators(home)
    assert (done['records'], done['written']) == (2, 2)
    assert attachments.checkpoint_ids(home) == {
        'synthetic:parent/att/0',
        'synthetic:parent/att/1',
    }
    # Resumable: a second run finds nothing left to write.
    assert attachments.index_locators(home)['written'] == 0


def test_attachment_raw_refuses_a_hidden_parent(tmp_path, monkeypatch):
    from qmd2 import attachments

    home = whatsapp_home(tmp_path, archived_attachment=True)
    identifier = PARENT + '/att/0'
    assert cli(home, 'get', identifier, '--raw').stdout == PAYLOAD
    monkeypatch.setenv('QMD_UNTIL', '2000-01-01T00:00:00Z')
    assert cli(home, 'get', PARENT).returncode == 3
    assert cli(home, 'get', identifier, '--raw').returncode == 3
    # The fetch path refuses before it fetches anything.
    (tmp_path / 'fresh').mkdir()
    fresh = whatsapp_home(tmp_path / 'fresh')
    monkeypatch.setattr(
        attachments, 'fetch', lambda *a, **k: pytest.fail('fetched under a ceiling')
    )
    with pytest.raises(LookupError):
        attachments.media_source(fresh, identifier)


def test_attachment_raw_without_a_parent_obeys_a_ceiling(tmp_path, monkeypatch):
    home = archived(tmp_path)  # an archived record whose parent document is not indexed
    identifier = 'synthetic:parent/att/0'
    assert cli(home, 'get', identifier, '--raw').stdout == PAYLOAD
    monkeypatch.setenv('QMD_UNTIL', '2000-01-01T00:00:00Z')
    assert cli(home, 'get', identifier, '--raw').returncode == 3


def test_raw_returns_original_bytes_of_any_type(tmp_path):
    import io
    import zipfile

    from qmd2.attachments import Unavailable, stream, stream_original

    xlsx = io.BytesIO()
    with zipfile.ZipFile(xlsx, 'w') as archive:
        archive.writestr('xl/workbook.xml', '<workbook/>')
    bodies = [xlsx.getvalue(), b'\0\0\0\x18ftypmp42' + b'\0' * 96, bytes(range(256)) * 4]
    # Extraction still routes by type; the archive copy does not, so every original is archived.
    for body in bodies:
        with pytest.raises(Unavailable, match='unsupported_type'):
            stream(io.BytesIO(body), tmp_path / 'routed')
        assert stream_original(io.BytesIO(body), tmp_path / 'original').read_bytes() == body
    home = tmp_path / 'qmd'
    home.mkdir()
    spool(
        home,
        'attachment',
        [
            (f'payload{index}', entry('attachment', f'synthetic:parent/att/{index}', body), body)
            for index, body in enumerate(bodies)
        ],
    )
    ingest(home)
    for index, body in enumerate(bodies):
        result = cli(home, 'get', f'synthetic:parent/att/{index}', '--raw')
        assert (result.returncode, result.stdout) == (0, body)


def test_archive_limit_is_enforced_on_bytes_read(tmp_path, monkeypatch):
    import io

    from qmd2 import attachments

    monkeypatch.setattr(attachments, 'ARCHIVE_LIMIT', 64)
    assert attachments.stream_original(io.BytesIO(b'x' * 64), tmp_path / 'at-cap').read_bytes()
    # A declared size is only a hint; the cap holds on what was actually read.
    assert not attachments.too_large({'fileLength': '1'})
    with pytest.raises(attachments.Unavailable, match='too_large'):
        attachments.stream_original(io.BytesIO(b'x' * 65), tmp_path / 'over-cap')


def test_verify_skips_records_raw_does_not_accept(tmp_path, monkeypatch):
    from qmd2 import verify
    from qmd2.packs import Packs
    from qmd2.state import State

    wrapper = tmp_path / 'qmd2'
    wrapper.write_text(f'#!/bin/sh\nexec {sys.executable} -m qmd2.cli "$@"\n')
    wrapper.chmod(0o700)
    monkeypatch.setenv('QMD2_VERIFY_CLI', str(wrapper))
    home = tmp_path / 'qmd'
    home.mkdir()
    key = 'link:abc/att/0/transcript-raw'  # archive-only output record: not an attachment id
    spool(home, 'attachment', [('payload', entry('attachment', key, PAYLOAD), PAYLOAD)])
    ingest(home)
    state = State(home)
    assert verify.raw_attachment(home, state) is True
    # A checkpoint whose pack is gone from this host is not sampled: verify never reads B2.
    (tmp_path / 'recalled').mkdir()
    recalled = archived(tmp_path / 'recalled')
    for sealed in (False, True):
        Packs(State(recalled)).path('raw', 'attachment', 0, sealed).unlink(missing_ok=True)
    assert verify.raw_attachment(recalled, State(recalled)) is True


def test_manual_raw_falls_back_to_its_document(tmp_path):
    import io

    from qmd2 import attachments
    from qmd2.state import State

    home = tmp_path / 'qmd'
    home.mkdir()
    body = b'Synthetic manual lighthouse\n'
    identifier = 'manual:' + hashlib.sha256(body).hexdigest()
    attachments.add(home, io.BytesIO(body), 'sample.txt', None, lambda message: None)
    ingest(home)
    assert cli(home, 'get', identifier, '--raw').stdout == body
    state = State(home, writable=True)
    state.db.execute('DELETE FROM checkpoints WHERE key=?', ('media:' + identifier,))
    state.commit()
    state.db.close()
    result = cli(home, 'get', identifier, '--raw')
    # No archive record: the document's own raw record, as before -- not the original bytes.
    assert (result.returncode, result.stdout != body) == (0, True)
    assert identifier.encode() in result.stdout


def test_rescue_counts_a_bad_item_and_continues(tmp_path, monkeypatch):
    from qmd2 import attachments

    second = dict(WHATSAPP, id='message2', text='Synthetic harbor two')
    home = whatsapp_home(tmp_path, parents=(WHATSAPP, second))
    identifier = PARENT + '/att/0'
    real = attachments.attachment_ref

    def ref(home, value, parent=None):
        if value == identifier:
            raise IndexError('media list shorter than attachments')
        return real(home, value, parent)

    monkeypatch.setattr(attachments, 'attachment_ref', ref)
    monkeypatch.setattr(attachments, 'RESCUE_PACE', 0)
    monkeypatch.setattr(
        attachments, 'fetch', lambda home, row, target, original=False: target.write_bytes(PAYLOAD)
    )
    result = attachments.rescue(home, days=3650)
    assert (result['total'], result['archived'], result['failed'], result['queued']) == (
        2,
        1,
        {'IndexError': 1},
        0,
    )


def test_rescue_skips_a_parents_link_entries(tmp_path, monkeypatch):
    from qmd2 import attachments
    from qmd2.attachments import normalized_url

    url = 'https://example.test/synthetic-harbor'
    parent = dict(WHATSAPP, text='Synthetic harbor ' + url)
    home = whatsapp_home(tmp_path, parents=(parent,))
    digest = hashlib.sha256(normalized_url(url).encode()).hexdigest()
    link = f'{PARENT}/att/link-{digest}'
    # A link entry is not media: fetching it is a named refusal, never a KeyError or a ValueError.
    with pytest.raises(attachments.Unavailable) as caught:
        attachments.attachment_ref(home, link)
    assert str(caught.value) == 'not_media'
    monkeypatch.setattr(attachments, 'RESCUE_PACE', 0)
    monkeypatch.setattr(
        attachments, 'fetch', lambda home, row, target, original=False: target.write_bytes(PAYLOAD)
    )
    result = attachments.rescue(home, days=3650)
    assert (result['total'], result['skipped'], result['archived']) == (1, 1, 1)
    assert result['failed'] == {}


def test_rescue_retries_an_outcome_qmd_failed(tmp_path, monkeypatch):
    from qmd2 import attachments

    home = whatsapp_home(tmp_path)
    monkeypatch.setattr(attachments, 'RESCUE_PACE', 0)

    def broken(home, row, target, original=False):
        raise attachments.Failed('archive_lane_unavailable')

    monkeypatch.setattr(attachments, 'fetch', broken)
    first = attachments.rescue(home, days=3650)
    assert (first['archived'], first['failed']) == (0, {'archive_lane_unavailable': 1})
    ingest(home)  # what qmd itself broke is now a stored outcome, state failed
    attachments.index_locators(home)
    monkeypatch.setattr(
        attachments, 'fetch', lambda home, row, target, original=False: target.write_bytes(PAYLOAD)
    )
    second = attachments.rescue(home, days=3650)
    assert (second['total'], second['archived'], second['failed']) == (1, 1, {})


def test_rescue_does_not_retry_a_source_refusal(tmp_path, monkeypatch):
    from qmd2 import attachments

    home = whatsapp_home(tmp_path)
    monkeypatch.setattr(attachments, 'RESCUE_PACE', 0)

    def expired(home, row, target, original=False):
        raise urllib.error.HTTPError('/v/synthetic', 410, 'Gone', None, None)

    monkeypatch.setattr(attachments, 'fetch', expired)
    first = attachments.rescue(home, days=3650)
    assert first['unavailable'] == {'media_expired (HTTP 410)': 1}
    ingest(home)
    attachments.index_locators(home)
    fetched = []
    monkeypatch.setattr(
        attachments, 'fetch', lambda home, row, target, original=False: fetched.append(row['id'])
    )
    second = attachments.rescue(home, days=3650)
    assert (second['unavailable'], fetched) == ({'media_expired (HTTP 410)': 1}, [])


def test_rescue_without_archive_lanes_needs_no_backfill(tmp_path):
    from qmd2.attachments import rescue

    home = whatsapp_home(tmp_path)  # nothing archived yet: nothing to record, nothing to refuse
    result = rescue(home, days=3650, dry_run=True)
    assert (result['total'], 'reason' in result) == (1, False)


def test_extract_refuses_a_kind_it_has_no_branch_for(tmp_path):
    """An mp4 or binary is never decoded as text: the gate refuses it before the whole read."""
    from qmd2 import attachments

    bodies = {
        'mp4': b'\0\0\0\x18ftypmp42' + b'\x01\x02abc\xff' * 50,
        'binary': bytes(range(256)) * 4,
    }
    for name, body in bodies.items():
        path = tmp_path / name
        path.write_bytes(body)
        assert attachments.route(attachments.detect(body)) not in attachments.CAPS
        with pytest.raises(attachments.Unavailable, match='unsupported_type'):
            attachments.extract(dict(ref=dict(source='whatsapp')), path)


def test_materialise_refuses_an_archived_binary(tmp_path):
    """An archived original of an unsupported type ends unavailable, and its bytes stay readable."""
    from qmd2 import attachments

    binary = b'\0\0\0\x18ftypmp42' + b'\x01\x02abc\xff' * 50
    home = tmp_path / 'qmd'
    home.mkdir()
    raw = dumps({'raw': {'journal': 'synthetic'}, 'decoded': WHATSAPP}).encode()
    spool(home, 'whatsapp', [('payload', entry('whatsapp', '9@g.us:message', raw, False), raw)])
    spool(home, 'attachment', [('payload', entry('attachment', PARENT + '/att/0', binary), binary)])
    ingest(home)
    row = attachments.demand(home, PARENT + '/att/0', lambda message: None)
    assert row['state'] == 'unavailable'
    assert row['reason'] == 'unsupported_type'
    result = cli(home, 'get', PARENT + '/att/0', '--raw')
    assert (result.returncode, result.stdout) == (0, binary)


def test_no_lane_walk_for_an_id_no_parent_lists(tmp_path, monkeypatch):
    from qmd2 import attachments

    home = whatsapp_home(tmp_path, archived_attachment=True)
    assert attachments.index_locators(home)['written'] == 0
    walked = lane_walks(monkeypatch)
    monkeypatch.setattr(attachments, 'fetch', lambda *args, **kwargs: pytest.fail('fetched'))
    with pytest.raises(LookupError):
        attachments.media_source(home, 'nonsense:parent/att/9')
    assert walked == []
    assert cli(home, 'get', 'nonsense:parent/att/9', '--raw').returncode == 3


def test_no_lane_walk_for_a_listed_id_once_the_backfill_has_run(tmp_path, monkeypatch):
    from qmd2 import attachments

    parent = dict(
        WHATSAPP,
        media=[
            dict(type='document', fileName='invoice.pdf', fileLength=str(len(PAYLOAD))),
            dict(type='image', fileName='photo.jpg', fileLength='10'),
        ],
    )
    home = tmp_path / 'qmd'
    home.mkdir()
    raw = dumps({'raw': {'journal': 'synthetic'}, 'decoded': parent}).encode()
    spool(home, 'whatsapp', [('payload', entry('whatsapp', '9@g.us:message', raw, False), raw)])
    spool(home, 'attachment', [('payload', entry('attachment', PARENT + '/att/0', PAYLOAD), PAYLOAD)])
    ingest(home)
    assert attachments.index_locators(home)['written'] == 0
    # att/1 is listed but was never archived: after the backfill a miss is final, so it is fetched.
    walked = lane_walks(monkeypatch)
    monkeypatch.setattr(
        attachments, 'fetch', lambda home, row, target, original=False: target.write_bytes(b'JPEG')
    )
    assert attachments.media_source(home, PARENT + '/att/1').read_bytes() == b'JPEG'
    assert walked == []


def test_manual_raw_obeys_the_ceiling(tmp_path, monkeypatch):
    """A manual id resolves its own document, so the ceiling filters its bytes too."""
    import io

    from qmd2 import attachments

    home = tmp_path / 'qmd'
    home.mkdir()
    body = b'Synthetic manual lighthouse\n'
    identifier = 'manual:' + hashlib.sha256(body).hexdigest()
    attachments.add(home, io.BytesIO(body), 'sample.txt', None, lambda message: None)
    ingest(home)
    assert cli(home, 'get', identifier).returncode == 0
    assert cli(home, 'get', identifier, '--raw').stdout == body
    monkeypatch.setenv('QMD_UNTIL', '2000-01-01T00:00:00Z')
    assert cli(home, 'get', identifier).returncode == 3
    result = cli(home, 'get', identifier, '--raw')
    assert (result.returncode, result.stdout) == (3, b'')
