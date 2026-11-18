"""Real CLI/ingestd/filesystem chain: a reset keeps what capture cannot rewrite; revive restores it.

Spool entries, the request file, the daemon and the CLI are the production paths; nothing is mocked.
"""

import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

BOOKMARKS = {
    '1': ('Rewritten article', '<p>original lighthouse</p>', 'rewritten lighthouse'),
    '2': ('Unavailable article', '<p>original harbor</p>', None),
    '3': ('Failing article', '<p>original beacon</p>', None),
}
# The bookmark clock every fixture item carries; snapshot times are this plus the key.
ITEM_EPOCH = 1791856800
STATE_VERSIONS = {'instapaper': 'instapaper-snapshot-v1', 'whatsapp': 'whatsapp-decoded-v1'}


def stamp(offset=0):
    moment = datetime.fromtimestamp(time.time() + offset, timezone.utc)
    return moment.isoformat().replace('+00:00', 'Z')


def run(store, *args):
    return subprocess.run(
        [sys.executable, '-m', *args],
        env=dict(os.environ, HOME=str(store.parents[2])),
        capture_output=True,
        text=True,
        check=True,
    )


def ingest(store):
    run(store, 'qmd2.ingestd', '--home', str(store), '--no-b2', '--once')


def get(store, identifier):
    result = subprocess.run(
        [sys.executable, '-m', 'qmd2.cli', '--home', str(store), 'get', identifier],
        env=dict(os.environ, HOME=str(store.parents[2])),
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout


def snapshot(key, html):
    bookmark = {
        'bookmark_id': int(key),
        'title': BOOKMARKS[key][0],
        'time': ITEM_EPOCH + int(key),
        'url': 'https://example.test',
    }
    return {'bookmark': bookmark, 'html': html}


def spool(store, captured_at, payloads, source='instapaper', item_time=None):
    """One capture's spool: id -> (payload, labels), as qmd2-fetch leaves it."""
    root = store / 'spool' / source
    root.mkdir(parents=True, exist_ok=True)
    lines = []
    for key, (payload, labels) in payloads.items():
        body = json.dumps(payload).encode()
        name = f'objects/{key}'
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_bytes(body)
        lines.append(
            dict(
                schema='qmd.spool-entry.v1',
                source=source,
                account='synthetic',
                key=key,
                path=name,
                bytes=len(body),
                sha256=hashlib.sha256(body).hexdigest(),
                state=dict(version=STATE_VERSIONS[source], labels=labels),
                captured_at=captured_at,
                **({'item_time': item_time} if item_time else {}),
            )
        )
    with (root / 'index.jsonl').open('a') as f:
        for line in lines:
            f.write(json.dumps(line) + '\n')


def initial(store, keys=BOOKMARKS):
    rows = {key: (snapshot(key, BOOKMARKS[key][1]), {'folder': 'archive'}) for key in keys}
    spool(store, stamp(-3600), rows)
    ingest(store)


def request(store, identifier, **body):
    (store / 'reindex.request').write_text(json.dumps(dict(id=identifier, **body)))
    ingest(store)
    return json.loads((store / 'reindex.result').read_text())


def reset_tombstones(store, ids, reset_at, capture, source='instapaper'):
    """Leave the store as the reset before this fix left it: a tombstone per bound doc."""
    from qmd2.ingestd import Ingestd

    daemon = Ingestd(store, remote=False)
    try:
        daemon.commit()
        daemon.queue_checkpoint('reset:' + source, reset_at)
        for identifier in ids:
            old = daemon.index.lookup(identifier)
            doc = daemon.tombstone(source, old)
            daemon.documents(source, [doc], old['version']['raw'], capture)
        daemon.commit()
    finally:
        daemon.upload_pool.shutdown()
        daemon.state.db.close()
        daemon.lock.close()


def clockless_tombstone(store, identifier, reset_at, raw):
    """A reset tombstone from a version with no source clock: null version time, its raw kept."""
    from qmd2.doc import version
    from qmd2.ingestd import Ingestd

    daemon = Ingestd(store, remote=False)
    try:
        daemon.commit()
        origin = daemon.record(
            'instapaper',
            raw,
            {'account': 'synthetic', 'key': '9', 'time': stamp(-3600)},
            docs=[],
        )
        old = {
            'id': identifier,
            'item': identifier,
            'group': 'articles',
            'version': {'time': None, 'parser': 'instapaper@3', 'raw': origin},
        }
        tombstone = daemon.tombstone('instapaper', old) | {'raw': origin}
        daemon.queue_checkpoint('reset:instapaper', reset_at)
        daemon.state.set('deleted:' + identifier, version(tombstone, reset_at, 0))
        daemon.commit()
    finally:
        daemon.upload_pool.shutdown()
        daemon.state.db.close()
        daemon.lock.close()


def test_reset_keeps_docs_capture_cannot_rewrite(tmp_path):
    store = tmp_path / '.local/share/qmd2'
    initial(store)
    for key in BOOKMARKS:
        assert get(store, f'instapaper:synthetic:{key}')[0] == 0

    cursor = store / 'capture-state/instapaper'
    cursor.mkdir(parents=True)
    (cursor / 'cursor.json').write_text('{}')
    assert request(store, 'reset', source='instapaper', reset=True) == {'id': 'reset', 'ok': True}
    assert not cursor.exists()
    # No doc is tombstoned up front: every id answers until capture rewrites it.
    for key in BOOKMARKS:
        assert get(store, f'instapaper:synthetic:{key}')[0] == 0

    # Capture after the reset: id 1 rewritten, id 2 text_unavailable, id 3's fetch failed.
    spool(
        store,
        stamp(60),
        {
            '1': (snapshot('1', f'<p>{BOOKMARKS["1"][2]}</p>'), {'folder': 'archive'}),
            '2': (
                {'bookmark': snapshot('2', '')['bookmark'], 'text_unavailable': True},
                {'folder': 'archive', 'reason': 'text_unavailable'},
            ),
        },
    )
    ingest(store)
    code, text = get(store, 'instapaper:synthetic:1')
    assert code == 0 and BOOKMARKS['1'][2] in text
    # The excluded id answers capture's text_unavailable snapshot, not its pre-reset text; a
    # failed id, which capture wrote nothing for, keeps its last version.
    code, text = get(store, 'instapaper:synthetic:2')
    assert code == 0 and 'Article text unavailable' in text and 'original harbor' not in text
    code, text = get(store, 'instapaper:synthetic:3')
    assert code == 0 and 'original beacon' in text


def test_revive_reset_writes_the_version_a_reset_tombstoned(tmp_path):
    store = tmp_path / '.local/share/qmd2'
    initial(store)
    reset_at = time.time_ns()
    ids = [f'instapaper:synthetic:{key}' for key in BOOKMARKS]
    reset_tombstones(store, ids, reset_at, reset_at)
    for key in BOOKMARKS:
        assert get(store, f'instapaper:synthetic:{key}')[0] != 0

    # The verb prints the revived ids as JSON, through the request file the daemon reads.
    expected = [f'instapaper:synthetic:{key}' for key in BOOKMARKS]
    revive = [sys.executable, '-m', 'qmd2.cli', '--home', str(store), 'reindex', '--revive-reset']
    cli = subprocess.Popen(
        [*revive, 'instapaper'],
        env=dict(os.environ, HOME=str(store.parents[2])),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not (store / 'reindex.request').exists():
            assert cli.poll() is None and time.monotonic() < deadline
            time.sleep(0.02)
        ingest(store)
        stdout, stderr = cli.communicate(timeout=20)
        assert cli.returncode == 0 and stderr == ''
        assert json.loads(stdout) == expected
    finally:
        if cli.poll() is None:
            cli.kill()
            cli.communicate()

    for key, (title, _, _) in BOOKMARKS.items():
        code, text = get(store, f'instapaper:synthetic:{key}')
        assert code == 0 and title in text
    # A revived doc is no tombstone: a second pass has nothing to write.
    assert request(store, 'revive', source='instapaper', revive=True) == {
        'id': 'revive',
        'ok': True,
        'revived': [],
    }


def test_revive_reset_leaves_a_tombstone_older_than_its_reset(tmp_path):
    store = tmp_path / '.local/share/qmd2'
    initial(store, ('1',))
    reset_at = time.time_ns()
    reset_tombstones(store, ['instapaper:synthetic:1'], reset_at, reset_at - 3600 * 10**9)
    assert request(store, 'revive', source='instapaper', revive=True)['revived'] == []
    assert get(store, 'instapaper:synthetic:1')[0] != 0


def test_revive_reset_finds_a_whatsapp_tombstone(tmp_path):
    store = tmp_path / '.local/share/qmd2'
    identifier = 'wa:synthetic:8@s.whatsapp.net:one'
    message = {
        'id': 'one',
        'chat': '8@s.whatsapp.net',
        'kind': 'message',
        'text': 'synthetic lighthouse',
        'timestamp': stamp(-7200),
    }
    spool(
        store,
        stamp(-3600),
        {'8@s.whatsapp.net:one': ({'decoded': message}, {})},
        source='whatsapp',
        item_time=message['timestamp'],
    )
    ingest(store)
    assert get(store, identifier)[0] == 0
    reset_at = time.time_ns()
    reset_tombstones(store, [identifier], reset_at, reset_at, source='whatsapp')
    assert get(store, identifier)[0] != 0

    # A whatsapp id begins `wa:`, so only the version's own source names its tombstone.
    assert request(store, 'revive', source='whatsapp', revive=True)['revived'] == [identifier]
    code, text = get(store, identifier)
    assert code == 0 and 'synthetic lighthouse' in text


def test_revive_reset_keeps_the_watermark_at_the_item_times(tmp_path):
    store = tmp_path / '.local/share/qmd2'
    initial(store)
    assert request(store, 'reset', source='instapaper', reset=True) == {'id': 'reset', 'ok': True}
    reset_at = time.time_ns()
    ids = [f'instapaper:synthetic:{key}' for key in BOOKMARKS]
    reset_tombstones(store, ids, reset_at, reset_at)
    request(store, 'revive', source='instapaper', revive=True)

    # Both watermarks are native item times (qmd.status.v1), not the revive's wall clock.
    status = json.loads((store / 'status.json').read_text())['sources']['instapaper']
    assert status['searchable_through'] == ITEM_EPOCH + max(int(key) for key in BOOKMARKS)


def test_revive_reset_does_not_report_a_clockless_tombstone(tmp_path):
    store = tmp_path / '.local/share/qmd2'
    identifier = 'instapaper:synthetic:9'
    bookmark = {'bookmark_id': 9, 'title': 'Clockless article', 'url': 'https://example.test'}
    payload = {'bookmark': bookmark, 'html': '<p>clockless harbor</p>'}
    clockless_tombstone(store, identifier, time.time_ns(), json.dumps(payload).encode())

    # The raw reparses but the clockless fence keeps the tombstone: nothing is restored, so the
    # verb must not name the id, on this pass or a second one.
    assert get(store, identifier)[0] != 0
    assert request(store, 'revive', source='instapaper', revive=True)['revived'] == []
    assert request(store, 'revive', source='instapaper', revive=True)['revived'] == []
    assert get(store, identifier)[0] != 0
