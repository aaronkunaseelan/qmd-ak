from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from qmd2.doc import dumps, version
from qmd2.packs import Packs
from qmd2.state import State


WRITER = '''
import json,sys
from pathlib import Path
from qmd2.ingestd import Ingestd
home=Path(sys.argv[1]); phase=sys.argv[2]
d=Ingestd(home,remote=False)
ctx={'account':'synthetic','time':'2026-10-13T00:00:00Z'}
def write(n):
 text='synthetic lighthouse ' + ('x' * (9 * 1024 * 1024) if n == 1 else str(n))
 raw=json.dumps({'decoded':{'id':str(n),'chat':'7@lid','kind':'message','text':text,'timestamp':ctx['time']}}).encode()
 d.record('whatsapp',raw,dict(ctx,key=str(n)))
if phase == 'start':
 write(0);write(1)
elif phase == 'continue':
 for n in range(2,512):write(n)
elif phase == 'reparse':
 d.queue_checkpoint('ctx:whatsapp',{'aliases':{'7@lid':'8@s.whatsapp.net'}})
 d.commit();d.reindex('whatsapp')
elif phase == 'late':
 write(512)
d.commit()
'''


def test_feed_pack_restart_reparse_cli_chain(tmp_path):
    store = tmp_path / 'store'
    state = State(store, True)
    packs = Packs(state, writable=True)
    legacy = dict(
        id='wa:synthetic:8@s.whatsapp.net:legacy',
        item='wa:synthetic:8@s.whatsapp.net:legacy',
        source='whatsapp',
        group='mail',
        kind='message',
        time='2026-10-12T00:00:00Z',
        parts=[dict(kind='authored', text='synthetic legacy')],
        parser='whatsapp@2',
    )
    legacy['version'] = version(legacy)
    legacy_pointer = packs.append(
        'doc', 'whatsapp', {'id': legacy['id'], 'arrived': None}, dumps(legacy).encode()
    )
    packs.sync()
    for stream in packs.files.values():
        stream.close()
    state.commit()
    state.db.close()
    env = dict(os.environ, HOME=str(tmp_path))

    def phase(name):
        subprocess.run([sys.executable, '-c', WRITER, str(store), name], env=env, check=True)

    def cli(*args, until=None, check=True):
        return subprocess.run(
            [sys.executable, '-m', 'qmd2.cli', '--home', str(store), *args],
            env=dict(env, **({'QMD_UNTIL': until} if until else {})),
            capture_output=True,
            text=True,
            check=check,
        )

    contract = tmp_path / 'feed-contract.json'
    contract.write_text(cli('contract', 'qmd.feed.v1').stdout)
    validator = Path(__file__).with_name('validate.py')

    def feed(*args, limit=127, until=None):
        output = cli(
            'feed', '--source', 'whatsapp', *args, '--limit', str(limit), '--json', until=until
        )
        validated = subprocess.run(
            [sys.executable, '-I', str(validator), str(contract)],
            input=output.stdout,
            text=True,
            capture_output=True,
        )
        assert validated.returncode == 0, validated.stdout
        return json.loads(output.stdout)

    phase('start')
    epoch = State(store).get('feed_epoch')['at']
    since = datetime.fromtimestamp(epoch, timezone.utc).isoformat()
    page = feed('--since-arrival', since, limit=1)
    items = page['items']
    assert items[0]['id'] == 'wa:synthetic:7@lid:0' and not page['complete']
    page = feed('--after', page['next'], limit=1)
    items += page['items']
    assert page['complete'] and items[-1]['id'] == 'wa:synthetic:7@lid:1'
    large_token = page['next']
    empty = feed('--after', large_token)
    assert empty['items'] == [] and empty['next'] == large_token and empty['complete']
    phase('continue')
    assert State(store).get('feed_epoch')['at'] == epoch
    start = time.monotonic()
    batch = feed('--after', large_token, limit=500)
    elapsed = time.monotonic() - start
    print(f'feed local 500 items: {elapsed:.3f} s (CLI and contract validation)')
    assert len(batch['items']) == 500 and elapsed <= 2
    assert batch['items'][0]['token'] > large_token
    while True:
        page = feed('--after', page['next'])
        items += page['items']
        if page['complete']:
            break
    assert [row['id'] for row in items] == [f'wa:synthetic:7@lid:{n}' for n in range(512)]
    phase('reparse')
    while True:
        page = feed('--after', page['next'])
        items += page['items']
        if page['complete']:
            break
    assert len(items) == 1536
    assert [row['id'] for row in items[512:1024]] == [
        f'wa:synthetic:8@s.whatsapp.net:{n}' for n in range(512)
    ]
    assert all(row['reparse'] for row in items[512:])
    assert all(row['tombstone'] for row in items[1024:])
    assert {row['id'] for row in items[1024:]} == {row['id'] for row in items[:512]}
    assert all(row['chat'] == '8@s.whatsapp.net' for row in items[512:1024])
    assert len({(row['id'], row['version']) for row in items}) == len(items)
    assert [row['token'] for row in items] == sorted(row['token'] for row in items)
    assert all(row['arrived'] >= epoch for row in items)
    again = feed('--since-arrival', since, limit=2000)
    assert again['items'] == items and again['complete']
    precise = datetime.fromtimestamp(items[511]['arrived'], timezone.utc).isoformat()
    assert feed('--since-arrival', precise)['items'][0]['id'] == items[511]['id']
    from qmd2.feed import token

    legacy_page = feed('--after', token('whatsapp', legacy_pointer['seq'], 0), limit=1)
    assert legacy_page['items'][0]['arrived'] is None
    assert legacy_page['items'][0]['id'] == legacy['id']
    assert 'synthetic lighthouse' in cli('get', 'wa:synthetic:8@s.whatsapp.net:2').stdout
    assert json.loads(cli('search', 'lighthouse', '--json').stdout)
    phase('late')
    late = feed('--after', page['next'])
    assert len(late['items']) == 1 and late['items'][0]['id'].endswith(':512')
    assert late['items'][0]['time'] < late['items'][0]['arrived']
    hidden = feed('--since-arrival', since, until='2026-10-11T00:00:00Z')
    assert hidden['items'] == [] and hidden['head'] is None and hidden['complete']
    legacy_only = feed(
        '--after', token('whatsapp', legacy_pointer['seq'], 0), until='2026-10-12T00:00:00Z'
    )
    assert [row['id'] for row in legacy_only['items']] == [legacy['id']]
    assert legacy_only['complete'] and legacy_only['next'] == legacy_only['head']
    invalid = [
        [],
        ['--after', large_token, '--since-arrival', since],
        ['--after', 'bad'],
        ['--after', token('pi', 0, 0)],
        ['--since-arrival', 'nonsense'],
        ['--since-arrival', '2020-01-01T00:00:00Z'],
        ['--after', large_token, '--limit', '0'],
    ]
    for args in invalid:
        result = cli('feed', '--source', 'whatsapp', *args, '--json', check=False)
        assert result.returncode == 2
        if args == ['--since-arrival', '2020-01-01T00:00:00Z']:
            assert (
                'epoch' in result.stderr
                and str(datetime.fromtimestamp(epoch, timezone.utc).year) in result.stderr
            )
    assert (
        cli('feed', '--source', 'mail', '--since-arrival', since, '--json', check=False).returncode
        == 2
    )
    assert cli('feed', '--source', 'whatsapp', '--after', large_token, check=False).returncode == 2
    assert cli('feed', '--since-arrival', since, '--json', check=False).returncode == 2
    empty_source = json.loads(
        cli('feed', '--source', 'calendar', '--since-arrival', since, '--json').stdout
    )
    assert empty_source['items'] == [] and empty_source['next'] is None and empty_source['complete']
    assert (
        cli(
            'feed',
            '--source',
            'whatsapp',
            '--after',
            large_token,
            '--json',
            until='bad',
            check=False,
        ).returncode
        == 2
    )


def test_feed_origin_from_the_doc_and_its_tombstone(tmp_path):
    """Feed items carry their own origin, so a tombstone is filtered like the doc it removes."""
    from qmd2.ingestd import Ingestd
    from qmd2.sources.sessions import ROOTS

    def line(n, project):
        return json.dumps(
            {
                'type': 'user',
                'uuid': f'u{n}',
                'sessionId': 's',
                'cwd': project,
                'timestamp': '2026-10-13T01:00:00Z',
                'message': {'role': 'user', 'content': f'origin synthetic {n}'},
            }
        ).encode() + b'\n'

    root = tmp_path / 'sessions'
    root.mkdir()
    store = tmp_path / 'store'
    steward = str(Path.home()) + '/.local/state/steward/wakes/165'
    (root / 's.jsonl').write_bytes(line(1, steward) + line(2, steward))
    original = ROOTS[:]
    ROOTS[:] = [('claude', root)]
    daemon = Ingestd(store, remote=False)
    try:
        daemon.last_walk -= 15
        daemon.sessions(seconds=30)
        daemon.commit()
        for old in daemon.index.item('claude:s'):
            daemon.documents(
                'claude', [daemon.tombstone('claude', old)], old['version']['raw'], time.time_ns()
            )
        daemon.commit()
    finally:
        ROOTS[:] = original
        daemon.upload_pool.shutdown()
        daemon.index.writer.wait_merging_threads()
        del daemon.index
        daemon.state.db.close()
        daemon.lock.close()

    env = dict(os.environ, HOME=str(tmp_path))

    def cli(*args):
        return subprocess.run(
            [sys.executable, '-m', 'qmd2.cli', '--home', str(store), *args],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        ).stdout

    start = 'qmd1.claude.' + '0' * 20 + '.' + '0' * 20

    def feed(*args):
        return json.loads(
            cli('feed', '--source', 'claude', '--after', start, *args, '--json')
        )

    everything = feed()
    assert [(row['id'], row['tombstone']) for row in everything['items']] == [
        ('claude:s#u1', False),
        ('claude:s#u2', False),
        ('claude:s#u1', True),
        ('claude:s#u2', True),
    ]
    assert {row['origin'] for row in everything['items']} == {'steward'}
    assert [row['id'] for row in feed('--origin', 'steward')['items']] == [
        row['id'] for row in everything['items']
    ]
    assert feed('--not-origin', 'steward')['items'] == []
    assert feed('--not-origin', 'steward,gsd')['items'] == []
    assert feed('--origin', 'interactive')['items'] == []
    contract = tmp_path / 'feed-contract.json'
    contract.write_text(cli('contract', 'qmd.feed.v1'))
    validated = subprocess.run(
        [sys.executable, '-I', str(Path(__file__).with_name('validate.py')), str(contract)],
        input=json.dumps(everything),
        text=True,
        capture_output=True,
    )
    assert validated.returncode == 0, validated.stdout


def instapaper_store(tmp_path):
    """An Instapaper store built through the real record path: two labelled docs and one bare."""
    from qmd2.ingestd import Ingestd

    store = tmp_path / 'store'
    daemon = Ingestd(store, remote=False)
    ctx = {'account': 'synthetic', 'time': '2026-10-13T00:00:00Z'}
    try:
        rows = (
            ('42', 'archive', {}),
            ('43', 'archive', {'starred': '1'}),
            ('44', None, {}),
        )
        for key, folder, starred in rows:
            bookmark = dict(
                starred, bookmark_id=int(key), title='article ' + key, time=1791849600
            )
            raw = json.dumps({'bookmark': bookmark, 'html': '<p>synthetic lighthouse</p>'})
            raw = raw.encode()
            labels = {'folder': folder} if folder else {}
            daemon.record('instapaper', raw, dict(ctx, key=key, **labels))
        daemon.commit()
    finally:
        daemon.upload_pool.shutdown()
        daemon.index.writer.wait_merging_threads()
        del daemon.index
        daemon.state.db.close()
        daemon.lock.close()

    def cli(*args):
        return subprocess.run(
            [sys.executable, '-m', 'qmd2.cli', '--home', str(store), *args],
            env=dict(os.environ, HOME=str(tmp_path)),
            capture_output=True,
            text=True,
            check=True,
        ).stdout

    return cli


FOLDER_WHERE = {
    'instapaper:synthetic:42': ['archive'],
    'instapaper:synthetic:43': ['archive', 'starred'],
    'instapaper:synthetic:44': None,
}


def test_feed_carries_instapaper_folder_where(tmp_path):
    """An Instapaper doc's folder labels reach consumers as the feed item's `where`."""
    cli = instapaper_store(tmp_path)
    start = 'qmd1.instapaper.' + '0' * 20 + '.' + '0' * 20
    page = json.loads(cli('feed', '--source', 'instapaper', '--after', start, '--json'))
    assert {row['id']: row['where'] for row in page['items']} == FOLDER_WHERE
    contract = tmp_path / 'feed-contract.json'
    contract.write_text(cli('contract', 'qmd.feed.v1'))
    validated = subprocess.run(
        [sys.executable, '-I', str(Path(__file__).with_name('validate.py')), str(contract)],
        input=json.dumps(page),
        text=True,
        capture_output=True,
    )
    assert validated.returncode == 0, validated.stdout


def test_browse_shows_instapaper_folder_where(tmp_path):
    """browse reads `where` from an Instapaper doc as it does from a calendar doc."""
    cli = instapaper_store(tmp_path)
    browsed = json.loads(cli('browse', '--source', 'instapaper', '--json'))
    assert {row['id']: row['where'] for row in browsed['results']} == FOLDER_WHERE
    assert all('end' not in row for row in browsed['results'])
