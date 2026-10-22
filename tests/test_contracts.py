import json
import os
from pathlib import Path
import subprocess
import sys
import shutil
import time

from qmd2.ingestd import Ingestd
from qmd2.sources import SOURCES
from qmd2.verify import lateness_limit


def test_status_contract_rejects_extra_key_when_closed(tmp_path):
    (tmp_path / 'status.json').write_text(json.dumps({'sources': {}}))
    env = dict(os.environ, HOME=str(tmp_path))
    command = [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path)]
    schema = json.loads(subprocess.check_output(command + ['contract', 'qmd.status.v1'], env=env))
    schema['additionalProperties'] = False
    contract = tmp_path / 'closed-contract.json'
    contract.write_text(json.dumps(schema))
    payload = json.loads(subprocess.check_output(command + ['status', '--json'], env=env))
    validator = Path(__file__).with_name('validate.py')
    for extra in (False, True):
        if extra:
            payload['unexpected'] = True
        result = subprocess.run(
            [sys.executable, '-I', str(validator), str(contract)],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
        )
        assert result.returncode == int(extra), result.stdout
        assert result.stdout == ('$.unexpected: unexpected\n' if extra else 'valid\n')


def test_currency_expiry_filesystem(tmp_path, monkeypatch):
    from qmd2.status import view

    monkeypatch.setattr(time, 'time', lambda: 10000)
    for cadence, fresh, expiry, current in (
        (15, 6400, 10000, True),
        (15, 6399, 9999, False),
        (15, 6401, 10001, True),
        (86400, 6400, 179200, True),
        (15, None, None, None),
        (0, 10000, None, None),
        (None, 10000, None, None),
    ):
        status = dict(sources={'synthetic': dict(cadence_s=cadence, fresh_through=fresh)})
        path = tmp_path / 'status.json'
        path.write_text(json.dumps(status))
        row = view(tmp_path, json.loads(path.read_text()))['sources']['synthetic']
        assert row['valid_until'] == expiry
        assert row['current'] is current


def test_status_contract_filesystem_cli(tmp_path):
    daemon = Ingestd(tmp_path, remote=False)
    daemon.record(
        'pi',
        json.dumps(
            {
                'type': 'message',
                'id': 'one',
                'timestamp': '2026-10-13T00:00:00Z',
                'message': {'role': 'user', 'content': 'synthetic lighthouse'},
            }
        ).encode(),
        {'runtime': 'pi', 'session': 'synthetic'},
    )
    daemon.commit()
    daemon.upload_pool.shutdown()
    daemon.state.db.close()
    daemon.lock.close()
    env = dict(os.environ, HOME=str(tmp_path))

    def cli(*args):
        return subprocess.run(
            [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path), *args],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        ).stdout

    contract = tmp_path / 'status-contract.json'
    contract.write_text(cli('contract', 'qmd.status.v1'))
    snapshot = json.loads((tmp_path / 'status.json').read_text())
    expected_text = (
        '\n'.join(f"{source}: lag {row['lag_s']} s" for source, row in snapshot['sources'].items())
        + '\n'
    )
    assert cli('status') == expected_text
    payload = json.loads(cli('status', '--json'))
    assert payload['installed_git_sha'] is None
    assert payload['alive'] is True
    assert payload['verify'] == dict(state=None, at=None, red_checks=[])
    assert set(payload['sources']) == {source for source, *_ in SOURCES}
    assert cli('search', 'lighthouse', '--json')
    assert 'synthetic lighthouse' in cli('get', 'pi:synthetic#one')
    for source, row in payload['sources'].items():
        assert row['limit_s'] == lateness_limit(row['cadence_s'])
        assert row['current'] is None and row['error'] is None
        assert row['valid_until'] is None
        if source in snapshot['sources']:
            assert row['cadence_s'] == (snapshot['sources'][source]['cadence_s'] or None)
        else:
            assert row['cadence_s'] is None
    validator = Path(__file__).with_name('validate.py')

    def validate():
        result = subprocess.run(
            [sys.executable, '-I', str(validator), str(contract)],
            input=cli('status', '--json'),
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout

    validate()
    # Read installed package metadata, not the working directory or the data home.
    package = tmp_path / 'installed' / 'qmd2'
    shutil.copytree(Path(__file__).parents[1] / 'qmd2', package)
    sha = '0123456789abcdef0123456789abcdef01234567'
    with (package / '__init__.py').open('a') as stream:
        stream.write(f'\ninstalled_git_sha = "{sha}"\n')
    installed = subprocess.run(
        [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path), 'status', '--json'],
        cwd=package.parent,
        env=dict(env, PYTHONPATH=str(package.parent)),
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(installed.stdout)['installed_git_sha'] == sha
    (tmp_path / 'verify.json').write_text(
        json.dumps(dict(state='red', time=123, red_checks=['synthetic:check']))
    )
    assert json.loads(cli('status', '--json'))['verify'] == dict(
        state='red', at=123, red_checks=['synthetic:check']
    )
    # An excluded head has an ingest outcome without becoming a searchable doc.
    now = time.time()
    snapshot['sources']['gmail'].update(
        ok=True,
        last_complete=now,
        fresh_through=now,
        source_head=now,
        processed_through=now,
        searchable_through=now - 9073,
        lag_s=9073,
    )
    (tmp_path / 'status.json').write_text(json.dumps(snapshot))
    gmail = json.loads(cli('status', '--json'))['sources']['gmail']
    assert gmail['ok'] is True and gmail['current'] is True
    assert gmail['lag_s'] == 0
    assert gmail['valid_until'] == gmail['fresh_through'] + gmail['limit_s']
    snapshot['heartbeat_at'] = 1
    snapshot['sources']['pi']['fresh_through'] = 1
    (tmp_path / 'status.json').write_text(json.dumps(snapshot))
    payload = json.loads(cli('status', '--json'))
    assert payload['alive'] is False and payload['sources']['pi']['current'] is False
    (tmp_path / 'verify.json').write_text('broken')
    assert json.loads(cli('status', '--json'))['verify']['state'] is None
    validate()
