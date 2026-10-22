"""One refusal classifier and two tries through the real fetch/spool edges."""

import ssl
import sys
import urllib.error

import pytest

from qmd2 import attachments
from qmd2.ingestd import Ingestd


@pytest.mark.parametrize(
    'error,category,reason',
    [
        *[
            (urllib.error.HTTPError('', code, '', None, None), category, f'{reason} (HTTP {code})')
            for code, category, reason in [
                (403, 'blocked', 'remote_blocked'),
                (451, 'blocked', 'remote_blocked'),
                (429, 'temporary', 'rate_limited'),
                (500, 'temporary', 'remote_unavailable'),
                (503, 'temporary', 'remote_unavailable'),
                (599, 'temporary', 'remote_unavailable'),
                (404, 'permanent', 'media_expired'),
                (410, 'permanent', 'media_expired'),
                (400, 'permanent', 'remote_unavailable'),
            ]
        ],
        (TimeoutError(), 'temporary', 'remote_unavailable (timeout)'),
        (urllib.error.URLError(TimeoutError()), 'temporary', 'remote_unavailable (timeout)'),
        (ConnectionResetError(), 'temporary', 'remote_unavailable (connection_reset)'),
        (
            urllib.error.URLError(ConnectionRefusedError()),
            'temporary',
            'remote_unavailable (connection_refused)',
        ),
        (ssl.SSLError(), 'permanent', 'tls_error'),
        (urllib.error.URLError(ssl.SSLCertVerificationError()), 'permanent', 'tls_error'),
        (attachments.ConverterTimeout('converter deadline'), 'permanent', 'converter deadline'),
        (attachments.Failed('mullvad_key_missing'), 'permanent', 'mullvad_key_missing'),
        (OSError(), 'permanent', 'OSError'),
    ],
)
def test_refusal_classifier_table(error, category, reason):
    assert attachments.temporary_refusal(error) == (category, reason)


@pytest.mark.parametrize('code', [429, 503])
@pytest.mark.parametrize('prefix', ['HTTP', 'Azure OpenAI transcription error'])
def test_azure_subprocess_retains_provider_class(tmp_path, code, prefix):
    with pytest.raises(Exception) as caught:
        attachments.run(
            [
                sys.executable,
                '-c',
                f'import sys; print("{prefix} {code}: private-response", file=sys.stderr); sys.exit(1)',
            ],
            azure=True,
        )
    category = 'rate_limited' if code == 429 else 'remote_unavailable'
    assert attachments.temporary_refusal(caught.value) == ('temporary', f'{category} (HTTP {code})')
    assert 'private-response' not in str(caught.value)


@pytest.mark.parametrize('code', [429, 503])
def test_transcription_refusal_reaches_terminal_outcome(tmp_path, monkeypatch, code):
    import wave

    clock = [1800000000.0]
    monkeypatch.setattr(attachments.time, 'time', lambda: clock[0])
    tool = tmp_path / 'transcribe.py'
    tool.write_text(
        f'import sys\nprint("Azure OpenAI transcription error {code}: private-response", file=sys.stderr)\nsys.exit(1)\n'
    )
    monkeypatch.setattr(attachments, 'TOOL', tool)
    audio = tmp_path / 'tone.wav'
    with wave.open(str(audio), 'wb') as wav:
        wav.setparams((1, 2, 16000, 0, 'NONE', 'not compressed'))
        wav.writeframes(b'\x00\x00' * 1600)
    identifier = 'manual:synthetic-audio'
    attachments.enqueue(
        tmp_path,
        identifier,
        dict(id=identifier, source='manual', group='files', time=clock[0]),
        dict(source='manual', path=str(audio)),
    )
    first = clock[0]
    for attempt, offset in enumerate((0, 180), 1):
        clock[0] = first + offset
        row = attachments.process(tmp_path, {'id': identifier})
        assert row['attempts'] == attempt
        assert row['state'] == ('failed' if attempt == 2 else 'queued')
    daemon = Ingestd(tmp_path, remote=False)
    try:
        daemon.spool(20)
        daemon.commit()
        stored = attachments.stored_outcome(tmp_path, identifier)
        assert stored['state'] == 'failed' and stored['attempts'] == 2
        category = 'rate_limited' if code == 429 else 'remote_unavailable'
        assert stored['reason'] == f'{category} (HTTP {code})'
        assert attachments.snapshot(tmp_path)['failed'] == 1
        assert attachments.snapshot(tmp_path)['last_failure_at'] == clock[0]
    finally:
        daemon.upload_pool.shutdown()
        daemon.index.writer.wait_merging_threads()
        daemon.state.db.close()
        daemon.lock.close()


@pytest.mark.parametrize('code', [429, 503])
def test_remote_retry_schedule(tmp_path, monkeypatch, exits, code):
    clock = [1800000000.0]
    monkeypatch.setattr(attachments.time, 'time', lambda: clock[0])
    base, calls = exits
    identifier, _ = attachments.materialise(tmp_path, f'{base}/{code}')
    first = clock[0]
    row = attachments.process(tmp_path, {'id': identifier})
    assert row['state'] == 'queued' and row['attempts'] == 1
    assert len(row['fetched']['attempts']) == 3
    # An old override and a provider Retry-After cannot add another schedule.
    row['next_attempt_at'] = first + 86400
    attachments.save(attachments.queue_path(tmp_path, identifier), row)
    assert attachments.next_attempt_at(row) == first + 180
    clock[0] = first + 179
    assert attachments.process(tmp_path, {'id': identifier})['attempts'] == 1
    assert calls.count(f'/{code}') == 1
    clock[0] += 1
    row = attachments.process(tmp_path, {'id': identifier})
    assert row['state'] == 'failed' and row['attempts'] == 2
    assert len(row['fetched']['attempts']) == 6
    assert calls.count(f'/{code}') == 2
    assert attachments.next_attempt_at(row) is None
    daemon = Ingestd(tmp_path, remote=False)
    try:
        daemon.spool(20)
        daemon.commit()
        daemon.attachment_outcomes()
        stored = attachments.stored_outcome(tmp_path, identifier)
        assert stored['attempts'] == 2 and len(stored['fetched']['attempts']) == 6
        assert stored['state'] == 'failed'
        assert attachments.snapshot(tmp_path)['failed'] == 1
        assert attachments.snapshot(tmp_path)['last_failure_at'] == clock[0]
    finally:
        daemon.upload_pool.shutdown()
        daemon.index.writer.wait_merging_threads()
        daemon.state.db.close()
        daemon.lock.close()
