"""A transcript names the engine that ran it, read from the released transcriber it ran."""

import json
import sys

import pytest

from qmd2 import attachments

AUDIO = b'Synthetic ogg opus voice note\n'
SHA = '7af619beae2c50cb71103a1275a8683387f6d31c'
TOOL = '''import json, sys

flags = sys.argv[1:]
if '--engine-file' not in flags:
    sys.exit('engine file not requested')
open(flags[flags.index('--engine-file') + 1], 'w').write(
    json.dumps({{'provider': {provider!r}, 'model': {model!r}}})
)
print('synthetic transcript')
'''


def release(tmp_path, provider, model):
    """A tg-tools release as deploy materialises it: releases/<sha>/ with `current` beside it."""
    root = tmp_path / 'releases' / SHA / 'transcribe-audio'
    root.mkdir(parents=True)
    (root / 'transcribe.py').write_text(TOOL.format(provider=provider, model=model))
    (tmp_path / 'current').symlink_to(root.parent)
    return tmp_path / 'current/transcribe-audio/transcribe.py'


@pytest.fixture
def audio(tmp_path):
    path = tmp_path / 'note.ogg'
    path.write_bytes(AUDIO)
    return path


def test_self_hosted_engine_is_labelled_not_the_azure_alias(tmp_path, monkeypatch, audio):
    tool = release(tmp_path, 'self-hosted', 'parakeet-ultra@fluidaudio-0.18.0')
    monkeypatch.setattr(attachments, 'TOOL', tool)
    # qmd still asks for the Azure alias it falls back to; the engine that ran is what is recorded.
    text, model = attachments.transcribe(audio)
    assert text == 'synthetic transcript'
    assert model == f'self-hosted:parakeet-ultra@fluidaudio-0.18.0+{SHA}'


def test_azure_fallback_says_azure_and_the_release_names_its_version(tmp_path, monkeypatch, audio):
    tool = release(tmp_path, 'azure', 'transcribe-mini')
    monkeypatch.setattr(attachments, 'TOOL', tool)
    _, model = attachments.transcribe(audio)
    assert model == f'azure:transcribe-mini+{SHA}'
    assert attachments.transcriber_release(tool) == SHA


def test_a_transcriber_that_reports_no_engine_fails_loudly(tmp_path, monkeypatch, audio):
    tool = tmp_path / 'transcribe.py'
    tool.write_text("print('synthetic transcript')\n")
    monkeypatch.setattr(attachments, 'TOOL', tool)
    with pytest.raises(OSError) as caught:
        attachments.transcribe(audio)
    assert str(caught.value) == 'engine label missing'


def test_a_transcriber_that_heard_no_speech_is_no_text(tmp_path, monkeypatch, audio):
    tool = tmp_path / 'transcribe.py'
    tool.write_text(
        'import sys\n'
        'print("no speech detected in 12s of audio", file=sys.stderr)\n'
        'sys.exit(3)\n'
    )
    monkeypatch.setattr(attachments, 'TOOL', tool)
    # tg-tools says no engine heard speech: the audio's answer, as an image Vision reads empty.
    with pytest.raises(attachments.Unavailable) as refused:
        attachments.transcribe(audio)
    assert str(refused.value) == 'no_text'
