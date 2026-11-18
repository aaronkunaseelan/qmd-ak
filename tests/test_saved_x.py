"""A bookmark that left the X list is re-read live: the new version is no bookmark."""

import json
import os
import subprocess
import sys

from qmd2.cli import get_docs
from qmd2.doc import dumps
from qmd2.ingestd import Ingestd

CTX = {'account': 'synthetic', 'key': '42', 'time': '2026-10-13T00:00:00Z'}
TWEET = {
    'id': '42',
    'text': 'synthetic lighthouse',
    'createdAt': 'Tue Oct 13 00:00:00 +0000 2026',
    'author': {'username': 'synthetic'},
}
IDENT = 'x:synthetic:42'
START = 'qmd1.x.' + '0' * 20 + '.' + '0' * 20


def test_removed_bookmark_versions_as_post(tmp_path):
    daemon = Ingestd(tmp_path, remote=False)
    # The head scan archives the post while it is bookmarked: bird reports the viewer flag.
    saved = dumps({'tweet': dict(TWEET, bookmarked=True), 'captured_at': CTX['time']}).encode()
    daemon.record('x', saved, CTX, capture=1)
    daemon.commit()
    doc = get_docs(tmp_path, IDENT)[0]
    assert (doc['kind'], doc['bookmarked']) == ('bookmark', True)

    # The walk queues a refresh, and the demand fetch re-reads the post without the viewer flag.
    live = dumps({'tweet': dict(TWEET), 'captured_at': CTX['time']}).encode()
    daemon.record('x', live, CTX, capture=2)
    daemon.commit()
    doc = get_docs(tmp_path, IDENT)[0]
    assert (doc['kind'], doc['bookmarked']) == ('post', False)

    env = dict(os.environ, HOME=str(tmp_path))
    command = [sys.executable, '-m', 'qmd2.cli', '--home', str(tmp_path)]
    fetched = json.loads(subprocess.check_output(command + ['get', IDENT, '--json'], env=env))
    assert [(row['kind'], row['bookmarked']) for row in fetched['docs']] == [('post', False)]
    # Both versions reach consumers, each with the bookmark state of its own capture.
    page = json.loads(
        subprocess.check_output(
            command + ['feed', '--source', 'x', '--after', START, '--json'], env=env
        )
    )
    assert [(row['kind'], row['bookmarked']) for row in page['items']] == [
        ('bookmark', True),
        ('post', False),
    ]
    browsed = json.loads(
        subprocess.check_output(command + ['browse', '--source', 'x', '--json'], env=env)
    )
    assert [(row['kind'], row['bookmarked']) for row in browsed['results']] == [('post', False)]
