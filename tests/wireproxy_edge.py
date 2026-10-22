#!/usr/bin/env python3
"""Synthetic wireproxy subprocess: a SOCKS listener, never a real VPN."""

import configparser
import json
import os
from pathlib import Path
import socketserver
import struct
import sys

config = configparser.ConfigParser()
config.read_file(sys.stdin)
host, port = config['Socks5']['BindAddress'].rsplit(':', 1)
relay = config['Peer']['PublicKey']
log = Path(__file__).with_name('tunnels.jsonl')
with log.open('a') as output:
    output.write(json.dumps(dict(pid=os.getpid(), relay=relay)) + '\n')


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        if self.rfile.read(3) != b'\x05\x01\x00':
            return
        self.wfile.write(b'\x05\x00')
        self.wfile.flush()
        head = self.rfile.read(4)
        assert head == b'\x05\x01\x00\x03'
        hostname = self.rfile.read(self.rfile.read(1)[0]).decode()
        self.rfile.read(2)
        self.wfile.write(b'\x05\x00\x00\x01' + bytes(4) + struct.pack('!H', 0))
        self.wfile.flush()
        request = self.rfile.readline().decode()
        while self.rfile.readline() not in (b'\r\n', b''):
            pass
        path = request.split()[1]
        assert hostname == 'localhost'
        code = 200 if path == '/recover' else 429 if path == '/mixed' else int(path[1:])
        body = b'Synthetic remote lighthouse harbor'
        location = b'Location: /recover\r\n' if path == '/302' else b''
        self.wfile.write(
            f'HTTP/1.1 {code} Synthetic\r\nContent-Length: {len(body)}\r\n'.encode()
            + location
            + b'Connection: close\r\n\r\n'
            + body
        )


with socketserver.ThreadingTCPServer((host, int(port)), Handler) as server:
    server.serve_forever()
