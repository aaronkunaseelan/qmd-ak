"""Per-fetch userspace exits; never share gluetun's peer with its WireGuard key."""

import base64
import fcntl
import http.client
import ipaddress
import json
import os
import random
import re
import socket
import subprocess
import time
import urllib.request
from contextlib import contextmanager
from pathlib import Path

from .attachments import Failed, Unavailable, env_file

RELAYS = 'https://api.mullvad.net/www/relays/wireguard/'
AM_I = 'https://am.i.mullvad.net/json'
GLUETUN_PROXY = 'http://127.0.0.1:8888'
CONFIG = Path.home() / '.config/qmd2/fetch.json'
VPN_ENV = Path.home() / '.config/agent-secrets/vpn-browser.env'
WIREPROXY = Path.home() / '.local/share/qmd2/bin/wireproxy'
LOCK = Path('/tmp') / f'qmd2-mullvad-{os.getuid()}.lock'


def shared_relay():
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({'http': GLUETUN_PROXY, 'https': GLUETUN_PROXY})
    )
    try:
        with opener.open(AM_I, timeout=15) as response:
            result = json.load(response)
        hostname = result['mullvad_exit_ip_hostname']
        if result['mullvad_exit_ip'] is not True or not re.fullmatch(r'[\w-]+-wg-\d+', hostname):
            raise ValueError()
        return hostname
    except Exception:
        raise Unavailable('shared_key_relay_unknown') from None


def select_relays():
    try:
        config = json.loads(CONFIG.read_text()) if CONFIG.exists() else {}
        countries = config.get('mullvad_countries', ['us', 'se', 'nl', 'de', 'ch'])
        if (
            not isinstance(countries, list)
            or not countries
            or not all(isinstance(c, str) and re.fullmatch('[a-z]{2}', c) for c in countries)
        ):
            raise ValueError()
    except (OSError, ValueError, AttributeError):
        raise Failed('mullvad_config') from None
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(RELAYS, timeout=15) as response:
            relays = json.load(response)
        candidates = [r for r in relays if r['active'] is True and r['country_code'] in countries]
    except Exception:
        raise Failed('mullvad_relays_unavailable') from None
    excluded = shared_relay()
    candidates = [r for r in candidates if r['hostname'] != excluded]
    random.SystemRandom().shuffle(candidates)
    if not candidates:
        raise Failed('mullvad_no_eligible_relay')
    return candidates


def credentials():
    try:
        if not VPN_ENV.exists():
            raise Failed('mullvad_key_missing')
        values = env_file(VPN_ENV)
    except OSError:
        raise Failed('mullvad_key_unreadable') from None
    try:
        key = values['WIREGUARD_PRIVATE_KEY']
        if len(base64.b64decode(key, validate=True)) != 32:
            raise ValueError()
        addresses = ', '.join(
            str(ipaddress.ip_interface(a.strip())) for a in values['WIREGUARD_ADDRESSES'].split(',')
        )
        return key, addresses
    except (KeyError, ValueError):
        raise Failed('mullvad_key_invalid') from None


def receive(sock, size):
    result = b''
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise ConnectionResetError('SOCKS connection closed')
        result += chunk
    return result


def socks_connect(proxy, host, port, timeout):
    sock = socket.create_connection(proxy, timeout=timeout)
    try:
        sock.sendall(b'\x05\x01\x00')
        if receive(sock, 2) != b'\x05\x00':
            raise ConnectionRefusedError('SOCKS negotiation failed')
        name = host.encode('idna')
        sock.sendall(b'\x05\x01\x00\x03' + bytes([len(name)]) + name + port.to_bytes(2))
        reply = receive(sock, 4)
        if reply[:3] != b'\x05\x00\x00':
            raise ConnectionRefusedError('SOCKS connect failed')
        size = {1: 4, 4: 16}.get(reply[3])
        if reply[3] == 3:
            size = receive(sock, 1)[0]
        if size is None:
            raise ConnectionRefusedError('SOCKS address invalid')
        receive(sock, size + 2)
        return sock
    except BaseException:
        sock.close()
        raise


class SocksHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host, proxy, **kwargs):
        super().__init__(host, **kwargs)
        self.proxy = proxy

    def connect(self):
        self.sock = socks_connect(self.proxy, self.host, self.port, self.timeout)


class SocksHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, proxy, **kwargs):
        super().__init__(host, **kwargs)
        self.proxy = proxy

    def connect(self):
        sock = socks_connect(self.proxy, self.host, self.port, self.timeout)
        try:
            self.sock = self._context.wrap_socket(sock, server_hostname=self.host)
        except BaseException:
            sock.close()
            raise


class SocksHandler(urllib.request.HTTPHandler, urllib.request.HTTPSHandler):
    def __init__(self, proxy):
        super().__init__()
        self.proxy = proxy

    def http_open(self, req):
        return self.do_open(lambda host, **kw: SocksHTTPConnection(host, self.proxy, **kw), req)

    def https_open(self, req):
        return self.do_open(lambda host, **kw: SocksHTTPSConnection(host, self.proxy, **kw), req)


@contextmanager
def tunnel(relay, secret):
    with os.fdopen(os.open(LOCK, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600), 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if relay['hostname'] == shared_relay():
            raise Unavailable('shared_key_relay_changed')
        key, addresses = secret
        config = (
            f'[Interface]\nPrivateKey = {key}\nAddress = {addresses}\nDNS = 10.64.0.1\n'
            f'[Peer]\nPublicKey = {relay["pubkey"]}\n'
            f'Endpoint = {ipaddress.IPv4Address(relay["ipv4_addr_in"])}:51820\n'
        )
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            proxy = listener.getsockname()
        config += f'[Socks5]\nBindAddress = 127.0.0.1:{proxy[1]}\n'
        try:
            process = subprocess.Popen(
                [
                    '/usr/bin/setpriv',
                    '--pdeathsig',
                    'KILL',
                    '--',
                    str(WIREPROXY),
                    '-s',
                    '-c',
                    '/dev/stdin',
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError:
            raise Failed('mullvad_tunnel_start_failed') from None
        try:
            try:
                process.stdin.write(config.encode())
                process.stdin.close()
                deadline = time.monotonic() + 10
                while True:
                    if process.poll() is not None or time.monotonic() >= deadline:
                        raise Failed('mullvad_tunnel_start_failed')
                    try:
                        with socket.create_connection(proxy, timeout=0.1):
                            break
                    except OSError:
                        time.sleep(0.05)
            except OSError:
                raise Failed('mullvad_tunnel_start_failed') from None
            try:
                yield urllib.request.build_opener(
                    urllib.request.ProxyHandler({}), SocksHandler(proxy)
                ).open
            except Exception:
                if process.poll() is not None:
                    raise Failed('mullvad_tunnel_failed') from None
                raise
        finally:
            if not process.stdin.closed:
                process.stdin.close()
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
