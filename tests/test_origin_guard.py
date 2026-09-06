"""Same-origin guard for state-changing endpoints (audit finding #8).

Basic auth is attached automatically by browsers; a <form
enctype="text/plain"> submission produces a body that parses as JSON, so
without an Origin check any page the operator visits can fire
/api/library/delete or rewrite .env. Reverse-proxied deployments
allow-list their public origin via STATUS_UI_TRUSTED_ORIGINS."""
import json
import socket
import threading
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer

import pytest

from utils.status_server import StatusHandler


@pytest.fixture
def server():
    StatusHandler.auth_credentials = None
    StatusHandler.status_data_ref = None
    StatusHandler.trusted_origins = frozenset()
    srv = ThreadingHTTPServer(('127.0.0.1', 0), StatusHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f'127.0.0.1:{srv.server_address[1]}'
    srv.shutdown()
    StatusHandler.trusted_origins = frozenset()


def _post(host, path, headers=None):
    req = urllib.request.Request(
        f'http://{host}{path}', data=b'{}', method='POST',
        headers={'Content-Type': 'application/json', **(headers or {})})
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, json.loads(resp.read() or b'{}')
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b'{}')


def test_no_origin_allowed(server):
    """curl/scripts (no Origin, no Referer) must keep working."""
    status, _ = _post(server, '/api/library/refresh')
    assert status != 403 or 'cross-origin' not in str(_)


def test_matching_origin_allowed(server):
    status, body = _post(server, '/api/library/refresh',
                         {'Origin': f'http://{server}'})
    assert 'cross-origin' not in body.get('error', '')


def test_mismatched_origin_rejected(server):
    status, body = _post(server, '/api/library/refresh',
                         {'Origin': 'http://evil.example.com'})
    assert status == 403
    assert 'cross-origin' in body.get('error', '').lower()


def test_null_origin_rejected(server):
    status, _ = _post(server, '/api/library/refresh', {'Origin': 'null'})
    assert status == 403


def test_trusted_origin_allowed(server):
    StatusHandler.trusted_origins = frozenset({'https://zurgarr.example.com'})
    status, body = _post(server, '/api/library/refresh',
                         {'Origin': 'https://zurgarr.example.com'})
    assert 'cross-origin' not in body.get('error', '')


def test_referer_fallback_rejected_cross_site(server):
    status, _ = _post(server, '/api/library/refresh',
                      {'Referer': 'http://evil.example.com/attack.html'})
    assert status == 403


def test_delete_method_guarded(server):
    req = urllib.request.Request(
        f'http://{server}/api/history', method='DELETE',
        headers={'Origin': 'http://evil.example.com'})
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        code = resp.status
    except urllib.error.HTTPError as e:
        code = e.code
    assert code == 403
