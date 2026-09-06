"""Same-origin guard for state-changing endpoints (audit finding #8).

Basic auth is attached automatically by browsers; a <form
enctype="text/plain"> submission produces a body that parses as JSON, so
without an Origin check any page the operator visits can fire
/api/library/delete or rewrite .env. Reverse-proxied deployments
allow-list their public origin via STATUS_UI_TRUSTED_ORIGINS.

Note on test design: with auth_credentials=None (the fixture default),
do_POST/do_DELETE already 403 on the STATUS_UI_AUTH check, so asserting
only status==403 passes even with the origin guard deleted. Rejection
tests must assert on the 'cross-origin' error text; allow-path tests
must assert the error is the STATUS_UI_AUTH one (proving the request got
past the origin guard, not that no request was made at all)."""
import json
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


def _delete(host, path, headers=None):
    req = urllib.request.Request(
        f'http://{host}{path}', method='DELETE', headers=headers or {})
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, json.loads(resp.read() or b'{}')
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b'{}')


def test_no_origin_allowed(server):
    """curl/scripts (no Origin, no Referer) must keep working — and must
    reach past the origin guard to the STATUS_UI_AUTH check, not be
    rejected for cross-origin reasons."""
    status, body = _post(server, '/api/library/refresh')
    assert status == 403
    assert 'status_ui_auth' in body.get('error', '').lower()


def test_matching_origin_allowed(server):
    status, body = _post(server, '/api/library/refresh',
                         {'Origin': f'http://{server}'})
    assert status == 403
    assert 'status_ui_auth' in body.get('error', '').lower()


def test_mismatched_origin_rejected(server):
    status, body = _post(server, '/api/library/refresh',
                         {'Origin': 'http://evil.example.com'})
    assert status == 403
    assert 'cross-origin' in body.get('error', '').lower()


def test_null_origin_rejected(server):
    status, body = _post(server, '/api/library/refresh', {'Origin': 'null'})
    assert status == 403
    assert 'cross-origin' in body.get('error', '').lower()


def test_trusted_origin_allowed(server):
    StatusHandler.trusted_origins = frozenset({'https://zurgarr.example.com'})
    status, body = _post(server, '/api/library/refresh',
                         {'Origin': 'https://zurgarr.example.com'})
    assert status == 403
    assert 'status_ui_auth' in body.get('error', '').lower()


def test_trusted_origin_configured_mixed_case_allowed(server):
    """STATUS_UI_TRUSTED_ORIGINS may be entered with mixed case (e.g.
    copy-pasted from a browser address bar); the comparison itself must
    lowercase both sides so it still matches a lowercase browser Origin."""
    StatusHandler.trusted_origins = frozenset({'HTTPS://Zurgarr.Example.COM'})
    status, body = _post(server, '/api/library/refresh',
                         {'Origin': 'https://zurgarr.example.com'})
    assert status == 403
    assert 'status_ui_auth' in body.get('error', '').lower()


def test_referer_fallback_rejected_cross_site(server):
    status, body = _post(server, '/api/library/refresh',
                      {'Referer': 'http://evil.example.com/attack.html'})
    assert status == 403
    assert 'cross-origin' in body.get('error', '').lower()


def test_delete_method_guarded(server):
    status, body = _delete(server, '/api/history',
                            {'Origin': 'http://evil.example.com'})
    assert status == 403
    assert 'cross-origin' in body.get('error', '').lower()
