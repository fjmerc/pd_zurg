"""Endpoint tests for /api/search and /api/search/add — multi-provider
search (providers list, cache annotation opt-in, per-provider add)."""

import base64
import json
import os
import sys
import threading
import urllib.error
import urllib.request

import pytest
from unittest.mock import patch

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from http.server import ThreadingHTTPServer
from utils.status_server import StatusHandler

_AUTH = 'test:secret'
_AUTH_HEADER = {
    'Authorization': 'Basic ' + base64.b64encode(_AUTH.encode()).decode(),
}


@pytest.fixture
def status_server():
    """Spin up a StatusHandler on a random localhost port.  POST endpoints
    require auth, so credentials are configured and sent by _post_json."""
    StatusHandler.auth_credentials = _AUTH
    StatusHandler.status_data_ref = None

    # Bind with port=0 directly — the OS assigns and HOLDS the port
    # atomically, avoiding the close/re-bind TOCTOU race of probing a
    # free port with a throwaway socket first.
    server = ThreadingHTTPServer(('127.0.0.1', 0), StatusHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{port}'
    finally:
        StatusHandler.auth_credentials = None
        server.shutdown()
        server.server_close()


def _post_json(url, payload):
    """POST JSON, return (status_code, parsed_body) — 4xx included."""
    body = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        url, data=body, method='POST',
        headers={'Content-Type': 'application/json', **_AUTH_HEADER},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode('utf-8'))


_SAMPLE_RESULT = {
    'info_hash': 'a' * 40, 'title': 'Movie.2024.1080p', 'seeds': 100,
    'quality': {'label': '1080p', 'score': 3}, 'size_bytes': 1000,
    'source_name': 'S', 'cached': True, 'cached_service': 'torbox',
}


class TestApiSearch:

    @patch('utils.search.list_configured_services')
    @patch('utils.search.search_torrents')
    def test_returns_results_and_providers(self, mock_search, mock_list,
                                           status_server):
        mock_search.return_value = [dict(_SAMPLE_RESULT)]
        mock_list.return_value = ['realdebrid', 'torbox']
        status, data = _post_json(
            status_server + '/api/search',
            {'imdb_id': 'tt1234567', 'type': 'movie'},
        )
        assert status == 200
        assert data['providers'] == ['realdebrid', 'torbox']
        assert data['results'][0]['cached'] is True
        assert data['results'][0]['cached_service'] == 'torbox'

    @patch('utils.search.list_configured_services')
    @patch('utils.search.search_torrents')
    def test_opts_into_auto_probe_annotation(self, mock_search, mock_list,
                                             status_server):
        """The UI endpoint must request annotation with the TB-preferring
        probe service — the whole point of the cache badge column."""
        mock_search.return_value = []
        mock_list.return_value = []
        status, _ = _post_json(
            status_server + '/api/search',
            {'imdb_id': 'tt1234567', 'type': 'movie'},
        )
        assert status == 200
        _, kwargs = mock_search.call_args
        assert kwargs['annotate_cache'] is True
        assert kwargs['cache_service'] == 'auto_probe'


class TestApiSearchAdd:

    _ADD_OK = {'success': True, 'torrent_id': '1', 'service': 'torbox',
               'error': ''}

    @patch('utils.search.add_to_debrid')
    @patch('utils.search.is_service_configured')
    def test_explicit_service_passed_through(self, mock_cfg, mock_add,
                                             status_server):
        mock_cfg.return_value = True
        mock_add.return_value = dict(self._ADD_OK)
        status, data = _post_json(
            status_server + '/api/search/add',
            {'info_hash': 'a' * 40, 'title': 'T', 'service': 'torbox'},
        )
        assert status == 200
        assert data['success'] is True
        mock_cfg.assert_called_once_with('torbox')
        _, kwargs = mock_add.call_args
        assert kwargs['service'] == 'torbox'

    @patch('utils.search.add_to_debrid')
    @patch('utils.search.is_service_configured')
    def test_unconfigured_service_rejected(self, mock_cfg, mock_add,
                                           status_server):
        mock_cfg.return_value = False
        status, data = _post_json(
            status_server + '/api/search/add',
            {'info_hash': 'a' * 40, 'title': 'T', 'service': 'alldebrid'},
        )
        assert status == 400
        assert 'not configured' in data['error']
        mock_add.assert_not_called()

    @patch('utils.search.add_to_debrid')
    def test_non_string_service_rejected(self, mock_add, status_server):
        status, data = _post_json(
            status_server + '/api/search/add',
            {'info_hash': 'a' * 40, 'title': 'T', 'service': 123},
        )
        assert status == 400
        assert 'service must be a string' in data['error']
        mock_add.assert_not_called()

    @patch('utils.search.add_to_debrid')
    def test_omitted_service_defaults_to_autodetect(self, mock_add,
                                                    status_server):
        """Backward compat: no service field → add_to_debrid gets
        service=None and applies its own RD-first auto-detection."""
        mock_add.return_value = {'success': True, 'torrent_id': '1',
                                 'service': 'realdebrid', 'error': ''}
        status, _ = _post_json(
            status_server + '/api/search/add',
            {'info_hash': 'a' * 40, 'title': 'T'},
        )
        assert status == 200
        _, kwargs = mock_add.call_args
        assert kwargs['service'] is None

    @patch('utils.search.add_to_debrid')
    def test_oversized_service_rejected(self, mock_add, status_server):
        """A giant service string is capped before validation, so the 400
        error body can't echo an attacker-sized payload back."""
        status, data = _post_json(
            status_server + '/api/search/add',
            {'info_hash': 'a' * 40, 'title': 'T', 'service': 'x' * 10000},
        )
        assert status == 400
        assert len(data['error']) < 200
        mock_add.assert_not_called()

    @patch('utils.search.add_to_debrid')
    def test_malformed_info_hash_rejected(self, mock_add, status_server):
        """Non-hex / wrong-length hashes are rejected at the endpoint —
        never truncated, which could silently target a different valid
        hash."""
        for bad in ('a' * 39, 'a' * 41, 'g' * 40, 'a' * 1000):
            status, data = _post_json(
                status_server + '/api/search/add',
                {'info_hash': bad, 'title': 'T'},
            )
            assert status == 400
            assert 'info_hash' in data['error']
        mock_add.assert_not_called()
