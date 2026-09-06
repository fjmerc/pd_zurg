"""Scoped debrid deletion (audit finding #2, CRITICAL data loss).

The TV 'mixed' prefer-local toggle deleted EVERY torrent matching
title+year — including sole copies of debrid-only episodes — seconds
after triggering an async Sonarr search that takes minutes to hours.
Torrents must only be deleted when every episode they claim has a local
copy; unparseable claims fail closed (kept)."""
import json
import threading

import pytest

from utils.debrid_client import filter_safe_torrent_deletes, _torrent_episode_claim


def _m(filename, tid='1'):
    return {'id': tid, 'filename': filename, 'hash': '', 'parsed_title': '',
            'year': None, 'service': 'realdebrid'}


class TestTorrentEpisodeClaim:
    def test_single_episode(self):
        seasons, eps = _torrent_episode_claim('Show.S01E06.1080p.WEB.mkv')
        assert eps == {(1, 6)}

    def test_multi_episode(self):
        _, eps = _torrent_episode_claim('Show.S01E04E05.mkv')
        assert eps == {(1, 4), (1, 5)}

    def test_episode_range(self):
        _, eps = _torrent_episode_claim('Show.S01E04-E06.mkv')
        assert eps == {(1, 4), (1, 5), (1, 6)}

    def test_season_pack(self):
        seasons, eps = _torrent_episode_claim('Show.S01.1080p.Complete')
        assert seasons == {1}
        assert eps == set()

    def test_season_word(self):
        seasons, _ = _torrent_episode_claim('Show Season 2 720p')
        assert seasons == {2}

    def test_season_range(self):
        seasons, _ = _torrent_episode_claim('Show.S01-S03.1080p')
        assert seasons == {1, 2, 3}

    def test_unparseable_is_whole_show(self):
        seasons, eps = _torrent_episode_claim('Show Complete Collection 1080p')
        assert seasons == set() and eps == set()


class TestFilterSafeTorrentDeletes:
    def test_duplicate_episode_deletable(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show.S01E02.mkv')], unsafe_episodes={(1, 6), (1, 7)})
        assert len(deletable) == 1 and kept == []

    def test_sole_copy_episode_kept(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show.S01E06.mkv')], unsafe_episodes={(1, 6)})
        assert deletable == [] and len(kept) == 1
        assert 'S01E06' in kept[0]['kept_reason']

    def test_season_pack_spanning_unsafe_kept(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show.S01.Complete.mkv')], unsafe_episodes={(1, 8)})
        assert deletable == [] and len(kept) == 1

    def test_season_pack_of_safe_season_deletable(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show.S02.Complete.mkv')], unsafe_episodes={(1, 8)})
        assert len(deletable) == 1 and kept == []

    def test_unparseable_kept_when_any_unsafe(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show Complete Collection')], unsafe_episodes={(3, 1)})
        assert deletable == [] and len(kept) == 1

    def test_unparseable_deletable_when_no_unsafe(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show Complete Collection')], unsafe_episodes=set())
        assert len(deletable) == 1 and kept == []

    def test_audit_regression_scenario(self):
        """S1E1-5 both-source, S1E6-8 debrid-only: nothing backing E6-8
        may be deleted."""
        matches = [
            _m('Show.S01E01.mkv', '1'), _m('Show.S01E02.mkv', '2'),
            _m('Show.S01E06.mkv', '6'), _m('Show.S01E07E08.mkv', '7'),
            _m('Show.S01.Pack.mkv', 'p'),
        ]
        unsafe = {(1, 6), (1, 7), (1, 8)}
        deletable, kept = filter_safe_torrent_deletes(matches, unsafe)
        assert {m['id'] for m in deletable} == {'1', '2'}
        assert {m['id'] for m in kept} == {'6', '7', 'p'}


def test_debrid_only_episodes_from_indexes():
    from utils.library import LibraryScanner
    scanner = LibraryScanner.__new__(LibraryScanner)  # no full init
    scanner._path_lock = threading.Lock()
    scanner._alias_norms = {}
    scanner._path_index = {
        ('myshow', 1, 1): '/d/e1', ('myshow', 1, 6): '/d/e6',
        ('myshow (2007)', 2, 1): '/d/s2e1',
        ('othershow', 1, 1): '/d/o',
    }
    scanner._local_path_index = {('myshow', 1, 1): '/l/e1'}
    assert scanner.debrid_only_episodes('myshow') == {(1, 6), (2, 1)}


# --- Endpoint contract test (step 8) ------------------------------------
# Pattern copied from tests/test_compromise_observability.py:132-144.

@pytest.fixture
def status_server_harness(clean_env):
    import base64
    from http.server import ThreadingHTTPServer
    import urllib.request
    import urllib.error
    from utils import status_server as status_server_module

    StatusHandler = status_server_module.StatusHandler
    prior_auth = StatusHandler.auth_credentials
    prior_status_data_ref = StatusHandler.status_data_ref
    # do_POST rejects every POST with 403 unless auth_credentials is set
    # (see status_server.py:1846-1851) — supply real creds and send them.
    creds = 'testuser:testpass'
    StatusHandler.auth_credentials = creds
    StatusHandler.status_data_ref = None
    basic_auth = base64.b64encode(creds.encode()).decode()

    httpd = ThreadingHTTPServer(('127.0.0.1', 0), StatusHandler)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    class Harness:
        def post_json(self, path, payload):
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f'http://127.0.0.1:{port}{path}', data=data,
                headers={'Content-Type': 'application/json',
                         'Authorization': f'Basic {basic_auth}'}, method='POST')
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return resp.status, json.loads(resp.read().decode('utf-8'))
            except urllib.error.HTTPError as e:
                return e.code, json.loads(e.read().decode('utf-8'))

    try:
        yield Harness()
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
        StatusHandler.auth_credentials = prior_auth
        StatusHandler.status_data_ref = prior_status_data_ref


def test_confirm_endpoint_requires_type(status_server_harness):
    """POST /api/library/remove-debrid/confirm without type -> 400."""
    status, body = status_server_harness.post_json(
        '/api/library/remove-debrid/confirm',
        {'items': [{'id': '1', 'service': 'realdebrid'}]})
    assert status == 400
    assert 'type' in body.get('error', '')
