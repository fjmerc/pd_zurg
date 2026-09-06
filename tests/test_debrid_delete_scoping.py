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

    def test_bare_season_range_two_digit(self):
        """Fix-round-1 CRITICAL 1: 'S01-03' (no second S) must be a season
        range, NEVER an episode claim of E03."""
        seasons, eps = _torrent_episode_claim('The.Wire.S01-03.1080p')
        assert seasons >= {1, 2, 3}
        assert eps == set()

    def test_bare_season_range_single_digit(self):
        seasons, eps = _torrent_episode_claim('Show.S1-3.1080p')
        assert seasons >= {1, 2, 3}
        assert eps == set()

    def test_cross_season_episode_span_covers_both_seasons(self):
        """Fix-round-1 CRITICAL 2: a cross-season episode range must
        season-wide-block every season it touches, since the middle
        episodes (S01E21-24, S02E01-04) are never named explicitly."""
        seasons, eps = _torrent_episode_claim('Show.S01E20-S02E05.mkv')
        assert 1 in seasons and 2 in seasons
        assert eps == {(1, 20), (2, 5)}

    def test_year_titled_show_still_parses_episode(self):
        _, eps = _torrent_episode_claim('1923.S01E06.mkv')
        assert eps == {(1, 6)}


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

    def test_bare_season_range_pack_kept_when_spanned_season_unsafe(self):
        """Fix-round-1 CRITICAL 1 regression: an 'S01-03' bare-range pack
        must be kept when a later season in the range has an unsafe
        episode — not treated as an (incorrect) E03 episode claim."""
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show.S01-03.1080p')], unsafe_episodes={(3, 5)})
        assert deletable == [] and len(kept) == 1

    def test_cross_season_episode_span_blocks_unnamed_middle_episodes(self):
        """Fix-round-1 CRITICAL 2 regression: neither an unsafe episode in
        the first season's unnamed tail nor the second season's unnamed
        head may be deleted alongside a 'S01E20-S02E05' release."""
        deletable1, kept1 = filter_safe_torrent_deletes(
            [_m('Show.S01E20-S02E05.mkv')], unsafe_episodes={(1, 22)})
        assert deletable1 == [] and len(kept1) == 1

        deletable2, kept2 = filter_safe_torrent_deletes(
            [_m('Show.S01E20-S02E05.mkv')], unsafe_episodes={(2, 3)})
        assert deletable2 == [] and len(kept2) == 1

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


def test_confirm_endpoint_uses_provider_error_reason_on_outage(
        status_server_harness, monkeypatch):
    """Fix-round-1 IMPORTANT 5: when the confirm-time fresh re-listing hits
    a provider outage, a requested item on that provider must be refused
    with a reason naming the outage — not the generic 'not found in a
    fresh provider listing' reason, which would misleadingly suggest the
    torrent no longer exists rather than 'try again once RD is back'."""
    class _FakeScanner:
        def aliases_for(self, norm):
            return set()

        def debrid_only_episodes(self, norm):
            return set()

    monkeypatch.setattr('utils.library.get_scanner', lambda: _FakeScanner())
    monkeypatch.setattr(
        'utils.debrid_client.find_torrents_by_title_multi',
        lambda norms, target_year=None: ([], {'realdebrid': 'RD API down'}))

    status, body = status_server_harness.post_json(
        '/api/library/remove-debrid/confirm',
        {'items': [{'id': '1', 'service': 'realdebrid'}],
         'title': 'Some Show', 'type': 'show'})
    assert status == 200
    assert body.get('status') == 'skipped'
    skipped = body.get('skipped', [])
    assert len(skipped) == 1
    assert 'could not be queried' in skipped[0]['reason']
    assert 'not found' not in skipped[0]['reason']
