"""Tests for the Arr API client module (utils/arr_client.py)."""

import json
import os
import urllib.error
import pytest
from unittest.mock import patch, MagicMock

from utils.arr_client import (
    SonarrClient, RadarrClient, OverseerrClient,
    get_download_service, get_configured_services,
    _NOT_FOUND,
    _force_grab_eligible, _force_grab_sort_key, _release_identifier,
    _PROFILE_CACHE_TTL_SECONDS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sonarr():
    return SonarrClient('http://sonarr:8989', 'test-key')


@pytest.fixture
def radarr():
    return RadarrClient('http://radarr:7878', 'test-key')


@pytest.fixture
def overseerr():
    return OverseerrClient('http://overseerr:5055', 'test-key')


def _mock_urlopen(response_data, status=200):
    """Create a mock for urllib.request.urlopen that returns JSON data."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(response_data).encode('utf-8')
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


# ---------------------------------------------------------------------------
# Force-grab helpers (_force_grab_eligible, _force_grab_sort_key,
# _release_identifier)
# ---------------------------------------------------------------------------

class TestForceGrabEligible:
    def test_non_rejected_is_eligible(self):
        assert _force_grab_eligible({'rejected': False}) is True
        assert _force_grab_eligible({}) is True  # rejected key absent

    def test_cutoff_only_rejection_is_eligible(self):
        r = {'rejected': True,
             'rejections': ['Existing file on disk meets quality cutoff: WEBDL-1080p']}
        assert _force_grab_eligible(r) is True

    def test_preference_only_rejection_is_eligible(self):
        r = {'rejected': True,
             'rejections': ['Existing file on disk is of equal or higher preference: WEBDL-1080p v1']}
        assert _force_grab_eligible(r) is True

    def test_profile_violation_is_ineligible(self):
        r = {'rejected': True,
             'rejections': ['Remux-2160p is not wanted in profile',
                            'Existing file on disk meets quality cutoff: WEBDL-1080p']}
        assert _force_grab_eligible(r) is False

    def test_empty_rejections_list_is_ineligible(self, caplog):
        """rejected=True with no reasons is a malformed response; log + reject."""
        import logging
        with caplog.at_level(logging.WARNING):
            assert _force_grab_eligible({'rejected': True, 'rejections': []}) is False
        assert any('no rejection reasons' in rec.message for rec in caplog.records)

    def test_missing_rejections_key_is_ineligible(self):
        assert _force_grab_eligible({'rejected': True}) is False

    def test_non_string_rejection_entry_does_not_crash(self):
        """Regression: earlier version crashed with AttributeError on None."""
        r = {'rejected': True,
             'rejections': [None, 'Existing file on disk meets quality cutoff: WEBDL-1080p']}
        assert _force_grab_eligible(r) is False  # None entry fails isinstance check

    def test_dict_rejection_entry_does_not_crash(self):
        r = {'rejected': True,
             'rejections': [{'reason': 'x'}, 'meets quality cutoff']}
        assert _force_grab_eligible(r) is False


class TestForceGrabSortKey:
    def test_numeric_score_and_seeders(self):
        assert _force_grab_sort_key({'customFormatScore': 3400, 'seeders': 500}) == (3400, 500)

    def test_zero_score_sorts_above_negative(self):
        assert _force_grab_sort_key({'customFormatScore': 0, 'seeders': 0}) > \
               _force_grab_sort_key({'customFormatScore': -50, 'seeders': 9999})

    def test_missing_score_sorts_below_negative(self):
        negative = _force_grab_sort_key({'customFormatScore': -50, 'seeders': 10})
        missing = _force_grab_sort_key({'seeders': 500})
        assert negative > missing

    def test_string_score_demoted(self):
        """Stringified score from a buggy proxy is treated as unknown."""
        assert _force_grab_sort_key({'customFormatScore': '3400', 'seeders': 5})[0] == float('-inf')

    def test_bool_score_demoted(self):
        """True/False must not sneak past isinstance int."""
        assert _force_grab_sort_key({'customFormatScore': True, 'seeders': 0})[0] == float('-inf')

    def test_float_seeders_accepted(self):
        """Some proxies emit seeders as float; don't demote them."""
        assert _force_grab_sort_key({'customFormatScore': 0, 'seeders': 500.0})[1] == 500.0


class TestReleaseIdentifier:
    def test_prefers_guid(self):
        assert _release_identifier({'guid': 'g', 'infoHash': 'h', 'title': 't'}) == 'g'

    def test_falls_back_to_infohash(self):
        assert _release_identifier({'guid': '', 'infoHash': 'h', 'title': 't'}) == 'h'

    def test_falls_back_to_title(self):
        assert _release_identifier({'title': 't'}) == 't'

    def test_returns_none_when_empty(self):
        assert _release_identifier({'guid': '', 'infoHash': '', 'title': ''}) is None
        assert _release_identifier({}) is None


# ---------------------------------------------------------------------------
# Configuration & routing
# ---------------------------------------------------------------------------

class TestConfiguration:

    def test_unconfigured_client(self):
        client = SonarrClient('', '')
        assert not client.configured

    def test_configured_client(self, sonarr):
        assert sonarr.configured

    def test_missing_url(self):
        client = SonarrClient('', 'key')
        assert not client.configured

    def test_missing_key(self):
        client = SonarrClient('http://localhost', '')
        assert not client.configured

    def test_env_fallback(self, monkeypatch):
        monkeypatch.setenv('SONARR_URL', 'http://env-sonarr:8989')
        monkeypatch.setenv('SONARR_API_KEY', 'env-key')
        client = SonarrClient()
        assert client.configured

    def test_overseerr_uses_seerr_env(self, monkeypatch):
        monkeypatch.setenv('SEERR_ADDRESS', 'http://overseerr:5055')
        monkeypatch.setenv('SEERR_API_KEY', 'seerr-key')
        client = OverseerrClient()
        assert client.configured


class TestServiceRouting:

    def test_nothing_configured(self, monkeypatch):
        for var in ('SONARR_URL', 'SONARR_API_KEY', 'RADARR_URL',
                    'RADARR_API_KEY', 'SEERR_ADDRESS', 'SEERR_API_KEY'):
            monkeypatch.delenv(var, raising=False)
        client, name = get_download_service('show')
        assert client is None
        assert name is None

    def test_sonarr_priority_over_overseerr(self, monkeypatch):
        monkeypatch.setenv('SEERR_ADDRESS', 'http://overseerr:5055')
        monkeypatch.setenv('SEERR_API_KEY', 'key')
        monkeypatch.setenv('SONARR_URL', 'http://sonarr:8989')
        monkeypatch.setenv('SONARR_API_KEY', 'key')
        client, name = get_download_service('show')
        assert name == 'sonarr'

    def test_radarr_priority_over_overseerr(self, monkeypatch):
        monkeypatch.setenv('SEERR_ADDRESS', 'http://overseerr:5055')
        monkeypatch.setenv('SEERR_API_KEY', 'key')
        monkeypatch.setenv('RADARR_URL', 'http://radarr:7878')
        monkeypatch.setenv('RADARR_API_KEY', 'key')
        client, name = get_download_service('movie')
        assert name == 'radarr'

    def test_overseerr_fallback_when_no_sonarr(self, monkeypatch):
        monkeypatch.delenv('SONARR_URL', raising=False)
        monkeypatch.delenv('SONARR_API_KEY', raising=False)
        monkeypatch.setenv('SEERR_ADDRESS', 'http://overseerr:5055')
        monkeypatch.setenv('SEERR_API_KEY', 'key')
        client, name = get_download_service('show')
        assert name == 'overseerr'

    def test_overseerr_fallback_when_no_radarr(self, monkeypatch):
        monkeypatch.delenv('RADARR_URL', raising=False)
        monkeypatch.delenv('RADARR_API_KEY', raising=False)
        monkeypatch.setenv('SEERR_ADDRESS', 'http://overseerr:5055')
        monkeypatch.setenv('SEERR_API_KEY', 'key')
        client, name = get_download_service('movie')
        assert name == 'overseerr'

    def test_get_configured_services_overseerr_only(self, monkeypatch):
        monkeypatch.setenv('SEERR_ADDRESS', 'http://overseerr:5055')
        monkeypatch.setenv('SEERR_API_KEY', 'key')
        monkeypatch.delenv('SONARR_URL', raising=False)
        monkeypatch.delenv('RADARR_URL', raising=False)
        result = get_configured_services()
        assert result == {'show': 'overseerr', 'movie': 'overseerr'}

    def test_mixed_services(self, monkeypatch):
        monkeypatch.delenv('SEERR_ADDRESS', raising=False)
        monkeypatch.delenv('SEERR_API_KEY', raising=False)
        monkeypatch.setenv('SONARR_URL', 'http://sonarr:8989')
        monkeypatch.setenv('SONARR_API_KEY', 'key')
        monkeypatch.delenv('RADARR_URL', raising=False)
        monkeypatch.delenv('RADARR_API_KEY', raising=False)
        result = get_configured_services()
        assert result == {'show': 'sonarr', 'movie': None}


# ---------------------------------------------------------------------------
# Sonarr client
# ---------------------------------------------------------------------------

class TestSonarrClient:

    @patch('urllib.request.urlopen')
    def test_lookup_by_title(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen([{'title': 'Breaking Bad', 'tvdbId': 81189}])
        result = sonarr.lookup_series(title='Breaking Bad')
        assert result['title'] == 'Breaking Bad'

    @patch('urllib.request.urlopen')
    def test_lookup_by_tmdb_id(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen([{'title': 'Breaking Bad', 'tmdbId': 1396}])
        result = sonarr.lookup_series(tmdb_id=1396)
        assert result['tmdbId'] == 1396

    @patch('urllib.request.urlopen')
    def test_lookup_empty_result(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen([])
        result = sonarr.lookup_series(title='Nonexistent')
        assert result is None

    def test_lookup_no_args(self, sonarr):
        assert sonarr.lookup_series() is None

    @patch('urllib.request.urlopen')
    def test_find_series_in_library_by_tmdb(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen([
            {'id': 1, 'title': 'Show A', 'tmdbId': 100},
            {'id': 2, 'title': 'Show B', 'tmdbId': 200},
        ])
        result = sonarr.find_series_in_library(tmdb_id=200)
        assert result['id'] == 2

    @patch('urllib.request.urlopen')
    def test_find_series_in_library_not_found(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen([])
        result = sonarr.find_series_in_library(tmdb_id=999)
        assert result is None

    @patch('urllib.request.urlopen')
    def test_search_episodes(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen({'id': 42})
        result = sonarr.search_episodes([10, 11, 12])
        assert result['id'] == 42

    def test_search_episodes_empty(self, sonarr):
        assert sonarr.search_episodes([]) is None

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_existing_series(self, mock_urlopen, sonarr):
        # get_all_series, get_episodes, queue cleanup, search_episodes
        responses = [
            _mock_urlopen([{'id': 5, 'title': 'My Show', 'tmdbId': 123}]),
            _mock_urlopen([
                {'id': 100, 'seasonNumber': 1, 'episodeNumber': 1},
                {'id': 101, 'seasonNumber': 1, 'episodeNumber': 2},
                {'id': 102, 'seasonNumber': 1, 'episodeNumber': 3},
            ]),
            _mock_urlopen({'records': []}),  # queue cleanup
            _mock_urlopen({'id': 42}),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr.ensure_and_search('My Show', 123, 1, [1, 3])
        assert result['status'] == 'sent'
        assert result['service'] == 'sonarr'
        assert '2 episode(s)' in result['message']

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_not_found(self, mock_urlopen, sonarr):
        # get_all_series empty, lookup empty
        responses = [
            _mock_urlopen([]),
            _mock_urlopen([]),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr.ensure_and_search('Missing Show', None, 1, [1])
        assert result['status'] == 'error'

    @patch('urllib.request.urlopen')
    def test_http_error_returns_none(self, mock_urlopen, sonarr):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            'http://sonarr:8989/api/v3/series', 500, 'Server Error', {}, None
        )
        result = sonarr.lookup_series(title='Test')
        assert result is None

    @patch('urllib.request.urlopen')
    def test_connection_error_returns_none(self, mock_urlopen, sonarr):
        mock_urlopen.side_effect = urllib.error.URLError('Connection refused')
        result = sonarr.lookup_series(title='Test')
        assert result is None

    def test_unconfigured_returns_none(self):
        client = SonarrClient('', '')
        assert client.lookup_series(title='Test') is None

    @patch('urllib.request.urlopen')
    def test_remove_episodes(self, mock_urlopen, sonarr):
        # get_all_series, get_episodes, delete file1, delete file2
        responses = [
            _mock_urlopen([{'id': 5, 'title': 'My Show', 'tmdbId': 123}]),
            _mock_urlopen([
                {'id': 100, 'seasonNumber': 1, 'episodeNumber': 1, 'hasFile': True, 'episodeFileId': 50},
                {'id': 101, 'seasonNumber': 1, 'episodeNumber': 2, 'hasFile': True, 'episodeFileId': 51},
                {'id': 102, 'seasonNumber': 1, 'episodeNumber': 3, 'hasFile': False, 'episodeFileId': 0},
            ]),
            _mock_urlopen({}),  # delete file 50
            _mock_urlopen({}),  # delete file 51
        ]
        mock_urlopen.side_effect = responses
        result = sonarr.remove_episodes('My Show', 123, 1, [1, 2, 3])
        assert result['status'] == 'removed'
        assert result['removed'] == 2

    @patch('urllib.request.urlopen')
    def test_remove_episodes_not_found(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen([])
        result = sonarr.remove_episodes('Missing', None, 1, [1])
        assert result['status'] == 'error'

    @patch('urllib.request.urlopen')
    def test_remove_episodes_no_files(self, mock_urlopen, sonarr):
        responses = [
            _mock_urlopen([{'id': 5, 'title': 'My Show', 'tmdbId': 123}]),
            _mock_urlopen([
                {'id': 100, 'seasonNumber': 1, 'episodeNumber': 1, 'hasFile': False, 'episodeFileId': 0},
            ]),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr.remove_episodes('My Show', 123, 1, [1])
        assert result['status'] == 'error'
        assert 'No files' in result['message']

    @patch('urllib.request.urlopen')
    def test_get_blackhole_tag_id_found(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen([
            {'implementation': 'QBittorrent', 'enable': True, 'tags': []},
            {'implementation': 'TorrentBlackhole', 'enable': True, 'tags': [7]},
        ])
        assert sonarr._get_blackhole_tag_id() == 7

    @patch('urllib.request.urlopen')
    def test_get_blackhole_tag_id_not_found(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen([
            {'implementation': 'QBittorrent', 'enable': True, 'tags': []},
        ])
        assert sonarr._get_blackhole_tag_id() is None

    @patch('urllib.request.urlopen')
    def test_get_blackhole_tag_id_cached(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen([
            {'implementation': 'TorrentBlackhole', 'enable': True, 'tags': [7]},
        ])
        assert sonarr._get_blackhole_tag_id() == 7
        # Second call should not hit the API
        mock_urlopen.side_effect = Exception('should not be called')
        assert sonarr._get_blackhole_tag_id() == 7

    @patch('urllib.request.urlopen')
    def test_get_blackhole_tag_id_zero(self, mock_urlopen, sonarr):
        """Tag ID 0 should be handled correctly, not treated as falsy."""
        mock_urlopen.return_value = _mock_urlopen([
            {'implementation': 'TorrentBlackhole', 'enable': True, 'tags': [0]},
        ])
        assert sonarr._get_blackhole_tag_id() == 0

    @patch('urllib.request.urlopen')
    def test_ensure_debrid_routing_adds_tag(self, mock_urlopen, sonarr):
        sonarr._blackhole_tag_id = 7
        series = {'id': 5, 'title': 'My Show', 'tags': []}
        mock_urlopen.return_value = _mock_urlopen(dict(series, tags=[7]))
        result = sonarr._ensure_debrid_routing(series)
        assert 7 in result['tags']

    @patch('urllib.request.urlopen')
    def test_ensure_debrid_routing_already_tagged(self, mock_urlopen, sonarr):
        sonarr._blackhole_tag_id = 7
        series = {'id': 5, 'title': 'My Show', 'tags': [7]}
        result = sonarr._ensure_debrid_routing(series)
        assert result is series  # no API call needed
        mock_urlopen.assert_not_called()

    @patch('urllib.request.urlopen')
    def test_ensure_local_routing_removes_tag(self, mock_urlopen, sonarr):
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = 8
        series = {'id': 5, 'title': 'My Show', 'tags': [7]}
        mock_urlopen.return_value = _mock_urlopen(dict(series, tags=[8]))
        result = sonarr._ensure_local_routing(series)
        assert 7 not in result['tags']
        assert 8 in result['tags']

    def test_ensure_local_routing_noop_when_no_local_tag(self, sonarr):
        """When no local tag exists, don't remove debrid tag (would leave series unroutable)."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = _NOT_FOUND
        series = {'id': 5, 'title': 'My Show', 'tags': [7]}
        result = sonarr._ensure_local_routing(series)
        assert result is series  # unchanged, no PUT

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_with_prefer_debrid(self, mock_urlopen, sonarr):
        """prefer_debrid=True should add the blackhole tag before searching."""
        sonarr._blackhole_tag_id = 7
        responses = [
            _mock_urlopen([{'id': 5, 'title': 'My Show', 'tmdbId': 123, 'tags': []}]),
            _mock_urlopen({'id': 5, 'title': 'My Show', 'tmdbId': 123, 'tags': [7]}),  # PUT
            _mock_urlopen([
                {'id': 100, 'seasonNumber': 1, 'episodeNumber': 1},
            ]),
            _mock_urlopen({'records': []}),  # queue cleanup
            _mock_urlopen({'id': 42}),  # search
        ]
        mock_urlopen.side_effect = responses
        result = sonarr.ensure_and_search('My Show', 123, 1, [1], prefer_debrid=True)
        assert result['status'] == 'sent'

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_prefer_debrid_force_grabs_when_has_file(self, mock_urlopen, sonarr):
        """prefer_debrid=True with existing files should interactive-grab each, dedup by GUID."""
        sonarr._blackhole_tag_id = 7
        responses = [
            _mock_urlopen([{'id': 5, 'title': 'Tulsa King', 'tmdbId': 200, 'tags': [7]}]),  # find
            _mock_urlopen([  # episodes — both have files
                {'id': 100, 'seasonNumber': 1, 'episodeNumber': 1, 'hasFile': True},
                {'id': 101, 'seasonNumber': 1, 'episodeNumber': 2, 'hasFile': True},
            ]),
            _mock_urlopen({'records': []}),  # queue cleanup
            # Episode 100: unique release → push
            _mock_urlopen([
                {'guid': 'ep1-torrent', 'indexerId': 2, 'protocol': 'torrent', 'title': 'S01E01', 'seasonNumber': 1},
            ]),
            _mock_urlopen({'id': 99}),  # push ep1
            # Episode 101: different release → push
            _mock_urlopen([
                {'guid': 'ep2-torrent', 'indexerId': 2, 'protocol': 'torrent', 'title': 'S01E02', 'seasonNumber': 1},
            ]),
            _mock_urlopen({'id': 98}),  # push ep2
        ]
        mock_urlopen.side_effect = responses
        result = sonarr.ensure_and_search('Tulsa King', 200, 1, [1, 2], prefer_debrid=True)
        assert result['status'] == 'sent'
        assert 'Force-grabbed 2' in result['message']

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_prefer_debrid_tries_next_on_failure(self, mock_urlopen, sonarr):
        """If first episode grab fails, try next episode."""
        sonarr._blackhole_tag_id = 7
        responses = [
            _mock_urlopen([{'id': 5, 'title': 'Show', 'tmdbId': 200, 'tags': [7]}]),
            _mock_urlopen([
                {'id': 100, 'seasonNumber': 1, 'episodeNumber': 1, 'hasFile': True},
                {'id': 101, 'seasonNumber': 1, 'episodeNumber': 2, 'hasFile': True},
            ]),
            _mock_urlopen({'records': []}),  # queue cleanup
            _mock_urlopen([]),  # ep 100: no releases
            _mock_urlopen([  # ep 101: has accepted torrent
                {'guid': 'found', 'indexerId': 2, 'protocol': 'torrent', 'title': 'S01E02', 'rejected': False, 'seasonNumber': 1},
            ]),
            _mock_urlopen({'id': 99}),  # push
        ]
        mock_urlopen.side_effect = responses
        result = sonarr.ensure_and_search('Show', 200, 1, [1, 2], prefer_debrid=True)
        assert result['status'] == 'sent'
        assert 'Force-grabbed' in result['message']

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_prefer_debrid_mixed_has_file_and_no_file(self, mock_urlopen, sonarr):
        """Mixed: interactive grab for has-file episodes, normal search for no-file."""
        sonarr._blackhole_tag_id = 7
        responses = [
            _mock_urlopen([{'id': 5, 'title': 'Show', 'tmdbId': 200, 'tags': [7]}]),  # find
            _mock_urlopen([  # episodes: ep1 has file, ep2+3 don't
                {'id': 100, 'seasonNumber': 1, 'episodeNumber': 1, 'hasFile': True},
                {'id': 101, 'seasonNumber': 1, 'episodeNumber': 2, 'hasFile': False},
                {'id': 102, 'seasonNumber': 1, 'episodeNumber': 3, 'hasFile': False},
            ]),
            _mock_urlopen({'records': []}),  # queue cleanup
            _mock_urlopen([  # interactive search for ep1
                {'guid': 'pack', 'indexerId': 2, 'protocol': 'torrent', 'title': 'Season.Pack', 'rejected': False, 'seasonNumber': 1},
            ]),
            _mock_urlopen({'id': 99}),  # push release
            _mock_urlopen({'id': 43}),  # search_episodes for no_file_ids [101, 102]
        ]
        mock_urlopen.side_effect = responses
        result = sonarr.ensure_and_search('Show', 200, 1, [1, 2, 3], prefer_debrid=True)
        assert result['status'] == 'sent'
        assert 'Force-grabbed' in result['message']
        # Verify no_file episodes were searched via normal EpisodeSearch
        search_call = mock_urlopen.call_args_list[-1]
        search_body = json.loads(search_call[0][0].data)
        assert search_body['name'] == 'EpisodeSearch'
        assert set(search_body['episodeIds']) == {101, 102}

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_prefer_debrid_falls_through_when_no_torrents(self, mock_urlopen, sonarr):
        """When interactive grab finds no torrents, fall through to normal search."""
        sonarr._blackhole_tag_id = 7
        responses = [
            _mock_urlopen([{'id': 5, 'title': 'My Show', 'tmdbId': 123, 'tags': [7]}]),  # find
            _mock_urlopen([  # episodes with files
                {'id': 100, 'seasonNumber': 1, 'episodeNumber': 1, 'hasFile': True},
            ]),
            _mock_urlopen({'records': []}),  # queue cleanup
            _mock_urlopen([  # interactive search — only usenet, no torrents
                {'guid': 'abc', 'indexerId': 1, 'protocol': 'usenet', 'title': 'NZB', 'rejected': False},
            ]),
            _mock_urlopen({'id': 42}),  # fallback: normal EpisodeSearch
        ]
        mock_urlopen.side_effect = responses
        result = sonarr.ensure_and_search('My Show', 123, 1, [1], prefer_debrid=True)
        assert result['status'] == 'sent'
        # Should have fallen through to normal search (message won't say Force-grabbed)
        assert 'Force-grabbed' not in result['message']

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_skips_rejected(self, mock_urlopen, sonarr):
        """Rejected releases are excluded — profile rules are respected."""
        responses = [
            _mock_urlopen([
                {'guid': 'bad', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'Remux-2160p', 'rejected': True, 'customFormatScore': -10000},
                {'guid': 'good', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'WEBDL-1080p', 'rejected': False, 'customFormatScore': 3400},
            ]),
            _mock_urlopen({'id': 1}),  # push
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(100, title='Test')
        assert result == 'good'
        push_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert push_body['guid'] == 'good'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_all_rejected_returns_none(self, mock_urlopen, sonarr):
        """When every candidate is rejected, fall through (no push)."""
        responses = [
            _mock_urlopen([
                {'guid': 'r1', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'Remux', 'rejected': True, 'customFormatScore': -10000},
                {'guid': 'r2', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'BR-DISK', 'rejected': True, 'customFormatScore': -20000},
            ]),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(100, title='Test')
        assert result is None
        assert mock_urlopen.call_count == 1  # only GET, no push

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_prefers_higher_custom_format_score(self, mock_urlopen, sonarr):
        """Among accepted releases, highest customFormatScore wins."""
        responses = [
            _mock_urlopen([
                {'guid': 'low', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'HDTV', 'rejected': False, 'customFormatScore': 100},
                {'guid': 'high', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'WEBDL-1080p', 'rejected': False, 'customFormatScore': 3400},
                {'guid': 'mid', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'WEBRip', 'rejected': False, 'customFormatScore': 1600},
            ]),
            _mock_urlopen({'id': 1}),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(100, title='Test')
        assert result == 'high'
        push_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert push_body['guid'] == 'high'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_tie_breaks_on_seeders(self, mock_urlopen, sonarr):
        """Same customFormatScore → higher seeders wins."""
        responses = [
            _mock_urlopen([
                {'guid': 'few', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'A', 'rejected': False, 'customFormatScore': 100, 'seeders': 5},
                {'guid': 'many', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'B', 'rejected': False, 'customFormatScore': 100, 'seeders': 200},
            ]),
            _mock_urlopen({'id': 1}),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(100, title='Test')
        assert result == 'many'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_allows_cutoff_only_rejection(self, mock_urlopen, sonarr):
        """A release rejected ONLY for cutoff-met is still eligible — that's
        the force-grab's intended bypass."""
        responses = [
            _mock_urlopen([
                {'guid': 'cutoff-only', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'S01E01 WEBDL-1080p', 'rejected': True,
                 'rejections': ['Existing file on disk meets quality cutoff: WEBDL-1080p'],
                 'customFormatScore': 3400},
            ]),
            _mock_urlopen({'id': 1}),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(100, title='Test')
        assert result == 'cutoff-only'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_excludes_profile_violation_even_with_cutoff(self, mock_urlopen, sonarr):
        """A rejection combining cutoff-met AND a profile violation is NOT eligible."""
        responses = [
            _mock_urlopen([
                {'guid': 'mixed', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'Remux-2160p', 'rejected': True,
                 'rejections': [
                     'Existing file on disk meets quality cutoff: WEBDL-1080p',
                     'Remux-2160p is not wanted in profile',
                 ],
                 'customFormatScore': -10000},
            ]),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(100, title='Test')
        assert result is None
        assert mock_urlopen.call_count == 1  # no push attempted

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_missing_score_sorts_below_negative(self, mock_urlopen, sonarr):
        """Releases with missing customFormatScore must NOT outrank releases with a
        legitimately negative score. Regression guard for the `or 0` bug."""
        responses = [
            _mock_urlopen([
                {'guid': 'no-score', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'A', 'rejected': False, 'seeders': 500},  # score absent
                {'guid': 'negative', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'B', 'rejected': False, 'customFormatScore': -50, 'seeders': 10},
            ]),
            _mock_urlopen({'id': 1}),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(100, title='Test')
        # -50 > -inf, so the negative-score release wins over the unknown one
        assert result == 'negative'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_dedups_by_infohash(self, mock_urlopen, sonarr):
        """Releases already pushed in a prior iteration (identified by infoHash)
        are skipped, even when the GUID differs."""
        seen = {'abc123hash'}
        responses = [
            _mock_urlopen([
                {'guid': 'different-guid', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'Duplicate pack', 'rejected': False,
                 'infoHash': 'abc123hash', 'seasonNumber': 1},
                {'guid': 'new', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'Fresh', 'rejected': False,
                 'infoHash': 'different-hash', 'seasonNumber': 1},
            ]),
            _mock_urlopen({'id': 1}),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(100, season_number=1, title='Test', seen_guids=seen)
        assert result == 'new'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_skips_usenet(self, mock_urlopen, sonarr):
        """Only torrent releases should be grabbed, not usenet."""
        responses = [
            _mock_urlopen([
                {'guid': 'nzb', 'indexerId': 1, 'protocol': 'usenet', 'title': 'NZB'},
            ]),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(100, title='Test')
        assert result is None

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_filters_by_season(self, mock_urlopen, sonarr):
        """Only releases matching the requested season should be grabbed."""
        responses = [
            _mock_urlopen([
                {'guid': 's1', 'indexerId': 1, 'protocol': 'torrent', 'title': 'S01E01', 'seasonNumber': 1},
                {'guid': 's2', 'indexerId': 1, 'protocol': 'torrent', 'title': 'S02E01', 'seasonNumber': 2},
            ]),
            _mock_urlopen({'id': 10}),  # push
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(200, season_number=2, title='Test')
        assert result == 's2'
        push_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert push_body['guid'] == 's2'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_accepts_multiseason_pack(self, mock_urlopen, sonarr):
        """Multi-season packs (seasonNumber=0) should be accepted for any season."""
        responses = [
            _mock_urlopen([
                {'guid': 'pack', 'indexerId': 1, 'protocol': 'torrent', 'title': 'S01-S03 Complete', 'seasonNumber': 0},
            ]),
            _mock_urlopen({'id': 10}),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(200, season_number=2, title='Test')
        assert result == 'pack'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_accepts_missing_season(self, mock_urlopen, sonarr):
        """Releases without seasonNumber should be accepted (untagged indexer results)."""
        responses = [
            _mock_urlopen([
                {'guid': 'unknown', 'indexerId': 1, 'protocol': 'torrent', 'title': 'Tulsa King'},
            ]),
            _mock_urlopen({'id': 10}),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(200, season_number=1, title='Test')
        assert result == 'unknown'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_deduplicates_by_guid(self, mock_urlopen, sonarr):
        """Releases already in seen_guids should be skipped."""
        responses = [
            _mock_urlopen([
                {'guid': 'already-seen', 'indexerId': 1, 'protocol': 'torrent', 'title': 'Pack', 'seasonNumber': 1},
                {'guid': 'new-one', 'indexerId': 1, 'protocol': 'torrent', 'title': 'Ep2', 'seasonNumber': 1},
            ]),
            _mock_urlopen({'id': 10}),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(200, season_number=1, title='Test', seen_guids={'already-seen'})
        assert result == 'new-one'

    # --- _fix_indexer_routing: torrent indexer debrid tag ---

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_adds_debrid_tag_to_local_tagged_torrent(self, mock_urlopen, sonarr):
        """Torrent indexer with only the local tag gets debrid tag added."""
        indexers = [
            {'id': 1, 'name': '1337x', 'protocol': 'torrent', 'tags': [5], 'downloadClientId': 0},
        ]
        mock_urlopen.side_effect = [
            _mock_urlopen(indexers),       # GET /indexer
            _mock_urlopen(indexers[0]),     # PUT /indexer/1
        ]
        result = sonarr._fix_indexer_routing(set(), 5, debrid_tag=3)
        assert result is True
        put_call = mock_urlopen.call_args_list[1]
        put_body = json.loads(put_call[0][0].data)
        assert 3 in put_body['tags']
        assert 5 in put_body['tags']

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_warns_custom_tagged_torrent(self, mock_urlopen, sonarr):
        """Torrent indexer with custom tags (not just local) is warned, not modified."""
        indexers = [
            {'id': 1, 'name': '1337x', 'protocol': 'torrent', 'tags': [99], 'downloadClientId': 0},
        ]
        mock_urlopen.return_value = _mock_urlopen(indexers)
        result = sonarr._fix_indexer_routing(set(), 5, debrid_tag=3)
        assert result is False
        assert mock_urlopen.call_count == 1  # only GET, no PUT

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_tags_untagged_torrent(self, mock_urlopen, sonarr):
        """Untagged torrent indexer gets debrid tag (Sonarr v4 requires shared tags)."""
        indexers = [
            {'id': 1, 'name': 'TPB', 'protocol': 'torrent', 'tags': [], 'downloadClientId': 0},
        ]
        mock_urlopen.side_effect = [_mock_urlopen(indexers), _mock_urlopen(indexers[0])]
        result = sonarr._fix_indexer_routing(set(), None, debrid_tag=3)
        assert result is True
        assert mock_urlopen.call_count == 2  # GET + PUT
        put_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert 3 in put_body['tags']

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_tags_untagged_torrent_with_local(self, mock_urlopen, sonarr):
        """Untagged torrent indexer gets both debrid and local tags for dual routing."""
        indexers = [
            {'id': 1, 'name': 'TPB', 'protocol': 'torrent', 'tags': [], 'downloadClientId': 0},
        ]
        mock_urlopen.side_effect = [_mock_urlopen(indexers), _mock_urlopen(indexers[0])]
        result = sonarr._fix_indexer_routing(set(), 5, debrid_tag=3)
        assert result is True
        put_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert 3 in put_body['tags']
        assert 5 in put_body['tags']

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_skips_torrent_already_tagged(self, mock_urlopen, sonarr):
        """Torrent indexer already carrying debrid tag should not be re-written."""
        indexers = [
            {'id': 1, 'name': 'YTS', 'protocol': 'torrent', 'tags': [5, 3], 'downloadClientId': 0},
        ]
        mock_urlopen.return_value = _mock_urlopen(indexers)
        result = sonarr._fix_indexer_routing(set(), None, debrid_tag=3)
        assert result is False
        assert mock_urlopen.call_count == 1  # only GET, no PUT

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_no_debrid_tag_skips_torrent(self, mock_urlopen, sonarr):
        """When debrid_tag is None, torrent indexers are not touched."""
        indexers = [
            {'id': 1, 'name': '1337x', 'protocol': 'torrent', 'tags': [5], 'downloadClientId': 0},
        ]
        mock_urlopen.return_value = _mock_urlopen(indexers)
        result = sonarr._fix_indexer_routing(set(), None, debrid_tag=None)
        assert result is False
        assert mock_urlopen.call_count == 1  # only GET, no PUT

    # --- _search_debrid_missing ---

    @patch('urllib.request.urlopen')
    def test_search_debrid_missing_triggers_search(self, mock_urlopen, sonarr):
        """After indexer fix, debrid-tagged series with missing episodes get searched."""
        sonarr._blackhole_tag_id = 3
        series = [
            {'id': 10, 'tags': [3], 'monitored': True, 'statistics': {'episodeCount': 10, 'episodeFileCount': 5}},
            {'id': 20, 'tags': [5], 'monitored': True, 'statistics': {'episodeCount': 8, 'episodeFileCount': 2}},
            {'id': 30, 'tags': [3], 'monitored': True, 'statistics': {'episodeCount': 6, 'episodeFileCount': 6}},
        ]
        mock_urlopen.side_effect = [
            _mock_urlopen(series),      # GET /series
            _mock_urlopen({}),          # POST /command (series 10)
        ]
        sonarr._search_debrid_missing()
        assert mock_urlopen.call_count == 2  # GET + 1 POST (only series 10 is debrid+missing)
        post_body = json.loads(mock_urlopen.call_args_list[1][0][0].data)
        assert post_body['name'] == 'SeriesSearch'
        assert post_body['seriesId'] == 10

    @patch('urllib.request.urlopen')
    def test_search_debrid_missing_noop_when_none_missing(self, mock_urlopen, sonarr):
        """No search triggered when all debrid series are complete."""
        sonarr._blackhole_tag_id = 3
        series = [
            {'id': 10, 'tags': [3], 'monitored': True, 'statistics': {'episodeCount': 6, 'episodeFileCount': 6}},
        ]
        mock_urlopen.return_value = _mock_urlopen(series)
        sonarr._search_debrid_missing()
        assert mock_urlopen.call_count == 1  # only GET, no POST

    # --- Usenet tag routing ---

    @patch('urllib.request.urlopen')
    def test_get_usenet_tag_id_found(self, mock_urlopen, sonarr):
        """Usenet client + blackhole → usenet tag is created and cached."""
        mock_urlopen.side_effect = [
            # GET /downloadclient
            _mock_urlopen([
                {'implementation': 'Nzbget', 'enable': True, 'tags': [8], 'id': 1, 'name': 'NZBget'},
                {'implementation': 'TorrentBlackhole', 'enable': True, 'tags': [7], 'id': 2},
                {'implementation': 'QBittorrent', 'enable': True, 'tags': [8], 'id': 3, 'name': 'qBit'},
            ]),
            # GET /tag (usenet tag doesn't exist yet)
            _mock_urlopen([{'label': 'debrid', 'id': 7}, {'label': 'standard', 'id': 8}]),
            # POST /tag (create usenet)
            _mock_urlopen({'label': 'usenet', 'id': 9}),
            # PUT /downloadclient/1 (add usenet tag to NZBget)
            _mock_urlopen({'id': 1, 'tags': [8, 9]}),
            # GET /indexer
            _mock_urlopen([]),
        ]
        assert sonarr._get_usenet_tag_id() == 9

    @patch('urllib.request.urlopen')
    def test_get_usenet_tag_id_no_usenet_client(self, mock_urlopen, sonarr):
        """No usenet client → usenet tag is None."""
        mock_urlopen.return_value = _mock_urlopen([
            {'implementation': 'QBittorrent', 'enable': True, 'tags': [8], 'id': 1, 'name': 'qBit'},
            {'implementation': 'TorrentBlackhole', 'enable': True, 'tags': [7], 'id': 2},
        ])
        assert sonarr._get_usenet_tag_id() is None

    @patch('urllib.request.urlopen')
    def test_ensure_local_routing_prefers_usenet(self, mock_urlopen, sonarr):
        """When usenet tag exists, _ensure_local_routing applies usenet tag, not local."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = 8
        sonarr._usenet_tag_id = 9
        series = {'id': 5, 'title': 'My Show', 'tags': [7]}
        mock_urlopen.return_value = _mock_urlopen(dict(series, tags=[9]))
        result = sonarr._ensure_local_routing(series)
        assert 9 in result['tags']
        assert 7 not in result['tags']
        assert 8 not in result['tags']

    @patch('urllib.request.urlopen')
    def test_ensure_local_routing_falls_back_to_local(self, mock_urlopen, sonarr):
        """When no usenet tag exists, _ensure_local_routing uses local tag."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = 8
        sonarr._usenet_tag_id = _NOT_FOUND
        series = {'id': 5, 'title': 'My Show', 'tags': [7]}
        mock_urlopen.return_value = _mock_urlopen(dict(series, tags=[8]))
        result = sonarr._ensure_local_routing(series)
        assert 8 in result['tags']
        assert 7 not in result['tags']

    @patch('urllib.request.urlopen')
    def test_ensure_local_routing_removes_stale_local_tag(self, mock_urlopen, sonarr):
        """When switching to usenet, stale local tag is removed from series."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = 8
        sonarr._usenet_tag_id = 9
        series = {'id': 5, 'title': 'My Show', 'tags': [8]}
        mock_urlopen.return_value = _mock_urlopen(dict(series, tags=[9]))
        result = sonarr._ensure_local_routing(series)
        assert 9 in result['tags']
        assert 8 not in result['tags']

    @patch('urllib.request.urlopen')
    def test_ensure_debrid_routing_removes_usenet_tag(self, mock_urlopen, sonarr):
        """Switching to debrid strips usenet tag alongside local tag."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = 8
        sonarr._usenet_tag_id = 9
        series = {'id': 5, 'title': 'My Show', 'tags': [9]}
        mock_urlopen.return_value = _mock_urlopen(dict(series, tags=[7]))
        result = sonarr._ensure_debrid_routing(series)
        assert 7 in result['tags']
        assert 9 not in result['tags']

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_usenet_indexer_gets_both_tags(self, mock_urlopen, sonarr):
        """Usenet indexer gets both local and usenet tags."""
        indexers = [
            {'id': 1, 'name': 'NZBgeek', 'protocol': 'usenet', 'tags': [], 'downloadClientId': 0},
        ]
        mock_urlopen.side_effect = [
            _mock_urlopen(indexers),       # GET /indexer
            _mock_urlopen(indexers[0]),     # PUT /indexer/1
        ]
        sonarr._fix_indexer_routing(set(), 8, debrid_tag=7, usenet_tag=9)
        put_body = json.loads(mock_urlopen.call_args_list[1][0][0].data)
        assert 8 in put_body['tags']
        assert 9 in put_body['tags']

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_usenet_indexer_adds_missing_usenet_tag(self, mock_urlopen, sonarr):
        """Usenet indexer with only local tag gets usenet tag added."""
        indexers = [
            {'id': 1, 'name': 'NZBgeek', 'protocol': 'usenet', 'tags': [8], 'downloadClientId': 0},
        ]
        mock_urlopen.side_effect = [
            _mock_urlopen(indexers),
            _mock_urlopen(indexers[0]),
        ]
        sonarr._fix_indexer_routing(set(), 8, debrid_tag=7, usenet_tag=9)
        put_body = json.loads(mock_urlopen.call_args_list[1][0][0].data)
        assert 8 in put_body['tags']
        assert 9 in put_body['tags']

    # -----------------------------------------------------------------------
    # _audit_untagged_series (self-heal for Overseerr tag=[] misconfig)
    # -----------------------------------------------------------------------

    @patch('urllib.request.urlopen')
    def test_audit_untagged_tags_and_searches(self, mock_urlopen, sonarr):
        """Untagged monitored series gets the debrid tag + SeriesSearch fired."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = _NOT_FOUND
        sonarr._usenet_tag_id = _NOT_FOUND
        series_list = [
            {'id': 1, 'title': 'Paradise', 'tags': [], 'monitored': True},
        ]
        mock_urlopen.side_effect = [
            _mock_urlopen(series_list),                                    # GET /series
            _mock_urlopen(dict(series_list[0], tags=[7])),                 # PUT /series/1
            _mock_urlopen({'id': 42}),                                     # POST /command SeriesSearch
        ]
        sonarr._audit_untagged_series()
        # Verify PUT carried the debrid tag
        put_body = json.loads(mock_urlopen.call_args_list[1][0][0].data)
        assert 7 in put_body['tags']
        # Verify SeriesSearch was called for that series id
        cmd_body = json.loads(mock_urlopen.call_args_list[2][0][0].data)
        assert cmd_body == {'name': 'SeriesSearch', 'seriesId': 1}

    @patch('urllib.request.urlopen')
    def test_audit_untagged_skips_already_routed(self, mock_urlopen, sonarr):
        """Series already carrying a routing tag (debrid/local/usenet) left alone."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = 8
        sonarr._usenet_tag_id = 9
        series_list = [
            {'id': 1, 'title': 'Debrid', 'tags': [7], 'monitored': True},
            {'id': 2, 'title': 'Local', 'tags': [8], 'monitored': True},
            {'id': 3, 'title': 'Usenet', 'tags': [9], 'monitored': True},
            {'id': 4, 'title': 'Mixed', 'tags': [100, 7], 'monitored': True},
        ]
        mock_urlopen.side_effect = [_mock_urlopen(series_list)]
        sonarr._audit_untagged_series()
        # Only the GET — no PUTs, no searches
        assert mock_urlopen.call_count == 1

    @patch('urllib.request.urlopen')
    def test_audit_untagged_skips_unmonitored(self, mock_urlopen, sonarr):
        """Unmonitored series are not auto-tagged even if untagged."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = _NOT_FOUND
        sonarr._usenet_tag_id = _NOT_FOUND
        series_list = [{'id': 1, 'title': 'Archived', 'tags': [], 'monitored': False}]
        mock_urlopen.side_effect = [_mock_urlopen(series_list)]
        sonarr._audit_untagged_series()
        assert mock_urlopen.call_count == 1

    @patch('urllib.request.urlopen')
    def test_audit_untagged_preserves_user_tags(self, mock_urlopen, sonarr):
        """Non-routing user tags are preserved when adding the debrid tag."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = 8
        sonarr._usenet_tag_id = _NOT_FOUND
        series = {'id': 1, 'title': 'Tagged', 'tags': [42], 'monitored': True}
        mock_urlopen.side_effect = [
            _mock_urlopen([series]),
            _mock_urlopen(dict(series, tags=[42, 7])),
            _mock_urlopen({'id': 99}),
        ]
        sonarr._audit_untagged_series()
        put_body = json.loads(mock_urlopen.call_args_list[1][0][0].data)
        assert 42 in put_body['tags']
        assert 7 in put_body['tags']

    @patch('urllib.request.urlopen')
    def test_audit_untagged_respects_cap(self, mock_urlopen, sonarr):
        """More than 25 untagged series: only the first 25 are processed."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = _NOT_FOUND
        sonarr._usenet_tag_id = _NOT_FOUND
        series_list = [
            {'id': i, 'title': f'S{i}', 'tags': [], 'monitored': True}
            for i in range(30)
        ]
        responses = [_mock_urlopen(series_list)]
        # Implementation PUTs all 25 first, then searches all 25
        for s in series_list[:25]:
            responses.append(_mock_urlopen(dict(s, tags=[7])))
        for _ in range(25):
            responses.append(_mock_urlopen({'id': 1}))
        mock_urlopen.side_effect = responses
        sonarr._audit_untagged_series()
        # 1 GET + 25 PUT + 25 search = 51 calls
        assert mock_urlopen.call_count == 51

    @patch('urllib.request.urlopen')
    def test_audit_untagged_no_blackhole_tag(self, mock_urlopen, sonarr):
        """No blackhole tag configured → early return, no API calls."""
        sonarr._blackhole_tag_id = _NOT_FOUND
        sonarr._audit_untagged_series()
        mock_urlopen.assert_not_called()

    def test_audit_untagged_kill_switch(self, sonarr, monkeypatch):
        """ROUTING_AUTO_TAG_UNTAGGED=false disables the sweep entirely."""
        monkeypatch.setenv('ROUTING_AUTO_TAG_UNTAGGED', 'false')
        sonarr._blackhole_tag_id = 7
        with patch('urllib.request.urlopen') as mock_urlopen:
            sonarr._audit_untagged_series()
            mock_urlopen.assert_not_called()

    @patch('urllib.request.urlopen')
    def test_audit_untagged_search_only_for_successfully_tagged(self, mock_urlopen, sonarr):
        """PUT failure on one series means that series is not searched."""
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = _NOT_FOUND
        sonarr._usenet_tag_id = _NOT_FOUND
        series_list = [
            {'id': 1, 'title': 'OK', 'tags': [], 'monitored': True},
            {'id': 2, 'title': 'Fails', 'tags': [], 'monitored': True},
        ]
        # GET, PUT-OK, PUT-fail (HTTPError), then only ONE search for id=1
        mock_urlopen.side_effect = [
            _mock_urlopen(series_list),
            _mock_urlopen(dict(series_list[0], tags=[7])),
            urllib.error.HTTPError('u', 500, 'err', {}, None),
            _mock_urlopen({'id': 99}),
        ]
        sonarr._audit_untagged_series()
        # Last call should be SeriesSearch for id=1 only
        last_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert last_body == {'name': 'SeriesSearch', 'seriesId': 1}

    @patch('urllib.request.urlopen')
    def test_audit_untagged_empty_body_put_still_triggers_search(self, mock_urlopen, sonarr):
        """200-with-empty-body PUT (proxy strips body) must still count as success.

        Regression guard: an earlier revision relied on `_ensure_debrid_routing`'s
        return-shape check, which treated empty-body responses as failure and
        silently skipped the search for series that were actually tagged
        server-side.
        """
        sonarr._blackhole_tag_id = 7
        sonarr._local_tag_id = _NOT_FOUND
        sonarr._usenet_tag_id = _NOT_FOUND
        series = {'id': 1, 'title': 'EmptyBody', 'tags': [], 'monitored': True}
        # Mock a 200 response with empty body (urlopen -> empty bytes -> {})
        empty_resp = MagicMock()
        empty_resp.read.return_value = b''
        empty_resp.__enter__ = MagicMock(return_value=empty_resp)
        empty_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.side_effect = [
            _mock_urlopen([series]),
            empty_resp,                    # PUT returns 200 with empty body
            _mock_urlopen({'id': 42}),     # Search should still fire
        ]
        sonarr._audit_untagged_series()
        # Verify a search was actually fired for id=1
        assert mock_urlopen.call_count == 3
        last_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert last_body == {'name': 'SeriesSearch', 'seriesId': 1}


# ---------------------------------------------------------------------------
# Radarr client
# ---------------------------------------------------------------------------

class TestRadarrClient:

    @patch('urllib.request.urlopen')
    def test_lookup_by_title(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen([{'title': 'Inception', 'tmdbId': 27205}])
        result = radarr.lookup_movie(title='Inception')
        assert result['title'] == 'Inception'

    @patch('urllib.request.urlopen')
    def test_lookup_by_tmdb_id(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen([{'title': 'Inception', 'tmdbId': 27205}])
        result = radarr.lookup_movie(tmdb_id=27205)
        assert result['tmdbId'] == 27205

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_existing_with_file(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen([
            {'id': 1, 'title': 'Inception', 'tmdbId': 27205, 'hasFile': True, 'tags': []}
        ])
        result = radarr.ensure_and_search('Inception', 27205)
        assert result['status'] == 'exists'

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_existing_with_file_prefer_debrid_triggers_search(self, mock_urlopen, radarr):
        """When prefer_debrid is set (local), hasFile should not block routing + search."""
        responses = [
            _mock_urlopen([{'id': 1, 'title': 'Inception', 'tmdbId': 27205, 'hasFile': True, 'tags': []}]),
            _mock_urlopen([]),  # download clients (no blackhole)
            _mock_urlopen({'records': []}),  # queue cleanup
            _mock_urlopen({'id': 42}),  # search_movie
        ]
        mock_urlopen.side_effect = responses
        result = radarr.ensure_and_search('Inception', 27205, prefer_debrid=False)
        assert result['status'] == 'sent'

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_prefer_debrid_force_grabs_when_has_file(self, mock_urlopen, radarr):
        """prefer_debrid=True with existing file should use interactive grab."""
        radarr._blackhole_tag_id = 7
        responses = [
            _mock_urlopen([{'id': 1, 'title': 'Inception', 'tmdbId': 27205, 'hasFile': True, 'tags': [7]}]),
            _mock_urlopen({'records': []}),  # queue cleanup
            _mock_urlopen([  # interactive search releases
                {'guid': 'abc', 'indexerId': 1, 'protocol': 'usenet', 'title': 'NZB', 'rejected': False},
                {'guid': 'def', 'indexerId': 2, 'protocol': 'torrent', 'title': 'Torrent', 'rejected': False},
            ]),
            _mock_urlopen({'id': 99}),  # push release
        ]
        mock_urlopen.side_effect = responses
        result = radarr.ensure_and_search('Inception', 27205, prefer_debrid=True)
        assert result['status'] == 'sent'
        assert 'Force-grabbed' in result['message']
        push_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert push_body['protocol'] == 'torrent'

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_prefer_debrid_falls_through_movie(self, mock_urlopen, radarr):
        """When interactive grab finds no torrents for movie, fall through to normal search."""
        radarr._blackhole_tag_id = 7
        responses = [
            _mock_urlopen([{'id': 1, 'title': 'Inception', 'tmdbId': 27205, 'hasFile': True, 'tags': [7]}]),
            _mock_urlopen({'records': []}),  # queue cleanup
            _mock_urlopen([]),  # interactive search — no releases at all
            _mock_urlopen({'id': 42}),  # fallback: normal MoviesSearch
        ]
        mock_urlopen.side_effect = responses
        result = radarr.ensure_and_search('Inception', 27205, prefer_debrid=True)
        assert result['status'] == 'sent'
        assert 'Force-grabbed' not in result['message']

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_skips_rejected_movie(self, mock_urlopen, radarr):
        """Rejected releases are excluded — Radarr profile rules are respected."""
        responses = [
            _mock_urlopen([
                {'guid': 'bad', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'Remux-2160p', 'rejected': True, 'customFormatScore': -10000},
                {'guid': 'good', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'WEBDL-1080p', 'rejected': False, 'customFormatScore': 3400},
            ]),
            _mock_urlopen({'id': 1}),  # push
        ]
        mock_urlopen.side_effect = responses
        result = radarr._grab_debrid_release(50, title='Test')
        assert result is True
        push_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert push_body['guid'] == 'good'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_all_rejected_returns_false_movie(self, mock_urlopen, radarr):
        """When every candidate is rejected, force-grab fails without pushing."""
        responses = [
            _mock_urlopen([
                {'guid': 'r1', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'Remux', 'rejected': True, 'customFormatScore': -10000},
                {'guid': 'r2', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'BR-DISK', 'rejected': True, 'customFormatScore': -20000},
            ]),
        ]
        mock_urlopen.side_effect = responses
        result = radarr._grab_debrid_release(50, title='Test')
        assert result is False
        assert mock_urlopen.call_count == 1  # only GET, no push

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_prefers_higher_custom_format_score_movie(self, mock_urlopen, radarr):
        """Among accepted releases, highest customFormatScore wins."""
        responses = [
            _mock_urlopen([
                {'guid': 'low', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'HDTV', 'rejected': False, 'customFormatScore': 100},
                {'guid': 'high', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'WEBDL-1080p', 'rejected': False, 'customFormatScore': 3400},
                {'guid': 'mid', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'WEBRip', 'rejected': False, 'customFormatScore': 1600},
            ]),
            _mock_urlopen({'id': 1}),
        ]
        mock_urlopen.side_effect = responses
        result = radarr._grab_debrid_release(50, title='Test')
        assert result is True
        push_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert push_body['guid'] == 'high'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_tie_breaks_on_seeders_movie(self, mock_urlopen, radarr):
        """Same customFormatScore → higher seeders wins."""
        responses = [
            _mock_urlopen([
                {'guid': 'few', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'A', 'rejected': False, 'customFormatScore': 100, 'seeders': 5},
                {'guid': 'many', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'B', 'rejected': False, 'customFormatScore': 100, 'seeders': 200},
            ]),
            _mock_urlopen({'id': 1}),
        ]
        mock_urlopen.side_effect = responses
        result = radarr._grab_debrid_release(50, title='Test')
        assert result is True
        push_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert push_body['guid'] == 'many'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_allows_cutoff_only_rejection_movie(self, mock_urlopen, radarr):
        """A Radarr release rejected ONLY for cutoff/preference-met is still eligible."""
        responses = [
            _mock_urlopen([
                {'guid': 'cutoff-only', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'WEBDL-1080p', 'rejected': True,
                 'rejections': ['Existing file on disk is of equal or higher preference: WEBDL-1080p v1'],
                 'customFormatScore': 3400},
            ]),
            _mock_urlopen({'id': 1}),
        ]
        mock_urlopen.side_effect = responses
        result = radarr._grab_debrid_release(50, title='Test')
        assert result is True
        push_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert push_body['guid'] == 'cutoff-only'

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_excludes_profile_violation_even_with_cutoff_movie(self, mock_urlopen, radarr):
        """Live-data pattern: Remux-2160p rejections pair 'cutoff met' with a profile
        violation.  The profile violation must disqualify the release."""
        responses = [
            _mock_urlopen([
                {'guid': 'remux', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'Joker.Folie.a.Deux.2024.Remux-2160p', 'rejected': True,
                 'rejections': [
                     "Custom Formats DV have score -10000 below Movie's profile minimum 0",
                     'Remux-2160p is not wanted in profile',
                     'Existing file on disk meets quality cutoff: WEBDL-1080p',
                 ],
                 'customFormatScore': -10000},
            ]),
        ]
        mock_urlopen.side_effect = responses
        result = radarr._grab_debrid_release(50, title='Test')
        assert result is False
        assert mock_urlopen.call_count == 1  # no push attempted

    @patch('urllib.request.urlopen')
    def test_grab_debrid_release_missing_score_sorts_below_negative_movie(self, mock_urlopen, radarr):
        """Regression guard: missing customFormatScore sorts to the BOTTOM, not to 0."""
        responses = [
            _mock_urlopen([
                {'guid': 'no-score', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'A', 'rejected': False, 'seeders': 500},  # score absent
                {'guid': 'negative', 'indexerId': 1, 'protocol': 'torrent',
                 'title': 'B', 'rejected': False, 'customFormatScore': -50, 'seeders': 10},
            ]),
            _mock_urlopen({'id': 1}),
        ]
        mock_urlopen.side_effect = responses
        result = radarr._grab_debrid_release(50, title='Test')
        assert result is True
        push_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert push_body['guid'] == 'negative'

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_existing_no_file(self, mock_urlopen, radarr):
        responses = [
            _mock_urlopen([{'id': 1, 'title': 'Inception', 'tmdbId': 27205, 'hasFile': False}]),
            _mock_urlopen({'records': []}),  # queue cleanup
            _mock_urlopen({'id': 42}),
        ]
        mock_urlopen.side_effect = responses
        result = radarr.ensure_and_search('Inception', 27205)
        assert result['status'] == 'sent'
        assert result['service'] == 'radarr'

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_adds_new(self, mock_urlopen, radarr):
        responses = [
            _mock_urlopen([]),  # get_all_movies
            _mock_urlopen([{'title': 'New Movie', 'tmdbId': 999, 'titleSlug': 'new-movie',
                           'images': [], 'year': 2024}]),  # lookup
            _mock_urlopen([]),  # download clients (for routing tag discovery)
            _mock_urlopen([{'id': 1, 'path': '/movies'}]),  # rootfolder
            _mock_urlopen([{'id': 1, 'name': 'HD'}]),  # qualityprofile
            _mock_urlopen({'id': 10, 'title': 'New Movie'}),  # add_movie
        ]
        mock_urlopen.side_effect = responses
        result = radarr.ensure_and_search('New Movie', 999)
        assert result['status'] == 'sent'
        assert 'Added' in result['message']

    @patch('urllib.request.urlopen')
    def test_ensure_and_search_not_found(self, mock_urlopen, radarr):
        responses = [
            _mock_urlopen([]),  # get_all_movies
            _mock_urlopen([]),  # lookup
        ]
        mock_urlopen.side_effect = responses
        result = radarr.ensure_and_search('Missing', None)
        assert result['status'] == 'error'

    @patch('urllib.request.urlopen')
    def test_get_blackhole_tag_id_found(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen([
            {'implementation': 'TorrentBlackhole', 'enable': True, 'tags': [3]},
        ])
        assert radarr._get_blackhole_tag_id() == 3

    @patch('urllib.request.urlopen')
    def test_get_blackhole_tag_id_not_found(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen([
            {'implementation': 'QBittorrent', 'enable': True, 'tags': []},
        ])
        assert radarr._get_blackhole_tag_id() is None

    @patch('urllib.request.urlopen')
    def test_ensure_debrid_routing_adds_tag(self, mock_urlopen, radarr):
        radarr._blackhole_tag_id = 3
        movie = {'id': 1, 'title': 'Inception', 'tags': []}
        mock_urlopen.return_value = _mock_urlopen(dict(movie, tags=[3]))
        result = radarr._ensure_debrid_routing(movie)
        assert 3 in result['tags']

    @patch('urllib.request.urlopen')
    def test_ensure_local_routing_removes_tag(self, mock_urlopen, radarr):
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = 5
        movie = {'id': 1, 'title': 'Inception', 'tags': [3]}
        mock_urlopen.return_value = _mock_urlopen(dict(movie, tags=[5]))
        result = radarr._ensure_local_routing(movie)
        assert 3 not in result['tags']
        assert 5 in result['tags']

    @patch('urllib.request.urlopen')
    def test_remove_movie(self, mock_urlopen, radarr):
        responses = [
            _mock_urlopen([{
                'id': 1, 'title': 'Inception', 'tmdbId': 27205,
                'hasFile': True, 'movieFile': {'id': 99},
            }]),
            _mock_urlopen({}),  # delete file
        ]
        mock_urlopen.side_effect = responses
        result = radarr.remove_movie('Inception', 27205)
        assert result['status'] == 'removed'
        assert result['removed'] == 1

    @patch('urllib.request.urlopen')
    def test_remove_movie_no_file(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen([{
            'id': 1, 'title': 'Inception', 'tmdbId': 27205, 'hasFile': False,
        }])
        result = radarr.remove_movie('Inception', 27205)
        assert result['status'] == 'error'
        assert 'no file' in result['message']

    @patch('urllib.request.urlopen')
    def test_remove_movie_not_found(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen([])
        result = radarr.remove_movie('Missing', None)
        assert result['status'] == 'error'

    @patch('urllib.request.urlopen')
    def test_remove_movie_null_movie_file(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen([{
            'id': 1, 'title': 'Inception', 'tmdbId': 27205,
            'hasFile': True, 'movieFile': None,
        }])
        result = radarr.remove_movie('Inception', 27205)
        assert result['status'] == 'error'
        assert 'file ID' in result['message']

    # --- _fix_indexer_routing: torrent indexer debrid tag ---

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_adds_debrid_tag_to_local_tagged_torrent(self, mock_urlopen, radarr):
        """Torrent indexer with only the local tag gets debrid tag added."""
        indexers = [
            {'id': 1, 'name': '1337x', 'protocol': 'torrent', 'tags': [5], 'downloadClientId': 0},
        ]
        mock_urlopen.side_effect = [
            _mock_urlopen(indexers),       # GET /indexer
            _mock_urlopen(indexers[0]),     # PUT /indexer/1
        ]
        result = radarr._fix_indexer_routing(set(), 5, debrid_tag=3)
        assert result is True
        put_call = mock_urlopen.call_args_list[1]
        put_body = json.loads(put_call[0][0].data)
        assert 3 in put_body['tags']
        assert 5 in put_body['tags']

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_warns_custom_tagged_torrent(self, mock_urlopen, radarr):
        """Torrent indexer with custom tags (not just local) is warned, not modified."""
        indexers = [
            {'id': 1, 'name': '1337x', 'protocol': 'torrent', 'tags': [99], 'downloadClientId': 0},
        ]
        mock_urlopen.return_value = _mock_urlopen(indexers)
        result = radarr._fix_indexer_routing(set(), 5, debrid_tag=3)
        assert result is False
        assert mock_urlopen.call_count == 1  # only GET, no PUT

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_tags_untagged_torrent(self, mock_urlopen, radarr):
        """Untagged torrent indexer gets debrid tag (Radarr v4 requires shared tags)."""
        indexers = [
            {'id': 1, 'name': 'TPB', 'protocol': 'torrent', 'tags': [], 'downloadClientId': 0},
        ]
        mock_urlopen.side_effect = [_mock_urlopen(indexers), _mock_urlopen(indexers[0])]
        result = radarr._fix_indexer_routing(set(), None, debrid_tag=3)
        assert result is True
        assert mock_urlopen.call_count == 2  # GET + PUT
        put_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert 3 in put_body['tags']

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_tags_untagged_torrent_with_local(self, mock_urlopen, radarr):
        """Untagged torrent indexer gets both debrid and local tags for dual routing."""
        indexers = [
            {'id': 1, 'name': 'TPB', 'protocol': 'torrent', 'tags': [], 'downloadClientId': 0},
        ]
        mock_urlopen.side_effect = [_mock_urlopen(indexers), _mock_urlopen(indexers[0])]
        result = radarr._fix_indexer_routing(set(), 5, debrid_tag=3)
        assert result is True
        put_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert 3 in put_body['tags']
        assert 5 in put_body['tags']

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_skips_torrent_already_tagged(self, mock_urlopen, radarr):
        """Torrent indexer already carrying debrid tag should not be re-written."""
        indexers = [
            {'id': 1, 'name': 'YTS', 'protocol': 'torrent', 'tags': [5, 3], 'downloadClientId': 0},
        ]
        mock_urlopen.return_value = _mock_urlopen(indexers)
        result = radarr._fix_indexer_routing(set(), None, debrid_tag=3)
        assert result is False
        assert mock_urlopen.call_count == 1  # only GET, no PUT

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_no_debrid_tag_skips_torrent(self, mock_urlopen, radarr):
        """When debrid_tag is None, torrent indexers are not touched."""
        indexers = [
            {'id': 1, 'name': '1337x', 'protocol': 'torrent', 'tags': [5], 'downloadClientId': 0},
        ]
        mock_urlopen.return_value = _mock_urlopen(indexers)
        result = radarr._fix_indexer_routing(set(), None, debrid_tag=None)
        assert result is False
        assert mock_urlopen.call_count == 1  # only GET, no PUT

    # --- _search_debrid_missing ---

    @patch('urllib.request.urlopen')
    def test_search_debrid_missing_triggers_search(self, mock_urlopen, radarr):
        """After indexer fix, debrid-tagged movies without files get searched."""
        radarr._blackhole_tag_id = 3
        movies = [
            {'id': 10, 'tags': [3], 'monitored': True, 'hasFile': False},
            {'id': 20, 'tags': [5], 'monitored': True, 'hasFile': False},
            {'id': 30, 'tags': [3], 'monitored': True, 'hasFile': True},
        ]
        mock_urlopen.side_effect = [
            _mock_urlopen(movies),      # GET /movie
            _mock_urlopen({}),          # POST /command
        ]
        radarr._search_debrid_missing()
        assert mock_urlopen.call_count == 2  # GET + 1 POST
        post_body = json.loads(mock_urlopen.call_args_list[1][0][0].data)
        assert post_body['name'] == 'MoviesSearch'
        assert post_body['movieIds'] == [10]

    @patch('urllib.request.urlopen')
    def test_search_debrid_missing_noop_when_none_missing(self, mock_urlopen, radarr):
        """No search triggered when all debrid movies have files."""
        radarr._blackhole_tag_id = 3
        movies = [
            {'id': 10, 'tags': [3], 'monitored': True, 'hasFile': True},
        ]
        mock_urlopen.return_value = _mock_urlopen(movies)
        radarr._search_debrid_missing()
        assert mock_urlopen.call_count == 1  # only GET, no POST

    # --- Usenet tag routing ---

    @patch('urllib.request.urlopen')
    def test_get_usenet_tag_id_found(self, mock_urlopen, radarr):
        """Usenet client + blackhole → usenet tag is created and cached."""
        mock_urlopen.side_effect = [
            _mock_urlopen([
                {'implementation': 'Nzbget', 'enable': True, 'tags': [5], 'id': 1, 'name': 'NZBget'},
                {'implementation': 'TorrentBlackhole', 'enable': True, 'tags': [3], 'id': 2},
                {'implementation': 'QBittorrent', 'enable': True, 'tags': [5], 'id': 3, 'name': 'qBit'},
            ]),
            _mock_urlopen([{'label': 'debrid', 'id': 3}, {'label': 'standard', 'id': 5}]),
            _mock_urlopen({'label': 'usenet', 'id': 6}),
            _mock_urlopen({'id': 1, 'tags': [5, 6]}),
            _mock_urlopen([]),
        ]
        assert radarr._get_usenet_tag_id() == 6

    @patch('urllib.request.urlopen')
    def test_get_usenet_tag_id_no_usenet_client(self, mock_urlopen, radarr):
        """No usenet client → usenet tag is None."""
        mock_urlopen.return_value = _mock_urlopen([
            {'implementation': 'QBittorrent', 'enable': True, 'tags': [5], 'id': 1, 'name': 'qBit'},
            {'implementation': 'TorrentBlackhole', 'enable': True, 'tags': [3], 'id': 2},
        ])
        assert radarr._get_usenet_tag_id() is None

    @patch('urllib.request.urlopen')
    def test_ensure_local_routing_prefers_usenet(self, mock_urlopen, radarr):
        """When usenet tag exists, _ensure_local_routing applies usenet tag."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = 5
        radarr._usenet_tag_id = 6
        movie = {'id': 1, 'title': 'Inception', 'tags': [3]}
        mock_urlopen.return_value = _mock_urlopen(dict(movie, tags=[6]))
        result = radarr._ensure_local_routing(movie)
        assert 6 in result['tags']
        assert 3 not in result['tags']
        assert 5 not in result['tags']

    @patch('urllib.request.urlopen')
    def test_ensure_local_routing_falls_back_to_local(self, mock_urlopen, radarr):
        """When no usenet tag exists, _ensure_local_routing uses local tag."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = 5
        radarr._usenet_tag_id = _NOT_FOUND
        movie = {'id': 1, 'title': 'Inception', 'tags': [3]}
        mock_urlopen.return_value = _mock_urlopen(dict(movie, tags=[5]))
        result = radarr._ensure_local_routing(movie)
        assert 5 in result['tags']
        assert 3 not in result['tags']

    def test_ensure_local_routing_noop_when_no_local_tag(self, radarr):
        """When no local tag exists, don't remove debrid tag (would leave movie unroutable)."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = _NOT_FOUND
        radarr._usenet_tag_id = _NOT_FOUND
        movie = {'id': 1, 'title': 'Inception', 'tags': [3]}
        result = radarr._ensure_local_routing(movie)
        assert result is movie  # unchanged, no PUT

    @patch('urllib.request.urlopen')
    def test_ensure_local_routing_removes_stale_local_tag(self, mock_urlopen, radarr):
        """When switching to usenet, stale local tag is removed from movie."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = 5
        radarr._usenet_tag_id = 6
        movie = {'id': 1, 'title': 'Inception', 'tags': [5]}
        mock_urlopen.return_value = _mock_urlopen(dict(movie, tags=[6]))
        result = radarr._ensure_local_routing(movie)
        assert 6 in result['tags']
        assert 5 not in result['tags']

    @patch('urllib.request.urlopen')
    def test_ensure_debrid_routing_removes_usenet_tag(self, mock_urlopen, radarr):
        """Switching to debrid strips usenet tag."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = 5
        radarr._usenet_tag_id = 6
        movie = {'id': 1, 'title': 'Inception', 'tags': [6]}
        mock_urlopen.return_value = _mock_urlopen(dict(movie, tags=[3]))
        result = radarr._ensure_debrid_routing(movie)
        assert 3 in result['tags']
        assert 6 not in result['tags']

    @patch('urllib.request.urlopen')
    def test_fix_indexer_routing_usenet_indexer_gets_both_tags(self, mock_urlopen, radarr):
        """Usenet indexer gets both local and usenet tags."""
        indexers = [
            {'id': 1, 'name': 'NZBgeek', 'protocol': 'usenet', 'tags': [], 'downloadClientId': 0},
        ]
        mock_urlopen.side_effect = [
            _mock_urlopen(indexers),
            _mock_urlopen(indexers[0]),
        ]
        radarr._fix_indexer_routing(set(), 5, debrid_tag=3, usenet_tag=6)
        put_body = json.loads(mock_urlopen.call_args_list[1][0][0].data)
        assert 5 in put_body['tags']
        assert 6 in put_body['tags']

    # -----------------------------------------------------------------------
    # _audit_untagged_movies (self-heal for Overseerr tag=[] misconfig)
    # -----------------------------------------------------------------------

    @patch('urllib.request.urlopen')
    def test_audit_untagged_tags_and_searches(self, mock_urlopen, radarr):
        """Untagged monitored movie gets the debrid tag + MoviesSearch fired for its id."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = _NOT_FOUND
        radarr._usenet_tag_id = _NOT_FOUND
        movies = [{'id': 10, 'title': 'Inception', 'tags': [], 'monitored': True}]
        mock_urlopen.side_effect = [
            _mock_urlopen(movies),                                       # GET /movie
            _mock_urlopen(dict(movies[0], tags=[3])),                    # PUT /movie/10
            _mock_urlopen({'id': 55}),                                   # POST /command MoviesSearch
        ]
        radarr._audit_untagged_movies()
        put_body = json.loads(mock_urlopen.call_args_list[1][0][0].data)
        assert 3 in put_body['tags']
        cmd_body = json.loads(mock_urlopen.call_args_list[2][0][0].data)
        assert cmd_body == {'name': 'MoviesSearch', 'movieIds': [10]}

    @patch('urllib.request.urlopen')
    def test_audit_untagged_skips_already_routed(self, mock_urlopen, radarr):
        """Movie already carrying a routing tag (debrid/local/usenet) left alone."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = 5
        radarr._usenet_tag_id = 6
        movies = [
            {'id': 1, 'title': 'Debrid', 'tags': [3], 'monitored': True},
            {'id': 2, 'title': 'Local', 'tags': [5], 'monitored': True},
            {'id': 3, 'title': 'Usenet', 'tags': [6], 'monitored': True},
            {'id': 4, 'title': 'Mixed', 'tags': [99, 3], 'monitored': True},
        ]
        mock_urlopen.side_effect = [_mock_urlopen(movies)]
        radarr._audit_untagged_movies()
        assert mock_urlopen.call_count == 1

    @patch('urllib.request.urlopen')
    def test_audit_untagged_skips_unmonitored(self, mock_urlopen, radarr):
        """Unmonitored movies are not auto-tagged even if untagged."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = _NOT_FOUND
        radarr._usenet_tag_id = _NOT_FOUND
        movies = [{'id': 1, 'title': 'Archived', 'tags': [], 'monitored': False}]
        mock_urlopen.side_effect = [_mock_urlopen(movies)]
        radarr._audit_untagged_movies()
        assert mock_urlopen.call_count == 1

    @patch('urllib.request.urlopen')
    def test_audit_untagged_preserves_user_tags(self, mock_urlopen, radarr):
        """Non-routing user tags are preserved when adding the debrid tag."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = 5
        radarr._usenet_tag_id = _NOT_FOUND
        movie = {'id': 1, 'title': 'Tagged', 'tags': [42], 'monitored': True}
        mock_urlopen.side_effect = [
            _mock_urlopen([movie]),
            _mock_urlopen(dict(movie, tags=[42, 3])),
            _mock_urlopen({'id': 99}),
        ]
        radarr._audit_untagged_movies()
        put_body = json.loads(mock_urlopen.call_args_list[1][0][0].data)
        assert 42 in put_body['tags']
        assert 3 in put_body['tags']

    @patch('urllib.request.urlopen')
    def test_audit_untagged_respects_cap(self, mock_urlopen, radarr):
        """More than 25 untagged movies: only the first 25 tagged, one batched search."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = _NOT_FOUND
        radarr._usenet_tag_id = _NOT_FOUND
        movies = [{'id': i, 'title': f'M{i}', 'tags': [], 'monitored': True} for i in range(30)]
        responses = [_mock_urlopen(movies)]
        for m in movies[:25]:
            responses.append(_mock_urlopen(dict(m, tags=[3])))
        responses.append(_mock_urlopen({'id': 1}))  # single batched MoviesSearch
        mock_urlopen.side_effect = responses
        radarr._audit_untagged_movies()
        # 1 GET + 25 PUT + 1 batched search = 27
        assert mock_urlopen.call_count == 27
        last_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert last_body['name'] == 'MoviesSearch'
        assert len(last_body['movieIds']) == 25

    @patch('urllib.request.urlopen')
    def test_audit_untagged_no_blackhole_tag(self, mock_urlopen, radarr):
        """No blackhole tag configured → early return, no API calls."""
        radarr._blackhole_tag_id = _NOT_FOUND
        radarr._audit_untagged_movies()
        mock_urlopen.assert_not_called()

    def test_audit_untagged_kill_switch(self, radarr, monkeypatch):
        """ROUTING_AUTO_TAG_UNTAGGED=false disables the sweep entirely."""
        monkeypatch.setenv('ROUTING_AUTO_TAG_UNTAGGED', 'false')
        radarr._blackhole_tag_id = 3
        with patch('urllib.request.urlopen') as mock_urlopen:
            radarr._audit_untagged_movies()
            mock_urlopen.assert_not_called()

    @patch('urllib.request.urlopen')
    def test_audit_untagged_search_only_for_successfully_tagged(self, mock_urlopen, radarr):
        """PUT failure on one movie means its id is not in the MoviesSearch batch."""
        radarr._blackhole_tag_id = 3
        radarr._local_tag_id = _NOT_FOUND
        radarr._usenet_tag_id = _NOT_FOUND
        movies = [
            {'id': 1, 'title': 'OK', 'tags': [], 'monitored': True},
            {'id': 2, 'title': 'Fails', 'tags': [], 'monitored': True},
        ]
        mock_urlopen.side_effect = [
            _mock_urlopen(movies),
            _mock_urlopen(dict(movies[0], tags=[3])),
            urllib.error.HTTPError('u', 500, 'err', {}, None),
            _mock_urlopen({'id': 1}),
        ]
        radarr._audit_untagged_movies()
        last_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert last_body == {'name': 'MoviesSearch', 'movieIds': [1]}


# ---------------------------------------------------------------------------
# Overseerr client
# ---------------------------------------------------------------------------

class TestOverseerrClient:

    @patch('urllib.request.urlopen')
    def test_search(self, mock_urlopen, overseerr):
        mock_urlopen.return_value = _mock_urlopen({
            'results': [{'id': 1396, 'mediaType': 'tv', 'name': 'Breaking Bad'}]
        })
        result = overseerr.search('Breaking Bad')
        assert result['id'] == 1396

    @patch('urllib.request.urlopen')
    def test_search_no_results(self, mock_urlopen, overseerr):
        mock_urlopen.return_value = _mock_urlopen({'results': []})
        result = overseerr.search('Nonexistent')
        assert result is None

    @patch('urllib.request.urlopen')
    def test_request_tv(self, mock_urlopen, overseerr):
        mock_urlopen.return_value = _mock_urlopen({'id': 1, 'status': 2})
        result = overseerr.request_tv(1396, [1, 2])
        assert result is not None

    @patch('urllib.request.urlopen')
    def test_request_movie(self, mock_urlopen, overseerr):
        mock_urlopen.return_value = _mock_urlopen({'id': 1, 'status': 2})
        result = overseerr.request_movie(27205)
        assert result is not None

    @patch('urllib.request.urlopen')
    def test_ensure_and_request_tv_with_tmdb_id(self, mock_urlopen, overseerr):
        mock_urlopen.return_value = _mock_urlopen({'id': 1, 'status': 2})
        result = overseerr.ensure_and_request_tv('Breaking Bad', 1396, [1])
        assert result['status'] == 'requested'
        assert result['service'] == 'overseerr'

    @patch('urllib.request.urlopen')
    def test_ensure_and_request_tv_searches_when_no_tmdb(self, mock_urlopen, overseerr):
        responses = [
            _mock_urlopen({'results': [{'id': 1396}]}),  # search
            _mock_urlopen({'id': 1, 'status': 2}),  # request_tv
        ]
        mock_urlopen.side_effect = responses
        result = overseerr.ensure_and_request_tv('Breaking Bad', None, [1])
        assert result['status'] == 'requested'

    @patch('urllib.request.urlopen')
    def test_ensure_and_request_tv_not_found(self, mock_urlopen, overseerr):
        mock_urlopen.return_value = _mock_urlopen({'results': []})
        result = overseerr.ensure_and_request_tv('Missing', None, [1])
        assert result['status'] == 'error'

    @patch('urllib.request.urlopen')
    def test_ensure_and_request_movie_with_tmdb_id(self, mock_urlopen, overseerr):
        mock_urlopen.return_value = _mock_urlopen({'id': 1, 'status': 2})
        result = overseerr.ensure_and_request_movie('Inception', 27205)
        assert result['status'] == 'requested'

    @patch('urllib.request.urlopen')
    def test_ensure_and_request_movie_not_found(self, mock_urlopen, overseerr):
        mock_urlopen.return_value = _mock_urlopen({'results': []})
        result = overseerr.ensure_and_request_movie('Missing', None)
        assert result['status'] == 'error'


# ---------------------------------------------------------------------------
# get_recent_grabs — client-side eventType filtering
# ---------------------------------------------------------------------------

class TestGetRecentGrabs:
    """Tests for SonarrClient.get_recent_grabs and RadarrClient.get_recent_grabs."""

    @pytest.fixture(params=['sonarr', 'radarr'])
    def client(self, request, sonarr, radarr):
        return sonarr if request.param == 'sonarr' else radarr

    @patch('urllib.request.urlopen')
    def test_filters_grabbed_events_only(self, mock_urlopen, client):
        mock_urlopen.return_value = _mock_urlopen({
            'records': [
                {'eventType': 'grabbed', 'title': 'Show A'},
                {'eventType': 'downloadFolderImported', 'title': 'Show A'},
                {'eventType': 'grabbed', 'title': 'Show B'},
                {'eventType': 'episodeFileRenamed', 'title': 'Show C'},
                {'eventType': 'episodeFileDeleted', 'title': 'Show D'},
            ]
        })
        result = client.get_recent_grabs(page_size=10)
        assert len(result) == 2
        assert all(r['eventType'] == 'grabbed' for r in result)
        assert result[0]['title'] == 'Show A'
        assert result[1]['title'] == 'Show B'

    @patch('urllib.request.urlopen')
    def test_returns_empty_when_no_grabs(self, mock_urlopen, client):
        mock_urlopen.return_value = _mock_urlopen({
            'records': [
                {'eventType': 'downloadFolderImported', 'title': 'Show A'},
                {'eventType': 'episodeFileRenamed', 'title': 'Show B'},
            ]
        })
        result = client.get_recent_grabs(page_size=10)
        assert result == []

    @patch('urllib.request.urlopen')
    def test_returns_empty_on_api_error(self, mock_urlopen, client):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            'http://test', 500, 'Server Error', {}, None
        )
        result = client.get_recent_grabs()
        assert result == []

    @patch('urllib.request.urlopen')
    def test_returns_empty_on_empty_records(self, mock_urlopen, client):
        mock_urlopen.return_value = _mock_urlopen({'records': []})
        result = client.get_recent_grabs()
        assert result == []

    @patch('urllib.request.urlopen')
    def test_returns_empty_on_missing_records_key(self, mock_urlopen, client):
        mock_urlopen.return_value = _mock_urlopen({'page': 1})
        result = client.get_recent_grabs()
        assert result == []

    @patch('urllib.request.urlopen')
    def test_returns_empty_on_url_error(self, mock_urlopen, client):
        mock_urlopen.side_effect = urllib.error.URLError('Connection refused')
        result = client.get_recent_grabs()
        assert result == []

    @patch('urllib.request.urlopen')
    def test_skips_records_missing_eventtype_key(self, mock_urlopen, client):
        mock_urlopen.return_value = _mock_urlopen({
            'records': [
                {'title': 'No eventType field'},
                {'eventType': 'grabbed', 'title': 'Good'},
            ]
        })
        result = client.get_recent_grabs()
        assert len(result) == 1
        assert result[0]['title'] == 'Good'

    @patch('urllib.request.urlopen')
    def test_skips_non_dict_records(self, mock_urlopen, client):
        mock_urlopen.return_value = _mock_urlopen({
            'records': [None, 42, 'bad', {'eventType': 'grabbed', 'title': 'OK'}]
        })
        result = client.get_recent_grabs()
        assert len(result) == 1
        assert result[0]['title'] == 'OK'

    @patch('urllib.request.urlopen')
    def test_returns_empty_on_non_dict_response(self, mock_urlopen, client):
        mock_urlopen.return_value = _mock_urlopen([{'eventType': 'grabbed'}])
        result = client.get_recent_grabs()
        assert result == []

    @patch('urllib.request.urlopen')
    def test_does_not_send_eventtype_param(self, mock_urlopen, client):
        """Ensure eventType is NOT sent but sort params ARE (older arr compat)."""
        mock_urlopen.return_value = _mock_urlopen({'records': []})
        client.get_recent_grabs(page_size=30)
        assert mock_urlopen.called
        url = mock_urlopen.call_args[0][0].full_url
        assert 'eventType' not in url
        assert 'sortKey=date' in url
        assert 'sortDirection=descending' in url

    @patch('urllib.request.urlopen')
    def test_respects_page_size(self, mock_urlopen, client):
        mock_urlopen.return_value = _mock_urlopen({'records': []})
        client.get_recent_grabs(page_size=200)
        assert mock_urlopen.called
        assert 'pageSize=200' in mock_urlopen.call_args[0][0].full_url

    @patch('urllib.request.urlopen')
    def test_all_grabs_returned_when_page_is_all_grabs(self, mock_urlopen, client):
        records = [{'eventType': 'grabbed', 'title': f'Item {i}'} for i in range(30)]
        mock_urlopen.return_value = _mock_urlopen({'records': records})
        result = client.get_recent_grabs(page_size=30)
        assert len(result) == 30


# ---------------------------------------------------------------------------
# Quality profile reader (plan 33 Phase 1 — Sonarr/Radarr parity, I5)
# ---------------------------------------------------------------------------

# Fixture representing a real Sonarr/Radarr v3 profile response shape.
# Top-level items are ordered top-to-bottom as the user ranked them in the
# UI; that order drives preference.  Inner items within a group inherit the
# group's allowed flag visually but still carry their own ``allowed`` field
# (matches what the API actually returns — we respect partial group ticks).
_PROFILE_WITH_GROUP = {
    'id': 4,
    'name': 'HD-2160p',
    'items': [
        # Sonarr/Radarr return profile items in ASCENDING quality order
        # (SDTV / lowest first, Remux-2160p / highest last — this is the
        # engine's quality-weight ordering).  ``get_tier_order`` reverses
        # this so callers see preferred (highest) first.
        #
        # Disallowed group — entire subtree suppressed even though inner
        # items carry ``allowed: true`` (matches Sonarr UI: unticking the
        # group hides everything inside it).
        {'name': 'SD',
         'allowed': False,
         'items': [
             {'quality': {'id': 1, 'name': 'SDTV', 'resolution': 480},
              'allowed': True, 'items': []},
         ]},
        # Bare disallowed quality at a lower tier — must not appear in output
        {'quality': {'id': 4, 'name': 'HDTV-720p', 'resolution': 720},
         'allowed': False, 'items': []},
        # Group — HD 1080p bucket with multiple sources collapsed to one tier
        {'name': 'HD-1080p',
         'allowed': True,
         'items': [
             {'quality': {'id': 3, 'name': 'WEBDL-1080p', 'resolution': 1080},
              'allowed': True, 'items': []},
             {'quality': {'id': 7, 'name': 'Bluray-1080p', 'resolution': 1080},
              'allowed': True, 'items': []},
         ]},
        # Bare top-level qualities at the top of the profile (highest-weight)
        {'quality': {'id': 18, 'name': 'WEBDL-2160p', 'source': 'web', 'resolution': 2160},
         'allowed': True, 'items': []},
        {'quality': {'id': 19, 'name': 'Bluray-2160p', 'source': 'bluray', 'resolution': 2160},
         'allowed': True, 'items': []},
    ],
}


class TestQualityProfileReader:
    """Phase 1 of plan 33 — profile reader, tier ordering, TTL cache, parity."""

    @patch('urllib.request.urlopen')
    def test_get_quality_profile_fetches_single_profile(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen(_PROFILE_WITH_GROUP)
        profile = sonarr.get_quality_profile(4)
        assert profile is not None
        assert profile['id'] == 4
        # URL targets the specific-profile endpoint, not the collection listing
        called_url = mock_urlopen.call_args[0][0].full_url
        assert '/api/v3/qualityprofile/4' in called_url

    def test_get_quality_profile_rejects_non_positive_ids(self, sonarr):
        # Invalid IDs short-circuit without hitting HTTP — tests no mock needed
        assert sonarr.get_quality_profile(0) is None
        assert sonarr.get_quality_profile(-1) is None
        assert sonarr.get_quality_profile(None) is None
        assert sonarr.get_quality_profile('4') is None  # string, not int
        assert sonarr.get_quality_profile(True) is None  # bool disallowed

    @patch('urllib.request.urlopen')
    def test_get_quality_profile_returns_none_on_http_error(self, mock_urlopen, sonarr):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            'http://sonarr:8989/api/v3/qualityprofile/99', 404, 'Not Found', {}, None
        )
        assert sonarr.get_quality_profile(99) is None

    @patch('urllib.request.urlopen')
    def test_get_quality_profile_does_not_cache_failures(self, mock_urlopen, sonarr):
        # First call fails, second succeeds — failed fetches must not be
        # cached or a transient 5xx would lock the profile for 15 min.
        mock_urlopen.side_effect = [
            urllib.error.URLError('Connection refused'),
            _mock_urlopen(_PROFILE_WITH_GROUP),
        ]
        assert sonarr.get_quality_profile(4) is None
        assert sonarr.get_quality_profile(4) is not None
        assert mock_urlopen.call_count == 2

    @patch('urllib.request.urlopen')
    def test_get_quality_profile_cache_hit_within_ttl(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen(_PROFILE_WITH_GROUP)
        first = sonarr.get_quality_profile(4)
        second = sonarr.get_quality_profile(4)
        assert first is second  # cache returns same object reference
        assert mock_urlopen.call_count == 1  # only one HTTP round-trip

    @patch('utils.arr_client.time.monotonic')
    @patch('urllib.request.urlopen')
    def test_get_quality_profile_cache_expires_after_ttl(self, mock_urlopen, mock_mono, sonarr):
        mock_urlopen.return_value = _mock_urlopen(_PROFILE_WITH_GROUP)
        # Drive monotonic from a mutable clock so the test stays robust if
        # get_quality_profile ever changes how many times it samples the
        # clock per call.  First call is "now"; second is well past the
        # 15-min TTL so the cache entry must be treated as expired.
        current_time = {'now': 1000.0}
        mock_mono.side_effect = lambda: current_time['now']
        sonarr.get_quality_profile(4)
        current_time['now'] = 1000.0 + _PROFILE_CACHE_TTL_SECONDS + 1.0
        sonarr.get_quality_profile(4)
        assert mock_urlopen.call_count == 2  # cache miss forced a second fetch

    @patch('urllib.request.urlopen')
    def test_get_tier_order_simple_profile(self, mock_urlopen, sonarr):
        # Bare qualities at 720p + 1080p (two sources) — the real Sonarr API
        # returns items ASCENDING (lowest quality first); ``get_tier_order``
        # reverses so the caller sees preferred-first.  720p below; 1080p
        # duplicates collapse to one tier.
        mock_urlopen.return_value = _mock_urlopen({
            'id': 1,
            'name': 'HD',
            'items': [
                {'quality': {'id': 4, 'name': 'HDTV-720p', 'resolution': 720},
                 'allowed': True, 'items': []},
                {'quality': {'id': 3, 'name': 'WEBDL-1080p', 'resolution': 1080},
                 'allowed': True, 'items': []},
                {'quality': {'id': 7, 'name': 'Bluray-1080p', 'resolution': 1080},
                 'allowed': True, 'items': []},
            ],
        })
        assert sonarr.get_tier_order(1) == ['1080p', '720p']

    @patch('urllib.request.urlopen')
    def test_get_tier_order_grouped_profile(self, mock_urlopen, sonarr):
        # HD-1080p group with multiple inner sources should collapse to a
        # single 1080p tier.  The plan calls this out explicitly — groups are
        # a user's way of saying "any of these sources at this resolution".
        mock_urlopen.return_value = _mock_urlopen({
            'id': 2,
            'name': 'Grouped',
            'items': [
                {'name': 'HD-1080p',
                 'allowed': True,
                 'items': [
                     {'quality': {'id': 7, 'name': 'Bluray-1080p', 'resolution': 1080},
                      'allowed': True, 'items': []},
                     {'quality': {'id': 3, 'name': 'WEBDL-1080p', 'resolution': 1080},
                      'allowed': True, 'items': []},
                     {'quality': {'id': 8, 'name': 'HDTV-1080p', 'resolution': 1080},
                      'allowed': True, 'items': []},
                 ]},
            ],
        })
        assert sonarr.get_tier_order(2) == ['1080p']

    @patch('urllib.request.urlopen')
    def test_get_tier_order_skips_disallowed(self, mock_urlopen, sonarr):
        # Mix of allowed/disallowed at every level — I1: profile is the
        # ceiling, so disallowed items NEVER appear in the tier list.
        mock_urlopen.return_value = _mock_urlopen(_PROFILE_WITH_GROUP)
        # 720p is individually disallowed; SD group is disallowed (suppressing
        # the inner allowed SDTV item); only 2160p + 1080p should surface.
        assert sonarr.get_tier_order(4) == ['2160p', '1080p']

    @patch('urllib.request.urlopen')
    def test_get_tier_order_disallowed_group_suppresses_inner_allowed(
            self, mock_urlopen, sonarr):
        # Regression guard — a disallowed group must hide its inner items
        # even when the API marks them as allowed individually.
        mock_urlopen.return_value = _mock_urlopen({
            'id': 5,
            'name': 'SDOnlyOff',
            'items': [
                {'quality': {'id': 7, 'name': 'Bluray-1080p', 'resolution': 1080},
                 'allowed': True, 'items': []},
                {'name': 'SD',
                 'allowed': False,
                 'items': [
                     {'quality': {'id': 1, 'name': 'SDTV', 'resolution': 480},
                      'allowed': True, 'items': []},
                 ]},
            ],
        })
        assert sonarr.get_tier_order(5) == ['1080p']

    @patch('urllib.request.urlopen')
    def test_get_tier_order_uses_resolution_int_when_present(self, mock_urlopen, sonarr):
        # resolution=1080 and a non-standard name string — we should still
        # return 1080p via the numeric path rather than guessing from the name.
        mock_urlopen.return_value = _mock_urlopen({
            'id': 6,
            'items': [
                {'quality': {'id': 99, 'name': 'Custom-Unusual-Tag', 'resolution': 1080},
                 'allowed': True, 'items': []},
            ],
        })
        assert sonarr.get_tier_order(6) == ['1080p']

    @patch('urllib.request.urlopen')
    def test_get_tier_order_falls_back_to_name_parse_when_resolution_missing(
            self, mock_urlopen, sonarr):
        # Older Sonarr might not populate resolution — name parser fallback.
        # Items ASCENDING as the real API returns; reversed output.
        mock_urlopen.return_value = _mock_urlopen({
            'id': 7,
            'items': [
                {'quality': {'id': 4, 'name': 'HDTV-720p'},
                 'allowed': True, 'items': []},
                {'quality': {'id': 7, 'name': 'Bluray-1080p'},
                 'allowed': True, 'items': []},
            ],
        })
        assert sonarr.get_tier_order(7) == ['1080p', '720p']

    @patch('urllib.request.urlopen')
    def test_get_tier_order_drops_custom_name_with_trailing_suffix(
            self, mock_urlopen, sonarr):
        # Regression: older arrs may not populate quality.resolution, so we
        # parse the name — but the token must be at end-of-name.  A custom
        # user quality named `Mobile-480p-low` must NOT be collapsed into
        # the standard `480p` tier (I1: profile is the ceiling, never
        # promote an unrecognised custom quality into a sibling tier).
        mock_urlopen.return_value = _mock_urlopen({
            'id': 10,
            'items': [
                {'quality': {'id': 50, 'name': 'Mobile-480p-low'},
                 'allowed': True, 'items': []},
                {'quality': {'id': 7, 'name': 'Bluray-1080p'},
                 'allowed': True, 'items': []},
            ],
        })
        assert sonarr.get_tier_order(10) == ['1080p']

    @patch('urllib.request.urlopen')
    def test_get_tier_order_drops_unrecognised_quality(self, mock_urlopen, sonarr):
        # Unparseable name and no resolution → drop rather than guess.
        # Invariant I1 demands we never invent a tier; dropping is safe.
        mock_urlopen.return_value = _mock_urlopen({
            'id': 8,
            'items': [
                {'quality': {'id': 42, 'name': 'Unknown-Quality'},
                 'allowed': True, 'items': []},
                {'quality': {'id': 7, 'name': 'Bluray-1080p', 'resolution': 1080},
                 'allowed': True, 'items': []},
            ],
        })
        assert sonarr.get_tier_order(8) == ['1080p']

    @patch('urllib.request.urlopen')
    def test_get_tier_order_empty_on_missing_profile(self, mock_urlopen, sonarr):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            'http://sonarr:8989/api/v3/qualityprofile/99', 404, 'Not Found', {}, None
        )
        assert sonarr.get_tier_order(99) == []

    @patch('urllib.request.urlopen')
    def test_get_tier_order_empty_on_all_disallowed(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen({
            'id': 9,
            'items': [
                {'quality': {'id': 1, 'name': 'SDTV', 'resolution': 480},
                 'allowed': False, 'items': []},
            ],
        })
        assert sonarr.get_tier_order(9) == []

    @patch('urllib.request.urlopen')
    def test_get_tier_order_cached(self, mock_urlopen, sonarr):
        # Consecutive calls within TTL hit the cache once.
        mock_urlopen.return_value = _mock_urlopen(_PROFILE_WITH_GROUP)
        first = sonarr.get_tier_order(4)
        second = sonarr.get_tier_order(4)
        assert first == second == ['2160p', '1080p']
        assert mock_urlopen.call_count == 1

    @patch('urllib.request.urlopen')
    def test_get_tier_order_parity_sonarr_radarr(self, mock_urlopen, sonarr, radarr):
        # I5 parity: identical profile fixture → identical tier output.
        # Two HTTP calls (one per client) — caches are per-client so the
        # second call does not pick up the first client's cached entry.
        mock_urlopen.side_effect = [
            _mock_urlopen(_PROFILE_WITH_GROUP),
            _mock_urlopen(_PROFILE_WITH_GROUP),
        ]
        sonarr_order = sonarr.get_tier_order(4)
        radarr_order = radarr.get_tier_order(4)
        assert sonarr_order == radarr_order == ['2160p', '1080p']

    @patch('urllib.request.urlopen')
    def test_get_tier_order_real_sonarr_any_profile(self, mock_urlopen, sonarr):
        # Regression guard — frozen copy of the actual ``/api/v3/qualityprofile/1``
        # response from a live Sonarr v3 instance for the "Any" profile.  The API
        # returns items ASCENDING by quality weight (SDTV, 480p variants, DVD,
        # 720p, 1080p variants, 2160p variants) — this is the engine's internal
        # ordering and pre-dates any UI-level preference sorting.  The compromise
        # engine expects preferred-first, so ``get_tier_order`` must reverse the
        # API output.  Without the reversal the engine treats 480p as the
        # "preferred" tier and compromise advancement goes UPWARD in quality —
        # the opposite of user intent.
        mock_urlopen.return_value = _mock_urlopen({
            'id': 1, 'name': 'Any',
            'cutoff': 7,
            'items': [
                {'quality': {'id': 0, 'name': 'Unknown'}, 'allowed': False, 'items': []},
                {'quality': {'id': 1, 'name': 'SDTV', 'resolution': 480},
                 'allowed': True, 'items': []},
                {'name': 'WEB 480p', 'allowed': True, 'items': [
                    {'quality': {'id': 12, 'name': 'WEBRip-480p', 'resolution': 480},
                     'allowed': True, 'items': []},
                    {'quality': {'id': 8, 'name': 'WEBDL-480p', 'resolution': 480},
                     'allowed': True, 'items': []},
                ]},
                {'quality': {'id': 2, 'name': 'DVD', 'resolution': 480},
                 'allowed': True, 'items': []},
                {'quality': {'id': 4, 'name': 'HDTV-720p', 'resolution': 720},
                 'allowed': True, 'items': []},
                {'quality': {'id': 5, 'name': 'HDTV-1080p', 'resolution': 1080},
                 'allowed': True, 'items': []},
                {'name': 'WEB 720p', 'allowed': True, 'items': [
                    {'quality': {'id': 14, 'name': 'WEBRip-720p', 'resolution': 720},
                     'allowed': True, 'items': []},
                    {'quality': {'id': 5, 'name': 'WEBDL-720p', 'resolution': 720},
                     'allowed': True, 'items': []},
                ]},
                {'name': 'WEB 1080p', 'allowed': True, 'items': [
                    {'quality': {'id': 15, 'name': 'WEBRip-1080p', 'resolution': 1080},
                     'allowed': True, 'items': []},
                    {'quality': {'id': 3, 'name': 'WEBDL-1080p', 'resolution': 1080},
                     'allowed': True, 'items': []},
                ]},
                {'quality': {'id': 7, 'name': 'Bluray-1080p', 'resolution': 1080},
                 'allowed': True, 'items': []},
            ],
        })
        # Preferred-first: 1080p (the user's top-allowed tier) → 720p → 480p.
        # A fall-through to 720p must NEVER be reached before a 1080p probe.
        assert sonarr.get_tier_order(1) == ['1080p', '720p', '480p']

    @patch('urllib.request.urlopen')
    def test_profile_cache_is_per_client(self, mock_urlopen, sonarr, radarr):
        # Each client has its own cache — Sonarr caching profile 4 must not
        # affect Radarr's fetch for the same ID.
        mock_urlopen.side_effect = [
            _mock_urlopen(_PROFILE_WITH_GROUP),
            _mock_urlopen(_PROFILE_WITH_GROUP),
        ]
        sonarr.get_quality_profile(4)
        sonarr.get_quality_profile(4)  # cache hit
        radarr.get_quality_profile(4)  # separate cache — cache miss
        assert mock_urlopen.call_count == 2


class TestProfileIdLookups:
    """Phase 1 — convenience accessors off the series/movie record."""

    @patch('urllib.request.urlopen')
    def test_get_profile_id_for_series(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen({'id': 7, 'qualityProfileId': 4})
        assert sonarr.get_profile_id_for_series(7) == 4

    @patch('urllib.request.urlopen')
    def test_get_profile_id_for_series_missing_field(self, mock_urlopen, sonarr):
        mock_urlopen.return_value = _mock_urlopen({'id': 7})
        assert sonarr.get_profile_id_for_series(7) is None

    @patch('urllib.request.urlopen')
    def test_get_profile_id_for_series_not_found(self, mock_urlopen, sonarr):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            'http://sonarr:8989/api/v3/series/99', 404, 'Not Found', {}, None
        )
        assert sonarr.get_profile_id_for_series(99) is None

    @patch('urllib.request.urlopen')
    def test_get_profile_id_for_movie(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen({'id': 12, 'qualityProfileId': 2})
        assert radarr.get_profile_id_for_movie(12) == 2

    @patch('urllib.request.urlopen')
    def test_get_profile_id_for_movie_missing_field(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen({'id': 12})
        assert radarr.get_profile_id_for_movie(12) is None

    @patch('urllib.request.urlopen')
    def test_get_profile_id_rejects_bool(self, mock_urlopen, sonarr):
        # bool is-a int in Python — a buggy serialiser returning True must
        # not be promoted to profile ID 1 (mirrors _is_number bool guard).
        mock_urlopen.return_value = _mock_urlopen({'id': 7, 'qualityProfileId': True})
        assert sonarr.get_profile_id_for_series(7) is None

    @patch('urllib.request.urlopen')
    def test_get_profile_id_rejects_non_positive(self, mock_urlopen, radarr):
        mock_urlopen.return_value = _mock_urlopen({'id': 12, 'qualityProfileId': 0})
        assert radarr.get_profile_id_for_movie(12) is None
