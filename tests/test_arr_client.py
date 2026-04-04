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
        """prefer_debrid=True with existing files should interactive-grab each episode."""
        sonarr._blackhole_tag_id = 7
        responses = [
            _mock_urlopen([{'id': 5, 'title': 'Tulsa King', 'tmdbId': 200, 'tags': [7]}]),  # find
            _mock_urlopen([  # episodes — both have files
                {'id': 100, 'seasonNumber': 1, 'episodeNumber': 1, 'hasFile': True},
                {'id': 101, 'seasonNumber': 1, 'episodeNumber': 2, 'hasFile': True},
            ]),
            _mock_urlopen({'records': []}),  # queue cleanup
            # Episode 100 interactive search + push
            _mock_urlopen([
                {'guid': 'ep1', 'indexerId': 2, 'protocol': 'torrent', 'title': 'S01E01', 'rejected': True},
            ]),
            _mock_urlopen({'id': 99}),
            # Episode 101 interactive search + push
            _mock_urlopen([
                {'guid': 'ep2', 'indexerId': 2, 'protocol': 'torrent', 'title': 'S01E02', 'rejected': True},
            ]),
            _mock_urlopen({'id': 98}),
        ]
        mock_urlopen.side_effect = responses
        result = sonarr.ensure_and_search('Tulsa King', 200, 1, [1, 2], prefer_debrid=True)
        assert result['status'] == 'sent'
        assert 'Force-grabbed 2' in result['message']

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
                {'guid': 'pack', 'indexerId': 2, 'protocol': 'torrent', 'title': 'Season.Pack', 'rejected': False},
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
    def test_grab_debrid_release_ignores_rejected_flag(self, mock_urlopen, sonarr):
        """Rejected flag is ignored — force-grab bypasses quality cutoff."""
        responses = [
            _mock_urlopen([
                {'guid': 'first', 'indexerId': 1, 'protocol': 'torrent', 'title': 'Release', 'rejected': True},
            ]),
            _mock_urlopen({'id': 1}),  # push
        ]
        mock_urlopen.side_effect = responses
        result = sonarr._grab_debrid_release(100, title='Test')
        assert result is True
        push_body = json.loads(mock_urlopen.call_args_list[-1][0][0].data)
        assert push_body['guid'] == 'first'

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
        assert result is False

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
