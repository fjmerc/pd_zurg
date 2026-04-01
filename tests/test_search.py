"""Tests for utils/search.py — Torrentio search, cache checks, and debrid add."""

import json
import os
import sys
import pytest
from unittest.mock import patch, MagicMock

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.search import (
    parse_quality,
    _parse_size_bytes,
    _parse_seeds,
    _parse_size_from_title,
    _parse_source,
    search_torrentio,
    check_cache,
    search_with_cache,
    add_to_debrid,
)


# ---------------------------------------------------------------------------
# Torrentio response parsing
# ---------------------------------------------------------------------------

SAMPLE_TORRENTIO_RESPONSE = {
    'streams': [
        {
            'name': 'Torrentio\n4k',
            'title': 'Movie.Name.2024.2160p.WEB-DL.DDP5.1.Atmos.DV.x265-GROUP\n'
                     '👤 250 💾 8.5 GB ⚙️ TorrentGalaxy',
            'infoHash': 'a' * 40,
        },
        {
            'name': 'Torrentio\n1080p',
            'title': 'Movie.Name.2024.1080p.BluRay.x264-OTHER\n'
                     '👤 120 💾 4.2 GB ⚙️ RARBG',
            'infoHash': 'b' * 40,
        },
        {
            'name': 'Torrentio\n720p',
            'title': 'Movie.Name.2024.720p.WEBRip.x264\n'
                     '👤 30 💾 1.8 GB ⚙️ 1337x',
            'infoHash': 'c' * 40,
        },
        {
            'name': 'Torrentio',
            'title': 'Movie.Name.2024.HDTV\n👤 5 💾 700 MB',
            'infoHash': 'd' * 40,
        },
    ]
}


class TestParseQuality:

    def test_2160p(self):
        q = parse_quality('Movie.2024.2160p.WEB-DL')
        assert q['label'] == '2160p'
        assert q['score'] == 4

    def test_4k(self):
        q = parse_quality('Movie.2024.4K.HDR')
        assert q['label'] == '2160p'
        assert q['score'] == 4

    def test_1080p(self):
        q = parse_quality('Movie.2024.1080p.BluRay')
        assert q['label'] == '1080p'
        assert q['score'] == 3

    def test_720p(self):
        q = parse_quality('Movie.2024.720p.WEBRip')
        assert q['label'] == '720p'
        assert q['score'] == 2

    def test_480p(self):
        q = parse_quality('Movie.2024.480p.DVDRip')
        assert q['label'] == '480p'
        assert q['score'] == 1

    def test_unknown(self):
        q = parse_quality('Movie.2024.HDTV')
        assert q['label'] == 'Unknown'
        assert q['score'] == 0

    def test_case_insensitive(self):
        q = parse_quality('movie.2024.UHD.remux')
        assert q['label'] == '2160p'


class TestParseSizeBytes:

    def test_gb(self):
        assert _parse_size_bytes('4.2 GB') == int(4.2 * 1024**3)

    def test_mb(self):
        assert _parse_size_bytes('700 MB') == int(700 * 1024**2)

    def test_tb(self):
        assert _parse_size_bytes('1.5 TB') == int(1.5 * 1024**4)

    def test_empty(self):
        assert _parse_size_bytes('') == 0

    def test_no_match(self):
        assert _parse_size_bytes('no size here') == 0


class TestParseSeeds:

    def test_emoji_format(self):
        assert _parse_seeds('👤 250 💾 8 GB') == 250

    def test_zero(self):
        assert _parse_seeds('no seeders listed') == 0


class TestParseSizeFromTitle:

    def test_emoji_format(self):
        assert _parse_size_from_title('👤 250 💾 8.5 GB ⚙️ Source') == '8.5 GB'

    def test_plain_format(self):
        assert _parse_size_from_title('Size: 4.2 GB') == '4.2 GB'


class TestParseSource:

    def test_emoji_format(self):
        assert _parse_source('👤 250 💾 8.5 GB ⚙️ TorrentGalaxy') == 'TorrentGalaxy'

    def test_no_source(self):
        assert _parse_source('no source info') == ''


class TestSearchTorrentio:

    @patch('utils.search._urllib_get')
    def test_parse_torrentio_response(self, mock_get, monkeypatch):
        """Mock Torrentio response JSON, verify parsed results."""
        monkeypatch.setenv('TORRENTIO_URL', 'https://torrentio.strem.fun')
        mock_get.return_value = SAMPLE_TORRENTIO_RESPONSE

        results = search_torrentio('tt1234567', media_type='movie')

        assert len(results) == 4
        assert results[0]['info_hash'] == 'a' * 40
        assert results[0]['quality']['label'] == '2160p'
        assert results[0]['quality']['score'] == 4
        assert results[0]['seeds'] == 250
        assert results[0]['size_bytes'] == int(8.5 * 1024**3)
        assert results[0]['source_name'] == 'TorrentGalaxy'

        assert results[1]['quality']['label'] == '1080p'
        assert results[2]['quality']['label'] == '720p'
        assert results[3]['quality']['label'] == 'Unknown'

    @patch('utils.search._urllib_get')
    def test_series_url_format(self, mock_get, monkeypatch):
        """Series search should include season:episode in URL."""
        monkeypatch.setenv('TORRENTIO_URL', 'https://torrentio.strem.fun')
        mock_get.return_value = {'streams': []}

        search_torrentio('tt1234567', media_type='series', season=2, episode=5)

        call_url = mock_get.call_args[0][0]
        assert '/stream/series/tt1234567:2:5.json' in call_url

    @patch('utils.search._urllib_get')
    def test_deduplication(self, mock_get, monkeypatch):
        """Duplicate info hashes should be filtered."""
        monkeypatch.setenv('TORRENTIO_URL', 'https://torrentio.strem.fun')
        mock_get.return_value = {
            'streams': [
                {'infoHash': 'a' * 40, 'title': 'First', 'name': 'T'},
                {'infoHash': 'a' * 40, 'title': 'Duplicate', 'name': 'T'},
            ]
        }
        results = search_torrentio('tt1234567')
        assert len(results) == 1

    def test_no_url_configured(self, monkeypatch):
        """Should return empty list if TORRENTIO_URL not set."""
        monkeypatch.delenv('TORRENTIO_URL', raising=False)
        results = search_torrentio('tt1234567')
        assert results == []

    def test_invalid_imdb_id(self, monkeypatch):
        """Should return empty list for invalid IMDb IDs."""
        monkeypatch.setenv('TORRENTIO_URL', 'https://torrentio.strem.fun')
        assert search_torrentio('') == []
        assert search_torrentio('invalid') == []
        assert search_torrentio('tt') == []
        assert search_torrentio('tt../../admin') == []
        assert search_torrentio('tt12345') == []  # too short

    @patch('utils.search._urllib_get')
    def test_empty_streams(self, mock_get, monkeypatch):
        """Should handle empty response gracefully."""
        monkeypatch.setenv('TORRENTIO_URL', 'https://torrentio.strem.fun')
        mock_get.return_value = {'streams': []}
        assert search_torrentio('tt1234567') == []

    @patch('utils.search._urllib_get')
    def test_api_error(self, mock_get, monkeypatch):
        """Should return empty list on API error."""
        monkeypatch.setenv('TORRENTIO_URL', 'https://torrentio.strem.fun')
        mock_get.return_value = None
        assert search_torrentio('tt1234567') == []


class TestSearchWithCacheMerge:

    @patch('utils.search.check_cache')
    @patch('utils.search.search_torrentio')
    def test_cached_field_set_correctly(self, mock_search, mock_cache):
        """Verify cached field is set correctly from cache check."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'R1', 'seeds': 100,
             'quality': {'label': '1080p', 'score': 3}, 'size_bytes': 1000, 'source_name': 'S'},
            {'info_hash': 'b' * 40, 'title': 'R2', 'seeds': 50,
             'quality': {'label': '720p', 'score': 2}, 'size_bytes': 500, 'source_name': 'S'},
        ]
        mock_cache.return_value = {'a' * 40}

        results = search_with_cache('tt1234567')

        cached_result = next(r for r in results if r['info_hash'] == 'a' * 40)
        not_cached = next(r for r in results if r['info_hash'] == 'b' * 40)
        assert cached_result['cached'] is True
        assert not_cached['cached'] is False

    @patch('utils.search.check_cache')
    @patch('utils.search.search_torrentio')
    def test_blocklist_filtering(self, mock_search, mock_cache):
        """Verify blocked hashes are excluded from results."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'R1', 'seeds': 100,
             'quality': {'label': '1080p', 'score': 3}, 'size_bytes': 1000, 'source_name': 'S'},
            {'info_hash': 'b' * 40, 'title': 'R2', 'seeds': 50,
             'quality': {'label': '720p', 'score': 2}, 'size_bytes': 500, 'source_name': 'S'},
        ]
        mock_cache.return_value = set()

        with patch('utils.blocklist.is_blocked', side_effect=lambda h: h == 'a' * 40):
            results = search_with_cache('tt1234567')

        hashes = [r['info_hash'] for r in results]
        assert 'a' * 40 not in hashes
        assert 'b' * 40 in hashes

    @patch('utils.search.check_cache')
    @patch('utils.search.search_torrentio')
    def test_sort_order(self, mock_search, mock_cache):
        """Verify sort: cached first, then quality desc, then seeds desc."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'R1', 'seeds': 10,
             'quality': {'label': '720p', 'score': 2}, 'size_bytes': 500, 'source_name': 'S'},
            {'info_hash': 'b' * 40, 'title': 'R2', 'seeds': 200,
             'quality': {'label': '1080p', 'score': 3}, 'size_bytes': 1000, 'source_name': 'S'},
            {'info_hash': 'c' * 40, 'title': 'R3', 'seeds': 50,
             'quality': {'label': '2160p', 'score': 4}, 'size_bytes': 2000, 'source_name': 'S'},
        ]
        # Only 'a' is cached
        mock_cache.return_value = {'a' * 40}

        results = search_with_cache('tt1234567')

        # 'a' is cached so it should be first despite lower quality
        assert results[0]['info_hash'] == 'a' * 40
        assert results[0]['cached'] is True
        # Then non-cached sorted by quality desc: c (2160p) > b (1080p)
        assert results[1]['info_hash'] == 'c' * 40
        assert results[2]['info_hash'] == 'b' * 40


class TestAddToDebrid:

    @patch('utils.search._urllib_post')
    @patch('utils.search._get_debrid_service')
    def test_add_to_rd_success(self, mock_service, mock_post):
        """RD add should call addMagnet then selectFiles."""
        mock_service.return_value = ('realdebrid', 'test_key')
        mock_post.side_effect = [
            {'id': 'ABC123'},  # addMagnet response
            {},                # selectFiles response
        ]

        result = add_to_debrid('a' * 40, title='Test Movie')

        assert result['success'] is True
        assert result['torrent_id'] == 'ABC123'
        assert result['service'] == 'realdebrid'
        assert mock_post.call_count == 2

    @patch('utils.search._get_debrid_service')
    def test_no_service_configured(self, mock_service):
        """Should fail gracefully when no debrid service configured."""
        mock_service.return_value = (None, None)
        result = add_to_debrid('a' * 40)
        assert result['success'] is False
        assert 'No debrid service' in result['error']

    def test_invalid_hash(self):
        """Should reject invalid info hashes."""
        result = add_to_debrid('invalid')
        assert result['success'] is False
        assert 'Invalid' in result['error']

    def test_empty_hash(self):
        """Should reject empty hashes."""
        result = add_to_debrid('')
        assert result['success'] is False

    @patch('utils.search._urllib_post')
    @patch('utils.search._get_debrid_service')
    def test_add_to_ad_success(self, mock_service, mock_post):
        """AD add should succeed with correct response."""
        mock_service.return_value = ('alldebrid', 'test_key')
        mock_post.return_value = {
            'status': 'success',
            'data': {'magnets': [{'id': 456}]},
        }

        result = add_to_debrid('b' * 40)
        assert result['success'] is True
        assert result['torrent_id'] == '456'
        assert result['service'] == 'alldebrid'

    @patch('utils.search._urllib_post')
    @patch('utils.search._get_debrid_service')
    def test_add_to_tb_success(self, mock_service, mock_post):
        """TorBox add should succeed with correct response."""
        mock_service.return_value = ('torbox', 'test_key')
        mock_post.return_value = {
            'success': True,
            'data': {'torrent_id': 789},
        }

        result = add_to_debrid('c' * 40)
        assert result['success'] is True
        assert result['torrent_id'] == '789'
        assert result['service'] == 'torbox'

    @patch('utils.search._urllib_post')
    @patch('utils.search._get_debrid_service')
    def test_add_failure_returns_error(self, mock_service, mock_post):
        """Should return error dict on API failure."""
        mock_service.return_value = ('realdebrid', 'test_key')
        mock_post.return_value = None  # API error

        result = add_to_debrid('a' * 40, title='Test')
        assert result['success'] is False
        assert result['error'] != ''
