"""Tests for utils/search.py — Torrentio search and debrid add."""

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
    search_torrents,
    add_to_debrid,
    check_debrid_cache,
    _coerce_instant,
    _TORBOX_MAX_PROBES,
    _existing_hashes,
    remember_added_hash,
    invalidate_existing_hashes_cache,
)


@pytest.fixture(autouse=True)
def _reset_search_env_and_cache(monkeypatch):
    """Each test starts with gates at their default values and the dedup
    cache empty so cross-test pollution (e.g. a previous test priming the
    cache with ``remember_added_hash``) cannot leak into the next."""
    monkeypatch.delenv('SEARCH_DEDUP_ENABLED', raising=False)
    monkeypatch.delenv('SEARCH_REQUIRE_CACHED', raising=False)
    invalidate_existing_hashes_cache()
    yield
    invalidate_existing_hashes_cache()


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


class TestSearchSortAndFilter:

    @patch('utils.search.search_torrentio')
    def test_blocklist_filtering(self, mock_search):
        """Verify blocked hashes are excluded from results."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'R1', 'seeds': 100,
             'quality': {'label': '1080p', 'score': 3}, 'size_bytes': 1000, 'source_name': 'S'},
            {'info_hash': 'b' * 40, 'title': 'R2', 'seeds': 50,
             'quality': {'label': '720p', 'score': 2}, 'size_bytes': 500, 'source_name': 'S'},
        ]

        with patch('utils.blocklist.is_blocked', side_effect=lambda h: h == 'a' * 40):
            results = search_torrents('tt1234567')

        hashes = [r['info_hash'] for r in results]
        assert 'a' * 40 not in hashes
        assert 'b' * 40 in hashes

    @patch('utils.search.search_torrentio')
    def test_sort_order(self, mock_search):
        """Verify sort: quality desc, then seeds desc."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'R1', 'seeds': 10,
             'quality': {'label': '720p', 'score': 2}, 'size_bytes': 500, 'source_name': 'S'},
            {'info_hash': 'b' * 40, 'title': 'R2', 'seeds': 200,
             'quality': {'label': '1080p', 'score': 3}, 'size_bytes': 1000, 'source_name': 'S'},
            {'info_hash': 'c' * 40, 'title': 'R3', 'seeds': 50,
             'quality': {'label': '2160p', 'score': 4}, 'size_bytes': 2000, 'source_name': 'S'},
        ]

        results = search_torrents('tt1234567')

        # Sorted by quality desc: c (2160p) > b (1080p) > a (720p)
        assert results[0]['info_hash'] == 'c' * 40
        assert results[1]['info_hash'] == 'b' * 40
        assert results[2]['info_hash'] == 'a' * 40


class TestAddToDebrid:

    @pytest.fixture(autouse=True)
    def _stub_dedup_probe(self, monkeypatch):
        """Stub the debrid-account list call so the dedup gate never makes a
        real HTTP request.  Each test that needs to exercise dedup can still
        patch ``_existing_hashes`` directly."""
        monkeypatch.setattr('utils.search._existing_hashes',
                            lambda *a, **kw: set())

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


# ---------------------------------------------------------------------------
# Debrid cache probe (plan 33 Phase 3)
# ---------------------------------------------------------------------------


def _mock_urlopen_response(payload):
    """Build a context-manager mock that yields *payload* as JSON bytes."""
    resp = MagicMock()
    resp.read.return_value = json.dumps(payload).encode('utf-8')
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return resp


class TestCheckDebridCache:
    """check_debrid_cache contract — batch probe, unknown semantics,
    provider dispatch, URL redaction."""

    def test_empty_input_returns_empty(self):
        assert check_debrid_cache([]) == {}
        assert check_debrid_cache(None) == {}

    def test_invalid_hashes_filtered(self):
        """Non-string and non-hex entries must be dropped before probing."""
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = (None, None)
            result = check_debrid_cache(['not-a-hash', 'a' * 40, None, 123, 'b' * 39])
        # Only the 40-char hex passes through
        assert list(result.keys()) == ['a' * 40]

    def test_hash_dedup_preserves_order(self):
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = (None, None)
            result = check_debrid_cache(['a' * 40, 'b' * 40, 'a' * 40])
        assert list(result.keys()) == ['a' * 40, 'b' * 40]

    def test_no_service_configured_returns_none_map(self):
        """All hashes map to None when no debrid is configured — the
        'unknown, safe default' branch of the I4 contract."""
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = (None, None)
            result = check_debrid_cache(['a' * 40, 'b' * 40])
        assert result == {'a' * 40: None, 'b' * 40: None}

    def test_real_debrid_returns_unknown(self):
        """RD deprecated instantAvailability Nov 2024 — probe is a
        deliberate no-op that returns None uniformly so compromise
        logic treats RD responses as 'unknown' (safe default refuses
        escalation unless QUALITY_COMPROMISE_ONLY_CACHED=false)."""
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('realdebrid', 'rd-key')
            result = check_debrid_cache(['a' * 40, 'b' * 40])
        assert result == {'a' * 40: None, 'b' * 40: None}

    def test_real_debrid_does_not_hit_network(self):
        """Regression: the RD stub must NOT emit an HTTP call — the
        deprecated endpoint would just return {} but a stray call
        wastes an RD API-rate-limit slot on every compromise decision."""
        with patch('utils.search._get_debrid_service') as ms, \
             patch('urllib.request.urlopen') as mock_urlopen:
            ms.return_value = ('realdebrid', 'rd-key')
            check_debrid_cache(['a' * 40])
            assert mock_urlopen.call_count == 0

    @patch('urllib.request.urlopen')
    def test_alldebrid_batch_success(self, mock_urlopen):
        """AD returns the batch in a single call; True/False mapped
        back by the hash the API echoes (not by list index, so a
        dropped entry can't mis-tag another hash)."""
        mock_urlopen.return_value = _mock_urlopen_response({
            'status': 'success',
            'data': {
                'magnets': [
                    {'hash': 'a' * 40, 'instant': True},
                    {'hash': 'b' * 40, 'instant': False},
                ],
            },
        })
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('alldebrid', 'ad-key')
            result = check_debrid_cache(['a' * 40, 'b' * 40])
        assert result == {'a' * 40: True, 'b' * 40: False}
        assert mock_urlopen.call_count == 1

    @patch('urllib.request.urlopen')
    def test_alldebrid_missing_hash_defaults_to_none(self, mock_urlopen):
        """AD dropping a hash from the response must leave that hash
        as None (unknown), not False (safe conservatism: absence is
        not evidence of uncached)."""
        mock_urlopen.return_value = _mock_urlopen_response({
            'status': 'success',
            'data': {'magnets': [{'hash': 'a' * 40, 'instant': True}]},
        })
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('alldebrid', 'ad-key')
            result = check_debrid_cache(['a' * 40, 'b' * 40])
        assert result == {'a' * 40: True, 'b' * 40: None}

    @patch('urllib.request.urlopen')
    def test_alldebrid_status_failure_returns_none_map(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen_response({
            'status': 'error', 'data': {'error': {'message': 'bad key'}},
        })
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('alldebrid', 'ad-key')
            result = check_debrid_cache(['a' * 40])
        assert result == {'a' * 40: None}

    @patch('urllib.request.urlopen')
    def test_alldebrid_timeout_returns_none_map(self, mock_urlopen):
        """Silent failure returns None for every hash — per the plan
        contract, the caller decides whether to treat unknown as
        'not cached' or 'assume cached'."""
        import socket
        mock_urlopen.side_effect = socket.timeout('timed out')
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('alldebrid', 'ad-key')
            result = check_debrid_cache(['a' * 40, 'b' * 40])
        assert result == {'a' * 40: None, 'b' * 40: None}

    @patch('urllib.request.urlopen')
    def test_alldebrid_url_redaction(self, mock_urlopen, caplog):
        """API key must NOT leak into warning logs on probe failure.
        Query string (with apikey) is stripped by _safe_log_url."""
        import logging
        mock_urlopen.side_effect = OSError('boom')
        with patch('utils.search._get_debrid_service') as ms, \
             caplog.at_level(logging.WARNING, logger='ProjectDebridZurg'):
            ms.return_value = ('alldebrid', 'SUPER-SECRET-KEY-42')
            check_debrid_cache(['a' * 40])
        for record in caplog.records:
            assert 'SUPER-SECRET-KEY-42' not in record.message
            assert 'apikey' not in record.message

    @patch('urllib.request.urlopen')
    def test_torbox_per_hash_success(self, mock_urlopen):
        """TB returns per-hash payload; presence in ``data`` dict = cached."""
        mock_urlopen.side_effect = [
            _mock_urlopen_response({
                'success': True,
                'data': {'a' * 40: {'name': 'some.file'}},
            }),
            _mock_urlopen_response({'success': True, 'data': {}}),
        ]
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('torbox', 'tb-key')
            result = check_debrid_cache(['a' * 40, 'b' * 40])
        assert result == {'a' * 40: True, 'b' * 40: False}
        assert mock_urlopen.call_count == 2

    @patch('urllib.request.urlopen')
    def test_torbox_failure_per_hash_isolates_unknowns(self, mock_urlopen):
        """A failure on one hash must not poison the other — each
        probe is independent, and partial info is better than none."""
        mock_urlopen.side_effect = [
            _mock_urlopen_response({
                'success': True, 'data': {'a' * 40: {'name': 'ok'}},
            }),
            OSError('transient'),
        ]
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('torbox', 'tb-key')
            result = check_debrid_cache(['a' * 40, 'b' * 40])
        assert result == {'a' * 40: True, 'b' * 40: None}

    @patch('urllib.request.urlopen')
    def test_torbox_url_redaction(self, mock_urlopen, caplog):
        import logging
        mock_urlopen.side_effect = OSError('boom')
        with patch('utils.search._get_debrid_service') as ms, \
             caplog.at_level(logging.WARNING, logger='ProjectDebridZurg'):
            ms.return_value = ('torbox', 'TB-SECRET-XYZ')
            check_debrid_cache(['a' * 40])
        for record in caplog.records:
            assert 'TB-SECRET-XYZ' not in record.message

    @patch('urllib.request.urlopen')
    def test_alldebrid_uppercase_hash_in_response(self, mock_urlopen):
        """Defensive: AD could return uppercase hashes.  The membership
        check lowercases before comparing so the correct mapping still
        holds — flagging a regression if someone removes that guard."""
        mock_urlopen.return_value = _mock_urlopen_response({
            'status': 'success',
            'data': {
                'magnets': [
                    {'hash': ('A' * 40), 'instant': True},
                ],
            },
        })
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('alldebrid', 'ad-key')
            result = check_debrid_cache(['a' * 40])
        assert result == {'a' * 40: True}

    @patch('urllib.request.urlopen')
    def test_alldebrid_coerces_string_instant(self, mock_urlopen):
        """Defensive: if AD ever serialises instant as 'true'/'false'
        strings, the coercion helper must still yield a bool rather
        than dropping the value to None."""
        mock_urlopen.return_value = _mock_urlopen_response({
            'status': 'success',
            'data': {
                'magnets': [
                    {'hash': 'a' * 40, 'instant': 'true'},
                    {'hash': 'b' * 40, 'instant': 'FALSE'},
                ],
            },
        })
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('alldebrid', 'ad-key')
            result = check_debrid_cache(['a' * 40, 'b' * 40])
        assert result == {'a' * 40: True, 'b' * 40: False}

    @patch('urllib.request.urlopen')
    def test_alldebrid_response_cannot_poison_with_extra_hashes(self, mock_urlopen):
        """A hostile/buggy AD response echoing hashes the caller did
        not ask about must not inject keys into the result map."""
        mock_urlopen.return_value = _mock_urlopen_response({
            'status': 'success',
            'data': {
                'magnets': [
                    {'hash': 'a' * 40, 'instant': True},
                    # Not requested — must be ignored
                    {'hash': 'c' * 40, 'instant': True},
                ],
            },
        })
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('alldebrid', 'ad-key')
            result = check_debrid_cache(['a' * 40, 'b' * 40])
        assert result == {'a' * 40: True, 'b' * 40: None}
        assert 'c' * 40 not in result

    def test_coerce_instant_helper(self):
        assert _coerce_instant(True) is True
        assert _coerce_instant(False) is False
        assert _coerce_instant('true') is True
        assert _coerce_instant('FALSE') is False
        assert _coerce_instant(' True ') is True
        assert _coerce_instant(None) is None
        assert _coerce_instant(1) is None  # int is not a bool truthiness — safe
        assert _coerce_instant('maybe') is None

    @patch('urllib.request.urlopen')
    def test_torbox_none_payload_returns_none_not_false(self, mock_urlopen):
        """I4: unknown (null payload / non-dict) must stay None — not
        be conflated with 'confirmed uncached'.  Previously the code
        treated non-dict payload as False, violating the plan's
        'caller decides how to treat unknown' contract."""
        mock_urlopen.return_value = _mock_urlopen_response({
            'success': True, 'data': None,
        })
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('torbox', 'tb-key')
            result = check_debrid_cache(['a' * 40])
        assert result == {'a' * 40: None}

    @patch('urllib.request.urlopen')
    def test_torbox_non_dict_payload_returns_none(self, mock_urlopen):
        """Similar: a list payload (broken TB response) also → None."""
        mock_urlopen.return_value = _mock_urlopen_response({
            'success': True, 'data': ['not', 'a', 'dict'],
        })
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('torbox', 'tb-key')
            result = check_debrid_cache(['a' * 40])
        assert result == {'a' * 40: None}

    @patch('urllib.request.urlopen')
    def test_torbox_caps_hash_fan_out(self, mock_urlopen):
        """HIGH: unbounded TB per-hash calls is a DoS vector
        (50+ Torrentio results × 10 s timeout = 8-min worker stall).
        The cap keeps the wall-clock budget bounded; hashes beyond
        the cap stay as None (unknown)."""
        mock_urlopen.return_value = _mock_urlopen_response({
            'success': True, 'data': {},
        })
        n = _TORBOX_MAX_PROBES + 5
        # Build N unique valid hashes (hex 40 chars each)
        hashes = [f'{i:040x}' for i in range(n)]
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('torbox', 'tb-key')
            result = check_debrid_cache(hashes)
        # Exactly _TORBOX_MAX_PROBES HTTP calls — the overflow hashes
        # never hit the network.
        assert mock_urlopen.call_count == _TORBOX_MAX_PROBES
        # Probed hashes get a confirmed False (empty dict = uncached);
        # un-probed overflow hashes stay as None (unknown).
        probed = [h for h, v in result.items() if v is False]
        unprobed = [h for h, v in result.items() if v is None]
        assert len(probed) == _TORBOX_MAX_PROBES
        assert len(unprobed) == 5

    def test_rd_warning_emits_once(self, caplog):
        """Users with RD + only-cached mode must see a one-time warning
        explaining why compromise never fires.  Repeated probes must
        not spam the log."""
        import logging
        import utils.search as search_mod
        # Reset the module-level flag so test order doesn't hide the emit
        search_mod._rd_cache_warning_emitted = False
        with patch('utils.search._get_debrid_service') as ms, \
             caplog.at_level(logging.WARNING, logger='ProjectDebridZurg'):
            ms.return_value = ('realdebrid', 'rd-key')
            check_debrid_cache(['a' * 40])
            check_debrid_cache(['b' * 40])
            check_debrid_cache(['c' * 40])
        rd_msgs = [r for r in caplog.records if 'RD' in r.message or 'Real-Debrid' in r.message]
        assert len(rd_msgs) == 1
        assert 'deprecated' in rd_msgs[0].message.lower()
        # Reset so other tests start from a clean slate
        search_mod._rd_cache_warning_emitted = False

    def test_service_override(self):
        """Explicit service+key must bypass auto-detect."""
        with patch('utils.search._get_debrid_service') as ms:
            ms.return_value = ('realdebrid', 'auto-key')  # auto-detected RD
            # Override to an unconfigured service — api_key must also be
            # provided to enter the dispatch path
            result = check_debrid_cache(['a' * 40], service='unknown-service',
                                        api_key='x')
            # Unknown service falls through to the default None map
            assert result == {'a' * 40: None}
            # Auto-detect must NOT have been consulted when both overrides
            # supplied
            ms.assert_not_called()


class TestSearchTorrentsCacheAnnotation:
    """search_torrents annotate_cache / sort_mode kwargs (plan 33 Phase 3)."""

    @patch('utils.search.search_torrentio')
    def test_annotate_cache_default_off(self, mock_search):
        """Default annotate_cache=False: results carry no cached field.
        The manual-search UI's behaviour is preserved unchanged."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'R1', 'seeds': 100,
             'quality': {'label': '1080p', 'score': 3},
             'size_bytes': 1000, 'source_name': 'S'},
        ]
        results = search_torrents('tt1234567')
        assert 'cached' not in results[0]
        assert 'cached_service' not in results[0]

    @patch('utils.search.check_debrid_cache')
    @patch('utils.search._get_debrid_service')
    @patch('utils.search.search_torrentio')
    def test_annotate_cache_populates_fields(self, mock_search, mock_service,
                                             mock_check):
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'R1', 'seeds': 100,
             'quality': {'label': '2160p', 'score': 4},
             'size_bytes': 1000, 'source_name': 'S'},
            {'info_hash': 'b' * 40, 'title': 'R2', 'seeds': 50,
             'quality': {'label': '1080p', 'score': 3},
             'size_bytes': 500, 'source_name': 'S'},
        ]
        mock_service.return_value = ('alldebrid', 'ad-key')
        mock_check.return_value = {'a' * 40: False, 'b' * 40: True}
        results = search_torrents('tt1234567', annotate_cache=True)
        by_hash = {r['info_hash']: r for r in results}
        assert by_hash['a' * 40]['cached'] is False
        assert by_hash['a' * 40]['cached_service'] == 'alldebrid'
        assert by_hash['b' * 40]['cached'] is True
        assert by_hash['b' * 40]['cached_service'] == 'alldebrid'

    @patch('utils.search.check_debrid_cache')
    @patch('utils.search._get_debrid_service')
    @patch('utils.search.search_torrentio')
    def test_cached_first_sort_outranks_higher_quality_uncached(
            self, mock_search, mock_service, mock_check):
        """Plan 33 core demo: cached 1080p ranks above uncached 2160p
        under sort_mode='cached_first' — the user gets something that
        streams immediately rather than something that makes them wait."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'Uncached 2160p', 'seeds': 500,
             'quality': {'label': '2160p', 'score': 4},
             'size_bytes': 8000, 'source_name': 'S'},
            {'info_hash': 'b' * 40, 'title': 'Cached 1080p', 'seeds': 10,
             'quality': {'label': '1080p', 'score': 3},
             'size_bytes': 4000, 'source_name': 'S'},
        ]
        mock_service.return_value = ('alldebrid', 'k')
        mock_check.return_value = {'a' * 40: False, 'b' * 40: True}
        results = search_torrents('tt1234567', sort_mode='cached_first')
        assert results[0]['info_hash'] == 'b' * 40  # cached 1080p on top
        assert results[1]['info_hash'] == 'a' * 40

    @patch('utils.search.check_debrid_cache')
    @patch('utils.search._get_debrid_service')
    @patch('utils.search.search_torrentio')
    def test_cached_first_sort_unknown_treated_as_uncached(
            self, mock_search, mock_service, mock_check):
        """cached=None must not promote a release — we only boost to
        the top when the provider confirms True.  Unknown stays with
        uncached."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'Unknown 2160p', 'seeds': 100,
             'quality': {'label': '2160p', 'score': 4},
             'size_bytes': 1000, 'source_name': 'S'},
            {'info_hash': 'b' * 40, 'title': 'Cached 720p', 'seeds': 1,
             'quality': {'label': '720p', 'score': 2},
             'size_bytes': 500, 'source_name': 'S'},
        ]
        mock_service.return_value = ('realdebrid', 'k')
        mock_check.return_value = {'a' * 40: None, 'b' * 40: True}
        results = search_torrents('tt1234567', sort_mode='cached_first')
        assert results[0]['info_hash'] == 'b' * 40  # cached wins over unknown

    @patch('utils.search.check_debrid_cache')
    @patch('utils.search._get_debrid_service')
    @patch('utils.search.search_torrentio')
    def test_cached_first_ties_fall_back_to_quality(
            self, mock_search, mock_service, mock_check):
        """Among all-cached or all-uncached, sort falls back to the
        existing quality-then-seeders order."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'Cached 720p', 'seeds': 999,
             'quality': {'label': '720p', 'score': 2},
             'size_bytes': 500, 'source_name': 'S'},
            {'info_hash': 'b' * 40, 'title': 'Cached 2160p', 'seeds': 1,
             'quality': {'label': '2160p', 'score': 4},
             'size_bytes': 8000, 'source_name': 'S'},
        ]
        mock_service.return_value = ('alldebrid', 'k')
        mock_check.return_value = {'a' * 40: True, 'b' * 40: True}
        results = search_torrents('tt1234567', sort_mode='cached_first')
        # Both cached → quality wins
        assert results[0]['info_hash'] == 'b' * 40
        assert results[1]['info_hash'] == 'a' * 40

    @patch('utils.search.check_debrid_cache')
    @patch('utils.search._get_debrid_service')
    @patch('utils.search.search_torrentio')
    def test_annotated_search_resolves_service_once(
            self, mock_search, mock_service, mock_check):
        """search_torrents must call _get_debrid_service exactly once
        and pass the resolved service into check_debrid_cache — no
        double env-var read, no chance of a mid-call key rotation
        making cached_service disagree with the probe service."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'R1', 'seeds': 10,
             'quality': {'label': '1080p', 'score': 3},
             'size_bytes': 1000, 'source_name': 'S'},
        ]
        mock_service.return_value = ('alldebrid', 'ad-key')
        mock_check.return_value = {'a' * 40: True}
        search_torrents('tt1234567', annotate_cache=True)
        # Single resolution
        assert mock_service.call_count == 1
        # Probe got the explicit service, not None
        _, kwargs = mock_check.call_args
        assert kwargs['service'] == 'alldebrid'
        assert kwargs['api_key'] == 'ad-key'

    @patch('utils.search.search_torrentio')
    def test_sort_mode_quality_preserves_existing_behaviour(self, mock_search):
        """Regression guard: sort_mode='quality' (the default) must
        never touch the cache probe and must preserve the pre-Phase-3
        ordering contract."""
        mock_search.return_value = [
            {'info_hash': 'a' * 40, 'title': 'R', 'seeds': 10,
             'quality': {'label': '720p', 'score': 2},
             'size_bytes': 500, 'source_name': 'S'},
            {'info_hash': 'b' * 40, 'title': 'R', 'seeds': 50,
             'quality': {'label': '2160p', 'score': 4},
             'size_bytes': 8000, 'source_name': 'S'},
        ]
        with patch('utils.search.check_debrid_cache') as mock_check:
            results = search_torrents('tt1234567')  # defaults
            mock_check.assert_not_called()
        assert results[0]['info_hash'] == 'b' * 40


# ---------------------------------------------------------------------------
# Dedup and require-cached gates on add_to_debrid
# ---------------------------------------------------------------------------

class TestAddToDebridDedup:
    """Guards for the pre-add dedup probe — hashes already on the account
    must not be re-submitted."""

    @patch('utils.search._urllib_post')
    @patch('utils.search._existing_hashes')
    @patch('utils.search._get_debrid_service')
    def test_duplicate_hash_blocks_add(self, mock_service, mock_existing, mock_post):
        mock_service.return_value = ('realdebrid', 'test_key')
        mock_existing.return_value = {'a' * 40}
        result = add_to_debrid('a' * 40, title='Dup')
        assert result['success'] is False
        assert result.get('duplicate') is True
        mock_post.assert_not_called()

    @patch('utils.search._urllib_post')
    @patch('utils.search._existing_hashes')
    @patch('utils.search._get_debrid_service')
    def test_new_hash_not_blocked(self, mock_service, mock_existing, mock_post):
        mock_service.return_value = ('realdebrid', 'test_key')
        mock_existing.return_value = {'b' * 40}  # different hash
        mock_post.side_effect = [{'id': 'ABC'}, {}]
        result = add_to_debrid('a' * 40)
        assert result['success'] is True

    @patch('utils.search._urllib_post')
    @patch('utils.search._existing_hashes')
    @patch('utils.search._get_debrid_service')
    def test_unknown_existing_does_not_block(self, mock_service, mock_existing, mock_post):
        """When we cannot query the account (``None``), dedup must defer
        to the add — refusing would lock users out on transient API errors."""
        mock_service.return_value = ('realdebrid', 'test_key')
        mock_existing.return_value = None  # unknown
        mock_post.side_effect = [{'id': 'ABC'}, {}]
        result = add_to_debrid('a' * 40)
        assert result['success'] is True

    @patch('utils.search._urllib_post')
    @patch('utils.search._existing_hashes')
    @patch('utils.search._get_debrid_service')
    def test_dedup_disabled_bypasses_probe(self, mock_service, mock_existing,
                                            mock_post, monkeypatch):
        monkeypatch.setenv('SEARCH_DEDUP_ENABLED', 'false')
        mock_service.return_value = ('realdebrid', 'test_key')
        mock_post.side_effect = [{'id': 'ABC'}, {}]
        result = add_to_debrid('a' * 40)
        assert result['success'] is True
        mock_existing.assert_not_called()

    @patch('utils.search._urllib_post')
    @patch('utils.search._existing_hashes')
    @patch('utils.search._get_debrid_service')
    def test_successful_add_primes_cache(self, mock_service, mock_existing,
                                           mock_post):
        """After a successful add, ``remember_added_hash`` should have
        injected the hash so a duplicate-click within TTL is caught."""
        mock_service.return_value = ('realdebrid', 'test_key')
        # Prime the real cache with an empty set so ``remember_added_hash``
        # has somewhere to write.
        invalidate_existing_hashes_cache()
        import utils.search as s
        s._existing_hashes_cache['realdebrid'] = (
            s.time.time(), set()
        )
        mock_existing.return_value = s._existing_hashes_cache['realdebrid'][1]
        mock_post.side_effect = [{'id': 'ABC'}, {}]
        add_to_debrid('a' * 40)
        assert 'a' * 40 in s._existing_hashes_cache['realdebrid'][1]


class TestAddToDebridRequireCached:
    """Guards for the pre-add cache check — uncached / unknown hashes must
    be refused when the strict gate is on."""

    @patch('utils.search._urllib_post')
    @patch('utils.search.check_debrid_cache')
    @patch('utils.search._existing_hashes')
    @patch('utils.search._get_debrid_service')
    def test_uncached_blocked(self, mock_service, mock_existing, mock_cache,
                                mock_post, monkeypatch):
        monkeypatch.setenv('SEARCH_REQUIRE_CACHED', 'true')
        mock_service.return_value = ('alldebrid', 'test_key')
        mock_existing.return_value = set()
        mock_cache.return_value = {'a' * 40: False}
        result = add_to_debrid('a' * 40)
        assert result['success'] is False
        assert 'cached' in result['error'].lower()
        mock_post.assert_not_called()

    @patch('utils.search._urllib_post')
    @patch('utils.search.check_debrid_cache')
    @patch('utils.search._existing_hashes')
    @patch('utils.search._get_debrid_service')
    def test_unknown_blocked(self, mock_service, mock_existing, mock_cache,
                               mock_post, monkeypatch):
        """RD's cache probe always returns None — strict mode must treat
        that as 'not cached' so uncached RD grabs are not snuck in."""
        monkeypatch.setenv('SEARCH_REQUIRE_CACHED', 'true')
        mock_service.return_value = ('realdebrid', 'test_key')
        mock_existing.return_value = set()
        mock_cache.return_value = {'a' * 40: None}
        result = add_to_debrid('a' * 40)
        assert result['success'] is False
        mock_post.assert_not_called()

    @patch('utils.search._urllib_post')
    @patch('utils.search.check_debrid_cache')
    @patch('utils.search._existing_hashes')
    @patch('utils.search._get_debrid_service')
    def test_cached_allowed(self, mock_service, mock_existing, mock_cache,
                              mock_post, monkeypatch):
        monkeypatch.setenv('SEARCH_REQUIRE_CACHED', 'true')
        mock_service.return_value = ('alldebrid', 'test_key')
        mock_existing.return_value = set()
        mock_cache.return_value = {'a' * 40: True}
        mock_post.return_value = {
            'status': 'success', 'data': {'magnets': [{'id': 1}]}
        }
        result = add_to_debrid('a' * 40)
        assert result['success'] is True

    @patch('utils.search._urllib_post')
    @patch('utils.search.check_debrid_cache')
    @patch('utils.search._existing_hashes')
    @patch('utils.search._get_debrid_service')
    def test_gate_off_allows_uncached(self, mock_service, mock_existing,
                                        mock_cache, mock_post):
        """Default behaviour (gate OFF) must be unchanged — uncached
        releases still land on the account."""
        mock_service.return_value = ('alldebrid', 'test_key')
        mock_existing.return_value = set()
        mock_post.return_value = {
            'status': 'success', 'data': {'magnets': [{'id': 1}]}
        }
        result = add_to_debrid('a' * 40)
        assert result['success'] is True
        mock_cache.assert_not_called()


class TestExistingHashesHelpers:
    """Parsing and caching guards for the account-listing helpers."""

    def test_rd_parses_list_response(self):
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = [
                {'id': '1', 'hash': 'A' * 40, 'status': 'downloaded'},
                {'id': '2', 'hash': 'B' * 40, 'status': 'queued'},
                {'id': '3', 'hash': 'not-a-hash', 'status': 'x'},  # dropped
                'garbage',  # dropped
            ]
            from utils.search import _existing_hashes_rd
            result = _existing_hashes_rd('key')
        assert result == {'a' * 40, 'b' * 40}

    def test_ad_parses_success_response(self):
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = {
                'status': 'success',
                'data': {'magnets': [
                    {'hash': 'A' * 40}, {'hash': 'C' * 40},
                    {'hash': ''}, 'garbage',
                ]},
            }
            from utils.search import _existing_hashes_ad
            result = _existing_hashes_ad('key')
        assert result == {'a' * 40, 'c' * 40}

    def test_ad_failure_returns_none(self):
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = {'status': 'error'}
            from utils.search import _existing_hashes_ad
            assert _existing_hashes_ad('key') is None

    def test_tb_parses_success_response(self):
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = {
                'success': True,
                'data': [{'hash': 'D' * 40}, {'hash': 'e' * 40}],
            }
            from utils.search import _existing_hashes_tb
            result = _existing_hashes_tb('key')
        assert result == {'d' * 40, 'e' * 40}

    def test_cache_hit_within_ttl_skips_api(self):
        invalidate_existing_hashes_cache()
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = [{'id': '1', 'hash': 'a' * 40}]
            _existing_hashes('realdebrid', 'key')
            _existing_hashes('realdebrid', 'key')
            _existing_hashes('realdebrid', 'key')
            assert mock_get.call_count == 1

    def test_force_refresh_bypasses_ttl(self):
        invalidate_existing_hashes_cache()
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = [{'id': '1', 'hash': 'a' * 40}]
            _existing_hashes('realdebrid', 'key')
            _existing_hashes('realdebrid', 'key', force_refresh=True)
            assert mock_get.call_count == 2

    def test_missing_service_returns_none(self):
        assert _existing_hashes(None, 'key') is None
        assert _existing_hashes('realdebrid', '') is None

    def test_remember_added_hash_updates_cache(self):
        invalidate_existing_hashes_cache()
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = []
            cached = _existing_hashes('realdebrid', 'key')
        assert cached == set()
        remember_added_hash('realdebrid', 'A' * 40)
        # Re-read: same cache object, now contains the new hash
        cached = _existing_hashes('realdebrid', 'key')
        assert 'a' * 40 in cached

    def test_remember_ignores_invalid_hash(self):
        invalidate_existing_hashes_cache()
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = []
            _existing_hashes('realdebrid', 'key')
        remember_added_hash('realdebrid', 'not-a-hash')
        cached = _existing_hashes('realdebrid', 'key')
        assert cached == set()

    def test_rd_non_list_response_returns_none(self):
        """RD error responses are dicts (``{'error': 'not_authenticated'}``);
        the dedup helper must return None, not crash on ``.strip()`` of the
        dict's 'hash' field."""
        from utils.search import _existing_hashes_rd
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = {'error': 'not_authenticated'}
            assert _existing_hashes_rd('key') is None

    def test_ad_non_dict_response_returns_none(self):
        """AD is supposed to return a dict; a list / string / None slipping
        through (gateway HTML error page that parsed as JSON, etc.) must
        not raise AttributeError from the new helper."""
        from utils.search import _existing_hashes_ad
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = ['garbage']
            assert _existing_hashes_ad('key') is None
            mock_get.return_value = None
            assert _existing_hashes_ad('key') is None

    def test_tb_non_dict_response_returns_none(self):
        from utils.search import _existing_hashes_tb
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = ['garbage']
            assert _existing_hashes_tb('key') is None

    def test_hash_field_not_a_string_skipped(self):
        """Entries with non-string ``hash`` fields (e.g. ``{'hash': 123}``)
        must be skipped, not crash on ``.strip()``."""
        from utils.search import _existing_hashes_rd
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = [
                {'id': '1', 'hash': 123},  # int — skip
                {'id': '2', 'hash': None},  # null — skip
                {'id': '3', 'hash': 'A' * 40},  # valid
            ]
            result = _existing_hashes_rd('key')
        assert result == {'a' * 40}

    def test_rd_truncation_warning(self, caplog):
        """When RD returns the full 2500-entry page, log a one-time warning
        so heavy users know dedup is degraded."""
        from utils.search import _existing_hashes_rd, _RD_LIST_LIMIT
        import utils.search as s
        s._rd_list_truncation_warned = False  # reset for this test
        with patch('utils.search._urllib_get') as mock_get:
            mock_get.return_value = [
                {'id': str(i), 'hash': f'{i:040x}'[:40]}
                for i in range(_RD_LIST_LIMIT)
            ]
            with caplog.at_level('WARNING'):
                _existing_hashes_rd('key')
        assert any('dedup may miss older entries' in r.message
                   for r in caplog.records)

    def test_unexpected_exception_returns_none(self):
        """If a dedup helper raises an unexpected exception (schema drift,
        etc.) the outer ``_existing_hashes`` must swallow it and return
        None — a crash here would propagate into the ``/api/search/add``
        handler and the blackhole watcher."""
        with patch('utils.search._existing_hashes_rd',
                    side_effect=AttributeError('unexpected')):
            assert _existing_hashes('realdebrid', 'key') is None


class TestAddToDebridInFlightRace:
    """Guard against two simultaneous Add clicks (or two browser tabs)
    both passing the dedup probe before either hits ``addMagnet``."""

    def test_inflight_blocks_concurrent_add(self, monkeypatch):
        """While one thread is mid-add, a sibling call for the same hash
        must be rejected — the account-list probe cannot see the in-flight
        submission yet."""
        import utils.search as s
        monkeypatch.setattr(s, '_existing_hashes', lambda *a, **kw: set())
        monkeypatch.setattr(s, '_get_debrid_service',
                             lambda: ('realdebrid', 'key'))
        h = 'a' * 40

        # Pre-mark the hash as in-flight so add_to_debrid sees it.
        with s._existing_hashes_lock:
            s._inflight_adds.add(('realdebrid', h))
        try:
            result = add_to_debrid(h)
        finally:
            with s._existing_hashes_lock:
                s._inflight_adds.discard(('realdebrid', h))
        assert result['success'] is False
        assert result.get('duplicate') is True
        assert 'in progress' in result['error'].lower()

    def test_inflight_released_after_add(self, monkeypatch):
        import utils.search as s
        monkeypatch.setattr(s, '_existing_hashes', lambda *a, **kw: set())
        monkeypatch.setattr(s, '_get_debrid_service',
                             lambda: ('realdebrid', 'key'))
        monkeypatch.setattr(s, '_urllib_post',
                             lambda *a, **kw: {'id': 'ABC'})
        h = 'a' * 40
        add_to_debrid(h)
        # After the add completes the in-flight tuple must be cleared so
        # the user can retry without a spurious "already in progress".
        with s._existing_hashes_lock:
            assert ('realdebrid', h) not in s._inflight_adds
