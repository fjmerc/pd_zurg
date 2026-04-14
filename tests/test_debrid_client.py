"""Tests for the debrid provider API client module (utils/debrid_client.py)."""

import json
import pytest
from unittest.mock import patch, MagicMock

from utils.debrid_client import (
    DebridClientBase,
    RealDebridClient,
    AllDebridClient,
    TorBoxClient,
    get_debrid_client,
    MAX_BATCH_DELETE,
    _SAFE_ID,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def rd():
    return RealDebridClient(api_key='test-rd-key')


@pytest.fixture
def ad():
    return AllDebridClient(api_key='test-ad-key')


@pytest.fixture
def tb():
    return TorBoxClient(api_key='test-tb-key')


def _mock_response(json_data=None, status_code=200, raise_for_status=None):
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}
    if raise_for_status:
        resp.raise_for_status.side_effect = raise_for_status
    else:
        resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# Configuration & factory
# ---------------------------------------------------------------------------

class TestConfiguration:

    def test_unconfigured_client(self):
        with patch('utils.debrid_client.load_secret_or_env', return_value=''):
            client = RealDebridClient(api_key='')
            assert not client.configured

    def test_configured_client(self, rd):
        assert rd.configured

    def test_rd_priority(self, monkeypatch):
        monkeypatch.setenv('RD_API_KEY', 'rd-key')
        monkeypatch.delenv('AD_API_KEY', raising=False)
        monkeypatch.delenv('TORBOX_API_KEY', raising=False)
        with patch('utils.debrid_client.load_secret_or_env', side_effect=lambda k: {
            'rd_api_key': 'rd-key', 'ad_api_key': '', 'torbox_api_key': ''
        }.get(k, '')):
            client, name = get_debrid_client()
            assert name == 'realdebrid'
            assert client.configured

    def test_ad_fallback(self):
        with patch('utils.debrid_client.load_secret_or_env', side_effect=lambda k: {
            'rd_api_key': '', 'ad_api_key': 'ad-key', 'torbox_api_key': ''
        }.get(k, '')):
            client, name = get_debrid_client()
            assert name == 'alldebrid'

    def test_tb_fallback(self):
        with patch('utils.debrid_client.load_secret_or_env', side_effect=lambda k: {
            'rd_api_key': '', 'ad_api_key': '', 'torbox_api_key': 'tb-key'
        }.get(k, '')):
            client, name = get_debrid_client()
            assert name == 'torbox'

    def test_nothing_configured(self):
        with patch('utils.debrid_client.load_secret_or_env', return_value=''):
            client, name = get_debrid_client()
            assert client is None
            assert name is None


# ---------------------------------------------------------------------------
# Torrent ID validation
# ---------------------------------------------------------------------------

class TestSafeID:

    def test_alphanumeric(self):
        assert _SAFE_ID.match('3LSYZCDOOPXDQ')

    def test_numeric(self):
        assert _SAFE_ID.match('12345')

    def test_with_hyphens(self):
        assert _SAFE_ID.match('abc-def-123')

    def test_with_underscores(self):
        assert _SAFE_ID.match('abc_def_123')

    def test_rejects_path_traversal(self):
        assert not _SAFE_ID.match('../../etc/passwd')

    def test_rejects_slashes(self):
        assert not _SAFE_ID.match('abc/def')

    def test_rejects_spaces(self):
        assert not _SAFE_ID.match('abc def')

    def test_rejects_empty(self):
        assert not _SAFE_ID.match('')

    def test_rejects_special_chars(self):
        assert not _SAFE_ID.match('abc;rm -rf /')


# ---------------------------------------------------------------------------
# Title matching
# ---------------------------------------------------------------------------

class TestTitleMatching:
    """Tests for find_torrents_by_title using RealDebridClient."""

    def _make_torrents(self, filenames, hashes=None):
        return [
            {
                'id': str(i),
                'filename': f,
                'hash': (hashes[i] if hashes else f'HASH{i}').upper(),
                'status': 'downloaded',
                'bytes': 1000,
            }
            for i, f in enumerate(filenames)
        ]

    @patch.object(RealDebridClient, 'list_torrents')
    def test_basic_match(self, mock_list, rd):
        mock_list.return_value = self._make_torrents([
            'The.Eternaut.S01.DUAL.1080p.WEBRip.x265-KONTRAST',
            'Some.Other.Show.S01E01.mkv',
        ])
        matches = rd.find_torrents_by_title('the eternaut')
        assert len(matches) == 1
        assert matches[0]['filename'] == 'The.Eternaut.S01.DUAL.1080p.WEBRip.x265-KONTRAST'

    @patch.object(RealDebridClient, 'list_torrents')
    def test_multiple_matches(self, mock_list, rd):
        mock_list.return_value = self._make_torrents([
            'The.Eternaut.S01.DUAL.1080p.WEBRip.x265-KONTRAST',
            'The.Eternaut.S01E01.1080p.WEB.h264-EDITH[EZTVx.to].mkv',
            'The.Eternaut.S01E02.1080p.WEB.h264-EDITH[EZTVx.to].mkv',
        ])
        matches = rd.find_torrents_by_title('the eternaut')
        assert len(matches) == 3

    @patch.object(RealDebridClient, 'list_torrents')
    def test_no_matches(self, mock_list, rd):
        mock_list.return_value = self._make_torrents([
            'Breaking.Bad.S01E01.720p.mkv',
        ])
        matches = rd.find_torrents_by_title('the eternaut')
        assert len(matches) == 0

    @patch.object(RealDebridClient, 'list_torrents')
    def test_strips_mkv_extension(self, mock_list, rd):
        mock_list.return_value = self._make_torrents([
            'Alien Earth S01E07 Emergence REPACK 1080p DSNP WEB-DL DDP5 1 H 264-FLUX.mkv',
        ])
        matches = rd.find_torrents_by_title('alien earth')
        assert len(matches) == 1

    @patch.object(RealDebridClient, 'list_torrents')
    def test_strips_site_prefix(self, mock_list, rd):
        mock_list.return_value = self._make_torrents([
            'www.UIndex.org    -    Alien Earth S01E01 Neverland REPACK2 1080p DSNP WEB-DL DDP5 1 H 264-FLUX',
        ])
        matches = rd.find_torrents_by_title('alien earth')
        assert len(matches) == 1

    @patch.object(RealDebridClient, 'list_torrents')
    def test_year_matching_both_present_agree(self, mock_list, rd):
        mock_list.return_value = self._make_torrents([
            'Dune (2021) 1080p BluRay',
        ])
        matches = rd.find_torrents_by_title('dune', target_year=2021)
        assert len(matches) == 1

    @patch.object(RealDebridClient, 'list_torrents')
    def test_year_matching_both_present_disagree(self, mock_list, rd):
        """Dune 1984 should NOT match when target year is 2021."""
        mock_list.return_value = self._make_torrents([
            'Dune (1984) 1080p BluRay',
        ])
        matches = rd.find_torrents_by_title('dune', target_year=2021)
        assert len(matches) == 0

    @patch.object(RealDebridClient, 'list_torrents')
    def test_year_matching_torrent_missing_year(self, mock_list, rd):
        """When torrent has no year, it should still match (could be any version)."""
        mock_list.return_value = self._make_torrents([
            'Dune.S01E01.1080p.WEB.mkv',
        ])
        matches = rd.find_torrents_by_title('dune', target_year=2021)
        assert len(matches) == 1

    @patch.object(RealDebridClient, 'list_torrents')
    def test_year_matching_target_missing_year(self, mock_list, rd):
        """When target has no year, match all versions."""
        mock_list.return_value = self._make_torrents([
            'Dune (1984) 1080p BluRay',
            'Dune (2021) 1080p BluRay',
        ])
        matches = rd.find_torrents_by_title('dune', target_year=None)
        assert len(matches) == 2

    @patch.object(RealDebridClient, 'list_torrents')
    def test_year_returned_in_results(self, mock_list, rd):
        mock_list.return_value = self._make_torrents([
            'Dune (2021) 1080p BluRay',
        ])
        matches = rd.find_torrents_by_title('dune')
        assert matches[0]['year'] == 2021

    @patch.object(RealDebridClient, 'list_torrents')
    def test_empty_filename_skipped(self, mock_list, rd):
        mock_list.return_value = [
            {'id': '1', 'filename': '', 'status': 'downloaded', 'bytes': 0},
            {'id': '2', 'filename': 'The.Eternaut.S01E01.mkv', 'status': 'downloaded', 'bytes': 1000},
        ]
        matches = rd.find_torrents_by_title('the eternaut')
        assert len(matches) == 1

    @patch.object(RealDebridClient, 'list_torrents')
    def test_case_insensitive(self, mock_list, rd):
        mock_list.return_value = self._make_torrents([
            'THE.ETERNAUT.S01E01.1080P.WEB.mkv',
        ])
        matches = rd.find_torrents_by_title('the eternaut')
        assert len(matches) == 1

    @patch.object(RealDebridClient, 'list_torrents')
    def test_api_error_propagates(self, mock_list, rd):
        """list_torrents raises on error — find_torrents_by_title should propagate."""
        import requests
        mock_list.side_effect = requests.ConnectionError('API down')
        with pytest.raises(requests.ConnectionError):
            rd.find_torrents_by_title('the eternaut')

    @patch.object(RealDebridClient, 'list_torrents')
    def test_hash_propagated(self, mock_list, rd):
        """find_torrents_by_title should include the hash from list_torrents."""
        mock_list.return_value = self._make_torrents(
            ['Why.Him.2016.2160p.BluRay.HEVC.DTS-HD.MA.7.1-COASTER'],
            hashes=['7f6beaa275ecad714e5dbd57b236e2c4ed2a93aa'],
        )
        matches = rd.find_torrents_by_title('why him', target_year=2016)
        assert len(matches) == 1
        assert matches[0]['hash'] == '7F6BEAA275ECAD714E5DBD57B236E2C4ED2A93AA'


# ---------------------------------------------------------------------------
# RealDebrid operations
# ---------------------------------------------------------------------------

class TestRealDebrid:

    @patch('utils.debrid_client.requests.get')
    def test_list_torrents_success(self, mock_get, rd):
        mock_get.return_value = _mock_response([
            {'id': 'ABC123', 'filename': 'Test.mkv', 'hash': 'abc123def', 'status': 'downloaded', 'bytes': 1000},
        ])
        result = rd.list_torrents()
        assert len(result) == 1
        assert result[0]['id'] == 'ABC123'
        assert result[0]['filename'] == 'Test.mkv'
        assert result[0]['hash'] == 'ABC123DEF'

    @patch('utils.debrid_client.requests.get')
    def test_list_torrents_api_error(self, mock_get, rd):
        import requests as req
        mock_get.return_value = _mock_response(raise_for_status=req.HTTPError('403'))
        with pytest.raises(req.HTTPError):
            rd.list_torrents()

    @patch('utils.debrid_client.requests.delete')
    def test_delete_success(self, mock_del, rd):
        mock_del.return_value = _mock_response(status_code=204)
        assert rd.delete_torrent('ABC123') is True

    @patch('utils.debrid_client.requests.delete')
    def test_delete_failure(self, mock_del, rd):
        mock_del.return_value = _mock_response(status_code=404)
        assert rd.delete_torrent('ABC123') is False

    def test_delete_invalid_id(self, rd):
        assert rd.delete_torrent('../../etc/passwd') is False

    @patch('utils.debrid_client.requests.delete')
    def test_delete_network_error(self, mock_del, rd):
        import requests as req
        mock_del.side_effect = req.ConnectionError('timeout')
        assert rd.delete_torrent('ABC123') is False

    @patch('utils.debrid_client.requests.get')
    def test_auth_header(self, mock_get, rd):
        mock_get.return_value = _mock_response([])
        rd.list_torrents()
        call_kwargs = mock_get.call_args
        assert call_kwargs[1]['headers']['Authorization'] == 'Bearer test-rd-key'

    @patch('utils.debrid_client.requests.get')
    def test_limit_param(self, mock_get, rd):
        mock_get.return_value = _mock_response([])
        rd.list_torrents()
        assert mock_get.call_args[1]['params']['limit'] == 2500


# ---------------------------------------------------------------------------
# AllDebrid operations
# ---------------------------------------------------------------------------

class TestAllDebrid:

    @patch('utils.debrid_client.requests.get')
    def test_list_torrents_success(self, mock_get, ad):
        mock_get.return_value = _mock_response({
            'status': 'success',
            'data': {'magnets': [
                {'id': 123, 'filename': 'Test.mkv', 'hash': 'deadbeef', 'statusCode': 4, 'size': 1000},
            ]}
        })
        result = ad.list_torrents()
        assert len(result) == 1
        assert result[0]['id'] == '123'
        assert result[0]['hash'] == 'DEADBEEF'

    @patch('utils.debrid_client.requests.get')
    def test_delete_success(self, mock_get, ad):
        mock_get.return_value = _mock_response({'status': 'success'})
        assert ad.delete_torrent('123') is True

    @patch('utils.debrid_client.requests.get')
    def test_delete_failure(self, mock_get, ad):
        mock_get.return_value = _mock_response({'status': 'error', 'message': 'not found'})
        assert ad.delete_torrent('123') is False

    def test_delete_invalid_id(self, ad):
        assert ad.delete_torrent('../../../bad') is False

    @patch('utils.debrid_client.requests.get')
    def test_apikey_in_params(self, mock_get, ad):
        mock_get.return_value = _mock_response({'status': 'success', 'data': {'magnets': []}})
        ad.list_torrents()
        params = mock_get.call_args[1]['params']
        assert params['apikey'] == 'test-ad-key'
        assert params['agent'] == 'pd_zurg'


# ---------------------------------------------------------------------------
# TorBox operations
# ---------------------------------------------------------------------------

class TestTorBox:

    @patch('utils.debrid_client.requests.get')
    def test_list_torrents_success(self, mock_get, tb):
        mock_get.return_value = _mock_response({
            'success': True,
            'data': [
                {'id': 456, 'name': 'Test.mkv', 'hash': 'cafebabe', 'download_state': 'completed', 'size': 1000},
            ]
        })
        result = tb.list_torrents()
        assert len(result) == 1
        assert result[0]['id'] == '456'
        assert result[0]['filename'] == 'Test.mkv'
        assert result[0]['hash'] == 'CAFEBABE'

    @patch('utils.debrid_client.requests.post')
    def test_delete_success(self, mock_post, tb):
        mock_post.return_value = _mock_response({'success': True})
        assert tb.delete_torrent('456') is True

    @patch('utils.debrid_client.requests.post')
    def test_delete_sends_int_id(self, mock_post, tb):
        mock_post.return_value = _mock_response({'success': True})
        tb.delete_torrent('456')
        body = mock_post.call_args[1]['json']
        assert body['torrent_id'] == 456
        assert isinstance(body['torrent_id'], int)
        assert body['operation'] == 'Delete'

    def test_delete_rejects_float_id(self, tb):
        """IDs with dots are rejected by _SAFE_ID validation."""
        assert tb.delete_torrent('456.0') is False

    def test_delete_failure_response(self, tb):
        """Non-success response returns False without leaking response body."""
        with patch('utils.debrid_client.requests.post') as mock_post:
            mock_post.return_value = _mock_response({'success': False, 'detail': 'not found'})
            assert tb.delete_torrent('456') is False

    def test_delete_invalid_id(self, tb):
        assert tb.delete_torrent('../../bad') is False


# ---------------------------------------------------------------------------
# Error sanitization
# ---------------------------------------------------------------------------

class TestErrorSanitization:

    def test_sanitize_strips_api_key(self, rd):
        error = Exception('Connection failed for url: https://api.real-debrid.com/?key=test-rd-key')
        sanitized = rd._sanitize_error(error)
        assert 'test-rd-key' not in sanitized
        assert '***' in sanitized

    def test_sanitize_no_key(self):
        client = RealDebridClient(api_key='')
        error = Exception('some error')
        sanitized = client._sanitize_error(error)
        assert sanitized == 'some error'

    def test_sanitize_ad_key_in_url(self, ad):
        error = Exception('https://api.alldebrid.com/v4/magnet/status?agent=pd_zurg&apikey=test-ad-key')
        sanitized = ad._sanitize_error(error)
        assert 'test-ad-key' not in sanitized

    @patch('utils.debrid_client.requests.get')
    def test_ad_error_log_sanitized(self, mock_get, ad):
        """Verify AD delete doesn't leak key in log on network error."""
        import requests as req
        mock_get.side_effect = req.ConnectionError(
            'Failed for url: https://api.alldebrid.com/v4/magnet/delete?apikey=test-ad-key&id=123'
        )
        # Should not raise, should return False
        result = ad.delete_torrent('123')
        assert result is False


# ---------------------------------------------------------------------------
# Batch cap constant
# ---------------------------------------------------------------------------

class TestBatchCap:

    def test_max_batch_delete_is_50(self):
        assert MAX_BATCH_DELETE == 50
