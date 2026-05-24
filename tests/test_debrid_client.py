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

    def test_explicit_service_overrides_priority(self):
        """With service='alldebrid' specified, MUST return an AD client
        even when RD is also configured — otherwise callers that route
        a service-specific torrent ID through the default priority can
        hit the wrong account."""
        with patch('utils.debrid_client.load_secret_or_env', side_effect=lambda k: {
            'rd_api_key': 'rd-key', 'ad_api_key': 'ad-key', 'torbox_api_key': ''
        }.get(k, '')):
            client, name = get_debrid_client(service='alldebrid')
        assert name == 'alldebrid'
        assert client.configured

    def test_explicit_service_with_api_key_override(self):
        """Explicit api_key must take precedence over the env-configured key."""
        with patch('utils.debrid_client.load_secret_or_env', return_value=''):
            client, name = get_debrid_client(service='realdebrid', api_key='explicit-key')
        assert name == 'realdebrid'
        assert client.configured
        assert client._api_key == 'explicit-key'

    def test_explicit_service_unknown_returns_none(self):
        client, name = get_debrid_client(service='premiumize')
        assert client is None
        assert name is None

    def test_explicit_service_but_unconfigured_returns_none(self):
        """service='realdebrid' with no RD key available must return (None,None)
        rather than falling through to another provider."""
        with patch('utils.debrid_client.load_secret_or_env', return_value=''):
            client, name = get_debrid_client(service='realdebrid')
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
# RealDebrid probe_file — debrid health reconcile detection primitive
# ---------------------------------------------------------------------------

class TestProbeFile:
    """Tests for RealDebridClient.probe_file — the detection primitive
    for the May 2026 RD keyword filter-gate (infringing_file / error 35).

    The probe POSTs to ``/unrestrict/link`` with a sample file link and
    classifies the response:
        200 → healthy
        403 + ``error_code: 35`` or ``error: 'infringing_file'`` → blocked
        404 → blocked (not_found)
        anything else → unknown (retry-eligible)
    """

    _SAMPLE_LINK = 'https://real-debrid.com/d/ABC123XYZ'

    def _info_response(self, files, links):
        """Build a /torrents/info response body."""
        return {'id': 'ABC123', 'files': files, 'links': links}

    @patch('utils.debrid_client.requests.post')
    def test_healthy_returns_status_healthy(self, mock_post, rd):
        mock_post.return_value = _mock_response(status_code=200, json_data={'download': 'https://...'})
        result = rd.probe_file('ABC123', sample_file_link=self._SAMPLE_LINK)
        assert result == {'status': 'healthy'}

    @patch('utils.debrid_client.requests.post')
    def test_blocked_infringing_by_error_code(self, mock_post, rd):
        """RD's documented filter response: HTTP 403 + error_code 35."""
        mock_post.return_value = _mock_response(
            status_code=403,
            json_data={'error': 'infringing_file', 'error_code': 35},
        )
        result = rd.probe_file('ABC123', sample_file_link=self._SAMPLE_LINK)
        assert result == {'status': 'blocked', 'reason': 'infringing_file', 'http': 403}

    @patch('utils.debrid_client.requests.post')
    def test_blocked_infringing_by_error_key_alone(self, mock_post, rd):
        """If RD drops error_code but keeps the error string, still classify
        as blocked — defense against minor body-format drift."""
        mock_post.return_value = _mock_response(
            status_code=403,
            json_data={'error': 'infringing_file'},
        )
        result = rd.probe_file('ABC123', sample_file_link=self._SAMPLE_LINK)
        assert result == {'status': 'blocked', 'reason': 'infringing_file', 'http': 403}

    @patch('utils.debrid_client.requests.post')
    def test_blocked_not_found(self, mock_post, rd):
        mock_post.return_value = _mock_response(status_code=404)
        result = rd.probe_file('ABC123', sample_file_link=self._SAMPLE_LINK)
        assert result == {'status': 'blocked', 'reason': 'not_found', 'http': 404}

    @patch('utils.debrid_client.requests.post')
    def test_503_returns_unknown_retry_eligible(self, mock_post, rd):
        mock_post.return_value = _mock_response(status_code=503)
        result = rd.probe_file('ABC123', sample_file_link=self._SAMPLE_LINK)
        assert result == {'status': 'unknown', 'error': 'http_503'}

    @patch('utils.debrid_client.requests.post')
    def test_403_with_malformed_body_does_not_crash(self, mock_post, rd):
        """A 403 with a body that fails JSON parsing must NOT crash the
        sweep — classifying as unknown lets the next probe retry."""
        resp = _mock_response(status_code=403)
        resp.json.side_effect = ValueError('not json')
        mock_post.return_value = resp
        result = rd.probe_file('ABC123', sample_file_link=self._SAMPLE_LINK)
        assert result == {'status': 'unknown', 'error': 'http_403_unclassified'}

    @patch('utils.debrid_client.requests.post')
    def test_403_with_unrecognised_body_is_unknown_not_blocked(self, mock_post, rd, caplog):
        """A 403 whose body shape we don't recognise must NOT be silently
        treated as blocked (would mass-delete on auto-remediate). Surface
        at WARN so future RD filter-format drift is visible."""
        mock_post.return_value = _mock_response(
            status_code=403,
            json_data={'error': 'some_other_403_reason'},
        )
        import logging
        with caplog.at_level(logging.WARNING):
            result = rd.probe_file('ABC123', sample_file_link=self._SAMPLE_LINK)
        assert result == {'status': 'unknown', 'error': 'http_403_unclassified'}
        assert any('unclassified 403' in r.message for r in caplog.records)

    @patch('utils.debrid_client.requests.post')
    def test_network_error_returns_unknown(self, mock_post, rd):
        import requests as req
        mock_post.side_effect = req.ConnectionError('timeout')
        result = rd.probe_file('ABC123', sample_file_link=self._SAMPLE_LINK)
        assert result == {'status': 'unknown', 'error': 'ConnectionError'}

    def test_invalid_torrent_id_returns_unknown_without_http_call(self, rd):
        """Bad torrent IDs must be rejected before any HTTP call —
        same posture as ``delete_torrent``."""
        with patch('utils.debrid_client.requests.post') as mock_post, \
             patch('utils.debrid_client.requests.get') as mock_get:
            result = rd.probe_file('../../etc/passwd', sample_file_link=self._SAMPLE_LINK)
            assert result == {'status': 'unknown', 'error': 'invalid_torrent_id'}
            assert mock_post.call_count == 0
            assert mock_get.call_count == 0

    @patch('utils.debrid_client.requests.post')
    @patch('utils.debrid_client.requests.get')
    def test_picks_smallest_media_file_when_link_not_provided(self, mock_get, mock_post, rd):
        """No sample_file_link → fetch /torrents/info, pick smallest media
        file, probe that link. Non-media files (.nfo, .srt) and unselected
        files are excluded."""
        mock_get.return_value = _mock_response(json_data=self._info_response(
            files=[
                {'id': 1, 'path': '/big.mkv',    'bytes': 5_000_000_000, 'selected': 1},
                {'id': 2, 'path': '/small.mkv',  'bytes': 1_000_000_000, 'selected': 1},
                {'id': 3, 'path': '/medium.mkv', 'bytes': 3_000_000_000, 'selected': 1},
                {'id': 4, 'path': '/sample.nfo', 'bytes': 1_000,         'selected': 1},
                {'id': 5, 'path': '/extras.mkv', 'bytes': 100_000,       'selected': 0},  # unselected
            ],
            links=[
                'https://real-debrid.com/d/BIG',
                'https://real-debrid.com/d/SMALL',
                'https://real-debrid.com/d/MEDIUM',
                'https://real-debrid.com/d/NFO',
            ],
        ))
        mock_post.return_value = _mock_response(status_code=200)
        result = rd.probe_file('ABC123')
        assert result == {'status': 'healthy'}
        # Smallest MEDIA file is /small.mkv at 1GB (not /sample.nfo, not /extras.mkv).
        assert mock_post.call_args[1]['data']['link'] == 'https://real-debrid.com/d/SMALL'

    @patch('utils.debrid_client.requests.get')
    def test_no_media_files_in_torrent_is_unknown(self, mock_get, rd):
        """Torrent with only non-media files → can't probe, return unknown.
        Don't POST anything."""
        mock_get.return_value = _mock_response(json_data=self._info_response(
            files=[
                {'id': 1, 'path': '/readme.txt', 'bytes': 1000, 'selected': 1},
                {'id': 2, 'path': '/cover.jpg',  'bytes': 5000, 'selected': 1},
            ],
            links=['https://real-debrid.com/d/TXT', 'https://real-debrid.com/d/JPG'],
        ))
        with patch('utils.debrid_client.requests.post') as mock_post:
            result = rd.probe_file('ABC123')
            assert result == {'status': 'unknown', 'error': 'no_media_files'}
            assert mock_post.call_count == 0

    @patch('utils.debrid_client.requests.get')
    def test_info_files_links_length_mismatch_bails(self, mock_get, rd):
        """RD's contract: links is parallel to selected files. A mismatch
        means we can't safely pair link to file — bail rather than probe
        the wrong one (which could mis-flag the wrong torrent on filter
        detection)."""
        mock_get.return_value = _mock_response(json_data=self._info_response(
            files=[
                {'id': 1, 'path': '/a.mkv', 'bytes': 1000, 'selected': 1},
                {'id': 2, 'path': '/b.mkv', 'bytes': 2000, 'selected': 1},
            ],
            links=['https://real-debrid.com/d/A'],  # length 1 vs selected 2
        ))
        with patch('utils.debrid_client.requests.post') as mock_post:
            result = rd.probe_file('ABC123')
            assert result == {'status': 'unknown', 'error': 'no_media_files'}
            assert mock_post.call_count == 0

    @patch('utils.debrid_client.requests.get')
    def test_info_request_fails_returns_unknown(self, mock_get, rd):
        import requests as req
        mock_get.side_effect = req.ConnectionError('timeout')
        result = rd.probe_file('ABC123')
        assert result == {'status': 'unknown', 'error': 'no_media_files'}

    @patch('utils.debrid_client.requests.get')
    def test_info_returns_non_dict_is_unknown(self, mock_get, rd):
        """Defensive: if RD returns a list or scalar instead of an info
        dict (e.g. an error envelope shape change), don't crash."""
        mock_get.return_value = _mock_response(json_data=['not', 'a', 'dict'])
        result = rd.probe_file('ABC123')
        assert result == {'status': 'unknown', 'error': 'no_media_files'}

    @patch('utils.debrid_client.requests.post')
    def test_probe_does_not_leak_api_key_in_logs(self, mock_post, rd, caplog):
        """The sample file link contains the user's RD path. A network
        failure log line must go through _sanitize_error so the API key
        (if it appears in the exception message) is masked."""
        import requests as req
        mock_post.side_effect = req.ConnectionError('failure with key test-rd-key embedded')
        import logging
        with caplog.at_level(logging.WARNING):
            rd.probe_file('ABC123', sample_file_link=self._SAMPLE_LINK)
        for record in caplog.records:
            assert 'test-rd-key' not in record.message, \
                f"API key leaked in log: {record.message!r}"


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
        assert params['agent'] == 'zurgarr'


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
        error = Exception('https://api.alldebrid.com/v4/magnet/status?agent=zurgarr&apikey=test-ad-key')
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
