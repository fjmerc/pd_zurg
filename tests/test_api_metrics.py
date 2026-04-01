"""Tests for the debrid API metrics tracker."""

import threading
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from utils.api_metrics import APIMetricsTracker, _ProviderMetrics, _sanitize_error, tracked_request


class TestProviderMetrics:

    def test_initial_state(self):
        m = _ProviderMetrics()
        d = m.to_dict()
        assert d['calls_today'] == 0
        assert d['errors_today'] == 0
        assert d['avg_response_ms'] == 0.0
        assert d['last_error'] is None
        assert d['last_error_time'] is None
        assert 'rate_limit_remaining' not in d
        assert 'rate_limit_limit' not in d

    def test_day_reset(self):
        m = _ProviderMetrics()
        m.calls_today = 10
        m.errors_today = 2
        m._total_response_ms = 5000.0
        m._call_count_for_avg = 10
        m.last_error = 'HTTP 429'
        m.last_error_time = '2026-04-01T12:00:00'
        m.rate_limit_remaining = 50
        m.rate_limit_limit = 500

        # Simulate next day
        tomorrow = date(2026, 4, 2)
        with patch('utils.api_metrics.date') as mock_date:
            mock_date.today.return_value = tomorrow
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            m.check_day_reset()

        assert m.calls_today == 0
        assert m.errors_today == 0
        assert m.avg_response_ms == 0.0
        assert m.last_error is None
        assert m.rate_limit_remaining is None

    def test_avg_response_ms(self):
        m = _ProviderMetrics()
        m._total_response_ms = 600.0
        m._call_count_for_avg = 3
        assert m.avg_response_ms == pytest.approx(200.0)

    def test_rate_limit_omitted_when_none(self):
        m = _ProviderMetrics()
        d = m.to_dict()
        assert 'rate_limit_remaining' not in d
        assert 'rate_limit_limit' not in d

    def test_rate_limit_included_when_set(self):
        m = _ProviderMetrics()
        m.rate_limit_remaining = 100
        m.rate_limit_limit = 500
        d = m.to_dict()
        assert d['rate_limit_remaining'] == 100
        assert d['rate_limit_limit'] == 500


class TestAPIMetricsTracker:

    def test_record_success(self):
        t = APIMetricsTracker()
        t.record_call('realdebrid', 200, 150.0)
        d = t.get_metrics('realdebrid')
        assert d['calls_today'] == 1
        assert d['errors_today'] == 0
        assert d['avg_response_ms'] == 150.0
        assert d['last_error'] is None

    def test_record_multiple_calls(self):
        t = APIMetricsTracker()
        t.record_call('realdebrid', 200, 100.0)
        t.record_call('realdebrid', 200, 200.0)
        t.record_call('realdebrid', 200, 300.0)
        d = t.get_metrics('realdebrid')
        assert d['calls_today'] == 3
        assert d['avg_response_ms'] == 200.0

    def test_record_error_by_status(self):
        t = APIMetricsTracker()
        t.record_call('realdebrid', 429, 50.0)
        d = t.get_metrics('realdebrid')
        assert d['errors_today'] == 1
        assert d['last_error'] == 'HTTP 429'
        assert d['last_error_time'] is not None

    def test_record_error_by_message(self):
        t = APIMetricsTracker()
        t.record_call('realdebrid', 0, 0.0, error='Connection timeout')
        d = t.get_metrics('realdebrid')
        assert d['errors_today'] == 1
        assert d['last_error'] == 'Connection timeout'

    def test_error_message_preferred_over_status(self):
        t = APIMetricsTracker()
        t.record_call('realdebrid', 500, 100.0, error='Internal Server Error')
        d = t.get_metrics('realdebrid')
        assert d['last_error'] == 'Internal Server Error'

    def test_rate_limit_headers(self):
        t = APIMetricsTracker()
        t.record_call('realdebrid', 200, 100.0,
                       rate_limit_remaining=358, rate_limit_limit=500)
        d = t.get_metrics('realdebrid')
        assert d['rate_limit_remaining'] == 358
        assert d['rate_limit_limit'] == 500

    def test_rate_limit_updates_on_each_call(self):
        t = APIMetricsTracker()
        t.record_call('realdebrid', 200, 100.0, rate_limit_remaining=100)
        t.record_call('realdebrid', 200, 100.0, rate_limit_remaining=99)
        d = t.get_metrics('realdebrid')
        assert d['rate_limit_remaining'] == 99

    def test_multiple_providers(self):
        t = APIMetricsTracker()
        t.record_call('realdebrid', 200, 100.0)
        t.record_call('alldebrid', 200, 200.0)
        t.record_call('torbox', 200, 300.0)

        all_metrics = t.get_metrics()
        assert len(all_metrics) == 3
        assert all_metrics['realdebrid']['avg_response_ms'] == 100.0
        assert all_metrics['alldebrid']['avg_response_ms'] == 200.0
        assert all_metrics['torbox']['avg_response_ms'] == 300.0

    def test_get_unknown_provider(self):
        t = APIMetricsTracker()
        assert t.get_metrics('nonexistent') is None

    def test_get_all_empty(self):
        t = APIMetricsTracker()
        assert t.get_metrics() == {}

    def test_success_does_not_overwrite_last_error(self):
        """A successful call should not clear a previous error."""
        t = APIMetricsTracker()
        t.record_call('realdebrid', 429, 50.0, error='Rate limited')
        t.record_call('realdebrid', 200, 100.0)
        d = t.get_metrics('realdebrid')
        assert d['errors_today'] == 1
        assert d['last_error'] == 'Rate limited'

    def test_latest_error_wins(self):
        t = APIMetricsTracker()
        t.record_call('realdebrid', 429, 50.0, error='Rate limited')
        t.record_call('realdebrid', 503, 50.0, error='Service unavailable')
        d = t.get_metrics('realdebrid')
        assert d['errors_today'] == 2
        assert d['last_error'] == 'Service unavailable'

    def test_thread_safety(self):
        """Concurrent record_call should not corrupt data."""
        t = APIMetricsTracker()
        n_threads = 10
        calls_per_thread = 100

        def worker():
            for _ in range(calls_per_thread):
                t.record_call('realdebrid', 200, 10.0)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        d = t.get_metrics('realdebrid')
        assert d['calls_today'] == n_threads * calls_per_thread

    def test_mixed_success_and_errors(self):
        t = APIMetricsTracker()
        t.record_call('realdebrid', 200, 100.0)
        t.record_call('realdebrid', 200, 150.0)
        t.record_call('realdebrid', 500, 50.0, error='Server error')
        t.record_call('realdebrid', 200, 200.0)

        d = t.get_metrics('realdebrid')
        assert d['calls_today'] == 4
        assert d['errors_today'] == 1
        assert d['avg_response_ms'] == 125.0  # (100+150+50+200)/4


class TestTrackedRequest:

    def test_success_records_metrics(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {}
        mock_method = MagicMock(return_value=mock_resp)

        with patch('utils.api_metrics.api_metrics') as mock_metrics:
            result = tracked_request('realdebrid', mock_method, 'https://example.com', timeout=5)

        assert result is mock_resp
        mock_method.assert_called_once_with('https://example.com', timeout=5)
        mock_metrics.record_call.assert_called_once()
        call_args = mock_metrics.record_call.call_args
        assert call_args[0][0] == 'realdebrid'
        assert call_args[0][1] == 200
        assert call_args[0][2] > 0  # response time > 0
        assert call_args[1].get('error') is None

    def test_http_error_records_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.headers = {}
        mock_method = MagicMock(return_value=mock_resp)

        with patch('utils.api_metrics.api_metrics') as mock_metrics:
            result = tracked_request('realdebrid', mock_method, 'https://example.com')

        assert result is mock_resp
        call_args = mock_metrics.record_call.call_args
        assert call_args[1].get('error') == 'HTTP 429'

    def test_exception_records_and_reraises(self):
        mock_method = MagicMock(side_effect=ConnectionError('timeout'))

        with patch('utils.api_metrics.api_metrics') as mock_metrics:
            with pytest.raises(ConnectionError, match='timeout'):
                tracked_request('realdebrid', mock_method, 'https://example.com')

        mock_metrics.record_call.assert_called_once()
        call_args = mock_metrics.record_call.call_args
        assert call_args[0][0] == 'realdebrid'
        assert call_args[0][1] == 0  # no HTTP status on exception
        assert call_args[1].get('error') == 'timeout'

    def test_rate_limit_headers_parsed(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {
            'X-RateLimit-Remaining': '358',
            'X-RateLimit-Limit': '500',
        }
        mock_method = MagicMock(return_value=mock_resp)

        with patch('utils.api_metrics.api_metrics') as mock_metrics:
            tracked_request('realdebrid', mock_method, 'https://example.com')

        call_args = mock_metrics.record_call.call_args
        assert call_args[1]['rate_limit_remaining'] == 358
        assert call_args[1]['rate_limit_limit'] == 500

    def test_ietf_rate_limit_headers_parsed(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {
            'RateLimit-Remaining': '42',
            'RateLimit-Limit': '100',
        }
        mock_method = MagicMock(return_value=mock_resp)

        with patch('utils.api_metrics.api_metrics') as mock_metrics:
            tracked_request('alldebrid', mock_method, 'https://example.com')

        call_args = mock_metrics.record_call.call_args
        assert call_args[1]['rate_limit_remaining'] == 42
        assert call_args[1]['rate_limit_limit'] == 100

    def test_no_rate_limit_headers(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {}
        mock_method = MagicMock(return_value=mock_resp)

        with patch('utils.api_metrics.api_metrics') as mock_metrics:
            tracked_request('torbox', mock_method, 'https://example.com')

        call_args = mock_metrics.record_call.call_args
        assert call_args[1]['rate_limit_remaining'] is None
        assert call_args[1]['rate_limit_limit'] is None

    def test_metrics_error_does_not_break_caller(self):
        """If metrics recording itself fails, the response is still returned."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {}
        mock_method = MagicMock(return_value=mock_resp)

        with patch('utils.api_metrics.api_metrics') as mock_metrics:
            mock_metrics.record_call.side_effect = RuntimeError('metrics broken')
            result = tracked_request('realdebrid', mock_method, 'https://example.com')

        assert result is mock_resp

    def test_exception_path_metrics_failure_still_reraises(self):
        """If HTTP call raises AND metrics recording fails, the original exception propagates."""
        mock_method = MagicMock(side_effect=ConnectionError('network down'))

        with patch('utils.api_metrics.api_metrics') as mock_metrics:
            mock_metrics.record_call.side_effect = RuntimeError('metrics broken')
            with pytest.raises(ConnectionError, match='network down'):
                tracked_request('realdebrid', mock_method, 'https://example.com')

    def test_get_metrics_empty_string_provider(self):
        """Empty string provider should return None, not all providers."""
        t = APIMetricsTracker()
        t.record_call('realdebrid', 200, 100.0)
        assert t.get_metrics('') is None

    def test_exception_error_sanitizes_credentials(self):
        """API keys in exception messages should be redacted."""
        url_with_key = 'https://api.alldebrid.com/v4/magnet/upload?agent=pd_zurg&apikey=SECRET123KEY'
        mock_method = MagicMock(side_effect=ConnectionError(url_with_key))

        with patch('utils.api_metrics.api_metrics') as mock_metrics:
            with pytest.raises(ConnectionError):
                tracked_request('alldebrid', mock_method, 'https://example.com')

        call_args = mock_metrics.record_call.call_args
        error_msg = call_args[1].get('error') or call_args[0][3]
        assert 'SECRET123KEY' not in error_msg
        assert 'apikey=***' in error_msg


class TestSanitizeError:

    def test_strips_apikey_param(self):
        assert 'MY_SECRET' not in _sanitize_error(
            'ConnectionError: https://example.com?apikey=MY_SECRET&foo=bar'
        )

    def test_strips_bearer_token(self):
        assert 'tok_abc123' not in _sanitize_error(
            'Error at url with Authorization: Bearer tok_abc123 header'
        )

    def test_strips_api_key_param(self):
        assert 'KEY456' not in _sanitize_error(
            'Failed: https://host/path?api_key=KEY456'
        )

    def test_preserves_non_sensitive_content(self):
        msg = 'Connection timed out after 30s'
        assert _sanitize_error(msg) == msg
