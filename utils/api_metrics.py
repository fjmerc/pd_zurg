"""In-memory API metrics tracker for debrid provider health monitoring.

Tracks per-provider call counts, error rates, response times, and rate
limit status.  Counters reset automatically at midnight.  All state is
in-memory only -- lost on restart, which is acceptable for a dashboard
health card.
"""

import re
import threading
import time
from datetime import date, datetime


class _ProviderMetrics:
    """Mutable counters for a single debrid provider, reset daily."""

    __slots__ = (
        'calls_today', 'errors_today',
        '_total_response_ms', '_call_count_for_avg',
        'rate_limit_remaining', 'rate_limit_limit',
        'last_error', 'last_error_time',
        '_date',
    )

    def __init__(self):
        self._date = date.today()
        self._reset()

    def _reset(self):
        self.calls_today = 0
        self.errors_today = 0
        self._total_response_ms = 0.0
        self._call_count_for_avg = 0
        self.rate_limit_remaining = None
        self.rate_limit_limit = None
        self.last_error = None
        self.last_error_time = None

    def check_day_reset(self):
        """Reset counters if the date has rolled over."""
        today = date.today()
        if today != self._date:
            self._date = today
            self._reset()

    @property
    def avg_response_ms(self):
        if self._call_count_for_avg == 0:
            return 0.0
        return self._total_response_ms / self._call_count_for_avg

    def to_dict(self):
        self.check_day_reset()
        result = {
            'calls_today': self.calls_today,
            'errors_today': self.errors_today,
            'avg_response_ms': round(self.avg_response_ms, 1),
            'last_error': self.last_error,
            'last_error_time': self.last_error_time,
        }
        if self.rate_limit_remaining is not None:
            result['rate_limit_remaining'] = self.rate_limit_remaining
        if self.rate_limit_limit is not None:
            result['rate_limit_limit'] = self.rate_limit_limit
        return result


class APIMetricsTracker:
    """Thread-safe tracker aggregating debrid API call metrics.

    Usage::

        from utils.api_metrics import api_metrics

        api_metrics.record_call('realdebrid', 200, 342.5)
        api_metrics.record_call('realdebrid', 429, 50.0, error='Rate limited')

        api_metrics.get_metrics()            # all providers
        api_metrics.get_metrics('realdebrid') # single provider
    """

    def __init__(self):
        self._providers: dict[str, _ProviderMetrics] = {}
        self._lock = threading.Lock()

    def record_call(self, provider, status_code, response_time_ms,
                    rate_limit_remaining=None, rate_limit_limit=None,
                    error=None):
        """Record the outcome of a single debrid API call.

        Args:
            provider: Provider key ('realdebrid', 'alldebrid', 'torbox').
            status_code: HTTP status code from the response.
            response_time_ms: Round-trip time in milliseconds.
            rate_limit_remaining: Value from rate-limit response header, if present.
            rate_limit_limit: Total rate-limit quota from header, if present.
            error: Human-readable error string (set for non-2xx or exceptions).
        """
        with self._lock:
            if provider not in self._providers:
                self._providers[provider] = _ProviderMetrics()

            m = self._providers[provider]
            m.check_day_reset()

            m.calls_today += 1
            m._total_response_ms += response_time_ms
            m._call_count_for_avg += 1

            if status_code >= 400 or error:
                m.errors_today += 1
                m.last_error = error or f'HTTP {status_code}'
                m.last_error_time = datetime.now().isoformat(timespec='seconds')

            if rate_limit_remaining is not None:
                m.rate_limit_remaining = rate_limit_remaining
            if rate_limit_limit is not None:
                m.rate_limit_limit = rate_limit_limit

    def get_metrics(self, provider=None):
        """Return metrics dict(s).

        Args:
            provider: If given, return that provider's dict (or ``None``).
                      If omitted, return ``{provider: {...}, ...}`` for all.
        """
        with self._lock:
            if provider is not None:
                m = self._providers.get(provider)
                return m.to_dict() if m else None
            return {p: m.to_dict() for p, m in self._providers.items()}


# Module-level singleton
api_metrics = APIMetricsTracker()


# ── Rate-limit header names (checked in order) ──────────────────────

_RL_REMAINING_HEADERS = ('X-RateLimit-Remaining', 'RateLimit-Remaining')
_RL_LIMIT_HEADERS = ('X-RateLimit-Limit', 'RateLimit-Limit')


_CREDENTIAL_PARAM_RE = re.compile(
    r'(apikey|api_key|token|secret|password|authorization)\s*[=:]\s*\S+',
    re.IGNORECASE,
)
_BEARER_RE = re.compile(r'(Bearer)\s+\S+', re.IGNORECASE)


def _sanitize_error(error_str):
    """Remove potential API keys/tokens from error messages."""
    s = _BEARER_RE.sub(r'\1 ***', error_str)
    s = _CREDENTIAL_PARAM_RE.sub(r'\1=***', s)
    return s


def _parse_rl_header(response, names):
    """Return the first parseable int from *names*, or None."""
    for name in names:
        val = response.headers.get(name)
        if val is not None:
            try:
                return int(val)
            except (ValueError, TypeError):
                pass
    return None


def tracked_request(provider, method, *args, **kwargs):
    """Execute *method* (e.g. ``requests.get``) and record API metrics.

    On success the response is returned unchanged.  On exception the
    error is recorded and the exception is re-raised so callers behave
    identically to a bare ``requests`` call.
    """
    start = time.monotonic()
    try:
        resp = method(*args, **kwargs)
    except Exception as exc:
        elapsed = (time.monotonic() - start) * 1000
        try:
            api_metrics.record_call(provider, 0, elapsed, error=_sanitize_error(str(exc)))
        except Exception:
            pass
        raise

    elapsed = (time.monotonic() - start) * 1000
    try:
        rl_remaining = _parse_rl_header(resp, _RL_REMAINING_HEADERS)
        rl_limit = _parse_rl_header(resp, _RL_LIMIT_HEADERS)
        error = None
        if resp.status_code >= 400:
            error = f'HTTP {resp.status_code}'
        api_metrics.record_call(
            provider, resp.status_code, elapsed,
            rate_limit_remaining=rl_remaining,
            rate_limit_limit=rl_limit,
            error=error,
        )
    except Exception:
        pass  # never let metrics recording break the caller
    return resp
