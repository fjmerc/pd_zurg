"""Tests for utils/plex_refresh.py — the Plex library section refresh helper
the library scanner calls after symlinking new debrid content.

The helper is urllib-only, authenticates via the X-Plex-Token *header* (never
in the URL, so the token stays out of logs), and is best-effort (it must never
raise into the scan loop)."""

import json
import urllib.error
from unittest.mock import patch, MagicMock

import pytest

from utils.plex_refresh import refresh_plex_sections


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SECTIONS = {
    "MediaContainer": {
        "Directory": [
            {"key": "1", "type": "show", "title": "TV Shows"},
            {"key": "2", "type": "movie", "title": "Movies"},
            {"key": "3", "type": "show", "title": "Anime"},
        ]
    }
}


def _resp(data=b"", json_data=None):
    m = MagicMock()
    if json_data is not None:
        m.read.return_value = json.dumps(json_data).encode("utf-8")
    else:
        m.read.return_value = data
    m.__enter__ = MagicMock(return_value=m)
    m.__exit__ = MagicMock(return_value=False)
    return m


@pytest.fixture
def plex_env(monkeypatch):
    monkeypatch.setenv("PLEX_ADDRESS", "http://plex:32400")
    monkeypatch.setenv("PLEX_TOKEN", "s3cr3t-token")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_refreshes_all_show_sections(plex_env):
    """A 'show' refresh hits /library/sections/{id}/refresh for every section
    whose type is 'show' (and none of the movie sections)."""
    with patch("urllib.request.urlopen") as m:
        m.side_effect = [_resp(json_data=_SECTIONS), _resp(), _resp()]
        count = refresh_plex_sections("show")

    assert count == 2  # sections 1 and 3
    refresh_urls = [c.args[0].full_url for c in m.call_args_list[1:]]
    assert "/library/sections/1/refresh" in refresh_urls[0]
    assert "/library/sections/3/refresh" in refresh_urls[1]
    assert all("/2/refresh" not in u for u in refresh_urls)


def test_refreshes_only_movie_section(plex_env):
    with patch("urllib.request.urlopen") as m:
        m.side_effect = [_resp(json_data=_SECTIONS), _resp()]
        count = refresh_plex_sections("movie")

    assert count == 1
    assert "/library/sections/2/refresh" in m.call_args_list[1].args[0].full_url


def test_no_matching_section_type_is_noop(plex_env):
    sections = {"MediaContainer": {"Directory": [
        {"key": "2", "type": "movie", "title": "Movies"}]}}
    with patch("urllib.request.urlopen") as m:
        m.side_effect = [_resp(json_data=sections)]
        count = refresh_plex_sections("show")

    assert count == 0
    # only the section listing call, no refresh calls
    assert m.call_count == 1


def test_missing_config_is_noop(monkeypatch):
    monkeypatch.delenv("PLEX_ADDRESS", raising=False)
    monkeypatch.delenv("PLEX_TOKEN", raising=False)
    with patch("urllib.request.urlopen") as m:
        count = refresh_plex_sections("show")
    assert count == 0
    m.assert_not_called()


def test_token_sent_as_header_not_in_url(plex_env):
    with patch("urllib.request.urlopen") as m:
        m.side_effect = [_resp(json_data=_SECTIONS), _resp(), _resp()]
        refresh_plex_sections("show")

    for c in m.call_args_list:
        req = c.args[0]
        # token present as a header
        header_vals = list(req.headers.values())
        assert "s3cr3t-token" in header_vals
        assert any("plex-token" in k.lower() for k in req.headers)
        # token never in the URL
        assert "s3cr3t-token" not in req.full_url


def test_listing_failure_is_swallowed(plex_env):
    with patch("urllib.request.urlopen") as m:
        m.side_effect = urllib.error.URLError("connection refused")
        count = refresh_plex_sections("show")  # must not raise
    assert count == 0


def test_per_section_http_error_continues(plex_env):
    """An isolated per-section HTTP error (e.g. 500) doesn't stop the others."""
    with patch("urllib.request.urlopen") as m:
        m.side_effect = [
            _resp(json_data=_SECTIONS),
            urllib.error.HTTPError("u", 500, "err", {}, None),  # section 1 fails
            _resp(),                                            # section 3 ok
        ]
        count = refresh_plex_sections("show")
    assert count == 1


def test_connection_error_aborts_remaining_sections(plex_env):
    """A connection-level failure (URLError, Plex unreachable) stops the loop —
    the remaining sections aren't hammered with the same doomed request."""
    with patch("urllib.request.urlopen") as m:
        m.side_effect = [
            _resp(json_data=_SECTIONS),                 # listing (2 show sections)
            urllib.error.URLError("connection refused"),  # section 1 unreachable
            _resp(),                                    # section 3 — must NOT be tried
        ]
        count = refresh_plex_sections("show")
    assert count == 0
    # listing + first section only; second section never attempted
    assert m.call_count == 2


def test_invalid_media_type_is_noop(plex_env):
    with patch("urllib.request.urlopen") as m:
        count = refresh_plex_sections("bogus")
    assert count == 0
    m.assert_not_called()


@pytest.mark.parametrize("body", [
    ["not", "a", "dict"],                       # JSON array (e.g. proxy error page)
    {"MediaContainer": None},                   # null container
    "just a string",                            # JSON string
    {"MediaContainer": {"Directory": None}},    # null directory list
])
def test_non_dict_or_malformed_json_is_swallowed(plex_env, body):
    """A valid-JSON-but-unexpected-shape response must return 0, not raise —
    the helper documents 'never raises'."""
    with patch("urllib.request.urlopen") as m:
        m.side_effect = [_resp(json_data=body)]
        count = refresh_plex_sections("show")  # must not raise
    assert count == 0


def test_secret_read_oserror_is_swallowed(monkeypatch):
    """A misconfigured Docker secret (e.g. path is a directory -> IsADirectoryError,
    an OSError) must not propagate out of the helper."""
    monkeypatch.setenv("PLEX_ADDRESS", "http://plex:32400")
    monkeypatch.delenv("PLEX_TOKEN", raising=False)  # force the secret-file path
    with patch("builtins.open", side_effect=IsADirectoryError("is a directory")):
        with patch("urllib.request.urlopen") as m:
            count = refresh_plex_sections("show")  # must not raise
    assert count == 0
    m.assert_not_called()


# ---------------------------------------------------------------------------
# Library scanner integration: _maybe_refresh_plex
# ---------------------------------------------------------------------------

class TestMaybeRefreshPlex:
    """The library scanner's gate: after symlinking, ask Plex to refresh the
    show/movie sections that received content, only when PLEX_REFRESH is on."""

    def test_shows_and_movies_refresh_both_types(self, monkeypatch):
        monkeypatch.setenv("PLEX_REFRESH", "true")
        from utils.library import _maybe_refresh_plex
        with patch("utils.plex_refresh.refresh_plex_sections") as ref:
            _maybe_refresh_plex({"Some Show"}, {"Some Movie"})
        called_types = sorted(c.args[0] for c in ref.call_args_list)
        assert called_types == ["movie", "show"]

    def test_only_shows_refreshes_show_only(self, monkeypatch):
        monkeypatch.setenv("PLEX_REFRESH", "true")
        from utils.library import _maybe_refresh_plex
        with patch("utils.plex_refresh.refresh_plex_sections") as ref:
            _maybe_refresh_plex({"Some Show"}, set())
        ref.assert_called_once_with("show")

    def test_disabled_flag_is_noop(self, monkeypatch):
        monkeypatch.setenv("PLEX_REFRESH", "false")
        from utils.library import _maybe_refresh_plex
        with patch("utils.plex_refresh.refresh_plex_sections") as ref:
            _maybe_refresh_plex({"Some Show"}, {"Some Movie"})
        ref.assert_not_called()

    def test_flag_unset_is_noop(self, monkeypatch):
        monkeypatch.delenv("PLEX_REFRESH", raising=False)
        from utils.library import _maybe_refresh_plex
        with patch("utils.plex_refresh.refresh_plex_sections") as ref:
            _maybe_refresh_plex({"Some Show"}, set())
        ref.assert_not_called()

    def test_nothing_symlinked_is_noop(self, monkeypatch):
        monkeypatch.setenv("PLEX_REFRESH", "true")
        from utils.library import _maybe_refresh_plex
        with patch("utils.plex_refresh.refresh_plex_sections") as ref:
            _maybe_refresh_plex(set(), set())
        ref.assert_not_called()

    def test_refresh_exception_is_swallowed(self, monkeypatch):
        monkeypatch.setenv("PLEX_REFRESH", "true")
        from utils.library import _maybe_refresh_plex
        with patch("utils.plex_refresh.refresh_plex_sections",
                   side_effect=RuntimeError("boom")):
            # must not raise into the scan loop
            _maybe_refresh_plex({"Some Show"}, set())
