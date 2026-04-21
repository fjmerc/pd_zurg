"""Tests for Status UI enhancements (Plan 11).

Covers: log reader, config viewer, mount history, and restart_service.
"""

import json
import os
import time
import pytest
from utils.status_server import (
    MountHistory, read_log_lines, get_sanitized_config,
    _SENSITIVE_PATTERNS, _CONFIG_PREFIXES,
)
from utils.processes import restart_service


# ---------------------------------------------------------------------------
# MountHistory
# ---------------------------------------------------------------------------

class TestMountHistory:

    def test_initial_empty(self):
        mh = MountHistory()
        assert mh.to_dict() == {}

    def test_record_first_entry(self):
        mh = MountHistory()
        mh.record('/data/test', True, True)
        hist = mh.to_dict()
        assert '/data/test' in hist
        assert len(hist['/data/test']) == 1
        entry = hist['/data/test'][0]
        assert entry['mounted'] is True
        assert entry['accessible'] is True
        assert 'timestamp' in entry

    def test_no_duplicate_on_same_state(self):
        """Unchanged state should not create a new entry."""
        mh = MountHistory()
        mh.record('/data/test', True, True)
        mh.record('/data/test', True, True)
        mh.record('/data/test', True, True)
        assert len(mh.to_dict()['/data/test']) == 1

    def test_records_state_change(self):
        """State changes should create new entries."""
        mh = MountHistory()
        mh.record('/data/test', True, True)
        mh.record('/data/test', False, False)
        mh.record('/data/test', True, True)
        assert len(mh.to_dict()['/data/test']) == 3

    def test_records_accessibility_change(self):
        """Mounted but inaccessible should be a distinct state."""
        mh = MountHistory()
        mh.record('/data/test', True, True)
        mh.record('/data/test', True, False)  # Mounted but not accessible
        entries = mh.to_dict()['/data/test']
        assert len(entries) == 2
        assert entries[1]['mounted'] is True
        assert entries[1]['accessible'] is False

    def test_multiple_paths_independent(self):
        """Different mount paths should have independent histories."""
        mh = MountHistory()
        mh.record('/data/rd', True, True)
        mh.record('/data/ad', False, False)
        hist = mh.to_dict()
        assert len(hist['/data/rd']) == 1
        assert len(hist['/data/ad']) == 1
        assert hist['/data/rd'][0]['mounted'] is True
        assert hist['/data/ad'][0]['mounted'] is False

    def test_max_entries_capped(self):
        """History should be capped at max_entries."""
        mh = MountHistory(max_entries=5)
        for i in range(10):
            mh.record('/data/test', i % 2 == 0, True)
        assert len(mh.to_dict()['/data/test']) == 5

    def test_to_dict_returns_lists(self):
        """to_dict should return plain lists, not deques."""
        mh = MountHistory()
        mh.record('/data/test', True, True)
        hist = mh.to_dict()
        assert isinstance(hist['/data/test'], list)

    def test_to_dict_serializable(self):
        """to_dict output should be JSON serializable."""
        mh = MountHistory()
        mh.record('/data/test', True, True)
        mh.record('/data/test', False, False)
        result = json.dumps(mh.to_dict())
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Log reader
# ---------------------------------------------------------------------------

class TestReadLogLines:

    def test_empty_dir(self, tmp_dir):
        """Should return empty list when no log files exist."""
        lines = read_log_lines(log_dir=tmp_dir)
        assert lines == []

    def test_reads_last_n_lines(self, tmp_dir):
        """Should return last N lines from log file."""
        log_file = os.path.join(tmp_dir, 'PDZURG-2026-03-21.log')
        with open(log_file, 'w') as f:
            for i in range(20):
                f.write(f'Mar 21, 2026 10:{i:02d}:00 - INFO - Line {i}\n')
        lines = read_log_lines(lines=5, log_dir=tmp_dir)
        assert len(lines) == 5
        assert 'Line 19' in lines[-1]
        assert 'Line 15' in lines[0]

    def test_level_filter(self, tmp_dir):
        """Should filter by log level."""
        log_file = os.path.join(tmp_dir, 'PDZURG-2026-03-21.log')
        with open(log_file, 'w') as f:
            f.write('INFO - info line\n')
            f.write('ERROR - error line\n')
            f.write('WARNING - warning line\n')
            f.write('ERROR - another error\n')
        lines = read_log_lines(lines=100, level='ERROR', log_dir=tmp_dir)
        assert len(lines) == 2
        assert all('ERROR' in l for l in lines)

    def test_picks_most_recent_file(self, tmp_dir):
        """Should read from the most recent log file."""
        with open(os.path.join(tmp_dir, 'PDZURG-2026-03-20.log'), 'w') as f:
            f.write('old log\n')
        with open(os.path.join(tmp_dir, 'PDZURG-2026-03-21.log'), 'w') as f:
            f.write('new log\n')
        lines = read_log_lines(lines=10, log_dir=tmp_dir)
        assert len(lines) == 1
        assert 'new log' in lines[0]

    def test_handles_empty_log_file(self, tmp_dir):
        """Should return empty list for empty log file."""
        log_file = os.path.join(tmp_dir, 'PDZURG-2026-03-21.log')
        open(log_file, 'w').close()
        lines = read_log_lines(log_dir=tmp_dir)
        assert lines == []

    def test_nonexistent_dir(self):
        """Should return empty list for nonexistent directory."""
        lines = read_log_lines(log_dir='/nonexistent/path')
        assert lines == []


# ---------------------------------------------------------------------------
# Config viewer
# ---------------------------------------------------------------------------

class TestGetSanitizedConfig:

    def test_masks_api_keys(self, monkeypatch):
        """API keys should be masked."""
        monkeypatch.setenv('RD_API_KEY', 'abcdefghijklmnop')
        config = get_sanitized_config()
        assert 'RD_API_KEY' in config
        assert 'abcdefghijklmnop' not in config['RD_API_KEY']
        assert '****' in config['RD_API_KEY']

    def test_masks_tokens(self, monkeypatch):
        """Tokens should be masked."""
        monkeypatch.setenv('PLEX_TOKEN', 'my-secret-token-value')
        config = get_sanitized_config()
        assert 'PLEX_TOKEN' in config
        assert 'my-secret-token-value' not in config['PLEX_TOKEN']

    def test_shows_non_sensitive_values(self, monkeypatch):
        """Non-sensitive values should be shown in full."""
        monkeypatch.setenv('ZURG_ENABLED', 'true')
        config = get_sanitized_config()
        assert config.get('ZURG_ENABLED') == 'true'

    def test_excludes_unrelated_vars(self, monkeypatch):
        """Non-pd_zurg env vars should be excluded."""
        monkeypatch.setenv('HOME', '/root')
        monkeypatch.setenv('PATH', '/usr/bin')
        config = get_sanitized_config()
        assert 'HOME' not in config
        assert 'PATH' not in config

    def test_empty_value_shows_not_set(self, monkeypatch):
        """Empty values should show '(not set)'."""
        monkeypatch.setenv('ZURG_ENABLED', '')
        config = get_sanitized_config()
        assert config.get('ZURG_ENABLED') == '(not set)'

    def test_short_sensitive_value_fully_masked(self, monkeypatch):
        """Short sensitive values should be fully masked."""
        monkeypatch.setenv('RD_API_KEY', 'short')
        config = get_sanitized_config()
        assert config['RD_API_KEY'] == '****'

    def test_long_sensitive_shows_partial(self, monkeypatch):
        """Long sensitive values should show first/last 4 chars."""
        monkeypatch.setenv('RD_API_KEY', 'abcdefghijklmnop')
        config = get_sanitized_config()
        val = config['RD_API_KEY']
        assert val.startswith('abcd')
        assert val.endswith('mnop')
        assert '****' in val

    def test_result_json_serializable(self, monkeypatch):
        """Config output should be JSON serializable."""
        monkeypatch.setenv('ZURG_ENABLED', 'true')
        result = json.dumps(get_sanitized_config())
        assert isinstance(result, str)

    def test_config_prefixes_coverage(self):
        """All expected prefixes should be defined."""
        assert 'ZURG' in _CONFIG_PREFIXES
        assert 'PLEX' in _CONFIG_PREFIXES
        assert 'BLACKHOLE' in _CONFIG_PREFIXES
        assert 'NOTIFICATION' in _CONFIG_PREFIXES

    def test_sensitive_patterns_coverage(self):
        """All expected sensitive patterns should be defined."""
        assert 'KEY' in _SENSITIVE_PATTERNS
        assert 'TOKEN' in _SENSITIVE_PATTERNS
        assert 'PASS' in _SENSITIVE_PATTERNS
        assert 'SECRET' in _SENSITIVE_PATTERNS


# ---------------------------------------------------------------------------
# localStorage migration (plan 35 Phase 3)
# ---------------------------------------------------------------------------

class TestLocalStorageMigration:
    """pd_zurg_* localStorage keys are renamed to zurgarr_* via a one-shot
    migration helper embedded in every served page. These tests lock down
    the invariants: helper present everywhere it's needed, no literal legacy
    keys surviving in any write path, theme + log_wrap both routed through
    the helper, and the standalone settings-setup fallback page carries its
    own inline migration.
    """

    def test_base_head_embeds_migration_helper(self):
        from utils.ui_common import get_base_head
        html = get_base_head('Test')
        assert '_zurgarrLSGet' in html
        assert '_zurgarrLSSet' in html
        # Helper dual-reads: new key then legacy + migrate + delete
        assert "'zurgarr_'" in html
        assert "'pd_zurg_'" in html
        assert 'removeItem' in html

    def test_base_head_has_no_literal_legacy_theme_key(self):
        """No caller reads or writes the literal 'pd_zurg_theme' key —
        all access goes through ``_zurgarrLS{Get,Set}('theme')``. The
        helper itself concatenates `'pd_zurg_' + key`, so the full literal
        `pd_zurg_theme` must not appear anywhere in the served head."""
        from utils.ui_common import get_base_head
        html = get_base_head('Test')
        assert 'pd_zurg_theme' not in html
        assert 'zurgarr_theme' not in html

    def test_theme_init_reads_via_helper(self):
        """The FOUC-preventing head script must use the migration helper
        so a user who only has the legacy key still gets the right theme
        on the page load that also migrates the key."""
        from utils.ui_common import get_base_head, THEME_INIT_SCRIPT
        assert "_zurgarrLSGet('theme')" in THEME_INIT_SCRIPT
        # And is actually embedded in the served head output
        assert "_zurgarrLSGet('theme')" in get_base_head('Test')
        # FOUC behaviour preserved
        assert "setAttribute('data-theme'" in THEME_INIT_SCRIPT

    def test_theme_toggle_writes_via_helper(self):
        from utils.ui_common import THEME_TOGGLE_JS
        assert "_zurgarrLSSet('theme'" in THEME_TOGGLE_JS
        # No surviving direct writes to legacy or literal new key
        assert "setItem('pd_zurg_theme'" not in THEME_TOGGLE_JS
        assert "setItem('zurgarr_theme'" not in THEME_TOGGLE_JS

    def test_system_page_log_wrap_uses_helper(self):
        from utils.system_page import get_system_html
        html = get_system_html()
        assert "_zurgarrLSGet('log_wrap')" in html
        assert "_zurgarrLSSet('log_wrap'" in html
        # No literal legacy key appears anywhere on the system page
        assert 'pd_zurg_log_wrap' not in html
        assert 'zurgarr_log_wrap' not in html

    def test_settings_setup_fallback_embeds_shared_helper(self):
        """The auth-not-configured settings page is a self-contained HTML
        string that doesn't share ``get_base_head()``. To avoid a hand-rolled
        migration snippet drifting from the canonical helper, it must embed
        ``LS_MIGRATION_JS`` verbatim and read via ``_zurgarrLSGet`` — the
        same contract the other pages have.
        """
        from utils.status_server import _SETTINGS_SETUP_HTML
        from utils.ui_common import LS_MIGRATION_JS
        assert LS_MIGRATION_JS in _SETTINGS_SETUP_HTML
        assert "_zurgarrLSGet('theme')" in _SETTINGS_SETUP_HTML
        # FOUC contract preserved for the fallback page
        assert "setAttribute('data-theme'" in _SETTINGS_SETUP_HTML
        # No literal legacy key should leak out of the helper's concat-path
        assert 'pd_zurg_theme' not in _SETTINGS_SETUP_HTML

    def test_migration_helper_copies_before_deleting_legacy(self):
        """The dual-read path must write the new key BEFORE removing the
        legacy key — if the order inverted, a crash between the two calls
        (or a storage-quota failure on the setItem) would destroy the
        user's preference instead of preserving it.
        """
        from utils.ui_common import LS_MIGRATION_JS
        set_idx = LS_MIGRATION_JS.index('setItem(nk,ov)')
        rm_idx = LS_MIGRATION_JS.index('removeItem(ok)')
        assert set_idx < rm_idx, (
            'copy-then-delete invariant violated: setItem(nk,ov) must '
            'precede removeItem(ok) in LS_MIGRATION_JS'
        )

    def test_no_writes_to_legacy_keys_anywhere(self):
        """Sanity sweep across every surface: no JS path writes to the
        pd_zurg_* keys. A write would defeat the migration, because the
        next page load would see the legacy key again and re-migrate an
        outdated value."""
        from utils.ui_common import get_base_head, THEME_TOGGLE_JS
        from utils.system_page import get_system_html
        from utils.status_server import _SETTINGS_SETUP_HTML
        surfaces = [
            get_base_head('Test'),
            THEME_TOGGLE_JS,
            get_system_html(),
            _SETTINGS_SETUP_HTML,
        ]
        for html in surfaces:
            assert "setItem('pd_zurg_theme'" not in html
            assert "setItem('pd_zurg_log_wrap'" not in html


# ---------------------------------------------------------------------------
# restart_service
# ---------------------------------------------------------------------------

class TestRestartService:

    def test_nonexistent_service_returns_false(self):
        """Restarting unknown service should return False."""
        result = restart_service('nonexistent_service')
        assert result is False

    def test_case_insensitive_match(self):
        """Service name matching should be case-insensitive."""
        # With no processes registered, all return False
        # but we verify it doesn't crash with different cases
        assert restart_service('ZURG') is False
        assert restart_service('zurg') is False
        assert restart_service('Zurg') is False
