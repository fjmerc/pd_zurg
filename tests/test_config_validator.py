"""Tests for startup config validation (Plan 06)."""

import os
import pytest
from utils.config_validator import validate_config, _is_valid_url, _is_truthy, run_validation
from base import config


def _validate_with_reload():
    """Reload config singleton then validate. Ensures env var changes are picked up."""
    config.load()
    return validate_config()


class TestURLValidation:

    def test_valid_http_url(self):
        assert _is_valid_url('http://192.168.1.100:32400') is True

    def test_valid_https_url(self):
        assert _is_valid_url('https://plex.example.com') is True

    def test_missing_scheme(self):
        assert _is_valid_url('192.168.1.100:32400') is False

    def test_empty_string(self):
        assert _is_valid_url('') is False

    def test_just_scheme(self):
        assert _is_valid_url('http://') is False

    def test_ftp_scheme(self):
        assert _is_valid_url('ftp://example.com') is False

    def test_url_with_path(self):
        assert _is_valid_url('http://localhost:32400/web') is True


class TestIsTruthy:

    def test_true_string(self):
        assert _is_truthy('true') is True

    def test_True_string(self):
        assert _is_truthy('True') is True

    def test_false_string(self):
        assert _is_truthy('false') is False

    def test_none_value(self):
        assert _is_truthy(None) is False

    def test_one_string(self):
        assert _is_truthy('1') is True

    def test_yes_string(self):
        assert _is_truthy('yes') is True


class TestConfigValidation:

    def test_valid_blackhole_debrid(self, clean_env, env_vars):
        """Valid BLACKHOLE_DEBRID value should not error."""
        env_vars(BLACKHOLE_DEBRID='realdebrid')
        result = _validate_with_reload()
        debrid_errors = [e for e in result.errors if 'BLACKHOLE_DEBRID' in e]
        assert len(debrid_errors) == 0

    def test_invalid_blackhole_debrid(self, clean_env, env_vars):
        """Typo in BLACKHOLE_DEBRID should produce an error."""
        env_vars(BLACKHOLE_DEBRID='realdebird')
        result = _validate_with_reload()
        debrid_errors = [e for e in result.errors if 'BLACKHOLE_DEBRID' in e]
        assert len(debrid_errors) == 1
        assert 'realdebird' in debrid_errors[0]

    def test_invalid_notification_level(self, clean_env, env_vars):
        """Invalid NOTIFICATION_LEVEL should error."""
        env_vars(NOTIFICATION_LEVEL='verbose')
        result = _validate_with_reload()
        level_errors = [e for e in result.errors if 'NOTIFICATION_LEVEL' in e]
        assert len(level_errors) == 1

    def test_valid_notification_level(self, clean_env, env_vars):
        """Valid NOTIFICATION_LEVEL should pass."""
        env_vars(NOTIFICATION_LEVEL='warning')
        result = _validate_with_reload()
        level_errors = [e for e in result.errors if 'NOTIFICATION_LEVEL' in e]
        assert len(level_errors) == 0

    def test_invalid_numeric_var(self, clean_env, env_vars):
        """Non-integer STATUS_UI_PORT should error."""
        env_vars(STATUS_UI_PORT='not_a_number')
        result = _validate_with_reload()
        port_errors = [e for e in result.errors if 'STATUS_UI_PORT' in e]
        assert len(port_errors) == 1

    def test_port_out_of_range(self, clean_env, env_vars):
        """Out-of-range port should warn."""
        env_vars(STATUS_UI_PORT='99999')
        result = _validate_with_reload()
        port_warnings = [w for w in result.warnings if 'STATUS_UI_PORT' in w]
        assert len(port_warnings) == 1

    def test_valid_port(self, clean_env, env_vars):
        """Valid port should pass without warnings."""
        env_vars(STATUS_UI_PORT='8080')
        result = _validate_with_reload()
        port_issues = [x for x in result.errors + result.warnings if 'STATUS_UI_PORT' in x]
        assert len(port_issues) == 0

    def test_status_auth_missing_colon(self, clean_env, env_vars):
        """STATUS_UI_AUTH without colon should error."""
        env_vars(STATUS_UI_AUTH='adminchangeme')
        result = _validate_with_reload()
        auth_errors = [e for e in result.errors if 'STATUS_UI_AUTH' in e]
        assert len(auth_errors) == 1

    def test_status_auth_valid(self, clean_env, env_vars):
        """STATUS_UI_AUTH with colon should pass."""
        env_vars(STATUS_UI_AUTH='admin:changeme')
        result = _validate_with_reload()
        auth_errors = [e for e in result.errors if 'STATUS_UI_AUTH' in e]
        assert len(auth_errors) == 0

    def test_invalid_url_plex_address(self, clean_env, env_vars):
        """PLEX_ADDRESS without scheme should error."""
        env_vars(PLEX_ADDRESS='192.168.1.100:32400')
        result = _validate_with_reload()
        url_errors = [e for e in result.errors if 'PLEX_ADDRESS' in e]
        assert len(url_errors) == 1
        assert 'http://' in url_errors[0]

    def test_valid_url_plex_address(self, clean_env, env_vars):
        """PLEX_ADDRESS with scheme should pass."""
        env_vars(PLEX_ADDRESS='http://192.168.1.100:32400')
        result = _validate_with_reload()
        url_errors = [e for e in result.errors if 'PLEX_ADDRESS' in e]
        assert len(url_errors) == 0

    def test_bad_log_level_warns(self, clean_env, env_vars):
        """Non-standard log level should warn, not error."""
        env_vars(ZURG_LOG_LEVEL='VERBOSE')
        result = _validate_with_reload()
        assert any('ZURG_LOG_LEVEL' in w for w in result.warnings)
        assert not any('ZURG_LOG_LEVEL' in e for e in result.errors)

    def test_bad_log_level_warns_legacy_pdzurg(self, clean_env, env_vars):
        """Legacy PDZURG_LOG_LEVEL should still be validated during the 2.19.x window."""
        env_vars(PDZURG_LOG_LEVEL='BOGUS')
        result = _validate_with_reload()
        assert any('PDZURG_LOG_LEVEL' in w for w in result.warnings)
        assert not any('PDZURG_LOG_LEVEL' in e for e in result.errors)

    def test_bad_log_level_warns_new_zurgarr(self, clean_env, env_vars):
        """New ZURGARR_LOG_LEVEL should warn on invalid value just like the legacy name."""
        env_vars(ZURGARR_LOG_LEVEL='BOGUS')
        result = _validate_with_reload()
        assert any('ZURGARR_LOG_LEVEL' in w for w in result.warnings)
        assert not any('ZURGARR_LOG_LEVEL' in e for e in result.errors)

    def test_duplicate_cleanup_keep_valid_local(self, clean_env, env_vars):
        """DUPLICATE_CLEANUP_KEEP=local should pass."""
        env_vars(DUPLICATE_CLEANUP_KEEP='local')
        result = _validate_with_reload()
        keep_errors = [e for e in result.errors if 'DUPLICATE_CLEANUP_KEEP' in e]
        assert len(keep_errors) == 0

    def test_duplicate_cleanup_keep_valid_zurg(self, clean_env, env_vars):
        """DUPLICATE_CLEANUP_KEEP=zurg should pass."""
        env_vars(DUPLICATE_CLEANUP_KEEP='zurg')
        result = _validate_with_reload()
        keep_errors = [e for e in result.errors if 'DUPLICATE_CLEANUP_KEEP' in e]
        assert len(keep_errors) == 0

    def test_duplicate_cleanup_keep_invalid(self, clean_env, env_vars):
        """Invalid DUPLICATE_CLEANUP_KEEP should error."""
        env_vars(DUPLICATE_CLEANUP_KEEP='remote')
        result = _validate_with_reload()
        keep_errors = [e for e in result.errors if 'DUPLICATE_CLEANUP_KEEP' in e]
        assert len(keep_errors) == 1

    def test_rclone_mount_name_special_chars(self, clean_env, env_vars):
        """Mount name with special characters should warn."""
        env_vars(RCLONE_MOUNT_NAME='my mount/name')
        result = _validate_with_reload()
        assert any('RCLONE_MOUNT_NAME' in w for w in result.warnings)

    def test_rclone_mount_name_valid(self, clean_env, env_vars):
        """Valid mount name should pass."""
        env_vars(RCLONE_MOUNT_NAME='pd_zurg-RD')
        result = _validate_with_reload()
        assert not any('RCLONE_MOUNT_NAME' in w for w in result.warnings)

    def test_notification_url_missing_scheme(self, clean_env, env_vars):
        """Notification URL without :// should warn."""
        env_vars(NOTIFICATION_URL='not-a-url')
        result = _validate_with_reload()
        assert any('NOTIFICATION_URL' in w for w in result.warnings)


class TestRunValidation:

    def test_skip_validation(self, clean_env, env_vars):
        """SKIP_VALIDATION=true should return True without checking."""
        env_vars(SKIP_VALIDATION='true')
        assert run_validation() is True

    def test_clean_config_passes(self, clean_env):
        """No env vars set should pass validation (no features enabled)."""
        config.load()
        assert run_validation() is True
