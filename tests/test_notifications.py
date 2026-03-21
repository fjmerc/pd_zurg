"""Tests for notification event and level filtering."""

import pytest
from utils.notifications import LEVEL_ORDER, _VALID_LEVELS


class TestLevelOrder:

    def test_info_lowest(self):
        """Info should have the lowest severity."""
        assert LEVEL_ORDER['info'] < LEVEL_ORDER['warning']
        assert LEVEL_ORDER['info'] < LEVEL_ORDER['error']

    def test_warning_middle(self):
        """Warning should be between info and error."""
        assert LEVEL_ORDER['warning'] > LEVEL_ORDER['info']
        assert LEVEL_ORDER['warning'] < LEVEL_ORDER['error']

    def test_error_highest(self):
        """Error should have the highest severity."""
        assert LEVEL_ORDER['error'] > LEVEL_ORDER['info']
        assert LEVEL_ORDER['error'] > LEVEL_ORDER['warning']

    def test_all_valid_levels_in_order(self):
        """All valid levels should have an entry in LEVEL_ORDER."""
        for level in _VALID_LEVELS:
            assert level in LEVEL_ORDER


class TestValidLevels:

    def test_contains_expected_levels(self):
        """Valid levels should include info, warning, error."""
        assert 'info' in _VALID_LEVELS
        assert 'warning' in _VALID_LEVELS
        assert 'error' in _VALID_LEVELS

    def test_no_unexpected_levels(self):
        """Should not include debug or critical."""
        assert 'debug' not in _VALID_LEVELS
        assert 'critical' not in _VALID_LEVELS


class TestLevelFiltering:
    """Test the level filtering logic used in notify()."""

    def _should_notify(self, event_level, min_level):
        """Simulate the level check from notify()."""
        return LEVEL_ORDER.get(event_level, 0) >= LEVEL_ORDER.get(min_level, 0)

    def test_info_passes_at_info_level(self):
        assert self._should_notify('info', 'info') is True

    def test_warning_passes_at_info_level(self):
        assert self._should_notify('warning', 'info') is True

    def test_error_passes_at_info_level(self):
        assert self._should_notify('error', 'info') is True

    def test_info_blocked_at_warning_level(self):
        assert self._should_notify('info', 'warning') is False

    def test_warning_passes_at_warning_level(self):
        assert self._should_notify('warning', 'warning') is True

    def test_error_passes_at_warning_level(self):
        assert self._should_notify('error', 'warning') is True

    def test_info_blocked_at_error_level(self):
        assert self._should_notify('info', 'error') is False

    def test_warning_blocked_at_error_level(self):
        assert self._should_notify('warning', 'error') is False

    def test_error_passes_at_error_level(self):
        assert self._should_notify('error', 'error') is True
