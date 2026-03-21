"""Tests for process management and restart backoff."""

import pytest
from utils.processes import RestartPolicy, _get_backoff_delay


class TestRestartPolicy:

    def test_default_values(self):
        """Default policy should have sensible defaults."""
        policy = RestartPolicy()
        assert policy.max_restarts == 5
        assert policy.backoff_seconds == [5, 15, 45, 120, 300]
        assert policy.window_seconds == 3600

    def test_custom_values(self):
        """Custom policy values should be stored."""
        policy = RestartPolicy(max_restarts=3, backoff_seconds=[1, 2, 3], window_seconds=600)
        assert policy.max_restarts == 3
        assert policy.backoff_seconds == [1, 2, 3]
        assert policy.window_seconds == 600


class TestBackoffDelay:

    def test_backoff_sequence(self):
        """Verify exponential backoff delays match policy."""
        policy = RestartPolicy()
        assert _get_backoff_delay(policy, 0) == 5
        assert _get_backoff_delay(policy, 1) == 15
        assert _get_backoff_delay(policy, 2) == 45
        assert _get_backoff_delay(policy, 3) == 120
        assert _get_backoff_delay(policy, 4) == 300

    def test_backoff_clamps_at_max(self):
        """Restart count beyond list length should clamp to last value."""
        policy = RestartPolicy(backoff_seconds=[5, 10, 20])
        assert _get_backoff_delay(policy, 0) == 5
        assert _get_backoff_delay(policy, 1) == 10
        assert _get_backoff_delay(policy, 2) == 20
        assert _get_backoff_delay(policy, 3) == 20  # Clamped
        assert _get_backoff_delay(policy, 100) == 20  # Still clamped

    def test_single_backoff_value(self):
        """Policy with single backoff value should always return it."""
        policy = RestartPolicy(backoff_seconds=[30])
        assert _get_backoff_delay(policy, 0) == 30
        assert _get_backoff_delay(policy, 5) == 30

    def test_custom_backoff_sequence(self):
        """Custom backoff values should be respected."""
        policy = RestartPolicy(backoff_seconds=[1, 2, 4, 8, 16])
        for i, expected in enumerate([1, 2, 4, 8, 16]):
            assert _get_backoff_delay(policy, i) == expected
