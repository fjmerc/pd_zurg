"""Shared test fixtures for pd_zurg test suite."""

import os
import sys
import pytest
import tempfile
import shutil

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture
def tmp_dir():
    """Create a temporary directory, cleaned up after test."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def env_vars(monkeypatch):
    """Helper to set multiple environment variables for a test."""
    def _set(**kwargs):
        for key, value in kwargs.items():
            monkeypatch.setenv(key, str(value))
    return _set


@pytest.fixture
def clean_env(monkeypatch):
    """Remove all pd_zurg-related env vars for a clean test slate."""
    pd_vars = [
        'ZURG_ENABLED', 'RD_API_KEY', 'AD_API_KEY', 'PLEX_TOKEN',
        'PLEX_ADDRESS', 'JF_ADDRESS', 'JF_API_KEY', 'PD_ENABLED',
        'BLACKHOLE_ENABLED', 'BLACKHOLE_DEBRID', 'BLACKHOLE_DIR',
        'BLACKHOLE_POLL_INTERVAL', 'BLACKHOLE_SYMLINK_ENABLED',
        'BLACKHOLE_COMPLETED_DIR', 'BLACKHOLE_RCLONE_MOUNT',
        'BLACKHOLE_SYMLINK_TARGET_BASE', 'BLACKHOLE_MOUNT_POLL_TIMEOUT',
        'BLACKHOLE_MOUNT_POLL_INTERVAL', 'BLACKHOLE_SYMLINK_MAX_AGE',
        'NOTIFICATION_URL', 'NOTIFICATION_LEVEL',
        'NOTIFICATION_EVENTS', 'STATUS_UI_ENABLED', 'STATUS_UI_PORT',
        'STATUS_UI_AUTH', 'DUPLICATE_CLEANUP', 'PLEX_REFRESH',
        'SKIP_VALIDATION', 'RCLONE_MOUNT_NAME', 'ZURG_LOG_LEVEL',
        'RCLONE_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'PD_LOG_LEVEL',
        'TORBOX_API_KEY', 'SEERR_ADDRESS', 'SEERR_API_KEY',
        'ZURG_PORT', 'NFS_PORT', 'FFPROBE_STUCK_TIMEOUT',
        'FFPROBE_POLL_INTERVAL', 'AUTO_UPDATE_INTERVAL', 'CLEANUP_INTERVAL',
    ]
    for var in pd_vars:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch
