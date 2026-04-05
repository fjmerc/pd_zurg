"""Tests for rclone command construction and VFS flag handling."""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _extract_flag(command, flag_prefix):
    """Extract the value of a --flag=value from a command list."""
    for arg in command:
        if arg.startswith(f'--{flag_prefix}='):
            return arg.split('=', 1)[1]
    return None


def _has_flag(command, flag_prefix):
    """Check if a --flag appears in the command list."""
    return any(arg.startswith(f'--{flag_prefix}') for arg in command)


@pytest.fixture
def rclone_env(monkeypatch):
    """Set minimum env vars for rclone setup and clean rclone-specific vars."""
    monkeypatch.setenv('RD_API_KEY', 'test_key')
    monkeypatch.setenv('RCLONE_MOUNT_NAME', 'test_mount')
    for var in ['NFS_ENABLED', 'RCLONE_DIR_CACHE_TIME', 'RCLONE_VFS_CACHE_MODE',
                'RCLONE_VFS_CACHE_MAX_SIZE', 'RCLONE_VFS_CACHE_MAX_AGE']:
        monkeypatch.delenv(var, raising=False)


def _run_setup(monkeypatch, nfs=False):
    """Run rclone.setup() with all externals mocked, return the rclone command."""
    captured = {}

    with patch('rclone.rclone.ProcessHandler') as mock_ph, \
         patch('rclone.rclone.wait_for_url', return_value=True), \
         patch('rclone.rclone.notify'), \
         patch('rclone.rclone.atomic_write', MagicMock()), \
         patch('rclone.rclone.get_port_from_config', return_value='9999'), \
         patch('rclone.rclone.refresh_globals'), \
         patch('rclone.rclone.find_available_port', return_value=8080), \
         patch('os.path.exists', return_value=False), \
         patch('os.makedirs'), \
         patch('subprocess.run'), \
         patch('builtins.open', MagicMock()):

        mock_handler = MagicMock()
        mock_ph.return_value = mock_handler

        def capture_cmd(name, cwd, cmd, *args, **kwargs):
            captured['cmd'] = cmd
        mock_handler.start_process.side_effect = capture_cmd

        import rclone.rclone as mod
        monkeypatch.setattr(mod, 'RCLONEMN', 'test_mount')
        monkeypatch.setattr(mod, 'RDAPIKEY', 'test_key')
        monkeypatch.setattr(mod, 'ADAPIKEY', None)
        monkeypatch.setattr(mod, 'NFSMOUNT', 'true' if nfs else None)
        monkeypatch.setattr(mod, 'NFSPORT', None)
        monkeypatch.setattr(mod, 'PLEXDEBRID', None)
        monkeypatch.setattr(mod, 'ZURGUSER', None)
        monkeypatch.setattr(mod, 'ZURGPASS', None)
        monkeypatch.setattr(mod, 'RCLONELOGLEVEL', 'NOTICE')

        mod.setup()

    return captured['cmd']


class TestFuseCommandFlags:
    """Test rclone FUSE mount command construction."""

    def test_default_dir_cache_time(self, rclone_env, monkeypatch):
        """Without RCLONE_DIR_CACHE_TIME set, default is 10s."""
        cmd = _run_setup(monkeypatch)
        assert _extract_flag(cmd, 'dir-cache-time') == '10s'

    def test_custom_dir_cache_time(self, rclone_env, monkeypatch):
        """RCLONE_DIR_CACHE_TIME=5m overrides the default."""
        monkeypatch.setenv('RCLONE_DIR_CACHE_TIME', '5m')
        cmd = _run_setup(monkeypatch)
        assert _extract_flag(cmd, 'dir-cache-time') == '5m'

    def test_empty_dir_cache_time_uses_default(self, rclone_env, monkeypatch):
        """Empty string RCLONE_DIR_CACHE_TIME falls back to 10s, not ''."""
        monkeypatch.setenv('RCLONE_DIR_CACHE_TIME', '')
        cmd = _run_setup(monkeypatch)
        assert _extract_flag(cmd, 'dir-cache-time') == '10s'

    def test_whitespace_dir_cache_time_uses_default(self, rclone_env, monkeypatch):
        """Whitespace-only RCLONE_DIR_CACHE_TIME falls back to 10s."""
        monkeypatch.setenv('RCLONE_DIR_CACHE_TIME', '  ')
        cmd = _run_setup(monkeypatch)
        assert _extract_flag(cmd, 'dir-cache-time') == '10s'

    def test_fuse_no_vfs_cache_mode_flag(self, rclone_env, monkeypatch):
        """FUSE mount does not include --vfs-cache-mode (rclone native env var handles it)."""
        cmd = _run_setup(monkeypatch)
        assert not _has_flag(cmd, 'vfs-cache-mode')

    def test_vfs_cache_max_size(self, rclone_env, monkeypatch):
        """RCLONE_VFS_CACHE_MAX_SIZE is passed as --vfs-cache-max-size flag."""
        monkeypatch.setenv('RCLONE_VFS_CACHE_MAX_SIZE', '10G')
        cmd = _run_setup(monkeypatch)
        assert _extract_flag(cmd, 'vfs-cache-max-size') == '10G'

    def test_vfs_cache_max_age(self, rclone_env, monkeypatch):
        """RCLONE_VFS_CACHE_MAX_AGE is passed as --vfs-cache-max-age flag."""
        monkeypatch.setenv('RCLONE_VFS_CACHE_MAX_AGE', '24h')
        cmd = _run_setup(monkeypatch)
        assert _extract_flag(cmd, 'vfs-cache-max-age') == '24h'

    def test_vfs_cache_max_size_not_set(self, rclone_env, monkeypatch):
        """Without RCLONE_VFS_CACHE_MAX_SIZE, no --vfs-cache-max-size flag."""
        cmd = _run_setup(monkeypatch)
        assert not _has_flag(cmd, 'vfs-cache-max-size')

    def test_vfs_cache_max_size_empty(self, rclone_env, monkeypatch):
        """Empty RCLONE_VFS_CACHE_MAX_SIZE does not produce a flag."""
        monkeypatch.setenv('RCLONE_VFS_CACHE_MAX_SIZE', '')
        cmd = _run_setup(monkeypatch)
        assert not _has_flag(cmd, 'vfs-cache-max-size')

    def test_vfs_cache_max_size_whitespace(self, rclone_env, monkeypatch):
        """Whitespace-only RCLONE_VFS_CACHE_MAX_SIZE does not produce a flag."""
        monkeypatch.setenv('RCLONE_VFS_CACHE_MAX_SIZE', '  ')
        cmd = _run_setup(monkeypatch)
        assert not _has_flag(cmd, 'vfs-cache-max-size')


class TestNfsCommandFlags:
    """Test rclone NFS server command construction."""

    def test_nfs_default_vfs_cache_mode(self, rclone_env, monkeypatch):
        """NFS mode defaults --vfs-cache-mode to full."""
        cmd = _run_setup(monkeypatch, nfs=True)
        assert _extract_flag(cmd, 'vfs-cache-mode') == 'full'
        assert _extract_flag(cmd, 'dir-cache-time') == '10s'

    def test_nfs_custom_vfs_cache_mode(self, rclone_env, monkeypatch):
        """NFS mode respects RCLONE_VFS_CACHE_MODE override."""
        monkeypatch.setenv('RCLONE_VFS_CACHE_MODE', 'minimal')
        cmd = _run_setup(monkeypatch, nfs=True)
        assert _extract_flag(cmd, 'vfs-cache-mode') == 'minimal'

    def test_nfs_empty_vfs_cache_mode_uses_default(self, rclone_env, monkeypatch):
        """Empty string RCLONE_VFS_CACHE_MODE falls back to full, not ''."""
        monkeypatch.setenv('RCLONE_VFS_CACHE_MODE', '')
        cmd = _run_setup(monkeypatch, nfs=True)
        assert _extract_flag(cmd, 'vfs-cache-mode') == 'full'

    def test_nfs_whitespace_vfs_cache_mode_uses_default(self, rclone_env, monkeypatch):
        """Whitespace-only RCLONE_VFS_CACHE_MODE falls back to full."""
        monkeypatch.setenv('RCLONE_VFS_CACHE_MODE', '  ')
        cmd = _run_setup(monkeypatch, nfs=True)
        assert _extract_flag(cmd, 'vfs-cache-mode') == 'full'

    def test_nfs_vfs_cache_max_size(self, rclone_env, monkeypatch):
        """NFS mode passes RCLONE_VFS_CACHE_MAX_SIZE as flag."""
        monkeypatch.setenv('RCLONE_VFS_CACHE_MAX_SIZE', '50G')
        cmd = _run_setup(monkeypatch, nfs=True)
        assert _extract_flag(cmd, 'vfs-cache-max-size') == '50G'

    def test_nfs_vfs_cache_max_age(self, rclone_env, monkeypatch):
        """NFS mode passes RCLONE_VFS_CACHE_MAX_AGE as flag."""
        monkeypatch.setenv('RCLONE_VFS_CACHE_MAX_AGE', '1h')
        cmd = _run_setup(monkeypatch, nfs=True)
        assert _extract_flag(cmd, 'vfs-cache-max-age') == '1h'
