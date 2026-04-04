"""Tests for get_system_stats() system metrics collection."""

import os
import pytest
from unittest.mock import patch, mock_open, MagicMock, call

_real_open = open  # capture before any patching


def _make_open_side_effect(file_contents, blocked=None):
    """Return a side_effect for builtins.open that intercepts specific paths.

    file_contents: dict of {path: content_string} — these return mock data.
    blocked: set of paths that should raise FileNotFoundError.
    All other paths pass through to the real open().
    """
    blocked = blocked or set()
    _real_mock_files = {}
    for path, content in file_contents.items():
        _real_mock_files[path] = mock_open(read_data=content)

    def _side_effect(path, *args, **kwargs):
        if path in _real_mock_files:
            return _real_mock_files[path](path, *args, **kwargs)
        if path in blocked:
            raise FileNotFoundError(f'mock: {path} not found')
        return _real_open(path, *args, **kwargs)

    return _side_effect


class TestDiskStats:
    """Tests for disk space collection via os.statvfs."""

    def test_disk_stats_returned(self):
        """Should return disk used/total/percent when statvfs succeeds."""
        from utils.status_server import get_system_stats

        fake_stat = MagicMock()
        fake_stat.f_frsize = 4096
        fake_stat.f_blocks = 1000000    # ~4 GB total
        fake_stat.f_bavail = 400000     # ~1.6 GB free

        with patch('os.statvfs', return_value=fake_stat), \
             patch('os.path.isdir', return_value=True):
            stats = get_system_stats()

        expected_total = 4096 * 1000000
        expected_used = expected_total - 4096 * 400000
        assert stats['disk_total_bytes'] == expected_total
        assert stats['disk_used_bytes'] == expected_used
        assert stats['disk_percent'] == round(expected_used / expected_total * 100, 1)

    def test_disk_fallback_to_root(self):
        """Should fall back to / when /config doesn't exist."""
        from utils.status_server import get_system_stats

        fake_stat = MagicMock()
        fake_stat.f_frsize = 4096
        fake_stat.f_blocks = 500000
        fake_stat.f_bavail = 250000

        with patch('os.path.isdir', return_value=False), \
             patch('os.statvfs', return_value=fake_stat) as mock_statvfs:
            get_system_stats()

        mock_statvfs.assert_called_with('/')

    def test_disk_config_preferred(self):
        """Should use /config when it exists."""
        from utils.status_server import get_system_stats

        fake_stat = MagicMock()
        fake_stat.f_frsize = 4096
        fake_stat.f_blocks = 500000
        fake_stat.f_bavail = 250000

        with patch('os.path.isdir', return_value=True), \
             patch('os.statvfs', return_value=fake_stat) as mock_statvfs:
            get_system_stats()

        mock_statvfs.assert_called_with('/config')

    def test_disk_oserror_graceful(self):
        """Should not crash when statvfs raises OSError."""
        from utils.status_server import get_system_stats

        with patch('os.path.isdir', return_value=True), \
             patch('os.statvfs', side_effect=OSError('no such device')):
            stats = get_system_stats()

        assert 'disk_used_bytes' not in stats
        assert 'disk_total_bytes' not in stats
        assert 'disk_percent' not in stats

    def test_disk_used_clamped_to_zero(self):
        """Should clamp disk_used to 0 when bavail > blocks (overlayfs quirk)."""
        from utils.status_server import get_system_stats

        fake_stat = MagicMock()
        fake_stat.f_frsize = 4096
        fake_stat.f_blocks = 100000
        fake_stat.f_bavail = 200000  # more available than total

        with patch('os.path.isdir', return_value=True), \
             patch('os.statvfs', return_value=fake_stat):
            stats = get_system_stats()

        assert stats['disk_used_bytes'] == 0
        assert stats['disk_percent'] == 0.0

    def test_disk_zero_blocks_skipped(self):
        """Should skip disk stats when f_blocks is 0 (pseudo-fs)."""
        from utils.status_server import get_system_stats

        fake_stat = MagicMock()
        fake_stat.f_frsize = 4096
        fake_stat.f_blocks = 0
        fake_stat.f_bavail = 0

        with patch('os.path.isdir', return_value=True), \
             patch('os.statvfs', return_value=fake_stat):
            stats = get_system_stats()

        assert 'disk_used_bytes' not in stats


class TestFdStats:
    """Tests for open file descriptor counting."""

    def test_fd_open_counted(self):
        """Should count entries in /proc/self/fd."""
        from utils.status_server import get_system_stats

        limits_content = (
            "Limit                     Soft Limit           Hard Limit           Units\n"
            "Max cpu time              unlimited            unlimited            seconds\n"
            "Max open files            1048576              1048576              files\n"
            "Max processes             unlimited            unlimited            processes\n"
        )

        open_side_effect = _make_open_side_effect({
            '/proc/self/limits': limits_content,
        })

        with patch('os.listdir', return_value=['0', '1', '2', '3', '4']), \
             patch('builtins.open', side_effect=open_side_effect):
            stats = get_system_stats()

        # 5 entries minus 1 for listdir's own FD
        assert stats['fd_open'] == 4
        assert stats['fd_max'] == 1048576

    def test_fd_max_parsed_correctly(self):
        """Should parse the soft limit from /proc/self/limits."""
        from utils.status_server import get_system_stats

        limits_content = (
            "Limit                     Soft Limit           Hard Limit           Units\n"
            "Max open files            65536                131072               files\n"
        )

        open_side_effect = _make_open_side_effect({
            '/proc/self/limits': limits_content,
        })

        with patch('os.listdir', return_value=['0', '1']), \
             patch('builtins.open', side_effect=open_side_effect):
            stats = get_system_stats()

        assert stats['fd_open'] == 1  # 2 entries minus 1 for listdir's own FD
        assert stats['fd_max'] == 65536  # soft limit, not hard

    def test_fd_open_oserror_graceful(self):
        """Should not crash when /proc/self/fd is inaccessible."""
        from utils.status_server import get_system_stats

        with patch('os.listdir', side_effect=OSError('permission denied')):
            stats = get_system_stats()

        # fd_open fails but fd_max is read independently and may still succeed
        assert 'fd_open' not in stats

    def test_fd_limits_missing_graceful(self):
        """Should still return fd_open even if limits file is missing."""
        from utils.status_server import get_system_stats

        open_side_effect = _make_open_side_effect(
            {}, blocked={'/proc/self/limits'}
        )

        with patch('os.listdir', return_value=['0', '1', '2']), \
             patch('builtins.open', side_effect=open_side_effect):
            stats = get_system_stats()

        # fd_open is in a separate try block, so it survives
        assert stats.get('fd_open') == 2  # 3 entries minus 1
        assert 'fd_max' not in stats


class TestNetworkStats:
    """Tests for network I/O collection from /proc/net/dev."""

    PROC_NET_DEV = (
        "Inter-|   Receive                                                |  Transmit\n"
        " face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n"
        "    lo: 1000000   5000    0    0    0     0          0         0  1000000   5000    0    0    0     0       0          0\n"
        "  eth0: 5000000   3000    0    0    0     0          0         0  2000000   2000    0    0    0     0       0          0\n"
    )

    def test_network_bytes_summed(self):
        """Should sum rx/tx bytes across non-lo interfaces."""
        from utils.status_server import get_system_stats

        open_side_effect = _make_open_side_effect({
            '/proc/net/dev': self.PROC_NET_DEV,
        })

        with patch('builtins.open', side_effect=open_side_effect):
            stats = get_system_stats()

        assert stats['net_rx_bytes'] == 5000000
        assert stats['net_tx_bytes'] == 2000000

    def test_network_excludes_loopback(self):
        """Loopback interface should be excluded from totals."""
        from utils.status_server import get_system_stats

        proc_data = (
            "Inter-|   Receive\n"
            " face |bytes\n"
            "    lo: 9999999   5000    0    0    0     0          0         0  9999999   5000    0    0    0     0       0          0\n"
        )

        open_side_effect = _make_open_side_effect({
            '/proc/net/dev': proc_data,
        })

        with patch('builtins.open', side_effect=open_side_effect):
            stats = get_system_stats()

        assert 'net_rx_bytes' not in stats

    def test_network_multiple_interfaces(self):
        """Should sum across multiple non-lo interfaces."""
        from utils.status_server import get_system_stats

        proc_data = (
            "Inter-|   Receive\n"
            " face |bytes\n"
            "  eth0: 1000   100    0    0    0     0          0         0  2000   100    0    0    0     0       0          0\n"
            "  wlan0: 3000   200    0    0    0     0          0         0  4000   200    0    0    0     0       0          0\n"
        )

        open_side_effect = _make_open_side_effect({
            '/proc/net/dev': proc_data,
        })

        with patch('builtins.open', side_effect=open_side_effect):
            stats = get_system_stats()

        assert stats['net_rx_bytes'] == 4000
        assert stats['net_tx_bytes'] == 6000

    def test_network_oserror_graceful(self):
        """Should not crash when /proc/net/dev is unavailable."""
        from utils.status_server import get_system_stats

        open_side_effect = _make_open_side_effect(
            {}, blocked={'/proc/net/dev'}
        )

        with patch('builtins.open', side_effect=open_side_effect):
            stats = get_system_stats()

        assert 'net_rx_bytes' not in stats
        assert 'net_tx_bytes' not in stats


class TestSystemStatsIntegration:
    """Integration tests for the full get_system_stats function."""

    def test_returns_dict(self):
        """Should always return a dict, even if all collection fails."""
        from utils.status_server import get_system_stats
        stats = get_system_stats()
        assert isinstance(stats, dict)

    def test_to_dict_includes_system_key(self):
        """StatusData.to_dict() should include a system dict."""
        from utils.status_server import StatusData
        sd = StatusData()
        data = sd.to_dict()
        assert 'system' in data
        assert isinstance(data['system'], dict)


class TestMetricsNewGauges:
    """Tests that new system metrics appear in Prometheus output."""

    def _format_with_system(self, system_stats):
        """Helper: format metrics with mocked system stats."""
        from utils.metrics import MetricsRegistry
        from utils.status_server import StatusData

        m = MetricsRegistry()
        with patch.object(StatusData, 'to_dict', return_value={
            'version': '0.0.0',
            'uptime_seconds': 100,
            'processes': [],
            'mounts': [],
            'services': [],
            'system': system_stats,
            'recent_events': [],
            'error_count': 0,
            'provider_health': {},
        }):
            return m.format_metrics()

    def test_disk_metrics_included(self):
        """Disk gauges should appear when disk stats are available."""
        output = self._format_with_system({
            'disk_used_bytes': 500000,
            'disk_total_bytes': 1000000,
            'disk_percent': 50.0,
        })
        assert 'pd_zurg_disk_used_bytes 500000' in output
        assert 'pd_zurg_disk_total_bytes 1000000' in output
        assert 'pd_zurg_disk_usage_percent 50.0' in output

    def test_fd_metrics_included(self):
        """FD gauges should appear when fd stats are available."""
        output = self._format_with_system({
            'fd_open': 142,
            'fd_max': 1048576,
        })
        assert 'pd_zurg_fd_open 142' in output
        assert 'pd_zurg_fd_max 1048576' in output

    def test_network_metrics_included(self):
        """Network counters should appear when network stats are available."""
        output = self._format_with_system({
            'net_rx_bytes': 5000000,
            'net_tx_bytes': 2000000,
        })
        assert 'pd_zurg_net_rx_bytes_total 5000000' in output
        assert 'pd_zurg_net_tx_bytes_total 2000000' in output
        assert '# TYPE pd_zurg_net_rx_bytes_total counter' in output

    def test_missing_stats_omitted(self):
        """Metrics should be omitted when stats are missing."""
        output = self._format_with_system({})
        assert 'disk_used_bytes' not in output
        assert 'fd_open' not in output
        assert 'net_rx_bytes_total' not in output

    def test_type_annotations_present(self):
        """New metrics should have TYPE and HELP annotations."""
        output = self._format_with_system({
            'disk_used_bytes': 1000,
            'fd_open': 10,
            'net_rx_bytes': 500,
        })
        assert '# TYPE pd_zurg_disk_used_bytes gauge' in output
        assert '# HELP pd_zurg_fd_open' in output
        assert '# TYPE pd_zurg_net_rx_bytes_total counter' in output
