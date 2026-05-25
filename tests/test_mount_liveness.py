"""Tests for utils/scheduled_tasks.py::mount_liveness_probe + _probe_mount.

Plan 39 dual-debrid: the probe was extended to cover the TB mount so a
half-stuck FUSE (mount table entry persists, rclone process dead → every
traversal returns ENOTCONN) on either RD or TB is surfaced as an error,
not silently green.
"""

import os
import pytest
from unittest.mock import patch

from utils import scheduled_tasks


# ---------------------------------------------------------------------------
# _probe_mount — unit
# ---------------------------------------------------------------------------

class TestProbeMount:

    def test_alive_mount_returns_success(self, tmp_path, monkeypatch):
        """A real, listable directory that ``ismount`` says is a mount
        returns ``('success', msg, items)``."""
        (tmp_path / 'a').mkdir()
        (tmp_path / 'b').mkdir()
        monkeypatch.setattr(os.path, 'ismount', lambda p: p == str(tmp_path))
        status, msg, items = scheduled_tasks._probe_mount(str(tmp_path))
        assert status == 'success'
        assert items == 2
        assert 'entries' in msg

    def test_path_not_a_directory_returns_absent(self):
        status, msg, _ = scheduled_tasks._probe_mount('/nonexistent/path/xyz')
        assert status == 'absent'
        assert 'does not exist' in msg

    def test_path_is_dir_but_not_mountpoint_returns_absent(self, tmp_path, monkeypatch):
        monkeypatch.setattr(os.path, 'ismount', lambda p: False)
        status, msg, _ = scheduled_tasks._probe_mount(str(tmp_path))
        assert status == 'absent'
        assert 'Not a mount point' in msg

    def test_unresponsive_mount_returns_error(self, tmp_path, monkeypatch):
        """The canonical failure mode this probe needs to catch:
        mount table entry persists, but ``os.listdir`` raises
        ``OSError(ENOTCONN)`` — half-stuck FUSE after rclone process died."""
        monkeypatch.setattr(os.path, 'ismount', lambda p: True)
        def boom(_):
            raise OSError(107, 'Transport endpoint is not connected')
        monkeypatch.setattr(os, 'listdir', boom)
        status, msg, _ = scheduled_tasks._probe_mount(str(tmp_path))
        assert status == 'error'
        assert 'unresponsive' in msg
        assert 'Transport endpoint' in msg


# ---------------------------------------------------------------------------
# mount_liveness_probe — integration of primary + TB
# ---------------------------------------------------------------------------

class TestMountLivenessProbe:

    @pytest.fixture(autouse=True)
    def _silence_local_lib(self, monkeypatch):
        """Local-library health check has its own state and notifications —
        no-op it so these tests focus on the mount probe combinatorics."""
        monkeypatch.setattr(
            scheduled_tasks, '_check_local_library_health', lambda: None,
        )

    @pytest.fixture
    def tb_configured(self, monkeypatch):
        """The three env vars that gate TB-mount probing.  Must match
        the gate in ``mount_liveness_probe`` (which mirrors the
        ``_torbox_mount_configured`` predicate in rclone/rclone.py)."""
        monkeypatch.setenv('TORBOX_API_KEY', 'tb-fake-key')
        monkeypatch.setenv('TORBOX_WEBDAV_USER', 'tb-user')
        monkeypatch.setenv('TORBOX_WEBDAV_PASS', 'tb-pass')

    def test_rd_only_alive_success(self, tmp_path, monkeypatch):
        """No TB configured: success when RD mount is alive.

        Mocks the TB env vars to "unset" rather than mocking
        ``mount_for_debrid`` — the latter always returns a path for
        TORBOX so the function-mock didn't actually exercise the
        production "TB not configured" code path.
        """
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', str(tmp_path))
        monkeypatch.delenv('TORBOX_API_KEY', raising=False)
        monkeypatch.delenv('TORBOX_WEBDAV_USER', raising=False)
        monkeypatch.delenv('TORBOX_WEBDAV_PASS', raising=False)
        monkeypatch.setattr(os.path, 'ismount', lambda p: p == str(tmp_path))
        result = scheduled_tasks.mount_liveness_probe()
        assert result['status'] == 'success'
        # TB not configured → no TB fields at all in the result.
        assert 'tb_status' not in result
        assert 'tb_items' not in result
        assert 'tb_mount' not in result

    def test_tb_dead_degrades_overall_status(self, tmp_path, monkeypatch, tb_configured):
        """Live TB mount with ENOTCONN MUST degrade the overall status —
        this is the regression the fix exists to prevent (pre-fix the
        probe ignored TB entirely so a dead TB looked healthy)."""
        rd_path = tmp_path / 'rd'
        tb_path = tmp_path / 'tb'
        rd_path.mkdir()
        tb_path.mkdir()
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', str(rd_path))

        # Both look like mounts to ``ismount``.
        monkeypatch.setattr(os.path, 'ismount', lambda p: True)

        # RD listdir works; TB listdir raises ENOTCONN.
        real_listdir = os.listdir
        def selective_listdir(p):
            if p == str(tb_path):
                raise OSError(107, 'Transport endpoint is not connected')
            return real_listdir(p)
        monkeypatch.setattr(os, 'listdir', selective_listdir)

        with patch('utils.debrid_routing.mount_for_debrid', return_value=str(tb_path)):
            result = scheduled_tasks.mount_liveness_probe()

        assert result['status'] == 'error'
        assert result['tb_status'] == 'error'
        assert 'unresponsive' in result['message']

    def test_tb_alive_message_includes_tb_summary(self, tmp_path, monkeypatch, tb_configured):
        rd_path = tmp_path / 'rd'
        tb_path = tmp_path / 'tb'
        rd_path.mkdir()
        tb_path.mkdir()
        (tb_path / 'release-1').mkdir()
        (tb_path / 'release-2').mkdir()
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', str(rd_path))
        monkeypatch.setattr(os.path, 'ismount', lambda p: True)

        with patch('utils.debrid_routing.mount_for_debrid', return_value=str(tb_path)):
            result = scheduled_tasks.mount_liveness_probe()

        assert result['status'] == 'success'
        assert result['tb_status'] == 'success'
        assert result['tb_items'] == 2
        assert 'TB:' in result['message']

    def test_rd_dead_degrades_even_with_tb_alive(self, tmp_path, monkeypatch, tb_configured):
        """RD is the load-bearing primary — its failure always wins."""
        rd_path = tmp_path / 'rd'
        tb_path = tmp_path / 'tb'
        rd_path.mkdir()
        tb_path.mkdir()
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', str(rd_path))
        monkeypatch.setattr(os.path, 'ismount', lambda p: True)

        real_listdir = os.listdir
        def selective_listdir(p):
            if p == str(rd_path):
                raise OSError(107, 'Transport endpoint is not connected')
            return real_listdir(p)
        monkeypatch.setattr(os, 'listdir', selective_listdir)

        with patch('utils.debrid_routing.mount_for_debrid', return_value=str(tb_path)):
            result = scheduled_tasks.mount_liveness_probe()

        assert result['status'] == 'error'
        assert 'RD:' in result['message']

    def test_tb_discovery_exception_does_not_propagate(self, tmp_path, monkeypatch, tb_configured):
        """If ``mount_for_debrid`` (or its import) raises, the probe
        must still complete and return a sensible status — the RD
        result alone, with no TB fields. Defensive against future
        debrid_routing refactors."""
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', str(tmp_path))
        monkeypatch.setattr(os.path, 'ismount', lambda p: p == str(tmp_path))

        with patch('utils.debrid_routing.mount_for_debrid',
                   side_effect=ImportError('simulated upstream breakage')):
            result = scheduled_tasks.mount_liveness_probe()

        assert result['status'] == 'success'
        # TB discovery failed → no TB fields surfaced (treated as
        # "TB unconfigured" from the result-shape perspective).
        assert 'tb_status' not in result


# ---------------------------------------------------------------------------
# _listdir_with_timeout — the hung-FUSE guard
# ---------------------------------------------------------------------------

class TestListdirWithTimeout:

    def test_returns_entries_on_success(self, tmp_path):
        (tmp_path / 'a').mkdir()
        (tmp_path / 'b').mkdir()
        entries = scheduled_tasks._listdir_with_timeout(str(tmp_path), 5)
        assert sorted(entries) == ['a', 'b']

    def test_propagates_oserror(self, tmp_path):
        with pytest.raises(OSError):
            scheduled_tasks._listdir_with_timeout('/nonexistent/abc/xyz', 5)

    def test_raises_timeout_when_worker_hangs(self, tmp_path, monkeypatch):
        """A wedged FUSE that never returns from listdir must be
        timeout-bounded, not allowed to hang the scheduler thread."""
        import threading
        hang = threading.Event()
        def hang_listdir(_):
            hang.wait(timeout=5)  # never set; bounded so test doesn't leak forever
            return []
        monkeypatch.setattr(os, 'listdir', hang_listdir)
        with pytest.raises(TimeoutError):
            scheduled_tasks._listdir_with_timeout(str(tmp_path), 0.2)
        # Release the leaked worker so it doesn't slow the test suite.
        hang.set()

    def test_slow_path_logs_warning(self, tmp_path, monkeypatch, caplog):
        """A slow-but-responsive listdir should still succeed and
        WARN.  Covers the previously-untested code path between the
        success and timeout branches."""
        import time as _time
        real_listdir = os.listdir
        # Push elapsed past the threshold via monkeypatched time.time.
        # Patches inside scheduled_tasks's module so the probe's
        # ``time.time()`` calls see the spoofed clock.
        ticks = [1000.0, 1000.0 + scheduled_tasks._SLOW_MOUNT_THRESHOLD_SEC + 1]
        idx = {'i': 0}
        def fake_time():
            t = ticks[min(idx['i'], len(ticks) - 1)]
            idx['i'] += 1
            return t
        monkeypatch.setattr(scheduled_tasks.time, 'time', fake_time)
        monkeypatch.setattr(os.path, 'ismount', lambda p: True)

        with caplog.at_level('WARNING'):
            status, msg, items = scheduled_tasks._probe_mount(str(tmp_path))
        assert status == 'success'
        assert 'slow' in msg.lower()
        assert any('slow' in r.message.lower() for r in caplog.records)
