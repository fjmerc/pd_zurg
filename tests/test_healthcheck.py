"""Tests for healthcheck.py mount liveness probing.

The container healthcheck must distinguish a *dead* FUSE mount (rclone
process gone → ``os.listdir`` raises ENOTCONN) from a *slow* one
(TorBox 429-throttling the FUSE walk → ``os.listdir`` just blocks).  A
dead mount must fail the check; a slow-but-alive mount must NOT, or the
container flaps ``unhealthy`` on every TorBox walk.

healthcheck.py guards its orchestration under ``__name__ == '__main__'``
so importing it here does not run the live check.
"""

import os
import threading
import urllib.error

import pytest

import healthcheck


# ---------------------------------------------------------------------------
# _listdir_with_timeout
# ---------------------------------------------------------------------------

class TestListdirWithTimeout:

    def test_returns_entries_on_success(self, tmp_path):
        (tmp_path / 'a').mkdir()
        (tmp_path / 'b').mkdir()
        entries = healthcheck._listdir_with_timeout(str(tmp_path), 5)
        assert sorted(entries) == ['a', 'b']

    def test_propagates_oserror(self):
        with pytest.raises(OSError):
            healthcheck._listdir_with_timeout('/nonexistent/abc/xyz', 5)

    def test_raises_timeout_when_worker_hangs(self, tmp_path, monkeypatch):
        hang = threading.Event()

        def hang_listdir(_):
            hang.wait(timeout=5)  # never set in time; bounded so it can't leak forever
            return []

        monkeypatch.setattr(os, 'listdir', hang_listdir)
        with pytest.raises(TimeoutError):
            healthcheck._listdir_with_timeout(str(tmp_path), 0.2)
        hang.set()  # release the leaked worker


# ---------------------------------------------------------------------------
# _mount_alive
# ---------------------------------------------------------------------------

class TestMountAlive:

    def test_not_a_mount_point(self, tmp_path, monkeypatch):
        monkeypatch.setattr(os.path, 'ismount', lambda p: False)
        alive, why = healthcheck._mount_alive(str(tmp_path))
        assert alive is False
        assert 'not a mount point' in why

    def test_alive_mount(self, tmp_path, monkeypatch):
        (tmp_path / 'x').mkdir()
        monkeypatch.setattr(os.path, 'ismount', lambda p: True)
        alive, why = healthcheck._mount_alive(str(tmp_path))
        assert alive is True
        assert why == ""

    def test_dead_mount_enotconn_is_not_alive(self, tmp_path, monkeypatch):
        """Half-stuck FUSE: ENOTCONN must fail the healthcheck."""
        monkeypatch.setattr(os.path, 'ismount', lambda p: True)

        def boom(_):
            raise OSError(107, 'Transport endpoint is not connected')

        monkeypatch.setattr(os, 'listdir', boom)
        alive, why = healthcheck._mount_alive(str(tmp_path))
        assert alive is False
        assert 'Transport endpoint' in why

    def test_slow_mount_timeout_is_treated_as_alive(self, tmp_path, monkeypatch, capsys):
        """The core fix: a listdir that blocks past the probe timeout
        (TorBox rate-limit storm) must report alive, not flap unhealthy."""
        monkeypatch.setattr(os.path, 'ismount', lambda p: True)
        monkeypatch.setattr(healthcheck, '_MOUNT_PROBE_TIMEOUT_SEC', 0.2)

        hang = threading.Event()

        def hang_listdir(_):
            hang.wait(timeout=5)
            return []

        monkeypatch.setattr(os, 'listdir', hang_listdir)
        try:
            alive, why = healthcheck._mount_alive(str(tmp_path))
        finally:
            hang.set()
        assert alive is True
        assert why == ""
        err = capsys.readouterr().err
        assert 'slow' in err.lower()

    def test_explicit_timeout_overrides_default(self, tmp_path, monkeypatch, capsys):
        """The shared-budget wiring in main() passes a per-call timeout
        (min of the per-mount cap and the remaining budget).  A slow mount
        probed with a tiny explicit timeout must still report alive-and-slow,
        and the warning must echo the explicit budget, not the default."""
        monkeypatch.setattr(os.path, 'ismount', lambda p: True)
        monkeypatch.setattr(healthcheck, '_MOUNT_PROBE_TIMEOUT_SEC', 99)

        hang = threading.Event()

        def hang_listdir(_):
            hang.wait(timeout=5)
            return []

        monkeypatch.setattr(os, 'listdir', hang_listdir)
        try:
            alive, why = healthcheck._mount_alive(str(tmp_path), timeout=0.2)
        finally:
            hang.set()
        assert alive is True
        assert why == ""
        err = capsys.readouterr().err
        assert 'slow' in err.lower()
        # Warning reflects the explicit 0.2s budget, not the 99s default.
        assert '0.2s' in err


# ---------------------------------------------------------------------------
# _status_server_alive
# ---------------------------------------------------------------------------

class TestStatusServerAlive:

    def test_2xx_is_alive(self, monkeypatch):
        monkeypatch.setattr(healthcheck.urllib.request, 'urlopen',
                            lambda *a, **k: object())
        assert healthcheck._status_server_alive(8080, 3) is True

    def test_http_error_is_alive(self, monkeypatch):
        """A 401 from the auth-gated UI proves the server is responding —
        it must NOT be reported as "not responding"."""
        def raise_401(*a, **k):
            raise urllib.error.HTTPError(
                'http://localhost:8080/', 401, 'Unauthorized', {}, None)
        monkeypatch.setattr(healthcheck.urllib.request, 'urlopen', raise_401)
        assert healthcheck._status_server_alive(8080, 3) is True

    def test_connection_refused_is_down(self, monkeypatch):
        def refuse(*a, **k):
            raise urllib.error.URLError('Connection refused')
        monkeypatch.setattr(healthcheck.urllib.request, 'urlopen', refuse)
        assert healthcheck._status_server_alive(8080, 3) is False

    def test_timeout_is_down(self, monkeypatch):
        def slow(*a, **k):
            raise TimeoutError('timed out')
        monkeypatch.setattr(healthcheck.urllib.request, 'urlopen', slow)
        assert healthcheck._status_server_alive(8080, 3) is False
