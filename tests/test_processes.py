"""Tests for process management and restart backoff."""

import pytest
from unittest.mock import patch, MagicMock

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


class TestPreRestartHook:
    """restart_process() must run the pre_restart hook before relaunching."""

    def _handler(self):
        from utils.processes import ProcessHandler
        h = ProcessHandler(MagicMock())
        h._command = ['rclone', 'mount']
        h._config_dir = '/tmp'
        h._process_name = 'rclone'
        h._key_type = 'torbox'
        h._suppress_logging = True
        return h

    def test_hook_runs_before_relaunch(self):
        h = self._handler()
        order = []
        h.pre_restart = lambda: order.append('hook')
        with patch('utils.processes.subprocess.Popen',
                   side_effect=lambda *a, **k: order.append('popen') or MagicMock()):
            h.restart_process()
        assert order == ['hook', 'popen']

    def test_hook_exception_does_not_block_relaunch(self):
        h = self._handler()
        h.pre_restart = MagicMock(side_effect=RuntimeError('boom'))
        with patch('utils.processes.subprocess.Popen',
                   return_value=MagicMock()) as popen:
            h.restart_process()
        popen.assert_called_once()

    def test_no_hook_still_relaunches(self):
        h = self._handler()
        assert h.pre_restart is None
        with patch('utils.processes.subprocess.Popen',
                   return_value=MagicMock()) as popen:
            h.restart_process()
        popen.assert_called_once()

    def test_run_pre_restart_false_skips_hook(self):
        """_handle_restart runs the hook itself before taking the registry
        lock, then relaunches with run_pre_restart=False — no double-run."""
        h = self._handler()
        h.pre_restart = MagicMock()
        with patch('utils.processes.subprocess.Popen',
                   return_value=MagicMock()):
            h.restart_process(run_pre_restart=False)
        h.pre_restart.assert_not_called()

    def test_handle_restart_runs_hook_outside_registry_lock(self):
        """The hook may sleep/fork for seconds; it must run while
        _registry_lock is free."""
        import utils.processes as proc
        h = self._handler()
        h.restart_policy = RestartPolicy(backoff_seconds=[0])
        h.process = MagicMock()
        h.process.returncode = 1
        lock_states = []
        h.pre_restart = lambda: lock_states.append(
            proc._registry_lock.acquire(blocking=False)
            and (proc._registry_lock.release() or True))
        # 'Zurg' has no dependencies, so the restart isn't deferred.
        entry = {'handler': h, 'process_name': 'Zurg', 'key_type': None}
        with patch('utils.processes.subprocess.Popen',
                   return_value=MagicMock()), \
             patch.object(proc, '_process_registry', [entry]):
            proc._handle_restart(entry, MagicMock())
        # Hook ran, and could acquire the registry lock => it was not held.
        assert lock_states == [True]


class TestDependencyCheckMultiEntry:
    """_check_dependencies_alive with a name registered more than once
    (per-mount rclone entries): any live instance satisfies the dependency."""

    def _entry(self, name, alive, key_type=None):
        h = MagicMock()
        h.process.poll.return_value = None if alive else 1
        return {'handler': h, 'process_name': name, 'key_type': key_type}

    def test_any_alive_instance_satisfies_dependency(self, monkeypatch):
        import utils.processes as proc
        monkeypatch.setattr(proc, '_process_registry', [
            self._entry('rclone', alive=False, key_type='zurgarr'),
            self._entry('rclone', alive=True, key_type='torbox'),
        ])
        assert proc._check_dependencies_alive('plex_debrid') == (True, None)

    def test_all_dead_instances_fail_dependency(self, monkeypatch):
        import utils.processes as proc
        monkeypatch.setattr(proc, '_process_registry', [
            self._entry('rclone', alive=False, key_type='zurgarr'),
            self._entry('rclone', alive=False, key_type='torbox'),
        ])
        assert proc._check_dependencies_alive('plex_debrid') == (False, 'rclone')

    def test_unregistered_dependency_fails(self, monkeypatch):
        import utils.processes as proc
        monkeypatch.setattr(proc, '_process_registry', [])
        assert proc._check_dependencies_alive('plex_debrid') == (False, 'rclone')

    def _exhausted_entry(self, key_type):
        from utils.processes import RestartPolicy
        h = MagicMock()
        h.process.poll.return_value = 1
        h.restart_policy = RestartPolicy(max_restarts=5)
        h._restart_count = 5
        return {'handler': h, 'process_name': 'rclone', 'key_type': key_type}

    def _dependent_entry(self):
        from utils.processes import ProcessHandler, RestartPolicy
        h = ProcessHandler(MagicMock())
        h.process = MagicMock()
        h.process.returncode = 1
        h.restart_policy = RestartPolicy()
        return {'handler': h, 'process_name': 'plex_debrid', 'key_type': None}

    def test_all_instances_exhausted_marks_dependent_permanently_dead(self, monkeypatch):
        import utils.processes as proc
        dep_entry = self._dependent_entry()
        monkeypatch.setattr(proc, '_process_registry', [
            self._exhausted_entry('zurgarr'),
            self._exhausted_entry('torbox'),
            dep_entry,
        ])
        with patch.object(proc, '_on_restart_exhausted') as exhausted:
            proc._handle_restart(dep_entry, MagicMock())
        exhausted.assert_called_once()
        assert dep_entry['handler'].restart_policy is None

    def test_intentionally_stopped_instance_defers_instead_of_killing_dependent(self, monkeypatch):
        """restart_policy=None (stopped mid-reload) must NOT count as
        exhausted — the dependent keeps deferring, awaiting its return."""
        import utils.processes as proc
        stopped = self._exhausted_entry('torbox')
        stopped['handler'].restart_policy = None
        dep_entry = self._dependent_entry()
        monkeypatch.setattr(proc, '_process_registry', [
            self._exhausted_entry('zurgarr'),
            stopped,
            dep_entry,
        ])
        with patch.object(proc, '_on_restart_exhausted') as exhausted:
            proc._handle_restart(dep_entry, MagicMock())
        exhausted.assert_not_called()
        assert dep_entry['handler'].restart_policy is not None
