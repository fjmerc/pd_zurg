"""Supervision regression tests (audit findings #4, #6, #7).

#4: stop_process() clears restart_policy; restart_process() must re-arm it
    or every SIGHUP reload / UI restart leaves the service unsupervised.
#6: restart-exhaustion must notify ONCE, not every 10s monitor tick.
#7: suppress_logging must not create stdout/stderr PIPEs nobody drains
    (child deadlocks at ~64KB with all liveness signals green)."""
import logging
import subprocess

import pytest

from utils import processes
from utils.processes import ProcessHandler, RestartPolicy


@pytest.fixture
def logger():
    return logging.getLogger('test_supervision')


@pytest.fixture
def handler(logger):
    h = ProcessHandler(logger)
    h._command = ['sleep', '30']
    h._config_dir = '/tmp'
    h._process_name = 'testproc'
    h._key_type = None
    yield h
    try:
        if h.process and h.process.poll() is None:
            h.process.kill()
            h.process.wait(timeout=5)
    except Exception:
        pass


def test_restart_process_rearms_restart_policy(handler):
    handler.restart_policy = RestartPolicy()
    handler._restart_count = 3
    handler.stop_process('testproc')
    assert handler.restart_policy is None  # stop_process contract unchanged
    handler.restart_process()
    assert handler.restart_policy is not None, (
        'restart after intentional stop must re-arm supervision')
    assert handler._restart_count == 0
    assert handler._first_restart_time is None


def test_restart_process_respects_restore_policy_false(handler):
    handler.stop_process('testproc')
    handler.restart_process(restore_policy=False)
    assert handler.restart_policy is None


def test_restart_process_keeps_existing_policy_object(handler):
    """A monitor-driven restart (policy still armed) must not reset the
    restart counters — the sliding-window budget depends on them."""
    policy = RestartPolicy()
    handler.restart_policy = policy
    handler._restart_count = 2
    handler.restart_process()
    assert handler.restart_policy is policy
    assert handler._restart_count == 2


def test_exhausted_notification_fires_once(handler, logger, monkeypatch):
    calls = []
    monkeypatch.setattr(processes, '_on_restart_exhausted',
                        lambda *a, **k: calls.append(a))

    class FakeDead:
        returncode = 1
        pid = 99999
        def poll(self):
            return 1

    handler.process = FakeDead()
    handler.restart_policy = RestartPolicy(max_restarts=5)
    handler._restart_count = 5  # budget already exhausted
    entry = {'handler': handler, 'process_name': 'testproc', 'key_type': None}

    for _ in range(4):  # four monitor ticks
        processes._handle_restart(entry, logger)

    assert len(calls) == 1, 'exhaustion must notify exactly once, not per tick'


def test_exhausted_latch_clears_on_counter_reset(handler, logger, monkeypatch):
    calls = []
    monkeypatch.setattr(processes, '_on_restart_exhausted',
                        lambda *a, **k: calls.append(a))

    class FakeDead:
        returncode = 1
        pid = 99999
        def poll(self):
            return 1

    handler.process = FakeDead()
    handler.restart_policy = RestartPolicy(max_restarts=5, window_seconds=3600)
    handler._restart_count = 5
    entry = {'handler': handler, 'process_name': 'testproc', 'key_type': None}
    processes._handle_restart(entry, logger)
    assert len(calls) == 1

    # Simulate the sliding-window reset path: first restart long ago
    import time
    handler._first_restart_time = time.time() - 7200
    # window reset inside _handle_restart clears the counter AND the latch,
    # then a fresh exhaustion cycle may notify again later
    monkeypatch.setattr(processes._monitor_stop_event, 'wait', lambda d: True)
    processes._handle_restart(entry, logger)
    assert handler._exhausted_notified is False
    assert handler._restart_count in (0, 1)


@pytest.mark.parametrize('suppress,expected_out', [
    (True, subprocess.DEVNULL),
    (False, subprocess.PIPE),
])
def test_start_process_stream_targets(logger, monkeypatch, suppress, expected_out):
    captured = {}

    class FakeProc:
        pid = 12345
        def poll(self):
            return None

    def fake_popen(cmd, **kwargs):
        captured.update(kwargs)
        return FakeProc()

    monkeypatch.setattr(processes.subprocess, 'Popen', fake_popen)
    monkeypatch.setattr(processes, 'register_process', lambda *a, **k: None)
    if not suppress:
        class FakeSPL:
            def __init__(self, *a, **k): pass
            def start_logging_stdout(self, p): pass
            def start_monitoring_stderr(self, p, k, n): pass
        monkeypatch.setattr(processes, 'SubprocessLogger', FakeSPL)

    h = ProcessHandler(logger)
    h.start_process('testproc', '/tmp', ['sleep', '1'], suppress_logging=suppress)
    assert captured['stdout'] == expected_out
    assert captured['stderr'] == expected_out


@pytest.mark.parametrize('suppress,expected_out', [
    (True, subprocess.DEVNULL),
    (False, subprocess.PIPE),
])
def test_restart_process_stream_targets(logger, monkeypatch, suppress, expected_out):
    captured = {}

    class FakeProc:
        pid = 12345
        def poll(self):
            return None

    monkeypatch.setattr(processes.subprocess, 'Popen',
                        lambda cmd, **kw: (captured.update(kw), FakeProc())[1])
    if not suppress:
        class FakeSPL:
            def __init__(self, *a, **k): pass
            def start_logging_stdout(self, p): pass
            def start_monitoring_stderr(self, p, k, n): pass
        monkeypatch.setattr(processes, 'SubprocessLogger', FakeSPL)

    h = ProcessHandler(logger)
    h._command = ['sleep', '1']
    h._config_dir = '/tmp'
    h._process_name = 'testproc'
    h._suppress_logging = suppress
    h.restart_process()
    assert captured['stdout'] == expected_out
    assert captured['stderr'] == expected_out


def test_handle_restart_skips_double_start_if_already_alive(handler, logger, monkeypatch):
    """If a process is already alive when _handle_restart acquires the lock
    (restarted by config_reload or restart_service during backoff), do not
    spawn a duplicate. This prevents the monitor/reload race from creating
    two instances of the same service."""
    restart_calls = []

    def fake_restart(*args, **kwargs):
        restart_calls.append((args, kwargs))

    class FakeAlive:
        returncode = None  # Still running
        pid = 12345
        def poll(self):
            return None  # Still alive

    # Setup handler as if it died and was collected for restart
    handler.process = FakeAlive()
    handler.restart_policy = RestartPolicy(max_restarts=5)
    handler._restart_count = 0
    entry = {'handler': handler, 'process_name': 'testproc', 'key_type': None}

    # Patch restart_process to record calls
    monkeypatch.setattr(handler, 'restart_process', fake_restart)
    # Skip the backoff wait (pretend it completed)
    monkeypatch.setattr(processes._monitor_stop_event, 'wait', lambda d: False)
    # Hook is a no-op
    monkeypatch.setattr(handler, 'run_pre_restart', lambda: None)

    # Call _handle_restart — process is alive at final gate, should not restart
    processes._handle_restart(entry, logger)

    # Verify restart_process was never called (process was already alive)
    assert len(restart_calls) == 0, (
        'process already alive at final gate must not be double-started')
