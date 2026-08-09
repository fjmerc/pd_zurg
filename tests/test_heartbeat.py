"""Tests for the worker-thread heartbeat registry (utils/heartbeat.py)
and the liveness instrumentation of the three worker loops.

The Docker healthcheck runs as a separate process and cannot see threads
inside the main process — a dead scheduler loop silently stops ALL
periodic work while the container stays healthy.  Workers beat into a
JSON snapshot; ``stale_entries`` evaluates it from the healthcheck side.

Failure posture under test:
  - heartbeat plumbing must NEVER raise into the loop it protects
  - missing/corrupt snapshot degrades to the pre-heartbeat healthcheck
  - slow-but-alive must not read as stale (generous ceilings, future
    timestamps clamp to age 0)
"""

import json
import threading
import time

import pytest

from utils import heartbeat


@pytest.fixture(autouse=True)
def hb_isolated(tmp_path, monkeypatch):
    """Redirect the snapshot file and clear registry state per-test."""
    monkeypatch.setattr(heartbeat, 'HEARTBEAT_FILE',
                        str(tmp_path / 'heartbeats.json'))
    monkeypatch.setattr(heartbeat, '_last_flush', 0.0)
    with heartbeat._lock:
        heartbeat._entries.clear()
    yield
    with heartbeat._lock:
        heartbeat._entries.clear()


def _read_snapshot():
    with open(heartbeat.HEARTBEAT_FILE, encoding='utf-8') as f:
        return json.load(f)


class OneShotEvent:
    """Event whose first wait() flips it set — runs a loop exactly once."""

    def __init__(self):
        self._set = False

    def is_set(self):
        return self._set

    def wait(self, timeout=None):
        self._set = True
        return True

    def set(self):
        self._set = True

    def clear(self):
        self._set = False


# ---------------------------------------------------------------------------
# Registry: register / beat / unregister / reset
# ---------------------------------------------------------------------------

class TestRegistry:

    def test_register_flushes_immediately(self):
        heartbeat.register('worker', 300)
        data = _read_snapshot()
        assert data['worker']['stale_after'] == 300
        # Timestamps are monotonic (system-wide on Linux), not wall clock
        assert data['worker']['last_beat'] == pytest.approx(
            time.monotonic(), abs=5)

    def test_beat_updates_last_beat(self):
        heartbeat.register('worker', 300)
        with heartbeat._lock:
            heartbeat._entries['worker']['last_beat'] = 0.0
        heartbeat.beat('worker')
        with heartbeat._lock:
            assert heartbeat._entries['worker']['last_beat'] > 0.0

    def test_beat_on_unregistered_name_is_noop(self):
        heartbeat.beat('ghost')
        with heartbeat._lock:
            assert 'ghost' not in heartbeat._entries

    def test_unregister_removes_entry_and_flushes(self):
        heartbeat.register('worker', 300)
        heartbeat.unregister('worker')
        assert _read_snapshot() == {}

    def test_unregister_unknown_name_is_noop(self):
        heartbeat.unregister('never-registered')  # must not raise

    def test_reregister_resets_beat_window(self):
        heartbeat.register('worker', 300)
        with heartbeat._lock:
            heartbeat._entries['worker']['last_beat'] = 0.0
        heartbeat.register('worker', 600)
        with heartbeat._lock:
            entry = heartbeat._entries['worker']
        assert entry['stale_after'] == 600
        assert entry['last_beat'] > 0.0

    def test_reset_clears_all_entries(self):
        heartbeat.register('a', 300)
        heartbeat.register('b', 600)
        heartbeat.reset()
        assert _read_snapshot() == {}
        with heartbeat._lock:
            assert heartbeat._entries == {}

    def test_beat_flush_is_throttled(self, monkeypatch):
        heartbeat.register('worker', 300)
        first = _read_snapshot()
        # A beat right after the forced register-flush stays in memory only
        with heartbeat._lock:
            heartbeat._entries['worker']['last_beat'] = 9999999999.0
        heartbeat.beat('worker')
        # beat() overwrote our sentinel in memory, but the file must still
        # hold the register-time snapshot (throttled)
        assert _read_snapshot() == first

    def test_flush_failure_does_not_raise(self, monkeypatch):
        def boom(*a, **k):
            raise OSError('disk gone')
        monkeypatch.setattr(heartbeat, 'atomic_write', boom)
        heartbeat.register('worker', 300)   # must not raise
        heartbeat.beat('worker')            # must not raise
        heartbeat.unregister('worker')      # must not raise
        heartbeat.reset()                   # must not raise


# ---------------------------------------------------------------------------
# stale_entries — the healthcheck-side evaluator
# ---------------------------------------------------------------------------

class TestStaleEntries:

    def _write(self, tmp_path, data):
        p = tmp_path / 'hb.json'
        p.write_text(json.dumps(data) if not isinstance(data, str) else data)
        return str(p)

    def test_fresh_entries_not_stale(self, tmp_path):
        now = time.monotonic()
        path = self._write(tmp_path, {
            'w': {'last_beat': now - 10, 'stale_after': 300}})
        assert heartbeat.stale_entries(path=path, now=now) == []

    def test_stale_entry_reported_with_age_and_ceiling(self, tmp_path):
        now = time.monotonic()
        path = self._write(tmp_path, {
            'w': {'last_beat': now - 400, 'stale_after': 300}})
        result = heartbeat.stale_entries(path=path, now=now)
        assert len(result) == 1
        name, age, ceiling = result[0]
        assert name == 'w'
        assert age == pytest.approx(400, abs=1)
        assert ceiling == 300

    def test_missing_file_degrades_to_empty(self):
        assert heartbeat.stale_entries(path='/nonexistent/hb.json') == []

    def test_corrupt_json_degrades_to_empty(self, tmp_path):
        path = self._write(tmp_path, '{not json')
        assert heartbeat.stale_entries(path=path) == []

    def test_non_dict_top_level_degrades_to_empty(self, tmp_path):
        path = self._write(tmp_path, [1, 2, 3])
        assert heartbeat.stale_entries(path=path) == []

    def test_malformed_entries_skipped(self, tmp_path):
        now = time.monotonic()
        path = self._write(tmp_path, {
            'not-a-dict': 42,
            'missing-fields': {},
            'bad-types': {'last_beat': 'yesterday', 'stale_after': 'soon'},
            'null-beat': {'last_beat': None, 'stale_after': 300},
            'good': {'last_beat': now - 400, 'stale_after': 300},
        })
        result = heartbeat.stale_entries(path=path, now=now)
        assert [r[0] for r in result] == ['good']

    def test_nonpositive_ceiling_skipped(self, tmp_path):
        now = time.monotonic()
        path = self._write(tmp_path, {
            'w': {'last_beat': now - 99999, 'stale_after': 0},
            'v': {'last_beat': now - 99999, 'stale_after': -5}})
        assert heartbeat.stale_entries(path=path, now=now) == []

    def test_future_last_beat_reads_as_age_zero(self, tmp_path):
        # A leftover file from a previous boot epoch (monotonic restarts
        # at 0 on host reboot) must never flag a live worker
        now = time.monotonic()
        path = self._write(tmp_path, {
            'w': {'last_beat': now + 5000, 'stale_after': 300}})
        assert heartbeat.stale_entries(path=path, now=now) == []

    def test_default_path_reads_registry_file(self):
        heartbeat.register('w', 300)
        stale = heartbeat.stale_entries(now=time.monotonic() + 400)
        assert [s[0] for s in stale] == ['w']


# ---------------------------------------------------------------------------
# Scheduler loop: beats + survives Thread.start() failure (verified kill path)
# ---------------------------------------------------------------------------

class TestSchedulerLoopHardening:

    def test_loop_registers_and_beats(self, monkeypatch):
        from utils.task_scheduler import TaskScheduler
        sched = TaskScheduler()
        monkeypatch.setattr(sched, '_stop_event', OneShotEvent())
        sched._scheduler_loop()
        with heartbeat._lock:
            assert 'task_scheduler' in heartbeat._entries

    def test_loop_survives_thread_start_failure_and_resets_running(
            self, monkeypatch):
        from utils import task_scheduler
        sched = task_scheduler.TaskScheduler()
        sched.register('doomed', lambda: None, 60, initial_delay=0)

        class BoomThread:
            def __init__(self, *a, **k):
                pass

            def start(self):
                raise RuntimeError("can't start new thread")

        monkeypatch.setattr(task_scheduler.threading, 'Thread', BoomThread)
        monkeypatch.setattr(sched, '_stop_event', OneShotEvent())
        sched._scheduler_loop()  # must not raise

        task = sched._tasks['doomed']
        # running flag must be cleared or the task is locked out forever
        assert task.running is False

    def test_stop_unregisters(self, monkeypatch):
        from utils.task_scheduler import TaskScheduler
        sched = TaskScheduler()
        heartbeat.register('task_scheduler', 300)
        sched.stop()
        with heartbeat._lock:
            assert 'task_scheduler' not in heartbeat._entries


# ---------------------------------------------------------------------------
# Process monitor loop: beats + survives _handle_restart raising
# ---------------------------------------------------------------------------

class TestMonitorLoopHardening:

    def test_loop_survives_restart_failure(self, monkeypatch):
        from utils import processes
        from utils.logger import get_logger

        class FakeProc:
            def poll(self):
                return 1  # dead

        class FakeHandler:
            restart_policy = object()
            process = FakeProc()

        entry = {'handler': FakeHandler(), 'process_name': 'fake',
                 'key_type': None}
        monkeypatch.setattr(processes, '_process_registry', [entry])
        monkeypatch.setattr(processes, '_shutting_down', False)

        def boom(entry, logger):
            raise RuntimeError('spawn failed')

        monkeypatch.setattr(processes, '_handle_restart', boom)
        monkeypatch.setattr(processes, '_monitor_stop_event', OneShotEvent())

        processes._monitor_loop(get_logger())  # must not raise

        with heartbeat._lock:
            assert 'process_monitor' in heartbeat._entries


# ---------------------------------------------------------------------------
# Blackhole watcher: registers on run, unregisters on stop
# ---------------------------------------------------------------------------

class TestBlackholeWatcherHeartbeat:

    def test_run_registers_stop_unregisters(self, tmp_path):
        from utils.blackhole import BlackholeWatcher
        watcher = BlackholeWatcher(str(tmp_path), 'fake_key', 'realdebrid',
                                   poll_interval=0.05)
        t = threading.Thread(target=watcher.run, daemon=True)
        t.start()
        deadline = time.time() + 5
        while time.time() < deadline:
            with heartbeat._lock:
                if 'blackhole_watcher' in heartbeat._entries:
                    break
            time.sleep(0.02)
        else:
            pytest.fail('watcher never registered its heartbeat')
        watcher.stop()
        t.join(timeout=5)
        assert not t.is_alive()
        with heartbeat._lock:
            assert 'blackhole_watcher' not in heartbeat._entries

    def test_stopped_watcher_never_beats_new_registration(self, tmp_path):
        """SIGHUP reload: an old watcher still mid-scan must not keep the
        name a NEW watcher registered fresh — that would mask a wedge in
        the new thread for as long as the old one keeps working."""
        from utils.blackhole import BlackholeWatcher
        old = BlackholeWatcher(str(tmp_path), 'fake_key', 'realdebrid')
        old.stop()  # reload path: stop() before the new watcher spawns
        heartbeat.register('blackhole_watcher', 900)  # new generation
        with heartbeat._lock:
            heartbeat._entries['blackhole_watcher']['last_beat'] = 0.0
        old._beat()  # old thread's per-file beat, post-stop
        with heartbeat._lock:
            assert heartbeat._entries['blackhole_watcher']['last_beat'] == 0.0
