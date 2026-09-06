# 2026-09 Audit Fix Batch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all ten findings from the 2026-09-06 silent-gap audit: three env-boolean truthiness bugs, three process-supervision holes, the SIGCHLD exit-code blackout, the title-level debrid-delete data-loss race, missing observability on manual UI endpoints, and HTTP-server hardening (CSRF, fsync, param validation).

**Architecture:** Six independent, individually-revertable commits on branch `audit-fixes-2026-09` (already created, spec committed). Destructive-config fixes land first; the highest-blast-radius change (SIGCHLD) lands after supervision is fixed. Each commit is TDD: failing test → minimal fix → green → commit.

**Tech Stack:** Python 3.12 (stdlib only in `utils/` — urllib, http.server, no requests), pytest via `.venv/bin/pytest`, embedded vanilla JS in `utils/library_page.py`.

**Spec:** `docs/superpowers/specs/2026-09-06-audit-fixes-design.md`

## Global Constraints

- Run tests ONLY with `.venv/bin/pytest` — system Python lacks deps.
- Boolean env configs are strings: the ONLY valid truth test is `str(VAR).lower() == 'true'`.
- All file writes to state/config files go through `utils/file_utils.py` `atomic_write`.
- Never use raw `subprocess.Popen` outside `utils/processes.py` wrappers.
- `arr_client.py`, `webdav.py`, `search.py`, `status_server.py` stay urllib/stdlib-only.
- Sonarr and Radarr are parallel systems — any arr-facing change must cover both.
- New `log_event` calls must carry `meta['cause']` from the existing `CAUSE_*` vocabulary in `utils/history.py` (this plan reuses `preference_source_switch` and `user_triggered_search`; NO new slugs, so no `activity_format`/`FORMATTER_JS` changes).
- Every commit message ends with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01QiAFuoLptYU9abh6mw4KGc`
- CHANGELOG.md entries are written once, in Task 7 (not per-commit), to avoid rebase churn.

---

### Task 1: Truthiness trio (findings #3, #5)

**Files:**
- Modify: `main.py:107`, `main.py:113`, `main.py:123`
- Modify: `utils/duplicate_cleanup.py:171`
- Test: `tests/test_env_bool_gates.py` (create)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: nothing other tasks rely on.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_env_bool_gates.py`:

```python
"""Regression tests for env-boolean truthiness (audit findings #3, #5).

Boolean env configs are strings; 'false' is truthy in Python, so any
`if VAR:` / `bool(VAR)` gate silently enables a feature the user turned
off. DUPLICATE_CLEANUP=false with KEEP=zurg deletes local media."""
import re
from pathlib import Path

import pytest

MAIN_PY = Path(__file__).resolve().parent.parent / 'main.py'


@pytest.mark.parametrize('pattern,description', [
    (r'bool\(ZURGUPDATE\)', 'ZURG_UPDATE gate must use str().lower() == "true"'),
    (r'if\s+DUPECLEAN\s*:', 'DUPLICATE_CLEANUP gate must use str().lower() == "true"'),
    (r'if\s+PDUPDATE\s+and\s+PDREPO', 'PD_UPDATE gate must use str().lower() == "true"'),
])
def test_main_py_has_no_truthiness_bool_gates(pattern, description):
    source = MAIN_PY.read_text()
    assert not re.search(pattern, source), description


@pytest.mark.parametrize('dupeclean_value', ['false', 'False', '', None, '0', 'no'])
def test_duplicate_cleanup_setup_disabled(monkeypatch, dupeclean_value):
    from utils import duplicate_cleanup as dc
    monkeypatch.setattr(dc, 'PLEXADD', 'http://plex:32400', raising=False)
    monkeypatch.setattr(dc, 'PLEXTOKEN', 'tok', raising=False)
    monkeypatch.setattr(dc, 'RCLONEMN', 'zurgarr', raising=False)
    monkeypatch.setattr(dc, 'DUPECLEAN', dupeclean_value, raising=False)
    registered = []
    from utils import task_scheduler
    monkeypatch.setattr(task_scheduler.scheduler, 'register',
                        lambda *a, **k: registered.append(a))
    dc.setup()
    assert registered == [], (
        f'DUPLICATE_CLEANUP={dupeclean_value!r} must NOT register the cleanup task')


def test_duplicate_cleanup_setup_enabled(monkeypatch):
    from utils import duplicate_cleanup as dc
    monkeypatch.setattr(dc, 'PLEXADD', 'http://plex:32400', raising=False)
    monkeypatch.setattr(dc, 'PLEXTOKEN', 'tok', raising=False)
    monkeypatch.setattr(dc, 'RCLONEMN', 'zurgarr', raising=False)
    monkeypatch.setattr(dc, 'DUPECLEAN', 'true', raising=False)
    registered = []
    from utils import task_scheduler
    monkeypatch.setattr(task_scheduler.scheduler, 'register',
                        lambda *a, **k: registered.append(a))
    dc.setup()
    assert len(registered) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_env_bool_gates.py -v`
Expected: the three source-pattern tests FAIL (patterns present), and the `'false'`/`'False'`/`'0'`/`'no'` parametrizations of `test_duplicate_cleanup_setup_disabled` FAIL (task registered). `''`/`None` cases may already pass — that's fine.

- [ ] **Step 3: Fix the four gates**

In `main.py` line 107, replace:
```python
            z_updater.auto_update('Zurg', bool(ZURGUPDATE))
```
with:
```python
            z_updater.auto_update('Zurg', str(ZURGUPDATE).lower() == 'true')
```

In `main.py` line 113, replace:
```python
                if DUPECLEAN:
```
with:
```python
                if str(DUPECLEAN).lower() == 'true':
```

In `main.py` line 123, replace:
```python
            if PDUPDATE and PDREPO:
```
with:
```python
            if str(PDUPDATE).lower() == 'true' and PDREPO:
```

In `utils/duplicate_cleanup.py` line 171, replace:
```python
        if all(app_env_variables.values()) and DUPECLEAN is not None:
```
with:
```python
        if all(app_env_variables.values()) and str(DUPECLEAN).lower() == 'true':
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_env_bool_gates.py -v`
Expected: all PASS.

- [ ] **Step 5: Run neighboring suites for regressions**

Run: `.venv/bin/pytest tests/test_config_validator.py -v`
Expected: PASS (validator already used `_is_truthy`; runtime now agrees with it).

- [ ] **Step 6: Commit**

```bash
git add tests/test_env_bool_gates.py main.py utils/duplicate_cleanup.py
git commit -m "Fix env-boolean truthiness on DUPLICATE_CLEANUP / ZURG_UPDATE / PD_UPDATE gates

'false' is a truthy string, so all three features ran when explicitly
disabled. Worst case: DUPLICATE_CLEANUP=false + DUPLICATE_CLEANUP_KEEP=zurg
still deleted local media every 24h. These were the only three remaining
truthiness violations codebase-wide (audit 2026-09-06, finding #3/#5)."
```
(Append the attribution trailer from Global Constraints.)

---

### Task 2: Supervision fixes in utils/processes.py (findings #4, #6, #7)

**Files:**
- Modify: `utils/processes.py` (`ProcessHandler.__init__` ~:362, `restart_process` ~:444, `_handle_restart` ~:190-197, `restart_service` ~:347-352, `start_process` ~:409, Popen call in `restart_process` ~:463)
- Test: `tests/test_processes_supervision.py` (create)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `ProcessHandler.restart_process(run_pre_restart=True, restore_policy=True)` (new kwarg, default preserves supervision); `ProcessHandler._exhausted_notified` (bool attr); Task 3 modifies the same file's `_monitor_loop` — do not touch `_monitor_loop` in this task.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_processes_supervision.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_processes_supervision.py -v`
Expected: FAIL — `test_restart_process_rearms_restart_policy` (policy stays None), `test_exhausted_notification_fires_once` (4 calls), suppress=True stream tests (PIPE instead of DEVNULL). `_exhausted_notified` attribute error on the latch test.

- [ ] **Step 3: Implement the three fixes**

In `ProcessHandler.__init__` (after `self._first_restart_time = None`):
```python
        self._first_restart_time = None
        # Latch so restart-exhaustion notifies once, not on every 10s
        # monitor tick (finding #6). Cleared wherever _restart_count resets.
        self._exhausted_notified = False
```

In `restart_process`, change the signature and add re-arming right after the `_command is None` guard (BEFORE the relaunch, so a failed spawn is still supervised and retried by the monitor):
```python
    def restart_process(self, run_pre_restart=True, restore_policy=True):
        """Stop logging threads and re-launch the process with the same parameters."""
        if self._command is None:
            self.logger.error("Cannot restart: no command recorded from initial start")
            return

        # stop_process() clears restart_policy to suppress the monitor
        # during an intentional stop; a relaunch must re-arm supervision or
        # the process is unmonitored until container restart (finding #4).
        if restore_policy and self.restart_policy is None:
            self.restart_policy = RestartPolicy()
            self._restart_count = 0
            self._first_restart_time = None
            self._exhausted_notified = False
```

In `_handle_restart`, the sliding-window reset (currently lines 190-192) also clears the latch:
```python
    if handler._first_restart_time and (now - handler._first_restart_time) > policy.window_seconds:
        handler._restart_count = 0
        handler._first_restart_time = None
        handler._exhausted_notified = False
```

In `_handle_restart`, latch the exhaustion branch (currently lines 194-197):
```python
    if handler._restart_count >= policy.max_restarts:
        if not handler._exhausted_notified:
            logger.error(f"{desc} has exceeded max restarts ({policy.max_restarts}). Not restarting.")
            _on_restart_exhausted(desc, handler._restart_count, policy.max_restarts)
            handler._exhausted_notified = True
        return
```

In `restart_service`, the counter reset (currently lines 347-349) also clears the latch:
```python
                # Reset restart counter for clean restart
                handler._restart_count = 0
                handler._first_restart_time = None
                handler._exhausted_notified = False
```

In `start_process`, replace the Popen stream args (currently lines 409-417):
```python
            # DEVNULL when suppressed — a PIPE nobody drains deadlocks the
            # child at ~64KB while poll()/healthcheck stay green (finding #7).
            _stream = subprocess.DEVNULL if suppress_logging else subprocess.PIPE
            self.process = subprocess.Popen(
                command,
                stdout=_stream,
                stderr=_stream,
                start_new_session=True,
                cwd=config_dir,
                universal_newlines=True,
                bufsize=1
            )
```

In `restart_process`, same change to its Popen (currently lines 463-471), using `self._suppress_logging`:
```python
            _stream = subprocess.DEVNULL if self._suppress_logging else subprocess.PIPE
            self.process = subprocess.Popen(
                self._command,
                stdout=_stream,
                stderr=_stream,
                start_new_session=True,
                cwd=self._config_dir,
                universal_newlines=True,
                bufsize=1
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_processes_supervision.py tests/test_processes.py tests/test_config_reload.py -v`
Expected: all PASS. If `test_config_reload.py` asserts `restart_policy is None` after a reload cycle, that assertion embodied the bug — update the test to assert the policy IS re-armed, and note it in the commit message.

- [ ] **Step 5: Commit**

```bash
git add tests/test_processes_supervision.py utils/processes.py
git commit -m "Fix supervision holes: re-arm restart policy after relaunch, latch exhaustion notify, no undrained pipes

- restart_process() now restores restart_policy cleared by stop_process(),
  so SIGHUP config reloads and UI restarts no longer leave Zurg/rclone/
  plex_debrid permanently unsupervised (finding #4)
- restart-exhaustion notification latches: one alert instead of ~360/hr
  Discord/dashboard floods (finding #6)
- suppress_logging (LOG_LEVEL=off) uses DEVNULL instead of PIPEs nobody
  drains, which deadlocked the child at 64KB of output (finding #7)"
```
(Append the attribution trailer.)

---

### Task 3: SIGCHLD — real subprocess exit codes (finding #1)

**Files:**
- Modify: `main.py:165-166`
- Modify: `utils/processes.py` (`_monitor_loop` ~:261, new module function `_reap_orphans`)
- Modify: `rclone/rclone.py` `_write_zurg_remote` (~:346-351)
- Modify: `zurg/setup.py` `check_and_set_zurg_version` (~:149-150)
- Test: `tests/test_sigchld_exit_codes.py` (create)

**Interfaces:**
- Consumes: Task 2's version of `utils/processes.py` (edits are in different functions).
- Produces: `processes._reap_orphans()` — module-level, no args, never raises; called each `_monitor_loop` tick.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_sigchld_exit_codes.py`:

```python
"""SIGCHLD disposition regression tests (audit finding #1, CRITICAL).

signal.SIG_IGN on SIGCHLD makes the kernel auto-reap children; CPython's
subprocess then maps waitpid ECHILD to returncode 0 for EVERY child, so
check=True never raises and every exit-status branch in the codebase is
dead (umount fallback, rclone obscure failure, crash exit logging)."""
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

MAIN_PY = Path(__file__).resolve().parent.parent / 'main.py'


def test_main_py_does_not_ignore_sigchld():
    source = MAIN_PY.read_text()
    assert not re.search(r'SIGCHLD\s*,\s*signal\.SIG_IGN', source), (
        'SIG_IGN on SIGCHLD forces every subprocess returncode to 0')
    assert re.search(r'SIGCHLD\s*,\s*signal\.SIG_DFL', source), (
        'main.py must explicitly set SIGCHLD to SIG_DFL')


def test_exit_codes_visible_under_sig_dfl():
    """The behavior SIG_IGN destroyed: nonzero exit codes must be seen."""
    old = signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    try:
        r = subprocess.run([sys.executable, '-c', 'import sys; sys.exit(7)'])
        assert r.returncode == 7
    finally:
        signal.signal(signal.SIGCHLD, old)


def test_reap_orphans_exists_and_never_raises():
    from utils import processes
    old = signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    try:
        # No children at all -> ChildProcessError path must be swallowed
        processes._reap_orphans()
        # An exited child is drained without raising (it may equally be
        # claimed by subprocess internals first — both outcomes are fine)
        p = subprocess.Popen(['true'])
        deadline = time.time() + 5
        while p.poll() is None and time.time() < deadline:
            time.sleep(0.05)
        processes._reap_orphans()
    finally:
        signal.signal(signal.SIGCHLD, old)


def test_write_zurg_remote_logs_error_on_obscure_failure(monkeypatch, tmp_path):
    """With real exit codes, `rclone obscure` failure returns None; the
    config writer must scream, not silently omit user/pass (which yields
    an unexplained 401 mount failure)."""
    from rclone import rclone as rclone_mod
    monkeypatch.setattr(rclone_mod, 'ZURGUSER', 'user', raising=False)
    monkeypatch.setattr(rclone_mod, 'ZURGPASS', 'pass', raising=False)
    monkeypatch.setattr(rclone_mod, 'obscure_password', lambda p: None)
    errors = []
    monkeypatch.setattr(rclone_mod.logger, 'error',
                        lambda msg, *a, **k: errors.append(msg))
    cfg = tmp_path / 'config.yml'
    cfg.write_text('port: 9999\n')
    out = tmp_path / 'rclone.config'
    with open(out, 'w') as f:
        rclone_mod._write_zurg_remote(f, 'zurgarr', str(cfg))
    assert any('obscure' in e.lower() for e in errors), (
        'silent credential omission — user gets an unexplained 401')
    assert 'pass =' not in out.read_text()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_sigchld_exit_codes.py -v`
Expected: FAIL — `test_main_py_does_not_ignore_sigchld` (SIG_IGN present), `test_reap_orphans_exists_and_never_raises` (AttributeError), `test_write_zurg_remote_logs_error_on_obscure_failure` (no error logged). `test_exit_codes_visible_under_sig_dfl` passes already (it sets SIG_DFL itself) — it exists as the executable statement of the invariant.

- [ ] **Step 3: Implement**

In `main.py`, replace lines 165-166:
```python
    # SIGCHLD must stay at SIG_DFL. SIG_IGN makes the kernel auto-reap
    # children, and CPython's subprocess then maps waitpid ECHILD to
    # returncode 0 for EVERY child — check=True never raises, the umount
    # fallback never runs, and crashes log as clean exits. Orphaned
    # grandchildren reparented to PID 1 are drained by
    # processes._reap_orphans() in the monitor loop instead.
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
```

In `utils/processes.py`, add a module-level function (near `_monitor_loop`):
```python
def _reap_orphans():
    """Drain zombie children reparented to PID 1 (orphaned grandchildren
    of managed processes). Called each monitor tick AFTER every registered
    handler has been polled, so subprocess has already claimed the statuses
    it owns. Accepted residual race: a managed child exiting between its
    poll() and this drain gets reaped here and later logs exit code 0 —
    restart behavior is unaffected. Never raises."""
    try:
        while True:
            pid, _status = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                break
    except ChildProcessError:
        pass  # no children at all
    except OSError:
        pass
```

In `_monitor_loop`, call it after the restart loop, before the sleep (currently line 261):
```python
            # Restart outside the lock to avoid holding it during backoff
            for entry in entries_to_restart:
                ...
                    )

            _reap_orphans()

        _monitor_stop_event.wait(10)
```
(`_reap_orphans()` goes inside the `if not _shutting_down:` block, after the `for entry in entries_to_restart:` loop.)

In `rclone/rclone.py` `_write_zurg_remote`, add the loud failure branch (currently lines 346-351):
```python
    if ZURGUSER and ZURGPASS:
        obscured_password = obscure_password(ZURGPASS)
        if obscured_password:
            file_handle.write(f"user = {ZURGUSER}\n")
            file_handle.write(f"pass = {obscured_password}\n")
        else:
            logger.error(
                f"[rclone] Failed to obscure Zurg password — writing "
                f"[{section_name}] WITHOUT credentials. rclone will get 401 "
                f"from Zurg and this mount will fail until the container is "
                f"restarted with a working `rclone obscure`.")
    return port
```

In `zurg/setup.py` `check_and_set_zurg_version`, make the now-reachable error branch diagnostic (currently lines 149-150):
```python
                else:
                    logger.error(
                        f"Error checking Zurg version (exit "
                        f"{result.returncode}): {result.stderr.strip()}")
```

- [ ] **Step 4: Verify the five consumer sites**

Re-read each site and confirm behavior with real exit codes; no further code change expected:
1. `main.py:50-55` — `umount` fallback chain: with real codes the `umount -l` retry and the error log both become reachable; logic is already correct.
2. `rclone/rclone.py:330` `obscure_password` — already catches `CalledProcessError` and returns None; the new `_write_zurg_remote` branch makes the consumer loud.
3. `zurg/setup.py:144` — error branch now reachable and diagnostic (changed above). `ZURG_CURRENT_VERSION` env var is simply not set on failure; `version_check()`/consumers already tolerate absence.
4. `rclone/rclone.py:186` — leftover-mount `umount` failure now correctly warns; the `_is_mount_point` re-check made this self-correcting before, so no change.
5. `utils/processes.py:156` — crash exit codes now log truthfully; no change.

- [ ] **Step 5: Run tests**

Run: `.venv/bin/pytest tests/test_sigchld_exit_codes.py tests/test_processes.py tests/test_processes_supervision.py tests/test_rclone.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/test_sigchld_exit_codes.py main.py utils/processes.py rclone/rclone.py zurg/setup.py
git commit -m "Stop ignoring SIGCHLD — subprocess exit codes were always 0

SIG_IGN made the kernel auto-reap children; CPython then reported
returncode 0 for every subprocess: check=True never raised, the shutdown
umount -l fallback never ran (log claimed success on failure), a failed
rclone obscure silently produced a credential-less rclone.config, and
every crash logged as a clean exit. Now SIG_DFL + an explicit WNOHANG
orphan drain in the process monitor tick. Zurg-remote credential omission
and the Zurg version-check failure now log loudly (audit finding #1)."
```
(Append the attribution trailer.)

---

### Task 4: Scoped prefer-local debrid delete (finding #2)

**Files:**
- Modify: `utils/debrid_client.py` (add `_torrent_episode_claim`, `filter_safe_torrent_deletes` after `find_torrents_by_title_multi` ~:855)
- Modify: `utils/library.py` (add `LibraryScanner.debrid_only_episodes` method near `aliases_for`)
- Modify: `utils/status_server.py` (`/api/library/remove-debrid` ~:2411-2471, `/api/library/remove-debrid/confirm` ~:2473-2560)
- Modify: `utils/library_page.py` (`_postRemoveDebrid` ~:4251, `_showDebridConfirmation` ~:4214, call sites :3513, :3527, :4061, case-3 confirm copy ~:3520-3523)
- Test: `tests/test_debrid_delete_scoping.py` (create)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces:
  - `debrid_client.filter_safe_torrent_deletes(matches, unsafe_episodes) -> (deletable, kept)` — `matches` are the dicts from `find_torrents_by_title[_multi]`; `unsafe_episodes` is a `set[(season:int, episode:int)]`; `kept` entries gain a `kept_reason: str` key.
  - `LibraryScanner.debrid_only_episodes(norm: str) -> set[tuple[int, int]]`.
  - API contract: `/api/library/remove-debrid` accepts optional `type` (`'show'`/`'movie'`) and returns `kept: [...]` alongside `torrents`; `/api/library/remove-debrid/confirm` REQUIRES `type` and, for shows, `title` (+optional `year`), and returns `skipped: [...]` for items refused server-side. Task 5 adds logging to the confirm endpoint — coordinate if executing out of order.

- [ ] **Step 1: Write the failing unit tests**

Create `tests/test_debrid_delete_scoping.py`:

```python
"""Scoped debrid deletion (audit finding #2, CRITICAL data loss).

The TV 'mixed' prefer-local toggle deleted EVERY torrent matching
title+year — including sole copies of debrid-only episodes — seconds
after triggering an async Sonarr search that takes minutes to hours.
Torrents must only be deleted when every episode they claim has a local
copy; unparseable claims fail closed (kept)."""
import pytest

from utils.debrid_client import filter_safe_torrent_deletes, _torrent_episode_claim


def _m(filename, tid='1'):
    return {'id': tid, 'filename': filename, 'hash': '', 'parsed_title': '',
            'year': None, 'service': 'realdebrid'}


class TestTorrentEpisodeClaim:
    def test_single_episode(self):
        seasons, eps = _torrent_episode_claim('Show.S01E06.1080p.WEB.mkv')
        assert eps == {(1, 6)}

    def test_multi_episode(self):
        _, eps = _torrent_episode_claim('Show.S01E04E05.mkv')
        assert eps == {(1, 4), (1, 5)}

    def test_episode_range(self):
        _, eps = _torrent_episode_claim('Show.S01E04-E06.mkv')
        assert eps == {(1, 4), (1, 5), (1, 6)}

    def test_season_pack(self):
        seasons, eps = _torrent_episode_claim('Show.S01.1080p.Complete')
        assert seasons == {1}
        assert eps == set()

    def test_season_word(self):
        seasons, _ = _torrent_episode_claim('Show Season 2 720p')
        assert seasons == {2}

    def test_season_range(self):
        seasons, _ = _torrent_episode_claim('Show.S01-S03.1080p')
        assert seasons == {1, 2, 3}

    def test_unparseable_is_whole_show(self):
        seasons, eps = _torrent_episode_claim('Show Complete Collection 1080p')
        assert seasons == set() and eps == set()


class TestFilterSafeTorrentDeletes:
    def test_duplicate_episode_deletable(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show.S01E02.mkv')], unsafe_episodes={(1, 6), (1, 7)})
        assert len(deletable) == 1 and kept == []

    def test_sole_copy_episode_kept(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show.S01E06.mkv')], unsafe_episodes={(1, 6)})
        assert deletable == [] and len(kept) == 1
        assert 'S01E06' in kept[0]['kept_reason']

    def test_season_pack_spanning_unsafe_kept(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show.S01.Complete.mkv')], unsafe_episodes={(1, 8)})
        assert deletable == [] and len(kept) == 1

    def test_season_pack_of_safe_season_deletable(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show.S02.Complete.mkv')], unsafe_episodes={(1, 8)})
        assert len(deletable) == 1 and kept == []

    def test_unparseable_kept_when_any_unsafe(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show Complete Collection')], unsafe_episodes={(3, 1)})
        assert deletable == [] and len(kept) == 1

    def test_unparseable_deletable_when_no_unsafe(self):
        deletable, kept = filter_safe_torrent_deletes(
            [_m('Show Complete Collection')], unsafe_episodes=set())
        assert len(deletable) == 1 and kept == []

    def test_audit_regression_scenario(self):
        """S1E1-5 both-source, S1E6-8 debrid-only: nothing backing E6-8
        may be deleted."""
        matches = [
            _m('Show.S01E01.mkv', '1'), _m('Show.S01E02.mkv', '2'),
            _m('Show.S01E06.mkv', '6'), _m('Show.S01E07E08.mkv', '7'),
            _m('Show.S01.Pack.mkv', 'p'),
        ]
        unsafe = {(1, 6), (1, 7), (1, 8)}
        deletable, kept = filter_safe_torrent_deletes(matches, unsafe)
        assert {m['id'] for m in deletable} == {'1', '2'}
        assert {m['id'] for m in kept} == {'6', '7', 'p'}


def test_debrid_only_episodes_from_indexes():
    from utils.library import LibraryScanner
    scanner = LibraryScanner.__new__(LibraryScanner)  # no full init
    import threading
    scanner._path_lock = threading.Lock()
    scanner._alias_norms = {}
    scanner._path_index = {
        ('myshow', 1, 1): '/d/e1', ('myshow', 1, 6): '/d/e6',
        ('myshow (2007)', 2, 1): '/d/s2e1',
        ('othershow', 1, 1): '/d/o',
    }
    scanner._local_path_index = {('myshow', 1, 1): '/l/e1'}
    assert scanner.debrid_only_episodes('myshow') == {(1, 6), (2, 1)}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_debrid_delete_scoping.py -v`
Expected: FAIL with ImportError (`filter_safe_torrent_deletes` not defined) / AttributeError (`debrid_only_episodes`).

- [ ] **Step 3: Implement the pure helpers**

In `utils/debrid_client.py`, after `find_torrents_by_title_multi`:

```python
# --- Episode-scoped deletion safety (audit finding #2) -----------------
# A title-level match may include the SOLE debrid copy of episodes that
# have no local file yet. Deleting those seconds after triggering an
# async arr search (minutes-to-hours) leaves the episode with no copy
# anywhere. Claims are parsed from the torrent name; anything that can't
# be parsed fails CLOSED (kept).

_EP_GROUP_RE = re.compile(r'S(\d{1,2})((?:[E\-]E?\d{1,4})+)', re.IGNORECASE)
_SEASON_ONLY_RE = re.compile(r'(?:^|[\s._\-\[])S(\d{1,2})(?![E\d])', re.IGNORECASE)
_SEASON_WORD_RE = re.compile(r'Season[\s._\-]*(\d{1,2})', re.IGNORECASE)
_SEASON_RANGE_RE = re.compile(r'S(\d{1,2})\s*[-–]\s*S(\d{1,2})', re.IGNORECASE)


def _torrent_episode_claim(filename):
    """Parse which (season, episode) pairs / whole seasons a release name
    claims. Returns ``(seasons, episodes)``:

    - ``episodes``: set of (season, episode) for episode-specific releases
      (S01E04, S01E04E05, S01E04-E06).
    - ``seasons``: set of season ints claimed WITHOUT episode detail
      (season packs: "S01.", "Season 1", "S01-S03").
    - both empty: whole-show pack or unparseable — caller must fail closed.
    """
    name = filename or ''
    episodes = set()
    for m in _EP_GROUP_RE.finditer(name):
        season = int(m.group(1))
        ep_str = m.group(2)
        nums = [int(x) for x in re.findall(r'\d+', ep_str)]
        if len(nums) == 2 and '-' in ep_str:
            lo, hi = nums
            if lo <= hi and (hi - lo) < 100:
                nums = list(range(lo, hi + 1))
        episodes.update((season, n) for n in nums)

    seasons = set()
    for m in _SEASON_RANGE_RE.finditer(name):
        lo, hi = int(m.group(1)), int(m.group(2))
        if lo <= hi and (hi - lo) < 50:
            seasons.update(range(lo, hi + 1))
    ep_seasons = {s for s, _ in episodes}
    for m in _SEASON_ONLY_RE.finditer(name):
        s = int(m.group(1))
        if s not in ep_seasons:
            seasons.add(s)
    for m in _SEASON_WORD_RE.finditer(name):
        s = int(m.group(1))
        if s not in ep_seasons:
            seasons.add(s)
    return seasons, episodes


def filter_safe_torrent_deletes(matches, unsafe_episodes):
    """Split title-matched torrents into ``(deletable, kept)``.

    ``unsafe_episodes`` is a set of (season, episode) that exist ONLY on
    debrid — no local copy. A torrent is kept when it (possibly) backs any
    unsafe episode:

    - episode-specific claim: kept iff it claims an unsafe episode
    - season-pack claim: kept iff any unsafe episode is in a claimed season
    - no claim (whole-show / unparseable): kept iff ANY unsafe episode
      exists (fail closed)

    Kept entries get a ``kept_reason`` string for the UI.
    """
    unsafe = set(unsafe_episodes)
    deletable, kept = [], []
    for m in matches:
        seasons, episodes = _torrent_episode_claim(m.get('filename', ''))
        if episodes:
            blocking = episodes & unsafe
            blocking |= {u for u in unsafe if u[0] in seasons}
        elif seasons:
            blocking = {u for u in unsafe if u[0] in seasons}
        else:
            blocking = set(unsafe)
        if blocking:
            entry = dict(m)
            shown = ', '.join(f'S{s:02d}E{e:02d}'
                              for s, e in sorted(blocking)[:5])
            if len(blocking) > 5:
                shown += f' (+{len(blocking) - 5} more)'
            entry['kept_reason'] = f'only debrid copy of {shown}'
            kept.append(entry)
        else:
            deletable.append(m)
    return deletable, kept
```

In `utils/library.py`, add to `LibraryScanner` (next to `aliases_for`):

```python
    def debrid_only_episodes(self, norm):
        """(season, episode) keys that exist on debrid with NO local copy
        for a normalized title, aliases and year-qualified sibling norms
        ("show (2007)") included. Used to scope debrid deletion — an
        episode returned here must never lose its torrent. Errs toward
        returning MORE episodes (safe direction: more kept torrents)."""
        accept = {norm} | self.aliases_for(norm)
        qual_prefixes = tuple(f'{n} (' for n in accept)
        out = set()
        with self._path_lock:
            for key in self._path_index:
                n, s, e = key
                if n in accept or n.startswith(qual_prefixes):
                    if key not in self._local_path_index:
                        out.add((s, e))
        return out
```
(Check `aliases_for` exists on the scanner — it is called as `_sc.aliases_for(norm)` in `status_server.py:2450`; if it takes a lock internally, call it OUTSIDE `self._path_lock` as written above to avoid re-entrancy.)

- [ ] **Step 4: Run unit tests to verify they pass**

Run: `.venv/bin/pytest tests/test_debrid_delete_scoping.py -v`
Expected: all PASS.

- [ ] **Step 5: Wire the search endpoint (`/api/library/remove-debrid`)**

In `utils/status_server.py`, after `matches, errors = find_torrents_by_title_multi(...)` (currently line 2456), replace the response block with:

```python
                matches, errors = find_torrents_by_title_multi(accept_norms, target_year=year)

                # Scope show deletions: never offer torrents that (may) back
                # episodes with no local copy (audit finding #2).
                media_type = str(values.get('type', '')).strip()
                kept = []
                if media_type == 'show':
                    if _sc is None:
                        self._send_json_response(503, json.dumps({
                            'error': 'Library scanner not initialized — cannot '
                                     'verify which torrents are safe to delete'
                        }))
                        return
                    from utils.debrid_client import filter_safe_torrent_deletes
                    unsafe = _sc.debrid_only_episodes(norm)
                    matches, kept = filter_safe_torrent_deletes(matches, unsafe)

                self._send_json_response(200, json.dumps({
                    'status': 'found',
                    'title': title,
                    'normalized_title': norm,
                    'torrents': matches,
                    'kept': kept,
                    'count': len(matches),
                    'errors': errors,
                }))
```

- [ ] **Step 6: Wire the confirm endpoint (`/api/library/remove-debrid/confirm`) — server-authoritative re-check**

After the `norm_items` validation loop and `title = values.get('title', '').strip()` (currently line 2515), insert:

```python
                title = values.get('title', '').strip()
                media_type = str(values.get('type', '')).strip()
                if media_type not in ('show', 'movie'):
                    self._send_json_response(400, json.dumps({
                        'error': "type ('show' or 'movie') is required"
                    }))
                    return

                skipped = []
                if media_type == 'show':
                    # Re-derive safety server-side — the item list is
                    # client-supplied and stale by at least one round trip.
                    # Fail closed on every gap (audit finding #2).
                    if not title:
                        self._send_json_response(400, json.dumps({
                            'error': 'title is required for show deletions'
                        }))
                        return
                    from utils.library import get_scanner, normalize_title
                    from utils.debrid_client import (
                        find_torrents_by_title_multi, filter_safe_torrent_deletes,
                    )
                    _sc = get_scanner()
                    if _sc is None:
                        self._send_json_response(503, json.dumps({
                            'error': 'Library scanner not initialized — cannot '
                                     'verify which torrents are safe to delete'
                        }))
                        return
                    year = values.get('year')
                    if year is not None:
                        try:
                            year = int(year)
                        except (ValueError, TypeError):
                            year = None
                    norm = normalize_title(title)
                    accept_norms = {norm} | _sc.aliases_for(norm)
                    fresh, _errs = find_torrents_by_title_multi(
                        accept_norms, target_year=year)
                    unsafe = _sc.debrid_only_episodes(norm)
                    deletable, kept = filter_safe_torrent_deletes(fresh, unsafe)
                    allowed = {(m['service'], str(m['id'])) for m in deletable}
                    kept_by_key = {(m['service'], str(m['id'])): m for m in kept}
                    verified = []
                    for it in norm_items:
                        key = (it['service'], it['id'])
                        if key in allowed:
                            verified.append(it)
                        elif key in kept_by_key:
                            skipped.append({'id': it['id'], 'service': it['service'],
                                            'reason': kept_by_key[key]['kept_reason']})
                        else:
                            skipped.append({'id': it['id'], 'service': it['service'],
                                            'reason': 'not found in a fresh provider '
                                                      'listing — refused (fail closed)'})
                    norm_items = verified
                    if not norm_items:
                        self._send_json_response(200, json.dumps({
                            'status': 'skipped', 'deleted': 0, 'skipped': skipped,
                            'message': 'No torrents deleted — every requested item '
                                       'is (or may be) the only copy of episodes '
                                       'with no local file yet.',
                        }))
                        return

                deleted, failed = delete_torrents_multi(norm_items)
```

And extend the result dict (after `if failed:` block):
```python
                if skipped:
                    result['skipped'] = skipped
                    result['message'] += f' ({len(skipped)} kept — sole debrid copy)'
```

- [ ] **Step 7: Update the JS client in `utils/library_page.py`**

`_postRemoveDebrid` (line 4251) gains a `mediaType` param, sends `type` on both requests, surfaces kept torrents:

```python
function _postRemoveDebrid(title, year, mediaType) {
```
- payload: `var payload = {title: title, type: mediaType || 'show'};` (keep `if (year) payload.year = year;`)
- after `var torrents = res.d.torrents || [];` add: `var keptList = res.d.kept || [];`
- when `!res.d.count`: if `keptList.length`, show
  `_showMsg('Nothing deleted: all ' + keptList.length + ' matching torrent(s) are the only debrid copy of episodes with no local file yet. Retry after the downloads finish.', 'error');` instead of the generic "No debrid torrents found".
- confirm POST body: `JSON.stringify({items: items, title: title, type: mediaType || 'show', year: year || null})`
- pass `keptList` to `_showDebridConfirmation(torrents, keptList, title, onConfirm, onCancel)` and inside it, after the delete `</ul>`, render:
```js
  if (keptList && keptList.length) {
    html += '<div style="font-size:.82em;color:var(--text2);margin:6px 0 2px">' + esc(String(keptList.length)) + ' torrent(s) kept (only debrid copy of episodes not yet local):</div>';
    html += '<ul class="confirm-list">';
    for (var k = 0; k < keptList.length && k < 5; k++) {
      html += '<li style="color:var(--text3)">' + esc(keptList[k].filename || keptList[k].id) + ' — ' + esc(keptList[k].kept_reason || '') + '</li>';
    }
    if (keptList.length > 5) html += '<li style="color:var(--text3)">... and ' + (keptList.length - 5) + ' more</li>';
    html += '</ul>';
  }
```
- Call sites: line 3513 and 3527 become `_postRemoveDebrid(_detailItem.title, _detailItem.year, 'show')`; line 4061 becomes `_postRemoveDebrid(_detailItem.title, _detailItem.year, 'movie')`.
- Case-3 confirm copy (line 3522-3523): change to
  `'Switch ' + totalDlEps + ' episode(s) to local via ' + svcLabel2 + ' and remove debrid duplicates? Torrents still needed by episodes without a local copy are kept automatically.'`

- [ ] **Step 8: Endpoint-level regression test**

Append to `tests/test_debrid_delete_scoping.py` (follow the live-server harness pattern from `tests/test_compromise_observability.py:132-144` — spin up `ThreadingHTTPServer(('127.0.0.1', 0), StatusHandler)` with `StatusHandler.auth_credentials = None`, request via `urllib.request`):

```python
def test_confirm_endpoint_requires_type(status_server_harness):
    """POST /api/library/remove-debrid/confirm without type -> 400."""
    status, body = status_server_harness.post_json(
        '/api/library/remove-debrid/confirm',
        {'items': [{'id': '1', 'service': 'realdebrid'}]})
    assert status == 400
    assert 'type' in body.get('error', '')
```
Write the harness as a fixture in this file (copy the pattern; do not modify conftest.py). A deeper end-to-end confirm test requires mocking `find_torrents_by_title_multi` and the scanner inside the handler process — cover that logic through the unit tests of `filter_safe_torrent_deletes` (Step 1) plus this contract test; the full-flow scenario is Task 7's app-testing agent's job.

- [ ] **Step 9: Run tests**

Run: `.venv/bin/pytest tests/test_debrid_delete_scoping.py tests/test_debrid_client.py tests/test_library.py -v`
Expected: all PASS.

- [ ] **Step 10: Commit**

```bash
git add tests/test_debrid_delete_scoping.py utils/debrid_client.py utils/library.py utils/status_server.py utils/library_page.py
git commit -m "Scope prefer-local debrid deletion to episodes with a local copy

The TV 'mixed' prefer-local toggle deleted every torrent matching
title+year seconds after triggering an async Sonarr search — including
sole copies of debrid-only episodes (and unrelated debrid-only seasons),
which could leave episodes with no copy anywhere. The server now derives
the debrid-only episode set from the scanner and refuses to delete any
torrent whose parsed claim (SxxEyy / season pack / whole-show, fail
closed on unparseable) touches it, re-verified authoritatively at
confirm time. The UI lists kept torrents and why (audit finding #2)."
```
(Append the attribution trailer.)

---

### Task 5: Observability wiring (finding #9)

**Files:**
- Modify: `utils/status_server.py` (`/api/library/delete` ~:2679-2687, `/api/library/remove-local` three success paths ~:2202-2213/:2215-2224/:2265-2271, `/api/library/switch-to-debrid` ~:2393-2401, `/api/library/remove-debrid/confirm` after delete)
- Modify: `utils/arr_client.py` (`ensure_and_request_tv` ~:2443-2452, `ensure_and_request_movie` ~:2471-2479)
- Test: `tests/test_manual_action_observability.py` (create)

**Interfaces:**
- Consumes: Task 4's confirm-endpoint shape (`deleted`, `skipped` present). No new symbols.
- Produces: nothing other tasks rely on. Reuses existing causes `preference_source_switch` and `user_triggered_search` — NO new `CAUSE_*` constants, formatters, or `FORMATTER_JS` entries.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_manual_action_observability.py`:

```python
"""Manual UI actions must be as observable as the scheduled enforcement
path performing identical operations (audit finding #9): history events +
notifications for destructive library changes, and the advertised
'arr_deleted' notification must actually fire."""
import re
from pathlib import Path

STATUS_SERVER = Path(__file__).resolve().parent.parent / 'utils' / 'status_server.py'
ARR_CLIENT = Path(__file__).resolve().parent.parent / 'utils' / 'arr_client.py'


def _endpoint_block(source, path_literal):
    """Slice the handler block for one endpoint: from its `if self.path ==`
    line to the next `if self.path` line."""
    start = source.index(path_literal)
    rest = source[start + len(path_literal):]
    m = re.search(r"\n        if self\.path", rest)
    return rest[:m.start()] if m else rest


def test_arr_deleted_notification_fires():
    src = STATUS_SERVER.read_text()
    block = _endpoint_block(src, "'/api/library/delete'")
    assert "notify('arr_deleted'" in block, (
        "arr_deleted is advertised in Settings but never sent")


@pytest.mark.parametrize('endpoint', [
    "'/api/library/remove-local'",
    "'/api/library/switch-to-debrid'",
    "'/api/library/remove-debrid/confirm'",
])
def test_manual_endpoints_log_and_notify(endpoint):
    src = STATUS_SERVER.read_text()
    block = _endpoint_block(src, endpoint)
    assert "log_event('switched_source'" in block, (
        f'{endpoint}: destructive action leaves no Activity Log entry')
    assert "notify('library_refresh'" in block, (
        f'{endpoint}: destructive action sends no notification')


def test_overseerr_requests_log_search_triggered():
    src = ARR_CLIENT.read_text()
    tv = src[src.index('def ensure_and_request_tv'):src.index('def ensure_and_request_movie')]
    movie = src[src.index('def ensure_and_request_movie'):]
    movie = movie[:re.search(r'\nclass |\ndef |\n# ---', movie[10:]).start() + 10] \
        if re.search(r'\nclass |\ndef |\n# ---', movie[10:]) else movie
    for name, body in (('ensure_and_request_tv', tv),
                       ('ensure_and_request_movie', movie)):
        assert "log_event('search_triggered'" in body, (
            f'{name}: Overseerr fallback logs nothing while Sonarr/Radarr '
            f'paths log search_triggered')
```
Add `import pytest` at the top.

(Source-level assertions are the right altitude here: the handler bodies are inline in a 3400-line `do_POST` and the emission calls are fire-and-forget inside `try/except` — behavior tests would only re-mock what these assert structurally. The golden cause-formatter tests in `test_activity_format.py` already cover rendering.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_manual_action_observability.py -v`
Expected: all FAIL (none of the emissions exist).

- [ ] **Step 3: Implement — status_server.py**

Add a module-level helper near the top of the handler class (after `_send_json_response` is fine) or as a plain function near other helpers:

```python
def _emit_source_switch(title, from_src, to_src, count, media_type, detail):
    """History + notification for a user-triggered source change. Mirrors
    the scheduled enforce_source_preferences emissions (library.py) so
    manual UI actions are equally observable. Never raises."""
    try:
        from utils import history as _hist
        _hist.log_event('switched_source', title, source='library',
                        detail=detail,
                        meta={'cause': 'preference_source_switch',
                              'from': from_src, 'to': to_src,
                              'count': count, 'media_type': media_type,
                              'trigger': 'user'})
    except Exception:
        pass
    try:
        from utils.notifications import notify
        notify('library_refresh', f"Source switch: {title}", detail)
    except Exception:
        pass
```

Call sites:

1. `/api/library/remove-local`, Sonarr-episode success path (inside `if result.get('status') != 'error':`, next to the existing `clear_pending`/`scanner.refresh()` at ~:2203-2210):
```python
                        _emit_source_switch(
                            title, 'local', 'debrid', len(ep_nums), 'show',
                            f"Removed {len(ep_nums)} local episode(s) via Sonarr — now debrid-only")
```
2. Radarr-movie success path (~:2217-2221):
```python
                        _emit_source_switch(
                            title, 'local', 'debrid', 1, 'movie',
                            "Removed local movie via Radarr — now debrid-only")
```
3. Direct-file-deletion fallback success path (after `clear_pending(norm, cleared)` ~:2269):
```python
                if result.get('removed', 0) > 0:
                    ...
                    _emit_source_switch(
                        title, 'local', 'debrid', result.get('removed', 0), 'show',
                        f"Deleted {result.get('removed', 0)} local file(s) — now debrid-only")
```
4. `/api/library/switch-to-debrid` success path (inside `if result.get('switched', 0) > 0:` ~:2393):
```python
                    _emit_source_switch(
                        title, 'local', 'debrid', result.get('switched', 0), 'show',
                        f"Switched {result.get('switched', 0)} episode(s) to debrid symlinks")
```
5. `/api/library/remove-debrid/confirm`, after `deleted, failed = delete_torrents_multi(norm_items)` when `deleted > 0` (use `title or 'Debrid torrents'`):
```python
                if deleted > 0:
                    _emit_source_switch(
                        title or 'Debrid torrents', 'debrid', 'local', deleted,
                        media_type or 'show',
                        f"Removed {deleted} debrid torrent(s) via Library UI")
```
(Task 4 introduced `media_type` in this handler; if executing this task first, use `values.get('type', 'show')`.)
6. `/api/library/delete` — next to the existing `log_event('arr_deleted', ...)` (~:2679-2687), add inside its own try:
```python
                    try:
                        from utils.notifications import notify
                        notify('arr_deleted', f'Deleted: {title}',
                               f'{title} deleted from {service_name.capitalize()} '
                               f'via Library UI (files, debrid torrents, and '
                               f'symlinks cleaned up)',
                               level='warning')
                    except Exception:
                        pass
```

- [ ] **Step 4: Implement — arr_client.py Overseerr parity**

In `ensure_and_request_tv`, before the success `return` (~:2448):
```python
        season_str = ', '.join(f'S{s:02d}' for s in seasons)
        if _history:
            _history.log_event('search_triggered', f'Overseerr request: {title}',
                               source='arr',
                               detail=f'Requested {title} {season_str} in Overseerr',
                               meta={'cause': 'user_triggered_search',
                                     'arr_service': 'overseerr'},
                               media_title=title)
        return {
```
In `ensure_and_request_movie`, before its success `return` (~:2475):
```python
        if _history:
            _history.log_event('search_triggered', f'Overseerr request: {title}',
                               source='arr',
                               detail=f'Requested {title} in Overseerr',
                               meta={'cause': 'user_triggered_search',
                                     'arr_service': 'overseerr'},
                               media_title=title)
        return {
```
(`_history` is the module-level import already used at `arr_client.py:1130`.)

- [ ] **Step 5: Run tests**

Run: `.venv/bin/pytest tests/test_manual_action_observability.py tests/test_activity_format.py tests/test_arr_client.py tests/test_history.py -v`
Expected: all PASS (existing causes → golden formatter tests unaffected).

- [ ] **Step 6: Commit**

```bash
git add tests/test_manual_action_observability.py utils/status_server.py utils/arr_client.py
git commit -m "Log and notify manual library actions; make arr_deleted notifications real

The manual remove-local / switch-to-debrid / remove-debrid endpoints
performed the same destructive operations as the scheduled enforcement
path but emitted no history events and no notifications — library
changes from the UI were invisible in Activity. The advertised
arr_deleted notification event had no sender anywhere. Overseerr
fallback requests now log search_triggered like the Sonarr/Radarr
paths. Reuses existing cause slugs; no formatter changes (finding #9)."
```
(Append the attribution trailer.)

---

### Task 6: HTTP hardening — CSRF, atomic_write fsync, /api/logs (findings #8, #10)

**Files:**
- Modify: `utils/status_server.py` (new `_origin_allowed` method + `trusted_origins` class attr; guards at top of `do_POST` ~:1853 and `do_DELETE` ~:3190; parse env in `setup()` ~:3378)
- Modify: `utils/config_reload.py` (~:299-300 re-apply on SIGHUP; add var name to the STATUS_UI watch list ~:50)
- Modify: `base/__init__.py` (Config attr ~:267, module global ~:462, `__all__` ~:66)
- Modify: `utils/settings_api.py` (schema tuple in the Status UI group ~:265)
- Modify: `utils/file_utils.py` (`atomic_write` fsync)
- Modify: `utils/tmdb.py:455` (pass `fsync=False`)
- Modify: `utils/status_server.py` `/api/logs` (~:1525-1526)
- Modify: `CONFIGURATION.md`, `.env.example`, `TROUBLESHOOTING.md`
- Test: `tests/test_origin_guard.py` (create); extend `tests/test_file_utils.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `atomic_write(target_path, mode='w', encoding='utf-8', fsync=True)` (new kwarg, default True — all existing callers unchanged); env var `STATUS_UI_TRUSTED_ORIGINS`; `StatusHandler._origin_allowed()`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_origin_guard.py` (live-server harness per `tests/test_compromise_observability.py:132-144`):

```python
"""Same-origin guard for state-changing endpoints (audit finding #8).

Basic auth is attached automatically by browsers; a <form
enctype="text/plain"> submission produces a body that parses as JSON, so
without an Origin check any page the operator visits can fire
/api/library/delete or rewrite .env. Reverse-proxied deployments
allow-list their public origin via STATUS_UI_TRUSTED_ORIGINS."""
import json
import socket
import threading
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer

import pytest

from utils.status_server import StatusHandler


@pytest.fixture
def server():
    StatusHandler.auth_credentials = None
    StatusHandler.status_data_ref = None
    StatusHandler.trusted_origins = frozenset()
    srv = ThreadingHTTPServer(('127.0.0.1', 0), StatusHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f'127.0.0.1:{srv.server_address[1]}'
    srv.shutdown()
    StatusHandler.trusted_origins = frozenset()


def _post(host, path, headers=None):
    req = urllib.request.Request(
        f'http://{host}{path}', data=b'{}', method='POST',
        headers={'Content-Type': 'application/json', **(headers or {})})
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, json.loads(resp.read() or b'{}')
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b'{}')


def test_no_origin_allowed(server):
    """curl/scripts (no Origin, no Referer) must keep working."""
    status, _ = _post(server, '/api/library/refresh')
    assert status != 403 or 'cross-origin' not in str(_)


def test_matching_origin_allowed(server):
    status, body = _post(server, '/api/library/refresh',
                         {'Origin': f'http://{server}'})
    assert 'cross-origin' not in body.get('error', '')


def test_mismatched_origin_rejected(server):
    status, body = _post(server, '/api/library/refresh',
                         {'Origin': 'http://evil.example.com'})
    assert status == 403
    assert 'cross-origin' in body.get('error', '').lower()


def test_null_origin_rejected(server):
    status, _ = _post(server, '/api/library/refresh', {'Origin': 'null'})
    assert status == 403


def test_trusted_origin_allowed(server):
    StatusHandler.trusted_origins = frozenset({'https://zurgarr.example.com'})
    status, body = _post(server, '/api/library/refresh',
                         {'Origin': 'https://zurgarr.example.com'})
    assert 'cross-origin' not in body.get('error', '')


def test_referer_fallback_rejected_cross_site(server):
    status, _ = _post(server, '/api/library/refresh',
                      {'Referer': 'http://evil.example.com/attack.html'})
    assert status == 403


def test_delete_method_guarded(server):
    req = urllib.request.Request(
        f'http://{server}/api/history', method='DELETE',
        headers={'Origin': 'http://evil.example.com'})
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        code = resp.status
    except urllib.error.HTTPError as e:
        code = e.code
    assert code == 403
```

Note: `/api/library/refresh` without auth configured — `do_POST` currently 403s when `auth_credentials` is None with error text about STATUS_UI_AUTH. The origin guard must run BEFORE that check so these tests can distinguish rejections; assert on the error text (`'cross-origin' in error`) as written, and for the allowed cases assert the error is the STATUS_UI_AUTH one, not the cross-origin one.

Extend `tests/test_file_utils.py`:

```python
def test_atomic_write_fsyncs_by_default(tmp_path, monkeypatch):
    synced = []
    real_fsync = os.fsync
    monkeypatch.setattr(os, 'fsync', lambda fd: (synced.append(fd), real_fsync(fd))[1])
    target = tmp_path / 'state.json'
    with atomic_write(str(target)) as f:
        f.write('{"k": 1}')
    assert len(synced) >= 2, 'expected file fsync + directory fsync'
    assert target.read_text() == '{"k": 1}'


def test_atomic_write_fsync_false_skips(tmp_path, monkeypatch):
    synced = []
    monkeypatch.setattr(os, 'fsync', lambda fd: synced.append(fd))
    target = tmp_path / 'cache.json'
    with atomic_write(str(target), fsync=False) as f:
        f.write('x')
    assert synced == []
    assert target.read_text() == 'x'
```
(Match the existing import style in that file; add `import os` if missing.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_origin_guard.py tests/test_file_utils.py -v`
Expected: origin tests FAIL (no guard: mismatched/null/referer/delete cases get non-403 or wrong error); fsync-by-default test FAILS (no fsync calls).

- [ ] **Step 3: Implement the origin guard**

In `StatusHandler`, add a class attribute next to `auth_credentials` usage (declare near the top of the class):
```python
    trusted_origins = frozenset()
```

Add the method (next to `_is_authenticated`):
```python
    def _origin_allowed(self):
        """Reject browser-initiated cross-site state changes (CSRF).
        Basic auth is attached automatically by browsers, and a
        text/plain form submission parses as JSON — so Origin (falling
        back to Referer) must match Host or be explicitly allow-listed
        via STATUS_UI_TRUSTED_ORIGINS (reverse-proxy deployments).
        Requests with neither header (curl, scripts) are allowed."""
        origin = self.headers.get('Origin')
        if origin is None:
            ref = self.headers.get('Referer')
            if not ref:
                return True
            p = urlparse(ref)
            if not p.scheme or not p.netloc:
                return True
            origin = f'{p.scheme}://{p.netloc}'
        origin = origin.rstrip('/')
        if origin == 'null':
            return False
        if origin in self.trusted_origins:
            return True
        host = self.headers.get('Host', '')
        return bool(host) and urlparse(origin).netloc == host

    def _reject_cross_origin(self):
        self._send_json_response(403, json.dumps({
            'error': 'cross-origin request rejected. If the dashboard is '
                     'served behind a reverse proxy or a different hostname, '
                     'add its public origin (e.g. https://zurgarr.example.com) '
                     'to STATUS_UI_TRUSTED_ORIGINS.'
        }))
```

At the very top of `do_POST` (before the `auth_credentials` check at ~:1846):
```python
    def do_POST(self):
        if not self._origin_allowed():
            self._reject_cross_origin()
            return
```
Same two lines at the very top of `do_DELETE` (~:3184).

In `setup()` (~:3378-3381), after the auth parsing:
```python
    auth = os.environ.get('STATUS_UI_AUTH')

    StatusHandler.status_data_ref = status_data
    StatusHandler.auth_credentials = auth if auth and ':' in auth else None
    trusted = os.environ.get('STATUS_UI_TRUSTED_ORIGINS', '')
    StatusHandler.trusted_origins = frozenset(
        o.strip().rstrip('/') for o in trusted.split(',') if o.strip())
```

In `utils/config_reload.py` (~:299-300), mirror it so SIGHUP reload picks up changes:
```python
                auth = os.environ.get('STATUS_UI_AUTH')
                StatusHandler.auth_credentials = auth if auth and ':' in auth else None
                trusted = os.environ.get('STATUS_UI_TRUSTED_ORIGINS', '')
                StatusHandler.trusted_origins = frozenset(
                    o.strip().rstrip('/') for o in trusted.split(',') if o.strip())
```
And add `'STATUS_UI_TRUSTED_ORIGINS'` to the STATUS_UI var list at `config_reload.py:50`.

- [ ] **Step 4: Env plumbing (CLAUDE.md checklist)**

- `base/__init__.py`: in `Config.__init__` next to `self.STATUS_UI_AUTH` (~:267): `self.STATUS_UI_TRUSTED_ORIGINS = os.getenv('STATUS_UI_TRUSTED_ORIGINS')`; module global next to line ~462: `STATUS_UI_TRUSTED_ORIGINS = config.STATUS_UI_TRUSTED_ORIGINS`; add `'STATUS_UI_TRUSTED_ORIGINS'` to `__all__` next to the other STATUS_UI entries (~:66).
- `utils/settings_api.py`: in the Status UI group (~:265), add:
  `('STATUS_UI_TRUSTED_ORIGINS', 'Trusted origins', 'string', False, 'Comma-separated origins allowed to make state-changing requests when the dashboard is served behind a reverse proxy (e.g. https://zurgarr.example.com). Direct IP:port access needs no entry.'),`
  (No `_ENV_DEFAULTS` entry — default is empty.)
- `CONFIGURATION.md`: table row in the Status UI section: name, default `(empty)`, description matching the schema text.
- `.env.example`: commented line near the STATUS_UI block:
  `# STATUS_UI_TRUSTED_ORIGINS=https://zurgarr.example.com`
- `TROUBLESHOOTING.md`: symptom-first entry: **"Dashboard buttons fail with 'cross-origin request rejected' behind a reverse proxy"** — explain that state-changing requests verify the browser Origin against the Host header, and that proxied/renamed deployments must set `STATUS_UI_TRUSTED_ORIGINS` to the public origin (scheme://host[:port], comma-separated for several), then reload config (Settings → Save & Reload or SIGHUP).

- [ ] **Step 5: Implement atomic_write fsync + /api/logs hardening**

`utils/file_utils.py` — new kwarg and durability (docstring already claims crash-safe; make it true):
```python
@contextmanager
def atomic_write(target_path, mode='w', encoding='utf-8', fsync=True):
```
Inside, after `yield tmp_file` (still within the `with os.fdopen(...)` block):
```python
        with os.fdopen(fd, **fdopen_kwargs) as tmp_file:
            yield tmp_file
            if fsync:
                tmp_file.flush()
                os.fsync(tmp_file.fileno())
```
And after `os.replace(tmp_path, target_path)`:
```python
        os.replace(tmp_path, target_path)

        # Persist the rename itself — os.replace is atomic for concurrent
        # readers but not durable across power loss without a dir fsync.
        if fsync:
            dir_fd = os.open(target_dir, os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
```
Update the docstring to document `fsync` ("pass fsync=False only for rebuildable caches").

`utils/tmdb.py:455`: `with atomic_write(_CACHE_PATH, fsync=False) as f:` (rebuildable cache, hot path).

`utils/status_server.py` `/api/logs` (~:1525-1526):
```python
            try:
                lines = int(params.get('lines', ['100'])[0])
            except (ValueError, TypeError):
                lines = 100
            lines = max(1, min(lines, 1000))
```

- [ ] **Step 6: Run tests**

Run: `.venv/bin/pytest tests/test_origin_guard.py tests/test_file_utils.py tests/test_settings_api.py tests/test_config_reload.py tests/test_status_ui_enhancements.py tests/test_compromise_observability.py tests/test_reboot_families.py -v`
Expected: all PASS (the last two exercise the live-server harness and must not trip the origin guard — they send no Origin header, which stays allowed).

- [ ] **Step 7: Commit**

```bash
git add tests/test_origin_guard.py tests/test_file_utils.py utils/status_server.py utils/config_reload.py base/__init__.py utils/settings_api.py utils/file_utils.py utils/tmdb.py CONFIGURATION.md .env.example TROUBLESHOOTING.md
git commit -m "HTTP hardening: same-origin guard, durable atomic_write, /api/logs validation

- do_POST/do_DELETE reject cross-origin browser requests (Origin/Referer
  vs Host, or STATUS_UI_TRUSTED_ORIGINS allow-list for reverse proxies) —
  closes the text/plain-form CSRF vector against /api/library/delete,
  /api/settings/env, and restarts (finding #8)
- atomic_write now fsyncs file + directory (fsync=False opt-out used by
  the TMDB cache), matching its crash-safe docstring (finding #10)
- /api/logs?lines= handles non-numeric and negative values (finding #10)"
```
(Append the attribution trailer.)

---

### Task 7: Full verification, CHANGELOG, mandatory review pass

**Files:**
- Modify: `CHANGELOG.md`
- No other source changes expected (reviewer findings excepted).

**Interfaces:**
- Consumes: all six commits.
- Produces: the batch, verified and reviewed, ready for the owner's merge decision.

- [ ] **Step 1: Full test suite**

Run: `.venv/bin/pytest`
Expected: 100% pass. Fix any regression before proceeding (amend the responsible commit if trivially attributable; otherwise a fix commit).

- [ ] **Step 2: CHANGELOG entries**

Under the current unreleased version heading in `CHANGELOG.md`, using the existing `- **Bold title**: Description` format, add (adjust wording to match neighboring entries' voice):

```markdown
- **Fixed disabled features running anyway**: `DUPLICATE_CLEANUP=false`, `ZURG_UPDATE=false`, and `PD_UPDATE=false` were treated as enabled (string truthiness). Worst case deleted local media with `DUPLICATE_CLEANUP_KEEP=zurg`.
- **Fixed supervision loss after config reload**: Saving settings (SIGHUP) or using the UI Restart button left Zurg/rclone/plex_debrid without auto-restart until the container was recreated.
- **Fixed notification floods from dead processes**: a process that exhausted its restart budget re-notified every 10 seconds; it now alerts once.
- **Fixed subprocess exit codes always reading 0**: SIGCHLD was set to SIG_IGN, hiding every child failure — shutdown unmount fallbacks never ran and crashes logged as clean exits.
- **Fixed prefer-local deleting sole debrid copies**: switching a partially-local show to "Prefer Local" could delete torrents backing episodes that had no local file yet. Deletion is now scoped per episode and fails closed; the UI shows which torrents were kept and why.
- **Fixed silent log-suppression deadlock**: `ZURG_LOG_LEVEL=off` / `RCLONE_LOG_LEVEL=off` created pipes nobody read, freezing the child once it wrote 64KB.
- **Manual library actions now logged and notified**: remove-local, switch-to-debrid, and remove-debrid from the Library UI write Activity history and send notifications, matching the scheduled enforcement path; Overseerr requests log like Sonarr/Radarr searches; the `arr_deleted` notification event now actually fires.
- **Added cross-origin request protection**: state-changing dashboard endpoints reject cross-site browser requests. Reverse-proxy deployments set the new `STATUS_UI_TRUSTED_ORIGINS` variable.
- **Hardened state-file writes**: atomic writes now fsync, surviving power loss / OOM kills without truncated pending/blocklist/config files.
```

```bash
git add CHANGELOG.md
git commit -m "Changelog for the 2026-09 audit fix batch"
```
(Append the attribution trailer.)

- [ ] **Step 3: Mandatory reviewer pass (in order)**

1. Dispatch the **code-reviewer** agent over `git diff master...audit-fixes-2026-09`.
2. Then the **bug-hunter** agent over the same range, with emphasis on: the SIGCHLD reap race, origin-guard bypasses (Origin header casing, port-in-Host edge cases, IPv6 hosts), and fail-closed completeness of the confirm-endpoint scoping.
3. Then the **app-testing** agent: full suite + targeted runs of the six new/extended test files + a Docker build (`docker build -t pd_zurg .`).

Per CLAUDE.md: every real bug a reviewer surfaces gets fixed in this batch — amend or add commits, then re-run the affected tests. Stylistic or provably-can't-fire findings may be declined explicitly.

- [ ] **Step 4: Report**

Summarize to the owner: commits on `audit-fixes-2026-09`, test results, reviewer findings and their dispositions. Merging to `master` and prod deploy (`rebuild.sh master` on plex-host) are the owner's call — do NOT merge or push without explicit instruction.
