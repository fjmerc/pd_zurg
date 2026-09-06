from base import *
from utils.logger import SubprocessLogger
from utils import heartbeat


class RestartPolicy:
    """Configuration for automatic process restart behavior."""

    def __init__(self, max_restarts=5, backoff_seconds=None, window_seconds=3600):
        self.max_restarts = max_restarts
        self.backoff_seconds = backoff_seconds or [5, 15, 45, 120, 300]
        self.window_seconds = window_seconds


# Per-process shutdown timeouts (seconds). Processes not listed get the default.
_SHUTDOWN_TIMEOUTS = {
    'plex_debrid': 15,   # May be mid-scrape
    'Zurg': 10,          # WebDAV server
    'rclone': 10,        # FUSE mount
}
_DEFAULT_SHUTDOWN_TIMEOUT = 10

# Dependency chain for restart ordering. If a dependency is dead, defer
# the restart to avoid wasting retry budget (e.g., rclone against dead Zurg).
_PROCESS_DEPENDENCIES = {
    'rclone': ['Zurg'],
    'plex_debrid': ['rclone'],
}

# Global registry of all tracked processes for graceful shutdown
_process_registry = []
_registry_lock = threading.Lock()
_shutting_down = False
_monitor_stop_event = threading.Event()
_monitor_thread = None

# Liveness ceiling for the healthcheck.  A single _handle_restart can
# legitimately block ~300s (max backoff) plus restart time; beating before
# each restart bounds the gap to one entry's worth, so 15 minutes means
# dead, not busy.
_HEARTBEAT_NAME = 'process_monitor'
_HEARTBEAT_STALE_AFTER = 900


def register_process(handler, process_name, key_type=None):
    with _registry_lock:
        # Avoid duplicate entries for the same handler
        for entry in _process_registry:
            if entry['handler'] is handler:
                return
        _process_registry.append({
            'handler': handler,
            'process_name': process_name,
            'key_type': key_type,
        })


def shutdown_all_processes(logger):
    global _shutting_down
    _shutting_down = True
    stop_process_monitor()

    with _registry_lock:
        total_start = time.time()
        for entry in reversed(_process_registry):
            handler = entry['handler']
            process_name = entry['process_name']
            key_type = entry['key_type']
            try:
                if handler.process and handler.process.poll() is None:
                    desc = f"{process_name} w/ {key_type}" if key_type else process_name
                    timeout = _SHUTDOWN_TIMEOUTS.get(process_name, _DEFAULT_SHUTDOWN_TIMEOUT)
                    logger.info(f"Terminating {desc} (pid {handler.process.pid}, timeout {timeout}s)...")
                    proc_start = time.time()
                    handler.process.terminate()
                    try:
                        handler.process.wait(timeout=timeout)
                        elapsed = time.time() - proc_start
                        logger.info(f"{desc} exited in {elapsed:.1f}s")
                    except subprocess.TimeoutExpired:
                        elapsed = time.time() - proc_start
                        logger.warning(f"{desc} did not exit after {elapsed:.1f}s, killing...")
                        handler.process.kill()
                        handler.process.wait(timeout=5)
            except Exception as e:
                logger.error(f"Error shutting down process: {e}")
        total_elapsed = time.time() - total_start
        logger.info(f"All processes shut down in {total_elapsed:.1f}s")
        _process_registry.clear()


def _get_backoff_delay(policy, restart_count):
    """Get the backoff delay for the given restart attempt."""
    idx = min(restart_count, len(policy.backoff_seconds) - 1)
    return policy.backoff_seconds[idx]


def _on_restart_exhausted(desc, restart_count, max_restarts):
    """Fire notification and status event when a process exhausts all restart attempts."""
    try:
        from utils.notifications import notify
        notify(
            'health_error',
            f'Process Dead: {desc}',
            f'{desc} has crashed {restart_count} times and exhausted all '
            f'{max_restarts} restart attempts. The process will NOT be restarted '
            f'automatically. Check logs and restart the container.',
            level='error'
        )
    except Exception:
        pass

    try:
        from utils.status_server import status_data
        status_data.add_event(
            'process_manager',
            f'{desc} exhausted {max_restarts} restart attempts — process is dead',
            level='error'
        )
    except Exception:
        pass


def _check_dependencies_alive(process_name):
    """Check if all dependencies for a process are alive.

    A name can be registered more than once (each rclone mount is its own
    entry) — the dependency is satisfied when at least one instance is
    alive, so a single dead mount doesn't wedge dependents that can run
    degraded on the surviving one.

    Returns (ok, dead_dep_name). Caller must acquire _registry_lock.
    """
    deps = _PROCESS_DEPENDENCIES.get(process_name, [])
    for dep_name in deps:
        any_alive = False
        for entry in _process_registry:
            if entry['process_name'] == dep_name:
                h = entry['handler']
                if h.process and h.process.poll() is None:
                    any_alive = True
                    break
        if not any_alive:
            # Also covers "dependency not registered" (e.g., disabled)
            return False, dep_name
    return True, None


def _handle_restart(entry, logger):
    """Attempt to restart a dead process according to its restart policy."""
    handler = entry['handler']
    process_name = entry['process_name']
    key_type = entry['key_type']
    desc = f"{process_name} w/ {key_type}" if key_type else process_name

    exit_code = handler.process.returncode
    logger.warning(f"{desc} exited with code {exit_code}")

    policy = handler.restart_policy
    if policy is None:
        return

    # Check dependencies before consuming a restart attempt
    with _registry_lock:
        deps_ok, dead_dep = _check_dependencies_alive(process_name)
        if not deps_ok:
            # If the dependency has permanently died (every registered
            # instance exhausted its own restarts), mark this process as
            # dead too rather than deferring forever. An instance whose
            # restart_policy is None (intentionally stopped, e.g. mid
            # config-reload) deliberately does NOT count as exhausted —
            # it may come back, so keep deferring.
            dep_entries = [e for e in _process_registry
                           if e['process_name'] == dead_dep]
            dep_exhausted = bool(dep_entries) and all(
                e['handler'].restart_policy and
                e['handler']._restart_count >= e['handler'].restart_policy.max_restarts
                for e in dep_entries)
            if dep_exhausted:
                logger.error(f"{desc} cannot restart — dependency '{dead_dep}' is permanently dead")
                _on_restart_exhausted(desc, 0, policy.max_restarts)
                handler.restart_policy = None
                return
            logger.info(f"{desc} restart deferred — dependency '{dead_dep}' is not running")
            return

    now = time.time()

    # Reset restart count if outside the sliding window
    if handler._first_restart_time and (now - handler._first_restart_time) > policy.window_seconds:
        handler._restart_count = 0
        handler._first_restart_time = None
        handler._exhausted_notified = False

    if handler._restart_count >= policy.max_restarts:
        if not handler._exhausted_notified:
            logger.error(f"{desc} has exceeded max restarts ({policy.max_restarts}). Not restarting.")
            _on_restart_exhausted(desc, handler._restart_count, policy.max_restarts)
            handler._exhausted_notified = True
        return

    if handler._first_restart_time is None:
        handler._first_restart_time = now

    delay = _get_backoff_delay(policy, handler._restart_count)
    handler._restart_count += 1

    logger.info(f"Restarting {desc} in {delay}s (attempt {handler._restart_count}/{policy.max_restarts})...")

    # Wait for backoff delay, but check for shutdown
    if _monitor_stop_event.wait(delay):
        return  # Shutdown requested during backoff

    # Run the pre-restart hook BEFORE taking the registry lock — it may
    # sleep and fork for seconds (rclone stale-mount clearing), and holding
    # the lock through that stalls the status UI, service_registered(), and
    # every other registry consumer. Ordering is still correct: the process
    # is already dead here, so the corpse it left behind exists to clear.
    handler.run_pre_restart()

    # Re-check shutdown and restart_policy under lock to close TOCTOU gap.
    # restart_policy is set to None by stop_process() during config reload —
    # if reload already restarted this process, we must not start a duplicate.
    with _registry_lock:
        if _shutting_down:
            return
        if handler.restart_policy is None:
            return
        # Process was dead at collection time but may have been restarted
        # by config reload or restart_service during the backoff delay.
        # Never double-start.
        if handler.process and handler.process.poll() is None:
            return
        handler.restart_process(run_pre_restart=False, restore_policy=False)


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


def _monitor_loop(logger):
    """Poll registered processes and restart any that have died."""
    logger.info("Process monitor started")
    heartbeat.register(_HEARTBEAT_NAME, _HEARTBEAT_STALE_AFTER)
    while not _monitor_stop_event.is_set():
        heartbeat.beat(_HEARTBEAT_NAME)
        if not _shutting_down:
            with _registry_lock:
                entries_to_restart = []
                for entry in _process_registry:
                    handler = entry['handler']
                    if (handler.restart_policy and
                            handler.process and
                            handler.process.poll() is not None):
                        entries_to_restart.append(entry)

            # Restart outside the lock to avoid holding it during backoff
            for entry in entries_to_restart:
                if _shutting_down:
                    break
                heartbeat.beat(_HEARTBEAT_NAME)
                try:
                    _handle_restart(entry, logger)
                except Exception as e:
                    # restart_process() can raise (spawn failure, dead
                    # config).  A dead monitor means NO process ever
                    # auto-restarts again — log and keep the loop alive.
                    logger.error(
                        f"Process monitor: restart of "
                        f"{entry['process_name']} failed: {e}", exc_info=True
                    )

            _reap_orphans()

        _monitor_stop_event.wait(10)
    logger.info("Process monitor stopped")


def start_process_monitor(logger):
    """Start the background thread that monitors and restarts processes."""
    global _monitor_thread
    if _monitor_thread and _monitor_thread.is_alive():
        return
    _monitor_stop_event.clear()
    _monitor_thread = threading.Thread(target=_monitor_loop, args=(logger,), daemon=True)
    _monitor_thread.start()


def stop_process_monitor():
    """Signal the monitor thread to stop and wait for it."""
    global _monitor_thread
    _monitor_stop_event.set()
    if _monitor_thread and _monitor_thread.is_alive():
        _monitor_thread.join(timeout=15)
    _monitor_thread = None
    heartbeat.unregister(_HEARTBEAT_NAME)


def service_registered(service_name, key_type=None):
    """Return True if a process with this name (and optional key_type) is
    in the registry.  Lets callers verify a restart target exists BEFORE
    taking destructive preparation steps (e.g. the mount self-heal must
    not lazy-unmount a path it has no rclone process to remount)."""
    with _registry_lock:
        for entry in _process_registry:
            if entry['process_name'].lower() != service_name.lower():
                continue
            if key_type is not None and \
                    (entry['key_type'] or '').lower() != key_type.lower():
                continue
            return True
    return False


def restart_service(service_name, key_type=None):
    """Restart a specific service by name. For admin-triggered restarts.

    Terminates the process and immediately re-launches it, resetting the
    restart counter so no backoff delay is applied.

    Args:
        service_name: Process name to match (e.g., 'Zurg', 'rclone', 'plex_debrid')
        key_type: Optional key-type filter for names registered more than
            once (both rclone mounts register as 'rclone', distinguished by
            mount name — e.g. 'zurgarr' vs 'torbox').  None restarts EVERY
            registered instance of the name — the UI's "restart rclone"
            must reach both mounts, not just the first-registered one.

    Returns:
        True if at least one matching process was found and restarted.
    """
    from utils.logger import get_logger
    logger = get_logger()

    restarted_any = False
    with _registry_lock:
        for entry in _process_registry:
            name = entry['process_name']
            handler = entry['handler']
            key_type_entry = entry['key_type']
            if key_type is not None and \
                    (key_type_entry or '').lower() != key_type.lower():
                continue
            if name.lower() == service_name.lower():
                desc = f"{name} w/ {key_type_entry}" if key_type_entry else name

                # Terminate if running
                if handler.process and handler.process.poll() is None:
                    logger.info(f"[restart_service] Terminating {desc}")
                    if handler.subprocess_logger:
                        handler.subprocess_logger.stop_logging_stdout()
                        handler.subprocess_logger.stop_monitoring_stderr()
                        handler.subprocess_logger = None
                    handler.process.terminate()
                    try:
                        handler.process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        handler.process.kill()
                        handler.process.wait(timeout=5)

                # Reset restart counter for clean restart
                handler._restart_count = 0
                handler._first_restart_time = None
                handler._exhausted_notified = False

                # Re-launch
                handler.restart_process()
                logger.info(f"[restart_service] {desc} restarted successfully")
                restarted_any = True

    if not restarted_any:
        logger.warning(f"[restart_service] Process '{service_name}' not found in registry")
    return restarted_any


class ProcessHandler:
    def __init__(self, logger):
        self.logger = logger
        self.process = None
        self.subprocess_logger = None
        self.stdout = ""
        self.stderr = ""
        self.returncode = None
        # Restart support
        self.restart_policy = None
        self._restart_count = 0
        self._first_restart_time = None
        # Latch so restart-exhaustion notifies once, not on every 10s
        # monitor tick (finding #6). Cleared wherever _restart_count resets.
        self._exhausted_notified = False
        # Stored for restart_process()
        self._command = None
        self._config_dir = None
        self._process_name = None
        self._key_type = None
        self._suppress_logging = False
        # Optional callable invoked before every relaunch in
        # restart_process() — e.g. rclone clears a dead FUSE corpse left by
        # the crashed instance so the relaunch doesn't refuse with
        # "directory already mounted". A raising hook must not block the
        # relaunch; failures are logged and the restart proceeds.
        self.pre_restart = None

    _DEFAULT_RESTART = object()  # sentinel

    def start_process(self, process_name, config_dir, command, key_type=None,
                      suppress_logging=False, restart_policy=_DEFAULT_RESTART):
        if restart_policy is self._DEFAULT_RESTART:
            restart_policy = RestartPolicy()

        try:
            if key_type is not None:
                self.logger.info(f"Starting {process_name} w/ {key_type}")
                process_description = f"{process_name} w/ {key_type}"
            else:
                self.logger.info(f"Starting {process_name}")
                process_description = f"{process_name}"

            # Store for restart
            self._command = command
            self._config_dir = config_dir
            self._process_name = process_name
            self._key_type = key_type
            self._suppress_logging = suppress_logging
            self.restart_policy = restart_policy

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
            if not suppress_logging:
                self.subprocess_logger = SubprocessLogger(self.logger, f"{process_description}")
                self.subprocess_logger.start_logging_stdout(self.process)
                self.subprocess_logger.start_monitoring_stderr(self.process, key_type, process_name)
            register_process(self, process_name, key_type)
            return self.process
        except Exception as e:
            self.logger.error(f"Error running subprocess for {process_description}: {e}")
            return None

    def run_pre_restart(self):
        """Invoke the pre_restart hook, if any. Never raises.

        Separated from restart_process() so callers that hold
        ``_registry_lock`` across the relaunch (``_handle_restart``) can run
        the hook — which may sleep and fork for seconds — BEFORE taking the
        lock, then relaunch with ``run_pre_restart=False``.
        """
        if self.pre_restart is None:
            return
        try:
            self.pre_restart()
        except Exception as e:
            desc = f"{self._process_name} w/ {self._key_type}" if self._key_type else self._process_name
            self.logger.error(f"pre-restart hook for {desc} failed: {e}")

    def restart_process(self, run_pre_restart=True, restore_policy=True):
        """Stop logging threads and re-launch the process with the same parameters.

        Args:
            run_pre_restart: Whether to run the pre_restart hook (default True).
            restore_policy: Whether to restore restart_policy if it was cleared
                by stop_process() (default True). When False (e.g., from monitor),
                preserves the current policy and restart counters.
        """
        if self._command is None:
            self.logger.error("Cannot restart: no command recorded from initial start")
            return

        desc = f"{self._process_name} w/ {self._key_type}" if self._key_type else self._process_name

        # Clean up old subprocess logger
        if self.subprocess_logger:
            self.subprocess_logger.stop_logging_stdout()
            self.subprocess_logger.stop_monitoring_stderr()
            self.subprocess_logger = None

        if run_pre_restart:
            self.run_pre_restart()

        # Re-arm supervision just before spawn — after potentially slow hooks
        # complete but before the spawn attempt. This delays arming past slow
        # pre_restart hooks (e.g., rclone stale-mount clearing) so a concurrent
        # monitor tick can't see armed policy + hook delay and queue a duplicate.
        # stop_process() clears restart_policy to suppress the monitor during an
        # intentional stop; a relaunch must re-arm supervision or the process is
        # unmonitored until container restart (finding #4).
        if restore_policy and self.restart_policy is None:
            self.restart_policy = RestartPolicy()
            self._restart_count = 0
            self._first_restart_time = None
            self._exhausted_notified = False

        try:
            self.logger.info(f"Restarting {desc}")
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
            if not self._suppress_logging:
                self.subprocess_logger = SubprocessLogger(self.logger, desc)
                self.subprocess_logger.start_logging_stdout(self.process)
                self.subprocess_logger.start_monitoring_stderr(self.process, self._key_type, self._process_name)
            self.logger.info(f"{desc} restarted (pid {self.process.pid})")
        except Exception as e:
            self.logger.error(f"Failed to restart {desc}: {e}")

    def wait(self):
        if self.process:
            self.stdout, self.stderr = self.process.communicate()
            self.returncode = self.process.returncode
            self.stdout = self.stdout.strip() if self.stdout else ""
            self.stderr = self.stderr.strip() if self.stderr else ""
            if self.subprocess_logger:
                self.subprocess_logger.stop_logging_stdout()
                self.subprocess_logger.stop_monitoring_stderr()

    def stop_process(self, process_name, key_type=None):
        # Disable auto-restart for intentional stops (e.g., during updates)
        self.restart_policy = None
        try:
            if key_type:
                self.logger.info(f"Stopping {process_name} w/ {key_type}")
                process_description = f"{process_name} w/ {key_type}"
            else:
                self.logger.info(f"Stopping {process_name}")
                process_description = f"{process_name}"
            if self.process:
                self.process.kill()
                if self.subprocess_logger:
                    self.subprocess_logger.stop_logging_stdout()
                    self.subprocess_logger.stop_monitoring_stderr()
        except Exception as e:
            self.logger.error(f"Error stopping subprocess for {process_description}: {e}")
