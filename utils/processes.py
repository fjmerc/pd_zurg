from base import *
from utils.logger import SubprocessLogger


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

# Global registry of all tracked processes for graceful shutdown
_process_registry = []
_registry_lock = threading.Lock()
_shutting_down = False
_monitor_stop_event = threading.Event()
_monitor_thread = None


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

    now = time.time()

    # Reset restart count if outside the sliding window
    if handler._first_restart_time and (now - handler._first_restart_time) > policy.window_seconds:
        handler._restart_count = 0
        handler._first_restart_time = None

    if handler._restart_count >= policy.max_restarts:
        logger.error(f"{desc} has exceeded max restarts ({policy.max_restarts}). Not restarting.")
        _on_restart_exhausted(desc, handler._restart_count, policy.max_restarts)
        return

    if handler._first_restart_time is None:
        handler._first_restart_time = now

    delay = _get_backoff_delay(policy, handler._restart_count)
    handler._restart_count += 1

    logger.info(f"Restarting {desc} in {delay}s (attempt {handler._restart_count}/{policy.max_restarts})...")

    # Wait for backoff delay, but check for shutdown
    if _monitor_stop_event.wait(delay):
        return  # Shutdown requested during backoff

    # Re-check shutdown under lock to close TOCTOU gap
    with _registry_lock:
        if _shutting_down:
            return
        handler.restart_process()


def _monitor_loop(logger):
    """Poll registered processes and restart any that have died."""
    logger.info("Process monitor started")
    while not _monitor_stop_event.is_set():
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
                _handle_restart(entry, logger)

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


def restart_service(service_name):
    """Restart a specific service by name. For admin-triggered restarts.

    Terminates the process and immediately re-launches it, resetting the
    restart counter so no backoff delay is applied.

    Args:
        service_name: Process name to match (e.g., 'Zurg', 'rclone', 'plex_debrid')

    Returns:
        True if process was found and restarted, False otherwise.
    """
    from utils.logger import get_logger
    logger = get_logger()

    with _registry_lock:
        for entry in _process_registry:
            name = entry['process_name']
            handler = entry['handler']
            key_type = entry['key_type']
            if name.lower() == service_name.lower():
                desc = f"{name} w/ {key_type}" if key_type else name

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

                # Re-launch
                handler.restart_process()
                logger.info(f"[restart_service] {desc} restarted successfully")
                return True

    logger.warning(f"[restart_service] Process '{service_name}' not found in registry")
    return False


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
        # Stored for restart_process()
        self._command = None
        self._config_dir = None
        self._process_name = None
        self._key_type = None
        self._suppress_logging = False

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

            self.process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
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

    def restart_process(self):
        """Stop logging threads and re-launch the process with the same parameters."""
        if self._command is None:
            self.logger.error("Cannot restart: no command recorded from initial start")
            return

        desc = f"{self._process_name} w/ {self._key_type}" if self._key_type else self._process_name

        # Clean up old subprocess logger
        if self.subprocess_logger:
            self.subprocess_logger.stop_logging_stdout()
            self.subprocess_logger.stop_monitoring_stderr()
            self.subprocess_logger = None

        try:
            self.logger.info(f"Restarting {desc}")
            self.process = subprocess.Popen(
                self._command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
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
