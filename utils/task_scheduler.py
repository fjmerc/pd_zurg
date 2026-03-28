"""Centralized task scheduler for periodic operations.

Provides a single place to register, schedule, and monitor all periodic
tasks (library scan, queue cleanup, routing audit, etc.) with status
tracking exposed via API for the WebUI Tasks tab.
"""

import threading
import time
from datetime import datetime, timezone
from utils.logger import get_logger

logger = get_logger()


class ScheduledTask:
    """A single registered task with scheduling metadata and status tracking."""

    __slots__ = (
        'name', 'func', 'interval', 'enabled', 'description',
        'last_run', 'last_duration', 'last_result', 'next_run',
        'running', '_lock',
    )

    def __init__(self, name, func, interval, enabled=True, description=''):
        self.name = name
        self.func = func
        self.interval = interval          # seconds
        self.enabled = enabled
        self.description = description
        self.last_run = None              # ISO timestamp string
        self.last_duration = None         # seconds (float)
        self.last_result = None           # {'status': 'success'|'error', 'message': ..., 'items': N}
        self.next_run = None              # epoch float
        self.running = False
        self._lock = threading.Lock()

    def to_dict(self):
        with self._lock:
            next_run_iso = None
            if self.next_run:
                try:
                    next_run_iso = datetime.fromtimestamp(
                        self.next_run, tz=timezone.utc
                    ).isoformat(timespec='seconds')
                except (OSError, ValueError):
                    pass
            return {
                'name': self.name,
                'description': self.description,
                'interval': self.interval,
                'enabled': self.enabled,
                'running': self.running,
                'last_run': self.last_run,
                'last_duration': self.last_duration,
                'last_result': self.last_result,
                'next_run': next_run_iso,
            }


class TaskScheduler:
    """Central scheduler for all periodic tasks.

    Usage::

        scheduler = TaskScheduler()
        scheduler.register('cleanup', my_cleanup_func, interval_seconds=3600)
        scheduler.start()
        # ...
        scheduler.stop()
    """

    def __init__(self):
        self._tasks = {}          # name -> ScheduledTask
        self._stop_event = threading.Event()
        self._thread = None

    def register(self, name, func, interval_seconds, enabled=True,
                 description='', initial_delay=None):
        """Register a task for periodic execution.

        Args:
            name: Unique task name (used in API and logs).
            func: Callable to invoke. May return a dict with 'status',
                  'message', and/or 'items' keys for result tracking.
            interval_seconds: Time between runs.
            enabled: Whether the task runs on schedule (can still be
                     triggered manually when disabled).
            description: Human-readable description for the WebUI.
            initial_delay: Seconds to wait before first run. Defaults to
                          the task interval (i.e. first run after one full
                          interval). Set to 0 for immediate first run.
        """
        task = ScheduledTask(name, func, interval_seconds, enabled, description)
        if initial_delay is None:
            initial_delay = interval_seconds
        task.next_run = time.time() + initial_delay
        self._tasks[name] = task
        logger.debug(
            f"[scheduler] Registered task '{name}' "
            f"(interval={interval_seconds}s, enabled={enabled})"
        )

    def run_now(self, name):
        """Trigger immediate execution of a task in a background thread.

        Returns True if the task was found and queued, False otherwise.
        """
        task = self._tasks.get(name)
        if not task:
            return False
        with task._lock:
            if task.running:
                return False
            task.running = True
        threading.Thread(
            target=self._execute_task, args=(task,),
            daemon=True, name=f'task-{name}'
        ).start()
        return True

    def get_status(self):
        """Return status of all tasks as a list of dicts (for API)."""
        return [task.to_dict() for task in self._tasks.values()]

    def get_task(self, name):
        """Return a single task's status dict, or None."""
        task = self._tasks.get(name)
        return task.to_dict() if task else None

    def start(self):
        """Start the scheduler loop in a background thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._scheduler_loop, daemon=True, name='task-scheduler'
        )
        self._thread.start()
        logger.info(f"[scheduler] Started with {len(self._tasks)} tasks")

    def stop(self):
        """Signal the scheduler to stop and wait for it."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=15)
        self._thread = None
        logger.info("[scheduler] Stopped")

    def _scheduler_loop(self):
        """Main loop: check for due tasks every 10 seconds."""
        while not self._stop_event.is_set():
            now = time.time()
            for task in list(self._tasks.values()):
                if not task.enabled or task.next_run is None or now < task.next_run:
                    continue
                with task._lock:
                    if task.running:
                        continue
                    task.running = True
                threading.Thread(
                    target=self._execute_task, args=(task,),
                    daemon=True, name=f'task-{task.name}'
                ).start()
            self._stop_event.wait(10)

    def _execute_task(self, task):
        """Run a single task, tracking timing and result.

        Caller must set task.running = True under task._lock before
        spawning this method in a thread.
        """
        start = time.time()
        task_name = task.name
        try:
            logger.info(f"[scheduler] Running task '{task_name}'")
            result = task.func()
            duration = time.time() - start

            if isinstance(result, dict):
                if 'status' not in result:
                    result['status'] = 'success'
                last_result = result
            else:
                last_result = {'status': 'success'}

            logger.info(
                f"[scheduler] Task '{task_name}' completed in {duration:.1f}s"
            )
        except Exception as e:
            duration = time.time() - start
            last_result = {'status': 'error', 'message': str(e)}
            logger.error(
                f"[scheduler] Task '{task_name}' failed after {duration:.1f}s: {e}"
            )

        # Fire status event before clearing running flag so the WebUI
        # never sees running=False with stale last_result
        try:
            from utils.status_server import status_data
            status = last_result.get('status', 'unknown')
            msg = last_result.get('message', '')
            items = last_result.get('items')
            detail = f" — {msg}" if msg else ''
            detail += f" ({items} items)" if items is not None else ''
            level = 'error' if status == 'error' else 'info'
            status_data.add_event('scheduler', f"Task '{task_name}' {status}{detail}", level=level)
        except Exception:
            pass

        # Publish results and clear running flag atomically
        with task._lock:
            task.last_run = datetime.now(timezone.utc).isoformat(timespec='seconds')
            task.last_duration = round(duration, 2)
            task.last_result = last_result
            task.next_run = time.time() + task.interval
            task.running = False


# Module-level singleton
scheduler = TaskScheduler()
