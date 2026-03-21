"""Monitor and recover stuck ffprobe processes.

When Plex/Jellyfin scan media on rclone/debrid mounts, ffprobe can hang
indefinitely in 'D' (uninterruptible sleep) state when I/O never completes
(debrid link expired, network hiccup). This monitor detects stuck ffprobe
processes and attempts recovery.

Recovery strategy (from DUMB):
1. Detect ffprobe in 'D' state for longer than the stuck threshold
2. "Poke" the process by running a new ffprobe on the same file,
   which generates I/O that can unstick the blocked kernel read
3. If still stuck after max poke attempts, kill the process
"""

import os
import signal
import subprocess
import threading
import time
from utils.logger import get_logger

logger = get_logger()


class FfprobeMonitor:
    def __init__(self, stuck_timeout=300, poll_interval=30, max_poke_attempts=3, poke_cooldown=60):
        self.stuck_timeout = stuck_timeout
        self.poll_interval = poll_interval
        self.max_poke_attempts = max_poke_attempts
        self.poke_cooldown = poke_cooldown
        self._stop_event = threading.Event()
        # Track when we first saw each PID in 'D' state: {pid: first_seen_time}
        self._stuck_since = {}
        # Track poke attempts per PID: {pid: (poke_count, last_poke_time)}
        self._poke_state = {}

    def _get_process_state(self, pid):
        """Read process state from /proc/PID/stat. Returns state char or None."""
        try:
            with open(f'/proc/{pid}/stat', 'r') as f:
                stat_line = f.read()
            # Format: pid (comm) state ... — state is after the closing paren
            close_paren = stat_line.rfind(')')
            if close_paren == -1:
                return None
            fields = stat_line[close_paren + 2:].split()
            return fields[0] if fields else None
        except (FileNotFoundError, PermissionError, IndexError):
            return None

    def _get_cmdline(self, pid):
        """Read command line from /proc/PID/cmdline."""
        try:
            with open(f'/proc/{pid}/cmdline', 'rb') as f:
                cmdline = f.read().decode('utf-8', errors='replace')
            return cmdline.split('\x00')
        except (FileNotFoundError, PermissionError):
            return []

    def _find_ffprobe_pids(self):
        """Find all ffprobe process PIDs."""
        pids = []
        try:
            for entry in os.listdir('/proc'):
                if not entry.isdigit():
                    continue
                pid = int(entry)
                cmdline = self._get_cmdline(pid)
                if cmdline and any('ffprobe' in arg for arg in cmdline[:2]):
                    pids.append((pid, cmdline))
        except (FileNotFoundError, PermissionError):
            pass
        return pids

    def _extract_file_path(self, cmdline):
        """Extract the media file path from ffprobe command line."""
        # ffprobe typically has the file as the last argument
        # Skip flags (starting with -)
        for arg in reversed(cmdline):
            if arg and not arg.startswith('-') and 'ffprobe' not in arg:
                return arg
        return None

    def _poke_process(self, pid, file_path):
        """Run a quick ffprobe on the same file to generate I/O."""
        logger.info(f"[ffprobe_monitor] Poking stuck ffprobe (pid {pid}) by probing: {file_path}")
        try:
            subprocess.run(
                ['ffprobe', '-v', 'quiet', '-select_streams', 'v:0',
                 '-show_entries', 'format=duration', file_path],
                timeout=10,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        except subprocess.TimeoutExpired:
            logger.debug(f"[ffprobe_monitor] Poke ffprobe also timed out for pid {pid}")
        except FileNotFoundError:
            logger.debug("[ffprobe_monitor] ffprobe binary not found, cannot poke")
        except Exception as e:
            logger.debug(f"[ffprobe_monitor] Poke failed for pid {pid}: {e}")

    def _check_and_recover(self):
        """Scan for stuck ffprobe processes and attempt recovery."""
        now = time.time()
        ffprobe_pids = self._find_ffprobe_pids()
        active_pids = set()

        for pid, cmdline in ffprobe_pids:
            active_pids.add(pid)
            state = self._get_process_state(pid)

            if state != 'D':
                # Process is not stuck, clear tracking
                self._stuck_since.pop(pid, None)
                self._poke_state.pop(pid, None)
                continue

            # Process is in 'D' state
            if pid not in self._stuck_since:
                self._stuck_since[pid] = now
                continue

            stuck_duration = now - self._stuck_since[pid]
            if stuck_duration < self.stuck_timeout:
                continue

            # Process has been stuck longer than threshold
            file_path = self._extract_file_path(cmdline)
            poke_count, last_poke = self._poke_state.get(pid, (0, 0))

            if poke_count >= self.max_poke_attempts:
                # Exceeded poke attempts — kill it
                logger.warning(
                    f"[ffprobe_monitor] ffprobe pid {pid} stuck for {stuck_duration:.0f}s, "
                    f"exceeded {self.max_poke_attempts} poke attempts. Killing."
                )
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self._stuck_since.pop(pid, None)
                self._poke_state.pop(pid, None)
                continue

            if now - last_poke < self.poke_cooldown:
                continue  # Wait for cooldown

            if file_path:
                self._poke_process(pid, file_path)
                self._poke_state[pid] = (poke_count + 1, now)
            else:
                logger.warning(
                    f"[ffprobe_monitor] Cannot determine file for stuck ffprobe pid {pid}, killing"
                )
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self._stuck_since.pop(pid, None)
                self._poke_state.pop(pid, None)

        # Clean up tracking for processes that no longer exist
        for pid in list(self._stuck_since):
            if pid not in active_pids:
                self._stuck_since.pop(pid, None)
                self._poke_state.pop(pid, None)

    def run(self):
        """Main monitor loop."""
        logger.info(
            f"[ffprobe_monitor] Started (stuck_timeout={self.stuck_timeout}s, "
            f"poll={self.poll_interval}s, max_pokes={self.max_poke_attempts})"
        )
        while not self._stop_event.is_set():
            try:
                self._check_and_recover()
            except Exception as e:
                logger.error(f"[ffprobe_monitor] Error in monitor loop: {e}")
            self._stop_event.wait(self.poll_interval)
        logger.info("[ffprobe_monitor] Stopped")

    def stop(self):
        self._stop_event.set()


def setup():
    """Initialize and start the ffprobe monitor if enabled."""
    enabled = os.environ.get('FFPROBE_MONITOR_ENABLED', 'true').lower() == 'true'
    if not enabled:
        return None

    stuck_timeout = int(os.environ.get('FFPROBE_STUCK_TIMEOUT', '300'))
    poll_interval = int(os.environ.get('FFPROBE_POLL_INTERVAL', '30'))

    monitor = FfprobeMonitor(stuck_timeout=stuck_timeout, poll_interval=poll_interval)
    thread = threading.Thread(target=monitor.run, daemon=True)
    thread.start()
    return monitor
