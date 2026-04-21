"""Prometheus metrics exposition for Zurgarr.

Generates metrics in Prometheus text exposition format from
the existing StatusData singleton. No external dependencies.
"""

import threading
import time


class MetricsRegistry:
    """Collects counters and formats Prometheus metrics."""

    def __init__(self):
        self._counters = {}  # name -> {labels_tuple: value}
        self._lock = threading.Lock()

    def inc(self, name, labels=None, value=1):
        """Increment a counter."""
        key = tuple(sorted((labels or {}).items()))
        with self._lock:
            if name not in self._counters:
                self._counters[name] = {}
            self._counters[name][key] = self._counters[name].get(key, 0) + value

    def get_counter(self, name, labels=None):
        """Get current counter value."""
        key = tuple(sorted((labels or {}).items()))
        with self._lock:
            return self._counters.get(name, {}).get(key, 0)

    def format_metrics(self):
        """Generate Prometheus exposition format string."""
        from utils.status_server import status_data

        lines = []
        data = status_data.to_dict()

        _emit(lines, 'up', 'Whether Zurgarr is running', 'gauge',
              [('', 1)])

        _emit(lines, 'uptime_seconds', 'Seconds since Zurgarr started', 'gauge',
              [('', data['uptime_seconds'])])

        procs = data.get('processes', [])
        if procs:
            samples = [
                (f'name="{_sanitize_label(p.get("name", "unknown"))}"',
                 1 if p.get('running') else 0)
                for p in procs
            ]
            _emit(lines, 'process_running',
                  'Whether a managed process is running', 'gauge', samples)

            samples = [
                (f'name="{_sanitize_label(p.get("name", "unknown"))}"',
                 p.get('restart_count', 0))
                for p in procs
            ]
            _emit(lines, 'process_restart_total',
                  'Total restart count per process', 'counter', samples)

        mounts = data.get('mounts', [])
        if mounts:
            samples = [
                (f'path="{_sanitize_label(m.get("path", ""))}"',
                 1 if m.get('mounted') else 0)
                for m in mounts
            ]
            _emit(lines, 'mount_mounted',
                  'Whether a mount point is mounted', 'gauge', samples)

            samples = [
                (f'path="{_sanitize_label(m.get("path", ""))}"',
                 1 if m.get('accessible') else 0)
                for m in mounts
            ]
            _emit(lines, 'mount_accessible',
                  'Whether a mount point is readable', 'gauge', samples)

        with self._lock:
            bh_counters = dict(self._counters.get('blackhole_processed', {}))
        if bh_counters:
            samples = [
                (_format_labels(label_key), val)
                for label_key, val in bh_counters.items()
            ]
            _emit(lines, 'blackhole_processed_total',
                  'Torrent files processed by blackhole', 'counter', samples)

        retry_val = self.get_counter('blackhole_retry')
        if retry_val:
            _emit(lines, 'blackhole_retry_total',
                  'Total retry attempts for failed files', 'counter',
                  [('', retry_val)])

        event_samples = [
            (f'level="{level}"', self.get_counter('events', {'level': level}))
            for level in ('info', 'warning', 'error')
        ]
        _emit(lines, 'events_total', 'Total events by level', 'counter',
              event_samples)

        system = data.get('system', {})
        if 'memory_percent' in system:
            _emit(lines, 'memory_usage_percent',
                  'Container memory usage percentage', 'gauge',
                  [('', system['memory_percent'])])
        if 'memory_used_bytes' in system:
            _emit(lines, 'memory_used_bytes',
                  'Container memory used in bytes', 'gauge',
                  [('', system['memory_used_bytes'])])
        if 'cpu_percent' in system:
            _emit(lines, 'cpu_usage_percent',
                  'Container CPU usage percentage', 'gauge',
                  [('', system['cpu_percent'])])
        if 'disk_used_bytes' in system:
            _emit(lines, 'disk_used_bytes',
                  'Config volume disk used in bytes', 'gauge',
                  [('', system['disk_used_bytes'])])
        if 'disk_total_bytes' in system:
            _emit(lines, 'disk_total_bytes',
                  'Config volume disk total in bytes', 'gauge',
                  [('', system['disk_total_bytes'])])
        if 'disk_percent' in system:
            _emit(lines, 'disk_usage_percent',
                  'Config volume disk usage percentage', 'gauge',
                  [('', system['disk_percent'])])
        if 'fd_open' in system:
            _emit(lines, 'fd_open',
                  'Current number of open file descriptors', 'gauge',
                  [('', system['fd_open'])])
        if 'fd_max' in system:
            _emit(lines, 'fd_max',
                  'Maximum file descriptor limit (soft)', 'gauge',
                  [('', system['fd_max'])])
        if 'net_rx_bytes' in system:
            _emit(lines, 'net_rx_bytes_total',
                  'Total network bytes received', 'counter',
                  [('', system['net_rx_bytes'])])
        if 'net_tx_bytes' in system:
            _emit(lines, 'net_tx_bytes_total',
                  'Total network bytes transmitted', 'counter',
                  [('', system['net_tx_bytes'])])

        services = data.get('services', [])
        if services:
            samples = [
                (f'name="{_sanitize_label(s.get("name", "unknown"))}",'
                 f'type="{_sanitize_label(s.get("type", "unknown"))}"',
                 1 if s.get('status') == 'ok' else 0)
                for s in services
            ]
            _emit(lines, 'service_up',
                  'Whether an external service is reachable', 'gauge', samples)

        return '\n'.join(lines) + '\n'


def _emit(lines, name, help_text, metric_type, samples):
    """Emit a metric under the ``zurgarr_*`` prefix.

    ``samples`` is an iterable of ``(labels_str, value)`` pairs where
    ``labels_str`` is the already-formatted ``k="v",k2="v2"`` payload
    (empty string for metrics without labels). When ``labels_str`` is
    empty the sample renders as ``metric value`` (no braces), matching
    the conventional Prometheus exporter format for unlabelled
    counters/gauges.
    """
    full = f'zurgarr_{name}'
    lines.append(f'# HELP {full} {help_text}')
    lines.append(f'# TYPE {full} {metric_type}')
    for labels_str, value in samples:
        if labels_str:
            lines.append(f'{full}{{{labels_str}}} {value}')
        else:
            lines.append(f'{full} {value}')
    lines.append('')


def _sanitize_label(value):
    """Escape label values for Prometheus format."""
    return str(value).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')


def _format_labels(label_tuple):
    """Convert a labels tuple back to Prometheus label format."""
    parts = []
    for k, v in label_tuple:
        parts.append(f'{k}="{_sanitize_label(v)}"')
    return ','.join(parts)


# Module-level singleton
metrics = MetricsRegistry()
