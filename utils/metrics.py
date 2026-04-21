"""Prometheus metrics exposition for Zurgarr.

Generates metrics in Prometheus text exposition format from
the existing StatusData singleton. No external dependencies.

During the 2.19.0 → 2.20.0 deprecation window every metric is exported
under BOTH the legacy ``pd_zurg_*`` prefix and the new ``zurgarr_*``
prefix so existing Grafana dashboards keep working while users migrate
their queries. The legacy prefix's ``# HELP`` line carries a
``DEPRECATED`` marker pointing at the new name and the removal version.
Sample values and labels are byte-identical between prefixes — only the
metric name changes. 2.20.0 drops the ``pd_zurg_*`` side of every pair.
"""

import threading
import time


_LEGACY_PREFIX = 'pd_zurg'
_NEW_PREFIX = 'zurgarr'
_DEPRECATED_SINCE = '2.19.0'
_REMOVED_IN = '2.20.0'


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
    """Emit a metric under both ``pd_zurg_*`` and ``zurgarr_*`` prefixes.

    During the 2.19.0 → 2.20.0 deprecation window every metric is
    exported twice so existing Grafana dashboards keyed on the legacy
    prefix keep working while users migrate queries to the new prefix.
    The legacy ``# HELP`` line carries a DEPRECATED marker naming the
    new metric and the removal version; sample values and labels are
    byte-identical between the two prefixes.

    ``samples`` is an iterable of ``(labels_str, value)`` pairs where
    ``labels_str`` is the already-formatted ``k="v",k2="v2"`` payload
    (empty string for metrics without labels). The iterable is
    materialised to a list on entry so callers can safely pass a
    generator without the second prefix's iteration silently reading
    an exhausted source.

    When ``labels_str`` is empty the sample renders as ``metric value``
    (no braces), matching the conventional Prometheus exporter format
    for unlabelled counters/gauges. Production call sites that go
    through this helper with no labels (``up``, ``uptime_seconds``,
    system gauges) always passed labelless strings in the pre-2.19
    output too, so the legacy prefix's bytes are preserved for those
    metrics. The hypothetical labelled-counter-called-with-empty-labels
    path (reachable only via ``m.inc('blackhole_processed')`` with no
    labels dict, which no production call site does) did previously
    render as ``metric{} value`` and now renders as ``metric value``;
    Prometheus treats the two as equivalent.
    """
    samples = list(samples)
    legacy_help_suffix = (
        f' (DEPRECATED since {_DEPRECATED_SINCE} — use {_NEW_PREFIX}_{name}; '
        f'removed in {_REMOVED_IN})'
    )
    for prefix, suffix in ((_LEGACY_PREFIX, legacy_help_suffix), (_NEW_PREFIX, '')):
        full = f'{prefix}_{name}'
        lines.append(f'# HELP {full} {help_text}{suffix}')
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
