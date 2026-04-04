"""Prometheus metrics exposition for pd_zurg.

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

        # Up gauge
        lines.append('# HELP pd_zurg_up Whether pd_zurg is running')
        lines.append('# TYPE pd_zurg_up gauge')
        lines.append('pd_zurg_up 1')
        lines.append('')

        # Uptime
        lines.append('# HELP pd_zurg_uptime_seconds Seconds since pd_zurg started')
        lines.append('# TYPE pd_zurg_uptime_seconds gauge')
        lines.append(f'pd_zurg_uptime_seconds {data["uptime_seconds"]}')
        lines.append('')

        # Process status
        procs = data.get('processes', [])
        if procs:
            lines.append('# HELP pd_zurg_process_running Whether a managed process is running')
            lines.append('# TYPE pd_zurg_process_running gauge')
            for proc in procs:
                name = _sanitize_label(proc.get('name', 'unknown'))
                running = 1 if proc.get('running') else 0
                lines.append(f'pd_zurg_process_running{{name="{name}"}} {running}')
            lines.append('')

            lines.append('# HELP pd_zurg_process_restart_total Total restart count per process')
            lines.append('# TYPE pd_zurg_process_restart_total counter')
            for proc in procs:
                name = _sanitize_label(proc.get('name', 'unknown'))
                restarts = proc.get('restart_count', 0)
                lines.append(f'pd_zurg_process_restart_total{{name="{name}"}} {restarts}')
            lines.append('')

        # Mount status
        mounts = data.get('mounts', [])
        if mounts:
            lines.append('# HELP pd_zurg_mount_mounted Whether a mount point is mounted')
            lines.append('# TYPE pd_zurg_mount_mounted gauge')
            for mount in mounts:
                path = _sanitize_label(mount.get('path', ''))
                mounted = 1 if mount.get('mounted') else 0
                lines.append(f'pd_zurg_mount_mounted{{path="{path}"}} {mounted}')
            lines.append('')

            lines.append('# HELP pd_zurg_mount_accessible Whether a mount point is readable')
            lines.append('# TYPE pd_zurg_mount_accessible gauge')
            for mount in mounts:
                path = _sanitize_label(mount.get('path', ''))
                accessible = 1 if mount.get('accessible') else 0
                lines.append(f'pd_zurg_mount_accessible{{path="{path}"}} {accessible}')
            lines.append('')

        # Blackhole counters
        with self._lock:
            bh_counters = self._counters.get('blackhole_processed', {})
        if bh_counters:
            lines.append('# HELP pd_zurg_blackhole_processed_total Torrent files processed by blackhole')
            lines.append('# TYPE pd_zurg_blackhole_processed_total counter')
            for label_key, val in bh_counters.items():
                labels_str = _format_labels(label_key)
                lines.append(f'pd_zurg_blackhole_processed_total{{{labels_str}}} {val}')
            lines.append('')

        # Blackhole retry counter
        retry_val = self.get_counter('blackhole_retry')
        if retry_val:
            lines.append('# HELP pd_zurg_blackhole_retry_total Total retry attempts for failed files')
            lines.append('# TYPE pd_zurg_blackhole_retry_total counter')
            lines.append(f'pd_zurg_blackhole_retry_total {retry_val}')
            lines.append('')

        # Event counters
        lines.append('# HELP pd_zurg_events_total Total events by level')
        lines.append('# TYPE pd_zurg_events_total counter')
        for level in ('info', 'warning', 'error'):
            val = self.get_counter('events', {'level': level})
            lines.append(f'pd_zurg_events_total{{level="{level}"}} {val}')
        lines.append('')

        # System stats
        system = data.get('system', {})
        if 'memory_percent' in system:
            lines.append('# HELP pd_zurg_memory_usage_percent Container memory usage percentage')
            lines.append('# TYPE pd_zurg_memory_usage_percent gauge')
            lines.append(f'pd_zurg_memory_usage_percent {system["memory_percent"]}')
            lines.append('')

        if 'memory_used_bytes' in system:
            lines.append('# HELP pd_zurg_memory_used_bytes Container memory used in bytes')
            lines.append('# TYPE pd_zurg_memory_used_bytes gauge')
            lines.append(f'pd_zurg_memory_used_bytes {system["memory_used_bytes"]}')
            lines.append('')

        if 'cpu_percent' in system:
            lines.append('# HELP pd_zurg_cpu_usage_percent Container CPU usage percentage')
            lines.append('# TYPE pd_zurg_cpu_usage_percent gauge')
            lines.append(f'pd_zurg_cpu_usage_percent {system["cpu_percent"]}')
            lines.append('')

        if 'disk_used_bytes' in system:
            lines.append('# HELP pd_zurg_disk_used_bytes Config volume disk used in bytes')
            lines.append('# TYPE pd_zurg_disk_used_bytes gauge')
            lines.append(f'pd_zurg_disk_used_bytes {system["disk_used_bytes"]}')
            lines.append('')

        if 'disk_total_bytes' in system:
            lines.append('# HELP pd_zurg_disk_total_bytes Config volume disk total in bytes')
            lines.append('# TYPE pd_zurg_disk_total_bytes gauge')
            lines.append(f'pd_zurg_disk_total_bytes {system["disk_total_bytes"]}')
            lines.append('')

        if 'disk_percent' in system:
            lines.append('# HELP pd_zurg_disk_usage_percent Config volume disk usage percentage')
            lines.append('# TYPE pd_zurg_disk_usage_percent gauge')
            lines.append(f'pd_zurg_disk_usage_percent {system["disk_percent"]}')
            lines.append('')

        if 'fd_open' in system:
            lines.append('# HELP pd_zurg_fd_open Current number of open file descriptors')
            lines.append('# TYPE pd_zurg_fd_open gauge')
            lines.append(f'pd_zurg_fd_open {system["fd_open"]}')
            lines.append('')

        if 'fd_max' in system:
            lines.append('# HELP pd_zurg_fd_max Maximum file descriptor limit (soft)')
            lines.append('# TYPE pd_zurg_fd_max gauge')
            lines.append(f'pd_zurg_fd_max {system["fd_max"]}')
            lines.append('')

        if 'net_rx_bytes' in system:
            lines.append('# HELP pd_zurg_net_rx_bytes_total Total network bytes received')
            lines.append('# TYPE pd_zurg_net_rx_bytes_total counter')
            lines.append(f'pd_zurg_net_rx_bytes_total {system["net_rx_bytes"]}')
            lines.append('')

        if 'net_tx_bytes' in system:
            lines.append('# HELP pd_zurg_net_tx_bytes_total Total network bytes transmitted')
            lines.append('# TYPE pd_zurg_net_tx_bytes_total counter')
            lines.append(f'pd_zurg_net_tx_bytes_total {system["net_tx_bytes"]}')
            lines.append('')

        # Service health
        services = data.get('services', [])
        if services:
            lines.append('# HELP pd_zurg_service_up Whether an external service is reachable')
            lines.append('# TYPE pd_zurg_service_up gauge')
            for svc in services:
                name = _sanitize_label(svc.get('name', 'unknown'))
                stype = _sanitize_label(svc.get('type', 'unknown'))
                up = 1 if svc.get('status') == 'ok' else 0
                lines.append(f'pd_zurg_service_up{{name="{name}",type="{stype}"}} {up}')
            lines.append('')

        return '\n'.join(lines) + '\n'


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
