"""Lightweight status web UI and JSON API.

Provides an at-a-glance dashboard showing service connectivity, process health,
mount status, system resources (cgroup-aware), and recent events. Uses Python's
built-in http.server — no framework dependencies.
"""

import base64
import collections
import glob as glob_mod
import gzip as gzip_mod
import hashlib
import hmac
import http.server
import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote as url_unquote
from utils.api_metrics import api_metrics as _api_metrics
from utils.logger import get_logger

logger = get_logger()


# ---------------------------------------------------------------------------
# Gzip compression cache (content hash → compressed bytes)
# ---------------------------------------------------------------------------

_gzip_cache = {}
_gzip_cache_lock = threading.Lock()
_GZIP_CACHE_MAX = 10
_GZIP_MIN_SIZE = 256  # Don't bother compressing tiny responses


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_cgroup_file(path):
    """Read a cgroup v2 file, return contents or None."""
    try:
        with open(path, 'r') as f:
            return f.read().strip()
    except (FileNotFoundError, PermissionError):
        return None


def _get_secret_or_env(secret_name, env_name=None):
    """Read from Docker secret file, fall back to environment variable."""
    try:
        with open(f'/run/secrets/{secret_name}', 'r') as f:
            return f.read().strip()
    except (FileNotFoundError, PermissionError):
        return os.environ.get(env_name or secret_name.upper())


def get_system_stats():
    """Get system stats, preferring cgroup values when in a container."""
    stats = {}

    mem_current = _read_cgroup_file('/sys/fs/cgroup/memory.current')
    mem_max = _read_cgroup_file('/sys/fs/cgroup/memory.max')

    if mem_current:
        stats['memory_used_bytes'] = int(mem_current)
        if mem_max and mem_max != 'max':
            stats['memory_limit_bytes'] = int(mem_max)
            stats['memory_percent'] = round(int(mem_current) / int(mem_max) * 100, 1)
    else:
        try:
            import psutil
            mem = psutil.virtual_memory()
            stats['memory_used_bytes'] = mem.used
            stats['memory_limit_bytes'] = mem.total
            stats['memory_percent'] = mem.percent
        except ImportError:
            pass

    cpu_stat = _read_cgroup_file('/sys/fs/cgroup/cpu.stat')
    if cpu_stat:
        for line in cpu_stat.split('\n'):
            if line.startswith('usage_usec'):
                stats['cpu_usage_usec'] = int(line.split()[1])
                break
    else:
        try:
            import psutil
            stats['cpu_percent'] = psutil.cpu_percent(interval=0)
        except ImportError:
            pass

    # Disk space (/config volume, fallback to /)
    try:
        disk_path = '/config' if os.path.isdir('/config') else '/'
        st = os.statvfs(disk_path)
        disk_total = st.f_frsize * st.f_blocks
        disk_free = st.f_frsize * st.f_bavail
        disk_used = max(0, disk_total - disk_free)
        if disk_total > 0:
            stats['disk_used_bytes'] = disk_used
            stats['disk_total_bytes'] = disk_total
            stats['disk_percent'] = round(disk_used / disk_total * 100, 1)
    except OSError:
        pass

    # Open file descriptors
    try:
        # Subtract 1: listdir() opens its own FD to /proc/self/fd
        stats['fd_open'] = max(0, len(os.listdir('/proc/self/fd')) - 1)
    except OSError:
        pass
    try:
        with open('/proc/self/limits', 'r') as f:
            for line in f:
                if line.startswith('Max open files'):
                    # fields: Max, open, files, soft_limit, hard_limit, units
                    stats['fd_max'] = int(line.split()[3])
                    break
    except (OSError, ValueError, IndexError):
        pass

    # Network I/O (cumulative bytes, all interfaces except lo)
    try:
        with open('/proc/net/dev', 'r') as f:
            rx_total = 0
            tx_total = 0
            for line in f:
                line = line.strip()
                if ':' not in line:
                    continue
                iface, data = line.split(':', 1)
                if iface.strip() == 'lo':
                    continue
                fields = data.split()
                rx_total += int(fields[0])
                tx_total += int(fields[8])
            if rx_total or tx_total:
                stats['net_rx_bytes'] = rx_total
                stats['net_tx_bytes'] = tx_total
    except (OSError, ValueError, IndexError):
        pass

    return stats


# ---------------------------------------------------------------------------
# Mount health history
# ---------------------------------------------------------------------------

class MountHistory:
    """Tracks mount status changes over time for timeline display."""

    def __init__(self, max_entries=500):
        self._history = {}  # path -> deque of {timestamp, mounted, accessible}
        self._max_entries = max_entries
        self._lock = threading.Lock()

    def record(self, path, mounted, accessible):
        """Record mount state, but only if it changed from last recorded state."""
        with self._lock:
            if path not in self._history:
                self._history[path] = collections.deque(maxlen=self._max_entries)

            entries = self._history[path]
            if entries:
                last = entries[-1]
                if last['mounted'] == mounted and last['accessible'] == accessible:
                    return  # No change

            entries.append({
                'timestamp': datetime.now().isoformat(timespec='seconds'),
                'mounted': mounted,
                'accessible': accessible,
            })

    def to_dict(self):
        with self._lock:
            return {
                path: list(entries)
                for path, entries in self._history.items()
            }


mount_history = MountHistory()


# ---------------------------------------------------------------------------
# Log reader
# ---------------------------------------------------------------------------

def read_log_lines(lines=100, level=None, log_dir='./log'):
    """Read last N lines from the most recent log file, optionally filtered by level."""
    try:
        log_files = glob_mod.glob(os.path.join(log_dir, 'PDZURG-*.log'))
        if not log_files:
            return []
        log_file = max(log_files)  # Lexicographic sort — date-stamped names sort correctly

        with open(log_file, 'rb') as f:
            f.seek(0, 2)
            file_size = f.tell()
            if file_size == 0:
                return []

            # Read from end in blocks
            block_size = 8192
            blocks = []
            remaining = file_size
            while remaining > 0:
                read_size = min(block_size, remaining)
                remaining -= read_size
                f.seek(remaining)
                blocks.insert(0, f.read(read_size))

        all_text = b''.join(blocks).decode('utf-8', errors='replace')
        all_lines = all_text.splitlines()

        if level:
            level_upper = level.upper()
            all_lines = [l for l in all_lines if level_upper in l]

        return all_lines[-lines:]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Config viewer
# ---------------------------------------------------------------------------

_SENSITIVE_PATTERNS = {'KEY', 'TOKEN', 'PASS', 'SECRET', 'AUTH'}
_CONFIG_PREFIXES = (
    'ZURG', 'RD_', 'AD_', 'RCLONE', 'PD_', 'PLEX',
    'JF_', 'SEERR', 'BLACKHOLE', 'NOTIFICATION',
    'STATUS_UI', 'FFPROBE', 'DUPLICATE', 'NFS',
    'PDZURG', 'AUTO_UPDATE', 'CLEANUP', 'TORBOX',
    'MDBLIST', 'SHOW_MENU', 'GITHUB', 'SKIP_VALIDATION',
    'TZ', 'SONARR_', 'RADARR_', 'TMDB_',
    'ROUTING_AUDIT', 'QUEUE_CLEANUP', 'LIBRARY_SCAN',
    'SYMLINK_VERIFY', 'PREFERENCE_ENFORCE', 'HOUSEKEEPING',
    'CONFIG_BACKUP', 'MOUNT_LIVENESS', 'HISTORY_',
)


def get_sanitized_config():
    """Return current pd_zurg config with sensitive values masked."""
    config = {}
    for key in sorted(os.environ.keys()):
        if not any(key.startswith(p) for p in _CONFIG_PREFIXES):
            continue

        value = os.environ[key]
        if any(s in key.upper() for s in _SENSITIVE_PATTERNS):
            if value and len(value) > 8:
                config[key] = value[:4] + '****' + value[-4:]
            elif value:
                config[key] = '****'
            else:
                config[key] = '(not set)'
        else:
            config[key] = value if value else '(not set)'

    return config


# ---------------------------------------------------------------------------
# Service health checks (cached)
# ---------------------------------------------------------------------------

_service_cache = []
_service_cache_time = 0
_SERVICE_CACHE_TTL = 60  # seconds


def _check_service(name, svc_type, url, headers=None, ok_codes=(200,)):
    """Check a single service. Returns a status dict."""
    import requests as req
    svc = {'name': name, 'type': svc_type, 'status': 'error'}
    try:
        r = req.get(url, headers=headers or {}, timeout=5)
        if r.status_code in ok_codes:
            svc['status'] = 'ok'
            return svc, r
        else:
            svc['detail'] = f'HTTP {r.status_code}'
            return svc, None
    except Exception as e:
        svc['detail'] = type(e).__name__
        return svc, None


def check_services():
    """Check connectivity to all configured external services. Cached."""
    global _service_cache, _service_cache_time
    now = time.time()
    if now - _service_cache_time < _SERVICE_CACHE_TTL and _service_cache:
        return _service_cache

    services = []

    # Real-Debrid
    rd_key = _get_secret_or_env('rd_api_key', 'RD_API_KEY')
    if rd_key:
        svc, resp = _check_service(
            'Real-Debrid', 'debrid',
            'https://api.real-debrid.com/rest/1.0/user',
            headers={'Authorization': f'Bearer {rd_key}'})
        if resp:
            try:
                data = resp.json()
                svc['username'] = data.get('username', '')
                svc['premium'] = data.get('type') == 'premium'
                exp_str = data.get('expiration', '')
                if exp_str:
                    svc['expiration'] = exp_str
                    try:
                        exp = datetime.fromisoformat(exp_str.replace('Z', '+00:00'))
                        days = (exp - datetime.now(timezone.utc)).days
                        svc['days_remaining'] = days
                    except (ValueError, TypeError):
                        pass
            except (ValueError, KeyError):
                pass
        svc['url'] = 'https://real-debrid.com'
        services.append(svc)

    # AllDebrid
    ad_key = _get_secret_or_env('ad_api_key', 'AD_API_KEY')
    if ad_key:
        svc, resp = _check_service(
            'AllDebrid', 'debrid',
            f'https://api.alldebrid.com/v4/user?agent=pd_zurg&apikey={ad_key}')
        if resp:
            try:
                data = resp.json()
                if data.get('status') == 'success' and data.get('data', {}).get('user'):
                    svc['username'] = data['data']['user'].get('username', '')
                    svc['premium'] = data['data']['user'].get('isPremium', False)
            except (ValueError, KeyError):
                pass
        svc['url'] = 'https://alldebrid.com'
        services.append(svc)

    # Plex
    plex_addr = os.environ.get('PLEX_ADDRESS') or _get_secret_or_env('plex_address', 'PLEX_ADDRESS')
    plex_token = os.environ.get('PLEX_TOKEN') or _get_secret_or_env('plex_token', 'PLEX_TOKEN')
    if plex_addr and plex_token:
        svc, resp = _check_service(
            'Plex', 'media_server',
            f'{plex_addr}/identity',
            headers={'X-Plex-Token': plex_token, 'Accept': 'application/json'})
        svc['url'] = plex_addr
        services.append(svc)

    # Jellyfin
    jf_addr = os.environ.get('JF_ADDRESS') or _get_secret_or_env('jf_address', 'JF_ADDRESS')
    jf_key = os.environ.get('JF_API_KEY') or _get_secret_or_env('jf_api_key', 'JF_API_KEY')
    if jf_addr and jf_key:
        svc, resp = _check_service(
            'Jellyfin', 'media_server',
            f'{jf_addr}/System/Info',
            headers={'X-Emby-Token': jf_key})
        svc['url'] = jf_addr
        services.append(svc)

    # Overseerr / Jellyseerr
    seerr_addr = os.environ.get('SEERR_ADDRESS') or _get_secret_or_env('seerr_address', 'SEERR_ADDRESS')
    seerr_key = os.environ.get('SEERR_API_KEY') or _get_secret_or_env('seerr_api_key', 'SEERR_API_KEY')
    if seerr_addr and seerr_key:
        svc, resp = _check_service(
            'Overseerr', 'automation',
            f'{seerr_addr}/api/v1/status',
            headers={'X-Api-Key': seerr_key})
        svc['url'] = seerr_addr
        services.append(svc)

    # Zurg WebDAV
    zurg_enabled = (os.environ.get('ZURG_ENABLED') or '').lower() == 'true'
    if zurg_enabled:
        zurg_user = os.environ.get('ZURG_USER') or _get_secret_or_env('zurg_user', 'ZURG_USER')
        zurg_pass = os.environ.get('ZURG_PASS') or _get_secret_or_env('zurg_pass', 'ZURG_PASS')
        for key_type, env_suffix in [('RD', 'RealDebrid'), ('AD', 'AllDebrid')]:
            port = os.environ.get(f'ZURG_PORT_{env_suffix}')
            if port:
                headers = {}
                auth = None
                if zurg_user and zurg_pass:
                    import base64 as b64
                    creds = b64.b64encode(f'{zurg_user}:{zurg_pass}'.encode()).decode()
                    headers['Authorization'] = f'Basic {creds}'
                svc, resp = _check_service(
                    f'Zurg WebDAV ({key_type})', 'storage',
                    f'http://localhost:{port}/dav/',
                    headers=headers, ok_codes=(200, 207, 301))
                services.append(svc)

    # FlareSolverr
    flare_url = os.environ.get('FLARESOLVERR_URL')
    if flare_url:
        base_url = flare_url.rsplit('/v1', 1)[0] if '/v1' in flare_url else flare_url
        svc, resp = _check_service('FlareSolverr', 'proxy', base_url)
        services.append(svc)

    _service_cache = services
    _service_cache_time = now
    return services


# ---------------------------------------------------------------------------
# Status data singleton
# ---------------------------------------------------------------------------

class StatusData:
    """Singleton collecting status from all components."""

    def __init__(self):
        self.start_time = time.time()
        self.version = '2.11.0'
        self.recent_events = collections.deque(maxlen=100)
        self.error_count = 0
        self._lock = threading.Lock()

    def add_event(self, component, message, level='info'):
        with self._lock:
            self.recent_events.appendleft({
                'timestamp': datetime.now().isoformat(timespec='seconds'),
                'component': component,
                'message': message,
                'level': level,
            })
            if level == 'error':
                self.error_count += 1
        # Increment metrics counter
        try:
            from utils.metrics import metrics
            metrics.inc('events', {'level': level})
        except Exception:
            pass

    def to_dict(self):
        from utils.processes import _process_registry, _registry_lock

        processes = []
        with _registry_lock:
            for entry in _process_registry:
                if isinstance(entry, dict):
                    handler = entry['handler']
                    name = entry['process_name']
                    key_type = entry['key_type']
                else:
                    handler, name, key_type = entry
                desc = f"{name} w/ {key_type}" if key_type else name
                running = handler.process is not None and handler.process.poll() is None
                proc_info = {
                    'name': desc,
                    'pid': handler.process.pid if handler.process else None,
                    'running': running,
                }
                if hasattr(handler, '_restart_count'):
                    proc_info['restart_count'] = handler._restart_count
                processes.append(proc_info)

        mounts = []
        seen_paths = set()

        def _probe(path, role):
            """Stat *path* and append a mount row tagged with *role*.

            Bind-mounted host paths (blackhole, local library) usually don't
            show up as ``mountpoint``s inside the container — we still report
            ``accessible`` so users can spot a misconfigured volume.
            """
            if not path or path in seen_paths:
                return
            seen_paths.add(path)
            try:
                exists = os.path.exists(path)
                mounted = os.path.ismount(path) if exists else False
                accessible = os.access(path, os.R_OK) if exists else False
            except OSError:
                exists, mounted, accessible = False, False, False
            mounts.append({
                'path': path,
                'role': role,
                'mounted': mounted,
                'accessible': accessible,
                'exists': exists,
            })
            mount_history.record(path, mounted, accessible)

        # Debrid mounts exposed by rclone under /data. Filter to actual
        # mountpoints — stray host directories under /data (e.g. a bare
        # parent dir whose children are mounted separately) would otherwise
        # surface as permanently-red "Not mounted" Debrid rows.
        try:
            if os.path.exists('/data'):
                for entry_name in sorted(os.listdir('/data')):
                    path = os.path.join('/data', entry_name)
                    try:
                        if os.path.ismount(path):
                            _probe(path, 'Debrid')
                    except OSError:
                        pass
        except OSError:
            pass

        # Blackhole + local library paths from env (bind mounts from the host)
        _probe(os.environ.get('BLACKHOLE_DIR'), 'Blackhole')
        _probe(os.environ.get('BLACKHOLE_COMPLETED_DIR'), 'Blackhole')
        _probe(os.environ.get('BLACKHOLE_LOCAL_LIBRARY_MOVIES'), 'Local Library')
        _probe(os.environ.get('BLACKHOLE_LOCAL_LIBRARY_TV'), 'Local Library')

        with self._lock:
            events = list(self.recent_events)
            error_count = self.error_count

        # Provider API health metrics
        provider_health = _api_metrics.get_metrics()

        return {
            'version': self.version,
            'uptime_seconds': int(time.time() - self.start_time),
            'processes': processes,
            'mounts': mounts,
            'services': check_services(),
            'system': get_system_stats(),
            'recent_events': events,
            'error_count': error_count,
            'provider_health': provider_health,
        }


# Module-level singleton
status_data = StatusData()


# ---------------------------------------------------------------------------
# Settings setup guide (shown when auth is not configured)
# ---------------------------------------------------------------------------

_SETTINGS_SETUP_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pd_zurg Settings - Setup</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#c9d1d9;--text2:#8b949e;--blue:#58a6ff;--green:#3fb950}
[data-theme="light"]{--bg:#f6f8fa;--card:#ffffff;--border:#d0d7de;--text:#1f2328;--text2:#656d76;--blue:#0969da;--green:#1a7f37}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);padding:20px;max-width:700px;margin:40px auto}
a{color:var(--blue);text-decoration:none}
h1{color:var(--blue);font-size:1.5em;margin-bottom:8px}
.subtitle{color:var(--text2);font-size:.9em;margin-bottom:32px}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:24px;margin-bottom:20px}
.card h2{font-size:1em;font-weight:600;margin-bottom:16px;color:var(--text)}
.step{display:flex;gap:14px;margin-bottom:20px}
.step-num{background:var(--blue);color:#fff;width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:.85em;font-weight:700;flex-shrink:0}
.step-content{flex:1}
.step-content p{font-size:.9em;line-height:1.6;color:var(--text2)}
.step-content p strong{color:var(--text)}
code{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:2px 8px;font-size:.85em;color:var(--green);font-family:monospace}
pre{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:14px 16px;font-size:.82em;color:var(--green);font-family:monospace;overflow-x:auto;margin:10px 0;line-height:1.6}
.note{font-size:.8em;color:var(--text2);margin-top:20px;padding-top:16px;border-top:1px solid var(--border)}
</style>
</head>
<body>
<h1>pd_zurg Settings Editor</h1>
<p class="subtitle">Configure everything from your browser — no SSH or file editing needed.</p>

<div class="card">
  <h2>Quick Setup</h2>
  <div class="step">
    <div class="step-num">1</div>
    <div class="step-content">
      <p>Add <strong>one line</strong> to your <code>docker-compose.yml</code> environment section:</p>
      <pre>- STATUS_UI_AUTH=admin:yourpassword</pre>
      <p>Replace <code>yourpassword</code> with a password of your choice.</p>
    </div>
  </div>
  <div class="step">
    <div class="step-num">2</div>
    <div class="step-content">
      <p>Restart the container:</p>
      <pre>docker compose up -d</pre>
    </div>
  </div>
  <div class="step">
    <div class="step-num">3</div>
    <div class="step-content">
      <p>Reload this page. You'll be prompted to log in, then the full settings editor will be available.</p>
    </div>
  </div>
</div>

<div class="note">
  The settings editor lets you configure all pd_zurg environment variables and plex_debrid settings through the browser, with live validation and reload — no container restart needed for most changes.
  <br><br>
  <a href="/status">&larr; Back to Dashboard</a>
</div>
<script>(function(){var t=localStorage.getItem('pd_zurg_theme');if(t)document.documentElement.setAttribute('data-theme',t);})()</script>
</body>
</html>'''


# ---------------------------------------------------------------------------
# HTML Dashboard
# ---------------------------------------------------------------------------

_DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
__BASE_HEAD__
</head>
<body>
__NAV_HTML__
<main class="main-content">
<div class="meta">Uptime: <span id="uptime"></span> <span class="freshness"><span class="pulse-dot" id="fetch-dot"></span><span id="freshness-text"></span></span></div>
<div class="meta" id="error-line" style="display:none;color:var(--red)">Errors: <span id="errors">0</span></div>
<div class="banner" id="banner"></div>
<div class="grid full">
  <div class="card">
    <h2>Services</h2>
    <div class="svc-grid" id="services"></div>
  </div>
</div>
<div class="grid">
  <div class="card">
    <h2>Processes</h2>
    <table><thead><tr><th>Name</th><th style="text-align:center">PID</th><th style="text-align:center">Restarts</th><th style="text-align:center">Status</th><th id="actions-hdr"></th></tr></thead>
    <tbody id="procs"></tbody></table>
  </div>
  <div class="card">
    <h2>Mounts</h2>
    <table><thead><tr><th>Role</th><th>Path</th><th style="text-align:center">Status</th></tr></thead>
    <tbody id="mounts"></tbody></table>
    <div class="mount-timeline" id="mount-timeline"></div>
  </div>
</div>
<div class="grid">
  <div class="card">
    <h2>System</h2>
    <div class="stats-row">
      <div class="stat-container"><svg class="stat-ring" viewBox="0 0 120 120"><circle cx="60" cy="60" r="52" class="ring-bg"/><circle cx="60" cy="60" r="52" class="ring-fill" id="mem-ring-fill"/></svg><div class="stat-inner"><div class="stat-value" id="mem-used">-</div><div class="stat-label" id="mem-label">Memory Used</div></div></div>
      <div class="stat-container"><svg class="stat-ring" viewBox="0 0 120 120"><circle cx="60" cy="60" r="52" class="ring-bg"/><circle cx="60" cy="60" r="52" class="ring-fill" id="cpu-ring-fill"/></svg><div class="stat-inner"><div class="stat-value" id="cpu-used">-</div><div class="stat-label" id="cpu-label">CPU</div></div></div>
      <div class="stat-container"><svg class="stat-ring" viewBox="0 0 120 120"><circle cx="60" cy="60" r="52" class="ring-bg"/><circle cx="60" cy="60" r="52" class="ring-fill" id="disk-ring-fill"/></svg><div class="stat-inner"><div class="stat-value" id="disk-used">-</div><div class="stat-label" id="disk-label">Disk</div></div></div>
    </div>
    <div class="info-row" id="sys-info-row">
      <div class="info-item"><span class="info-value" id="sys-uptime">-</span><span class="info-label">Uptime</span></div>
      <div class="info-item"><span class="info-value" id="sys-fds">-</span><span class="info-label">Open FDs</span></div>
      <div class="info-item"><span class="info-value" id="sys-net">-</span><span class="info-label">Network I/O</span></div>
    </div>
  </div>
  <div class="card">
    <h2>Recent Events</h2>
    <div class="events" id="events"></div>
  </div>
</div>
<div class="footer"><span id="conn-status"></span>Refresh: <select id="refresh-interval" onchange="setRefreshInterval(this.value)" style="background:var(--bg);color:var(--text2);border:1px solid var(--border);border-radius:3px;font-size:1em;padding:1px 4px"><option value="5">5s</option><option value="10" selected>10s</option><option value="30">30s</option><option value="0">Paused</option></select></div>
<script>
__THEME_TOGGLE_JS__

var _failCount=0;
var _statusTimer,_mtTimer;
var _refreshSec=10;
var _prevNet=null;
function dot(ok){return '<span class="dot '+(ok?'green':'red')+'"></span>'+(ok?'Running':'Stopped');}
function mdot(ok,yes,no){return '<span class="dot '+(ok?'green':'red')+'"></span>'+(ok?(yes||'Yes'):(no||'No'));}
function sdot(s){return '<span class="dot '+(s==='ok'?'green':'red')+'"></span>';}

const _providerKeyMap={'Real-Debrid':'realdebrid','AllDebrid':'alldebrid','TorBox':'torbox'};
let _providerHealth={};
function renderServices(svcs){
  if(!svcs||!svcs.length)return '<div style="color:var(--text2);padding:8px">No services configured</div>';
  let h='';
  svcs.forEach(s=>{
    const pk=_providerKeyMap[s.name];
    const ph=pk?_providerHealth[pk]:null;
    h+='<div class="svc-item">'+sdot(s.status)+'<div class="svc-info"><div class="svc-name">'+(s.url?'<a href="'+esc(s.url)+'" target="_blank" rel="noopener noreferrer">'+esc(s.name)+'</a>':esc(s.name))+'</div>';
    if(s.status==='ok'){
      let det='Connected';
      if(s.username)det=esc(s.username);
      h+='<div class="svc-detail">'+det+'</div>';
    }else{
      h+='<div class="svc-detail" style="color:var(--red)">'+(s.detail?esc(s.detail):'Unreachable')+'</div>';
    }
    if(ph&&ph.calls_today>0){
      h+='<div class="svc-health">';
      h+='<span>API: '+esc(ph.calls_today)+(ph.errors_today?' ('+esc(ph.errors_today)+' err)':'')+'</span>';
      h+='<span>Avg: '+esc((ph.avg_response_ms/1000).toFixed(1))+'s</span>';
      if(ph.rate_limit_remaining!=null&&ph.rate_limit_limit!=null&&ph.rate_limit_limit>0){
        const pct=Math.round((ph.rate_limit_remaining/ph.rate_limit_limit)*100);
        const used=100-pct;
        const cls=used>80?'red':used>50?'yellow':'green';
        h+='<span>RL: <span class="rl-bar"><span class="rl-fill '+cls+'" style="width:'+used+'%"></span></span> '+esc(ph.rate_limit_remaining)+'/'+esc(ph.rate_limit_limit)+'</span>';
      }
      h+='</div>';
      if(ph.last_error){
        h+='<div class="svc-health" style="color:var(--red)"><span>Last err: '+esc(ph.last_error)+(ph.last_error_time?' ('+esc(ph.last_error_time)+')':'')+'</span></div>';
      }
    }
    h+='</div>';
    if(s.days_remaining!==undefined&&s.days_remaining!==null){
      let cls='premium';
      let label=s.days_remaining+'d';
      if(s.days_remaining<=3){cls='crit';label=s.days_remaining+'d!';}
      else if(s.days_remaining<=7){cls='warn';label=s.days_remaining+'d';}
      h+='<span class="svc-badge '+cls+'">'+label+'</span>';
    }else if(s.premium===true){
      h+='<span class="svc-badge premium">Premium</span>';
    }else if(s.premium===false){
      h+='<span class="svc-badge crit">Free</span>';
    }
    h+='</div>';
  });
  return h;
}

var _lastFetchTime=0;
function updateRing(id,pct){var f=document.getElementById(id);if(!f)return;var c=326.73,o=c-(c*Math.min(pct,100)/100);f.style.strokeDashoffset=o;f.style.stroke=pct>85?'var(--red)':pct>60?'var(--yellow)':'var(--green)';}
function setCardHealth(h2Text,cls){var heads=document.querySelectorAll('.card h2');for(var i=0;i<heads.length;i++){if(heads[i].textContent.trim().startsWith(h2Text)){var c=heads[i].parentElement;c.classList.remove('card-ok','card-warn','card-crit');if(cls)c.classList.add(cls);break;}}}
function updateCardStates(d){
  var sH='ok',pH='ok',mH='ok',yH='ok',eH='ok',overall='ok';
  if(d.services){d.services.forEach(function(s){if(s.status!=='ok')sH='crit';if(s.days_remaining!=null){if(s.days_remaining<=3)sH='crit';else if(s.days_remaining<=7&&sH==='ok')sH='warn';}});for(var pk in _providerHealth){var ph=_providerHealth[pk];if(ph.rate_limit_remaining!=null&&ph.rate_limit_limit!=null&&ph.rate_limit_limit>0){var u=Math.round(((ph.rate_limit_limit-ph.rate_limit_remaining)/ph.rate_limit_limit)*100);if(u>=80&&sH==='ok')sH='warn';}}}
  d.processes.forEach(function(p){if(!p.running)pH='crit';});
  d.mounts.forEach(function(m){
    if(m.exists===false)mH='crit';
    else if(m.role==='Debrid'&&!m.mounted)mH='crit';
    else if(!m.accessible)mH='crit';
  });
  if(d.system.memory_percent!=null){if(d.system.memory_percent>85)yH='crit';else if(d.system.memory_percent>60)yH='warn';}
  if(d.system.cpu_percent!=null){if(d.system.cpu_percent>85)yH='crit';else if(d.system.cpu_percent>60&&yH==='ok')yH='warn';}
  if(d.system.disk_percent!=null){if(d.system.disk_percent>85)yH='crit';else if(d.system.disk_percent>60&&yH==='ok')yH='warn';}
  if(d.system.fd_open!=null&&d.system.fd_max!=null){var fdPct=d.system.fd_open/d.system.fd_max*100;if(fdPct>85)yH='crit';else if(fdPct>60&&yH==='ok')yH='warn';}
  if(d.recent_events)d.recent_events.forEach(function(v){if(v.level==='error')eH='warn';});
  setCardHealth('Services','card-'+sH);setCardHealth('Processes','card-'+pH);setCardHealth('Mounts','card-'+mH);setCardHealth('System','card-'+yH);setCardHealth('Recent Events','card-'+eH);
  if(sH==='crit'||pH==='crit'||mH==='crit'||yH==='crit')overall='crit';else if(sH==='warn'||pH==='warn'||mH==='warn'||yH==='warn')overall='warn';
  if(typeof updateFavicon==='function')updateFavicon(overall);
}

function update(){
  var _fd=document.getElementById('fetch-dot');if(_fd)_fd.className='pulse-dot fetching';
  fetch('/api/status').then(r=>r.json()).then(d=>{
    var hm=document.getElementById('header-meta');if(hm)hm.textContent='v'+d.version;
    document.getElementById('uptime').textContent=fmt(d.uptime_seconds);
    document.getElementById('errors').textContent=d.error_count;
    document.getElementById('error-line').style.display=d.error_count>0?'block':'none';

    // Store provider health for renderServices
    _providerHealth=d.provider_health||{};

    // Banner for RD premium expiry + rate limit warnings
    const banner=document.getElementById('banner');
    let bannerShown=false;
    if(d.services)d.services.forEach(s=>{
      if(s.days_remaining!==undefined&&s.days_remaining!==null&&s.days_remaining<=7){
        banner.className='banner '+(s.days_remaining<=3?'crit':'warn');
        banner.innerHTML=(s.days_remaining<=0?
          esc(s.name)+' premium has EXPIRED. Your setup will not work until renewed.':
          esc(s.name)+' premium expires in '+s.days_remaining+' day'+(s.days_remaining!==1?'s':'')+'. Renew to avoid service interruption.');
        bannerShown=true;
      }
    });
    if(!bannerShown){
      for(const[pk,ph] of Object.entries(_providerHealth)){
        if(ph.rate_limit_remaining!=null&&ph.rate_limit_limit!=null&&ph.rate_limit_limit>0){
          const usedPct=Math.round(((ph.rate_limit_limit-ph.rate_limit_remaining)/ph.rate_limit_limit)*100);
          if(usedPct>=80){
            const name=Object.entries(_providerKeyMap).find(([,v])=>v===pk);
            const label=name?name[0]:pk;
            banner.className='banner '+(usedPct>=95?'crit':'warn');
            banner.innerHTML=esc(label)+' API rate limit at '+usedPct+'% — automated searches may be throttled.';
            bannerShown=true;break;
          }
        }
      }
    }
    if(!bannerShown)banner.className='banner';

    // Services
    document.getElementById('services').innerHTML=renderServices(d.services);

    // Processes (with optional restart buttons when auth is configured)
    let p='';const hasAuth=window._hasAuth;
    d.processes.forEach(x=>{
      const svcName=x.name.split(' w/ ')[0].toLowerCase();
      const restartBtn=hasAuth?'<td><button class="btn btn-ghost btn-sm" onclick="restartSvc(this,\\x27'+esc(svcName)+'\\x27)" title="Restart">Restart</button></td>':'<td></td>';
      p+='<tr><td>'+esc(x.name)+'</td><td>'+(x.pid||'-')+'</td><td>'+(x.restart_count||0)+'</td><td>'+dot(x.running)+'</td>'+restartBtn+'</tr>';
    });
    document.getElementById('procs').innerHTML=p||'<tr><td colspan="5" style="color:var(--text2)">No processes</td></tr>';
    document.getElementById('actions-hdr').textContent=hasAuth?'Actions':'';

    // Mounts — group by role, show a single status signal per row
    let m='';
    const roleOrder=['Debrid','Blackhole','Local Library'];
    const byRole={};
    d.mounts.forEach(x=>{const r=x.role||'Other';(byRole[r]=byRole[r]||[]).push(x);});
    roleOrder.concat(Object.keys(byRole).filter(r=>!roleOrder.includes(r))).forEach(role=>{
      const rows=byRole[role];if(!rows)return;
      rows.forEach((x,i)=>{
        // Status: green if accessible, yellow if exists-but-not-accessible,
        // red if missing entirely. Debrid mounts additionally need ismount=true.
        let ok,label;
        if(x.exists===false){ok=false;label='Missing';}
        else if(x.role==='Debrid'&&!x.mounted){ok=false;label='Not mounted';}
        else if(!x.accessible){ok=false;label='Not accessible';}
        else{ok=true;label='OK';}
        const roleCell=i===0?'<td rowspan="'+rows.length+'" style="vertical-align:top;color:var(--text2);font-size:.85em">'+esc(role)+'</td>':'';
        m+='<tr>'+roleCell+'<td style="font-family:monospace;font-size:.88em">'+esc(x.path)+'</td><td>'+mdot(ok,label,label)+'</td></tr>';
      });
    });
    document.getElementById('mounts').innerHTML=m||'<tr><td colspan="3" style="color:var(--text2)">No mounts</td></tr>';

    // System — Memory
    if(d.system.memory_used_bytes!==undefined){
      if(d.system.memory_percent!==undefined&&d.system.memory_limit_bytes!==undefined){
        document.getElementById('mem-used').textContent=fmtBytes(d.system.memory_used_bytes)+' / '+fmtBytes(d.system.memory_limit_bytes);
        document.getElementById('mem-label').textContent='Memory ('+d.system.memory_percent+'%)';
      }else{
        document.getElementById('mem-used').textContent=fmtBytes(d.system.memory_used_bytes);
        document.getElementById('mem-label').textContent='Memory Used (no limit)';
      }
    }
    updateRing('mem-ring-fill',d.system.memory_percent||0);
    // System — CPU
    if(d.system.cpu_percent!==undefined){
      document.getElementById('cpu-used').textContent=d.system.cpu_percent.toFixed(1)+'%';
      document.getElementById('cpu-label').textContent='CPU';
      updateRing('cpu-ring-fill',d.system.cpu_percent);
    }else if(d.system.cpu_usage_usec!==undefined){
      document.getElementById('cpu-used').textContent=(d.system.cpu_usage_usec/1000000).toFixed(1)+'s';
      document.getElementById('cpu-label').textContent='CPU Time';
    }
    // System — Disk
    if(d.system.disk_used_bytes!==undefined&&d.system.disk_total_bytes!==undefined){
      document.getElementById('disk-used').textContent=(d.system.disk_percent||0)+'%';
      document.getElementById('disk-label').textContent=fmtBytes(d.system.disk_used_bytes)+' / '+fmtBytes(d.system.disk_total_bytes);
      updateRing('disk-ring-fill',d.system.disk_percent||0);
    }
    // System — Uptime
    if(d.uptime_seconds!==undefined){
      document.getElementById('sys-uptime').textContent=fmt(d.uptime_seconds);
    }
    // System — Open FDs
    if(d.system.fd_open!==undefined){
      var fdText=d.system.fd_open.toLocaleString();
      if(d.system.fd_max)fdText+=' / '+d.system.fd_max.toLocaleString();
      document.getElementById('sys-fds').textContent=fdText;
    }
    // System — Network I/O (show rate between polls)
    if(d.system.net_rx_bytes!==undefined){
      var now=Date.now()/1000;
      var rx=d.system.net_rx_bytes||0,tx=d.system.net_tx_bytes||0;
      if(_prevNet){
        var dt=now-_prevNet.t;
        if(dt>0){
          var rxRate=Math.max(0,(rx-_prevNet.rx)/dt);
          var txRate=Math.max(0,(tx-_prevNet.tx)/dt);
          document.getElementById('sys-net').textContent='\u2193 '+fmtBytes(rxRate)+'/s \u2191 '+fmtBytes(txRate)+'/s';
        }
      }
      _prevNet={rx:rx,tx:tx,t:now};
    }

    // Events (with relative time on hover)
    const validLevels=new Set(['info','warning','error']);
    let e='';d.recent_events.forEach(x=>{
      const lvl=validLevels.has(x.level)?x.level:'info';
      const t=x.timestamp.split('T')[1]||x.timestamp;
      const ago=timeAgo(x.timestamp);
      e+='<div class="event '+lvl+'"><span class="time" title="'+esc(ago)+'">'+esc(t)+'</span><span class="comp">'+esc(x.component)+'</span><span class="msg">'+esc(x.message)+'</span></div>';
    });
    if(!e){
      e='<div style="color:var(--text2);padding:8px 0">No events yet</div>';
    }else if(d.recent_events.length&&d.recent_events.length<=3){
      const newest=d.recent_events[0];
      if(newest){
        try{
          const evtTime=new Date(newest.timestamp);
          const ageMin=Math.floor((Date.now()-evtTime.getTime())/60000);
          if(ageMin>30){
            e+='<div style="color:var(--text3);padding:6px 0;font-size:.75em;border-top:1px solid var(--border2);margin-top:4px">No issues for '+fmt(ageMin*60)+' \u2014 all systems running normally</div>';
          }
        }catch(ex){}
      }
    }
    document.getElementById('events').innerHTML=e;
    _failCount=0;
    document.getElementById('conn-status').textContent='';
    _lastFetchTime=Date.now();
    if(_fd){_fd.className='pulse-dot';document.getElementById('freshness-text').textContent='Updated just now';}
    updateCardStates(d);
  }).catch(()=>{
    _failCount++;
    if(_failCount>=3){document.getElementById('conn-status').textContent='Connection lost \u2014 retrying... ';if(_fd)_fd.className='pulse-dot lost';}
  });
}
// Restart service
async function restartSvc(btn,name){
  if(!await showConfirm('Restart '+name+'?','This will restart the '+name+' process.'))return;
  btn.disabled=true;btn.textContent='...';
  fetch('/api/restart/'+name,{method:'POST'}).then(r=>r.json()).then(d=>{
    btn.textContent=d.status==='restarting'?'OK':'Err';
    setTimeout(()=>{btn.disabled=false;btn.textContent='Restart';},5000);
  }).catch(()=>{btn.disabled=false;btn.textContent='Restart';});
}

// Mount history timeline — only shown when state changes have occurred
function updateMountHistory(){
  fetch('/api/mount-history').then(r=>r.json()).then(hist=>{
    const el=document.getElementById('mount-timeline');
    if(!Object.keys(hist).length){el.innerHTML='';return;}
    // Check if any mount has more than 2 entries (actual state changes)
    let hasChanges=false;
    Object.keys(hist).forEach(path=>{if(hist[path].length>2)hasChanges=true;});
    if(!hasChanges){el.innerHTML='';return;}
    // Show timeline for mounts with state changes
    let h='<div style="font-size:.75em;color:var(--text2);margin-top:8px;padding-top:8px;border-top:1px solid var(--border2)">Mount History</div>';
    Object.keys(hist).forEach(path=>{
      const entries=hist[path];
      if(entries.length<=2)return;
      const shortPath=path.split('/').pop()||path;
      h+='<div class="mt-row"><span class="mt-path" title="'+esc(path)+'">'+esc(shortPath)+'</span><div class="mt-blocks">';
      const show=entries.slice(-60);
      show.forEach(e=>{
        let cls='ok';
        if(!e.mounted)cls='down';
        else if(!e.accessible)cls='partial';
        h+='<div class="mt-block '+cls+'" title="'+esc(e.timestamp)+' - '+(e.mounted?'mounted':'unmounted')+', '+(e.accessible?'accessible':'inaccessible')+'"></div>';
      });
      h+='</div></div>';
    });
    h+='<div style="font-size:.7em;color:var(--text3);margin-top:4px;display:flex;gap:10px;align-items:center"><span class="dot green"></span>Healthy <span class="dot yellow"></span>Degraded <span class="dot red"></span>Down</div>';
    el.innerHTML=h;
  }).catch(()=>{});
}

// Configurable refresh
function setRefreshInterval(sec){
  _refreshSec=parseInt(sec)||0;
  if(_statusTimer)clearInterval(_statusTimer);
  if(_mtTimer)clearInterval(_mtTimer);
  if(_refreshSec>0){
    _statusTimer=setInterval(update,_refreshSec*1000);
    _mtTimer=setInterval(updateMountHistory,Math.max(_refreshSec*3,30)*1000);
  }
}
update();
setRefreshInterval(10);
setTimeout(updateMountHistory,1000);
setInterval(function(){if(!_lastFetchTime)return;var s=Math.floor((Date.now()-_lastFetchTime)/1000);var el=document.getElementById('freshness-text');if(!el)return;if(s<5)el.textContent='Updated just now';else if(s<60)el.textContent='Updated '+s+'s ago';else el.textContent='Updated '+Math.floor(s/60)+'m ago';},1000);
__WANTED_BADGE_JS__
</script>
</main>
</body>
</html>'''

_DASHBOARD_EXTRA_CSS = """
.main-content{max-width:1600px}
.meta{color:var(--text2);font-size:.85em;margin-bottom:20px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.grid.full{grid-template-columns:1fr}
@media(max-width:768px){.grid{grid-template-columns:1fr}}
.card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:16px;opacity:0;animation:fadeIn .3s ease forwards}
@keyframes fadeIn{to{opacity:1}}
.card h2{font-size:.8em;color:var(--text2);margin-bottom:12px;text-transform:uppercase;letter-spacing:.08em;font-weight:600}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--border2);font-size:.85em}
th{color:var(--text2);font-weight:500;font-size:.75em;text-transform:uppercase;letter-spacing:.05em}
#procs td:nth-child(2),#procs td:nth-child(3){text-align:center}
#procs td:nth-child(4){white-space:nowrap;text-align:center}
#mounts td:last-child{text-align:center;white-space:nowrap}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;vertical-align:middle}
.dot.green{background:var(--green)}.dot.red{background:var(--red);border-radius:2px}.dot.yellow{background:transparent;border:2px solid var(--yellow);width:8px;height:8px}
.svc-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:10px}
.svc-item{display:flex;align-items:center;padding:10px 12px;background:var(--bg);border-radius:6px;border:1px solid var(--border2)}
.svc-item .svc-info{flex:1;margin-left:8px}
.svc-item .svc-name{font-size:.85em;font-weight:500;color:var(--text)}
.svc-name a{color:inherit;text-decoration:none}.svc-name a:hover{color:var(--blue)}
.svc-item .svc-detail{font-size:.75em;color:var(--text2);margin-top:2px}
.svc-item .svc-badge{font-size:.7em;padding:2px 6px;border-radius:4px;font-weight:500;margin-left:8px}
.svc-item .svc-badge.premium{background:#3fb9501a;color:var(--green)}
.svc-item .svc-badge.warn{background:#d299221a;color:var(--yellow)}
.svc-item .svc-badge.crit{background:#f851491a;color:var(--red)}
.svc-health{font-size:.72em;color:var(--text3);margin-top:3px;display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.svc-health span{white-space:nowrap}
.rl-bar{display:inline-block;width:40px;height:6px;background:var(--border);border-radius:3px;vertical-align:middle;overflow:hidden;position:relative}
.rl-fill{height:100%;border-radius:3px;transition:width .3s}
.rl-fill.green{background:var(--green)}.rl-fill.yellow{background:var(--yellow)}.rl-fill.red{background:var(--red)}
.events{max-height:280px;overflow-y:auto}
.event{padding:5px 0;border-bottom:1px solid var(--border2);font-size:.8em;display:flex;gap:8px}
.event .time{color:var(--text3);min-width:55px;font-family:monospace;font-size:.85em}
.event .comp{color:var(--blue);font-weight:500;min-width:70px}
.event.error .msg{color:var(--red)}.event.warning .msg{color:var(--yellow)}
.stat-value{font-size:1.8em;font-weight:600;color:var(--blue)}
.stat-label{font-size:.75em;color:var(--text2);margin-top:2px}
.stats-row{display:flex;gap:32px}.stats-row>div{flex:1;text-align:center}
.mount-timeline{margin-top:8px}
.mt-row{display:flex;align-items:center;gap:8px;margin-bottom:4px;font-size:.8em}
.mt-path{color:var(--text2);min-width:120px;overflow:hidden;text-overflow:ellipsis}
.mt-blocks{display:flex;gap:1px;flex:1}
.mt-block{height:16px;min-width:3px;flex:1;border-radius:2px}
.mt-block.ok{background:var(--green)}.mt-block.down{background:var(--red)}.mt-block.partial{background:var(--yellow)}
.mt-block:hover{opacity:.8}
.footer{display:flex;justify-content:flex-end;align-items:center;gap:8px}
#conn-status{color:var(--red);font-weight:500}
[data-theme="light"] .svc-item{background:var(--card);border-color:var(--border)}
.freshness{margin-left:12px;font-size:.9em;color:var(--text3);display:inline-flex;align-items:center;gap:4px}
.pulse-dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--green);flex-shrink:0}
.pulse-dot.fetching{background:var(--blue);animation:pulse-fetch 1s ease-in-out infinite}
@keyframes pulse-fetch{0%,100%{opacity:1}50%{opacity:.3}}
.pulse-dot.lost{background:var(--red)}
.card-ok{box-shadow:inset 3px 0 0 var(--green)}
.card-warn{box-shadow:inset 3px 0 0 var(--yellow)}
.card-crit{box-shadow:inset 3px 0 0 var(--red)}
.stat-container{position:relative;flex:1;display:flex;flex-direction:column;align-items:center}
.stat-ring{width:140px;height:140px;transform:rotate(-90deg)}
.ring-bg{fill:none;stroke:var(--border);stroke-width:6}
.ring-fill{fill:none;stroke:var(--green);stroke-width:6;stroke-linecap:round;stroke-dasharray:326.73;stroke-dashoffset:326.73;transition:stroke-dashoffset var(--motion-slow) ease,stroke var(--motion-normal)}
.stat-inner{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center;pointer-events:none}
.info-row{display:flex;gap:24px;justify-content:center;margin-top:24px;padding-top:16px;border-top:1px solid var(--border2)}
.info-item{text-align:center;flex:1;min-width:0}
.info-value{display:block;font-size:1em;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.info-label{display:block;font-size:.7em;color:var(--text3);margin-top:2px;letter-spacing:.05em}
@media(max-width:600px){.stats-row{flex-wrap:nowrap;gap:12px}.stats-row>div{flex:1 1 0;min-width:0;width:auto}.stat-ring{width:100%;height:auto;max-width:140px}.stat-value{font-size:1.3em}.info-row{flex-wrap:wrap;gap:12px}.info-item{flex:none;width:calc(50% - 6px)}}
"""


def get_dashboard_html():
    """Return the complete dashboard HTML page with shared CSS and nav."""
    from utils.ui_common import (get_base_head, get_nav_html, THEME_TOGGLE_JS,
                                 WANTED_BADGE_JS, KEYBOARD_JS, TOAST_JS)
    html = _DASHBOARD_HTML
    html = html.replace('__BASE_HEAD__', get_base_head('pd_zurg Status', _DASHBOARD_EXTRA_CSS))
    html = html.replace('__NAV_HTML__', get_nav_html('status'))
    html = html.replace('__THEME_TOGGLE_JS__', THEME_TOGGLE_JS + KEYBOARD_JS + TOAST_JS)
    html = html.replace('__WANTED_BADGE_JS__', WANTED_BADGE_JS)
    return html


# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------

class StatusHandler(http.server.BaseHTTPRequestHandler):
    status_data_ref = None
    auth_credentials = None

    def do_GET(self):
        # Prometheus metrics endpoint — served before auth check
        # (scrapers don't support basic auth easily)
        if self.path == '/metrics':
            try:
                from utils.metrics import metrics
                body = metrics.format_metrics().encode()
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_response(500)
                self.end_headers()
            return

        # Auth probe — returns 403 on auth wall, 200 otherwise. Bypasses the
        # normal 401 middleware so the frontend can distinguish "you hit an
        # auth wall" from "everything is fine" without a basic-auth challenge.
        if self.path == '/api/auth/check':
            creds = self.auth_credentials  # snapshot to avoid SIGHUP reload race
            if creds:
                auth_header = self.headers.get('Authorization', '')
                ok = False
                if auth_header.startswith('Basic '):
                    raw = auth_header[6:]
                    if len(raw) <= 256:
                        try:
                            decoded = base64.b64decode(raw, validate=True).decode('utf-8')
                            if hmac.compare_digest(decoded.encode(), creds.encode()):
                                ok = True
                        except (ValueError, UnicodeDecodeError):
                            pass
                if not ok:
                    self._send_json_response(403, json.dumps({'error': 'auth required'}))
                    return
            self._send_json_response(200, json.dumps({'ok': True}))
            return

        if self.auth_credentials:
            auth_header = self.headers.get('Authorization', '')
            if not auth_header.startswith('Basic '):
                self._send_auth_required()
                return
            raw = auth_header[6:]
            if len(raw) > 256:
                self._send_auth_required()
                return
            try:
                decoded = base64.b64decode(raw, validate=True).decode('utf-8')
                if not hmac.compare_digest(decoded.encode(), self.auth_credentials.encode()):
                    self._send_auth_required()
                    return
            except (ValueError, UnicodeDecodeError):
                self._send_auth_required()
                return

        if self.path == '/api/status':
            data = json.dumps(self.status_data_ref.to_dict())
            self._send_json_response(200, data)
        elif self.path.startswith('/api/logs'):
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            lines = int(params.get('lines', ['100'])[0])
            lines = min(lines, 1000)  # Cap at 1000
            level = params.get('level', [None])[0]
            log_lines = read_log_lines(lines=lines, level=level)
            self._send_json_response(200, json.dumps(log_lines))
        elif self.path == '/api/config':
            data = json.dumps(get_sanitized_config())
            self._send_json_response(200, data)
        elif self.path == '/api/mount-history':
            data = json.dumps(mount_history.to_dict())
            self._send_json_response(200, data)
        elif self.path == '/api/tasks':
            from utils.task_scheduler import scheduler
            data = json.dumps(scheduler.get_status())
            self._send_json_response(200, data)
        elif self.path == '/settings':
            # Settings editor — requires auth
            if not self.auth_credentials:
                self._send_html_response(_SETTINGS_SETUP_HTML.encode())
                return
            from utils.settings_api import get_env_schema, get_plex_debrid_schema
            from utils.settings_page import get_settings_html
            self._send_html_response(get_settings_html(get_env_schema(), get_plex_debrid_schema()).encode())
        elif self.path == '/api/settings/env':
            # Read current env values — requires auth to be configured
            if not self.auth_credentials:
                self._send_json_response(403, json.dumps({
                    'error': 'Settings API requires STATUS_UI_AUTH to be configured'
                }))
                return
            from utils.settings_api import read_env_values
            data = json.dumps(read_env_values())
            self._send_json_response(200, data)
        elif self.path == '/api/settings/plex-debrid':
            # Read plex_debrid settings — requires auth to be configured
            if not self.auth_credentials:
                self._send_json_response(403, json.dumps({
                    'error': 'Settings API requires STATUS_UI_AUTH to be configured'
                }))
                return
            from utils.settings_api import read_plex_debrid_values
            data = json.dumps(read_plex_debrid_values())
            self._send_json_response(200, data)
        elif self.path == '/api/settings/export/env':
            if not self.auth_credentials:
                self._send_json_response(403, json.dumps({
                    'error': 'Settings API requires STATUS_UI_AUTH to be configured'
                }))
                return
            from utils.settings_api import export_env
            content = export_env().encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Content-Disposition', 'attachment; filename=".env"')
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        elif self.path == '/api/settings/export/plex-debrid':
            if not self.auth_credentials:
                self._send_json_response(403, json.dumps({
                    'error': 'Settings API requires STATUS_UI_AUTH to be configured'
                }))
                return
            from utils.settings_api import export_plex_debrid
            content = export_plex_debrid().encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Disposition', 'attachment; filename="settings.json"')
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        elif self.path == '/library' or self.path.startswith('/library?'):
            from utils.library_page import get_library_html
            nav_page = 'wanted' if 'filter=missing' in self.path else 'library'
            self._send_html_response(get_library_html(nav_page).encode())
        elif self.path == '/api/library':
            from utils.library import get_scanner
            scanner = get_scanner()
            if scanner is None:
                self._send_json_response(503, json.dumps({
                    'error': 'Library scanner not initialized'
                }))
            else:
                from utils.arr_client import get_configured_services
                from utils.library_prefs import get_all_pending, get_all_preferences
                result = dict(scanner.get_data())
                result['scanning'] = scanner.is_scanning()
                result['download_services'] = get_configured_services()
                result['pending'] = get_all_pending()
                result['preferences'] = get_all_preferences()
                result['search_enabled'] = bool((os.environ.get('TORRENTIO_URL') or '').strip())
                data = json.dumps(result)
                self._send_json_response(200, data)
        elif self.path.startswith('/api/library/metadata'):
            qs = parse_qs(urlparse(self.path).query)
            title = qs.get('title', [''])[0]
            year = qs.get('year', [None])[0]
            media_type = qs.get('type', ['show'])[0]
            if not title:
                self._send_json_response(400, json.dumps({'error': 'title required'}))
            else:
                try:
                    year_int = int(year) if year else None
                except (ValueError, TypeError):
                    year_int = None
                from utils.tmdb import get_show_info, get_movie_info
                if media_type == 'movie':
                    result = get_movie_info(title, year_int)
                else:
                    result = get_show_info(title, year_int)
                if result is None:
                    self._send_json_response(200, json.dumps(None))
                else:
                    self._send_json_response(200, json.dumps(result))
        elif self.path.startswith('/api/history/show/'):
            # Strip query string before extracting title
            parsed = urlparse(self.path)
            title = url_unquote(parsed.path[len('/api/history/show/'):])
            if not title:
                self._send_json_response(400, json.dumps({'error': 'title required'}))
            else:
                from utils import history as history_mod
                params = parse_qs(parsed.query)
                try:
                    limit = max(1, min(int(params.get('limit', ['20'])[0]), 200))
                except (ValueError, TypeError):
                    limit = 20
                events = history_mod.query_by_show(title, limit=limit)
                self._send_json_response(200, json.dumps(events))
        elif self.path.startswith('/api/history'):
            from utils import history as history_mod
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            try:
                page = max(1, int(params.get('page', ['1'])[0]))
            except (ValueError, TypeError):
                page = 1
            try:
                limit = max(1, min(int(params.get('limit', ['50'])[0]), 200))
            except (ValueError, TypeError):
                limit = 50
            result = history_mod.query(
                type=params.get('type', [None])[0],
                title=params.get('title', [None])[0],
                start=params.get('start', [None])[0],
                end=params.get('end', [None])[0],
                page=page,
                limit=limit,
            )
            self._send_json_response(200, json.dumps(result))
        elif self.path == '/api/blocklist':
            from utils import blocklist as blocklist_mod
            self._send_json_response(200, json.dumps(blocklist_mod.get_all()))
        elif self.path == '/activity':
            from utils.activity_page import get_activity_html
            self._send_html_response(get_activity_html().encode())
        elif self.path == '/system':
            from utils.system_page import get_system_html
            self._send_html_response(get_system_html().encode())
        elif self.path in ('/', '/status'):
            self._send_html_response(get_dashboard_html().encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        # POST endpoints always require auth
        if not self.auth_credentials:
            self._send_json_response(403, json.dumps({
                'error': 'This endpoint requires STATUS_UI_AUTH to be configured'
            }))
            return

        if not self._check_auth():
            return

        # Library refresh — now requires auth since scan triggers preference enforcement
        if self.path == '/api/library/refresh':
            from utils.library import get_scanner
            scanner = get_scanner()
            if scanner is None:
                self._send_json_response(503, json.dumps({
                    'error': 'Library scanner not initialized'
                }))
            else:
                scanner.refresh()
                self._send_json_response(200, json.dumps({'status': 'scanning'}))
            return

        if self.path == '/api/library/preference':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                title = values.get('title', '').strip()
                preference = values.get('preference', '').strip()
                if not title or not preference:
                    self._send_json_response(400, json.dumps({'error': 'title and preference required'}))
                    return
                from utils.library_prefs import set_preference
                result = set_preference(title, preference)
                self._send_json_response(200, json.dumps(result))
            except ValueError as e:
                self._send_json_response(400, json.dumps({'error': str(e)}))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[preference] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
            return

        if self.path == '/api/library/pending':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                title = values.get('title', '').strip()
                episodes = values.get('episodes', [])
                direction = values.get('direction', 'to-debrid')
                action = values.get('action', 'set')
                if not title:
                    self._send_json_response(400, json.dumps({'error': 'title required'}))
                    return
                from utils.library import normalize_title
                from utils.library_prefs import set_pending, clear_pending
                norm = normalize_title(title)
                if action == 'clear':
                    clear_pending(norm, episodes if episodes else None)
                else:
                    set_pending(norm, episodes, direction)
                self._send_json_response(200, json.dumps({'status': 'ok'}))
            except ValueError as e:
                self._send_json_response(400, json.dumps({'error': str(e)}))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[pending] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
            return

        if self.path == '/api/library/download':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                title = values.get('title', '').strip()
                media_type = values.get('type', 'show').strip()
                tmdb_id = values.get('tmdb_id')
                if tmdb_id is not None:
                    try:
                        tmdb_id = int(tmdb_id)
                    except (ValueError, TypeError):
                        tmdb_id = None

                if not title:
                    self._send_json_response(400, json.dumps({'error': 'title required'}))
                    return
                if media_type not in ('show', 'movie'):
                    self._send_json_response(400, json.dumps({'error': 'type must be "show" or "movie"'}))
                    return

                from utils.arr_client import get_download_service
                client, service_name = get_download_service(media_type)
                if client is None:
                    self._send_json_response(400, json.dumps({
                        'error': 'No download service configured. Add Sonarr/Radarr or Overseerr in Settings.'
                    }))
                    return

                # Determine download routing from source preference
                prefer_debrid = values.get('prefer_debrid')
                if prefer_debrid is None and service_name in ('sonarr', 'radarr'):
                    from utils.library_prefs import get_all_preferences
                    from utils.library import normalize_title
                    prefs = get_all_preferences()
                    nk = normalize_title(title)
                    pref = prefs.get(nk, 'none')
                    if pref == 'prefer-debrid':
                        prefer_debrid = True
                    elif pref == 'prefer-local':
                        prefer_debrid = False

                if service_name == 'sonarr':
                    season = values.get('season')
                    episodes = values.get('episodes', [])
                    if not isinstance(episodes, list):
                        self._send_json_response(400, json.dumps({
                            'error': 'episodes must be a list'
                        }))
                        return
                    if season is None or not episodes:
                        self._send_json_response(400, json.dumps({
                            'error': 'season and episodes required for Sonarr'
                        }))
                        return
                    try:
                        season = int(season)
                        episodes = [int(e) for e in episodes]
                    except (ValueError, TypeError):
                        self._send_json_response(400, json.dumps({
                            'error': 'season and episodes must be integers'
                        }))
                        return
                    result = client.ensure_and_search(title, tmdb_id, season, episodes, prefer_debrid=prefer_debrid)

                elif service_name == 'radarr':
                    result = client.ensure_and_search(title, tmdb_id, prefer_debrid=prefer_debrid)

                elif service_name == 'overseerr':
                    if media_type == 'show':
                        season = values.get('season')
                        if season is None:
                            self._send_json_response(400, json.dumps({
                                'error': 'season required for Overseerr TV requests'
                            }))
                            return
                        try:
                            seasons = [int(season)]
                        except (ValueError, TypeError):
                            self._send_json_response(400, json.dumps({
                                'error': 'season must be an integer'
                            }))
                            return
                        result = client.ensure_and_request_tv(title, tmdb_id, seasons)
                    else:
                        result = client.ensure_and_request_movie(title, tmdb_id)
                else:
                    result = {'status': 'error', 'message': f'Unknown service: {service_name}'}

                status_code = 200 if result.get('status') != 'error' else 400
                self._send_json_response(status_code, json.dumps(result))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[download] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
            return

        if self.path == '/api/library/download-local-fallback':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                title = values.get('title', '').strip()
                media_type = values.get('type', 'show').strip()
                tmdb_id = values.get('tmdb_id')
                if tmdb_id is not None:
                    try:
                        tmdb_id = int(tmdb_id)
                    except (ValueError, TypeError):
                        tmdb_id = None

                if not title:
                    self._send_json_response(400, json.dumps({'error': 'title required'}))
                    return
                if media_type not in ('show', 'movie'):
                    self._send_json_response(400, json.dumps({'error': 'type must be "show" or "movie"'}))
                    return

                from utils.arr_client import get_download_service
                client, service_name = get_download_service(media_type)
                if client is None:
                    self._send_json_response(400, json.dumps({
                        'error': 'No download service configured.'
                    }))
                    return

                from utils.library import normalize_title
                from utils.library_prefs import set_pending
                norm = normalize_title(title)

                fallback_triggered = False
                season = None
                if service_name == 'sonarr':
                    season = values.get('season')
                    episodes = values.get('episodes', [])
                    if not isinstance(episodes, list):
                        self._send_json_response(400, json.dumps({'error': 'episodes must be a list'}))
                        return
                    if season is None or not episodes:
                        self._send_json_response(400, json.dumps({'error': 'season and episodes required'}))
                        return
                    try:
                        season = int(season)
                        episodes = [int(e) for e in episodes]
                    except (ValueError, TypeError):
                        self._send_json_response(400, json.dumps({'error': 'season and episodes must be integers'}))
                        return
                    result = client.ensure_and_search(title, tmdb_id, season, episodes, prefer_debrid=False)
                    if result.get('status') in ('sent', 'pending'):
                        pending_eps = [{'season': season, 'episode': e} for e in episodes]
                        set_pending(norm, pending_eps, 'to-local-fallback')
                        fallback_triggered = True
                elif service_name == 'radarr':
                    result = client.ensure_and_search(title, tmdb_id, prefer_debrid=False)
                    if result.get('status') in ('sent', 'pending'):
                        set_pending(norm, [{'season': 0, 'episode': 0}], 'to-local-fallback')
                        fallback_triggered = True
                else:
                    result = {'status': 'error', 'message': f'Local fallback requires Sonarr/Radarr, got {service_name}'}

                if fallback_triggered:
                    try:
                        from utils.notifications import notify
                        ep_detail = f' S{season:02d}' if service_name == 'sonarr' else ''
                        notify('local_fallback_triggered',
                               f'Local Fallback: {title}{ep_detail}',
                               f'Downloading locally as debrid fallback via {service_name}')
                    except Exception:
                        pass
                    try:
                        from utils import history as _hist
                        episode_str = f'S{season:02d}' if service_name == 'sonarr' else None
                        _hist.log_event('local_fallback_triggered', title,
                                        episode=episode_str, source='library',
                                        detail=f'Local fallback download via {service_name}')
                    except Exception:
                        pass

                status_code = 200 if result.get('status') != 'error' else 400
                self._send_json_response(status_code, json.dumps(result))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[download-local-fallback] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
            return

        if self.path == '/api/library/remove-local':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                title = values.get('title', '').strip()
                media_type = values.get('type', 'show').strip()
                tmdb_id = values.get('tmdb_id')
                if tmdb_id is not None:
                    try:
                        tmdb_id = int(tmdb_id)
                    except (ValueError, TypeError):
                        tmdb_id = None
                episodes = values.get('episodes', [])
                if not title:
                    self._send_json_response(400, json.dumps({'error': 'title required'}))
                    return

                # Try Sonarr/Radarr first (preferred — updates their database)
                from utils.arr_client import get_download_service, SonarrClient, RadarrClient
                client, service_name = get_download_service(media_type)

                if service_name == 'sonarr' and episodes:
                    season = values.get('season')
                    if season is None:
                        self._send_json_response(400, json.dumps({
                            'error': 'season is required for Sonarr episode removal'
                        }))
                        return
                    try:
                        season = int(season)
                        ep_nums = [int(e) for e in episodes] if isinstance(episodes, list) else []
                    except (ValueError, TypeError):
                        self._send_json_response(400, json.dumps({
                            'error': 'season and episodes must be integers'
                        }))
                        return
                    if not ep_nums:
                        self._send_json_response(400, json.dumps({
                            'error': 'episodes list is empty'
                        }))
                        return
                    result = client.remove_episodes(title, tmdb_id, season, ep_nums)
                    if result.get('status') != 'error':
                        from utils.library import get_scanner, normalize_title
                        from utils.library_prefs import clear_pending
                        cleared = [{'season': season, 'episode': e} for e in ep_nums]
                        clear_pending(normalize_title(title), cleared)
                        scanner = get_scanner()
                        if scanner:
                            scanner.refresh()
                    status_code = 200 if result.get('status') != 'error' else 400
                    self._send_json_response(status_code, json.dumps(result))
                    return

                if service_name == 'radarr' and media_type == 'movie':
                    result = client.remove_movie(title, tmdb_id)
                    if result.get('status') != 'error':
                        from utils.library import get_scanner
                        scanner = get_scanner()
                        if scanner:
                            scanner.refresh()
                    status_code = 200 if result.get('status') != 'error' else 400
                    self._send_json_response(status_code, json.dumps(result))
                    return

                # Movie removal requires Radarr — no fallback
                if media_type == 'movie':
                    self._send_json_response(400, json.dumps({
                        'error': 'Movie removal requires Radarr. Configure Radarr in Settings.'
                    }))
                    return

                # Fallback: direct file deletion for TV (requires writable mount)
                if not episodes:
                    self._send_json_response(400, json.dumps({'error': 'episodes required'}))
                    return
                from utils.library import get_scanner, normalize_title
                scanner = get_scanner()
                if scanner is None:
                    self._send_json_response(503, json.dumps({'error': 'Scanner not initialized'}))
                    return
                if not scanner._local_tv_path:
                    self._send_json_response(400, json.dumps({
                        'error': 'No writable local library and no Sonarr/Radarr configured'
                    }))
                    return

                norm = normalize_title(title)
                resolved = []
                for ep in episodes:
                    try:
                        s, e = int(ep.get('season', 0)), int(ep.get('episode', 0))
                    except (ValueError, TypeError):
                        self._send_json_response(400, json.dumps({
                            'error': 'season and episode must be integers'
                        }))
                        return
                    local_path = scanner.get_local_episode_path(norm, s, e)
                    if local_path:
                        resolved.append({'path': local_path})
                if not resolved:
                    self._send_json_response(404, json.dumps({'error': 'No local episodes found'}))
                    return

                from utils.library_prefs import remove_local_episodes, clear_pending
                result = remove_local_episodes(resolved, scanner._local_tv_path)
                if result.get('removed', 0) > 0:
                    cleared = [{'season': int(ep.get('season', 0)), 'episode': int(ep.get('episode', 0))} for ep in episodes]
                    clear_pending(norm, cleared)
                scanner.refresh()
                self._send_json_response(200, json.dumps(result))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[remove] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
            return

        if self.path == '/api/library/switch-to-debrid':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                title = values.get('title', '').strip()
                if not title:
                    self._send_json_response(400, json.dumps({'error': 'title required'}))
                    return

                from utils.library import get_scanner, normalize_title
                scanner = get_scanner()
                if scanner is None:
                    self._send_json_response(503, json.dumps({'error': 'Scanner not initialized'}))
                    return

                rclone_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '').strip()
                symlink_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '').strip()
                local_tv = scanner._local_tv_path

                if not rclone_mount or not symlink_base:
                    self._send_json_response(400, json.dumps({
                        'error': 'BLACKHOLE_RCLONE_MOUNT and BLACKHOLE_SYMLINK_TARGET_BASE must be configured'
                    }))
                    return
                if not local_tv:
                    self._send_json_response(400, json.dumps({
                        'error': 'No local TV library configured (BLACKHOLE_LOCAL_LIBRARY_TV)'
                    }))
                    return

                norm = normalize_title(title)
                season_eps = values.get('episodes', [])

                # Build the list of episodes to switch
                to_switch = []
                not_on_debrid = 0
                with scanner._path_lock:
                    for ep in season_eps:
                        try:
                            s = int(ep.get('season', 0))
                            e = int(ep.get('episode', 0))
                        except (ValueError, TypeError):
                            continue
                        local_p = scanner._local_path_index.get((norm, s, e))
                        debrid_p = scanner._path_index.get((norm, s, e))
                        if local_p and debrid_p:
                            to_switch.append({
                                'local_path': local_p,
                                'debrid_path': debrid_p,
                                'season': s,
                                'episode': e,
                            })
                        elif local_p and not debrid_p:
                            not_on_debrid += 1

                if not to_switch and not_on_debrid == 0:
                    self._send_json_response(400, json.dumps({
                        'error': f'No matching episodes found for {title}'
                    }))
                    return

                if not to_switch and not_on_debrid > 0:
                    self._send_json_response(200, json.dumps({
                        'status': 'none_available',
                        'message': f'{not_on_debrid} episode(s) have no debrid copy available',
                        'switched': 0,
                        'not_on_debrid': not_on_debrid,
                    }))
                    return

                from utils.library_prefs import replace_local_with_symlinks, clear_pending
                result = replace_local_with_symlinks(to_switch, local_tv, rclone_mount, symlink_base)
                if not_on_debrid > 0:
                    result['not_on_debrid'] = not_on_debrid
                    result['message'] = (
                        f"Switched {result['switched']} episode(s) to debrid. "
                        f"{not_on_debrid} episode(s) kept local (no debrid copy)."
                    )
                else:
                    result['message'] = f"Switched {result['switched']} episode(s) to debrid."

                if result.get('switched', 0) > 0:
                    cleared_eps = [
                        {'season': e['season'], 'episode': e['episode']}
                        for e in to_switch if os.path.islink(e['local_path'])
                    ]
                    if cleared_eps:
                        clear_pending(norm, cleared_eps)

                scanner.refresh()
                status_code = 200 if result.get('switched', 0) > 0 else 400
                self._send_json_response(status_code, json.dumps(result))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[switch-to-debrid] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
            return

        if self.path == '/api/library/remove-debrid':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                title = values.get('title', '').strip()
                if not title:
                    self._send_json_response(400, json.dumps({'error': 'title required'}))
                    return

                from utils.debrid_client import get_debrid_client
                from utils.library import normalize_title
                client, service_name = get_debrid_client()
                if client is None:
                    self._send_json_response(400, json.dumps({
                        'error': 'No debrid provider configured (RD_API_KEY, AD_API_KEY, or TORBOX_API_KEY required)'
                    }))
                    return

                norm = normalize_title(title)
                year = values.get('year')
                if year is not None:
                    try:
                        year = int(year)
                    except (ValueError, TypeError):
                        year = None

                try:
                    matches = client.find_torrents_by_title(norm, target_year=year)
                except Exception as e:
                    logger.error(f"[remove-debrid] Failed to query debrid provider: {e}")
                    self._send_json_response(502, json.dumps({
                        'error': 'Failed to query debrid provider API'
                    }))
                    return

                self._send_json_response(200, json.dumps({
                    'status': 'found',
                    'service': service_name,
                    'title': title,
                    'normalized_title': norm,
                    'torrents': matches,
                    'count': len(matches),
                }))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[remove-debrid] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
            return

        if self.path == '/api/library/remove-debrid/confirm':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                torrent_ids = values.get('torrent_ids', [])
                if not isinstance(torrent_ids, list) or not torrent_ids:
                    self._send_json_response(400, json.dumps({'error': 'torrent_ids list required'}))
                    return
                if not all(isinstance(t, (str, int)) for t in torrent_ids):
                    self._send_json_response(400, json.dumps({'error': 'torrent_ids must contain strings or integers'}))
                    return
                from utils.debrid_client import get_debrid_client, MAX_BATCH_DELETE
                if len(torrent_ids) > MAX_BATCH_DELETE:
                    self._send_json_response(400, json.dumps({
                        'error': f'Maximum {MAX_BATCH_DELETE} torrents per request'
                    }))
                    return
                title = values.get('title', '').strip()
                requested_service = values.get('service', '').strip()

                client, service_name = get_debrid_client()
                if client is None:
                    self._send_json_response(400, json.dumps({
                        'error': 'No debrid provider configured'
                    }))
                    return

                if requested_service and requested_service != service_name:
                    self._send_json_response(409, json.dumps({
                        'error': f'Provider mismatch: found with {requested_service}, '
                                 f'but current provider is {service_name}'
                    }))
                    return

                deleted = 0
                failed = []
                for tid in torrent_ids:
                    tid_str = str(tid)
                    if client.delete_torrent(tid_str):
                        deleted += 1
                    else:
                        failed.append(tid_str)

                # Trigger library refresh — Zurg auto-detects torrent deletion
                # within its check_for_changes_every_secs cycle (typically 10s),
                # then rclone mount updates after RCLONE_DIR_CACHE_TIME expires.
                from utils.library import get_scanner
                scanner = get_scanner()
                if scanner:
                    scanner.refresh()

                if deleted > 0 and failed:
                    status = 'partial'
                elif deleted > 0:
                    status = 'removed'
                else:
                    status = 'error'

                result = {
                    'status': status,
                    'service': service_name,
                    'deleted': deleted,
                    'message': f'Removed {deleted} torrent(s) from {service_name}',
                }
                if title:
                    result['title'] = title
                if failed:
                    result['failed'] = failed
                    result['message'] += f' ({len(failed)} failed)'

                status_code = 200 if deleted > 0 else 400
                self._send_json_response(status_code, json.dumps(result))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[remove-debrid/confirm] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
            return

        if self.path == '/api/library/delete':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                title = values.get('title', '').strip()
                media_type = values.get('type', '').strip()
                if not title or media_type not in ('movie', 'show'):
                    self._send_json_response(400, json.dumps({'error': 'title and type (movie/show) required'}))
                    return
                tmdb_id = values.get('tmdb_id')
                if tmdb_id is not None:
                    try:
                        tmdb_id = int(tmdb_id)
                    except (ValueError, TypeError):
                        tmdb_id = None

                from utils.arr_client import get_download_service
                client, service_name = get_download_service(media_type)
                if client is None or service_name not in ('sonarr', 'radarr'):
                    self._send_json_response(400, json.dumps({
                        'error': f'Delete requires {"Sonarr" if media_type == "show" else "Radarr"} — configure it in Settings'
                    }))
                    return

                if media_type == 'show' and service_name == 'sonarr':
                    result = client.delete_series(title, tmdb_id=tmdb_id)
                elif media_type == 'movie' and service_name == 'radarr':
                    result = client.delete_movie(title, tmdb_id=tmdb_id)
                else:
                    self._send_json_response(400, json.dumps({
                        'error': f'Cannot delete {media_type} via {service_name}'
                    }))
                    return

                if result.get('status') == 'deleted':
                    # --- Cleanup pd_zurg artifacts for the deleted title ---
                    from utils.library import normalize_title, remove_title_symlinks
                    norm = normalize_title(title)
                    cleanup = {}

                    # Parse optional year for year-aware matching
                    year = values.get('year')
                    if year is not None:
                        try:
                            year = int(year)
                        except (ValueError, TypeError):
                            year = None

                    # 1. Remove debrid torrents (opt-out via delete_debrid=false)
                    delete_debrid = values.get('delete_debrid', True)
                    if str(delete_debrid).lower() in ('true', '1', 'yes'):
                        try:
                            from utils.debrid_client import get_debrid_client
                            dclient, dservice = get_debrid_client()
                            if dclient:
                                matches = dclient.find_torrents_by_title(norm, target_year=year)
                                deleted_count = 0
                                for m in matches:
                                    if dclient.delete_torrent(str(m['id'])):
                                        deleted_count += 1
                                if deleted_count:
                                    cleanup['debrid_torrents_removed'] = deleted_count
                                    logger.info(f"[delete] Removed {deleted_count} debrid torrent(s) for '{title}'")
                        except Exception as e:
                            logger.warning(f"[delete] Debrid cleanup failed for '{title}': {e}")
                            cleanup['debrid_error'] = str(e)

                    # 2. Remove local library symlinks
                    try:
                        removed_dirs = remove_title_symlinks(title, media_type, year=year)
                        if removed_dirs:
                            cleanup['symlinks_removed'] = len(removed_dirs)
                    except Exception as e:
                        logger.warning(f"[delete] Symlink cleanup failed for '{title}': {e}")

                    # 3. Clean up preferences and pending state
                    try:
                        from utils.library_prefs import remove_preference, clear_pending
                        remove_preference(norm)
                        clear_pending(norm)
                    except Exception as e:
                        logger.warning(f"[delete] Prefs/pending cleanup failed for '{title}': {e}")

                    # 4. Remove TMDB cache entry
                    try:
                        from utils.tmdb import remove_cached_entry
                        remove_cached_entry(norm, media_type, year=year)
                    except Exception as e:
                        logger.warning(f"[delete] TMDB cache cleanup failed for '{title}': {e}")

                    # 5. Refresh library scanner (after all cleanup)
                    try:
                        from utils.library import get_scanner
                        scanner = get_scanner()
                        if scanner:
                            scanner.refresh()
                    except Exception as e:
                        logger.warning(f"[delete] Scanner refresh failed: {e}")

                    try:
                        from utils import history as _hist
                        _hist.log_event('arr_deleted', title, source='library',
                                        detail=f'Deleted from {service_name}')
                    except Exception:
                        pass
                    if cleanup:
                        result['cleanup'] = cleanup
                    self._send_json_response(200, json.dumps(result))
                else:
                    self._send_json_response(400, json.dumps(result))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[delete] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
            return

        if self.path == '/api/settings/env':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 1_000_000:  # 1MB max
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return

                from utils.settings_api import write_env_values
                result = write_env_values(values)
                self._send_json_response(200, json.dumps(result))

                if result.get('status') == 'saved':
                    self.status_data_ref.add_event(
                        'settings', 'Environment settings saved via web UI'
                    )
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception as e:
                self._send_json_response(500, json.dumps({'error': str(e)}))
            return

        if self.path == '/api/settings/validate':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 1_000_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return

                from utils.settings_api import validate_env_values
                result = validate_env_values(values)
                self._send_json_response(200, json.dumps(result))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception as e:
                self._send_json_response(500, json.dumps({'error': str(e)}))
            return

        if self.path == '/api/settings/plex-debrid':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 5_000_000:  # 5MB max (settings.json can be large)
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return

                from utils.settings_api import write_plex_debrid_values
                result = write_plex_debrid_values(values)
                self._send_json_response(200, json.dumps(result))

                if result.get('status') == 'saved':
                    self.status_data_ref.add_event(
                        'settings', 'plex_debrid settings saved via web UI'
                    )
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception as e:
                self._send_json_response(500, json.dumps({'error': str(e)}))
            return

        if self.path == '/api/settings/oauth/start':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length)
                data = json.loads(body.decode('utf-8'))
                service = data.get('service', '')

                from utils.settings_api import oauth_start
                result = oauth_start(service)
                self._send_json_response(200, json.dumps(result))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception as e:
                self._send_json_response(500, json.dumps({'error': str(e)}))
            return

        if self.path == '/api/settings/oauth/poll':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length)
                data = json.loads(body.decode('utf-8'))
                service = data.get('service', '')
                device_code = data.get('device_code', '')

                from utils.settings_api import oauth_poll
                result = oauth_poll(service, device_code)
                self._send_json_response(200, json.dumps(result))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception as e:
                self._send_json_response(500, json.dumps({'error': str(e)}))
            return

        if self.path == '/api/settings/reset/plex-debrid':
            try:
                from utils.settings_api import get_plex_debrid_defaults
                defaults = get_plex_debrid_defaults()
                self._send_json_response(200, json.dumps(defaults))
            except Exception as e:
                self._send_json_response(500, json.dumps({'error': str(e)}))
            return

        if self.path == '/api/settings/reset/env':
            try:
                from utils.settings_api import get_env_defaults
                defaults = get_env_defaults()
                self._send_json_response(200, json.dumps(defaults))
            except Exception as e:
                self._send_json_response(500, json.dumps({'error': str(e)}))
            return

        if self.path.startswith('/api/tasks/') and self.path.endswith('/run'):
            # POST /api/tasks/{name}/run — trigger a task manually
            task_name = self.path[len('/api/tasks/'):-len('/run')]
            if task_name:
                from utils.task_scheduler import scheduler
                task_status = scheduler.get_task(task_name)
                if task_status is None:
                    self._send_json_response(404, json.dumps({
                        'error': f'Unknown task: {task_name}'
                    }))
                elif task_status.get('running'):
                    self._send_json_response(409, json.dumps({
                        'error': f'Task {task_name} is already running'
                    }))
                else:
                    scheduler.run_now(task_name)
                    self._send_json_response(200, json.dumps({
                        'status': 'started', 'task': task_name
                    }))
                    self.status_data_ref.add_event(
                        'admin', f'Manual run triggered for task: {task_name}'
                    )
            else:
                self._send_json_response(400, json.dumps({'error': 'Invalid task path'}))
            return

        if self.path.startswith('/api/restart/'):
            service = self.path.split('/')[-1]
            allowed = {'zurg', 'rclone', 'plex_debrid'}
            if service not in allowed:
                self._send_json_response(400, json.dumps({
                    'error': f'Unknown service: {service}. Must be one of: {", ".join(sorted(allowed))}'
                }))
                return

            try:
                from utils.processes import restart_service
                threading.Thread(
                    target=restart_service,
                    args=(service,),
                    daemon=True
                ).start()
                self._send_json_response(200, json.dumps({
                    'status': 'restarting', 'service': service
                }))
                self.status_data_ref.add_event(
                    'admin', f'Manual restart triggered for {service}'
                )
            except Exception as e:
                self._send_json_response(500, json.dumps({'error': str(e)}))
        elif self.path == '/api/blocklist':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
            except (json.JSONDecodeError, ValueError):
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
                return
            info_hash = (values.get('info_hash') or '').strip()
            title = (values.get('title') or '').strip()
            reason = (values.get('reason') or '').strip()
            if not info_hash and not title:
                self._send_json_response(400, json.dumps({'error': 'info_hash or title required'}))
                return
            # If no info_hash provided, use a prefixed hash of the title as a synthetic key.
            # The TITLE: prefix prevents collision with real BitTorrent SHA1 info hashes.
            if not info_hash:
                import hashlib
                info_hash = 'TITLE:' + hashlib.sha1(title.encode('utf-8')).hexdigest().upper()
            from utils import blocklist as blocklist_mod
            entry_id = blocklist_mod.add(info_hash, title, reason=reason, source='manual')
            if entry_id:
                self._send_json_response(200, json.dumps({'status': 'added', 'id': entry_id}))
            else:
                self._send_json_response(500, json.dumps({'error': 'Failed to add entry'}))
        elif self.path == '/api/search':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                imdb_id = (values.get('imdb_id') or '').strip()
                media_type = (values.get('type') or 'movie').strip()
                season = values.get('season')
                episode = values.get('episode')
                if not imdb_id:
                    self._send_json_response(400, json.dumps({'error': 'imdb_id required'}))
                    return
                import re as _re
                if not _re.match(r'^tt\d{7,8}$', imdb_id):
                    self._send_json_response(400, json.dumps({'error': 'imdb_id must be tt followed by 7-8 digits'}))
                    return
                if media_type not in ('movie', 'series'):
                    self._send_json_response(400, json.dumps({'error': 'type must be "movie" or "series"'}))
                    return
                if season is not None:
                    try:
                        season = int(season)
                        if season < 0 or season > 1000:
                            self._send_json_response(400, json.dumps({'error': 'season out of range'}))
                            return
                    except (ValueError, TypeError):
                        self._send_json_response(400, json.dumps({'error': 'season must be integer'}))
                        return
                if episode is not None:
                    try:
                        episode = int(episode)
                        if episode < 0 or episode > 10000:
                            self._send_json_response(400, json.dumps({'error': 'episode out of range'}))
                            return
                    except (ValueError, TypeError):
                        self._send_json_response(400, json.dumps({'error': 'episode must be integer'}))
                        return
                from utils.search import search_torrents
                results = search_torrents(imdb_id, media_type, season, episode)
                self._send_json_response(200, json.dumps({'results': results}))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[search] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
        elif self.path == '/api/search/add':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 100_000:
                    self._send_json_response(400, json.dumps({'error': 'Request body too large'}))
                    return
                body = self.rfile.read(content_length)
                values = json.loads(body.decode('utf-8'))
                if not isinstance(values, dict):
                    self._send_json_response(400, json.dumps({'error': 'Expected JSON object'}))
                    return
                info_hash = (values.get('info_hash') or '').strip()
                title = (values.get('title') or '').strip()[:500]
                if not info_hash:
                    self._send_json_response(400, json.dumps({'error': 'info_hash required'}))
                    return
                from utils.search import add_to_debrid
                result = add_to_debrid(info_hash, title=title)
                status_code = 200 if result.get('success') else 400
                self._send_json_response(status_code, json.dumps(result))
            except json.JSONDecodeError:
                self._send_json_response(400, json.dumps({'error': 'Invalid JSON'}))
            except Exception:
                logger.exception("[search/add] Unexpected error")
                self._send_json_response(500, json.dumps({'error': 'Internal server error'}))
        else:
            self.send_response(404)
            self.end_headers()

    def do_DELETE(self):
        if not self.auth_credentials:
            self._send_json_response(403, json.dumps({
                'error': 'This endpoint requires STATUS_UI_AUTH to be configured'
            }))
            return
        if not self._check_auth():
            return
        if self.path == '/api/history':
            from utils import history as history_mod
            history_mod.clear()
            self._send_json_response(200, json.dumps({'status': 'cleared'}))
        elif self.path == '/api/blocklist':
            confirm = self.headers.get('X-Confirm-Clear', '')
            if confirm != 'true':
                self._send_json_response(400, json.dumps({
                    'error': 'Set X-Confirm-Clear: true header to confirm'
                }))
                return
            from utils import blocklist as blocklist_mod
            blocklist_mod.clear()
            self._send_json_response(200, json.dumps({'status': 'cleared'}))
        elif self.path.startswith('/api/blocklist/'):
            entry_id = self.path[len('/api/blocklist/'):]
            if not entry_id:
                self._send_json_response(400, json.dumps({'error': 'Entry ID required'}))
                return
            from utils import blocklist as blocklist_mod
            if blocklist_mod.remove(entry_id):
                self._send_json_response(200, json.dumps({'status': 'removed'}))
            else:
                self._send_json_response(404, json.dumps({'error': 'Entry not found'}))
        else:
            self.send_response(404)
            self.end_headers()

    def _send_auth_required(self):
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm="pd_zurg"')
        self.end_headers()

    def _accepts_gzip(self):
        """Check if the client accepts gzip encoding."""
        accept = self.headers.get('Accept-Encoding', '')
        return 'gzip' in accept

    def _gzip_body(self, body):
        """Compress body with gzip, using cache for repeated content."""
        content_hash = hashlib.md5(body, usedforsecurity=False).hexdigest()
        with _gzip_cache_lock:
            cached = _gzip_cache.get(content_hash)
            if cached is not None:
                return cached
        compressed = gzip_mod.compress(body, compresslevel=6)
        with _gzip_cache_lock:
            # Re-check: another thread may have inserted while we compressed
            if content_hash in _gzip_cache:
                return _gzip_cache[content_hash]
            if len(_gzip_cache) >= _GZIP_CACHE_MAX:
                try:
                    del _gzip_cache[next(iter(_gzip_cache))]
                except StopIteration:
                    pass
            _gzip_cache[content_hash] = compressed
        return compressed

    def _send_html_response(self, html_bytes):
        """Send an HTML response with gzip compression, ETag, and cache headers."""
        etag = '"' + hashlib.md5(html_bytes, usedforsecurity=False).hexdigest() + '"'

        # Check If-None-Match for 304
        if_none_match = self.headers.get('If-None-Match', '')
        if etag in [t.strip() for t in if_none_match.split(',')] or if_none_match == '*':
            self.send_response(304)
            self.send_header('ETag', etag)
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Vary', 'Accept-Encoding')
            self.end_headers()
            return

        body = html_bytes
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('ETag', etag)
        self.send_header('Cache-Control', 'no-cache')

        if self._accepts_gzip() and len(body) >= _GZIP_MIN_SIZE:
            body = self._gzip_body(body)
            self.send_header('Content-Encoding', 'gzip')
            self.send_header('Vary', 'Accept-Encoding')

        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json_response(self, code, data):
        """Send a JSON response with gzip compression and cache headers."""
        body = data.encode() if isinstance(data, str) else data
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store')

        if self._accepts_gzip() and len(body) >= _GZIP_MIN_SIZE:
            # Compress inline — don't pollute the cache with one-off JSON
            body = gzip_mod.compress(body, compresslevel=6)
            self.send_header('Content-Encoding', 'gzip')
            self.send_header('Vary', 'Accept-Encoding')

        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self):
        """Verify basic auth credentials. Returns True if valid, sends 401 if not."""
        if not self.auth_credentials:
            return True
        auth_header = self.headers.get('Authorization', '')
        if not auth_header.startswith('Basic '):
            self._send_auth_required()
            return False
        raw = auth_header[6:]
        if len(raw) > 256:
            self._send_auth_required()
            return False
        try:
            decoded = base64.b64decode(raw, validate=True).decode('utf-8')
            if not hmac.compare_digest(decoded.encode(), self.auth_credentials.encode()):
                self._send_auth_required()
                return False
        except (ValueError, UnicodeDecodeError):
            self._send_auth_required()
            return False
        return True

    def log_message(self, format, *args):
        pass  # Suppress default request logging


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def setup():
    """Start the status web UI server if enabled."""
    enabled = os.environ.get('STATUS_UI_ENABLED', 'false').lower() == 'true'
    if not enabled:
        return

    try:
        port = int(os.environ.get('STATUS_UI_PORT', '8080'))
        if not (1 <= port <= 65535):
            raise ValueError("port out of range")
    except ValueError:
        logger.error("Invalid STATUS_UI_PORT, defaulting to 8080")
        port = 8080
    auth = os.environ.get('STATUS_UI_AUTH')

    StatusHandler.status_data_ref = status_data
    StatusHandler.auth_credentials = auth if auth and ':' in auth else None

    # Initialize library scanner
    try:
        from utils import library as library_mod
        library_mod.setup()
    except Exception as e:
        logger.error(f"Failed to initialize library scanner: {e}")

    server = http.server.ThreadingHTTPServer(('0.0.0.0', port), StatusHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Status UI started on port {port}")

    status_data.add_event('status_ui', f'Dashboard available on port {port}')
