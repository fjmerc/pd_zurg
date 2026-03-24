"""Lightweight status web UI and JSON API.

Provides an at-a-glance dashboard showing service connectivity, process health,
mount status, system resources (cgroup-aware), and recent events. Uses Python's
built-in http.server — no framework dependencies.
"""

import base64
import collections
import glob as glob_mod
import hmac
import http.server
import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
from utils.logger import get_logger

logger = get_logger()


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
        log_file = max(log_files, key=os.path.getmtime)  # Most recently modified

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
    'TZ',
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
        services.append(svc)

    # Plex
    plex_addr = os.environ.get('PLEX_ADDRESS') or _get_secret_or_env('plex_address', 'PLEX_ADDRESS')
    plex_token = os.environ.get('PLEX_TOKEN') or _get_secret_or_env('plex_token', 'PLEX_TOKEN')
    if plex_addr and plex_token:
        svc, resp = _check_service(
            'Plex', 'media_server',
            f'{plex_addr}/identity',
            headers={'X-Plex-Token': plex_token, 'Accept': 'application/json'})
        services.append(svc)

    # Jellyfin
    jf_addr = os.environ.get('JF_ADDRESS') or _get_secret_or_env('jf_address', 'JF_ADDRESS')
    jf_key = os.environ.get('JF_API_KEY') or _get_secret_or_env('jf_api_key', 'JF_API_KEY')
    if jf_addr and jf_key:
        svc, resp = _check_service(
            'Jellyfin', 'media_server',
            f'{jf_addr}/System/Info',
            headers={'X-Emby-Token': jf_key})
        services.append(svc)

    # Overseerr / Jellyseerr
    seerr_addr = os.environ.get('SEERR_ADDRESS') or _get_secret_or_env('seerr_address', 'SEERR_ADDRESS')
    seerr_key = os.environ.get('SEERR_API_KEY') or _get_secret_or_env('seerr_api_key', 'SEERR_API_KEY')
    if seerr_addr and seerr_key:
        svc, resp = _check_service(
            'Overseerr', 'automation',
            f'{seerr_addr}/api/v1/status',
            headers={'X-Api-Key': seerr_key})
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
        try:
            if os.path.exists('/data'):
                for entry_name in os.listdir('/data'):
                    path = os.path.join('/data', entry_name)
                    try:
                        mounted = os.path.ismount(path)
                        accessible = os.access(path, os.R_OK)
                        mounts.append({
                            'path': path,
                            'mounted': mounted,
                            'accessible': accessible,
                        })
                        mount_history.record(path, mounted, accessible)
                    except OSError:
                        mounts.append({'path': path, 'mounted': False, 'accessible': False})
                        mount_history.record(path, False, False)
        except OSError:
            pass

        with self._lock:
            events = list(self.recent_events)
            error_count = self.error_count

        return {
            'version': self.version,
            'uptime_seconds': int(time.time() - self.start_time),
            'processes': processes,
            'mounts': mounts,
            'services': check_services(),
            'system': get_system_stats(),
            'recent_events': events,
            'error_count': error_count,
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
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark light">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⚡</text></svg>">
<title>pd_zurg Status</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--border2:#21262d;--text:#c9d1d9;--text2:#8b949e;--text3:#636e7b;--blue:#58a6ff;--green:#3fb950;--red:#f85149;--yellow:#d29922;--orange:#db6d28}
[data-theme="light"]{--bg:#f6f8fa;--card:#ffffff;--border:#d0d7de;--border2:#d8dee4;--text:#1f2328;--text2:#656d76;--text3:#8b949e;--blue:#0969da;--green:#1a7f37;--red:#cf222e;--yellow:#9a6700;--orange:#bc4c00}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);padding:20px;max-width:1200px;margin:0 auto}
a{color:var(--blue);text-decoration:none}
h1{color:var(--blue);margin-bottom:4px;font-size:1.6em;font-weight:600}
.header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px;gap:10px}
.meta{color:var(--text2);font-size:.85em;margin-bottom:20px}
.banner{padding:12px 16px;border-radius:8px;margin-bottom:16px;font-size:.9em;font-weight:500;display:none}
.banner.warn{display:block;background:#d299221a;border:1px solid var(--yellow);color:var(--yellow)}
.banner.crit{display:block;background:#f851491a;border:1px solid var(--red);color:var(--red)}
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
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;vertical-align:middle}
.dot.green{background:var(--green)}.dot.red{background:var(--red);border-radius:2px}.dot.yellow{background:transparent;border:2px solid var(--yellow);width:8px;height:8px}
.svc-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px}
.svc-item{display:flex;align-items:center;padding:10px 12px;background:var(--bg);border-radius:6px;border:1px solid var(--border2)}
.svc-item .svc-info{flex:1;margin-left:8px}
.svc-item .svc-name{font-size:.85em;font-weight:500;color:var(--text)}
.svc-item .svc-detail{font-size:.75em;color:var(--text2);margin-top:2px}
.svc-item .svc-badge{font-size:.7em;padding:2px 6px;border-radius:4px;font-weight:500;margin-left:8px}
.svc-item .svc-badge.premium{background:#3fb9501a;color:var(--green)}
.svc-item .svc-badge.warn{background:#d299221a;color:var(--yellow)}
.svc-item .svc-badge.crit{background:#f851491a;color:var(--red)}
.events{max-height:280px;overflow-y:auto}
.event{padding:5px 0;border-bottom:1px solid var(--border2);font-size:.8em;display:flex;gap:8px}
.event .time{color:var(--text3);min-width:55px;font-family:monospace;font-size:.85em}
.event .comp{color:var(--blue);font-weight:500;min-width:70px}
.event.error .msg{color:var(--red)}.event.warning .msg{color:var(--yellow)}
.stat-value{font-size:1.8em;font-weight:600;color:var(--blue)}
.stat-label{font-size:.75em;color:var(--text2);margin-top:2px}
.stats-row{display:flex;gap:32px}.stats-row>div{flex:1;text-align:center}
.btn-restart{background:none;border:1px solid var(--border);color:var(--text2);border-radius:4px;cursor:pointer;padding:2px 8px;font-size:.8em}
.btn-restart:hover{border-color:var(--blue);color:var(--blue)}
.btn-restart:disabled{opacity:.4;cursor:not-allowed}
.log-controls{display:flex;gap:8px;align-items:center;margin-bottom:8px}
.log-controls select{background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 8px;font-size:.8em}
.log-controls label{font-size:.8em;color:var(--text2)}
#log-content{max-height:350px;overflow-y:auto;background:var(--bg);border:1px solid var(--border2);border-radius:4px;padding:8px;font-size:.75em;line-height:1.5;white-space:pre-wrap;word-break:break-word}
.log-line.error{color:var(--red)}.log-line.warning{color:var(--yellow)}.log-line.debug{color:var(--text3)}
details{margin-top:0}
details summary{cursor:pointer;color:var(--text2);font-size:.8em;padding:4px 0}
details summary:hover{color:var(--blue)}
.cfg-table td{font-family:monospace;font-size:.8em}
.cfg-table td:first-child{color:var(--blue);font-weight:500;white-space:nowrap;padding-right:16px}
.mount-timeline{margin-top:8px}
.mt-row{display:flex;align-items:center;gap:8px;margin-bottom:4px;font-size:.8em}
.mt-path{color:var(--text2);min-width:120px;overflow:hidden;text-overflow:ellipsis}
.mt-blocks{display:flex;gap:1px;flex:1}
.mt-block{height:16px;min-width:3px;flex:1;border-radius:2px}
.mt-block.ok{background:var(--green)}.mt-block.down{background:var(--red)}.mt-block.partial{background:var(--yellow)}
.mt-block:hover{opacity:.8}
.footer{color:var(--text3);font-size:.78em;text-align:right;margin-top:12px;display:flex;justify-content:flex-end;align-items:center;gap:8px}
#conn-status{color:var(--red);font-weight:500}
#log-search:focus{border-color:var(--blue)}
#log-content.nowrap{white-space:pre;overflow-x:auto;word-break:normal}
.theme-toggle{background:none;border:1px solid var(--border);color:var(--text2);border-radius:6px;cursor:pointer;padding:4px 8px;font-size:.85em;line-height:1;transition:border-color .15s,color .15s}
.theme-toggle:hover{border-color:var(--blue);color:var(--blue)}
[data-theme="light"] .svc-item{background:var(--card);border-color:var(--border)}
dialog{background:var(--card);color:var(--text);border:1px solid var(--border);border-radius:10px;padding:24px;max-width:380px;box-shadow:0 8px 32px rgba(0,0,0,.5)}
dialog::backdrop{background:rgba(0,0,0,.6);backdrop-filter:blur(2px)}
dialog h3{margin-bottom:12px;font-size:1em;color:var(--text)}
dialog p{margin-bottom:20px;font-size:.9em;color:var(--text2)}
dialog .dlg-actions{display:flex;gap:8px;justify-content:flex-end}
dialog .dlg-btn{padding:8px 18px;border-radius:6px;font-size:.85em;cursor:pointer;border:none;font-weight:500}
dialog .dlg-cancel{background:var(--border);color:var(--text)}
dialog .dlg-confirm{background:var(--blue);color:#fff}
@media(prefers-reduced-motion:reduce){*{animation-duration:.01ms!important;animation-iteration-count:1!important;transition-duration:.01ms!important}}
</style>
<script>(function(){try{var t=localStorage.getItem('pd_zurg_theme');if(t){document.documentElement.setAttribute('data-theme',t);document.querySelector('meta[name="color-scheme"]').content=t==='light'?'light':'dark';}}catch(e){}})()</script>
</head>
<body>
<div class="header"><h1>pd_zurg</h1><span class="meta" id="header-meta"></span><a href="/settings" style="font-size:.85em;margin-left:auto;margin-right:12px">Settings</a><button class="theme-toggle" onclick="toggleTheme()" title="Toggle light/dark theme" id="theme-btn">☀️</button></div>
<div class="meta">Uptime: <span id="uptime"></span></div>
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
    <table><thead><tr><th>Name</th><th style="text-align:center">PID</th><th style="text-align:center">Restarts</th><th>Status</th><th id="actions-hdr"></th></tr></thead>
    <tbody id="procs"></tbody></table>
  </div>
  <div class="card">
    <h2>Mounts</h2>
    <table><thead><tr><th>Path</th><th>Mounted</th><th>Accessible</th></tr></thead>
    <tbody id="mounts"></tbody></table>
    <div class="mount-timeline" id="mount-timeline"></div>
  </div>
</div>
<div class="grid">
  <div class="card">
    <h2>System</h2>
    <div class="stats-row">
      <div><div class="stat-value" id="mem-used">-</div><div class="stat-label" id="mem-label">Memory Used</div></div>
      <div><div class="stat-value" id="cpu-used">-</div><div class="stat-label" id="cpu-label">CPU</div></div>
    </div>
  </div>
  <div class="card">
    <h2>Recent Events</h2>
    <div class="events" id="events"></div>
  </div>
</div>
<div class="grid full">
  <div class="card">
    <h2>Logs</h2>
    <div class="log-controls">
      <select id="log-level" onchange="updateLogs()">
        <option value="">All Levels</option>
        <option value="ERROR">Error</option>
        <option value="WARNING">Warning</option>
        <option value="INFO">Info</option>
        <option value="DEBUG">Debug</option>
      </select>
      <input type="text" id="log-search" placeholder="Search logs..." oninput="filterLogs()" style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:4px 8px;font-size:.8em;color:var(--text);outline:none;min-width:100px">
      <label><input type="checkbox" id="log-wrap" checked onchange="toggleLogWrap()"> Wrap</label>
      <label><input type="checkbox" id="log-autoscroll" checked> Auto-scroll</label>
    </div>
    <div id="log-content"></div>
  </div>
</div>
<div class="grid full">
  <div class="card">
    <details>
      <summary>Running Configuration (click to expand)</summary>
      <table class="cfg-table" id="config-table"><tbody></tbody></table>
    </details>
  </div>
</div>
<div class="grid full">
  <div class="card">
    <details>
      <summary>How it works (click to expand)</summary>
      <style>
.wf{margin:16px 0 8px}.wf-title{font-size:.8em;font-weight:600;color:var(--text);margin-bottom:10px;display:flex;align-items:center;gap:8px}.wf-title span{font-size:.85em;font-weight:400;color:var(--text3)}
.wf-row{display:flex;align-items:center;flex-wrap:wrap;gap:0;margin-bottom:6px}
.wf-node{padding:8px 14px;border-radius:8px;background:var(--bg);border:1.5px solid var(--border);font-size:.8em;font-weight:600;text-align:center;white-space:nowrap;min-width:80px}
.wf-node small{display:block;font-weight:400;color:var(--text2);font-size:.85em;margin-top:2px}
.wf-node.green{border-color:var(--green);color:var(--green)}.wf-node.blue{border-color:var(--blue);color:var(--blue)}.wf-node.yellow{border-color:var(--yellow);color:var(--yellow)}.wf-node.orange{border-color:var(--orange);color:var(--orange)}.wf-node.purple{border-color:#bc8cff;color:#bc8cff}.wf-node.muted{border-color:var(--border);color:var(--text2)}
.wf-arrow{color:var(--text3);font-size:1.1em;padding:0 6px;flex-shrink:0}
.wf-label{font-size:.65em;color:var(--text3);text-align:center;margin-top:-4px;margin-bottom:2px;padding:0 6px}
.wf-branch{display:flex;flex-direction:column;gap:10px;margin:8px 0 0 0}
.wf-branch>div{padding:12px;border-radius:8px;border:1px solid var(--border2);background:var(--bg)}
.wf-branch>div>.wf-branch-title{font-size:.75em;font-weight:600;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border2)}
.wf-branch>div>.wf-branch-title.local{color:var(--text2)}.wf-branch>div>.wf-branch-title.debrid{color:var(--yellow)}
.wf-glossary{margin-top:16px;padding-top:14px;border-top:1px solid var(--border2)}
.wf-glossary-title{font-size:.8em;font-weight:600;color:var(--text);margin-bottom:10px}
.wf-glossary dl{margin:0;font-size:.78em;line-height:1.6}
.wf-glossary dt{color:var(--text);font-weight:600;margin-top:8px}
.wf-glossary dd{color:var(--text2);margin:0 0 0 0;padding:0 0 8px 0;border-bottom:1px solid var(--border2)}
@media(max-width:600px){.wf-row{gap:2px}.wf-node{padding:6px 8px;font-size:.72em;min-width:60px}.wf-arrow{font-size:.9em;padding:0 3px}}
      </style>

      <!-- Workflow 1: Watchlist / plex_debrid flow -->
      <div class="wf">
        <div class="wf-title">Watchlist Flow <span>&mdash; plex_debrid monitors your watchlists automatically</span></div>
        <div class="wf-row">
          <div class="wf-node muted">Watchlist<small>Plex / Trakt / Overseerr</small></div>
          <div class="wf-arrow">&rarr;</div>
          <div class="wf-node green">plex_debrid<small>Search &amp; Match</small></div>
          <div class="wf-arrow">&rarr;</div>
          <div class="wf-node yellow">Real-Debrid<small>Cloud Cache</small></div>
          <div class="wf-arrow">&rarr;</div>
          <div class="wf-node blue">Zurg<small>WebDAV</small></div>
          <div class="wf-arrow">&rarr;</div>
          <div class="wf-node blue">rclone<small>/data mount</small></div>
          <div class="wf-arrow">&rarr;</div>
          <div class="wf-node green">Plex / Jellyfin<small>Stream</small></div>
        </div>
      </div>

      <!-- Workflow 2: Arr + Blackhole flow -->
      <div class="wf">
        <div class="wf-title">Arr + Blackhole Flow <span>&mdash; Sonarr/Radarr with tag-based routing</span></div>
        <div class="wf-row">
          <div class="wf-node muted">Overseerr<small>Requests</small></div>
          <div class="wf-arrow">&rarr;</div>
          <div class="wf-node muted">Sonarr / Radarr<small>Tag-based routing</small></div>
        </div>
        <div class="wf-branch">
          <div>
            <div class="wf-branch-title local">Local Path &mdash; no debrid tag</div>
            <div class="wf-row">
              <div class="wf-node purple">VPN<small>gluetun / Mullvad</small></div>
              <div class="wf-arrow">&rarr;</div>
              <div class="wf-node muted">qBittorrent / Usenet<small>Download</small></div>
              <div class="wf-arrow">&rarr;</div>
              <div class="wf-node muted">Local Disk<small>Storage</small></div>
              <div class="wf-arrow">&rarr;</div>
              <div class="wf-node green">Plex<small>Stream</small></div>
            </div>
          </div>
          <div>
            <div class="wf-branch-title debrid">Debrid Path &mdash; tag: debrid &mdash; no VPN needed</div>
            <div class="wf-row">
              <div class="wf-node orange">Blackhole<small>/watch folder</small></div>
              <div class="wf-arrow">&rarr;</div>
              <div class="wf-node orange">pd_zurg<small>Send to RD</small></div>
              <div class="wf-arrow">&rarr;</div>
              <div class="wf-node yellow">Real-Debrid<small>Cache</small></div>
              <div class="wf-arrow">&rarr;</div>
              <div class="wf-node blue">Zurg / rclone<small>Mount</small></div>
              <div class="wf-arrow">&rarr;</div>
              <div class="wf-node orange">Symlinks<small>/completed</small></div>
              <div class="wf-arrow">&rarr;</div>
              <div class="wf-node muted">Sonarr / Radarr<small>Import</small></div>
              <div class="wf-arrow">&rarr;</div>
              <div class="wf-node green">Plex<small>Stream</small></div>
          </div>
        </div>
      </div>

      <!-- Component glossary -->
      <div class="wf-glossary">
        <div class="wf-glossary-title">Glossary</div>
        <dl>
          <dt style="color:var(--green)">Plex / Jellyfin</dt>
          <dd>Media server that streams your library to any device. Scans local disks and rclone mounts for content.</dd>
          <dt style="color:var(--text)">Overseerr</dt>
          <dd>Request management UI. Users browse and request movies/shows, which get routed to Sonarr/Radarr for automated downloading.</dd>
          <dt style="color:var(--text)">Sonarr / Radarr</dt>
          <dd>Automated TV show and movie managers. Monitor indexers, manage quality profiles, rename files, and track new episodes. Route downloads to different clients using tags.</dd>
          <dt style="color:var(--green)">plex_debrid</dt>
          <dd>Monitors Plex/Trakt/Overseerr watchlists. Searches torrent indexers for cached releases matching your quality profile and sends the best match to your debrid service.</dd>
          <dt style="color:var(--yellow)">Real-Debrid / AllDebrid</dt>
          <dd>Cloud torrent cache. Stores popular torrents on fast servers. Instant access via HTTPS &mdash; no seeding, no VPN needed on this path.</dd>
          <dt style="color:var(--blue)">Zurg</dt>
          <dd>Connects to your debrid API and serves your cached content as a WebDAV file server. Makes your cloud library look like local files.</dd>
          <dt style="color:var(--blue)">rclone</dt>
          <dd>Mounts the Zurg WebDAV server as a local directory at <code style="color:var(--green);font-size:.95em">/data/pd_zurg</code> so your media server can access the files.</dd>
          <dt style="color:var(--orange)">Blackhole</dt>
          <dd>Watches a folder for .torrent/.magnet files dropped by Sonarr/Radarr. Sends them to Real-Debrid, waits for content on the mount, then creates symlinks in a completed directory for Sonarr/Radarr to import. Optional dedup checks your local library first.</dd>
          <dt>qBittorrent / Usenet</dt>
          <dd>Traditional download clients for the local path. qBittorrent handles torrents, SABnzbd/NZBGet handle Usenet NZBs. Both store files to local disk.</dd>
          <dt style="color:#bc8cff">VPN</dt>
          <dd>Encrypts torrent/Usenet traffic and hides your IP. Required for the local download path (gluetun, Mullvad, etc). Not needed for the debrid path &mdash; that&rsquo;s just HTTPS to Real-Debrid&rsquo;s API.</dd>
        </dl>
      </div>
    </details>
  </div>
</div>
<dialog id="confirm-dialog"><h3 id="dlg-title"></h3><p id="dlg-msg"></p><div class="dlg-actions"><button class="dlg-btn dlg-cancel" onclick="document.getElementById('confirm-dialog').close('cancel')">Cancel</button><button class="dlg-btn dlg-confirm" id="dlg-ok">Confirm</button></div></dialog>
<div class="footer"><span id="conn-status"></span>Refresh: <select id="refresh-interval" onchange="setRefreshInterval(this.value)" style="background:var(--bg);color:var(--text2);border:1px solid var(--border);border-radius:3px;font-size:1em;padding:1px 4px"><option value="5">5s</option><option value="10" selected>10s</option><option value="30">30s</option><option value="0">Paused</option></select></div>
<script>
// Theme toggle
function applyTheme(theme){
  document.documentElement.setAttribute('data-theme',theme);
  document.querySelector('meta[name="color-scheme"]').content=theme==='light'?'light':'dark';
  document.getElementById('theme-btn').textContent=theme==='light'?'\U0001F319':'\u2600\uFE0F';
}
function toggleTheme(){
  const cur=document.documentElement.getAttribute('data-theme')||'dark';
  const next=cur==='dark'?'light':'dark';
  applyTheme(next);
  try{localStorage.setItem('pd_zurg_theme',next);}catch(e){}
}

// Sync theme button icon on load (head script sets data-theme before body renders)
(function(){const t=document.documentElement.getAttribute('data-theme');if(t)applyTheme(t);})();

// Log wrap toggle
function toggleLogWrap(){
  const el=document.getElementById('log-content');
  const wrap=document.getElementById('log-wrap').checked;
  el.classList.toggle('nowrap',!wrap);
  try{localStorage.setItem('pd_zurg_log_wrap',wrap?'1':'0');}catch(e){}
}
(function(){try{const w=localStorage.getItem('pd_zurg_log_wrap');if(w==='0'){document.getElementById('log-wrap').checked=false;document.getElementById('log-content').classList.add('nowrap');}}catch(e){}})();

function fmt(s){
  if(s<60)return s+'s';
  if(s<3600)return Math.floor(s/60)+'m '+s%60+'s';
  const h=Math.floor(s/3600),m=Math.floor((s%3600)/60);
  if(h>=24){const d=Math.floor(h/24);return d+'d '+(h%24)+'h';}
  return h+'h '+m+'m';
}
function fmtBytes(b){
  if(b>1073741824)return(b/1073741824).toFixed(1)+'G';
  if(b>1048576)return(b/1048576).toFixed(0)+'M';
  return(b/1024).toFixed(0)+'K';
}
function timeAgo(ts){
  const sec=Math.floor((Date.now()-new Date(ts).getTime())/1000);
  if(sec<60)return sec+'s ago';
  if(sec<3600)return Math.floor(sec/60)+'m ago';
  if(sec<86400)return Math.floor(sec/3600)+'h ago';
  return Math.floor(sec/86400)+'d ago';
}
let _failCount=0;
let _statusTimer,_logTimer,_mtTimer;
let _refreshSec=10;
function esc(s){const d=document.createElement('div');d.appendChild(document.createTextNode(String(s)));return d.innerHTML;}
function dot(ok){return '<span class="dot '+(ok?'green':'red')+'"></span>'+(ok?'Running':'Stopped');}
function mdot(ok,yes,no){return '<span class="dot '+(ok?'green':'red')+'"></span>'+(ok?(yes||'Yes'):(no||'No'));}
function sdot(s){return '<span class="dot '+(s==='ok'?'green':'red')+'"></span>';}

function renderServices(svcs){
  if(!svcs||!svcs.length)return '<div style="color:var(--text2);padding:8px">No services configured</div>';
  let h='';
  svcs.forEach(s=>{
    h+='<div class="svc-item">'+sdot(s.status)+'<div class="svc-info"><div class="svc-name">'+esc(s.name)+'</div>';
    if(s.status==='ok'){
      let det='Connected';
      if(s.username)det=esc(s.username);
      h+='<div class="svc-detail">'+det+'</div>';
    }else{
      h+='<div class="svc-detail" style="color:var(--red)">'+(s.detail?esc(s.detail):'Unreachable')+'</div>';
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

function update(){
  fetch('/api/status').then(r=>r.json()).then(d=>{
    document.getElementById('header-meta').textContent='v'+d.version;
    document.getElementById('uptime').textContent=fmt(d.uptime_seconds);
    document.getElementById('errors').textContent=d.error_count;
    document.getElementById('error-line').style.display=d.error_count>0?'block':'none';

    // Banner for RD premium expiry
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
    if(!bannerShown)banner.className='banner';

    // Services
    document.getElementById('services').innerHTML=renderServices(d.services);

    // Processes (with optional restart buttons when auth is configured)
    let p='';const hasAuth=window._hasAuth;
    d.processes.forEach(x=>{
      const svcName=x.name.split(' w/ ')[0].toLowerCase();
      const restartBtn=hasAuth?'<td><button class="btn-restart" onclick="restartSvc(this,\\x27'+esc(svcName)+'\\x27)" title="Restart">Restart</button></td>':'<td></td>';
      p+='<tr><td>'+esc(x.name)+'</td><td>'+(x.pid||'-')+'</td><td>'+(x.restart_count||0)+'</td><td>'+dot(x.running)+'</td>'+restartBtn+'</tr>';
    });
    document.getElementById('procs').innerHTML=p||'<tr><td colspan="5" style="color:var(--text2)">No processes</td></tr>';
    document.getElementById('actions-hdr').textContent=hasAuth?'Actions':'';

    // Mounts
    let m='';d.mounts.forEach(x=>{m+='<tr><td>'+esc(x.path)+'</td><td>'+mdot(x.mounted,'Yes','No')+'</td><td>'+mdot(x.accessible,'Yes','No')+'</td></tr>';});
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
    // System — CPU
    if(d.system.cpu_percent!==undefined){
      document.getElementById('cpu-used').textContent=d.system.cpu_percent.toFixed(1)+'%';
      document.getElementById('cpu-label').textContent='CPU';
    }else if(d.system.cpu_usage_usec!==undefined){
      document.getElementById('cpu-used').textContent=(d.system.cpu_usage_usec/1000000).toFixed(1)+'s';
      document.getElementById('cpu-label').textContent='CPU Time';
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
  }).catch(()=>{
    _failCount++;
    if(_failCount>=3)document.getElementById('conn-status').textContent='Connection lost \u2014 retrying... ';
  });
}
// Detect auth by trying an auth-required endpoint
window._hasAuth=false;
fetch('/api/restart/test',{method:'POST'}).then(r=>{window._hasAuth=r.status!==403;}).catch(()=>{});

// Styled confirm dialog
function showConfirm(title,msg){
  return new Promise(resolve=>{
    const dlg=document.getElementById('confirm-dialog');
    document.getElementById('dlg-title').textContent=title;
    document.getElementById('dlg-msg').textContent=msg;
    const okBtn=document.getElementById('dlg-ok');
    const handler=()=>{dlg.close('ok');};
    okBtn.onclick=handler;
    dlg.onclose=()=>{okBtn.onclick=null;resolve(dlg.returnValue==='ok');};
    dlg.showModal();
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

// Log viewer
function updateLogs(){
  const level=document.getElementById('log-level').value;
  const url='/api/logs?lines=200'+(level?'&level='+level:'');
  fetch(url).then(r=>r.json()).then(lines=>{
    const el=document.getElementById('log-content');
    let h='';
    lines.forEach(l=>{
      let cls='';
      if(l.includes('ERROR'))cls='error';
      else if(l.includes('WARNING'))cls='warning';
      else if(l.includes('DEBUG'))cls='debug';
      h+='<div class="log-line '+cls+'">'+esc(l)+'</div>';
    });
    el.innerHTML=h||'<div style="color:var(--text2)">No log entries</div>';
    filterLogs();
    if(document.getElementById('log-autoscroll').checked)el.scrollTop=el.scrollHeight;
  }).catch(()=>{});
}

// Log search filter
function filterLogs(){
  const q=(document.getElementById('log-search').value||'').toLowerCase();
  const lines=document.querySelectorAll('#log-content .log-line');
  lines.forEach(l=>{l.style.display=(!q||l.textContent.toLowerCase().includes(q))?'':'none';});
}

// Config viewer (load once)
fetch('/api/config').then(r=>r.json()).then(cfg=>{
  let h='';
  Object.keys(cfg).forEach(k=>{
    h+='<tr><td>'+esc(k)+'</td><td>'+esc(cfg[k])+'</td></tr>';
  });
  document.querySelector('#config-table tbody').innerHTML=h||'<tr><td colspan="2" style="color:var(--text2)">No config</td></tr>';
}).catch(()=>{});

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
  if(_logTimer)clearInterval(_logTimer);
  if(_mtTimer)clearInterval(_mtTimer);
  if(_refreshSec>0){
    _statusTimer=setInterval(update,_refreshSec*1000);
    _logTimer=setInterval(updateLogs,_refreshSec*1000);
    _mtTimer=setInterval(updateMountHistory,Math.max(_refreshSec*3,30)*1000);
  }
}
update();updateLogs();
setRefreshInterval(10);
setTimeout(updateMountHistory,1000);
</script>
</body>
</html>'''


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
        elif self.path == '/settings':
            # Settings editor — requires auth
            if not self.auth_credentials:
                html = _SETTINGS_SETUP_HTML.encode()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(html)))
                self.end_headers()
                self.wfile.write(html)
                return
            from utils.settings_api import get_env_schema, get_plex_debrid_schema
            from utils.settings_page import get_settings_html
            html = get_settings_html(get_env_schema(), get_plex_debrid_schema()).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(html)))
            self.end_headers()
            self.wfile.write(html)
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
        elif self.path in ('/', '/status'):
            html = _DASHBOARD_HTML.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(html)))
            self.end_headers()
            self.wfile.write(html)
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
        else:
            self.send_response(404)
            self.end_headers()

    def _send_auth_required(self):
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm="pd_zurg"')
        self.end_headers()

    def _send_json_response(self, code, data):
        """Send a JSON response with proper headers."""
        body = data.encode() if isinstance(data, str) else data
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
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

    server = http.server.HTTPServer(('0.0.0.0', port), StatusHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Status UI started on port {port}")

    status_data.add_event('status_ui', f'Dashboard available on port {port}')
