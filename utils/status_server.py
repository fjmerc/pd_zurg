"""Lightweight status web UI and JSON API.

Provides an at-a-glance dashboard showing service connectivity, process health,
mount status, system resources (cgroup-aware), and recent events. Uses Python's
built-in http.server — no framework dependencies.
"""

import base64
import collections
import hmac
import http.server
import json
import os
import threading
import time
from datetime import datetime, timezone
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
                        mounts.append({
                            'path': path,
                            'mounted': os.path.ismount(path),
                            'accessible': os.access(path, os.R_OK),
                        })
                    except OSError:
                        mounts.append({'path': path, 'mounted': False, 'accessible': False})
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
# HTML Dashboard
# ---------------------------------------------------------------------------

_DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pd_zurg Status</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--border2:#21262d;--text:#c9d1d9;--text2:#8b949e;--text3:#484f58;--blue:#58a6ff;--green:#3fb950;--red:#f85149;--yellow:#d29922;--orange:#db6d28}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);padding:20px;max-width:1200px;margin:0 auto}
a{color:var(--blue);text-decoration:none}
h1{color:var(--blue);margin-bottom:4px;font-size:1.6em;font-weight:600}
.header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px}
.meta{color:var(--text2);font-size:.85em;margin-bottom:20px}
.banner{padding:12px 16px;border-radius:8px;margin-bottom:16px;font-size:.9em;font-weight:500;display:none}
.banner.warn{display:block;background:#d299221a;border:1px solid var(--yellow);color:var(--yellow)}
.banner.crit{display:block;background:#f851491a;border:1px solid var(--red);color:var(--red)}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.grid.full{grid-template-columns:1fr}
@media(max-width:768px){.grid{grid-template-columns:1fr}}
.card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:16px}
.card h2{font-size:.8em;color:var(--text2);margin-bottom:12px;text-transform:uppercase;letter-spacing:.08em;font-weight:600}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--border2);font-size:.85em}
th{color:var(--text2);font-weight:500;font-size:.75em;text-transform:uppercase;letter-spacing:.05em}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;vertical-align:middle}
.dot.green{background:var(--green)}.dot.red{background:var(--red)}.dot.yellow{background:var(--yellow)}
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
.stats-row{display:flex;gap:32px}
.footer{color:var(--text3);font-size:.7em;text-align:right;margin-top:12px}
</style>
</head>
<body>
<div class="header"><h1>pd_zurg</h1><span class="meta" id="header-meta"></span></div>
<div class="meta">Uptime: <span id="uptime"></span> &bull; Errors: <span id="errors">0</span></div>
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
    <table><thead><tr><th>Name</th><th>PID</th><th>Restarts</th><th>Status</th></tr></thead>
    <tbody id="procs"></tbody></table>
  </div>
  <div class="card">
    <h2>Mounts</h2>
    <table><thead><tr><th>Path</th><th>Mounted</th><th>Accessible</th></tr></thead>
    <tbody id="mounts"></tbody></table>
  </div>
</div>
<div class="grid">
  <div class="card">
    <h2>System</h2>
    <div class="stats-row">
      <div><div class="stat-value" id="mem-pct">-</div><div class="stat-label">Memory</div></div>
      <div><div class="stat-value" id="mem-used">-</div><div class="stat-label">Used</div></div>
    </div>
  </div>
  <div class="card">
    <h2>Recent Events</h2>
    <div class="events" id="events"></div>
  </div>
</div>
<div class="footer">Auto-refreshes every 10s</div>
<script>
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
function esc(s){const d=document.createElement('div');d.appendChild(document.createTextNode(String(s)));return d.innerHTML;}
function dot(ok){return '<span class="dot '+(ok?'green':'red')+'"></span>'+(ok?'Running':'Stopped');}
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

    // Processes
    let p='';d.processes.forEach(x=>{
      p+='<tr><td>'+esc(x.name)+'</td><td>'+(x.pid||'-')+'</td><td>'+(x.restart_count||0)+'</td><td>'+dot(x.running)+'</td></tr>';
    });
    document.getElementById('procs').innerHTML=p||'<tr><td colspan="4" style="color:var(--text2)">No processes</td></tr>';

    // Mounts
    let m='';d.mounts.forEach(x=>{m+='<tr><td>'+esc(x.path)+'</td><td>'+dot(x.mounted)+'</td><td>'+dot(x.accessible)+'</td></tr>';});
    document.getElementById('mounts').innerHTML=m||'<tr><td colspan="3" style="color:var(--text2)">No mounts</td></tr>';

    // System
    if(d.system.memory_percent!==undefined)document.getElementById('mem-pct').textContent=d.system.memory_percent+'%';
    if(d.system.memory_used_bytes!==undefined)document.getElementById('mem-used').textContent=fmtBytes(d.system.memory_used_bytes);

    // Events
    const validLevels=new Set(['info','warning','error']);
    let e='';d.recent_events.forEach(x=>{
      const lvl=validLevels.has(x.level)?x.level:'info';
      const t=x.timestamp.split('T')[1]||x.timestamp;
      e+='<div class="event '+lvl+'"><span class="time">'+esc(t)+'</span><span class="comp">'+esc(x.component)+'</span><span class="msg">'+esc(x.message)+'</span></div>';
    });
    document.getElementById('events').innerHTML=e||'<div style="color:var(--text2);padding:8px 0">No events yet</div>';
  }).catch(()=>{});
}
update();setInterval(update,10000);
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
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data.encode())
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

    def _send_auth_required(self):
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm="pd_zurg"')
        self.end_headers()

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
