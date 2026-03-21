"""Lightweight status web UI and JSON API.

Provides an at-a-glance dashboard showing process health, mount status,
system resources (cgroup-aware), and recent events. Uses Python's built-in
http.server — no framework dependencies.
"""

import base64
import collections
import http.server
import json
import os
import threading
import time
from datetime import datetime
from utils.logger import get_logger

logger = get_logger()


def _read_cgroup_file(path):
    """Read a cgroup v2 file, return contents or None."""
    try:
        with open(path, 'r') as f:
            return f.read().strip()
    except (FileNotFoundError, PermissionError):
        return None


def get_system_stats():
    """Get system stats, preferring cgroup values when in a container."""
    stats = {}

    # Memory
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

    # CPU — cgroup cpu.stat has usage_usec
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


class StatusData:
    """Singleton collecting status from all components."""

    def __init__(self):
        self.start_time = time.time()
        self.version = '2.10.0'
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
                # Support both tuple format (handler, name, key_type) and dict format
                if isinstance(entry, dict):
                    handler = entry['handler']
                    name = entry['process_name']
                    key_type = entry['key_type']
                else:
                    handler, name, key_type = entry
                desc = f"{name} w/ {key_type}" if key_type else name
                running = handler.process is not None and handler.process.poll() is None
                processes.append({
                    'name': desc,
                    'pid': handler.process.pid if handler.process else None,
                    'running': running,
                })

        mounts = []
        if os.path.exists('/data'):
            for entry_name in os.listdir('/data'):
                path = os.path.join('/data', entry_name)
                mounts.append({
                    'path': path,
                    'mounted': os.path.ismount(path),
                    'accessible': os.access(path, os.R_OK),
                })

        with self._lock:
            events = list(self.recent_events)
            error_count = self.error_count

        return {
            'version': self.version,
            'uptime_seconds': int(time.time() - self.start_time),
            'processes': processes,
            'mounts': mounts,
            'system': get_system_stats(),
            'recent_events': events,
            'error_count': error_count,
        }


# Module-level singleton
status_data = StatusData()


_DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pd_zurg Status</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0d1117;color:#c9d1d9;padding:20px}
h1{color:#58a6ff;margin-bottom:4px;font-size:1.5em}
.meta{color:#8b949e;font-size:.85em;margin-bottom:20px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}
@media(max-width:768px){.grid{grid-template-columns:1fr}}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px}
.card h2{font-size:1em;color:#8b949e;margin-bottom:12px;text-transform:uppercase;letter-spacing:.05em}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #21262d;font-size:.9em}
th{color:#8b949e;font-weight:500}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}
.dot.green{background:#3fb950}.dot.red{background:#f85149}.dot.yellow{background:#d29922}
.events{max-height:300px;overflow-y:auto}
.event{padding:6px 0;border-bottom:1px solid #21262d;font-size:.85em}
.event .time{color:#8b949e;margin-right:8px}
.event .comp{color:#58a6ff;margin-right:8px;font-weight:500}
.event.error .msg{color:#f85149}.event.warning .msg{color:#d29922}
.stat-value{font-size:1.8em;font-weight:600;color:#58a6ff}
.stat-label{font-size:.8em;color:#8b949e;margin-top:2px}
.stats-row{display:flex;gap:24px}
.refresh-note{color:#484f58;font-size:.75em;text-align:right;margin-top:8px}
</style>
</head>
<body>
<h1>pd_zurg</h1>
<div class="meta">
  <span id="version"></span> &bull;
  Uptime: <span id="uptime"></span> &bull;
  Errors: <span id="errors">0</span>
</div>
<div class="grid">
  <div class="card">
    <h2>Processes</h2>
    <table><thead><tr><th>Name</th><th>PID</th><th>Status</th></tr></thead>
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
<div class="refresh-note">Auto-refreshes every 10s</div>
<script>
function fmt(s){
  if(s<60)return s+'s';
  if(s<3600)return Math.floor(s/60)+'m '+s%60+'s';
  const h=Math.floor(s/3600),m=Math.floor((s%3600)/60);
  return h+'h '+m+'m';
}
function fmtBytes(b){
  if(b>1073741824)return(b/1073741824).toFixed(1)+'G';
  if(b>1048576)return(b/1048576).toFixed(0)+'M';
  return(b/1024).toFixed(0)+'K';
}
function dot(ok){return '<span class="dot '+(ok?'green':'red')+'"></span>'+(ok?'Running':'Stopped');}
function update(){
  fetch('/api/status').then(r=>r.json()).then(d=>{
    document.getElementById('version').textContent='v'+d.version;
    document.getElementById('uptime').textContent=fmt(d.uptime_seconds);
    document.getElementById('errors').textContent=d.error_count;
    let p='';d.processes.forEach(x=>{p+='<tr><td>'+x.name+'</td><td>'+(x.pid||'-')+'</td><td>'+dot(x.running)+'</td></tr>';});
    document.getElementById('procs').innerHTML=p||'<tr><td colspan="3" style="color:#8b949e">No processes</td></tr>';
    let m='';d.mounts.forEach(x=>{m+='<tr><td>'+x.path+'</td><td>'+dot(x.mounted)+'</td><td>'+dot(x.accessible)+'</td></tr>';});
    document.getElementById('mounts').innerHTML=m||'<tr><td colspan="3" style="color:#8b949e">No mounts</td></tr>';
    if(d.system.memory_percent!==undefined)document.getElementById('mem-pct').textContent=d.system.memory_percent+'%';
    if(d.system.memory_used_bytes!==undefined)document.getElementById('mem-used').textContent=fmtBytes(d.system.memory_used_bytes);
    let e='';d.recent_events.forEach(x=>{
      e+='<div class="event '+x.level+'"><span class="time">'+x.timestamp.split('T')[1]+'</span><span class="comp">'+x.component+'</span><span class="msg">'+x.message+'</span></div>';
    });
    document.getElementById('events').innerHTML=e||'<div style="color:#8b949e;padding:8px 0">No events yet</div>';
  }).catch(()=>{});
}
update();setInterval(update,10000);
</script>
</body>
</html>'''


class StatusHandler(http.server.BaseHTTPRequestHandler):
    status_data_ref = None
    auth_credentials = None

    def do_GET(self):
        if self.auth_credentials:
            auth_header = self.headers.get('Authorization', '')
            if not auth_header.startswith('Basic '):
                self._send_auth_required()
                return
            try:
                decoded = base64.b64decode(auth_header[6:]).decode()
                if decoded != self.auth_credentials:
                    self._send_auth_required()
                    return
            except Exception:
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


def setup():
    """Start the status web UI server if enabled."""
    enabled = os.environ.get('STATUS_UI_ENABLED', 'false').lower() == 'true'
    if not enabled:
        return

    port = int(os.environ.get('STATUS_UI_PORT', '8080'))
    auth = os.environ.get('STATUS_UI_AUTH')

    StatusHandler.status_data_ref = status_data
    StatusHandler.auth_credentials = auth if auth and ':' in auth else None

    server = http.server.HTTPServer(('0.0.0.0', port), StatusHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Status UI started on port {port}")

    status_data.add_event('status_ui', f'Dashboard available on port {port}')
