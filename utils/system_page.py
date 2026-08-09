"""HTML template for the System page (Logs + Tasks + Config).

Provides system administration tools in a three-tab interface.
Extracted from the monolithic dashboard to reduce scroll depth and match
the Sonarr/Radarr-style page-per-concern layout.
"""


def get_system_html():
    """Return the complete system page HTML with shared CSS and nav."""
    from utils.ui_common import (get_base_head, get_nav_html, THEME_TOGGLE_JS,
                                 WANTED_BADGE_JS, KEYBOARD_JS, TOAST_JS)
    html = _SYSTEM_HTML
    html = html.replace('__BASE_HEAD__', get_base_head('Zurgarr System',
                                                       _SYSTEM_EXTRA_CSS))
    html = html.replace('__NAV_HTML__', get_nav_html('system'))
    html = html.replace('__THEME_TOGGLE_JS__',
                        THEME_TOGGLE_JS + KEYBOARD_JS + TOAST_JS)
    html = html.replace('__WANTED_BADGE_JS__', WANTED_BADGE_JS)
    return html


_SYSTEM_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
__BASE_HEAD__
</head>
<body>
__NAV_HTML__
<main class="main-content">
<style>
.main-content{max-width:1200px}

/* Tabs */
.tabs{display:flex;gap:0;margin-bottom:0;border-bottom:2px solid var(--border)}
.tab{padding:10px 20px;cursor:pointer;color:var(--text2);font-size:.9em;font-weight:500;border-bottom:2px solid transparent;margin-bottom:-2px;transition:color .15s,border-color .15s;user-select:none}
.tab:hover{color:var(--text)}
.tab.active{color:var(--blue);border-bottom-color:var(--blue)}
.tab-panel{display:none;padding-top:16px}
.tab-panel.active{display:block}
</style>

<h2 style="font-size:1.1em;margin-bottom:12px">System</h2>

<div class="tabs">
  <div class="tab active" data-kb="tab-1" onclick="switchTab('logs')">Logs</div>
  <div class="tab" data-kb="tab-2" onclick="switchTab('tasks')">Tasks</div>
  <div class="tab" data-kb="tab-3" onclick="switchTab('config')">Config</div>
</div>

<!-- Logs Tab -->
<div class="tab-panel active" id="panel-logs">
  <div class="log-controls">
    <select id="log-level" onchange="updateLogs()">
      <option value="">All Levels</option>
      <option value="ERROR">Error</option>
      <option value="WARNING">Warning</option>
      <option value="INFO">Info</option>
      <option value="DEBUG">Debug</option>
    </select>
    <input type="text" id="log-search" data-kb="search" placeholder="Search logs... (/)" oninput="filterLogs()" style="flex:1;background:var(--input-bg);border:1px solid var(--input-border);border-radius:4px;padding:4px 8px;font-size:.8em;color:var(--text);outline:none;min-width:100px">
    <label><input type="checkbox" id="log-wrap" checked onchange="toggleLogWrap()"> Wrap</label>
    <label><input type="checkbox" id="log-autoscroll" checked> Auto-scroll</label>
    <button class="btn btn-ghost btn-sm" data-kb="refresh" onclick="updateLogs()">Refresh</button>
  </div>
  <div id="log-content"></div>
</div>

<!-- Tasks Tab -->
<div class="tab-panel" id="panel-tasks">
  <!-- Media Recovery tile: headline % of the library available/ready to
       play (pct_on_disk = debrid + local + both) with a sparkline of that
       daily series. Body breaks it down by source and demotes the
       debrid-recovery % (pct_debrid) to a secondary stat for the TB
       experiment, plus the filter-gate deltas. Hidden until the first
       recovery snapshot has been recorded. -->
  <div id="rec-card" style="display:none;margin-bottom:12px"></div>
  <!-- Debrid Health mini-dashboard (plan 38 phase 5, expanded for
       plan 39 phase 5 to render side-by-side RD + TB cards when both
       providers are configured; collapses to one card when only one is.
       Hidden entirely when no provider is configured. -->
  <div id="dh-heading" style="display:none;font-size:.85em;color:var(--text2);text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px">Debrid Health</div>
  <div id="dh-providers" class="dh-providers" style="display:none"></div>
  <div style="margin-bottom:8px">
    <button class="btn btn-ghost btn-sm" data-kb="refresh" onclick="updateTasks()">Refresh</button>
  </div>
  <table><thead><tr><th>Task</th><th>Interval</th><th>Last Run</th><th>Duration</th><th>Result</th><th>Next Run</th><th id="task-actions-hdr"></th></tr></thead>
  <tbody id="tasks"></tbody></table>
</div>

<!-- Config Tab -->
<div class="tab-panel" id="panel-config">
  <h3 style="font-size:.85em;color:var(--text2);text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px">Running Configuration</h3>
  <table class="cfg-table" id="config-table"><tbody></tbody></table>

  <details style="margin-top:24px">
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
            <div class="wf-node orange">Zurgarr<small>Send to RD</small></div>
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
        <dd>Mounts the Zurg WebDAV server as a local directory at <code style="color:var(--green);font-size:.95em">/data/zurgarr</code> so your media server can access the files.</dd>
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

<div class="footer" style="margin-top:16px"></div>

<script>
__THEME_TOGGLE_JS__

/* Tab switching */
var _tabs=['logs','tasks','config'];
function switchTab(name){
  document.querySelectorAll('.tab').forEach(function(t){t.classList.remove('active')});
  document.querySelectorAll('.tab-panel').forEach(function(p){p.classList.remove('active')});
  document.getElementById('panel-'+name).classList.add('active');
  var idx=_tabs.indexOf(name);
  if(idx>=0)document.querySelectorAll('.tab')[idx].classList.add('active');
}

/* Log viewer */
function toggleLogWrap(){
  var el=document.getElementById('log-content');
  var wrap=document.getElementById('log-wrap').checked;
  el.classList.toggle('nowrap',!wrap);
  try{localStorage.setItem('zurgarr_log_wrap',wrap?'1':'0');}catch(e){}
}
(function(){try{var w=localStorage.getItem('zurgarr_log_wrap');if(w==='0'){document.getElementById('log-wrap').checked=false;document.getElementById('log-content').classList.add('nowrap');}}catch(e){}})();

function updateLogs(){
  var level=document.getElementById('log-level').value;
  var url='/api/logs?lines=200'+(level?'&level='+level:'');
  fetch(url).then(function(r){return r.json()}).then(function(lines){
    var el=document.getElementById('log-content');
    var h='';
    lines.forEach(function(l){
      var cls='';
      if(l.includes('ERROR'))cls='error';
      else if(l.includes('WARNING'))cls='warning';
      else if(l.includes('DEBUG'))cls='debug';
      h+='<div class="log-line '+cls+'">'+esc(l)+'</div>';
    });
    el.innerHTML=h||'<div style="color:var(--text2)">No log entries</div>';
    filterLogs();
    if(document.getElementById('log-autoscroll').checked)el.scrollTop=el.scrollHeight;
  }).catch(function(){});
}
function filterLogs(){
  var q=(document.getElementById('log-search').value||'').toLowerCase();
  var lines=document.querySelectorAll('#log-content .log-line');
  lines.forEach(function(l){l.style.display=(!q||l.textContent.toLowerCase().includes(q))?'':'none';});
}

/* Scheduled tasks */
function fmtInterval(s){
  if(s<60)return s+'s';
  if(s<3600)return Math.floor(s/60)+'m';
  if(s<86400)return Math.floor(s/3600)+'h';
  return Math.floor(s/86400)+'d';
}
function updateTasks(){
  fetch('/api/tasks').then(function(r){return r.json()}).then(function(tasks){
    var el=document.getElementById('tasks');
    var hasAuth=window._hasAuth;
    document.getElementById('task-actions-hdr').textContent=hasAuth?'Actions':'';
    if(!tasks||!tasks.length){el.innerHTML='<tr><td colspan="7" style="color:var(--text2)">No tasks registered</td></tr>';return;}
    var h='';
    tasks.forEach(function(t){
      var intv=fmtInterval(t.interval);
      var lastRun=t.last_run?timeAgo(t.last_run):'Never';
      var dur=t.last_duration!==null?t.last_duration+'s':'-';
      var result='-';
      if(t.running){result='<span class="task-running">Running...</span>';}
      else if(t.last_result){
        var r=t.last_result;
        if(r.status==='success'){
          var msg=r.message||'OK';
          if(r.items!==undefined&&r.items!==null)msg+=' ('+r.items+')';
          result='<span class="task-ok">'+esc(msg)+'</span>';
        }else{
          result='<span class="task-err">'+esc(r.message||'Error')+'</span>';
        }
      }
      var nextLabel=t.next_run?(new Date(t.next_run)>new Date()?'in '+fmtInterval(Math.max(0,Math.floor((new Date(t.next_run)-Date.now())/1000))):'due'):'\u2014';
      var runBtn=hasAuth&&!t.running?'<td><button class="btn btn-ghost btn-sm" onclick="runTask(this,\''+esc(t.name)+'\')">Run</button></td>':'<td>'+(t.running?'<span class="task-running" style="font-size:.8em">...</span>':'')+'</td>';
      var enabledDot=t.enabled?'':'<span style="color:var(--text3);font-size:.75em" title="Disabled"> (off)</span>';
      h+='<tr><td><span title="'+esc(t.description||'')+'">'+esc(t.name)+'</span>'+enabledDot+'</td><td>'+intv+'</td><td>'+lastRun+'</td><td>'+dur+'</td><td>'+result+'</td><td>'+nextLabel+'</td>'+runBtn+'</tr>';
    });
    el.innerHTML=h;
  }).catch(function(){});
}
function runTask(btn,name){
  btn.disabled=true;btn.textContent='...';
  fetch('/api/tasks/'+encodeURIComponent(name)+'/run',{method:'POST'}).then(function(r){return r.json()}).then(function(d){
    btn.textContent=d.status==='started'?'OK':'Err';
    setTimeout(function(){btn.disabled=false;btn.textContent='Run';updateTasks();},3000);
  }).catch(function(){btn.disabled=false;btn.textContent='Run';});
}

/* Debrid Health mini-dashboard (plan 38 phase 5) */
function fmtAge(epoch){
  if(!epoch)return 'never';
  var s=Math.max(0,Math.floor(Date.now()/1000-epoch));
  if(s<60)return s+'s ago';
  if(s<3600)return Math.floor(s/60)+'m ago';
  if(s<86400)return Math.floor(s/3600)+'h ago';
  return Math.floor(s/86400)+'d ago';
}
function _dhDetails(rows,note){
  return '<details class="dh-details"><summary>Details</summary>'+rows.join('')+(note||'')+'</details>';
}
function _dhRenderRdBody(s,card){
  var c=card.counts||{};
  var headline;
  if(!c.total){
    headline=''
      +'<div class="dh-rate-wrap">'
      +'<span class="dh-rate dh-rate-na">&ndash;</span>'
      +'<span class="dh-rate-label">no sweep data yet<br>run a sweep to probe your library</span>'
      +'</div>';
  }else{
    var blocked=c.blocked||0;
    var cls=blocked>0?' dh-rate-low':'';
    headline=''
      +'<div class="dh-rate-wrap">'
      +'<span class="dh-rate'+cls+'">'+esc(String(blocked))+'</span>'
      +'<span class="dh-rate-label">blocked by the RD filter<br>'
      +esc(String(c.healthy||0))+' of '+esc(String(c.total))+' probed healthy &middot; last sweep '+esc(fmtAge(card.last_probe_ts))+'</span>'
      +'</div>';
  }
  var parts=[];
  parts.push(esc(String(c.total||0))+' total');
  parts.push('<span class="dh-good">'+esc(String(c.healthy||0))+' healthy</span>');
  var blockedPart=esc(String(c.blocked||0))+' blocked';
  if(c.blocked)blockedPart='<span class="dh-bad">'+blockedPart+'</span>';
  parts.push(blockedPart);
  if(c.unknown)parts.push(esc(String(c.unknown))+' unknown');
  var cfg=s.enabled
    ?('Auto-remediate '+(s.auto_remediate?'ON':'OFF')+(s.cross_rescue?' · Cross-rescue ON':''))
    :'Sweep disabled';
  var rows=[
    '<div class="dh-row"><span class="dh-label">Last sweep:</span><span>'+esc(fmtAge(card.last_probe_ts))+'</span></div>',
    '<div class="dh-row"><span class="dh-label">Probed torrents:</span><span>'+parts.join(' · ')+'</span></div>',
    '<div class="dh-row"><span class="dh-label">Remediated (24h):</span><span>'+esc(String(s.remediated_24h||0))+'</span></div>',
    '<div class="dh-row"><span class="dh-label">Config:</span><span>'+esc(cfg)+'</span></div>'
  ];
  var note='<div class="dh-note">The sweep asks Real-Debrid whether it will still serve each torrent. '
    +'<b>Blocked</b> = RD refuses it (the filter-gate). <b>Remediated</b> = a blocked item removed '
    +'from RD &amp; re-queued for download in the last 24h.</div>';
  return headline+_dhDetails(rows,note);
}
function _dhRenderTbBody(s,card){
  var rescued=(card.counts||{}).rescued||0;
  var blocked=(s.counts||{}).blocked||0;
  var denom=rescued+blocked;
  var headline;
  if(s.rd_configured&&denom>0){
    var pct=Math.round(rescued/denom*1000)/10;
    var lowCls=pct<50?' dh-rate-low':'';
    headline=''
      +'<div class="dh-rate-wrap">'
      +'<span class="dh-rate'+lowCls+'" title="rescued in 30d ÷ (rescued in 30d + currently blocked on RD)">'+esc(String(pct))+'%</span>'
      +'<span class="dh-rate-label">of RD-stuck titles recovered via TorBox<br>'
      +esc(String(rescued))+' rescued in 30d &middot; '+esc(String(s.rescued_24h||0))+' in 24h</span>'
      +'</div>';
  }else{
    headline=''
      +'<div class="dh-rate-wrap">'
      +'<span class="dh-rate'+(rescued?'':' dh-rate-na')+'">'+esc(String(rescued))+'</span>'
      +'<span class="dh-rate-label">rescued via TorBox in 30d<br>'+esc(String(s.rescued_24h||0))+' in 24h</span>'
      +'</div>';
  }
  var notRescued=blocked?'<span class="dh-bad">'+esc(String(blocked))+'</span>':esc(String(blocked));
  var rows=[
    '<div class="dh-row"><span class="dh-label">Rescued via TorBox:</span><span>'+esc(String(rescued))+' in 30d · '+esc(String(s.rescued_24h||0))+' in 24h</span></div>'
  ];
  if(s.rd_configured)rows.push('<div class="dh-row"><span class="dh-label">Still blocked on RD:</span><span>'+notRescued+'</span></div>');
  rows.push('<div class="dh-row"><span class="dh-label">WebDAV mount:</span><span>'+(card.configured?'configured':'not configured')+'</span></div>');
  var note='';
  if(s.rd_configured&&denom>0){
    note='<div class="dh-note"><b>Rescue rate</b> = rescues in the 30-day history window (sweep rescues, blackhole cached-alternative grabs, Wanted-backlog recoveries) vs. titles the latest sweep still reports blocked on RD. This is the core TorBox-viability signal.</div>';
  }else if(s.rd_configured){
    note='<div class="dh-note">No TorBox rescues or RD filter-blocks recorded yet — the rescue rate appears once either happens.</div>';
  }
  return headline+_dhDetails(rows,note);
}
function _dhRenderCardHtml(s,card){
  var bodyHtml=card.service==='torbox'?_dhRenderTbBody(s,card):_dhRenderRdBody(s,card);
  var title=card.service==='torbox'?'TorBox Rescues'
    :card.service==='realdebrid'?'RD Filter Blocks'
    :esc(card.label);
  var pills=[];
  if(card.service==='realdebrid'&&!s.enabled){
    pills.push('<span class="dh-pill dh-pill-off">Disabled</span>');
  }
  var actionsHtml='';
  if(card.service==='realdebrid'){
    actionsHtml=''
      +'<button class="btn btn-ghost btn-sm" onclick="runDebridHealth(this)">Run sweep now</button>'
      +'<a class="btn btn-ghost btn-sm" href="/activity?type=debrid">View activity &rarr;</a>';
  }else{
    actionsHtml='<a class="btn btn-ghost btn-sm" href="/activity?type=debrid">View activity &rarr;</a>';
  }
  return ''
    +'<div class="dh-card">'
    +'<div class="dh-card-head">'
    +'<span class="dh-card-title">'+title+'</span>'
    +'<span class="dh-card-pills">'+pills.join('')+'</span>'
    +'</div>'
    +'<div class="dh-card-body">'+bodyHtml+'</div>'
    +'<div class="dh-card-actions">'+actionsHtml+'</div>'
    +'</div>';
}
function updateDebridHealth(){
  fetch('/api/debrid_health/summary').then(function(r){return r.json()}).then(function(s){
    var container=document.getElementById('dh-providers');
    var heading=document.getElementById('dh-heading');
    if(!s){container.style.display='none';heading.style.display='none';return;}
    var providers=s.providers||[];
    if(!providers.length){
      // Back-compat fallback: synthesize a card from the legacy
      // top-level fields when the server isn't on the plan-39 schema yet.
      if(s.rd_configured){
        providers=[{service:'realdebrid',label:'Real-Debrid',configured:true,counts:s.counts||{},last_probe_ts:s.last_sweep_ts}];
      }else{
        container.style.display='none';heading.style.display='none';return;
      }
    }
    container.style.display='';
    heading.style.display='';
    container.className='dh-providers'+(providers.length>=2?' dh-providers-pair':'');
    container.innerHTML=providers.map(function(p){return _dhRenderCardHtml(s,p);}).join('');
  }).catch(function(){});
}
function runDebridHealth(btn){
  btn.disabled=true;btn.textContent='...';
  fetch('/api/tasks/debrid_health_reconcile/run',{method:'POST'}).then(function(r){return r.json()}).then(function(d){
    btn.textContent=d.status==='started'?'Started':'Already running';
    setTimeout(function(){btn.disabled=false;btn.textContent='Run sweep now';updateDebridHealth();updateTasks();},3000);
  }).catch(function(){btn.disabled=false;btn.textContent='Run sweep now';});
}

/* Media Recovery tile */
function _recSparkline(snaps){
  /* Tiny inline trend of the headline metric (pct_on_disk = available to
     play) across recorded days. Auto-scales to the observed min/max so a
     flat-but-high series doesn't look like noise; a single data point
     renders nothing (no line to draw yet). */
  var pts=snaps.map(function(s){return ((s.recovery||{}).pct_on_disk)||0;});
  if(pts.length<2)return '';
  var W=160,H=36,pad=2,n=pts.length;
  var lo=Math.min.apply(null,pts),hi=Math.max.apply(null,pts),range=(hi-lo)||1;
  var coords=pts.map(function(v,i){
    var x=pad+(W-2*pad)*(i/(n-1));
    var y=pad+(H-2*pad)*(1-(v-lo)/range);
    return x.toFixed(1)+','+y.toFixed(1);
  });
  return '<svg class="rec-spark" viewBox="0 0 '+W+' '+H+'" width="'+W+'" height="'+H+'" '
    +'preserveAspectRatio="none" aria-hidden="true">'
    +'<polyline points="'+coords.join(' ')+'" fill="none" stroke="var(--green)" stroke-width="1.5" '
    +'stroke-linejoin="round" stroke-linecap="round"/></svg>';
}
function _recDeltaPill(snaps){
  if(snaps.length<2)return '';
  var first=(snaps[0].recovery||{}).pct_on_disk||0;
  var last=(snaps[snaps.length-1].recovery||{}).pct_on_disk||0;
  var d=Math.round((last-first)*10)/10;
  var sign=d>0?'+':'';
  var cls=d>0?'rec-up':d<0?'rec-down':'rec-neutral';
  return '<span class="rec-delta '+cls+'" title="Change since '+esc(snaps[0].date||'')+'">'
    +sign+d+' pts</span>';
}
function updateRecovery(){
  fetch('/api/recovery').then(function(r){return r.json()}).then(function(d){
    var container=document.getElementById('rec-card');
    var snaps=(d&&d.snapshots)||[];
    var latest=(d&&d.latest)||null;
    if(!latest){container.style.display='none';return;}
    var r=latest.recovery||{};
    // Filter-gate / rescue signals intentionally live only on the Debrid
    // Health cards now (live, with a rescue-rate headline). Keeping a frozen
    // copy here duplicated the numbers under a stale snapshot date.
    var body=''
      +'<div class="dh-row"><span class="dh-label">Served by debrid (recovery):</span><span>'+esc(String(r.available_debrid||0))+' units ('+esc(String(r.pct_debrid||0))+'%)</span></div>'
      +'<div class="dh-row"><span class="dh-label">Local-only (fallback):</span><span>'+esc(String(r.available_local||0))+' units</span></div>'
      +'<div class="dh-row"><span class="dh-label">Wanted (not acquired):</span><span>'+esc(String(r.wanted||0))+' units</span></div>';
    container.style.display='';
    container.innerHTML=''
      +'<div class="dh-card">'
      +'<div class="dh-card-head">'
      +'<span class="dh-card-title">Media Recovery</span>'
      +'<span class="dh-card-pills"><span class="dh-pill dh-pill-tb">as of '+esc(latest.date||'')+'</span></span>'
      +'</div>'
      +'<div class="rec-headline">'
      +'<div class="rec-pct">'+esc(String(r.pct_on_disk||0))+'%</div>'
      +'<div class="rec-pct-label">available &middot; ready to play<br><small>'+esc(String(r.on_disk||0))+' of '+esc(String(r.total||0))+' units</small></div>'
      +'<div class="rec-trend">'+_recSparkline(snaps)+_recDeltaPill(snaps)+'</div>'
      +'</div>'
      +'<div class="dh-card-body">'+body+'</div>'
      +'<div class="dh-card-actions"><a class="btn btn-ghost btn-sm" href="/activity">View activity &rarr;</a></div>'
      +'</div>';
  }).catch(function(){});
}

/* Config viewer (load once) */
fetch('/api/config').then(function(r){return r.json()}).then(function(cfg){
  var h='';
  Object.keys(cfg).forEach(function(k){
    h+='<tr><td>'+esc(k)+'</td><td>'+esc(cfg[k])+'</td></tr>';
  });
  document.querySelector('#config-table tbody').innerHTML=h||'<tr><td colspan="2" style="color:var(--text2)">No config</td></tr>';
}).catch(function(){});

/* Escape handler */
window.onKbEscape=function(){
  var s=document.getElementById('log-search');
  if(s&&s.value){s.value='';filterLogs();return;}
};

/* Initial load (wait for auth detection) + polling */
window._hasAuthReady.then(function(){updateLogs();updateTasks();updateDebridHealth();updateRecovery();});
setInterval(updateLogs,10000);
setInterval(updateTasks,15000);
setInterval(updateDebridHealth,30000);
setInterval(updateRecovery,60000);
__WANTED_BADGE_JS__
</script>
</main>
</body>
</html>'''

_SYSTEM_EXTRA_CSS = """
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--border2);font-size:.85em}
th{color:var(--text2);font-weight:500;font-size:.75em;text-transform:uppercase;letter-spacing:.05em}
.log-controls{display:flex;gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap}
.log-controls select{background:var(--input-bg);color:var(--text);border:1px solid var(--input-border);border-radius:4px;padding:4px 8px;font-size:.8em}
.log-controls label{font-size:.8em;color:var(--text2)}
#log-content{max-height:600px;overflow-y:auto;background:var(--bg);border:1px solid var(--border2);border-radius:4px;padding:8px;font-size:.75em;line-height:1.5;white-space:pre-wrap;word-break:break-word;min-width:0}
#log-content.nowrap{white-space:pre;overflow-x:auto;word-break:normal}
.log-line.error{color:var(--red)}.log-line.warning{color:var(--yellow)}.log-line.debug{color:var(--text3)}
#log-search:focus{border-color:var(--input-focus)}
.task-ok{color:var(--green)}.task-err{color:var(--red)}.task-running{color:var(--blue)}
.dh-providers{display:grid;grid-template-columns:1fr;gap:12px;margin-bottom:12px}
@media(min-width:720px){.dh-providers.dh-providers-pair{grid-template-columns:1fr 1fr}}
.dh-card{border:1px solid var(--border2);border-radius:6px;padding:12px 14px;background:var(--bg)}
.dh-card-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;gap:8px;flex-wrap:wrap}
.dh-card-title{font-weight:600;font-size:.85em;color:var(--text)}
.dh-card-pills{display:flex;gap:6px;flex-wrap:wrap}
.dh-pill{font-size:.7em;padding:2px 8px;border-radius:10px;border:1px solid var(--border2);color:var(--text2);text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}
.dh-pill-on{color:var(--green);border-color:var(--green)}
.dh-pill-off{color:var(--text3)}
.dh-pill-rd{color:var(--yellow);border-color:var(--yellow)}
.dh-pill-tb{color:var(--blue);border-color:var(--blue)}
.dh-card-body{display:flex;flex-direction:column;gap:4px;margin-bottom:10px}
.dh-row{display:flex;gap:8px;font-size:.8em;color:var(--text)}
.dh-label{color:var(--text2);min-width:130px}
.dh-bad{color:var(--red);font-weight:600}
.dh-good{color:var(--green);font-weight:600}
.dh-rate-wrap{display:flex;align-items:baseline;gap:10px;margin-bottom:8px}
.dh-rate{font-size:1.9em;font-weight:700;line-height:1;color:var(--green)}
.dh-rate.dh-rate-low{color:var(--red)}
.dh-rate.dh-rate-na{color:var(--text3)}
.dh-rate-label{font-size:.74em;color:var(--text2);line-height:1.3}
.dh-note{font-size:.7em;color:var(--text3);margin-top:4px;line-height:1.45}
.dh-details{margin-top:2px}
.dh-details summary{font-size:.72em;color:var(--text3);cursor:pointer;user-select:none;text-transform:uppercase;letter-spacing:.04em}
.dh-details summary:hover{color:var(--text2)}
.dh-details[open] summary{margin-bottom:6px}
.dh-details .dh-row{margin-bottom:4px}
.dh-card-actions{display:flex;gap:8px;flex-wrap:wrap}
.rec-headline{display:flex;align-items:center;gap:16px;margin-bottom:10px;flex-wrap:wrap}
.rec-pct{font-size:2.1em;font-weight:700;color:var(--green);line-height:1}
.rec-pct-label{font-size:.78em;color:var(--text2);line-height:1.35}
.rec-pct-label small{color:var(--text3)}
.rec-trend{margin-left:auto;display:flex;align-items:center;gap:8px}
.rec-spark{display:block}
.rec-delta{font-size:.72em;font-weight:600;white-space:nowrap}
.rec-delta.rec-up{color:var(--green)}
.rec-delta.rec-down{color:var(--red)}
.rec-delta.rec-neutral{color:var(--text2)}
@media(max-width:600px){.rec-trend{margin-left:0;flex-basis:100%}}
details{margin-top:0}
details summary{cursor:pointer;color:var(--text2);font-size:.85em;padding:4px 0;font-weight:500}
details summary:hover{color:var(--blue)}
.cfg-table td{font-family:monospace;font-size:.8em}
.cfg-table td:first-child{color:var(--blue);font-weight:500;white-space:nowrap;padding-right:16px}
.footer{display:flex;justify-content:flex-end;align-items:center;gap:8px;color:var(--text3);font-size:.78em}

@media(max-width:600px){
  /* Tasks table: card-stacked layout. DOM: Task(1) Interval(2) Last(3)
     Duration(4) Result(5) Next(6) Actions(7). Visual via flex `order`:
     Task, Result, meta row (Interval/Last/Next/Duration), Run button. */
  #panel-tasks table,#panel-tasks tbody{display:block}
  #panel-tasks thead{display:none}
  #tasks tr{display:flex;flex-wrap:wrap;align-items:baseline;border:1px solid var(--border2);border-radius:6px;padding:10px 12px;margin-bottom:8px}
  #tasks td{border:none;padding:2px 0;width:auto !important;text-align:left !important}
  #tasks tr:has(td[colspan]){display:block;border:none;padding:16px 0;margin-bottom:0}
  #tasks tr td[colspan]{display:block;text-align:center !important;padding:16px 0 !important}
  #tasks td:nth-child(1){order:1;flex-basis:100%;font-weight:500;margin-bottom:2px;overflow-wrap:anywhere}
  #tasks td:nth-child(5){order:2;flex-basis:100%;font-size:.85em;margin-bottom:4px;overflow-wrap:anywhere}
  #tasks td:nth-child(2){order:3;margin-right:10px;font-size:.75em;color:var(--text3)}
  #tasks td:nth-child(3){order:4;margin-right:10px;font-size:.75em;color:var(--text3)}
  #tasks td:nth-child(6){order:5;margin-right:10px;font-size:.75em;color:var(--text3)}
  #tasks td:nth-child(4){order:6;font-size:.75em;color:var(--text3)}
  #tasks td:nth-child(7){order:7;flex-basis:100%;text-align:right !important;margin-top:6px}

  /* Config table: key as small uppercase label above the value, so long keys
     and long values both get their own line instead of fighting for width. */
  #config-table,#config-table tbody{display:block}
  #config-table tr{display:block;border-bottom:1px solid var(--border2);padding:6px 0}
  #config-table td{display:block;border:none;padding:2px 0;white-space:normal !important;word-break:break-all}
  #config-table td:first-child{font-size:.7em;text-transform:uppercase;letter-spacing:.04em;padding-right:0 !important;color:var(--blue)}
  #config-table td:last-child{font-size:.85em;color:var(--text)}
  #config-table tr:has(td[colspan]){border:none;padding:0}
  #config-table tr td[colspan]{text-align:center;padding:16px 0}
}
"""
