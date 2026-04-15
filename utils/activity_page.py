"""HTML template for the Activity page (History + Blocklist).

Displays event history and blocklisted torrents in a two-tab interface.
Extracted from the monolithic dashboard to reduce scroll depth and match
the Sonarr/Radarr-style page-per-concern layout.
"""


def get_activity_html():
    """Return the complete activity page HTML with shared CSS and nav."""
    from utils.ui_common import (get_base_head, get_nav_html, THEME_TOGGLE_JS,
                                 WANTED_BADGE_JS, KEYBOARD_JS, TOAST_JS)
    html = _ACTIVITY_HTML
    html = html.replace('__BASE_HEAD__', get_base_head('pd_zurg Activity',
                                                       _ACTIVITY_EXTRA_CSS))
    html = html.replace('__NAV_HTML__', get_nav_html('activity'))
    html = html.replace('__THEME_TOGGLE_JS__',
                        THEME_TOGGLE_JS + KEYBOARD_JS + TOAST_JS)
    html = html.replace('__WANTED_BADGE_JS__', WANTED_BADGE_JS)
    return html


_ACTIVITY_HTML = r'''<!DOCTYPE html>
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
.tab .badge{display:inline-block;background:var(--border);color:var(--text2);border-radius:10px;font-size:.72em;font-weight:600;padding:1px 7px;margin-left:6px;vertical-align:middle;min-width:22px;text-align:center}
.tab.active .badge{background:#58a6ff26;color:var(--blue)}
[data-theme="light"] .tab.active .badge{background:#0969da1a}
.tab-panel{display:none;padding-top:16px}
.tab-panel.active{display:block}
</style>

<h2 style="font-size:1.1em;margin-bottom:12px">Activity</h2>

<div class="tabs">
  <div class="tab active" data-kb="tab-1" onclick="switchTab('history')">History</div>
  <div class="tab" data-kb="tab-2" onclick="switchTab('blocklist')">Blocklist <span class="badge" id="bl-tab-count" style="display:none">0</span></div>
</div>

<!-- History Tab -->
<div class="tab-panel active" id="panel-history">
  <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap">
    <select id="activity-type" onchange="loadActivity(1)" style="background:var(--input-bg);color:var(--text);border:1px solid var(--input-border);border-radius:4px;padding:4px 8px;font-size:.8em">
      <option value="">All Types</option>
      <option value="grabbed">Grabbed</option>
      <option value="cached">Cached</option>
      <option value="symlink_created">Symlink</option>
      <option value="failed">Failed</option>
      <option value="cleanup">Cleanup</option>
      <option value="switched_source">Source Switch</option>
      <option value="search_triggered">Search</option>
      <option value="rescan_triggered">Rescan</option>
      <option value="task_completed">Task</option>
      <option value="blocklisted">Blocklisted</option>
      <option value="blocklist_added">Auto-Blocked</option>
    </select>
    <input type="text" id="activity-search" data-kb="search" placeholder="Search titles... (/)" oninput="loadActivity(1)" style="flex:1;background:var(--input-bg);border:1px solid var(--input-border);border-radius:4px;padding:4px 8px;font-size:.8em;color:var(--text);outline:none;min-width:120px">
    <button class="btn btn-ghost btn-sm" onclick="clearHistory()" id="activity-clear-btn" style="display:none">Clear</button>
    <button class="btn btn-ghost btn-sm" data-kb="refresh" onclick="loadActivity()">Refresh</button>
  </div>
  <table><thead><tr><th style="width:80px">Time</th><th style="width:90px;text-align:center">Type</th><th>Title</th><th>Detail</th><th style="width:60px">Source</th></tr></thead>
  <tbody id="activity-body"></tbody></table>
  <div style="display:flex;justify-content:center;margin-top:8px;gap:8px" id="activity-pager"></div>
</div>

<!-- Blocklist Tab -->
<div class="tab-panel" id="panel-blocklist">
  <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px">
    <button class="btn btn-ghost btn-sm" onclick="clearBlocklist()" id="blocklist-clear-btn" style="display:none">Clear All</button>
    <button class="btn btn-ghost btn-sm" data-kb="refresh" onclick="loadBlocklist()">Refresh</button>
  </div>
  <table><thead><tr><th>Title</th><th style="width:120px">Hash</th><th>Reason</th><th style="width:80px">Date</th><th style="width:60px">Source</th><th style="width:50px" id="bl-actions-hdr"></th></tr></thead>
  <tbody id="blocklist-body"></tbody></table>
</div>

<div class="footer" style="margin-top:16px"></div>

<script>
__THEME_TOGGLE_JS__

/* Tab switching */
function switchTab(name){
  document.querySelectorAll('.tab').forEach(function(t){t.classList.remove('active')});
  document.querySelectorAll('.tab-panel').forEach(function(p){p.classList.remove('active')});
  document.getElementById('panel-'+name).classList.add('active');
  var idx=name==='history'?0:1;
  document.querySelectorAll('.tab')[idx].classList.add('active');
}

/* Activity (History) */
var _actPage=1;
var _actIcons={grabbed:'\u2B07',cached:'\u2705',symlink_created:'\uD83D\uDD17',failed:'\u274C',cleanup:'\uD83D\uDDD1',switched_source:'\u21C4',search_triggered:'\uD83D\uDD0D',rescan_triggered:'\uD83D\uDD04',task_completed:'\u2699',blocklisted:'\uD83D\uDEAB',blocklist_added:'\u26D4'};
function loadActivity(page){
  if(page)_actPage=page; else if(!arguments.length){}else{_actPage=1;}
  var t=document.getElementById('activity-type').value;
  var q=document.getElementById('activity-search').value.trim();
  var url='/api/history?page='+_actPage+'&limit=50';
  if(t)url+='&type='+encodeURIComponent(t);
  if(q)url+='&title='+encodeURIComponent(q);
  fetch(url).then(function(r){return r.json()}).then(function(d){
    var el=document.getElementById('activity-body');
    if(!d.events||!d.events.length){el.innerHTML='<tr><td colspan="5" style="color:var(--text3);text-align:center;padding:16px">No activity recorded yet</td></tr>';document.getElementById('activity-pager').innerHTML='';return}
    var h='';
    d.events.forEach(function(e){
      var icon=_actIcons[e.type]||'\u2022';
      h+='<tr><td style="font-size:.8em;color:var(--text3);white-space:nowrap">'+timeAgo(e.ts)+'</td>';
      h+='<td><span class="type-badge type-'+esc(e.type)+'">'+icon+' '+esc(e.type.replace(/_/g,' '))+'</span></td>';
      /* Link titles to the library detail page when we have a canonical
         name: either the event was enriched with media_title (blackhole/arr),
         or it came from the library scanner where title is already canonical.
         Type is a best-effort hint — library_page._restoreDetailFromUrl
         falls back to the other list if the hint is wrong. */
      var _name=e.media_title||e.title;
      var _canLink=!!e.media_title||e.source==='library';
      var _mediaType=(e.title&&/^Sonarr /.test(e.title))||e.episode?'show':(e.title&&/^Radarr /.test(e.title))?'movie':'movie';
      var _titleCell=_canLink&&_name?'<a class="act-link" href="/library?detail='+encodeURIComponent(_name)+'&type='+_mediaType+'">'+esc(_name)+'</a>':esc(_name);
      h+='<td style="font-size:.85em">'+_titleCell+(e.episode?' <span style="color:var(--text2)">'+esc(e.episode)+'</span>':'')+'</td>';
      h+='<td style="font-size:.8em;color:var(--text2)">'+esc(e.detail||'')+'</td>';
      h+='<td style="font-size:.75em;color:var(--text3)">'+esc(e.source||'')+'</td></tr>';
    });
    el.innerHTML=h;
    /* Pager */
    var pg='';
    if(d.pages>1){
      for(var i=1;i<=d.pages;i++){
        if(i===d.page)pg+='<span style="color:var(--blue);font-weight:600;font-size:.85em">'+i+'</span>';
        else pg+='<a href="#" onclick="loadActivity('+i+');return false" style="font-size:.85em">'+i+'</a>';
      }
    }
    document.getElementById('activity-pager').innerHTML=pg;
    if(window._hasAuth)document.getElementById('activity-clear-btn').style.display='';
  }).catch(function(){});
}
function clearHistory(){
  showConfirm('Clear history?','This will remove all activity history entries.').then(function(ok){
    if(!ok)return;
    fetch('/api/history',{method:'DELETE'}).then(function(){loadActivity(1)}).catch(function(){});
  });
}

/* Blocklist */
function loadBlocklist(){
  fetch('/api/blocklist').then(function(r){return r.json()}).then(function(entries){
    var el=document.getElementById('blocklist-body');
    var cnt=document.getElementById('bl-tab-count');
    if(!entries||!entries.length){
      el.innerHTML='<tr><td colspan="6" style="color:var(--text3);text-align:center;padding:16px">No blocklisted torrents</td></tr>';
      cnt.style.display='none';
      return;
    }
    cnt.textContent=entries.length;
    cnt.style.display='';
    var h='';
    entries.forEach(function(e){
      var shortHash=e.info_hash?(e.info_hash.substring(0,12)+'\u2026'):'';
      var srcBadge=e.source==='auto'?'<span style="color:var(--orange);font-size:.75em">\u2699 auto</span>':'<span style="font-size:.75em">manual</span>';
      h+='<tr>';
      h+='<td style="font-size:.85em">'+esc(e.title||'')+'</td>';
      h+='<td class="bl-hash" style="font-size:.75em;font-family:monospace;color:var(--text2);cursor:pointer" title="Click to copy" data-hash="'+esc(e.info_hash||'')+'">'+esc(shortHash)+'</td>';
      h+='<td style="font-size:.8em;color:var(--text2)">'+esc(e.reason||'')+'</td>';
      h+='<td style="font-size:.8em;color:var(--text3);white-space:nowrap">'+timeAgo(e.date)+'</td>';
      h+='<td>'+srcBadge+'</td>';
      h+='<td>';
      if(window._hasAuth)h+='<button class="btn btn-ghost btn-sm bl-remove" style="font-size:.7em;padding:2px 6px" data-id="'+esc(e.id)+'">Remove</button>';
      h+='</td></tr>';
    });
    el.innerHTML=h;
    el.querySelectorAll('.bl-hash').forEach(function(td){td.addEventListener('click',function(){navigator.clipboard.writeText(this.dataset.hash||'')})});
    el.querySelectorAll('.bl-remove').forEach(function(btn){btn.addEventListener('click',function(){removeBlocklistEntry(this.dataset.id)})});
    if(window._hasAuth)document.getElementById('blocklist-clear-btn').style.display='';
    if(window._hasAuth)document.getElementById('bl-actions-hdr').textContent='Actions';
  }).catch(function(){});
}
function removeBlocklistEntry(id){
  fetch('/api/blocklist/'+encodeURIComponent(id),{method:'DELETE'}).then(function(r){
    if(r.ok)loadBlocklist();
  }).catch(function(){});
}
function clearBlocklist(){
  showConfirm('Clear blocklist?','Remove all blocklisted torrents? They may be re-downloaded.').then(function(ok){
    if(!ok)return;
    fetch('/api/blocklist',{method:'DELETE',headers:{'X-Confirm-Clear':'true'}}).then(function(){loadBlocklist()}).catch(function(){});
  });
}

/* Escape handler */
window.onKbEscape=function(){
  var s=document.getElementById('activity-search');
  if(s&&s.value){s.value='';loadActivity(1);return;}
};

/* Initial load (wait for auth detection) + polling */
window._hasAuthReady.then(function(){loadActivity();loadBlocklist();});
setInterval(loadActivity,15000);
setInterval(loadBlocklist,30000);
__WANTED_BADGE_JS__
</script>
</main>
</body>
</html>'''

_ACTIVITY_EXTRA_CSS = """
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--border2);font-size:.85em}
th{color:var(--text2);font-weight:500;font-size:.75em;text-transform:uppercase;letter-spacing:.05em}
#activity-body td:nth-child(2){text-align:center}
.act-link{color:inherit;text-decoration:none;border-bottom:1px dotted var(--text3);transition:color var(--motion-fast),border-color var(--motion-fast)}
.act-link:hover{color:var(--blue);border-bottom-color:var(--blue);text-decoration:none}
.type-badge{display:inline-flex;align-items:center;gap:3px;padding:2px 7px;border-radius:4px;font-size:.75em;font-weight:500;white-space:nowrap}
.type-grabbed{background:#58a6ff1a;color:var(--blue)}.type-cached{background:#3fb9501a;color:var(--green)}.type-symlink_created{background:#bc8cff1a;color:#bc8cff}.type-failed{background:#f851491a;color:var(--red)}.type-cleanup{background:#d299221a;color:var(--yellow)}.type-switched_source{background:#db6d281a;color:var(--orange)}.type-search_triggered{background:#58a6ff1a;color:var(--blue)}.type-rescan_triggered{background:#3fb9501a;color:var(--green)}.type-task_completed{background:var(--border);color:var(--text2)}.type-blocklisted{background:#f851491a;color:var(--red)}.type-blocklist_added{background:#db6d281a;color:var(--orange)}
#activity-search:focus{border-color:var(--input-focus)}
.footer{display:flex;justify-content:flex-end;align-items:center;gap:8px;color:var(--text3);font-size:.78em}
"""
