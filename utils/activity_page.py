"""HTML template for the Activity page (History + Blocklist + Stuck).

Displays event history, blocklisted torrents, and stuck-content records
(titles caught in retry loops, from /api/stuck) in a tabbed interface.
Extracted from the monolithic dashboard to reduce scroll depth and match
the Sonarr/Radarr-style page-per-concern layout.
"""


def get_activity_html():
    """Return the complete activity page HTML with shared CSS and nav."""
    from utils.activity_format import FORMATTER_JS
    from utils.ui_common import (get_base_head, get_nav_html, THEME_TOGGLE_JS,
                                 WANTED_BADGE_JS, KEYBOARD_JS, TOAST_JS)
    html = _ACTIVITY_HTML
    html = html.replace('__BASE_HEAD__', get_base_head('Zurgarr Activity',
                                                       _ACTIVITY_EXTRA_CSS))
    html = html.replace('__NAV_HTML__', get_nav_html('activity'))
    html = html.replace('__THEME_TOGGLE_JS__',
                        THEME_TOGGLE_JS + KEYBOARD_JS + TOAST_JS + FORMATTER_JS)
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
  <div class="tab" data-kb="tab-3" onclick="switchTab('stuck')">Stuck <span class="badge" id="stuck-tab-count" style="display:none">0</span></div>
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
      <option value="debrid">Debrid Health</option>
    </select>
    <input type="text" id="activity-search" data-kb="search" placeholder="Search titles... (/)" oninput="loadActivity(1)" style="flex:1;background:var(--input-bg);border:1px solid var(--input-border);border-radius:4px;padding:4px 8px;font-size:.8em;color:var(--text);outline:none;min-width:120px">
    <label style="font-size:.78em;color:var(--text2);display:inline-flex;align-items:center;gap:4px;user-select:none">
      <input type="checkbox" id="activity-collapse" checked onchange="loadActivity()" style="margin:0">
      Collapse repeats
    </label>
    <button class="btn btn-ghost btn-sm" onclick="clearHistory()" id="activity-clear-btn" style="display:none">Clear</button>
    <button class="btn btn-ghost btn-sm" data-kb="refresh" onclick="loadActivity()">Refresh</button>
  </div>
  <table><thead><tr><th style="width:80px;text-align:center">Time</th><th style="width:90px;text-align:center">Type</th><th>Title</th><th>Detail</th><th style="width:60px;text-align:center">Source</th></tr></thead>
  <tbody id="activity-body"></tbody></table>
  <div style="display:flex;justify-content:center;margin-top:8px;gap:8px" id="activity-pager"></div>
</div>

<!-- Blocklist Tab -->
<div class="tab-panel" id="panel-blocklist">
  <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px">
    <button class="btn btn-ghost btn-sm" onclick="clearBlocklist()" id="blocklist-clear-btn" style="display:none">Clear All</button>
    <button class="btn btn-ghost btn-sm" data-kb="refresh" onclick="loadBlocklist()">Refresh</button>
  </div>
  <table><thead><tr><th style="text-align:center">Title</th><th style="width:120px">Hash</th><th style="text-align:center">Reason</th><th style="width:80px;text-align:center">Date</th><th style="width:60px;text-align:center">Source</th><th style="width:50px;text-align:center" id="bl-actions-hdr"></th></tr></thead>
  <tbody id="blocklist-body"></tbody></table>
</div>

<!-- Stuck Tab -->
<div class="tab-panel" id="panel-stuck">
  <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap">
    <span style="font-size:.78em;color:var(--text3)">Titles caught in retry loops with no progress. Retry clears give-up caps so the next scan tries again.</span>
    <span id="stuck-dismissed-note" style="font-size:.78em;color:var(--text3);margin-left:auto;display:none"></span>
    <button class="btn btn-ghost btn-sm" data-kb="refresh" onclick="loadStuck()">Refresh</button>
  </div>
  <table><thead><tr><th>Title</th><th>Why</th><th style="width:80px;text-align:center">Since</th><th style="width:70px;text-align:center">Attempts</th><th>Last event</th><th style="width:180px;text-align:center" id="stuck-actions-hdr"></th></tr></thead>
  <tbody id="stuck-body"></tbody></table>
</div>

<div class="footer" style="margin-top:16px"></div>

<script>
__THEME_TOGGLE_JS__

/* Tab switching */
function switchTab(name){
  document.querySelectorAll('.tab').forEach(function(t){t.classList.remove('active')});
  document.querySelectorAll('.tab-panel').forEach(function(p){p.classList.remove('active')});
  document.getElementById('panel-'+name).classList.add('active');
  var idx=name==='history'?0:name==='blocklist'?1:2;
  document.querySelectorAll('.tab')[idx].classList.add('active');
}

/* Activity (History) */
var _actPage=1;
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
    /* Collapse adjacent (type, source, cause, media) runs into one summary
       row when "Collapse repeats" is on.  Threshold 3 keeps distinct
       events visible while taming the 6-hour retry spam on long-unmet
       items.  Events arrive newest-first. */
    var collapse = (document.getElementById('activity-collapse')||{}).checked !== false;
    var GROUP_MIN = 3;
    function spanHuman(firstTs, lastTs){
      var first = Date.parse(firstTs), last = Date.parse(lastTs);
      if (isNaN(first) || isNaN(last) || first <= last) return '';
      var sec = Math.floor((first - last) / 1000);
      if (sec < 3600) return Math.max(1, Math.floor(sec/60)) + 'm';
      if (sec < 86400) return Math.floor(sec/3600) + 'h';
      return Math.floor(sec/86400) + 'd';
    }
    var runs = [];
    if (collapse && window._formatActivityEvent) {
      for (var i=0;i<d.events.length;i++){
        var _ev = d.events[i];
        var gk = window._formatActivityEvent(_ev).groupKey;
        var _last = runs[runs.length-1];
        if (_last && _last.groupKey === gk){ _last.events.push(_ev); continue; }
        runs.push({groupKey: gk, events: [_ev]});
      }
    } else {
      for (var j=0;j<d.events.length;j++) runs.push({groupKey:'', events:[d.events[j]]});
    }
    function rowFor(e, runInfo){
      var fmt = window._formatActivityEvent ? window._formatActivityEvent(e) : {short: e.detail||''};
      var timeCell;
      if (runInfo){
        var sp = spanHuman(runInfo.events[0].ts, runInfo.events[runInfo.events.length-1].ts);
        timeCell = sp ? ('over ' + esc(sp)) : timeAgo(e.ts);
      } else {
        timeCell = timeAgo(e.ts);
      }
      var countBadge = runInfo ? (' <span class="act-run-count">' + runInfo.events.length + '×</span>') : '';
      var row='<tr><td style="font-size:.8em;color:var(--text3);white-space:nowrap">'+timeCell+'</td>';
      row+='<td><span class="type-badge type-'+esc(e.type)+'">'+esc(e.type.replace(/_/g,' '))+countBadge+'</span></td>';
      /* Link titles to the library detail page when we have a canonical
         name: either the event was enriched with media_title (blackhole/arr),
         or it came from the library scanner where title is already canonical.
         Type is a best-effort hint — library_page._restoreDetailFromUrl
         falls back to the other list if the hint is wrong. */
      var _name=e.media_title||e.title;
      var _canLink=!!e.media_title||e.source==='library';
      var _mediaType=(e.title&&/^Sonarr /.test(e.title))||e.episode?'show':(e.title&&/^Radarr /.test(e.title))?'movie':'movie';
      var _titleCell=_canLink&&_name?'<a class="act-link" href="/library?detail='+encodeURIComponent(_name)+'&type='+_mediaType+'&from=activity">'+esc(_name)+'</a>':esc(_name);
      row+='<td style="font-size:.85em">'+_titleCell+(e.episode?' <span style="color:var(--text2)">'+esc(e.episode)+'</span>':'')+'</td>';
      row+='<td style="font-size:.8em;color:var(--text2)">'+esc(fmt.short||e.detail||'')+'</td>';
      row+='<td style="font-size:.75em;color:var(--text3)">'+esc(e.source||'')+'</td></tr>';
      return row;
    }
    var h='';
    runs.forEach(function(r){
      if (r.events.length >= GROUP_MIN){
        h += rowFor(r.events[0], r);
      } else {
        for (var q=0;q<r.events.length;q++) h += rowFor(r.events[q], null);
      }
    });
    el.innerHTML=h;
    /* Pager — windowed: first, ±2 around current, last, with ellipsis gaps */
    var pg='';
    if(d.pages>1){
      var cur=d.page,last=d.pages;
      var mk=function(i){
        if(i===cur)return '<span style="color:var(--blue);font-weight:600;font-size:.85em">'+i+'</span>';
        return '<a href="#" onclick="loadActivity('+i+');return false" style="font-size:.85em">'+i+'</a>';
      };
      var nav=function(i,label){
        return '<a href="#" onclick="loadActivity('+i+');return false" style="font-size:.85em">'+label+'</a>';
      };
      var sep='<span style="color:var(--text3);font-size:.85em">\u2026</span>';
      var parts=[];
      if(cur>1)parts.push(nav(cur-1,'\u2039'));
      var cand=[1];
      for(var j=cur-2;j<=cur+2;j++){if(j>=1&&j<=last)cand.push(j);}
      cand.push(last);
      cand.sort(function(a,b){return a-b;});
      var prev=0;
      for(var k=0;k<cand.length;k++){
        var p=cand[k];
        if(p===prev)continue;
        if(prev&&p-prev>1)parts.push(sep);
        parts.push(mk(p));
        prev=p;
      }
      if(cur<last)parts.push(nav(cur+1,'\u203A'));
      pg=parts.join('');
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

/* Stuck */
var _stuckList=[];
function loadStuck(){
  fetch('/api/stuck').then(function(r){return r.json()}).then(function(d){
    var el=document.getElementById('stuck-body');
    var cnt=document.getElementById('stuck-tab-count');
    var note=document.getElementById('stuck-dismissed-note');
    _stuckList=[];
    if(d.dismissed){note.textContent=d.dismissed+' dismissed';note.style.display='';}
    else{note.style.display='none';}
    if(!d.items||!d.items.length){
      el.innerHTML='<tr><td colspan="6" style="color:var(--text3);text-align:center;padding:16px">Nothing stuck — all retry loops are making progress</td></tr>';
      cnt.style.display='none';
      return;
    }
    cnt.textContent=d.total;
    cnt.style.display='';
    var h='';
    d.items.forEach(function(it,i){
      _stuckList[i]=it;
      var chips=(it.reason_labels||[]).map(function(l){return '<span class="stuck-chip">'+esc(l)+'</span>'}).join(' ');
      if(it.blocklisted)chips+=' <span class="stuck-chip stuck-chip-red">On local blocklist</span>';
      var last='';
      if(it.last_event){
        var fmt=window._formatActivityEvent?window._formatActivityEvent(it.last_event):{short:it.last_event.detail||''};
        last=esc(fmt.short||it.last_event.detail||'')+' <span style="color:var(--text3)">('+timeAgo(it.last_event.ts)+')</span>';
      }
      var mt=it.media_type==='show'?'show':'movie';
      var titleCell=it.title?'<a class="act-link" href="/library?detail='+encodeURIComponent(it.title)+'&type='+mt+'&from=activity">'+esc(it.title)+'</a>':'<span style="color:var(--text3)">(unknown)</span>';
      h+='<tr>';
      h+='<td style="font-size:.85em">'+titleCell+(it.provider?' <span style="color:var(--text3);font-size:.8em">['+esc(it.provider)+']</span>':'')+'</td>';
      h+='<td>'+chips+'</td>';
      h+='<td style="font-size:.8em;color:var(--text3);white-space:nowrap">'+(it.since?timeAgo(it.since):'—')+'</td>';
      h+='<td style="font-size:.85em;font-family:monospace">'+(it.attempts||0)+'</td>';
      h+='<td style="font-size:.8em;color:var(--text2)">'+last+'</td>';
      h+='<td class="stuck-actions">';
      if(window._hasAuth){
        /* Buttons reference items by list index — item keys derive from
           torrent filenames and must never land in an HTML attribute
           (esc() does not escape quotes). */
        h+='<button class="btn btn-ghost btn-sm stuck-retry" data-i="'+i+'">Retry</button> ';
        if(it.title&&!it.blocklisted)h+='<button class="btn btn-ghost btn-sm stuck-block" data-i="'+i+'">Blocklist</button> ';
        h+='<button class="btn btn-ghost btn-sm stuck-dismiss" data-i="'+i+'">Dismiss</button>';
      }
      h+='</td></tr>';
    });
    el.innerHTML=h;
    el.querySelectorAll('.stuck-retry').forEach(function(b){b.addEventListener('click',function(){stuckRetry(_stuckList[+this.dataset.i])})});
    el.querySelectorAll('.stuck-block').forEach(function(b){b.addEventListener('click',function(){stuckBlock(_stuckList[+this.dataset.i])})});
    el.querySelectorAll('.stuck-dismiss').forEach(function(b){b.addEventListener('click',function(){stuckDismiss(_stuckList[+this.dataset.i])})});
    if(window._hasAuth)document.getElementById('stuck-actions-hdr').textContent='Actions';
  }).catch(function(){});
}
function stuckRetry(it){
  if(!it)return;
  var isMovie=it.media_type!=='show';
  var extra=(isMovie&&it.title)?' and trigger a Radarr search now':' — the next scan will retry';
  showConfirm('Retry '+(it.title||'this item')+'?','Clears the give-up caps and retry memos'+extra+'.').then(function(ok){
    if(!ok)return;
    fetch('/api/stuck/retry',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({key:it.key,title:it.title||'',imdb_id:it.imdb_id||''})
    }).then(function(r){
      if(!r.ok)throw 0;
      if(isMovie&&it.title){
        return fetch('/api/library/download',{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({title:it.title,type:'movie'})
        }).then(function(r2){
          showToast(r2.ok?'Retry state cleared — Radarr search triggered':'Retry state cleared — search failed, next scan will retry',r2.ok?'success':'warning');
        });
      }
      showToast('Retry state cleared — next scan will retry','success');
    }).then(function(){loadStuck()}).catch(function(){showToast('Retry failed','error')});
  });
}
function stuckBlock(it){
  if(!it||!it.title)return;
  showConfirm('Blocklist '+it.title+'?','Future grabs of this release will be rejected.').then(function(ok){
    if(!ok)return;
    fetch('/api/blocklist',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({info_hash:it.info_hash||'',title:it.title,reason:'stuck: manual block'})
    }).then(function(r){
      showToast(r.ok?'Blocklisted':'Blocklist failed',r.ok?'success':'error');
      if(r.ok){loadStuck();loadBlocklist();}
    }).catch(function(){showToast('Blocklist failed','error')});
  });
}
function stuckDismiss(it){
  if(!it)return;
  fetch('/api/stuck/dismiss',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({key:it.key})
  }).then(function(r){
    showToast(r.ok?'Dismissed for 7 days':'Dismiss failed',r.ok?'success':'error');
    if(r.ok)loadStuck();
  }).catch(function(){showToast('Dismiss failed','error')});
}

/* Escape handler */
window.onKbEscape=function(){
  var s=document.getElementById('activity-search');
  if(s&&s.value){s.value='';loadActivity(1);return;}
};

/* Preselect the type filter from the URL (?type=...) so deep links from
   other pages — e.g. the System page Debrid Health cards' "View activity"
   button (?type=debrid) — land on a pre-filtered view. */
(function(){
  var t=new URLSearchParams(window.location.search).get('type');
  if(!t)return;
  var sel=document.getElementById('activity-type');
  for(var i=0;i<sel.options.length;i++){
    if(sel.options[i].value===t){sel.value=t;break;}
  }
})();

/* Initial load (wait for auth detection) + polling */
window._hasAuthReady.then(function(){loadActivity();loadBlocklist();loadStuck();});
setInterval(loadActivity,15000);
setInterval(loadBlocklist,30000);
setInterval(loadStuck,60000);
__WANTED_BADGE_JS__
</script>
</main>
</body>
</html>'''

_ACTIVITY_EXTRA_CSS = """
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--border2);font-size:.85em}
th{color:var(--text2);font-weight:500;font-size:.75em;text-transform:uppercase;letter-spacing:.05em}
#activity-body td:nth-child(1),#activity-body td:nth-child(2),#activity-body td:nth-child(5){text-align:center}
#blocklist-body td:nth-child(5){text-align:center}
#stuck-body td:nth-child(3),#stuck-body td:nth-child(4),#stuck-body td:nth-child(6){text-align:center}
.stuck-chip{display:inline-block;padding:2px 7px;border-radius:4px;font-size:.72em;font-weight:500;white-space:nowrap;background:#d299221a;color:var(--yellow);margin:1px 0}
.stuck-chip-red{background:#f851491a;color:var(--red)}
.stuck-actions .btn{font-size:.7em;padding:2px 6px}
.act-link{color:inherit;text-decoration:none;border-bottom:1px dotted var(--text3);transition:color var(--motion-fast),border-color var(--motion-fast)}
.act-link:hover{color:var(--blue);border-bottom-color:var(--blue);text-decoration:none}
.type-badge{display:inline-flex;align-items:center;gap:3px;padding:2px 7px;border-radius:4px;font-size:.75em;font-weight:500;white-space:nowrap}
.act-run-count{display:inline-block;padding:0 5px;margin-left:4px;border-radius:3px;font-size:.78em;font-weight:600;background:var(--border);color:var(--text);font-family:monospace}
.type-grabbed{background:#58a6ff1a;color:var(--blue)}.type-cached{background:#3fb9501a;color:var(--green)}.type-symlink_created{background:#bc8cff1a;color:#bc8cff}.type-failed{background:#f851491a;color:var(--red)}.type-cleanup{background:#d299221a;color:var(--yellow)}.type-switched_source{background:#db6d281a;color:var(--orange)}.type-search_triggered{background:#58a6ff1a;color:var(--blue)}.type-rescan_triggered{background:#3fb9501a;color:var(--green)}.type-task_completed{background:var(--border);color:var(--text2)}.type-blocklisted{background:#f851491a;color:var(--red)}.type-blocklist_added{background:#db6d281a;color:var(--orange)}.type-debrid{background:#bc8cff1a;color:#bc8cff}
#activity-search:focus{border-color:var(--input-focus)}
.footer{display:flex;justify-content:flex-end;align-items:center;gap:8px;color:var(--text3);font-size:.78em}

@media (max-width:600px){
  /* Card-stacked layout for Activity tables. Uses flex `order` to reshuffle
     cells visually without touching DOM order (History/Blocklist JS renders
     in schema order; cards need a different visual sequence). */
  #panel-history table,#panel-blocklist table,#panel-stuck table{display:block}
  #panel-history thead,#panel-blocklist thead,#panel-stuck thead{display:none}
  #panel-history tbody,#panel-blocklist tbody,#panel-stuck tbody{display:block}
  #activity-body tr,#blocklist-body tr,#stuck-body tr{display:flex;flex-wrap:wrap;align-items:baseline;border:1px solid var(--border2);border-radius:6px;padding:10px 12px;margin-bottom:8px}
  #activity-body td,#blocklist-body td,#stuck-body td{border:none;padding:2px 0;width:auto !important;text-align:left !important}
  /* Empty-state row: single td with colspan. Strip card look, center message. */
  #activity-body tr:has(td[colspan]),#blocklist-body tr:has(td[colspan]),#stuck-body tr:has(td[colspan]){display:block;border:none;padding:0;margin-bottom:0}
  #activity-body tr td[colspan],#blocklist-body tr td[colspan],#stuck-body tr td[colspan]{display:block;text-align:center !important;padding:16px 0 !important}
  /* Stuck card: row1 = Title, row2 = Why chips, row3 = Since + Attempts, row4 = Last event, row5 = Actions. */
  #stuck-body td:nth-child(1){order:1;flex-basis:100%;font-size:.95em;margin-bottom:2px}
  #stuck-body td:nth-child(2){order:2;flex-basis:100%;margin-bottom:4px}
  #stuck-body td:nth-child(3){order:3;margin-right:10px;font-size:.75em;color:var(--text3)}
  #stuck-body td:nth-child(4){order:4;font-size:.75em}
  #stuck-body td:nth-child(5){order:5;flex-basis:100%;color:var(--text2);font-size:.8em}
  #stuck-body td:nth-child(6){order:6;flex-basis:100%;text-align:right !important;margin-top:6px}
  #stuck-body .stuck-actions .btn{font-size:.8em !important;padding:6px 12px !important}
  /* History card: row1 = Time + Type + Source, row2 = Title, row3 = Detail. */
  #activity-body td:nth-child(1){order:1;margin-right:8px;font-size:.75em}
  #activity-body td:nth-child(2){order:2;margin-right:8px}
  #activity-body td:nth-child(5){order:3;margin-left:auto;color:var(--text3);font-size:.7em}
  #activity-body td:nth-child(3){order:4;flex-basis:100%;margin-top:6px;font-size:.95em}
  #activity-body td:nth-child(4){order:5;flex-basis:100%;color:var(--text2);font-size:.8em}
  /* Blocklist card: row1 = Title, row2 = Reason, row3 = Hash + Date + Source, row4 = Remove. */
  #blocklist-body td:nth-child(1){order:1;flex-basis:100%;font-size:.95em;margin-bottom:2px}
  #blocklist-body td:nth-child(3){order:2;flex-basis:100%;color:var(--text2);font-size:.8em;margin-bottom:4px}
  #blocklist-body td:nth-child(2){order:3;margin-right:10px;font-size:.75em}
  #blocklist-body td:nth-child(4){order:4;margin-right:10px;font-size:.75em;color:var(--text3)}
  #blocklist-body td:nth-child(5){order:5}
  #blocklist-body td:nth-child(6){order:6;flex-basis:100%;text-align:right !important;margin-top:6px}
  #blocklist-body .bl-remove{font-size:.8em !important;padding:6px 12px !important}
}
"""
