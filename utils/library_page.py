"""HTML template for the library browser page.

Displays media available on debrid and/or local storage. Communicates
with /api/library and /api/library/refresh endpoints. Uses Python's
built-in http.server — no framework dependencies.
"""


def get_library_html():
    """Return the complete library browser HTML page."""
    return _LIBRARY_HTML


_LIBRARY_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark light">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#x26A1;</text></svg>">
<title>pd_zurg Library</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--border2:#21262d;--text:#c9d1d9;--text2:#8b949e;--text3:#636e7b;--blue:#58a6ff;--green:#3fb950;--red:#f85149;--yellow:#d29922;--orange:#db6d28;--input-bg:#0d1117;--input-border:#30363d;--input-focus:#58a6ff}
[data-theme="light"]{--bg:#f6f8fa;--card:#ffffff;--border:#d0d7de;--border2:#d8dee4;--text:#1f2328;--text2:#656d76;--text3:#8b949e;--blue:#0969da;--green:#1a7f37;--red:#cf222e;--yellow:#9a6700;--orange:#bc4c00;--input-bg:#ffffff;--input-border:#d0d7de;--input-focus:#0969da}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);padding:20px;max-width:1200px;margin:0 auto}
a{color:var(--blue);text-decoration:none}
a:hover{text-decoration:underline}

/* Header */
.header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:16px;flex-wrap:wrap;gap:8px}
.header h1{color:var(--blue);font-size:1.5em;font-weight:600}
.nav{display:flex;gap:12px;font-size:.85em;align-items:center}
.nav .current{color:var(--text);font-weight:600;pointer-events:none}

/* Tabs */
.tabs{display:flex;gap:0;margin-bottom:0;border-bottom:2px solid var(--border)}
.tab{padding:10px 20px;cursor:pointer;color:var(--text2);font-size:.9em;font-weight:500;border-bottom:2px solid transparent;margin-bottom:-2px;transition:color .15s,border-color .15s;user-select:none}
.tab:hover{color:var(--text)}
.tab.active{color:var(--blue);border-bottom-color:var(--blue)}
.tab .badge{display:inline-block;background:var(--border);color:var(--text2);border-radius:10px;font-size:.72em;font-weight:600;padding:1px 7px;margin-left:6px;vertical-align:middle;min-width:22px;text-align:center}
.tab.active .badge{background:#58a6ff26;color:var(--blue)}
[data-theme="light"] .tab.active .badge{background:#0969da1a}

/* Controls row */
.controls{display:flex;gap:8px;align-items:center;padding:12px 0;flex-wrap:wrap}
.search-wrap{flex:1;min-width:180px;position:relative}
.search-wrap input{width:100%;background:var(--input-bg);border:1px solid var(--input-border);border-radius:6px;padding:8px 10px 8px 32px;color:var(--text);font-size:.85em;outline:none;transition:border-color .15s;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='15' height='15' fill='%23636e7b' viewBox='0 0 16 16'%3E%3Cpath d='M11.742 10.344a6.5 6.5 0 1 0-1.397 1.398l3.85 3.85a1 1 0 0 0 1.415-1.414l-3.85-3.85zm-5.242.156a5 5 0 1 1 0-10 5 5 0 0 1 0 10z'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:9px center}
.search-wrap input:focus{border-color:var(--input-focus)}
.filter-select{background:var(--input-bg);border:1px solid var(--input-border);border-radius:6px;padding:8px 10px;color:var(--text);font-size:.85em;outline:none;cursor:pointer;transition:border-color .15s}
.filter-select:focus{border-color:var(--input-focus)}
.btn-refresh{background:none;border:1px solid var(--border);color:var(--text2);border-radius:6px;padding:8px 14px;font-size:.85em;cursor:pointer;white-space:nowrap;transition:border-color .15s,color .15s}
.btn-refresh:hover:not(:disabled){border-color:var(--blue);color:var(--blue)}
.btn-refresh:disabled{opacity:.5;cursor:not-allowed}
.scan-info{font-size:.78em;color:var(--text3);white-space:nowrap}

/* Scanning indicator */
.scanning-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--yellow);margin-right:5px;animation:pulse-dot 1s ease-in-out infinite}
@keyframes pulse-dot{0%,100%{opacity:1}50%{opacity:.3}}

/* Card grid */
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;margin-top:4px}

/* Media card */
.media-card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;display:flex;flex-direction:column;gap:6px;transition:border-color .15s}
.media-card:hover{border-color:var(--border2)}
.media-card.show-card,.media-card.movie-card{cursor:pointer;position:relative;padding-right:32px}
.media-card.show-card:hover,.media-card.movie-card:hover{border-color:var(--blue)}
.media-card.show-card::after,.media-card.movie-card::after{content:'\203A';position:absolute;right:14px;top:50%;transform:translateY(-50%);color:var(--text3);font-size:1.2em;transition:color .15s,transform .15s}
.media-card.show-card:hover::after,.media-card.movie-card:hover::after{color:var(--blue);transform:translateY(-50%) translateX(2px)}
.card-title{font-size:.9em;font-weight:500;color:var(--text);line-height:1.35}
.card-year{color:var(--text2);font-weight:400}
.card-meta{font-size:.78em;color:var(--text2)}
.card-badges{display:flex;gap:5px;flex-wrap:wrap}

/* Source badges */
.badge-local{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:#3fb9500f;color:var(--green);border:1px solid #3fb95033}
.badge-debrid{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:#58a6ff0f;color:var(--blue);border:1px solid #58a6ff33}
[data-theme="light"] .badge-local{background:#1a7f371a;border-color:#1a7f3740}
[data-theme="light"] .badge-debrid{background:#0969da1a;border-color:#0969da40}

/* Spinner */
.spinner{display:inline-block;width:16px;height:16px;border:2px solid var(--border);border-top-color:var(--blue);border-radius:50%;animation:spin .6s linear infinite;vertical-align:middle}
@keyframes spin{to{transform:rotate(360deg)}}

/* State panels */
.state-panel{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;min-height:220px;color:var(--text2);font-size:.9em;text-align:center;padding:24px}
.state-panel .state-hint{font-size:.82em;color:var(--text3)}
.state-panel.error-state{color:var(--red)}

/* Theme toggle */
.theme-toggle{background:none;border:1px solid var(--border);color:var(--text2);border-radius:6px;cursor:pointer;padding:4px 8px;font-size:.85em;line-height:1;transition:border-color .15s,color .15s}
.theme-toggle:hover{border-color:var(--blue);color:var(--blue)}

/* Detail view */
.detail-view{max-width:900px}
.detail-back{display:inline-block;background:none;border:none;color:var(--blue);cursor:pointer;font-size:.85em;margin-bottom:12px;user-select:none;padding:0;font-family:inherit}
.detail-back:hover{text-decoration:underline}
.detail-header{margin-bottom:16px}
.detail-header h2{font-size:1.3em;font-weight:600;margin-bottom:6px}
.detail-header .card-badges{margin-top:4px}

/* Season accordion */
.season-section{border:1px solid var(--border);border-radius:8px;margin-bottom:8px;overflow:hidden}
.season-header{padding:10px 14px;cursor:pointer;font-size:.9em;font-weight:500;color:var(--text);background:var(--card);display:flex;align-items:center;gap:8px;user-select:none;transition:background-color .15s}
.season-header:hover{background:var(--border2)}
.season-chevron{font-size:.7em;color:var(--text2);width:14px;text-align:center;transition:transform .15s}
.season-header.expanded .season-chevron{transform:rotate(90deg)}

/* Episode table */
.episode-table{width:100%;border-collapse:collapse}
.episode-table tr{border-top:1px solid var(--border)}
.episode-table td{padding:7px 14px;font-size:.82em;color:var(--text)}
.ep-num{font-weight:600;color:var(--text2);white-space:nowrap;width:50px}
.ep-file{color:var(--text);word-break:break-all}
.ep-source{white-space:nowrap;text-align:right}
.ep-actions{white-space:nowrap;text-align:right;width:80px}

/* Preference & action controls */
.pref-row{display:flex;align-items:center;gap:8px;margin-top:8px}
.pref-select{background:var(--input-bg);border:1px solid var(--input-border);border-radius:6px;padding:4px 8px;color:var(--text);font-size:.82em;outline:none;cursor:pointer}
.pref-select:focus{border-color:var(--input-focus)}
.btn-action{background:none;border:1px solid var(--border);color:var(--text2);border-radius:4px;padding:2px 8px;font-size:.75em;cursor:pointer;white-space:nowrap;transition:border-color .15s,color .15s}
.btn-action:hover{border-color:var(--blue);color:var(--blue)}
.btn-action.danger{color:var(--red);border-color:#f8514933}
.btn-action.danger:hover{border-color:var(--red);background:#f851490f}
.btn-action:disabled{opacity:.5;cursor:not-allowed}
.season-actions{margin-left:auto;display:flex;gap:4px}
.transfer-msg{font-size:.78em;color:var(--yellow);margin-top:4px}

/* Detail hero with poster */
.detail-hero{display:flex;gap:16px;margin-bottom:16px}
.detail-poster{width:150px;min-width:150px;border-radius:8px;overflow:hidden}
.detail-poster img{width:100%;display:block;border-radius:8px}
.detail-info{flex:1;min-width:0}
.detail-overview{font-size:.85em;color:var(--text2);margin-top:8px;line-height:1.5;max-height:6em;overflow:hidden;-webkit-mask-image:linear-gradient(to bottom,black 60%,transparent);mask-image:linear-gradient(to bottom,black 60%,transparent)}
.detail-status{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:var(--border);color:var(--text2);margin-left:6px}
.detail-runtime{font-size:.82em;color:var(--text2);margin-top:6px}

/* Episode titles and missing */
.ep-title{color:var(--text2);font-size:.78em;display:block}
.ep-date{color:var(--text3);font-size:.75em;white-space:nowrap}
.ep-missing td{color:var(--text3)}
.badge-missing{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:#d299220f;color:var(--yellow);border:1px solid #d2992233}
[data-theme="light"] .badge-missing{background:#9a67001a;border-color:#9a670040}

/* Season progress */
.season-progress{font-size:.78em;color:var(--text3);margin-left:6px}

/* Footer */
.footer{color:var(--text3);font-size:.78em;text-align:right;margin-top:16px}

/* Responsive */
@media(max-width:640px){
  .controls{gap:6px}
  .search-wrap{min-width:120px}
  .scan-info{display:none}
  .header{flex-direction:column;align-items:flex-start}
  .episode-table{display:block;overflow-x:auto}
  .detail-hero{flex-direction:column}
  .detail-poster{width:120px}
}
:focus-visible{outline:2px solid var(--blue);outline-offset:2px}
@media(prefers-reduced-motion:reduce){*{animation-duration:.01ms!important;animation-iteration-count:1!important;transition-duration:.01ms!important}}
</style>
<script>(function(){try{var t=localStorage.getItem('pd_zurg_theme');if(t){document.documentElement.setAttribute('data-theme',t);document.querySelector('meta[name="color-scheme"]').content=t==='light'?'light':'dark';}}catch(e){}})()</script>
</head>
<body>
<div class="header">
  <h1><a href="/status" style="color:inherit;text-decoration:none">pd_zurg</a></h1>
  <div class="nav">
    <a href="/status">Dashboard</a>
    <span class="current">Library</span>
    <a href="/settings">Settings</a>
    <button class="theme-toggle" onclick="toggleTheme()" title="Toggle light/dark theme" id="theme-btn">&#x2600;&#xFE0F;</button>
  </div>
</div>

<div class="tabs" role="tablist">
  <div class="tab active" role="tab" tabindex="0" aria-selected="true" aria-controls="tab-movies"
       onclick="switchTab('movies')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();switchTab('movies')}">
    Movies<span class="badge" id="badge-movies">0</span>
  </div>
  <div class="tab" role="tab" tabindex="0" aria-selected="false" aria-controls="tab-shows"
       onclick="switchTab('shows')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();switchTab('shows')}">
    Shows<span class="badge" id="badge-shows">0</span>
  </div>
</div>

<div class="controls">
  <div class="search-wrap">
    <input type="search" id="search-input" placeholder="Search titles..." autocomplete="off"
           oninput="clearTimeout(_searchTimer);_searchTimer=setTimeout(applyFilters,150)" aria-label="Search titles">
  </div>
  <select class="filter-select" id="source-filter" onchange="applyFilters()" aria-label="Filter by source">
    <option value="">All Sources</option>
    <option value="local">Local Only</option>
    <option value="debrid">Debrid Only</option>
  </select>
  <button class="btn-refresh" id="btn-refresh" onclick="triggerRefresh()">Refresh</button>
  <span class="scan-info" id="scan-info"></span>
</div>

<div id="content-area">
  <div class="state-panel">
    <span class="spinner"></span>
    <span>Loading library...</span>
  </div>
</div>

<div class="footer" id="footer"></div>

<script>
// ---------------------------------------------------------------------------
// Theme
// ---------------------------------------------------------------------------
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  document.querySelector('meta[name="color-scheme"]').content = theme === 'light' ? 'light' : 'dark';
  document.getElementById('theme-btn').innerHTML = theme === 'light' ? '\u{1F319}' : '\u{2600}\u{FE0F}';
}
function toggleTheme() {
  const cur = document.documentElement.getAttribute('data-theme') || 'dark';
  const next = cur === 'dark' ? 'light' : 'dark';
  applyTheme(next);
  try { localStorage.setItem('pd_zurg_theme', next); } catch(e) {}
}
(function() { const t = document.documentElement.getAttribute('data-theme'); if (t) applyTheme(t); })();

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let _allMovies = [];
let _allShows  = [];
let _activeTab = 'movies';
let _lastScan  = null;
let _scanDurationMs = null;
let _scanning  = false;
let _tsRefreshTimer = null;
let _displayedItems = [];
let _inDetailView = false;
let _preferences = {};
let _detailSeasons = [];
let _downloadServices = {show: null, movie: null};
let _searchTimer = null;

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function esc(s) {
  const d = document.createElement('div');
  d.appendChild(document.createTextNode(String(s ?? '')));
  return d.innerHTML;
}

function relativeTime(isoStr) {
  if (!isoStr) return '';
  const diff = Math.floor((Date.now() - new Date(isoStr).getTime()) / 1000);
  if (diff < 5)   return 'just now';
  if (diff < 60)  return diff + 's ago';
  if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
  if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
  return Math.floor(diff / 86400) + 'd ago';
}

function updateScanInfo() {
  const el = document.getElementById('scan-info');
  if (_scanning) {
    el.innerHTML = '<span class="scanning-dot"></span>Refreshing...';
    return;
  }
  if (_lastScan) {
    el.textContent = 'Last scanned: ' + relativeTime(_lastScan);
  } else {
    el.textContent = '';
  }
}

// ---------------------------------------------------------------------------
// Tab switching
// ---------------------------------------------------------------------------
function switchTab(name) {
  _activeTab = name;
  document.querySelectorAll('.tab').forEach(function(t) {
    const active = t.getAttribute('aria-controls') === 'tab-' + name;
    t.classList.toggle('active', active);
    t.setAttribute('aria-selected', active ? 'true' : 'false');
  });
  applyFilters();
}

// ---------------------------------------------------------------------------
// Filtering & rendering
// ---------------------------------------------------------------------------
function buildBadges(source) {
  if (source === 'both') {
    return '<span class="badge-local">Local</span><span class="badge-debrid">Debrid</span>';
  }
  if (source === 'local') return '<span class="badge-local">Local</span>';
  if (source === 'debrid') return '<span class="badge-debrid">Debrid</span>';
  return '<span class="badge-debrid">' + esc(source) + '</span>';
}

function buildCard(item, index) {
  var metaLine = '';
  if (item.type === 'show' && (item.seasons || item.episodes)) {
    var parts = [];
    if (item.seasons) parts.push(item.seasons + ' Season' + (item.seasons !== 1 ? 's' : ''));
    if (item.episodes) parts.push(item.episodes + ' Episode' + (item.episodes !== 1 ? 's' : ''));
    metaLine = '<div class="card-meta">' + parts.join(' &middot; ') + '</div>';
  }
  var isShow = item.type === 'show' && item.season_data && item.season_data.length > 0;
  var isMovie = item.type === 'movie';
  var isClickable = isShow || isMovie;
  var cardClass = isShow ? ' show-card' : (isMovie ? ' movie-card' : '');
  var clickAttr = isClickable ? ' onclick="showDetail(' + index + ')" tabindex="0" role="button" onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();showDetail(' + index + ')}"' : '';
  return '<div class="media-card' + cardClass + '"' + clickAttr + '>'
    + '<div class="card-title">' + esc(item.title) + '</div>'
    + '<div class="card-badges">' + buildBadges(item.source) + '</div>'
    + metaLine
    + '</div>';
}

function applyFilters() {
  const query  = document.getElementById('search-input').value.trim().toLowerCase();
  const source = document.getElementById('source-filter').value;
  const dataset = _activeTab === 'movies' ? _allMovies : _allShows;

  let filtered = dataset;

  if (source) {
    filtered = filtered.filter(function(item) {
      if (source === 'local')  return item.source === 'local'  || item.source === 'both';
      if (source === 'debrid') return item.source === 'debrid' || item.source === 'both';
      return true;
    });
  }

  if (query) {
    filtered = filtered.filter(function(item) {
      return item.title.toLowerCase().indexOf(query) !== -1;
    });
  }

  // Alphabetical sort
  filtered = filtered.slice().sort(function(a, b) {
    return a.title.localeCompare(b.title);
  });

  renderGrid(filtered);
  updateBadges(filtered.length);
}

function renderGrid(items) {
  const area = document.getElementById('content-area');
  _displayedItems = items;
  if (!items.length) {
    const isFiltered = document.getElementById('search-input').value.trim()
      || document.getElementById('source-filter').value;
    if (isFiltered) {
      area.innerHTML = '<div class="state-panel"><div>No results match your filters.</div></div>';
    } else {
      area.innerHTML = '<div class="state-panel">'
        + '<div>No media found.</div>'
        + '<div class="state-hint">Make sure your debrid mount and local paths are configured correctly.</div>'
        + '</div>';
    }
    return;
  }
  area.innerHTML = '<div class="grid">' + items.map(function(item, i) { return buildCard(item, i); }).join('') + '</div>';
}

function updateBadges(filteredCount) {
  const query  = document.getElementById('search-input').value.trim();
  const source = document.getElementById('source-filter').value;
  const isFiltered = query || source;

  if (isFiltered) {
    document.getElementById('badge-movies').textContent =
      _activeTab === 'movies' ? String(filteredCount) : String(_allMovies.length);
    document.getElementById('badge-shows').textContent =
      _activeTab === 'shows'  ? String(filteredCount) : String(_allShows.length);
  } else {
    document.getElementById('badge-movies').textContent = String(_allMovies.length);
    document.getElementById('badge-shows').textContent  = String(_allShows.length);
  }
}

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------
function fetchLibrary() {
  fetch('/api/library')
    .then(function(r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    })
    .then(function(data) {
      _allMovies      = Array.isArray(data.movies) ? data.movies : [];
      _allShows       = Array.isArray(data.shows)  ? data.shows  : [];
      _preferences    = data.preferences || {};
      _downloadServices = data.download_services || {show: null, movie: null};
      _lastScan       = data.last_scan || null;
      _scanDurationMs = data.scan_duration_ms || null;

      applyFilters();
      updateScanInfo();

      if (_scanDurationMs != null) {
        const footerEl = document.getElementById('footer');
        footerEl.textContent = 'Scan completed in ' + _scanDurationMs + ' ms';
      }
    })
    .catch(function(err) {
      document.getElementById('content-area').innerHTML =
        '<div class="state-panel error-state"><div>Failed to load library.</div>'
        + '<div class="state-hint">' + esc(String(err)) + '</div></div>';
      updateBadges(0);
    });
}

function triggerRefresh() {
  if (_scanning) return;
  _scanning = true;
  document.getElementById('btn-refresh').disabled = true;
  updateScanInfo();

  fetch('/api/library/refresh', {method: 'POST'})
    .catch(function() {})
    .finally(function() {
      setTimeout(function() {
        fetchLibrary();
        _scanning = false;
        document.getElementById('btn-refresh').disabled = false;
        updateScanInfo();
      }, 3000);
    });
}

// ---------------------------------------------------------------------------
// Show detail view
// ---------------------------------------------------------------------------
function normTitle(title) {
  return title.toLowerCase().replace(/\s*\(\d{4}\)\s*$/, '').trim();
}

var _detailItem = null;
var _detailMeta = null;

function showDetail(index) {
  var item = _displayedItems[index];
  if (!item) return;
  if (item.type === 'show' && (!item.season_data || !item.season_data.length)) return;
  _inDetailView = true;
  _detailItem = item;
  _detailMeta = null;
  document.title = (item.title || '') + ' \u2014 pd_zurg Library';

  document.querySelector('.tabs').style.display = 'none';
  document.querySelector('.controls').style.display = 'none';
  document.getElementById('footer').style.display = 'none';

  _renderDetail();
  var backBtn = document.querySelector('.detail-back');
  if (backBtn) backBtn.focus();

  // Fetch TMDB metadata
  var params = 'title=' + encodeURIComponent(item.title) + '&type=' + encodeURIComponent(item.type);
  if (item.year) params += '&year=' + item.year;
  fetch('/api/library/metadata?' + params)
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(meta) {
      if (meta && _inDetailView && _detailItem === item) {
        _detailMeta = meta;
        _renderDetail();
      }
    })
    .catch(function() {});
}

function _renderDetail() {
  var item = _detailItem;
  var meta = _detailMeta;
  if (!item) return;

  if (item.type === 'movie') {
    _renderMovieDetail(item, meta);
  } else {
    _renderShowDetail(item, meta);
  }
}

function _renderMovieDetail(movie, meta) {
  var area = document.getElementById('content-area');
  var html = '<div class="detail-view">';
  html += '<button class="detail-back" onclick="hideDetail()" tabindex="0">&larr; Back to Library</button>';

  html += '<div class="detail-hero">';
  if (meta && meta.poster_url) {
    html += '<div class="detail-poster"><img src="' + esc(meta.poster_url) + '" alt="Poster for ' + esc(movie.title) + '"></div>';
  }
  html += '<div class="detail-info">';
  html += '<h2>' + esc(movie.title);
  if (movie.year) html += ' <span class="card-year">(' + esc(String(movie.year)) + ')</span>';
  html += '</h2>';
  html += '<div class="card-badges">' + buildBadges(movie.source) + '</div>';
  if (meta) {
    var runtimeParts = [];
    if (meta.runtime) runtimeParts.push(esc(String(meta.runtime)) + ' min');
    if (meta.release_date) runtimeParts.push('Released ' + esc(meta.release_date));
    if (runtimeParts.length) html += '<div class="detail-runtime">' + runtimeParts.join(' &middot; ') + '</div>';
    if (meta.overview) html += '<div class="detail-overview">' + esc(meta.overview) + '</div>';
  }
  if (movie.source === 'debrid' && _downloadServices.movie) {
    var movieBtnLabel = _downloadServices.movie === 'overseerr' ? 'Request in Overseerr' : 'Download via Radarr';
    html += '<div style="margin-top:10px"><button class="btn-action" onclick="downloadMovie()">' + movieBtnLabel + '</button></div>';
  } else if (movie.source === 'debrid' && !_downloadServices.movie) {
    html += '<div style="margin-top:10px;font-size:.82em;color:var(--text3)">To download locally, configure <a href="/settings">Radarr or Overseerr</a> in Settings.</div>';
  }
  html += '</div></div>';
  html += '<div id="transfer-msg"></div>';
  html += '</div>';
  area.innerHTML = html;
}

function _mergeShowMeta(show, meta) {
  if (!meta || !meta.seasons) return show.season_data || [];

  var fileLookup = {};
  (show.season_data || []).forEach(function(s) {
    fileLookup[s.number] = {};
    s.episodes.forEach(function(ep) { fileLookup[s.number][ep.number] = ep; });
  });

  var merged = [];
  meta.seasons.forEach(function(tmdbS) {
    var fileEps = fileLookup[tmdbS.number] || {};
    var episodes = [];
    tmdbS.episodes.forEach(function(te) {
      var fe = fileEps[te.number];
      if (fe) {
        episodes.push({number: te.number, title: te.title, air_date: te.air_date, file: fe.file, source: fe.source});
        delete fileEps[te.number];
      } else {
        episodes.push({number: te.number, title: te.title, air_date: te.air_date, file: null, source: 'missing'});
      }
    });
    // Append file episodes not in TMDB
    var remaining = Object.keys(fileEps);
    for (var ri = 0; ri < remaining.length; ri++) {
      episodes.push(fileEps[remaining[ri]]);
    }
    episodes.sort(function(a, b) { return a.number - b.number; });
    var haveCount = episodes.filter(function(e) { return e.source !== 'missing'; }).length;
    merged.push({number: tmdbS.number, total_episodes: tmdbS.total_episodes, episode_count: haveCount, episodes: episodes});
  });
  // Append file seasons not in TMDB
  (show.season_data || []).forEach(function(s) {
    if (!meta.seasons.some(function(ms) { return ms.number === s.number; })) {
      merged.push(s);
    }
  });
  merged.sort(function(a, b) { return a.number - b.number; });
  return merged;
}

function _renderShowDetail(show, meta) {
  var area = document.getElementById('content-area');
  var nk = normTitle(show.title);
  var curPref = _preferences[nk] || 'none';
  var seasons = meta ? _mergeShowMeta(show, meta) : (show.season_data || []);
  _detailSeasons = seasons;

  // Save expanded state from previous render
  var expandedNums = {};
  var prevHeaders = document.querySelectorAll('.season-header.expanded');
  for (var pi = 0; pi < prevHeaders.length; pi++) {
    var ds = prevHeaders[pi].getAttribute('data-season');
    if (ds) expandedNums[ds] = true;
  }
  var hasPrev = Object.keys(expandedNums).length > 0;

  var html = '<div class="detail-view">';
  html += '<button class="detail-back" onclick="hideDetail()" tabindex="0">&larr; Back to Library</button>';

  html += '<div class="detail-hero">';
  if (meta && meta.poster_url) {
    html += '<div class="detail-poster"><img src="' + esc(meta.poster_url) + '" alt="Poster for ' + esc(show.title) + '"></div>';
  }
  html += '<div class="detail-info">';
  html += '<h2>' + esc(show.title);
  if (show.year) html += ' <span class="card-year">(' + esc(String(show.year)) + ')</span>';
  if (meta && meta.status) html += '<span class="detail-status">' + esc(meta.status) + '</span>';
  html += '</h2>';
  html += '<div class="card-badges">' + buildBadges(show.source) + '</div>';
  if (meta && meta.overview) html += '<div class="detail-overview">' + esc(meta.overview) + '</div>';
  if ((show.source === 'debrid' || show.source === 'both') && !_downloadServices.show) {
    html += '<div style="font-size:.82em;color:var(--text3);margin-top:8px">To download episodes locally, configure <a href="/settings">Sonarr or Overseerr</a> in Settings.</div>';
  }
  html += '<div class="pref-row"><label style="font-size:.82em;color:var(--text2)" title="When both local and debrid copies exist, which should your media server prefer">Preference:</label>';
  html += '<select class="pref-select" onchange="onPrefChange(this.value)" title="When both local and debrid copies exist, which should your media server prefer">';
  html += '<option value="none"' + (curPref === 'none' ? ' selected' : '') + '>No Preference</option>';
  html += '<option value="prefer-local"' + (curPref === 'prefer-local' ? ' selected' : '') + '>Prefer Local</option>';
  html += '<option value="prefer-debrid"' + (curPref === 'prefer-debrid' ? ' selected' : '') + '>Prefer Debrid</option>';
  html += '</select></div>';
  html += '</div></div>';

  for (var si = 0; si < seasons.length; si++) {
    var season = seasons[si];
    var expanded = hasPrev ? !!expandedNums[String(season.number)] : si === 0;
    var hasDebrid = false, hasLocal = false, debridCount = 0;
    for (var ci = 0; ci < season.episodes.length; ci++) {
      if (season.episodes[ci].source === 'debrid') { hasDebrid = true; debridCount++; }
      if (season.episodes[ci].source === 'local' || season.episodes[ci].source === 'both') hasLocal = true;
    }
    var progressText = '';
    if (season.total_episodes) {
      progressText = '<span class="season-progress">' + season.episode_count + '/' + season.total_episodes + '</span>';
    }
    html += '<div class="season-section">';
    html += '<div class="season-header' + (expanded ? ' expanded' : '') + '" data-season="' + season.number + '" tabindex="0" role="button" aria-expanded="' + expanded + '" onclick="toggleSeason(this)" onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();toggleSeason(this)}">';
    html += '<span class="season-chevron">&#9654;</span>';
    html += 'Season ' + season.number + ' &mdash; ' + season.episode_count + ' episode' + (season.episode_count !== 1 ? 's' : '') + progressText;
    html += '<span class="season-actions">';
    if (hasDebrid && _downloadServices.show) {
      if (_downloadServices.show === 'overseerr') {
        html += '<button class="btn-action" onclick="event.stopPropagation();requestSeason(' + season.number + ')">Request Season</button>';
      } else {
        var dlLabel = 'Download ' + debridCount + ' Episode' + (debridCount !== 1 ? 's' : '');
        html += '<button class="btn-action" onclick="event.stopPropagation();dlSeason(' + si + ')">' + dlLabel + '</button>';
      }
    }
    if (hasLocal) html += '<button class="btn-action danger" onclick="event.stopPropagation();rmSeason(' + si + ')">Remove Local</button>';
    html += '</span>';
    html += '</div>';
    html += '<div class="season-episodes"' + (expanded ? '' : ' style="display:none"') + '>';
    html += '<table class="episode-table"><tbody>';
    var eps = season.episodes || [];
    for (var ei = 0; ei < eps.length; ei++) {
      var ep = eps[ei];
      var epNum = String(ep.number);
      if (epNum.length < 2) epNum = '0' + epNum;
      var isMissing = ep.source === 'missing';
      var epLabel = 'S' + (season.number < 10 ? '0' : '') + season.number + 'E' + epNum;
      html += '<tr' + (isMissing ? ' class="ep-missing"' : '') + '>';
      html += '<td class="ep-num">E' + esc(epNum) + '</td>';
      html += '<td class="ep-file">';
      if (ep.title) html += '<span class="ep-title">' + esc(ep.title) + '</span>';
      if (ep.file) html += esc(ep.file);
      else if (!ep.title) html += '<span style="color:var(--text3)">&mdash;</span>';
      if (ep.air_date) html += ' <span class="ep-date">' + esc(ep.air_date) + '</span>';
      html += '</td>';
      html += '<td class="ep-source">';
      if (isMissing) html += '<span class="badge-missing">Missing</span>';
      else html += buildBadges(ep.source);
      html += '</td>';
      html += '<td class="ep-actions">';
      if (!isMissing) {
        if (ep.source === 'debrid' && _downloadServices.show && _downloadServices.show !== 'overseerr') {
          html += '<button class="btn-action" aria-label="Download ' + epLabel + '" onclick="downloadEp(' + season.number + ',' + ep.number + ')">Download</button>';
        }
        if (ep.source === 'local' || ep.source === 'both') {
          html += '<button class="btn-action danger" aria-label="Remove ' + epLabel + '" onclick="removeEp(' + season.number + ',' + ep.number + ')">Remove</button>';
        }
      }
      html += '</td>';
      html += '</tr>';
    }
    html += '</tbody></table></div></div>';
  }

  html += '<div id="transfer-msg"></div>';
  html += '</div>';
  area.innerHTML = html;
}

function hideDetail() {
  _inDetailView = false;
  _detailItem = null;
  _detailSeasons = [];
  document.title = 'pd_zurg Library';
  document.querySelector('.tabs').style.display = '';
  document.querySelector('.controls').style.display = '';
  document.getElementById('footer').style.display = '';
  applyFilters();
  document.getElementById('search-input').focus();
}

function toggleSeason(headerEl) {
  var episodes = headerEl.nextElementSibling;
  var isExpanded = headerEl.classList.toggle('expanded');
  headerEl.setAttribute('aria-expanded', isExpanded ? 'true' : 'false');
  episodes.style.display = isExpanded ? '' : 'none';
}

// ---------------------------------------------------------------------------
// Preference & action API calls
// ---------------------------------------------------------------------------
function onPrefChange(pref) {
  if (!_detailItem) return;
  var nk = normTitle(_detailItem.title);
  fetch('/api/library/preference', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({title: nk, preference: pref})
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (pref === 'none') { delete _preferences[nk]; }
    else { _preferences[nk] = pref; }
  }).catch(function(e) { alert('Failed to save preference: ' + e); });
}

function downloadEp(season, episode) {
  if (!_detailItem) return;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  _postDownload({
    title: _detailItem.title, type: 'show', tmdb_id: tmdbId,
    season: season, episodes: [episode]
  });
}

function removeEp(season, episode) {
  if (!_detailItem) return;
  if (!confirm('Remove local copy of S' + (season < 10 ? '0' : '') + season + 'E' + (episode < 10 ? '0' : '') + episode + '?')) return;
  _postRemove(_detailItem.title, [{season: season, episode: episode}]);
}

function dlSeason(seasonIdx) {
  if (!_detailItem || !_detailSeasons[seasonIdx]) return;
  var season = _detailSeasons[seasonIdx];
  var eps = [];
  for (var i = 0; i < season.episodes.length; i++) {
    if (season.episodes[i].source === 'debrid') {
      eps.push(season.episodes[i].number);
    }
  }
  if (!eps.length) return;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  _postDownload({
    title: _detailItem.title, type: 'show', tmdb_id: tmdbId,
    season: season.number, episodes: eps
  });
}

function requestSeason(seasonNumber) {
  if (!_detailItem) return;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  _postDownload({
    title: _detailItem.title, type: 'show', tmdb_id: tmdbId,
    season: seasonNumber, episodes: []
  });
}

function downloadMovie() {
  if (!_detailItem) return;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  _postDownload({
    title: _detailItem.title, type: 'movie', tmdb_id: tmdbId
  });
}

function rmSeason(seasonIdx) {
  if (!_detailItem || !_detailSeasons[seasonIdx]) return;
  var season = _detailSeasons[seasonIdx];
  var eps = [];
  for (var i = 0; i < season.episodes.length; i++) {
    if (season.episodes[i].source === 'local' || season.episodes[i].source === 'both') {
      eps.push({season: season.number, episode: season.episodes[i].number});
    }
  }
  if (!eps.length) return;
  if (!confirm('Remove ' + eps.length + ' local episode(s) from Season ' + season.number + '?')) return;
  _postRemove(_detailItem.title, eps);
}

var _svcNames = {sonarr: 'Sonarr', radarr: 'Radarr', overseerr: 'Overseerr'};
var _dlInFlight = false;

function _postDownload(payload) {
  if (_dlInFlight) return;
  _dlInFlight = true;
  // Disable all download/request buttons while in flight
  var btns = document.querySelectorAll('.btn-action:not(.danger)');
  for (var i = 0; i < btns.length; i++) btns[i].disabled = true;

  var msg = document.getElementById('transfer-msg');
  var svc = payload.type === 'movie' ? _downloadServices.movie : _downloadServices.show;
  var svcName = _svcNames[svc] || svc;
  var actionWord = svc === 'overseerr' ? 'Requesting' : 'Sending to ' + svcName;
  if (msg) msg.innerHTML = '<span class="scanning-dot"></span>' + esc(actionWord) + '...';
  fetch('/api/library/download', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r) {
    return r.json().then(function(d) { return {ok: r.ok, d: d}; });
  }).then(function(res) {
    var d = res.d;
    var errMsg = (!res.ok || d.status === 'error') ? (d.error || d.message || 'Unknown error') : null;
    if (msg) msg.textContent = errMsg ? 'Error: ' + errMsg : (d.message || 'Sent.');
  }).catch(function(e) {
    if (msg) msg.textContent = 'Request failed: ' + e;
  }).finally(function() {
    _dlInFlight = false;
    var btns = document.querySelectorAll('.btn-action:not(.danger)');
    for (var i = 0; i < btns.length; i++) btns[i].disabled = false;
  });
}

function _postRemove(title, episodes) {
  var msg = document.getElementById('transfer-msg');
  fetch('/api/library/remove-local', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({title: title, episodes: episodes})
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (d.status === 'removed') {
      if (msg) msg.textContent = 'Removed ' + d.removed + ' file(s).';
      setTimeout(_refreshDetailData, 1000);
    } else if (d.error) {
      if (msg) msg.textContent = 'Error: ' + d.error;
    }
  }).catch(function(e) {
    if (msg) msg.textContent = 'Remove failed: ' + e;
  });
}

function _refreshDetailData() {
  // Refresh library data and stay in detail view if still open
  fetch('/api/library')
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(data) {
      if (!data) return;
      _allMovies = Array.isArray(data.movies) ? data.movies : [];
      _allShows  = Array.isArray(data.shows)  ? data.shows  : [];
      _preferences = data.preferences || {};
      _downloadServices = data.download_services || {show: null, movie: null};
      _lastScan = data.last_scan || null;
      if (_inDetailView && _detailItem) {
        // Find updated show/movie by title
        var items = _detailItem.type === 'movie' ? _allMovies : _allShows;
        var nk = normTitle(_detailItem.title);
        for (var i = 0; i < items.length; i++) {
          if (normTitle(items[i].title) === nk) {
            _detailItem = items[i];
            _renderDetail();
            return;
          }
        }
      }
      applyFilters();
    })
    .catch(function() {});
}

// ---------------------------------------------------------------------------
// Timestamp auto-refresh (every 30 s)
// ---------------------------------------------------------------------------
function startTsRefresh() {
  if (_tsRefreshTimer) clearInterval(_tsRefreshTimer);
  _tsRefreshTimer = setInterval(function() {
    if (!_scanning) updateScanInfo();
  }, 30000);
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
fetchLibrary();
startTsRefresh();
</script>
</body>
</html>'''
