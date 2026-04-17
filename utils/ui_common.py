"""Shared CSS, navigation, and helper functions for all web UI pages.

Provides a single source of truth for CSS variables, reset styles, the
navigation bar, and the unified button system. Each page template imports
these constants rather than defining its own copy.
"""

# ---------------------------------------------------------------------------
# Shared CSS custom properties, reset, typography, and components
# ---------------------------------------------------------------------------

BASE_CSS = r"""
/* === CSS Custom Properties === */
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--border2:#21262d;--text:#c9d1d9;--text2:#8b949e;--text3:#636e7b;--blue:#58a6ff;--green:#3fb950;--red:#f85149;--yellow:#d29922;--orange:#db6d28;--input-bg:#0d1117;--input-border:#30363d;--input-focus:#58a6ff;--motion-fast:100ms;--motion-normal:200ms;--motion-slow:300ms;--sidebar-bg:#010409;--sidebar-w:220px}
[data-theme="light"]{--bg:#f6f8fa;--card:#ffffff;--border:#d0d7de;--border2:#d8dee4;--text:#1f2328;--text2:#656d76;--text3:#8b949e;--blue:#0969da;--green:#1a7f37;--red:#cf222e;--yellow:#9a6700;--orange:#bc4c00;--input-bg:#ffffff;--input-border:#d0d7de;--input-focus:#0969da;--sidebar-bg:#f0f3f6}

/* === Reset === */
*{margin:0;padding:0;box-sizing:border-box}

/* === Typography === */
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);margin:0}
a{color:var(--blue);text-decoration:none}
a:hover{text-decoration:underline}

/* === Sidebar === */
.sidebar{position:fixed;top:0;left:0;bottom:0;width:var(--sidebar-w);background:var(--sidebar-bg);border-right:1px solid var(--border);display:flex;flex-direction:column;z-index:1000;overflow-y:auto;overflow-x:hidden}
.sidebar-brand{padding:14px 16px;display:flex;align-items:center;gap:8px}
.sidebar-brand a{font-size:1.2em;font-weight:700;color:var(--blue);text-decoration:none}
.sidebar-brand a:hover{text-decoration:none;opacity:.85}
.sidebar-brand-info{flex:1;min-width:0}
.sidebar-version{display:block;font-size:.7em;color:var(--text3);margin-top:1px}
.sidebar-nav{display:flex;flex-direction:column;padding:8px 0}
.sidebar-link{display:flex;align-items:center;gap:10px;padding:9px 16px;color:var(--text2);font-size:.85em;font-weight:500;text-decoration:none;border-left:3px solid transparent;transition:color var(--motion-fast),background var(--motion-fast),border-color var(--motion-fast)}
.sidebar-link:hover{color:var(--text);background:var(--border2);text-decoration:none}
.sidebar-link.active{color:var(--blue);border-left-color:var(--blue);background:rgba(88,166,255,.08)}
.sidebar-link.active:hover{background:rgba(88,166,255,.12)}
[data-theme="light"] .sidebar-link.active{background:rgba(9,105,218,.08)}
[data-theme="light"] .sidebar-link.active:hover{background:rgba(9,105,218,.12)}
.sidebar-link svg{width:18px;height:18px;flex-shrink:0;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
.sidebar-badge{display:inline-block;background:var(--red);color:#fff;border-radius:8px;font-size:.72em;font-weight:700;padding:1px 6px;margin-left:auto;min-width:16px;text-align:center;line-height:1.4}
.sidebar-divider{height:1px;background:var(--border);margin:4px 12px}
.sidebar-theme{background:none;border:none;color:var(--text3);cursor:pointer;font-size:1em;padding:4px;border-radius:4px;line-height:1;flex-shrink:0;transition:color var(--motion-fast)}
.sidebar-theme:hover{color:var(--blue)}

/* === Main Content === */
.main-content{margin-left:var(--sidebar-w);padding:20px;min-height:100vh}

/* === Mobile Sidebar === */
.hamburger-btn{display:none;position:fixed;top:8px;left:8px;z-index:1001;background:var(--bg);border:1px solid var(--border);color:var(--text2);border-radius:6px;padding:7px;cursor:pointer;line-height:0;transition:color var(--motion-fast),border-color var(--motion-fast)}
.hamburger-btn:hover,.hamburger-btn:focus-visible{color:var(--text);border-color:var(--blue)}
.hamburger-btn svg{display:block;width:20px;height:20px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
.sidebar-backdrop{display:none;position:fixed;inset:0;z-index:999;background:rgba(0,0,0,.5)}
.sidebar-backdrop.visible{display:block}
@media(max-width:768px){
  .sidebar{transform:translateX(-100%);transition:transform .3s ease}
  .sidebar.open{transform:translateX(0)}
  .hamburger-btn{display:block}
  .main-content{margin-left:0;padding:16px;padding-top:44px}
}

/* === Unified Button System === */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:8px 16px;border-radius:6px;font-size:.85em;font-weight:500;font-family:inherit;cursor:pointer;border:1px solid transparent;background:none;color:var(--text);transition:background var(--motion-fast),border-color var(--motion-fast),color var(--motion-fast),opacity var(--motion-fast);white-space:nowrap;text-decoration:none;line-height:1.4}
.btn:disabled{opacity:.5;cursor:not-allowed}
.btn:focus-visible{outline:2px solid var(--blue);outline-offset:2px}
.btn-ghost{background:none;border-color:var(--border);color:var(--text2)}
.btn-ghost:hover:not(:disabled){border-color:var(--blue);color:var(--blue)}
.btn-primary{background:var(--green);color:#fff;border-color:var(--green)}
.btn-primary:hover:not(:disabled){opacity:.85}
.btn-primary.dirty{box-shadow:0 0 0 2px var(--yellow);animation:pulse-save 2s ease-in-out infinite}
@keyframes pulse-save{0%,100%{box-shadow:0 0 0 2px var(--yellow)}50%{box-shadow:0 0 8px 2px var(--yellow)}}
.btn-danger{color:var(--red);border-color:#f8514933}
.btn-danger:hover:not(:disabled){border-color:var(--red);color:var(--red);background:#f851490f}
.btn-danger.filled{background:var(--red);color:#fff;border-color:var(--red)}
.btn-danger.filled:hover:not(:disabled){filter:brightness(1.15);background:var(--red)}
.btn-sm{padding:4px 10px;font-size:.78em}
.btn-icon{padding:4px;width:28px;height:28px;font-size:.9em;flex-shrink:0}
.btn.confirming{border-color:var(--orange);color:var(--orange);font-weight:600;animation:pulse-confirm .8s ease-in-out infinite}
.btn.confirming.btn-danger{border-color:var(--red);color:var(--red)}
@keyframes pulse-confirm{0%,100%{opacity:1}50%{opacity:.7}}

/* === Theme Toggle === */
.theme-toggle{background:none;border:1px solid var(--border);color:var(--text2);border-radius:6px;cursor:pointer;padding:4px 8px;font-size:.85em;line-height:1;transition:border-color var(--motion-fast),color var(--motion-fast)}
.theme-toggle:hover{border-color:var(--blue);color:var(--blue)}

/* === Spinner === */
.spinner{display:inline-block;width:14px;height:14px;border:2px solid var(--border);border-top-color:var(--blue);border-radius:50%;animation:spin .6s linear infinite;vertical-align:middle}
@keyframes spin{to{transform:rotate(360deg)}}

/* === Banner === */
.banner{padding:12px 16px;border-radius:8px;margin-bottom:16px;font-size:.9em;font-weight:500;display:none;line-height:1.5}
.banner.success{display:block;background:#3fb9501a;border:1px solid var(--green);color:var(--green)}
.banner.error,.banner.crit{display:block;background:#f851491a;border:1px solid var(--red);color:var(--red)}
.banner.warning,.banner.warn{display:block;background:#d299221a;border:1px solid var(--yellow);color:var(--yellow)}
.banner.info{display:block;background:#58a6ff1a;border:1px solid var(--blue);color:var(--blue)}

/* === Footer === */
.footer{color:var(--text3);font-size:.78em;text-align:right;margin-top:16px}

/* === Focus === */
:focus-visible{outline:2px solid var(--blue);outline-offset:2px}

/* === Reduced Motion === */
@media(prefers-reduced-motion:reduce){*{animation-duration:.01ms!important;animation-iteration-count:1!important;transition-duration:.01ms!important}}

"""

# ---------------------------------------------------------------------------
# Theme initialisation script (goes in <head> to prevent FOUC)
# ---------------------------------------------------------------------------

THEME_INIT_SCRIPT = (
    "<script>(function(){try{var t=localStorage.getItem('pd_zurg_theme');"
    "if(t){document.documentElement.setAttribute('data-theme',t);"
    "document.querySelector('meta[name=\"color-scheme\"]').content="
    "t==='light'?'light':'dark';}}catch(e){}})()</script>"
)

# ---------------------------------------------------------------------------
# Theme toggle JS (applyTheme + toggleTheme — included in page <script>)
# ---------------------------------------------------------------------------

THEME_TOGGLE_JS = r"""
function applyTheme(theme){
  document.documentElement.setAttribute('data-theme',theme);
  document.querySelector('meta[name="color-scheme"]').content=theme==='light'?'light':'dark';
  document.getElementById('theme-btn').textContent=theme==='light'?'\u{1F319}':'\u{2600}\u{FE0F}';
}
function toggleTheme(){
  var cur=document.documentElement.getAttribute('data-theme')||'dark';
  var next=cur==='dark'?'light':'dark';
  applyTheme(next);
  try{localStorage.setItem('pd_zurg_theme',next);}catch(e){}
}
(function(){var t=document.documentElement.getAttribute('data-theme');if(t)applyTheme(t);})();
"""

# ---------------------------------------------------------------------------
# Wanted badge JS (fetch library summary, update nav badge)
# ---------------------------------------------------------------------------

WANTED_BADGE_JS = r"""
(function tryWanted(attempt){
  fetch('/api/library').then(function(r){
    if(r.status===503&&attempt<5){setTimeout(function(){tryWanted(attempt+1)},3000);return null}
    return r.ok?r.json():null;
  }).then(function(data){
    if(!data)return;
    var count=0;
    (data.shows||[]).forEach(function(s){if(s.missing_episodes>0)count++});
    (data.movies||[]).forEach(function(m){if(m.missing_episodes>0)count++});
    var link=document.getElementById('nav-wanted-link');
    var badge=document.getElementById('nav-wanted-count');
    if(link&&badge&&count>0){link.style.display='';badge.textContent=count}
  }).catch(function(){});
})(0);
"""

# ---------------------------------------------------------------------------
# Dynamic favicon JS (changes color based on system health)
# ---------------------------------------------------------------------------

FAVICON_JS = r"""
(function(){
  var _fts=0;
  function _fsvg(c){return 'data:image/svg+xml,'+encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><path d="M58 2L22 52h20L34 98 78 42H54z" fill="'+c+'"/></svg>');}
  window.updateFavicon=function(h){
    var c=h==='crit'?'#f85149':h==='warn'?'#d29922':'#3fb950';
    var l=document.querySelector('link[rel="icon"]');
    if(l)l.href=_fsvg(c);
    _fts=Date.now();
  };
  function _poll(){
    if(Date.now()-_fts<25000){setTimeout(_poll,30000);return;}
    fetch('/api/status').then(function(r){return r.json()}).then(function(d){
      var h='ok';
      if(d.services)d.services.forEach(function(s){if(s.status!=='ok')h='crit';});
      (d.processes||[]).forEach(function(p){if(!p.running)h='crit';});
      (d.mounts||[]).forEach(function(m){if(!m.mounted||!m.accessible)h='crit';});
      if(d.system){if(d.system.memory_percent!=null&&d.system.memory_percent>85&&h==='ok')h='warn';if(d.system.cpu_percent!=null&&d.system.cpu_percent>85&&h==='ok')h='warn';}
      if(h==='ok'&&d.services)d.services.forEach(function(s){if(s.days_remaining!=null&&s.days_remaining<=7){h=s.days_remaining<=3?'crit':'warn';}});
      updateFavicon(h);
    }).catch(function(){updateFavicon('warn');});
    setTimeout(_poll,30000);
  }
  setTimeout(_poll,2000);
})();
"""


# ---------------------------------------------------------------------------
# Keyboard shortcuts JS (/ search, r refresh, Esc close, 1/2/3 tabs, ? help)
# ---------------------------------------------------------------------------

KEYBOARD_CSS = r"""
/* === Keyboard Help Overlay === */
.kb-overlay{display:none;position:fixed;inset:0;z-index:20000;background:rgba(0,0,0,.6);align-items:center;justify-content:center}
.kb-overlay.visible{display:flex}
.kb-dialog{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:24px 28px;max-width:380px;width:90%;box-shadow:0 8px 32px rgba(0,0,0,.4)}
.kb-dialog h3{margin-bottom:12px;font-size:1em;color:var(--text)}
.kb-dialog dl{display:grid;grid-template-columns:auto 1fr;gap:6px 16px;font-size:.85em}
.kb-dialog dt{text-align:right}
.kb-dialog dd{color:var(--text2);margin:0}
.kb-dialog kbd{display:inline-block;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:1px 6px;font-family:inherit;font-size:.85em;min-width:20px;text-align:center;color:var(--text)}
.kb-dialog .kb-close{margin-top:16px;text-align:right}
"""

KEYBOARD_JS = r"""
(function(){
  /* Keyboard shortcut overlay HTML (injected once) */
  var _kbEl=null;
  function _ensureOverlay(){
    if(_kbEl)return _kbEl;
    var d=document.createElement('div');
    d.className='kb-overlay';
    d.setAttribute('role','dialog');
    d.setAttribute('aria-label','Keyboard shortcuts');
    d.innerHTML='<div class="kb-dialog">'
      +'<h3>Keyboard Shortcuts</h3>'
      +'<dl>'
      +'<dt><kbd>/</kbd></dt><dd>Focus search</dd>'
      +'<dt><kbd>R</kbd></dt><dd>Refresh data</dd>'
      +'<dt><kbd>Esc</kbd></dt><dd>Close / clear</dd>'
      +'<dt><kbd>1</kbd> <kbd>2</kbd> <kbd>3</kbd></dt><dd>Switch tabs</dd>'
      +'<dt><kbd>?</kbd></dt><dd>Show this help</dd>'
      +'</dl>'
      +'<div class="kb-close"><button class="btn btn-ghost btn-sm" onclick="window._kbToggle()">Close <kbd>?</kbd></button></div>'
      +'</div>';
    d.addEventListener('click',function(e){if(e.target===d)window._kbToggle();});
    document.body.appendChild(d);
    _kbEl=d;
    return d;
  }
  window._kbToggle=function(){
    var o=_ensureOverlay();
    o.classList.toggle('visible');
  };

  document.addEventListener('keydown',function(e){
    /* Skip when typing in inputs */
    var tag=document.activeElement&&document.activeElement.tagName;
    var editable=tag==='INPUT'||tag==='TEXTAREA'||tag==='SELECT'||
      (document.activeElement&&document.activeElement.isContentEditable);

    /* Escape always works — close overlay/modal/search */
    if(e.key==='Escape'){
      var ov=document.querySelector('.kb-overlay.visible');
      if(ov){ov.classList.remove('visible');e.preventDefault();return;}
      /* If typing in an input, blur it first */
      if(editable&&document.activeElement){document.activeElement.blur();e.preventDefault();return;}
      /* Then try page-specific escape handler */
      if(typeof window.onKbEscape==='function'){window.onKbEscape();e.preventDefault();return;}
      return;
    }

    if(editable)return;
    if(e.ctrlKey||e.altKey||e.metaKey)return;

    if(e.key==='/'){
      e.preventDefault();
      /* Find a visible search input (multiple may exist across tabs) */
      var all=document.querySelectorAll('[data-kb="search"]');
      for(var i=0;i<all.length;i++){if(all[i].offsetParent!==null){all[i].focus();all[i].select();break;}}
      return;
    }
    if(e.key==='r'||e.key==='R'){
      e.preventDefault();
      var rb=document.querySelector('[data-kb="refresh"]');
      if(rb&&!rb.disabled)rb.click();
      return;
    }
    if(e.key==='?'){
      e.preventDefault();
      window._kbToggle();
      return;
    }
    if(e.key>='1'&&e.key<='9'){
      var tb=document.querySelector('[data-kb="tab-'+e.key+'"]');
      if(tb){e.preventDefault();tb.click();}
      return;
    }
  });
})();
"""

# ---------------------------------------------------------------------------
# Toast notification system
# ---------------------------------------------------------------------------

TOAST_CSS = r"""
/* === Toast Notifications === */
.toast-container{position:fixed;bottom:20px;right:20px;z-index:15000;display:flex;flex-direction:column-reverse;gap:8px;pointer-events:none;max-width:380px;width:calc(100% - 40px)}
.toast{pointer-events:auto;display:flex;align-items:flex-start;gap:10px;padding:12px 16px;border-radius:8px;font-size:.85em;line-height:1.4;box-shadow:0 4px 16px rgba(0,0,0,.3);animation:toast-in var(--motion-slow) ease forwards;opacity:0;transform:translateX(40px)}
.toast.removing{animation:toast-out var(--motion-normal) ease forwards}
.toast-success{background:#3fb9501a;border:1px solid var(--green);color:var(--green)}
.toast-error{background:#f851491a;border:1px solid var(--red);color:var(--red)}
.toast-warning{background:#d299221a;border:1px solid var(--yellow);color:var(--yellow)}
.toast-info{background:#58a6ff1a;border:1px solid var(--blue);color:var(--blue)}
.toast-msg{flex:1;word-break:break-word}
.toast-close{background:none;border:none;color:inherit;cursor:pointer;font-size:1.1em;padding:0 2px;opacity:.7;line-height:1;flex-shrink:0}
.toast-close:hover{opacity:1}
@keyframes toast-in{to{opacity:1;transform:translateX(0)}}
@keyframes toast-out{from{opacity:1;transform:translateX(0)}to{opacity:0;transform:translateX(40px)}}
@media(max-width:480px){.toast-container{right:10px;bottom:10px;max-width:calc(100% - 20px);width:calc(100% - 20px)}}
"""

TOAST_JS = r"""
(function(){
  var _tc=null;
  function _container(){
    if(_tc)return _tc;
    var c=document.createElement('div');
    c.className='toast-container';
    c.setAttribute('aria-live','polite');
    c.setAttribute('role','status');
    document.body.appendChild(c);
    _tc=c;
    return c;
  }
  window.showToast=function(msg,type,duration){
    type=type||'info';
    if(typeof duration==='undefined'||duration===null){
      duration=type==='error'?0:type==='warning'?8000:5000;
    }
    var t=document.createElement('div');
    t.className='toast toast-'+type;
    var m=document.createElement('span');
    m.className='toast-msg';
    m.textContent=msg;
    t.appendChild(m);
    var cb=document.createElement('button');
    cb.className='toast-close';
    cb.innerHTML='&times;';
    cb.title='Dismiss';
    cb.onclick=function(){_remove(t)};
    t.appendChild(cb);
    var c=_container();
    c.appendChild(t);
    /* Cap at 5 visible toasts — force-remove oldest synchronously */
    while(c.children.length>5){c.removeChild(c.children[0]);}
    if(duration>0){
      setTimeout(function(){_remove(t)},duration);
    }
  };
  function _remove(el){
    if(!el||!el.parentNode||el.classList.contains('removing'))return;
    el.classList.add('removing');
    setTimeout(function(){if(el.parentNode)el.parentNode.removeChild(el)},200);
  }
})();
"""

# ---------------------------------------------------------------------------
# Shared JS utilities (used by multiple pages)
# ---------------------------------------------------------------------------

SHARED_UTILS_JS = r"""
function esc(s){var d=document.createElement('div');d.appendChild(document.createTextNode(String(s==null?'':s)));return d.innerHTML;}
function timeAgo(ts){
  var sec=Math.floor((Date.now()-new Date(ts).getTime())/1000);
  if(sec<60)return sec+'s ago';
  if(sec<3600)return Math.floor(sec/60)+'m ago';
  if(sec<86400)return Math.floor(sec/3600)+'h ago';
  return Math.floor(sec/86400)+'d ago';
}
function fmt(s){
  if(s<60)return s+'s';
  if(s<3600)return Math.floor(s/60)+'m '+s%60+'s';
  var h=Math.floor(s/3600),m=Math.floor((s%3600)/60);
  if(h>=24){var d=Math.floor(h/24);return d+'d '+(h%24)+'h';}
  return h+'h '+m+'m';
}
function fmtBytes(b){
  if(b>1073741824)return(b/1073741824).toFixed(1)+'G';
  if(b>1048576)return(b/1048576).toFixed(0)+'M';
  return(b/1024).toFixed(0)+'K';
}
/* Auth detection */
window._hasAuth=false;
window._hasAuthReady=fetch('/api/auth/check').then(function(r){window._hasAuth=r.status!==403;}).catch(function(){});
/* Version display in sidebar */
fetch('/api/status').then(function(r){return r.json()}).then(function(d){
  var el=document.getElementById('header-meta');
  if(el&&d.version)el.textContent='v'+d.version;
}).catch(function(){});
/* Lazy confirm dialog */
function _ensureDialog(){
  var dlg=document.getElementById('confirm-dialog');
  if(dlg)return dlg;
  dlg=document.createElement('dialog');
  dlg.id='confirm-dialog';
  dlg.innerHTML='<h3 id="dlg-title"></h3><p id="dlg-msg"></p><div class="dlg-actions"><button class="dlg-btn dlg-cancel" onclick="document.getElementById(\'confirm-dialog\').close(\'cancel\')">Cancel</button><button class="dlg-btn dlg-confirm" id="dlg-ok">Confirm</button></div>';
  document.body.appendChild(dlg);
  return dlg;
}
function showConfirm(title,msg){
  return new Promise(function(resolve){
    var dlg=_ensureDialog();
    document.getElementById('dlg-title').textContent=title;
    document.getElementById('dlg-msg').textContent=msg;
    var okBtn=document.getElementById('dlg-ok');
    var handler=function(){dlg.close('ok');};
    okBtn.onclick=handler;
    dlg.onclose=function(){okBtn.onclick=null;resolve(dlg.returnValue==='ok');};
    dlg.showModal();
  });
}
"""

# ---------------------------------------------------------------------------
# Sidebar toggle JS (mobile hamburger, backdrop dismiss)
# ---------------------------------------------------------------------------

# Global functions (must be on window for inline onclick handlers)
SIDEBAR_JS_GLOBALS = r"""
function toggleSidebar(){
  document.getElementById('sidebar').classList.toggle('open');
  document.getElementById('sidebar-backdrop').classList.toggle('visible');
}
function closeSidebar(){
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('sidebar-backdrop').classList.remove('visible');
}
"""

# Init code that requires DOM to be ready
SIDEBAR_JS_INIT = r"""
  /* Close sidebar when clicking a nav link on mobile */
  var links=document.querySelectorAll('.sidebar-link');
  for(var i=0;i<links.length;i++){
    links[i].addEventListener('click',function(){
      if(window.innerWidth<=768)closeSidebar();
    });
  }
  /* Auto-close sidebar when resizing to desktop */
  window.addEventListener('resize',function(){
    if(window.innerWidth>768)closeSidebar();
  });
"""

# ---------------------------------------------------------------------------
# SVG icons for sidebar navigation
# ---------------------------------------------------------------------------

_ICON_STATUS = '<svg viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>'
_ICON_LIBRARY = '<svg viewBox="0 0 24 24"><rect x="2" y="4" width="20" height="16" rx="2"/><path d="M2 8h20"/><circle cx="8" cy="14" r="2"/><path d="M14 12l3 2-3 2z"/></svg>'
_ICON_WANTED = '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg>'
_ICON_ACTIVITY = '<svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>'
_ICON_SETTINGS = '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>'
_ICON_SYSTEM = '<svg viewBox="0 0 24 24"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>'
_ICON_HAMBURGER = '<svg viewBox="0 0 24 24"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg>'

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_base_css():
    """Return the shared CSS block."""
    return BASE_CSS


def get_base_head(title, extra_css=''):
    """Return complete <head> content for a page.

    Includes meta tags, favicon, shared CSS (with keyboard help, toast, and
    dialog styles), optional extra CSS, theme init script, favicon JS,
    shared utility JS, and sidebar JS.
    """
    dialog_css = (
        'dialog{background:var(--card);color:var(--text);border:1px solid var(--border);'
        'border-radius:10px;padding:24px;max-width:min(380px,calc(100vw - 32px));margin:auto;box-shadow:0 8px 32px rgba(0,0,0,.5)}'
        'dialog::backdrop{background:rgba(0,0,0,.6);backdrop-filter:blur(2px)}'
        'dialog h3{margin-bottom:12px;font-size:1em;color:var(--text)}'
        'dialog p{margin-bottom:20px;font-size:.9em;color:var(--text2)}'
        'dialog .dlg-actions{display:flex;gap:8px;justify-content:flex-end}'
        'dialog .dlg-btn{padding:8px 18px;border-radius:6px;font-size:.85em;cursor:pointer;'
        'border:none;font-weight:500}'
        'dialog .dlg-cancel{background:var(--border);color:var(--text)}'
        'dialog .dlg-confirm{background:var(--blue);color:#fff}'
    )
    parts = [
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        '<meta name="color-scheme" content="dark light">',
        '<link rel="icon" href="data:image/svg+xml,<svg xmlns=\'http://www.w3.org/2000/svg\' viewBox=\'0 0 100 100\'><path d=\'M58 2L22 52h20L34 98 78 42H54z\' fill=\'%233fb950\'/></svg>">',
        '<title>' + title + '</title>',
        '<style>' + BASE_CSS + KEYBOARD_CSS + TOAST_CSS + dialog_css,
    ]
    if extra_css:
        parts.append(extra_css)
    parts.append('</style>')
    parts.append(THEME_INIT_SCRIPT)
    parts.append('<script>' + FAVICON_JS + SHARED_UTILS_JS
                 + SIDEBAR_JS_GLOBALS + '</script>')
    parts.append('<script>document.addEventListener("DOMContentLoaded",function(){'
                 + SIDEBAR_JS_INIT + '});</script>')
    return '\n'.join(parts)


def get_nav_html(current_page='status'):
    """Return the sidebar navigation HTML with mobile hamburger and backdrop.

    Args:
        current_page: One of 'status', 'library', 'wanted', 'activity',
                      'settings', or 'system'. The matching link gets the
                      active state.
    """
    def _link(href, icon, label, page_id, extra_id='', extra_style='', badge=''):
        cls = 'sidebar-link'
        if current_page == page_id:
            cls += ' active'
        aria = ' aria-current="page"' if current_page == page_id else ''
        id_attr = (' id="' + extra_id + '"') if extra_id else ''
        style_attr = (' style="' + extra_style + '"') if extra_style else ''
        return ('<a href="' + href + '" class="' + cls + '"'
                + aria + id_attr + style_attr + '>'
                + icon + ' ' + label + badge + '</a>')

    nav_main = [
        _link('/status', _ICON_STATUS, 'Status', 'status'),
        _link('/library', _ICON_LIBRARY, 'Library', 'library'),
        _link('/library?filter=missing', _ICON_WANTED, 'Wanted', 'wanted',
              extra_id='nav-wanted-link', extra_style='display:none',
              badge='<span class="sidebar-badge" id="nav-wanted-count">0</span>'),
        _link('/activity', _ICON_ACTIVITY, 'Activity', 'activity'),
    ]
    nav_system = [
        _link('/settings', _ICON_SETTINGS, 'Settings', 'settings'),
        _link('/system', _ICON_SYSTEM, 'System', 'system'),
    ]

    return (
        '<aside class="sidebar" id="sidebar">'
        '<div class="sidebar-brand">'
        '<div class="sidebar-brand-info">'
        '<a href="/status">pd_zurg</a>'
        '<span class="sidebar-version" id="header-meta"></span>'
        '</div>'
        '<button class="sidebar-theme" onclick="toggleTheme()" '
        'title="Toggle theme" id="theme-btn">&#x2600;&#xFE0F;</button>'
        '</div>'
        '<div class="sidebar-divider"></div>'
        '<nav class="sidebar-nav">'
        + ''.join(nav_main)
        + '<div class="sidebar-divider"></div>'
        + ''.join(nav_system)
        + '</nav>'
        '</aside>'
        '<div class="sidebar-backdrop" id="sidebar-backdrop" onclick="closeSidebar()"></div>'
        '<button class="hamburger-btn" id="hamburger-btn" onclick="toggleSidebar()" '
        'aria-label="Toggle navigation">' + _ICON_HAMBURGER + '</button>'
    )
