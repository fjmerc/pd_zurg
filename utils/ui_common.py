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
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--border2:#21262d;--text:#c9d1d9;--text2:#8b949e;--text3:#636e7b;--blue:#58a6ff;--green:#3fb950;--red:#f85149;--yellow:#d29922;--orange:#db6d28;--input-bg:#0d1117;--input-border:#30363d;--input-focus:#58a6ff;--motion-fast:100ms;--motion-normal:200ms;--motion-slow:300ms}
[data-theme="light"]{--bg:#f6f8fa;--card:#ffffff;--border:#d0d7de;--border2:#d8dee4;--text:#1f2328;--text2:#656d76;--text3:#8b949e;--blue:#0969da;--green:#1a7f37;--red:#cf222e;--yellow:#9a6700;--orange:#bc4c00;--input-bg:#ffffff;--input-border:#d0d7de;--input-focus:#0969da}

/* === Reset === */
*{margin:0;padding:0;box-sizing:border-box}

/* === Typography === */
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);padding:20px;margin:0 auto}
a{color:var(--blue);text-decoration:none}
a:hover{text-decoration:underline}

/* === Navigation Bar === */
.site-nav{display:flex;align-items:center;gap:16px;margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.nav-brand{font-size:1.3em;font-weight:700;color:var(--blue);text-decoration:none}
.nav-brand:hover{text-decoration:none;opacity:.85}
.nav-links{display:flex;gap:4px;align-items:center;margin-left:auto;font-size:.85em}
.nav-link{color:var(--text2);text-decoration:none;padding:6px 10px;border-radius:6px;transition:color var(--motion-fast),background var(--motion-fast)}
.nav-link:hover{color:var(--text);background:var(--border2);text-decoration:none}
.nav-link.active{color:var(--blue);font-weight:600;background:rgba(88,166,255,.08)}
.nav-link.active:hover{background:rgba(88,166,255,.12)}
[data-theme="light"] .nav-link.active{background:rgba(9,105,218,.08)}
[data-theme="light"] .nav-link.active:hover{background:rgba(9,105,218,.12)}
.nav-badge{display:inline-block;background:var(--red);color:#fff;border-radius:8px;font-size:.72em;font-weight:700;padding:1px 6px;margin-left:4px;min-width:16px;text-align:center;vertical-align:middle;line-height:1.4}

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

/* === Responsive Nav === */
@media(max-width:640px){
  .site-nav{gap:8px;margin-bottom:12px;padding-bottom:8px}
  .nav-brand{font-size:1.1em}
  .nav-links{gap:2px;font-size:.8em}
  .nav-link{padding:4px 6px}
}
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
# Helper functions
# ---------------------------------------------------------------------------

def get_base_css():
    """Return the shared CSS block."""
    return BASE_CSS


def get_base_head(title, extra_css=''):
    """Return complete <head> content for a page.

    Includes meta tags, favicon, shared CSS, optional extra CSS,
    and the theme initialisation script.
    """
    parts = [
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        '<meta name="color-scheme" content="dark light">',
        '<link rel="icon" href="data:image/svg+xml,<svg xmlns=\'http://www.w3.org/2000/svg\' viewBox=\'0 0 100 100\'><path d=\'M58 2L22 52h20L34 98 78 42H54z\' fill=\'%233fb950\'/></svg>">',
        '<title>' + title + '</title>',
        '<style>' + BASE_CSS,
    ]
    if extra_css:
        parts.append(extra_css)
    parts.append('</style>')
    parts.append(THEME_INIT_SCRIPT)
    parts.append('<script>' + FAVICON_JS + '</script>')
    return '\n'.join(parts)


def get_nav_html(current_page='dashboard'):
    """Return the unified navigation bar HTML.

    Args:
        current_page: One of 'dashboard', 'library', or 'settings'.
                      The matching link gets the active state.
    """
    def _link(href, label, page_id, extra_id='', extra_class='', extra_style='', badge=''):
        cls = 'nav-link'
        if extra_class:
            cls += ' ' + extra_class
        if current_page == page_id:
            cls += ' active'
        aria = ' aria-current="page"' if current_page == page_id else ''
        id_attr = (' id="' + extra_id + '"') if extra_id else ''
        style_attr = (' style="' + extra_style + '"') if extra_style else ''
        return '<a href="' + href + '" class="' + cls + '"' + aria + id_attr + style_attr + '>' + label + badge + '</a>'

    links = [
        _link('/status', 'Dashboard', 'dashboard'),
        _link('/library', 'Library', 'library'),
        _link('/library?filter=missing', 'Wanted', 'wanted',
              extra_id='nav-wanted-link', extra_style='display:none',
              badge='<span class="nav-badge" id="nav-wanted-count">0</span>'),
        _link('/settings', 'Settings', 'settings'),
        '<button class="theme-toggle" onclick="toggleTheme()" title="Toggle theme" id="theme-btn">&#x2600;&#xFE0F;</button>',
    ]
    return (
        '<nav class="site-nav">'
        '<a href="/status" class="nav-brand">pd_zurg</a>'
        '<div class="nav-links">' + ''.join(links) + '</div>'
        '</nav>'
    )
