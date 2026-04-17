"""HTML template for the library browser page.

Displays media available on debrid and/or local storage. Communicates
with /api/library and /api/library/refresh endpoints. Uses Python's
built-in http.server — no framework dependencies.
"""


def get_library_html(current_page='library'):
    """Return the complete library browser HTML page with shared CSS and nav.

    Args:
        current_page: 'library' or 'wanted' to control sidebar active state.
    """
    from utils.ui_common import (get_base_head, get_nav_html, THEME_TOGGLE_JS,
                                 KEYBOARD_JS, TOAST_JS)
    html = _LIBRARY_HTML
    html = html.replace('__BASE_HEAD__', get_base_head('pd_zurg Library'))
    html = html.replace('__NAV_HTML__', get_nav_html(current_page))
    html = html.replace('__THEME_TOGGLE_JS__', THEME_TOGGLE_JS + KEYBOARD_JS + TOAST_JS)
    return html


_LIBRARY_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
__BASE_HEAD__
</head>
<body>
__NAV_HTML__
<main class="main-content">
<style>
.main-content{max-width:1400px}

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
.btn-select{background:none;border:1px solid var(--border);color:var(--text2);border-radius:6px;padding:8px 14px;font-size:.85em;cursor:pointer;white-space:nowrap;transition:border-color .15s,color .15s,background .15s}
.btn-select:hover{border-color:var(--blue);color:var(--blue)}
.btn-select.active{background:var(--blue);color:#fff;border-color:var(--blue)}
.btn-select .select-count{display:inline-block;background:rgba(255,255,255,.25);border-radius:10px;font-size:.82em;font-weight:600;padding:1px 7px;margin-left:5px;min-width:18px;text-align:center}
.scan-info{font-size:.78em;color:var(--text3);white-space:nowrap}

/* Scanning indicator */
.scanning-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--yellow);margin-right:5px;animation:pulse-dot 1s ease-in-out infinite}
@keyframes pulse-dot{0%,100%{opacity:1}50%{opacity:.3}}

/* Card checkbox (select mode) */
.card-checkbox{position:absolute;top:8px;left:8px;width:22px;height:22px;border-radius:50%;border:2px solid rgba(255,255,255,.6);background:rgba(0,0,0,.4);z-index:3;display:none;align-items:center;justify-content:center;cursor:pointer;transition:background .15s,border-color .15s,transform .15s}
.select-mode .card-checkbox{display:flex}
.card-checkbox.checked{background:var(--blue);border-color:var(--blue);transform:scale(1.1)}
.card-checkbox.checked::after{content:'\2713';color:#fff;font-size:13px;font-weight:700;line-height:1}
.poster-card.selected .poster-container::after{content:'';position:absolute;inset:0;background:rgba(88,166,255,.15);z-index:2;pointer-events:none}
[data-theme="light"] .card-checkbox{border-color:rgba(0,0,0,.4);background:rgba(255,255,255,.7)}

/* Card grid */
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:16px;margin-top:4px}

/* Poster card (Sonarr-style grid cards) */
.poster-card{background:var(--card);border-radius:8px;overflow:hidden;cursor:pointer;transition:transform 200ms ease-in,box-shadow 200ms ease-in;position:relative}
.poster-card:hover{transform:translateY(-4px);box-shadow:0 0 12px rgba(0,0,0,.5);z-index:2}
.poster-card:focus-visible{outline:2px solid var(--blue);outline-offset:2px}
.poster-container{position:relative;aspect-ratio:2/3;overflow:hidden;background:var(--border)}
.poster-img{width:100%;height:100%;object-fit:cover;opacity:0;transition:opacity .3s}
.poster-img.loaded{opacity:1}
.poster-placeholder{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;text-align:center;overflow:hidden;position:relative}
.poster-placeholder .pp-initial{font-size:4.5em;font-weight:700;line-height:1;color:rgba(255,255,255,.85);text-shadow:0 2px 8px rgba(0,0,0,.3);margin-bottom:4px}
.poster-placeholder .pp-title{font-size:.75em;color:rgba(255,255,255,.7);padding:0 10px;max-height:2.8em;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;word-break:break-word;line-height:1.4}
[data-theme="light"] .poster-placeholder .pp-initial{color:rgba(255,255,255,.9)}
[data-theme="light"] .poster-placeholder .pp-title{color:rgba(255,255,255,.8)}
.corner-badge{position:absolute;top:0;right:0;width:0;height:0;border-style:solid;border-width:0 28px 28px 0;border-color:transparent transparent transparent transparent;z-index:1}
.corner-badge.ended{border-color:transparent #f05050 transparent transparent}
.progress-bar{height:5px;background:#5b5b5b;width:100%}
.progress-fill{height:100%;transition:width .3s ease}
[data-theme="light"] .progress-bar{background:#d0d7de}
.card-info{padding:8px 10px}
.card-header{display:flex;align-items:baseline;gap:6px}
.card-header .card-title{flex:1;min-width:0;font-size:.85em;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;line-height:1.35}
.card-header .card-badges{flex-shrink:0;display:flex;gap:3px}
.card-info .card-meta{font-size:.72em;color:var(--text2);margin-top:2px}
.card-status{display:flex;align-items:center;gap:6px;margin-top:3px;flex-wrap:wrap}

/* Poster skeleton */
.skeleton-poster{overflow:hidden;border-radius:8px;background:var(--card)}
.skeleton-poster .poster-container{aspect-ratio:2/3}

/* Color legend */
.legend{display:flex;flex-wrap:wrap;gap:8px 16px;padding:12px 0;font-size:.78em;color:var(--text2)}
.legend-item{display:inline-flex;align-items:center;gap:6px}
.legend-swatch{width:16px;height:5px;border-radius:1px;display:inline-block}

/* Alphabetical jump bar */
.jump-bar{position:fixed;right:10px;top:50%;transform:translateY(-50%);display:flex;flex-direction:column;align-items:center;gap:clamp(0px,0.3vh,2px);z-index:10;padding:clamp(4px,1vh,10px) 5px;border-radius:10px;background:var(--card);border:1px solid var(--border);box-shadow:0 2px 8px rgba(0,0,0,.2);max-height:90vh;overflow-y:auto}
.jump-letter{font-size:clamp(.7em,1.5vh,1.15em);font-weight:600;line-height:1;padding:clamp(1px,0.4vh,4px) 10px;cursor:pointer;color:var(--blue);border-radius:4px;user-select:none;transition:background .1s,color .1s}
.jump-letter:hover{background:var(--blue);color:var(--bg)}
.jump-letter.inactive{color:var(--text3);cursor:default;opacity:.4;pointer-events:none}
.jump-letter[title]{position:relative}
.jump-letter .jump-tip{display:none;position:absolute;right:100%;top:50%;transform:translateY(-50%);margin-right:6px;background:var(--card);border:1px solid var(--border);color:var(--text);font-size:.72em;font-weight:400;padding:3px 8px;border-radius:4px;white-space:nowrap;pointer-events:none;box-shadow:0 2px 6px rgba(0,0,0,.25);z-index:11}
.jump-letter:hover .jump-tip{display:block}
.poster-card.jump-highlight{outline:2px solid var(--blue);outline-offset:2px}
@media(max-width:900px) and (min-width:641px){.jump-bar{padding:clamp(3px,0.8vh,6px) 3px}.jump-letter{font-size:clamp(.6em,1.2vh,.9em);padding:clamp(0px,0.2vh,2px) 6px}.grid{padding-right:32px}}
@media(max-width:640px) and (min-width:481px){.jump-bar{position:sticky;top:0;right:auto;left:0;transform:none;flex-direction:row;max-height:none;overflow-x:auto;overflow-y:hidden;border-radius:0;border-left:none;border-right:none;padding:4px 8px;gap:0;z-index:15;width:100%;box-shadow:0 2px 6px rgba(0,0,0,.15)}.jump-letter{padding:3px 8px;font-size:.75em;flex-shrink:0}.jump-letter .jump-tip{display:none !important}.grid{padding-right:0}}
@media(max-width:480px){.jump-bar{display:none}.grid{padding-right:0}}

/* Media card (detail view only) */
.media-card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;display:flex;flex-direction:column;gap:6px;transition:border-color .15s,transform .15s,box-shadow .15s}
.media-card:hover{border-color:var(--border2);transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,.15)}
.media-card.show-card,.media-card.movie-card{cursor:pointer;position:relative;padding-right:32px}
.media-card.show-card:hover,.media-card.movie-card:hover{border-color:var(--blue);box-shadow:0 4px 12px rgba(88,166,255,.1)}
.media-card.show-card::after,.media-card.movie-card::after{content:'\203A';position:absolute;right:14px;top:50%;transform:translateY(-50%);color:var(--text3);font-size:1.2em;transition:color .15s,transform .15s}
.media-card.show-card:hover::after,.media-card.movie-card:hover::after{color:var(--blue);transform:translateY(-50%) translateX(2px)}
.card-title{font-size:.9em;font-weight:500;color:var(--text);line-height:1.35}
.card-year{color:var(--text2);font-weight:400}
.card-meta{font-size:.78em;color:var(--text2)}
.badge-pending.has-error{border-color:#db6d2866}
.badge-pending.has-error::after{content:'\26A0';margin-left:4px;font-size:.85em}
.pending-callout{font-size:.78em;color:var(--orange);margin-top:6px;padding:6px 10px;background:#db6d280a;border-left:3px solid var(--orange);border-radius:0 4px 4px 0;line-height:1.5}
[data-theme="light"] .pending-callout{background:#bc4c000d;border-left-color:#bc4c00;color:#bc4c00}
.pending-callout-icon{margin-right:3px}
.card-badges{display:flex;gap:5px;flex-wrap:wrap}

/* Source badges */
.badge-local{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:#3fb9500f;color:var(--green);border:1px solid #3fb95033;vertical-align:middle}
.badge-debrid{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:#c084fc0f;color:#c084fc;border:1px solid #c084fc33;vertical-align:middle}
.badge-local .badge-full,.badge-debrid .badge-full,.badge-missing .badge-full,.badge-pending .badge-full,.badge-migrating .badge-full,.badge-unavailable .badge-full,.badge-fallback .badge-full{display:inline}
.badge-local .badge-mini,.badge-debrid .badge-mini,.badge-missing .badge-mini,.badge-pending .badge-mini,.badge-migrating .badge-mini,.badge-unavailable .badge-mini,.badge-fallback .badge-mini{display:none}
@media(max-width:640px){
  .badge-local .badge-full,.badge-debrid .badge-full,.badge-missing .badge-full,.badge-pending .badge-full,.badge-migrating .badge-full,.badge-unavailable .badge-full,.badge-fallback .badge-full{display:none}
  .badge-local .badge-mini,.badge-debrid .badge-mini,.badge-missing .badge-mini,.badge-pending .badge-mini,.badge-migrating .badge-mini,.badge-unavailable .badge-mini,.badge-fallback .badge-mini{display:inline}
  .ep-actions .btn:not(.btn-icon){font-size:.68em;padding:2px 5px}
}
[data-theme="light"] .badge-local{background:#1a7f371a;border-color:#1a7f3740}
[data-theme="light"] .badge-debrid{background:#7c3aed1a;border-color:#7c3aed40;color:#7c3aed}

.btn-ghost.btn-switch:hover:not(:disabled):not(.confirming){border-color:#2dd4bf;color:#2dd4bf}

/* Quality badges */
.badge-quality{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;white-space:nowrap}
.badge-quality-2160p{background:#a855f70f;color:#a855f7;border:1px solid #a855f733}
.badge-quality-1080p{background:#58a6ff0f;color:var(--blue);border:1px solid #58a6ff33}
.badge-quality-720p{background:#db6d280f;color:var(--orange);border:1px solid #db6d2833}
.badge-quality-480p,.badge-quality-unknown{background:var(--border);color:var(--text3);border:1px solid var(--border2)}
[data-theme="light"] .badge-quality-2160p{background:#a855f71a;border-color:#a855f740}
[data-theme="light"] .badge-quality-1080p{background:#0969da1a;border-color:#0969da40}
[data-theme="light"] .badge-quality-720p{background:#bc4c001a;border-color:#bc4c0040}
.ep-size{font-size:.78em;color:var(--text3);white-space:nowrap}

/* Spinner (override shared 14px to 16px for library) */
.spinner{width:16px;height:16px}

/* Skeleton loading */
.skeleton-card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;display:flex;flex-direction:column;gap:8px}
.skeleton-line{background:linear-gradient(90deg,var(--border) 25%,var(--border2) 50%,var(--border) 75%);background-size:200% 100%;border-radius:4px;animation:skeleton-shimmer 1.5s ease-in-out infinite}
.skeleton-title{height:16px;width:70%}
.skeleton-meta{height:12px;width:40%}
.skeleton-badges{height:20px;width:50%}
@keyframes skeleton-shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}

/* State panels */
.state-panel{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;min-height:220px;color:var(--text2);font-size:.9em;text-align:center;padding:24px}
.state-panel .state-hint{font-size:.82em;color:var(--text3)}
.state-panel.error-state{color:var(--red)}

/* Detail view */
.detail-view{max-width:1200px}
.detail-back{display:inline-block;background:none;border:none;color:var(--blue);cursor:pointer;font-size:.85em;margin-bottom:12px;user-select:none;padding:0;font-family:inherit}
.detail-back:hover{text-decoration:underline}
.detail-header{margin-bottom:16px}
.detail-header h2{font-size:1.3em;font-weight:600;margin-bottom:6px}

/* Detail body — single column (Activity renders full-width below seasons) */
.detail-body{display:block}
.detail-main{min-width:0}
/* History sidebar — rendered as a full-width section below seasons */
.history-sidebar{border:1px solid var(--border);border-radius:8px;background:var(--card);padding:12px 14px;margin-top:18px}
.history-sidebar h3{font-size:.95em;font-weight:600;margin:0 0 10px;color:var(--text)}
.hs-empty{font-size:.82em;color:var(--text3);padding:8px 0}
/* Timeline rail */
.hs-timeline{position:relative;padding-left:20px}
.hs-timeline::before{content:'';position:absolute;left:6px;top:4px;bottom:4px;width:2px;background:var(--border2);border-radius:1px}
.hs-day-group{margin-bottom:12px}
.hs-day-group:last-child{margin-bottom:0}
.hs-day-label{font-size:.72em;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}
/* Event items */
.hs-event{position:relative;padding:4px 0;font-size:.8em}
.hs-event::before{content:'';position:absolute;left:-17px;top:9px;width:8px;height:8px;border-radius:50%;border:2px solid var(--border2);background:var(--bg)}
.hs-event.hs-cat-acquisition::before{border-color:var(--green);background:var(--green)}
.hs-event.hs-cat-failure::before{border-color:var(--red);background:var(--red)}
.hs-event.hs-cat-action::before{border-color:var(--blue);background:var(--blue)}
.hs-event.hs-cat-management::before{border-color:var(--yellow);background:var(--yellow)}
.hs-event.hs-cat-failure{border-left:2px solid var(--red);padding-left:6px;margin-left:-8px}
.hs-event-icon{display:inline-block;width:16px;text-align:center;margin-right:2px;font-size:.9em}
.hs-event-type{font-weight:500;color:var(--text)}
.hs-event-episode{display:inline-block;padding:1px 5px;border-radius:3px;font-size:.78em;font-weight:600;background:var(--border);color:var(--text2);margin-left:4px}
.hs-event-detail{color:var(--text2);font-size:.92em;margin-top:1px;word-break:break-word}
.hs-event-time{color:var(--text3);font-size:.82em;font-family:monospace}

/* Season accordion */
.season-section{border:1px solid var(--border);border-radius:8px;margin-bottom:8px;overflow:hidden}
.season-header{padding:10px 14px;cursor:pointer;font-size:.9em;font-weight:500;color:var(--text);background:var(--card);display:flex;align-items:center;gap:8px;user-select:none;transition:background-color .15s}
.season-header:hover{background:var(--border2)}
.season-chevron{font-size:.7em;color:var(--text2);width:14px;text-align:center;transition:transform .15s}
.season-header.expanded .season-chevron{transform:rotate(90deg)}

/* Episode table */
.episode-table{width:100%;border-collapse:collapse}
.episode-table tr{border-top:1px solid var(--border)}
.episode-table td{padding:7px 14px;font-size:.82em;color:var(--text);vertical-align:middle}
.ep-num{font-weight:600;color:var(--text2);white-space:nowrap;width:50px}
.ep-file{color:var(--text)}
.ep-source{white-space:nowrap;text-align:right}
.ep-quality{white-space:nowrap}
.ep-actions{white-space:nowrap;text-align:right}

/* Preference & action controls */
.pref-row{display:flex;align-items:center;gap:8px;margin-top:8px}
.pref-select{background:var(--input-bg);border:1px solid var(--input-border);border-radius:6px;padding:4px 8px;color:var(--text);font-size:.82em;outline:none;cursor:pointer}
.pref-select:focus{border-color:var(--input-focus)}
.pref-row .btn{font-size:.82em;padding:4px 12px}
/* Library primary buttons use blue instead of green */
.btn-primary{background:var(--blue);border-color:var(--blue)}
.btn-primary:hover:not(:disabled){opacity:.85;background:#4c9aff}
[data-theme="light"] .btn-primary:hover:not(:disabled){background:#0860ca}
.season-actions{margin-left:auto;display:flex;gap:4px}
.transfer-msg{font-size:.78em;color:var(--yellow);margin-top:4px}
.transfer-msg.msg-success{color:var(--green);border-left:3px solid var(--green);padding:6px 10px;background:#3fb9500a;border-radius:0 4px 4px 0}
.transfer-msg.msg-error{color:var(--red);border-left:3px solid var(--red);padding:6px 10px;background:#f851490a;border-radius:0 4px 4px 0}
.confirm-panel{border:1px solid var(--red);border-radius:8px;padding:12px;margin-top:8px;background:#f851490a}
.confirm-panel .confirm-title{font-weight:600;color:var(--red);margin-bottom:8px}
.confirm-panel .confirm-list{font-size:.8em;color:var(--text);margin:0 0 12px 16px;list-style:disc}
.confirm-panel .confirm-list li{margin-bottom:2px}
.confirm-panel .btn{font-size:.82em;padding:4px 14px}

/* Blocklist modal */
.bl-dialog{background:var(--card);border:1px solid var(--border);border-radius:10px;width:100%;max-width:420px;animation:modal-in .15s ease-out}
.bl-dialog-hdr{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid var(--border)}
.bl-dialog-hdr h3{margin:0;font-size:.95em;font-weight:600}
.bl-dialog-body{padding:18px}
.bl-reasons{display:flex;flex-direction:column;gap:6px;margin-bottom:14px}
.bl-reason-btn{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:8px 14px;color:var(--text);font-size:.85em;cursor:pointer;text-align:left;transition:border-color .15s,background .15s}
.bl-reason-btn:hover{border-color:var(--input-focus);background:var(--input-bg)}
.bl-reason-btn.selected{border-color:var(--blue);background:rgba(76,154,255,.1);color:var(--blue)}
[data-theme="light"] .bl-reason-btn.selected{background:#0969da1a}
.bl-custom-label{font-size:.8em;color:var(--text2);margin-bottom:4px}
.bl-custom-input{width:100%;background:var(--input-bg);border:1px solid var(--input-border);border-radius:6px;padding:8px 10px;color:var(--text);font-size:.85em;outline:none;box-sizing:border-box}
.bl-custom-input:focus{border-color:var(--input-focus)}
.bl-dialog-footer{display:flex;justify-content:flex-end;gap:8px;padding:14px 18px;border-top:1px solid var(--border)}

/* Detail hero with poster */
.detail-hero{display:flex;gap:16px;margin-bottom:16px}
.detail-poster{width:150px;min-width:150px;border-radius:8px;overflow:hidden}
.detail-poster img{width:100%;display:block;border-radius:8px}
.detail-info{flex:1;min-width:0}
.detail-info .card-badges{margin-top:6px}
.detail-overview{font-size:.85em;color:var(--text2);margin-top:8px;line-height:1.5;max-height:7.5em;overflow:hidden;-webkit-mask-image:linear-gradient(to bottom,black 85%,transparent);mask-image:linear-gradient(to bottom,black 85%,transparent);cursor:pointer;transition:max-height .3s ease}
.detail-overview.expanded{max-height:60em;-webkit-mask-image:none;mask-image:none}
.detail-status{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:var(--border);color:var(--text2);margin-left:6px}

/* Plex-style detail meta */
.detail-meta-row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:6px;font-size:.85em;color:var(--text2)}
.detail-meta-row .sep{color:var(--text3)}
.detail-cert{padding:1px 6px;border:1px solid var(--border);border-radius:4px;font-size:.78em;color:var(--text2)}
.detail-genres{font-size:.82em;color:var(--text2);margin-top:4px}
.detail-rating{display:inline-flex;align-items:center;gap:6px;margin-top:6px}
.detail-rating .rating-badge{background:#f5c51826;color:#d4a017;border:1px solid #d4a01740;padding:1px 6px;border-radius:3px;font-weight:600;font-size:.7em;letter-spacing:.3px}
[data-theme="light"] .detail-rating .rating-badge{background:#9a670014;color:#9a6700;border-color:#9a670033}
.detail-directors{font-size:.82em;color:var(--text2);margin-top:4px}

/* Media info block */
.media-info{display:flex;gap:16px;margin-top:10px;padding:8px 10px;background:var(--border2);border-radius:6px;font-size:.78em;flex-wrap:wrap}
.media-info .mi-group{display:flex;flex-direction:column;gap:2px}
.media-info .mi-label{color:var(--text3);text-transform:uppercase;font-size:.7em;letter-spacing:.5px}
.media-info .mi-value{color:var(--text2)}

/* Cast & Crew */
.cast-section{margin-top:18px}
.cast-section h3{font-size:1em;font-weight:600;margin-bottom:8px;color:var(--text)}
.cast-scroll{display:flex;gap:12px;overflow-x:auto;padding-bottom:4px;scrollbar-width:none;-ms-overflow-style:none}
.cast-scroll::-webkit-scrollbar{display:none}
.cast-card{flex:0 0 92px;text-align:center}
.cast-photo{width:92px;height:92px;border-radius:50%;background:var(--border);background-size:cover;background-position:center;margin-bottom:4px}
.cast-name{font-size:.78em;color:var(--text);font-weight:600;line-height:1.2;max-width:92px;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
.cast-role{font-size:.72em;color:var(--text3);line-height:1.2;max-width:92px;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}

/* Episode titles and missing */
.ep-title{color:var(--text);font-size:.95em;font-weight:600;display:inline}
.ep-date{color:var(--text2);font-size:.82em;white-space:nowrap;margin-left:8px}
.ep-relative{color:var(--text3);font-size:.9em;margin-left:4px}
.ep-filename{color:var(--text3);font-size:.75em;display:block;word-break:break-all;margin-top:2px}
.ep-missing td{color:var(--text3)}
.badge-missing{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:#f851490f;color:var(--red);border:1px solid #f8514933;vertical-align:middle}
.badge-upcoming{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:#58a6ff0f;color:var(--blue);border:1px solid #58a6ff33;vertical-align:middle}
.badge-tba{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:var(--border);color:var(--text3);border:1px solid var(--border2);vertical-align:middle}
.badge-airing-today{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;background:#d299220f;color:var(--yellow);border:1px solid #d2992233;vertical-align:middle}
[data-theme="light"] .badge-missing{background:#cf222e1a;border-color:#cf222e40}
[data-theme="light"] .badge-upcoming{background:#0969da1a;border-color:#0969da40}
[data-theme="light"] .badge-tba{background:#d0d7de40;border-color:#d0d7de}
[data-theme="light"] .badge-airing-today{background:#9a67000a;border-color:#9a670040;color:#9a6700}
.badge-pending{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;color:var(--orange);border:1px solid #db6d2833;position:relative;overflow:hidden;background:linear-gradient(90deg,#db6d2818 0%,#db6d2808 50%,#db6d2818 100%);background-size:200% 100%;animation:pending-shimmer 2s ease-in-out infinite;vertical-align:middle}
.badge-pending::before{content:'';display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--orange);margin-right:4px;vertical-align:middle;animation:pulse-dot 1s ease-in-out infinite}
@keyframes pending-shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
[data-theme="light"] .badge-pending{background:#bc4c001a;border-color:#bc4c0040;color:#bc4c00}
[data-theme="light"] .badge-pending::before{background:#bc4c00}
[data-theme="light"] .badge-pending.has-error{border-color:#bc4c0066}
.badge-migrating{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;color:#2dd4bf;border:1px solid #2dd4bf33;position:relative;overflow:hidden;background:linear-gradient(90deg,#2dd4bf18 0%,#2dd4bf08 50%,#2dd4bf18 100%);background-size:200% 100%;animation:pending-shimmer 2s ease-in-out infinite;vertical-align:middle}
.ep-source .badge-migrating{margin-left:5px}
.badge-migrating::before{content:'';display:inline-block;width:6px;height:6px;border-radius:50%;background:#2dd4bf;margin-right:4px;vertical-align:middle;animation:pulse-dot 1s ease-in-out infinite}
[data-theme="light"] .badge-migrating{color:#0d9488;border-color:#0d948840;background:linear-gradient(90deg,#0d94881a 0%,#0d94880a 50%,#0d94881a 100%);background-size:200% 100%}
[data-theme="light"] .badge-migrating::before{background:#0d9488}
.badge-unavailable{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;color:var(--red);border:1px solid #f8514933;background:#f851490f;vertical-align:middle}
[data-theme="light"] .badge-unavailable{background:#cf222e1a;border-color:#cf222e40;color:#cf222e}
.badge-fallback{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;color:var(--orange);border:1px solid #db6d2833;background:#db6d280f;vertical-align:middle}
[data-theme="light"] .badge-fallback{background:#bc4c001a;border-color:#bc4c0040;color:#bc4c00}

/* Season progress pill (Sonarr-style) */
.season-progress-pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72em;font-weight:600;margin-left:8px}
.progress-complete{background:#3fb9501a;color:var(--green);border:1px solid #3fb95033}
.progress-partial{background:#d299221a;color:var(--yellow);border:1px solid #d2992233}
.progress-missing{background:#f851490f;color:var(--red);border:1px solid #f8514933}
.progress-pending{background:#db6d280f;color:var(--orange);border:1px solid #db6d2833}
.progress-empty{background:var(--border);color:var(--text3);border:1px solid var(--border2)}
[data-theme="light"] .progress-complete{background:#1a7f371a;border-color:#1a7f3740}
[data-theme="light"] .progress-partial{background:#9a67001a;border-color:#9a670040}
[data-theme="light"] .progress-missing{background:#cf222e1a;border-color:#cf222e40}
[data-theme="light"] .progress-pending{background:#bc4c001a;border-color:#bc4c0040}
[data-theme="light"] .progress-empty{background:#d0d7de40;border-color:#d0d7de;color:var(--text3)}

/* Expand/collapse all */
.expand-all-row{display:flex;justify-content:flex-end;margin-bottom:8px}
.expand-all-row .btn{display:flex;align-items:center;gap:4px}

/* Ping dot for pending seasons */
.ping-dot{position:relative;display:inline-block;width:8px;height:8px;margin-left:6px;vertical-align:middle}
.ping-dot::before,.ping-dot::after{content:'';position:absolute;top:0;left:0;width:8px;height:8px;border-radius:50%;background:var(--orange)}
.ping-dot::after{animation:ping-anim 1.2s cubic-bezier(0,0,.2,1) infinite}
@keyframes ping-anim{0%{transform:scale(1);opacity:.8}75%,100%{transform:scale(2.2);opacity:0}}

/* Season collapse footer */
.season-collapse-footer{text-align:center;padding:4px 0;background:var(--border2);cursor:pointer;border-top:1px solid var(--border);transition:background .15s;font-size:.75em;color:var(--text3)}
.season-collapse-footer:hover{background:var(--border);color:var(--text2)}

/* Bulk action bar */
.bulk-bar{position:fixed;bottom:0;left:0;right:0;background:var(--card);border-top:1px solid var(--border);padding:10px 20px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;z-index:20;box-shadow:0 -4px 12px rgba(0,0,0,.2);justify-content:center}
.bulk-bar .bulk-count{font-size:.85em;font-weight:600;color:var(--text);white-space:nowrap}
.bulk-bar .btn{font-size:.82em}
.bulk-bar .filter-select{font-size:.82em;padding:6px 10px}
.bulk-progress{font-size:.82em;color:var(--yellow);white-space:nowrap}
body.has-bulk-bar{padding-bottom:60px}
[data-theme="light"] .bulk-bar{box-shadow:0 -4px 12px rgba(0,0,0,.08)}

/* Wanted preset pills */
.wanted-presets{display:flex;gap:6px;align-items:center;padding:0 0 8px;flex-wrap:wrap}
.wanted-pill{display:inline-flex;align-items:center;gap:4px;padding:4px 12px;border-radius:14px;font-size:.78em;font-weight:600;cursor:pointer;border:1px solid var(--border);background:none;color:var(--text2);transition:all .15s;user-select:none;font-family:inherit}
.wanted-pill:hover{border-color:var(--text3);color:var(--text)}
.wanted-pill .pill-count{font-weight:700;opacity:.7}
.wanted-pill--missing.active{background:#f851491a;border-color:#f8514966;color:var(--red)}
.wanted-pill--unavailable.active{background:#db6d281a;border-color:#db6d2866;color:var(--orange)}
.wanted-pill--pending.active{background:#d299221a;border-color:#d2992266;color:var(--yellow)}
.wanted-pill--fallback.active{background:#58a6ff1a;border-color:#58a6ff66;color:var(--blue)}
[data-theme="light"] .wanted-pill--missing.active{background:#cf222e1a;border-color:#cf222e66;color:#cf222e}
[data-theme="light"] .wanted-pill--unavailable.active{background:#bc4c001a;border-color:#bc4c0066;color:#bc4c00}
[data-theme="light"] .wanted-pill--pending.active{background:#9a67001a;border-color:#9a670066;color:#9a6700}
[data-theme="light"] .wanted-pill--fallback.active{background:#0969da1a;border-color:#0969da66;color:#0969da}
.wanted-pill--recent.active{background:#a371f71a;border-color:#a371f766;color:#a371f7}
[data-theme="light"] .wanted-pill--recent.active{background:#8250df1a;border-color:#8250df66;color:#8250df}

/* Wanted bulk actions bar */
.wanted-actions{display:flex;gap:8px;align-items:center;padding:8px 0;flex-wrap:wrap}
.wanted-actions .btn{font-size:.82em;padding:6px 14px}
.wanted-actions .wanted-progress{font-size:.82em;color:var(--yellow);white-space:nowrap}

/* Responsive */
@media(max-width:640px){
  .grid{grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:10px}
  .controls{gap:6px}
  .search-wrap{min-width:120px}
  .scan-info{display:none}
  .episode-table{display:block;overflow-x:auto}
  .episode-table td{padding:5px 8px}
  .ep-filename{display:none}
  .episode-table .ep-size{display:none}
  .ep-date{display:block;margin-left:0;margin-top:2px;white-space:normal}
  .season-header{flex-wrap:wrap}
  .season-actions{flex-wrap:wrap}
  .detail-hero{flex-direction:column}
  .detail-poster{width:120px}
  .cast-card{flex:0 0 72px}
  .cast-photo{width:72px;height:72px}
  .cast-name,.cast-role{max-width:72px}
  .card-header .card-title{font-size:.78em}
  .card-header .card-badges{gap:2px}
  .card-status{gap:4px;margin-top:2px}
  .legend{gap:6px 12px;font-size:.72em}
  body.has-bulk-bar{padding-bottom:120px}
  .bulk-bar{padding:8px 12px;gap:6px}
  .wanted-pill{padding:3px 8px;font-size:.72em}
}
.search-overlay{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.65);display:flex;align-items:flex-start;justify-content:center;z-index:1000;padding:40px 16px;overflow-y:auto;backdrop-filter:blur(2px)}
.search-dialog{background:var(--card);border:1px solid var(--border);border-radius:10px;width:100%;max-width:1000px;animation:modal-in .15s ease-out}
@keyframes modal-in{from{opacity:0;transform:translateY(-10px)}to{opacity:1;transform:none}}
.search-dialog-hdr{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid var(--border)}
.search-dialog-hdr h3{margin:0;font-size:.95em;font-weight:600}
.search-dialog-close{background:none;border:none;color:var(--text3);font-size:1.3em;cursor:pointer;padding:0 4px}
.search-dialog-close:hover{color:var(--text)}
.search-dialog-body{padding:14px 18px;min-height:120px}
.search-filter-row{display:flex;gap:10px;align-items:center;margin-bottom:12px;flex-wrap:wrap}
.search-filter-row label{font-size:.78em;color:var(--text2);display:flex;align-items:center;gap:4px;cursor:pointer}
.search-filter-row select{font-size:.78em;padding:2px 6px;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:4px}
.search-results-tbl{width:100%;border-collapse:collapse;font-size:.82em}
.search-results-tbl th{text-align:center;padding:6px 8px;border-bottom:1px solid var(--border);color:var(--text3);font-weight:500;font-size:.85em;cursor:pointer;user-select:none;white-space:nowrap}
.search-results-tbl th:hover{color:var(--text)}
.search-results-tbl th .sort-arrow{font-size:.7em;margin-left:2px}
.search-results-tbl td{padding:6px 8px;border-bottom:1px solid var(--border);vertical-align:middle;text-align:center}
.search-results-tbl tr:last-child td{border-bottom:none}
.search-results-tbl tr.added-row td{opacity:.5}
.search-results-tbl th:first-child{text-align:left}
.search-results-tbl th:first-child,.search-results-tbl th:last-child{cursor:default}
.search-results-tbl .sr-title{max-width:360px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:left}
.search-results-tbl .sr-indexer{max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.badge-quality{display:inline-block;padding:1px 6px;border-radius:4px;font-size:.78em;font-weight:600;white-space:nowrap}
.badge-quality.q-2160p{background:#ff6b0014;color:#ff8c3a;border:1px solid #ff6b0030}
.badge-quality.q-1080p{background:#58a6ff14;color:var(--blue);border:1px solid #58a6ff30}
.badge-quality.q-720p{background:#3fb95014;color:var(--green);border:1px solid #3fb95030}
.badge-quality.q-480p,.badge-quality.q-Unknown{background:var(--border);color:var(--text3);border:1px solid var(--border)}
.btn-add-debrid{background:none;border:1px solid var(--green);color:var(--green);border-radius:4px;padding:2px 8px;font-size:.78em;cursor:pointer;transition:all .15s}
.btn-add-debrid:hover:not(:disabled){background:#3fb95018;border-color:var(--green)}
.btn-add-debrid:disabled{opacity:.5;cursor:not-allowed}
.btn-add-debrid.added{border-color:var(--green);color:var(--green);cursor:default}
.search-empty{text-align:center;color:var(--text3);padding:24px 0;font-size:.88em}
.search-count{font-size:.75em;color:var(--text3);margin-left:auto}
</style>

<div class="tabs" role="tablist">
  <div class="tab active" role="tab" tabindex="0" aria-selected="true" aria-controls="tab-movies"
       data-kb="tab-1" onclick="switchTab('movies')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();switchTab('movies')}">
    Movies<span class="badge" id="badge-movies">0</span>
  </div>
  <div class="tab" role="tab" tabindex="0" aria-selected="false" aria-controls="tab-shows"
       data-kb="tab-2" onclick="switchTab('shows')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();switchTab('shows')}">
    Shows<span class="badge" id="badge-shows">0</span>
  </div>
</div>

<div class="controls">
  <div class="search-wrap">
    <input type="search" id="search-input" data-kb="search" placeholder="Search titles... (/)" autocomplete="off"
           oninput="clearTimeout(_searchTimer);_searchTimer=setTimeout(applyFilters,150)" aria-label="Search titles">
  </div>
  <select class="filter-select" id="source-filter" onchange="applyFilters()" aria-label="Filter by source">
    <option value="">All Sources</option>
    <option value="local">Local Only</option>
    <option value="debrid">Debrid Only</option>
  </select>
  <select class="filter-select" id="status-filter" onchange="applyFilters()" aria-label="Filter by status" style="display:none">
    <option value="">All Status</option>
    <option value="Continuing">Continuing</option>
    <option value="Ended">Ended</option>
  </select>
  <select class="filter-select" id="year-filter" onchange="applyFilters()" aria-label="Filter by year">
    <option value="">All Years</option>
    <option value="2020s">2020s</option>
    <option value="2010s">2010s</option>
    <option value="2000s">2000s</option>
    <option value="older">Older</option>
  </select>
  <select class="filter-select" id="sort-select" onchange="applyFilters()" aria-label="Sort by">
    <option value="az">Sort: A-Z</option>
    <option value="za">Sort: Z-A</option>
    <option value="added">Sort: Newest Added</option>
    <option value="year-new">Sort: Year (Newest)</option>
    <option value="year-old">Sort: Year (Oldest)</option>
    <option value="complete">Sort: % Complete</option>
    <option value="episodes">Sort: Episodes</option>
    <option value="size">Sort: Size</option>
  </select>
  <button class="btn-select" id="btn-select" onclick="toggleSelectMode()" aria-pressed="false">Select</button>
  <button class="btn btn-ghost" id="btn-refresh" data-kb="refresh" onclick="triggerRefresh()" title="Refresh library (R)">Refresh</button>
  <span class="scan-info" id="scan-info"></span>
</div>

<div class="wanted-presets" id="wanted-presets">
  <button class="wanted-pill wanted-pill--missing" data-preset="missing" onclick="toggleWantedPreset('missing')">Missing <span class="pill-count" id="pill-count-missing"></span></button>
  <button class="wanted-pill wanted-pill--unavailable" data-preset="unavailable" onclick="toggleWantedPreset('unavailable')">Unavailable <span class="pill-count" id="pill-count-unavailable"></span></button>
  <button class="wanted-pill wanted-pill--pending" data-preset="pending" onclick="toggleWantedPreset('pending')">Pending <span class="pill-count" id="pill-count-pending"></span></button>
  <button class="wanted-pill wanted-pill--fallback" data-preset="fallback" onclick="toggleWantedPreset('fallback')">Fallback <span class="pill-count" id="pill-count-fallback"></span></button>
  <button class="wanted-pill wanted-pill--recent" data-preset="recent" onclick="toggleWantedPreset('recent')">Recently Added <span class="pill-count" id="pill-count-recent"></span></button>
</div>
<div class="wanted-actions" id="wanted-actions" style="display:none">
  <button class="btn btn-ghost btn-sm" id="wanted-search-btn" onclick="wantedSearchAll()" style="display:none">Search All on Debrid</button>
  <button class="btn btn-ghost btn-sm" id="wanted-download-btn" onclick="wantedDownloadAll()" style="display:none">Download All Locally</button>
  <span class="wanted-progress" id="wanted-progress"></span>
</div>

<div class="jump-bar" id="jump-bar" role="navigation" aria-label="Alphabetical jump bar" style="display:none"></div>
<div id="content-area">
  <div class="grid" id="skeleton-grid"></div>
</div>
<script>
(function(){var g=document.getElementById('skeleton-grid');if(!g)return;var h='';for(var i=0;i<12;i++)h+='<div class="skeleton-poster"><div class="poster-container"><div class="skeleton-line" style="width:100%;height:100%;border-radius:0"></div></div><div class="skeleton-line" style="height:5px;width:100%;border-radius:0"></div><div class="card-info"><div class="skeleton-line skeleton-title"></div><div class="skeleton-line skeleton-meta"></div></div></div>';g.innerHTML=h})();
</script>

<div class="footer" id="footer"></div>

<div class="bulk-bar" id="bulk-bar" role="toolbar" aria-label="Bulk actions" style="display:none">
  <span class="bulk-count" id="bulk-count">0 selected</span>
  <select class="filter-select" id="bulk-pref-select" aria-label="Set bulk source preference">
    <option value="">Set Preference...</option>
    <option value="prefer-debrid">Prefer Debrid</option>
    <option value="prefer-local">Prefer Local</option>
    <option value="none">Clear Preference</option>
  </select>
  <button class="btn btn-primary" id="bulk-pref-apply" onclick="bulkApplyPreference()">Apply</button>
  <button class="btn btn-ghost btn-sm" id="bulk-search" onclick="bulkSearchMissing()">Search Missing</button>
  <button class="btn btn-ghost btn-sm" id="bulk-deselect" onclick="deselectAll()">Deselect All</button>
  <span class="bulk-progress" id="bulk-progress" aria-live="polite"></span>
</div>

<script>
__THEME_TOGGLE_JS__

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
let _detailOrigin = null;  // e.g. 'activity' when arrived via Activity link — controls back-button target
let _librarySavedScrollY = 0;  // window.scrollY captured when entering detail view, restored on exit
let _preferences = {};
let _pending = {};
let _detailSeasons = [];
let _downloadServices = {show: null, movie: null};
let _searchEnabled = false;
let _searchTimer = null;
let _refreshTimer = null;
let _activeWantedPreset = null;
let _wantedInFlight = false;

/* Keyboard shortcut: Escape handler for this page */
window.onKbEscape = function() {
  if (_inDetailView) { hideDetail(); return; }
  var si = document.getElementById('search-input');
  if (si && si.value) { si.value = ''; applyFilters(); return; }
};
let _lastTransferText = '';
let _lastTransferType = '';
let _transferClearTimer = null;
let _pollTimer = null;
let _pollActive = false;
let _refreshPollTimer = null;

// ---------------------------------------------------------------------------
// Select mode state
// ---------------------------------------------------------------------------
let _selectMode = false;
let _selectedItems = {};  // key: normTitle, value: {title, tab}
let _lastCheckedIndex = -1;

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function esc(s) {
  const d = document.createElement('div');
  d.appendChild(document.createTextNode(String(s ?? '')));
  return d.innerHTML;
}
function escAttr(s) {
  return esc(s).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
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
// Transfer message helpers
// ---------------------------------------------------------------------------
function _showMsg(text, type) {
  var el = document.getElementById('transfer-msg');
  if (!el) return;
  el.className = 'transfer-msg' + (type === 'success' ? ' msg-success' : type === 'error' ? ' msg-error' : '');
  el.textContent = text;
  _lastTransferText = text;
  _lastTransferType = type || '';
  if (_transferClearTimer) { clearTimeout(_transferClearTimer); _transferClearTimer = null; }
  if (text && type !== 'error') {
    _transferClearTimer = setTimeout(_clearTransferMsg, 10000);
  }
}

function _showMsgHtml(html) {
  var el = document.getElementById('transfer-msg');
  if (!el) return;
  el.className = 'transfer-msg';
  el.innerHTML = html;
}

function _clearTransferMsg() {
  _lastTransferText = '';
  _lastTransferType = '';
  _transferClearTimer = null;
  var el = document.getElementById('transfer-msg');
  if (el) { el.className = 'transfer-msg'; el.textContent = ''; }
}

function _restoreTransferMsg() {
  if (!_lastTransferText) return;
  var el = document.getElementById('transfer-msg');
  if (!el) return;
  el.className = 'transfer-msg' + (_lastTransferType === 'success' ? ' msg-success' : _lastTransferType === 'error' ? ' msg-error' : '');
  el.textContent = _lastTransferText;
}

var _activeConfirmTimer = null;
var _activeConfirmBtn = null;

function _confirmBtn(btn, callback) {
  if (btn.classList.contains('confirming')) {
    // Second click — execute
    clearTimeout(_activeConfirmTimer);
    _activeConfirmTimer = null;
    _activeConfirmBtn = null;
    btn.classList.remove('confirming');
    btn.textContent = btn._origText;
    callback();
    return;
  }
  // Cancel any other button's confirmation
  if (_activeConfirmTimer) { clearTimeout(_activeConfirmTimer); }
  if (_activeConfirmBtn && _activeConfirmBtn !== btn) {
    try { _activeConfirmBtn.classList.remove('confirming'); _activeConfirmBtn.textContent = _activeConfirmBtn._origText; } catch(e) {}
  }
  // First click — enter confirmation state
  btn._origText = btn.textContent;
  btn.textContent = 'Are you sure?';
  btn.classList.add('confirming');
  _activeConfirmBtn = btn;
  _activeConfirmTimer = setTimeout(function() {
    _activeConfirmTimer = null;
    _activeConfirmBtn = null;
    try { btn.classList.remove('confirming'); btn.textContent = btn._origText; } catch(e) {}
  }, 3000);
}

function _clearConfirmState() {
  if (_activeConfirmTimer) { clearTimeout(_activeConfirmTimer); _activeConfirmTimer = null; }
  _activeConfirmBtn = null;
}

// ---------------------------------------------------------------------------
// Select mode
// ---------------------------------------------------------------------------
function toggleSelectMode() {
  _selectMode = !_selectMode;
  var btn = document.getElementById('btn-select');
  var area = document.getElementById('content-area');
  if (_selectMode) {
    btn.classList.add('active');
    btn.setAttribute('aria-pressed', 'true');
    btn.innerHTML = 'Cancel';
    if (area) area.classList.add('select-mode');
  } else {
    btn.classList.remove('active');
    btn.setAttribute('aria-pressed', 'false');
    btn.innerHTML = 'Select';
    _selectedItems = {};
    _lastCheckedIndex = -1;
    if (area) area.classList.remove('select-mode');
    document.querySelectorAll('.poster-card.selected').forEach(function(c) { c.classList.remove('selected'); });
    document.querySelectorAll('.card-checkbox.checked').forEach(function(c) { c.classList.remove('checked'); });
  }
  _updateBulkBar();
}

function onCardClick(index, event) {
  if (_selectMode) {
    event.preventDefault();
    event.stopPropagation();
    if (event.shiftKey && _lastCheckedIndex >= 0) {
      // Range select
      var lo = Math.min(_lastCheckedIndex, index);
      var hi = Math.max(_lastCheckedIndex, index);
      for (var i = lo; i <= hi; i++) {
        if (_displayedItems[i]) _setItemSelected(i, true);
      }
    } else {
      _toggleItemSelected(index);
    }
    _lastCheckedIndex = index;
    _updateBulkBar();
    return;
  }
  showDetail(index);
}

function _toggleItemSelected(index) {
  var item = _displayedItems[index];
  if (!item) return;
  var nk = normTitle(item.title);
  if (_selectedItems[nk]) {
    delete _selectedItems[nk];
    _setCardVisual(index, false);
  } else {
    _selectedItems[nk] = {title: item.title, tab: _activeTab};
    _setCardVisual(index, true);
  }
}

function _setItemSelected(index, selected) {
  var item = _displayedItems[index];
  if (!item) return;
  var nk = normTitle(item.title);
  if (selected) {
    _selectedItems[nk] = {title: item.title, tab: _activeTab};
  } else {
    delete _selectedItems[nk];
  }
  _setCardVisual(index, selected);
}

function _setCardVisual(index, selected) {
  var cards = document.querySelectorAll('.poster-card[data-index="' + index + '"]');
  cards.forEach(function(card) {
    var cb = card.querySelector('.card-checkbox');
    if (selected) {
      card.classList.add('selected');
      if (cb) { cb.classList.add('checked'); cb.setAttribute('aria-checked', 'true'); }
    } else {
      card.classList.remove('selected');
      if (cb) { cb.classList.remove('checked'); cb.setAttribute('aria-checked', 'false'); }
    }
  });
}

function _updateBulkBar() {
  var bar = document.getElementById('bulk-bar');
  if (!bar) return;
  var count = Object.keys(_selectedItems).length;
  if (_selectMode && count > 0) {
    bar.style.display = '';
    document.body.classList.add('has-bulk-bar');
    document.getElementById('bulk-count').textContent = count + ' item' + (count !== 1 ? 's' : '') + ' selected';
  } else {
    bar.style.display = 'none';
    document.body.classList.remove('has-bulk-bar');
  }
  // Update select button badge
  var btn = document.getElementById('btn-select');
  if (_selectMode && count > 0) {
    btn.innerHTML = 'Cancel <span class="select-count">' + count + '</span>';
  } else if (_selectMode) {
    btn.innerHTML = 'Cancel';
  }
}

function deselectAll() {
  _selectedItems = {};
  _lastCheckedIndex = -1;
  document.querySelectorAll('.poster-card.selected').forEach(function(c) { c.classList.remove('selected'); });
  document.querySelectorAll('.card-checkbox.checked').forEach(function(c) { c.classList.remove('checked'); });
  _updateBulkBar();
}

function _getSelectedItemsList() {
  // Returns array of {nk, title, tab} for all selected items
  var list = [];
  var keys = Object.keys(_selectedItems);
  for (var i = 0; i < keys.length; i++) {
    list.push({nk: keys[i], title: _selectedItems[keys[i]].title, tab: _selectedItems[keys[i]].tab});
  }
  return list;
}

function _findItemByNk(nk, tab) {
  var dataset = tab === 'movies' ? _allMovies : _allShows;
  for (var i = 0; i < dataset.length; i++) {
    if (normTitle(dataset[i].title) === nk) return dataset[i];
  }
  return null;
}

// ---------------------------------------------------------------------------
// Bulk actions
// ---------------------------------------------------------------------------
var _bulkInFlight = false;

function _setBulkActionsDisabled(disabled) {
  ['bulk-pref-apply', 'bulk-search', 'bulk-deselect'].forEach(function(id) {
    var el = document.getElementById(id);
    if (el) el.disabled = disabled;
  });
  var sel = document.getElementById('bulk-pref-select');
  if (sel) sel.disabled = disabled;
}

function _showBulkProgress(text) {
  var el = document.getElementById('bulk-progress');
  if (el) el.textContent = text;
}

function bulkApplyPreference() {
  if (_bulkInFlight) return;
  var sel = document.getElementById('bulk-pref-select');
  if (!sel || !sel.value) return;
  var pref = sel.value;
  var items = _getSelectedItemsList();
  if (!items.length) return;
  var prefLabel = pref === 'prefer-debrid' ? 'Prefer Debrid' : pref === 'prefer-local' ? 'Prefer Local' : 'No Preference';
  if (!confirm('Set preference to "' + prefLabel + '" for ' + items.length + ' item(s)?')) return;
  _bulkInFlight = true;
  _setBulkActionsDisabled(true);
  var done = 0;
  var failed = 0;
  var total = items.length;
  function _next() {
    if (done >= total) {
      _bulkInFlight = false;
      var msg = 'Applied preference to ' + (total - failed) + '/' + total + ' item(s).';
      if (failed > 0) msg += ' ' + failed + ' failed.';
      _showBulkProgress(msg);
      _setBulkActionsDisabled(false);
      sel.value = '';
      setTimeout(function() {
        _showBulkProgress('');
        toggleSelectMode();
        fetchLibrary();
      }, 1500);
      return;
    }
    var it = items[done];
    _showBulkProgress('Applying ' + (done + 1) + '/' + total + '...');
    fetch('/api/library/preference', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({title: it.nk, preference: pref})
    }).then(function(r) {
      if (!r.ok) { failed++; return; }
      return r.json();
    }).then(function(d) {
      if (d) {
        if (pref === 'none') { delete _preferences[it.nk]; }
        else { _preferences[it.nk] = pref; }
      }
    }).catch(function() { failed++; }).finally(function() {
      done++;
      setTimeout(_next, 100);
    });
  }
  _next();
}

function bulkSearchMissing() {
  if (_bulkInFlight) return;
  var items = _getSelectedItemsList();
  if (!items.length) return;
  // Build search tasks: per-season download requests for items with missing episodes
  var tasks = [];
  for (var i = 0; i < items.length; i++) {
    var it = items[i];
    var item = _findItemByNk(it.nk, it.tab);
    if (!item) continue;
    if (item.type === 'show' && item.season_data) {
      for (var si = 0; si < item.season_data.length; si++) {
        var season = item.season_data[si];
        var missingEps = [];
        for (var ei = 0; ei < (season.episodes || []).length; ei++) {
          if (season.episodes[ei].source === 'missing') {
            missingEps.push(season.episodes[ei].number);
          }
        }
        if (missingEps.length) {
          (function(capturedItem, capturedSeason, capturedEps) {
            tasks.push({item: capturedItem, season: capturedSeason, episodes: capturedEps});
          })(item, season.number, missingEps);
        }
      }
    } else if (item.type === 'movie' && item.missing_episodes > 0) {
      tasks.push({item: item, season: null, episodes: []});
    }
  }
  if (!tasks.length) {
    showToast('No missing episodes found in selected items.', 'warning');
    return;
  }
  var totalShows = new Set(tasks.map(function(t) { return normTitle(t.item.title); })).size;
  if (!confirm('Search for missing content across ' + totalShows + ' item(s) (' + tasks.length + ' request(s))?')) return;
  _bulkInFlight = true;
  _setBulkActionsDisabled(true);
  var done = 0;
  var total = tasks.length;
  var succeeded = 0;
  function _nextSearch() {
    if (done >= total) {
      _bulkInFlight = false;
      var failed = total - succeeded;
      var msg = 'Triggered search for ' + succeeded + '/' + total + ' request(s).';
      if (failed > 0) msg += ' ' + failed + ' failed.';
      _showBulkProgress(msg);
      _setBulkActionsDisabled(false);
      setTimeout(function() {
        _showBulkProgress('');
        toggleSelectMode();
        fetchLibrary();
      }, 2000);
      return;
    }
    var t = tasks[done];
    _showBulkProgress('Searching ' + (done + 1) + '/' + total + '...');
    var payload = {title: t.item.title, type: t.item.type};
    if (t.season !== null) {
      payload.season = t.season;
      payload.episodes = t.episodes;
    }
    fetch('/api/library/download', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    }).then(function(r) {
      if (r.ok) succeeded++;
    }).catch(function() {}).finally(function() {
      done++;
      setTimeout(_nextSearch, 500);
    });
  }
  _nextSearch();
}

// ---------------------------------------------------------------------------
// Tab switching
// ---------------------------------------------------------------------------
function switchTab(name) {
  _activeTab = name;
  _lastCheckedIndex = -1;
  _activeWantedPreset = null;
  var url = new URL(window.location);
  url.searchParams.delete('filter');
  history.replaceState(null, '', url);
  document.querySelectorAll('.tab').forEach(function(t) {
    const active = t.getAttribute('aria-controls') === 'tab-' + name;
    t.classList.toggle('active', active);
    t.setAttribute('aria-selected', active ? 'true' : 'false');
  });
  applyFilters();
  _updateWantedUI();
}

// ---------------------------------------------------------------------------
// Filtering & rendering
// ---------------------------------------------------------------------------
function buildBadges(source) {
  if (source === 'both') {
    return '<span class="badge-local"><span class="badge-full">Local</span><span class="badge-mini">L</span></span><span class="badge-debrid"><span class="badge-full">Debrid</span><span class="badge-mini">D</span></span>';
  }
  if (source === 'local') return '<span class="badge-local"><span class="badge-full">Local</span><span class="badge-mini">L</span></span>';
  if (source === 'debrid') return '<span class="badge-debrid"><span class="badge-full">Debrid</span><span class="badge-mini">D</span></span>';
  return '<span class="badge-debrid"><span class="badge-full">' + esc(source) + '</span><span class="badge-mini">?</span></span>';
}

function _qualityBadge(quality) {
  if (!quality || !quality.label) return '<span class="badge-quality badge-quality-unknown">Unknown</span>';
  var res = quality.resolution || '';
  var cls = 'badge-quality-unknown';
  if (res === '2160p') cls = 'badge-quality-2160p';
  else if (res === '1080p') cls = 'badge-quality-1080p';
  else if (res === '720p') cls = 'badge-quality-720p';
  else if (res === '480p') cls = 'badge-quality-480p';
  return '<span class="badge-quality ' + cls + '">' + esc(quality.label) + '</span>';
}

function _formatBytes(bytes) {
  if (!bytes || bytes <= 0) return '';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
  if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
  return (bytes / 1073741824).toFixed(1) + ' GB';
}


// Returns a plain-text string. Callers must pass through esc() before inserting into HTML.
function formatPendingDetail(pe) {
  if (!pe || !pe.last_error) return '';
  var parts = [];
  var rc = Number(pe.retry_count) || 0;
  if (rc > 0) parts.push('failed ' + rc + (rc === 1 ? ' time' : ' times'));
  else parts.push('first attempt failed');
  parts.push(pe.last_error);
  if (pe.next_retry_at) {
    var nra = new Date(pe.next_retry_at);
    var diffMs = nra - Date.now();
    if (diffMs > 0) {
      parts.push('next retry at ' + nra.toLocaleTimeString([], {hour: 'numeric', minute: '2-digit'}));
    } else {
      parts.push('retry overdue');
    }
  }
  return parts.length ? parts.join(' \u00B7 ') : '';
}

function computeProgress(item) {
  var nk = normTitle(item.title);
  var isPending = !!_pending[nk];
  if (item.type === 'movie') {
    return {width:'100%', color: isPending ? '#7a43b6' : '#27c24c',
            tooltip: isPending ? 'Switching source' : 'Available'};
  }
  // Show — no TMDB data: invisible bar
  if (!item.total_episodes || item.total_episodes <= 0) {
    return {width:'100%', color:'transparent', tooltip: (item.episodes || 0) + ' episodes'};
  }
  var pct = Math.min(100, Math.round((item.episodes || 0) / item.total_episodes * 100));
  var color;
  if (isPending) color = '#7a43b6';
  else if (pct >= 100 && (item.tmdb_status === 'Ended' || item.tmdb_status === 'Canceled')) color = '#27c24c';
  else if (pct >= 100) color = '#5d9cec';
  else color = '#f05050';
  return {width: pct + '%', color: color,
          tooltip: (item.episodes || 0) + ' / ' + item.total_episodes + ' episodes'};
}

function _titleHue(title) {
  title = title || '';
  var h = 0;
  for (var i = 0; i < title.length; i++) { h = ((h << 5) - h + title.charCodeAt(i)) | 0; }
  return Math.abs(h) % 360;
}

function _placeholderBg(title) {
  var hue = _titleHue(title);
  return 'background:hsl(' + hue + ',45%,35%);background-image:radial-gradient(ellipse at 30% 20%,hsl(' + hue + ',55%,50%) 0%,hsl(' + hue + ',40%,25%) 100%)';
}

function _placeholderHtml(title) {
  var initial = (title || '?').charAt(0).toUpperCase();
  return '<div class="poster-placeholder" style="' + _placeholderBg(title) + '">'
    + '<span class="pp-initial">' + esc(initial) + '</span>'
    + '<span class="pp-title">' + esc(title) + '</span></div>';
}

function buildCard(item, index) {
  // Poster image or placeholder
  var posterHtml;
  var phBg = _placeholderBg(item.title);
  if (item.poster_url) {
    posterHtml = '<img class="poster-img" src="' + escAttr(item.poster_url) + '" loading="lazy" decoding="async" alt="" onload="this.classList.add(\'loaded\')" onerror="this.style.display=\'none\';var p=this.parentElement.querySelector(\'.poster-placeholder\');if(p)p.style.display=\'flex\'">'
      + '<div class="poster-placeholder" style="display:none;' + phBg + '">'
      + '<span class="pp-initial">' + esc((item.title || '?').charAt(0).toUpperCase()) + '</span>'
      + '<span class="pp-title">' + esc(item.title) + '</span></div>';
  } else {
    posterHtml = _placeholderHtml(item.title);
  }

  // Corner badge (Ended/Canceled shows get red triangle)
  var cornerBadge = '';
  if (item.type === 'show' && (item.tmdb_status === 'Ended' || item.tmdb_status === 'Canceled')) {
    cornerBadge = '<div class="corner-badge ended"></div>';
  }

  // Progress bar
  var prog = computeProgress(item);
  var progressHtml = '<div class="progress-bar"><div class="progress-fill" style="width:' + prog.width + ';background:' + prog.color + '" title="' + escAttr(prog.tooltip) + '"></div></div>';

  // Meta line
  var metaLine = '';
  if (item.type === 'show' && item.missing_episodes > 0) {
    metaLine = '<span class="card-meta" style="color:var(--red)">' + item.missing_episodes + ' missing</span>';
  }

  // Pending badge — distinguish migrating (available) vs searching (missing)
  var pendingBadge = '';
  var nk = normTitle(item.title);
  if (_pending[nk]) {
    var pe = _pending[nk];
    var dir = pe.direction || '';
    if (dir === 'debrid-unavailable') {
      pendingBadge = '<span class="badge-unavailable">Debrid N/A</span>';
    } else if (dir === 'to-local-fallback') {
      pendingBadge = '<span class="badge-fallback">Downloading Locally</span>';
    } else {
    var hasMissingPending = false;
    if (item.type === 'movie') {
      // Movie: searching if current source doesn't include the target
      var targetHas = (dir === 'to-local')
        ? (item.source === 'local' || item.source === 'both')
        : (item.source === 'debrid' || item.source === 'both');
      if (!targetHas) hasMissingPending = true;
    } else if (pe.episodes && item.season_data) {
      var availEps = {};
      item.season_data.forEach(function(s) {
        (s.episodes || []).forEach(function(e) { if (e.source && e.source !== 'missing') availEps[s.number + ',' + e.number] = true; });
      });
      for (var pi = 0; pi < pe.episodes.length; pi++) {
        if (!availEps[pe.episodes[pi].season + ',' + pe.episodes[pi].episode]) { hasMissingPending = true; break; }
      }
    }
    if (hasMissingPending) {
      pendingBadge = '<span class="badge-pending">Searching</span>';
    } else {
      var upDir = dir === 'to-local' ? 'Local' : 'Debrid';
      pendingBadge = '<span class="badge-migrating"><span class="badge-full">Migrating to ' + upDir + '</span><span class="badge-mini">\u2197</span></span>';
    }
    } // end else (not debrid-unavailable/local-fallback)
  }

  // Add warning indicator to "Searching" badge when errors exist
  if (_pending[nk] && _pending[nk].last_error && pendingBadge.indexOf('badge-pending') !== -1) {
    var errTooltip = formatPendingDetail(_pending[nk]);
    pendingBadge = pendingBadge.replace('class="badge-pending"',
      'class="badge-pending has-error" title="' + escAttr(errTooltip) + '"');
  }
  var sourceBadges = buildBadges(item.source);
  var statusLine = '';
  if (pendingBadge || metaLine) {
    statusLine = '<div class="card-status">' + pendingBadge + metaLine + '</div>';
  }
  var isSelected = !!_selectedItems[nk];
  var checkboxHtml = '<div class="card-checkbox' + (isSelected ? ' checked' : '') + '" role="checkbox" aria-checked="' + (isSelected ? 'true' : 'false') + '"></div>';
  return '<div class="poster-card' + (isSelected ? ' selected' : '') + '" data-title="' + escAttr(item.title) + '" data-type="' + escAttr(item.type) + '"'
    + (item.year ? ' data-year="' + escAttr(String(item.year)) + '"' : '')
    + ' data-index="' + index + '"'
    + ' onclick="onCardClick(' + index + ',event)" tabindex="0" role="button"'
    + ' onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();onCardClick(' + index + ',event)}">'
    + '<div class="poster-container">' + checkboxHtml + posterHtml + cornerBadge + '</div>'
    + progressHtml
    + '<div class="card-info">'
    + '<div class="card-header"><div class="card-title">' + esc(item.title) + '</div>'
    + '<div class="card-badges">' + sourceBadges + '</div></div>'
    + statusLine
    + '</div></div>';
}

function _getItemTotalSize(item) {
  if (item.type === 'movie') return item.size_bytes || 0;
  var total = 0;
  if (item.season_data) {
    for (var si = 0; si < item.season_data.length; si++) {
      var eps = item.season_data[si].episodes || [];
      for (var ei = 0; ei < eps.length; ei++) { total += eps[ei].size_bytes || 0; }
    }
  }
  return total;
}

function applyFilters() {
  _lastCheckedIndex = -1;
  const query  = document.getElementById('search-input').value.trim().toLowerCase();
  const source = document.getElementById('source-filter').value;
  const status = document.getElementById('status-filter').value;
  const yearRange = document.getElementById('year-filter').value;
  let sortBy = document.getElementById('sort-select').value;
  const dataset = _activeTab === 'movies' ? _allMovies : _allShows;

  // Show/hide status filter and shows-only sort options
  document.getElementById('status-filter').style.display = _activeTab === 'shows' ? '' : 'none';
  var showsOnlySorts = ['episodes', 'complete'];
  showsOnlySorts.forEach(function(val) {
    var opt = document.querySelector('#sort-select option[value="' + val + '"]');
    if (opt) opt.style.display = _activeTab === 'shows' ? '' : 'none';
  });
  if (_activeTab !== 'shows' && showsOnlySorts.indexOf(sortBy) !== -1) {
    document.getElementById('sort-select').value = 'az';
    sortBy = 'az';
  }

  let filtered = dataset;

  if (source) {
    filtered = filtered.filter(function(item) {
      if (source === 'local')  return item.source === 'local'  || item.source === 'both';
      if (source === 'debrid') return item.source === 'debrid' || item.source === 'both';
      return true;
    });
  }

  if (status && _activeTab === 'shows') {
    filtered = filtered.filter(function(item) {
      if (status === 'Continuing') return item.tmdb_status === 'Returning Series' || item.tmdb_status === 'In Production' || item.tmdb_status === 'Planned' || item.tmdb_status === 'Pilot';
      if (status === 'Ended') return item.tmdb_status === 'Ended' || item.tmdb_status === 'Canceled';
      return false;
    });
  }

  if (yearRange) {
    filtered = filtered.filter(function(item) {
      var y = item.year;
      if (!y) return yearRange === 'older';
      if (yearRange === '2020s') return y >= 2020 && y <= 2029;
      if (yearRange === '2010s') return y >= 2010 && y <= 2019;
      if (yearRange === '2000s') return y >= 2000 && y <= 2009;
      if (yearRange === 'older') return y < 2000;
      return true;
    });
  }

  if (query) {
    filtered = filtered.filter(function(item) {
      return item.title.toLowerCase().indexOf(query) !== -1;
    });
  }

  // Wanted preset filter
  if (_activeWantedPreset && _activeWantedPreset !== 'recent') {
    filtered = filtered.filter(function(item) {
      return _matchesWantedPreset(item, _activeWantedPreset);
    });
  }

  // Sort — "Recently Added" preset overrides sort to date_added desc + limit 20
  if (_activeWantedPreset === 'recent') {
    filtered = filtered.filter(function(item) { return (item.date_added || 0) > 0; });
    filtered = filtered.slice().sort(function(a, b) { return (b.date_added || 0) - (a.date_added || 0); });
    filtered = filtered.slice(0, 20);
  } else {
    filtered = filtered.slice().sort(function(a, b) {
      if (sortBy === 'za') return b.title.localeCompare(a.title);
      if (sortBy === 'added') return (b.date_added || 0) - (a.date_added || 0);
      if (sortBy === 'year-new') {
        return (b.year || 0) - (a.year || 0) || a.title.localeCompare(b.title);
      }
      if (sortBy === 'year-old') {
        return (a.year || 9999) - (b.year || 9999) || a.title.localeCompare(b.title);
      }
      if (sortBy === 'complete') {
        var pctA = (a.total_episodes > 0) ? (a.episodes || 0) / a.total_episodes : 0;
        var pctB = (b.total_episodes > 0) ? (b.episodes || 0) / b.total_episodes : 0;
        return (pctB - pctA) || a.title.localeCompare(b.title);
      }
      if (sortBy === 'episodes') return (b.episodes || 0) - (a.episodes || 0);
      if (sortBy === 'size') return _getItemTotalSize(b) - _getItemTotalSize(a);
      return a.title.localeCompare(b.title); // default A-Z
    });
  }

  // Persist preferences
  try {
    localStorage.setItem('pd_library_sort', sortBy);
    localStorage.setItem('pd_library_source', source);
    localStorage.setItem('pd_library_status', status);
    localStorage.setItem('pd_library_year', yearRange);
  } catch(e) {}

  renderGrid(filtered);
  updateBadges(filtered.length);
}

function renderGrid(items) {
  const area = document.getElementById('content-area');
  _displayedItems = items;
  if (!items.length) {
    _updateJumpBar([]);
    const isFiltered = document.getElementById('search-input').value.trim()
      || document.getElementById('source-filter').value
      || document.getElementById('status-filter').value
      || document.getElementById('year-filter').value
      || _activeWantedPreset;
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
  var gridHtml = '<div class="grid">' + items.map(function(item, i) { return buildCard(item, i); }).join('') + '</div>';

  // Color legend (only when TMDB data is present)
  var hasTmdb = items.some(function(i) { return !!i.poster_url || !!i.tmdb_status; });
  if (hasTmdb) {
    if (_activeTab === 'shows') {
      gridHtml += '<div class="legend">'
        + '<span class="legend-item"><span class="legend-swatch" style="background:#5d9cec"></span>Continuing (Complete)</span>'
        + '<span class="legend-item"><span class="legend-swatch" style="background:#27c24c"></span>Ended (Complete)</span>'
        + '<span class="legend-item"><span class="legend-swatch" style="background:#f05050"></span>Missing Episodes</span>'
        + '<span class="legend-item"><span class="legend-swatch" style="background:#7a43b6"></span>Switching Source</span>'
        + '</div>';
    } else {
      gridHtml += '<div class="legend">'
        + '<span class="legend-item"><span class="legend-swatch" style="background:#27c24c"></span>Available</span>'
        + '<span class="legend-item"><span class="legend-swatch" style="background:#7a43b6"></span>Switching Source</span>'
        + '</div>';
    }
  }

  area.innerHTML = gridHtml;
  if (_selectMode) area.classList.add('select-mode');
  else area.classList.remove('select-mode');
  _observeUncachedCards();
  _updateJumpBar(items);
}

var _JUMP_LETTERS = ['#','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'];

function _getItemLetter(title) {
  var ch = (title || '').charAt(0).toUpperCase();
  // Normalize diacritics: A->A, E->E, etc.
  var base = ch.normalize ? ch.normalize('NFD').charAt(0) : ch;
  if (base >= 'A' && base <= 'Z') return base;
  return '#';
}

function _updateJumpBar(items) {
  var bar = document.getElementById('jump-bar');
  if (!bar) return;
  if (!items || !items.length) { bar.style.display = 'none'; return; }
  // Hide jump bar when sort is not alphabetical or "recently added" preset is active
  var sortBy = document.getElementById('sort-select').value;
  if (_activeWantedPreset === 'recent') { bar.style.display = 'none'; return; }
  if (sortBy !== 'az' && sortBy !== 'za') { bar.style.display = 'none'; return; }

  // Build set of letters that have items + first title per letter
  var activeLetters = {};
  var firstTitle = {};
  for (var i = 0; i < items.length; i++) {
    var lt = _getItemLetter(items[i].title);
    activeLetters[lt] = true;
    if (!firstTitle[lt]) firstTitle[lt] = items[i].title;
  }

  var html = '';
  for (var li = 0; li < _JUMP_LETTERS.length; li++) {
    var letter = _JUMP_LETTERS[li];
    var active = !!activeLetters[letter];
    if (active) {
      var tipText = firstTitle[letter] ? esc(firstTitle[letter]) : '';
      html += '<span class="jump-letter" tabindex="0" role="button" aria-label="Jump to ' + letter + '"'
        + ' onclick="jumpToLetter(\'' + letter + '\')"'
        + ' onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();jumpToLetter(\'' + letter + '\')}"'
        + '>' + letter + (tipText ? '<span class="jump-tip">' + tipText + '</span>' : '') + '</span>';
    } else {
      html += '<span class="jump-letter inactive" aria-hidden="true">' + letter + '</span>';
    }
  }
  bar.innerHTML = html;
  bar.style.display = '';
}

function jumpToLetter(letter) {
  var cards = document.querySelectorAll('.poster-card[data-title]');
  var prefersReduced = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  for (var i = 0; i < cards.length; i++) {
    var cardLetter = _getItemLetter(cards[i].getAttribute('data-title'));
    if (cardLetter === letter) {
      cards[i].scrollIntoView({behavior: prefersReduced ? 'auto' : 'smooth', block: 'start'});
      cards[i].classList.add('jump-highlight');
      setTimeout(function(el) { el.classList.remove('jump-highlight'); }.bind(null, cards[i]), 1500);
      return;
    }
  }
}

// ---------------------------------------------------------------------------
// Lazy metadata fetch for uncached poster cards
// ---------------------------------------------------------------------------
var _metaObserver = null;
var _metaCache = {};

function _observeUncachedCards() {
  if (!window.IntersectionObserver) return;
  if (!_metaObserver) {
    _metaObserver = new IntersectionObserver(function(entries) {
      entries.forEach(function(entry) {
        if (!entry.isIntersecting) return;
        var card = entry.target;
        _metaObserver.unobserve(card);
        var title = card.getAttribute('data-title');
        var type = card.getAttribute('data-type');
        var year = card.getAttribute('data-year') || '';
        if (!title) return;
        var nk = normTitle(title);
        if (_metaCache[nk]) { _applyMeta(card, _metaCache[nk]); return; }
        var url = '/api/library/metadata?title=' + encodeURIComponent(title) + '&type=' + encodeURIComponent(type);
        if (year) url += '&year=' + year;
        fetch(url).then(function(r) { return r.ok ? r.json() : null; }).then(function(meta) {
          if (!meta) return;
          _metaCache[nk] = meta;
          _applyMeta(card, meta);
        }).catch(function(){});
      });
    }, {rootMargin: '200px'});
  }
  // Disconnect stale references from previous renders before observing new cards
  _metaObserver.disconnect();
  document.querySelectorAll('.poster-card').forEach(function(card) {
    if (!card.querySelector('.poster-img')) {
      _metaObserver.observe(card);
    }
  });
}

function _applyMeta(card, meta) {
  var container = card.querySelector('.poster-container');
  if (!container) return;

  // Update poster image
  if (meta.poster_url) {
    var placeholder = container.querySelector('.poster-placeholder');
    if (placeholder && !container.querySelector('.poster-img')) {
      var img = document.createElement('img');
      img.className = 'poster-img';
      img.loading = 'lazy';
      img.decoding = 'async';
      img.alt = '';
      img.onload = function() { img.classList.add('loaded'); };
      img.onerror = function() { img.style.display = 'none'; placeholder.style.display = 'flex'; };
      img.src = meta.poster_url;
      placeholder.style.display = 'none';
      container.insertBefore(img, placeholder);
    }
  }

  // Add corner badge for Ended/Canceled shows
  var type = card.getAttribute('data-type');
  if (type === 'show' && (meta.status === 'Ended' || meta.status === 'Canceled') && !container.querySelector('.corner-badge')) {
    var badge = document.createElement('div');
    badge.className = 'corner-badge ended';
    container.appendChild(badge);
  }

  // Update progress bar — only count AIRED episodes (air_date <= today)
  if (type === 'show' && meta.seasons) {
    var totalEps = 0;
    var today = new Date().toISOString().slice(0, 10);
    for (var si = 0; si < meta.seasons.length; si++) {
      var eps = meta.seasons[si].episodes || [];
      for (var ei = 0; ei < eps.length; ei++) {
        var ad = eps[ei].air_date;
        if (ad && ad <= today) totalEps++;
      }
    }
    if (totalEps > 0) {
      var title = card.getAttribute('data-title');
      var nk = normTitle(title || '');
      // Use data-type to pick the correct list (safe in async callbacks)
      var items = type === 'show' ? _allShows : _allMovies;
      var haveEps = 0;
      for (var i = 0; i < items.length; i++) {
        if (normTitle(items[i].title) === nk) { haveEps = items[i].episodes || 0; break; }
      }
      var pct = Math.min(100, Math.round(haveEps / totalEps * 100));
      var isPending = !!_pending[nk];
      var color;
      if (isPending) color = '#7a43b6';
      else if (pct >= 100 && (meta.status === 'Ended' || meta.status === 'Canceled')) color = '#27c24c';
      else if (pct >= 100) color = '#5d9cec';
      else color = '#f05050';
      var fill = card.querySelector('.progress-fill');
      if (fill) {
        fill.style.width = pct + '%';
        fill.style.background = color;
        fill.title = haveEps + ' / ' + totalEps + ' episodes';
      }
      // Update meta line with missing count
      var missing = totalEps - haveEps;
      if (missing > 0) {
        var metaEl = card.querySelector('.card-meta');
        if (!metaEl) {
          // Create status row + meta element if they didn't exist
          var statusEl = card.querySelector('.card-status');
          if (!statusEl) {
            var infoEl = card.querySelector('.card-info');
            if (infoEl) {
              statusEl = document.createElement('div');
              statusEl.className = 'card-status';
              infoEl.appendChild(statusEl);
            }
          }
          if (statusEl) {
            metaEl = document.createElement('span');
            metaEl.className = 'card-meta';
            statusEl.appendChild(metaEl);
          }
        }
        if (metaEl && !metaEl.hasAttribute('data-enriched')) {
          metaEl.setAttribute('data-enriched', '1');
          metaEl.innerHTML = '<span style="color:var(--red)">' + missing + ' missing</span>';
        }
      }
    }
  }
}

function updateBadges(filteredCount) {
  const query  = document.getElementById('search-input').value.trim();
  const source = document.getElementById('source-filter').value;
  const status = document.getElementById('status-filter').value;
  const yearRange = document.getElementById('year-filter').value;
  const isFiltered = query || source || status || yearRange || _activeWantedPreset;

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
// Wanted preset filters
// ---------------------------------------------------------------------------
function _countAiredMissing(item) {
  // missing_episodes is computed by TMDB cache enrichment (total - have).
  // season_data from the API only contains episodes WITH files, so we
  // cannot iterate it for source==='missing' — those entries only exist
  // after detail-view TMDB metadata merge.
  return (item.missing_episodes || 0) > 0 ? 1 : 0;
}

function _getPendingDirection(item) {
  var nk = normTitle(item.title);
  var pe = _pending[nk];
  if (!pe) return '';
  return pe.direction || '';
}

function _matchesWantedPreset(item, preset) {
  if (preset === 'missing') return _countAiredMissing(item) > 0;
  if (preset === 'unavailable') return _getPendingDirection(item) === 'debrid-unavailable';
  if (preset === 'pending') {
    var dir = _getPendingDirection(item);
    return dir === 'to-local' || dir === 'to-debrid' || dir === 'to-local-fallback';
  }
  if (preset === 'fallback') return _getPendingDirection(item) === 'to-local-fallback';
  if (preset === 'recent') return (item.date_added || 0) > 0;
  return false;
}

function _computeWantedCounts() {
  var dataset = _activeTab === 'movies' ? _allMovies : _allShows;
  var counts = {missing: 0, unavailable: 0, pending: 0, fallback: 0, recent: 0};
  for (var i = 0; i < dataset.length; i++) {
    if (_countAiredMissing(dataset[i]) > 0) counts.missing++;
    var dir = _getPendingDirection(dataset[i]);
    if (dir === 'debrid-unavailable') counts.unavailable++;
    if (dir === 'to-local' || dir === 'to-debrid' || dir === 'to-local-fallback') counts.pending++;
    if (dir === 'to-local-fallback') counts.fallback++;
    if ((dataset[i].date_added || 0) > 0) counts.recent++;
  }
  // Cap recent count to 20 (the display limit)
  counts.recent = Math.min(counts.recent, 20);
  return counts;
}

function _updateWantedUI() {
  var counts = _computeWantedCounts();
  var ids = ['missing', 'unavailable', 'pending', 'fallback', 'recent'];
  for (var i = 0; i < ids.length; i++) {
    var el = document.getElementById('pill-count-' + ids[i]);
    if (el) el.textContent = counts[ids[i]] > 0 ? '(' + counts[ids[i]] + ')' : '';
  }
  // Update pills active state
  var pills = document.querySelectorAll('.wanted-pill');
  for (var j = 0; j < pills.length; j++) {
    if (pills[j].getAttribute('data-preset') === _activeWantedPreset) {
      pills[j].classList.add('active');
    } else {
      pills[j].classList.remove('active');
    }
  }
  // Update wanted actions bar
  var actionsBar = document.getElementById('wanted-actions');
  var searchBtn = document.getElementById('wanted-search-btn');
  var downloadBtn = document.getElementById('wanted-download-btn');
  var hasActions = _activeWantedPreset && _activeWantedPreset !== 'recent';
  if (hasActions && !_wantedInFlight) {
    actionsBar.style.display = '';
    searchBtn.style.display = _activeWantedPreset === 'missing' ? '' : 'none';
    downloadBtn.style.display = _activeWantedPreset === 'unavailable' ? '' : 'none';
  } else if (hasActions && _wantedInFlight) {
    actionsBar.style.display = '';  // keep visible during bulk operation
  } else {
    actionsBar.style.display = 'none';
  }
  // Nav wanted badge — count across both movies and shows
  var totalMissing = 0;
  for (var mi = 0; mi < _allMovies.length; mi++) {
    if (_countAiredMissing(_allMovies[mi]) > 0) totalMissing++;
  }
  for (var si = 0; si < _allShows.length; si++) {
    if (_countAiredMissing(_allShows[si]) > 0) totalMissing++;
  }
  var navLink = document.getElementById('nav-wanted-link');
  var navCount = document.getElementById('nav-wanted-count');
  if (navLink && navCount) {
    if (totalMissing > 0) {
      navLink.style.display = '';
      navCount.textContent = totalMissing;
    } else {
      navLink.style.display = 'none';
    }
  }
}

function toggleWantedPreset(preset) {
  if (_activeWantedPreset === preset) {
    _activeWantedPreset = null;
  } else {
    _activeWantedPreset = preset;
  }
  // Update URL without reload
  var url = new URL(window.location);
  if (_activeWantedPreset) {
    url.searchParams.set('filter', _activeWantedPreset);
  } else {
    url.searchParams.delete('filter');
  }
  history.replaceState(null, '', url);
  applyFilters();
  _updateWantedUI();
}

function _resolveMissingTasks(items, callback) {
  // Build tasks from displayed items.  For shows, we need to fetch TMDB
  // metadata to discover which specific episodes are missing (season_data
  // from the main API only contains episodes that HAVE files).
  var movieTasks = [];
  var showsToResolve = [];
  for (var i = 0; i < items.length; i++) {
    var item = items[i];
    if (item.type === 'movie' && item.missing_episodes > 0) {
      movieTasks.push({item: item, season: null, episodes: []});
    } else if (item.type === 'show' && item.missing_episodes > 0) {
      showsToResolve.push(item);
    }
  }
  if (!showsToResolve.length) { callback(movieTasks); return; }

  var resolved = 0;
  var showTasks = [];
  var today = new Date().toISOString().slice(0, 10);
  for (var si = 0; si < showsToResolve.length; si++) {
    (function(show) {
      var params = 'title=' + encodeURIComponent(show.title) + '&type=show';
      if (show.year) params += '&year=' + encodeURIComponent(String(show.year));
      fetch('/api/library/metadata?' + params)
        .then(function(r) { return r.ok ? r.json() : null; })
        .then(function(meta) {
          if (meta) {
            var merged = _mergeShowMeta(show, meta);
            for (var mi = 0; mi < merged.length; mi++) {
              var season = merged[mi];
              var missingEps = [];
              for (var ei = 0; ei < (season.episodes || []).length; ei++) {
                var ep = season.episodes[ei];
                if (ep.source === 'missing' && ep.air_date && ep.air_date <= today) {
                  missingEps.push(ep.number);
                }
              }
              if (missingEps.length) {
                showTasks.push({item: show, season: season.number, episodes: missingEps});
              }
            }
          }
        })
        .catch(function() {})
        .finally(function() {
          resolved++;
          _showWantedProgress('Resolving ' + resolved + '/' + showsToResolve.length + ' shows...');
          if (resolved >= showsToResolve.length) {
            callback(movieTasks.concat(showTasks));
          }
        });
    })(showsToResolve[si]);
  }
}

function _runWantedBulk(endpoint, btnId, actionLabel, progressLabel) {
  if (_wantedInFlight) return;
  var items = _displayedItems;
  if (!items.length) return;
  _wantedInFlight = true;
  document.getElementById(btnId).disabled = true;
  _showWantedProgress('Resolving missing episodes...');

  _resolveMissingTasks(items, function(tasks) {
    if (!tasks.length) {
      _wantedInFlight = false;
      document.getElementById(btnId).disabled = false;
      _showWantedProgress('No missing items found.');
      setTimeout(function() { _showWantedProgress(''); }, 2000);
      return;
    }
    var totalShows = new Set(tasks.map(function(t) { return normTitle(t.item.title); })).size;
    if (!confirm(actionLabel + ' across ' + totalShows + ' item(s) (' + tasks.length + ' request(s))?')) {
      _wantedInFlight = false;
      document.getElementById(btnId).disabled = false;
      _showWantedProgress('');
      return;
    }
    var done = 0, total = tasks.length, succeeded = 0;
    function _next() {
      if (done >= total) {
        var msg = progressLabel + ' ' + succeeded + '/' + total + ' request(s).';
        if (total - succeeded > 0) msg += ' ' + (total - succeeded) + ' failed.';
        _showWantedProgress(msg);
        setTimeout(function() {
          _wantedInFlight = false;
          document.getElementById(btnId).disabled = false;
          _showWantedProgress('');
          fetchLibrary();
        }, 2000);
        return;
      }
      var t = tasks[done];
      _showWantedProgress(progressLabel + ' ' + (done + 1) + '/' + total + '...');
      var payload = {title: t.item.title, type: t.item.type};
      if (t.season !== null) { payload.season = t.season; payload.episodes = t.episodes; }
      fetch(endpoint, {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
      }).then(function(r) { if (r.ok) succeeded++; }).catch(function() {}).finally(function() {
        done++; setTimeout(_next, 500);
      });
    }
    _next();
  });
}

function wantedSearchAll() {
  _runWantedBulk('/api/library/download', 'wanted-search-btn', 'Search for missing content', 'Searched');
}

function wantedDownloadAll() {
  _runWantedBulk('/api/library/download-local-fallback', 'wanted-download-btn', 'Trigger local download', 'Downloaded');
}

function _showWantedProgress(msg) {
  var el = document.getElementById('wanted-progress');
  if (el) el.textContent = msg;
}

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------
function _applyLibraryData(data, opts) {
  opts = opts || {};
  _allMovies      = Array.isArray(data.movies) ? data.movies : [];
  _allShows       = Array.isArray(data.shows)  ? data.shows  : [];
  _preferences    = data.preferences || {};
  _pending        = data.pending || {};
  _downloadServices = data.download_services || {show: null, movie: null};
  _searchEnabled  = !!data.search_enabled;
  _lastScan       = data.last_scan || null;
  _scanDurationMs = data.scan_duration_ms || null;

  // Auto-switch tab if wanted preset has no matches in current tab but other tab does
  if (!_inDetailView && _activeWantedPreset && _activeWantedPreset !== 'recent') {
    var _curData = _activeTab === 'movies' ? _allMovies : _allShows;
    var _othData = _activeTab === 'movies' ? _allShows : _allMovies;
    var _othTab  = _activeTab === 'movies' ? 'shows' : 'movies';
    var _hasCur = _curData.some(function(item) { return _matchesWantedPreset(item, _activeWantedPreset); });
    if (!_hasCur) {
      var _hasOth = _othData.some(function(item) { return _matchesWantedPreset(item, _activeWantedPreset); });
      if (_hasOth) {
        _activeTab = _othTab;
        document.querySelectorAll('.tab').forEach(function(t) {
          var active = t.getAttribute('aria-controls') === 'tab-' + _othTab;
          t.classList.toggle('active', active);
          t.setAttribute('aria-selected', active ? 'true' : 'false');
        });
      }
    }
  }

  if (!opts.quiet) {
    if (!_inDetailView) {
      applyFilters();
      _updateWantedUI();
    }
    updateScanInfo();
  }
  _checkSmartPoll();

  if (_inDetailView && _detailItem) {
    var items = _detailItem.type === 'movie' ? _allMovies : _allShows;
    var nk = normTitle(_detailItem.title);
    for (var i = 0; i < items.length; i++) {
      if (normTitle(items[i].title) === nk) {
        _detailItem = items[i];
        _renderDetail();
        break;
      }
    }
  }

  if (!opts.quiet && _scanDurationMs != null) {
    document.getElementById('footer').textContent = 'Scan completed in ' + _scanDurationMs + ' ms';
  }
}

function fetchLibrary() {
  fetch('/api/library')
    .then(function(r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    })
    .then(function(data) {
      _applyLibraryData(data);
      _restoreDetailFromUrl();
    })
    .catch(function(err) {
      document.getElementById('content-area').innerHTML =
        '<div class="state-panel error-state"><div>Failed to load library.</div>'
        + '<div class="state-hint">' + esc(String(err)) + '</div></div>';
      updateBadges(0);
    });
}

function _backButtonHtml() {
  if (_detailOrigin === 'activity') {
    return '<button class="detail-back" onclick="location.href=\'/activity\'" tabindex="0">&larr; Back to Activity</button>';
  }
  return '<button class="detail-back" onclick="hideDetail()" tabindex="0">&larr; Back to Library</button>';
}

function _restoreDetailFromUrl() {
  try {
    var params = new URLSearchParams(window.location.search);
    var detailTitle = params.get('detail');
    var detailType = params.get('type');
    if (!detailTitle) return;
    _detailOrigin = params.get('from') || null;
    var nk = normTitle(detailTitle);
    // Try the hinted type first, then fall back to the other list. The hint
    // from Activity links isn't always right, but we can still resolve the
    // item by searching both lists.
    var order = detailType === 'movie' ? ['movie', 'show'] : ['show', 'movie'];
    for (var oi = 0; oi < order.length; oi++) {
      var tryType = order[oi];
      var items = tryType === 'movie' ? _allMovies : _allShows;
      for (var i = 0; i < items.length; i++) {
        if (normTitle(items[i].title) !== nk) continue;
        var wantTab = tryType === 'movie' ? 'movies' : 'shows';
        if (_activeTab !== wantTab) {
          _activeTab = wantTab;
          document.querySelectorAll('.tab').forEach(function(t) {
            var active = t.getAttribute('aria-controls') === 'tab-' + _activeTab;
            t.classList.toggle('active', active);
            t.setAttribute('aria-selected', active ? 'true' : 'false');
          });
        }
        // Bypass filters — saved localStorage filters (source/status/year)
        // could have excluded this item from _displayedItems via applyFilters.
        // Feed the item to showDetail directly. hideDetail() calls
        // applyFilters() to restore the normal grid on exit.
        _displayedItems = [items[i]];
        // On browser reload/direct URL entry the browser may have restored
        // scrollY from the last session — that Y belongs to the detail view,
        // not the library grid. Force to 0 before showDetail captures it, so
        // Back-to-Library lands at the top of the grid rather than random Y.
        window.scrollTo(0, 0);
        showDetail(0);
        return;
      }
    }
  } catch(e) {}
}

function _finishRefresh() {
  if (_refreshPollTimer) {
    clearTimeout(_refreshPollTimer);
    _refreshPollTimer = null;
  }
  _scanning = false;
  document.getElementById('btn-refresh').disabled = false;
  updateScanInfo();
}

function triggerRefresh() {
  if (_scanning) return;
  _scanning = true;
  document.getElementById('btn-refresh').disabled = true;
  updateScanInfo();

  var attempts = 0;
  var maxAttempts = 45; // ~90s
  var sawScanningTrue = false;

  function _pollRefresh() {
    attempts++;
    if (attempts > maxAttempts) {
      // Timeout — fetch whatever data is available and warn
      fetch('/api/library')
        .then(function(r) { return r.ok ? r.json() : null; })
        .then(function(data) { if (data) _applyLibraryData(data); })
        .catch(function() {})
        .finally(function() {
          _finishRefresh();
          _showMsg('Refresh timed out — data may be incomplete', 'error');
        });
      return;
    }
    fetch('/api/library')
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(data) {
        if (!data) { _refreshPollTimer = setTimeout(_pollRefresh, 2000); return; }
        if (data.scanning) sawScanningTrue = true;
        if (!data.scanning && sawScanningTrue) {
          _applyLibraryData(data);
          _finishRefresh();
        } else {
          _refreshPollTimer = setTimeout(_pollRefresh, 2000);
        }
      })
      .catch(function() { _refreshPollTimer = setTimeout(_pollRefresh, 2000); });
  }

  // Wait for POST acknowledgement before polling
  fetch('/api/library/refresh', {method: 'POST'})
    .then(function(r) {
      if (!r.ok) {
        _finishRefresh();
        _showMsg('Refresh failed (HTTP ' + r.status + ')', 'error');
        return;
      }
      sawScanningTrue = true; // POST succeeded — scan was started
      _refreshPollTimer = setTimeout(_pollRefresh, 1000);
    })
    .catch(function() {
      _finishRefresh();
      _showMsg('Refresh failed — could not reach server', 'error');
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
var _historySidebarGen = 0;

function showDetail(index) {
  var item = _displayedItems[index];
  if (!item) return;
  if (item.type === 'show' && (!item.season_data || !item.season_data.length)) return;
  // Capture scroll before hiding chrome so we can restore the user's spot on back.
  // Guard against nested re-entry overwriting the saved Y with a detail-view scroll.
  if (!_inDetailView) {
    _librarySavedScrollY = window.scrollY || window.pageYOffset || 0;
  }
  _inDetailView = true;
  _detailItem = item;
  _detailMeta = null;
  _lastTransferText = '';
  _lastTransferType = '';
  if (_transferClearTimer) { clearTimeout(_transferClearTimer); _transferClearTimer = null; }
  document.title = (item.title || '') + ' \u2014 pd_zurg Library';

  document.querySelector('.tabs').style.display = 'none';
  document.querySelector('.controls').style.display = 'none';
  document.getElementById('wanted-presets').style.display = 'none';
  document.getElementById('wanted-actions').style.display = 'none';
  document.getElementById('footer').style.display = 'none';
  document.getElementById('jump-bar').style.display = 'none';
  document.getElementById('bulk-bar').style.display = 'none';
  document.body.classList.remove('has-bulk-bar');

  // Persist detail view in URL so browser refresh restores it
  var _dUrl = new URL(window.location);
  _dUrl.searchParams.set('detail', item.title);
  _dUrl.searchParams.set('type', item.type);
  history.replaceState(null, '', _dUrl);

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
  _clearConfirmState();
  var item = _detailItem;
  var meta = _detailMeta;
  if (!item) return;

  if (item.type === 'movie') {
    _renderMovieDetail(item, meta);
  } else {
    _renderShowDetail(item, meta);
  }
  _restoreTransferMsg();
}

function _formatRuntime(mins) {
  if (!mins || mins <= 0) return '';
  var h = Math.floor(mins / 60);
  var m = mins % 60;
  if (h && m) return h + 'hr ' + m + 'min';
  if (h) return h + 'hr';
  return m + 'min';
}

function _renderDetailMeta(item, meta) {
  if (!meta) return '';
  var out = '';
  var metaBits = [];
  if (item.year) metaBits.push(esc(String(item.year)));
  var runtime = meta.runtime || meta.episode_run_time || 0;
  if (runtime) metaBits.push(esc(_formatRuntime(runtime)));
  var cert = meta.certification || meta.content_rating || '';
  if (cert) metaBits.push('<span class="detail-cert">' + esc(cert) + '</span>');
  if (metaBits.length) {
    out += '<div class="detail-meta-row">' + metaBits.join('<span class="sep">&middot;</span>') + '</div>';
  }
  if (meta.genres && meta.genres.length) {
    out += '<div class="detail-genres">' + meta.genres.map(esc).join(', ') + '</div>';
  }
  if (meta.vote_average && meta.vote_average > 0) {
    out += '<div class="detail-rating"><span class="rating-badge">TMDB ' + meta.vote_average.toFixed(1) + '</span></div>';
  }
  var byline = '';
  if (meta.directors && meta.directors.length) {
    byline = 'Directed by ' + meta.directors.map(esc).join(', ');
  } else if (meta.creators && meta.creators.length) {
    byline = 'Created by ' + meta.creators.map(esc).join(', ');
  }
  if (byline) {
    out += '<div class="detail-directors">' + byline + '</div>';
  }
  return out;
}

function _renderMediaInfo(quality) {
  if (!quality || !quality.label) return '';
  var videoParts = [];
  if (quality.resolution) videoParts.push(quality.resolution);
  if (quality.codec) videoParts.push('(' + quality.codec + ')');
  var videoStr = videoParts.join(' ');
  if (!videoStr) return '';
  return '<div class="media-info">'
    + '<div class="mi-group"><div class="mi-label">Video</div><div class="mi-value">' + esc(videoStr) + '</div></div>'
    + '</div>';
}

function _renderCastSection(meta) {
  if (!meta || !meta.cast || !meta.cast.length) return '';
  var out = '<div class="cast-section"><h3>Cast &amp; Crew</h3><div class="cast-scroll">';
  meta.cast.slice(0, 15).forEach(function(p) {
    var bg = p.profile_url ? 'background-image:url(\'' + escAttr(p.profile_url) + '\')' : '';
    out += '<div class="cast-card">'
         + '<div class="cast-photo" style="' + bg + '" role="img" aria-label="' + escAttr(p.name) + '"></div>'
         + '<div class="cast-name">' + esc(p.name) + '</div>'
         + (p.character ? '<div class="cast-role">' + esc(p.character) + '</div>' : '')
         + '</div>';
  });
  out += '</div></div>';
  return out;
}

function _renderMovieDetail(movie, meta) {
  var area = document.getElementById('content-area');
  var html = '<div class="detail-view">';
  html += _backButtonHtml();

  html += '<div class="detail-hero">';
  if (meta && meta.poster_url) {
    html += '<div class="detail-poster"><img src="' + escAttr(meta.poster_url) + '" alt="' + escAttr('Poster for ' + movie.title) + '"></div>';
  }
  html += '<div class="detail-info">';
  html += '<h2>' + esc(movie.title);
  if (movie.year) html += ' <span class="card-year">(' + esc(String(movie.year)) + ')</span>';
  html += '</h2>';
  var moviePnk = normTitle(movie.title);
  var moviePe = _pending[moviePnk];
  var moviePeDir = moviePe ? (moviePe.direction || '') : '';
  html += '<div class="card-badges">';
  html += buildBadges(movie.source);
  if (moviePeDir === 'debrid-unavailable') {
    html += ' <span class="badge-unavailable">Debrid N/A</span>';
  } else if (moviePeDir === 'to-local-fallback') {
    html += ' <span class="badge-fallback">Downloading Locally</span>';
  }
  if (movie.quality && movie.quality.label) {
    html += ' ' + _qualityBadge(movie.quality);
    var movieSzStr = _formatBytes(movie.size_bytes);
    if (movieSzStr) html += ' <span class="ep-size">' + esc(movieSzStr) + '</span>';
  }
  html += '</div>';
  var movieErrText = (moviePeDir === 'to-debrid' || moviePeDir === 'to-local') ? formatPendingDetail(moviePe) : '';
  if (movieErrText) {
    html += '<div class="pending-callout" role="note" aria-label="Search error details">'
          + '<span class="pending-callout-icon">\u26A0</span> '
          + esc(movieErrText) + '</div>';
  }
  if (meta) {
    html += _renderDetailMeta(movie, meta);
    // Movie-only: shows have per-episode quality rendered in the episode table
    html += _renderMediaInfo(movie.quality);
    if (meta.overview) html += '<div class="detail-overview" onclick="this.classList.toggle(\'expanded\')">' + esc(meta.overview) + '</div>';
  }
  // Movie preference dropdown + action buttons
  var movieNk = normTitle(movie.title);
  var moviePref = _preferences[movieNk] || 'none';
  if (_downloadServices.movie) {
    _savedPref = moviePref;
    html += '<div class="pref-row"><label for="movie-pref-select" style="font-size:.82em;color:var(--text2)">Source preference:</label>';
    html += '<select class="pref-select" id="movie-pref-select" onchange="onPrefSelectChange(this.value)">';
    html += '<option value="none"' + (moviePref === 'none' ? ' selected' : '') + '>No Preference</option>';
    html += '<option value="prefer-local"' + (moviePref === 'prefer-local' ? ' selected' : '') + '>Prefer Local</option>';
    if (_downloadServices.movie === 'radarr') {
      html += '<option value="prefer-debrid"' + (moviePref === 'prefer-debrid' ? ' selected' : '') + '>Prefer Debrid</option>';
    }
    html += '</select>';
    html += '<button class="btn btn-primary" id="movie-pref-apply-btn" style="display:none" onclick="applyMoviePreference()">Apply</button>';
    html += '</div>';
    html += '<div style="font-size:.75em;color:var(--text3);margin-top:2px;line-height:1.5"><strong style="color:var(--text2)">Prefer Local</strong> &mdash; switches the movie to a local copy.<br><strong style="color:var(--text2)">Prefer Debrid</strong> &mdash; removes the local copy and streams from debrid.</div>';
    html += '<div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">';
    if (moviePeDir === 'debrid-unavailable') {
      html += '<button class="btn btn-ghost btn-sm" onclick="_confirmBtn(this,function(){downloadMovieLocalFallback()})">Download Locally</button>';
    } else if (moviePeDir === 'to-local-fallback') {
      html += '<button class="btn btn-ghost btn-sm" disabled>Downloading\u2026</button>';
    } else if (movie.source === 'debrid') {
      var movieDlLabel = _downloadServices.movie === 'overseerr' ? 'Request in Overseerr' : 'Switch to Local';
      var movieDebridPref = _downloadServices.movie === 'overseerr' ? undefined : false;
      html += '<button class="btn btn-ghost btn-sm btn-switch" onclick="_confirmBtn(this,function(){downloadMovie(' + (movieDebridPref === undefined ? '' : movieDebridPref) + ')})">' + movieDlLabel + '</button>';
    }
    if ((movie.source === 'local' || movie.source === 'both') && _downloadServices.movie === 'radarr') {
      html += '<button class="btn btn-ghost btn-sm btn-switch" onclick="_confirmBtn(this,function(){removeMovie()})">Switch to Debrid</button>';
    }
    html += '</div>';
  } else if (movie.source === 'debrid') {
    html += '<div style="margin-top:10px;font-size:.82em;color:var(--text3)">To switch to local, configure <a href="/settings">Radarr or Overseerr</a> in Settings.</div>';
  }
  var movieActionBtns = [];
  if (movie.source === 'debrid' || movie.source === 'both') {
    movieActionBtns.push('<button class="btn btn-ghost btn-icon" title="Block this torrent file" onclick="event.stopPropagation();_blockItem()">&#128683;</button>');
  }
  if (_downloadServices.movie === 'radarr') {
    movieActionBtns.push('<button class="btn btn-ghost btn-sm btn-danger" title="Delete from Radarr" onclick="event.stopPropagation();_confirmBtn(this,function(){deleteItem(\'movie\')})">&#128465; Delete</button>');
  }
  if (_searchEnabled && movie.imdb_id) {
    movieActionBtns.push('<button class="btn btn-ghost btn-sm" data-imdb="' + escAttr(movie.imdb_id) + '" data-mtype="movie" data-label="' + escAttr(movie.title) + '" onclick="openSearchFromBtn(this)">&#128269; Search Torrents</button>');
  }
  if (movieActionBtns.length) {
    html += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">' + movieActionBtns.join('') + '</div>';
  }
  html += '</div></div>';
  html += '<div class="detail-body"><div class="detail-main">';
  html += _renderCastSection(meta);
  html += '<div id="transfer-msg" aria-live="polite"></div>';
  html += '<div class="history-sidebar" id="history-sidebar-content"><h3>Activity</h3><div class="hs-empty">Loading\u2026</div></div>';
  html += '</div>';
  html += '</div>';
  html += '</div>';
  area.innerHTML = html;
  _loadHistorySidebar();
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
        episodes.push({number: te.number, title: te.title, air_date: te.air_date, file: fe.file, source: fe.source, quality: fe.quality, size_bytes: fe.size_bytes});
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
    episodes.sort(function(a, b) { return b.number - a.number; });
    var haveCount = episodes.filter(function(e) { return e.source !== 'missing'; }).length;
    merged.push({number: tmdbS.number, total_episodes: tmdbS.total_episodes, episode_count: haveCount, episodes: episodes});
  });
  // Append file seasons not in TMDB
  (show.season_data || []).forEach(function(s) {
    if (!meta.seasons.some(function(ms) { return ms.number === s.number; })) {
      merged.push(s);
    }
  });
  merged.sort(function(a, b) { return b.number - a.number; });
  return merged;
}

var _shortMonths = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

function _formatDate(dateStr) {
  if (!dateStr) return '';
  var parts = dateStr.split('-');
  if (parts.length !== 3) return dateStr;
  var m = parseInt(parts[1],10);
  var d = parseInt(parts[2],10);
  var y = parseInt(parts[0],10);
  if (isNaN(m) || isNaN(d) || isNaN(y) || m < 1 || m > 12) return dateStr;
  return _shortMonths[m - 1] + ' ' + d + ', ' + y;
}


function _relativeDate(dateStr) {
  if (!dateStr) return '';
  var airMs = new Date(dateStr + 'T00:00:00').getTime();
  if (isNaN(airMs)) return '';
  var now = new Date(); now.setHours(0,0,0,0);
  var diffDays = Math.round((airMs - now.getTime()) / (24*60*60*1000));
  if (diffDays === 0) return '(today)';
  if (diffDays === 1) return '(tomorrow)';
  if (diffDays === -1) return '(yesterday)';
  if (diffDays > 1 && diffDays <= 8) return '(in ' + diffDays + ' days)';
  if (diffDays < -1 && diffDays >= -8) return '(' + Math.abs(diffDays) + ' days ago)';
  return '';
}

function _seasonProgressPill(season, hasPending) {
  if (!season.total_episodes) return '';
  var count = season.episode_count || 0;
  var total = season.total_episodes;
  var cls = 'progress-empty';
  if (hasPending) cls = 'progress-pending';
  else if (count >= total && total > 0) cls = 'progress-complete';
  else if (count > 0) cls = 'progress-partial';
  else cls = 'progress-missing';
  return '<span class="season-progress-pill ' + cls + '">' + count + ' / ' + total + '</span>';
}

function _renderSeasonEpisodes(season, si) {
  var html = '<table class="episode-table"><tbody>';
  var _now = new Date();
  var _todayStr = _now.getFullYear()+'-'+String(_now.getMonth()+1).padStart(2,'0')+'-'+String(_now.getDate()).padStart(2,'0');
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
    if (ep.air_date) {
      var rel = _relativeDate(ep.air_date);
      html += '<span class="ep-date">' + esc(_formatDate(ep.air_date));
      if (rel) html += ' <span class="ep-relative">' + esc(rel) + '</span>';
      html += '</span>';
    }
    if (ep.file) html += '<span class="ep-filename">' + esc(ep.file) + '</span>';
    else if (!ep.title) html += '<span style="color:var(--text3)">&mdash;</span>';
    html += '</td>';
    html += '<td class="ep-source">';
    var isPending = false;
    var isUnavailable = false;
    var isLocalFallback = false;
    if (_detailItem) {
      var pnk = normTitle(_detailItem.title);
      var pendingEntry = _pending[pnk];
      if (pendingEntry && pendingEntry.episodes) {
        var peDir = pendingEntry.direction || '';
        for (var pei = 0; pei < pendingEntry.episodes.length; pei++) {
          if (pendingEntry.episodes[pei].season === season.number && pendingEntry.episodes[pei].episode === ep.number) {
            if (peDir === 'debrid-unavailable') { isUnavailable = true; }
            else if (peDir === 'to-local-fallback') { isLocalFallback = true; }
            else { isPending = true; }
            break;
          }
        }
      }
    }
    var isMigrating = isPending && !isMissing && !!ep.source;
    if (isUnavailable) {
      html += '<span class="badge-unavailable"><span class="badge-full">Debrid N/A</span><span class="badge-mini">\u2715</span></span>';
    } else if (isLocalFallback) {
      html += '<span class="badge-fallback"><span class="badge-full">Local Fallback</span><span class="badge-mini">\u21B3</span></span>';
    } else if (isMigrating) {
      // Episode is available — show source badge + subtle upgrade indicator
      html += buildBadges(ep.source);
      html += '<span class="badge-migrating"><span class="badge-full">Migrating</span><span class="badge-mini">\u2197</span></span>';
    } else if (isPending) {
      // Episode is missing — actively searching
      html += '<span class="badge-pending">Searching</span>';
    } else if (isMissing) {
      if (!ep.air_date) {
        html += '<span class="badge-tba">TBA</span>';
      } else {
        var airMs = new Date(ep.air_date + 'T00:00:00').getTime();
        if (!isNaN(airMs) && airMs > Date.now()) {
          html += '<span class="badge-upcoming">Upcoming</span>';
        } else {
          if (ep.air_date === _todayStr) {
            html += '<span class="badge-airing-today">Airing Today</span>';
          } else {
            html += '<span class="badge-missing"><span class="badge-full">Missing</span><span class="badge-mini">!</span></span>';
          }
        }
      }
    } else {
      html += buildBadges(ep.source);
    }
    html += '</td>';
    html += '<td class="ep-quality">';
    if (!isMissing && ep.file) {
      if (ep.quality && ep.quality.label) html += _qualityBadge(ep.quality);
      var szStr = _formatBytes(ep.size_bytes);
      if (szStr) html += ' <span class="ep-size">' + esc(szStr) + '</span>';
    }
    html += '</td>';
    html += '<td class="ep-actions">';
    if (isUnavailable) {
      html += '<button class="btn btn-ghost btn-sm" aria-label="Download ' + epLabel + ' locally" onclick="_confirmBtn(this,function(){downloadLocalFallback(' + season.number + ',' + ep.number + ')})">Download Locally</button>';
    } else if (isLocalFallback) {
      html += '<button class="btn btn-ghost btn-sm" disabled>Downloading\u2026</button>';
    } else if (isPending) {
      // Searching: disabled placeholder; Migrating: no button (already in-flight)
      if (!isMigrating) html += '<button class="btn btn-ghost btn-sm" disabled>\u2026</button>';
    } else if (_downloadServices.show && _downloadServices.show !== 'overseerr') {
      if (ep.source === 'debrid') {
        html += '<button class="btn btn-ghost btn-sm btn-switch" aria-label="Switch ' + epLabel + ' to Local" onclick="_confirmBtn(this,function(){downloadEp(' + season.number + ',' + ep.number + ',false)})">Switch to Local</button>';
      } else if (ep.source === 'local') {
        html += '<button class="btn btn-ghost btn-sm btn-switch" aria-label="Switch ' + epLabel + ' to Debrid" onclick="_confirmBtn(this,function(){removeEp(' + season.number + ',' + ep.number + ')})">Switch to Debrid</button>';
      } else if (ep.source === 'both') {
        html += '<button class="btn btn-ghost btn-sm btn-switch" aria-label="Switch ' + epLabel + ' to Debrid" onclick="_confirmBtn(this,function(){removeEp(' + season.number + ',' + ep.number + ')})">Switch to Debrid</button>';
      } else if (isMissing && (!ep.air_date || new Date(ep.air_date + 'T00:00:00').getTime() <= Date.now())) {
        html += '<button class="btn btn-ghost btn-sm" aria-label="Search ' + epLabel + '" onclick="_confirmBtn(this,function(){downloadEp(' + season.number + ',' + ep.number + ',true)})">Search</button>';
      }
    }
    if (ep.source === 'debrid' || ep.source === 'both') {
      html += '<button class="btn btn-ghost btn-icon" title="Block this torrent file" aria-label="Block ' + epLabel + '" onclick="event.stopPropagation();_blockItem()">&#128683;</button>';
    }
    if (_searchEnabled && _detailItem && _detailItem.imdb_id) {
      html += ' <button class="btn btn-ghost btn-sm" title="Search torrents for ' + escAttr(epLabel) + '" data-imdb="' + escAttr(_detailItem.imdb_id) + '" data-mtype="series" data-season="' + season.number + '" data-episode="' + ep.number + '" data-label="' + escAttr(_detailItem.title + ' ' + epLabel) + '" onclick="event.stopPropagation();openSearchFromBtn(this)">&#128269;</button>';
    }
    html += '</td>';
    html += '</tr>';
  }
  html += '</tbody></table>';
  if (eps.length > 10) {
    html += '<div class="season-collapse-footer" role="button" tabindex="0" onclick="collapseSeason(this)" onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();collapseSeason(this)}" title="Collapse season">&#9650; Collapse</div>';
  }
  return html;
}

function _shouldAutoExpand(season, showTitle) {
  // Expand seasons with missing episodes being searched (not upgrade-only)
  if (showTitle) {
    var pnk = normTitle(showTitle);
    var pe = _pending[pnk];
    if (pe && pe.episodes) {
      var missingInSeason = {};
      (season.episodes || []).forEach(function(e) {
        if (e.source === 'missing') missingInSeason[e.number] = true;
      });
      for (var i = 0; i < pe.episodes.length; i++) {
        if (pe.episodes[i].season === season.number && missingInSeason[pe.episodes[i].episode]) return true;
      }
    }
  }
  // Expand seasons with recent or upcoming episodes (within 30 days)
  var now = Date.now();
  var thirtyDays = 30 * 24 * 60 * 60 * 1000;
  var eps = season.episodes || [];
  for (var i = 0; i < eps.length; i++) {
    if (eps[i].air_date) {
      var airMs = new Date(eps[i].air_date + 'T00:00:00').getTime();
      if (!isNaN(airMs) && Math.abs(now - airMs) <= thirtyDays) return true;
    }
  }
  // Expand incomplete seasons (have some but not all episodes)
  var count = season.episode_count || 0;
  var total = season.total_episodes || 0;
  if (count > 0 && count < total) return true;
  return false;
}

function _syncExpandAllBtn() {
  var btn = document.querySelector('.expand-all-row .btn');
  if (!btn) return;
  var headers = document.querySelectorAll('.season-header');
  var allExpanded = true;
  for (var i = 0; i < headers.length; i++) {
    if (!headers[i].classList.contains('expanded')) { allExpanded = false; break; }
  }
  btn.textContent = allExpanded ? 'Collapse All' : 'Expand All';
}

function toggleAllSeasons(btn) {
  var headers = document.querySelectorAll('.season-header');
  var anyCollapsed = false;
  for (var i = 0; i < headers.length; i++) {
    if (!headers[i].classList.contains('expanded')) { anyCollapsed = true; break; }
  }
  for (var i = 0; i < headers.length; i++) {
    var ep = headers[i].nextElementSibling;
    if (anyCollapsed) {
      headers[i].classList.add('expanded');
      headers[i].setAttribute('aria-expanded', 'true');
      if (ep) {
        ep.style.display = '';
        // Lazy-render: populate if empty
        if (!ep.querySelector('.episode-table')) {
          var idx = parseInt(ep.getAttribute('data-season-idx'), 10);
          if (!isNaN(idx) && _detailSeasons[idx]) {
            ep.innerHTML = _renderSeasonEpisodes(_detailSeasons[idx], idx);
          }
        }
      }
    } else {
      headers[i].classList.remove('expanded');
      headers[i].setAttribute('aria-expanded', 'false');
      if (ep) ep.style.display = 'none';
    }
  }
  _syncExpandAllBtn();
}

function collapseSeason(footerEl) {
  var section = footerEl.closest('.season-section');
  if (!section) return;
  var header = section.querySelector('.season-header');
  if (header) toggleSeason(header);
}

function _renderShowDetail(show, meta) {
  var area = document.getElementById('content-area');
  var nk = normTitle(show.title);
  var curPref = _preferences[nk] || 'none';
  _savedPref = curPref;
  var seasons = meta ? _mergeShowMeta(show, meta) : (show.season_data || []).slice();
  if (!meta) {
    seasons.sort(function(a, b) { return b.number - a.number; });
    for (var ri = 0; ri < seasons.length; ri++) {
      seasons[ri] = Object.assign({}, seasons[ri]);
      if (seasons[ri].episodes) {
        seasons[ri].episodes = seasons[ri].episodes.slice().sort(function(a, b) { return b.number - a.number; });
      }
    }
  }
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
  html += _backButtonHtml();

  html += '<div class="detail-hero">';
  if (meta && meta.poster_url) {
    html += '<div class="detail-poster"><img src="' + escAttr(meta.poster_url) + '" alt="' + escAttr('Poster for ' + show.title) + '"></div>';
  }
  html += '<div class="detail-info">';
  html += '<h2>' + esc(show.title);
  if (show.year) html += ' <span class="card-year">(' + esc(String(show.year)) + ')</span>';
  if (meta && meta.status) html += '<span class="detail-status">' + esc(meta.status) + '</span>';
  html += '</h2>';
  html += '<div class="card-badges">' + buildBadges(show.source) + '</div>';
  var showPe = _pending[nk];
  var showPeDir = showPe ? (showPe.direction || '') : '';
  var showErrText = (showPeDir === 'to-debrid' || showPeDir === 'to-local') ? formatPendingDetail(showPe) : '';
  if (showErrText) {
    html += '<div class="pending-callout" role="note" aria-label="Search error details">'
          + '<span class="pending-callout-icon">\u26A0</span> '
          + esc(showErrText) + '</div>';
  }
  if (meta) {
    html += _renderDetailMeta(show, meta);
    if (meta.overview) html += '<div class="detail-overview" onclick="this.classList.toggle(\'expanded\')">' + esc(meta.overview) + '</div>';
  }
  if ((show.source === 'debrid' || show.source === 'both') && !_downloadServices.show) {
    html += '<div style="font-size:.82em;color:var(--text3);margin-top:8px">To switch episodes to local, configure <a href="/settings">Sonarr or Overseerr</a> in Settings.</div>';
  }
  html += '<div class="pref-row"><label for="show-pref-select" style="font-size:.82em;color:var(--text2)">Source preference:</label>';
  html += '<select class="pref-select" id="show-pref-select" onchange="onPrefSelectChange(this.value)">';
  html += '<option value="none"' + (curPref === 'none' ? ' selected' : '') + '>No Preference</option>';
  html += '<option value="prefer-local"' + (curPref === 'prefer-local' ? ' selected' : '') + '>Prefer Local</option>';
  html += '<option value="prefer-debrid"' + (curPref === 'prefer-debrid' ? ' selected' : '') + '>Prefer Debrid</option>';
  html += '</select>';
  html += '<button class="btn btn-primary" id="show-pref-apply-btn" style="display:none" onclick="applyPreference()">Apply</button>';
  html += '</div>';
  html += '<div style="font-size:.75em;color:var(--text3);margin-top:2px;line-height:1.5"><strong style="color:var(--text2)">Prefer Local</strong> &mdash; switches debrid-only episodes to local copies.<br><strong style="color:var(--text2)">Prefer Debrid</strong> &mdash; removes local copies and streams from debrid.</div>';
  var showActionBtns = [];
  if (show.source === 'debrid' || show.source === 'both') {
    showActionBtns.push('<button class="btn btn-ghost btn-icon" title="Block this torrent file" onclick="event.stopPropagation();_blockItem()">&#128683;</button>');
  }
  if (_downloadServices.show === 'sonarr') {
    showActionBtns.push('<button class="btn btn-ghost btn-sm btn-danger" title="Delete from Sonarr" onclick="event.stopPropagation();_confirmBtn(this,function(){deleteItem(\'show\')})">&#128465; Delete</button>');
  }
  if (_searchEnabled && show.imdb_id) {
    showActionBtns.push('<button class="btn btn-ghost btn-sm" data-imdb="' + escAttr(show.imdb_id) + '" data-mtype="series" data-label="' + escAttr(show.title) + '" onclick="openSearchFromBtn(this)">&#128269; Search Torrents</button>');
  }
  if (showActionBtns.length) {
    html += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">' + showActionBtns.join('') + '</div>';
  }
  html += '</div></div>';

  html += '<div class="detail-body"><div class="detail-main">';
  html += _renderCastSection(meta);
  if (seasons.length > 1) {
    var allExpanded = hasPrev && seasons.every(function(s) { return !!expandedNums[String(s.number)]; });
    html += '<div class="expand-all-row"><button class="btn btn-ghost btn-sm" onclick="toggleAllSeasons(this)">' + (allExpanded ? 'Collapse All' : 'Expand All') + '</button></div>';
  }

  for (var si = 0; si < seasons.length; si++) {
    var season = seasons[si];
    var expanded = hasPrev ? !!expandedNums[String(season.number)] : _shouldAutoExpand(season, show.title);
    var hasDebrid = false, hasLocal = false, hasMissing = false, debridCount = 0, missingCount = 0;
    for (var ci = 0; ci < (season.episodes || []).length; ci++) {
      if (season.episodes[ci].source === 'debrid') { hasDebrid = true; debridCount++; }
      if (season.episodes[ci].source === 'local' || season.episodes[ci].source === 'both') hasLocal = true;
      if (season.episodes[ci].source === 'missing') { hasMissing = true; missingCount++; }
    }
    // Season is "pending" (orange pill) only if it has missing episodes
    // that are being searched for.  Available episodes being upgraded to
    // a preferred source don't warrant the alarming pending style.
    var seasonPending = false;
    if (hasMissing && _detailItem) {
      var pnk = normTitle(_detailItem.title);
      var pe = _pending[pnk];
      if (pe && pe.episodes) {
        var missingEps = {};
        for (var mi = 0; mi < (season.episodes || []).length; mi++) {
          if (season.episodes[mi].source === 'missing') missingEps[season.episodes[mi].number] = true;
        }
        for (var spi = 0; spi < pe.episodes.length; spi++) {
          if (pe.episodes[spi].season === season.number && missingEps[pe.episodes[spi].episode]) {
            seasonPending = true; break;
          }
        }
      }
    }
    var progressPill = _seasonProgressPill(season, seasonPending);
    html += '<div class="season-section">';
    html += '<div class="season-header' + (expanded ? ' expanded' : '') + '" data-season="' + escAttr(String(season.number)) + '" tabindex="0" role="button" aria-expanded="' + expanded + '" onclick="toggleSeason(this)" onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();toggleSeason(this)}">';
    html += '<span class="season-chevron">&#9654;</span>';
    html += 'Season ' + esc(String(season.number)) + ' &mdash; ' + esc(String(season.episode_count)) + ' episode' + (season.episode_count !== 1 ? 's' : '') + progressPill;
    if (seasonPending) html += '<span class="ping-dot"></span>';
    html += '<span class="season-actions">';
    if ((hasDebrid || hasMissing) && _downloadServices.show) {
      if (_downloadServices.show === 'overseerr') {
        html += '<button class="btn btn-ghost btn-sm" onclick="event.stopPropagation();_confirmBtn(this,function(){requestSeason(' + season.number + ')})">Request Season</button>';
      } else {
        if (hasDebrid) {
          var dlLabel = 'Switch ' + debridCount + ' Episode' + (debridCount !== 1 ? 's' : '') + ' to Local';
          html += '<button class="btn btn-ghost btn-sm btn-switch" onclick="event.stopPropagation();_confirmBtn(this,function(){dlSeason(' + si + ')})">' + dlLabel + '</button>';
        }
        if (hasMissing) {
          var searchLabel = 'Search ' + missingCount + ' Missing';
          html += '<button class="btn btn-ghost btn-sm" onclick="event.stopPropagation();_confirmBtn(this,function(){searchMissingSeason(' + si + ')})">' + searchLabel + '</button>';
        }
      }
    }
    if (hasLocal && _downloadServices.show && _downloadServices.show !== 'overseerr') {
      var localCount = 0;
      for (var lci = 0; lci < season.episodes.length; lci++) {
        if (season.episodes[lci].source === 'local' || season.episodes[lci].source === 'both') localCount++;
      }
      var rmLabel = 'Switch ' + localCount + ' to Debrid';
      html += '<button class="btn btn-ghost btn-sm btn-switch" onclick="event.stopPropagation();_confirmBtn(this,function(){rmSeason(' + si + ')})">' + rmLabel + '</button>';
    }
    html += '</span>';
    html += '</div>';
    html += '<div class="season-episodes" data-season-idx="' + si + '"' + (expanded ? '' : ' style="display:none"') + '>';
    if (!expanded) {
      // Lazy-render: defer episode table until first expand
      html += '</div></div>';
      continue;
    }
    html += _renderSeasonEpisodes(season, si);
    html += '</div></div>';
  }

  html += '<div id="transfer-msg" aria-live="polite"></div>';
  html += '<div class="history-sidebar" id="history-sidebar-content"><h3>Activity</h3><div class="hs-empty">Loading\u2026</div></div>';
  html += '</div>';
  html += '</div>';
  html += '</div>';
  area.innerHTML = html;
  _loadHistorySidebar();
}

function hideDetail() {
  _closeBlockModal();
  _inDetailView = false;
  _detailOrigin = null;
  _detailItem = null;
  _historySidebarGen = 0;
  _detailSeasons = [];
  _actionInFlight = false;
  _stopSmartPoll();
  if (_refreshTimer) { clearTimeout(_refreshTimer); _refreshTimer = null; }
  if (_pendingConfirmCleanup) { _pendingConfirmCleanup(); _pendingConfirmCleanup = null; }
  document.title = 'pd_zurg Library';
  var _hUrl = new URL(window.location);
  _hUrl.searchParams.delete('detail');
  _hUrl.searchParams.delete('type');
  _hUrl.searchParams.delete('from');
  history.replaceState(null, '', _hUrl);
  document.querySelector('.tabs').style.display = '';
  document.querySelector('.controls').style.display = '';
  document.getElementById('wanted-presets').style.display = '';
  document.getElementById('footer').style.display = '';
  applyFilters();
  _updateWantedUI();
  // Restore select mode UI after grid re-render
  if (_selectMode) {
    var area = document.getElementById('content-area');
    if (area) area.classList.add('select-mode');
    _updateBulkBar();
  }
  // Focus before scrollTo so that any focus-triggered scroll (legacy browsers
  // that ignore the preventScroll option) is immediately overridden.
  // applyFilters() rebuilds #content-area synchronously so document height is
  // already correct; the browser clamps scrollY to the new max if the filtered
  // set is shorter.
  var _si = document.getElementById('search-input');
  if (_si) _si.focus({preventScroll: true});
  window.scrollTo(0, _librarySavedScrollY);
}

// Excluded from detail sidebar: system-level events, startup-skip events
var _HS_EXCLUDE = {task_completed:1, cleanup:1, repair:1, blocklisted:1};
var _HS_CATEGORIES = {
  grabbed:'acquisition', cached:'acquisition', symlink_created:'acquisition', debrid_add:'acquisition',
  failed:'failure', debrid_add_failed:'failure', symlink_failed:'failure', debrid_unavailable:'failure',
  search_triggered:'action', rescan_triggered:'action', local_fallback_triggered:'action',
  switched_source:'management', arr_deleted:'management', blocklist_added:'management',
  task_completed:'management', cleanup:'management', repair:'action', blocklisted:'management'
};
var _HS_ICONS = {
  grabbed:'\u2B07', cached:'\u2705', symlink_created:'\uD83D\uDD17', debrid_add:'\u2601',
  failed:'\u274C', debrid_add_failed:'\u274C', symlink_failed:'\u274C', debrid_unavailable:'\u26A0',
  search_triggered:'\uD83D\uDD0D', rescan_triggered:'\uD83D\uDD04', local_fallback_triggered:'\u21B3',
  switched_source:'\u21C4', arr_deleted:'\uD83D\uDDD1', blocklist_added:'\uD83D\uDEAB',
  task_completed:'\u2699', cleanup:'\uD83D\uDDD1', repair:'\uD83D\uDD27', blocklisted:'\uD83D\uDEAB'
};
var _HS_LABELS = {
  grabbed:'Grabbed', cached:'Cached', symlink_created:'Symlinked', debrid_add:'Added to Debrid',
  failed:'Failed', debrid_add_failed:'Add Failed', symlink_failed:'Symlink Failed', debrid_unavailable:'Debrid N/A',
  search_triggered:'Search', rescan_triggered:'Rescan', local_fallback_triggered:'Local Fallback',
  switched_source:'Source Switch', arr_deleted:'Deleted', blocklist_added:'Blocklisted',
  task_completed:'Task Done', cleanup:'Cleanup', repair:'Repair', blocklisted:'Skipped'
};

function _loadHistorySidebar() {
  var container = document.getElementById('history-sidebar-content');
  if (!container || !_detailItem) return;
  if (container.querySelector('.hs-timeline')) return;
  var gen = ++_historySidebarGen;
  var title = encodeURIComponent(_detailItem.title);
  fetch('/api/history/show/' + title + '?limit=30')
    .then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
    .then(function(events) {
      if (gen !== _historySidebarGen) return;
      var c = document.getElementById('history-sidebar-content');
      if (c) _renderHistorySidebar(c, events);
    })
    .catch(function() {
      if (gen !== _historySidebarGen) return;
      var c = document.getElementById('history-sidebar-content');
      if (c) c.innerHTML = '<h3>Activity</h3><div class="hs-empty" style="color:var(--red)">Failed to load</div>';
    });
}

function _renderHistorySidebar(container, events) {
  var filtered = [];
  for (var i = 0; i < events.length; i++) {
    if (!_HS_EXCLUDE[events[i].type]) filtered.push(events[i]);
  }
  if (!filtered.length) {
    container.innerHTML = '<h3>Activity</h3><div class="hs-empty">No activity recorded yet</div>';
    return;
  }
  var groups = [];
  var currentDay = null;
  for (var i = 0; i < filtered.length; i++) {
    var day = _historyDayLabel(filtered[i].ts);
    if (day !== currentDay) {
      groups.push({label: day, events: []});
      currentDay = day;
    }
    groups[groups.length - 1].events.push(filtered[i]);
  }
  var h = '<h3>Activity</h3><div class="hs-timeline">';
  for (var g = 0; g < groups.length; g++) {
    h += '<div class="hs-day-group"><div class="hs-day-label">' + esc(groups[g].label) + '</div>';
    for (var j = 0; j < groups[g].events.length; j++) {
      var ev = groups[g].events[j];
      var cat = _HS_CATEGORIES[ev.type] || 'management';
      var icon = _HS_ICONS[ev.type] || '\u2022';
      var label = _HS_LABELS[ev.type] || ev.type.replace(/_/g, ' ');
      h += '<div class="hs-event hs-cat-' + escAttr(cat) + '">';
      h += '<span class="hs-event-icon">' + esc(icon) + '</span>';
      h += '<span class="hs-event-type">' + esc(label) + '</span>';
      if (ev.episode) h += '<span class="hs-event-episode">' + esc(ev.episode) + '</span>';
      h += ' <span class="hs-event-time">' + esc(_timeAgoHistory(ev.ts)) + '</span>';
      if (ev.detail) h += '<div class="hs-event-detail">' + esc(ev.detail) + '</div>';
      h += '</div>';
    }
    h += '</div>';
  }
  h += '</div>';
  container.innerHTML = h;
}

function _historyDayLabel(ts) {
  var d = new Date(ts);
  var now = new Date();
  var today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  var eventDay = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  var diff = Math.floor((today - eventDay) / 86400000);
  if (diff <= 0) return 'Today';
  if (diff === 1) return 'Yesterday';
  if (diff < 7) return diff + ' days ago';
  return d.toLocaleDateString(undefined, {month: 'short', day: 'numeric', year: 'numeric'});
}

function _timeAgoHistory(ts) {
  var sec = Math.max(0, Math.floor((Date.now() - new Date(ts).getTime()) / 1000));
  if (isNaN(sec)) return '';
  if (sec < 60) return sec + 's ago';
  if (sec < 3600) return Math.floor(sec / 60) + 'm ago';
  if (sec < 86400) return Math.floor(sec / 3600) + 'h ago';
  return Math.floor(sec / 86400) + 'd ago';
}

function toggleSeason(headerEl) {
  var episodes = headerEl.nextElementSibling;
  var isExpanded = headerEl.classList.toggle('expanded');
  headerEl.setAttribute('aria-expanded', isExpanded ? 'true' : 'false');
  episodes.style.display = isExpanded ? '' : 'none';
  // Lazy-render: populate episode table on first expand
  if (isExpanded && !episodes.querySelector('.episode-table')) {
    var idx = parseInt(episodes.getAttribute('data-season-idx'), 10);
    if (!isNaN(idx) && _detailSeasons[idx]) {
      episodes.innerHTML = _renderSeasonEpisodes(_detailSeasons[idx], idx);
    }
  }
  _syncExpandAllBtn();
}

// ---------------------------------------------------------------------------
// Preference & action API calls
// ---------------------------------------------------------------------------
var _savedPref = 'none';  // tracks saved pref to detect changes

function _getPrefElements() {
  // Show and movie detail views use distinct IDs; only one is rendered at a time
  var sel = document.getElementById('show-pref-select') || document.getElementById('movie-pref-select');
  var btn = document.getElementById('show-pref-apply-btn') || document.getElementById('movie-pref-apply-btn');
  return {sel: sel, btn: btn};
}

function onPrefSelectChange(pref) {
  var els = _getPrefElements();
  if (els.btn) els.btn.style.display = (pref !== _savedPref) ? '' : 'none';
}

function applyPreference() {
  if (!_detailItem || _actionInFlight) return;
  var sel = _getPrefElements().sel;
  if (!sel) return;
  var pref = sel.value;
  var nk = normTitle(_detailItem.title);
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  var seasons = _detailSeasons || [];
  var showSvc = _downloadServices.show;
  // Overseerr cannot remove files — only Sonarr/Radarr can
  var canRemove = showSvc && showSvc !== 'overseerr';

  if (pref === 'prefer-local' && showSvc) {
    // Collect debrid-only episodes (need download) and both-source episodes (need debrid removal)
    var dlTasks = [];
    var dlPendingEps = [];
    var totalDlEps = 0;
    var totalBothEps = 0;
    var isOverseerr = showSvc === 'overseerr';
    for (var si = 0; si < seasons.length; si++) {
      var eps = [];
      for (var ei = 0; ei < seasons[si].episodes.length; ei++) {
        var epSrc = seasons[si].episodes[ei].source;
        if (epSrc === 'debrid') { eps.push(seasons[si].episodes[ei].number); totalDlEps++; }
        else if (epSrc === 'both') { totalBothEps++; }
      }
      if (eps.length) {
        (function(sNum, epList) {
          dlTasks.push(function() {
            var payload = {title: _detailItem.title, type: 'show', tmdb_id: tmdbId, season: sNum, prefer_debrid: false};
            payload.episodes = isOverseerr ? [] : epList;
            return _postDownload(payload);
          });
          for (var pe = 0; pe < epList.length; pe++) {
            dlPendingEps.push({season: sNum, episode: epList[pe]});
          }
        })(seasons[si].number, eps);
      }
    }
    if (totalDlEps === 0 && totalBothEps === 0) {
      _savePref(nk, pref);
      _showMsg('All episodes already local. Preference saved.', 'success');
      return;
    }
    // Case 1: only debrid-only episodes (no both) — download them
    if (totalDlEps > 0 && totalBothEps === 0) {
      var svcLabel = _svcNames[showSvc] || showSvc;
      if (!confirm(isOverseerr
        ? 'Request ' + dlTasks.length + ' season(s) in Overseerr?'
        : 'Switch ' + totalDlEps + ' episode(s) to local via ' + svcLabel + '?')) return;
      _runSequential(dlTasks).then(function(ok) {
        if (ok) {
          _savePref(nk, pref);
          if (dlPendingEps.length) _setPending(_detailItem.title, dlPendingEps, 'to-local');
        }
      });
      return;
    }
    // Case 2: only both-source episodes — remove debrid copies
    if (totalDlEps === 0 && totalBothEps > 0) {
      _postRemoveDebrid(_detailItem.title, _detailItem.year).then(function(ok) {
        if (ok) _savePref(nk, pref);
      }).catch(function(e) {
        _showMsg('Operation failed: ' + e, 'error');
      });
      return;
    }
    // Case 3: mixed — download debrid-only, then remove debrid for both-source
    var svcLabel2 = _svcNames[showSvc] || showSvc;
    if (!confirm('Switch ' + totalDlEps + ' episode(s) to local via ' + svcLabel2
      + ' and remove ' + totalBothEps + ' debrid duplicate(s)?')) return;
    _runSequential(dlTasks).then(function(ok) {
      if (!ok) return false;
      return _postRemoveDebrid(_detailItem.title, _detailItem.year);
    }).then(function(ok) {
      if (ok) _savePref(nk, pref);
    }).catch(function(e) {
      _showMsg('Operation failed: ' + e, 'error');
    });

  } else if (pref === 'prefer-debrid') {
    // Collect episodes by current source state
    var switchEps = [];    // source='both' -> switch to symlink now
    var localOnlyEps = []; // source='local' -> need debrid search
    var missingEps = [];   // source='missing' -> need debrid search
    var totalSwitchable = 0;
    var totalLocalOnly = 0;
    var totalMissing = 0;
    for (var si2 = 0; si2 < seasons.length; si2++) {
      for (var ei2 = 0; ei2 < seasons[si2].episodes.length; ei2++) {
        var src = seasons[si2].episodes[ei2].source;
        if (src === 'both') {
          switchEps.push({season: seasons[si2].number, episode: seasons[si2].episodes[ei2].number});
          totalSwitchable++;
        } else if (src === 'local') {
          localOnlyEps.push({season: seasons[si2].number, episode: seasons[si2].episodes[ei2].number});
          totalLocalOnly++;
        } else if (src === 'missing') {
          missingEps.push({season: seasons[si2].number, episode: seasons[si2].episodes[ei2].number});
          totalMissing++;
        }
      }
    }
    // Combine local-only and missing into episodes that need a debrid search
    var searchEps = localOnlyEps.concat(missingEps);
    var totalSearchable = totalLocalOnly + totalMissing;

    if (totalSwitchable === 0 && totalSearchable === 0) { _savePref(nk, pref); return; }

    // Helper: trigger debrid searches for episodes grouped by season
    var _searchForDebrid = function(capturedTitle, capturedTmdbId, eps, onDone) {
      if (!eps.length) { if (onDone) onDone(true); return; }
      var bySeason = {};
      for (var li = 0; li < eps.length; li++) {
        var sn = eps[li].season;
        if (!bySeason[sn]) bySeason[sn] = [];
        bySeason[sn].push(eps[li].episode);
      }
      var tasks = [];
      Object.keys(bySeason).forEach(function(sn) {
        tasks.push(function() {
          return _postDownload({
            title: capturedTitle, type: 'show', tmdb_id: capturedTmdbId,
            season: parseInt(sn), episodes: bySeason[sn], prefer_debrid: true
          });
        });
      });
      _runSequential(tasks).then(function(ok) {
        if (onDone) onDone(ok);
      }).catch(function() {
        if (onDone) onDone(false);
      });
    };

    if (totalSwitchable === 0) {
      // No episodes can switch now — save pref and search for debrid copies
      var capturedTitle = _detailItem.title;
      var capturedTmdbId = tmdbId;
      _savePref(nk, pref).then(function(saved) {
        if (!saved) { _showMsg('Failed to save preference.', 'error'); return; }
        _setPending(capturedTitle, searchEps, 'to-debrid');
        _searchForDebrid(capturedTitle, capturedTmdbId, searchEps, function(ok) {
          if (ok) {
            _showMsg('Preference saved. Searching for debrid copies of ' + totalSearchable + ' episode(s).', 'success');
          } else {
            _showMsg('Preference saved but search failed for some episodes.', 'error');
          }
          _scheduleRefresh(1000);
        });
      });
      return;
    }

    // Mixed case: some episodes can switch now, others need searching
    var confirmMsg2 = 'Switch ' + totalSwitchable + ' episode(s) to debrid streaming?'
      + '\n\nLocal files will be removed. Playback will stream from your debrid service instead.';
    if (totalSearchable > 0) confirmMsg2 += '\n\n' + totalSearchable + ' additional episode(s) will be searched for debrid copies.';
    if (!confirm(confirmMsg2)) return;
    _actionInFlight = true;
    _setActionsDisabled(true);
    _showMsgHtml('<span class="scanning-dot"></span>Switching to debrid...');
    var capturedTitle2 = _detailItem.title;
    var capturedTmdbId2 = tmdbId;
    fetch('/api/library/switch-to-debrid', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({title: capturedTitle2, episodes: switchEps})
    }).then(function(r) {
      return r.json().then(function(d) { return {ok: r.ok, d: d}; });
    }).then(function(res) {
      if (res.ok && res.d.switched > 0) {
        // Clear action-in-flight before triggering searches (which use _postDownload)
        // Keep buttons disabled until the full chain completes
        _actionInFlight = false;
        _savePref(nk, pref).then(function(saved) {
          if (!saved) { _showMsg('Switched ' + res.d.switched + ' episode(s) but failed to save preference.', 'error'); return; }
          if (searchEps.length) {
            _setPending(capturedTitle2, searchEps, 'to-debrid');
            _searchForDebrid(capturedTitle2, capturedTmdbId2, searchEps, function(ok) {
              if (ok) {
                _showMsg('Switched ' + res.d.switched + ' episode(s). Searching for debrid copies of ' + totalSearchable + ' more.', 'success');
              } else {
                _showMsg('Switched ' + res.d.switched + ' episode(s) but search failed for remaining.', 'error');
              }
              _scheduleRefresh(1000);
              _setActionsDisabled(false);
            });
          } else {
            _showMsg('Switched ' + res.d.switched + ' episode(s) to debrid streaming.', 'success');
            _scheduleRefresh(1000);
            _setActionsDisabled(false);
          }
        }).catch(function() { _setActionsDisabled(false); });
      } else {
        _showMsg('Error: ' + (res.d.error || res.d.message || 'Switch failed'), 'error');
      }
    }).catch(function(e) {
      _showMsg('Switch failed: ' + e, 'error');
    }).finally(function() {
      // Only clear if not already cleared by the success path above
      if (_actionInFlight) {
        _actionInFlight = false;
        _setActionsDisabled(false);
      }
    });

  } else {
    _savePref(nk, pref);
    _showMsg('Preference saved.', 'success');
  }
}

function _savePref(nk, pref) {
  return fetch('/api/library/preference', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({title: nk, preference: pref})
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (pref === 'none') { delete _preferences[nk]; }
    else { _preferences[nk] = pref; }
    _savedPref = pref;
    var btn = _getPrefElements().btn;
    if (btn) btn.style.display = 'none';
    return true;
  }).catch(function(e) { showToast('Failed to save preference: ' + e, 'error'); return false; });
}

function downloadEp(season, episode, preferDebrid) {
  if (!_detailItem) return;
  var itemTitle = _detailItem.title;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  var payload = {
    title: itemTitle, type: 'show', tmdb_id: tmdbId,
    season: season, episodes: [episode]
  };
  if (preferDebrid !== undefined) payload.prefer_debrid = preferDebrid;
  _postDownload(payload).then(function(ok) {
    if (ok) {
      var dir = preferDebrid === false ? 'to-local' : 'to-debrid';
      _setPending(itemTitle, [{season: season, episode: episode}], dir);
      _scheduleRefresh(1000);
    }
  });
}

function downloadLocalFallback(season, episode) {
  if (!_detailItem) return;
  var itemTitle = _detailItem.title;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  var payload = {
    title: itemTitle, type: _detailItem.type, tmdb_id: tmdbId,
    season: season, episodes: [episode]
  };
  if (_actionInFlight) return;
  _actionInFlight = true;
  _setActionsDisabled(true);
  _showMsgHtml('<span class="scanning-dot"></span>Sending local fallback search...');
  fetch('/api/library/download-local-fallback', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r) {
    return r.json().then(function(d) { return {ok: r.ok, d: d}; });
  }).then(function(res) {
    var d = res.d;
    var errMsg = (!res.ok || d.status === 'error') ? (d.error || d.message || 'Unknown error') : null;
    if (errMsg) { _showMsg('Error: ' + errMsg, 'error'); }
    else {
      _showMsg(d.message || 'Local search triggered.', 'success');
      _setPending(itemTitle, [{season: season, episode: episode}], 'to-local-fallback');
      _scheduleRefresh(1000);
    }
  }).catch(function(e) {
    _showMsg('Network error: ' + e, 'error');
  }).finally(function() {
    _actionInFlight = false;
    _setActionsDisabled(false);
  });
}

function downloadMovieLocalFallback() {
  if (!_detailItem || _detailItem.type !== 'movie') return;
  var itemTitle = _detailItem.title;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  var payload = { title: itemTitle, type: 'movie', tmdb_id: tmdbId };
  if (_actionInFlight) return;
  _actionInFlight = true;
  _setActionsDisabled(true);
  _showMsgHtml('<span class="scanning-dot"></span>Sending local fallback search...');
  fetch('/api/library/download-local-fallback', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r) {
    return r.json().then(function(d) { return {ok: r.ok, d: d}; });
  }).then(function(res) {
    var d = res.d;
    var errMsg = (!res.ok || d.status === 'error') ? (d.error || d.message || 'Unknown error') : null;
    if (errMsg) { _showMsg('Error: ' + errMsg, 'error'); }
    else {
      _showMsg(d.message || 'Local search triggered.', 'success');
      _setPending(itemTitle, [{season: 0, episode: 0}], 'to-local-fallback');
      _scheduleRefresh(1000);
    }
  }).catch(function(e) {
    _showMsg('Network error: ' + e, 'error');
  }).finally(function() {
    _actionInFlight = false;
    _setActionsDisabled(false);
  });
}

function removeEp(season, episode) {
  if (!_detailItem) return;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  _postRemove({
    title: _detailItem.title, type: _detailItem.type, tmdb_id: tmdbId,
    season: season, episodes: [episode]
  });
}

function _blockItem() {
  if (!_detailItem) return;
  if (document.getElementById('bl-overlay')) return;
  var reasons = ['Wrong content', 'Corrupt file', 'Wrong language', 'Low quality', 'Other'];
  var overlay = document.createElement('div');
  overlay.className = 'search-overlay';
  overlay.id = 'bl-overlay';
  var html = '<div class="bl-dialog">';
  html += '<div class="bl-dialog-hdr"><h3>Block: ' + esc(_detailItem.title) + '</h3>';
  html += '<button class="search-dialog-close" onclick="_closeBlockModal()" title="Close">&times;</button></div>';
  html += '<div class="bl-dialog-body">';
  html += '<div class="bl-reasons">';
  for (var i = 0; i < reasons.length; i++) {
    html += '<button class="bl-reason-btn" data-reason="' + escAttr(reasons[i]) + '" onclick="_selectBlockReason(this)">' + (i + 1) + '. ' + esc(reasons[i]) + '</button>';
  }
  html += '</div>';
  html += '<label class="bl-custom-label" for="bl-custom-input">Or enter a custom reason:</label>';
  html += '<input class="bl-custom-input" id="bl-custom-input" type="text" placeholder="Custom reason...">';
  html += '</div>';
  html += '<div class="bl-dialog-footer">';
  html += '<button class="btn" onclick="_closeBlockModal()">Cancel</button>';
  html += '<button class="btn btn-primary" onclick="_confirmBlock()">Confirm</button>';
  html += '</div></div>';
  overlay.innerHTML = html;
  overlay.onclick = function(e) { if (e.target === overlay) _closeBlockModal(); };
  document.body.appendChild(overlay);
  document.body.style.overflow = 'hidden';
  var inp = overlay.querySelector('#bl-custom-input');
  if (inp) {
    inp.addEventListener('keydown', function(e) { if (e.key === 'Enter') _confirmBlock(); });
    inp.addEventListener('input', function() {
      if (inp.value) { var btns = document.querySelectorAll('.bl-reason-btn'); for (var i = 0; i < btns.length; i++) btns[i].classList.remove('selected'); }
    });
  }
  document.addEventListener('keydown', _blockModalKeyHandler);
}

function _blockModalKeyHandler(e) {
  if (e.key === 'Escape') { e.stopImmediatePropagation(); e.preventDefault(); _closeBlockModal(); }
}

function _selectBlockReason(btn) {
  var btns = document.querySelectorAll('.bl-reason-btn');
  for (var i = 0; i < btns.length; i++) btns[i].classList.remove('selected');
  btn.classList.add('selected');
  var inp = document.getElementById('bl-custom-input');
  if (inp) inp.value = '';
}

function _closeBlockModal() {
  var overlay = document.getElementById('bl-overlay');
  if (overlay) { overlay.remove(); document.body.style.overflow = ''; }
  document.removeEventListener('keydown', _blockModalKeyHandler);
}

function _confirmBlock() {
  if (!_detailItem) { _closeBlockModal(); return; }
  var reason = '';
  var selected = document.querySelector('.bl-reason-btn.selected');
  var customVal = (document.getElementById('bl-custom-input') || {}).value || '';
  if (customVal.trim()) {
    reason = customVal.trim();
  } else if (selected) {
    reason = selected.getAttribute('data-reason');
  } else {
    reason = 'Blocked from library';
  }
  var title = _detailItem.title;
  _closeBlockModal();
  fetch('/api/blocklist', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({title: title, reason: reason})
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (d.status === 'added') {
      _showMsg('Blocklisted: ' + title, 'success');
    } else {
      _showMsg('Failed to blocklist: ' + (d.error || ''), 'error');
    }
  }).catch(function() { _showMsg('Failed to blocklist', 'error'); });
}

function deleteItem(mediaType) {
  if (!_detailItem) return;
  var svc = mediaType === 'movie' ? 'Radarr' : 'Sonarr';
  if (!_detailMeta || !_detailMeta.tmdb_id) {
    _showMsg('Waiting for metadata to load — please try again in a moment.', 'error');
    return;
  }
  _actionInFlight = true;
  _setActionsDisabled(true);
  _showMsgHtml('<span class="scanning-dot"></span>Deleting from ' + svc + ' and cleaning up...');
  var titleCopy = _detailItem.title;
  fetch('/api/library/delete', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({title: titleCopy, type: mediaType, tmdb_id: _detailMeta.tmdb_id, delete_debrid: true, year: _detailItem.year || null})
  }).then(function(r) {
    return r.json().then(function(d) { return {ok: r.ok, d: d}; });
  }).then(function(res) {
    if (res.ok && res.d.status === 'deleted') {
      hideDetail();
      var parts = ['Deleted ' + titleCopy + ' from ' + svc];
      var c = res.d.cleanup || {};
      if (c.debrid_torrents_removed) parts.push(c.debrid_torrents_removed + ' debrid torrent(s) removed');
      if (c.symlinks_removed) parts.push(c.symlinks_removed + ' symlink dir(s) cleaned');
      _showMsg(parts.join(' \u2014 '), 'success');
      fetchLibrary();
    } else {
      _showMsg('Failed: ' + (res.d.error || res.d.message || 'Unknown error'), 'error');
    }
  }).catch(function(e) {
    _showMsg('Delete failed: ' + e, 'error');
  }).finally(function() {
    _actionInFlight = false;
    _setActionsDisabled(false);
  });
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
  var pendingEps = eps.map(function(e) { return {season: season.number, episode: e}; });
  var itemTitle = _detailItem.title;
  _postDownload({
    title: itemTitle, type: 'show', tmdb_id: tmdbId,
    season: season.number, episodes: eps, prefer_debrid: false
  }).then(function(ok) {
    if (ok) { _setPending(itemTitle, pendingEps, 'to-local'); _scheduleRefresh(1000); }
  });
}

function searchMissingSeason(seasonIdx) {
  if (!_detailItem || !_detailSeasons[seasonIdx]) return;
  var season = _detailSeasons[seasonIdx];
  var eps = [];
  for (var i = 0; i < season.episodes.length; i++) {
    if (season.episodes[i].source === 'missing') {
      eps.push(season.episodes[i].number);
    }
  }
  if (!eps.length) return;
  var itemTitle = _detailItem.title;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  var nk = normTitle(itemTitle);
  var pref = _preferences[nk] || 'none';
  var dir = pref === 'prefer-local' ? 'to-local' : 'to-debrid';
  var pendingEps = eps.map(function(e) { return {season: season.number, episode: e}; });
  _postDownload({
    title: itemTitle, type: 'show', tmdb_id: tmdbId,
    season: season.number, episodes: eps
  }).then(function(ok) {
    if (ok) { _setPending(itemTitle, pendingEps, dir); _scheduleRefresh(1000); }
  });
}

function requestSeason(seasonNumber) {
  if (!_detailItem) return;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  var nk = normTitle(_detailItem.title);
  var pref = _preferences[nk] || 'none';
  var payload = {
    title: _detailItem.title, type: 'show', tmdb_id: tmdbId,
    season: seasonNumber, episodes: []
  };
  if (pref === 'prefer-debrid') payload.prefer_debrid = true;
  else if (pref === 'prefer-local') payload.prefer_debrid = false;
  _postDownload(payload).then(function(ok) {
    if (ok) _scheduleRefresh(2000);
  });
}

function downloadMovie(preferDebrid) {
  if (!_detailItem) return;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  var payload = {
    title: _detailItem.title, type: 'movie', tmdb_id: tmdbId
  };
  if (preferDebrid !== undefined) payload.prefer_debrid = preferDebrid;
  _postDownload(payload);
}

function removeMovie() {
  if (!_detailItem || _actionInFlight) return;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  var nk = normTitle(_detailItem.title);

  if (_detailItem.source === 'local') {
    // No debrid copy yet — save preference and search via Radarr
    var capturedTitle = _detailItem.title;
    _savePref(nk, 'prefer-debrid').then(function(saved) {
      if (!saved) { _showMsg('Failed to save preference.', 'error'); return; }
      _setPending(capturedTitle, [{season: 0, episode: 0}], 'to-debrid');
      _postDownload({
        title: capturedTitle, type: 'movie', tmdb_id: tmdbId,
        prefer_debrid: true
      }).then(function(ok) {
        if (ok) {
          _showMsg('Preference saved. Searching for debrid copy.', 'success');
        } else {
          _showMsg('Preference saved but search failed.', 'error');
        }
        _scheduleRefresh(1000);
      });
    });
  } else {
    // source=both — debrid copy exists, remove local file
    var oldPref = _savedPref;
    var capturedBothTitle = _detailItem.title;
    _actionInFlight = true;
    _setActionsDisabled(true);
    _showMsgHtml('<span class="scanning-dot"></span>Switching to debrid...');
    _savePref(nk, 'prefer-debrid').then(function(saved) {
      if (!saved) { _showMsg('Failed to save preference.', 'error'); return; }
      _actionInFlight = false;
      return _postRemove({
        title: capturedBothTitle, type: 'movie', tmdb_id: tmdbId,
        episodes: []
      }).then(function(ok) {
        if (!ok) { _savePref(nk, oldPref); }
        else { _showMsg('Switched to debrid streaming.', 'success'); }
        _scheduleRefresh(1000);
      });
    }).catch(function(e) {
      _showMsg('Operation failed: ' + e, 'error');
    }).finally(function() {
      _actionInFlight = false;
      _setActionsDisabled(false);
    });
  }
}

function applyMoviePreference() {
  if (!_detailItem || _actionInFlight) return;
  var sel = _getPrefElements().sel;
  if (!sel) return;
  var pref = sel.value;
  var nk = normTitle(_detailItem.title);
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  var movieSvc = _downloadServices.movie;

  if (pref === 'prefer-local' && movieSvc && _detailItem.source === 'debrid') {
    var svcLabel = _svcNames[movieSvc] || movieSvc;
    if (!confirm('Switch ' + _detailItem.title + ' to local via ' + svcLabel + '?')) return;
    _postDownload({
      title: _detailItem.title, type: 'movie', tmdb_id: tmdbId,
      prefer_debrid: false
    }).then(function(ok) { if (ok) _savePref(nk, pref); });

  } else if (pref === 'prefer-local' && _detailItem.source === 'both') {
    // Movie exists in both — remove debrid copy
    _postRemoveDebrid(_detailItem.title, _detailItem.year).then(function(ok) {
      if (ok) _savePref(nk, pref);
    });

  } else if (pref === 'prefer-debrid' && (_detailItem.source === 'local' || _detailItem.source === 'both')) {
    if (_detailItem.source === 'local') {
      // No debrid copy — save preference and search if download service available
      var capturedMovieTitle = _detailItem.title;
      _savePref(nk, pref).then(function(saved) {
        if (!saved) { _showMsg('Failed to save preference.', 'error'); return; }
        if (movieSvc) {
          _setPending(capturedMovieTitle, [{season: 0, episode: 0}], 'to-debrid');
          _postDownload({
            title: capturedMovieTitle, type: 'movie', tmdb_id: tmdbId,
            prefer_debrid: true
          }).then(function(ok) {
            if (ok) {
              _showMsg('Preference saved. Searching for debrid copy.', 'success');
            } else {
              _showMsg('Preference saved but search failed.', 'error');
            }
            _scheduleRefresh(1000);
          });
        } else {
          _showMsg('Preference saved. Configure Radarr or Overseerr to search for debrid copies.', 'success');
        }
      });
    } else {
      // source=both — replace local file with link to debrid mount
      if (!confirm('Switch ' + _detailItem.title + ' to debrid streaming?'
        + '\n\nLocal file will be removed. Playback will stream from your debrid service.')) return;
      var oldPref = _savedPref;
      var capturedBothTitle = _detailItem.title;
      _actionInFlight = true;
      _setActionsDisabled(true);
      _showMsgHtml('<span class="scanning-dot"></span>Switching to debrid...');
      _savePref(nk, pref).then(function(saved) {
        if (!saved) return;
        // Clear before _postRemove which has its own _actionInFlight guard
        // Keep buttons disabled until the full chain completes
        _actionInFlight = false;
        return _postRemove({
          title: capturedBothTitle, type: 'movie', tmdb_id: tmdbId,
          episodes: []
        }).then(function(ok) {
          if (!ok) { _savePref(nk, oldPref); }
          else { _showMsg('Switched to debrid streaming. To get a local copy back, use the Switch to Local button.', 'success'); }
          _scheduleRefresh(1000);
        });
      }).catch(function(e) {
        _showMsg('Operation failed: ' + e, 'error');
      }).finally(function() {
        _actionInFlight = false;
        _setActionsDisabled(false);
      });
    }

  } else {
    _savePref(nk, pref);
    _showMsg('Preference saved.', 'success');
  }
}

function rmSeason(seasonIdx) {
  if (!_detailItem || !_detailSeasons[seasonIdx]) return;
  var season = _detailSeasons[seasonIdx];
  var epNums = [];
  for (var i = 0; i < season.episodes.length; i++) {
    if (season.episodes[i].source === 'local' || season.episodes[i].source === 'both') {
      epNums.push(season.episodes[i].number);
    }
  }
  if (!epNums.length) return;
  var tmdbId = _detailMeta ? _detailMeta.tmdb_id : null;
  _postRemove({
    title: _detailItem.title, type: _detailItem.type, tmdb_id: tmdbId,
    season: season.number, episodes: epNums
  });
}

var _svcNames = {sonarr: 'Sonarr', radarr: 'Radarr', overseerr: 'Overseerr'};
var _actionInFlight = false;

function _setActionsDisabled(disabled) {
  var btns = document.querySelectorAll('.detail-view .btn-ghost.btn-sm, .detail-view .btn-primary');
  for (var i = 0; i < btns.length; i++) btns[i].disabled = disabled;
}

function _postDownload(payload) {
  if (_actionInFlight) return Promise.resolve(false);
  _actionInFlight = true;
  _setActionsDisabled(true);
  var svc = payload.type === 'movie' ? _downloadServices.movie : _downloadServices.show;
  var svcName = _svcNames[svc] || svc;
  var actionWord = svc === 'overseerr' ? 'Requesting' : 'Sending to ' + svcName;
  _showMsgHtml('<span class="scanning-dot"></span>' + esc(actionWord) + '...');
  return fetch('/api/library/download', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r) {
    return r.json().then(function(d) { return {ok: r.ok, d: d}; });
  }).then(function(res) {
    var d = res.d;
    var errMsg = (!res.ok || d.status === 'error') ? (d.error || d.message || 'Unknown error') : null;
    if (errMsg) _showMsg('Error: ' + errMsg, 'error');
    else _showMsg(d.message || 'Sent.', 'success');
    return !errMsg;
  }).catch(function(e) {
    _showMsg('Request failed: ' + e, 'error');
    return false;
  }).finally(function() {
    _actionInFlight = false;
    _setActionsDisabled(false);
  });
}

function _postRemove(payload) {
  if (_actionInFlight) return Promise.resolve(false);
  _actionInFlight = true;
  _setActionsDisabled(true);
  _showMsgHtml('<span class="scanning-dot"></span>Removing...');
  return fetch('/api/library/remove-local', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r) {
    return r.json().then(function(d) { return {ok: r.ok, d: d}; });
  }).then(function(res) {
    var d = res.d;
    if (!res.ok || (d.status !== 'removed')) {
      var errMsg = d.error || d.message || 'Unknown error';
      _showMsg('Error: ' + errMsg, 'error');
      return false;
    } else {
      _showMsg('Switched ' + (d.removed || 0) + ' file(s) to debrid streaming. To switch back to local, trigger a search in your media manager.', 'success');
      _scheduleRefresh(1000);
      return true;
    }
  }).catch(function(e) {
    _showMsg('Remove failed: ' + e, 'error');
    return false;
  }).finally(function() {
    _actionInFlight = false;
    _setActionsDisabled(false);
  });
}

var _pendingConfirmCleanup = null;

function _showDebridConfirmation(torrents, title, service, onConfirm, onCancel) {
  if (_pendingConfirmCleanup) { _pendingConfirmCleanup(); _pendingConfirmCleanup = null; }
  var el = document.getElementById('transfer-msg');
  if (!el) { onCancel(); return; }
  var html = '<div class="confirm-panel" role="alertdialog" aria-labelledby="confirm-panel-title">';
  html += '<div class="confirm-title" id="confirm-panel-title">Permanently delete ' + esc(String(torrents.length)) + ' debrid torrent' + (torrents.length !== 1 ? 's' : '') + ' for ' + esc(title) + '?</div>';
  html += '<div style="font-size:.82em;color:var(--text2);margin-bottom:8px">The following will be removed from your ' + esc(service || 'debrid') + ' account:</div>';
  html += '<ul class="confirm-list">';
  for (var i = 0; i < torrents.length && i < 10; i++) {
    html += '<li>' + esc(torrents[i].filename || torrents[i].id || '(unknown)') + '</li>';
  }
  if (torrents.length > 10) html += '<li style="color:var(--text3)">... and ' + (torrents.length - 10) + ' more</li>';
  html += '</ul>';
  html += '<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">';
  html += '<button class="btn btn-danger filled" id="confirm-delete-btn">Delete Permanently</button>';
  html += '<button class="btn btn-ghost" id="cancel-delete-btn">Cancel</button>';
  html += '</div></div>';
  el.className = 'transfer-msg';
  el.innerHTML = html;
  var confirmBtn = document.getElementById('confirm-delete-btn');
  var cancelBtn = document.getElementById('cancel-delete-btn');
  function _cleanup() { document.removeEventListener('keydown', _onKey); _pendingConfirmCleanup = null; }
  function _onKey(e) { if (e.key === 'Escape') { _cleanup(); onCancel(); } }
  document.addEventListener('keydown', _onKey);
  _pendingConfirmCleanup = _cleanup;
  if (confirmBtn) { confirmBtn.disabled = false; confirmBtn.onclick = function() { _cleanup(); onConfirm(); }; }
  if (cancelBtn) { cancelBtn.disabled = false; cancelBtn.onclick = function() { _cleanup(); onCancel(); }; }
  if (cancelBtn) cancelBtn.focus();
  el.scrollIntoView({behavior: 'smooth', block: 'nearest'});
}

function _postRemoveDebrid(title, year) {
  if (_actionInFlight) return Promise.resolve(false);
  _actionInFlight = true;
  _setActionsDisabled(true);
  _showMsgHtml('<span class="scanning-dot"></span>Finding debrid torrents...');
  var payload = {title: title};
  if (year) payload.year = year;
  return fetch('/api/library/remove-debrid', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r) {
    return r.json().then(function(d) { return {ok: r.ok, d: d}; });
  }).then(function(res) {
    if (!res.ok) {
      _showMsg('Error: ' + (res.d.error || 'Unknown error'), 'error');
      return false;
    }
    if (!res.d.count) {
      _showMsg('No debrid torrents found for this title.', 'error');
      return false;
    }
    var torrents = res.d.torrents || [];
    var service = res.d.service || '';
    return new Promise(function(resolve) {
      _showDebridConfirmation(torrents, title, service, function() {
        _showMsgHtml('<span class="scanning-dot"></span>Removing debrid torrents...');
        var ids = torrents.map(function(t) { return t.id; });
        fetch('/api/library/remove-debrid/confirm', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({torrent_ids: ids, title: title, service: service})
        }).then(function(r2) {
          return r2.json().then(function(d2) { return {ok: r2.ok, d: d2}; });
        }).then(function(res2) {
          if (!res2.ok || res2.d.status === 'error') {
            _showMsg('Error: ' + (res2.d.error || res2.d.message || 'Deletion failed'), 'error');
            resolve(false);
          } else {
            _showMsg('Removed ' + (res2.d.deleted || 0) + ' torrent(s). To restore, re-add the torrents to your debrid account.', 'success');
            _scheduleRefresh(2000);
            resolve(true);
          }
        }).catch(function(e) {
          _showMsg('Remove debrid failed: ' + e, 'error');
          resolve(false);
        });
      }, function() {
        _clearTransferMsg();
        resolve(false);
      });
    });
  }).catch(function(e) {
    _showMsg('Remove debrid failed: ' + e, 'error');
    return false;
  }).finally(function() {
    _actionInFlight = false;
    _setActionsDisabled(false);
  });
}

function _scheduleRefresh(ms) {
  if (_refreshTimer) clearTimeout(_refreshTimer);
  _refreshTimer = setTimeout(function() {
    _refreshTimer = null;
    _refreshDetailData();
  }, ms);
}

function _startSmartPoll() {
  if (_pollActive) return;
  _pollActive = true;
  _pollTick();
}

function _stopSmartPoll() {
  _pollActive = false;
  if (_pollTimer) { clearTimeout(_pollTimer); _pollTimer = null; }
}

function _pollTick() {
  if (!_pollActive) return;
  _pollTimer = setTimeout(function() {
    _pollTimer = null;
    if (!_pollActive) return;
    _refreshDetailData().then(function() {
      if (_hasPendingTransitions()) {
        _pollTick();
      } else {
        _pollActive = false;
      }
    });
  }, 15000);
}

function _hasPendingTransitions() {
  return Object.keys(_pending).length > 0;
}

function _checkSmartPoll() {
  if (_hasPendingTransitions()) {
    _startSmartPoll();
  } else {
    _stopSmartPoll();
  }
}

function _setPending(title, episodes, direction) {
  return fetch('/api/library/pending', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({title: title, episodes: episodes, direction: direction})
  }).then(function(r) { return r.ok; }).catch(function() { return false; });
}

// Serialize an array of functions that each return a Promise<boolean>.
// Returns true if at least one task succeeded.
function _runSequential(tasks) {
  var anySuccess = false;
  return tasks.reduce(function(chain, task) {
    return chain.then(function() {
      return task().then(function(ok) { if (ok) anySuccess = true; });
    });
  }, Promise.resolve()).then(function() { return anySuccess; });
}

function _refreshDetailData() {
  // Refresh library data quietly — skip grid re-render to avoid poster
  // flicker.  Detail view is updated if open.  Grid picks up changes on
  // next user interaction (filter change, tab switch, manual refresh).
  return fetch('/api/library')
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(data) {
      if (!data) return;
      _applyLibraryData(data, {quiet: true});
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
// Debrid Search Modal (F9)
// ---------------------------------------------------------------------------
var _searchResults = [];
var _searchSortCol = 'quality';
var _searchSortAsc = false;
var _searchQualityFilter = 0;

function openSearchFromBtn(btn) {
  var imdbId = btn.getAttribute('data-imdb');
  var mediaType = btn.getAttribute('data-mtype') || 'movie';
  var season = btn.getAttribute('data-season');
  var episode = btn.getAttribute('data-episode');
  var label = btn.getAttribute('data-label') || '';
  openSearchModal(imdbId, mediaType, season ? parseInt(season, 10) : null, episode ? parseInt(episode, 10) : null, label);
}

function openSearchModal(imdbId, mediaType, season, episode, displayTitle) {
  var overlay = document.createElement('div');
  overlay.className = 'search-overlay';
  overlay.id = 'search-overlay';
  overlay.onclick = function(e) { if (e.target === overlay) closeSearchModal(); };

  var seasonStr = season !== null && season !== undefined ? String(season) : '';
  var episodeStr = episode !== null && episode !== undefined ? String(episode) : '';
  var headerTitle = displayTitle || 'Search';

  var html = '<div class="search-dialog">';
  html += '<div class="search-dialog-hdr"><h3>Search: ' + esc(headerTitle) + '</h3>';
  html += '<button class="search-dialog-close" onclick="closeSearchModal()" title="Close">&times;</button></div>';
  html += '<div class="search-dialog-body" id="search-body">';
  html += '<div style="text-align:center;padding:24px 0;color:var(--text3)"><span class="spinner" style="display:inline-block;width:16px;height:16px;border:2px solid var(--border);border-top-color:var(--blue);border-radius:50%;animation:spin .6s linear infinite;vertical-align:middle;margin-right:8px"></span>Searching Torrentio\u2026</div>';
  html += '</div></div>';
  overlay.innerHTML = html;
  document.body.appendChild(overlay);
  document.body.style.overflow = 'hidden';

  // Reset state
  _searchResults = [];
  _searchSortCol = 'quality';
  _searchSortAsc = false;
  _searchQualityFilter = 0;

  var payload = {imdb_id: imdbId, type: mediaType};
  if (seasonStr) payload.season = parseInt(seasonStr, 10);
  if (episodeStr) payload.episode = parseInt(episodeStr, 10);

  fetch('/api/search', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    _searchResults = data.results || [];
    _renderSearchResults();
  })
  .catch(function(err) {
    var body = document.getElementById('search-body');
    if (body) body.innerHTML = '<div class="search-empty">Search failed: ' + esc(String(err)) + '</div>';
  });
}

function closeSearchModal() {
  var overlay = document.getElementById('search-overlay');
  if (overlay) overlay.remove();
  document.body.style.overflow = '';
}

function _renderSearchResults() {
  var body = document.getElementById('search-body');
  if (!body) return;

  var filtered = _searchResults.filter(function(r) {
    if (_searchQualityFilter > 0 && r.quality.score !== _searchQualityFilter) return false;
    return true;
  });

  // Sort
  filtered.sort(function(a, b) {
    var va, vb;
    if (_searchSortCol === 'quality') { va = a.quality.score; vb = b.quality.score; }
    else if (_searchSortCol === 'size') { va = a.size_bytes; vb = b.size_bytes; }
    else if (_searchSortCol === 'seeds') { va = a.seeds; vb = b.seeds; }
    else { va = a.quality.score; vb = b.quality.score; }
    if (va === vb) {
      if (_searchSortCol !== 'quality' && a.quality.score !== b.quality.score) return b.quality.score - a.quality.score;
      return b.seeds - a.seeds;
    }
    return _searchSortAsc ? (va - vb) : (vb - va);
  });

  var html = '<div class="search-filter-row">';
  html += '<label>Quality: <select id="search-quality-filter" onchange="_searchQualityFilter=parseInt(this.value,10);_renderSearchResults()">';
  html += '<option value="0"' + (_searchQualityFilter === 0 ? ' selected' : '') + '>Any</option>';
  html += '<option value="1"' + (_searchQualityFilter === 1 ? ' selected' : '') + '>480p</option>';
  html += '<option value="2"' + (_searchQualityFilter === 2 ? ' selected' : '') + '>720p</option>';
  html += '<option value="3"' + (_searchQualityFilter === 3 ? ' selected' : '') + '>1080p</option>';
  html += '<option value="4"' + (_searchQualityFilter === 4 ? ' selected' : '') + '>2160p</option>';
  html += '</select></label>';
  html += '<span class="search-count">' + filtered.length + ' of ' + _searchResults.length + ' results</span>';
  html += '</div>';

  if (filtered.length === 0) {
    html += '<div class="search-empty">' + (_searchResults.length === 0 ? 'No results found' : 'No results match filters') + '</div>';
    body.innerHTML = html;
    return;
  }

  html += '<table class="search-results-tbl"><thead><tr>';
  var cols = [
    {key: 'title', label: 'Release'},
    {key: 'indexer', label: 'Indexer'},
    {key: 'quality', label: 'Quality'},
    {key: 'size', label: 'Size'},
    {key: 'seeds', label: 'Seeds'},
    {key: 'action', label: ''},
  ];
  for (var ci = 0; ci < cols.length; ci++) {
    var col = cols[ci];
    if (col.key === 'action' || col.key === 'title' || col.key === 'indexer') {
      html += '<th>' + col.label + '</th>';
    } else {
      var arrow = _searchSortCol === col.key ? (_searchSortAsc ? ' &#9650;' : ' &#9660;') : '';
      html += '<th onclick="sortSearchResults(\'' + col.key + '\')">' + col.label + '<span class="sort-arrow">' + arrow + '</span></th>';
    }
  }
  html += '</tr></thead><tbody>';

  for (var ri = 0; ri < filtered.length; ri++) {
    var r = filtered[ri];
    var addedClass = r._added ? ' added-row' : '';
    html += '<tr class="' + addedClass + '">';
    html += '<td class="sr-title" title="' + escAttr(r.title) + '">' + esc(r.title) + '</td>';
    html += '<td class="sr-indexer" title="' + escAttr(r.source_name || '') + '">' + esc(r.source_name || '') + '</td>';
    var qCls = 'q-' + escAttr(r.quality.label.replace(/\s/g, ''));
    html += '<td><span class="badge-quality ' + qCls + '">' + esc(r.quality.label) + '</span></td>';
    html += '<td>' + _formatBytes(r.size_bytes) + '</td>';
    html += '<td>' + (r.seeds || 0) + '</td>';
    html += '<td>';
    if (r._added) {
      html += '<span style="color:var(--green);font-size:.82em">&#10003; Added</span>';
    } else {
      html += '<button class="btn-add-debrid" data-hash="' + escAttr(r.info_hash) + '" onclick="addSearchResult(this)">Add</button>';
    }
    html += '</td></tr>';
  }

  html += '</tbody></table>';
  body.innerHTML = html;
}

function sortSearchResults(col) {
  if (_searchSortCol === col) {
    _searchSortAsc = !_searchSortAsc;
  } else {
    _searchSortCol = col;
    _searchSortAsc = false;
  }
  _renderSearchResults();
}

function addSearchResult(btn) {
  var hash = btn.getAttribute('data-hash');
  if (!hash) return;
  var r = null;
  for (var i = 0; i < _searchResults.length; i++) {
    if (_searchResults[i].info_hash === hash) { r = _searchResults[i]; break; }
  }
  if (!r) return;

  btn.disabled = true;
  btn.textContent = '\u2026';

  fetch('/api/search/add', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({info_hash: r.info_hash, title: r.title})
  })
  .then(function(resp) { return resp.json(); })
  .then(function(data) {
    if (data.success) {
      // Mark in the source data
      for (var i = 0; i < _searchResults.length; i++) {
        if (_searchResults[i].info_hash === r.info_hash) {
          _searchResults[i]._added = true;
          break;
        }
      }
      _renderSearchResults();
      // Show success message
      var msg = 'Added to ' + (data.service || 'debrid') + '! Library will update on next scan.';
      _showSearchMsg(msg, 'success');
    } else {
      btn.disabled = false;
      btn.textContent = 'Add';
      _showSearchMsg('Failed: ' + (data.error || 'Unknown error'), 'error');
    }
  })
  .catch(function(err) {
    btn.disabled = false;
    btn.textContent = 'Add';
    _showSearchMsg('Error: ' + String(err), 'error');
  });
}

function _showSearchMsg(msg, type) {
  var body = document.getElementById('search-body');
  if (!body) return;
  var existing = document.getElementById('search-msg');
  if (existing) existing.remove();
  var color = type === 'success' ? 'var(--green)' : 'var(--red)';
  var div = document.createElement('div');
  div.id = 'search-msg';
  div.style.cssText = 'padding:6px 0;font-size:.82em;color:' + color;
  div.textContent = msg;
  body.insertBefore(div, body.firstChild);
  setTimeout(function() { var el = document.getElementById('search-msg'); if (el) el.remove(); }, 5000);
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
// Restore persisted filter/sort preferences
try {
  var _savedSort = localStorage.getItem('pd_library_sort');
  var _savedSource = localStorage.getItem('pd_library_source');
  var _savedStatus = localStorage.getItem('pd_library_status');
  var _savedYear = localStorage.getItem('pd_library_year');
  if (_savedSort === 'year') _savedSort = 'year-new'; // migrate old value
  if (_savedSort) document.getElementById('sort-select').value = _savedSort;
  if (_savedSource) document.getElementById('source-filter').value = _savedSource;
  if (_savedStatus) document.getElementById('status-filter').value = _savedStatus;
  if (_savedYear) document.getElementById('year-filter').value = _savedYear;
} catch(e) {}
// Apply URL query param filter preset (e.g. ?filter=missing)
try {
  var _urlParams = new URLSearchParams(window.location.search);
  var _urlFilter = _urlParams.get('filter');
  if (_urlFilter && ['missing', 'unavailable', 'pending', 'fallback', 'recent'].indexOf(_urlFilter) !== -1) {
    _activeWantedPreset = _urlFilter;
  }
} catch(e) {}
// If restoring a detail view, hide library chrome immediately to avoid flash
try {
  var _dp = new URLSearchParams(window.location.search);
  if (_dp.get('detail')) {
    document.querySelector('.tabs').style.display = 'none';
    document.querySelector('.controls').style.display = 'none';
    document.getElementById('wanted-presets').style.display = 'none';
    document.getElementById('wanted-actions').style.display = 'none';
    document.getElementById('footer').style.display = 'none';
    document.getElementById('jump-bar').style.display = 'none';
    document.getElementById('content-area').innerHTML =
      '<div class="state-panel"><div>Loading...</div></div>';
  }
} catch(e) {}
fetchLibrary();
startTsRefresh();
/* Show tooltip on truncated card titles only */
document.addEventListener('mouseenter',function(e){
  var t=e.target;
  if(t&&t.classList&&t.classList.contains('card-title')){
    t.title=t.scrollWidth>t.clientWidth?t.textContent:'';
  }
},true);
</script>
</main>
</body>
</html>'''
