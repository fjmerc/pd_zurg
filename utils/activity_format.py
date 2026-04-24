"""Human-readable formatting for history events.

Single source of truth for the activity detail string — used by the
dashboard renderers (via /api/history) and by utils.notifications to
keep push-message wording consistent with the UI.

Contract: given an event dict from history.jsonl, ``format_event`` returns
a dict with:

    short       — one-line detail shown on the activity feed row
    long        — richer multi-line version for notification bodies
    group_key   — tuple used for consecutive-run grouping in the UI;
                  events sharing a group_key within a time window can
                  collapse to "N× over Xd"

When ``meta['cause']`` is missing (pre-vocab events within the 30-day
retention window) the helper falls back to the event's raw ``detail``
string so nothing renders empty.
"""

from datetime import datetime, timezone


_UNIT_TABLE = (
    (86400, 'd'),
    (3600, 'h'),
    (60, 'm'),
)


def _elapsed_human(first_ts):
    """Return '5d 3h' / '2h 14m' / '45m' / '30s' for an ISO timestamp, or ''.

    Mirrored in utils/activity_format.FORMATTER_JS — keep the two in sync.
    """
    if not first_ts:
        return ''
    try:
        dt = datetime.fromisoformat(first_ts.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return ''
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    sec = int((datetime.now(timezone.utc) - dt).total_seconds())
    if sec < 60:
        return f'{sec}s'
    # Divisors: d gets h-remainder (3600), h gets m-remainder (60).
    # The previous version used `unit // 60` for the 'd' case, which
    # produced 1440 (seconds-per-24-minutes) instead of 3600 — so 5d 3h
    # formatted as "5d 7h".  Use an explicit sub-unit per label.
    sub_units = {'d': ('h', 3600), 'h': ('m', 60)}
    for unit, label in _UNIT_TABLE:
        if sec >= unit:
            primary = sec // unit
            sub = sub_units.get(label)
            if sub:
                sub_label, sub_unit = sub
                rem = (sec - primary * unit) // sub_unit
                if rem:
                    return f'{primary}{label} {rem}{sub_label}'
            return f'{primary}{label}'
    return f'{sec}s'


def _size_human(size_bytes):
    """Return '4.3 GB' / '720 MB' / '' for an integer byte count."""
    try:
        n = int(size_bytes)
    except (TypeError, ValueError):
        return ''
    if n <= 0:
        return ''
    for unit, div in (('TB', 1 << 40), ('GB', 1 << 30), ('MB', 1 << 20), ('KB', 1 << 10)):
        if n >= div:
            return f'{n / div:.1f} {unit}' if n / div < 10 else f'{n / div:.0f} {unit}'
    return f'{n} B'


def _file_label(meta):
    """Compose a "(quality, size)" suffix from meta."""
    parts = []
    q = meta.get('quality')
    if q:
        parts.append(str(q))
    size_h = _size_human(meta.get('size_bytes'))
    if size_h:
        parts.append(size_h)
    return f' ({", ".join(parts)})' if parts else ''


def _cycle_suffix(meta):
    """Compose ' — retry #14, first attempt 5d 3h ago' from cycle meta."""
    n = meta.get('cycle_n')
    if not n or n < 2:
        return ''
    ago = _elapsed_human(meta.get('cycle_first_ts'))
    if ago:
        return f' — retry #{n}, first attempt {ago} ago'
    return f' — retry #{n}'


# ---------------------------------------------------------------------------
# Per-cause formatters
# ---------------------------------------------------------------------------

def _fmt_library_new_import(ev, meta):
    f = meta.get('file', '')
    short = f'New import: {f}{_file_label(meta)}' if f else 'New debrid file symlinked'
    return short, short


def _fmt_library_upgrade_replaced(ev, meta):
    f = meta.get('file', '')
    prior = meta.get('replaces', '')
    if f and prior:
        short = f'Upgraded: {prior} → {f}{_file_label(meta)}'
    elif f:
        short = f'Upgraded: new file {f}{_file_label(meta)}'
    else:
        short = 'Upgraded: previous symlink replaced'
    return short, short


def _fmt_library_state_init(ev, meta):
    f = meta.get('file', '')
    short = f'Initial scan linked: {f}{_file_label(meta)}' if f else 'Initial scan linked existing file'
    return short, short


def _fmt_blackhole_new_import(ev, meta):
    count = meta.get('count')
    release = meta.get('release') or ev.get('title') or ''
    if count and count > 1:
        short = f'Blackhole import: {count} files from {release}'
    else:
        short = f'Blackhole import from {release}' if release else 'Blackhole import'
    return short, short


def _fmt_blackhole_cache_hit(ev, meta):
    prov = meta.get('provider', 'debrid')
    short = f'Cached on {prov} — ready to link'
    return short, short


def _fmt_blackhole_grab_submitted(ev, meta):
    prov = meta.get('provider', 'debrid')
    return f'Submitted to {prov}', f'Submitted to {prov}'


def _fmt_compromise_grab(ev, meta):
    pref = meta.get('preferred_tier', '?')
    got = meta.get('grabbed_tier', '?')
    strat = meta.get('strategy', '')
    short = f'Compromise grab: preferred {pref}, grabbed {got}'
    if strat:
        short += f' ({strat})'
    return short, short


def _fmt_post_symlink_rescan(ev, meta):
    svc = meta.get('arr_service', 'arr').capitalize()
    short = f'{svc} rescan — new symlink available for import'
    return short, short


def _fmt_post_grab_rescan(ev, meta):
    svc = meta.get('arr_service', 'arr').capitalize()
    short = f'{svc} rescan — new grab dropped'
    return short, short


def _fmt_user_triggered_rescan(ev, meta):
    svc = meta.get('arr_service', 'arr').capitalize()
    return f'{svc} rescan — user-triggered', f'{svc} rescan — user-triggered'


def _fmt_user_triggered_search(ev, meta):
    svc = meta.get('arr_service', 'arr').capitalize()
    return f'{svc} search — user-triggered', f'{svc} search — user-triggered'


def _fmt_routing_audit_retry(ev, meta):
    svc = meta.get('arr_service', 'arr').capitalize()
    short = f'{svc} search — scheduled routing audit retry{_cycle_suffix(meta)}'
    return short, short


def _fmt_stale_grab_retry(ev, meta):
    svc = meta.get('arr_service', 'arr').capitalize()
    age = meta.get('age_minutes')
    extra = f' (previous grab idle {int(age)}m)' if age else ''
    short = f'{svc} search — stale-grab retry{extra}{_cycle_suffix(meta)}'
    return short, short


def _fmt_symlink_repair_research(ev, meta):
    svc = meta.get('arr_service', 'arr').capitalize()
    short = f'{svc} search — symlink repair fallback{_cycle_suffix(meta)}'
    return short, short


def _fmt_preference_enforce_search(ev, meta):
    svc = meta.get('arr_service', 'arr').capitalize()
    short = f'{svc} search — preference enforcement{_cycle_suffix(meta)}'
    return short, short


def _fmt_local_fallback_grab(ev, meta):
    svc = meta.get('arr_service', 'arr').capitalize()
    short = f'{svc} local fallback — grabbed from usenet/local indexer'
    return short, short


def _fmt_preference_source_switch(ev, meta):
    frm = meta.get('from', '?')
    to = meta.get('to', '?')
    short = f'Source switch: {frm} → {to}'
    return short, short


def _fmt_routing_repaired(ev, meta):
    tc = meta.get('tagged_count')
    sc = meta.get('search_count', 0)
    svc = meta.get('arr_service', 'arr').capitalize()
    pieces = []
    if tc:
        pieces.append(f'tagged {tc} item(s)')
    if sc:
        pieces.append(f'triggered {sc} search(es)')
    detail = ', '.join(pieces) or 'applied routing fix'
    return f'{svc} routing repaired — {detail}', f'{svc} routing repaired — {detail}'


def _fmt_arr_deleted(ev, meta):
    svc = meta.get('arr_service', meta.get('service', 'arr')).capitalize()
    reason = meta.get('reason', '')
    suffix = f' ({reason})' if reason else ''
    return f'Deleted from {svc}{suffix}', f'Deleted from {svc}{suffix}'


def _fmt_auto_blocklist(ev, meta):
    reason = meta.get('blocklist_reason') or meta.get('reason') or 'auto-blocklisted'
    return f'Auto-blocklisted: {reason}', f'Auto-blocklisted: {reason}'


def _fmt_debrid_unavailable_marked(ev, meta):
    days = meta.get('age_days')
    attempts = meta.get('search_attempts')
    base = f'Marked unavailable after {days}d' if days else 'Marked unavailable'
    tail = ' — retries continue in arr'
    if attempts and attempts > 1:
        tail = f' — {attempts} searches so far, retries continue in arr'
    return base + tail, base + tail


def _fmt_terminal_error(ev, meta):
    st = meta.get('status') or meta.get('error') or 'unknown'
    prov = meta.get('provider', 'debrid')
    return f'Failed on {prov}: {st}', f'Failed on {prov}: {st}'


def _fmt_uncached_timeout(ev, meta):
    deleted = meta.get('deleted')
    tail = ' — removed from debrid' if deleted else ' — debrid cleanup skipped'
    return f'Timed out waiting for cache{tail}', f'Timed out waiting for cache{tail}'


def _fmt_uncached_rejected(ev, meta):
    prov = meta.get('provider', 'debrid')
    return f'Rejected — not cached on {prov}', f'Rejected — not cached on {prov}'


def _fmt_incomplete_release(ev, meta):
    missing = meta.get('missing', [])
    if isinstance(missing, list):
        joined = ', '.join(str(m) for m in missing)
    else:
        joined = str(missing)
    s = f'Incomplete release — missing {joined}' if joined else 'Incomplete release'
    return s, s


def _fmt_alts_exhausted(ev, meta):
    return 'All alternative releases tried and failed', 'All alternative releases tried and failed'


def _fmt_duplicate_skipped(ev, meta):
    prov = meta.get('provider', 'debrid')
    return f'Skipped — already on {prov}', f'Skipped — already on {prov}'


def _fmt_blocklisted_hash(ev, meta):
    return 'Skipped — info hash is blocklisted', 'Skipped — info hash is blocklisted'


def _fmt_disc_rip_rejected(ev, meta):
    return 'Rejected — disc rip (no usable media files)', 'Rejected — disc rip (no usable media files)'


def _fmt_debrid_add_failed(ev, meta):
    err = meta.get('error', 'unknown error')
    return f'Debrid add failed — {err}', f'Debrid add failed — {err}'


def _fmt_debrid_add_via_search(ev, meta):
    svc = meta.get('service', 'debrid')
    return f'Added to {svc} via search', f'Added to {svc} via search'


def _fmt_symlink_create_failed(ev, meta):
    err = meta.get('error', 'unknown error')
    return f'Symlink creation failed — {err}', f'Symlink creation failed — {err}'


def _fmt_task_library_scan(ev, meta):
    m = meta.get('movies', 0)
    s = meta.get('shows', 0)
    sc = meta.get('symlinks_created', 0)
    dur = meta.get('duration_ms')
    pieces = [f'{m} movies', f'{s} shows']
    if sc:
        pieces.append(f'{sc} new symlinks')
    if dur:
        pieces.append(f'{int(dur)}ms')
    short = 'Library scan — ' + ', '.join(pieces)
    return short, short


def _fmt_task_housekeeping(ev, meta):
    return 'Housekeeping — retention/cleanup pass complete', 'Housekeeping — retention/cleanup pass complete'


def _fmt_task_stale_grab_detection(ev, meta):
    found = meta.get('stale_found', 0)
    retried = meta.get('searches_triggered', 0)
    short = f'Stale-grab detection — found {found}, retried {retried}'
    return short, short


def _fmt_task_routing_audit(ev, meta):
    return 'Routing audit — tag/search sweep complete', 'Routing audit — tag/search sweep complete'


def _fmt_task_verify_symlinks(ev, meta):
    r = meta.get('repaired', 0)
    s = meta.get('searched', 0)
    d = meta.get('deleted', 0)
    pieces = []
    if r:
        pieces.append(f'repaired {r}')
    if s:
        pieces.append(f'searched {s}')
    if d:
        pieces.append(f'deleted {d}')
    short = 'Symlink verify — ' + (', '.join(pieces) if pieces else 'nothing to do')
    return short, short


_CAUSE_FORMATTERS = {
    'library_new_import': _fmt_library_new_import,
    'library_upgrade_replaced': _fmt_library_upgrade_replaced,
    'library_state_init': _fmt_library_state_init,
    'blackhole_new_import': _fmt_blackhole_new_import,
    'blackhole_cache_hit': _fmt_blackhole_cache_hit,
    'blackhole_grab_submitted': _fmt_blackhole_grab_submitted,
    'compromise_grab': _fmt_compromise_grab,
    'post_symlink_rescan': _fmt_post_symlink_rescan,
    'post_grab_rescan': _fmt_post_grab_rescan,
    'user_triggered_rescan': _fmt_user_triggered_rescan,
    'user_triggered_search': _fmt_user_triggered_search,
    'routing_audit_retry': _fmt_routing_audit_retry,
    'stale_grab_retry': _fmt_stale_grab_retry,
    'symlink_repair_research': _fmt_symlink_repair_research,
    'preference_enforce_search': _fmt_preference_enforce_search,
    'local_fallback_grab': _fmt_local_fallback_grab,
    'preference_source_switch': _fmt_preference_source_switch,
    'routing_repaired': _fmt_routing_repaired,
    'arr_deleted_user': _fmt_arr_deleted,
    'arr_deleted_cleanup': _fmt_arr_deleted,
    'auto_blocklist_added': _fmt_auto_blocklist,
    'debrid_unavailable_marked': _fmt_debrid_unavailable_marked,
    'terminal_error': _fmt_terminal_error,
    'uncached_timeout': _fmt_uncached_timeout,
    'uncached_rejected': _fmt_uncached_rejected,
    'incomplete_release': _fmt_incomplete_release,
    'alts_exhausted': _fmt_alts_exhausted,
    'duplicate_skipped': _fmt_duplicate_skipped,
    'blocklisted_hash': _fmt_blocklisted_hash,
    'disc_rip_rejected': _fmt_disc_rip_rejected,
    'debrid_add_failed': _fmt_debrid_add_failed,
    'debrid_add_via_search': _fmt_debrid_add_via_search,
    'symlink_create_failed': _fmt_symlink_create_failed,
    'task_library_scan': _fmt_task_library_scan,
    'task_housekeeping': _fmt_task_housekeeping,
    'task_stale_grab_detection': _fmt_task_stale_grab_detection,
    'task_routing_audit': _fmt_task_routing_audit,
    'task_verify_symlinks': _fmt_task_verify_symlinks,
}


def format_event(event):
    """Return {'short': str, 'long': str, 'group_key': tuple} for an event.

    Events emitted before the cause vocabulary existed fall back to the
    raw ``detail`` string. Never raises — a malformed event still produces
    a best-effort string.
    """
    if not isinstance(event, dict):
        return {'short': '', 'long': '', 'group_key': ('',)}

    meta = event.get('meta') or {}
    cause = meta.get('cause') if isinstance(meta, dict) else None
    detail = event.get('detail', '') or ''

    fmt = _CAUSE_FORMATTERS.get(cause) if cause else None
    if fmt:
        try:
            short, long_ = fmt(event, meta)
        except Exception:
            short, long_ = detail, detail
    else:
        short, long_ = detail, detail

    group_key = (
        event.get('type', ''),
        event.get('source', ''),
        cause or '',
        (event.get('media_title') or event.get('title') or ''),
    )
    return {'short': short, 'long': long_, 'group_key': group_key}


__all__ = ['format_event', 'FORMATTER_JS']


# ---------------------------------------------------------------------------
# JS mirror — injected into both activity_page.py and library_page.py so
# the UI renders the same text.  Keep in sync with format_event above.
# ---------------------------------------------------------------------------

FORMATTER_JS = r"""
(function(){
  function sizeHuman(n){
    n = parseInt(n, 10);
    if (!n || n <= 0) return '';
    var units = [['TB', 1099511627776],['GB', 1073741824],['MB', 1048576],['KB', 1024]];
    for (var i=0;i<units.length;i++){
      var u = units[i];
      if (n >= u[1]){
        var v = n / u[1];
        return (v < 10 ? v.toFixed(1) : Math.round(v)) + ' ' + u[0];
      }
    }
    return n + ' B';
  }
  function elapsedHuman(iso){
    if (!iso) return '';
    var then = Date.parse(iso);
    if (isNaN(then)) return '';
    var sec = Math.max(0, Math.floor((Date.now() - then) / 1000));
    if (sec < 60) return sec + 's';
    if (sec < 3600) return Math.floor(sec/60) + 'm';
    if (sec < 86400){
      var h = Math.floor(sec/3600), m = Math.floor((sec%3600)/60);
      return m ? (h+'h '+m+'m') : (h+'h');
    }
    var d = Math.floor(sec/86400), hh = Math.floor((sec%86400)/3600);
    return hh ? (d+'d '+hh+'h') : (d+'d');
  }
  function fileLabel(meta){
    var parts = [];
    if (meta.quality) parts.push(String(meta.quality));
    var sz = sizeHuman(meta.size_bytes);
    if (sz) parts.push(sz);
    return parts.length ? ' (' + parts.join(', ') + ')' : '';
  }
  function cycleSuffix(meta){
    var n = meta.cycle_n;
    if (!n || n < 2) return '';
    var ago = elapsedHuman(meta.cycle_first_ts);
    return ago ? (' — retry #' + n + ', first attempt ' + ago + ' ago') : (' — retry #' + n);
  }
  function cap(s){ s = s || 'arr'; return s.charAt(0).toUpperCase() + s.slice(1); }

  var F = {
    library_new_import: function(ev,m){
      var f = m.file || '';
      return f ? ('New import: ' + f + fileLabel(m)) : 'New debrid file symlinked';
    },
    library_upgrade_replaced: function(ev,m){
      var f = m.file || '', p = m.replaces || '';
      if (f && p) return 'Upgraded: ' + p + ' → ' + f + fileLabel(m);
      if (f) return 'Upgraded: new file ' + f + fileLabel(m);
      return 'Upgraded: previous symlink replaced';
    },
    library_state_init: function(ev,m){
      var f = m.file || '';
      return f ? ('Initial scan linked: ' + f + fileLabel(m)) : 'Initial scan linked existing file';
    },
    blackhole_new_import: function(ev,m){
      var rel = m.release || ev.title || '';
      if (m.count && m.count > 1) return 'Blackhole import: ' + m.count + ' files from ' + rel;
      return rel ? ('Blackhole import from ' + rel) : 'Blackhole import';
    },
    blackhole_cache_hit: function(ev,m){ return 'Cached on ' + (m.provider || 'debrid') + ' — ready to link'; },
    blackhole_grab_submitted: function(ev,m){ return 'Submitted to ' + (m.provider || 'debrid'); },
    compromise_grab: function(ev,m){
      var s = 'Compromise grab: preferred ' + (m.preferred_tier||'?') + ', grabbed ' + (m.grabbed_tier||'?');
      if (m.strategy) s += ' (' + m.strategy + ')';
      return s;
    },
    post_symlink_rescan: function(ev,m){ return cap(m.arr_service) + ' rescan — new symlink available for import'; },
    post_grab_rescan:   function(ev,m){ return cap(m.arr_service) + ' rescan — new grab dropped'; },
    user_triggered_rescan: function(ev,m){ return cap(m.arr_service) + ' rescan — user-triggered'; },
    user_triggered_search: function(ev,m){ return cap(m.arr_service) + ' search — user-triggered'; },
    routing_audit_retry: function(ev,m){ return cap(m.arr_service) + ' search — scheduled routing audit retry' + cycleSuffix(m); },
    stale_grab_retry:   function(ev,m){
      var age = m.age_minutes ? (' (previous grab idle ' + Math.round(m.age_minutes) + 'm)') : '';
      return cap(m.arr_service) + ' search — stale-grab retry' + age + cycleSuffix(m);
    },
    symlink_repair_research: function(ev,m){ return cap(m.arr_service) + ' search — symlink repair fallback' + cycleSuffix(m); },
    preference_enforce_search: function(ev,m){ return cap(m.arr_service) + ' search — preference enforcement' + cycleSuffix(m); },
    local_fallback_grab: function(ev,m){ return cap(m.arr_service) + ' local fallback — grabbed from usenet/local indexer'; },
    preference_source_switch: function(ev,m){ return 'Source switch: ' + (m.from||'?') + ' → ' + (m.to||'?'); },
    routing_repaired: function(ev,m){
      var parts = [];
      if (m.tagged_count) parts.push('tagged ' + m.tagged_count + ' item(s)');
      if (m.search_count) parts.push('triggered ' + m.search_count + ' search(es)');
      var detail = parts.length ? parts.join(', ') : 'applied routing fix';
      return cap(m.arr_service) + ' routing repaired — ' + detail;
    },
    arr_deleted_user:    function(ev,m){ return 'Deleted from ' + cap(m.arr_service || m.service) + (m.reason ? ' (' + m.reason + ')' : ''); },
    arr_deleted_cleanup: function(ev,m){ return 'Deleted from ' + cap(m.arr_service || m.service) + (m.reason ? ' (' + m.reason + ')' : ''); },
    auto_blocklist_added: function(ev,m){ return 'Auto-blocklisted: ' + (m.blocklist_reason || m.reason || 'auto-blocklisted'); },
    debrid_unavailable_marked: function(ev,m){
      var base = m.age_days ? ('Marked unavailable after ' + m.age_days + 'd') : 'Marked unavailable';
      var tail = (m.search_attempts && m.search_attempts > 1)
        ? (' — ' + m.search_attempts + ' searches so far, retries continue in arr')
        : ' — retries continue in arr';
      return base + tail;
    },
    terminal_error: function(ev,m){ return 'Failed on ' + (m.provider||'debrid') + ': ' + (m.status || m.error || 'unknown'); },
    uncached_timeout: function(ev,m){ return 'Timed out waiting for cache' + (m.deleted ? ' — removed from debrid' : ' — debrid cleanup skipped'); },
    uncached_rejected: function(ev,m){ return 'Rejected — not cached on ' + (m.provider||'debrid'); },
    incomplete_release: function(ev,m){
      var miss = Array.isArray(m.missing) ? m.missing.join(', ') : String(m.missing || '');
      return miss ? ('Incomplete release — missing ' + miss) : 'Incomplete release';
    },
    alts_exhausted: function(){ return 'All alternative releases tried and failed'; },
    duplicate_skipped: function(ev,m){ return 'Skipped — already on ' + (m.provider||'debrid'); },
    blocklisted_hash: function(){ return 'Skipped — info hash is blocklisted'; },
    disc_rip_rejected: function(){ return 'Rejected — disc rip (no usable media files)'; },
    debrid_add_failed: function(ev,m){ return 'Debrid add failed — ' + (m.error || 'unknown error'); },
    debrid_add_via_search: function(ev,m){ return 'Added to ' + (m.service || 'debrid') + ' via search'; },
    symlink_create_failed: function(ev,m){ return 'Symlink creation failed — ' + (m.error || 'unknown error'); },
    task_library_scan: function(ev,m){
      var parts = [(m.movies||0) + ' movies', (m.shows||0) + ' shows'];
      if (m.symlinks_created) parts.push(m.symlinks_created + ' new symlinks');
      if (m.duration_ms) parts.push(Math.round(m.duration_ms) + 'ms');
      return 'Library scan — ' + parts.join(', ');
    },
    task_housekeeping: function(){ return 'Housekeeping — retention/cleanup pass complete'; },
    task_stale_grab_detection: function(ev,m){ return 'Stale-grab detection — found ' + (m.stale_found||0) + ', retried ' + (m.searches_triggered||0); },
    task_routing_audit: function(){ return 'Routing audit — tag/search sweep complete'; },
    task_verify_symlinks: function(ev,m){
      var parts = [];
      if (m.repaired) parts.push('repaired ' + m.repaired);
      if (m.searched) parts.push('searched ' + m.searched);
      if (m.deleted)  parts.push('deleted ' + m.deleted);
      return 'Symlink verify — ' + (parts.length ? parts.join(', ') : 'nothing to do');
    }
  };

  window._formatActivityEvent = function(ev){
    if (!ev || typeof ev !== 'object') return {short:'', long:'', groupKey:''};
    var meta = (ev.meta && typeof ev.meta === 'object') ? ev.meta : {};
    var cause = meta.cause || '';
    var detail = ev.detail || '';
    var s;
    var fmt = cause ? F[cause] : null;
    if (fmt){
      try { s = fmt(ev, meta); } catch(e){ s = detail; }
    } else {
      s = detail;
    }
    return {
      short: s,
      long:  s,
      /* NUL separator — avoids collisions when any field contains '|'. */
      groupKey: [ev.type||'', ev.source||'', cause||'', (ev.media_title||ev.title||'')].join('\u0000')
    };
  };
})();
"""

