# Library Load Performance — Never-Block Serving

**Date:** 2026-08-09
**Status:** Approved
**Branch:** filter-recovery-era

## Problem

The library page can hang for the duration of a full scan (~53s measured in
prod: 593 movies, 123 shows, `scan_duration_ms: 53241`). Three symptoms, one
root cause — HTTP requests are allowed to block on a scan:

1. **Mid-session hang.** The in-memory cache TTL is 600s but the scheduled
   `library_scan` task runs every 3600s. For ~50 minutes of every hour the
   cache is "expired," and the first `/api/library` request in that window
   runs a full synchronous scan on the HTTP thread
   (`LibraryScanner.get_data()`, `utils/library.py:3740`).
2. **Cold start after every release.** The persisted warm-start cache
   (`library_cache.json`) is rejected whenever the recorded
   `zurgarr_version` differs from the running version
   (`_deserialize_cache_state`, `utils/library.py:2535`). Routine version
   bumps discard the snapshot, so post-rebuild loads start empty and block.
   Worse, a request arriving while the startup scan runs with a `None`
   cache falls through to a *second*, duplicate synchronous scan
   (`get_data()` only serves stale when `self._cache is not None`).
3. **General sluggishness.** Same TTL-expiry blocking, encountered
   often enough to feel constant. (The payload itself is fine: ~2 MB
   served in 82 ms warm on LAN.)

Sonarr/Radarr never block UI requests on filesystem work: scans are
background jobs writing into a store, and the UI always reads the store.
zurgarr already has the store (in-memory cache + persisted snapshot); it
lacks the never-block discipline. No database is needed at this scale —
the full-snapshot-in-memory model is appropriate for ~700 items.

## Design

### 1. Never-block serving — `get_data()`

The HTTP path never scans. Replace the synchronous-scan fallback in
`get_data()` (`utils/library.py:3740`) with:

- **Cache exists (any age):** return it immediately. If the TTL has
  expired and no scan is running, call `self.refresh()` (background
  thread) before returning, so the same response reports
  `scanning: true` and the UI's existing "Refreshing…" indicator +
  poll-until-done machinery (`utils/library_page.py`) picks up fresh data
  ~53s later without a blocked request.
- **Cache is `None`** (fresh install, or first start after a schema
  bump): return an empty-but-valid payload with `scanning` true rather
  than blocking. `setup()` already launches `refresh()` at startup and
  `refresh()` self-dedupes via `_scanning`, so repeated polls cannot
  stack scans.
- The synchronous `self.scan()` call is removed from `get_data()`
  entirely. This also eliminates the duplicate-scan bug in symptom 2.

Unchanged: scheduled tasks (`utils/scheduled_tasks.py` `library_scan`,
`enforce_source_preferences`) keep calling `scanner.scan()` directly on
their own background threads. `peek_data()` and `get_cached_stats()`
already never block. The `ttl if mount else 10` expiry logic stays as
the *trigger cadence* for background refresh; it just no longer gates a
blocking path.

**Empty payload shape** must satisfy every consumer of
`/api/library` responses: at minimum `movies: []`, `shows: []`,
`last_scan`, `scan_duration_ms`, plus whatever top-level keys
`_scan_read()` always emits (enumerated during implementation, golden-
tested). `status_server` continues to overlay `scanning`,
`download_services`, `pending`, `preferences`, `search_enabled`.

### 2. Warm-start survives upgrades — `_deserialize_cache_state`

Drop the `envelope.get('zurgarr_version') != VERSION` rejection
(`utils/library.py:2535`). Keep:

- the `_LIBRARY_CACHE_SCHEMA` strict-int equality gate (the correct
  invalidation mechanism for real format breaks),
- all strict-type field validation,
- the future-`ts` clock-skew rejection and size cap,
- writing `zurgarr_version` into the envelope (diagnostic only).

Discipline note (existing, now load-bearing): any incompatible change to
the persisted cache shape MUST bump `_LIBRARY_CACHE_SCHEMA`.

### 3. Freshness — event-driven refresh audit

The user acts on the library right after grabs/deletes, so mutations must
schedule a background refresh. Existing triggers are kept:

- blackhole grab completion → `scanner.refresh()` (`utils/blackhole.py:3045`)
- six mutation endpoints in `utils/status_server.py` (delete,
  remove-local, remove-debrid, switch-to-debrid, download flows, manual
  `/api/library/refresh`)
- scan-effects invalidation: `_cache_time = 0` after enforcement changes
  files — which now means "next poll serves stale + starts background
  refresh" instead of "next poll blocks 53s"

Implementation includes an audit pass over mutation paths (pending
resolution, wanted-recovery adds, upgrade/compromise grabs — the latter
land via blackhole and are already covered) to confirm none lacks a
trigger. Preference changes need no scan trigger: preference display
comes from `get_all_preferences()` overlaid per-response, and effects are
applied by the next scan pass.

External changes made directly on the debrid (outside zurgarr) are
covered by the hourly scheduled scan — unchanged, accepted.

## Error handling

- A failed background refresh (mount missing, TB 429) leaves the previous
  snapshot in place — the UI keeps rendering last-known-good data, same
  as today's `refresh()` error path.
- Cold start with an unavailable mount: `refresh()` uses the existing
  mount-appeared-mid-scan rescan logic; the UI shows the empty payload
  with `scanning: true` until a scan lands.

## Testing

Unit tests (in `tests/`, run via `.venv/bin/pytest`):

- `get_data()` with expired TTL returns the stale cache without blocking
  and schedules exactly one background refresh.
- `get_data()` with `None` cache returns the empty payload immediately
  (no scan on the calling thread).
- Concurrent `get_data()` calls during a scan don't stack scans.
- Envelope with mismatched `zurgarr_version` but matching schema loads;
  mismatched schema still rejects (update existing persistence tests).
- Golden test: empty payload contains every key the UI destructures.

Live verification on prod (branch is production):

- `/api/library` stays fast (<200 ms) across a TTL-expiry boundary.
- After a rebuild with a version bump, the page renders the pre-rebuild
  snapshot instantly and refreshes in place.

Process: CHANGELOG entry; code-reviewer + bug-hunter agents before
declaring done.

## Out of scope

- SQLite backend (unnecessary at ~700 items; snapshot serves in 82 ms).
- Incremental/delta scanning (53s scan cost stops mattering once nothing
  blocks on it).
- Payload trimming/pagination/compression.

## Known trade-off

After a *schema* bump (rare, deliberate), the first page load shows an
empty library with "Refreshing…" for one scan duration (~53s). All other
loads — including after routine version bumps — render instantly from
snapshot.
