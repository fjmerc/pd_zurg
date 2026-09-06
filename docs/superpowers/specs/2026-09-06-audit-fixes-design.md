# 2026-09 Audit Fix Batch — Design

**Date:** 2026-09-06
**Status:** Approved by owner (chat), pending spec review
**Branch:** `audit-fixes-2026-09` (topic branch off `master`)

## Background

A proactive silent-gap audit on 2026-09-06 (e2e-flow-analyst + bug-hunter agents, all
findings verified against code with file:line evidence) surfaced ten findings after the
Plex-refresh scanner gap (c0c155c) prompted the question "what else did we miss." The
shared signature across the high-severity findings: the failure mode is *supervision or
coverage quietly stopping to exist* — nothing errors, the system degrades to "slow" or
"unmonitored," never to visibly "broken."

This spec covers fixing all ten findings in one batch: six commits on one topic branch,
each independently revertable, ordered so destructive-config fixes land first and the
highest-blast-radius change (SIGCHLD) lands after supervision is already fixed.

## Owner decisions (made during brainstorming)

1. **Finding #2 (mixed prefer-local data loss):** scope the delete server-side — only
   torrents whose episodes are all confirmed `both`-source get deleted. No polling, no
   dropped auto-chain.
2. **Finding #1 (SIGCHLD):** minimal reap — `SIG_DFL` plus a `WNOHANG` drain in the
   monitor tick. The residual race (an occasional exit code mislogged as 0) is accepted;
   no private CPython APIs (`Popen._handle_exitstatus`) are used.
3. **Finding #8 (CSRF):** the dashboard is accessed **behind a reverse proxy**, so the
   same-origin check ships with a `STATUS_UI_TRUSTED_ORIGINS` allow-list env var and
   full env plumbing.

## Commits

### Commit A — Truthiness trio (findings #3, #5)

Env-derived boolean strings compared by truthiness instead of the canonical
`str(VAR).lower() == 'true'` (CLAUDE.md invariant #1). A full-codebase sweep confirmed
these are the only three remaining violations.

- `main.py:107` — `z_updater.auto_update('Zurg', bool(ZURGUPDATE))` → pass
  `str(ZURGUPDATE).lower() == 'true'`.
- `main.py:113` — `if DUPECLEAN:` → `if str(DUPECLEAN).lower() == 'true':`.
- `main.py:123` — `if PDUPDATE and PDREPO:` → `if str(PDUPDATE).lower() == 'true' and PDREPO:`.
- `utils/duplicate_cleanup.py:171` — `and DUPECLEAN is not None` →
  `and str(DUPECLEAN).lower() == 'true'`.

Impact fixed: `DUPLICATE_CLEANUP=false` no longer runs cleanup (which, with
`DUPLICATE_CLEANUP_KEEP=zurg`, could delete local media); `ZURG_UPDATE=false` /
`PD_UPDATE=false` actually pin versions.

**Tests:** for each toggle, assert `'false'`, `''`, and unset disable the feature and
`'true'` enables it (parametrized; mock the downstream setup/auto_update calls).

### Commit B — Supervision fixes in `utils/processes.py` (findings #4, #6, #7)

**B1 — restart re-arms supervision (#4).** `stop_process()` intentionally sets
`restart_policy = None` to suppress the monitor during an intentional stop, but
`restart_process()` never restores it — after any SIGHUP config reload
(`utils/config_reload.py:221/:276`) or UI restart (`restart_service()`,
`processes.py:352`), the process is permanently unsupervised until container restart.
Fix: `restart_process(run_pre_restart=True, restore_policy=True)` — after a successful
relaunch, if `restart_policy is None`, re-arm with a fresh `RestartPolicy()` and reset
`_restart_count` / `_first_restart_time`.

**B2 — latch restart exhaustion (#6).** When `_restart_count >= policy.max_restarts`,
`_handle_restart` currently re-fires `_on_restart_exhausted()` (notification + error
event) on every 10s monitor tick, forever (~360/hr). Fix: `ProcessHandler._exhausted_notified`
flag; notify once, then return silently. Clear the flag wherever `_restart_count`
resets: the sliding-window reset (`processes.py:190-192`), `restart_service()`
(`processes.py:348`), and the B1 re-arm.

**B3 — no undrained pipes (#7).** `Popen` is created with `stdout/stderr=PIPE`
unconditionally (`processes.py:409-421`, `:463-475`), but reader threads start only
`if not suppress_logging` — with `ZURG_LOG_LEVEL=off` / `RCLONE_LOG_LEVEL=off` the child
deadlocks at ~64KB of output while poll/healthcheck/ismount all stay green. Fix:
`_out = subprocess.DEVNULL if suppress_logging else subprocess.PIPE` in both
`start_process` and `restart_process`; persist `self._suppress_logging` so restart
matches start.

**Tests:** (1) `restart_policy is not None` and counters reset after a
`stop_process → restart_process` cycle; (2) exhausted handler produces exactly one
notification across many simulated monitor ticks, and the latch clears on window reset
and manual restart; (3) Popen kwargs use DEVNULL when suppressed, PIPE otherwise, for
both start and restart.

### Commit C — SIGCHLD (finding #1)

`main.py:166` sets `signal.SIGCHLD` to `SIG_IGN`, which makes the kernel auto-reap
children; CPython's `subprocess` then maps `ECHILD` to `returncode = 0` for **every**
child — `check=True` never raises and every exit-status branch in the codebase is dead
(proven live against this repo's venv).

Fix (minimal-reap shape):

- `main.py`: `signal.signal(signal.SIGCHLD, signal.SIG_DFL)` with a comment explaining
  why `SIG_IGN` is forbidden.
- `utils/processes.py::_monitor_loop`: after all registered handlers are polled each
  tick, drain orphans reparented to PID 1:
  `while True: pid, _ = os.waitpid(-1, os.WNOHANG)` — break on `pid == 0` or
  `ChildProcessError`. Accepted residual race: a managed child exiting between its
  `poll()` and the drain may log exit code 0; restart behavior is unaffected.

**Re-verify all five consumer sites now that exit codes are real:**

1. `main.py:50-55` — the `umount -l` lazy fallback becomes reachable; verify shutdown
   messaging no longer claims success on failure.
2. `rclone/rclone.py:330` — `rclone obscure` with `check=True` can now raise
   `CalledProcessError`; wrap the call site so failure is **loud** (error log naming the
   consequence — credentials would be omitted from rclone.config) instead of silently
   producing an empty obscured password. Do not log the password itself.
3. `zurg/setup.py:144` — the version-check error branch becomes reachable; confirm the
   failure path logs and leaves `ZURG_CURRENT_VERSION` sane.
4. `rclone/rclone.py:186` — leftover-mount detection now sees real codes; confirm no
   behavior regression.
5. `utils/processes.py:156` — crash exit codes now log truthfully; confirm restart/
   backoff logic behaves with nonzero codes.

**Tests:** unit test the drain loop (spawn a real child, orphan-drain reaps it, no
zombie); a test that fails under `SIG_IGN` and passes under `SIG_DFL`
(`subprocess.run(['false'])` returns nonzero after signal setup); a
`CalledProcessError` path test for the obscure wrapper.

### Commit D — Scoped prefer-local delete (finding #2)

Today the TV "mixed" case of `applyPreference()` (`utils/library_page.py:3520-3533`)
triggers async Sonarr searches for debrid-only episodes, then immediately calls
`_postRemoveDebrid(title, year)` → `/api/library/remove-debrid`, whose matching
(`utils/debrid_client.py:58-115`) is **title + optional year only**. Torrents backing
debrid-only episodes (and unrelated debrid-only seasons of the same show) get deleted
seconds after a search that may take hours; if the re-grab fails, those episodes have no
copy anywhere, and the hourly gap-fill re-search masks the loss.

Fix (server-side scoping — defense in depth, not just JS). *Amended 2026-09-06 during
plan writing, after reading the real code:*

- **The server derives the unsafe set itself** — `LibraryScanner.debrid_only_episodes(norm)`
  returns the (season, episode) keys present in the debrid path index with no local
  counterpart (aliases and year-qualified sibling norms included). Nothing is trusted
  from the client; this is authoritative and mirrors the safety rule the scheduled
  `_enforce_preferences` already applies (`has_both and not has_debrid_only`,
  `library.py:4267-4286`).
- **Episode claims are parsed from the torrent's own name**, not `files[].name`:
  `DebridClientBase.list_torrents()` carries no per-file data, and adding per-torrent
  file-listing API calls would violate the TB request-throttling rules. Claims parse as
  episode-specific (`SxxEyy`, multi-ep, ranges), season-pack (`S01`, `Season 1`,
  `S01-S03`), or no-claim (whole-show / unparseable) — no-claim **fails closed** (kept
  whenever any debrid-only episode exists).
- A torrent is deleted **only if its claim touches no debrid-only episode**. Kept
  torrents are reported with a `kept_reason` ("only debrid copy of SxxEyy…") so the
  confirm dialog and result toast are truthful. The confirm endpoint re-derives the
  whole decision server-side against a fresh provider listing (requested ids absent
  from the fresh listing are refused, fail closed); `type` (`show`/`movie`) becomes a
  required field on confirm.
- The auto-chain (search-then-remove) stays: with scoping it is safe. The surviving
  duplicates are cleaned up later by the existing scheduled
  `enforce_source_preferences` pass once downloads land — no new polling machinery.
- Absent/empty `safe_episodes` on a show-type request: fail closed (delete nothing,
  return an explanatory error) rather than falling back to blind title-level deletion.
  Movie-type requests are single-item and keep current behavior; audit the Radarr path
  for parity but expect no change (no per-episode mixed state exists).

**Tests:** regression scenario — show with S1E1-5 `both` + S1E6-8 `debrid`-only: delete
request touches nothing backing E6-8; season pack spanning both sets is kept; movie path
unchanged; empty `safe_episodes` fails closed.

### Commit E — Observability wiring (finding #9)

Manual UI paths perform the same destructive operations as the scheduled
`enforce_source_preferences` task but emit none of its history events or notifications.

- `/api/library/delete` (`utils/status_server.py:2679-2687`): add
  `notify('arr_deleted', ...)` next to the existing `log_event` — making the event
  already advertised in `ALL_EVENTS` (`notifications.py:47`) and the Settings help text
  (`settings_api.py:239`) actually fire.
- `/api/library/remove-local`, `/api/library/remove-debrid/confirm`, and
  `/api/library/switch-to-debrid`: add `history.log_event` + `notify(...)` mirroring the
  scheduled path's `switched_source` / `library_refresh` emissions
  (`utils/library.py:4140-4161`, `4250-4261`, `4319-4334`), with metadata distinguishing
  manual origin.
- Overseerr fallback `ensure_and_request_tv` / `ensure_and_request_movie`
  (`utils/arr_client.py:2424-2479`): add `history.log_event('search_triggered', ...)`
  for parity with the Sonarr (`arr_client.py:1145`) and Radarr (`arr_client.py:2136`)
  direct-search paths.

Every new `log_event` carries a `meta['cause']` slug per CLAUDE.md: reuse existing
`CAUSE_*` slugs where one fits; any new slug gets the full three-place wiring —
`CAUSE_*` constant in `utils/history.py`, Python formatter in
`activity_format._CAUSE_FORMATTERS`, matching `F` entry in `FORMATTER_JS` — so the
per-cause golden tests in `tests/test_activity_format.py` pass.

**Tests:** each endpoint emits its event + notification (mock `notify`/`log_event`);
golden tests for any new cause slug.

### Commit F — HTTP hardening (findings #8, #10)

**F1 — same-origin gate.** `do_POST` / `do_DELETE` in `utils/status_server.py` gain a
check after authentication: if an `Origin` header is present (falling back to the
`Referer` origin), the request is rejected 403 unless the origin's netloc matches the
`Host` header **or** appears in `STATUS_UI_TRUSTED_ORIGINS` (comma-separated origins,
e.g. `https://zurgarr.example.com`). Requests with neither header (curl, scripts,
integrations) remain allowed. This closes the `<form enctype="text/plain">` CSRF vector
against `/api/library/delete`, `/api/settings/env`, `/api/restart/*`, etc.

New env var `STATUS_UI_TRUSTED_ORIGINS` — full plumbing per the CLAUDE.md checklist:
`base/__init__.py` (Config + `__all__` + globals), `utils/settings_api.py` (UI schema;
no `_ENV_DEFAULTS` entry — default empty), `CONFIGURATION.md` (table entry),
`.env.example` (commented template), plus a TROUBLESHOOTING entry keyed by symptom
("UI buttons fail with 403 behind a reverse proxy").

**F2 — atomic_write durability.** `utils/file_utils.py::atomic_write` gains
`tmp_file.flush()` + `os.fsync(tmp_file.fileno())` before `os.replace()`, and a
directory fsync after, matching its "crash-safe" docstring. Controlled by a new
`fsync=True` kwarg; the hot `tmdb_cache.json` write path passes `fsync=False`.

**F3 — `/api/logs` param hardening.** `int(params.get('lines', ['100'])[0])`
(`status_server.py:1525-1526`) wrapped in try/except defaulting to 100, then clamped
`max(1, min(lines, 1000))` — closes both the unhandled `ValueError` connection drop and
the negative-value passthrough.

**Tests:** same-origin matrix (no Origin allowed; matching Host allowed; mismatched
rejected 403; trusted-origins allowed; Referer fallback); fsync called when enabled;
`?lines=abc` and `?lines=-5` both return 200 with sane behavior.

## Process

- TDD per fix: failing test first, then the change (superpowers:test-driven-development).
- After all six commits: full `.venv/bin/pytest`, `CHANGELOG.md` entries per fix under
  the current unreleased version.
- Mandatory review pass before declaring done: code-reviewer, then bug-hunter, then the
  app-testing agent. Any real bug a reviewer surfaces gets fixed in the same batch
  (CLAUDE.md commit checklist).
- Merge to `master` and prod deploy (`rebuild.sh master` on plex-host) are the owner's
  call after review.

## Out of scope

- The PLAUSIBLE blackhole watcher lock-split across SIGHUP reload (needs a
  timing-instrumented repro first; revisit separately).
- The `library_refresh` event-granularity nit (finding #6 of the flow audit) — cosmetic,
  not wired into this batch.
- Full status handoff for the SIGCHLD race (explicitly declined in favor of minimal
  reap).
