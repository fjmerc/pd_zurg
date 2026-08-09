# Library Load Never-Block Serving Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the ~53s library page hang by making `/api/library` always serve the last-known snapshot instantly and refresh in the background.

**Architecture:** Three surgical changes to `utils/library.py`: (1) `_deserialize_cache_state` stops rejecting persisted caches on version mismatch (schema gate stays), so warm-start survives release bumps; (2) `get_data()` never scans on the calling thread — it serves any-age cache (or an empty placeholder payload) and triggers a background `refresh()` when stale; (3) an audit confirms every library-mutating path already schedules a background refresh. No new env vars, no schema change, no UI change (the "Refreshing…" indicator + poll machinery in `library_page.py` already handles `scanning: true`).

**Tech Stack:** Python 3.11, stdlib only (threading, json). Tests: pytest via `.venv/bin/pytest`.

**Spec:** `docs/superpowers/specs/2026-08-09-library-load-design.md`

## Global Constraints

- Run tests with `.venv/bin/pytest` — system Python lacks deps.
- Never use raw `subprocess.Popen`; not needed here (no new processes).
- `utils/library.py` payload keys are exactly: `movies`, `shows`, `preferences`, `last_scan`, `scan_duration_ms`, `arr_degraded` (see `_scan_read` return, library.py:3677-3684). The empty placeholder must use the same key set.
- `self._lock` is a non-reentrant `threading.Lock`. `refresh()` acquires it — NEVER call `refresh()` while holding `self._lock` (deadlock).
- Update `CHANGELOG.md` before the final commit (repo commit checklist).
- After all tasks: run code-reviewer and bug-hunter agents before declaring done (house rule); fix every real bug they surface in the same change.
- Commit message style follows repo history: `Library: <what>` prefix, imperative.

---

### Task 1: Warm-start cache survives version bumps

**Files:**
- Modify: `utils/library.py:2521` (`_deserialize_cache_state` — remove the `zurgarr_version` equality rejection and its now-unused `_VERSION` import)
- Test: `tests/test_library.py:6644` (invert `test_version_mismatch_rejected`)

**Interfaces:**
- Consumes: existing `library._serialize_cache_state(cache, path_index, local_path_index, alias_norms)` → envelope dict; `library._deserialize_cache_state(envelope)` → `(cache, path_index, local_path_index, alias_norms) | None`; test helpers `self._sample_state()` / `self._make_scanner(path)` already defined in `TestLibraryCachePersistence`.
- Produces: `_deserialize_cache_state` accepts envelopes from any zurgarr version as long as `schema == _LIBRARY_CACHE_SCHEMA` and all strict-type checks pass. `zurgarr_version` remains in the written envelope (diagnostic only).

- [ ] **Step 1: Rewrite the version-mismatch test to expect acceptance**

In `tests/test_library.py`, class `TestLibraryCachePersistence`, replace the entire `test_version_mismatch_rejected` method (starts line 6644) with:

```python
    def test_version_mismatch_accepted(self, tmp_dir):
        """Routine release bumps must not discard the warm-start snapshot.

        ``_LIBRARY_CACHE_SCHEMA`` is the only format gate; the recorded
        ``zurgarr_version`` is diagnostic.  (Spec 2026-08-09: version-gated
        rejection caused an empty library + ~53s blocking scan after every
        release rebuild.)
        """
        import json
        cache, pi, lpi, an = self._sample_state()
        env = library._serialize_cache_state(cache, pi, lpi, an)
        env['zurgarr_version'] = '0.0.0-not-real'
        path = os.path.join(tmp_dir, 'library_cache.json')
        with open(path, 'w') as fh:
            json.dump(env, fh)
        scanner = self._make_scanner(path)
        scanner._load_persisted_cache()
        assert scanner._cache is not None
        assert scanner._cache['movies'] == cache['movies']
        assert scanner._path_index == pi
```

Keep `test_schema_mismatch_rejected` untouched — the schema gate stays.

- [ ] **Step 2: Run the new test to verify it fails**

Run: `.venv/bin/pytest tests/test_library.py::TestLibraryCachePersistence::test_version_mismatch_accepted -v`
Expected: FAIL — `scanner._cache is None` (loader still rejects the version mismatch).

- [ ] **Step 3: Remove the version rejection from `_deserialize_cache_state`**

In `utils/library.py`, function `_deserialize_cache_state` (~line 2513), delete these two lines (~line 2535):

```python
    if envelope.get('zurgarr_version') != _VERSION:
        return None
```

and delete the now-unused import at the top of the same function (~line 2521):

```python
    from version import VERSION as _VERSION
```

(`_serialize_cache_state` keeps its own `_VERSION` import — it still writes the field.) Update the function docstring's mention of validation if it references the version check; the docstring currently says "Strict-types throughout" which remains true.

- [ ] **Step 4: Run the persistence test class to verify all pass**

Run: `.venv/bin/pytest tests/test_library.py::TestLibraryCachePersistence -v`
Expected: ALL PASS (round-trip, schema-mismatch rejection, size cap, corrupt JSON, and the new acceptance test).

- [ ] **Step 5: Commit**

```bash
git add utils/library.py tests/test_library.py
git commit -m "Library: warm-start cache survives version bumps

_LIBRARY_CACHE_SCHEMA is the only format gate; rejecting on
zurgarr_version meant every release rebuild cold-started the library
into a ~53s blocking scan."
```

---

### Task 2: `get_data()` never blocks — stale-while-revalidate

**Files:**
- Modify: `utils/library.py:3740-3761` (`LibraryScanner.get_data`)
- Create (in same file): module-level `_empty_scan_payload()` helper, placed directly above `class LibraryScanner` (~line 2600)
- Test: `tests/test_library.py` (new class `TestGetDataNeverBlocks`, appended after `TestLibraryCachePersistence`)

**Interfaces:**
- Consumes: `self.refresh()` (already self-dedupes via `self._scanning`; sets `_scanning = True` synchronously before spawning its thread, so `is_scanning()` reports `True` immediately after the call); `self._cache` / `self._cache_time` / `self._ttl` / `self._mount_path` / `self._scanning` under `self._lock`.
- Produces: `get_data() -> dict` — never calls `scan()` on the calling thread; always returns a payload with the six standard keys. `_empty_scan_payload() -> dict` — the pre-first-scan placeholder. `status_server.py:1618` (`dict(scanner.get_data())` + `result['scanning'] = scanner.is_scanning()`) needs no change: a triggered refresh makes `scanning: true` appear in the same response.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_library.py`:

```python
# ---------------------------------------------------------------------------
# Never-block get_data (spec 2026-08-09-library-load-design)
# ---------------------------------------------------------------------------


class TestGetDataNeverBlocks:
    """/api/library must never block on a scan.

    ``get_data()`` serves whatever snapshot exists (any age) and triggers a
    background ``refresh()`` when stale; with no snapshot it serves an
    empty placeholder.  The synchronous ``scan()`` fallback is gone.
    """

    _PAYLOAD_KEYS = {
        'movies', 'shows', 'preferences', 'last_scan',
        'scan_duration_ms', 'arr_degraded',
    }

    def _make_scanner(self, mount='/mnt/debrid'):
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = mount
        scanner._local_movies_path = None
        scanner._local_tv_path = None
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._library_cache_path = '/nonexistent/library_cache.json'

        # Any scan on the get_data() calling thread is the exact bug this
        # change removes — fail loudly.
        def _no_scan(*a, **k):
            raise AssertionError('get_data() must never scan synchronously')
        scanner.scan = _no_scan

        # Stub refresh: count calls, mimic the real synchronous
        # _scanning=True publication (library.py refresh() sets it under
        # the lock before spawning its worker thread).
        scanner.refresh_calls = 0

        def _fake_refresh(_rescan_depth=0):
            scanner.refresh_calls += 1
            with scanner._lock:
                scanner._scanning = True
        scanner.refresh = _fake_refresh
        return scanner

    def _sample_payload(self):
        return {
            'movies': [{'title': 'Some Movie', 'year': 2024}],
            'shows': [],
            'preferences': {},
            'last_scan': '2026-08-09T10:00:00+00:00',
            'scan_duration_ms': 53241,
            'arr_degraded': [],
        }

    def test_fresh_cache_served_without_refresh(self):
        scanner = self._make_scanner()
        payload = self._sample_payload()
        scanner._cache = payload
        scanner._cache_time = time.monotonic()
        assert scanner.get_data() is payload
        assert scanner.refresh_calls == 0

    def test_expired_cache_served_stale_and_refresh_triggered_once(self):
        scanner = self._make_scanner()
        payload = self._sample_payload()
        scanner._cache = payload
        scanner._cache_time = time.monotonic() - 601  # ttl=600 → expired
        assert scanner.get_data() is payload          # stale, instantly
        assert scanner.refresh_calls == 1
        # Second poll while the (stubbed) refresh is "running" — no stack.
        assert scanner.get_data() is payload
        assert scanner.refresh_calls == 1

    def test_no_mount_uses_short_ttl(self):
        scanner = self._make_scanner(mount=None)
        payload = self._sample_payload()
        scanner._cache = payload
        scanner._cache_time = time.monotonic() - 11   # ttl=10 → expired
        assert scanner.get_data() is payload
        assert scanner.refresh_calls == 1

    def test_cold_start_serves_empty_payload_and_triggers_refresh(self):
        scanner = self._make_scanner()
        data = scanner.get_data()                     # would raise pre-change
        assert scanner.refresh_calls == 1
        assert set(data.keys()) == self._PAYLOAD_KEYS
        assert data['movies'] == []
        assert data['shows'] == []
        assert data['preferences'] == {}
        assert data['scan_duration_ms'] == 0
        assert data['arr_degraded'] == []

    def test_cold_start_while_already_scanning_no_extra_refresh(self):
        scanner = self._make_scanner()
        scanner._scanning = True                      # startup refresh running
        data = scanner.get_data()
        assert scanner.refresh_calls == 0
        assert set(data.keys()) == self._PAYLOAD_KEYS

    def test_empty_payload_matches_scan_read_key_set(self):
        """Golden: the placeholder's keys mirror _scan_read's return keys.

        status_server overlays scanning/download_services/pending/
        preferences/search_enabled per-response; everything else the UI
        destructures must exist here too.
        """
        assert set(library._empty_scan_payload().keys()) == self._PAYLOAD_KEYS
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `.venv/bin/pytest tests/test_library.py::TestGetDataNeverBlocks -v`
Expected: FAIL — `test_cold_start_*` raise `AssertionError('get_data() must never scan synchronously')`, `test_expired_*` likewise (current code scans synchronously on expiry); `test_empty_payload_matches_scan_read_key_set` fails with `AttributeError: _empty_scan_payload`.

- [ ] **Step 3: Implement `_empty_scan_payload` and rewrite `get_data`**

In `utils/library.py`, add directly above `class LibraryScanner` (~line 2600):

```python
def _empty_scan_payload():
    """Pre-first-scan placeholder served by ``get_data``.

    Key set MUST mirror ``_scan_read``'s return payload — the UI and
    ``/api/library`` consumers destructure these fields unconditionally.
    """
    return {
        'movies': [],
        'shows': [],
        'preferences': {},
        'last_scan': None,
        'scan_duration_ms': 0,
        'arr_degraded': [],
    }
```

Replace the body of `get_data` (library.py:3740-3761) with:

```python
    def get_data(self):
        """Return the library snapshot without ever blocking on a scan.

        Serves the in-memory cache at any age; a TTL-expired (or absent)
        cache triggers a background ``refresh()`` so the same HTTP
        response reports ``scanning`` and the UI's poll loop picks up
        fresh data when the scan lands.  ``refresh()`` is called outside
        ``self._lock`` — it acquires the same non-reentrant lock.
        """
        needs_refresh = False
        with self._lock:
            now = time.monotonic()
            ttl = self._ttl if self._mount_path else 10
            cache = self._cache
            if cache is not None and (now - self._cache_time) < ttl:
                return cache
            if not self._scanning:
                needs_refresh = True
        if needs_refresh:
            self.refresh()
        if cache is not None:
            return cache
        return _empty_scan_payload()
```

Note what disappears with the old body: the synchronous `self.scan()` call, the `_snapshot_indexes_for_persist()` call, and the `_persist_cache(...)` call. Persistence still happens — `refresh()` persists after its read phase (library.py:3789), and the scheduled `library_scan` / `enforce_source_preferences` tasks call `scan()` directly on their own threads (unchanged).

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `.venv/bin/pytest tests/test_library.py::TestGetDataNeverBlocks -v`
Expected: ALL PASS.

- [ ] **Step 5: Run the full library + scan-state test files for regressions**

Run: `.venv/bin/pytest tests/test_library.py tests/test_library_scan_state.py -q`
Expected: ALL PASS. If any existing test asserted the old synchronous-scan behavior of `get_data()`, update it to the new contract (stale-serve + background refresh) rather than reintroducing blocking.

- [ ] **Step 6: Commit**

```bash
git add utils/library.py tests/test_library.py
git commit -m "Library: get_data() never blocks — stale-while-revalidate

The HTTP path served a full synchronous scan (~53s in prod) whenever
the 600s TTL lapsed — 50 min of every hour, plus a duplicate scan on
cold start.  Now it always returns the last snapshot (or an empty
placeholder) and triggers a background refresh; the UI's existing
scanning indicator + re-poll picks up fresh data."
```

---

### Task 3: Freshness audit — every mutation path schedules a refresh

**Files:**
- Read/verify (modify only if a gap is found): `utils/status_server.py`, `utils/blackhole.py`, `utils/library.py`, `utils/scheduled_tasks.py`

**Interfaces:**
- Consumes: `get_scanner()` → `LibraryScanner | None`; `scanner.refresh()` (background, self-deduping).
- Produces: written confirmation (in the task report/commit message if a fix was needed) that all library-mutating paths trigger a refresh. Expected finding: **no code change needed** — this task exists to verify, not assume.

- [ ] **Step 1: Enumerate existing refresh triggers**

Run: `grep -n "scanner.refresh()" utils/*.py`
Expected sites (verify each still present):
- `utils/blackhole.py:3045` — after grab completion + symlink creation (covers upgrade/compromise grabs too; they land through blackhole)
- `utils/status_server.py:1781` — POST `/api/library/refresh` (manual button)
- `utils/status_server.py:2126`, `2137` — remove-local flows
- `utils/status_server.py:2186` — switch-to-debrid
- `utils/status_server.py:2317` — remove-debrid
- `utils/status_server.py:2447` — remove-debrid/confirm
- `utils/status_server.py:2590` — delete
- `utils/library.py:8348` — startup (`setup()`)

(Line numbers may drift a few lines after Task 2 — match by surrounding code, not exact number.)

- [ ] **Step 2: Verify scan-effects invalidation still routes through the new non-blocking path**

Read `utils/library.py` around the `refresh()` effects phase (`_cache_time = 0` after `_scan_effects` reports changes, ~library.py:3816) and `scan()`'s equivalent (~library.py:3725). Confirm: with Task 2 in place, `_cache_time = 0` means the next `get_data()` serves the existing snapshot AND triggers a background refresh — i.e., invalidation can no longer produce a blocking request. No code change; this is a reasoning check. Record the confirmation.

- [ ] **Step 3: Check mutation endpoints WITHOUT refresh triggers are correctly trigger-free**

Verify these paths need no scan trigger (content doesn't change until a later, already-covered event):
- `/api/library/preference` (status_server.py:1785) — preference display is overlaid per-response via `get_all_preferences()`; enforcement happens in the next scan-effects pass.
- `/api/library/pending` and `/api/library/download*` — they create pending state; actual content lands via blackhole grab completion (already triggers refresh at blackhole.py:3045).
- Wanted-recovery adds (TB/RD legs) — run inside scan effects; covered by the effects-phase invalidation from Step 2.

If any OTHER `/api/library/*` POST endpoint in `status_server.py` mutates on-disk library content (symlinks, deletions, moves) and lacks a `scanner.refresh()` call, add one following the exact pattern at status_server.py:2447:

```python
                from utils.library import get_scanner
                scanner = get_scanner()
                if scanner:
                    scanner.refresh()
```

- [ ] **Step 4: Run the status server tests for regressions (only if Step 3 changed code)**

Run: `.venv/bin/pytest tests/ -q -k "status or server or endpoint"`
Expected: ALL PASS. Skip if no code changed.

- [ ] **Step 5: Commit (only if Step 3 changed code)**

```bash
git add utils/status_server.py
git commit -m "Library: add missing refresh trigger on <endpoint> mutation"
```

If no gaps were found, no commit — report the audit result instead.

---

### Task 4: CHANGELOG, full suite, reviewers

**Files:**
- Modify: `CHANGELOG.md` (entry under the current unreleased version heading, matching existing `- **Bold title**: Description` format)

**Interfaces:**
- Consumes: everything shipped in Tasks 1-3.
- Produces: release-notes entry; green full test suite; reviewer sign-off.

- [ ] **Step 1: Add the CHANGELOG entry**

Under the current unreleased version heading at the top of `CHANGELOG.md` (read the file to find it — do NOT create a new version heading), add:

```markdown
- **Library page never blocks on scans**: `/api/library` now always serves the last-known snapshot instantly and refreshes in the background — previously the first request after cache expiry blocked on a full scan (~53s on a ~700-item library) for up to 50 minutes of every hour, and a duplicate scan could fire during cold start. The persisted warm-start cache also survives version bumps, so the library renders immediately after container rebuilds instead of starting empty.
```

- [ ] **Step 2: Run the full test suite**

Run: `.venv/bin/pytest -q`
Expected: ALL PASS (same pass/skip counts as master baseline aside from the new tests).

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "Library: changelog for never-block serving"
```

- [ ] **Step 4: Dispatch reviewers (house rule — mandatory)**

Dispatch the `code-reviewer` agent, then the `bug-hunter` agent, over the diff since the spec commit (`git diff 45493b3..HEAD`). Fix every real bug surfaced — in the same change, regardless of scope (repo commit checklist). Re-run `.venv/bin/pytest -q` after any fix.

- [ ] **Step 5: Manual prod verification (after user-approved deploy)**

Not automatable from this repo — coordinate with the user (prod = `ssh fray@192.168.1.8`, rebuild via `rebuild.sh filter-recovery-era`):
1. Before rebuild: note current library counts on the dashboard.
2. Rebuild; immediately open `/library` — expect the pre-rebuild snapshot to render instantly with the "Refreshing…" indicator, then update in place.
3. Wait >10 min (TTL expiry), open `/library` — expect instant render (<1s) with "Refreshing…" instead of a hang; time `/api/library` (expect <200ms).
