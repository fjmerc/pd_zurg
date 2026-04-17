# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).



## Version [2.17.8] - 2026-04-17

### Added

- **Plex-style library detail page**: Library detail views now surface a compact `year · runtime · rating` meta row, a genres line, TMDB score badge, director (movie) or creator (show) byline, and a horizontal `Cast & Crew` photo strip of up to 15 top-billed actors. The cast scroller uses a hidden scrollbar (scroll via mouse wheel, trackpad, or drag) to match modern media UIs. The Activity panel moved from a right-hand sidebar to a full-width section below the seasons/cast, so it no longer floats awkwardly next to short content or wastes column space next to long shows. Powered by the existing TMDB cache — existing cache entries refresh transparently over 7 days as users browse, bulk lookups keep using legacy entries until the refetch lands (no regression during the migration window). Set `TMDB_RATING_COUNTRY` (default `US`) to pick MPAA/content ratings from a non-US country.

### Fixed

- **Library back button restores scroll position**: Clicking "← Back to Library" from a movie or show detail view previously scrolled all the way back to the top of the A's, forcing the user to re-scroll to find where they were. The library page now captures `window.scrollY` when opening the detail view and restores it after the grid re-renders on back, so the user lands exactly where they clicked. Also adds `preventScroll:true` to the search-input refocus call so the input no longer pulls the viewport when the detail view closes.
- **Blocklist no longer blocks replacement releases**: Blocklisting a specific release (e.g. a buffering 2160p) previously blocked ALL versions of the same title, preventing the library scanner from creating symlinks for a replacement release (e.g. a 1080p). The blocklist check in debrid symlink creation now matches on the full release folder name instead of the parsed title, so different quality releases of the same movie or show are handled independently.
- **Broken debrid symlinks are now auto-cleaned**: When a debrid torrent is removed or replaced, symlinks in the local library pointing to the old content became permanently broken — causing Zurg 404 errors and leaving arrs in a `hasFile: false` state. The library scanner now detects and removes broken debrid symlinks before creating new ones, allowing replacement content to be linked automatically in the same scan cycle.
- **Symlink verify no longer refuses to clean large backlogs**: The scheduled `verify_symlinks` task had a safety threshold that refused to delete broken symlinks when >50 were found and >50% appeared broken. This created a self-reinforcing cycle where broken symlinks accumulated past the threshold and could never be cleaned. Replaced the threshold with a proper mount health guard (non-empty mount listing), which is the correct protection against mass deletion during mount failures.

## Version [2.17.7] - 2026-04-14

### Added

- **Blackhole per-arr label routing**: pd_zurg can now route completed symlinks into per-arr subdirectories, preventing Sonarr from logging orphan "Directory not empty" warnings for Radarr-submitted movies (and vice versa). Drop a `.torrent` into `BLACKHOLE_DIR/sonarr/` and the symlink lands in `BLACKHOLE_COMPLETED_DIR/sonarr/`; Radarr works the same way under `radarr/`. Opt-in via directory layout — no new env var, and flat layout remains fully supported. Also supports custom labels (e.g. `sonarr-4k`, `sonarr-hd`, `readarr`) for multi-instance setups. See `BLACKHOLE_SYMLINK_GUIDE.md` for the labeled-layout setup and migration steps.
- **Rclone dir cache flush on demand**: Rclone's RC API is now enabled automatically. The blackhole flushes the dir cache before waiting for new files on the mount, and the library scanner flushes before FUSE fallback scans. This eliminates the 30-minute wait for `RCLONE_DIR_CACHE_TIME` expiry when Zurg adds or removes torrents.
- **Early detection of BluRay disc rips**: The blackhole now inspects the debrid file list immediately after a torrent is ready, before the 300-second mount wait. Torrents containing only non-media files (e.g. `.m2ts` BluRay rips) are auto-blocklisted and deleted from debrid, allowing the arr to grab a different release. Works across all three debrid providers (RealDebrid, AllDebrid, TorBox). The library scanner also sweeps the mount hourly for existing disc rips and cleans them up automatically.
- **Dashboard shows blackhole and local-library mounts**: The status dashboard previously only listed rclone debrid mounts under `/data/`. It now also lists the blackhole watch and completed directories, plus the local-library TV and movie paths, each tagged with its role (Debrid / Blackhole / Local Library). Rows with missing-on-disk paths surface as "Missing" so a misconfigured bind mount is obvious at a glance instead of being invisible.

### Fixed

- **Dashboard System circles no longer touch on mobile**: The System card's Memory/CPU/Disk stat rings were fixed at 140×140px while their mobile containers shrank to ~92px, causing the rings to overflow and visibly touch on phones. Rings now scale fluidly within their container (capped at 140px) and the inner value font shrinks to match, keeping the three circles cleanly spaced on narrow viewports.
- **Processes status no longer wraps on mobile**: The Status cell in the Processes table showed the green/red dot on one line and the "Running"/"Stopped" label on the next when the column got narrow. The cell is now `white-space:nowrap`, keeping the dot and label inline on all viewports.
- **Processes and Mounts status columns are now centered**: The Status column in both the Processes and Mounts tables was left-aligned, which made the dot+label drift toward the left edge and feel disconnected from its narrower header. Both cell and header are now centered, matching the PID/Restarts convention, and Mounts status labels (e.g. "Not mounted", "Not accessible") also stay on a single line.
- **Activity Time and Type columns centered**: The Time and Type headers sat flush-left above their narrow, visually-symmetric cell contents (timestamps and colored badges). Both headers and the matching cells now center, so the columns read as tidy vertical stacks.
- **Blocklist Hash header and Source cell centered**: On the Blocklist tab the Hash header is now centered over its narrow column, and the Source column's auto/manual badge is centered within the cell so the badge sits visually under the header.
- **Confirm dialogs now center on screen**: Dialogs opened via `showConfirm()` (e.g. the Clear button on the Activity and Blocklist tabs) were rendering in the top-left corner because the global `*{margin:0}` reset in the base CSS overrode the browser's default `<dialog>{margin:auto}` centering. Added an explicit `margin:auto` to the dialog style so modals center correctly in both dark and light themes.
- **Activity titles hyperlink to the library detail page**: Canonical show/movie names in the Activity table are now clickable links that open the corresponding library detail page (e.g. clicking "Breaking Bad" on a Rescan row jumps to `/library?detail=Breaking+Bad&type=show&from=activity`). Linking is enabled whenever the event was enriched with `media_title` or the event came from the library scanner (where `title` is already canonical); raw torrent filenames and scheduler task names stay plain. The library page's URL restorer also now falls back to the other list when the `type=` hint points to the wrong tab, and bypasses the saved-filter-dependent `_displayedItems` lookup so a detail link still opens even when the user's persisted source/status/year filters would have hidden the target from the grid. When the link carries `from=activity`, the detail view's back button becomes "← Back to Activity" and routes back to the Activity page instead of the library grid.
- **Confirm dialogs no longer overflow on narrow phones**: The `max-width:380px` dialog rule could clip horizontally on viewports under ~380px. Switched to `max-width:min(380px,calc(100vw - 32px))` so the dialog always fits the viewport with a 16px gutter on each side.
- **Activity filter no longer shows empty results from stale page state**: Changing the event type filter or search query while paged deep into a large "All Types" result set (e.g. page 25 of 48) would keep the same page index and hand the server a page that was past the end of the now-smaller filtered set, so the API returned no rows and the UI rendered "No activity recorded yet" — making rarer event types like Failed / Cleanup / Source Switch / Blocklisted / Auto-Blocked look like they were never being logged. The filter dropdown, search input, clear-history action, and Escape-to-clear handler now explicitly reset to page 1, so filter changes always show the first page of matching events.
- **Activity table shows canonical media titles**: Rescan and search events were logged with technical identifiers like "Sonarr series 123" / "Radarr movie 456" / "Sonarr episodes [1,2]" in the `title` field while the real show/movie name sat in `media_title`. The Activity table now prefers `media_title` for display and falls back to `title` for legacy rows and events without enrichment (e.g. raw torrent filenames from the blackhole), so "Rescan" rows finally show "Breaking Bad" instead of a database ID.
- **Mobile hamburger button feels lighter and less intrusive**: The sidebar toggle was rendered as a card-colored tile with uneven padding and a baseline-aligned icon, making it feel like a second floating card sitting on top of every page. It now uses the page background with a thin border, equal 7px padding for a clean square, a block-level SVG to drop baseline space, and a subtle blue hover/focus accent. The mobile page top padding was also trimmed from 48px to 44px so content sits closer to the button without being covered by it.
- **Show detail no longer auto-expands most recent season**: The show detail page previously force-expanded the first (most recent) season regardless of state. Seasons now only auto-expand when they have active state — pending searches, incomplete episode counts, or episodes airing within 30 days.
- **BluRay disc rips miscategorized as shows**: Zurg's `has_episodes` filter incorrectly matches BluRay disc rips (numbered `.m2ts` files) as TV shows, placing them in `shows/` instead of `movies/`. pd_zurg now reclassifies items in the shows category as movies when they contain no recognizable media files (extensions not in `MEDIA_EXTENSIONS`). Items with valid media files but non-standard naming (e.g. anime) still respect Zurg's category hint. This fixes cross-source matching — previously, a local movie copy couldn't merge with its debrid counterpart because they were in different type lists (movie vs phantom show with 0 episodes), causing the title to stay stuck on "Local" source.
- **System Tasks table stacks into cards on mobile**: The scheduled-tasks table on the System page packed 7 columns (Task, Interval, Last Run, Duration, Result, Next Run, Actions) with no fixed widths, so long task identifiers like `duplicate_cleanup_sweep` pushed the row offscreen on phone viewports. Under `@media(max-width:600px)` each task now renders as a bordered card with the task name on top, the run result on its own line, a muted meta row with interval/last/next/duration, and the Run button bottom-right with a larger touch target. Flex `order` handles visual sequencing so the JS row renderer stays untouched.
- **System Config key/value table stacks on mobile**: The running-configuration table set `white-space:nowrap` on monospace keys, so a long `RCLONE_POLL_INTERVAL`-style key plus its value overflowed horizontally on phones. Each config row now stacks the key as a small uppercase label above the value, with `word-break:break-all` on the value so long paths wrap cleanly.
- **Settings list rows no longer overflow on mobile**: `.list-row` flex containers (used for env-var list and pair inputs) left their inputs at default `min-width:auto`, so a long URL or path kept the input at content width and pushed the delete button offscreen on narrow viewports. Inputs inside `.list-row` now set `min-width:0` under `@media(max-width:600px)` so they compress to the available flex track. The OAuth device-code display also shrinks from 1.8em to 1.3em with tighter letter-spacing on phones so 9-character codes fit the panel.
- **Activity tabs stack into cards on mobile**: The History and Blocklist tables overflowed horizontally on phone-width viewports (~375-414px) because their fixed-width columns (Time/Type/Source on History; Hash/Date/Source/Actions on Blocklist) left almost no room for Title and Detail/Reason. Both tabs now switch to a card-stacked layout under `@media(max-width:600px)`: History cards show a meta header (time + type badge + source) above the title and detail; Blocklist cards show title and reason on top, a meta row with hash/date/source below, and a larger-touch-target Remove button at the bottom-right. The tap-to-copy hash behavior and library-detail hyperlinks on History titles are preserved. Desktop layout is unchanged.
- **Blackhole Phase 2 no longer times out waiting for visible new torrents**: When Zurg ingested a new torrent the FUSE mount could stay stale for the full 300s mount-wait, even though the torrent was clearly cached and served via WebDAV. Two compounding causes: (1) rclone was started with `--poll-interval=0`, so it never actively diffed the backend, leaving the kernel FUSE dentry cache to decide when to revalidate on its own; (2) the blackhole called `vfs/forget` once before the poll loop, but `vfs/forget` only clears rclone's in-process VFS — it does not emit kernel `FUSE_NOTIFY_INVAL_ENTRY`, so the kernel kept returning cached listings for up to `--dir-cache-time`. Rclone mounts now run with `--poll-interval=15s` (tunable via `RCLONE_POLL_INTERVAL`) so the backend is actively diffed and kernel invalidations are emitted automatically, and the blackhole kicks an immediate `vfs/refresh` at the start of Phase 2 to catch the new torrent before rclone's next poll tick.

## Version [2.17.6] - 2026-04-13

### Fixed

- **Delete now fully cleans up all artifacts**: Deleting a show/movie from the library now also removes debrid torrents, local library symlinks, preferences, pending state, and TMDB cache entries. Previously, only the Sonarr/Radarr entry was deleted, leaving orphaned symlinks and debrid content that caused the title to reappear on the next library scan.

## Version [2.17.5] - 2026-04-12

### Fixed

- **Year-aware debrid symlink matching**: Torrents with year-disambiguated titles (e.g. "The Bridge 2013") now correctly match the right Sonarr/Radarr series when multiple same-title entries exist (e.g. "The Bridge (2011)" vs "The Bridge (2013)"). Previously, the parsed year was discarded during arr lookup, causing symlinks to land in whichever series was indexed first.
- **Year-aware TMDB cache keys**: Same-title-different-year shows/movies now get distinct TMDB cache entries (e.g. "the bridge (2011)" vs "the bridge (2013)") instead of colliding on a yearless key. Fixes wrong posters and metadata for year-disambiguated titles. Existing yearless cache entries are still found via fallback and expire naturally after 7 days.
- **SIGHUP reload no longer clobbers docker-compose env vars**: The config reload was clearing environment variables set via docker-compose (not in `.env`) because the removal detection compared against `os.environ` instead of the previous `.env` contents. This broke rclone mounts, blackhole, and other services after a SIGHUP. Now only tracks and removes keys that were actually in `.env`.

## Version [2.17.4] - 2026-04-07

### Added

- **"Airing Today" episode badge**: Episodes with an air date matching today now show an amber "Airing Today" badge instead of the misleading red "Missing" badge. Tomorrow's episodes remain "Upcoming" (blue) and past-date episodes remain "Missing" (red).
- **History data enrichment**: History events from the blackhole (grabbed, cached, failed, symlinked) and arr client (search/rescan triggered) now include canonical media titles via a new `media_title` field. Previously these events used torrent filenames or technical IDs, making them invisible on show/movie detail pages.
- **Activity timeline sidebar**: The detail page now features a sticky sidebar on the right with a timeline-style activity feed. Events are grouped by day ("Today", "Yesterday", etc.) with colored dots by category (acquisition/failure/action/management), Unicode icons, and episode badges. Replaces the old collapsed history section at the bottom of the page.

### Changed

- **Detail page layout**: Widened from 900px to 1200px max-width. Content below the hero is now a two-column layout: main content (seasons/actions) on the left, activity sidebar on the right. Collapses to single column on mobile (<768px).
- **History sidebar auto-loads**: Activity timeline loads automatically when opening a detail page instead of requiring a click to expand.
- **System events filtered**: Scheduler events (Library Scan, Housekeeping, Stale Grab Detection) and startup blocklist-skip events are excluded from the detail page timeline to reduce noise.

## Version [2.17.3] - 2026-04-06

### Added

- **Blocklist expiry**: New `BLOCKLIST_EXPIRY_DAYS` setting auto-expires auto-added blocklist entries after N days (default: 0/disabled). Manual entries are kept forever. Runs during daily housekeeping.
- **Expanded config backup**: Daily backup now includes `blocklist.json` and `history.jsonl` alongside `.env`, `settings.json`, and `preferences.json`.

### Fixed

- **Shutdown/reload race condition**: SIGHUP config reload now checks the `_shutting_down` flag before restarting services, preventing a race where reload could spawn new processes after shutdown cleanup.
- **Dependency-aware process restarts**: The process monitor now defers rclone restarts when Zurg is down (and plex_debrid when rclone is down) instead of consuming retry budget against a dead dependency.
- **Orphaned debrid torrents on crash**: Blackhole pipeline now writes to `pending_monitors.json` immediately after debrid submission, before file cleanup. Previously a crash between submission and pending write would leave an untracked torrent in the debrid account.
- **ARCHITECTURE.md accuracy**: Fixed misleading `.replace()` pseudocode (actual code uses `startswith` + slice), corrected notification threading description (synchronous, not per-call threads), added missing history rotation to housekeeping docs, documented `STATUS_UI_AUTH`, Docker `HEALTHCHECK`, and `while True` signal loop.

## Version [2.17.2] - 2026-04-05

### Added

- **Pending failure context**: Pending entries now track `last_error`, `retry_count`, and `next_retry_at` fields. The library UI shows error reasons, retry counts, and time-until-next-retry for items stuck in "Searching" state, replacing the opaque "Searching" badge with actionable context.
- **Pending warning notifications**: New `pending_warning` notification event fires once when items are stuck searching for 24+ hours (configurable via `PENDING_WARNING_HOURS` env var, default 24), bridging the silent gap between first search and 3-day escalation.
- **Blackhole alt-exhaustion notification**: When all alternative releases are exhausted for a blackhole item, a `download_error` notification is now sent and a history event logged. Previously the file moved to `failed/` with no user signal.
- **Architecture guide**: Added `ARCHITECTURE.md` — a developer reference documenting module layers, data flows, cross-container path model, threading model, scheduled tasks, and error recovery patterns.
- **Rclone VFS cache configuration**: `RCLONE_VFS_CACHE_MODE`, `RCLONE_VFS_CACHE_MAX_SIZE`, and `RCLONE_VFS_CACHE_MAX_AGE` are now configurable via environment variables and the settings UI. Previously `--vfs-cache-mode` and `--dir-cache-time` were hardcoded, ignoring user-set values.
- **`BLOCKLIST_AUTO_ADD` in settings UI**: The auto-blocklist toggle is now configurable from the Blackhole section of the settings editor instead of requiring manual `.env` edits.

### Fixed

- **Radarr retry count inflation**: Fixed "No debrid results found" on the Radarr path incorrectly incrementing `retry_count` on every scan cycle, causing misleadingly high failure counts in the UI. Now matches the Sonarr path (`increment_retry=False`).
- **Pending warning notification reliability**: `warned_at` is now persisted only after the notification is successfully sent. Previously, a failed notification would permanently mark the item as warned, silently skipping the alert with no retry.
- **Attribute XSS in library UI**: Replaced all `esc()` calls in HTML attribute contexts with `escAttr()` (which also escapes `"` and `'`). The previous `esc()` function only escaped `<`, `>`, and `&`, leaving attribute breakout possible via crafted titles — especially in the torrent search results table where data is attacker-controlled.
- **Version string**: Fixed the version reported at startup (was stuck at `2.11.0`).
- **Settings URL validation**: Sonarr, Radarr, and Torrentio URLs are now validated for correct format (must start with `http://` or `https://`) when saving settings, matching the existing validation for Plex and Overseerr URLs.
- **`MEDIA_EXTENSIONS` naming consistency**: Renamed `_MEDIA_EXTENSIONS` in `scheduled_tasks.py` to `MEDIA_EXTENSIONS` to match `library.py` and `blackhole.py`, reducing risk of the three sets drifting out of sync.
- **`LIBRARY_PREFERENCE_AUTO_ENFORCE` undocumented**: Added to README.md configuration reference.

## Version [2.17.1] - 2026-04-04

### Added

- **Multi-season torrent splitting**: Blackhole symlink mode now auto-detects multi-season packs (e.g., "Show.S01-S05.1080p") and splits them into per-season directories with Sonarr-parseable names. Supports `S01-S05`, `S01-05`, `Seasons 1-5`, `Complete Series/Collection`, and cross-season episode ranges. Falls back to single-directory behavior when files lack parseable season info.
- **Extended system metrics**: The Status dashboard System card now displays disk space (`/config` volume) as a third ring chart alongside Memory and CPU, plus a compact info row showing container uptime, open file descriptors, and live network I/O rates. All new metrics include health indicator thresholds (>60% warn, >85% critical) and Prometheus gauge exports.
- **Local library mount health monitoring**: Mount liveness probe now checks local library paths (movies/TV) for real (non-symlink) media files. When a network mount (NFS/SMB) drops silently, the probe detects the absence of real files within ~60 seconds and sends a `health_error` notification.
- **Library scanner mount-drop alert**: The library scanner now tracks whether local content was previously found. If local items drop to zero after being present, it logs a warning and sends a one-time `health_error` notification instead of silently skipping symlink creation.
- **Symlink repair worker**: Broken debrid symlinks are now repaired before deletion. When content moves between Zurg mount categories (e.g., `movies/` → `shows/`), the symlink is automatically recreated with the correct path. When content is truly gone, an optional `SYMLINK_REPAIR_AUTO_SEARCH` setting triggers Sonarr/Radarr to re-search, sharing the existing 2-hour cooldown to prevent search storms. Repair activity is logged to history and sent via the new `symlink_repaired` notification event.

### Changed

- **Larger system metric rings**: Increased the Status dashboard System ring chart size from 110px to 140px so longer values (e.g., "1105.5s", "75.6G / 116.3G") fit within the rings without clipping.

### Fixed

- **Prefer-debrid search retry**: Debrid migration searches that silently fail (e.g., all indexers down, no results) are now automatically retried every 6 hours instead of being permanently skipped. Previously, a single failed search attempt would leave the title stuck in "Migrating to Debrid" until the 3-day escalation timeout.
- **Untagged torrent indexers invisible to debrid series**: Sonarr/Radarr v4 requires indexers to share a tag with the series — untagged indexers are no longer universal. `_fix_indexer_routing` now adds the debrid tag to untagged torrent indexers, not just those already carrying the local tag. Previously, debrid-tagged series saw "0 active indexers" and searches silently returned nothing.
- **Prefer-debrid skipped for episodes with existing files**: Sonarr/Radarr won't re-download episodes that already meet the quality cutoff, even when the download client routing changes to debrid. `ensure_and_search` now uses interactive search + manual push to bypass the cutoff when `prefer_debrid=True` and episodes already have files, force-grabbing a torrent release through the blackhole.
- **Season-aware TMDB show matching**: Fixed a bug where shows sharing a common title across reboots/revivals (e.g. Netflix "Marvel's Daredevil" vs Disney+ "Daredevil: Born Again") could receive the wrong poster, metadata, and Sonarr folder assignment. When the primary TMDB cache entry doesn't cover the show's season range, the system now searches for alternative cache entries with matching title words that do cover the needed season.
- **Blocklist modal**: Replaced native browser `prompt()` dialog with a styled in-app modal for blocking torrents. Preset reasons are now clickable buttons with a custom reason input field, matching the application's dark theme.
- **Migrating badge alignment**: Fixed vertical misalignment between source badge and "Migrating" badge in episode rows by adding `vertical-align:middle` and proper spacing.
- **Episode table mobile responsiveness**: Hid torrent filenames and file sizes on small screens, moved air dates below episode titles, added flex-wrap to season header buttons, and tightened cell padding for a cleaner mobile layout.
- **Sonarr/Radarr history API compatibility**: The `detect_stale_grabs` task no longer sends the `eventType` query parameter to `/api/v3/history`, which older Sonarr/Radarr versions reject with HTTP 400. Filtering is now done client-side for universal compatibility.

## Version [2.17.0] - 2026-04-03

### Added

- **Sidebar navigation**: Replaced the horizontal top navigation bar with a fixed left sidebar inspired by Sonarr/Radarr. Includes SVG icons, active-state left border accent, dark/light theme support, and a mobile hamburger menu with slide-out overlay.
- **Activity page**: New dedicated page at `/activity` with two tabs — History (event log with type filter, search, pagination) and Blocklist (manage blocked torrents). Previously these were buried at the bottom of the dashboard.
- **System page**: New dedicated page at `/system` with three tabs — Logs (level filter, search, wrap/auto-scroll), Tasks (scheduled task management with manual run), and Config (running configuration + "How it Works" reference). Previously these required scrolling through the entire dashboard.
- **Shared JS utilities**: Common functions (`esc`, `timeAgo`, `fmt`, `fmtBytes`, `showConfirm`, auth detection) extracted into a shared module available to all pages, eliminating duplication.

### Changed

- **Dashboard slimmed to Status page**: The dashboard now shows only health-critical information — services, processes, mounts, system stats, and recent events. Activity, blocklist, logs, tasks, and config have moved to their dedicated pages.
- **Wanted sidebar highlighting**: Navigating to the library with `?filter=missing` now highlights "Wanted" in the sidebar instead of "Library".

## Version [2.16.2] - 2026-04-03

### Fixed

- **Movie prefer-debrid auto-enforcement**: Movies with `prefer-debrid` preference now automatically replace the local file with a debrid symlink when `source=both` is detected during library scans. Previously only TV shows had this auto-enforcement — movies required a manual second click.
- **Blackhole dedup bypass for prefer-debrid**: The blackhole local dedup check now respects source preferences. Previously it would reject debrid grabs for titles that already existed locally, blocking the prefer-debrid workflow entirely.
- **Stale source badge after enforcement**: After auto-enforcement replaced a local file with a debrid symlink, the UI continued showing "Local & Debrid" until the next scan. The library cache is now invalidated immediately after enforcement so the next UI poll reflects the correct source.
- **Switch to Debrid button deleting local-only movies**: For movies with only a local copy, clicking "Switch to Debrid" would delete the local file without searching for a debrid alternative — the user lost their only copy. It now saves a prefer-debrid preference and triggers a Radarr search, preserving the local file until the debrid copy arrives and auto-enforcement swaps it.
- **Slow preference enforcement after blackhole completion**: After a torrent completed and symlinks were created, auto-enforcement of source preferences could be delayed up to 1 hour (the default library scan interval). The blackhole now triggers a library scan immediately after symlink creation.
- **Refresh button exits detail view**: Clicking the Refresh button while on a movie or show detail page would navigate back to the library grid instead of refreshing the detail page in place. The grid re-render is now skipped when in detail view, matching the behavior of the auto-refresh path.
- **Browser refresh loses detail view**: Pressing F5 or using the browser refresh button on a movie/show detail page would return to the library grid. The detail view now persists its state in the URL (`?detail=...&type=...`) so browser refresh restores it.

## Version [2.16.1] - 2026-04-02

### Added

- **Delete from Sonarr/Radarr**: Movie and show detail pages now have a "Delete from Radarr/Sonarr" button that removes the entry and its files from the respective arr service. Includes two-click confirmation, history logging (`arr_deleted` event), and notification support.
- **Expandable descriptions**: Show/movie overview text can now be clicked to expand the full description instead of being permanently truncated with a fade.

### Changed

- **Search modal redesign**: Removed the "Cached only" checkbox and "Cached" column (Real-Debrid deprecated their instantAvailability endpoint in Nov 2024, so cache status was always empty). Added a dedicated "Indexer" column, widened the modal, and centered data columns. Search results now sort by quality then seeds. The backend no longer makes the failing cache check API call, so searches return faster.

### Fixed

- **Detail page action buttons**: "Delete from Radarr/Sonarr" and "Search Torrents" buttons now display in a horizontal row instead of stacking vertically. "Switch to Debrid" is no longer styled as a destructive (red) action. Block button is now an icon-only (🚫) with tooltip, moved to the action row.
- **Badge spacing**: Source badge (Local/Debrid) under movie/show titles now has proper spacing from the title.
- **Quality badge text wrapping**: Quality badges like "WEB-DL 1080p" and "Remux 1080p" no longer wrap onto two lines in episode tables.
- **Apply button sizing**: The source preference Apply button now matches the dropdown's proportions.
- **Badge spacing**: Added breathing room between the title and the source badge on detail pages.
- **Description fade**: The overview text fade-out gradient now starts at 85% instead of 60%, so only the very bottom edge fades.

## Version [2.16.0] - 2026-04-01

### Changed

- **Shared CSS Foundation**: Extracted common CSS variables, reset, typography, spinner, banner, footer, focus styles, and reduced-motion preferences into a new `utils/ui_common.py` module. All three web pages (Dashboard, Library, Settings) now share a single source of truth for base styles instead of maintaining independent copies. Dashboard now includes `--input-bg`/`--input-border`/`--input-focus` variables for consistency.
- **Unified Navigation Bar**: All three pages now use an identical navigation bar generated by `get_nav_html()`, replacing three different header/nav implementations. Navigation includes pd_zurg brand, Dashboard, Library, Wanted (with badge count), Settings, and theme toggle. Active page is highlighted with `aria-current="page"`. Responsive layout collapses gracefully below 640px.
- **Standardized Button System**: Replaced three independent button vocabularies (`.btn-restart`/`.btn-run`, `.btn-action`/`.btn-apply`/`.btn-refresh`, `.btn`/`.btn-primary`/`.btn-secondary`) with a unified set: `.btn` (base), `.btn-ghost` (transparent), `.btn-primary` (filled green), `.btn-danger` (destructive), `.btn-sm` (small), `.btn-icon` (square). Confirming and dirty states preserved.

### Fixed

- **Wanted nav link 404**: The "Wanted" navigation link (`/library?filter=missing`) returned a 404 because the server route used exact path matching, ignoring query parameters.
- **Wanted filter showing empty results**: Clicking the nav "Wanted" link defaulted to the movies tab, showing "No results match your filters" when all missing content was in shows. The library now auto-switches to the tab that has matching items.

### Added

- **Data Freshness Indicator**: Dashboard now shows "Updated Xs ago" next to uptime, with a pulsing dot during active fetch and red indicator on connection loss.
- **Card Priority Signaling**: Dashboard cards display colored left-border accents based on health state — green (healthy), yellow (warnings like rate limits near threshold), red (errors like service down or mount failure). Health is evaluated per-card for Services, Processes, Mounts, System, and Events.
- **System Stats Progress Rings**: CPU and Memory stats now feature SVG circular progress rings behind the numeric values, color-coded by percentage (green <60%, yellow 60-85%, red >85%).
- **Dynamic Favicon**: The browser tab favicon is now an SVG lightning bolt that changes color based on overall system health (green/yellow/red). Updates on each status poll on the dashboard, and via a lightweight 30-second poller on Library and Settings pages.
- **Library Sort Options**: Added "Year (Newest)", "Year (Oldest)", and "% Complete" sort options to the library browser. Shows-only sorts (Episodes, % Complete) are hidden when viewing movies. Sort preference persists in localStorage.
- **Colored Poster Placeholders**: When TMDB poster art is unavailable, items now display a Netflix/Plex-style colored-initial placeholder — a large first letter centered on a deterministic hue-based radial gradient, with the title text below.
- **Jump Bar Improvements**: The A-Z jump bar now adapts responsively: thinner at 640-900px, converts to a horizontal sticky scroll bar at 480-640px (instead of disappearing), and shows a tooltip with the first title for each letter on hover.
- **Sticky Save Bar**: Settings editor now shows a fixed bar at the bottom of the viewport when there are unsaved changes, with an accurate change count, Save & Apply, and Discard buttons.
- **Category Modification Indicators**: Settings category headers display a yellow dot and "N changed" badge when fields inside have been modified.
- **Search Result Highlighting**: Settings search highlights matching text in field labels, help text, and category names using `<mark>` elements.
- **Per-Field Reset**: Each modified settings field shows an undo button (↺) that reverts that individual field to its last-saved value.
- **Gzip Compression**: All HTML and JSON responses from the status server are now gzip-compressed when the client supports it, reducing transfer sizes by ~70-80%. Compressed pages are cached by content hash for zero-overhead repeat requests.
- **Cache Headers**: HTML pages now include ETag and Cache-Control headers. Browsers can send conditional requests with If-None-Match to receive 304 Not Modified responses, avoiding unnecessary re-transfers. API endpoints use Cache-Control: no-store for always-fresh data.
- **Keyboard Shortcuts**: All pages now support keyboard navigation — `/` to focus search, `R` to refresh, `Escape` to close modals or clear search, `1`/`2`/`3` to switch tabs, `?` to show a help overlay. Shortcuts are disabled while typing in inputs. Relevant buttons show shortcut hints in tooltips.
- **Toast Notifications**: New `showToast(message, type, duration)` system available on all pages. Toasts appear bottom-right, stack up to 5 deep, auto-dismiss (5s success, 8s warning, persistent errors), and slide in/out with animation. Migrated `alert()` calls and added toast echoes for settings save results.
- **Recently Added Filter**: New purple "Recently Added" pill in the library filter bar shows the 20 most recently added items sorted by date, with automatic jump bar and sort override handling.
- **Provider Health Card**: Dashboard service cards for debrid providers now display API call counts, error rates, average response times, and rate limit usage. A color-coded rate limit bar visualizes remaining quota. A warning banner appears when rate limit usage exceeds 80%.
- **Structured Activity History**: All debrid pipeline events (grabs, cache hits, symlinks, failures, source switches, searches, rescans, task completions) are now logged to persistent JSONL storage. New Activity section on the dashboard displays events in a filterable, paginated table with type badges and auto-refresh. Per-show/movie history is available as a collapsible section in the library detail view. History is automatically rotated based on `HISTORY_RETENTION_DAYS` (default 30). API endpoints: `GET /api/history`, `GET /api/history/show/{title}`, `DELETE /api/history`.
- **Mass Editor**: Library browser now supports bulk selection mode — click "Select" to toggle checkboxes on poster cards, shift-click for range selection, then apply preferences or trigger missing-episode searches across multiple shows/movies at once via a fixed action bar. Selections persist across tab switches.
- **File Quality Badges**: Library browser now displays parsed quality information (resolution, source, codec, HDR) extracted from media filenames. Episode rows show color-coded badges (e.g., "WEB-DL 1080p") with file sizes. Poster cards display a resolution corner badge (4K/1080p/720p) for at-a-glance quality visibility.
- **Library Sorting & Filtering**: Library browser now supports sorting by A-Z, Z-A, Newest Added, Year, Episode Count, and Size. New filter dropdowns for show status (Continuing/Ended) and year range (2020s, 2010s, 2000s, Older). The jump bar automatically hides for non-alphabetical sorts. All sort and filter preferences persist across page loads via localStorage.
- **Blocklist**: Torrent blocklist system for permanently rejecting bad debrid torrents by info hash. Blocklisted hashes are automatically skipped during blackhole processing and library symlink creation. Torrents that hit terminal debrid errors (virus, dead, magnet error) are auto-blocklisted when `BLOCKLIST_AUTO_ADD=true` (default). Dashboard Blocklist section shows all entries with title, hash, reason, date, and source. "Block" button available on debrid-sourced episodes and movies in the library detail view with preset reason options. API endpoints: `GET /api/blocklist`, `POST /api/blocklist`, `DELETE /api/blocklist/{id}`, `DELETE /api/blocklist`. Persists across restarts in `/config/blocklist.json`.
- **Wanted/Missing Filter**: Library browser now has quick-filter preset pills — Missing (aired episodes without files), Unavailable (debrid search exhausted), Pending (active transitions), and Fallback (downloading locally). Each pill shows a live count. A "Wanted" link with badge appears in the nav bar when items need attention. Bulk actions appear when a preset is active: "Search All on Debrid" for missing items and "Download All Locally" for unavailable items, both rate-limited with progress feedback. Filters are linkable via URL query parameters (e.g., `/library?filter=missing`).
- **Enhanced Notification Events**: Five new notification event types for granular debrid pipeline visibility: `symlink_created` (batch notification when debrid symlinks are added to local library), `symlink_failed` (warning when symlink creation fails), `debrid_unavailable` (warning when content is marked unavailable after threshold), `local_fallback_triggered` (when local download starts as debrid fallback), and `blocklist_added` (when a torrent is blocklisted). All events are filterable via `NOTIFICATION_EVENTS`. Optional daily digest (`NOTIFICATION_DIGEST_ENABLED=true`, `NOTIFICATION_DIGEST_TIME=08:00`) sends a single summary of the day's pipeline activity instead of individual notifications.
- **Interactive Debrid Search**: Search for torrents directly from the library detail view using Torrentio. Set `TORRENTIO_URL` (e.g. `https://torrentio.strem.fun`) to enable. Movie detail views and episode rows show a "Search Torrents" button that opens a modal with results including release name, quality badge, size, seeders, and instant-availability (cached) status on your debrid provider. Results are sortable by quality, size, seeds, or cached status, with a "Cached only" filter toggle and minimum quality dropdown. One-click "Add" sends the torrent to your debrid provider (Real-Debrid, AllDebrid, or TorBox) with visual feedback. IMDb IDs are automatically resolved from TMDB metadata for accurate search results. History events and notifications are emitted on add success/failure (`debrid_add_success`, `debrid_add_failed`). API endpoints: `POST /api/search`, `POST /api/search/add`.

## Version [2.15.0] - 2026-03-30

### Added

- **Debrid search escalation**: Episodes stuck searching on debrid are automatically marked "debrid unavailable" after a configurable threshold (`DEBRID_UNAVAILABLE_THRESHOLD_DAYS`, default 3 days). UI shows a "Debrid N/A" badge and stops retrying.
- **Local fallback download**: "Download Locally" button appears for debrid-unavailable episodes and movies. Routes downloads through local/usenet indexers while preserving the series prefer-debrid setting. Automatically re-routes the series back to debrid after the local download completes.

### Fixed

- **Pending state cleanup**: `set_pending` now writes a `created` timestamp, fixing a bug where housekeeping could never clean up stale pending entries (they accumulated forever).
- **Pending state path traversal**: Fixed `_cleanup_empty_dirs` boundary check in `library_prefs.py` that could match sibling directories.

## Version [2.14.0] - 2026-03-29

### Added

- **Usenet-preferred local routing**: "Prefer Local" now routes downloads exclusively through usenet clients (NZBget/SABnzbd) when available, using a dedicated `usenet` tag. Falls back to any local client if no usenet client is configured. Usenet indexers are tagged with both `local` and `usenet` tags to support both routing modes.
- **Download client routing**: Automatically routes downloads through debrid or local download clients in Sonarr/Radarr based on source preference. Auto-tags untagged download clients, tags usenet indexers with local tag, and manages dual-tag exclusivity to prevent debrid interception of local downloads.
- **Sonarr-style poster cards**: Library browser now shows poster images with progress bars, replacing the plain list view. Includes TMDB metadata enrichment for poster artwork.
- **TMDB dedup and alias merge**: Debrid entries sharing a TMDB ID but with different parsed titles (e.g. "Andor" vs "Star Wars Andor") are deduplicated. Cross-source merge now uses TMDB IDs to match debrid and local items even when titles differ. TMDB disambiguation used when adding shows/movies to Sonarr/Radarr.
- **Alphabetical jump bar**: Library browser includes an A-Z jump bar that scales dynamically with viewport height for quick navigation through large libraries.
- **Centralized task scheduler**: Background task scheduler with WebUI for periodic maintenance tasks (library scan, symlink verification, duplicate cleanup) with configurable intervals.
- **Prefer-debrid active search**: Titles with prefer-debrid preference now actively search Sonarr/Radarr for debrid copies of local-only episodes, with search budgets and cooldown to avoid API spam.
- **Auto-retry alternative releases**: Blackhole automatically retries with alternative releases when a debrid provider rejects a torrent, and detects stale grabs that never completed.
- **Episode-aware blackhole dedup**: Deduplication now checks at the episode level instead of season level, preventing false positives when only some episodes exist locally.
- **Fuzzy title matching**: Debrid symlink creation uses fuzzy/normalized title matching for arr folder names, handling punctuation differences like "(500) Days of Summer" vs "500 Days of Summer".
- **Library UI overhaul**: 13 improvements inspired by Overseerr, Sonarr, and Maintainerr — including preference help text formatting, pending badge state distinction (migrating vs searching), and missing env vars added to Settings UI.
- **Direct WebDAV scanning**: Library scanner now queries Zurg's WebDAV API directly via PROPFIND, bypassing the FUSE/rclone mount for directory enumeration. Reduces mount scan time from 10-20 seconds to under 1 second. Falls back to FUSE scanning automatically if Zurg is unreachable.
- **Smart refresh polling**: Library refresh button now polls the backend until the scan completes, replacing the fixed 3-second delay. Shows a scanning indicator and updates the UI automatically when data is ready. Includes error feedback for timeouts and server failures.

### Changed

- **Split scan architecture**: Library scan is now split into a fast read-only phase (mount enumeration, cross-referencing, TMDB enrichment) and a background effects phase (preference enforcement, arr searches, symlink creation). The UI receives fresh library data after the read phase completes (~1-5s) while effects continue running in the background (~30-60s).
- **Season pack preference**: Scanner now prefers season pack files over individual episode downloads, using per-season episode count to avoid lower-quality mega-packs beating higher-quality season packs.
- **Fresh preferences**: `/api/library` response always returns preferences fresh from disk instead of using stale scan-time values.
- **Concurrent scan protection**: Added `_effects_running` guard to prevent overlapping preference enforcement and arr API calls from concurrent refresh requests.
- **Cache safety**: API responses now use a shallow copy of cached scan data to prevent mutation of the shared cache object across concurrent requests.
- **Stale cache during scan**: `get_data()` returns stale cached data when a background scan is running instead of triggering a duplicate synchronous scan.
- **Grid re-render optimization**: Smart poll skips full grid re-render to prevent poster image flicker during background data updates.

### Fixed

- **Indexer routing**: Debrid tag now applied to torrent indexers, and re-search triggered for missing content after routing changes.
- **Library scanner rescans**: Fixed rescan triggers and improved symlink target path validation to reject paths outside the mount.
- **False 'both' source**: Debrid symlinks in local library scan no longer cause false 'both' source classification. Expanded `verify_symlinks` to check local library directories.
- **Title parsing**: Filter non-media folders (plex versions, subs, featurettes), fix empty parentheses in titles, and prevent MAX title corruption from quality pattern over-matching.
- **RD torrent monitoring**: Fixed monitor polling deleted RD torrents and accept `selectFiles` 202 status responses.
- **Alt-retry race condition**: Hardened failure paths in automatic alternative release retry logic.
- **Episode dedup regex**: Updated to handle Sonarr-standard naming patterns (S01E01 format).
- **Download routing e2e**: Fixed usenet client skipping, indexer downloadClientId override clearing, and stale unavailable queue item cleanup.
- **Stale pending entries**: Auto-clear pending entries for titles removed from the library.
- **TMDB year-filter search failure**: TMDB searches with a year filter now retry without the year when the filtered search returns no results. Fixes shows like "Marvel's Spidey and His Amazing Friends" where the folder year (season air date) doesn't match the show's premiere year, preventing TMDB caching and alias-based dedup.
- **WebDAV scanner Zurg compatibility**: WebDAV PROPFIND scanner now handles both absolute (`/dav/movies/...`) and relative (`folder/file`) hrefs from Zurg. Also detects when Zurg doesn't support recursive depth-infinity PROPFIND (returns folders without files) and falls back to FUSE mount scanning automatically.
- **Stale debrid symlink cleanup**: `verify_symlinks` now checks symlinks pointing to `BLACKHOLE_SYMLINK_TARGET_BASE` (e.g. `/mnt/debrid/`) in addition to the rclone mount path. Previously only checked the mount path, so symlinks created with a different target base were never cleaned up when torrents expired.
- **Faster debrid library on startup**: When the rclone mount appears after the initial scan has already started, the scanner now automatically triggers a follow-up scan so debrid content appears within seconds instead of waiting for the next scheduled scan (up to 2 minutes).
- **Title parsing for mid-string year**: Folder names like `Movie (2000) DC (1080p BluRay...)` now correctly extract the year before quality truncation, preventing mangled titles like `Movie (2000) DC (1080p` and duplicate library entries without posters.
- **Symlink creation guard for empty local library**: Debrid symlink creation now skips when the local library scan found zero local content, preventing symlink pollution when network mounts (NFS/SMB) aren't propagated into the container.
- **TMDB year-preference matching**: TMDB search now prefers results whose release year matches the folder year instead of blindly taking the first (most popular) result. Only activates when the top result is >2 years off (avoiding false corrections from season air years), limits scan to top 5 results, and skips year preference entirely on fallback-no-year retries. Fixes movies like "Cover Up (2025)" incorrectly showing the poster and metadata for the 1983 French film "La Crime".
- **Verify symlinks deleting valid debrid symlinks**: `verify_symlinks` and blackhole's `_cleanup_symlinks` were checking symlink targets against `BLACKHOLE_SYMLINK_TARGET_BASE` (e.g. `/mnt/debrid`) which only exists inside Radarr/Sonarr's container, not pd_zurg's. Every debrid symlink appeared broken and was removed. Now translates target paths to the rclone mount before checking existence. Also adds a mount health check (aborts if rclone mount is unresponsive), a safety threshold (refuses to delete >50 symlinks if >50% appear broken), and cleans up empty parent directories after deletion so the library scanner doesn't misclassify them as local content.
- **Phantom local movie classification**: After symlinks were deleted, movie folders containing only Radarr metadata (.nfo, .jpg) but no media files were misclassified as `source='local'`, permanently blocking symlink recreation. `_scan_local_movies` now requires at least one media file to classify a directory as local. Same fix applied to shows via `_scan_local_shows`.
- **Symlink dir name mismatch with Radarr**: Debrid symlinks were created under the torrent-parsed title (e.g. `F1 The Movie (2025)`) instead of Radarr's canonical folder name (`F1 (2025)`). Added TMDB ID-based fallback matching when title lookup against Radarr fails, so the symlink lands in the correct Radarr-managed folder. Also fixed `_norm_for_matching` to treat hyphens as word separators ("Cover-Up" matches "Cover Up") and `&` as "and" ("Me, Myself & Irene" matches "Me Myself And Irene"). Movies with `source='both'` now also get symlinks created in Radarr's folder when it's empty, fixing the case where a wrong-named symlink dir existed but Radarr's canonical dir was empty.

## Version [2.13.0] - 2026-03-26

### Added

- **Auto-create debrid symlinks**: When `BLACKHOLE_SYMLINK_ENABLED=true` and local library paths are configured, the library scanner automatically creates organized symlinks in the local TV/movie library for debrid-only content. Sonarr/Radarr can then discover content that only exists on the debrid mount without manual import. Symlinks use canonical arr folder names when available.
- **Sonarr/Radarr rescan triggers**: After creating debrid symlinks, automatically triggers disk rescans in Sonarr (RescanSeries) and Radarr (RescanMovie) so they pick up new files without manual intervention.
- **Sonarr-inspired episode list UX**: Seasons sorted newest-first, color-coded progress pills (green=complete, yellow=partial, gray=empty), Expand All / Collapse All button, collapse footer on long seasons, formatted air dates (e.g., "Mar 15, 2025").
- **Pending state auto-cleanup**: Every library scan checks pending "Switching to debrid/local" entries against actual episode sources and clears resolved ones, even for manual changes outside the app.

### Fixed

- **Stale pending badges**: "Switching to debrid" badges persisted forever because switch-to-debrid and remove-local endpoints never cleared pending state. Now cleared immediately after successful operations.
- **applyMoviePreference prefer-debrid path**: Missing `_actionInFlight` guard and error recovery caused buttons to get permanently disabled after network failures.
- **Keydown listener leak**: Debrid removal confirmation dialog leaked keyboard event listeners when the DOM was destroyed by navigation or re-render.
- **Log file selection**: `read_log_lines` picked log files by mtime (unreliable for same-second writes), now uses lexicographic sort on date-stamped filenames.
- **Thread safety**: `get_all_pending()` now acquires `_pending_lock` for consistent reads.
- **Direction validation**: `set_pending()` rejects invalid direction values to prevent permanently stale entries.
- **XSS hardening**: Season numbers escaped in data attributes, log messages use `%r` formatting.

## Version [2.12.0] - 2026-03-24

### Added

- **Web-based settings editor**: Browser-based configuration for pd_zurg environment variables and plex_debrid settings.json with proper input types, inline validation, and SIGHUP reload (no container restart needed)
- **Quality profile editor**: JSON editor for plex_debrid quality profiles with first-run setup experience
- **OAuth device code flows**: Connect Trakt, Debrid Link, Put.io, and Orionoid accounts directly from the settings editor
- **Settings import/export**: Download or upload .env and settings files for backup and migration
- **Blackhole symlink mode**: Creates symlinks from completed debrid downloads to a target directory for Sonarr/Radarr import — zero-copy, no local storage used
- **Local library dedup**: Checks existing TV/movie library before submitting torrents to debrid to avoid duplicate downloads (`BLACKHOLE_DEDUP_ENABLED`)
- **RD account dedup**: Deduplicates incoming torrents against existing Real-Debrid account torrents before submitting
- **Workflow diagrams**: Interactive "How it works" diagrams in the status dashboard showing Watchlist and Arr+Blackhole flows with component glossary
- **Status UI enhancements**: Log viewer with level filtering, process restart buttons, running config viewer, mount event history
- **Service health checks**: Dashboard shows connectivity status and premium expiry for debrid, Plex, Overseerr, and other integrated services
- **Config validation**: Startup validation of environment variables with clear error messages
- **SIGHUP reload**: Reload pd_zurg configuration without restarting the container
- **.env import button**: Restore configuration from a previously exported .env file via the settings editor
- **Bidirectional settings sync**: Changes in .env propagate to plex_debrid settings.json and vice versa
- **DUPLICATE_CLEANUP_KEEP**: New option to control which copy is kept during duplicate cleanup — `local` (default, logs Zurg dupes) or `zurg` (deletes local copy)

### Changed

- `.env` is now the single source of configuration — docker-compose.yml no longer contains inline env vars, making it safe to pull updates without losing settings
- Duplicate cleanup reworked: skips Zurg copies by default (read-only mount), logs summary at INFO level with per-item detail at DEBUG
- NFS mode documented as not creating a local mount — cross-machine WebDAV setup recommended instead

### Fixed

- `atomic_write()` crash from invalid kwargs passed to `mkstemp()`
- Auth lockout: StatusHandler credentials now update on SIGHUP reload
- Phantom AllDebrid instance created when only Real-Debrid is configured
- False Plex/Jellyfin conflict error during startup validation
- Crash when `AUTO_UPDATE_INTERVAL` is set to empty string
- `SimpleNamespace` missing `.status` attribute crash in RD torrent info handling
- Environment variables overridden by stale `/config/.env` on container restart
- Log viewer stuck displaying rotated log file after log rotation
- Single-file torrent extension mismatch in blackhole symlinks
- Stale config after SIGHUP reload, missing service triggers, and settings sync gaps
- JS syntax error in status UI restart button handler
- Dashboard layout issues: header spacing, column alignment, stat centering


## Version [2.11.0] - 2026-03-21

### Added

- **Process auto-restart**: Crashed processes are automatically detected and restarted with exponential backoff (5s → 300s), sliding window restart counting, and max restart limits
- **Zombie reaping**: `SIGCHLD` set to `SIG_IGN` for automatic kernel-level zombie child reaping without conflicting with subprocess management
- **Apprise notifications**: Event-driven notifications to 90+ services (Discord, Telegram, Slack, email, etc.) via `NOTIFICATION_URL` environment variable, with event and severity filtering
- **Blackhole watch folder**: Arr-stack compatible watch directory for `.torrent` and `.magnet` files with Real-Debrid, AllDebrid, and TorBox support. Failed files quarantined to `failed/` subdirectory
- **ffprobe stuck-process recovery**: Monitors for ffprobe processes stuck in uninterruptible sleep on debrid mounts, attempts recovery via I/O poke, then kills after max attempts
- **Status Web UI**: Lightweight dashboard at `/status` with JSON API at `/api/status`, showing process health, mount status, cgroup-aware system stats, and recent events. Optional basic auth
- **MDBList content source**: Subscribe to curated MDBList lists (IMDB Top 250, trending, custom) that feed plex_debrid's download pipeline. Configure via plex_debrid settings menu
- **Atomic config writes**: Zurg config.yml and rclone.config updates use temp-file-then-rename to prevent corruption on crash
- **Wait-for-URL with exponential backoff**: Extracted generic `wait_for_url()` utility from rclone module with 5s → 60s exponential backoff
- **Ordered shutdown**: Per-process shutdown timeouts (plex_debrid: 15s, Zurg/rclone: 10s) with elapsed time logging

### Changed

- Process registry changed from tuples to dicts for extensibility
- `stop_process()` now disables auto-restart to prevent spurious restarts during update cycles
- Shutdown notification sent after critical cleanup with 5s timeout thread

## Version [2.10.0] - 2026-03-21

### Fixed

- Plex API: Migrated all Plex API calls from deprecated `metadata.provider.plex.tv` to `discover.provider.plex.tv` — fixes broken watchlist, metadata, scrobble, and search
- Torrentio: Fixed `qualityfilter` parameter format for current Torrentio API
- Error handling: Replaced bare `except: pass` in main.py that silently swallowed all exceptions including SystemExit and KeyboardInterrupt
- Error handling: Flattened 3-4 levels of nested try/except that re-wrapped exceptions and lost tracebacks
- Healthcheck: Removed redundant in-process healthcheck subprocess loop (Dockerfile HEALTHCHECK already handles this)
- Thread safety: Fixed SubprocessLogger thread join hangs by checking stop_event in read loops and adding join timeouts

### Added

- FLARESOLVERR_URL: Optional environment variable to enable FlareSolverr proxy for Torrentio scraping when Cloudflare protection blocks direct requests
- Graceful shutdown: All tracked child processes (Zurg, rclone, plex_debrid) are now terminated on SIGTERM/SIGINT before unmounting filesystems
- Port collision detection: Random port assignment for Zurg and rclone NFS now checks availability via socket binding
- Rclone config backup: Existing `/config/rclone.config` is backed up to `.bak` before being overwritten on startup
- Config class: Configuration variables wrapped in a `Config` class with `load()` method for testability and runtime reload
- `__all__` defined in `base/__init__.py` to control wildcard import scope

### Changed

- Dependency pinning: All packages in requirements.txt pinned to specific versions
- Dockerfile: Pinned rclone to 1.73.2, Python base to 3.11.12-alpine3.21, zurg-testing files to commit SHA
- Duplicate cleanup: Merged near-identical `process_tv_shows()` and `process_movies()` into shared function with single PlexServer instance
- docker-compose.yml: Removed deprecated `version: "3.8"` key
- main.py: Replaced `threading.Event().wait()` with `signal.pause()` for idle loop
- Removed duplicate `import zipfile` in `base/__init__.py`
- Removed unused `from urllib import response` in `utils/download.py`


## Version [2.9.2] - 2024-12-12

### Fixed

- [Issue #85](https://github.com/I-am-PUID-0/pd_zurg/issues/85) - Updated the default plex_debrid files to the latest changes from the [elfhosted](https://github.com/elfhosted/plex_debrid)


## Version [2.9.1] - 2024-09-03

### Fixed

- [Issue #68](https://github.com/I-am-PUID-0/pd_zurg/issues/68) Docker Compose fetches incorrect architecture binary on Raspbian arm64
- [Issue #69](https://github.com/I-am-PUID-0/pd_zurg/issues/69) Dockerfile pulling the wrong zurg architecture, when running on aarch64


## Version [2.9.0] - 2024-08-09

### Changed

- plex_debrid: Pulled in [elfhosted](https://github.com/elfhosted/plex_debrid) fork of plex_debrid as the base plex_debrid within the pd_zurg image

### Added

- TRAKT_CLIENT_ID: Environment variable to set the trakt client ID for plex_debrid - when not set, it will use **[itsToggle's](https://github.com/itsToggle)** trakt client ID and secret
- TRAKT_CLIENT_SECRET: Environment variable to set the trakt client secret for plex_debrid - when not set, it will use **[itsToggle's](https://github.com/itsToggle)** trakt client ID and secret

### Notes

- Per [elfhosted](https://github.com/elfhosted/plex_debrid/tree/main#improvements), below are the improvements made to the plex_debrid fork:

* Support [ElfHosted internal URLs](https://elfhosted.com/how-to/connect-apps/) for [Plex](https://elfhosted.com/app/plex/), [Jellyfin](https://elfhosted.com/app/jellyfin/), [Overseerr](https://elfhosted.com/app/overseerr/), [Jackett](https://elfhosted.com/app/jackett/), [Prowlarr](https://elfhosted.com/app/prowlarr/) by default.
* Trakt OAuth [fixed](https://github.com/elfhosted/plex_debrid/commit/c678fa1e5974a5c666b2fe70d65228c6fdfb4047) (*by passing your own client ID / secret in ENV vars*).
* Integrated with [Zilean](https://github.com/iPromKnight/zilean/) for scraping [DebridMediaManager](https://debridmediamanager.com/) (DMM) public hashes, defaults to ElfHosted internal Zilean service.
* Parametize watchlist loop interval (*defaults to 30s instead of hard-coded 30 min*)
* Single episode downloads [fixed](https://github.com/elfhosted/plex_debrid/pull/1)

- **Zilean support is not yet implemented in pd_zurg, but will be in a future release**

## Version [2.8.1] - 2024-08-09

## Fixed

- healthcheck: Fixed healthcheck for zurg w/ armv7


## Version [2.8.0] - 2024-08-09

### Changed

- plex_debrid: Debug printing for plex_debrid no longer linked to PDZURG_LOG_LEVEL
- Downloader: Add linux-arm-7 to get_architecture function

### Added

- PD_LOG_LEVEL: Environment variable to set the log level for plex_debrid - Only DEBUG and INFO are supported for plex_debrid ; Default is INFO
- Suppress Logs: If the LOG_LEVEL for a process is set to OFF, then logs will be suppressed for the process
- Zurg: Check for arm-7 architecture for compatibility with armv7 devices and set `ln -sf /lib/ld-musl-armhf.so.1 /lib/ld-linux-armhf.so.3`

### Notes

- Setting RCLONE_LOG_LEVEL to OFF will break rclone - will patch in future release
- Thank you @barneyphife for the support with the armV7 compatibility


## Version [2.7.0] - 2024-07-30

### Changed

- Refactored to use additional common functions under utils
- Update process: Refactored update process to apply updates to Zurg and plex_debrid before starting the processes

### Added

- Ratelimit for GitHub API requests
- Retries for GitHub API requests
- plex_debrid: Debug printing for plex_debrid linked to PDZURG_LOG_LEVEL
- Zurg: Add plex_update.sh from Zurg to working directory for Zurg use
- Shutdown: Added a shutdown function to gracefully stop the pd_zurg container; e.g., unmount the rclone mounts
- ffmpeg: Added ffmpeg to the Dockerfile for Zurg use of ffprobe to extract media information from files, enhancing media metadata accuracy.
- COLOR_LOG_ENABLED: Environment variable to enable color logging; Default is false

### Fixed

[PR #62](https://github.com/I-am-PUID-0/pd_zurg/pull/62) - Allow nightly release custom versions for ZURG_VERSION


## Version [2.6.0] - 2024-07-26

### Changed

- [PR #62](https://github.com/I-am-PUID-0/pd_zurg/pull/62) - Allow nightly release custom versions for ZURG_VERSION


## Version [2.5.0] - 2024-07-22

### Added

- [Issue #59](https://github.com/I-am-PUID-0/pd_zurg/issues/59): Added PDZURG_LOG_SIZE environment variable to set the maximum size of the log file; Default is 10MB
- [Issue #60](https://github.com/I-am-PUID-0/pd_zurg/issues/60): Added PD_REPO environment variable to set the plex_debrid repository to pull from; Default is `None`

### Changed

- Refactored to use common functions under utils 
- Dockerfile: Updated to use the python:3.11-alpine image
- plex_debrid: Updates for plex_debrid are now enabled with PD_UPDATE when PD_REPO is used

### Notes

- The PDZURG_LOG_SIZE environment variable only applies to the pd_zurg log file; not the Zurg or plex_debrid log files. 

- The PD_REPO environment variable is used to set the plex_debrid repository to pull from. If used, the value must be a comma seperated list for the GitHub username,repository_name,and optionally the branch; e.g., PD_REPO=itsToggle,plex_debrid,main - the branch is defaulted to main if not specified

- PD_UPDATE is only functional when PD_REPO is used


## Version [2.4.3] - 2024-07-17

### Fixed

- Rclone: Fixed WebDAV URL check for Zurg startup processes to accept all 2xx status codes


## Version [2.4.2] - 2024-07-16

### Fixed

- Rclone: Fixed WebDAV URL check for Zurg startup processes when Zurg user and password are set in config.yml


## Version [2.4.1] - 2024-07-16

### Fixed

- Zurg: Fixed the removal of Zurg user and password if previously set in config.yml
- Rclone: Introduced a Rclone startup check for the Zurg WebDAV URL to ensure the Zurg startup processes have finished before starting Rclone


## Version [2.4.0] - 2024-06-26

### Added

- Zurg: GITHUB_TOKEN environment variable to use for access to the private sponsored zurg repository


## Version [2.3.0] - 2024-06-19

### Changed
- plex_debrid: The original plex_debird repository files are now stored within this repository. This is to ensure that if the original repository is deleted or removed from GitHub, this repository will still function. It's also simpler than maintaining a forked repository.


## Version [2.2.0] - 2024-06-19

### Changed

- plex_debrid: Updates for plex_debrid are disabled, as plex_debrid is no longer maintained.


## Version [2.1.5] - 2024-05-09

### Fixed
 
- [Issue #666](https://github.com/itsToggle/plex_debrid/issues/666) - Fixed issue with trakt sync not working properly in plex_debrid. Thanks to @mash2k3 for the fix!


## Version [2.1.4] - 2024-02-27

### Changed

- plex_debrid: plex_debrid setup process automatically checks for existing Plex libraries and adds them to settings.json for Library update services

### Fixed

- [Issue #2](https://github.com/I-am-PUID-0/pd_zurg/issues/2)
- [Issue #35](https://github.com/I-am-PUID-0/pd_zurg/issues/35)


## Version [2.1.3] - 2024-02-10

### Changed

- Zurg: Zurg setup process uncomments appropriate lines in config.yml for Zurg setup


## Version [2.1.2] - 2024-02-09

### Changed

- plex_debrid: plex_debrid setup process now checks for existing additional Plex users


## Version [2.1.1] - 2024-02-01

### Changed

- Zurg: Download release version parsing using GitHub release tags

### Fixed

- Healthcheck: Fixed healthcheck for rclone serve NFS  


## Version [2.1.0] - 2024-01-29

### Added

- ZURG_USER: ZURG_USER env var added to enable Zurg username for Zurg endpoints
- ZURG_PASS: ZURG_PASS env var added to enable Zurg password for Zurg endpoints
- NFS_ENABLED: NFS_ENABLED env var added to enable NFS mount for rclone w/ Zurg
- NFS_PORT: NFS_PORT env var added to define the NFS mount port for rclone w/ Zurg
- ZURG_PORT: ZURG_PORT env var added to define the Zurg port for Zurg endpoints

## Version [2.0.5] - 2024-01-29

### Fixed

- Healthcheck: Fixed healthcheck for Zurg and plex_debrid services to ensure they are checked for "true" or "false" values


## Version [2.0.4] - 2024-01-23

### Fixed

- Zurg: Fixed AllDebrid setup process for Zurg


## Version [2.0.3] - 2024-01-22

### Fixed

- plex_debrid: Fixed Plex users for Jellyfin deployments


## Version [2.0.2] - 2024-01-22

### Fixed

- PLEX_REFRESH: Fixed Plex library refresh w/ Zurg when using docker secrets

## Version [2.0.1] - 2024-01-16

### Fixed

- logging: Fixed logging for subprocesses


## Version [2.0.0] - 2024-01-04

### Breaking Change

- PD_ENABLED: Added PD_ENABLED env var to enable/disable plex_debrid service
- PLEX_USER: PLEX_USER env var no longer enables plex_debrid service

### Added

- JF_API_KEY: JF_API_KEY env var added to enable Jellyfin integration
- JF_ADDRESS: JF_ADDRESS env var added to enable Jellyfin integration
- SEERR_API_KEY: SEERR_API_KEY env var added to enable Overseerr/Jellyseerr integration
- SEERR_ADDRESS: SEERR_ADDRESS env var added to enable Overseerr/Jellyseerr integration
- PLEX_REFRESH: PLEX_REFRESH env var added to enable Plex library refresh w/ Zurg
- PLEX_MOUNT_DIR: PLEX_MOUNT_DIR env var added to enable Plex library refresh w/ Zurg

### Changed

- plex_debrid setup: plex_debrid setup process now allows for selection of Plex or Jellyfin

### Removed

- ZURG_LOG_LEVEL: Removed the need for ZURG_LOG_LEVEL env var - now set by PDZURG_LOG_LEVEL
- RCLONE_LOG_LEVEL: Removed the need for RCLONE_LOG_LEVEL env var - now set by PDZURG_LOG_LEVEL


## Version [1.1.0] - 2024-01-04

### Added

- Docker Secrets: Added support for the use of docker secrets


## Version [1.0.3] - 2024-01-03

### Changed

- Zurg: Increased read timeout to 10 seconds for GitHub repository checks
- Zurg: Setup process now checks for existing config.yml in debrid service directory
- Zurg: Setup process now checks for existing zurg app in debrid service directory
- Logging: Cleaned up logging and added additional logging details

## Version [1.0.2] - 2024-01-02

### Changed

- Zurg: Download release version parsing


## Version [1.0.1] - 2023-12-21

### Changed

- plex_debrid: increased read timeout to 5 seconds for GitHub repository checks 


## Version [1.0.0] - 2023-12-21

### Breaking Change

- Automatic Updates: AUTO_UPDATE env var renamed to PD_UPDATE

### Changed

- Automatic Updates: Refactored update process to allow for scaling of update process
- Healthcheck: Refactored healthcheck process to allow for scaling of healthcheck process
- Healthcheck: rclone mount check now uses rclone process instead of rclone mount location
- Rclone: Subprocess logs are now captured and logged to the pd_zurg logs

### Added

- ZURG_UPDATE: ZURG_UPDATE env var added to enable automatic update process for ZURG
- Zurg: Added automatic update process for Zurg
- Healthcheck: Added healthcheck for Zurg process


## Version [0.2.0] - 2023-12-13

### Added

- ZURG_LOG_LEVEL: The log level to use for Zurg as defined with the ZURG_LOG_LEVEL env var


## Version [0.1.0] - 2023-12-12

### Added

- ZURG_VERSION: The version of ZURG to use as defined with the ZURG_VERSION env var 

### Changed

- Zurg: Container pulls latest or user-defined version of ZURG from github upon startup


## Version [0.0.5] - 2023-12-06

### Fixed

- Duplicate Cleanup: Process not called correctly


## Version [0.0.4] - 2023-12-06

### Changed

- Dockerfile: Pull latest config.yml from zurg repo for base file


## Version [0.0.3] - 2023-12-05

### Fixed

- Zurg: config.yml override


## Version [0.0.2] - 2023-12-05

### Changed

- base: Update envs
- main.py: Order of execution
- healthcheck.py: Order of execution


## Version [0.0.1] - 2023-12-05

### Added

- Initial Push 
