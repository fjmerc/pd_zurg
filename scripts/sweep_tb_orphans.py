#!/usr/bin/env python3
"""One-shot symlink sweep for orphaned TorBox releases.

Background: during the plan-39-phase-1 → tb-mount-fix window, every TB
grab that reached "Torrent ready" then went silent because
``_find_on_mount`` only probed Zurg's categorized layout (shows/movies/
anime/__all__) — none of which exist on TB's flat WebDAV mount.  The
magnet file in ``/watch/sonarr/`` was consumed by the grab path, so
each release landed on the TB mount with no corresponding
``/completed/sonarr|radarr/<release>/`` symlink.  Sonarr/Radarr never
imported anything; from their perspective the content never arrived.

This script reconciles the gap: it walks the TB mount, looks up each
release in Sonarr (shows) or Radarr (movies), and creates the symlink
that the blackhole would have created on a successful grab.  Reuses
``BlackholeWatcher._create_symlinks`` so the path-construction logic
stays in one place.

Idempotent — re-running skips releases whose completed-dir entry
already exists.  DRY-RUN by default; pass ``--apply`` to create
symlinks.

Usage (inside the zurgarr container so the env vars are populated):

    docker exec zurgarr /bin/sh -c \\
        'source /venv/bin/activate && python3 /scripts/sweep_tb_orphans.py'
    # Then with --apply once the dry-run report looks correct.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, '/')

import re

from utils.blackhole import BlackholeWatcher, _is_safe_mount_name  # noqa: E402
from utils.arr_client import RadarrClient, SonarrClient  # noqa: E402
from utils.debrid_routing import TORBOX, mount_for_debrid  # noqa: E402
from utils.library import parse_folder_name  # noqa: E402

# SxxExx in the original folder name flags a TV release.  Match both
# dotted and spaced forms; the year-based heuristic in blackhole's
# ``parse_release_name`` is unreliable on noisy TB names so we look
# directly at the season marker.
_TV_PATTERN = re.compile(r'[\.\s_-]S\d{1,2}(?:E\d{1,3})?[\.\s_-]?', re.IGNORECASE)


def _discover_tb_mount() -> str | None:
    """Resolve the TorBox rclone mount path from env (same convention as
    ``BlackholeWatcher._mount_for``)."""
    rclonemn = os.environ.get('RCLONE_MOUNT_NAME') or ''
    base = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '/data').rstrip('/')
    if rclonemn and os.path.basename(base) == rclonemn:
        parent = os.path.dirname(base)
        if parent:
            base = parent
    return mount_for_debrid(TORBOX, rclone_mount_base=base)


def _route_release(folder: str, sonarr: SonarrClient, radarr: RadarrClient) -> tuple[str | None, str | None]:
    """Return ``(label, matched_title)`` for *folder*.

    Uses ``library.parse_folder_name`` to derive a clean title — it already
    strips ``www.<indexer>.<tld>    -    `` and leading ``[bracket]``
    site prefixes that ``blackhole.parse_release_name`` doesn't.  Without
    that strip, the 44 TB orphans named like ``www.UIndex.org    -    For
    All Mankind S04E01...`` got fed into the arrs as titles like ``"www
    UIndex org For All Mankind"`` and missed every library lookup.

    SxxExx detection drives the arr-routing order: TV-shaped → Sonarr
    first, movie-shaped → Radarr first.  Falls through to the other arr
    if the first misses.
    """
    title, _year = parse_folder_name(folder)
    if not title:
        return None, None

    is_tv = bool(_TV_PATTERN.search(folder))

    if is_tv:
        if sonarr.configured:
            hit = sonarr.find_series_in_library(title=title)
            if hit:
                return 'sonarr', hit.get('title') or title
        if radarr.configured:
            hit = radarr.find_movie_in_library(title=title)
            if hit:
                return 'radarr', hit.get('title') or title
    else:
        if radarr.configured:
            hit = radarr.find_movie_in_library(title=title)
            if hit:
                return 'radarr', hit.get('title') or title
        if sonarr.configured:
            hit = sonarr.find_series_in_library(title=title)
            if hit:
                return 'sonarr', hit.get('title') or title
    return None, None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--apply', action='store_true',
                    help='Actually create symlinks (default: dry-run report only).')
    ap.add_argument('--limit', type=int, default=0,
                    help='Process at most N releases (0 = all).')
    args = ap.parse_args()

    tb_mount = _discover_tb_mount()
    if not tb_mount or not os.path.isdir(tb_mount):
        print(f'ERROR: TB mount not found at {tb_mount!r}', file=sys.stderr)
        return 1

    completed_dir = os.environ.get('BLACKHOLE_COMPLETED_DIR', '/completed')
    if not os.path.isdir(completed_dir):
        print(f'ERROR: completed dir not found at {completed_dir!r}', file=sys.stderr)
        return 1

    watcher = BlackholeWatcher(
        os.environ.get('BLACKHOLE_DIR', '/watch'),
        'sweeper',  # bogus API key — we do not make debrid API calls
        'torbox',
        symlink_enabled=True,
        completed_dir=completed_dir,
        rclone_mount=os.environ.get('BLACKHOLE_RCLONE_MOUNT', '/data'),
        symlink_target_base=os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', ''),
    )

    sonarr = SonarrClient()
    radarr = RadarrClient()

    try:
        folders = sorted(os.listdir(tb_mount))
    except OSError as e:
        print(f'ERROR: cannot list TB mount {tb_mount}: {e}', file=sys.stderr)
        return 1

    print(f'TB mount: {tb_mount}')
    print(f'completed dir: {completed_dir}')
    print(f'Sonarr configured: {sonarr.configured}')
    print(f'Radarr configured: {radarr.configured}')
    print(f'Mode: {"APPLY" if args.apply else "DRY-RUN"}')
    print(f'Found {len(folders)} TB folder(s)')
    print('---')

    stats = {
        'total': 0, 'symlinked': 0, 'already': 0,
        'unresolved': 0, 'unsafe': 0, 'errors': 0,
    }

    for folder in folders:
        if args.limit and stats['total'] >= args.limit:
            break
        stats['total'] += 1

        if not _is_safe_mount_name(folder):
            print(f'  SKIP unsafe: {folder!r}')
            stats['unsafe'] += 1
            continue

        mount_path = os.path.join(tb_mount, folder)
        if not os.path.isdir(mount_path):
            continue

        label, matched_title = _route_release(folder, sonarr, radarr)
        if not label:
            print(f'  UNRESOLVED: {folder!r} (no arr library hit)')
            stats['unresolved'] += 1
            continue

        target_dir = os.path.join(completed_dir, label, folder)
        if os.path.exists(target_dir):
            print(f'  EXISTS [{label}]: {folder!r}')
            stats['already'] += 1
            continue

        if not args.apply:
            print(f'  DRY [{label} -> {matched_title!r}]: would symlink {folder!r}')
            stats['symlinked'] += 1
            continue

        try:
            count = watcher._create_symlinks(
                folder, '', mount_path, label=label, debrid='torbox',
            )
            if count > 0:
                print(f'  OK [{label}]: {count} file(s) for {folder!r}')
                stats['symlinked'] += 1
            else:
                print(f'  NO-MEDIA [{label}]: {folder!r} (no media files found)')
                stats['errors'] += 1
        except Exception as e:  # noqa: BLE001 — surface the cause; one bad folder shouldn't abort the run
            print(f'  FAIL [{label}]: {folder!r}: {e}')
            stats['errors'] += 1

    print('---')
    print(f'Summary: {stats}')
    if not args.apply and stats['symlinked']:
        print(f'\nRe-run with --apply to create the {stats["symlinked"]} symlink(s) above.')
    return 0 if stats['errors'] == 0 else 2


if __name__ == '__main__':
    sys.exit(main())
