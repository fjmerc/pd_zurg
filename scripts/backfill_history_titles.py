#!/usr/bin/env python3
"""Backfill canonical media_title on existing history.jsonl events.

Re-runs the new canonical-title resolver on each event's stored ``title``
field — which for blackhole grab events is the original release filename
(``Gattaca.Ethan.Hawke.Sci.Fi.(1997).1080p.BluRay.x264-GROUP.torrent``)
— and rewrites ``media_title`` whenever the new resolution is cleaner
than the stored one.

Default mode is dry-run.  Pass ``--apply`` to actually write the file.
The write is atomic (temp file + rename) so a crash mid-write cannot
corrupt history.

Usage (inside the pd_zurg container, where /config/history.jsonl lives
and the utils/ package is importable):

    python3 scripts/backfill_history_titles.py                # dry-run
    python3 scripts/backfill_history_titles.py --apply        # actually rewrite
    python3 scripts/backfill_history_titles.py --path /tmp/history.jsonl

Only events whose ``source`` is in ``{blackhole, search}`` and whose
``type`` is a grab/grab-failure event are eligible — these are the only
events whose ``title`` field is a release filename rather than a
canonical title.
"""
import argparse
import json
import os
import sys
import tempfile


# Event types where ``title`` is a raw release filename and re-resolution
# is meaningful.  Other event types (library scans, scheduler runs, etc.)
# already store canonical titles in ``title`` and must not be rewritten.
_RESOLVABLE_TYPES = frozenset({
    'grabbed', 'cached', 'failed', 'duplicate', 'uncached_rejected',
    'blocklisted', 'blocklist_added', 'compromise_grabbed', 'compromise_grab',
    'debrid_add', 'debrid_add_failed', 'symlink_created',
})

_RESOLVABLE_SOURCES = frozenset({'blackhole', 'search'})


def _bootstrap_imports():
    """Make ``utils.*`` importable when the script is run from the repo
    root or from ``/app`` inside the container."""
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(here)
    for candidate in (repo_root, '/app', os.getcwd()):
        if candidate and os.path.isdir(os.path.join(candidate, 'utils')):
            if candidate not in sys.path:
                sys.path.insert(0, candidate)
            return candidate
    return None


def _is_eligible(ev):
    """Return True if the event's ``title`` looks like something the
    resolver can improve.  Skips events with no ``title``, events whose
    type/source aren't grab-flavored, and events whose stored
    media_title already equals the resolved canonical (no-op)."""
    if ev.get('type') not in _RESOLVABLE_TYPES:
        return False
    if ev.get('source') not in _RESOLVABLE_SOURCES:
        return False
    if not isinstance(ev.get('title'), str) or not ev['title']:
        return False
    return True


def _resolve(ev, _enrich):
    """Run the canonical resolver on the event's release filename.

    Returns the new ``media_title`` (str) if it differs from the stored
    one, else None.
    """
    filename = ev['title']
    try:
        new_media_title, _ep = _enrich(filename)
    except Exception as e:
        return None, f'resolver error: {type(e).__name__}: {e}'

    old_media_title = ev.get('media_title') or ''
    if not new_media_title:
        return None, 'resolver returned empty'
    if new_media_title == old_media_title:
        return None, 'no change'
    return new_media_title, None


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        '--path',
        default='/config/history.jsonl',
        help='Path to history.jsonl (default: /config/history.jsonl)',
    )
    ap.add_argument(
        '--apply',
        action='store_true',
        help='Actually rewrite the file. Without this flag, the script '
             'is a dry-run that prints planned changes and exits.',
    )
    ap.add_argument(
        '--limit',
        type=int,
        default=0,
        help='In dry-run mode, stop after N planned changes (0 = no limit).',
    )
    args = ap.parse_args()

    if not _bootstrap_imports():
        print('error: cannot locate utils/ package; run from repo root '
              'or ensure /app/utils exists in this container.',
              file=sys.stderr)
        return 2

    try:
        from utils.blackhole import _enrich_for_history as _enrich
    except Exception as e:
        print(f'error: failed to import _enrich_for_history: {e}', file=sys.stderr)
        return 2

    if not os.path.isfile(args.path):
        print(f'error: history file not found: {args.path}', file=sys.stderr)
        return 2

    total = 0
    eligible = 0
    changed = 0
    errors = 0
    pending_writes = []  # list of (line_no, original_line, updated_line)

    with open(args.path, 'r', encoding='utf-8') as f:
        for line_no, raw in enumerate(f, start=1):
            total += 1
            line = raw.rstrip('\n')
            if not line:
                pending_writes.append((line_no, raw, raw))
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError as e:
                # Preserve corrupted lines verbatim — never destroy data.
                print(f'  line {line_no}: skipping malformed JSON: {e}',
                      file=sys.stderr)
                pending_writes.append((line_no, raw, raw))
                errors += 1
                continue

            if not _is_eligible(ev):
                pending_writes.append((line_no, raw, raw))
                continue
            eligible += 1

            new_title, reason = _resolve(ev, _enrich)
            if new_title is None:
                pending_writes.append((line_no, raw, raw))
                continue

            old_title = ev.get('media_title') or '(none)'
            print(f'  line {line_no} [{ev.get("type")}] '
                  f'{old_title!r} -> {new_title!r}')

            ev['media_title'] = new_title
            updated = json.dumps(ev, separators=(',', ':')) + '\n'
            pending_writes.append((line_no, raw, updated))
            changed += 1

            if args.limit and changed >= args.limit:
                print(f'  (--limit {args.limit} reached, stopping scan)')
                break

    print()
    print(f'Total events scanned:    {total}')
    print(f'Eligible (grab-typed):   {eligible}')
    print(f'Would update media_title: {changed}')
    print(f'JSON parse errors:       {errors}')

    if not args.apply:
        print()
        print('Dry-run only — no file changes.  Re-run with --apply to '
              'write changes back to disk.')
        return 0

    if changed == 0:
        print('No changes to apply.')
        return 0

    # Atomic rewrite: write to a sibling tempfile, fsync, rename.
    target_dir = os.path.dirname(os.path.abspath(args.path)) or '.'
    fd, tmp_path = tempfile.mkstemp(prefix='.history-backfill-', dir=target_dir)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as out:
            for _ln, _orig, updated in pending_writes:
                out.write(updated)
            out.flush()
            os.fsync(out.fileno())
        # Preserve original mode if possible.
        try:
            st = os.stat(args.path)
            os.chmod(tmp_path, st.st_mode)
        except OSError:
            pass
        os.replace(tmp_path, args.path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    print(f'Applied {changed} update(s) to {args.path}.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
