#!/usr/bin/env python3
"""One-shot cleanup for spuriously-blocklisted TorBox hashes.

Background: before the TB readiness-state fix (commit landing TB_READY_STATES),
the blackhole's ``_is_torrent_ready`` for TorBox only accepted
``download_state == 'completed'``.  TB returns ``'cached'`` for instant
cache hits (the dominant case under plan 39 cache_aware routing), so
every cached TB grab timed out at ``mount_poll_timeout`` (default 300s)
and got auto-blocklisted by the existing ``BLOCKLIST_AUTO_ADD`` path
with reason ``"Uncached on debrid (timed out)"``.

This script reads ``/config/blocklist.json``, finds every entry with that
reason + ``source='auto'``, probes TB's ``/checkcached`` for the hash,
and removes entries whose hash IS cached on TB (proving the original
blocklist add was spurious).  Entries whose hash is genuinely uncached
on TB are kept — those were correctly blocklisted.

DRY-RUN by default; pass ``--apply`` to actually rewrite the file.

Usage (inside the zurgarr container so /config/blocklist.json and
TORBOX_API_KEY are both present):

    docker exec zurgarr python3 /scripts/cleanup_tb_false_blocklist.py
    docker exec zurgarr python3 /scripts/cleanup_tb_false_blocklist.py --apply

Outputs a per-hash decision line + a summary.  Safe to re-run: idempotent
once the false positives are gone.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


BLOCKLIST_PATH = '/config/blocklist.json'
TARGET_REASON = 'Uncached on debrid (timed out)'
TARGET_SOURCE = 'auto'
TB_CHECKCACHED = 'https://api.torbox.app/v1/api/torrents/checkcached'


def is_cached_on_tb(info_hash, api_key, timeout=10):
    """Probe TB checkcached for one hash. Returns True / False / None
    (None = API error; treat as 'unknown — do not remove')."""
    if not info_hash or not isinstance(info_hash, str):
        return None
    url = f'{TB_CHECKCACHED}?hash={info_hash}'
    req = urllib.request.Request(
        url, headers={'Authorization': f'Bearer {api_key}',
                      'User-Agent': 'zurgarr-blocklist-cleanup/1.0'},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read(1 * 1024 * 1024).decode('utf-8'))
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, OSError, ValueError) as e:
        print(f"  probe error: {type(e).__name__}", file=sys.stderr)
        return None
    if not data.get('success'):
        return None
    payload = data.get('data')
    if not isinstance(payload, dict):
        return None
    # TB normalises hash case differently between input and output —
    # check both.
    return (info_hash in payload) or (info_hash.lower() in payload) or (info_hash.upper() in payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--apply', action='store_true',
                        help='Actually rewrite the blocklist (default: dry-run, no changes)')
    parser.add_argument('--blocklist', default=BLOCKLIST_PATH,
                        help=f'Path to blocklist.json (default: {BLOCKLIST_PATH})')
    args = parser.parse_args()

    api_key = os.environ.get('TORBOX_API_KEY')
    if not api_key:
        print('ERROR: TORBOX_API_KEY env var not set', file=sys.stderr)
        return 2

    try:
        with open(args.blocklist, 'r', encoding='utf-8') as f:
            entries = json.load(f)
    except (OSError, ValueError) as e:
        print(f'ERROR: cannot read {args.blocklist}: {e}', file=sys.stderr)
        return 2

    if not isinstance(entries, list):
        print(f'ERROR: {args.blocklist} is not a list (top-level type: '
              f'{type(entries).__name__})', file=sys.stderr)
        return 2

    candidates = [
        e for e in entries
        if isinstance(e, dict)
        and e.get('reason') == TARGET_REASON
        and e.get('source') == TARGET_SOURCE
        and e.get('info_hash')
    ]
    print(f'Scanning {len(entries)} blocklist entries; '
          f'{len(candidates)} match the TB false-positive pattern '
          f'(reason={TARGET_REASON!r}, source={TARGET_SOURCE!r}).')

    to_remove_ids = set()
    cached_count = 0
    uncached_count = 0
    unknown_count = 0
    for e in candidates:
        h = e['info_hash']
        title = (e.get('title') or '')[:60]
        result = is_cached_on_tb(h, api_key)
        if result is True:
            cached_count += 1
            to_remove_ids.add(e.get('id'))
            print(f'  REMOVE {h[:16]}... (cached on TB) — {title}')
        elif result is False:
            uncached_count += 1
            print(f'  KEEP   {h[:16]}... (not cached on TB) — {title}')
        else:
            unknown_count += 1
            print(f'  SKIP   {h[:16]}... (probe failed/unknown) — {title}')

    print()
    print(f'Summary: {cached_count} cached (will remove), '
          f'{uncached_count} legitimately uncached (kept), '
          f'{unknown_count} unknown (kept).')

    if not args.apply:
        print()
        print('Dry-run mode — no changes written.  Re-run with --apply to commit.')
        return 0

    if not to_remove_ids:
        print('Nothing to remove.')
        return 0

    new_entries = [e for e in entries if e.get('id') not in to_remove_ids]

    # Atomic write — temp file in the same directory, then rename.
    tmp_path = args.blocklist + '.cleanup-tmp'
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(new_entries, f, indent=2)
        os.replace(tmp_path, args.blocklist)
    except OSError as e:
        print(f'ERROR: write failed: {e}', file=sys.stderr)
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass
        return 2

    print(f'Wrote {len(new_entries)} entries to {args.blocklist} '
          f'(removed {len(to_remove_ids)}).')
    return 0


if __name__ == '__main__':
    sys.exit(main())
