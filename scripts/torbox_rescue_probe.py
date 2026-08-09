#!/usr/bin/env python3
"""Smoke test: how many RD-filter-blocked hashes are cached on TorBox?

Answers the go/no-go question for the native-TorBox-as-rescue-debrid
patch.  Reads the auto-populated blocklist (entries that the May 2026
debrid-health reconciler added with ``reason='RD filter (...)'``) and
asks TorBox's ``/torrents/checkcached`` endpoint about each hash.

Usage (inside the zurgarr container, where /config/blocklist.json lives):

    TORBOX_API_KEY=... python3 scripts/torbox_rescue_probe.py
    TORBOX_API_KEY=... python3 scripts/torbox_rescue_probe.py --limit 50
    TORBOX_API_KEY=... python3 scripts/torbox_rescue_probe.py --path /config/blocklist.json --sleep 200

The script is read-only — it queries TorBox and prints a report.  It
does not add torrents, does not write the blocklist, does not touch RD.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


CHECKCACHED_URL = 'https://api.torbox.app/v1/api/torrents/checkcached'
REQUEST_TIMEOUT = 10  # seconds per hash probe
GO_THRESHOLD = 70     # >=70% TB hit rate = green-light the patch


def load_rd_blocked_hashes(path):
    """Return list of (info_hash, title) for RD-filter-blocked entries.

    Filters blocklist.json to entries the debrid-health reconciler added
    automatically — source='auto' and reason starting with 'RD filter'.
    """
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, list):
        sys.exit(f"error: {path} is not a list of entries")

    rd_entries = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        if entry.get('source') != 'auto':
            continue
        reason = entry.get('reason') or ''
        if not reason.startswith('RD filter'):
            continue
        info_hash = (entry.get('info_hash') or '').strip().upper()
        if not info_hash:
            continue
        rd_entries.append((info_hash, entry.get('title') or ''))

    # Dedup by hash, preserve first-seen title
    seen = {}
    for h, t in rd_entries:
        if h not in seen:
            seen[h] = t
    return [(h, t) for h, t in seen.items()]


def probe_one(api_key, info_hash):
    """Return 'cached', 'uncached', 'error' for a single hash.

    TorBox's response shape: ``{'success': true, 'data': {<hash>: {...}}}``
    when cached, ``{'success': true, 'data': {}}`` when not.  Anything
    else is treated as error (we never conflate API failure with
    confirmed-uncached — same invariant the in-tree probe enforces).
    """
    url = f'{CHECKCACHED_URL}?hash={urllib.parse.quote(info_hash.lower())}'
    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {api_key}',
        'User-Agent': 'zurgarr-rescue-probe/1.0',
    })
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            raw = resp.read(1024 * 1024)
            data = json.loads(raw.decode('utf-8'))
    except (urllib.error.HTTPError, urllib.error.URLError,
            json.JSONDecodeError, OSError, ValueError):
        return 'error'
    if not isinstance(data, dict) or not data.get('success'):
        return 'error'
    payload = data.get('data')
    if not isinstance(payload, dict):
        return 'error'
    # TorBox keys the response by lowercase hash
    return 'cached' if info_hash.lower() in payload else 'uncached'


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--path', default='/config/blocklist.json',
                    help='Path to blocklist.json (default: /config/blocklist.json)')
    ap.add_argument('--limit', type=int, default=0,
                    help='Probe only the first N hashes (default: 0 = all)')
    ap.add_argument('--sleep', type=int, default=100,
                    help='Milliseconds between probes (default: 100)')
    ap.add_argument('--samples', type=int, default=10,
                    help='How many cached titles to list in the report (default: 10)')
    args = ap.parse_args()

    api_key = os.environ.get('TORBOX_API_KEY', '').strip()
    if not api_key:
        sys.exit("error: TORBOX_API_KEY env var not set")

    if not os.path.isfile(args.path):
        sys.exit(f"error: blocklist not found at {args.path}")

    entries = load_rd_blocked_hashes(args.path)
    total = len(entries)
    if total == 0:
        print("No RD-filter-blocked entries found in the blocklist.")
        print("(Either DEBRID_HEALTH_AUTO_REMEDIATE has never run, or no")
        print(" filter blocks have been detected yet.)")
        return

    if args.limit and args.limit < total:
        entries = entries[:args.limit]
        print(f"Probing first {args.limit} of {total} RD-filter-blocked hashes...")
    else:
        print(f"Probing all {total} RD-filter-blocked hashes...")
    print()

    cached_titles = []
    counts = {'cached': 0, 'uncached': 0, 'error': 0}
    sleep_s = max(0, args.sleep) / 1000.0

    for i, (h, title) in enumerate(entries, 1):
        result = probe_one(api_key, h)
        counts[result] += 1
        if result == 'cached':
            cached_titles.append(title or h)
        # Progress every 10 to keep output readable
        if i % 10 == 0 or i == len(entries):
            print(f"  [{i}/{len(entries)}]  "
                  f"cached={counts['cached']}  "
                  f"uncached={counts['uncached']}  "
                  f"errors={counts['error']}")
        if sleep_s and i < len(entries):
            time.sleep(sleep_s)

    probed = len(entries)
    # Hit rate excludes API errors from the denominator so a flaky
    # network doesn't suppress an otherwise-good signal.
    answered = counts['cached'] + counts['uncached']
    hit_rate = (counts['cached'] / answered * 100) if answered else 0.0

    print()
    print("=" * 60)
    print(f"  Total RD-filter-blocked hashes in blocklist: {total}")
    print(f"  Probed:                                      {probed}")
    print(f"  Cached on TorBox:                            {counts['cached']}")
    print(f"  Confirmed NOT on TorBox:                     {counts['uncached']}")
    print(f"  API errors / timeouts:                       {counts['error']}")
    print(f"  Hit rate (cached / answered):                {hit_rate:.1f}%")
    print("=" * 60)
    if hit_rate >= GO_THRESHOLD:
        verdict = f"GO  — TorBox covers >={GO_THRESHOLD}% of RD-blocked content."
    elif hit_rate >= 40:
        verdict = ("MARGINAL — TorBox covers some but not most RD-blocked "
                   "content.  Patch may still be worth it if your specific "
                   "missing titles are in the covered set.")
    else:
        verdict = (f"NO-GO — TorBox covers <40% of RD-blocked content.  "
                   "Native rescue patch isn't worth the engineering.")
    print(f"  Verdict: {verdict}")
    print("=" * 60)

    if cached_titles and args.samples > 0:
        print()
        print(f"Sample of titles TorBox has that RD filter-blocked "
              f"(first {min(args.samples, len(cached_titles))}):")
        for t in cached_titles[:args.samples]:
            print(f"  - {t}")


if __name__ == '__main__':
    main()
