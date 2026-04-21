# plex_debrid attribution

The contents of this directory are a vendored copy of upstream
[plex_debrid by itsToggle](https://github.com/itsToggle/plex_debrid),
with modifications applied for integration into pd_zurg.

The upstream plex_debrid project does not declare an explicit license;
all rights to the upstream code are reserved by its author. This
vendored copy is preserved here so pd_zurg can ship a self-contained
Docker image with attribution to the original author. pd_zurg does
not assert ownership of, or grant any license to, the upstream
plex_debrid code. Users who need formal redistribution rights for
plex_debrid should contact the upstream author (itsToggle).

## Modifications

Local modifications to the vendored tree are visible in the pd_zurg git
history (`git log -- plex_debrid/`). Notable areas of change include
content service additions (e.g. `content/services/mdblist.py`), debrid
service tweaks (`debrid/services/realdebrid.py`), and scraper adjustments
(`scraper/services/torrentio.py`). The pd_zurg-authored wrappers that
launch and supervise plex_debrid live in `plex_debrid_/` (with trailing
underscore) and are covered by pd_zurg's own LICENSE.

## Reporting

Issues that originate in vendored upstream code should ideally be
reported to the upstream plex_debrid project. Issues with the
pd_zurg-side wrappers, vendoring, or modifications should be reported
to the pd_zurg issue tracker.
