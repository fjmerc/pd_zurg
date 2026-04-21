# Third-Party Notices

Zurgarr integrates and/or redistributes the following third-party
components. The Zurgarr project's own MIT License (see `LICENSE`)
covers the code authored for this repository; third-party components
retain the copyright and license terms of their respective upstreams.

## rclone

- **Upstream:** https://github.com/rclone/rclone
- **Author:** Nick Craig-Wood
- **License:** MIT (see `LICENSES/rclone.LICENSE`)
- **How it is used:** rclone is downloaded as a binary at Docker image
  build time and invoked as a subprocess at runtime to mount a debrid
  WebDAV endpoint as a local filesystem. The Python wrappers under
  `rclone/` are Zurgarr-original code that orchestrates the rclone
  binary; they are covered by Zurgarr's own LICENSE.

## Zurg

- **Upstream:** https://github.com/debridmediamanager/zurg-testing
- **Author:** yowmamasita / debridmediamanager
- **License:** No license declared by the upstream project.
- **How it is used:** Zurg is downloaded as a binary at Docker image
  build time and invoked as a subprocess at runtime to expose a debrid
  account as a WebDAV server. The Python wrappers under `zurg/` are
  Zurgarr-original code that orchestrates the Zurg binary; they are
  covered by Zurgarr's own LICENSE.
- **Note on licensing:** the upstream Zurg repository does not declare
  an explicit license. Zurgarr fetches and bundles Zurg in its
  publicly distributed binary form and does not redistribute Zurg's
  source code. Users who require formal licensing terms for Zurg
  should contact the upstream author directly.

## plex_debrid

- **Upstream:** https://github.com/itsToggle/plex_debrid
- **Author:** itsToggle
- **License:** No license declared by the upstream project.
- **How it is used:** the `plex_debrid/` directory in this repository
  is a vendored copy of the upstream plex_debrid source, with local
  modifications. See `plex_debrid/ATTRIBUTION.md` for details on the
  vendoring relationship and the modifications made.
- **Note on licensing:** the upstream plex_debrid repository does not
  declare an explicit license. The vendored copy is preserved here so
  that Zurgarr can ship a self-contained Docker image. Zurgarr does
  not assert ownership of, or grant any license to, the upstream
  plex_debrid code; the original author's rights are reserved. Users
  who require formal licensing terms for plex_debrid should consult
  the upstream project directly.

## Python runtime dependencies

Python packages installed via `requirements.txt` and `requirements-dev.txt`
are not vendored into this repository — they are fetched at install or
build time from PyPI and retain their own licenses as published there.
Each dependency's license can be inspected via `pip show <package>` in a
running container or via the package's PyPI listing.
