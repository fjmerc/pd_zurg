from json import load, dump
from dotenv import load_dotenv, find_dotenv
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler, BaseRotatingHandler
from packaging.version import Version, parse as parse_version
import time
import os
import ast
import requests
import zipfile
import io
import shutil
import regex
import subprocess
import schedule
import psutil
import sys
import threading
import glob
import re
import random
import platform
import fnmatch
import signal
import socket
from colorlog import ColoredFormatter
from ruamel.yaml import YAML


__all__ = [
    # Standard library modules
    'load', 'dump', 'datetime', 'timedelta',
    'logging', 'RotatingFileHandler', 'TimedRotatingFileHandler', 'BaseRotatingHandler',
    'Version', 'parse_version',
    'time', 'os', 'ast', 'requests', 'zipfile', 'io', 'shutil', 'regex',
    'subprocess', 'schedule', 'psutil', 'sys', 'threading', 'glob', 're',
    'random', 'platform', 'fnmatch', 'signal', 'socket',
    # Third-party
    'ColoredFormatter', 'YAML',
    # Functions
    'load_secret_or_env', 'is_port_available', 'find_available_port',
    'refresh_globals',
    # Config
    'Config', 'config',
    # Config variables
    'PLEXDEBRID', 'PDLOGLEVEL', 'PLEXUSER', 'PLEXTOKEN',
    'JFADD', 'JFAPIKEY', 'RDAPIKEY', 'ADAPIKEY',
    'TORBOXAPIKEY', 'TORBOXWEBDAVUSER', 'TORBOXWEBDAVPASS', 'TORBOX_MOUNT_NAME',
    'TORBOX_RCLONE_TPSLIMIT', 'TORBOX_RCLONE_TPSLIMIT_BURST',
    'TORBOX_RCLONE_DIR_CACHE_TIME', 'TORBOX_SCAN_TIMEOUT',
    'GHTOKEN',
    'SEERRAPIKEY', 'SEERRADD', 'PLEXADD', 'ZURGUSER', 'ZURGPASS',
    'SHOWMENU', 'LOGFILE', 'PDUPDATE', 'PDREPO',
    'DUPECLEAN', 'CLEANUPINT', 'DUPECLEANKEEP', 'RCLONEMN', 'RCLONELOGLEVEL',
    'ZURG', 'ZURGVERSION', 'ZURGLOGLEVEL', 'ZURGUPDATE',
    'PLEXREFRESH', 'PLEXMOUNT', 'NFSMOUNT', 'NFSPORT', 'ZURGPORT',
    'TRAKTCLIENTID', 'TRAKTCLIENTSECRET',
    'NOTIFICATION_URL', 'NOTIFICATION_EVENTS', 'NOTIFICATION_LEVEL',
    'BLACKHOLE_ENABLED', 'BLACKHOLE_DIR', 'BLACKHOLE_POLL_INTERVAL', 'BLACKHOLE_DEBRID',
    'BLACKHOLE_DEBRID_ROUTING', 'BLACKHOLE_DEBRID_PRIMARY',
    'BLACKHOLE_SYMLINK_ENABLED', 'BLACKHOLE_COMPLETED_DIR', 'BLACKHOLE_RCLONE_MOUNT',
    'BLACKHOLE_SYMLINK_TARGET_BASE', 'BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX',
    'BLACKHOLE_MOUNT_POLL_TIMEOUT',
    'BLACKHOLE_MOUNT_POLL_INTERVAL', 'BLACKHOLE_SYMLINK_MAX_AGE',
    'STATUS_UI_ENABLED', 'STATUS_UI_PORT', 'STATUS_UI_AUTH',
    'SONARR_URL', 'SONARR_API_KEY', 'RADARR_URL', 'RADARR_API_KEY',
    # Scheduled task intervals
    'ROUTING_AUDIT_INTERVAL', 'QUEUE_CLEANUP_INTERVAL',
    'LIBRARY_SCAN_INTERVAL', 'LIBRARY_RESCAN_NFS_DELAY',
    'SYMLINK_VERIFY_INTERVAL',
    'PREFERENCE_ENFORCE_INTERVAL', 'HOUSEKEEPING_INTERVAL',
    'CONFIG_BACKUP_INTERVAL', 'CONFIG_BACKUP_RETENTION', 'MOUNT_LIVENESS_INTERVAL',
    'MOUNT_SELFHEAL_ENABLED',
    # History
    'HISTORY_RETENTION_DAYS',
    # Notification digest
    'NOTIFICATION_DIGEST_ENABLED', 'NOTIFICATION_DIGEST_TIME',
    # Blocklist
    'BLOCKLIST_AUTO_ADD', 'BLOCKLIST_EXPIRY_DAYS',
    # Symlink repair
    'SYMLINK_REPAIR_AUTO_SEARCH',
    # Debrid health reconciler (plan 38)
    'DEBRID_HEALTH_ENABLED', 'DEBRID_HEALTH_AUTO_REMEDIATE',
    'DEBRID_HEALTH_CROSS_RESCUE',
    # Routing audit
    'ROUTING_AUTO_TAG_UNTAGGED',
    # Gap-fill reconcile
    'GAP_FILL_ENABLED',
    # Wanted proactive recovery (TorBox + RealDebrid legs)
    'WANTED_TB_RECOVERY_ENABLED', 'WANTED_TB_RECOVERY_MAX_PER_SCAN',
    'WANTED_RD_RECOVERY_ENABLED', 'WANTED_RD_RECOVERY_MAX_PER_SCAN',
    'WANTED_SEASON_RECOVERY_ENABLED',
    # Debrid search
    'TORRENTIO_URL', 'SEARCH_REQUIRE_CACHED', 'SEARCH_DEDUP_ENABLED',
    # Blackhole cache / debrid-account dedup gates
    'BLACKHOLE_REQUIRE_CACHED', 'BLACKHOLE_DEBRID_DEDUP_ENABLED',
    'BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'BLACKHOLE_TB_ALT_RECOVERY_ENABLED',
    'BLACKHOLE_ARR_FAILED_FEEDBACK_ENABLED',
    # plex_debrid content-version cache-rule enforcer
    'PD_ENFORCE_CACHED_VERSIONS',
    # Quality compromise (plan 33)
    'QUALITY_COMPROMISE_ENABLED', 'QUALITY_COMPROMISE_DWELL_DAYS',
    'QUALITY_COMPROMISE_MIN_SEEDERS', 'QUALITY_COMPROMISE_ONLY_CACHED',
    'QUALITY_COMPROMISE_MAX_TIER_DROP', 'QUALITY_COMPROMISE_NOTIFY',
    'SEASON_PACK_FALLBACK_ENABLED', 'SEASON_PACK_FALLBACK_MIN_MISSING',
    'SEASON_PACK_FALLBACK_MIN_RATIO',
]

load_dotenv(find_dotenv('./config/.env'))


def load_secret_or_env(secret_name, default=None):
    secret_file = f'/run/secrets/{secret_name}'
    try:
        with open(secret_file, 'r') as file:
            return file.read().strip()
    except IOError:
        return os.getenv(secret_name.upper(), default)


def is_port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # SO_REUSEADDR matches how the Go services (zurg/rclone) bind, so a
        # port lingering in TIME_WAIT isn't falsely reported as unavailable.
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(('', port))
            return True
        except OSError:
            return False


def find_available_port(range_start, range_end, max_attempts=50):
    for _ in range(max_attempts):
        port = random.randint(range_start, range_end)
        if is_port_available(port):
            return port
    raise RuntimeError(f"Could not find an available port in range {range_start}-{range_end} after {max_attempts} attempts")


def refresh_globals(target_globals):
    """Refresh a module's config globals from the Config singleton.

    After a SIGHUP config reload, modules that used ``from base import *``
    still hold stale values because Python copied them at import time.
    Call this at the top of any setup/init function that may be re-invoked
    after a reload::

        def setup():
            refresh_globals(globals())
            # RDAPIKEY, PLEXADD, etc. are now up-to-date
    """
    for name in __all__:
        if hasattr(config, name):
            target_globals[name] = getattr(config, name)


class Config:
    """Centralized configuration loaded from environment variables and secrets.

    Supports reload() for re-reading environment at runtime and can be
    instantiated independently for testing.
    """

    def __init__(self):
        self.load()

    def load(self):
        load_dotenv(find_dotenv('./config/.env'), override=False)

        self.PLEXDEBRID = os.getenv("PD_ENABLED")
        self.PDLOGLEVEL = os.getenv("PD_LOG_LEVEL")
        self.PLEXUSER = load_secret_or_env('plex_user')
        self.PLEXTOKEN = load_secret_or_env('plex_token')
        self.JFADD = load_secret_or_env('jf_address')
        self.JFAPIKEY = load_secret_or_env('jf_api_key')
        self.RDAPIKEY = load_secret_or_env('rd_api_key')
        self.ADAPIKEY = load_secret_or_env('ad_api_key')
        # TorBox co-debrid (plan 39).  Zurg can't proxy TorBox, so the mount
        # goes through rclone's native webdav remote against
        # https://webdav.torbox.app/ — auth is HTTP Basic with the account
        # email and a *separately configured* WebDAV-only password (set in
        # TorBox dashboard → Settings → Integrations → WebDAV).  The API key
        # alone does NOT authenticate the mount; it's the credential used by
        # the existing search.py / blackhole.py / debrid_client.py code paths.
        self.TORBOXAPIKEY = load_secret_or_env('torbox_api_key')
        self.TORBOXWEBDAVUSER = load_secret_or_env('torbox_webdav_user')
        self.TORBOXWEBDAVPASS = load_secret_or_env('torbox_webdav_pass')
        # Attr name matches env var name (with underscores) so the
        # _ENV_DEFAULTS drift guard in tests/test_settings_api.py
        # ::test_env_defaults_stays_in_sync_with_config can verify it.
        self.TORBOX_MOUNT_NAME = os.getenv('TORBOX_MOUNT_NAME', 'torbox')
        # Plan 41 phase D: TB rclone tpslimit knobs.  TB rate-limits
        # reads aggressively under concurrent Plex/Bazarr scans; default
        # 5 tps / 10-burst stays under the observed Essential-tier
        # ceiling.  Set either to '0' to omit the flag entirely.
        self.TORBOX_RCLONE_TPSLIMIT = os.getenv('TORBOX_RCLONE_TPSLIMIT', '5')
        self.TORBOX_RCLONE_TPSLIMIT_BURST = os.getenv('TORBOX_RCLONE_TPSLIMIT_BURST', '3')
        # TB's dir cache must outlive the hourly library scan. With the
        # default 30m it expires before the next scan, so every scan re-lists
        # all release folders over the throttled (tpslimit) FUSE mount and
        # times out — dropping TB titles, which then flip to "Wanted".
        # Default 2h > LIBRARY_SCAN_INTERVAL (1h); the blackhole grab hook
        # calls vfs/refresh, so new content still appears promptly between
        # expiries rather than waiting out the full TTL.
        self.TORBOX_RCLONE_DIR_CACHE_TIME = os.getenv('TORBOX_RCLONE_DIR_CACHE_TIME', '2h')
        # Wall-clock budget (seconds) for the TB FUSE walk during a library
        # scan. The shared scan deadline (30s) can't enumerate a large TB
        # mount on a cold cache at 5 tps (~450 folders ≈ 90s); give TB its
        # own budget so cold scans complete instead of truncating.
        self.TORBOX_SCAN_TIMEOUT = os.getenv('TORBOX_SCAN_TIMEOUT', '180')
        self.GHTOKEN = load_secret_or_env('GITHUB_TOKEN')
        self.SEERRAPIKEY = load_secret_or_env('seerr_api_key')
        self.SEERRADD = load_secret_or_env('seerr_address')
        self.PLEXADD = load_secret_or_env('plex_address')
        self.ZURGUSER = load_secret_or_env('zurg_user')
        self.ZURGPASS = load_secret_or_env('zurg_pass')
        self.SHOWMENU = os.getenv('SHOW_MENU')
        self.LOGFILE = os.getenv('PD_LOGFILE')
        self.PDUPDATE = os.getenv('PD_UPDATE')
        self.PDREPO = os.getenv('PD_REPO')
        self.DUPECLEAN = os.getenv('DUPLICATE_CLEANUP')
        self.CLEANUPINT = os.getenv('CLEANUP_INTERVAL')
        self.DUPECLEANKEEP = os.getenv('DUPLICATE_CLEANUP_KEEP')
        self.RCLONEMN = os.getenv("RCLONE_MOUNT_NAME")
        self.RCLONELOGLEVEL = os.getenv("RCLONE_LOG_LEVEL")
        self.ZURG = os.getenv("ZURG_ENABLED")
        self.ZURGVERSION = os.getenv("ZURG_VERSION")
        self.ZURGLOGLEVEL = os.getenv("ZURG_LOG_LEVEL")
        self.ZURGUPDATE = os.getenv('ZURG_UPDATE')
        self.PLEXREFRESH = os.getenv('PLEX_REFRESH')
        self.PLEXMOUNT = os.getenv('PLEX_MOUNT_DIR')
        self.NFSMOUNT = os.getenv('NFS_ENABLED')
        self.NFSPORT = os.getenv('NFS_PORT')
        self.ZURGPORT = os.getenv('ZURG_PORT')
        self.TRAKTCLIENTID = os.getenv('TRAKT_CLIENT_ID')
        self.TRAKTCLIENTSECRET = os.getenv('TRAKT_CLIENT_SECRET')
        self.NOTIFICATION_URL = os.getenv('NOTIFICATION_URL')
        self.NOTIFICATION_EVENTS = os.getenv('NOTIFICATION_EVENTS')
        self.NOTIFICATION_LEVEL = os.getenv('NOTIFICATION_LEVEL')
        self.BLACKHOLE_ENABLED = os.getenv('BLACKHOLE_ENABLED')
        self.BLACKHOLE_DIR = os.getenv('BLACKHOLE_DIR')
        self.BLACKHOLE_POLL_INTERVAL = os.getenv('BLACKHOLE_POLL_INTERVAL')
        self.BLACKHOLE_DEBRID = os.getenv('BLACKHOLE_DEBRID')
        # Per-grab debrid routing (plan 39 phase 2).  Both vars resolve
        # to runtime defaults in utils/debrid_routing.py when left empty:
        # routing mode defaults to ``cache_aware`` when two or more
        # debrids are configured, ``primary_only`` otherwise; primary
        # defaults to the first-configured of RD, AD, TB.
        self.BLACKHOLE_DEBRID_ROUTING = os.getenv('BLACKHOLE_DEBRID_ROUTING')
        self.BLACKHOLE_DEBRID_PRIMARY = os.getenv('BLACKHOLE_DEBRID_PRIMARY')
        self.BLACKHOLE_SYMLINK_ENABLED = os.getenv('BLACKHOLE_SYMLINK_ENABLED')
        self.BLACKHOLE_COMPLETED_DIR = os.getenv('BLACKHOLE_COMPLETED_DIR')
        self.BLACKHOLE_RCLONE_MOUNT = os.getenv('BLACKHOLE_RCLONE_MOUNT')
        self.BLACKHOLE_SYMLINK_TARGET_BASE = os.getenv('BLACKHOLE_SYMLINK_TARGET_BASE')
        # TorBox symlink target base (plan 39 phase 2) — host-side path
        # for TB-routed symlinks.  When unset and the RD base is set,
        # debrid_routing.symlink_target_base_for_debrid() falls back to
        # ``<RD base>_torbox`` so most users get a sensible default.
        self.BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX = os.getenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX')
        self.BLACKHOLE_MOUNT_POLL_TIMEOUT = os.getenv('BLACKHOLE_MOUNT_POLL_TIMEOUT')
        self.BLACKHOLE_MOUNT_POLL_INTERVAL = os.getenv('BLACKHOLE_MOUNT_POLL_INTERVAL')
        self.BLACKHOLE_SYMLINK_MAX_AGE = os.getenv('BLACKHOLE_SYMLINK_MAX_AGE')
        self.STATUS_UI_ENABLED = os.getenv('STATUS_UI_ENABLED')
        self.STATUS_UI_PORT = os.getenv('STATUS_UI_PORT')
        self.STATUS_UI_AUTH = os.getenv('STATUS_UI_AUTH')
        self.SONARR_URL = os.getenv('SONARR_URL')
        self.SONARR_API_KEY = load_secret_or_env('sonarr_api_key')
        self.RADARR_URL = os.getenv('RADARR_URL')
        self.RADARR_API_KEY = load_secret_or_env('radarr_api_key')
        # Scheduled task intervals (seconds, stored as strings from env)
        self.ROUTING_AUDIT_INTERVAL = os.getenv('ROUTING_AUDIT_INTERVAL')
        self.QUEUE_CLEANUP_INTERVAL = os.getenv('QUEUE_CLEANUP_INTERVAL')
        self.LIBRARY_SCAN_INTERVAL = os.getenv('LIBRARY_SCAN_INTERVAL')
        # Delay (seconds, default '0') inserted between symlink creation
        # and the immediate arr rescan trigger to let an NFS attribute
        # cache invalidate before Sonarr/Radarr walks the share.  Plan
        # 41 phase B.2 — see TROUBLESHOOTING entry for the symptom.
        self.LIBRARY_RESCAN_NFS_DELAY = os.getenv('LIBRARY_RESCAN_NFS_DELAY', '0')
        self.SYMLINK_VERIFY_INTERVAL = os.getenv('SYMLINK_VERIFY_INTERVAL')
        self.PREFERENCE_ENFORCE_INTERVAL = os.getenv('PREFERENCE_ENFORCE_INTERVAL')
        self.HOUSEKEEPING_INTERVAL = os.getenv('HOUSEKEEPING_INTERVAL')
        self.CONFIG_BACKUP_INTERVAL = os.getenv('CONFIG_BACKUP_INTERVAL')
        self.CONFIG_BACKUP_RETENTION = os.getenv('CONFIG_BACKUP_RETENTION', '7')
        self.MOUNT_LIVENESS_INTERVAL = os.getenv('MOUNT_LIVENESS_INTERVAL')
        self.MOUNT_SELFHEAL_ENABLED = os.getenv('MOUNT_SELFHEAL_ENABLED', 'true')
        # History
        self.HISTORY_RETENTION_DAYS = os.getenv('HISTORY_RETENTION_DAYS')
        # Notification digest
        self.NOTIFICATION_DIGEST_ENABLED = os.getenv('NOTIFICATION_DIGEST_ENABLED', 'false')
        self.NOTIFICATION_DIGEST_TIME = os.getenv('NOTIFICATION_DIGEST_TIME', '08:00')
        # Blocklist
        self.BLOCKLIST_AUTO_ADD = os.getenv('BLOCKLIST_AUTO_ADD', 'true')
        self.BLOCKLIST_EXPIRY_DAYS = os.getenv('BLOCKLIST_EXPIRY_DAYS', '0')
        # Symlink repair
        self.SYMLINK_REPAIR_AUTO_SEARCH = os.getenv('SYMLINK_REPAIR_AUTO_SEARCH', 'false')
        # Debrid health reconciler (plan 38) — detects RD's May 2026 keyword
        # filter blocks on the existing torrent set. Default ON for detection;
        # remediation (delete + arr re-search) gates separately via
        # AUTO_REMEDIATE — opt-in because the remediation path mutates RD
        # account state (deletes torrents) and triggers arr searches.
        self.DEBRID_HEALTH_ENABLED = os.getenv('DEBRID_HEALTH_ENABLED', 'true')
        self.DEBRID_HEALTH_AUTO_REMEDIATE = os.getenv('DEBRID_HEALTH_AUTO_REMEDIATE', 'false')
        # Cross-debrid rescue (plan 39 phase 3) — when RD filter-blocks
        # a torrent and TB has it cached, re-host on TB instead of just
        # deleting + asking the arr to find a different release (which
        # loops on the same filter).  Default-resolved at runtime in
        # ``utils.debrid_health._cross_rescue_enabled``: on when both
        # RD_API_KEY and TORBOX_API_KEY are set, off otherwise.  Explicit
        # ``true``/``false`` overrides the default.
        self.DEBRID_HEALTH_CROSS_RESCUE = os.getenv('DEBRID_HEALTH_CROSS_RESCUE')
        # Routing audit (auto-tag untagged monitored series/movies with debrid tag)
        self.ROUTING_AUTO_TAG_UNTAGGED = os.getenv('ROUTING_AUTO_TAG_UNTAGGED', 'true')
        # Gap-fill reconcile — unconditional missing-episode search across
        # debrid + local.  Also auto-enables verify_symlinks re-search.
        self.GAP_FILL_ENABLED = os.getenv('GAP_FILL_ENABLED', 'true')
        # Wanted→TorBox proactive recovery — grab TB-cached copies of Wanted
        # ghosts the arr never grabbed, bounded per scan.
        self.WANTED_TB_RECOVERY_ENABLED = os.getenv('WANTED_TB_RECOVERY_ENABLED', 'true')
        self.WANTED_TB_RECOVERY_MAX_PER_SCAN = os.getenv('WANTED_TB_RECOVERY_MAX_PER_SCAN', '2')
        # RD leg: RD's cache probe is dead, so the add is the probe — add,
        # keep if instantly ready, delete + fall back to the TB leg if not.
        self.WANTED_RD_RECOVERY_ENABLED = os.getenv('WANTED_RD_RECOVERY_ENABLED', 'true')
        self.WANTED_RD_RECOVERY_MAX_PER_SCAN = os.getenv('WANTED_RD_RECOVERY_MAX_PER_SCAN', '4')
        # Season-pack extension: probe TB packs for partial-show seasons
        # with missing aired episodes (TB-only, shares the TB budget).
        self.WANTED_SEASON_RECOVERY_ENABLED = os.getenv('WANTED_SEASON_RECOVERY_ENABLED', 'true')
        # Debrid search
        self.TORRENTIO_URL = os.getenv('TORRENTIO_URL')
        # Refuse the interactive "Add" button when the chosen hash is not
        # confirmed cached on the debrid provider (default OFF — RD has no
        # working cache probe, so ON effectively blocks all RD adds).
        self.SEARCH_REQUIRE_CACHED = os.getenv('SEARCH_REQUIRE_CACHED', 'false')
        # Skip the interactive "Add" call when the hash is already on the
        # account — stops duplicate entries that the user has to clean up
        # in DMM.  Default ON (cheap API list + 30s TTL cache).
        self.SEARCH_DEDUP_ENABLED = os.getenv('SEARCH_DEDUP_ENABLED', 'true')
        # Same two gates for the Sonarr/Radarr blackhole.  Dedup defaults ON;
        # require-cached defaults OFF (see SEARCH_REQUIRE_CACHED for the RD
        # caveat).
        self.BLACKHOLE_REQUIRE_CACHED = os.getenv('BLACKHOLE_REQUIRE_CACHED', 'false')
        # NOTE: ``BLACKHOLE_DEDUP_ENABLED`` (read directly in ``blackhole.py``)
        # is the local-filesystem library-dedup gate — a different feature.
        # This one skips hashes already on the debrid account.
        self.BLACKHOLE_DEBRID_DEDUP_ENABLED = os.getenv('BLACKHOLE_DEBRID_DEDUP_ENABLED', 'true')
        # When ON, a blackhole torrent that's still uncached when
        # BLACKHOLE_MOUNT_POLL_TIMEOUT expires is actively deleted from the
        # debrid account, not just dropped from pending tracking.  Prevents
        # 0%/0-seed junk from accumulating on RD.  Default OFF because it
        # changes data state (deletes torrents from the debrid account)
        # and a user who tolerates long cache waits may want the torrent
        # to survive past Zurgarr's patience.
        self.BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT = os.getenv(
            'BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'false'
        )
        # When ON, an uncached blackhole grab that would otherwise be
        # rejected triggers a search for a same-title, same-tier release
        # cached on TorBox to grab instead — so a well-cached title isn't
        # dropped to "Wanted" just because the specific hash the arr picked
        # is uncached.  Read live from os.environ in blackhole.py so a UI
        # change applies on the next grab; declared here for the globals
        # export + settings drift guard.  No-op without TorBox configured.
        self.BLACKHOLE_TB_ALT_RECOVERY_ENABLED = os.getenv(
            'BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true'
        )
        # When ON, an uncached-rejected grab is reported back to the owning
        # arr via the failed-download API (blocklist + immediate re-search)
        # instead of being silently deleted — which otherwise makes the arr
        # re-grab the identical release on every RSS pass, forever.  Read
        # live from os.environ in blackhole.py; declared here for the
        # globals export + settings drift guard.
        self.BLACKHOLE_ARR_FAILED_FEEDBACK_ENABLED = os.getenv(
            'BLACKHOLE_ARR_FAILED_FEEDBACK_ENABLED', 'true'
        )
        # When ON, plex_debrid setup injects the ``cache status / requirement
        # / cached`` rule into every content version on startup so the
        # vendored download path refuses uncached releases.  Default OFF
        # to preserve existing behavior for users who deliberately want
        # uncached fallback.  Idempotent — safe to leave ON.
        self.PD_ENFORCE_CACHED_VERSIONS = os.getenv('PD_ENFORCE_CACHED_VERSIONS', 'false')
        # Quality compromise (plan 33) — opt-in, strict defaults.  Phase 7
        # exposes these in the settings UI and the soft-reload set; string
        # shapes are preserved so ``str(VAR).lower() == 'true'`` keeps
        # working uniformly across boolean toggles (CLAUDE.md rule).
        self.QUALITY_COMPROMISE_ENABLED = os.getenv('QUALITY_COMPROMISE_ENABLED', 'false')
        self.QUALITY_COMPROMISE_DWELL_DAYS = os.getenv('QUALITY_COMPROMISE_DWELL_DAYS', '3')
        self.QUALITY_COMPROMISE_MIN_SEEDERS = os.getenv('QUALITY_COMPROMISE_MIN_SEEDERS', '3')
        self.QUALITY_COMPROMISE_ONLY_CACHED = os.getenv('QUALITY_COMPROMISE_ONLY_CACHED', 'true')
        self.QUALITY_COMPROMISE_MAX_TIER_DROP = os.getenv('QUALITY_COMPROMISE_MAX_TIER_DROP', '2')
        self.QUALITY_COMPROMISE_NOTIFY = os.getenv('QUALITY_COMPROMISE_NOTIFY', 'true')
        self.SEASON_PACK_FALLBACK_ENABLED = os.getenv('SEASON_PACK_FALLBACK_ENABLED', 'false')
        self.SEASON_PACK_FALLBACK_MIN_MISSING = os.getenv('SEASON_PACK_FALLBACK_MIN_MISSING', '4')
        self.SEASON_PACK_FALLBACK_MIN_RATIO = os.getenv('SEASON_PACK_FALLBACK_MIN_RATIO', '0.4')


# Default singleton instance — used by existing code via module-level globals
config = Config()

# Backward-compatible module-level variables
PLEXDEBRID = config.PLEXDEBRID
PDLOGLEVEL = config.PDLOGLEVEL
PLEXUSER = config.PLEXUSER
PLEXTOKEN = config.PLEXTOKEN
JFADD = config.JFADD
JFAPIKEY = config.JFAPIKEY
RDAPIKEY = config.RDAPIKEY
ADAPIKEY = config.ADAPIKEY
TORBOXAPIKEY = config.TORBOXAPIKEY
TORBOXWEBDAVUSER = config.TORBOXWEBDAVUSER
TORBOXWEBDAVPASS = config.TORBOXWEBDAVPASS
TORBOX_MOUNT_NAME = config.TORBOX_MOUNT_NAME
TORBOX_RCLONE_TPSLIMIT = config.TORBOX_RCLONE_TPSLIMIT
TORBOX_RCLONE_TPSLIMIT_BURST = config.TORBOX_RCLONE_TPSLIMIT_BURST
TORBOX_RCLONE_DIR_CACHE_TIME = config.TORBOX_RCLONE_DIR_CACHE_TIME
TORBOX_SCAN_TIMEOUT = config.TORBOX_SCAN_TIMEOUT
GHTOKEN = config.GHTOKEN
SEERRAPIKEY = config.SEERRAPIKEY
SEERRADD = config.SEERRADD
PLEXADD = config.PLEXADD
ZURGUSER = config.ZURGUSER
ZURGPASS = config.ZURGPASS
SHOWMENU = config.SHOWMENU
LOGFILE = config.LOGFILE
PDUPDATE = config.PDUPDATE
PDREPO = config.PDREPO
DUPECLEAN = config.DUPECLEAN
CLEANUPINT = config.CLEANUPINT
DUPECLEANKEEP = config.DUPECLEANKEEP
RCLONEMN = config.RCLONEMN
RCLONELOGLEVEL = config.RCLONELOGLEVEL
ZURG = config.ZURG
ZURGVERSION = config.ZURGVERSION
ZURGLOGLEVEL = config.ZURGLOGLEVEL
ZURGUPDATE = config.ZURGUPDATE
PLEXREFRESH = config.PLEXREFRESH
PLEXMOUNT = config.PLEXMOUNT
NFSMOUNT = config.NFSMOUNT
NFSPORT = config.NFSPORT
ZURGPORT = config.ZURGPORT
TRAKTCLIENTID = config.TRAKTCLIENTID
TRAKTCLIENTSECRET = config.TRAKTCLIENTSECRET
NOTIFICATION_URL = config.NOTIFICATION_URL
NOTIFICATION_EVENTS = config.NOTIFICATION_EVENTS
NOTIFICATION_LEVEL = config.NOTIFICATION_LEVEL
BLACKHOLE_ENABLED = config.BLACKHOLE_ENABLED
BLACKHOLE_DIR = config.BLACKHOLE_DIR
BLACKHOLE_POLL_INTERVAL = config.BLACKHOLE_POLL_INTERVAL
BLACKHOLE_DEBRID = config.BLACKHOLE_DEBRID
BLACKHOLE_DEBRID_ROUTING = config.BLACKHOLE_DEBRID_ROUTING
BLACKHOLE_DEBRID_PRIMARY = config.BLACKHOLE_DEBRID_PRIMARY
BLACKHOLE_SYMLINK_ENABLED = config.BLACKHOLE_SYMLINK_ENABLED
BLACKHOLE_COMPLETED_DIR = config.BLACKHOLE_COMPLETED_DIR
BLACKHOLE_RCLONE_MOUNT = config.BLACKHOLE_RCLONE_MOUNT
BLACKHOLE_SYMLINK_TARGET_BASE = config.BLACKHOLE_SYMLINK_TARGET_BASE
BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX = config.BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX
BLACKHOLE_MOUNT_POLL_TIMEOUT = config.BLACKHOLE_MOUNT_POLL_TIMEOUT
BLACKHOLE_MOUNT_POLL_INTERVAL = config.BLACKHOLE_MOUNT_POLL_INTERVAL
BLACKHOLE_SYMLINK_MAX_AGE = config.BLACKHOLE_SYMLINK_MAX_AGE
STATUS_UI_ENABLED = config.STATUS_UI_ENABLED
STATUS_UI_PORT = config.STATUS_UI_PORT
STATUS_UI_AUTH = config.STATUS_UI_AUTH
SONARR_URL = config.SONARR_URL
SONARR_API_KEY = config.SONARR_API_KEY
RADARR_URL = config.RADARR_URL
RADARR_API_KEY = config.RADARR_API_KEY
ROUTING_AUDIT_INTERVAL = config.ROUTING_AUDIT_INTERVAL
QUEUE_CLEANUP_INTERVAL = config.QUEUE_CLEANUP_INTERVAL
LIBRARY_SCAN_INTERVAL = config.LIBRARY_SCAN_INTERVAL
LIBRARY_RESCAN_NFS_DELAY = config.LIBRARY_RESCAN_NFS_DELAY
SYMLINK_VERIFY_INTERVAL = config.SYMLINK_VERIFY_INTERVAL
PREFERENCE_ENFORCE_INTERVAL = config.PREFERENCE_ENFORCE_INTERVAL
HOUSEKEEPING_INTERVAL = config.HOUSEKEEPING_INTERVAL
CONFIG_BACKUP_INTERVAL = config.CONFIG_BACKUP_INTERVAL
CONFIG_BACKUP_RETENTION = config.CONFIG_BACKUP_RETENTION
MOUNT_LIVENESS_INTERVAL = config.MOUNT_LIVENESS_INTERVAL
MOUNT_SELFHEAL_ENABLED = config.MOUNT_SELFHEAL_ENABLED
HISTORY_RETENTION_DAYS = config.HISTORY_RETENTION_DAYS
NOTIFICATION_DIGEST_ENABLED = config.NOTIFICATION_DIGEST_ENABLED
NOTIFICATION_DIGEST_TIME = config.NOTIFICATION_DIGEST_TIME
BLOCKLIST_AUTO_ADD = config.BLOCKLIST_AUTO_ADD
BLOCKLIST_EXPIRY_DAYS = config.BLOCKLIST_EXPIRY_DAYS
SYMLINK_REPAIR_AUTO_SEARCH = config.SYMLINK_REPAIR_AUTO_SEARCH
DEBRID_HEALTH_ENABLED = config.DEBRID_HEALTH_ENABLED
DEBRID_HEALTH_AUTO_REMEDIATE = config.DEBRID_HEALTH_AUTO_REMEDIATE
DEBRID_HEALTH_CROSS_RESCUE = config.DEBRID_HEALTH_CROSS_RESCUE
ROUTING_AUTO_TAG_UNTAGGED = config.ROUTING_AUTO_TAG_UNTAGGED
GAP_FILL_ENABLED = config.GAP_FILL_ENABLED
WANTED_TB_RECOVERY_ENABLED = config.WANTED_TB_RECOVERY_ENABLED
WANTED_TB_RECOVERY_MAX_PER_SCAN = config.WANTED_TB_RECOVERY_MAX_PER_SCAN
WANTED_RD_RECOVERY_ENABLED = config.WANTED_RD_RECOVERY_ENABLED
WANTED_RD_RECOVERY_MAX_PER_SCAN = config.WANTED_RD_RECOVERY_MAX_PER_SCAN
WANTED_SEASON_RECOVERY_ENABLED = config.WANTED_SEASON_RECOVERY_ENABLED
TORRENTIO_URL = config.TORRENTIO_URL
SEARCH_REQUIRE_CACHED = config.SEARCH_REQUIRE_CACHED
SEARCH_DEDUP_ENABLED = config.SEARCH_DEDUP_ENABLED
BLACKHOLE_REQUIRE_CACHED = config.BLACKHOLE_REQUIRE_CACHED
BLACKHOLE_DEBRID_DEDUP_ENABLED = config.BLACKHOLE_DEBRID_DEDUP_ENABLED
BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT = config.BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT
BLACKHOLE_TB_ALT_RECOVERY_ENABLED = config.BLACKHOLE_TB_ALT_RECOVERY_ENABLED
BLACKHOLE_ARR_FAILED_FEEDBACK_ENABLED = config.BLACKHOLE_ARR_FAILED_FEEDBACK_ENABLED
PD_ENFORCE_CACHED_VERSIONS = config.PD_ENFORCE_CACHED_VERSIONS
QUALITY_COMPROMISE_ENABLED = config.QUALITY_COMPROMISE_ENABLED
QUALITY_COMPROMISE_DWELL_DAYS = config.QUALITY_COMPROMISE_DWELL_DAYS
QUALITY_COMPROMISE_MIN_SEEDERS = config.QUALITY_COMPROMISE_MIN_SEEDERS
QUALITY_COMPROMISE_ONLY_CACHED = config.QUALITY_COMPROMISE_ONLY_CACHED
QUALITY_COMPROMISE_MAX_TIER_DROP = config.QUALITY_COMPROMISE_MAX_TIER_DROP
QUALITY_COMPROMISE_NOTIFY = config.QUALITY_COMPROMISE_NOTIFY
SEASON_PACK_FALLBACK_ENABLED = config.SEASON_PACK_FALLBACK_ENABLED
SEASON_PACK_FALLBACK_MIN_MISSING = config.SEASON_PACK_FALLBACK_MIN_MISSING
SEASON_PACK_FALLBACK_MIN_RATIO = config.SEASON_PACK_FALLBACK_MIN_RATIO
