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
    'JFADD', 'JFAPIKEY', 'RDAPIKEY', 'ADAPIKEY', 'GHTOKEN',
    'SEERRAPIKEY', 'SEERRADD', 'PLEXADD', 'ZURGUSER', 'ZURGPASS',
    'SHOWMENU', 'LOGFILE', 'PDUPDATE', 'PDREPO',
    'DUPECLEAN', 'CLEANUPINT', 'DUPECLEANKEEP', 'RCLONEMN', 'RCLONELOGLEVEL',
    'ZURG', 'ZURGVERSION', 'ZURGLOGLEVEL', 'ZURGUPDATE',
    'PLEXREFRESH', 'PLEXMOUNT', 'NFSMOUNT', 'NFSPORT', 'ZURGPORT',
    'TRAKTCLIENTID', 'TRAKTCLIENTSECRET',
    'NOTIFICATION_URL', 'NOTIFICATION_EVENTS', 'NOTIFICATION_LEVEL',
    'BLACKHOLE_ENABLED', 'BLACKHOLE_DIR', 'BLACKHOLE_POLL_INTERVAL', 'BLACKHOLE_DEBRID',
    'BLACKHOLE_SYMLINK_ENABLED', 'BLACKHOLE_COMPLETED_DIR', 'BLACKHOLE_RCLONE_MOUNT',
    'BLACKHOLE_SYMLINK_TARGET_BASE', 'BLACKHOLE_MOUNT_POLL_TIMEOUT',
    'BLACKHOLE_MOUNT_POLL_INTERVAL', 'BLACKHOLE_SYMLINK_MAX_AGE',
    'STATUS_UI_ENABLED', 'STATUS_UI_PORT', 'STATUS_UI_AUTH',
    'SONARR_URL', 'SONARR_API_KEY', 'RADARR_URL', 'RADARR_API_KEY',
    # Scheduled task intervals
    'ROUTING_AUDIT_INTERVAL', 'QUEUE_CLEANUP_INTERVAL',
    'LIBRARY_SCAN_INTERVAL', 'SYMLINK_VERIFY_INTERVAL',
    'PREFERENCE_ENFORCE_INTERVAL', 'HOUSEKEEPING_INTERVAL',
    'CONFIG_BACKUP_INTERVAL', 'MOUNT_LIVENESS_INTERVAL',
    # History
    'HISTORY_RETENTION_DAYS',
    # Notification digest
    'NOTIFICATION_DIGEST_ENABLED', 'NOTIFICATION_DIGEST_TIME',
    # Blocklist
    'BLOCKLIST_AUTO_ADD', 'BLOCKLIST_EXPIRY_DAYS',
    # Symlink repair
    'SYMLINK_REPAIR_AUTO_SEARCH',
    # Routing audit
    'ROUTING_AUTO_TAG_UNTAGGED',
    # Debrid search
    'TORRENTIO_URL',
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
        self.BLACKHOLE_SYMLINK_ENABLED = os.getenv('BLACKHOLE_SYMLINK_ENABLED')
        self.BLACKHOLE_COMPLETED_DIR = os.getenv('BLACKHOLE_COMPLETED_DIR')
        self.BLACKHOLE_RCLONE_MOUNT = os.getenv('BLACKHOLE_RCLONE_MOUNT')
        self.BLACKHOLE_SYMLINK_TARGET_BASE = os.getenv('BLACKHOLE_SYMLINK_TARGET_BASE')
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
        self.SYMLINK_VERIFY_INTERVAL = os.getenv('SYMLINK_VERIFY_INTERVAL')
        self.PREFERENCE_ENFORCE_INTERVAL = os.getenv('PREFERENCE_ENFORCE_INTERVAL')
        self.HOUSEKEEPING_INTERVAL = os.getenv('HOUSEKEEPING_INTERVAL')
        self.CONFIG_BACKUP_INTERVAL = os.getenv('CONFIG_BACKUP_INTERVAL')
        self.MOUNT_LIVENESS_INTERVAL = os.getenv('MOUNT_LIVENESS_INTERVAL')
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
        # Routing audit (auto-tag untagged monitored series/movies with debrid tag)
        self.ROUTING_AUTO_TAG_UNTAGGED = os.getenv('ROUTING_AUTO_TAG_UNTAGGED', 'true')
        # Debrid search
        self.TORRENTIO_URL = os.getenv('TORRENTIO_URL')


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
BLACKHOLE_SYMLINK_ENABLED = config.BLACKHOLE_SYMLINK_ENABLED
BLACKHOLE_COMPLETED_DIR = config.BLACKHOLE_COMPLETED_DIR
BLACKHOLE_RCLONE_MOUNT = config.BLACKHOLE_RCLONE_MOUNT
BLACKHOLE_SYMLINK_TARGET_BASE = config.BLACKHOLE_SYMLINK_TARGET_BASE
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
SYMLINK_VERIFY_INTERVAL = config.SYMLINK_VERIFY_INTERVAL
PREFERENCE_ENFORCE_INTERVAL = config.PREFERENCE_ENFORCE_INTERVAL
HOUSEKEEPING_INTERVAL = config.HOUSEKEEPING_INTERVAL
CONFIG_BACKUP_INTERVAL = config.CONFIG_BACKUP_INTERVAL
MOUNT_LIVENESS_INTERVAL = config.MOUNT_LIVENESS_INTERVAL
HISTORY_RETENTION_DAYS = config.HISTORY_RETENTION_DAYS
NOTIFICATION_DIGEST_ENABLED = config.NOTIFICATION_DIGEST_ENABLED
NOTIFICATION_DIGEST_TIME = config.NOTIFICATION_DIGEST_TIME
BLOCKLIST_AUTO_ADD = config.BLOCKLIST_AUTO_ADD
BLOCKLIST_EXPIRY_DAYS = config.BLOCKLIST_EXPIRY_DAYS
SYMLINK_REPAIR_AUTO_SEARCH = config.SYMLINK_REPAIR_AUTO_SEARCH
ROUTING_AUTO_TAG_UNTAGGED = config.ROUTING_AUTO_TAG_UNTAGGED
TORRENTIO_URL = config.TORRENTIO_URL
