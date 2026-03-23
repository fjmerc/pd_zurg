"""Startup configuration validator.

Catches misconfiguration early with clear error messages instead of
letting services fail silently at runtime. Runs before any service
is launched.
"""

import os
import re
from urllib.parse import urlparse
from utils.logger import get_logger

logger = get_logger()


class ValidationResult:
    """Collects validation errors and warnings for batch reporting."""

    def __init__(self):
        self.errors = []
        self.warnings = []

    def error(self, message):
        self.errors.append(message)

    def warn(self, message):
        self.warnings.append(message)

    @property
    def ok(self):
        return len(self.errors) == 0


def _is_valid_url(url):
    """Check if a string is a valid http(s) URL."""
    try:
        parsed = urlparse(url)
        return parsed.scheme in ('http', 'https') and bool(parsed.netloc)
    except Exception:
        return False


def _is_truthy(value):
    """Check if a string value represents a truthy boolean."""
    return str(value).lower() in ('true', '1', 'yes')


def validate_config():
    """Run all validation checks against current config. Returns ValidationResult."""
    from base import config

    # Read from the Config singleton for values that come from secrets/env
    ZURG = config.ZURG
    RDAPIKEY = config.RDAPIKEY
    ADAPIKEY = config.ADAPIKEY
    PLEXTOKEN = config.PLEXTOKEN
    PLEXADD = config.PLEXADD
    JFADD = config.JFADD
    JFAPIKEY = config.JFAPIKEY
    PLEXDEBRID = config.PLEXDEBRID
    DUPECLEAN = config.DUPECLEAN
    PLEXREFRESH = config.PLEXREFRESH
    RCLONEMN = config.RCLONEMN

    result = ValidationResult()

    # --- Required API Keys ---
    if _is_truthy(ZURG):
        if not RDAPIKEY and not ADAPIKEY:
            result.error(
                "ZURG_ENABLED=true but neither RD_API_KEY nor AD_API_KEY is set. "
                "At least one debrid API key is required."
            )

    # --- URL Format Validation ---
    url_vars = {
        'PLEX_ADDRESS': PLEXADD,
        'JF_ADDRESS': JFADD,
        'SEERR_ADDRESS': os.environ.get('SEERR_ADDRESS', ''),
    }
    for name, value in url_vars.items():
        if value and not _is_valid_url(value):
            result.error(
                f"{name}='{value}' is not a valid URL. "
                f"Must start with http:// or https://"
            )

    # --- Enum Validation ---
    blackhole_debrid = os.environ.get('BLACKHOLE_DEBRID', '').lower()
    valid_debrid_services = ('realdebrid', 'alldebrid', 'torbox')
    if blackhole_debrid and blackhole_debrid not in valid_debrid_services:
        result.error(
            f"BLACKHOLE_DEBRID='{blackhole_debrid}' is not valid. "
            f"Must be one of: {', '.join(valid_debrid_services)}"
        )

    log_levels = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    for var in ('ZURG_LOG_LEVEL', 'RCLONE_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'PD_LOG_LEVEL'):
        val = os.environ.get(var, '').upper()
        if val and val not in log_levels:
            result.warn(
                f"{var}='{val}' is not a standard log level. "
                f"Expected one of: {', '.join(log_levels)}"
            )

    notification_level = os.environ.get('NOTIFICATION_LEVEL', '').lower()
    if notification_level and notification_level not in ('info', 'warning', 'error'):
        result.error(
            f"NOTIFICATION_LEVEL='{notification_level}' is not valid. "
            f"Must be one of: info, warning, error"
        )

    # --- Numeric Validation ---
    numeric_vars = {
        'BLACKHOLE_POLL_INTERVAL': (1, 3600),
        'STATUS_UI_PORT': (1, 65535),
        'ZURG_PORT': (1, 65535),
        'NFS_PORT': (1, 65535),
        'AUTO_UPDATE_INTERVAL': (1, 168),
        'CLEANUP_INTERVAL': (1, 168),
        'FFPROBE_STUCK_TIMEOUT': (10, 600),
        'FFPROBE_POLL_INTERVAL': (5, 300),
        'BLACKHOLE_MOUNT_POLL_TIMEOUT': (30, 3600),
        'BLACKHOLE_MOUNT_POLL_INTERVAL': (5, 120),
        'BLACKHOLE_SYMLINK_MAX_AGE': (0, 720),
    }
    for var, (lo, hi) in numeric_vars.items():
        val = os.environ.get(var, '')
        if val:
            try:
                n = int(val)
                if n < lo or n > hi:
                    result.warn(
                        f"{var}={n} is outside recommended range [{lo}-{hi}]"
                    )
            except ValueError:
                result.error(f"{var}='{val}' is not a valid integer")

    # --- Logical Consistency ---
    if _is_truthy(PLEXDEBRID) and not _is_truthy(ZURG):
        result.warn(
            "PD_ENABLED=true but ZURG_ENABLED is not true. "
            "plex_debrid typically requires Zurg to function."
        )

    if _is_truthy(DUPECLEAN) and not PLEXTOKEN:
        result.error(
            "DUPLICATE_CLEANUP=true but PLEX_TOKEN is not set. "
            "Duplicate cleanup requires Plex API access."
        )

    dupeclean_keep = (config.DUPECLEANKEEP or '').lower()
    if dupeclean_keep and dupeclean_keep not in ('local', 'zurg'):
        result.error(
            f"DUPLICATE_CLEANUP_KEEP={dupeclean_keep!r} is not valid. "
            "Must be 'local' (default) or 'zurg'."
        )

    if _is_truthy(PLEXREFRESH) and not PLEXTOKEN:
        result.error(
            "PLEX_REFRESH=true but PLEX_TOKEN is not set. "
            "Plex library refresh requires Plex API access."
        )

    blackhole_enabled = os.environ.get('BLACKHOLE_ENABLED', 'false').lower() == 'true'
    if blackhole_enabled and not RDAPIKEY and not ADAPIKEY:
        torbox_key = os.environ.get('TORBOX_API_KEY', '')
        if not torbox_key:
            result.error(
                "BLACKHOLE_ENABLED=true but no debrid API key found. "
                "Set RD_API_KEY, AD_API_KEY, or TORBOX_API_KEY."
            )

    symlink_enabled = os.environ.get('BLACKHOLE_SYMLINK_ENABLED', 'false').lower() == 'true'
    if symlink_enabled:
        if not blackhole_enabled:
            result.error(
                "BLACKHOLE_SYMLINK_ENABLED=true but BLACKHOLE_ENABLED is not true. "
                "Symlinks require the blackhole watcher to be enabled."
            )
        if not os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', ''):
            result.error(
                "BLACKHOLE_SYMLINK_ENABLED=true but BLACKHOLE_SYMLINK_TARGET_BASE is not set. "
                "This must be the mount path as seen on the Sonarr/Radarr host (e.g., /mnt/debrid)."
            )

    # --- Auth Format ---
    status_auth = os.environ.get('STATUS_UI_AUTH', '')
    if status_auth and ':' not in status_auth:
        result.error(
            f"STATUS_UI_AUTH format is invalid. "
            f"Must be in format 'username:password'"
        )

    # --- Notification URL Validation ---
    notification_url = os.environ.get('NOTIFICATION_URL', '')
    if notification_url:
        for url in notification_url.split(','):
            url = url.strip()
            if url and '://' not in url:
                result.warn(
                    f"NOTIFICATION_URL contains '{url[:30]}...' which doesn't "
                    f"look like a valid Apprise URL (missing ://)"
                )

    # --- rclone Mount Name ---
    if RCLONEMN and not re.match(r'^[a-zA-Z0-9_-]+$', RCLONEMN):
        result.warn(
            f"RCLONE_MOUNT_NAME='{RCLONEMN}' contains special characters. "
            f"This may cause issues with mount paths."
        )

    return result


def run_validation():
    """Run validation and handle results. Called from main.py.

    Returns True if startup should proceed, False if fatal errors found.
    """
    skip = os.environ.get('SKIP_VALIDATION', 'false').lower() == 'true'
    if skip:
        logger.info("Config validation skipped (SKIP_VALIDATION=true)")
        return True

    result = validate_config()

    for warning in result.warnings:
        logger.warning(f"[config] {warning}")

    for error in result.errors:
        logger.error(f"[config] {error}")

    if not result.ok:
        logger.error(
            f"[config] {len(result.errors)} configuration error(s) found. "
            f"Fix the above errors or set SKIP_VALIDATION=true to bypass."
        )
        return False

    if result.warnings:
        logger.info(
            f"[config] Validation passed with {len(result.warnings)} warning(s)"
        )
    else:
        logger.info("[config] Validation passed")

    return True
