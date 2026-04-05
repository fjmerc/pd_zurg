"""Settings API for the web-based settings editor.

Provides schema definitions, read/write, and validation for both
pd_zurg environment variables and plex_debrid settings.json.
Used by the status server to power the /settings UI and
/api/settings/* endpoints.
"""

import json as _json
import os
import re
import signal
import threading
from dotenv import dotenv_values
from urllib.parse import urlparse
from utils.file_utils import atomic_write
from utils.logger import get_logger

logger = get_logger()

ENV_FILE = '/config/.env'

# ---------------------------------------------------------------------------
# Schema definition — one tuple per field:
#   (key, label, type, required, help_text)
#
# Types: boolean, string, secret, url, number:MIN-MAX, select:OPT1,OPT2,...
# ---------------------------------------------------------------------------

ENV_SCHEMA = [
    {
        'name': 'Zurg',
        'description': 'Core debrid service and WebDAV server',
        'fields': [
            ('ZURG_ENABLED', 'Enable Zurg', 'boolean', True, 'Enable the Zurg WebDAV server'),
            ('RD_API_KEY', 'Real-Debrid API Key', 'secret', False, 'API key from real-debrid.com/apitoken'),
            ('AD_API_KEY', 'AllDebrid API Key', 'secret', False, 'API key from alldebrid.com'),
            ('TORBOX_API_KEY', 'TorBox API Key', 'secret', False, 'API key from torbox.app'),
            ('ZURG_VERSION', 'Zurg Version', 'string', False, 'Pin to specific version (e.g., v0.9.2-hotfix.4)'),
            ('ZURG_UPDATE', 'Auto-Update Zurg', 'boolean', False, 'Check for Zurg updates on startup'),
            ('ZURG_LOG_LEVEL', 'Zurg Log Level', 'select:DEBUG,INFO,WARNING,ERROR', False, 'Log level for Zurg process'),
            ('ZURG_PORT', 'Zurg Port', 'number:1-65535', False, 'WebDAV server port (auto-assigned if empty)'),
            ('ZURG_USER', 'Zurg Username', 'string', False, 'Basic auth username for WebDAV'),
            ('ZURG_PASS', 'Zurg Password', 'secret', False, 'Basic auth password for WebDAV'),
        ],
    },
    {
        'name': 'rclone',
        'description': 'Mount configuration and VFS tuning',
        'fields': [
            ('RCLONE_MOUNT_NAME', 'Mount Name', 'string', True, 'Name for the rclone mount point under /data'),
            ('RCLONE_LOG_LEVEL', 'Log Level', 'select:DEBUG,INFO,NOTICE,ERROR', False, 'rclone log verbosity'),
            ('NFS_ENABLED', 'Enable NFS', 'boolean', False, 'Use NFS server instead of FUSE mount'),
            ('NFS_PORT', 'NFS Port', 'number:1-65535', False, 'NFS server port'),
            ('RCLONE_CACHE_DIR', 'Cache Directory', 'string', False, 'Directory for VFS cache files'),
            ('RCLONE_DIR_CACHE_TIME', 'Dir Cache Time', 'string', False, 'How long to cache directory listings (e.g., 10s, 5m)'),
            ('RCLONE_VFS_READ_CHUNK_SIZE', 'VFS Read Chunk Size', 'string', False, 'Initial chunk size for streaming reads (e.g., 8M)'),
            ('RCLONE_VFS_READ_CHUNK_SIZE_LIMIT', 'VFS Read Chunk Size Limit', 'string', False, 'Max chunk size (e.g., 64M, off to disable)'),
            ('RCLONE_VFS_CACHE_MODE', 'VFS Cache Mode', 'select:off,minimal,writes,full', False, 'VFS file caching mode (default: off for FUSE, full for NFS)'),
            ('RCLONE_VFS_CACHE_MAX_SIZE', 'VFS Cache Max Size', 'string', False, 'Max total size of VFS cache (e.g., 10G)'),
            ('RCLONE_VFS_CACHE_MAX_AGE', 'VFS Cache Max Age', 'string', False, 'Max age of VFS cache files (e.g., 1h, 24h)'),
            ('RCLONE_BUFFER_SIZE', 'Buffer Size', 'string', False, 'In-memory buffer per open file (e.g., 16M)'),
            ('RCLONE_TRANSFERS', 'Transfers', 'string', False, 'Number of parallel transfers'),
        ],
    },
    {
        'name': 'plex_debrid',
        'description': 'Plex/Debrid integration service',
        'fields': [
            ('PD_ENABLED', 'Enable plex_debrid', 'boolean', False, 'Run the plex_debrid service'),
            ('SHOW_MENU', 'Show Menu', 'boolean', False, 'Show plex_debrid interactive menu on startup'),
            ('PLEX_USER', 'Plex Username', 'string', False, 'Plex account username'),
            ('PLEX_TOKEN', 'Plex Token', 'secret', False, 'Plex authentication token'),
            ('PLEX_ADDRESS', 'Plex Address', 'url', False, 'Plex server URL (e.g., http://192.168.1.100:32400)'),
            ('SEERR_ADDRESS', 'Overseerr/Jellyseerr Address', 'url', False, 'Request management server URL'),
            ('SEERR_API_KEY', 'Overseerr/Jellyseerr API Key', 'secret', False, 'API key for Overseerr/Jellyseerr'),
            ('PD_LOG_LEVEL', 'Log Level', 'select:DEBUG,INFO,WARNING,ERROR', False, 'plex_debrid log level'),
            ('PD_UPDATE', 'Auto-Update plex_debrid', 'boolean', False, 'Check for updates on startup'),
            ('PD_REPO', 'plex_debrid Repository', 'string', False, 'GitHub repo (owner/repo format)'),
            ('TRAKT_CLIENT_ID', 'Trakt Client ID', 'string', False, 'Trakt API application client ID'),
            ('TRAKT_CLIENT_SECRET', 'Trakt Client Secret', 'secret', False, 'Trakt API application client secret'),
            ('FLARESOLVERR_URL', 'FlareSolverr URL', 'url', False, 'FlareSolverr proxy URL for Cloudflare bypass'),
        ],
    },
    {
        'name': 'Jellyfin',
        'description': 'Jellyfin media server integration',
        'fields': [
            ('JF_ADDRESS', 'Jellyfin Address', 'url', False, 'Jellyfin server URL (e.g., http://192.168.1.100:8096)'),
            ('JF_API_KEY', 'Jellyfin API Key', 'secret', False, 'Jellyfin API key for library access'),
        ],
    },
    {
        'name': 'Plex Library',
        'description': 'Plex library maintenance features',
        'fields': [
            ('PLEX_REFRESH', 'Auto Refresh Library', 'boolean', False, 'Automatically refresh Plex libraries after mount changes'),
            ('PLEX_MOUNT_DIR', 'Plex Mount Directory', 'string', False, 'Path where Plex sees the rclone mount'),
            ('DUPLICATE_CLEANUP', 'Duplicate Cleanup', 'boolean', False, 'Automatically remove duplicate media entries'),
            ('CLEANUP_INTERVAL', 'Cleanup Interval (hours)', 'number:1-168', False, 'How often to run duplicate cleanup'),
            ('DUPLICATE_CLEANUP_KEEP', 'Keep Copy From', 'select:local,zurg', False, 'Which copy to keep: "local" (default, logs Zurg dupes) or "zurg" (deletes local copies)'),
        ],
    },
    {
        'name': 'Notifications',
        'description': 'Apprise notification service',
        'fields': [
            ('NOTIFICATION_URL', 'Notification URL(s)', 'string', False, 'Apprise notification URL(s), comma-separated'),
            ('NOTIFICATION_EVENTS', 'Notification Events', 'string', False,
             'Comma-separated event types: startup, shutdown, download_complete, download_error, '
             'library_refresh, symlink_created, symlink_failed, debrid_unavailable, pending_warning, '
             'local_fallback_triggered, blocklist_added, arr_deleted, health_error, symlink_repaired, '
             'daily_digest, debrid_add_success, debrid_add_failed. '
             'Leave empty for all events'),
            ('NOTIFICATION_LEVEL', 'Minimum Level', 'select:info,warning,error', False, 'Minimum severity to send notifications'),
            ('NOTIFICATION_DIGEST_ENABLED', 'Daily Digest', 'boolean', False, 'Send a daily summary notification'),
            ('NOTIFICATION_DIGEST_TIME', 'Digest Time (HH:MM)', 'string', False, 'When to send the daily digest (24h format, default: 08:00)'),
        ],
    },
    {
        'name': 'Blackhole',
        'description': 'Torrent blackhole watcher for *arr integration',
        'fields': [
            ('BLACKHOLE_ENABLED', 'Enable Blackhole', 'boolean', False, 'Watch a directory for .torrent/.magnet files'),
            ('BLACKHOLE_DIR', 'Watch Directory', 'string', False, 'Directory to watch for torrent files'),
            ('BLACKHOLE_POLL_INTERVAL', 'Poll Interval (seconds)', 'number:1-3600', False, 'How often to check for new files'),
            ('BLACKHOLE_DEBRID', 'Debrid Service', 'select:realdebrid,alldebrid,torbox', False, 'Which debrid service to use'),
            ('BLACKHOLE_SYMLINK_ENABLED', 'Enable Symlinks', 'boolean', False, 'Create symlinks in completed dir after debrid download finishes'),
            ('BLACKHOLE_COMPLETED_DIR', 'Completed Directory', 'string', False, 'Directory for completed symlinks (container path, default: /completed)'),
            ('BLACKHOLE_RCLONE_MOUNT', 'rclone Mount Path', 'string', False, 'rclone mount path inside container (default: /data)'),
            ('BLACKHOLE_SYMLINK_TARGET_BASE', 'Symlink Target Base', 'string', False, 'Mount path as seen on Sonarr/Radarr host (e.g., /mnt/debrid)'),
            ('BLACKHOLE_MOUNT_POLL_TIMEOUT', 'Mount Poll Timeout (seconds)', 'number:30-3600', False, 'Max time to wait for content on mount (default: 300)'),
            ('BLACKHOLE_MOUNT_POLL_INTERVAL', 'Mount Poll Interval (seconds)', 'number:5-120', False, 'How often to check for content on mount (default: 10)'),
            ('BLACKHOLE_SYMLINK_MAX_AGE', 'Symlink Max Age (hours)', 'number:0-720', False, 'Remove symlink dirs older than this (0=disabled, default: 72)'),
            ('SYMLINK_REPAIR_AUTO_SEARCH', 'Repair Auto-Search', 'boolean', False, 'When broken symlinks can\'t be repaired from mount, trigger arr re-search'),
            ('BLOCKLIST_AUTO_ADD', 'Auto-Blocklist Failed Torrents', 'boolean', False, 'Automatically blocklist torrents that hit terminal debrid errors (default: true)'),
            ('BLACKHOLE_DEDUP_ENABLED', 'Enable Local Library Dedup', 'boolean', False, 'Skip torrents that match content already in your local library'),
            ('BLACKHOLE_LOCAL_LIBRARY_TV', 'Local TV Library Path', 'string', False, 'Path to local TV library (for dedup and auto debrid symlinks)'),
            ('BLACKHOLE_LOCAL_LIBRARY_MOVIES', 'Local Movie Library Path', 'string', False, 'Path to local movie library (for dedup and auto debrid symlinks)'),
        ],
    },
    {
        'name': 'Status UI',
        'description': 'Web dashboard and API settings',
        'fields': [
            ('STATUS_UI_ENABLED', 'Enable Status UI', 'boolean', False, 'Enable the web status dashboard'),
            ('STATUS_UI_PORT', 'Port', 'number:1-65535', False, 'Port for the status web server'),
            ('STATUS_UI_AUTH', 'Authentication', 'string', False,
             'Basic auth credentials (username:password). '
             'If you forget this password, edit /config/.env on the host volume to recover'),
        ],
    },
    {
        'name': 'Library Metadata',
        'description': 'TMDB integration for episode titles, posters, and missing episode detection',
        'fields': [
            ('TMDB_API_KEY', 'TMDB API Key', 'secret', False, 'API key from themoviedb.org (free, enables metadata in Library page)'),
        ],
    },
    {
        'name': 'Media Services',
        'description': 'Sonarr/Radarr/Overseerr integration for downloads, rescans, and library symlinks',
        'fields': [
            ('SONARR_URL', 'Sonarr URL', 'url', False, 'Sonarr base URL (e.g. http://sonarr:8989). Used for downloads, rescans, and folder naming'),
            ('SONARR_API_KEY', 'Sonarr API Key', 'secret', False, 'Sonarr API key (Settings > General in Sonarr)'),
            ('RADARR_URL', 'Radarr URL', 'url', False, 'Radarr base URL (e.g. http://radarr:7878). Used for downloads, rescans, and folder naming'),
            ('RADARR_API_KEY', 'Radarr API Key', 'secret', False, 'Radarr API key (Settings > General in Radarr)'),
            ('LIBRARY_PREFERENCE_AUTO_ENFORCE', 'Auto-Enforce Preferences', 'boolean', False, 'Automatically switch sources when content arrives matching a stored preference'),
            ('DEBRID_UNAVAILABLE_THRESHOLD_DAYS', 'Debrid Unavailable After (days)', 'number:1-30', False, 'Days of failed searches before marking content as debrid-unavailable (default: 3)'),
            ('PENDING_WARNING_HOURS', 'Pending Warning After (hours)', 'number:0-168', False, 'Hours before sending a warning notification for stuck pending items (default: 24, 0 to disable)'),
        ],
    },
    {
        'name': 'Debrid Search',
        'description': 'Interactive torrent search and one-click add to debrid',
        'fields': [
            ('TORRENTIO_URL', 'Torrentio URL', 'url', False,
             'Torrentio API base URL (e.g. https://torrentio.strem.fun). Enables interactive torrent search in the Library detail view'),
        ],
    },
    {
        'name': 'Monitoring',
        'description': 'ffprobe monitoring and auto-update',
        'fields': [
            ('FFPROBE_MONITOR_ENABLED', 'Enable ffprobe Monitor', 'boolean', False, 'Monitor for stuck ffprobe processes'),
            ('FFPROBE_STUCK_TIMEOUT', 'Stuck Timeout (seconds)', 'number:10-600', False, 'Seconds before an ffprobe process is considered stuck'),
            ('FFPROBE_POLL_INTERVAL', 'Poll Interval (seconds)', 'number:5-300', False, 'How often to check for stuck processes'),
            ('AUTO_UPDATE_INTERVAL', 'Auto-Update Interval (hours)', 'number:1-168', False, 'How often to check for Zurg/plex_debrid updates'),
        ],
    },
    {
        'name': 'Logging',
        'description': 'Application logging configuration',
        'fields': [
            ('PDZURG_LOG_LEVEL', 'pd_zurg Log Level', 'select:DEBUG,INFO,WARNING,ERROR,CRITICAL', False, 'Main application log level'),
            ('PDZURG_LOG_COUNT', 'Log File Count', 'string', False, 'Number of rotated log files to keep'),
            ('PDZURG_LOG_SIZE', 'Max Log Size', 'string', False, 'Max size per log file (e.g., 10M)'),
            ('COLOR_LOG_ENABLED', 'Color Logs', 'boolean', False, 'Enable colored console log output'),
            ('PD_LOGFILE', 'plex_debrid Log File', 'string', False, 'Path for plex_debrid log output'),
        ],
    },
    {
        'name': 'General',
        'description': 'General container settings',
        'fields': [
            ('TZ', 'Timezone', 'string', False, 'Container timezone (e.g., America/New_York, Europe/London)'),
            ('HISTORY_RETENTION_DAYS', 'History Retention (days)', 'number:1-365', False, 'Number of days to keep activity history events (default: 30)'),
        ],
    },
    {
        'name': 'Advanced',
        'description': 'Rarely changed options',
        'fields': [
            ('GITHUB_TOKEN', 'GitHub Token', 'secret', False, 'GitHub personal access token (avoids rate limits)'),
            ('SKIP_VALIDATION', 'Skip Validation', 'boolean', False, 'Skip startup config validation checks'),
        ],
    },
]

# All known env var keys from the schema
_ALL_KEYS = {field[0] for cat in ENV_SCHEMA for field in cat['fields']}

# Sensitive key patterns — values should be masked in certain contexts
_SENSITIVE_PATTERNS = {'KEY', 'TOKEN', 'PASS', 'SECRET', 'AUTH'}


def _is_sensitive(key):
    return any(p in key.upper() for p in _SENSITIVE_PATTERNS)


# ---------------------------------------------------------------------------
# Schema API
# ---------------------------------------------------------------------------

def get_env_schema():
    """Return the env var schema as a JSON-serializable structure."""
    categories = []
    for cat in ENV_SCHEMA:
        fields = []
        for key, label, ftype, required, help_text in cat['fields']:
            field = {
                'key': key,
                'label': label,
                'type': ftype,
                'required': required,
                'help': help_text,
                'sensitive': _is_sensitive(key),
            }
            fields.append(field)
        categories.append({
            'name': cat['name'],
            'description': cat['description'],
            'fields': fields,
        })
    return {'categories': categories}


# ---------------------------------------------------------------------------
# Read / Write
# ---------------------------------------------------------------------------

def read_env_values():
    """Read current .env file and return key-value dict.

    Reads from the .env file first, then falls back to os.environ for
    values set via docker-compose or other mechanisms. This ensures the
    form shows what's actually active, not just what's in the file.
    """
    file_values = {}
    if os.path.exists(ENV_FILE):
        file_values = dotenv_values(ENV_FILE)

    result = {}
    for key in sorted(_ALL_KEYS):
        if key in file_values:
            result[key] = file_values[key] or ''
        else:
            result[key] = os.environ.get(key, '')
    return result


def _sanitize_value(value):
    """Sanitize a single env var value for safe .env file writing."""
    if value is None:
        return ''
    value = str(value).strip()
    # Remove null bytes and carriage returns
    value = value.replace('\x00', '').replace('\r', '')
    # Reject newlines — they'd break .env format
    if '\n' in value:
        raise ValueError('Value must not contain newlines')
    return value


def _needs_quoting(value):
    """Check if a value needs to be quoted in the .env file."""
    if not value:
        return False
    # Quote if contains spaces, #, $, ', ", \, or backtick ($ triggers interpolation)
    if re.search(r'[\s#\'"\\$`]', value):
        return True
    return False


def _format_env_line(key, value):
    """Format a single KEY=VALUE line for the .env file."""
    if not value:
        return f'{key}='
    if _needs_quoting(value):
        # Use double quotes, escape existing double quotes and backslashes
        escaped = value.replace('\\', '\\\\').replace('"', '\\"')
        return f'{key}="{escaped}"'
    return f'{key}={value}'


def write_env_values(values):
    """Validate and write env var values to .env, then trigger reload.

    Args:
        values: dict of key-value pairs to write

    Returns:
        dict with 'status', 'errors', 'warnings', 'restarted' keys
    """
    # Filter to only known keys and sanitize
    filtered = {}
    sanitize_errors = []
    for key, value in values.items():
        if key in _ALL_KEYS:
            try:
                filtered[key] = _sanitize_value(value)
            except ValueError as e:
                sanitize_errors.append(f'{key}: {e}')
    if sanitize_errors:
        return {
            'status': 'error',
            'errors': sanitize_errors,
            'warnings': [],
        }

    # Merge with existing values (preserve keys not in the form submission)
    # Lock to prevent races with _sync_plex_debrid_to_env
    with _env_write_lock:
        existing = read_env_values()
        merged = {**existing, **filtered}

        # Validate before writing
        validation = validate_env_values(merged)
        if validation['errors']:
            return {
                'status': 'error',
                'errors': validation['errors'],
                'warnings': validation['warnings'],
            }

        # Write .env file atomically
        try:
            with atomic_write(ENV_FILE) as f:
                f.write('# pd_zurg configuration — managed by settings editor\n')
                f.write('# Manual edits are preserved on next save\n\n')
                for cat in ENV_SCHEMA:
                    cat_has_values = False
                    lines = []
                    for key, label, ftype, required, help_text in cat['fields']:
                        val = merged.get(key, '')
                        if val:
                            cat_has_values = True
                        lines.append(_format_env_line(key, val))
                    if cat_has_values:
                        f.write(f'# --- {cat["name"]} ---\n')
                        for line in lines:
                            f.write(line + '\n')
                        f.write('\n')
        except Exception as e:
            logger.error(f'[settings] Failed to write .env: {e}')
            return {
                'status': 'error',
                'errors': [f'Failed to write config file: {e}'],
                'warnings': [],
            }

    # Sync relevant .env changes into settings.json so plex_debrid picks
    # them up immediately on restart (not just on container restart)
    try:
        _sync_env_to_plex_debrid(merged)
    except Exception as e:
        logger.warning(f'[settings] settings.json sync failed (.env still saved): {e}')

    # Trigger SIGHUP for config reload
    # Note: 'restarted' is a best-effort preview based on pre-reload os.environ.
    # The actual SIGHUP handler independently re-computes which services restart.
    restarted = []
    try:
        from utils.config_reload import _determine_restarts
        changed = set()
        for key, new_val in merged.items():
            old_val = os.environ.get(key, '')
            if old_val != (new_val or ''):
                changed.add(key)
        if changed:
            restarted = sorted(_determine_restarts(changed))

        os.kill(os.getpid(), signal.SIGHUP)
        logger.info(f'[settings] Saved .env and triggered reload ({len(changed)} changed vars)')
    except Exception as e:
        logger.error(f'[settings] Saved .env but reload failed: {e}')
        return {
            'status': 'saved_no_reload',
            'errors': [],
            'warnings': [f'Config saved but reload failed: {e}. Restart container to apply.'],
            'restarted': [],
        }

    return {
        'status': 'saved',
        'errors': [],
        'warnings': validation['warnings'],
        'restarted': restarted,
    }


# ---------------------------------------------------------------------------
# Validation (standalone, does not touch os.environ)
# ---------------------------------------------------------------------------

def _is_valid_url(url):
    try:
        parsed = urlparse(url)
        return parsed.scheme in ('http', 'https') and bool(parsed.netloc)
    except Exception:
        return False


def validate_env_values(values):
    """Validate a dict of proposed env var values. Returns {errors:[], warnings:[]}."""
    errors = []
    warnings = []

    def _truthy(key):
        return str(values.get(key, '')).lower() in ('true', '1', 'yes')

    # Required API keys when Zurg enabled
    if _truthy('ZURG_ENABLED'):
        if not values.get('RD_API_KEY') and not values.get('AD_API_KEY'):
            errors.append(
                'ZURG_ENABLED=true but neither RD_API_KEY nor AD_API_KEY is set. '
                'At least one debrid API key is required.'
            )

    # URL format validation
    url_fields = ['PLEX_ADDRESS', 'JF_ADDRESS', 'SEERR_ADDRESS', 'FLARESOLVERR_URL',
                  'SONARR_URL', 'RADARR_URL', 'TORRENTIO_URL']
    for key in url_fields:
        val = values.get(key, '')
        if val and not _is_valid_url(val):
            errors.append(f"{key}='{val}' is not a valid URL. Must start with http:// or https://")

    # Enum validation
    blackhole_debrid = values.get('BLACKHOLE_DEBRID', '').lower()
    valid_debrid = ('realdebrid', 'alldebrid', 'torbox')
    if blackhole_debrid and blackhole_debrid not in valid_debrid:
        errors.append(
            f"BLACKHOLE_DEBRID='{blackhole_debrid}' is not valid. "
            f"Must be one of: {', '.join(valid_debrid)}"
        )

    log_levels = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    for var in ('ZURG_LOG_LEVEL', 'RCLONE_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'PD_LOG_LEVEL'):
        val = values.get(var, '').upper()
        allowed = log_levels + (('NOTICE',) if var == 'RCLONE_LOG_LEVEL' else ())
        if val and val not in allowed:
            warnings.append(f"{var}='{val}' is not a standard log level.")

    notification_level = values.get('NOTIFICATION_LEVEL', '').lower()
    if notification_level and notification_level not in ('info', 'warning', 'error'):
        errors.append(
            f"NOTIFICATION_LEVEL='{notification_level}' is not valid. "
            f"Must be one of: info, warning, error"
        )

    # Numeric validation
    numeric_ranges = {
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
    for var, (lo, hi) in numeric_ranges.items():
        val = values.get(var, '')
        if val:
            try:
                n = int(val)
                if n < lo or n > hi:
                    warnings.append(f"{var}={n} is outside recommended range [{lo}-{hi}]")
            except ValueError:
                errors.append(f"{var}='{val}' is not a valid integer")

    # Logical consistency
    if _truthy('PD_ENABLED') and not _truthy('ZURG_ENABLED'):
        warnings.append(
            'PD_ENABLED=true but ZURG_ENABLED is not true. '
            'plex_debrid typically requires Zurg to function.'
        )

    if _truthy('DUPLICATE_CLEANUP') and not values.get('PLEX_TOKEN'):
        errors.append(
            'DUPLICATE_CLEANUP=true but PLEX_TOKEN is not set. '
            'Duplicate cleanup requires Plex API access.'
        )

    keep_val = values.get('DUPLICATE_CLEANUP_KEEP', '').lower()
    if keep_val and keep_val not in ('local', 'zurg'):
        errors.append(
            f'DUPLICATE_CLEANUP_KEEP={keep_val!r} is not valid. '
            "Must be 'local' (default) or 'zurg'."
        )

    if _truthy('PLEX_REFRESH') and not values.get('PLEX_TOKEN'):
        errors.append(
            'PLEX_REFRESH=true but PLEX_TOKEN is not set. '
            'Plex library refresh requires Plex API access.'
        )

    blackhole_enabled = _truthy('BLACKHOLE_ENABLED')
    if blackhole_enabled:
        if not values.get('RD_API_KEY') and not values.get('AD_API_KEY') and not values.get('TORBOX_API_KEY'):
            errors.append(
                'BLACKHOLE_ENABLED=true but no debrid API key found. '
                'Set RD_API_KEY, AD_API_KEY, or TORBOX_API_KEY.'
            )

    if _truthy('BLACKHOLE_SYMLINK_ENABLED'):
        if not blackhole_enabled:
            errors.append(
                'BLACKHOLE_SYMLINK_ENABLED=true but BLACKHOLE_ENABLED is not true. '
                'Symlinks require the blackhole watcher to be enabled.'
            )
        if not values.get('BLACKHOLE_SYMLINK_TARGET_BASE'):
            errors.append(
                'BLACKHOLE_SYMLINK_ENABLED=true but BLACKHOLE_SYMLINK_TARGET_BASE is not set. '
                'This must be the mount path as seen on the Sonarr/Radarr host (e.g., /mnt/debrid).'
            )

    # Auth format
    status_auth = values.get('STATUS_UI_AUTH', '')
    if status_auth and ':' not in status_auth:
        errors.append("STATUS_UI_AUTH format is invalid. Must be 'username:password'")

    # Notification URL
    notification_url = values.get('NOTIFICATION_URL', '')
    if notification_url:
        for url in notification_url.split(','):
            url = url.strip()
            if url and '://' not in url:
                truncated = url[:30] + ('...' if len(url) > 30 else '')
                warnings.append(
                    f"NOTIFICATION_URL contains '{truncated}' which doesn't "
                    f"look like a valid Apprise URL (missing ://)"
                )

    # Mount name
    rclonemn = values.get('RCLONE_MOUNT_NAME', '')
    if rclonemn and not re.match(r'^[a-zA-Z0-9_-]+$', rclonemn):
        warnings.append(
            f"RCLONE_MOUNT_NAME='{rclonemn}' contains special characters. "
            f"This may cause issues with mount paths."
        )

    return {'errors': errors, 'warnings': warnings}


# ===========================================================================
# plex_debrid settings.json
# ===========================================================================

SETTINGS_JSON_FILE = '/config/settings.json'
SETTINGS_DEFAULT_FILE = '/app/plex_debrid_/settings-default.json'

# ---------------------------------------------------------------------------
# Quality profile presets and rule metadata
# ---------------------------------------------------------------------------

# Common exclusion regexes
_EXCLUDE_CAM = r'([^A-Z0-9]|HD|HQ)(CAM|T(ELE)?(S(YNC)?|C(INE)?)|ADS|HINDI)([^A-Z0-9]|RIP|$)'
_EXCLUDE_3D = r'(3D)'
_EXCLUDE_DV = r'(DO?VI?)'
_EXCLUDE_HDR = r'(HDR)'
_PREFER_EDITIONS = (
    r'(EXTENDED|REMASTERED|DIRECTORS|THEATRICAL|UNRATED|UNCUT|CRITERION|'
    r'ANNIVERSARY|COLLECTORS|LIMITED|SPECIAL|DELUXE|SUPERBIT|RESTORED|REPACK)'
)
_DEFAULT_CONDITIONS = [['retries', '<=', '48'], ['media type', 'all', '']]

VERSION_PRESETS = {
    '1080p_sdr': {
        'name': '1080p SDR',
        'description': 'Up to 1080p, no HDR/DV. Good default for most setups.',
        'profile': [
            '1080p SDR',
            _DEFAULT_CONDITIONS,
            'en',
            [
                ['cache status', 'requirement', 'cached', ''],
                ['resolution', 'requirement', '<=', '1080'],
                ['resolution', 'preference', 'highest', ''],
                ['title', 'requirement', 'exclude', _EXCLUDE_CAM],
                ['title', 'requirement', 'exclude', _EXCLUDE_3D],
                ['title', 'requirement', 'exclude', _EXCLUDE_DV],
                ['title', 'requirement', 'exclude', _EXCLUDE_HDR],
                ['title', 'preference', 'include', _PREFER_EDITIONS],
                ['size', 'preference', 'highest', ''],
                ['seeders', 'preference', 'highest', ''],
                ['size', 'requirement', '>=', '0.1'],
            ],
        ],
    },
    '4k_hdr': {
        'name': '4K HDR',
        'description': 'Up to 4K, prefer HDR/Dolby Vision. For premium setups.',
        'profile': [
            '4K HDR',
            _DEFAULT_CONDITIONS,
            'en',
            [
                ['cache status', 'requirement', 'cached', ''],
                ['resolution', 'requirement', '<=', '2160'],
                ['resolution', 'preference', 'highest', ''],
                ['title', 'requirement', 'exclude', _EXCLUDE_CAM],
                ['title', 'requirement', 'exclude', _EXCLUDE_3D],
                ['title', 'preference', 'include', r'(HDR|HDR10|HDR10.|DOLBY.?VISION|DO?VI?)'],
                ['title', 'preference', 'include', _PREFER_EDITIONS],
                ['size', 'preference', 'highest', ''],
                ['seeders', 'preference', 'highest', ''],
                ['size', 'requirement', '>=', '0.1'],
            ],
        ],
    },
    '4k_sdr': {
        'name': '4K SDR',
        'description': 'Up to 4K, no HDR/DV. High resolution without HDR.',
        'profile': [
            '4K SDR',
            _DEFAULT_CONDITIONS,
            'en',
            [
                ['cache status', 'requirement', 'cached', ''],
                ['resolution', 'requirement', '<=', '2160'],
                ['resolution', 'preference', 'highest', ''],
                ['title', 'requirement', 'exclude', _EXCLUDE_CAM],
                ['title', 'requirement', 'exclude', _EXCLUDE_3D],
                ['title', 'requirement', 'exclude', _EXCLUDE_DV],
                ['title', 'requirement', 'exclude', _EXCLUDE_HDR],
                ['title', 'preference', 'include', _PREFER_EDITIONS],
                ['size', 'preference', 'highest', ''],
                ['seeders', 'preference', 'highest', ''],
                ['size', 'requirement', '>=', '0.1'],
            ],
        ],
    },
    '720p': {
        'name': '720p',
        'description': 'Up to 720p. Lower bandwidth and storage usage.',
        'profile': [
            '720p',
            _DEFAULT_CONDITIONS,
            'en',
            [
                ['cache status', 'requirement', 'cached', ''],
                ['resolution', 'requirement', '<=', '720'],
                ['resolution', 'preference', 'highest', ''],
                ['title', 'requirement', 'exclude', _EXCLUDE_CAM],
                ['title', 'preference', 'include', _PREFER_EDITIONS],
                ['size', 'preference', 'highest', ''],
                ['seeders', 'preference', 'highest', ''],
                ['size', 'requirement', '>=', '0.1'],
            ],
        ],
    },
    'any_quality': {
        'name': 'Any Quality',
        'description': 'No resolution filter. Grabs the best available cached release.',
        'profile': [
            'Any Quality',
            _DEFAULT_CONDITIONS,
            'en',
            [
                ['cache status', 'requirement', 'cached', ''],
                ['resolution', 'preference', 'highest', ''],
                ['title', 'requirement', 'exclude', _EXCLUDE_CAM],
                ['title', 'preference', 'include', _PREFER_EDITIONS],
                ['size', 'preference', 'highest', ''],
                ['seeders', 'preference', 'highest', ''],
                ['size', 'requirement', '>=', '0.1'],
            ],
        ],
    },
    'anime': {
        'name': 'Anime',
        'description': 'Up to 1080p, optimized for anime releases.',
        'profile': [
            'Anime',
            [['retries', '<=', '48'], ['media type', 'shows', '']],
            'en',
            [
                ['cache status', 'requirement', 'cached', ''],
                ['resolution', 'requirement', '<=', '1080'],
                ['resolution', 'preference', 'highest', ''],
                ['title', 'requirement', 'exclude', _EXCLUDE_CAM],
                ['source', 'preference', 'include', r'(nyaa|subsplease|erai|judas|ember)'],
                ['title', 'preference', 'include', r'(10.?bit|x265|HEVC|BDRip|BluRay)'],
                ['seeders', 'preference', 'highest', ''],
                ['size', 'requirement', '>=', '0.05'],
            ],
        ],
    },
}

# Rule field definitions for the visual editor
VERSION_RULE_FIELDS = {
    'cache status': {'operators': ['cached', 'uncached'], 'has_value': False},
    'resolution': {'operators': ['==', '>=', '<=', 'highest', 'lowest'], 'has_value': True, 'unit': 'px'},
    'size': {'operators': ['==', '>=', '<=', 'highest', 'lowest'], 'has_value': True, 'unit': 'GB'},
    'seeders': {'operators': ['==', '>=', '<=', 'highest', 'lowest'], 'has_value': True},
    'bitrate': {'operators': ['==', '>=', '<=', 'highest', 'lowest'], 'has_value': True, 'unit': 'Mbit/s'},
    'title': {'operators': ['==', 'include', 'exclude'], 'has_value': True, 'value_type': 'regex'},
    'source': {'operators': ['==', 'include', 'exclude'], 'has_value': True, 'value_type': 'regex'},
    'file names': {'operators': ['include', 'exclude'], 'has_value': True, 'value_type': 'regex'},
    'file sizes': {'operators': ['all files >=', 'all files <=', 'video files >=', 'video files <='],
                   'has_value': True, 'unit': 'GB'},
}

VERSION_RULE_WEIGHTS = ['requirement', 'preference']

VERSION_CONDITION_FIELDS = {
    'retries': {'operators': ['==', '>=', '<='], 'has_value': True},
    'media type': {'operators': ['all', 'movies', 'shows'], 'has_value': False},
    'year': {'operators': ['==', '>=', '<='], 'has_value': True},
    'title': {'operators': ['==', 'include', 'exclude'], 'has_value': True},
    'user': {'operators': ['==', 'include', 'exclude'], 'has_value': True},
    'genre': {'operators': ['==', 'include', 'exclude'], 'has_value': True},
}


def get_version_presets():
    """Return presets as a JSON-serializable dict."""
    return {
        key: {'name': p['name'], 'description': p['description'], 'profile': p['profile']}
        for key, p in VERSION_PRESETS.items()
    }


def get_version_editor_metadata():
    """Return rule field definitions for the visual editor."""
    return {
        'rule_fields': VERSION_RULE_FIELDS,
        'rule_weights': VERSION_RULE_WEIGHTS,
        'condition_fields': VERSION_CONDITION_FIELDS,
    }


# Field types for plex_debrid schema:
#   multiselect  — checkbox group, value is list of selected option names
#   radio        — radio group, value is list with 0 or 1 element
#   string       — text input, value is string
#   secret       — password input with show/hide
#   boolean_str  — toggle, value is "true"/"false" string
#   select       — dropdown, value is string
#   list_strings — repeatable text inputs, value is list of strings
#   list_pairs   — repeatable two-column inputs, value is list of [a, b]
#   json         — raw JSON textarea for complex structures
#   hidden       — not shown in UI (e.g., internal version field)

# Available service options for multi-select/radio fields
_CONTENT_SERVICES = ['Plex', 'Trakt', 'Overseerr', 'MDBList', 'Local Text File', 'Jellyfin']
_LIBRARY_COLLECTION = ['Plex Library', 'Trakt Collection', 'Overseerr Requests',
                       'MDBList Library', 'Local Media List', 'Jellyfin Library']
_LIBRARY_UPDATE = ['Plex Libraries', 'Plex Labels', 'Trakt Collection',
                   'Overseerr Requests', 'Jellyfin Libraries']
_LIBRARY_IGNORE = ['Plex Discover Watch Status', 'Trakt Watch Status', 'Local Ignore List']
_SCRAPER_SOURCES = ['torrentio', 'rarbg', '1337x', 'jackett', 'prowlarr',
                    'orionoid', 'nyaa', 'zilean', 'torbox', 'mediafusion', 'comet']
_DEBRID_SERVICES = ['Real Debrid', 'All Debrid', 'Premiumize', 'Debrid Link', 'PUT.io', 'Torbox']
_AUTO_REMOVE_OPTIONS = ['movie', 'show', 'both', 'none']

# Schema: list of (json_key, label, type, options_or_meta, hidden, help)
# For multiselect/radio: options_or_meta is the options list
# For list_pairs: options_or_meta is [col1_label, col2_label]
# For select: options_or_meta is the options list
# For others: options_or_meta is None

PLEX_DEBRID_SCHEMA = [
    {
        'name': 'Content Services',
        'description': 'Sources to monitor for new content requests',
        'fields': [
            ('Content Services', 'Content Services', 'multiselect', _CONTENT_SERVICES, False,
             'Choose which content services plex_debrid should monitor for new content.'),
            ('Plex users', 'Plex Users', 'list_pairs', ['Name', 'Token'], True,
             'Plex usernames and their authentication tokens.'),
            ('Plex auto remove', 'Plex Auto Remove', 'select', _AUTO_REMOVE_OPTIONS, True,
             'Which media types to remove from watchlist after download.'),
            ('Trakt users', 'Trakt Users', 'list_pairs', ['Name', 'Token/Code'], True,
             'Trakt usernames and auth codes. Use OAuth in Phase 3 for proper auth.'),
            ('Trakt lists', 'Trakt Lists', 'list_strings', None, True,
             'Trakt list URLs or IDs to monitor.'),
            ('Trakt auto remove', 'Trakt Auto Remove', 'select', _AUTO_REMOVE_OPTIONS, True,
             'Which media types to remove from Trakt watchlist after download.'),
            ('Trakt early movie releases', 'Trakt Early Releases', 'boolean_str', None, True,
             'Check Trakt for early movie releases.'),
            ('Overseerr users', 'Overseerr Users', 'list_strings', None, True,
             'Overseerr usernames whose requests to monitor. Use "all" for everyone.'),
            ('Overseerr API Key', 'Overseerr API Key', 'secret', None, True,
             'API key for your Overseerr instance.'),
            ('Overseerr Base URL', 'Overseerr Base URL', 'string', None, True,
             'Base URL for your Overseerr instance.'),
            ('MDBList API Key', 'MDBList API Key', 'secret', None, True,
             'API key from mdblist.com.'),
            ('MDBList List IDs', 'MDBList List IDs', 'list_strings', None, True,
             'MDBList list IDs to monitor. Find IDs in MDBList URLs.'),
        ],
    },
    {
        'name': 'Library Services',
        'description': 'Library detection, refresh, and ignore services',
        'fields': [
            ('Library collection service', 'Library Collection', 'radio', _LIBRARY_COLLECTION, False,
             'Service to determine your current media collection.'),
            ('Library update services', 'Library Update', 'multiselect', _LIBRARY_UPDATE, False,
             'Services to update after a complete download.'),
            ('Library ignore services', 'Library Ignore', 'multiselect', _LIBRARY_IGNORE, False,
             'Services to track content that should be ignored.'),
            ('Trakt library user', 'Trakt Library User', 'list_strings', None, True, ''),
            ('Trakt refresh user', 'Trakt Refresh User', 'list_strings', None, True, ''),
            ('Plex library refresh', 'Plex Library Sections', 'list_strings', None, True,
             'Plex library section IDs to refresh.'),
            ('Plex library partial scan', 'Plex Partial Scan', 'boolean_str', None, True,
             'Attempt partial scans instead of full library scans.'),
            ('Plex library refresh delay', 'Plex Refresh Delay (sec)', 'string', None, True,
             'Seconds to wait between adding a torrent and scanning libraries.'),
            ('Plex server address', 'Plex Server Address', 'string', None, True,
             'Plex server URL for library operations.'),
            ('Plex library check', 'Plex Library Check Sections', 'list_strings', None, True,
             'Limit existing-content checks to these Plex library section numbers.'),
            ('Plex ignore user', 'Plex Ignore User', 'string', None, True, ''),
            ('Trakt ignore user', 'Trakt Ignore User', 'string', None, True, ''),
            ('Local ignore list path', 'Ignore List Path', 'string', None, True,
             'Path for the local ignore list file.'),
            ('Jellyfin API Key', 'Jellyfin API Key', 'secret', None, True,
             'Jellyfin API key for library access.'),
            ('Jellyfin server address', 'Jellyfin Server Address', 'string', None, True,
             'Jellyfin server URL.'),
        ],
    },
    {
        'name': 'Scraper Settings',
        'description': 'Torrent/debrid scraper configuration',
        'fields': [
            ('Sources', 'Scraper Sources', 'multiselect', _SCRAPER_SOURCES, False,
             'Torrent indexers and scrapers to search.'),
            ('Versions', 'Release Versions / Quality Profiles', 'json', None, False,
             'Complex release matching rules. Edit the JSON directly.'),
            ('Special character renaming', 'Character Renaming Rules', 'list_pairs',
             ['Find', 'Replace'], False,
             'Character or regex replacements applied to release titles.'),
            ('Rarbg API Key', 'Rarbg API Key', 'string', None, True, ''),
            ('Jackett Base URL', 'Jackett Base URL', 'string', None, True, ''),
            ('Jackett API Key', 'Jackett API Key', 'secret', None, True, ''),
            ('Jackett resolver timeout', 'Jackett Timeout (sec)', 'string', None, True, ''),
            ('Jackett indexer filter', 'Jackett Indexer Filter', 'string', None, True, ''),
            ('Prowlarr Base URL', 'Prowlarr Base URL', 'string', None, True, ''),
            ('Prowlarr API Key', 'Prowlarr API Key', 'secret', None, True, ''),
            ('Orionoid API Key', 'Orionoid API Key', 'secret', None, True, ''),
            ('Orionoid Scraper Parameters', 'Orionoid Parameters', 'list_pairs',
             ['Parameter', 'Value'], True, ''),
            ('Nyaa parameters', 'Nyaa URL Parameters', 'string', None, True, ''),
            ('Nyaa sleep time', 'Nyaa Sleep Time (sec)', 'string', None, True, ''),
            ('Nyaa proxy', 'Nyaa Proxy', 'string', None, True, ''),
            ('Torrentio Scraper Parameters', 'Torrentio Manifest URL', 'string', None, True,
             'Configure at torrentio.strem.fun/configure and paste the manifest URL.'),
            ('Zilean Base URL', 'Zilean Base URL', 'string', None, True, ''),
            ('Mediafusion Base URL', 'Mediafusion Base URL', 'string', None, True, ''),
            ('Mediafusion API Key', 'Mediafusion API Key', 'secret', None, True, ''),
            ('Mediafusion Request Timeout', 'Mediafusion Timeout (sec)', 'string', None, True, ''),
            ('Mediafusion Rate Limit', 'Mediafusion Rate Limit (sec)', 'string', None, True, ''),
            ('Mediafusion Scraper Parameters', 'Mediafusion Manifest URL', 'string', None, True, ''),
            ('Comet Request Timeout', 'Comet Timeout (sec)', 'string', None, True, ''),
            ('Comet Rate Limit', 'Comet Rate Limit (sec)', 'string', None, True, ''),
            ('Comet Scraper Parameters', 'Comet Manifest URL', 'string', None, True, ''),
        ],
    },
    {
        'name': 'Debrid Services',
        'description': 'Debrid service accounts for cached torrent access',
        'fields': [
            ('Debrid Services', 'Active Debrid Services', 'multiselect', _DEBRID_SERVICES, False,
             'Choose which debrid services to use.'),
            ('Tracker specific Debrid Services', 'Tracker-Specific Rules', 'list_pairs',
             ['Tracker Regex', 'Service (RD/PM/AD/PUT/DL)'], False,
             'Route specific trackers to specific debrid services.'),
            ('Real Debrid API Key', 'Real Debrid API Key', 'secret', None, True, ''),
            ('All Debrid API Key', 'All Debrid API Key', 'secret', None, True, ''),
            ('Premiumize API Key', 'Premiumize API Key', 'secret', None, True, ''),
            ('Debrid Link API Key', 'Debrid Link API Key', 'secret', None, True,
             'Uses OAuth device code flow. Set up via plex_debrid menu or Phase 3 web OAuth.'),
            ('Put.io API Key', 'Put.io API Key', 'secret', None, True,
             'Uses OAuth device code flow. Set up via plex_debrid menu or Phase 3 web OAuth.'),
            ('Torbox API Key', 'Torbox API Key', 'secret', None, True, ''),
        ],
    },
    {
        'name': 'UI Settings',
        'description': 'plex_debrid runtime behavior',
        'fields': [
            ('Show Menu on Startup', 'Show Menu on Startup', 'boolean_str', None, False,
             'Show the interactive plex_debrid menu on container start.'),
            ('Debug printing', 'Debug Printing', 'boolean_str', None, False,
             'Enable verbose debug output.'),
            ('Log to file', 'Log to File', 'boolean_str', None, False,
             'Write plex_debrid output to a log file.'),
            ('Watchlist loop interval (sec)', 'Watchlist Check Interval (sec)', 'string', None, False,
             'How often to check watchlists for new content.'),
            ('version', 'Version', 'hidden', None, True, 'Internal version tracking.'),
        ],
    },
]

# All known plex_debrid setting keys
_PD_ALL_KEYS = {field[0] for cat in PLEX_DEBRID_SCHEMA for field in cat['fields']}


def get_plex_debrid_schema():
    """Return the plex_debrid settings schema as a JSON-serializable structure."""
    categories = []
    for cat in PLEX_DEBRID_SCHEMA:
        fields = []
        for json_key, label, ftype, options, hidden, help_text in cat['fields']:
            field = {
                'key': json_key,
                'label': label,
                'type': ftype,
                'hidden': hidden,
                'help': help_text,
                'sensitive': any(p in json_key.upper() for p in ('KEY', 'TOKEN', 'SECRET')),
            }
            if options is not None:
                field['options'] = options
            if json_key in _OAUTH_FIELD_MAP:
                field['oauth'] = _OAUTH_FIELD_MAP[json_key]
            fields.append(field)
        categories.append({
            'name': cat['name'],
            'description': cat['description'],
            'fields': fields,
        })
    return {
        'categories': categories,
        'version_presets': get_version_presets(),
        'version_editor': get_version_editor_metadata(),
    }


# ---------------------------------------------------------------------------
# Bidirectional sync: settings.json → .env
#
# pd_setup() seeds settings.json from .env on container startup.  Without
# syncing the other direction, WebUI edits to plex_debrid settings are
# overwritten on the next container restart.  This mapping lets us write
# changed values back to .env so both stay consistent.
# ---------------------------------------------------------------------------

# Simple 1:1 mappings: settings.json key → .env variable name
_SETTINGS_JSON_TO_ENV = {
    'Overseerr Base URL':       'SEERR_ADDRESS',
    'Overseerr API Key':        'SEERR_API_KEY',
    'Plex server address':      'PLEX_ADDRESS',
    'Jellyfin API Key':         'JF_API_KEY',
    'Jellyfin server address':  'JF_ADDRESS',
    'Real Debrid API Key':      'RD_API_KEY',
    'All Debrid API Key':       'AD_API_KEY',
    'Show Menu on Startup':     'SHOW_MENU',
    'Log to file':              'PD_LOGFILE',
    'Torbox API Key':           'TORBOX_API_KEY',
}

# Lock to prevent concurrent .env writes from racing
_env_write_lock = threading.Lock()


def _sync_plex_debrid_to_env(values):
    """Sync plex_debrid settings back to .env so pd_setup() stays consistent.

    Only updates keys that actually changed.  Does NOT trigger SIGHUP
    because the caller already handles the plex_debrid restart.
    """
    env_updates = {}

    # Simple 1:1 mappings
    for json_key, env_key in _SETTINGS_JSON_TO_ENV.items():
        if json_key in values:
            val = values[json_key]
            if val is None:
                env_updates[env_key] = ''
            elif isinstance(val, bool):
                env_updates[env_key] = str(val).lower()
            else:
                try:
                    env_updates[env_key] = _sanitize_value(val)
                except ValueError as e:
                    logger.warning(f'[settings] Skipping .env sync for {env_key}: {e}')

    # Special: "Plex users" → PLEX_USER + PLEX_TOKEN (first pair)
    plex_users = values.get('Plex users')
    if isinstance(plex_users, list) and plex_users:
        first = plex_users[0]
        if isinstance(first, list) and len(first) >= 2:
            env_updates['PLEX_USER'] = str(first[0]) if first[0] else ''
            env_updates['PLEX_TOKEN'] = str(first[1]) if first[1] else ''

    # Special: "Debug printing" → PD_LOG_LEVEL (lossy: only DEBUG vs non-DEBUG)
    debug_printing = values.get('Debug printing')
    if debug_printing is not None:
        if str(debug_printing).lower() == 'true':
            env_updates['PD_LOG_LEVEL'] = 'DEBUG'
        else:
            # Only downgrade from DEBUG; don't overwrite other levels
            current_level = os.environ.get('PD_LOG_LEVEL', '')
            if current_level.upper() == 'DEBUG':
                env_updates['PD_LOG_LEVEL'] = 'INFO'

    if not env_updates:
        return

    with _env_write_lock:
        # Read current .env values to detect actual changes
        current = {}
        if os.path.exists(ENV_FILE):
            current = dotenv_values(ENV_FILE)

        changed = {}
        for key, new_val in env_updates.items():
            file_val = current.get(key)
            old_val = file_val if file_val is not None else os.environ.get(key, '')
            if old_val != new_val:
                changed[key] = new_val

        if not changed:
            return

        # Merge and rewrite .env (preserves all existing keys)
        existing = read_env_values()
        merged = {**existing, **changed}

        try:
            with atomic_write(ENV_FILE) as f:
                f.write('# pd_zurg configuration — managed by settings editor\n')
                f.write('# Manual edits are preserved on next save\n\n')
                for cat in ENV_SCHEMA:
                    cat_has_values = False
                    lines = []
                    for key, label, ftype, required, help_text in cat['fields']:
                        val = merged.get(key, '')
                        if val:
                            cat_has_values = True
                        lines.append(_format_env_line(key, val))
                    if cat_has_values:
                        f.write(f'# --- {cat["name"]} ---\n')
                        for line in lines:
                            f.write(line + '\n')
                        f.write('\n')
        except Exception as e:
            logger.error(f'[settings] Failed to sync plex_debrid settings to .env: {e}')
            return

    # Update os.environ so in-process reads are consistent
    for key, val in changed.items():
        os.environ[key] = val

    logger.info(
        f'[settings] Synced {len(changed)} plex_debrid setting(s) back to .env: '
        f'{", ".join(sorted(changed.keys()))}'
    )


# Reverse mapping: .env variable → settings.json key
_ENV_TO_SETTINGS_JSON = {v: k for k, v in _SETTINGS_JSON_TO_ENV.items()}


def _sync_env_to_plex_debrid(env_values):
    """Sync .env values into settings.json so plex_debrid picks them up on restart.

    Without this, changing e.g. SEERR_ADDRESS in the env tab and clicking
    Save & Apply would restart plex_debrid, but it would still read the old
    value from settings.json (pd_setup() only runs on container startup).

    Only updates keys that actually changed.  Called from write_env_values()
    before the SIGHUP trigger.
    """
    if not os.path.exists(SETTINGS_JSON_FILE):
        return

    try:
        with open(SETTINGS_JSON_FILE, 'r') as f:
            settings = _json.load(f)
    except (ValueError, OSError):
        return

    changed = False

    # Simple 1:1 mappings
    for env_key, json_key in _ENV_TO_SETTINGS_JSON.items():
        if env_key not in env_values:
            continue
        new_val = env_values.get(env_key, '')
        old_val = settings.get(json_key, '')
        if new_val != old_val:
            settings[json_key] = new_val
            changed = True

    # Special: PLEX_USER + PLEX_TOKEN → "Plex users" (first pair)
    plex_user = env_values.get('PLEX_USER', '')
    plex_token = env_values.get('PLEX_TOKEN', '')
    if plex_user and plex_token:
        plex_users = settings.get('Plex users', [])
        new_pair = [plex_user, plex_token]
        if not any(pair == new_pair for pair in plex_users):
            # Update first pair or append
            if plex_users:
                if plex_users[0] != new_pair:
                    plex_users[0] = new_pair
                    changed = True
            else:
                plex_users.append(new_pair)
                changed = True
            settings['Plex users'] = plex_users

    # Special: PD_LOG_LEVEL → "Debug printing"
    pd_log_level = env_values.get('PD_LOG_LEVEL', '')
    if pd_log_level:
        new_debug = 'true' if pd_log_level.upper() == 'DEBUG' else 'false'
        if settings.get('Debug printing', '') != new_debug:
            settings['Debug printing'] = new_debug
            changed = True

    # Rebuild "Debrid Services" based on which API keys are present
    _KEY_TO_SERVICE = {
        'RD_API_KEY': 'Real Debrid',
        'AD_API_KEY': 'All Debrid',
        'TORBOX_API_KEY': 'Torbox',
    }
    debrid_services = list(settings.get('Debrid Services', []))
    for env_key, svc_name in _KEY_TO_SERVICE.items():
        has_key = bool(env_values.get(env_key, ''))
        in_list = svc_name in debrid_services
        if has_key and not in_list:
            debrid_services.append(svc_name)
            changed = True
        elif not has_key and in_list:
            debrid_services.remove(svc_name)
            changed = True
    if debrid_services != settings.get('Debrid Services', []):
        settings['Debrid Services'] = debrid_services

    # Plex/Jellyfin mutual exclusion (mirrors pd_setup() behavior)
    jf_key = env_values.get('JF_API_KEY', '')
    jf_addr = env_values.get('JF_ADDRESS', '')
    plex_user = env_values.get('PLEX_USER', '')
    if jf_key and jf_addr and not plex_user:
        # Jellyfin mode: clear Plex settings
        for field, default in [('Plex users', []), ('Plex server address', 'http://localhost:32400'),
                               ('Plex library refresh', [])]:
            if settings.get(field) != default:
                settings[field] = default
                changed = True
    elif plex_user and not (jf_key and jf_addr):
        # Plex mode: clear Jellyfin settings
        for field, default in [('Jellyfin API Key', ''), ('Jellyfin server address', 'http://localhost:8096')]:
            if settings.get(field) != default:
                settings[field] = default
                changed = True

    if not changed:
        return

    try:
        with atomic_write(SETTINGS_JSON_FILE) as f:
            _json.dump(settings, f, indent=4, ensure_ascii=False)
            f.write('\n')
    except Exception as e:
        logger.error(f'[settings] Failed to sync .env values to settings.json: {e}')
        return

    logger.info('[settings] Synced .env changes into settings.json')


def read_plex_debrid_values():
    """Read current plex_debrid settings.json. Returns the parsed dict."""
    if os.path.exists(SETTINGS_JSON_FILE):
        try:
            with open(SETTINGS_JSON_FILE, 'r') as f:
                return _json.load(f)
        except (ValueError, OSError) as e:
            logger.error(f'[settings] Failed to read {SETTINGS_JSON_FILE}: {e}')

    # Fall back to defaults
    if os.path.exists(SETTINGS_DEFAULT_FILE):
        try:
            with open(SETTINGS_DEFAULT_FILE, 'r') as f:
                return _json.load(f)
        except (ValueError, OSError):
            pass

    return {}


def write_plex_debrid_values(values):
    """Validate and write plex_debrid settings, then restart the service.

    Args:
        values: dict representing the full settings.json content

    Returns:
        dict with 'status', 'errors', 'warnings' keys
    """
    if not isinstance(values, dict):
        return {'status': 'error', 'errors': ['Expected a JSON object'], 'warnings': []}

    # Validate
    validation = validate_plex_debrid_values(values)
    if validation['errors']:
        return {
            'status': 'error',
            'errors': validation['errors'],
            'warnings': validation['warnings'],
        }

    # Write settings.json atomically
    try:
        with atomic_write(SETTINGS_JSON_FILE) as f:
            _json.dump(values, f, indent=4, ensure_ascii=False)
            f.write('\n')
    except Exception as e:
        logger.error(f'[settings] Failed to write {SETTINGS_JSON_FILE}: {e}')
        return {
            'status': 'error',
            'errors': [f'Failed to write settings file: {e}'],
            'warnings': [],
        }

    # Sync changed values back to .env so pd_setup() stays consistent
    # on container restart (must happen before the service restart)
    try:
        _sync_plex_debrid_to_env(values)
    except Exception as e:
        logger.warning(f'[settings] .env sync failed (settings.json still saved): {e}')

    # Restart plex_debrid to pick up changes
    restarted = False
    try:
        from utils.processes import restart_service
        import threading
        threading.Thread(target=restart_service, args=('plex_debrid',), daemon=True).start()
        restarted = True
        logger.info('[settings] Saved settings.json and triggered plex_debrid restart')
    except Exception as e:
        logger.error(f'[settings] Saved settings.json but restart failed: {e}')
        return {
            'status': 'saved_no_restart',
            'errors': [],
            'warnings': [f'Settings saved but plex_debrid restart failed: {e}'],
            'restarted': False,
        }

    return {
        'status': 'saved',
        'errors': [],
        'warnings': validation['warnings'],
        'restarted': restarted,
    }


def validate_plex_debrid_values(values):
    """Validate proposed plex_debrid settings. Returns {errors:[], warnings:[]}."""
    errors = []
    warnings = []

    if not isinstance(values, dict):
        return {'errors': ['Settings must be a JSON object'], 'warnings': []}

    # Check that list fields are actually lists
    for cat in PLEX_DEBRID_SCHEMA:
        for json_key, label, ftype, options, hidden, help_text in cat['fields']:
            if json_key not in values:
                continue
            val = values[json_key]

            if ftype in ('multiselect', 'radio', 'list_strings', 'list_pairs'):
                if not isinstance(val, list):
                    errors.append(f'"{json_key}" must be a list, got {type(val).__name__}')
                    continue

            if ftype == 'multiselect' and options and isinstance(val, list):
                for item in val:
                    if item not in options:
                        warnings.append(
                            f'"{json_key}" contains unknown option "{item}". '
                            f'Known options: {", ".join(options)}'
                        )

            if ftype == 'radio' and options and isinstance(val, list):
                if len(val) > 1:
                    warnings.append(
                        f'"{json_key}" should have at most one selection, got {len(val)}'
                    )
                for item in val:
                    if item not in options:
                        warnings.append(
                            f'"{json_key}" contains unknown option "{item}". '
                            f'Known options: {", ".join(options)}'
                        )

            if ftype == 'list_pairs' and isinstance(val, list):
                for i, item in enumerate(val):
                    if not isinstance(item, list) or len(item) < 2:
                        errors.append(
                            f'"{json_key}" entry {i + 1} must be a list with at least 2 elements'
                        )

            if ftype == 'json' and json_key == 'Versions':
                if not isinstance(val, list):
                    errors.append('"Versions" must be a list')

            if ftype == 'boolean_str' and isinstance(val, str):
                if val.lower() not in ('true', 'false', ''):
                    warnings.append(f'"{json_key}" should be "true" or "false", got "{val}"')

    return {'errors': errors, 'warnings': warnings}


# ===========================================================================
# OAuth device code flows
# ===========================================================================

# Map plex_debrid setting keys to their OAuth service identifier
_OAUTH_FIELD_MAP = {
    'Trakt users': 'trakt',
    'Debrid Link API Key': 'debridlink',
    'Put.io API Key': 'putio',
    'Orionoid API Key': 'orionoid',
}

OAUTH_SERVICES = {
    'trakt': {
        'name': 'Trakt',
        'verification_url': 'https://trakt.tv/activate',
        'interval': 5,
        'settings_key': 'Trakt users',
    },
    'debridlink': {
        'name': 'Debrid Link',
        'verification_url': 'https://debrid-link.fr/device',
        'client_id': '0KLCzpbPTCsWZtQ9Ad0aZA',
        'interval': 5,
        'settings_key': 'Debrid Link API Key',
    },
    'putio': {
        'name': 'Put.io',
        'verification_url': 'https://put.io/link',
        'client_id': '5843',
        'interval': 5,
        'settings_key': 'Put.io API Key',
    },
    'orionoid': {
        'name': 'Orionoid',
        'verification_url': 'https://auth.orionoid.com',
        'client_id': 'GPQJBFGJKAHVFM37LJDNNLTHKJMXEAJJ',
        'interval': 5,
        'settings_key': 'Orionoid API Key',
    },
}


def oauth_start(service):
    """Initiate an OAuth device code flow for a service.

    Returns dict with verification_url, user_code, device_code, interval
    or an error dict.
    """
    import requests

    if service not in OAUTH_SERVICES:
        return {'error': f'Unknown OAuth service: {service}'}

    svc = OAUTH_SERVICES[service]

    try:
        if service == 'trakt':
            client_id = os.environ.get('TRAKT_CLIENT_ID', '')
            if not client_id:
                return {'error': 'TRAKT_CLIENT_ID environment variable is not set. '
                        'Set it in the pd_zurg tab first.'}
            resp = requests.post(
                'https://api.trakt.tv/oauth/device/code',
                json={'client_id': client_id},
                headers={'Content-Type': 'application/json'},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                'verification_url': data.get('verification_url', svc['verification_url']),
                'user_code': data['user_code'],
                'device_code': data['device_code'],
                'interval': data.get('interval', svc['interval']),
            }

        elif service == 'debridlink':
            resp = requests.post(
                'https://debrid-link.fr/api/oauth/device/code',
                data=f'client_id={svc["client_id"]}',
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            value = data.get('value', data)
            return {
                'verification_url': svc['verification_url'],
                'user_code': value.get('user_code', value.get('userCode', '')),
                'device_code': value.get('device_code', value.get('deviceCode', '')),
                'interval': value.get('interval', svc['interval']),
            }

        elif service == 'putio':
            resp = requests.get(
                f'https://api.put.io/v2/oauth2/oob/code?app_id={svc["client_id"]}',
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            code = data.get('code', '')
            return {
                'verification_url': svc['verification_url'],
                'user_code': code,
                'device_code': code,
                'interval': svc['interval'],
            }

        elif service == 'orionoid':
            resp = requests.get(
                f'https://api.orionoid.com?keyapp={svc["client_id"]}'
                f'&mode=user&action=authenticate',
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            code = data.get('data', {}).get('code', '')
            return {
                'verification_url': svc['verification_url'],
                'user_code': code,
                'device_code': code,
                'interval': svc['interval'],
            }

    except requests.RequestException as e:
        logger.error(f'[oauth] {svc["name"]} device code request failed: {e}')
        return {'error': f'Failed to reach {svc["name"]}: {e}'}
    except (KeyError, ValueError) as e:
        logger.error(f'[oauth] {svc["name"]} unexpected response: {e}')
        return {'error': f'Unexpected response from {svc["name"]}: {e}'}


def oauth_poll(service, device_code):
    """Poll an OAuth service to check if the user has authorized.

    Returns {status: "pending"} or {status: "complete", token: "..."}
    or an error dict.
    """
    import requests

    if service not in OAUTH_SERVICES:
        return {'error': f'Unknown OAuth service: {service}'}

    svc = OAUTH_SERVICES[service]

    try:
        if service == 'trakt':
            client_id = os.environ.get('TRAKT_CLIENT_ID', '')
            client_secret = os.environ.get('TRAKT_CLIENT_SECRET', '')
            if not client_id or not client_secret:
                return {'error': 'TRAKT_CLIENT_ID and TRAKT_CLIENT_SECRET must be set'}
            resp = requests.post(
                'https://api.trakt.tv/oauth/device/token',
                json={
                    'code': device_code,
                    'client_id': client_id,
                    'client_secret': client_secret,
                },
                headers={'Content-Type': 'application/json'},
                timeout=15,
            )
            if resp.status_code == 400:
                return {'status': 'pending'}
            resp.raise_for_status()
            data = resp.json()
            token = data.get('access_token', '')
            if token:
                return {'status': 'complete', 'token': token}
            return {'status': 'pending'}

        elif service == 'debridlink':
            resp = requests.post(
                'https://debrid-link.fr/api/oauth/token',
                data=(f'client_id={svc["client_id"]}'
                      f'&code={device_code}'
                      f'&grant_type=http%3A%2F%2Foauth.net%2Fgrant_type%2Fdevice%2F1.0'),
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=15,
            )
            if resp.status_code in (400, 403):
                return {'status': 'pending'}
            resp.raise_for_status()
            data = resp.json()
            value = data.get('value', data)
            token = value.get('access_token', '')
            if token:
                return {'status': 'complete', 'token': token}
            return {'status': 'pending'}

        elif service == 'putio':
            resp = requests.get(
                f'https://api.put.io/v2/oauth2/oob/code/{device_code}',
                timeout=15,
            )
            if resp.status_code == 400:
                return {'status': 'pending'}
            resp.raise_for_status()
            data = resp.json()
            token = data.get('oauth_token', '')
            if token:
                return {'status': 'complete', 'token': token}
            return {'status': 'pending'}

        elif service == 'orionoid':
            resp = requests.get(
                f'https://api.orionoid.com?keyapp={svc["client_id"]}'
                f'&mode=user&action=authenticate&code={device_code}',
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            token_data = data.get('data', {})
            token = token_data.get('token', '')
            if token:
                return {'status': 'complete', 'token': token}
            return {'status': 'pending'}

    except requests.RequestException as e:
        logger.error(f'[oauth] {svc["name"]} poll failed: {e}')
        return {'error': f'Failed to reach {svc["name"]}: {e}'}
    except (KeyError, ValueError) as e:
        return {'error': f'Unexpected response from {svc["name"]}: {e}'}


# ===========================================================================
# Import / Export / Reset
# ===========================================================================

def export_env():
    """Read the raw .env file content for download."""
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'r') as f:
            return f.read()
    return ''


def export_plex_debrid():
    """Read the raw settings.json content for download."""
    if os.path.exists(SETTINGS_JSON_FILE):
        with open(SETTINGS_JSON_FILE, 'r') as f:
            return f.read()
    return '{}'


def get_plex_debrid_defaults():
    """Read the default settings.json template."""
    if os.path.exists(SETTINGS_DEFAULT_FILE):
        try:
            with open(SETTINGS_DEFAULT_FILE, 'r') as f:
                return _json.load(f)
        except (ValueError, OSError):
            pass
    return {}


def get_env_defaults():
    """Return empty values for all env schema keys (the application defaults)."""
    return {key: '' for key in _ALL_KEYS}
