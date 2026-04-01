"""Event notification system via Apprise.

Supports 90+ notification services (Discord, Telegram, Slack, email, etc.)
through a single NOTIFICATION_URL environment variable.
"""

import os
import threading
from utils.logger import get_logger

try:
    import apprise
    _apprise_available = True
except ImportError:
    _apprise_available = False

logger = get_logger()

_notifier = None
_enabled_events = None
_min_level = 'info'
_lock = threading.Lock()

LEVEL_ORDER = {'info': 0, 'warning': 1, 'error': 2}
_VALID_LEVELS = ('info', 'warning', 'error')

# All supported event types for NOTIFICATION_EVENTS filtering.
# Users can subscribe to any subset via comma-separated list.
# Set — order is not significant; see settings_api.py for documentation order.
ALL_EVENTS = {
    # Core lifecycle
    'startup',                    # Service/container started or config reloaded
    'shutdown',                   # Service shutting down
    # Downloads (blackhole)
    'download_complete',          # Torrent download & symlink succeeded
    'download_error',             # Torrent download failed
    # Library
    'library_refresh',            # Source switch or library change
    'symlink_created',            # Debrid symlinks created in local library
    'symlink_failed',             # Symlink creation failed (mount/path issue)
    # Debrid availability
    'debrid_unavailable',         # Content marked debrid-unavailable after threshold
    'local_fallback_triggered',   # Local download started as debrid fallback
    # Safety
    'blocklist_added',            # Torrent added to blocklist
    'health_error',               # Process crash, mount failure
    # Debrid search
    'debrid_add_success',         # Torrent added to debrid from interactive search
    'debrid_add_failed',          # Failed to add torrent to debrid
    # Digest
    'daily_digest',               # Daily summary notification
}


def init():
    """Initialize Apprise from environment. Call once at startup."""
    global _notifier, _enabled_events, _min_level
    url = os.environ.get('NOTIFICATION_URL')
    if not url:
        return

    if not _apprise_available:
        logger.warning("NOTIFICATION_URL is set but 'apprise' package is not installed")
        return

    with _lock:
        _notifier = apprise.Apprise()
        for u in url.split(','):
            if not _notifier.add(u.strip()):
                logger.warning(f"Notification URL not recognized: {u.strip()[:30]}...")

        events_str = os.environ.get('NOTIFICATION_EVENTS')
        if events_str:
            _enabled_events = set(e.strip() for e in events_str.split(','))

        _min_level = os.environ.get('NOTIFICATION_LEVEL', 'info').lower()
        if _min_level not in _VALID_LEVELS:
            logger.warning(f"Invalid NOTIFICATION_LEVEL '{_min_level}', defaulting to 'info'")
            _min_level = 'info'

    logger.info("Notifications initialized")


def notify(event, title, body, level='info'):
    """Send a notification if the event and level are enabled.

    Args:
        event: Event type string (e.g., 'download_complete')
        title: Notification title
        body: Notification body text
        level: 'info', 'warning', or 'error'
    """
    # Take a snapshot of config under lock
    with _lock:
        notifier = _notifier
        enabled = _enabled_events
        min_level = _min_level

    if not notifier:
        return

    if enabled and event not in enabled:
        return

    if LEVEL_ORDER.get(level, 0) < LEVEL_ORDER.get(min_level, 0):
        return

    notify_type = {
        'info': apprise.NotifyType.INFO,
        'warning': apprise.NotifyType.WARNING,
        'error': apprise.NotifyType.FAILURE,
    }.get(level, apprise.NotifyType.INFO)

    try:
        notifier.notify(title=title, body=body, notify_type=notify_type)
    except Exception as e:
        # Avoid logging full exception which may contain credential-bearing URLs
        logger.error(f"Notification failed for event '{event}': {type(e).__name__}")
