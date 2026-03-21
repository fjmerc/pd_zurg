"""Event notification system via Apprise.

Supports 90+ notification services (Discord, Telegram, Slack, email, etc.)
through a single NOTIFICATION_URL environment variable.
"""

import apprise
import os
from utils.logger import get_logger

logger = get_logger()

_notifier = None
_enabled_events = None
_min_level = 'info'

LEVEL_ORDER = {'info': 0, 'warning': 1, 'error': 2}


def init():
    """Initialize Apprise from environment. Call once at startup."""
    global _notifier, _enabled_events, _min_level
    url = os.environ.get('NOTIFICATION_URL')
    if not url:
        return

    _notifier = apprise.Apprise()
    for u in url.split(','):
        _notifier.add(u.strip())

    events_str = os.environ.get('NOTIFICATION_EVENTS')
    if events_str:
        _enabled_events = set(e.strip() for e in events_str.split(','))

    _min_level = os.environ.get('NOTIFICATION_LEVEL', 'info').lower()
    logger.info("Notifications initialized")


def notify(event, title, body, level='info'):
    """Send a notification if the event and level are enabled.

    Args:
        event: Event type string (e.g., 'download_complete')
        title: Notification title
        body: Notification body text
        level: 'info', 'warning', or 'error'
    """
    if not _notifier:
        return

    if _enabled_events and event not in _enabled_events:
        return

    if LEVEL_ORDER.get(level, 0) < LEVEL_ORDER.get(_min_level, 0):
        return

    notify_type = {
        'info': apprise.NotifyType.INFO,
        'warning': apprise.NotifyType.WARNING,
        'error': apprise.NotifyType.FAILURE,
    }.get(level, apprise.NotifyType.INFO)

    try:
        _notifier.notify(title=title, body=body, notify_type=notify_type)
    except Exception as e:
        logger.error(f"Notification failed: {e}")
