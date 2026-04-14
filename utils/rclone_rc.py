"""Rclone RC (remote control) API helpers.

Provides cache invalidation via rclone's RC interface so the FUSE mount
reflects Zurg changes immediately instead of waiting for dir-cache-time
expiry.
"""

import json
import urllib.request
import urllib.error

from utils.logger import get_logger

logger = get_logger()


def forget_dir_cache(dir_path=None):
    """Flush rclone's directory cache via RC ``vfs/forget``.

    Flushes all registered rclone mounts (handles dual RD+AD setups).

    Args:
        dir_path: Optional directory path to forget (relative to mount root).
                  If *None*, forgets the entire cache.

    Returns:
        True if at least one mount's cache was flushed, False on error
        (logged, never raises).
    """
    try:
        from rclone.rclone import get_all_rc_urls
    except ImportError:
        return False

    urls = get_all_rc_urls()
    if not urls:
        logger.debug("[rclone-rc] No RC URLs registered, skipping cache flush")
        return False

    payload = {}
    if dir_path:
        payload = {'dir': dir_path}
    data = json.dumps(payload).encode()

    success = False
    for rc_url in urls:
        try:
            req = urllib.request.Request(
                f"{rc_url}/vfs/forget", data=data,
                headers={'Content-Type': 'application/json'},
                method='POST')
            with urllib.request.urlopen(req, timeout=2) as resp:
                resp.read()
            success = True
        except (urllib.error.URLError, OSError, ValueError) as e:
            logger.debug("[rclone-rc] Cache flush failed for %s: %s", rc_url, e)

    if success:
        if dir_path:
            logger.debug("[rclone-rc] Flushed dir cache for %s", dir_path)
        else:
            logger.debug("[rclone-rc] Flushed entire dir cache")
    return success
