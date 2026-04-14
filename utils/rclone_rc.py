"""Rclone RC (remote control) API helpers.

Provides cache invalidation via rclone's RC interface so the FUSE mount
reflects Zurg changes immediately instead of waiting for dir-cache-time
expiry.

Use :func:`refresh_dir`, not :func:`forget_dir_cache`. ``vfs/forget`` only
clears rclone's in-process VFS cache without notifying the kernel FUSE
layer, so stale dentries persist. ``vfs/refresh`` re-reads from the backend
and, because it diffs new vs. existing entries, rclone emits
``FUSE_NOTIFY_INVAL_ENTRY`` for changes so the kernel drops its cached
dentries.
"""

import json
import urllib.request
import urllib.error

from utils.logger import get_logger

logger = get_logger()


def _post(rc_url, path, payload, timeout=2):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{rc_url}{path}", data=data,
        headers={'Content-Type': 'application/json'},
        method='POST')
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    return json.loads(body) if body else {}


def refresh_dir(dir_path='', recursive=False):
    """Refresh rclone's dir cache via RC ``vfs/refresh`` on all registered mounts.

    Unlike ``vfs/forget``, this re-reads the directory from the backend and
    diffs against the cached entries, emitting kernel FUSE notifications
    for added/removed items so the kernel dentry cache actually updates.

    The kernel-invalidation behaviour is FUSE-specific. On ``NFSMOUNT=true``
    rclone serves NFS instead and the kernel NFS client does its own attribute
    caching that rclone has no hook into — ``vfs/refresh`` will still update
    rclone's internal VFS, but NFS client staleness is governed by the NFS
    mount's ``acregmin``/``acregmax``/``acdirmin``/``acdirmax`` timers.

    Args:
        dir_path: Mount-relative directory. Empty string refreshes the mount
            root (each top-level category dir — ``movies``, ``shows``,
            ``__all__`` — gets re-listed).
        recursive: Walk into subdirectories. Leave ``False`` for hot paths;
            recursive=True is expensive on large libraries.

    Returns:
        True if at least one mount refreshed successfully, False otherwise.
    """
    try:
        from rclone.rclone import get_all_rc_urls
    except ImportError:
        return False

    urls = get_all_rc_urls()
    if not urls:
        logger.debug("[rclone-rc] No RC URLs registered, skipping refresh")
        return False

    payload = {'recursive': bool(recursive)}
    if dir_path:
        payload['dir'] = dir_path

    success = False
    for rc_url in urls:
        try:
            result = _post(rc_url, '/vfs/refresh', payload)
            success = True
            logger.debug("[rclone-rc] Refreshed %s on %s: %s",
                         dir_path or '<root>', rc_url, result.get('result'))
        except (urllib.error.URLError, OSError, ValueError) as e:
            logger.debug("[rclone-rc] Refresh failed for %s: %s", rc_url, e)
    return success


def forget_dir_cache(dir_path=None):
    """Clear rclone's in-process VFS dir cache via ``vfs/forget``.

    .. warning::
       Prefer :func:`refresh_dir`. ``vfs/forget`` clears rclone's VFS but
       does NOT emit kernel FUSE invalidation, so a subsequent ``os.listdir``
       on the mount can keep returning stale dentries until
       ``--dir-cache-time`` expires. Left for the rare case where wiping
       without re-reading is actually wanted.

    Args:
        dir_path: Mount-relative directory to forget. ``None`` forgets every
            cached directory (rclone returns ``{"forgotten": []}`` in that
            case — the empty list is normal, not an error).

    Returns:
        True if at least one mount responded, False otherwise.
    """
    try:
        from rclone.rclone import get_all_rc_urls
    except ImportError:
        return False

    urls = get_all_rc_urls()
    if not urls:
        return False

    payload = {'dir': dir_path} if dir_path else {}
    success = False
    for rc_url in urls:
        try:
            _post(rc_url, '/vfs/forget', payload, timeout=2)
            success = True
        except (urllib.error.URLError, OSError, ValueError) as e:
            logger.debug("[rclone-rc] Forget failed for %s: %s", rc_url, e)
    return success
