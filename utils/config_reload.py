"""Graceful config reload via SIGHUP.

Reloads .env file, detects changed variables, and restarts
only the affected services. Eliminates the need for full
container restart on config changes.

Usage:
    docker kill -s HUP zurgarr
"""

import os
import threading
from dotenv import dotenv_values
from utils.logger import get_logger

logger = get_logger()

ENV_FILE = '/config/.env'

# Which env vars affect which services
SERVICE_DEPENDENCIES = {
    'zurg': {
        'RD_API_KEY', 'AD_API_KEY', 'TORBOX_API_KEY', 'ZURG_ENABLED',
        'ZURG_VERSION', 'ZURG_LOG_LEVEL', 'ZURG_USER', 'ZURG_PASS',
        'ZURG_PORT',
    },
    'rclone': {
        'RCLONE_MOUNT_NAME', 'RCLONE_LOG_LEVEL', 'RCLONE_CACHE_DIR',
        'RCLONE_DIR_CACHE_TIME', 'RCLONE_VFS_CACHE_MODE',
        'RCLONE_VFS_CACHE_MAX_SIZE', 'RCLONE_VFS_CACHE_MAX_AGE',
        'RCLONE_VFS_READ_CHUNK_SIZE',
        'RCLONE_VFS_READ_CHUNK_SIZE_LIMIT', 'RCLONE_BUFFER_SIZE',
        'RCLONE_TRANSFERS', 'NFS_ENABLED', 'NFS_PORT',
    },
    'plex_debrid': {
        'PD_ENABLED', 'PLEX_USER', 'PLEX_TOKEN', 'PLEX_ADDRESS',
        'SHOW_MENU', 'SEERR_API_KEY', 'SEERR_ADDRESS',
        'JF_API_KEY', 'JF_ADDRESS', 'RD_API_KEY', 'AD_API_KEY',
        'TORBOX_API_KEY', 'TRAKT_CLIENT_ID', 'TRAKT_CLIENT_SECRET',
        'FLARESOLVERR_URL', 'PD_LOGFILE',
    },
    'blackhole': {
        'BLACKHOLE_ENABLED', 'BLACKHOLE_DIR', 'BLACKHOLE_POLL_INTERVAL',
        'BLACKHOLE_DEBRID',
    },
    'notifications': {
        'NOTIFICATION_URL', 'NOTIFICATION_EVENTS', 'NOTIFICATION_LEVEL',
    },
    'status_ui': {
        'STATUS_UI_ENABLED', 'STATUS_UI_PORT', 'STATUS_UI_AUTH',
    },
}

# Changes that only need variable reload, no service restart
SOFT_RELOAD = {
    # Log levels — dual-read window (2.19.0 → 2.20.0). New ZURGARR_LOG_*
    # names are preferred; legacy PDZURG_LOG_* still trigger soft-reload so
    # users mid-migration don't see a regression.
    'ZURGARR_LOG_LEVEL', 'ZURGARR_LOG_COUNT', 'ZURGARR_LOG_SIZE',
    'PDZURG_LOG_LEVEL', 'PD_LOG_LEVEL', 'NOTIFICATION_LEVEL',
    'NOTIFICATION_EVENTS', 'DUPLICATE_CLEANUP', 'CLEANUP_INTERVAL', 'DUPLICATE_CLEANUP_KEEP',
    'PLEX_REFRESH', 'SKIP_VALIDATION', 'LIBRARY_PREFERENCE_AUTO_ENFORCE',
    'BLOCKLIST_AUTO_ADD',
    # Quality compromise (plan 33): all nine toggles are read fresh
    # from os.environ on each blackhole retry cycle via `_compromise_enabled`,
    # `_int_env`, `_float_env`, etc. — no module globals to restart,
    # so SIGHUP gets them with zero service churn.
    'QUALITY_COMPROMISE_ENABLED', 'QUALITY_COMPROMISE_DWELL_DAYS',
    'QUALITY_COMPROMISE_MIN_SEEDERS', 'QUALITY_COMPROMISE_ONLY_CACHED',
    'QUALITY_COMPROMISE_MAX_TIER_DROP', 'QUALITY_COMPROMISE_NOTIFY',
    'SEASON_PACK_FALLBACK_ENABLED', 'SEASON_PACK_FALLBACK_MIN_MISSING',
    'SEASON_PACK_FALLBACK_MIN_RATIO',
}


# Snapshot of .env keys from the last load — used to detect removals.
# Only keys that were previously IN the .env file should be cleared on
# removal.  Keys set via docker-compose environment: (which are in
# os.environ but NOT in .env) must never be touched.
# Initialized from the current .env at import time so the first SIGHUP
# can correctly detect removals.
_last_env_keys = set(dotenv_values(ENV_FILE).keys()) if os.path.exists(ENV_FILE) else set()


def _reload_env():
    """Reload .env file and return set of changed variable names."""
    global _last_env_keys

    if not os.path.exists(ENV_FILE):
        logger.warning(f"[reload] No .env file found at {ENV_FILE}")
        return set()

    new_values = dotenv_values(ENV_FILE)
    changed = set()

    for key, new_val in new_values.items():
        old_val = os.environ.get(key)
        if old_val != new_val:
            # Mask sensitive values in logs
            if any(s in key.upper() for s in ('KEY', 'TOKEN', 'PASS', 'SECRET', 'AUTH')):
                logger.info(f"[reload] {key} changed: *** -> ***")
            else:
                logger.info(f"[reload] {key} changed: '{old_val}' -> '{new_val}'")
            os.environ[key] = new_val if new_val is not None else ''
            changed.add(key)

    # Detect keys removed from .env — only clear keys that were in the
    # PREVIOUS .env snapshot, not keys from docker-compose or other sources.
    for key in _last_env_keys:
        if key not in new_values and os.environ.get(key, ''):
            logger.info(f"[reload] {key} removed from .env")
            os.environ[key] = ''
            changed.add(key)

    _last_env_keys = set(new_values.keys())

    return changed


def _determine_restarts(changed_vars):
    """Given changed env var names, return services that need restart."""
    services = set()

    for service, deps in SERVICE_DEPENDENCIES.items():
        if changed_vars & deps:
            services.add(service)

    # Dependency chain: rclone depends on zurg
    if 'zurg' in services:
        services.add('rclone')

    # plex_debrid depends on rclone mounts
    if 'rclone' in services:
        services.add('plex_debrid')

    return services


_reload_lock = threading.Lock()


def _do_reload():
    """Perform the actual reload work. Runs in a separate thread."""
    if not _reload_lock.acquire(blocking=False):
        logger.info("[reload] Reload already in progress, skipping")
        return
    try:
        import utils.processes as _proc_mod
        if _proc_mod._shutting_down:
            logger.info("[reload] Aborting — shutdown in progress")
            return

        changed = _reload_env()

        if not changed:
            logger.info("[reload] No changes detected")
            return

        # Reload the Config singleton so module-level vars update
        try:
            from base import Config, config
            config.load()
        except Exception as e:
            logger.error(f"[reload] Failed to reload base config: {e}")
            return

        # Determine what needs restarting
        soft_only = changed <= SOFT_RELOAD
        if soft_only:
            logger.info(
                f"[reload] Soft reload complete — {len(changed)} variable(s) updated, "
                f"no service restarts needed"
            )
            _notify_reload(changed, set())
            return

        services = _determine_restarts(changed)
        logger.info(f"[reload] Services to restart: {', '.join(sorted(services))}")

        # Handle process-based services
        process_services = {'zurg', 'rclone', 'plex_debrid'} & services
        if process_services:
            from utils.processes import _process_registry, _registry_lock

            # Stop affected services (reverse dependency order)
            stop_order = ['plex_debrid', 'rclone', 'zurg']
            start_entries = []

            with _registry_lock:
                for svc_name in stop_order:
                    if svc_name not in process_services:
                        continue
                    for entry in _process_registry:
                        name = entry['process_name']
                        handler = entry['handler']
                        if name.lower() == svc_name.lower():
                            if handler.process and handler.process.poll() is None:
                                desc = f"{name} w/ {entry['key_type']}" if entry['key_type'] else name
                                logger.info(f"[reload] Stopping {desc}")
                                handler.stop_process(name, entry['key_type'])
                            start_entries.append(entry)

            # Re-run setup functions to regenerate config files before restart
            if 'zurg' in process_services:
                try:
                    from zurg.setup import zurg_setup
                    logger.info("[reload] Regenerating zurg config")
                    zurg_setup()
                except Exception as e:
                    logger.error(f"[reload] Failed to regenerate zurg config: {e}")

            if 'rclone' in process_services:
                try:
                    from rclone.rclone import regenerate_config
                    logger.info("[reload] Regenerating rclone config")
                    regenerate_config()
                except Exception as e:
                    logger.error(f"[reload] Failed to regenerate rclone config: {e}")

            # Rewrite the plex_debrid Trakt .env if credentials changed
            if 'plex_debrid' in process_services and changed & {'TRAKT_CLIENT_ID', 'TRAKT_CLIENT_SECRET'}:
                try:
                    client_id = os.environ.get('TRAKT_CLIENT_ID', '')
                    client_secret = os.environ.get('TRAKT_CLIENT_SECRET', '')
                    if not (client_id and client_secret):
                        client_id = '0183a05ad97098d87287fe46da4ae286f434f32e8e951caad4cc147c947d79a3'
                        client_secret = '87109ed53fe1b4d6b0239e671f36cd2f17378384fa1ae09888a32643f83b7e6c'
                    env_path = './.env'
                    with open(env_path, 'w') as f:
                        f.write(f'CLIENT_ID={client_id}\n')
                        f.write(f'CLIENT_SECRET={client_secret}\n')
                    logger.info("[reload] Rewrote plex_debrid Trakt .env")
                except Exception as e:
                    logger.error(f"[reload] Failed to rewrite Trakt .env: {e}")

            # Re-check shutdown before starting new processes
            if _proc_mod._shutting_down:
                logger.info("[reload] Aborting restart — shutdown in progress")
                return

            # Start affected services (forward dependency order)
            for svc_name in reversed(stop_order):
                if _proc_mod._shutting_down:
                    logger.info("[reload] Aborting restart — shutdown in progress")
                    return
                if svc_name not in process_services:
                    continue
                for entry in start_entries:
                    name = entry['process_name']
                    handler = entry['handler']
                    if name.lower() == svc_name.lower():
                        desc = f"{name} w/ {entry['key_type']}" if entry['key_type'] else name
                        logger.info(f"[reload] Starting {desc}")
                        handler.restart_process()

        # Handle non-process services
        if 'notifications' in services:
            try:
                from utils.notifications import init
                init()
                logger.info("[reload] Notifications reinitialized")
            except Exception as e:
                logger.error(f"[reload] Failed to reinitialize notifications: {e}")

        if 'blackhole' in services:
            try:
                from utils import blackhole
                blackhole.stop()
                blackhole.setup()
                logger.info("[reload] Blackhole watcher restarted")
            except Exception as e:
                logger.error(f"[reload] Failed to restart blackhole: {e}")

        if 'status_ui' in services:
            try:
                from utils.status_server import StatusHandler
                auth = os.environ.get('STATUS_UI_AUTH')
                StatusHandler.auth_credentials = auth if auth and ':' in auth else None
                logger.info("[reload] Status UI auth credentials updated")
            except Exception as e:
                logger.error(f"[reload] Failed to update Status UI auth: {e}")

        logger.info("[reload] Config reload complete")
        _notify_reload(changed, services)

        try:
            from utils.status_server import status_data
            status_data.add_event(
                'config_reload',
                f'Reloaded {len(changed)} var(s), restarted: {", ".join(sorted(services)) or "none"}'
            )
        except Exception:
            pass

    except Exception as e:
        logger.error(f"[reload] Reload failed: {e}")
    finally:
        _reload_lock.release()


def _notify_reload(changed, services):
    """Send notification about config reload."""
    try:
        from utils.notifications import notify
        body = f'Reloaded {len(changed)} variable(s)'
        if services:
            body += f', restarted: {", ".join(sorted(services))}'
        notify('startup', 'Config Reloaded', body)
    except Exception:
        pass


def handle_sighup(signum, frame):
    """SIGHUP handler — dispatch reload to a separate thread.

    Signal handlers should be fast and not block. The actual reload
    work (which may involve stopping/starting processes) runs in
    a background thread.
    """
    logger.info("[reload] SIGHUP received — reloading configuration")
    t = threading.Thread(target=_do_reload, daemon=True)
    t.start()
