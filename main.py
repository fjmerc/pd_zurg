from base import *
from utils.logger import *
from version import VERSION
import plex_debrid_ as p
import zurg as z
from rclone import rclone
from utils import duplicate_cleanup
from utils import auto_update
from utils.processes import shutdown_all_processes, start_process_monitor
from utils import notifications
from utils import history
from utils import blocklist
from utils import blackhole
from utils import ffprobe_monitor
from utils import status_server
from utils.config_validator import run_validation
from utils.config_reload import handle_sighup
from utils.task_scheduler import scheduler


def shutdown(signum, frame):
    logger = get_logger()
    logger.info("Shutdown signal received. Cleaning up...")

    scheduler.stop()
    shutdown_all_processes(logger)

    for mount_point in os.listdir('/data'):
        full_path = os.path.join('/data', mount_point)
        if os.path.ismount(full_path):
            logger.info(f"Unmounting {full_path}...")
            umount = subprocess.run(['umount', full_path], capture_output=True, text=True)
            if umount.returncode == 0:
                logger.info(f"Successfully unmounted {full_path}")
            else:
                logger.error(f"Failed to unmount {full_path}: {umount.stderr.strip()}")

    # Best-effort shutdown notification after critical cleanup
    t = threading.Thread(target=notifications.notify,
                         args=('shutdown', 'pd_zurg Shutting Down', 'Shutdown complete'))
    t.daemon = True
    t.start()
    t.join(timeout=5)

    sys.exit(0)

def main():
    logger = get_logger()

    version = VERSION

    banner = (
        "\n"
        "============================================================\n"
        f"  pd_zurg v{version}\n"
        "============================================================\n"
    )

    logger.info(banner)

    if not run_validation():
        sys.exit(1)

    status_server.setup()
    status_server.status_data.add_event('main', f'pd_zurg v{version} starting')

    history.init()
    blocklist.init()
    notifications.init()
    notifications.notify('startup', 'pd_zurg Started', f'Version {version}')

    if str(ZURG).lower() == 'true':
        if not (RDAPIKEY or ADAPIKEY):
            raise MissingAPIKeyException()

        try:
            z.setup.zurg_setup()
            z_updater = z.update.ZurgUpdate()
            z_updater.auto_update('Zurg', bool(ZURGUPDATE))
        except Exception as e:
            logger.error(f"Error in Zurg setup: {e}", exc_info=True)

        if RCLONEMN:
            try:
                if DUPECLEAN:
                    duplicate_cleanup.setup()
                rclone.setup()
            except Exception as e:
                logger.error(f"Error in rclone/cleanup setup: {e}", exc_info=True)

    if str(PLEXDEBRID).lower() == 'true':
        try:
            p.setup.pd_setup()
            pd_updater = p.update.PlexDebridUpdate()
            if PDUPDATE and PDREPO:
                pd_updater.auto_update('plex_debrid', True)
            elif PDREPO:
                p.download.get_latest_release()
                pd_updater.auto_update('plex_debrid', False)
            else:
                pd_updater.auto_update('plex_debrid', False)
        except Exception as e:
            logger.error(f"Error in plex_debrid setup: {e}", exc_info=True)

    blackhole.setup()

    try:
        ffprobe_monitor.setup()
    except Exception as e:
        logger.error(f"Error in ffprobe monitor setup: {e}", exc_info=True)

    start_process_monitor(logger)

    # Watch settings.json for changes from the plex_debrid interactive menu
    # or manual edits, and sync them back to .env
    try:
        from utils import settings_watcher
        settings_watcher.start()
    except Exception as e:
        logger.error(f"Error starting settings watcher: {e}", exc_info=True)

    # Start the centralized task scheduler (tasks registered during setup above)
    try:
        from utils import scheduled_tasks
        scheduled_tasks.register_all()
        scheduler.start()
    except Exception as e:
        logger.error(f"Error starting task scheduler: {e}", exc_info=True)

    while True:
        signal.pause()

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGHUP, handle_sighup)
    # Auto-reap zombie children without a handler that conflicts with subprocess.Popen
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    main()
