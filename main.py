from base import *
from utils.logger import *
import plex_debrid_ as p
import zurg as z
from rclone import rclone
from utils import duplicate_cleanup
from utils import auto_update
from utils.processes import shutdown_all_processes
from utils import notifications


def shutdown(signum, frame):
    logger = get_logger()
    logger.info("Shutdown signal received. Cleaning up...")
    notifications.notify('shutdown', 'pd_zurg Shutting Down', 'Received shutdown signal')

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

    sys.exit(0)

def main():
    logger = get_logger()

    version = '2.9.2'

    ascii_art = f'''

 _______  ______       _______           _______  _______
(  ____ )(  __  \\     / ___   )|\\     /|(  ____ )(  ____ \\
| (    )|| (  \\  )    \\/   )  || )   ( || (    )|| (    \\/
| (____)|| |   ) |        /   )| |   | || (____)|| |
|  _____)| |   | |       /   / | |   | ||     __)| | ____
| (      | |   ) |      /   /  | |   | || (\\ (   | | \\_  )
| )      | (__/  )     /   (_/\\| (___) || ) \\ \\__| (___) |
|/       (______/_____(_______/(_______)|/   \\__/(_______)
                (_____)
                        Version: {version}
'''

    logger.info(ascii_art.format(version=version)  + "\n" + "\n")

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

    while True:
        signal.pause()

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    main()
