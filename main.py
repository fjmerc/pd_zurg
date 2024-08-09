from base import *
from utils.logger import *
import plex_debrid_ as p
import zurg as z 
from rclone import rclone
from utils import duplicate_cleanup
from utils import auto_update


def shutdown(signum, frame):
    logger = get_logger()
    logger.info("Shutdown signal received. Cleaning up...")

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

    version = '2.8.0'

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

    def healthcheck():
        while True:
            time.sleep(10)
            try:
                result = subprocess.run(['python', 'healthcheck.py'], capture_output=True, text=True) 
                if result.stderr:
                    logger.error(result.stderr.strip())
            except Exception as e:
                logger.error('Error running healthcheck.py: %s', e)
            time.sleep(50)
    thread = threading.Thread(target=healthcheck)
    thread.daemon = True
    thread.start()
       
    try:
        if ZURG is None or str(ZURG).lower() == 'false':
            pass
        elif str(ZURG).lower() == 'true':
            try:
                if RDAPIKEY or ADAPIKEY:
                    try:
                        z.setup.zurg_setup() 
                        z_updater = z.update.ZurgUpdate()
                        if ZURGUPDATE:
                            z_updater.auto_update('Zurg',True)
                        else:
                            z_updater.auto_update('Zurg',False)
                    except Exception as e:
                        raise Exception(f"Error in Zurg setup: {e}")
                    try:    
                        if RCLONEMN:
                            try:
                                if not DUPECLEAN:
                                    pass
                                elif DUPECLEAN:
                                    duplicate_cleanup.setup()
                                rclone.setup()      
                            except Exception as e:
                                logger.error(e)                         
                    except Exception as e:
                        raise Exception(f"Error in setup: {e}")                          
                else:
                    raise MissingAPIKeyException()
            except Exception as e:
                logger.error(e)                    
    except Exception as e:
        logger.error(e)
        
    try:
        if PLEXDEBRID is None or str(PLEXDEBRID).lower() == 'false':
            pass
        elif str(PLEXDEBRID).lower() == 'true':
            try:
                p.setup.pd_setup()
                pd_updater = p.update.PlexDebridUpdate()
                if PDUPDATE and PDREPO:
                    pd_updater.auto_update('plex_debrid',True)
                    pass
                elif PDREPO:
                    p.download.get_latest_release()
                    pd_updater.auto_update('plex_debrid',False)
                else:
                    pd_updater.auto_update('plex_debrid',False)
            except Exception as e:
                logger.error(f"An error occurred in the plex_debrid setup: {e}")
    except:
        pass
    def perpetual_wait():
        stop_event = threading.Event()
        stop_event.wait()
    perpetual_wait()    
if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    
    main()