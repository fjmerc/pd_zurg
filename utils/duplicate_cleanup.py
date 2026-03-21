from base import *
from utils.logger import *
from plexapi.server import PlexServer
from plexapi import exceptions as plexapi_exceptions
from requests.exceptions import HTTPError


logger = get_logger()

max_retry_attempts = 5
retry_interval = 10

def delete_media_with_retry(media):
    #logger = get_logger(log_name='duplicate_cleanup')
    retry_attempt = 0
    continue_execution = True

    while retry_attempt < max_retry_attempts:
        try:
            media.delete()
            break
        except requests.exceptions.ReadTimeout:
            retry_attempt += 1
            logger.warning(f"Read timeout occurred. Retrying delete operation (Attempt {retry_attempt})...")
            time.sleep(retry_interval)
        except plexapi_exceptions.NotFound as e:
            logger.warning(f"404 Not Found error occurred. Skipping delete operation for media ID: {media.id}")
            continue_execution = False
            break
    else:
        logger.error(f"Max retry attempts reached. Unable to delete media ID: {media.id}")

    return continue_execution

def _get_item_label(item, section_type):
    if section_type == "show":
        return f"Show: {item.show().title} - Episode: {item.title}"
    return item.title


def _process_library(plex_server, section_type, libtype):
    section = None
    for s in plex_server.library.sections():
        if s.type == section_type:
            section = s
            break

    if section is None:
        logger.error(f"{section_type.capitalize()} library section not found.")
        return

    logger.info(f"{section_type.capitalize()} library section: {section.title}")
    duplicates = section.search(duplicate=True, libtype=libtype)
    items_to_delete = []

    for item in duplicates:
        has_RCLONEMN = False
        has_other_directory = False
        media_id = ""
        for media in item.media:
            for part in media.parts:
                if re.search(f"/{RCLONEMN}[0-9a-zA-Z_]*?/", part.file):
                    has_RCLONEMN = True
                    media_id = media.id
                else:
                    has_other_directory = True
            if has_RCLONEMN and has_other_directory:
                label = _get_item_label(item, section_type)
                for part in media.parts:
                    logger.info(f"Duplicate {libtype} found: {label} (Media ID: {media_id})")
                    items_to_delete.append((item, media_id))

    if items_to_delete:
        logger.info(f"Number of {libtype}s to delete: {len(items_to_delete)}")
    else:
        logger.info(f"No duplicate {libtype}s found.")

    for item, media_id in items_to_delete:
        for media in item.media:
            if media.id == media_id:
                label = _get_item_label(item, section_type)
                for part in media.parts:
                    logger.info(f"Deleting {libtype} from Rclone directory: {label} (Media ID: {media_id})")
                    continue_execution = delete_media_with_retry(media)
                    if not continue_execution:
                        break
                if not continue_execution:
                    break


def process_duplicates(plex_server, section_type, libtype):
    try:
        _process_library(plex_server, section_type, libtype)
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error while processing {section_type} library: {e}")
    except Exception as e:
        logger.error(f"Error while processing {section_type} library: {e}")

def setup():
    try:
        app_env_variables = {
            "PLEX_ADDRESS": PLEXADD,
            "PLEX_TOKEN": PLEXTOKEN,
            "RCLONE_MOUNT_NAME": RCLONEMN
        }

        logger.info("Checking required duplicate cleanup environment variables.")
        for var_name, value in app_env_variables.items():
            if value is None:
                logger.error(f"Application environment variable '{var_name}' is not set.")
            else:
                logger.debug(f"Application environment variable '{var_name}' is set.")

        if all(app_env_variables.values()):
            if DUPECLEAN is not None and cleanup_interval() == 24:
                logger.info("Duplicate cleanup interval missing")
                logger.info("Defaulting to " + format_time(cleanup_interval()))
                cleanup_thread()
            elif DUPECLEAN is not None:
                logger.info("Duplicate cleanup interval set to " + format_time(cleanup_interval()))
                cleanup_thread()
    except Exception as e:
        logger.error(e)

def cleanup_interval():
    if CLEANUPINT is None:
        interval = 24
    else:
        interval = float(CLEANUPINT)
    return interval

def cleanup_schedule():
    time.sleep(60)
    interval = cleanup_interval()
    interval_minutes = int(interval * 60)
    schedule.every(interval_minutes).minutes.do(start_cleanup)
    while True:
        schedule.run_pending()
        time.sleep(1)

def start_cleanup():
    logger.info("Starting duplicate cleanup")
    start_time = get_start_time()
    try:
        plex_server = PlexServer(PLEXADD, PLEXTOKEN)
        process_duplicates(plex_server, "show", "episode")
        process_duplicates(plex_server, "movie", "movie")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error creating Plex server connection: {e}")
    except Exception as e:
        logger.error(f"Error creating Plex server connection: {e}")
    total_time = time_to_complete(start_time)
    logger.info("Duplicate cleanup complete.")
    logger.info(f"Total time required: {total_time}")

def cleanup_thread():
    thread = threading.Thread(target=cleanup_schedule)
    thread.daemon = True
    thread.start()