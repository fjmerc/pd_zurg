from base import *
from utils.logger import *
from plexapi.server import PlexServer
from plexapi import exceptions as plexapi_exceptions
from requests.exceptions import HTTPError


logger = get_logger()

max_retry_attempts = 5
retry_interval = 10


def delete_media_with_retry(media):
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


def _find_duplicates(duplicates, section_type, libtype):
    """Find duplicate items that have both rclone-mounted and local copies.

    Returns a list of (item, label, rclone_media_ids, local_media_ids) tuples.
    Each entry represents a Plex item with at least one copy on the rclone mount
    and at least one copy on local storage.
    """
    from base import config
    rclonemn = config.RCLONEMN
    results = []

    for item in duplicates:
        rclone_media_ids = []
        local_media_ids = []

        for media in item.media:
            is_rclone = False
            is_local = False
            for part in media.parts:
                if re.search(f"/{rclonemn}[0-9a-zA-Z_]*?/", part.file):
                    is_rclone = True
                else:
                    is_local = True

            if is_rclone and not is_local:
                rclone_media_ids.append(media.id)
            elif is_local and not is_rclone:
                local_media_ids.append(media.id)
            # Mixed (parts on both) — skip, ambiguous

        if rclone_media_ids and local_media_ids:
            label = _get_item_label(item, section_type)
            results.append((item, label, rclone_media_ids, local_media_ids))

    return results


def _process_library(plex_server, section_type, libtype):
    from base import config
    keep_mode = (config.DUPECLEANKEEP or 'local').lower()

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
    found = _find_duplicates(duplicates, section_type, libtype)

    if not found:
        logger.info(f"No duplicate {libtype}s found.")
        return

    if keep_mode == 'zurg':
        # Delete local copies, keep Zurg — file deletion works on local storage
        delete_ids = []
        for item, label, rclone_media_ids, local_media_ids in found:
            for mid in local_media_ids:
                logger.info(f"Duplicate {libtype} found: {label} — keeping Zurg copy, deleting local (Media ID: {mid})")
                delete_ids.append((item, mid, label))

        if delete_ids:
            logger.info(f"Number of local {libtype} copies to delete: {len(delete_ids)}")

        for item, media_id, label in delete_ids:
            for media in item.media:
                if media.id == media_id:
                    logger.info(f"Deleting local {libtype}: {label} (Media ID: {media_id})")
                    continue_execution = delete_media_with_retry(media)
                    if not continue_execution:
                        break
                    break  # found the matching media, no need to keep iterating
    else:
        # Default (keep_mode == 'local'): detect but skip Zurg copies
        # Zurg's WebDAV is read-only — calling media.delete() triggers Plex
        # to attempt file removal via rclone, which returns 501. The file
        # stays on the mount and Plex rediscovers it on next scan, creating
        # an endless delete-rediscover loop that floods logs.
        for item, label, rclone_media_ids, local_media_ids in found:
            logger.info(
                f"Duplicate {libtype} found: {label} — "
                f"{len(rclone_media_ids)} Zurg copy(s), {len(local_media_ids)} local copy(s). "
                f"Skipping: Zurg mount is read-only, cannot delete files. "
                f"Set DUPLICATE_CLEANUP_KEEP=zurg to delete local copies instead."
            )


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
    while True:
        start_cleanup()
        # Re-read interval each cycle so WebUI changes take effect
        from base import config
        interval_hours = float(config.CLEANUPINT) if config.CLEANUPINT else 24
        interval_seconds = int(interval_hours * 3600)
        logger.debug(f"Next duplicate cleanup in {interval_hours}h")
        time.sleep(interval_seconds)

def start_cleanup():
    logger.info("Starting duplicate cleanup")
    start_time = get_start_time()
    try:
        # Read fresh values from config singleton (module globals may be stale after reload)
        from base import config
        plex_server = PlexServer(config.PLEXADD, config.PLEXTOKEN)
        process_duplicates(plex_server, "show", "episode")
        process_duplicates(plex_server, "movie", "movie")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error creating Plex server connection: {e}")
    except Exception as e:
        logger.error(f"Error creating Plex server connection: {e}")
    total_time = time_to_complete(start_time)
    logger.info("Duplicate cleanup complete.")
    logger.info(f"Total time required: {total_time}")
    from utils.notifications import notify
    notify('library_refresh', 'Duplicate Cleanup Complete', f'Cleaned up in {total_time}')

def cleanup_thread():
    thread = threading.Thread(target=cleanup_schedule)
    thread.daemon = True
    thread.start()
