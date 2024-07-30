from base import *
from utils.logger import *
from utils.download import Downloader

logger = get_logger()
downloader = Downloader()


def parse_repo_info(repo_info):
    repo_info = os.getenv('PD_REPO')
    return downloader.parse_repo_info(repo_info=repo_info)

def get_latest_release():
    try:
        try:
            repo_info = os.getenv('PD_REPO')
            username, repository, branch = parse_repo_info(repo_info=repo_info)
        except ValueError as e:
            logger.error(str(e))
            return False, str(e)

        repo_url = f"https://github.com/{username}/{repository}/archive/refs/heads/{branch}.zip"
        logger.debug(f"Repository URL for plex_debrid download: {repo_url}")  

        target = './plex_debrid'
        zip_folder_name = f'{repository}-{branch.replace("/", "-")}'
        success, error = downloader.download_and_extract(repo_url, target, zip_folder_name)
        if not success:
            logger.error(f"Error downloading latest plex_debrid release: {error}")
            return False, error
        
        return True, None
    except Exception as e:
        logger.error(f"Error downloading latest plex_debrid release: {e}")
        return False, str(e)

