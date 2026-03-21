"""Network utility functions."""

import time
import requests
from utils.logger import get_logger

logger = get_logger()


def wait_for_url(url, endpoint="/", auth=None, timeout=600, description="service"):
    """Wait for a URL to become accessible with exponential backoff.

    Args:
        url: Base URL to check (e.g., 'http://localhost:9999')
        endpoint: Path to append to url (e.g., '/dav/')
        auth: Optional (username, password) tuple for basic auth
        timeout: Maximum seconds to wait (default: 600)
        description: Human-readable name for log messages

    Returns:
        True if the URL became accessible, False on timeout.
    """
    start_time = time.time()
    full_url = f"{url}{endpoint}"
    logger.info(f"Waiting for {description} at {full_url} to become accessible...")

    delay = 5
    max_delay = 60

    while time.time() - start_time < timeout:
        try:
            kwargs = {'timeout': 10}
            if auth:
                kwargs['auth'] = auth
            response = requests.get(full_url, **kwargs)

            if 200 <= response.status_code < 300:
                logger.debug(f"{description} at {full_url} is accessible (status {response.status_code})")
                return True
            else:
                logger.debug(f"Received status {response.status_code} from {full_url}")
        except requests.ConnectionError:
            logger.debug(f"Connection refused for {full_url}, retrying in {delay}s...")
        except requests.RequestException as e:
            logger.debug(f"Request error for {full_url}: {e}")

        time.sleep(delay)
        delay = min(delay * 2, max_delay)

    logger.error(f"Timeout: {description} at {full_url} not accessible after {timeout}s")
    return False
