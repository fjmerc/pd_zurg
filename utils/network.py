"""Network utility functions."""

import time
import requests
from utils.logger import get_logger

logger = get_logger()


def wait_for_url(url, endpoint="/", auth=None, timeout=600,
                 description="service", method="GET"):
    """Wait for a URL to become accessible with exponential backoff.

    Args:
        url: Base URL to check (e.g., 'http://localhost:9999')
        endpoint: Path to append to url (e.g., '/dav/')
        auth: Optional (username, password) tuple for basic auth
        timeout: Maximum seconds to wait (default: 600)
        description: Human-readable name for log messages
        method: HTTP verb (default 'GET').  Some WebDAV endpoints —
            notably TorBox at ``webdav.torbox.app`` — reject plain GET on
            the root with 401 but accept PROPFIND with 207, so pass
            ``method='PROPFIND'`` for those probes.  Any 2xx response
            (including 207 Multi-Status) is treated as success.

    Returns:
        True if the URL became accessible, False on timeout.
    """
    start_time = time.time()
    full_url = f"{url}{endpoint}"
    logger.info(f"Waiting for {description} at {full_url} ({method}) to become accessible...")

    delay = 5
    max_delay = 60
    # PROPFIND requires a Depth header; default 0 keeps the probe cheap
    # (no recursive listing).  Body is empty — TorBox accepts that.
    propfind_headers = {'Depth': '0'} if method.upper() == 'PROPFIND' else None

    while time.time() - start_time < timeout:
        try:
            kwargs = {'timeout': 10}
            if auth:
                kwargs['auth'] = auth
            if propfind_headers:
                kwargs['headers'] = propfind_headers
            response = requests.request(method, full_url, **kwargs)

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
