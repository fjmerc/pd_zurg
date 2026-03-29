"""Minimal WebDAV PROPFIND client for querying Zurg directly.

Uses urllib only (no external dependencies) consistent with the rest of
the codebase.  Sends PROPFIND requests and parses the multistatus XML
response to produce directory listings without going through FUSE/rclone.
"""

import base64
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from urllib.parse import unquote

from utils.logger import get_logger

logger = get_logger()

_DAV_NS = 'DAV:'
_PROPFIND_BODY = (
    b'<?xml version="1.0" encoding="utf-8"?>'
    b'<D:propfind xmlns:D="DAV:">'
    b'<D:prop><D:resourcetype/><D:getcontentlength/></D:prop>'
    b'</D:propfind>'
)


def propfind(url, depth=1, auth=None, timeout=30):
    """Send a PROPFIND request and return parsed entries.

    Args:
        url: WebDAV URL (e.g. http://localhost:9001/dav/shows/)
        depth: PROPFIND depth — 0, 1, or 'infinity'
        auth: Optional (username, password) tuple for Basic Auth
        timeout: Socket timeout in seconds

    Returns:
        List of dicts: [{href, name, is_collection, size}, ...]

    Raises:
        urllib.error.URLError, urllib.error.HTTPError, ET.ParseError
    """
    req = urllib.request.Request(url, data=_PROPFIND_BODY, method='PROPFIND')
    req.add_header('Content-Type', 'application/xml; charset=utf-8')
    req.add_header('Depth', str(depth))
    if auth:
        creds = base64.b64encode(f'{auth[0]}:{auth[1]}'.encode()).decode()
        req.add_header('Authorization', f'Basic {creds}')

    with urllib.request.urlopen(req, timeout=timeout) as resp:
        xml_bytes = resp.read()
    return _parse_multistatus(xml_bytes)


def _parse_multistatus(xml_bytes):
    """Parse a WebDAV multistatus XML response into a flat entry list."""
    root = ET.fromstring(xml_bytes)
    entries = []
    for response in root.findall(f'{{{_DAV_NS}}}response'):
        href_raw = response.findtext(f'{{{_DAV_NS}}}href', '')
        href = unquote(href_raw)

        is_collection = False
        size = 0
        for propstat in response.findall(f'{{{_DAV_NS}}}propstat'):
            prop = propstat.find(f'{{{_DAV_NS}}}prop')
            if prop is None:
                continue
            rt = prop.find(f'{{{_DAV_NS}}}resourcetype')
            if rt is not None and rt.find(f'{{{_DAV_NS}}}collection') is not None:
                is_collection = True
            cl = prop.findtext(f'{{{_DAV_NS}}}getcontentlength', '')
            if cl:
                try:
                    size = int(cl)
                except (ValueError, TypeError):
                    pass

        # Derive a clean name from the href (last path segment)
        path = href.rstrip('/')
        name = path.rsplit('/', 1)[-1] if '/' in path else path

        entries.append({
            'href': href,
            'name': name,
            'is_collection': is_collection,
            'size': size,
        })
    return entries
