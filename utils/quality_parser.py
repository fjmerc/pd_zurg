"""Parse video quality attributes from media filenames.

Extracts resolution, source, codec, and HDR information using regex patterns
based on common scene naming conventions. Covers 95%+ of real-world filenames
without requiring ffprobe.
"""

import re

# -- Resolution patterns ------------------------------------------------
# Order matters: check 4K/UHD before generic numbers
_RESOLUTION_PATTERNS = [
    (re.compile(r'(?<![a-zA-Z\d])(?:2160p|4K|UHD)(?![a-zA-Z\d])', re.IGNORECASE), '2160p'),
    (re.compile(r'(?<![a-zA-Z\d])1080[pi](?![a-zA-Z\d])', re.IGNORECASE), '1080p'),
    (re.compile(r'(?<![a-zA-Z\d])720p(?![a-zA-Z\d])', re.IGNORECASE), '720p'),
    (re.compile(r'(?<![a-zA-Z\d])480p(?![a-zA-Z\d])', re.IGNORECASE), '480p'),
    (re.compile(r'(?<![a-zA-Z\d])(?:576p|576i)(?![a-zA-Z\d])', re.IGNORECASE), '480p'),
]

# -- Source patterns ----------------------------------------------------
# Remux before BluRay; WEB-DL before WEB; specific before generic
_SOURCE_PATTERNS = [
    (re.compile(r'(?<![a-zA-Z\d])(?:BD[\s.\-]?Remux|BDRemux|REMUX)(?![a-zA-Z\d])', re.IGNORECASE), 'Remux'),
    (re.compile(r'(?<![a-zA-Z\d])(?:WEB[\s.\-]?DL|WEBDL)(?![a-zA-Z\d])', re.IGNORECASE), 'WEB-DL'),
    (re.compile(r'(?<![a-zA-Z\d])WEBRip(?![a-zA-Z\d])', re.IGNORECASE), 'WEBRip'),
    (re.compile(r'(?<![a-zA-Z\d])(?:Blu[\s.\-]?Ray|BDRip|BRRip|BDMV)(?![a-zA-Z\d])', re.IGNORECASE), 'BluRay'),
    (re.compile(r'(?<![a-zA-Z\d])HDTV(?![a-zA-Z\d])', re.IGNORECASE), 'HDTV'),
    (re.compile(r'(?<![a-zA-Z\d])(?:DVDRip|DVD(?:[\s.\-]?R)?)(?![a-zA-Z\d])', re.IGNORECASE), 'DVDRip'),
    (re.compile(r'(?<![a-zA-Z\d])HDRip(?![a-zA-Z\d])', re.IGNORECASE), 'HDRip'),
    # Generic "WEB" last — only if no more specific WEB source was found
    (re.compile(r'(?<![a-zA-Z\d])WEB(?![a-zA-Z\d\-])', re.IGNORECASE), 'WEB-DL'),
]

# -- Codec patterns -----------------------------------------------------
_CODEC_PATTERNS = [
    (re.compile(r'(?<![a-zA-Z\d])(?:x\.?265|[Hh]\.?265|HEVC)(?![a-zA-Z\d])', re.IGNORECASE), 'x265'),
    (re.compile(r'(?<![a-zA-Z\d])(?:x\.?264|[Hh]\.?264|AVC)(?![a-zA-Z\d])', re.IGNORECASE), 'x264'),
    (re.compile(r'(?<![a-zA-Z\d])AV1(?![a-zA-Z\d])', re.IGNORECASE), 'AV1'),
]

# -- HDR patterns -------------------------------------------------------
# HDR10+ before HDR10 before HDR; DV/DoVi before generic
_HDR_PATTERNS = [
    (re.compile(r'(?<![a-zA-Z\d])(?:DV|DoVi|Dolby[\s.\-]?Vision)(?![a-zA-Z\d])', re.IGNORECASE), 'DV'),
    (re.compile(r'(?<![a-zA-Z\d])HDR10\+(?![a-zA-Z\d])', re.IGNORECASE), 'HDR10+'),
    (re.compile(r'(?<![a-zA-Z\d])HDR10(?![a-zA-Z\d+])', re.IGNORECASE), 'HDR10'),
    (re.compile(r'(?<![a-zA-Z\d])HDR(?![a-zA-Z\d])', re.IGNORECASE), 'HDR'),
]


def parse_quality(filename):
    """Parse quality attributes from a media filename.

    Args:
        filename: Media filename (e.g., "Show.S01E01.1080p.WEB-DL.x265-GROUP.mkv")

    Returns:
        dict with keys:
            resolution: '2160p' | '1080p' | '720p' | '480p' | None
            source: 'WEB-DL' | 'WEBRip' | 'BluRay' | 'Remux' | 'HDTV' | 'DVDRip' | 'HDRip' | None
            codec: 'x265' | 'x264' | 'AV1' | None
            hdr: 'HDR' | 'HDR10+' | 'HDR10' | 'DV' | None
            label: Human-readable combined label (e.g., "WEB-DL 1080p") or None
    """
    if not filename:
        return {'resolution': None, 'source': None, 'codec': None, 'hdr': None, 'label': None}
    resolution = _first_match(filename, _RESOLUTION_PATTERNS)
    source = _first_match(filename, _SOURCE_PATTERNS)
    codec = _first_match(filename, _CODEC_PATTERNS)
    hdr = _first_match(filename, _HDR_PATTERNS)

    # Build human-readable label
    parts = []
    if source:
        parts.append(source)
    if resolution:
        parts.append(resolution)
    if hdr:
        parts.append(hdr)
    label = ' '.join(parts) if parts else None

    return {
        'resolution': resolution,
        'source': source,
        'codec': codec,
        'hdr': hdr,
        'label': label,
    }


def _first_match(text, patterns):
    """Return the value for the first matching pattern, or None."""
    for pattern, value in patterns:
        if pattern.search(text):
            return value
    return None
