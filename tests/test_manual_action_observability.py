"""Manual UI actions must be as observable as the scheduled enforcement
path performing identical operations (audit finding #9): history events +
notifications for destructive library changes, and the advertised
'arr_deleted' notification must actually fire."""
import re
from pathlib import Path

import pytest

STATUS_SERVER = Path(__file__).resolve().parent.parent / 'utils' / 'status_server.py'
ARR_CLIENT = Path(__file__).resolve().parent.parent / 'utils' / 'arr_client.py'


def _endpoint_block(source, path_literal):
    """Slice the handler block for one endpoint: from its `if self.path ==`
    line to the next `if self.path` line."""
    start = source.index(path_literal)
    rest = source[start + len(path_literal):]
    m = re.search(r"\n        if self\.path", rest)
    return rest[:m.start()] if m else rest


def test_arr_deleted_notification_fires():
    src = STATUS_SERVER.read_text()
    block = _endpoint_block(src, "'/api/library/delete'")
    assert "notify('arr_deleted'" in block, (
        "arr_deleted is advertised in Settings but never sent")


def test_emit_source_switch_helper_emits_both():
    src = STATUS_SERVER.read_text()
    start = src.index('def _emit_source_switch')
    rest = src[start:]
    m = re.search(r'\n(?:def |class )', rest[10:])
    helper = rest[:m.start() + 10] if m else rest
    assert "log_event('switched_source'" in helper
    assert "'cause': 'preference_source_switch'" in helper
    assert "notify('library_refresh'" in helper


@pytest.mark.parametrize('endpoint', [
    "'/api/library/remove-local'",
    "'/api/library/switch-to-debrid'",
    "'/api/library/remove-debrid/confirm'",
])
def test_manual_endpoints_log_and_notify(endpoint):
    src = STATUS_SERVER.read_text()
    block = _endpoint_block(src, endpoint)
    assert "_emit_source_switch(" in block, (
        f'{endpoint}: destructive action must emit via the shared helper')


def test_overseerr_requests_log_search_triggered():
    src = ARR_CLIENT.read_text()
    tv = src[src.index('def ensure_and_request_tv'):src.index('def ensure_and_request_movie')]
    movie = src[src.index('def ensure_and_request_movie'):]
    movie = movie[:re.search(r'\nclass |\ndef |\n# ---', movie[10:]).start() + 10] \
        if re.search(r'\nclass |\ndef |\n# ---', movie[10:]) else movie
    for name, body in (('ensure_and_request_tv', tv),
                       ('ensure_and_request_movie', movie)):
        assert "log_event('search_triggered'" in body, (
            f'{name}: Overseerr fallback logs nothing while Sonarr/Radarr '
            f'paths log search_triggered')
