"""Tests for plan 33 Phase 6 — observability for quality-compromise grabs.

Covers:
  * ``compromise_grabbed`` notifications honour ``NOTIFICATION_EVENTS``
    filtering (user opts out → apprise never receives the dispatch).
  * ``GET /api/blackhole/compromises`` returns structured compromise
    events in the documented shape: newest-first, limit capped, other
    event types filtered out, structured fields lifted from ``meta``.
"""

import json
import os
import socket
import sys
import threading
import time
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils import history, notifications
from utils.status_server import StatusHandler


# ---------------------------------------------------------------------------
# Notification filter test
# ---------------------------------------------------------------------------

class _FakeApprise:
    """Stand-in for apprise.Apprise(): records notify() calls without
    touching the network. The real ``apprise`` package may or may not be
    installed in CI, so we monkeypatch the module's ``apprise`` reference
    to avoid depending on it for this test."""

    class NotifyType:
        INFO = 'info'
        WARNING = 'warning'
        FAILURE = 'failure'

    def __init__(self):
        self.urls = []
        self.calls = []

    def Apprise(self):
        # Return self so ``apprise.Apprise()`` in init() yields an object
        # whose add/notify we can observe.
        return self

    def add(self, url):
        self.urls.append(url)
        return True

    def notify(self, title, body, notify_type=None):
        self.calls.append({'title': title, 'body': body, 'notify_type': notify_type})


_UNSET = object()


@pytest.fixture
def _reset_notifications():
    """Snapshot + restore notifications module globals so tests don't
    leak state into each other or the rest of the suite.  We rely on
    ``monkeypatch`` to restore the ``apprise`` attribute it set
    (``raising=False``), so we only track the fields this module owns."""
    saved = (notifications._notifier, notifications._enabled_events,
             notifications._min_level, notifications._apprise_available)
    yield
    (notifications._notifier, notifications._enabled_events,
     notifications._min_level, notifications._apprise_available) = saved


def test_notification_compromise_grabbed_respects_user_prefs(
        monkeypatch, _reset_notifications):
    """User configures NOTIFICATION_EVENTS without ``compromise_grabbed``
    → emitting that event MUST NOT dispatch to apprise, even when the
    level matches."""
    fake = _FakeApprise()
    monkeypatch.setattr(notifications, 'apprise', fake, raising=False)
    monkeypatch.setattr(notifications, '_apprise_available', True)
    monkeypatch.setenv('NOTIFICATION_URL', 'json://example.invalid')
    # User explicitly opts into a subset that EXCLUDES compromise_grabbed.
    monkeypatch.setenv('NOTIFICATION_EVENTS', 'download_complete,download_error')
    monkeypatch.setenv('NOTIFICATION_LEVEL', 'info')

    # Clear any prior state so init() runs fresh.
    notifications._notifier = None
    notifications._enabled_events = None
    notifications.init()

    # Pre-flight: init wired the FakeApprise and registered the URL.
    assert notifications._notifier is fake
    assert 'compromise_grabbed' not in notifications._enabled_events

    notifications.notify('compromise_grabbed', 'Blackhole: Quality Compromise',
                         'Show.S01E01 grabbed 1080p (preferred 2160p)', level='info')

    assert fake.calls == [], (
        f"compromise_grabbed dispatched despite user opt-out: {fake.calls}"
    )

    # Sanity check: a subscribed event at the same level DOES dispatch,
    # proving the filter is what blocked compromise_grabbed and not a
    # broken fake.
    notifications.notify('download_complete', 'Blackhole',
                         'Other event', level='info')
    assert len(fake.calls) == 1
    assert fake.calls[0]['title'] == 'Blackhole'


# ---------------------------------------------------------------------------
# /api/blackhole/compromises endpoint test
# ---------------------------------------------------------------------------

@pytest.fixture
def history_in_tmp(tmp_dir):
    """Init the history module against a fresh tmp path for the duration
    of the test; reset back to None on teardown so other tests start
    clean."""
    history.init(tmp_dir)
    yield tmp_dir
    history._file_path = None


@pytest.fixture
def status_server():
    """Spin up a StatusHandler on a random localhost port with auth
    disabled so tests can hit it directly.  Returns the base URL."""
    StatusHandler.auth_credentials = None
    StatusHandler.status_data_ref = None

    # Pick a free port — letting the OS assign avoids collisions when
    # tests run in parallel.
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()

    server = ThreadingHTTPServer(('127.0.0.1', port), StatusHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{port}'
    finally:
        server.shutdown()
        server.server_close()


def _fetch_json(url):
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read().decode('utf-8'))


def test_api_blackhole_compromises_endpoint(history_in_tmp, status_server):
    """Fixture history with compromise_grabbed + unrelated events.
    The endpoint must return only compromise events, newest first, with
    structured fields lifted from event.meta, and honour the limit kwarg."""
    # Non-compromise events must be filtered out.
    history.log_event('grabbed', 'Other.Show.S01E01.torrent',
                      episode='S01E01', source='blackhole')
    history.log_event('failed', 'Some.Movie.torrent',
                      detail='timeout', source='blackhole')

    # Oldest compromise first so we can assert newest-first ordering.
    history.log_event(
        'compromise_grabbed', 'Show.Alpha.S01E01.torrent',
        episode='S01E01', source='blackhole',
        detail='Show.Alpha.S01E01.torrent: grabbed 1080p (preferred 2160p)',
        media_title='Show Alpha',
        meta={
            'preferred_tier': '2160p', 'grabbed_tier': '1080p',
            'reason': 'dwell_elapsed', 'strategy': 'tier_drop',
            'dwell_seconds': 3 * 86400,
            'cached_alts_at_preferred': 0,
            'uncached_alts_at_preferred': 5,
        },
    )
    # Ensure distinct ts ordering; history keys on the seconds-resolution
    # isoformat timestamp.
    time.sleep(1.1)
    history.log_event(
        'compromise_grabbed', 'Show.Beta.S02E04.torrent',
        episode='S02E04', source='blackhole',
        detail='Show.Beta.S02E04.torrent: grabbed 720p (preferred 1080p)',
        media_title='Show Beta',
        meta={
            'preferred_tier': '1080p', 'grabbed_tier': '720p',
            'reason': 'dwell_elapsed', 'strategy': 'tier_drop',
            'dwell_seconds': 5 * 86400,
            'cached_alts_at_preferred': 0,
            'uncached_alts_at_preferred': 2,
        },
    )

    payload = _fetch_json(status_server + '/api/blackhole/compromises')

    assert 'compromises' in payload, payload
    items = payload['compromises']

    # Non-compromise events (``grabbed``, ``failed``) must not appear.
    assert len(items) == 2
    for it in items:
        assert it['preferred_tier'] is not None
        assert it['strategy'] in ('tier_drop', 'season_pack')

    # Newest first (Show.Beta was logged last).
    assert items[0]['title'] == 'Show.Beta.S02E04.torrent'
    assert items[1]['title'] == 'Show.Alpha.S01E01.torrent'

    newest = items[0]
    assert newest['media_title'] == 'Show Beta'
    assert newest['episode'] == 'S02E04'
    assert newest['preferred_tier'] == '1080p'
    assert newest['grabbed_tier'] == '720p'
    assert newest['reason'] == 'dwell_elapsed'
    assert newest['strategy'] == 'tier_drop'
    assert newest['dwell_days'] == 5
    assert newest['cached_alts_at_preferred'] == 0
    assert newest['uncached_alts_at_preferred'] == 2
    # ISO8601 timestamp — history module writes timespec='seconds'.
    assert newest['compromised_at'] and newest['compromised_at'].endswith('+00:00')


def test_api_blackhole_compromises_endpoint_respects_limit(
        history_in_tmp, status_server):
    """?limit=N caps returned events; defaults to 50 but clamps to 200."""
    # Log 3 compromise events; request limit=2 and verify only the two
    # newest come back.
    for i in range(3):
        history.log_event(
            'compromise_grabbed', f'Show.{i}.torrent',
            source='blackhole', detail='test',
            meta={'preferred_tier': '2160p', 'grabbed_tier': '1080p',
                  'reason': 'dwell_elapsed', 'strategy': 'tier_drop',
                  'dwell_seconds': 3 * 86400},
        )
        time.sleep(1.1)

    payload = _fetch_json(status_server + '/api/blackhole/compromises?limit=2')
    assert len(payload['compromises']) == 2
    # Newest first → last logged is Show.2.
    assert payload['compromises'][0]['title'] == 'Show.2.torrent'


def test_api_blackhole_compromises_endpoint_empty(history_in_tmp, status_server):
    """Empty history → endpoint returns an empty list, not 500 or null."""
    payload = _fetch_json(status_server + '/api/blackhole/compromises')
    assert payload == {'compromises': []}


def test_api_blackhole_compromises_endpoint_season_pack_strategy(
        history_in_tmp, status_server):
    """Season-pack strategy (preferred == grabbed) surfaces correctly —
    distinguishing it from tier-drop is a first-class observability
    concern because users want to know WHY the feature fired."""
    history.log_event(
        'compromise_grabbed', 'Show.Delta.S01E01.torrent',
        episode='S01E01', source='blackhole',
        detail='Show.Delta.S01E01.torrent: grabbed 2160p (season pack)',
        media_title='Show Delta',
        meta={
            'preferred_tier': '2160p', 'grabbed_tier': '2160p',
            'reason': 'season_pack_before_tier_drop',
            'strategy': 'season_pack',
            'dwell_seconds': 3 * 86400,
            'cached_alts_at_preferred': 0,
            'uncached_alts_at_preferred': 6,
        },
    )

    payload = _fetch_json(status_server + '/api/blackhole/compromises')
    assert len(payload['compromises']) == 1
    it = payload['compromises'][0]
    assert it['strategy'] == 'season_pack'
    assert it['preferred_tier'] == it['grabbed_tier'] == '2160p'
    assert it['reason'] == 'season_pack_before_tier_drop'


def test_api_blackhole_compromises_endpoint_meta_missing(
        history_in_tmp, status_server):
    """Events without a ``meta`` field (corrupt JSONL, legacy writer,
    non-dict values) degrade gracefully — no 500, just null fields.
    Exercises the ``isinstance(meta, dict)`` guard in the endpoint."""
    # Legacy-shaped event: compromise_grabbed with no meta.
    history.log_event(
        'compromise_grabbed', 'Show.Legacy.torrent',
        source='blackhole', detail='legacy shape — no meta dict',
    )
    # Corrupt-shaped event: meta is a string, not a dict.  log_event
    # rejects non-dict meta at the schema level (falsy filter), so we
    # write this one directly to the JSONL to simulate a corrupt row.
    legacy_path = os.path.join(history_in_tmp, 'history.jsonl')
    with open(legacy_path, 'a') as f:
        f.write(json.dumps({
            'id': 'corrupt-1', 'ts': '2026-04-20T10:00:00+00:00',
            'type': 'compromise_grabbed', 'title': 'Show.Corrupt.torrent',
            'meta': 'not-a-dict',
        }) + '\n')

    payload = _fetch_json(status_server + '/api/blackhole/compromises')
    titles = {it['title'] for it in payload['compromises']}
    assert 'Show.Legacy.torrent' in titles
    assert 'Show.Corrupt.torrent' in titles
    for it in payload['compromises']:
        # Missing/invalid meta → structured fields are None, not crashes.
        assert it['preferred_tier'] is None
        assert it['grabbed_tier'] is None
        assert it['dwell_days'] is None
