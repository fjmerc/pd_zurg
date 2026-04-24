"""Tests for utils/history.py — structured event history."""

import json
import os
import time
import pytest
from datetime import datetime, timezone, timedelta

from utils import history


@pytest.fixture(autouse=True)
def _reset_history_state():
    """Reset history module globals after each test."""
    yield
    history._file_path = None
    history._retention_days = 30


class TestLogEvent:
    """F5.4 — test_log_event: log 3 events, verify file has 3 lines, each valid JSON."""

    def test_log_event(self, tmp_dir):
        history.init(tmp_dir)
        history.log_event('grabbed', 'Show A', episode='S01E01', source='blackhole')
        history.log_event('cached', 'Show A', episode='S01E01', source='blackhole')
        history.log_event('failed', 'Movie B', detail='timeout', source='blackhole')

        fpath = os.path.join(tmp_dir, 'history.jsonl')
        with open(fpath, 'r') as f:
            lines = [l.strip() for l in f if l.strip()]

        assert len(lines) == 3
        for line in lines:
            event = json.loads(line)
            assert 'id' in event
            assert 'ts' in event
            assert 'type' in event
            assert 'title' in event

    def test_log_event_meta(self, tmp_dir):
        history.init(tmp_dir)
        history.log_event('grabbed', 'Movie X', meta={'provider': 'realdebrid', 'size_bytes': 1234})

        fpath = os.path.join(tmp_dir, 'history.jsonl')
        with open(fpath, 'r') as f:
            event = json.loads(f.readline())

        assert event['meta']['provider'] == 'realdebrid'
        assert event['meta']['size_bytes'] == 1234

    def test_log_event_noop_before_init(self):
        """log_event should be a no-op before init() is called."""
        history._file_path = None
        history.log_event('grabbed', 'Test')  # should not raise

    def test_log_event_returns_event_id(self, tmp_dir):
        """Callers chain events via the returned id (symlink → rescan)."""
        history.init(tmp_dir)
        ev_id = history.log_event(
            'symlink_created', 'Guardians of the Galaxy', source='library',
            meta={'cause': 'library_new_import', 'file': 'x.mkv'})
        assert ev_id
        assert isinstance(ev_id, str)

    def test_log_event_none_when_uninitialised(self):
        """Returning None (not raising) keeps callers simple before init."""
        history._file_path = None
        assert history.log_event('grabbed', 'Test') is None

    def test_legacy_event_without_meta_still_renders(self, tmp_dir):
        """Events written before the cause vocab must still format cleanly."""
        fpath = os.path.join(tmp_dir, 'history.jsonl')
        legacy = {'id': 'old', 'ts': '2026-04-17T10:22:49+00:00',
                  'type': 'symlink_created',
                  'title': 'Guardians of the Galaxy',
                  'detail': 'Debrid symlink(s) created in local library',
                  'source': 'library'}
        with open(fpath, 'w') as f:
            f.write(json.dumps(legacy) + '\n')

        history.init(tmp_dir)
        r = history.query()
        assert r['total'] == 1
        assert r['events'][0]['detail'] == 'Debrid symlink(s) created in local library'

        from utils.activity_format import format_event
        assert format_event(r['events'][0])['short'] == (
            'Debrid symlink(s) created in local library')


class TestQueryAll:
    """F5.4 — test_query_all: log 5 events, query all, verify order (newest first)."""

    def test_query_all(self, tmp_dir):
        history.init(tmp_dir)
        for i in range(5):
            history.log_event('grabbed', f'Show {i}', source='test')

        result = history.query()
        assert result['total'] == 5
        assert len(result['events']) == 5
        # Newest first — Show 4 should be first
        assert result['events'][0]['title'] == 'Show 4'
        assert result['events'][4]['title'] == 'Show 0'


class TestQueryByType:
    """F5.4 — test_query_by_type: log mixed types, filter by 'failed'."""

    def test_query_by_type(self, tmp_dir):
        history.init(tmp_dir)
        history.log_event('grabbed', 'A')
        history.log_event('failed', 'B', detail='error')
        history.log_event('grabbed', 'C')
        history.log_event('failed', 'D', detail='timeout')
        history.log_event('cached', 'E')

        result = history.query(type='failed')
        assert result['total'] == 2
        titles = [e['title'] for e in result['events']]
        assert 'B' in titles
        assert 'D' in titles
        assert 'A' not in titles


class TestQueryByShow:
    """F5.4 — test_query_by_show: log events for 2 shows, query one."""

    def test_query_by_show(self, tmp_dir):
        history.init(tmp_dir)
        history.log_event('grabbed', 'Breaking Bad', episode='S01E01')
        history.log_event('grabbed', 'The Office', episode='S02E03')
        history.log_event('cached', 'Breaking Bad', episode='S01E01')
        history.log_event('grabbed', 'The Office', episode='S02E04')

        result = history.query_by_show('Breaking Bad')
        assert len(result) == 2
        for e in result:
            assert e['title'] == 'Breaking Bad'

    def test_query_by_show_case_insensitive(self, tmp_dir):
        history.init(tmp_dir)
        history.log_event('grabbed', 'Breaking Bad', episode='S01E01')

        result = history.query_by_show('breaking bad')
        assert len(result) == 1

    def test_query_by_show_limit(self, tmp_dir):
        history.init(tmp_dir)
        for i in range(10):
            history.log_event('grabbed', 'TestShow', episode=f'S01E{i:02d}')

        result = history.query_by_show('TestShow', limit=3)
        assert len(result) == 3


class TestQueryPagination:
    """F5.4 — test_query_pagination: log 10 events, query page=2 limit=3."""

    def test_query_pagination(self, tmp_dir):
        history.init(tmp_dir)
        for i in range(10):
            history.log_event('grabbed', f'Item {i}')

        result = history.query(page=2, limit=3)
        assert result['total'] == 10
        assert result['page'] == 2
        assert result['pages'] == 4  # ceil(10/3)
        assert len(result['events']) == 3
        # Events are newest first: Item 9, 8, 7, 6, 5, 4, 3, 2, 1, 0
        # Page 2 with limit 3: Item 6, 5, 4
        assert result['events'][0]['title'] == 'Item 6'
        assert result['events'][2]['title'] == 'Item 4'


class TestRotation:
    """F5.4 — test_rotation: log events with old timestamps, call rotate()."""

    def test_rotation(self, tmp_dir):
        history.init(tmp_dir)
        history._retention_days = 7

        fpath = os.path.join(tmp_dir, 'history.jsonl')

        # Write events with old timestamps directly
        old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat(timespec='seconds')
        recent_ts = datetime.now(timezone.utc).isoformat(timespec='seconds')

        events = [
            {'id': '1', 'ts': old_ts, 'type': 'grabbed', 'title': 'Old Event'},
            {'id': '2', 'ts': old_ts, 'type': 'failed', 'title': 'Old Failure'},
            {'id': '3', 'ts': recent_ts, 'type': 'grabbed', 'title': 'Recent Event'},
        ]
        with open(fpath, 'w') as f:
            for e in events:
                f.write(json.dumps(e) + '\n')

        history.rotate()

        # Only the recent event should remain
        result = history.query()
        assert result['total'] == 1
        assert result['events'][0]['title'] == 'Recent Event'

    def test_rotation_noop_if_nothing_old(self, tmp_dir):
        history.init(tmp_dir)
        history._retention_days = 30

        history.log_event('grabbed', 'Fresh Event')
        history.rotate()

        result = history.query()
        assert result['total'] == 1


class TestClear:
    """Test history.clear() truncates file."""

    def test_clear(self, tmp_dir):
        history.init(tmp_dir)
        history.log_event('grabbed', 'A')
        history.log_event('grabbed', 'B')

        result = history.query()
        assert result['total'] == 2

        history.clear()

        result = history.query()
        assert result['total'] == 0
        assert result['pages'] == 0

    def test_empty_query_pages_zero(self, tmp_dir):
        """When no events match, pages should be 0."""
        history.init(tmp_dir)
        history.log_event('grabbed', 'A')

        result = history.query(type='nonexistent')
        assert result['total'] == 0
        assert result['pages'] == 0


class TestQueryTitleFilter:
    """Test title substring filter in query()."""

    def test_query_title_filter(self, tmp_dir):
        history.init(tmp_dir)
        history.log_event('grabbed', 'Breaking Bad')
        history.log_event('grabbed', 'The Office')
        history.log_event('grabbed', 'Bad Boys')

        result = history.query(title='bad')
        assert result['total'] == 2
        titles = [e['title'] for e in result['events']]
        assert 'Breaking Bad' in titles
        assert 'Bad Boys' in titles


class TestMediaTitle:
    """Tests for media_title field — canonical show/movie name for detail page matching."""

    def test_log_event_media_title_written(self, tmp_dir):
        """media_title should be written to JSONL when provided."""
        history.init(tmp_dir)
        history.log_event('grabbed', 'breaking.bad.s01e01.1080p.web.mkv',
                          source='blackhole', media_title='Breaking Bad')

        fpath = os.path.join(tmp_dir, 'history.jsonl')
        with open(fpath, 'r') as f:
            event = json.loads(f.readline())

        assert event['media_title'] == 'Breaking Bad'
        assert event['title'] == 'breaking.bad.s01e01.1080p.web.mkv'

    def test_log_event_media_title_omitted(self, tmp_dir):
        """No media_title key should exist when not provided (backward compat)."""
        history.init(tmp_dir)
        history.log_event('grabbed', 'Show A', source='blackhole')

        fpath = os.path.join(tmp_dir, 'history.jsonl')
        with open(fpath, 'r') as f:
            event = json.loads(f.readline())

        assert 'media_title' not in event

    def test_query_by_show_matches_media_title(self, tmp_dir):
        """query_by_show should match events by media_title field."""
        history.init(tmp_dir)
        history.log_event('grabbed', 'breaking.bad.s01e01.1080p.web.mkv',
                          source='blackhole', media_title='Breaking Bad')
        history.log_event('cached', 'breaking.bad.s01e01.1080p.web.mkv',
                          source='blackhole', media_title='Breaking Bad')
        history.log_event('grabbed', 'the.office.s02e03.mkv',
                          source='blackhole', media_title='The Office')

        result = history.query_by_show('Breaking Bad')
        assert len(result) == 2
        for e in result:
            assert e['media_title'] == 'Breaking Bad'

    def test_query_by_show_matches_both_fields(self, tmp_dir):
        """query_by_show should match on title OR media_title."""
        history.init(tmp_dir)
        # Old-style event without media_title (uses canonical title directly)
        history.log_event('switched_source', 'Breaking Bad', source='library')
        # New-style event with media_title
        history.log_event('grabbed', 'breaking.bad.s01e01.mkv',
                          source='blackhole', media_title='Breaking Bad')

        result = history.query_by_show('Breaking Bad')
        assert len(result) == 2

    def test_query_title_filter_matches_media_title(self, tmp_dir):
        """query() title substring filter should also search media_title."""
        history.init(tmp_dir)
        history.log_event('grabbed', 'some.torrent.filename.mkv',
                          source='blackhole', media_title='The Office')

        result = history.query(title='office')
        assert result['total'] == 1
        assert result['events'][0]['media_title'] == 'The Office'
