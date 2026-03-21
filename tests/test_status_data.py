"""Tests for StatusData event tracking."""

import pytest
from utils.status_server import StatusData


class TestStatusData:

    def test_initial_state(self):
        """Fresh StatusData should have no events and no errors."""
        sd = StatusData()
        assert sd.error_count == 0
        assert len(sd.recent_events) == 0

    def test_add_event(self):
        """Events should be retrievable after adding."""
        sd = StatusData()
        sd.add_event('test', 'hello')
        assert len(sd.recent_events) == 1
        event = sd.recent_events[0]
        assert event['component'] == 'test'
        assert event['message'] == 'hello'
        assert event['level'] == 'info'
        assert 'timestamp' in event

    def test_add_event_with_level(self):
        """Events should preserve their level."""
        sd = StatusData()
        sd.add_event('test', 'warning msg', level='warning')
        assert sd.recent_events[0]['level'] == 'warning'

    def test_events_prepended(self):
        """Newer events should appear first (prepended)."""
        sd = StatusData()
        sd.add_event('test', 'first')
        sd.add_event('test', 'second')
        assert sd.recent_events[0]['message'] == 'second'
        assert sd.recent_events[1]['message'] == 'first'

    def test_max_events_capped(self):
        """Deque should cap at 100 events."""
        sd = StatusData()
        for i in range(150):
            sd.add_event('test', f'event {i}')
        assert len(sd.recent_events) == 100

    def test_error_count_incremented(self):
        """Error events should increment the error counter."""
        sd = StatusData()
        sd.add_event('test', 'ok', level='info')
        sd.add_event('test', 'bad', level='error')
        sd.add_event('test', 'worse', level='error')
        assert sd.error_count == 2

    def test_warning_does_not_increment_error_count(self):
        """Warning events should not increment the error counter."""
        sd = StatusData()
        sd.add_event('test', 'warn', level='warning')
        assert sd.error_count == 0

    def test_to_dict_structure(self):
        """to_dict should return expected keys."""
        sd = StatusData()
        sd.add_event('test', 'hello')
        data = sd.to_dict()
        assert 'version' in data
        assert 'uptime_seconds' in data
        assert 'processes' in data
        assert 'mounts' in data
        assert 'recent_events' in data
        assert 'error_count' in data
        assert isinstance(data['recent_events'], list)
        assert isinstance(data['processes'], list)

    def test_uptime_positive(self):
        """Uptime should be non-negative."""
        sd = StatusData()
        data = sd.to_dict()
        assert data['uptime_seconds'] >= 0

    def test_events_in_to_dict(self):
        """to_dict should include all added events."""
        sd = StatusData()
        sd.add_event('comp1', 'msg1')
        sd.add_event('comp2', 'msg2')
        data = sd.to_dict()
        messages = [e['message'] for e in data['recent_events']]
        assert 'msg1' in messages
        assert 'msg2' in messages
