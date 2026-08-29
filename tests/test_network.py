"""Tests for utils/network.py — wait_for_url."""

from utils import network


def _fake_clock(monkeypatch):
    """Replace network.time with a virtual clock so no real sleeping
    happens; sleep() advances the clock."""
    clock = {'t': 0.0}
    monkeypatch.setattr(network.time, 'time', lambda: clock['t'])
    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)
        clock['t'] += s

    monkeypatch.setattr(network.time, 'sleep', fake_sleep)
    return clock, sleeps


def test_success_returns_true(monkeypatch):
    _fake_clock(monkeypatch)

    class Resp:
        status_code = 200

    monkeypatch.setattr(network.requests, 'request',
                        lambda method, url, **kw: Resp())
    assert network.wait_for_url('http://x', timeout=30) is True


def test_deadline_is_wall_clock_bound(monkeypatch):
    """The timeout is a wall-clock cap, not just a between-attempts
    check — a 30s probe must not sleep past the deadline.  The deferred
    mount retry runs on the scheduler thread with a 30s probe; an
    unbounded overshoot per pending mount stalls every other scheduled
    task."""
    clock, sleeps = _fake_clock(monkeypatch)
    request_timeouts = []

    def refused(method, url, **kwargs):
        request_timeouts.append(kwargs.get('timeout'))
        raise network.requests.ConnectionError()

    monkeypatch.setattr(network.requests, 'request', refused)

    assert network.wait_for_url('http://x', timeout=30) is False
    assert clock['t'] <= 30

    # The per-request socket timeout must also shrink to the remaining
    # window so a hanging endpoint can't push the probe past deadline.
    assert network.wait_for_url('http://x', timeout=22) is False
    assert all(t <= 10 for t in request_timeouts)
    assert min(request_timeouts) < 10  # at least one capped below default
