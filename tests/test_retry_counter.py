"""In-memory retry counter tests."""

from utils import retry_counter


def setup_function():
    retry_counter.reset_all()


def test_first_bump_returns_one():
    count, first_ts = retry_counter.bump('radarr', 608)
    assert count == 1
    assert first_ts  # ISO string


def test_repeat_bumps_increment_but_keep_first_ts():
    c1, ts1 = retry_counter.bump('radarr', 608)
    c2, ts2 = retry_counter.bump('radarr', 608)
    c3, ts3 = retry_counter.bump('radarr', 608)
    assert (c1, c2, c3) == (1, 2, 3)
    assert ts1 == ts2 == ts3


def test_independent_keys_do_not_cross():
    retry_counter.bump('radarr', 608)
    retry_counter.bump('radarr', 608)
    retry_counter.bump('radarr', 609)
    assert retry_counter.get('radarr', 608)[0] == 2
    assert retry_counter.get('radarr', 609)[0] == 1
    assert retry_counter.get('sonarr', 608)[0] == 0


def test_reset_drops_entry():
    retry_counter.bump('radarr', 608)
    retry_counter.reset('radarr', 608)
    assert retry_counter.get('radarr', 608) == (0, None)


def test_reset_missing_key_is_safe():
    retry_counter.reset('radarr', 999)  # does not raise


def test_size_tracks_entries():
    assert retry_counter.size() == 0
    retry_counter.bump('a', 1)
    retry_counter.bump('b', 2)
    assert retry_counter.size() == 2
