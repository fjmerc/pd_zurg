"""Tests for the library preferences module (utils/library_prefs.py)."""

import json
import os
import threading
from datetime import datetime, timezone, timedelta
import pytest
import utils.library_prefs as lp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_prefs(tmp_dir, monkeypatch):
    """Point prefs to a temp dir and reset module state between tests."""
    prefs_path = os.path.join(tmp_dir, 'library_prefs.json')
    pending_path = os.path.join(tmp_dir, 'library_pending.json')
    monkeypatch.setattr(lp, 'PREFS_PATH', prefs_path)
    monkeypatch.setattr(lp, 'PENDING_PATH', pending_path)


# ---------------------------------------------------------------------------
# Preference CRUD
# ---------------------------------------------------------------------------

class TestPreferences:

    def test_load_missing_file(self):
        assert lp.load_preferences() == {}

    def test_load_corrupt_file(self):
        with open(lp.PREFS_PATH, 'w') as f:
            f.write('not json{{{')
        assert lp.load_preferences() == {}

    def test_load_non_dict(self):
        with open(lp.PREFS_PATH, 'w') as f:
            json.dump([1, 2, 3], f)
        assert lp.load_preferences() == {}

    def test_save_and_load_roundtrip(self):
        prefs = {'show a': 'prefer-local', 'show b': 'prefer-debrid'}
        lp.save_preferences(prefs)
        assert lp.load_preferences() == prefs

    def test_set_preference_creates_entry(self):
        result = lp.set_preference('my show', 'prefer-local')
        assert result['status'] == 'saved'
        assert lp.load_preferences()['my show'] == 'prefer-local'

    def test_set_preference_updates_entry(self):
        lp.set_preference('my show', 'prefer-local')
        lp.set_preference('my show', 'prefer-debrid')
        assert lp.load_preferences()['my show'] == 'prefer-debrid'

    def test_set_preference_none_removes_entry(self):
        lp.set_preference('my show', 'prefer-local')
        lp.set_preference('my show', 'none')
        assert 'my show' not in lp.load_preferences()

    def test_set_preference_invalid_raises(self):
        with pytest.raises(ValueError):
            lp.set_preference('show', 'invalid-value')

    def test_get_all_preferences(self):
        lp.set_preference('a', 'prefer-local')
        lp.set_preference('b', 'prefer-debrid')
        prefs = lp.get_all_preferences()
        assert prefs == {'a': 'prefer-local', 'b': 'prefer-debrid'}

    def test_set_preference_thread_safety(self):
        errors = []

        def _set(name, pref):
            try:
                lp.set_preference(name, pref)
            except Exception as e:
                errors.append(str(e))

        threads = [
            threading.Thread(target=_set, args=(f'show{i}', 'prefer-local'))
            for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert not errors
        prefs = lp.load_preferences()
        assert len(prefs) == 10


# ---------------------------------------------------------------------------
# File removal
# ---------------------------------------------------------------------------

class TestRemoveLocalEpisodes:

    def test_removes_files(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, 'local_tv')
        show_dir = os.path.join(local_tv, 'Show', 'Season 1')
        os.makedirs(show_dir)
        ep = os.path.join(show_dir, 'ep.mkv')
        open(ep, 'w').close()

        result = lp.remove_local_episodes([{'path': ep}], local_tv)
        assert result['removed'] == 1
        assert not os.path.exists(ep)

    def test_cleans_empty_dirs(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, 'local_tv')
        show_dir = os.path.join(local_tv, 'Show', 'Season 1')
        os.makedirs(show_dir)
        ep = os.path.join(show_dir, 'ep.mkv')
        open(ep, 'w').close()

        lp.remove_local_episodes([{'path': ep}], local_tv)
        # Season dir and show dir should be cleaned up
        assert not os.path.exists(show_dir)
        assert not os.path.exists(os.path.join(local_tv, 'Show'))

    def test_preserves_nonempty_dirs(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, 'local_tv')
        show_dir = os.path.join(local_tv, 'Show', 'Season 1')
        os.makedirs(show_dir)
        ep1 = os.path.join(show_dir, 'ep1.mkv')
        ep2 = os.path.join(show_dir, 'ep2.mkv')
        open(ep1, 'w').close()
        open(ep2, 'w').close()

        lp.remove_local_episodes([{'path': ep1}], local_tv)
        assert os.path.exists(show_dir)
        assert os.path.isfile(ep2)

    def test_rejects_path_traversal(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, 'local_tv')
        os.makedirs(local_tv)
        outside = os.path.join(tmp_dir, 'outside.txt')
        open(outside, 'w').close()

        result = lp.remove_local_episodes([{'path': outside}], local_tv)
        assert result['removed'] == 0
        assert len(result['errors']) > 0
        assert os.path.exists(outside)

    def test_handles_already_deleted(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, 'local_tv')
        os.makedirs(local_tv)
        missing = os.path.join(local_tv, 'gone.mkv')

        result = lp.remove_local_episodes([{'path': missing}], local_tv)
        assert result['removed'] == 0


# ---------------------------------------------------------------------------
# Pending transitions
# ---------------------------------------------------------------------------

class TestPending:

    def test_set_pending_creates_entry_with_last_searched(self):
        eps = [{'season': 1, 'episode': 1}]
        lp.set_pending('my show', eps, 'to-debrid')
        pending = lp.get_all_pending()
        entry = pending['my show']
        assert entry['direction'] == 'to-debrid'
        assert 'created' in entry
        assert 'last_searched' in entry
        assert entry['last_searched'] == entry['created']

    def test_set_pending_merge_preserves_last_searched(self):
        eps1 = [{'season': 1, 'episode': 1}]
        lp.set_pending('my show', eps1, 'to-debrid')
        original = lp.get_all_pending()['my show']['last_searched']

        # Merge additional episodes — last_searched should NOT change
        eps2 = [{'season': 1, 'episode': 2}]
        lp.set_pending('my show', eps2, 'to-debrid')
        merged = lp.get_all_pending()['my show']
        assert merged['last_searched'] == original
        assert len(merged['episodes']) == 2

    def test_set_pending_direction_change_resets_last_searched(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        old_ts = lp.get_all_pending()['show']['last_searched']

        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-local')
        new_entry = lp.get_all_pending()['show']
        assert new_entry['direction'] == 'to-local'
        assert 'last_searched' in new_entry

    def test_touch_pending_searched_updates_timestamp(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        original = lp.get_all_pending()['show']['last_searched']

        # Manually set last_searched to the past to confirm touch updates it
        with lp._pending_lock:
            pending = lp._load_pending()
            old_ts = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat(timespec='seconds')
            pending['show']['last_searched'] = old_ts
            lp._save_pending(pending)

        lp.touch_pending_searched('show')
        updated = lp.get_all_pending()['show']['last_searched']
        assert updated != old_ts

    def test_touch_pending_searched_noop_for_missing_title(self):
        # Should not raise or create an entry
        lp.touch_pending_searched('nonexistent')
        assert lp.get_all_pending() == {}

    def test_clear_pending_specific_episodes(self):
        lp.set_pending('show', [
            {'season': 1, 'episode': 1},
            {'season': 1, 'episode': 2},
            {'season': 1, 'episode': 3},
        ], 'to-debrid')
        lp.clear_pending('show', [{'season': 1, 'episode': 2}])
        remaining = lp.get_all_pending()['show']['episodes']
        assert len(remaining) == 2
        keys = {(e['season'], e['episode']) for e in remaining}
        assert (1, 2) not in keys

    def test_clear_pending_all_removes_entry(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        lp.clear_pending('show')
        assert 'show' not in lp.get_all_pending()


# ---------------------------------------------------------------------------
# Pending error tracking
# ---------------------------------------------------------------------------

class TestUpdatePendingError:

    def test_stores_error_and_increments_retry(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        lp.update_pending_error('show', 'Sonarr: connection refused')
        entry = lp.get_all_pending()['show']
        assert entry['last_error'] == 'Sonarr: connection refused'
        assert entry['retry_count'] == 1

    def test_increments_retry_count_on_successive_calls(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        lp.update_pending_error('show', 'error 1')
        lp.update_pending_error('show', 'error 2')
        lp.update_pending_error('show', 'error 3')
        entry = lp.get_all_pending()['show']
        assert entry['retry_count'] == 3
        assert entry['last_error'] == 'error 3'

    def test_no_increment_when_flag_false(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        lp.update_pending_error('show', 'first error')
        lp.update_pending_error('show', 'waiting status', increment_retry=False)
        entry = lp.get_all_pending()['show']
        assert entry['retry_count'] == 1  # not incremented
        assert entry['last_error'] == 'waiting status'

    def test_stores_next_retry_at(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        lp.update_pending_error('show', 'waiting', next_retry_at='2026-04-05T16:00:00+00:00')
        entry = lp.get_all_pending()['show']
        assert entry['next_retry_at'] == '2026-04-05T16:00:00+00:00'

    def test_noop_for_missing_entry(self):
        lp.update_pending_error('nonexistent', 'some error')
        assert lp.get_all_pending() == {}

    def test_preserves_existing_fields(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        lp.update_pending_error('show', 'some error')
        entry = lp.get_all_pending()['show']
        assert entry['direction'] == 'to-debrid'
        assert len(entry['episodes']) == 1
        assert 'created' in entry

    def test_clears_stale_next_retry_at_on_real_error(self):
        """When a real error replaces a 'waiting' status, stale next_retry_at should be cleared."""
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        # First: set a "waiting" status with next_retry_at
        lp.update_pending_error('show', 'Waiting for retry',
                                next_retry_at='2026-04-05T16:00:00+00:00',
                                increment_retry=False)
        entry = lp.get_all_pending()['show']
        assert entry['next_retry_at'] == '2026-04-05T16:00:00+00:00'

        # Then: a real error comes in without next_retry_at
        lp.update_pending_error('show', 'Sonarr: connection refused')
        entry = lp.get_all_pending()['show']
        assert entry['last_error'] == 'Sonarr: connection refused'
        assert 'next_retry_at' not in entry  # stale value should be cleared


# ---------------------------------------------------------------------------
# Pending warned_at tracking
# ---------------------------------------------------------------------------

class TestSetPendingWarned:

    def test_sets_warned_at(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        lp.set_pending_warned('show')
        entry = lp.get_all_pending()['show']
        assert 'warned_at' in entry
        # Verify it's a valid ISO timestamp
        dt = datetime.fromisoformat(entry['warned_at'])
        assert dt.tzinfo is not None

    def test_noop_for_missing_entry(self):
        lp.set_pending_warned('nonexistent')
        assert lp.get_all_pending() == {}

    def test_preserves_other_fields(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        lp.update_pending_error('show', 'some error')
        lp.set_pending_warned('show')
        entry = lp.get_all_pending()['show']
        assert entry['last_error'] == 'some error'
        assert entry['direction'] == 'to-debrid'
        assert 'warned_at' in entry
