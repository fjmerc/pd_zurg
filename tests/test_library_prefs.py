"""Tests for the library preferences module (utils/library_prefs.py)."""

import json
import os
import threading
import time
import pytest
import utils.library_prefs as lp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_prefs(tmp_dir, monkeypatch):
    """Point prefs to a temp dir and reset module state between tests."""
    prefs_path = os.path.join(tmp_dir, 'library_prefs.json')
    monkeypatch.setattr(lp, 'PREFS_PATH', prefs_path)
    # Reset transfer state
    with lp._transfers_lock:
        lp._transfers.clear()
    lp._transfer_counter = 0


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
# File copy
# ---------------------------------------------------------------------------

class TestCopyEpisodes:

    def test_copy_creates_directory_structure(self, tmp_dir):
        src = os.path.join(tmp_dir, 'source.mkv')
        with open(src, 'w') as f:
            f.write('video data')
        local_tv = os.path.join(tmp_dir, 'local_tv')
        os.makedirs(local_tv)

        episodes = [{'season': 2, 'episode': 3, 'source_path': src, 'filename': 'ep.mkv'}]
        tid = lp.copy_episodes_to_local(episodes, 'My Show', local_tv)

        # Wait for background thread
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status = lp.get_transfer_status(tid)
            if status['status'] != 'running':
                break
            time.sleep(0.05)

        dest = os.path.join(local_tv, 'My Show', 'Season 2', 'ep.mkv')
        assert os.path.isfile(dest)
        with open(dest) as f:
            assert f.read() == 'video data'

    def test_copy_returns_transfer_id(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, 'local_tv')
        os.makedirs(local_tv)
        tid = lp.copy_episodes_to_local([], 'Show', local_tv)
        assert tid is not None

    def test_copy_skips_existing_file(self, tmp_dir):
        src = os.path.join(tmp_dir, 'source.mkv')
        with open(src, 'w') as f:
            f.write('original')
        local_tv = os.path.join(tmp_dir, 'local_tv')
        dest_dir = os.path.join(local_tv, 'Show', 'Season 1')
        os.makedirs(dest_dir)
        dest = os.path.join(dest_dir, 'ep.mkv')
        with open(dest, 'w') as f:
            f.write('already here')

        episodes = [{'season': 1, 'episode': 1, 'source_path': src, 'filename': 'ep.mkv'}]
        tid = lp.copy_episodes_to_local(episodes, 'Show', local_tv)

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status = lp.get_transfer_status(tid)
            if status['status'] != 'running':
                break
            time.sleep(0.05)

        with open(dest) as f:
            assert f.read() == 'already here'

    def test_copy_handles_missing_source(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, 'local_tv')
        os.makedirs(local_tv)
        episodes = [{'season': 1, 'episode': 1, 'source_path': '/nonexistent/file.mkv', 'filename': 'ep.mkv'}]
        tid = lp.copy_episodes_to_local(episodes, 'Show', local_tv)

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status = lp.get_transfer_status(tid)
            if status['status'] != 'running':
                break
            time.sleep(0.05)

        status = lp.get_transfer_status(tid)
        assert status['status'] == 'failed'
        assert len(status['errors']) > 0


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
# Transfer tracking
# ---------------------------------------------------------------------------

class TestTransferTracking:

    def test_get_status_by_id(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, 'local_tv')
        os.makedirs(local_tv)
        tid = lp.copy_episodes_to_local([], 'Show', local_tv)
        status = lp.get_transfer_status(tid)
        assert 'status' in status

    def test_get_all_statuses(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, 'local_tv')
        os.makedirs(local_tv)
        lp.copy_episodes_to_local([], 'A', local_tv)
        lp.copy_episodes_to_local([], 'B', local_tv)
        time.sleep(0.1)
        all_status = lp.get_transfer_status()
        assert isinstance(all_status, dict)
        assert len(all_status) >= 2

    def test_not_found_returns_status(self):
        result = lp.get_transfer_status('nonexistent')
        assert result['status'] == 'not_found'

    def test_pruning_old_transfers(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, 'local_tv')
        os.makedirs(local_tv)
        tid = lp.copy_episodes_to_local([], 'Show', local_tv)
        time.sleep(0.1)
        # Simulate old finished transfer
        with lp._transfers_lock:
            lp._transfers[tid]['finished'] = time.monotonic() - 7200
        # Fetching should prune it
        lp.get_transfer_status()
        with lp._transfers_lock:
            assert tid not in lp._transfers
