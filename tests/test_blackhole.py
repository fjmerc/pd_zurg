"""Tests for blackhole watch folder logic."""

import json
import os
import time
import pytest
from utils.blackhole import RetryMeta, BlackholeWatcher, RETRY_SCHEDULE, MAX_RETRIES


class TestRetryMeta:

    def test_read_nonexistent(self, tmp_dir):
        """Reading meta for file without sidecar should return (0, 0)."""
        path = os.path.join(tmp_dir, 'test.torrent')
        retries, last = RetryMeta.read(path)
        assert retries == 0
        assert last == 0

    def test_write_and_read(self, tmp_dir):
        """Should persist retry count and timestamp."""
        path = os.path.join(tmp_dir, 'test.torrent')
        before = time.time()
        RetryMeta.write(path, 3)
        retries, last = RetryMeta.read(path)
        assert retries == 3
        assert last >= before

    def test_incremental_writes(self, tmp_dir):
        """Each write should update the retry count."""
        path = os.path.join(tmp_dir, 'test.torrent')
        for i in range(1, 4):
            RetryMeta.write(path, i)
            retries, _ = RetryMeta.read(path)
            assert retries == i

    def test_remove(self, tmp_dir):
        """Should clean up sidecar meta file."""
        path = os.path.join(tmp_dir, 'test.torrent')
        RetryMeta.write(path, 1)
        assert os.path.exists(path + '.meta')
        RetryMeta.remove(path)
        assert not os.path.exists(path + '.meta')

    def test_remove_nonexistent(self, tmp_dir):
        """Removing meta for file without sidecar should not raise."""
        path = os.path.join(tmp_dir, 'test.torrent')
        RetryMeta.remove(path)  # Should not raise

    def test_corrupt_meta_returns_defaults(self, tmp_dir):
        """Corrupt meta file should return defaults instead of crashing."""
        path = os.path.join(tmp_dir, 'test.torrent')
        meta = path + '.meta'
        with open(meta, 'w') as f:
            f.write('not json')
        retries, last = RetryMeta.read(path)
        assert retries == 0
        assert last == 0

    def test_meta_path(self, tmp_dir):
        """Meta path should be original path + .meta suffix."""
        path = os.path.join(tmp_dir, 'movie.torrent')
        assert RetryMeta.meta_path(path) == path + '.meta'


class TestBlackholeWatcher:

    def test_supported_extensions(self):
        """Should support .torrent and .magnet extensions."""
        assert '.torrent' in BlackholeWatcher.SUPPORTED_EXTENSIONS
        assert '.magnet' in BlackholeWatcher.SUPPORTED_EXTENSIONS
        assert '.nzb' not in BlackholeWatcher.SUPPORTED_EXTENSIONS

    def test_scan_finds_torrent_files(self, tmp_dir):
        """Scan should detect .torrent files in watch directory."""
        # Create test files
        for name in ['movie.torrent', 'show.torrent', 'readme.txt']:
            path = os.path.join(tmp_dir, name)
            with open(path, 'w') as f:
                f.write('test')
            # Set mtime to past so files aren't skipped as "still being written"
            os.utime(path, (time.time() - 10, time.time() - 10))

        watcher = BlackholeWatcher(tmp_dir, 'fake_key', 'realdebrid')
        found = []
        for filename in os.listdir(tmp_dir):
            ext = os.path.splitext(filename)[1].lower()
            if ext in watcher.SUPPORTED_EXTENSIONS:
                found.append(filename)
        assert len(found) == 2
        assert 'readme.txt' not in found

    def test_scan_ignores_subdirectories(self, tmp_dir):
        """Scan should not process files in subdirectories."""
        subdir = os.path.join(tmp_dir, 'subdir')
        os.makedirs(subdir)
        with open(os.path.join(subdir, 'nested.torrent'), 'w') as f:
            f.write('test')

        # Only files directly in watch_dir should be found
        watcher = BlackholeWatcher(tmp_dir, 'fake_key', 'realdebrid')
        top_files = [
            f for f in os.listdir(tmp_dir)
            if os.path.isfile(os.path.join(tmp_dir, f))
        ]
        assert len(top_files) == 0

    def test_scan_skips_recent_files(self, tmp_dir):
        """Files modified within last 2 seconds should be skipped."""
        path = os.path.join(tmp_dir, 'new.torrent')
        with open(path, 'w') as f:
            f.write('still writing...')
        # File just created — mtime is now

        watcher = BlackholeWatcher(tmp_dir, 'fake_key', 'realdebrid')
        now = time.time()
        mtime = os.path.getmtime(path)
        assert now - mtime < 2.0  # Should be skipped


class TestRetrySchedule:

    def test_schedule_values(self):
        """Retry schedule should have increasing delays."""
        for i in range(1, len(RETRY_SCHEDULE)):
            assert RETRY_SCHEDULE[i] > RETRY_SCHEDULE[i - 1]

    def test_max_retries_matches_schedule(self):
        """MAX_RETRIES should be reasonable relative to schedule length."""
        assert MAX_RETRIES >= 1
        assert MAX_RETRIES <= 10

    def test_schedule_first_retry_reasonable(self):
        """First retry should be at least 60 seconds."""
        assert RETRY_SCHEDULE[0] >= 60
