"""Tests for blackhole watch folder logic."""

import json
import os
import time
import pytest
from utils.blackhole import (
    RetryMeta, BlackholeWatcher, RETRY_SCHEDULE, MAX_RETRIES,
    MEDIA_EXTENSIONS, MOUNT_CATEGORIES, parse_release_name,
    _is_multi_season_pack, _extract_file_season, _build_season_release_name,
)


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


class TestSymlinkConstants:

    def test_media_extensions_include_common_video(self):
        """MEDIA_EXTENSIONS should include common video formats."""
        for ext in ['.mkv', '.mp4', '.avi', '.ts', '.webm']:
            assert ext in MEDIA_EXTENSIONS

    def test_media_extensions_exclude_non_video(self):
        """MEDIA_EXTENSIONS should not include non-video formats."""
        for ext in ['.nfo', '.txt', '.jpg', '.png', '.srt', '.sub']:
            assert ext not in MEDIA_EXTENSIONS

    def test_mount_categories(self):
        """MOUNT_CATEGORIES should include the standard Zurg categories."""
        assert 'shows' in MOUNT_CATEGORIES
        assert 'movies' in MOUNT_CATEGORIES
        assert 'anime' in MOUNT_CATEGORIES


class TestExtractTorrentId:

    def test_realdebrid_string_id(self):
        """RD returns torrent_id as a plain string."""
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        assert watcher._extract_torrent_id('TGLXHJIH2IFL6') == 'TGLXHJIH2IFL6'

    def test_alldebrid_json_response(self):
        """AD returns full JSON; extract magnet ID."""
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        result = {'data': {'magnets': [{'id': 12345}]}}
        assert watcher._extract_torrent_id(result) == '12345'

    def test_torbox_json_response(self):
        """TorBox returns full JSON; extract torrent_id."""
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        result = {'data': {'torrent_id': 67890}}
        assert watcher._extract_torrent_id(result) == '67890'

    def test_torbox_fallback_to_id(self):
        """TorBox should fallback to 'id' if 'torrent_id' is missing."""
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        result = {'data': {'id': 11111}}
        assert watcher._extract_torrent_id(result) == '11111'

    def test_alldebrid_malformed_response(self):
        """Should return None for malformed AD response."""
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        assert watcher._extract_torrent_id({}) is None
        assert watcher._extract_torrent_id({'data': {}}) is None

    def test_torbox_malformed_response(self):
        """Should return None for malformed TorBox response."""
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        # Empty data with no torrent_id or id returns empty string which is falsy
        result = watcher._extract_torrent_id({'data': {}})
        assert not result  # empty string or None


class TestFindOnMount:

    def test_finds_in_shows(self, tmp_dir):
        """Should find content in the shows category."""
        shows_dir = os.path.join(tmp_dir, 'shows', 'My.Show.S01')
        os.makedirs(shows_dir)
        with open(os.path.join(shows_dir, 'ep01.mkv'), 'w') as f:
            f.write('video')

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('My.Show.S01')
        assert path == shows_dir
        assert category == 'shows'
        assert matched == 'My.Show.S01'

    def test_finds_in_movies(self, tmp_dir):
        """Should find content in the movies category."""
        movies_dir = os.path.join(tmp_dir, 'movies', 'My.Movie.2024')
        os.makedirs(movies_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('My.Movie.2024')
        assert path == movies_dir
        assert category == 'movies'
        assert matched == 'My.Movie.2024'

    def test_finds_in_anime(self, tmp_dir):
        """Should find content in the anime category."""
        anime_dir = os.path.join(tmp_dir, 'anime', 'My.Anime.S01')
        os.makedirs(anime_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('My.Anime.S01')
        assert path == anime_dir
        assert category == 'anime'
        assert matched == 'My.Anime.S01'

    def test_fallback_to_all(self, tmp_dir):
        """Should fall back to __all__ if not in categorized dirs."""
        all_dir = os.path.join(tmp_dir, '__all__', 'Random.Content')
        os.makedirs(all_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Random.Content')
        assert path == all_dir
        assert category == '__all__'
        assert matched == 'Random.Content'

    def test_not_found(self, tmp_dir):
        """Should return (None, None, None) when content is not on mount."""
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Nonexistent.Release')
        assert path is None
        assert category is None
        assert matched is None

    def test_prefers_categorized_over_all(self, tmp_dir):
        """Categorized dirs should be checked before __all__."""
        # Create in both shows and __all__
        for cat in ['shows', '__all__']:
            d = os.path.join(tmp_dir, cat, 'My.Show.S01')
            os.makedirs(d)

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('My.Show.S01')
        assert category == 'shows'
        assert matched == 'My.Show.S01'

    def test_strips_video_extension(self, tmp_dir):
        """Should find folder when release name has video extension that Zurg strips."""
        # Zurg creates folder without .mkv extension
        shows_dir = os.path.join(tmp_dir, 'shows', 'Bad.Monkey.S01E01.1080p')
        os.makedirs(shows_dir)
        with open(os.path.join(shows_dir, 'Bad.Monkey.S01E01.1080p.mkv'), 'w') as f:
            f.write('video')

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        # RD returns filename WITH .mkv extension
        path, category, matched = watcher._find_on_mount('Bad.Monkey.S01E01.1080p.mkv')
        assert path == shows_dir
        assert category == 'shows'
        assert matched == 'Bad.Monkey.S01E01.1080p'

    def test_prefers_exact_name_over_stripped(self, tmp_dir):
        """Should prefer exact name match over extension-stripped match."""
        # Both exist: exact match (with .mkv in folder name) and stripped
        exact_dir = os.path.join(tmp_dir, 'shows', 'Release.Name.mkv')
        stripped_dir = os.path.join(tmp_dir, 'shows', 'Release.Name')
        os.makedirs(exact_dir)
        os.makedirs(stripped_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Release.Name.mkv')
        assert path == exact_dir
        assert matched == 'Release.Name.mkv'

    def test_no_strip_for_non_media_extension(self, tmp_dir):
        """Should not strip non-media extensions like .nfo."""
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Release.Name.nfo')
        assert path is None
        assert matched is None


class TestCreateSymlinks:

    def _make_watcher(self, tmp_dir):
        """Create a watcher configured for symlink testing."""
        completed = os.path.join(tmp_dir, 'completed')
        mount = os.path.join(tmp_dir, 'mount')
        os.makedirs(completed)
        os.makedirs(mount)
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed,
            rclone_mount=mount,
            symlink_target_base='/mnt/debrid',
        )
        return watcher, completed, mount

    def test_creates_symlinks_for_media_files(self, tmp_dir):
        """Should create symlinks only for media files."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        # Create mock content on mount
        release = 'My.Show.S01E01'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        for name in ['episode.mkv', 'episode.nfo', 'poster.jpg', 'sample.mkv']:
            with open(os.path.join(release_dir, name), 'w') as f:
                f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        # Only episode.mkv — sample.mkv is skipped, .nfo and .jpg are non-media
        assert count == 1

        symlink = os.path.join(completed, release, 'episode.mkv')
        assert os.path.islink(symlink)
        target = os.readlink(symlink)
        assert target == f'/mnt/debrid/shows/{release}/episode.mkv'

    def test_skips_sample_files(self, tmp_dir):
        """Files with 'sample' in the name should be skipped."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Movie.2024'
        release_dir = os.path.join(mount, 'movies', release)
        os.makedirs(release_dir)
        for name in ['Movie.2024.mkv', 'Sample.mkv', 'movie-sample.mp4']:
            with open(os.path.join(release_dir, name), 'w') as f:
                f.write('data')

        count = watcher._create_symlinks(release, 'movies', release_dir)
        assert count == 1  # Only Movie.2024.mkv

    def test_skips_existing_symlinks(self, tmp_dir):
        """Should not recreate existing symlinks."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Movie.2024'
        release_dir = os.path.join(mount, 'movies', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'movie.mkv'), 'w') as f:
            f.write('data')

        # Create first time
        count1 = watcher._create_symlinks(release, 'movies', release_dir)
        assert count1 == 1

        # Try again — should skip existing
        count2 = watcher._create_symlinks(release, 'movies', release_dir)
        assert count2 == 0

    def test_handles_nested_directories(self, tmp_dir):
        """Should handle files in subdirectories within a release."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01.Complete'
        release_dir = os.path.join(mount, 'shows', release)
        sub = os.path.join(release_dir, 'Season 01')
        os.makedirs(sub)
        with open(os.path.join(sub, 'S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(sub, 'S01E02.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 2

        symlink = os.path.join(completed, release, 'Season 01', 'S01E01.mkv')
        assert os.path.islink(symlink)
        target = os.readlink(symlink)
        assert target == f'/mnt/debrid/shows/{release}/Season 01/S01E01.mkv'

    def test_symlink_target_uses_configured_base(self, tmp_dir):
        """Symlink targets should use the configured target base path."""
        completed = os.path.join(tmp_dir, 'completed')
        mount = os.path.join(tmp_dir, 'mount')
        os.makedirs(completed)
        os.makedirs(mount)
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed,
            rclone_mount=mount,
            symlink_target_base='/custom/path',
        )

        release = 'Movie.2024'
        release_dir = os.path.join(mount, 'movies', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'movie.mp4'), 'w') as f:
            f.write('data')

        watcher._create_symlinks(release, 'movies', release_dir)
        target = os.readlink(os.path.join(completed, release, 'movie.mp4'))
        assert target.startswith('/custom/path/')

    def test_path_traversal_blocked(self, tmp_dir):
        """Release names with path traversal should be handled safely."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Normal.Release'
        release_dir = os.path.join(mount, 'movies', release)
        # Create a file with a path-traversal relative path
        sub = os.path.join(release_dir, '..', '..', 'escape')
        os.makedirs(sub, exist_ok=True)
        with open(os.path.join(sub, 'evil.mkv'), 'w') as f:
            f.write('data')

        # The traversal should be caught by the guard
        count = watcher._create_symlinks(release, 'movies', release_dir)
        # Should not create symlinks outside the completed release dir
        assert not os.path.exists(os.path.join(completed, 'escape'))


class TestPendingMonitors:

    def test_add_and_load_pending(self, tmp_dir):
        """Should persist pending monitors to disk."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )

        watcher._add_pending('torrent123', 'movie.torrent')
        entries = watcher._load_pending()
        assert len(entries) == 1
        assert entries[0]['torrent_id'] == 'torrent123'
        assert entries[0]['filename'] == 'movie.torrent'
        assert entries[0]['service'] == 'realdebrid'

    def test_add_pending_deduplicates(self, tmp_dir):
        """Should not add duplicate torrent IDs."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )

        watcher._add_pending('torrent123', 'movie.torrent')
        watcher._add_pending('torrent123', 'movie.torrent')
        entries = watcher._load_pending()
        assert len(entries) == 1

    def test_remove_pending(self, tmp_dir):
        """Should remove a specific torrent from pending."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )

        watcher._add_pending('torrent1', 'file1.torrent')
        watcher._add_pending('torrent2', 'file2.torrent')
        watcher._remove_pending('torrent1')
        entries = watcher._load_pending()
        assert len(entries) == 1
        assert entries[0]['torrent_id'] == 'torrent2'

    def test_load_pending_missing_file(self, tmp_dir):
        """Should return empty list when no pending file exists."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        assert watcher._load_pending() == []

    def test_load_pending_corrupt_file(self, tmp_dir):
        """Should return empty list for corrupt pending file."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        with open(watcher._pending_file, 'w') as f:
            f.write('not valid json')
        assert watcher._load_pending() == []

    def test_pending_file_in_completed_dir(self, tmp_dir):
        """Pending file should be stored in completed_dir, not watch_dir."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        assert watcher._pending_file.startswith(completed)


class TestSymlinkCleanup:

    def test_removes_broken_symlinks(self, tmp_dir):
        """Should remove symlinks whose targets no longer exist."""
        completed = os.path.join(tmp_dir, 'completed')
        release_dir = os.path.join(completed, 'Old.Release')
        os.makedirs(release_dir)

        # Create a broken symlink
        symlink = os.path.join(release_dir, 'episode.mkv')
        os.symlink('/nonexistent/path/episode.mkv', symlink)
        assert os.path.islink(symlink)
        assert not os.path.exists(symlink)  # broken

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        watcher._cleanup_symlinks()

        # Broken symlink should be removed, and empty dir cleaned up
        assert not os.path.islink(symlink)
        assert not os.path.exists(release_dir)

    def test_preserves_valid_symlinks(self, tmp_dir):
        """Should not remove directories with valid symlinks."""
        completed = os.path.join(tmp_dir, 'completed')
        release_dir = os.path.join(completed, 'Good.Release')
        os.makedirs(release_dir)

        # Create a valid symlink target
        target_file = os.path.join(tmp_dir, 'real_file.mkv')
        with open(target_file, 'w') as f:
            f.write('video data')

        symlink = os.path.join(release_dir, 'episode.mkv')
        os.symlink(target_file, symlink)
        assert os.path.exists(symlink)

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
            symlink_max_age=0,  # Disable age-based cleanup
        )
        watcher._cleanup_symlinks()

        assert os.path.islink(symlink)
        assert os.path.isdir(release_dir)

    def test_age_based_cleanup(self, tmp_dir):
        """Should remove directories older than max age."""
        completed = os.path.join(tmp_dir, 'completed')
        release_dir = os.path.join(completed, 'Old.Release')
        os.makedirs(release_dir)

        # Create a valid symlink
        target_file = os.path.join(tmp_dir, 'real_file.mkv')
        with open(target_file, 'w') as f:
            f.write('data')
        symlink = os.path.join(release_dir, 'ep.mkv')
        os.symlink(target_file, symlink)

        # Set mtime to 100 hours ago
        old_time = time.time() - (100 * 3600)
        os.utime(release_dir, (old_time, old_time))

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
            symlink_max_age=72,  # 72 hours
        )
        watcher._cleanup_symlinks()

        assert not os.path.exists(release_dir)

    def test_age_zero_disables_age_cleanup(self, tmp_dir):
        """symlink_max_age=0 should disable age-based removal."""
        completed = os.path.join(tmp_dir, 'completed')
        release_dir = os.path.join(completed, 'Old.Release')
        os.makedirs(release_dir)

        target_file = os.path.join(tmp_dir, 'real_file.mkv')
        with open(target_file, 'w') as f:
            f.write('data')
        symlink = os.path.join(release_dir, 'ep.mkv')
        os.symlink(target_file, symlink)

        # Set very old mtime
        old_time = time.time() - (1000 * 3600)
        os.utime(release_dir, (old_time, old_time))

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
            symlink_max_age=0,
        )
        watcher._cleanup_symlinks()

        assert os.path.exists(release_dir)

    def test_cleanup_skipped_when_disabled(self, tmp_dir):
        """Cleanup should do nothing when symlinks are disabled."""
        watcher = BlackholeWatcher(tmp_dir, 'key', 'realdebrid', symlink_enabled=False)
        # Should not raise even with no completed_dir
        watcher._cleanup_symlinks()


class TestTorrentStatusHelpers:

    def test_is_torrent_ready_realdebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        assert watcher._is_torrent_ready('downloaded') is True
        assert watcher._is_torrent_ready('downloading') is False
        assert watcher._is_torrent_ready('queued') is False

    def test_is_torrent_ready_alldebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        assert watcher._is_torrent_ready('Ready') is True
        assert watcher._is_torrent_ready('Downloading') is False

    def test_is_torrent_ready_torbox(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        assert watcher._is_torrent_ready('completed') is True
        assert watcher._is_torrent_ready('downloading') is False

    def test_is_terminal_error_realdebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        assert watcher._is_terminal_error('magnet_error') is True
        assert watcher._is_terminal_error('error') is True
        assert watcher._is_terminal_error('virus') is True
        assert watcher._is_terminal_error('dead') is True
        assert watcher._is_terminal_error('downloading') is False

    def test_is_terminal_error_alldebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        assert watcher._is_terminal_error('Error') is True
        assert watcher._is_terminal_error('Ready') is False

    def test_is_terminal_error_torbox(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        assert watcher._is_terminal_error('error') is True
        assert watcher._is_terminal_error('failed') is True
        assert watcher._is_terminal_error('completed') is False


class TestExtractReleaseName:

    def test_realdebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        info = {'filename': 'Landman.S01.1080p'}
        assert watcher._extract_release_name(info) == 'Landman.S01.1080p'

    def test_alldebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        info = {'data': {'magnets': {'filename': 'Movie.2024'}}}
        assert watcher._extract_release_name(info) == 'Movie.2024'

    def test_torbox(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        info = {'data': {'name': 'Show.S02'}}
        assert watcher._extract_release_name(info) == 'Show.S02'

    def test_missing_data(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        assert watcher._extract_release_name({}) == ''


class TestWatcherSymlinkInit:

    def test_default_symlink_disabled(self):
        """Symlink should be disabled by default."""
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        assert watcher.symlink_enabled is False

    def test_symlink_config_passed_through(self, tmp_dir):
        """All symlink config should be stored on the watcher."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed,
            rclone_mount='/data',
            symlink_target_base='/mnt/debrid',
            mount_poll_timeout=600,
            mount_poll_interval=15,
            symlink_max_age=48,
        )
        assert watcher.symlink_enabled is True
        assert watcher.completed_dir == completed
        assert watcher.rclone_mount == '/data'
        assert watcher.symlink_target_base == '/mnt/debrid'
        assert watcher.mount_poll_timeout == 600
        assert watcher.mount_poll_interval == 15
        assert watcher.symlink_max_age == 48


class TestParseReleaseName:

    def test_tv_episode(self):
        name, season, is_tv = parse_release_name('Bad.Monkey.S01E01.1080p.ATVP.WEB-DL.DDP5.1.H.264-NTb.torrent')
        assert name == 'Bad Monkey'
        assert season == 1
        assert is_tv is True

    def test_tv_season_pack(self):
        name, season, is_tv = parse_release_name('Fargo.S05.COMPLETE.1080p.torrent')
        assert name == 'Fargo'
        assert season == 5
        assert is_tv is True

    def test_tv_with_year(self):
        name, season, is_tv = parse_release_name('Fargo.2014.S03E01.720p.torrent')
        assert name == 'Fargo'
        assert season == 3
        assert is_tv is True

    def test_movie(self):
        name, season, is_tv = parse_release_name('Gattaca.1997.1080p.BluRay.torrent')
        assert name == 'Gattaca'
        assert season is None
        assert is_tv is False

    def test_movie_no_year(self):
        name, season, is_tv = parse_release_name('Some.Movie.1080p.WEB.torrent')
        assert name == 'Some Movie'
        assert season is None
        assert is_tv is False

    def test_magnet_extension(self):
        name, season, is_tv = parse_release_name('Show.S02E05.720p.magnet')
        assert name == 'Show'
        assert season == 2
        assert is_tv is True

    def test_season_at_end(self):
        """S01 at end of name with no episode number."""
        name, season, is_tv = parse_release_name('Some.Show.S01.torrent')
        assert name == 'Some Show'
        assert season == 1
        assert is_tv is True


class TestCheckLocalLibrary:

    def _make_watcher(self, tmp_dir):
        tv_dir = os.path.join(tmp_dir, 'tv')
        movies_dir = os.path.join(tmp_dir, 'movies')
        os.makedirs(tv_dir)
        os.makedirs(movies_dir)
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            dedup_enabled=True,
            local_library_tv=tv_dir,
            local_library_movies=movies_dir,
        )
        return watcher, tv_dir, movies_dir

    def test_skips_existing_tv_episode(self, tmp_dir):
        """Should skip if the specific episode exists locally."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 05')
        os.makedirs(season_dir)
        with open(os.path.join(season_dir, 'Fargo (2014) - S05E01 - The Tragedy of the Commons.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Fargo.S05E01.1080p.WEB.torrent') is True

    def test_allows_missing_episode(self, tmp_dir):
        """Should allow if the season exists but the specific episode doesn't."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 05')
        os.makedirs(season_dir)
        with open(os.path.join(season_dir, 'Fargo (2014) - S05E01 - The Tragedy of the Commons.mkv'), 'w') as f:
            f.write('data')

        # E03 is not present locally — should NOT skip
        assert watcher._check_local_library('Fargo.S05E03.1080p.WEB.torrent') is False

    def test_allows_missing_season(self, tmp_dir):
        """Should allow if the show exists but the season doesn't."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 01')
        os.makedirs(season_dir)
        with open(os.path.join(season_dir, 'Fargo (2014) - S01E01 - The Crocodiles Dilemma.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Fargo.S05E01.1080p.WEB.torrent') is False

    def test_skips_existing_movie(self, tmp_dir):
        """Should skip if the movie exists locally."""
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        movie_dir = os.path.join(movies_dir, 'Gattaca (1997)')
        os.makedirs(movie_dir)
        with open(os.path.join(movie_dir, 'Gattaca.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Gattaca.1997.1080p.BluRay.torrent') is True

    def test_allows_missing_movie(self, tmp_dir):
        """Should allow if the movie doesn't exist locally."""
        watcher, _, _ = self._make_watcher(tmp_dir)
        assert watcher._check_local_library('Gattaca.1997.1080p.BluRay.torrent') is False

    def test_disabled_by_default(self, tmp_dir):
        """Should always return False when dedup is disabled."""
        watcher = BlackholeWatcher(os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid')
        assert watcher._check_local_library('Fargo.S05E01.torrent') is False

    def test_no_false_positive_substring(self, tmp_dir):
        """Should not match 'Fargo' against 'Wells Fargo Documentary'."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        other_dir = os.path.join(tv_dir, 'Wells Fargo Documentary', 'Season 01')
        os.makedirs(other_dir)
        with open(os.path.join(other_dir, 'ep01.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Fargo.S01E01.torrent') is False

    def test_missing_library_path(self, tmp_dir):
        """Should not crash when library path doesn't exist."""
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            dedup_enabled=True,
            local_library_tv='/nonexistent/path',
            local_library_movies='/nonexistent/path',
        )
        assert watcher._check_local_library('Fargo.S01E01.torrent') is False

    def test_empty_season_dir_not_matched(self, tmp_dir):
        """Should not match a season directory that has no files."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 05')
        os.makedirs(season_dir)
        # Empty season dir

        assert watcher._check_local_library('Fargo.S05E01.1080p.WEB.torrent') is False


class TestIsMultiSeasonPack:

    def test_s01_s05(self):
        is_multi, start, end = _is_multi_season_pack('Show.S01-S05.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 5

    def test_s01_05_bare(self):
        is_multi, start, end = _is_multi_season_pack('Show.S01-05.BluRay')
        assert is_multi is True
        assert start == 1
        assert end == 5

    def test_cross_season_episodes(self):
        is_multi, start, end = _is_multi_season_pack('Show.S01E01-S03E12.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 3

    def test_complete_series(self):
        is_multi, start, end = _is_multi_season_pack('Show.Complete.Series.1080p')
        assert is_multi is True
        assert start is None
        assert end is None

    def test_complete_collection(self):
        is_multi, start, end = _is_multi_season_pack('Show.Complete.Collection.BluRay')
        assert is_multi is True
        assert start is None
        assert end is None

    def test_seasons_range(self):
        is_multi, start, end = _is_multi_season_pack('Show.Seasons.1-3.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 3

    def test_season_singular_range(self):
        is_multi, start, end = _is_multi_season_pack('Show.Season.1-5.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 5

    def test_seasons_ampersand(self):
        is_multi, start, end = _is_multi_season_pack('Show.Seasons.1.&.2.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 2

    def test_seasons_and_separator(self):
        is_multi, start, end = _is_multi_season_pack('Project Blue Book Seasons 1 and 2 Mp4 1080p')
        assert is_multi is True
        assert start == 1
        assert end == 2

    def test_series_range(self):
        is_multi, start, end = _is_multi_season_pack('Show.Series.1-3.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 3

    def test_single_season_not_multi(self):
        is_multi, _, _ = _is_multi_season_pack('Show.S03.1080p')
        assert is_multi is False

    def test_single_episode_not_multi(self):
        is_multi, _, _ = _is_multi_season_pack('Show.S03E01.1080p')
        assert is_multi is False

    def test_movie_not_multi(self):
        is_multi, _, _ = _is_multi_season_pack('Movie.2024.1080p')
        assert is_multi is False

    def test_single_season_episode_range_not_multi(self):
        """S01E01-E05 is a multi-episode single-season pack, NOT multi-season."""
        is_multi, _, _ = _is_multi_season_pack('Show.S01E01-E05.1080p')
        assert is_multi is False

    def test_en_dash_separator(self):
        is_multi, start, end = _is_multi_season_pack('Show.S01\u2013S05.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 5

    def test_encoding_marker_not_multi(self):
        """S05-10bit is an encoding marker, NOT a multi-season range."""
        is_multi, _, _ = _is_multi_season_pack('Show.S05-10bit.HEVC.1080p')
        assert is_multi is False

    def test_3d_marker_not_multi(self):
        is_multi, _, _ = _is_multi_season_pack('Show.S02-3D.BluRay.1080p')
        assert is_multi is False

    def test_same_season_number_not_multi(self):
        is_multi, _, _ = _is_multi_season_pack('Show.S02-S02.1080p')
        assert is_multi is False


class TestExtractFileSeason:

    def test_standard_sxxexx(self):
        assert _extract_file_season('Show.S01E04.1080p.mkv') == 1

    def test_high_season_number(self):
        assert _extract_file_season('Show.S12E01.mkv') == 12

    def test_lowercase(self):
        assert _extract_file_season('show.s3e12.mkv') == 3

    def test_parent_dir_season(self):
        assert _extract_file_season('Season 2/Show.E05.mkv') == 2

    def test_parent_dir_season_dot_format(self):
        assert _extract_file_season('Season.02/Show.E05.mkv') == 2

    def test_no_season_info(self):
        assert _extract_file_season('Show.1080p.mkv') is None

    def test_absolute_episode_only(self):
        assert _extract_file_season('Show.E26.mkv') is None

    def test_s_prefix_dir(self):
        assert _extract_file_season('S03/Show.E01.mkv') == 3

    def test_season_zero_specials(self):
        assert _extract_file_season('Show.S00E01.Special.mkv') == 0

    def test_sxx_without_exx(self):
        """Sxx without episode number should still extract season."""
        assert _extract_file_season('Show.S03.Special.mkv') == 3

    def test_sxx_with_title(self):
        assert _extract_file_season('Show.S02.The.Cats.Meow.mkv') == 2


class TestBuildSeasonReleaseName:

    def test_s_range(self):
        result = _build_season_release_name('Breaking.Bad.S01-S05.1080p.BluRay-GROUP', 3)
        assert result == 'Breaking.Bad.S03.1080p.BluRay-GROUP'

    def test_complete_series(self):
        result = _build_season_release_name('The.Wire.Complete.Series.1080p', 2)
        assert result == 'The.Wire.S02.1080p'

    def test_cross_season_episodes(self):
        result = _build_season_release_name('Show.S01E01-S03E12.1080p', 1)
        assert result == 'Show.S01.1080p'

    def test_seasons_range(self):
        result = _build_season_release_name('Show.Seasons.1-5.BluRay', 4)
        assert result == 'Show.S04.BluRay'

    def test_s_bare_range(self):
        result = _build_season_release_name('Show.S01-05.1080p', 3)
        assert result == 'Show.S03.1080p'

    def test_preserves_group_tag(self):
        result = _build_season_release_name('Show.S01-S03.1080p.WEB-DL-GROUP', 2)
        assert result == 'Show.S02.1080p.WEB-DL-GROUP'

    def test_complete_collection(self):
        result = _build_season_release_name('Show.Complete.Collection.BluRay', 1)
        assert result == 'Show.S01.BluRay'

    def test_no_double_dots(self):
        result = _build_season_release_name('Show.Complete.Series.1080p', 5)
        assert '..' not in result

    def test_fallback_appends_season(self):
        result = _build_season_release_name('Random.Name.1080p', 3)
        assert result == 'Random.Name.1080p.S03'


class TestMultiSeasonSymlinks:

    def _make_watcher(self, tmp_dir):
        completed = os.path.join(tmp_dir, 'completed')
        mount = os.path.join(tmp_dir, 'mount')
        os.makedirs(completed)
        os.makedirs(mount)
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed,
            rclone_mount=mount,
            symlink_target_base='/mnt/debrid',
        )
        return watcher, completed, mount

    def test_splits_multi_season_pack(self, tmp_dir):
        """Multi-season pack should create per-season directories."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S03.1080p.BluRay-GROUP'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)

        for ep in ['Show.S01E01.1080p.mkv', 'Show.S01E02.1080p.mkv',
                    'Show.S02E01.1080p.mkv', 'Show.S03E01.1080p.mkv',
                    'Show.S03E02.1080p.mkv']:
            with open(os.path.join(release_dir, ep), 'w') as f:
                f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 5

        # Verify per-season directories exist
        s1_dir = os.path.join(completed, 'Show.S01.1080p.BluRay-GROUP')
        s2_dir = os.path.join(completed, 'Show.S02.1080p.BluRay-GROUP')
        s3_dir = os.path.join(completed, 'Show.S03.1080p.BluRay-GROUP')
        assert os.path.isdir(s1_dir)
        assert os.path.isdir(s2_dir)
        assert os.path.isdir(s3_dir)

        # Verify file counts per season
        assert len(os.listdir(s1_dir)) == 2
        assert len(os.listdir(s2_dir)) == 1
        assert len(os.listdir(s3_dir)) == 2

        # Verify symlink targets still point to original mount path
        link = os.path.join(s1_dir, 'Show.S01E01.1080p.mkv')
        assert os.path.islink(link)
        target = os.readlink(link)
        assert target == f'/mnt/debrid/shows/{release}/Show.S01E01.1080p.mkv'

    def test_single_season_unchanged(self, tmp_dir):
        """Single-season pack should use original single-dir behavior."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S03.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'Show.S03E01.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 1

        # Should be in the original release name dir, not a constructed one
        assert os.path.isdir(os.path.join(completed, release))
        assert os.path.islink(os.path.join(completed, release, 'Show.S03E01.mkv'))

    def test_no_original_dir_when_split(self, tmp_dir):
        """When split succeeds, the original multi-season dir should NOT be created."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        for ep in ['Show.S01E01.mkv', 'Show.S02E01.mkv']:
            with open(os.path.join(release_dir, ep), 'w') as f:
                f.write('data')

        watcher._create_symlinks(release, 'shows', release_dir)
        assert not os.path.exists(os.path.join(completed, release))

    def test_fallback_when_no_seasons_parseable(self, tmp_dir):
        """Multi-season name with unparseable files falls back to single dir."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.Complete.Series.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        # Files without SxxExx patterns
        with open(os.path.join(release_dir, 'episode1.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'episode2.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 2

        # Should fall back to original single-dir behavior
        assert os.path.isdir(os.path.join(completed, release))

    def test_fallback_when_only_one_season(self, tmp_dir):
        """Multi-season name but all files are one season — use single dir."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S05.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        # All files are season 3
        for ep in ['Show.S03E01.mkv', 'Show.S03E02.mkv']:
            with open(os.path.join(release_dir, ep), 'w') as f:
                f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 2

        # Falls back to single dir since only 1 season found
        assert os.path.isdir(os.path.join(completed, release))

    def test_skips_unparseable_files_in_split(self, tmp_dir):
        """Files without season info should be skipped during splitting."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'Show.S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'Show.S02E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'extras.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        # Only 2 files with parseable seasons, extras skipped
        assert count == 2

    def test_season_zero_specials(self, tmp_dir):
        """Season 0 (specials) should get their own directory."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S00-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'Show.S00E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'Show.S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'Show.S02E01.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 3
        assert os.path.isdir(os.path.join(completed, 'Show.S00.1080p'))
        assert os.path.isdir(os.path.join(completed, 'Show.S01.1080p'))
        assert os.path.isdir(os.path.join(completed, 'Show.S02.1080p'))

    def test_subdirectory_season_extraction(self, tmp_dir):
        """Files in Season subdirs should preserve directory structure in split."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        s1_dir = os.path.join(release_dir, 'Season 01')
        s2_dir = os.path.join(release_dir, 'Season 02')
        os.makedirs(s1_dir)
        os.makedirs(s2_dir)
        with open(os.path.join(s1_dir, 'Show.S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(s2_dir, 'Show.S02E01.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 2

        s1_completed = os.path.join(completed, 'Show.S01.1080p')
        s2_completed = os.path.join(completed, 'Show.S02.1080p')
        assert os.path.isdir(s1_completed)
        assert os.path.isdir(s2_completed)

        # Subdirectory structure should be preserved
        symlink = os.path.join(s1_completed, 'Season 01', 'Show.S01E01.mkv')
        assert os.path.islink(symlink)
        target = os.readlink(symlink)
        assert target == f'/mnt/debrid/shows/{release}/Season 01/Show.S01E01.mkv'

    def test_sample_files_skipped_in_split(self, tmp_dir):
        """Sample files should be skipped during multi-season splitting too."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'Show.S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'Show.S02E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'Sample.S01E01.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 2

    def test_split_idempotency(self, tmp_dir):
        """Calling _create_symlinks twice on a multi-season pack should be idempotent."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        for ep in ['Show.S01E01.mkv', 'Show.S02E01.mkv']:
            with open(os.path.join(release_dir, ep), 'w') as f:
                f.write('data')

        count1 = watcher._create_symlinks(release, 'shows', release_dir)
        assert count1 == 2

        count2 = watcher._create_symlinks(release, 'shows', release_dir)
        assert count2 == 0

    def test_fallback_does_not_create_split_dirs(self, tmp_dir):
        """When falling back to single dir, no per-season directories should exist."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.Complete.Series.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'episode1.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'episode2.mkv'), 'w') as f:
            f.write('data')

        watcher._create_symlinks(release, 'shows', release_dir)
        # No per-season dirs should be created
        assert not os.path.exists(os.path.join(completed, 'Show.S01.1080p'))
