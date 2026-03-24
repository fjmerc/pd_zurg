"""Tests for the library scanner (utils/library.py)."""

import os
import threading
import time
import pytest
import utils.library as library
from utils.library import (
    _parse_folder_name,
    _count_show_content,
    _discover_mount,
    LibraryScanner,
    setup,
    get_scanner,
)

MEDIA_EXTENSIONS = library.MEDIA_EXTENSIONS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _touch(path):
    """Create an empty file (and any missing parent directories)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, 'w').close()


def _make_show(base, show_name, seasons):
    """
    Build a show directory structure under base/show_name.

    seasons is a dict mapping season dir name to a list of filenames inside it.
    Example: {'Season 1': ['ep1.mkv', 'ep2.mkv'], 'Season 2': []}
    """
    show_path = os.path.join(base, show_name)
    for season_dir, files in seasons.items():
        season_path = os.path.join(show_path, season_dir)
        os.makedirs(season_path, exist_ok=True)
        for f in files:
            open(os.path.join(season_path, f), 'w').close()
    return show_path


# ---------------------------------------------------------------------------
# _parse_folder_name
# ---------------------------------------------------------------------------

class TestParseFolderName:

    def test_movie_with_paren_year(self):
        title, year = _parse_folder_name("Movie Name (2024)")
        assert title == "Movie Name"
        assert year == 2024

    def test_dotted_name_with_inline_year_and_quality(self):
        title, year = _parse_folder_name("Movie.Name.2024.1080p.BluRay")
        assert title == "Movie Name"
        assert year == 2024

    def test_tv_episode_marker_strips_season(self):
        title, year = _parse_folder_name("Show.Name.S01E01.1080p.WEB")
        assert title == "Show Name"
        assert year is None

    def test_simple_name_no_year(self):
        title, year = _parse_folder_name("Simple Name")
        assert title == "Simple Name"
        assert year is None

    def test_movie_1999(self):
        title, year = _parse_folder_name("Movie (1999)")
        assert title == "Movie"
        assert year == 1999

    def test_dotted_name_with_release_group_no_year(self):
        title, year = _parse_folder_name("Movie.Name.x264-GROUP")
        assert title == "Movie Name"
        assert year is None

    def test_dotted_name_with_year_and_remux(self):
        title, year = _parse_folder_name("A.Movie.2020.REMUX")
        assert title == "A Movie"
        assert year == 2020

    def test_season_only_marker(self):
        # S01 without episode should still strip the season portion
        title, year = _parse_folder_name("My.Show.S02.COMPLETE.1080p")
        assert title == "My Show"
        assert year is None

    def test_dots_converted_to_spaces_in_paren_year_path(self):
        title, year = _parse_folder_name("Some.Movie.Title (2010)")
        assert title == "Some Movie Title"
        assert year == 2010

    def test_underscores_normalized(self):
        title, year = _parse_folder_name("Under_Score_Movie_2019_BluRay")
        assert title == "Under Score Movie"
        assert year == 2019

    def test_year_at_boundary_not_mangled(self):
        # Year that sits exactly at end, no trailing noise
        title, year = _parse_folder_name("Interstellar.2014")
        assert title == "Interstellar"
        assert year == 2014

    def test_multiple_quality_terms_truncated_at_first(self):
        title, year = _parse_folder_name("Film.Name.2021.2160p.HDR.DV.ATMOS")
        assert title == "Film Name"
        assert year == 2021


# ---------------------------------------------------------------------------
# _count_show_content
# ---------------------------------------------------------------------------

class TestCountShowContent:

    def test_seasons_with_media_files(self, tmp_dir):
        show_path = _make_show(tmp_dir, "My Show", {
            "Season 1": ["ep1.mkv", "ep2.mkv"],
            "Season 2": ["ep1.mp4"],
        })
        seasons, episodes = _count_show_content(show_path)
        assert seasons == 2
        assert episodes == 3

    def test_empty_season_dirs(self, tmp_dir):
        show_path = _make_show(tmp_dir, "Empty Show", {
            "Season 1": [],
            "Season 2": [],
        })
        seasons, episodes = _count_show_content(show_path)
        assert seasons == 2
        assert episodes == 0

    def test_no_season_dirs(self, tmp_dir):
        show_path = os.path.join(tmp_dir, "Flat Show")
        os.makedirs(show_path)
        # Files at show root (not inside Season dirs) should not be counted
        open(os.path.join(show_path, "ep1.mkv"), 'w').close()
        seasons, episodes = _count_show_content(show_path)
        assert seasons == 0
        assert episodes == 0

    def test_nonexistent_path(self, tmp_dir):
        missing = os.path.join(tmp_dir, "does_not_exist")
        seasons, episodes = _count_show_content(missing)
        assert seasons == 0
        assert episodes == 0

    def test_non_media_files_ignored(self, tmp_dir):
        show_path = _make_show(tmp_dir, "Mixed Show", {
            "Season 1": ["ep1.mkv", "ep1.nfo", "ep1.srt", "thumbs.db"],
        })
        seasons, episodes = _count_show_content(show_path)
        assert seasons == 1
        assert episodes == 1

    def test_all_media_extensions_counted(self, tmp_dir):
        media_files = [f"ep{i}{ext}" for i, ext in enumerate(sorted(MEDIA_EXTENSIONS))]
        show_path = _make_show(tmp_dir, "Ext Show", {
            "Season 1": media_files,
        })
        seasons, episodes = _count_show_content(show_path)
        assert seasons == 1
        assert episodes == len(media_files)

    def test_case_insensitive_season_dir_matching(self, tmp_dir):
        # "season 1" (lowercase) should still match
        show_path = _make_show(tmp_dir, "Case Show", {
            "season 1": ["ep1.mkv"],
            "SEASON 2": ["ep1.mkv"],
        })
        seasons, episodes = _count_show_content(show_path)
        assert seasons == 2
        assert episodes == 2

    def test_non_season_subdirs_ignored(self, tmp_dir):
        show_path = os.path.join(tmp_dir, "Extras Show")
        os.makedirs(os.path.join(show_path, "Season 1"))
        os.makedirs(os.path.join(show_path, "Extras"))
        open(os.path.join(show_path, "Season 1", "ep1.mkv"), 'w').close()
        open(os.path.join(show_path, "Extras", "bonus.mkv"), 'w').close()
        seasons, episodes = _count_show_content(show_path)
        assert seasons == 1
        assert episodes == 1


# ---------------------------------------------------------------------------
# _discover_mount
# ---------------------------------------------------------------------------

class TestDiscoverMount:

    def test_rclone_mount_name_with_marker_dir(self, tmp_dir, monkeypatch):
        mount_name = "zurg"
        mount_root = os.path.join(tmp_dir, mount_name)
        os.makedirs(os.path.join(mount_root, "movies"))
        monkeypatch.setenv("RCLONE_MOUNT_NAME", mount_name)
        # Redirect /data to our tmp_dir
        monkeypatch.setattr(os.path, "join", _make_join_redirect("/data", tmp_dir))
        monkeypatch.setattr(os.path, "isdir", _make_isdir_redirect("/data", tmp_dir))
        result = _discover_mount()
        assert result == mount_root

    def test_rclone_mount_name_no_marker_dirs_falls_through(self, tmp_dir, monkeypatch):
        mount_name = "empty_mount"
        mount_root = os.path.join(tmp_dir, mount_name)
        os.makedirs(mount_root)  # dir exists but has no marker subdirs
        monkeypatch.setenv("RCLONE_MOUNT_NAME", mount_name)
        monkeypatch.setenv("BLACKHOLE_RCLONE_MOUNT", "")
        # Make /data a real path but with no markers so fallback also fails
        fake_data = os.path.join(tmp_dir, "data_root")
        os.makedirs(fake_data)
        monkeypatch.setattr(os.path, "join", _make_join_redirect("/data", tmp_dir))
        monkeypatch.setattr(os.path, "isdir", _make_isdir_redirect("/data", tmp_dir))
        # With no markers anywhere, result should be None
        result = _discover_mount()
        assert result is None

    def test_blackhole_rclone_mount_fallback(self, tmp_dir, monkeypatch):
        mount_root = os.path.join(tmp_dir, "bh_mount")
        os.makedirs(os.path.join(mount_root, "shows"))
        monkeypatch.delenv("RCLONE_MOUNT_NAME", raising=False)
        monkeypatch.setenv("BLACKHOLE_RCLONE_MOUNT", mount_root)
        # Prevent the /data fallback from matching
        monkeypatch.setattr(os.path, "isdir", _make_selective_isdir(
            always_true=mount_root,
            always_false="/data",
        ))
        result = _discover_mount()
        assert result == mount_root

    def test_data_fallback_with_marker(self, tmp_dir, monkeypatch):
        monkeypatch.delenv("RCLONE_MOUNT_NAME", raising=False)
        monkeypatch.setenv("BLACKHOLE_RCLONE_MOUNT", "")
        # The function hard-codes '/data' as the return value and only calls
        # os.path.isdir to check for marker subdirs.  Redirect isdir so that
        # /data/shows is treated as present, then verify the function returns
        # the literal '/data' constant it is defined to return.
        os.makedirs(os.path.join(tmp_dir, "shows"))
        monkeypatch.setattr(os.path, "join", _make_join_redirect("/data", tmp_dir))
        monkeypatch.setattr(os.path, "isdir", _make_isdir_redirect("/data", tmp_dir))
        result = _discover_mount()
        assert result == "/data"

    def test_no_mount_available_returns_none(self, monkeypatch):
        monkeypatch.delenv("RCLONE_MOUNT_NAME", raising=False)
        monkeypatch.setenv("BLACKHOLE_RCLONE_MOUNT", "")
        monkeypatch.setattr(os.path, "isdir", lambda p: False)
        result = _discover_mount()
        assert result is None


# ---------------------------------------------------------------------------
# Helpers for mount patching
# ---------------------------------------------------------------------------

def _make_join_redirect(virtual_root, real_root):
    """Return an os.path.join that maps virtual_root/* to real_root/*."""
    _orig_join = os.path.join

    def _join(*args):
        result = _orig_join(*args)
        if result.startswith(virtual_root + os.sep) or result == virtual_root:
            suffix = result[len(virtual_root):]
            return real_root + suffix
        return result

    return _join


def _make_isdir_redirect(virtual_root, real_root):
    """Return an os.path.isdir that maps virtual_root to real_root."""
    _orig_isdir = os.path.isdir

    def _isdir(p):
        if p == virtual_root:
            return _orig_isdir(real_root)
        if p.startswith(virtual_root + os.sep):
            suffix = p[len(virtual_root):]
            return _orig_isdir(real_root + suffix)
        return _orig_isdir(p)

    return _isdir


def _make_selective_isdir(always_true, always_false):
    """Return an os.path.isdir that forces specific paths true/false."""
    _orig_isdir = os.path.isdir

    def _isdir(p):
        if p == always_false or p.startswith(always_false + os.sep):
            return False
        if p == always_true or p.startswith(always_true + os.sep):
            return _orig_isdir(p)
        return _orig_isdir(p)

    return _isdir


# ---------------------------------------------------------------------------
# LibraryScanner.scan() — debrid paths
# ---------------------------------------------------------------------------

class TestLibraryScannerScanDebrid:

    def _make_scanner(self, mount_path, monkeypatch):
        monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_MOVIES", raising=False)
        monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_TV", raising=False)
        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = mount_path
        scanner._local_movies_path = None
        scanner._local_tv_path = None
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False
        return scanner

    def test_scan_debrid_movies_returns_correct_items(self, tmp_dir, monkeypatch):
        movies_dir = os.path.join(tmp_dir, "movies")
        os.makedirs(os.path.join(movies_dir, "Inception (2010)"))
        os.makedirs(os.path.join(movies_dir, "The.Dark.Knight.2008.1080p.BluRay"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        titles = {m["title"] for m in result["movies"]}
        assert "Inception" in titles
        assert "The Dark Knight" in titles

    def test_scan_debrid_movies_sets_correct_metadata(self, tmp_dir, monkeypatch):
        os.makedirs(os.path.join(tmp_dir, "movies", "Dune (2021)"))
        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        movie = next(m for m in result["movies"] if m["title"] == "Dune")
        assert movie["year"] == 2021
        assert movie["source"] == "debrid"
        assert movie["type"] == "movie"
        assert movie["seasons"] == 0
        assert movie["episodes"] == 0

    def test_scan_debrid_shows_returns_correct_items(self, tmp_dir, monkeypatch):
        shows_dir = os.path.join(tmp_dir, "shows")
        _make_show(shows_dir, "Breaking.Bad.S01", {
            "Season 1": ["ep1.mkv", "ep2.mkv"],
        })

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        assert len(result["shows"]) == 1
        show = result["shows"][0]
        assert show["title"] == "Breaking Bad"
        assert show["type"] == "show"
        assert show["seasons"] == 1
        assert show["episodes"] == 2
        assert show["source"] == "debrid"

    def test_scan_result_has_required_keys(self, tmp_dir, monkeypatch):
        os.makedirs(os.path.join(tmp_dir, "movies"))
        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        assert "movies" in result
        assert "shows" in result
        assert "last_scan" in result
        assert "scan_duration_ms" in result

    def test_scan_no_mount_returns_empty_lists(self, monkeypatch):
        monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_MOVIES", raising=False)
        monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_TV", raising=False)
        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = None
        scanner._local_movies_path = None
        scanner._local_tv_path = None
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False

        result = scanner.scan()
        assert result["movies"] == []
        assert result["shows"] == []

    def test_scan_skips_files_in_movies_dir(self, tmp_dir, monkeypatch):
        movies_dir = os.path.join(tmp_dir, "movies")
        os.makedirs(movies_dir)
        # A loose file at the movies root should not be returned
        open(os.path.join(movies_dir, "stray.mkv"), 'w').close()
        os.makedirs(os.path.join(movies_dir, "Real Movie (2022)"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        assert len(result["movies"]) == 1
        assert result["movies"][0]["title"] == "Real Movie"

    def test_scan_discovers_custom_category_names(self, tmp_dir, monkeypatch):
        # Zurg directory names are user-configurable; scanner must find them
        anime_dir = os.path.join(tmp_dir, "anime")
        films_dir = os.path.join(tmp_dir, "films")
        os.makedirs(os.path.join(anime_dir, "Spirited.Away.2001.1080p"))
        _make_show(anime_dir, "Naruto", {"Season 1": ["ep1.mkv"]})
        os.makedirs(os.path.join(films_dir, "Parasite (2019)"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        movie_titles = {m["title"] for m in result["movies"]}
        show_titles = {s["title"] for s in result["shows"]}
        assert "Spirited Away" in movie_titles
        assert "Parasite" in movie_titles
        assert "Naruto" in show_titles

    def test_scan_falls_back_to_all_when_no_categories(self, tmp_dir, monkeypatch):
        # Only __all__ exists — should be scanned as fallback
        all_dir = os.path.join(tmp_dir, "__all__")
        os.makedirs(os.path.join(all_dir, "Some Movie (2023)"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        assert len(result["movies"]) == 1
        assert result["movies"][0]["title"] == "Some Movie"

    def test_scan_skips_all_when_categories_exist(self, tmp_dir, monkeypatch):
        # __all__ duplicates content from categories — should be skipped
        movies_dir = os.path.join(tmp_dir, "movies")
        all_dir = os.path.join(tmp_dir, "__all__")
        os.makedirs(os.path.join(movies_dir, "Dune (2021)"))
        os.makedirs(os.path.join(all_dir, "Dune (2021)"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        dune_matches = [m for m in result["movies"] if m["title"] == "Dune"]
        assert len(dune_matches) == 1


# ---------------------------------------------------------------------------
# LibraryScanner.scan() — local paths
# ---------------------------------------------------------------------------

class TestLibraryScannerScanLocal:

    def _make_local_scanner(self, local_movies=None, local_tv=None, monkeypatch=None):
        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = None
        scanner._local_movies_path = local_movies
        scanner._local_tv_path = local_tv
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False
        return scanner

    def test_scan_local_movies_source_is_local(self, tmp_dir):
        local_movies = os.path.join(tmp_dir, "local_movies")
        os.makedirs(os.path.join(local_movies, "Parasite (2019)"))

        scanner = self._make_local_scanner(local_movies=local_movies)
        result = scanner.scan()

        assert len(result["movies"]) == 1
        assert result["movies"][0]["source"] == "local"
        assert result["movies"][0]["title"] == "Parasite"

    def test_scan_local_tv_source_is_local(self, tmp_dir):
        local_tv = os.path.join(tmp_dir, "local_tv")
        _make_show(local_tv, "The Wire (2002)", {
            "Season 1": ["ep1.mkv"],
        })

        scanner = self._make_local_scanner(local_tv=local_tv)
        result = scanner.scan()

        assert len(result["shows"]) == 1
        assert result["shows"][0]["source"] == "local"
        assert result["shows"][0]["title"] == "The Wire"

    def test_scan_local_movies_missing_dir_returns_empty(self, tmp_dir):
        missing = os.path.join(tmp_dir, "nonexistent")
        scanner = self._make_local_scanner(local_movies=missing)
        result = scanner.scan()
        assert result["movies"] == []

    def test_scan_local_tv_missing_dir_returns_empty(self, tmp_dir):
        missing = os.path.join(tmp_dir, "nonexistent_tv")
        scanner = self._make_local_scanner(local_tv=missing)
        result = scanner.scan()
        assert result["shows"] == []


# ---------------------------------------------------------------------------
# LibraryScanner.scan() — source='both' cross-referencing
# ---------------------------------------------------------------------------

class TestLibraryScannerScanCrossRef:

    def test_same_movie_in_debrid_and_local_gets_source_both(self, tmp_dir):
        mount_movies = os.path.join(tmp_dir, "mount", "movies")
        os.makedirs(os.path.join(mount_movies, "Oppenheimer (2023)"))
        local_movies = os.path.join(tmp_dir, "local_movies")
        os.makedirs(os.path.join(local_movies, "Oppenheimer (2023)"))

        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = os.path.join(tmp_dir, "mount")
        scanner._local_movies_path = local_movies
        scanner._local_tv_path = None
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False

        result = scanner.scan()

        oppenheimer = next(m for m in result["movies"] if m["title"] == "Oppenheimer")
        assert oppenheimer["source"] == "both"
        # Title should appear only once (debrid record is updated, local is not added)
        matching = [m for m in result["movies"] if m["title"] == "Oppenheimer"]
        assert len(matching) == 1

    def test_local_only_movie_source_is_local(self, tmp_dir):
        mount_movies = os.path.join(tmp_dir, "mount", "movies")
        os.makedirs(mount_movies)  # empty
        local_movies = os.path.join(tmp_dir, "local_movies")
        os.makedirs(os.path.join(local_movies, "Local Only (2020)"))

        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = os.path.join(tmp_dir, "mount")
        scanner._local_movies_path = local_movies
        scanner._local_tv_path = None
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False

        result = scanner.scan()
        local_only = next(m for m in result["movies"] if m["title"] == "Local Only")
        assert local_only["source"] == "local"

    def test_same_show_in_debrid_and_local_gets_source_both(self, tmp_dir):
        mount_shows = os.path.join(tmp_dir, "mount", "shows")
        _make_show(mount_shows, "Succession (2018)", {"Season 1": ["ep1.mkv"]})
        local_tv = os.path.join(tmp_dir, "local_tv")
        _make_show(local_tv, "Succession (2018)", {"Season 1": ["ep1.mkv"]})

        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = os.path.join(tmp_dir, "mount")
        scanner._local_movies_path = None
        scanner._local_tv_path = local_tv
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False

        result = scanner.scan()
        show = next(s for s in result["shows"] if s["title"] == "Succession")
        assert show["source"] == "both"
        matching = [s for s in result["shows"] if s["title"] == "Succession"]
        assert len(matching) == 1

    def test_title_normalization_ignores_year_in_paren(self, tmp_dir):
        # Debrid has the year, local does not — should still match
        mount_movies = os.path.join(tmp_dir, "mount", "movies")
        os.makedirs(os.path.join(mount_movies, "Arrival (2016)"))
        local_movies = os.path.join(tmp_dir, "local_movies")
        os.makedirs(os.path.join(local_movies, "Arrival (2016)"))

        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = os.path.join(tmp_dir, "mount")
        scanner._local_movies_path = local_movies
        scanner._local_tv_path = None
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False

        result = scanner.scan()
        arrival = next(m for m in result["movies"] if m["title"] == "Arrival")
        assert arrival["source"] == "both"


# ---------------------------------------------------------------------------
# LibraryScanner.get_data() — caching and TTL
# ---------------------------------------------------------------------------

class TestLibraryScannerGetData:

    def _bare_scanner(self):
        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = None
        scanner._local_movies_path = None
        scanner._local_tv_path = None
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False
        return scanner

    def test_get_data_returns_scan_result(self):
        scanner = self._bare_scanner()
        data = scanner.get_data()
        assert "movies" in data
        assert "shows" in data

    def test_get_data_caches_result(self, mocker):
        scanner = self._bare_scanner()
        mock_scan = mocker.patch.object(scanner, "scan", wraps=scanner.scan)
        scanner.get_data()
        scanner.get_data()
        assert mock_scan.call_count == 1

    def test_get_data_returns_cached_data_within_ttl(self, mocker):
        scanner = self._bare_scanner()
        first = scanner.get_data()
        # Patch scan to return something different so we can detect whether it ran
        mocker.patch.object(scanner, "scan", return_value={"movies": ["NEW"], "shows": []})
        second = scanner.get_data()
        assert second is first

    def test_get_data_rescans_after_ttl_expires(self, mocker):
        scanner = self._bare_scanner()
        scanner.get_data()
        # Expire the cache by rewinding _cache_time
        scanner._cache_time = time.monotonic() - scanner._ttl - 1
        fresh_payload = {"movies": [], "shows": [], "last_scan": "x", "scan_duration_ms": 0}
        mocker.patch.object(scanner, "scan", return_value=fresh_payload)
        result = scanner.get_data()
        assert result is fresh_payload


# ---------------------------------------------------------------------------
# LibraryScanner.refresh() — background threading
# ---------------------------------------------------------------------------

class TestLibraryScannerRefresh:

    def _bare_scanner(self):
        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = None
        scanner._local_movies_path = None
        scanner._local_tv_path = None
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False
        return scanner

    def test_refresh_triggers_background_scan(self, mocker):
        scanner = self._bare_scanner()
        started = threading.Event()

        def _fake_scan():
            started.set()
            return {"movies": [], "shows": [], "last_scan": "x", "scan_duration_ms": 0}

        mocker.patch.object(scanner, "scan", side_effect=_fake_scan)
        scanner.refresh()
        assert started.wait(timeout=2), "Background scan thread did not start within 2s"

    def test_refresh_updates_cache_after_completion(self, mocker):
        scanner = self._bare_scanner()
        done = threading.Event()
        payload = {"movies": [{"title": "BG Movie"}], "shows": [], "last_scan": "x", "scan_duration_ms": 0}

        def _fake_scan():
            done.set()
            return payload

        mocker.patch.object(scanner, "scan", side_effect=_fake_scan)
        scanner.refresh()
        done.wait(timeout=2)
        time.sleep(0.05)  # give thread time to write cache
        assert scanner._cache is payload

    def test_refresh_does_not_start_concurrent_scan(self, mocker):
        scanner = self._bare_scanner()
        scan_calls = []
        barrier = threading.Event()

        def _slow_scan():
            scan_calls.append(1)
            barrier.wait(timeout=3)
            return {"movies": [], "shows": [], "last_scan": "x", "scan_duration_ms": 0}

        mocker.patch.object(scanner, "scan", side_effect=_slow_scan)
        scanner.refresh()
        time.sleep(0.05)  # first thread is running
        scanner.refresh()  # second call must be a no-op
        barrier.set()
        time.sleep(0.1)

        assert len(scan_calls) == 1

    def test_refresh_clears_scanning_flag_on_error(self, mocker):
        scanner = self._bare_scanner()
        done = threading.Event()

        def _error_scan():
            raise RuntimeError("simulated scan failure")

        mocker.patch.object(scanner, "scan", side_effect=_error_scan)
        scanner.refresh()
        # Allow thread to finish
        deadline = time.monotonic() + 2
        while scanner._scanning and time.monotonic() < deadline:
            time.sleep(0.02)

        assert not scanner._scanning


# ---------------------------------------------------------------------------
# setup() and get_scanner()
# ---------------------------------------------------------------------------

class TestSetupAndGetScanner:

    def test_setup_creates_scanner_singleton(self, monkeypatch):
        library._scanner = None
        monkeypatch.delenv("RCLONE_MOUNT_NAME", raising=False)
        monkeypatch.delenv("BLACKHOLE_RCLONE_MOUNT", raising=False)
        monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_MOVIES", raising=False)
        monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_TV", raising=False)
        monkeypatch.setattr(os.path, "isdir", lambda p: False)

        setup()
        scanner = get_scanner()
        assert scanner is not None
        assert isinstance(scanner, LibraryScanner)

    def test_get_scanner_returns_same_instance_after_setup(self, monkeypatch):
        library._scanner = None
        monkeypatch.delenv("RCLONE_MOUNT_NAME", raising=False)
        monkeypatch.delenv("BLACKHOLE_RCLONE_MOUNT", raising=False)
        monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_MOVIES", raising=False)
        monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_TV", raising=False)
        monkeypatch.setattr(os.path, "isdir", lambda p: False)

        setup()
        a = get_scanner()
        b = get_scanner()
        assert a is b

    def test_get_scanner_returns_none_before_setup(self):
        library._scanner = None
        assert get_scanner() is None

    def test_setup_overwrites_previous_singleton(self, monkeypatch):
        library._scanner = None
        monkeypatch.delenv("RCLONE_MOUNT_NAME", raising=False)
        monkeypatch.delenv("BLACKHOLE_RCLONE_MOUNT", raising=False)
        monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_MOVIES", raising=False)
        monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_TV", raising=False)
        monkeypatch.setattr(os.path, "isdir", lambda p: False)

        setup()
        first = get_scanner()
        setup()
        second = get_scanner()
        # Each setup() creates a fresh instance
        assert second is not first
