"""Tests for the library scanner (utils/library.py)."""

import os
import threading
import time
import pytest
import utils.library as library
from utils.library import (
    _parse_folder_name,
    _clean_title,
    _count_show_content,
    _collect_episodes,
    _build_season_data,
    _discover_mount,
    _norm_for_matching,
    get_wanted_counts,
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

    def test_site_prefix_stripped(self):
        title, year = _parse_folder_name("www.UIndex.org.12.Monkeys.S01E01.1080p.WEB")
        assert title == "12 Monkeys"
        assert year is None

    def test_site_prefix_various_tlds(self):
        title, _ = _parse_folder_name("www.RARBG.com.Movie.Name.2020.1080p")
        assert title == "Movie Name"

    def test_bracket_tag_stripped(self):
        title, _ = _parse_folder_name("[TorrentDay] Some.Show.S02E03.720p")
        assert title == "Some Show"

    # Season text stripping
    def test_season_text_stripped_dotted(self):
        title, year = _parse_folder_name("Arrested.Development.(2003).Season.1.1080p")
        assert title == "Arrested Development"
        assert year == 2003

    def test_season_text_stripped_spaced(self):
        title, year = _parse_folder_name("Arrested Development (2003) Season 1")
        assert title == "Arrested Development"
        assert year == 2003

    def test_season_range_stripped(self):
        title, year = _parse_folder_name("12.Monkeys.Season.1.to.4.Mp4")
        assert title == "12 Monkeys"

    def test_season_dash_range_stripped(self):
        title, _ = _parse_folder_name("Vice.Principals.Season.1-2.1080p")
        assert title == "Vice Principals"

    def test_seasons_plural_stripped(self):
        title, _ = _parse_folder_name("Show.Name.Seasons.1.and.2.WEB")
        assert title == "Show Name"

    def test_s01_s02_range_stripped(self):
        title, _ = _parse_folder_name("Show.Name.S01-S03.COMPLETE")
        assert title == "Show Name"

    def test_container_suffix_stripped(self):
        title, _ = _parse_folder_name("Some.Show.Season.1.Mp4")
        assert title == "Some Show"

    def test_complete_suffix_stripped(self):
        title, _ = _parse_folder_name("Show.Name.S01.Complete")
        assert title == "Show Name"

    def test_mid_year_extracted(self):
        title, year = _parse_folder_name("iCarly (2021) Season 2")
        assert title == "iCarly"
        assert year == 2021

    def test_trailing_bare_year_extracted(self):
        title, year = _parse_folder_name("iCarly 2020")
        assert title == "iCarly"
        assert year == 2020

    def test_trailing_year_preserves_numeric_title(self):
        # "1883" is all digits — trailing year should not eat the entire title
        title, year = _parse_folder_name("1883")
        assert title == "1883"

    def test_extras_suffix_stripped(self):
        title, _ = _parse_folder_name("Show.Name.S01.1080p + Extras")
        assert title == "Show Name"


# ---------------------------------------------------------------------------
# _clean_title
# ---------------------------------------------------------------------------

class TestCleanTitle:

    def test_passthrough_simple(self):
        assert _clean_title("Movie Name", 2020) == ("Movie Name", 2020)

    def test_strips_season_text(self):
        title, year = _clean_title("Show.Season.3", None)
        assert title == "Show"

    def test_strips_season_range_to(self):
        title, _ = _clean_title("Show.Season.1.to.4", None)
        assert title == "Show"

    def test_extracts_mid_year(self):
        title, year = _clean_title("Show (2003) leftover", None)
        assert title == "Show leftover"
        assert year == 2003

    def test_does_not_overwrite_existing_year(self):
        title, year = _clean_title("Show (2003)", 2005)
        # Existing year takes priority
        assert year == 2005

    def test_trailing_year_needs_nonempty_remainder(self):
        title, year = _clean_title("1999", None)
        # "1999" alone — stripping it would leave empty title
        assert title == "1999"
        assert year is None


# ---------------------------------------------------------------------------
# _collect_episodes
# ---------------------------------------------------------------------------

class TestCollectEpisodes:

    def test_flat_episode_files(self, tmp_dir):
        folder = os.path.join(tmp_dir, "show")
        os.makedirs(folder)
        open(os.path.join(folder, "Show.S01E01.mkv"), 'w').close()
        open(os.path.join(folder, "Show.S01E02.mkv"), 'w').close()
        open(os.path.join(folder, "Show.S02E01.mkv"), 'w').close()
        eps = _collect_episodes(folder)
        assert set(eps.keys()) == {(1, 1), (1, 2), (2, 1)}
        assert eps[(1, 1)]['file'] == "Show.S01E01.mkv"
        assert 'path' in eps[(1, 1)]

    def test_season_dir_with_episode_files(self, tmp_dir):
        folder = os.path.join(tmp_dir, "show")
        season = os.path.join(folder, "Season 1")
        os.makedirs(season)
        open(os.path.join(season, "Show.S01E01.mkv"), 'w').close()
        open(os.path.join(season, "Show.S01E02.mkv"), 'w').close()
        eps = _collect_episodes(folder)
        assert set(eps.keys()) == {(1, 1), (1, 2)}
        assert eps[(1, 1)]['file'] == "Show.S01E01.mkv"
        assert eps[(1, 2)]['file'] == "Show.S01E02.mkv"

    def test_nonexistent_path(self, tmp_dir):
        eps = _collect_episodes(os.path.join(tmp_dir, "nope"))
        assert eps == {}

    def test_non_media_files_ignored(self, tmp_dir):
        folder = os.path.join(tmp_dir, "show")
        os.makedirs(folder)
        open(os.path.join(folder, "Show.S01E01.nfo"), 'w').close()
        open(os.path.join(folder, "Show.S01E01.mkv"), 'w').close()
        eps = _collect_episodes(folder)
        assert set(eps.keys()) == {(1, 1)}

    def test_mixed_season_dirs_and_flat_files(self, tmp_dir):
        folder = os.path.join(tmp_dir, "show")
        season = os.path.join(folder, "Season 1")
        os.makedirs(season)
        open(os.path.join(season, "Show.S01E01.mkv"), 'w').close()
        open(os.path.join(folder, "Show.S02E01.mkv"), 'w').close()
        eps = _collect_episodes(folder)
        assert (1, 1) in eps
        assert (2, 1) in eps
        assert len(eps) == 2

    def test_season_dir_files_without_episode_pattern(self, tmp_dir):
        """Files in Season dirs without S##E## get sequential IDs."""
        folder = os.path.join(tmp_dir, "show")
        season = os.path.join(folder, "Season 3")
        os.makedirs(season)
        open(os.path.join(season, "episode1.mkv"), 'w').close()
        eps = _collect_episodes(folder)
        assert len(eps) == 1
        # Should be assigned to season 3 with a high sequential number
        key = list(eps.keys())[0]
        assert key[0] == 3
        assert key[1] >= 1000


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

    def test_flat_episode_files_detected(self, tmp_dir):
        show_path = os.path.join(tmp_dir, "Flat Show")
        os.makedirs(show_path)
        open(os.path.join(show_path, "Show.Name.S03E01.1080p.mkv"), 'w').close()
        open(os.path.join(show_path, "Show.Name.S03E02.1080p.mkv"), 'w').close()
        open(os.path.join(show_path, "Show.Name.S03E03.1080p.mkv"), 'w').close()
        seasons, episodes = _count_show_content(show_path)
        assert seasons == 1
        assert episodes == 3

    def test_flat_non_episode_media_not_counted(self, tmp_dir):
        show_path = os.path.join(tmp_dir, "Not Episodes")
        os.makedirs(show_path)
        # Media file without episode pattern — should not count as episode
        open(os.path.join(show_path, "movie.mkv"), 'w').close()
        open(os.path.join(show_path, "bonus.mp4"), 'w').close()
        seasons, episodes = _count_show_content(show_path)
        assert seasons == 0
        assert episodes == 0

    def test_season_dirs_take_priority_over_flat_episodes(self, tmp_dir):
        show_path = os.path.join(tmp_dir, "Mixed Show")
        os.makedirs(os.path.join(show_path, "Season 1"))
        open(os.path.join(show_path, "Season 1", "ep1.mkv"), 'w').close()
        # Flat episode file alongside Season dir — Season dirs win
        open(os.path.join(show_path, "S01E01.mkv"), 'w').close()
        seasons, episodes = _count_show_content(show_path)
        assert seasons == 1
        assert episodes == 1  # Only counts from Season dirs


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
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False
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
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False

        result = scanner.scan()
        assert result["movies"] == []
        assert result["shows"] == []

    def test_scan_retries_mount_discovery_when_none(self, tmp_dir, monkeypatch):
        # Simulates mount appearing after scanner was created (race condition fix)
        scanner = self._make_scanner(None, monkeypatch)
        assert scanner._mount_path is None

        # Now create a mount structure and patch _discover_mount to find it
        movies_dir = os.path.join(tmp_dir, "movies")
        os.makedirs(os.path.join(movies_dir, "Late Movie (2024)"))
        monkeypatch.setattr(library, '_discover_mount', lambda: tmp_dir)

        result = scanner.scan()
        assert scanner._mount_path == tmp_dir
        assert len(result["movies"]) == 1
        assert result["movies"][0]["title"] == "Late Movie"

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
        _make_show(anime_dir, "Naruto", {"Season 1": ["ep1.mkv"]})
        os.makedirs(os.path.join(films_dir, "Parasite (2019)"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        movie_titles = {m["title"] for m in result["movies"]}
        show_titles = {s["title"] for s in result["shows"]}
        assert "Parasite" in movie_titles
        assert "Naruto" in show_titles

    def test_category_name_classifies_flat_shows(self, tmp_dir, monkeypatch):
        # Items under 'shows'/'anime' category should be classified as shows
        # even without Season subdirs (flat episode files)
        shows_dir = os.path.join(tmp_dir, "shows")
        show_folder = os.path.join(shows_dir, "Silo.S02.1080p")
        os.makedirs(show_folder)
        open(os.path.join(show_folder, "Silo.S02E01.mkv"), 'w').close()
        open(os.path.join(show_folder, "Silo.S02E02.mkv"), 'w').close()

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        assert len(result["shows"]) == 1
        assert result["shows"][0]["title"] == "Silo"
        assert result["shows"][0]["episodes"] == 2
        assert len(result["movies"]) == 0

    def test_anime_category_with_media_files_classifies_as_show(self, tmp_dir, monkeypatch):
        anime_dir = os.path.join(tmp_dir, "anime")
        folder = os.path.join(anime_dir, "[SubGroup] Spirited Away [1080p][ABCD1234]")
        os.makedirs(folder)
        # Anime with media files but no S##E## pattern — trust category hint
        open(os.path.join(folder, "[SubGroup] Spirited Away [1080p][ABCD1234].mkv"), "w").close()

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        assert len(result["shows"]) == 1
        assert "Spirited Away" in result["shows"][0]["title"]
        assert len(result["movies"]) == 0

    def test_anime_category_with_media_in_subdir_classifies_as_show(self, tmp_dir, monkeypatch):
        """Anime with media files in a non-Season subdir still stays a show."""
        anime_dir = os.path.join(tmp_dir, "anime")
        folder = os.path.join(anime_dir, "Spirited.Away.2001.1080p")
        arc_dir = os.path.join(folder, "Part 1")
        os.makedirs(arc_dir)
        open(os.path.join(arc_dir, "01.mkv"), "w").close()

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        assert len(result["shows"]) == 1
        assert len(result["movies"]) == 0

    def test_bluray_rip_no_media_files_demoted_to_movie(self, tmp_dir, monkeypatch):
        """BluRay disc rip with .m2ts in BDMV/STREAM/ (not in MEDIA_EXTENSIONS) → movie."""
        shows_dir = os.path.join(tmp_dir, "shows")
        folder = os.path.join(shows_dir, "21.Jump.Street.2012.2160p.BluRay.HEVC.TrueHD.7.1.Atmos-EATDIK")
        bdmv = os.path.join(folder, "BDMV", "STREAM")
        os.makedirs(bdmv)
        open(os.path.join(bdmv, "00100.m2ts"), "w").close()
        open(os.path.join(bdmv, "00101.m2ts"), "w").close()

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        assert len(result["shows"]) == 0
        assert len(result["movies"]) == 1
        assert result["movies"][0]["title"] == "21 Jump Street"

    def test_scan_skips_unplayable_category(self, tmp_dir, monkeypatch):
        movies_dir = os.path.join(tmp_dir, "movies")
        unplayable_dir = os.path.join(tmp_dir, "__unplayable__")
        os.makedirs(os.path.join(movies_dir, "Good Movie (2023)"))
        os.makedirs(os.path.join(unplayable_dir, "Bad File"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        all_titles = {m["title"] for m in result["movies"]} | {s["title"] for s in result["shows"]}
        assert "Good Movie" in all_titles
        assert "Bad File" not in all_titles

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

    def test_show_aggregation_merges_duplicate_titles(self, tmp_dir, monkeypatch):
        shows_dir = os.path.join(tmp_dir, "shows")
        # Multiple torrent folders for the same show
        f1 = os.path.join(shows_dir, "Yellowjackets.S01E01.1080p")
        f2 = os.path.join(shows_dir, "Yellowjackets.S01E02.1080p")
        f3 = os.path.join(shows_dir, "Yellowjackets.S02E01.1080p")
        for d in (f1, f2, f3):
            os.makedirs(d)
            base = os.path.basename(d)
            open(os.path.join(d, base + ".mkv"), 'w').close()

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        # Should be aggregated into one card
        yj = [s for s in result["shows"] if "yellowjackets" in s["title"].lower()]
        assert len(yj) == 1
        assert yj[0]["seasons"] == 2
        assert yj[0]["episodes"] == 3

    def test_movie_aggregation_deduplicates(self, tmp_dir, monkeypatch):
        movies_dir = os.path.join(tmp_dir, "movies")
        os.makedirs(os.path.join(movies_dir, "Dune.2021.1080p.WEB"))
        os.makedirs(os.path.join(movies_dir, "Dune.2021.2160p.BluRay"))
        os.makedirs(os.path.join(movies_dir, "Dune (2021)"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        dune = [m for m in result["movies"] if "dune" in m["title"].lower()]
        assert len(dune) == 1

    def test_site_prefix_stripped_in_aggregation(self, tmp_dir, monkeypatch):
        shows_dir = os.path.join(tmp_dir, "shows")
        f1 = os.path.join(shows_dir, "www.UIndex.org.The.White.Lotus.S01E01.1080p")
        f2 = os.path.join(shows_dir, "The.White.Lotus.S01E02.1080p")
        for d in (f1, f2):
            os.makedirs(d)
            base = os.path.basename(d)
            open(os.path.join(d, base + ".mkv"), 'w').close()

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        wl = [s for s in result["shows"] if "white lotus" in s["title"].lower()]
        assert len(wl) == 1
        assert wl[0]["episodes"] == 2
        # Title should not have the www prefix
        assert not wl[0]["title"].startswith("www")


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
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False
        return scanner

    def test_scan_local_movies_source_is_local(self, tmp_dir):
        local_movies = os.path.join(tmp_dir, "local_movies")
        movie_dir = os.path.join(local_movies, "Parasite (2019)")
        os.makedirs(movie_dir)
        open(os.path.join(movie_dir, "Parasite.2019.mkv"), "w").close()

        scanner = self._make_local_scanner(local_movies=local_movies)
        result = scanner.scan()

        assert len(result["movies"]) == 1
        assert result["movies"][0]["source"] == "local"
        assert result["movies"][0]["title"] == "Parasite"

    def test_scan_local_movies_skips_empty_dirs(self, tmp_dir):
        """Dirs with only metadata (.nfo/.jpg) but no media files are skipped.

        After symlinks are deleted, leftover Radarr metadata dirs should not
        be classified as local content (which would block symlink recreation).
        """
        local_movies = os.path.join(tmp_dir, "local_movies")
        empty_dir = os.path.join(local_movies, "F1 (2025)")
        os.makedirs(empty_dir)
        # Only metadata, no media file
        open(os.path.join(empty_dir, "movie.nfo"), "w").close()
        open(os.path.join(empty_dir, "poster.jpg"), "w").close()

        scanner = self._make_local_scanner(local_movies=local_movies)
        result = scanner.scan()

        assert len(result["movies"]) == 0

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
        local_dir = os.path.join(local_movies, "Oppenheimer (2023)")
        os.makedirs(local_dir)
        open(os.path.join(local_dir, "Oppenheimer.2023.mkv"), "w").close()

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
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False

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
        local_dir = os.path.join(local_movies, "Local Only (2020)")
        os.makedirs(local_dir)
        open(os.path.join(local_dir, "Local.Only.2020.mkv"), "w").close()

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
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False

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
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False

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
        local_dir = os.path.join(local_movies, "Arrival (2016)")
        os.makedirs(local_dir)
        open(os.path.join(local_dir, "Arrival.2016.mkv"), "w").close()

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
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False

        result = scanner.scan()
        arrival = next(m for m in result["movies"] if m["title"] == "Arrival")
        assert arrival["source"] == "both"


# ---------------------------------------------------------------------------
# _build_season_data
# ---------------------------------------------------------------------------

class TestBuildSeasonData:

    def test_empty_episodes(self):
        assert _build_season_data({}) == []

    def test_single_season(self):
        eps = {
            (1, 2): {'file': 'S01E02.mkv'},
            (1, 1): {'file': 'S01E01.mkv'},
        }
        result = _build_season_data(eps, 'debrid')
        assert len(result) == 1
        assert result[0]['number'] == 1
        assert result[0]['episode_count'] == 2
        # Episodes sorted by number
        assert result[0]['episodes'][0]['number'] == 1
        assert result[0]['episodes'][1]['number'] == 2
        assert result[0]['episodes'][0]['source'] == 'debrid'

    def test_multiple_seasons_sorted(self):
        eps = {
            (2, 1): {'file': 'S02E01.mkv'},
            (1, 1): {'file': 'S01E01.mkv'},
            (1, 2): {'file': 'S01E02.mkv'},
        }
        result = _build_season_data(eps, 'local')
        assert len(result) == 2
        assert result[0]['number'] == 1
        assert result[1]['number'] == 2
        assert result[0]['episode_count'] == 2
        assert result[1]['episode_count'] == 1

    def test_explicit_source_overrides_default(self):
        eps = {
            (1, 1): {'file': 'S01E01.mkv', 'source': 'both'},
            (1, 2): {'file': 'S01E02.mkv'},
        }
        result = _build_season_data(eps, 'debrid')
        assert result[0]['episodes'][0]['source'] == 'both'
        assert result[0]['episodes'][1]['source'] == 'debrid'

    def test_folder_ep_count_stripped(self):
        """_folder_ep_count metadata should not appear in output."""
        eps = {
            (1, 1): {'file': 'S01E01.mkv', '_folder_ep_count': 10},
            (1, 2): {'file': 'S01E02.mkv', '_folder_ep_count': 10},
        }
        result = _build_season_data(eps, 'debrid')
        for ep in result[0]['episodes']:
            assert '_folder_ep_count' not in ep


# ---------------------------------------------------------------------------
# Season pack preference in episode merge
# ---------------------------------------------------------------------------

class TestSeasonPackPreference:
    """Season packs should be preferred over individual episode downloads."""

    def test_pack_beats_individual(self):
        """A season pack (10 eps) should win over a single-episode folder."""
        existing = {
            (1, 1): {'file': 'individual.S01E01.mkv', 'path': '/a', '_folder_ep_count': 1},
        }
        pack_eps = {
            (1, 1): {'file': 'pack.S01E01.mkv', 'path': '/b', '_folder_ep_count': 10},
            (1, 2): {'file': 'pack.S01E02.mkv', 'path': '/b', '_folder_ep_count': 10},
        }
        for ep_key, ep_info in pack_eps.items():
            if ep_key not in existing:
                existing[ep_key] = ep_info
            elif ep_info.get('_folder_ep_count', 1) > existing[ep_key].get('_folder_ep_count', 1):
                existing[ep_key] = ep_info
        # Pack should win for S01E01
        assert existing[(1, 1)]['file'] == 'pack.S01E01.mkv'
        # Pack's S01E02 should be added
        assert existing[(1, 2)]['file'] == 'pack.S01E02.mkv'

    def test_individual_does_not_overwrite_pack(self):
        """A single-episode folder should not overwrite a season pack entry."""
        existing = {
            (1, 1): {'file': 'pack.S01E01.mkv', 'path': '/b', '_folder_ep_count': 10},
        }
        individual = {
            (1, 1): {'file': 'individual.S01E01.mkv', 'path': '/a', '_folder_ep_count': 1},
        }
        for ep_key, ep_info in individual.items():
            if ep_key not in existing:
                existing[ep_key] = ep_info
            elif ep_info.get('_folder_ep_count', 1) > existing[ep_key].get('_folder_ep_count', 1):
                existing[ep_key] = ep_info
        # Pack should still be there
        assert existing[(1, 1)]['file'] == 'pack.S01E01.mkv'

    def test_equal_size_first_wins(self):
        """On ties (same folder ep count), first-seen wins."""
        existing = {
            (1, 1): {'file': 'first.S01E01.mkv', 'path': '/a', '_folder_ep_count': 5},
        }
        second = {
            (1, 1): {'file': 'second.S01E01.mkv', 'path': '/b', '_folder_ep_count': 5},
        }
        for ep_key, ep_info in second.items():
            if ep_key not in existing:
                existing[ep_key] = ep_info
            elif ep_info.get('_folder_ep_count', 1) > existing[ep_key].get('_folder_ep_count', 1):
                existing[ep_key] = ep_info
        assert existing[(1, 1)]['file'] == 'first.S01E01.mkv'


# ---------------------------------------------------------------------------
# _norm_for_matching
# ---------------------------------------------------------------------------

class TestNormForMatching:
    """Fuzzy title normalization for arr matching."""

    def test_strips_punctuation(self):
        assert _norm_for_matching("Mission: Impossible - Rogue Nation") == "mission impossible rogue nation"

    def test_strips_parentheses(self):
        assert _norm_for_matching("(500) Days of Summer") == "500 days of summer"

    def test_preserves_year_for_disambiguation(self):
        """Years should be kept so 'Flash (2014)' != 'Flash (2023)'."""
        assert _norm_for_matching("Lioness (2023)") == "lioness 2023"
        assert _norm_for_matching("Flash (2014)") != _norm_for_matching("Flash (2023)")

    def test_matches_across_naming(self):
        """Titles from torrent names and arr canonical names should normalize the same."""
        assert _norm_for_matching("500 Days of Summer") == _norm_for_matching("(500) Days of Summer")
        assert _norm_for_matching("Mission Impossible Rogue Nation") == _norm_for_matching("Mission: Impossible - Rogue Nation")
        assert _norm_for_matching("Monsters Inc") == _norm_for_matching("Monsters, Inc.")
        assert _norm_for_matching("I Tonya") == _norm_for_matching("I, Tonya")

    def test_empty_string(self):
        assert _norm_for_matching("") == ""

    def test_unicode_transliteration(self):
        """Accented characters should be transliterated, not dropped."""
        assert _norm_for_matching("Amélie") == "amelie"
        assert _norm_for_matching("Señor") == "senor"


# ---------------------------------------------------------------------------
# season_data in scan results
# ---------------------------------------------------------------------------

class TestSeasonDataInScanResults:

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
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False
        return scanner

    def test_shows_have_season_data(self, tmp_dir, monkeypatch):
        shows_dir = os.path.join(tmp_dir, "shows")
        f1 = os.path.join(shows_dir, "TestShow.S01E01.1080p")
        os.makedirs(f1)
        open(os.path.join(f1, "TestShow.S01E01.mkv"), 'w').close()

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        show = result["shows"][0]
        assert "season_data" in show
        assert len(show["season_data"]) == 1
        assert show["season_data"][0]["number"] == 1
        assert show["season_data"][0]["episode_count"] == 1
        assert show["season_data"][0]["episodes"][0]["number"] == 1
        assert show["season_data"][0]["episodes"][0]["source"] == "debrid"

    def test_movies_have_no_season_data(self, tmp_dir, monkeypatch):
        movies_dir = os.path.join(tmp_dir, "movies")
        os.makedirs(os.path.join(movies_dir, "Movie (2023)"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        movie = result["movies"][0]
        assert "season_data" not in movie

    def test_aggregated_show_season_data_correct(self, tmp_dir, monkeypatch):
        shows_dir = os.path.join(tmp_dir, "shows")
        f1 = os.path.join(shows_dir, "Show.S01E01.1080p")
        f2 = os.path.join(shows_dir, "Show.S01E02.1080p")
        f3 = os.path.join(shows_dir, "Show.S02E01.1080p")
        for d in (f1, f2, f3):
            os.makedirs(d)
            base = os.path.basename(d)
            open(os.path.join(d, base + ".mkv"), 'w').close()

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        show = result["shows"][0]
        assert show["seasons"] == 2
        assert show["episodes"] == 3
        sd = show["season_data"]
        assert len(sd) == 2
        assert sd[0]["number"] == 1
        assert sd[0]["episode_count"] == 2
        assert sd[1]["number"] == 2
        assert sd[1]["episode_count"] == 1

    def test_no_internal_episodes_key_in_result(self, tmp_dir, monkeypatch):
        shows_dir = os.path.join(tmp_dir, "shows")
        f1 = os.path.join(shows_dir, "Show.S01E01.1080p")
        os.makedirs(f1)
        open(os.path.join(f1, "Show.S01E01.mkv"), 'w').close()

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        show = result["shows"][0]
        assert "_episodes" not in show


# ---------------------------------------------------------------------------
# Episode-level cross-referencing
# ---------------------------------------------------------------------------

class TestEpisodeLevelCrossRef:

    def _make_cross_ref_scanner(self, tmp_dir, mount_shows_setup, local_tv_setup):
        """Create a scanner with debrid mount and local TV paths.

        mount_shows_setup: callable(shows_dir) that creates debrid show folders
        local_tv_setup: callable(local_tv) that creates local show folders
        """
        mount_dir = os.path.join(tmp_dir, "mount")
        shows_dir = os.path.join(mount_dir, "shows")
        os.makedirs(shows_dir, exist_ok=True)
        mount_shows_setup(shows_dir)

        local_tv = os.path.join(tmp_dir, "local_tv")
        os.makedirs(local_tv, exist_ok=True)
        local_tv_setup(local_tv)

        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = mount_dir
        scanner._local_movies_path = None
        scanner._local_tv_path = local_tv
        scanner._cache = None
        scanner._cache_time = 0
        scanner._ttl = 600
        scanner._lock = threading.Lock()
        scanner._scanning = False
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False
        return scanner

    def test_same_episode_both_sources_gets_both(self, tmp_dir):
        def debrid(shows_dir):
            f = os.path.join(shows_dir, "Show.S01E01.1080p")
            os.makedirs(f)
            open(os.path.join(f, "Show.S01E01.mkv"), 'w').close()

        def local(local_tv):
            show = os.path.join(local_tv, "Show (2020)")
            s1 = os.path.join(show, "Season 1")
            os.makedirs(s1)
            open(os.path.join(s1, "Show.S01E01.mkv"), 'w').close()

        scanner = self._make_cross_ref_scanner(tmp_dir, debrid, local)
        result = scanner.scan()

        show = next(s for s in result["shows"] if s["title"] == "Show")
        assert show["source"] == "both"
        ep = show["season_data"][0]["episodes"][0]
        assert ep["number"] == 1
        assert ep["source"] == "both"

    def test_different_episodes_get_respective_sources(self, tmp_dir):
        def debrid(shows_dir):
            f = os.path.join(shows_dir, "Show.S01E01.1080p")
            os.makedirs(f)
            open(os.path.join(f, "Show.S01E01.mkv"), 'w').close()

        def local(local_tv):
            show = os.path.join(local_tv, "Show (2020)")
            s1 = os.path.join(show, "Season 1")
            os.makedirs(s1)
            open(os.path.join(s1, "Show.S01E02.mkv"), 'w').close()

        scanner = self._make_cross_ref_scanner(tmp_dir, debrid, local)
        result = scanner.scan()

        show = next(s for s in result["shows"] if s["title"] == "Show")
        assert show["source"] == "both"  # has both debrid and local episodes
        sd = show["season_data"]
        assert len(sd) == 1
        eps = {e["number"]: e["source"] for e in sd[0]["episodes"]}
        assert eps[1] == "debrid"
        assert eps[2] == "local"

    def test_source_rollup_all_debrid(self, tmp_dir):
        def debrid(shows_dir):
            f = os.path.join(shows_dir, "OnlyDebrid.S01E01.1080p")
            os.makedirs(f)
            open(os.path.join(f, "OnlyDebrid.S01E01.mkv"), 'w').close()

        def local(local_tv):
            pass  # no local shows

        scanner = self._make_cross_ref_scanner(tmp_dir, debrid, local)
        result = scanner.scan()

        show = next(s for s in result["shows"] if "OnlyDebrid" in s["title"])
        assert show["source"] == "debrid"
        assert show["season_data"][0]["episodes"][0]["source"] == "debrid"

    def test_source_rollup_all_local(self, tmp_dir):
        def debrid(shows_dir):
            pass  # no debrid shows

        def local(local_tv):
            show = os.path.join(local_tv, "OnlyLocal (2020)")
            s1 = os.path.join(show, "Season 1")
            os.makedirs(s1)
            open(os.path.join(s1, "OnlyLocal.S01E01.mkv"), 'w').close()

        scanner = self._make_cross_ref_scanner(tmp_dir, debrid, local)
        result = scanner.scan()

        show = next(s for s in result["shows"] if "OnlyLocal" in s["title"])
        assert show["source"] == "local"
        assert show["season_data"][0]["episodes"][0]["source"] == "local"

    def test_cross_ref_updates_counts(self, tmp_dir):
        def debrid(shows_dir):
            f = os.path.join(shows_dir, "Merged.S01E01.1080p")
            os.makedirs(f)
            open(os.path.join(f, "Merged.S01E01.mkv"), 'w').close()

        def local(local_tv):
            show = os.path.join(local_tv, "Merged (2020)")
            s1 = os.path.join(show, "Season 1")
            s2 = os.path.join(show, "Season 2")
            os.makedirs(s1)
            os.makedirs(s2)
            open(os.path.join(s1, "Merged.S01E02.mkv"), 'w').close()
            open(os.path.join(s2, "Merged.S02E01.mkv"), 'w').close()

        scanner = self._make_cross_ref_scanner(tmp_dir, debrid, local)
        result = scanner.scan()

        show = next(s for s in result["shows"] if "Merged" in s["title"])
        # Debrid has S01E01, local has S01E02 + S02E01 = 3 episodes, 2 seasons
        assert show["episodes"] == 3
        assert show["seasons"] == 2
        assert show["source"] == "both"

    def test_cross_ref_no_duplicate_shows(self, tmp_dir):
        def debrid(shows_dir):
            f = os.path.join(shows_dir, "Shared.S01E01.1080p")
            os.makedirs(f)
            open(os.path.join(f, "Shared.S01E01.mkv"), 'w').close()

        def local(local_tv):
            show = os.path.join(local_tv, "Shared (2020)")
            s1 = os.path.join(show, "Season 1")
            os.makedirs(s1)
            open(os.path.join(s1, "Shared.S01E01.mkv"), 'w').close()

        scanner = self._make_cross_ref_scanner(tmp_dir, debrid, local)
        result = scanner.scan()

        matching = [s for s in result["shows"] if "Shared" in s["title"]]
        assert len(matching) == 1

    def test_path_index_populated_for_debrid(self, tmp_dir):
        def debrid(shows_dir):
            f = os.path.join(shows_dir, "Indexed.S01E01.1080p")
            os.makedirs(f)
            open(os.path.join(f, "Indexed.S01E01.mkv"), 'w').close()

        def local(local_tv):
            pass

        scanner = self._make_cross_ref_scanner(tmp_dir, debrid, local)
        scanner.scan()

        path = scanner.get_episode_path("indexed", 1, 1)
        assert path is not None
        assert path.endswith("Indexed.S01E01.mkv")

    def test_local_path_index_populated(self, tmp_dir):
        def debrid(shows_dir):
            pass

        def local(local_tv):
            show = os.path.join(local_tv, "LocalShow (2020)")
            s1 = os.path.join(show, "Season 1")
            os.makedirs(s1)
            open(os.path.join(s1, "LocalShow.S01E01.mkv"), 'w').close()

        scanner = self._make_cross_ref_scanner(tmp_dir, debrid, local)
        scanner.scan()

        path = scanner.get_local_episode_path("localshow", 1, 1)
        assert path is not None
        assert path.endswith("LocalShow.S01E01.mkv")

    def test_both_source_preserves_local_path(self, tmp_dir):
        def debrid(shows_dir):
            f = os.path.join(shows_dir, "Both.S01E01.1080p")
            os.makedirs(f)
            open(os.path.join(f, "Both.S01E01.mkv"), 'w').close()

        def local(local_tv):
            show = os.path.join(local_tv, "Both (2020)")
            s1 = os.path.join(show, "Season 1")
            os.makedirs(s1)
            open(os.path.join(s1, "Both.S01E01.mkv"), 'w').close()

        scanner = self._make_cross_ref_scanner(tmp_dir, debrid, local)
        scanner.scan()

        debrid_path = scanner.get_episode_path("both", 1, 1)
        local_path = scanner.get_local_episode_path("both", 1, 1)
        assert debrid_path is not None
        assert local_path is not None
        assert debrid_path != local_path

    def test_scan_result_includes_preferences(self, tmp_dir, monkeypatch):
        def debrid(shows_dir):
            pass

        def local(local_tv):
            pass

        scanner = self._make_cross_ref_scanner(tmp_dir, debrid, local)
        # Mock preferences to avoid needing /config
        monkeypatch.setattr('utils.library_prefs.load_preferences', lambda: {'test': 'prefer-local'})
        result = scanner.scan()

        assert 'preferences' in result
        assert result['preferences'] == {'test': 'prefer-local'}


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
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False
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

    def test_get_data_uses_short_ttl_when_no_mount(self, mocker):
        scanner = self._bare_scanner()
        assert scanner._mount_path is None
        # First call populates cache
        scanner.get_data()
        # Rewind cache_time by 11 seconds — short TTL (10s) should expire
        scanner._cache_time = time.monotonic() - 11
        fresh = {"movies": ["fresh"], "shows": [], "last_scan": "x", "scan_duration_ms": 0}
        mocker.patch.object(scanner, "scan", return_value=fresh)
        result = scanner.get_data()
        assert result is fresh

    def test_get_data_uses_full_ttl_when_mount_present(self, tmp_dir, mocker):
        scanner = self._bare_scanner()
        scanner._mount_path = tmp_dir  # mount exists
        scanner._cache = {"movies": [], "shows": [], "last_scan": "x", "scan_duration_ms": 0}
        scanner._cache_time = time.monotonic() - 11  # 11s ago
        # With mount present, full 600s TTL applies — cache should still be valid
        mock_scan = mocker.patch.object(scanner, "scan")
        result = scanner.get_data()
        mock_scan.assert_not_called()
        assert result is scanner._cache


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
        scanner._effects_running = False
        scanner._path_index = {}
        scanner._local_path_index = {}
        scanner._path_lock = threading.Lock()
        scanner._search_cooldown = {}
        scanner._alias_norms = {}
        scanner._debrid_unavailable_days = 3
        scanner._pending_warning_hours = 24
        scanner._last_had_local = None
        scanner._local_drop_alerted = False
        return scanner

    def test_refresh_triggers_background_scan(self, mocker):
        scanner = self._bare_scanner()
        started = threading.Event()

        def _fake_scan():
            started.set()
            return {"movies": [], "shows": [], "last_scan": "x", "scan_duration_ms": 0}

        mocker.patch.object(scanner, "_scan_read", side_effect=_fake_scan)
        scanner.refresh()
        assert started.wait(timeout=2), "Background scan thread did not start within 2s"

    def test_refresh_updates_cache_after_completion(self, mocker):
        scanner = self._bare_scanner()
        done = threading.Event()
        payload = {"movies": [{"title": "BG Movie"}], "shows": [], "last_scan": "x", "scan_duration_ms": 0}

        def _fake_scan():
            done.set()
            return payload

        mocker.patch.object(scanner, "_scan_read", side_effect=_fake_scan)
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

        mocker.patch.object(scanner, "_scan_read", side_effect=_slow_scan)
        scanner.refresh()
        time.sleep(0.05)  # first thread is running
        scanner.refresh()  # second call must be a no-op
        barrier.set()
        time.sleep(0.1)

        assert len(scan_calls) == 1

    def test_refresh_sets_short_cache_when_no_mount(self, mocker):
        scanner = self._bare_scanner()
        assert scanner._mount_path is None
        done = threading.Event()

        def _fake_scan():
            done.set()
            return {"movies": [], "shows": [], "last_scan": "x", "scan_duration_ms": 0}

        mocker.patch.object(scanner, "_scan_read", side_effect=_fake_scan)
        scanner.refresh()
        done.wait(timeout=2)
        time.sleep(0.05)
        # Cache time should be set so it expires in ~10s, not 600s
        elapsed = time.monotonic() - scanner._cache_time
        assert elapsed > scanner._ttl - 15  # at least 585s "ago"

    def test_refresh_sets_normal_cache_when_mount_present(self, tmp_dir, mocker):
        scanner = self._bare_scanner()
        scanner._mount_path = tmp_dir
        done = threading.Event()

        def _fake_scan():
            done.set()
            return {"movies": [], "shows": [], "last_scan": "x", "scan_duration_ms": 0}

        mocker.patch.object(scanner, "_scan_read", side_effect=_fake_scan)
        scanner.refresh()
        done.wait(timeout=2)
        time.sleep(0.05)
        # Cache time should be recent (within last second)
        elapsed = time.monotonic() - scanner._cache_time
        assert elapsed < 2

    def test_refresh_clears_scanning_flag_on_error(self, mocker):
        scanner = self._bare_scanner()
        done = threading.Event()

        def _error_scan():
            raise RuntimeError("simulated scan failure")

        mocker.patch.object(scanner, "_scan_read", side_effect=_error_scan)
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


# ---------------------------------------------------------------------------
# Wanted counts (Feature 4)
# ---------------------------------------------------------------------------

class TestGetWantedCounts:
    """Tests for get_wanted_counts() — counts items needing attention."""

    def test_no_data_returns_zeros(self):
        counts = get_wanted_counts({})
        assert counts == {'missing': 0, 'unavailable': 0, 'pending': 0, 'fallback': 0}

    def test_show_with_missing_episodes_counted(self):
        data = {'shows': [{
            'title': 'Test Show',
            'missing_episodes': 3,
            'season_data': [],
        }], 'movies': []}
        counts = get_wanted_counts(data)
        assert counts['missing'] == 1

    def test_show_with_zero_missing_not_counted(self):
        data = {'shows': [{
            'title': 'Complete Show',
            'missing_episodes': 0,
            'season_data': [],
        }], 'movies': []}
        counts = get_wanted_counts(data)
        assert counts['missing'] == 0

    def test_show_with_none_missing_not_counted(self):
        data = {'shows': [{
            'title': 'Unenriched Show',
            'missing_episodes': None,
            'season_data': [],
        }], 'movies': []}
        counts = get_wanted_counts(data)
        assert counts['missing'] == 0

    def test_movie_with_missing_episodes(self):
        data = {'shows': [], 'movies': [
            {'title': 'Missing Movie', 'missing_episodes': 1},
            {'title': 'Available Movie', 'missing_episodes': 0},
        ]}
        counts = get_wanted_counts(data)
        assert counts['missing'] == 1

    def test_pending_directions(self):
        data = {'shows': [
            {'title': 'Show A', 'season_data': []},
            {'title': 'Show B', 'season_data': []},
            {'title': 'Show C', 'season_data': []},
        ], 'movies': []}
        pending = {
            'show a': {'direction': 'debrid-unavailable'},
            'show b': {'direction': 'to-debrid'},
            'show c': {'direction': 'to-local-fallback'},
        }
        counts = get_wanted_counts(data, pending)
        assert counts['unavailable'] == 1
        assert counts['pending'] == 2  # to-debrid + to-local-fallback
        assert counts['fallback'] == 1

    def test_multiple_shows_with_missing(self):
        data = {'shows': [
            {'title': 'Show 1', 'missing_episodes': 5, 'season_data': []},
            {'title': 'Show 2', 'missing_episodes': 0, 'season_data': []},
            {'title': 'Show 3', 'missing_episodes': 2, 'season_data': []},
        ], 'movies': []}
        counts = get_wanted_counts(data)
        assert counts['missing'] == 2

    def test_movie_with_none_missing_episodes_not_counted(self):
        data = {'shows': [], 'movies': [
            {'title': 'Unenriched Movie', 'missing_episodes': None},
        ]}
        counts = get_wanted_counts(data)
        assert counts['missing'] == 0

    def test_movie_pending_directions(self):
        data = {'shows': [], 'movies': [
            {'title': 'Movie A', 'missing_episodes': 0},
        ]}
        pending = {
            'movie a': {'direction': 'to-local-fallback'},
        }
        counts = get_wanted_counts(data, pending)
        assert counts['fallback'] == 1
        assert counts['pending'] == 1


class TestCleanupDiscRips:
    """Tests for LibraryScanner._cleanup_disc_rips()."""

    @pytest.fixture
    def scanner(self, monkeypatch, tmp_dir):
        monkeypatch.setenv('RCLONE_MOUNT_NAME', 'test')
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', tmp_dir)
        monkeypatch.delenv('BLACKHOLE_LOCAL_LIBRARY_MOVIES', raising=False)
        monkeypatch.delenv('BLACKHOLE_LOCAL_LIBRARY_TV', raising=False)
        return LibraryScanner()

    def _make_disc_rip_folder(self, tmp_dir, name):
        """Create a folder with .m2ts files (disc rip) and return its path."""
        path = os.path.join(tmp_dir, name)
        os.makedirs(path, exist_ok=True)
        for f in ['00001.m2ts', '00002.m2ts', 'index.bdmv']:
            with open(os.path.join(path, f), 'w') as fh:
                fh.write('fake')
        return path

    def _make_media_folder(self, tmp_dir, name):
        """Create a folder with a real media file and return its path."""
        path = os.path.join(tmp_dir, name)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, 'movie.mkv'), 'w') as fh:
            fh.write('fake')
        return path

    def test_disc_rip_detected_and_cleaned(self, scanner, tmp_dir, monkeypatch):
        """Disc rip movie (size=0, .m2ts only) should be blocklisted and deleted."""
        rip_path = self._make_disc_rip_folder(tmp_dir, 'Why.Him.2016')
        movies = [
            {'title': 'Why Him', 'year': 2016, 'source': 'debrid', 'size_bytes': 0,
             'path': rip_path, 'quality': {}},
        ]
        mock_client = type('MockClient', (), {
            'find_torrents_by_title': lambda self, n, target_year=None: [
                {'id': 'T1', 'filename': 'Why.Him.2016.BluRay', 'hash': 'AABBCCDD', 'year': 2016}
            ],
            'delete_torrent': lambda self, tid: True,
        })()
        monkeypatch.setattr('utils.debrid_client.get_debrid_client',
                            lambda: (mock_client, 'realdebrid'))
        import utils.library as _lib
        monkeypatch.setattr(_lib, '_blocklist', type('MockBL', (), {
            'add': lambda self, *a, **kw: 'id1',
        })())
        monkeypatch.setenv('BLOCKLIST_AUTO_ADD', 'true')
        monkeypatch.setattr(_lib, '_history', None)

        cleaned = scanner._cleanup_disc_rips(movies)
        assert cleaned == 1
        assert len(movies) == 0  # Removed from list

    def test_normal_movie_not_cleaned(self, scanner, tmp_dir, monkeypatch):
        """Movie with real media files should not be touched."""
        media_path = self._make_media_folder(tmp_dir, 'Good.Movie.2024')
        movies = [
            {'title': 'Good Movie', 'year': 2024, 'source': 'debrid', 'size_bytes': 5000000,
             'path': media_path, 'quality': {'resolution': '1080p'}},
        ]
        cleaned = scanner._cleanup_disc_rips(movies)
        assert cleaned == 0
        assert len(movies) == 1

    def test_empty_folder_not_treated_as_disc_rip(self, scanner, tmp_dir, monkeypatch):
        """Empty mount folder (possible mount issue) should not be cleaned."""
        empty_path = os.path.join(tmp_dir, 'Empty.Movie.2024')
        os.makedirs(empty_path, exist_ok=True)
        movies = [
            {'title': 'Empty Movie', 'year': 2024, 'source': 'debrid', 'size_bytes': 0,
             'path': empty_path, 'quality': {}},
        ]
        cleaned = scanner._cleanup_disc_rips(movies)
        assert cleaned == 0
        assert len(movies) == 1

    def test_local_movies_skipped(self, scanner, tmp_dir, monkeypatch):
        """Local-source movies should never be considered for disc rip cleanup."""
        rip_path = self._make_disc_rip_folder(tmp_dir, 'Local.Rip.2024')
        movies = [
            {'title': 'Local Rip', 'year': 2024, 'source': 'local', 'size_bytes': 0,
             'path': rip_path, 'quality': {}},
        ]
        cleaned = scanner._cleanup_disc_rips(movies)
        assert cleaned == 0
        assert len(movies) == 1

    def test_no_debrid_client_still_safe(self, scanner, tmp_dir, monkeypatch):
        """If debrid client is unavailable, cleanup should not crash."""
        rip_path = self._make_disc_rip_folder(tmp_dir, 'NoCli.2024')
        movies = [
            {'title': 'NoCli', 'year': 2024, 'source': 'debrid', 'size_bytes': 0,
             'path': rip_path, 'quality': {}},
        ]
        import utils.library as _lib
        monkeypatch.setattr(_lib, '_blocklist', None)
        monkeypatch.setattr(_lib, '_history', None)
        # get_debrid_client raises
        monkeypatch.setattr('utils.debrid_client.get_debrid_client',
                            lambda: (_ for _ in ()).throw(ImportError('no client')))
        cleaned = scanner._cleanup_disc_rips(movies)
        # Can't blocklist or delete without client, but shouldn't crash
        assert cleaned == 0

    def test_nonexistent_path_skipped(self, scanner, tmp_dir, monkeypatch):
        """Movie pointing to nonexistent path should be skipped, not crash."""
        movies = [
            {'title': 'Gone Movie', 'year': 2024, 'source': 'debrid', 'size_bytes': 0,
             'path': '/nonexistent/path/movie', 'quality': {}},
        ]
        cleaned = scanner._cleanup_disc_rips(movies)
        assert cleaned == 0

    def test_media_in_subdirectory_not_treated_as_disc_rip(self, scanner, tmp_dir, monkeypatch):
        """Movie with .mkv nested in a subdirectory should not be cleaned."""
        path = os.path.join(tmp_dir, 'Nested.Movie.2024')
        subdir = os.path.join(path, 'Movie')
        os.makedirs(subdir, exist_ok=True)
        with open(os.path.join(subdir, 'movie.mkv'), 'w') as fh:
            fh.write('fake')
        movies = [
            {'title': 'Nested Movie', 'year': 2024, 'source': 'debrid', 'size_bytes': 0,
             'path': path, 'quality': {}},
        ]
        cleaned = scanner._cleanup_disc_rips(movies)
        assert cleaned == 0
        assert len(movies) == 1

    def test_only_cleaned_items_removed_from_list(self, scanner, tmp_dir, monkeypatch):
        """Only disc rips that were actually actioned should be removed from the list."""
        rip_path = self._make_disc_rip_folder(tmp_dir, 'Actioned.2024')
        norip_path = self._make_disc_rip_folder(tmp_dir, 'NoMatch.2024')
        movies = [
            {'title': 'Actioned', 'year': 2024, 'source': 'debrid', 'size_bytes': 0,
             'path': rip_path, 'quality': {}},
            {'title': 'NoMatch', 'year': 2024, 'source': 'debrid', 'size_bytes': 0,
             'path': norip_path, 'quality': {}},
        ]
        # Client returns matches only for "Actioned", not "NoMatch"
        mock_client = type('MockClient', (), {
            'find_torrents_by_title': lambda self, n, target_year=None:
                [{'id': 'T1', 'filename': 'Actioned.2024', 'hash': 'AABB', 'year': 2024}]
                if 'actioned' in n else [],
            'delete_torrent': lambda self, tid: True,
        })()
        monkeypatch.setattr('utils.debrid_client.get_debrid_client',
                            lambda: (mock_client, 'realdebrid'))
        import utils.library as _lib
        monkeypatch.setattr(_lib, '_blocklist', type('MockBL', (), {
            'add': lambda self, *a, **kw: 'id1',
        })())
        monkeypatch.setenv('BLOCKLIST_AUTO_ADD', 'true')
        monkeypatch.setattr(_lib, '_history', None)

        cleaned = scanner._cleanup_disc_rips(movies)
        assert cleaned == 1
        assert len(movies) == 1
        assert movies[0]['title'] == 'NoMatch'  # Only un-actioned item remains
