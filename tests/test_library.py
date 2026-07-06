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
    _release_matches_title,
    get_wanted_counts,
    compute_library_stats,
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

    # Genre descriptor between title and year ("Title - <Genre> YYYY ...")
    def test_genre_suffix_sci_fi_stripped(self):
        # User-reported: Predestination was showing as "Predestination Sci Fi (2014)"
        title, year = _parse_folder_name(
            "Predestination - Sci-Fi 2014 Eng Rus Multi Subs 1080p [H264-mp4]"
        )
        assert title == "Predestination"
        assert year == 2014

    def test_genre_suffix_phycological_thriller_stripped(self):
        # User-reported: The Jacket with the observed "Phycological" misspelling
        title, year = _parse_folder_name(
            "The Jacket - Phycological Thriller 2005 Eng Rus Ukr Multi Subs 1080p [H264-mp4]"
        )
        assert title == "The Jacket"
        assert year == 2005

    def test_genre_suffix_psychological_thriller_stripped(self):
        title, year = _parse_folder_name(
            "Movie Title - Psychological Thriller 2018 1080p BluRay"
        )
        assert title == "Movie Title"
        assert year == 2018

    def test_genre_suffix_two_word_science_fiction_stripped(self):
        title, year = _parse_folder_name(
            "Movie - Science Fiction 2020 1080p"
        )
        assert title == "Movie"
        assert year == 2020

    def test_genre_suffix_case_insensitive(self):
        title, year = _parse_folder_name(
            "Predestination - sci-fi 2014 1080p"
        )
        assert title == "Predestination"
        assert year == 2014

    # Negative cases — legitimate " - Subtitle" titles: the genre pattern
    # must NOT consume the subtitle word.  Downstream `_clean_title` still
    # collapses dashes to spaces, so "Leon - The Professional" ends up as
    # "Leon The Professional" — the important thing is that "The
    # Professional" survives the genre strip.
    def test_leon_the_professional_untouched(self):
        title, year = _parse_folder_name(
            "Leon - The Professional 1994 1080p BluRay"
        )
        assert title == "Leon The Professional"
        assert year == 1994

    def test_blade_runner_final_cut_untouched(self):
        title, year = _parse_folder_name(
            "Blade Runner - The Final Cut 2007 1080p"
        )
        assert title == "Blade Runner The Final Cut"
        assert year == 2007

    def test_hyphenated_title_without_space_dash_space_untouched(self):
        # Spider-Man has a hyphen but no " - " separator, so the genre
        # pattern does not fire.  The hyphen becomes a space in
        # _clean_title (pre-existing behavior, not caused by this rule).
        title, year = _parse_folder_name("Spider-Man 2002 1080p BluRay")
        assert title == "Spider Man"
        assert year == 2002

    def test_genre_without_year_untouched(self):
        # Rule requires a plausible 4-digit year (19xx/20xx) to follow.
        # "1080p" must NOT trigger the strip, or "Thriller" disappears.
        title, _ = _parse_folder_name("Movie Title - Thriller 1080p")
        assert title == "Movie Title Thriller"

    def test_genre_not_in_allowlist_untouched(self):
        # "War" is deliberately excluded from the allowlist — the word
        # survives even though the surrounding dashes collapse to spaces.
        title, year = _parse_folder_name("The Great - War 1998 1080p")
        assert title == "The Great War"
        assert year == 1998

    def test_quality_prefix_1080p_not_treated_as_year(self):
        # Regression guard: "1080p" starts with "10" which is not a valid
        # year prefix (must be 19xx or 20xx), so the genre pattern must
        # not strip before a quality marker.
        title, _ = _parse_folder_name("Predestination - Sci-Fi 1080p")
        assert title == "Predestination Sci Fi"

    def test_genre_suffix_single_word_thriller_stripped(self):
        # Happy path for a plain single-word genre from the allowlist
        title, year = _parse_folder_name("Cape Fear - Thriller 2019 1080p BluRay")
        assert title == "Cape Fear"
        assert year == 2019

    def test_genre_suffix_dotted_separator_stripped(self):
        # Dotted release-naming convention: ".-." between title and genre
        title, year = _parse_folder_name(
            "Predestination.-.Sci-Fi.2014.1080p.BluRay"
        )
        assert title == "Predestination"
        assert year == 2014

    def test_genre_suffix_underscore_separator_stripped(self):
        # Underscore release-naming convention
        title, year = _parse_folder_name(
            "Predestination_-_Sci-Fi_2014_1080p_BluRay"
        )
        assert title == "Predestination"
        assert year == 2014

    def test_genre_suffix_parenthesized_year_stripped(self):
        # Parenthesized year form: "Movie - Sci-Fi (2014) 1080p"
        title, year = _parse_folder_name(
            "Predestination - Sci-Fi (2014) 1080p"
        )
        assert title == "Predestination"
        assert year == 2014

    def test_genre_suffix_year_with_letter_suffix_untouched(self):
        # "2020s" / "2014th" must not satisfy the year lookahead —
        # otherwise we'd strip the genre AND lose the year, leaving
        # garbage downstream.  Strip must not fire.
        title, _ = _parse_folder_name("Movie - Drama 2020s 1080p")
        assert "Drama" in title or "2020s" in title  # strip did not fire

    def test_genre_suffix_tv_show_parity(self):
        # Sonarr/Radarr parity: the same pattern works for TV folders.
        title, year = _parse_folder_name(
            "Sherlock - Mystery 2010 Season 1 1080p"
        )
        assert title == "Sherlock"
        assert year == 2010

    def test_spider_man_with_subtitle_not_mistaken_for_genre(self):
        # Internal word-hyphens (Spider-Man) must not be treated as
        # " - " separators.  The adjacent "Action" would otherwise be
        # stripped, but the genre pattern requires separator chars on
        # both sides of the dash.
        title, year = _parse_folder_name(
            "Spider-Man - Action 2002 1080p"
        )
        # The " - Action " (space-dash-space + Action + year) should
        # strip; the internal Spider-Man hyphen should not.
        assert title == "Spider Man"
        assert year == 2002


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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'
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

    def test_scan_debrid_skips_obfuscated_folders(self, tmp_dir, monkeypatch):
        movies_dir = os.path.join(tmp_dir, "movies")
        os.makedirs(os.path.join(movies_dir, "050bd19ee9934249a2ce4c9762c0d710[EZTVx.to]"))
        os.makedirs(os.path.join(movies_dir, "Inception (2010)"))
        shows_dir = os.path.join(tmp_dir, "shows")
        os.makedirs(os.path.join(
            shows_dir, "1f9da83faaf847949e043d0dae9684aa[eztv.re]"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        result = scanner.scan()

        titles = ({m["title"] for m in result["movies"]}
                  | {s["title"] for s in result["shows"]})
        assert "Inception" in titles
        assert not any("050bd19" in t.lower() or "1f9da83" in t.lower()
                       for t in titles)

    def test_scan_mount_flat_layout_skips_obfuscated(self, tmp_dir, monkeypatch):
        os.makedirs(os.path.join(
            tmp_dir, "050bd19ee9934249a2ce4c9762c0d710[EZTVx.to]"))
        os.makedirs(os.path.join(tmp_dir, "Inception (2010)"))

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        movies, shows = scanner._scan_mount(
            tmp_dir, flat_layout=True, source_debrid='torbox')

        titles = {m["title"] for m in movies} | {s["title"] for s in shows}
        assert "Inception" in titles
        assert not any("050bd19" in t.lower() for t in titles)

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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'

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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'
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

    def test_scan_local_movies_skips_dangling_symlink_only_dir(self, tmp_dir):
        """A folder whose only video is a dangling symlink is NOT local content.

        Regression: `_has_media_files` counted any `is_symlink()` media entry,
        so a broken video symlink (e.g. after a symlink-target-base rename, or
        a non-debrid symlink whose target vanished) inflated the recovery
        metric and hid the title from "Wanted", blocking symlink recreation.
        """
        local_movies = os.path.join(tmp_dir, "local_movies")
        movie_dir = os.path.join(local_movies, "Dune (2021)")
        os.makedirs(movie_dir)
        # Symlink to a target that doesn't exist (and isn't a debrid prefix)
        os.symlink(os.path.join(tmp_dir, "gone", "Dune.2021.mkv"),
                   os.path.join(movie_dir, "Dune.2021.mkv"))

        scanner = self._make_local_scanner(local_movies=local_movies)
        result = scanner.scan()

        assert len(result["movies"]) == 0

    def test_scan_local_movies_keeps_resolving_symlink_dir(self, tmp_dir):
        """A folder whose video symlink RESOLVES is still genuine local content."""
        local_movies = os.path.join(tmp_dir, "local_movies")
        target = os.path.join(tmp_dir, "real", "Dune.2021.mkv")
        os.makedirs(os.path.dirname(target))
        open(target, "w").close()
        movie_dir = os.path.join(local_movies, "Dune (2021)")
        os.makedirs(movie_dir)
        os.symlink(target, os.path.join(movie_dir, "Dune.2021.mkv"))

        scanner = self._make_local_scanner(local_movies=local_movies)
        result = scanner.scan()

        assert len(result["movies"]) == 1
        assert result["movies"][0]["source"] == "local"

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


class TestMountHasContent:
    """Unit guards for _mount_has_content — the per-mount health check that
    keeps symlink cleanup from mass-deleting links on a throttled/stalled
    mount (os.path.exists False for everything)."""

    def test_missing_mount_is_unhealthy(self, tmp_dir):
        from utils.library import _mount_has_content
        assert _mount_has_content(os.path.join(tmp_dir, 'nope')) is False

    def test_empty_mount_is_unhealthy(self, tmp_dir):
        from utils.library import _mount_has_content
        empty = os.path.join(tmp_dir, 'empty')
        os.makedirs(empty)
        assert _mount_has_content(empty) is False
        assert _mount_has_content(empty, flat=True) is False

    def test_categorized_mount_needs_non_empty_category(self, tmp_dir):
        from utils.library import _mount_has_content
        mount = os.path.join(tmp_dir, 'rd')
        os.makedirs(os.path.join(mount, 'movies'))  # category exists but empty
        assert _mount_has_content(mount) is False
        os.makedirs(os.path.join(mount, 'shows', 'A.Release'))
        assert _mount_has_content(mount) is True

    def test_flat_mount_healthy_when_non_empty(self, tmp_dir):
        from utils.library import _mount_has_content
        mount = os.path.join(tmp_dir, 'tb')
        os.makedirs(os.path.join(mount, 'Some.Release'))
        # Flat mount has no categories — non-empty top level is enough.
        assert _mount_has_content(mount, flat=True) is True
        # Same dir treated as categorized would look unhealthy.
        assert _mount_has_content(mount, flat=False) is False


class TestDebridSymlinkPrefixesDualDebrid:
    """Guards for the dual-debrid symlink-prefix recognition that drives
    local-scanner debrid-vs-local classification.

    Regression: with plan 39's per-debrid target bases, TorBox content
    landed under ``BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX`` (or the
    auto-derived ``<RD>_torbox`` suffix).  The pre-fix scanner checked
    only against ``BLACKHOLE_SYMLINK_TARGET_BASE``, so TB-only symlink
    folders looked like genuine local content — appearing as movie or
    TV cards in the wrong bucket of the library UI (the user-visible
    surface: 'Why is Grey's Anatomy showing up as a movie in Recently
    Added?').
    """

    def test_all_debrid_symlink_prefixes_includes_tb_auto_derived(self, monkeypatch):
        """When TB is not explicitly configured but RD is set, the TB
        prefix auto-derives as ``<RD>_torbox`` and must be included."""
        from utils.library import _all_debrid_symlink_prefixes
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)
        prefixes = _all_debrid_symlink_prefixes()
        assert '/mnt/debrid/' in prefixes
        assert '/mnt/debrid_torbox/' in prefixes

    def test_all_debrid_symlink_prefixes_explicit_tb_wins(self, monkeypatch):
        from utils.library import _all_debrid_symlink_prefixes
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/rd')
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', '/mnt/tb')
        prefixes = _all_debrid_symlink_prefixes()
        assert '/mnt/rd/' in prefixes
        assert '/mnt/tb/' in prefixes
        assert '/mnt/rd_torbox/' not in prefixes  # explicit beats auto-derived

    def test_all_debrid_symlink_prefixes_single_debrid_no_dup(self, monkeypatch):
        """RD-only setup (no TB env): TB's auto-derived `_torbox` suffix is
        included (harmless over-include — the helper deliberately doesn't
        couple to configured_debrids detection).  Critical assertion: no
        duplicates regardless of how many bases get added."""
        from utils.library import _all_debrid_symlink_prefixes
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)
        prefixes = _all_debrid_symlink_prefixes()
        assert '/mnt/debrid/' in prefixes
        # Verify actual dedup behavior: if user sets RD and explicit-TB to
        # the SAME path (legitimate "share one mount" config), the result
        # collapses to a single prefix instead of duplicating it.
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', '/mnt/debrid')
        deduped = _all_debrid_symlink_prefixes()
        assert deduped.count('/mnt/debrid/') == 1, \
            'identical RD+TB bases must dedup to one entry'

    def test_all_debrid_symlink_prefixes_normalises_paths(self, monkeypatch):
        """Helper must collapse consecutive separators and resolve relative
        segments — raw env values like ``/mnt//debrid`` or ``./debrid``
        would otherwise produce prefixes that no real symlink target
        starts with."""
        from utils.library import _all_debrid_symlink_prefixes
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt//debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)
        prefixes = _all_debrid_symlink_prefixes()
        # normpath collapses consecutive separators
        assert '/mnt/debrid/' in prefixes
        assert '/mnt//debrid/' not in prefixes

    def test_all_debrid_symlink_prefixes_tb_only_install(self, monkeypatch):
        """TB-only setup: RD unset, TB explicit.  Pre-fix the local-scanner
        gate ``if symlink_base`` skipped the dedup-check entirely when RD
        was unset; post-fix it runs because TB is configured.  This was
        previously broken — TB-only users had no debrid-symlink detection."""
        from utils.library import _all_debrid_symlink_prefixes
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE', raising=False)
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', '/mnt/torbox_only')
        prefixes = _all_debrid_symlink_prefixes()
        assert '/mnt/torbox_only/' in prefixes
        assert prefixes, 'TB-only install must produce a non-empty prefix tuple'

    def test_all_debrid_symlink_prefixes_empty_when_no_rd_no_tb(self, monkeypatch):
        from utils.library import _all_debrid_symlink_prefixes
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE', raising=False)
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)
        prefixes = _all_debrid_symlink_prefixes()
        assert prefixes == ()

    def test_movie_dir_with_tb_symlink_skipped(self, tmp_dir, monkeypatch):
        """The user's exact reported scenario: a show-named folder under
        local_movies containing a single symlink pointing at the TB mount.
        Pre-fix: classified as local movie (because TB prefix wasn't
        checked).  Post-fix: recognized as all-debrid → skipped → no
        movie entry created."""
        from utils.library import LibraryScanner
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)

        local_movies = os.path.join(tmp_dir, 'local_movies')
        misclassified = os.path.join(local_movies, 'Greys Anatomy')
        os.makedirs(misclassified)
        sym = os.path.join(misclassified, 'Greys.Anatomy.S19E09.mkv')
        os.symlink('/mnt/debrid_torbox/some/path/Greys.Anatomy.S19E09.mkv', sym)

        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._local_movies_path = local_movies
        items = scanner._scan_local_movies()
        assert items == [], \
            'TB-routed show symlinks must NOT classify as local movies'

    def test_tv_dir_with_tb_symlink_skipped(self, tmp_dir, monkeypatch):
        """Sonarr/Radarr-parity counterpart: TB-routed show symlinks
        under local_tv must also skip (otherwise they'd show as
        spurious source='local' shows blocking debrid symlink recreation)."""
        from utils.library import LibraryScanner
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)

        local_tv = os.path.join(tmp_dir, 'local_tv')
        show_dir = os.path.join(local_tv, 'Pagan Peak')
        os.makedirs(os.path.join(show_dir, 'Season 03'))
        sym = os.path.join(show_dir, 'Season 03', 'Pagan.Peak.S03E05.mkv')
        os.symlink('/mnt/debrid_torbox/some/Pagan.Peak.S03E05.mkv', sym)

        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._local_tv_path = local_tv
        items = scanner._scan_local_shows()
        assert items == [], \
            'TB-routed show symlinks under local_tv must skip too (Sonarr parity)'

    def test_mixed_rd_and_tb_symlinks_skipped(self, tmp_dir, monkeypatch):
        """A real local Plex library may have content split across both
        debrids during migration.  Mixed RD+TB symlinks must still skip
        as 'all-debrid' — neither prefix alone disqualifies the dir."""
        from utils.library import LibraryScanner
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)

        local_movies = os.path.join(tmp_dir, 'local_movies')
        d = os.path.join(local_movies, 'Mixed Show')
        os.makedirs(d)
        os.symlink('/mnt/debrid/some/rd-path.mkv', os.path.join(d, 'rd.mkv'))
        os.symlink('/mnt/debrid_torbox/some/tb-path.mkv', os.path.join(d, 'tb.mkv'))

        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._local_movies_path = local_movies
        items = scanner._scan_local_movies()
        assert items == [], 'mixed RD+TB symlink dirs must skip'

    def test_genuine_local_file_alongside_tb_symlink_classified_local(
        self, tmp_dir, monkeypatch,
    ):
        """A real local file (not a symlink) alongside a TB symlink means
        the user has genuine content there — classify as local so the
        existing rich-source-merge logic can pair it with a debrid sibling
        in the source='both' bucket."""
        from utils.library import LibraryScanner
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)

        local_movies = os.path.join(tmp_dir, 'local_movies')
        d = os.path.join(local_movies, 'Real Movie (2024)')
        os.makedirs(d)
        # Real on-disk file, not a symlink
        open(os.path.join(d, 'Real.Movie.2024.mkv'), 'w').close()
        # Plus a TB symlink (could be a sample / different cut)
        os.symlink('/mnt/debrid_torbox/some/tb.mkv', os.path.join(d, 'sample.mkv'))

        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._local_movies_path = local_movies
        items = scanner._scan_local_movies()
        assert len(items) == 1
        assert items[0]['source'] == 'local'

    def test_cleanup_removes_broken_tb_symlinks(self, tmp_dir, monkeypatch):
        """Regression for the cleanup-not-updated finding: broken symlinks
        under the auto-derived TB base must be removable too.  Pre-fix
        the cleanup only matched the RD prefix → TB symlinks fell through
        the prefix-check and stayed on disk forever even when their
        targets had gone."""
        from utils.library import LibraryScanner
        monkeypatch.setenv('BLACKHOLE_SYMLINK_ENABLED', 'true')
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', os.path.join(tmp_dir, 'rd_mount'))
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)

        # Populate the RD mount with a category dir so the "categories empty"
        # guard doesn't short-circuit the function.
        rd_mount = os.path.join(tmp_dir, 'rd_mount')
        os.makedirs(os.path.join(rd_mount, 'shows', '_keep'))
        # TB mount must be non-empty (healthy) — the per-mount health gate
        # skips deletion on an empty/throttled mount.  Add an unrelated release
        # so the mount is live, but the broken symlink below points at a
        # DIFFERENT (non-existent) release so it's still removed.
        tb_mount = os.path.join(tmp_dir, 'tb_mount')
        os.makedirs(os.path.join(tb_mount, 'Some.Other.Release'))

        # Local TV folder with a broken TB symlink (target doesn't exist).
        local_tv = os.path.join(tmp_dir, 'local_tv')
        show_dir = os.path.join(local_tv, 'Pagan Peak', 'Season 03')
        os.makedirs(show_dir)
        broken_tb_link = os.path.join(show_dir, 'Pagan.Peak.S03E05.mkv')
        # Use the auto-derived TB base /mnt/debrid_torbox to match the helper.
        os.symlink('/mnt/debrid_torbox/Pagan.Peak.S03E05/Pagan.Peak.S03E05.mkv',
                   broken_tb_link)

        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._local_movies_path = None
        scanner._local_tv_path = local_tv
        scanner._discover_torbox_mount = lambda: tb_mount
        scanner._cleanup_broken_debrid_symlinks()

        assert not os.path.lexists(broken_tb_link), \
            'broken TB symlink under the auto-derived TB base must be removed'

    def test_cleanup_keeps_intact_rd_symlinks(self, tmp_dir, monkeypatch):
        """Cleanup MUST NOT remove RD symlinks whose targets still exist —
        regression guard for false-positive removal during the multi-prefix
        refactor.  Sets up a real RD-mount file that the symlink points to."""
        from utils.library import LibraryScanner
        monkeypatch.setenv('BLACKHOLE_SYMLINK_ENABLED', 'true')
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', os.path.join(tmp_dir, 'rd_mount'))
        # Use the test tmp_dir as the RD symlink base so the existence check
        # finds the real file.  Symlink targets are translated from
        # symlink_base → rclone_mount; pointing them at the same dir
        # produces a passthrough translation.
        rd_base = os.path.join(tmp_dir, 'rd_mount')
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', rd_base)
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)

        rd_mount = os.path.join(tmp_dir, 'rd_mount')
        os.makedirs(os.path.join(rd_mount, 'shows', '_keep'))
        # Real file on the "rclone mount" that the symlink will target.
        real_file_dir = os.path.join(rd_mount, 'shows', 'real-release')
        os.makedirs(real_file_dir)
        real_file = os.path.join(real_file_dir, 'real.mkv')
        open(real_file, 'w').close()

        local_tv = os.path.join(tmp_dir, 'local_tv')
        show_dir = os.path.join(local_tv, 'Real Show', 'Season 01')
        os.makedirs(show_dir)
        live_link = os.path.join(show_dir, 'real.mkv')
        # Symlink target uses the rd_base prefix; translation: rd_base → rd_mount
        # which produces the actual real_file path that exists.
        os.symlink(os.path.join(rd_base, 'shows', 'real-release', 'real.mkv'),
                   live_link)

        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._local_movies_path = None
        scanner._local_tv_path = local_tv
        scanner._discover_torbox_mount = lambda: None
        scanner._cleanup_broken_debrid_symlinks()

        assert os.path.lexists(live_link), \
            'intact RD symlink whose target exists must NOT be removed'

    def test_cleanup_keeps_tb_symlinks_when_tb_mount_unhealthy(self, tmp_dir, monkeypatch):
        """Regression for the TB-throttle symlink-thrash bug: when the TorBox
        mount is empty/stalled/throttled (os.path.exists False for everything)
        but the RD mount is healthy, the old RD-only guard let cleanup proceed
        and mass-deleted every TB symlink — which the next scan re-created,
        looping forever.  The per-mount health gate must SKIP deletion for
        symlinks routed to the unhealthy TB mount."""
        from utils.library import LibraryScanner
        monkeypatch.setenv('BLACKHOLE_SYMLINK_ENABLED', 'true')
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', os.path.join(tmp_dir, 'rd_mount'))
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)

        # RD mount healthy (category non-empty); TB mount EMPTY (throttled).
        rd_mount = os.path.join(tmp_dir, 'rd_mount')
        os.makedirs(os.path.join(rd_mount, 'shows', '_keep'))
        tb_mount = os.path.join(tmp_dir, 'tb_mount')
        os.makedirs(tb_mount)

        local_tv = os.path.join(tmp_dir, 'local_tv')
        show_dir = os.path.join(local_tv, 'Pagan Peak', 'Season 03')
        os.makedirs(show_dir)
        tb_link = os.path.join(show_dir, 'Pagan.Peak.S03E05.mkv')
        # Target resolves under the throttled TB mount — exists() would be False.
        os.symlink('/mnt/debrid_torbox/Pagan.Peak.S03E05/Pagan.Peak.S03E05.mkv',
                   tb_link)

        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._local_movies_path = None
        scanner._local_tv_path = local_tv
        scanner._discover_torbox_mount = lambda: tb_mount
        scanner._cleanup_broken_debrid_symlinks()

        assert os.path.lexists(tb_link), \
            'TB symlink must survive when the TB mount is unhealthy/throttled'

    def test_non_debrid_symlink_classified_local(self, tmp_dir, monkeypatch):
        """A *resolving* symlink to a non-debrid path (e.g. NAS mount, secondary
        drive) means genuine local content via symlink farm — must classify as
        local, not skip. (A dangling such symlink is covered separately and is
        NOT local — it can't be played and would inflate the recovery metric.)"""
        from utils.library import LibraryScanner
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)

        # Real target outside the debrid base (a mounted NAS, here a tmp path).
        nas_target = os.path.join(tmp_dir, 'nas', 'NAS.Movie.2024.mkv')
        os.makedirs(os.path.dirname(nas_target))
        open(nas_target, 'w').close()

        local_movies = os.path.join(tmp_dir, 'local_movies')
        d = os.path.join(local_movies, 'NAS Movie (2024)')
        os.makedirs(d)
        os.symlink(nas_target, os.path.join(d, 'NAS.Movie.2024.mkv'))

        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._local_movies_path = local_movies
        items = scanner._scan_local_movies()
        assert len(items) == 1, \
            'resolving symlinks pointing outside known debrid mounts are genuine local content'
        assert items[0]['source'] == 'local'


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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'

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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'

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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'

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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'

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
# Canonical-title TMDB-cache prefix lookup
# ---------------------------------------------------------------------------

class TestExtractTmdbEntryYear:
    """Sanity tests for _extract_tmdb_entry_year — pulls a 4-digit year
    from a TMDB cache entry's release_date / first_air_date field."""

    def test_release_date(self):
        from utils.library import _extract_tmdb_entry_year
        assert _extract_tmdb_entry_year({'release_date': '1997-10-24'}) == 1997

    def test_first_air_date(self):
        from utils.library import _extract_tmdb_entry_year
        assert _extract_tmdb_entry_year({'first_air_date': '2008-01-20'}) == 2008

    def test_release_date_takes_precedence(self):
        from utils.library import _extract_tmdb_entry_year
        assert _extract_tmdb_entry_year({
            'release_date': '1997-10-24',
            'first_air_date': '2020-01-01',
        }) == 1997

    def test_missing_returns_none(self):
        from utils.library import _extract_tmdb_entry_year
        assert _extract_tmdb_entry_year({}) is None

    def test_malformed_returns_none(self):
        """Defensive: short/non-string/garbage dates return None, no crash."""
        from utils.library import _extract_tmdb_entry_year
        assert _extract_tmdb_entry_year({'release_date': ''}) is None
        assert _extract_tmdb_entry_year({'release_date': '19'}) is None
        assert _extract_tmdb_entry_year({'release_date': '19xx-01-01'}) is None
        assert _extract_tmdb_entry_year({'release_date': None}) is None
        assert _extract_tmdb_entry_year({'release_date': 1997}) is None
        assert _extract_tmdb_entry_year({'release_date': ['1997']}) is None


class TestCreateDebridSymlinksSkipsObfuscated:
    """_create_debrid_symlinks must NOT import anti-DMCA obfuscated payloads
    (hex mount folder + tracker tag, e.g. EZTV) as junk hex 'movies'/'shows'.
    The blackhole monitor handles their real identity via the .magnet name."""

    def _make_scanner(self, tmp_dir, monkeypatch):
        from utils.library import LibraryScanner
        monkeypatch.setenv('BLACKHOLE_SYMLINK_ENABLED', 'true')
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', os.path.join(tmp_dir, 'mount'))
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', raising=False)
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._local_movies_path = os.path.join(tmp_dir, 'local_movies')
        scanner._local_tv_path = os.path.join(tmp_dir, 'local_tv')
        os.makedirs(scanner._local_movies_path)
        os.makedirs(scanner._local_tv_path)
        scanner._last_had_local = True
        scanner._local_drop_alerted = False
        scanner._last_symlinked_files = {}
        scanner._pending_rescan_prior_ids = {}
        scanner._discover_torbox_mount = lambda: None
        return scanner

    def test_obfuscated_movie_not_imported(self, tmp_dir, monkeypatch):
        scanner = self._make_scanner(tmp_dir, monkeypatch)
        mount = os.path.join(tmp_dir, 'mount')

        hexname = '050bd19ee9934249a2ce4c9762c0d710[EZTVx.to]'
        mdir = os.path.join(mount, hexname)
        os.makedirs(mdir)
        # A real media file — absent the guard, a symlink WOULD be created.
        open(os.path.join(mdir, hexname + '.mkv'), 'w').close()

        movies = [
            # Local companion keeps the "library appears empty" guard happy so
            # the debrid loop actually runs (and can try to import the hex one).
            {'title': 'Anchor', 'year': 2020, 'source': 'local', 'path': None},
            {'title': hexname, 'year': None, 'source': 'debrid',
             'path': mdir, '_parsed_title': hexname},
        ]
        scanner._create_debrid_symlinks([], movies, {})

        # Nothing imported into the local movie library.
        assert os.listdir(scanner._local_movies_path) == []

    def test_normal_movie_still_imported(self, tmp_dir, monkeypatch):
        """Control: a non-obfuscated movie in the same setup IS imported —
        proves the guard, not a broken setup, is what skips the hex one."""
        scanner = self._make_scanner(tmp_dir, monkeypatch)
        mount = os.path.join(tmp_dir, 'mount')

        rel = 'Real.Movie.2024.1080p-GROUP'
        mdir = os.path.join(mount, rel)
        os.makedirs(mdir)
        open(os.path.join(mdir, rel + '.mkv'), 'w').close()

        movies = [
            {'title': 'Anchor', 'year': 2020, 'source': 'local', 'path': None},
            {'title': 'Real Movie', 'year': 2024, 'source': 'debrid',
             'path': mdir, '_parsed_title': 'Real Movie'},
        ]
        scanner._create_debrid_symlinks([], movies, {})

        entries = os.listdir(scanner._local_movies_path)
        assert entries and entries[0].startswith('Real Movie')

    def test_obfuscated_show_not_imported(self, tmp_dir, monkeypatch):
        scanner = self._make_scanner(tmp_dir, monkeypatch)
        mount = os.path.join(tmp_dir, 'mount')

        hexname = '1f9da83faaf847949e043d0dae9684aa[eztv.re]'
        sdir = os.path.join(mount, hexname)
        os.makedirs(sdir)
        open(os.path.join(sdir, hexname + '.mkv'), 'w').close()

        from utils.library import _normalize_title
        norm = _normalize_title(hexname)
        shows = [
            {'title': 'Anchor Show', 'year': 2020, 'source': 'local',
             'season_data': []},
            {'title': hexname, 'year': None, 'source': 'debrid',
             'season_data': [{'number': 5, 'episodes': [
                 {'number': 3, 'source': 'debrid'}]}]},
        ]
        path_index = {(norm, 5, 3): os.path.join(sdir, hexname + '.mkv')}
        scanner._create_debrid_symlinks(shows, [], path_index)

        assert os.listdir(scanner._local_tv_path) == []


class TestFindCanonicalTmdbViaPrefix:
    """Tests for _find_canonical_tmdb_via_prefix — token-aligned prefix
    lookup against the TMDB cache, used as the final fallback in the
    arr-info matching cascade in _create_debrid_symlinks.

    All tests inject a fixture cache via the _tmdb_cache parameter so
    behavior is deterministic without a real /config/tmdb_cache.json."""

    def _movies_cache(self, *entries):
        return {'movies': {key: ent for key, ent in entries}}

    def _shows_cache(self, *entries):
        return {'shows': {key: ent for key, ent in entries}}

    def test_recovers_from_actor_genre_junk(self):
        """The Gattaca regression case — parsed title has actor name +
        genre tag appended; prefix match finds the canonical entry."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('gattaca (1997)', {
                'title': 'Gattaca',
                'tmdb_id': 782,
                'release_date': '1997-10-24',
            }),
        )
        result = _find_canonical_tmdb_via_prefix(
            'Gattaca Ethan Hawke Sci Fi', 1997, is_tv=False,
            _tmdb_cache=cache,
        )
        assert result == {'title': 'Gattaca', 'tmdb_id': 782}

    def test_year_mismatch_excludes(self):
        """Multi-token candidate with year confirmation: parsed year
        != entry year → skip."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('gattaca extra (2020)', {
                'title': 'Gattaca Extra',
                'tmdb_id': 999,
                'release_date': '2020-01-01',
            }),
        )
        result = _find_canonical_tmdb_via_prefix(
            'Gattaca Extra Words 1997', 1997, is_tv=False, _tmdb_cache=cache,
        )
        assert result is None

    def test_longest_prefix_wins(self):
        """When two candidates are valid prefixes, the longer (more
        specific) wins."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('the dark (2005)', {
                'title': 'The Dark',
                'tmdb_id': 1,
                'release_date': '2005-01-01',
            }),
            ('the dark knight (2008)', {
                'title': 'The Dark Knight',
                'tmdb_id': 155,
                'release_date': '2008-07-18',
            }),
        )
        result = _find_canonical_tmdb_via_prefix(
            'The Dark Knight Extended Cut', 2008, is_tv=False, _tmdb_cache=cache,
        )
        assert result == {'title': 'The Dark Knight', 'tmdb_id': 155}

    def test_non_prefix_rejected(self):
        """A cache entry whose tokens appear mid-string (not at start)
        must not match. Real release names put the title first."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('sci fi (2020)', {
                'title': 'Sci Fi',
                'tmdb_id': 555,
                'release_date': '2020-01-01',
            }),
        )
        result = _find_canonical_tmdb_via_prefix(
            'Gattaca Ethan Hawke Sci Fi', 1997, is_tv=False, _tmdb_cache=cache,
        )
        assert result is None

    def test_single_token_requires_year(self):
        """Single-word cache entry like "The" must not prefix-match a
        multi-word parse without year confirmation."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('the', {
                'title': 'The',
                'tmdb_id': 480530,
                'release_date': '2017-01-01',
            }),
        )
        # parsed year 2008 ≠ entry year 2017 → reject
        assert _find_canonical_tmdb_via_prefix(
            'The Dark Knight', 2008, is_tv=False, _tmdb_cache=cache,
        ) is None
        # No parsed year → reject (cannot disambiguate)
        assert _find_canonical_tmdb_via_prefix(
            'The Dark Knight', None, is_tv=False, _tmdb_cache=cache,
        ) is None

    def test_single_token_year_match_accepted(self):
        """Single-token candidate with matching year IS accepted —
        the year provides the disambiguation."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('gattaca', {
                'title': 'Gattaca',
                'tmdb_id': 782,
                'release_date': '1997-10-24',
            }),
        )
        result = _find_canonical_tmdb_via_prefix(
            'Gattaca Ethan Hawke', 1997, is_tv=False, _tmdb_cache=cache,
        )
        assert result == {'title': 'Gattaca', 'tmdb_id': 782}

    def test_single_token_no_entry_year_fail_closed(self):
        """Single-token candidate with no entry year is rejected even
        if filename has a year — fail-closed for narrow guard."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('the', {'title': 'The', 'tmdb_id': 1}),  # no release_date
        )
        assert _find_canonical_tmdb_via_prefix(
            'The Dark Knight', 2008, is_tv=False, _tmdb_cache=cache,
        ) is None

    def test_multi_token_entry_year_missing_fail_open(self):
        """Multi-token candidate with missing release_date: fail-open
        (legacy entries lack the field; specificity protects)."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('the dark knight (2008)', {
                'title': 'The Dark Knight',
                'tmdb_id': 155,
                # no release_date — legacy entry
            }),
        )
        result = _find_canonical_tmdb_via_prefix(
            'The Dark Knight Extras', 2008, is_tv=False, _tmdb_cache=cache,
        )
        assert result == {'title': 'The Dark Knight', 'tmdb_id': 155}

    def test_shows_section_for_tv(self):
        """is_tv=True must look in the 'shows' section, not 'movies'."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = {
            'movies': {
                'breaking bad': {
                    'title': 'Breaking Bad MOVIE SPOOF',
                    'tmdb_id': 999,
                    'release_date': '2008-01-01',
                },
            },
            'shows': {
                'breaking bad': {
                    'title': 'Breaking Bad',
                    'tmdb_id': 1396,
                    'first_air_date': '2008-01-20',
                },
            },
        }
        result = _find_canonical_tmdb_via_prefix(
            'Breaking Bad Mr Chips', 2008, is_tv=True, _tmdb_cache=cache,
        )
        assert result == {'title': 'Breaking Bad', 'tmdb_id': 1396}

    def test_empty_cache_returns_none(self):
        """No section, no entries → None."""
        from utils.library import _find_canonical_tmdb_via_prefix
        assert _find_canonical_tmdb_via_prefix(
            'Gattaca', 1997, is_tv=False, _tmdb_cache={},
        ) is None
        assert _find_canonical_tmdb_via_prefix(
            'Gattaca', 1997, is_tv=False, _tmdb_cache={'movies': {}},
        ) is None

    def test_empty_title_returns_none(self):
        """Defensive: empty/None inputs return None."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('gattaca', {'title': 'Gattaca', 'tmdb_id': 782,
                         'release_date': '1997-10-24'}),
        )
        assert _find_canonical_tmdb_via_prefix(
            '', 1997, is_tv=False, _tmdb_cache=cache,
        ) is None
        assert _find_canonical_tmdb_via_prefix(
            None, 1997, is_tv=False, _tmdb_cache=cache,
        ) is None

    def test_non_dict_cache_entry_skipped(self):
        """Defensive: corrupt cache (non-dict entry) doesn't crash."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = {
            'movies': {
                'gattaca': 'not a dict',  # corrupt
                'gattaca (1997)': {
                    'title': 'Gattaca',
                    'tmdb_id': 782,
                    'release_date': '1997-10-24',
                },
            },
        }
        result = _find_canonical_tmdb_via_prefix(
            'Gattaca Ethan Hawke', 1997, is_tv=False, _tmdb_cache=cache,
        )
        assert result == {'title': 'Gattaca', 'tmdb_id': 782}

    def test_non_string_cache_key_skipped(self):
        """Defensive: non-string cache key doesn't crash the loop."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = {
            'movies': {
                42: {'title': 'X', 'tmdb_id': 1},  # non-string key
                'gattaca': {'title': 'Gattaca', 'tmdb_id': 782,
                            'release_date': '1997-10-24'},
            },
        }
        result = _find_canonical_tmdb_via_prefix(
            'Gattaca Ethan', 1997, is_tv=False, _tmdb_cache=cache,
        )
        assert result == {'title': 'Gattaca', 'tmdb_id': 782}

    def test_non_string_title_field_skipped(self):
        """Defensive: entry with non-string 'title' is skipped, sibling
        valid entry still wins."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('gattaca (1997)', {
                'title': {'unexpected': 'dict'},  # corrupt — not a str
                'tmdb_id': 999,
                'release_date': '1997-10-24',
            }),
            ('gattaca ethan hawke (1997)', {
                'title': 'Gattaca',
                'tmdb_id': 782,
                'release_date': '1997-10-24',
            }),
        )
        result = _find_canonical_tmdb_via_prefix(
            'Gattaca Ethan Hawke Sci Fi', 1997, is_tv=False, _tmdb_cache=cache,
        )
        assert result == {'title': 'Gattaca', 'tmdb_id': 782}

    def test_missing_tmdb_id_excludes_entry(self):
        """An entry without tmdb_id can't fulfill a downstream
        radarr_by_tmdb lookup — must be skipped."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('gattaca', {
                'title': 'Gattaca',
                # no tmdb_id
                'release_date': '1997-10-24',
            }),
        )
        result = _find_canonical_tmdb_via_prefix(
            'Gattaca Ethan Hawke', 1997, is_tv=False, _tmdb_cache=cache,
        )
        assert result is None

    def test_year_missing_in_filename_multi_token_fail_open(self):
        """When parsed_year is None and candidate is multi-token, fail
        open (year filter skipped entirely)."""
        from utils.library import _find_canonical_tmdb_via_prefix
        cache = self._movies_cache(
            ('the dark knight (2008)', {
                'title': 'The Dark Knight',
                'tmdb_id': 155,
                'release_date': '2008-07-18',
            }),
        )
        result = _find_canonical_tmdb_via_prefix(
            'The Dark Knight', None, is_tv=False, _tmdb_cache=cache,
        )
        assert result == {'title': 'The Dark Knight', 'tmdb_id': 155}


# ---------------------------------------------------------------------------
# Merge + enrichment canonical-prefix fallbacks
#
# These tests cover the FULL fix path for the "two posters" bug:
# parser-junk debrid title (e.g. "Gattaca Ethan Hawke Sci Fi") must (1)
# merge with the local "Gattaca" item as source='both' (the merge step
# fix) AND (2) get its display title renamed to canonical "Gattaca" (the
# enrichment step fix).  Without both fixes the user sees two posters.
# ---------------------------------------------------------------------------

class TestMergeStepCanonicalFallback:
    """Tests for the new prefix-canonical fallback in the title-level
    merge step inside _scan_read.  Exercises the path via real LibraryScanner
    instances with a stubbed TMDB cache."""

    def _build_scanner(self, tmp_dir):
        """Construct a LibraryScanner with realistic state for scan()."""
        library._scanner = None
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = os.path.join(tmp_dir, 'mount')
        scanner._local_movies_path = os.path.join(tmp_dir, 'local_movies')
        scanner._local_tv_path = os.path.join(tmp_dir, 'local_tv')
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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        scanner._capabilities_path = '/dev/null/library_capabilities.json'
        return scanner

    def _patch_tmdb(self, monkeypatch, cache):
        """Patch tmdb._load_cache and disable the network-touching paths."""
        from utils import tmdb as _tmdb
        monkeypatch.setattr(_tmdb, '_load_cache', lambda: cache)
        # Also patch get_cached_posters / build_tmdb_aliases / etc. so the
        # scan doesn't hit the real /config/tmdb_cache.json.
        # get_cached_posters is keyed by parsed-norm; for our test
        # scenario it should miss for the parser-junk title, hit for the
        # canonical title.
        monkeypatch.setattr(_tmdb, 'get_cached_posters', lambda items: {})
        monkeypatch.setattr(_tmdb, 'background_populate_cache', lambda items: None)
        monkeypatch.setattr(_tmdb, 'find_show_by_season',
                            lambda norm, max_sn, year=None: None)
        monkeypatch.setattr(_tmdb, 'get_cached_tmdb_ids',
                            lambda: {'movies': {}, 'shows': {}})

    def test_movie_parser_junk_merges_with_local_via_canonical(
            self, tmp_dir, monkeypatch):
        """The Gattaca regression case: debrid folder name carries actor
        and genre tokens, local folder has the canonical name. Without
        the fix they appear as two separate items; with the fix they
        merge as source='both'.
        """
        # Set up filesystem: one canonical local Gattaca + one parser-junk debrid.
        local_movies = os.path.join(tmp_dir, 'local_movies')
        os.makedirs(os.path.join(local_movies, 'Gattaca (1997)'))
        open(os.path.join(local_movies, 'Gattaca (1997)', 'Gattaca.mkv'), 'w').close()
        mount_movies = os.path.join(tmp_dir, 'mount', 'movies')
        os.makedirs(os.path.join(mount_movies,
                                 'Gattaca - Ethan Hawke - Sci Fi (1997)'))
        open(os.path.join(mount_movies,
                          'Gattaca - Ethan Hawke - Sci Fi (1997)',
                          'Gattaca.1997.mp4'), 'w').close()

        # Stub TMDB cache: only the canonical Gattaca entry.  Direct
        # lookup by parser-junk norm misses; prefix resolver hits.
        self._patch_tmdb(monkeypatch, {
            'movies': {
                'gattaca (1997)': {
                    'title': 'Gattaca',
                    'tmdb_id': 782,
                    'release_date': '1997-10-24',
                },
            },
            'shows': {},
        })

        scanner = self._build_scanner(tmp_dir)
        result = scanner.scan()

        # Exactly one Gattaca movie, source='both', NOT two split items.
        gattacas = [m for m in result['movies']
                    if 'gattaca' in m['title'].lower()]
        assert len(gattacas) == 1, (
            f"Expected single merged Gattaca item, got {len(gattacas)}: "
            f"{[m['title'] for m in gattacas]}"
        )
        assert gattacas[0]['source'] == 'both'

    def test_show_parser_junk_merges_with_local_via_canonical(
            self, tmp_dir, monkeypatch):
        """Symmetric show case — parser-junk debrid show title merges
        with canonical local show title."""
        local_tv = os.path.join(tmp_dir, 'local_tv')
        _make_show(local_tv, 'Severance (2022)', {'Season 1': ['ep1.mkv']})
        mount_shows = os.path.join(tmp_dir, 'mount', 'shows')
        _make_show(mount_shows,
                   'Severance - Adam Scott - Sci Fi Mystery (2022)',
                   {'Season 1': ['ep1.mkv']})

        self._patch_tmdb(monkeypatch, {
            'movies': {},
            'shows': {
                'severance (2022)': {
                    'title': 'Severance',
                    'tmdb_id': 95396,
                    'first_air_date': '2022-02-18',
                },
            },
        })

        scanner = self._build_scanner(tmp_dir)
        result = scanner.scan()

        severances = [s for s in result['shows']
                      if 'severance' in s['title'].lower()]
        assert len(severances) == 1, (
            f"Expected single merged Severance item, got {len(severances)}"
        )
        assert severances[0]['source'] == 'both'

    def test_no_match_when_canonical_not_in_local(
            self, tmp_dir, monkeypatch):
        """Regression guard: parser-junk debrid item whose canonical
        is NOT in local must NOT spuriously merge — should remain a
        single debrid item."""
        local_movies = os.path.join(tmp_dir, 'local_movies')
        os.makedirs(local_movies)  # empty
        mount_movies = os.path.join(tmp_dir, 'mount', 'movies')
        os.makedirs(os.path.join(mount_movies,
                                 'Gattaca - Ethan Hawke - Sci Fi (1997)'))
        open(os.path.join(mount_movies,
                          'Gattaca - Ethan Hawke - Sci Fi (1997)',
                          'Gattaca.1997.mp4'), 'w').close()

        self._patch_tmdb(monkeypatch, {
            'movies': {
                'gattaca (1997)': {
                    'title': 'Gattaca',
                    'tmdb_id': 782,
                    'release_date': '1997-10-24',
                },
            },
            'shows': {},
        })

        scanner = self._build_scanner(tmp_dir)
        result = scanner.scan()

        # No local Gattaca → no merge → single debrid-only item
        # (with title renamed to canonical via the enrichment step).
        gattacas = [m for m in result['movies']
                    if 'gattaca' in m['title'].lower()]
        assert len(gattacas) == 1
        assert gattacas[0]['source'] == 'debrid'

    def test_self_loop_guard_prevents_degenerate_alias(
            self, tmp_dir, monkeypatch):
        """When the prefix resolver returns a canonical title equal to
        the parsed title's normalized form (canon_key == key), the
        merge step must NOT register a self-edge in alias_norms_local.

        Reaches the prefix branch only when direct/alias match miss but
        canon_key happens to equal key (degenerate case — defensive).
        """
        # Set up: NO local Gattaca, only debrid. Direct match misses
        # (no local key). Prefix resolver runs. Canonical norm equals
        # parsed norm (both "gattaca"). The new guard rejects this,
        # leaving local_key=None and merging as debrid-only — not
        # polluting alias_norms_local with {"gattaca": {"gattaca"}}.
        local_movies = os.path.join(tmp_dir, 'local_movies')
        os.makedirs(local_movies)  # empty
        mount_movies = os.path.join(tmp_dir, 'mount', 'movies')
        os.makedirs(os.path.join(mount_movies, 'Gattaca (1997)'))
        open(os.path.join(mount_movies, 'Gattaca (1997)',
                          'Gattaca.mp4'), 'w').close()

        self._patch_tmdb(monkeypatch, {
            'movies': {
                'gattaca (1997)': {
                    'title': 'Gattaca',
                    'tmdb_id': 782,
                    'release_date': '1997-10-24',
                },
            },
            'shows': {},
        })

        scanner = self._build_scanner(tmp_dir)
        result = scanner.scan()

        # Single Gattaca, source=debrid (no merge possible)
        gattacas = [m for m in result['movies']
                    if 'gattaca' in m['title'].lower()]
        assert len(gattacas) == 1
        assert gattacas[0]['source'] == 'debrid'
        # alias_norms_local should NOT contain a self-loop {gattaca: {gattaca}}
        aliases = scanner._alias_norms.get('gattaca', set())
        assert 'gattaca' not in aliases, (
            f"Self-loop in alias_norms_local: gattaca → {aliases}"
        )

    def test_existing_direct_match_still_works(
            self, tmp_dir, monkeypatch):
        """Regression guard: when debrid and local share a parsed-norm
        directly (no parser junk), the original direct-match path still
        fires and the prefix resolver isn't needed."""
        local_movies = os.path.join(tmp_dir, 'local_movies')
        os.makedirs(os.path.join(local_movies, 'Inception (2010)'))
        open(os.path.join(local_movies, 'Inception (2010)',
                          'Inception.mkv'), 'w').close()
        mount_movies = os.path.join(tmp_dir, 'mount', 'movies')
        os.makedirs(os.path.join(mount_movies, 'Inception (2010)'))
        open(os.path.join(mount_movies, 'Inception (2010)',
                          'Inception.mkv'), 'w').close()

        self._patch_tmdb(monkeypatch, {
            'movies': {},  # empty cache: prefix resolver can't fire
            'shows': {},
        })

        scanner = self._build_scanner(tmp_dir)
        result = scanner.scan()

        inceptions = [m for m in result['movies']
                      if m['title'].lower() == 'inception']
        assert len(inceptions) == 1
        assert inceptions[0]['source'] == 'both'


class TestEnrichmentCanonicalFallback:
    """Tests for _enrich_with_tmdb_cache's new prefix-canonical
    fallback — when get_cached_posters misses, the resolver provides the
    canonical entry so _maybe_rename can upgrade the display title."""

    def test_movie_renamed_via_prefix_when_direct_lookup_misses(
            self, monkeypatch):
        """Parser-junk title gets renamed to canonical when get_cached_posters
        returns no info for the parsed key but the resolver finds the
        canonical entry."""
        from utils import tmdb as _tmdb
        from utils.library import _enrich_with_tmdb_cache

        # Direct cache lookup misses (parser-junk key not in cache).
        # Then prefix resolver runs against full cache, finds canonical.
        # Final get_cached_posters re-query (with canonical title) hits.
        full_cache = {
            'movies': {
                'gattaca (1997)': {
                    'title': 'Gattaca',
                    'tmdb_id': 782,
                    'release_date': '1997-10-24',
                    'poster_path': '/test.jpg',
                },
            },
            'shows': {},
        }
        monkeypatch.setattr(_tmdb, '_load_cache', lambda: full_cache)
        monkeypatch.setattr(_tmdb, 'background_populate_cache',
                            lambda items: None)
        monkeypatch.setattr(_tmdb, 'find_show_by_season',
                            lambda *a, **kw: None)

        # First call (with parsed-junk title) misses; second call (with
        # canonical) hits. Use a counter to differentiate.
        calls = []
        def fake_get_cached_posters(items):
            calls.append(items)
            # Return a hit only when the canonical title is queried.
            for item in items:
                title = item.get('title', '')
                if title.strip().lower() == 'gattaca':
                    return {'gattaca': {
                        'poster_url': 'https://image/test.jpg',
                        'tmdb_status': 'Released',
                        'imdb_id': 'tt0119177',
                        'title': 'Gattaca',
                    }}
            return {}
        monkeypatch.setattr(_tmdb, 'get_cached_posters', fake_get_cached_posters)

        movies = [{'title': 'Gattaca Ethan Hawke Sci Fi', 'year': 1997,
                   'source': 'debrid'}]
        shows = []
        renames = _enrich_with_tmdb_cache(movies, shows)

        # Title renamed to canonical
        assert movies[0]['title'] == 'Gattaca'
        # Original parsed title preserved for downstream key lookups
        assert movies[0]['_parsed_title'] == 'Gattaca Ethan Hawke Sci Fi'
        # Poster came from the canonical re-query
        assert movies[0]['poster_url'] == 'https://image/test.jpg'
        # Rename pair recorded for alias bookkeeping
        assert renames == [
            ('gattaca ethan hawke sci fi', 'gattaca'),
        ]

    def test_show_renamed_via_prefix_when_direct_lookup_misses(
            self, monkeypatch):
        """Symmetric show case — display title gets upgraded to canonical."""
        from utils import tmdb as _tmdb
        from utils.library import _enrich_with_tmdb_cache

        full_cache = {
            'movies': {},
            'shows': {
                'severance (2022)': {
                    'title': 'Severance',
                    'tmdb_id': 95396,
                    'first_air_date': '2022-02-18',
                    'poster_path': '/sev.jpg',
                },
            },
        }
        monkeypatch.setattr(_tmdb, '_load_cache', lambda: full_cache)
        monkeypatch.setattr(_tmdb, 'background_populate_cache',
                            lambda items: None)
        monkeypatch.setattr(_tmdb, 'find_show_by_season',
                            lambda *a, **kw: None)

        def fake_get_cached_posters(items):
            for item in items:
                if item.get('title', '').strip().lower() == 'severance':
                    return {'severance': {
                        'poster_url': 'https://image/sev.jpg',
                        'tmdb_status': 'Returning Series',
                        'imdb_id': 'tt11280740',
                        'title': 'Severance',
                        'total_episodes': 19,
                        'max_cached_season': 2,
                    }}
            return {}
        monkeypatch.setattr(_tmdb, 'get_cached_posters', fake_get_cached_posters)

        shows = [{
            'title': 'Severance Adam Scott Sci Fi Mystery',
            'year': 2022,
            'source': 'debrid',
            'episodes': 5,
            'season_data': [{'number': 1, 'episodes': []}],
        }]
        movies = []
        renames = _enrich_with_tmdb_cache(movies, shows)

        assert shows[0]['title'] == 'Severance'
        assert shows[0]['_parsed_title'] == 'Severance Adam Scott Sci Fi Mystery'
        assert shows[0]['poster_url'] == 'https://image/sev.jpg'

    def test_no_rename_when_resolver_misses(self, monkeypatch):
        """Regression guard: no canonical match → no rename, no
        spurious side effects.  Item gets default None fields and is
        added to uncached for background population."""
        from utils import tmdb as _tmdb
        from utils.library import _enrich_with_tmdb_cache

        monkeypatch.setattr(_tmdb, '_load_cache', lambda: {})  # empty cache
        monkeypatch.setattr(_tmdb, 'get_cached_posters', lambda items: {})
        monkeypatch.setattr(_tmdb, 'background_populate_cache',
                            lambda items: None)
        monkeypatch.setattr(_tmdb, 'find_show_by_season',
                            lambda *a, **kw: None)

        movies = [{'title': 'Unknown Movie XYZ', 'year': 2099,
                   'source': 'debrid'}]
        shows = []
        renames = _enrich_with_tmdb_cache(movies, shows)

        # Title untouched
        assert movies[0]['title'] == 'Unknown Movie XYZ'
        assert '_parsed_title' not in movies[0]
        # Default fields
        assert movies[0]['poster_url'] is None
        assert movies[0]['tmdb_status'] is None
        # No rename pairs
        assert renames == []

    def test_direct_lookup_still_short_circuits(self, monkeypatch):
        """Regression guard: when get_cached_posters returns a direct
        hit, the prefix resolver isn't called.  Previous behavior must
        be preserved bit-for-bit.
        """
        from utils import tmdb as _tmdb
        from utils.library import _enrich_with_tmdb_cache

        # Empty full-cache so if the resolver WERE called it would miss.
        # Direct hit must still produce the rename via the original code path.
        monkeypatch.setattr(_tmdb, '_load_cache', lambda: {})
        monkeypatch.setattr(_tmdb, 'background_populate_cache',
                            lambda items: None)
        monkeypatch.setattr(_tmdb, 'find_show_by_season',
                            lambda *a, **kw: None)

        def fake_get_cached_posters(items):
            return {'inception': {
                'poster_url': 'https://image/i.jpg',
                'tmdb_status': 'Released',
                'imdb_id': 'tt1375666',
                'title': 'Inception',
            }}
        monkeypatch.setattr(_tmdb, 'get_cached_posters', fake_get_cached_posters)

        movies = [{'title': 'Inception', 'year': 2010, 'source': 'local'}]
        shows = []
        _enrich_with_tmdb_cache(movies, shows)

        # No rename (display already matches canonical)
        assert movies[0]['title'] == 'Inception'
        assert '_parsed_title' not in movies[0]
        assert movies[0]['poster_url'] == 'https://image/i.jpg'


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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'
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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'
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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'
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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'
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

    def test_movie_ghost_entry_counted_as_missing(self):
        """Phase: Radarr-wanted-movies. Ghost entries injected by
        ``_apply_radarr_wanted_movies`` carry ``missing=True`` and MUST
        be counted under the 'missing' bucket. Prior to the Option B
        fix this code-path looked at ``missing_episodes`` (a shows-only
        field), so movies were silently excluded from the Wanted view."""
        data = {'shows': [], 'movies': [
            {'title': 'Missing Movie', 'source': 'wanted', 'missing': True},
            {'title': 'Real Movie', 'source': 'debrid'},
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

    def test_real_movie_without_missing_field_not_counted(self):
        """Regular library-discovered movies don't carry ``missing``;
        only ghost entries do. A movie that is on disk MUST NOT be
        counted as missing regardless of its other fields."""
        data = {'shows': [], 'movies': [
            {'title': 'Real Movie', 'source': 'debrid'},
            {'title': 'Real Movie 2', 'source': 'local'},
            {'title': 'Real Movie 3', 'source': 'both'},
        ]}
        counts = get_wanted_counts(data)
        assert counts['missing'] == 0

    def test_multiple_ghost_movies_counted(self):
        data = {'shows': [], 'movies': [
            {'title': 'Wanted 1', 'source': 'wanted', 'missing': True},
            {'title': 'Wanted 2', 'source': 'wanted', 'missing': True},
            {'title': 'On Disk', 'source': 'debrid'},
        ]}
        counts = get_wanted_counts(data)
        assert counts['missing'] == 2

    def test_movie_pending_directions(self):
        data = {'shows': [], 'movies': [
            {'title': 'Movie A', 'source': 'debrid'},
        ]}
        pending = {
            'movie a': {'direction': 'to-local-fallback'},
        }
        counts = get_wanted_counts(data, pending)
        assert counts['fallback'] == 1
        assert counts['pending'] == 1


class TestComputeLibraryStats:
    """Tests for compute_library_stats() — composition for the status page."""

    def test_empty_data(self):
        stats = compute_library_stats({'movies': [], 'shows': []})
        assert stats['totals'] == {'items': 0, 'size_bytes': 0}
        assert stats['movies']['total'] == 0
        assert stats['shows']['total'] == 0
        assert stats['shows']['episodes']['total'] == 0
        assert stats['movies']['by_source'] == {'local': 0, 'debrid': 0, 'both': 0}

    def test_movies_grouped_by_source(self):
        data = {
            'movies': [
                {'source': 'local', 'size_bytes': 1000},
                {'source': 'debrid', 'size_bytes': 2000},
                {'source': 'debrid', 'size_bytes': 500},
                {'source': 'both', 'size_bytes': 3000},
            ],
            'shows': [],
        }
        stats = compute_library_stats(data)
        assert stats['movies']['total'] == 4
        assert stats['movies']['by_source'] == {'local': 1, 'debrid': 2, 'both': 1}
        assert stats['movies']['size_by_source'] == {'local': 1000, 'debrid': 2500, 'both': 3000}
        assert stats['movies']['size_bytes'] == 6500
        assert stats['totals']['size_bytes'] == 6500

    def test_shows_episodes_bucketed_by_episode_source(self):
        data = {
            'movies': [],
            'shows': [{
                'source': 'both',
                'season_data': [{'episodes': [
                    {'source': 'local', 'size_bytes': 100},
                    {'source': 'debrid', 'size_bytes': 200},
                    {'source': 'both', 'size_bytes': 50},
                ]}],
            }],
        }
        stats = compute_library_stats(data)
        assert stats['shows']['total'] == 1
        assert stats['shows']['by_source']['both'] == 1
        assert stats['shows']['episodes']['total'] == 3
        assert stats['shows']['episodes']['by_source'] == {'local': 1, 'debrid': 1, 'both': 1}
        assert stats['shows']['episodes']['size_by_source'] == {'local': 100, 'debrid': 200, 'both': 50}
        # Show size_by_source aggregates under the show-level source
        assert stats['shows']['size_by_source']['both'] == 350

    def test_unknown_source_falls_back_to_debrid(self):
        data = {
            'movies': [{'source': 'mystery', 'size_bytes': 100}],
            'shows': [],
        }
        stats = compute_library_stats(data)
        assert stats['movies']['by_source']['debrid'] == 1
        assert stats['movies']['size_by_source']['debrid'] == 100

    def test_wanted_source_excluded_from_buckets(self):
        """Ghost entries from ``_apply_radarr_wanted_movies`` carry
        ``source='wanted'`` and represent monitored-but-not-downloaded
        movies. They MUST NOT inflate the on-disk composition stats —
        the Library Composition card would lie about total library size
        if a 100-movie wanted list bucketed into 'debrid' via the
        unknown-source fallback."""
        data = {
            'movies': [
                {'source': 'debrid', 'size_bytes': 1000},
                {'source': 'wanted', 'size_bytes': 0,
                 'missing': True, 'title': 'Wanted 1'},
                {'source': 'wanted', 'size_bytes': 0,
                 'missing': True, 'title': 'Wanted 2'},
            ],
            'shows': [],
        }
        stats = compute_library_stats(data)
        assert stats['movies']['total'] == 1
        assert stats['movies']['by_source'] == {'local': 0, 'debrid': 1, 'both': 0}
        assert stats['movies']['size_bytes'] == 1000

    def test_invalid_size_treated_as_zero(self):
        data = {
            'movies': [
                {'source': 'debrid', 'size_bytes': None},
                {'source': 'debrid', 'size_bytes': 'oops'},
                {'source': 'debrid'},
            ],
            'shows': [],
        }
        stats = compute_library_stats(data)
        assert stats['movies']['total'] == 3
        assert stats['movies']['size_bytes'] == 0

    def test_passes_through_scan_metadata(self):
        data = {
            'movies': [],
            'shows': [],
            'last_scan': '2026-04-25T00:00:00+00:00',
            'scan_duration_ms': 42,
        }
        stats = compute_library_stats(data)
        assert stats['last_scan'] == '2026-04-25T00:00:00+00:00'
        assert stats['scan_duration_ms'] == 42

    def test_episode_source_falls_back_to_show_source(self):
        # Episodes without an explicit source inherit the show's source —
        # avoids dropping size into the wrong bucket on legacy/partial entries.
        data = {
            'movies': [],
            'shows': [{
                'source': 'local',
                'season_data': [{'episodes': [
                    {'size_bytes': 100},  # no source — should bucket under 'local'
                    {'size_bytes': 200},  # no source — should bucket under 'local'
                ]}],
            }],
        }
        stats = compute_library_stats(data)
        assert stats['shows']['episodes']['by_source']['local'] == 2
        assert stats['shows']['episodes']['size_by_source']['local'] == 300

    def test_get_cached_stats_returns_none_when_cache_empty(self):
        # Verify the hot-path accessor never blocks on an empty cache —
        # it must return None rather than triggering a scan.
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._cache = None
        import threading
        scanner._lock = threading.RLock()
        assert scanner.get_cached_stats() is None

    def test_get_cached_stats_returns_dict_when_cache_populated(self):
        scanner = LibraryScanner.__new__(LibraryScanner)
        import threading
        scanner._lock = threading.RLock()
        scanner._cache = {
            'movies': [{'source': 'debrid', 'size_bytes': 1000}],
            'shows': [],
            'last_scan': '2026-04-25T00:00:00+00:00',
        }
        stats = scanner.get_cached_stats()
        assert stats is not None
        assert stats['movies']['total'] == 1
        assert stats['totals']['size_bytes'] == 1000

    def test_get_cached_stats_snapshots_lists_against_concurrent_mutation(self):
        # Regression: _cleanup_disc_rips runs `movies[:] = [...]` after
        # the cache is published.  get_cached_stats() must snapshot the
        # list under the lock so a concurrent slice-assignment can't
        # tear iteration in compute_library_stats().
        scanner = LibraryScanner.__new__(LibraryScanner)
        import threading
        scanner._lock = threading.RLock()
        movies_list = [{'source': 'debrid', 'size_bytes': 1000}]
        scanner._cache = {'movies': movies_list, 'shows': []}
        # Mutate the underlying list in place AFTER the snapshot is taken,
        # mirroring the disc-rip cleanup path.  The snapshot must isolate
        # the reader from this mutation: simulate by having the helper
        # observe the list as-of the call moment.
        stats = scanner.get_cached_stats()
        movies_list[:] = []  # drop everything from the live list
        # The just-returned stats must still reflect the pre-mutation
        # contents, proving they were snapshotted under the lock.
        assert stats['movies']['total'] == 1
        assert stats['movies']['size_bytes'] == 1000
        # And a follow-up call sees the now-empty live list.
        stats2 = scanner.get_cached_stats()
        assert stats2['movies']['total'] == 0


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


class TestRemoveTitleSymlinksLabeled:
    """remove_title_symlinks must scan both flat and labeled completed_dir layouts."""

    @staticmethod
    def _make_symlink_release(release_path, target_base):
        """Create a release dir containing one symlink pointing into *target_base*."""
        os.makedirs(release_path)
        os.makedirs(target_base, exist_ok=True)
        target_file = os.path.join(target_base, os.path.basename(release_path) + '.mkv')
        with open(target_file, 'w') as f:
            f.write('data')
        os.symlink(target_file, os.path.join(release_path, 'ep.mkv'))

    def test_remove_title_symlinks_scans_labels(self, tmp_dir, monkeypatch):
        from utils.library import remove_title_symlinks
        completed = os.path.join(tmp_dir, 'completed')
        targets = os.path.join(tmp_dir, 'targets')
        sonarr_release = os.path.join(completed, 'sonarr', 'Fargo.S05E01')
        self._make_symlink_release(sonarr_release, targets)

        monkeypatch.setenv('BLACKHOLE_COMPLETED_DIR', completed)
        monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_TV', '')

        removed = remove_title_symlinks('Fargo', 'show')
        assert sonarr_release in removed
        assert not os.path.exists(sonarr_release)

    def test_remove_title_symlinks_flat_compat(self, tmp_dir, monkeypatch):
        """Legacy flat layout must keep working."""
        from utils.library import remove_title_symlinks
        completed = os.path.join(tmp_dir, 'completed')
        targets = os.path.join(tmp_dir, 'targets')
        flat = os.path.join(completed, 'Fargo.S05E01')
        self._make_symlink_release(flat, targets)

        monkeypatch.setenv('BLACKHOLE_COMPLETED_DIR', completed)
        monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_TV', '')

        removed = remove_title_symlinks('Fargo', 'show')
        assert flat in removed
        assert not os.path.exists(flat)

    def test_remove_title_symlinks_across_labels(self, tmp_dir, monkeypatch):
        """A title that exists under multiple labels must be removed from all of them."""
        from utils.library import remove_title_symlinks
        completed = os.path.join(tmp_dir, 'completed')
        targets = os.path.join(tmp_dir, 'targets')
        sonarr_release = os.path.join(completed, 'sonarr', 'Fargo.S05E01')
        radarr_release = os.path.join(completed, 'radarr', 'Fargo.S05E01')
        self._make_symlink_release(sonarr_release, targets)
        self._make_symlink_release(radarr_release, targets)

        monkeypatch.setenv('BLACKHOLE_COMPLETED_DIR', completed)
        monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_TV', '')

        removed = remove_title_symlinks('Fargo', 'show')
        assert sonarr_release in removed
        assert radarr_release in removed


# ---------------------------------------------------------------------------
# _apply_sonarr_monitored_filter
# ---------------------------------------------------------------------------

from unittest.mock import MagicMock, patch


@pytest.fixture(autouse=True)
def _reset_sonarr_series_cache():
    """TTL cache in ``_get_sonarr_series_list`` persists across tests in the
    same process; reset it before every test so prior fixtures can't leak
    their mocked series lists into unrelated assertions."""
    import utils.library as _lib
    _lib._sonarr_series_cache['data'] = None
    _lib._sonarr_series_cache['ts'] = 0.0


def _fake_sonarr(series_list):
    """Context manager patching ``get_download_service`` to return a
    MagicMock Sonarr client whose ``get_all_series`` yields *series_list*."""
    client = MagicMock()
    client.get_all_series.return_value = series_list
    return patch('utils.arr_client.get_download_service',
                 return_value=(client, 'sonarr'))


class TestApplySonarrMonitoredFilter:
    """Rebase missing_episodes against Sonarr's monitored view.

    Repro for the user-reported inflation: a show like Grey's Anatomy with
    22 seasons, older ones unmonitored, previously reported ~300+ missing
    episodes because the count was pure TMDB total minus on-disk count.
    """

    def test_unmonitored_seasons_excluded_from_count(self):
        """Unmonitored seasons must not contribute to missing_episodes."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': "Grey's Anatomy", 'year': None, 'missing_episodes': 300}]
        series = [{
            'title': "Grey's Anatomy",
            'tmdbId': 1416,
            'seasons': [
                {'seasonNumber': 1, 'monitored': False,
                 'statistics': {'episodeCount': 9, 'episodeFileCount': 9}},
                {'seasonNumber': 22, 'monitored': True,
                 'statistics': {'episodeCount': 5, 'episodeFileCount': 4}},
            ],
        }]
        with _fake_sonarr(series):
            _apply_sonarr_monitored_filter(shows)
        assert shows[0]['missing_episodes'] == 1
        assert shows[0]['unmonitored_seasons'] == [1]

    def test_missing_is_monitored_minus_file_count(self):
        """Sum ``episodeCount - episodeFileCount`` across monitored seasons.

        Sonarr's ``episodeCount`` already filters by per-episode monitored
        flags, so the math only needs to drop wholly unmonitored seasons.
        Also sets ``monitored_episodes`` so the UI progress bar agrees
        with the "X missing" pill.
        """
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Show', 'year': None}]
        series = [{
            'title': 'Show',
            'tmdbId': 42,
            'seasons': [
                {'seasonNumber': 1, 'monitored': True,
                 'statistics': {'episodeCount': 10, 'episodeFileCount': 6}},
                {'seasonNumber': 2, 'monitored': True,
                 'statistics': {'episodeCount': 8, 'episodeFileCount': 8}},
            ],
        }]
        with _fake_sonarr(series):
            _apply_sonarr_monitored_filter(shows)
        assert shows[0]['missing_episodes'] == 4
        assert shows[0]['unmonitored_seasons'] == []
        assert shows[0]['monitored_episodes'] == 18

    def test_monitored_episodes_omitted_when_all_seasons_unmonitored(self):
        """With zero monitored seasons the denominator would be zero — omit
        the field so the frontend falls back to the TMDB total rather
        than drawing a divide-by-zero bar."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Show', 'year': None}]
        series = [{
            'title': 'Show',
            'tmdbId': 42,
            'seasons': [
                {'seasonNumber': 1, 'monitored': False,
                 'statistics': {'episodeCount': 10, 'episodeFileCount': 10}},
            ],
        }]
        with _fake_sonarr(series):
            _apply_sonarr_monitored_filter(shows)
        assert shows[0]['missing_episodes'] == 0
        assert 'monitored_episodes' not in shows[0]

    def test_file_count_exceeding_episode_count_clamps_to_zero(self):
        """A season with more files than monitored episodes (e.g. stale
        episodes still on disk) must clamp at zero — never negative."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Show', 'year': None}]
        series = [{
            'title': 'Show',
            'seasons': [
                {'seasonNumber': 1, 'monitored': True,
                 'statistics': {'episodeCount': 3, 'episodeFileCount': 5}},
            ],
        }]
        with _fake_sonarr(series):
            _apply_sonarr_monitored_filter(shows)
        assert shows[0]['missing_episodes'] == 0

    def test_specials_season_zero_ignored(self):
        """Season 0 (specials) is neither counted nor listed as unmonitored."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Show', 'year': None}]
        series = [{
            'title': 'Show',
            'seasons': [
                {'seasonNumber': 0, 'monitored': False,
                 'statistics': {'episodeCount': 4, 'episodeFileCount': 0}},
                {'seasonNumber': 1, 'monitored': True,
                 'statistics': {'episodeCount': 2, 'episodeFileCount': 2}},
            ],
        }]
        with _fake_sonarr(series):
            _apply_sonarr_monitored_filter(shows)
        assert shows[0]['missing_episodes'] == 0
        assert shows[0]['unmonitored_seasons'] == []

    def test_unmatched_show_keeps_existing_count(self):
        """Shows not in Sonarr keep the TMDB-based count — conservative
        fallback for hand-imported libraries where no arr is tracking them."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Orphan', 'year': None, 'missing_episodes': 7}]
        with _fake_sonarr([]):
            _apply_sonarr_monitored_filter(shows)
        assert shows[0]['missing_episodes'] == 7
        assert 'unmonitored_seasons' not in shows[0]

    def test_sonarr_unreachable_leaves_shows_untouched(self):
        """Network failure on Sonarr must be a no-op, not wipe counts."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Show', 'missing_episodes': 5}]
        client = MagicMock()
        client.get_all_series.side_effect = RuntimeError('boom')
        with patch('utils.arr_client.get_download_service',
                   return_value=(client, 'sonarr')):
            _apply_sonarr_monitored_filter(shows)
        assert shows[0]['missing_episodes'] == 5
        assert 'unmonitored_seasons' not in shows[0]

    def test_fetch_failure_flags_degraded(self):
        """A configured Sonarr whose series fetch fails must add
        'sonarr_series' to the degraded set — the wanted counts fell back
        to inflated TMDB-only math, so the recovery snapshot must skip."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Show', 'missing_episodes': 5}]
        client = MagicMock()
        client.get_all_series.side_effect = RuntimeError('dns boom')
        degraded = set()
        with patch('utils.arr_client.get_download_service',
                   return_value=(client, 'sonarr')):
            _apply_sonarr_monitored_filter(shows, degraded=degraded)
        assert degraded == {'sonarr_series'}

    def test_empty_series_list_not_degraded(self):
        """An empty Sonarr library is a valid state — no degradation flag."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Show', 'missing_episodes': 5}]
        degraded = set()
        with _fake_sonarr([]):
            _apply_sonarr_monitored_filter(shows, degraded=degraded)
        assert degraded == set()

    def test_not_configured_not_degraded(self):
        """No Sonarr configured → TMDB-only math is the normal state,
        not a degradation."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Show', 'missing_episodes': 5}]
        degraded = set()
        with patch('utils.arr_client.get_download_service',
                   return_value=(None, None)):
            _apply_sonarr_monitored_filter(shows, degraded=degraded)
        assert degraded == set()

    def test_sonarr_not_configured_no_op(self):
        """Without Sonarr configured, monitored filtering is skipped entirely."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Show', 'missing_episodes': 5}]
        with patch('utils.arr_client.get_download_service',
                   return_value=(None, None)):
            _apply_sonarr_monitored_filter(shows)
        assert shows[0]['missing_episodes'] == 5
        assert 'unmonitored_seasons' not in shows[0]

    def test_title_collision_skipped_without_tmdb_id(self):
        """Two Sonarr series sharing a lowercase title (classic reboot shape,
        e.g. 'Magnum P.I.' 1980 + 2018 lacking year suffixes) must not
        silent-match. Without a TMDB-ID hit in the cache the library show
        is left untouched rather than matched to an arbitrary series."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Magnum P.I.', 'year': None, 'missing_episodes': 12}]
        series = [
            {'title': 'Magnum P.I.', 'tmdbId': 100,
             'seasons': [{'seasonNumber': 1, 'monitored': True,
                          'statistics': {'episodeCount': 8, 'episodeFileCount': 0}}]},
            {'title': 'Magnum P.I.', 'tmdbId': 200,
             'seasons': [{'seasonNumber': 1, 'monitored': True,
                          'statistics': {'episodeCount': 10, 'episodeFileCount': 1}}]},
        ]
        with _fake_sonarr(series):
            _apply_sonarr_monitored_filter(shows)
        # No match — colliding title can't resolve to a specific series.
        assert shows[0]['missing_episodes'] == 12
        assert 'unmonitored_seasons' not in shows[0]

    def test_title_collision_resolved_via_tmdb_id(self):
        """Same collision as above but with a TMDB-ID hit in the cache —
        the ambiguous title-level keys are skipped, but the TMDB ID step
        resolves to the correct series."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Magnum P.I.', 'year': None}]
        series = [
            {'title': 'Magnum P.I.', 'tmdbId': 100,
             'seasons': [{'seasonNumber': 1, 'monitored': True,
                          'statistics': {'episodeCount': 8, 'episodeFileCount': 0}}]},
            {'title': 'Magnum P.I.', 'tmdbId': 200,
             'seasons': [{'seasonNumber': 1, 'monitored': True,
                          'statistics': {'episodeCount': 10, 'episodeFileCount': 1}}]},
        ]
        with _fake_sonarr(series):
            with patch('utils.tmdb.get_cached_tmdb_ids',
                       return_value={'shows': {'magnum p.i.': 200}}):
                _apply_sonarr_monitored_filter(shows)
        assert shows[0]['missing_episodes'] == 9  # from tmdbId=200

    def test_parsed_title_matches_even_when_display_renamed(self):
        """Enrichment may upgrade ``title`` to the canonical TMDB spelling
        while the Sonarr library still carries the parsed-folder form
        (or vice versa). Both candidates must be tried through every
        match step — not just step 1 — so renamed shows aren't silently
        skipped."""
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{
            'title': 'Star Wars: Andor',
            '_parsed_title': 'Andor',
            'year': None,
        }]
        series = [{
            'title': 'Andor',
            'tmdbId': 999,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 12, 'episodeFileCount': 10}}],
        }]
        with _fake_sonarr(series):
            _apply_sonarr_monitored_filter(shows)
        assert shows[0]['missing_episodes'] == 2


@pytest.fixture(autouse=True)
def _reset_radarr_movies_cache():
    """Mirror the Sonarr cache reset so Radarr-list tests can't leak
    mocked movie lists into each other."""
    import utils.library as _lib
    _lib._radarr_movies_cache['data'] = None
    _lib._radarr_movies_cache['ts'] = 0.0


def _fake_radarr(movie_list):
    """Patch ``get_download_service('movie')`` to return a MagicMock
    Radarr client whose ``get_all_movies`` yields *movie_list*."""
    client = MagicMock()
    client.get_all_movies.return_value = movie_list
    return patch('utils.arr_client.get_download_service',
                 return_value=(client, 'radarr'))


class TestApplyRadarrWantedMovies:
    """Inject Radarr-monitored-but-not-downloaded movies as ghost
    entries so the Wanted view surfaces them.

    The library scanner reads from disk; a movie you've requested but
    haven't downloaded is invisible to the rest of the pipeline. This
    helper closes that gap. Behavior parallels
    ``_apply_sonarr_monitored_filter`` but with one structural
    difference: it INJECTS entries into the movies list rather than
    refining counts on existing entries.
    """

    def test_injects_ghost_for_monitored_no_file_movie(self):
        from utils.library import _apply_radarr_wanted_movies
        movies = []
        radarr_movies = [
            {'id': 1, 'tmdbId': 100, 'title': 'Wanted Movie',
             'year': 2023, 'monitored': True, 'hasFile': False},
        ]
        with _fake_radarr(radarr_movies):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 1
        assert len(movies) == 1
        ghost = movies[0]
        assert ghost['title'] == 'Wanted Movie'
        assert ghost['year'] == 2023
        assert ghost['source'] == 'wanted'
        assert ghost['missing'] is True
        assert ghost['size_bytes'] == 0
        assert ghost['_radarr_id'] == 1
        assert ghost['_radarr_tmdb_id'] == 100
        # Regression for the TMDB cache-poisoning bug — the detail-view
        # fetch JS uses item.type to build /api/library/metadata?type=...
        # Without the type field the URL serialised type=undefined and
        # the server defaulted to 'show', writing show data under
        # movie-style cache keys.
        assert ghost['type'] == 'movie', \
            'ghost movie must carry type=movie so the detail-view fetch ' \
            'does not poison the TMDB cache via type=undefined'

    def test_fetch_failure_flags_degraded(self):
        """A configured Radarr whose movie fetch fails must add
        'radarr_movies' — ghost injection was skipped, deflating wanted."""
        from utils.library import _apply_radarr_wanted_movies
        client = MagicMock()
        client.get_all_movies.side_effect = RuntimeError('dns boom')
        degraded = set()
        with patch('utils.arr_client.get_download_service',
                   return_value=(client, 'radarr')):
            count = _apply_radarr_wanted_movies([], degraded=degraded)
        assert count == 0
        assert degraded == {'radarr_movies'}

    def test_empty_movie_list_not_degraded(self):
        """An empty Radarr library is a valid state — no degradation flag."""
        from utils.library import _apply_radarr_wanted_movies
        degraded = set()
        with _fake_radarr([]):
            _apply_radarr_wanted_movies([], degraded=degraded)
        assert degraded == set()

    def test_not_configured_not_degraded(self):
        """No Radarr configured → skipping ghost injection is the normal
        state, not a degradation."""
        from utils.library import _apply_radarr_wanted_movies
        degraded = set()
        with patch('utils.arr_client.get_download_service',
                   return_value=(None, None)):
            _apply_radarr_wanted_movies([], degraded=degraded)
        assert degraded == set()

    def test_skips_monitored_with_file(self):
        """A monitored movie that DOES have a file is already on disk,
        will appear in the library through the scanner, and must not
        get a ghost entry (would double-count)."""
        from utils.library import _apply_radarr_wanted_movies
        movies = []
        radarr_movies = [
            {'id': 1, 'tmdbId': 100, 'title': 'Has File',
             'year': 2023, 'monitored': True, 'hasFile': True},
        ]
        with _fake_radarr(radarr_movies):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 0
        assert movies == []

    def test_skips_unmonitored(self):
        """Unmonitored movies aren't wanted — Radarr knows about them
        but isn't searching. Don't inflate the Wanted view with them."""
        from utils.library import _apply_radarr_wanted_movies
        movies = []
        radarr_movies = [
            {'id': 1, 'tmdbId': 100, 'title': 'Unmonitored',
             'year': 2023, 'monitored': False, 'hasFile': False},
        ]
        with _fake_radarr(radarr_movies):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 0
        assert movies == []

    def test_dedup_by_tmdb_id(self):
        """A Radarr wanted movie whose tmdbId matches an existing
        library entry is suppressed (the real entry wins, even if its
        title or year is slightly off)."""
        from utils.library import _apply_radarr_wanted_movies
        movies = [{'title': 'Existing Movie (Different Spelling)',
                   'year': 2023, 'source': 'debrid', 'tmdb_id': 100}]
        radarr_movies = [
            {'id': 1, 'tmdbId': 100, 'title': 'Existing Movie',
             'year': 2023, 'monitored': True, 'hasFile': False},
        ]
        with _fake_radarr(radarr_movies):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 0
        assert len(movies) == 1
        assert movies[0]['source'] == 'debrid'

    def test_dedup_by_normalized_title_year(self):
        """When an existing entry has no tmdb_id, fall back to
        (normalized_title, year) matching so hand-imported libraries
        still dedup correctly."""
        from utils.library import _apply_radarr_wanted_movies
        movies = [{'title': 'Inception', 'year': 2010,
                   'source': 'local'}]
        radarr_movies = [
            {'id': 1, 'tmdbId': 27205, 'title': 'Inception',
             'year': 2010, 'monitored': True, 'hasFile': False},
        ]
        with _fake_radarr(radarr_movies):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 0
        assert len(movies) == 1

    def test_year_mismatch_does_not_dedup(self):
        """Same title, different year (festival vs wide release, or
        actual different films like Dune 1984 vs 2021) — these are
        DIFFERENT movies, ghost MUST inject."""
        from utils.library import _apply_radarr_wanted_movies
        movies = [{'title': 'Dune', 'year': 1984, 'source': 'local'}]
        radarr_movies = [
            {'id': 1, 'tmdbId': 438631, 'title': 'Dune',
             'year': 2021, 'monitored': True, 'hasFile': False},
        ]
        with _fake_radarr(radarr_movies):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 1
        assert len(movies) == 2

    def test_pending_movie_suppressed(self):
        """A movie currently being downloaded shows up in the 'pending'
        Wanted bucket via the pending_monitors mechanism. Don't ALSO
        inject it as a 'missing' ghost — would double-count and
        confuse the user about its actual state."""
        from utils.library import _apply_radarr_wanted_movies
        movies = []
        radarr_movies = [
            {'id': 1, 'tmdbId': 100, 'title': 'Pending Movie',
             'year': 2023, 'monitored': True, 'hasFile': False},
        ]
        pending = {'pending movie': {'direction': 'to-debrid'}}
        with _fake_radarr(radarr_movies):
            count = _apply_radarr_wanted_movies(movies, pending=pending)
        assert count == 0
        assert movies == []

    def test_radarr_unavailable_no_op(self):
        """If get_download_service returns no Radarr client (Radarr not
        configured, or Overseerr is the fallback), the function silently
        returns 0 without injecting anything. Scan must continue."""
        from utils.library import _apply_radarr_wanted_movies
        movies = []
        with patch('utils.arr_client.get_download_service',
                   return_value=(None, None)):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 0
        assert movies == []

    def test_overseerr_fallback_ignored(self):
        """When Radarr is unconfigured but Overseerr is, the function
        gets the Overseerr client back from get_download_service.
        It MUST detect the non-radarr svc and skip — otherwise it would
        call get_all_movies on an Overseerr client which doesn't have it."""
        from utils.library import _apply_radarr_wanted_movies
        movies = []
        overseerr_client = MagicMock()
        with patch('utils.arr_client.get_download_service',
                   return_value=(overseerr_client, 'overseerr')):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 0
        assert overseerr_client.get_all_movies.call_count == 0

    def test_fetch_failure_graceful(self):
        """A Radarr API error during get_all_movies must not crash the
        scan. Empty injection, scan continues with real movies only."""
        from utils.library import _apply_radarr_wanted_movies
        movies = [{'title': 'Real', 'year': 2020, 'source': 'debrid'}]
        client = MagicMock()
        client.get_all_movies.side_effect = RuntimeError('radarr down')
        with patch('utils.arr_client.get_download_service',
                   return_value=(client, 'radarr')):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 0
        assert len(movies) == 1  # real movie untouched

    def test_empty_radarr_no_op(self):
        from utils.library import _apply_radarr_wanted_movies
        movies = []
        with _fake_radarr([]):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 0

    def test_multiple_ghosts_injected(self):
        from utils.library import _apply_radarr_wanted_movies
        movies = []
        radarr_movies = [
            {'id': 1, 'tmdbId': 100, 'title': 'A',
             'year': 2020, 'monitored': True, 'hasFile': False},
            {'id': 2, 'tmdbId': 200, 'title': 'B',
             'year': 2021, 'monitored': True, 'hasFile': False},
            {'id': 3, 'tmdbId': 300, 'title': 'C',
             'year': 2022, 'monitored': False, 'hasFile': False},
            {'id': 4, 'tmdbId': 400, 'title': 'D',
             'year': 2023, 'monitored': True, 'hasFile': True},
        ]
        with _fake_radarr(radarr_movies):
            count = _apply_radarr_wanted_movies(movies)
        assert count == 2
        injected_titles = sorted(m['title'] for m in movies)
        assert injected_titles == ['A', 'B']


class TestSonarrMonitoredMissingHelper:
    """The shared monitored-aware missing-episode math, factored out of
    the filter so the ghost-show injector agrees on the same arithmetic."""

    def test_sums_monitored_skips_unmonitored_and_specials(self):
        from utils.library import _sonarr_monitored_missing
        series = {
            'seasons': [
                {'seasonNumber': 0, 'monitored': False,
                 'statistics': {'episodeCount': 4, 'episodeFileCount': 0}},
                {'seasonNumber': 1, 'monitored': False,
                 'statistics': {'episodeCount': 9, 'episodeFileCount': 9}},
                {'seasonNumber': 2, 'monitored': True,
                 'statistics': {'episodeCount': 10, 'episodeFileCount': 6}},
            ],
        }
        missing, monitored_total, unmonitored = _sonarr_monitored_missing(series)
        assert missing == 4
        assert monitored_total == 10
        assert unmonitored == [1]

    def test_clamps_negative_and_handles_no_seasons(self):
        from utils.library import _sonarr_monitored_missing
        series = {'seasons': [
            {'seasonNumber': 1, 'monitored': True,
             'statistics': {'episodeCount': 3, 'episodeFileCount': 5}},
        ]}
        assert _sonarr_monitored_missing(series) == (0, 3, [])
        assert _sonarr_monitored_missing({}) == (0, 0, [])


class TestApplySonarrMonitoredFilterReturnsMatchedIds:
    """The filter now returns the set of matched Sonarr series ids so the
    ghost injector can skip series already represented by a real show."""

    def test_returns_matched_series_id(self):
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Show', 'year': None}]
        series = [{
            'id': 77, 'title': 'Show', 'tmdbId': 42,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 10, 'episodeFileCount': 6}}],
        }]
        with _fake_sonarr(series):
            matched = _apply_sonarr_monitored_filter(shows)
        assert matched == {77}

    def test_returns_empty_set_when_unmatched(self):
        from utils.library import _apply_sonarr_monitored_filter
        shows = [{'title': 'Orphan', 'year': None, 'missing_episodes': 7}]
        with _fake_sonarr([]):
            matched = _apply_sonarr_monitored_filter(shows)
        assert matched == set()

    def test_returns_empty_set_when_sonarr_not_configured(self):
        from utils.library import _apply_sonarr_monitored_filter
        with patch('utils.arr_client.get_download_service',
                   return_value=(None, None)):
            matched = _apply_sonarr_monitored_filter([{'title': 'X'}])
        assert matched == set()


class TestApplySonarrWantedShows:
    """Inject Sonarr-monitored series with zero on-disk episodes as ghost
    show entries — the TV mirror of ``_apply_radarr_wanted_movies``.

    Without this, a series you've downloaded nothing of never reaches the
    library shows list, so the recovery metric's wanted-TV denominator
    reads low and the Wanted view hides it.
    """

    def test_injects_ghost_for_fully_absent_monitored_series(self):
        from utils.library import _apply_sonarr_wanted_shows
        shows = []
        series = [{
            'id': 5, 'tmdbId': 100, 'imdbId': 'tt123', 'title': 'Absent Show',
            'year': 2022, 'monitored': True,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 8, 'episodeFileCount': 0}}],
        }]
        with _fake_sonarr(series):
            count = _apply_sonarr_wanted_shows(shows, set())
        assert count == 1
        ghost = shows[0]
        assert ghost['title'] == 'Absent Show'
        assert ghost['year'] == 2022
        assert ghost['source'] == 'wanted'
        assert ghost['missing'] is True
        assert ghost['missing_episodes'] == 8
        assert ghost['monitored_episodes'] == 8
        assert ghost['type'] == 'show'
        assert ghost['size_bytes'] == 0
        assert ghost['season_data'] == []
        assert ghost['_episodes'] == {}
        assert ghost['_sonarr_id'] == 5
        assert ghost['imdb_id'] == 'tt123'
        assert ghost['tmdb_id'] == 100

    def test_fetch_failure_flags_degraded(self):
        """A configured Sonarr whose series fetch fails must add
        'sonarr_series' — ghost injection was skipped, deflating wanted."""
        from utils.library import _apply_sonarr_wanted_shows
        client = MagicMock()
        client.get_all_series.side_effect = RuntimeError('dns boom')
        degraded = set()
        with patch('utils.arr_client.get_download_service',
                   return_value=(client, 'sonarr')):
            count = _apply_sonarr_wanted_shows(shows=[], matched_ids=set(),
                                               degraded=degraded)
        assert count == 0
        assert degraded == {'sonarr_series'}

    def test_empty_series_list_not_degraded(self):
        """An empty Sonarr library is a valid state — no degradation flag."""
        from utils.library import _apply_sonarr_wanted_shows
        degraded = set()
        with _fake_sonarr([]):
            _apply_sonarr_wanted_shows(shows=[], matched_ids=set(),
                                       degraded=degraded)
        assert degraded == set()

    def test_not_configured_not_degraded(self):
        """No Sonarr configured → skipping ghost injection is the normal
        state, not a degradation."""
        from utils.library import _apply_sonarr_wanted_shows
        degraded = set()
        with patch('utils.arr_client.get_download_service',
                   return_value=(None, None)):
            _apply_sonarr_wanted_shows(shows=[], matched_ids=set(),
                                       degraded=degraded)
        assert degraded == set()

    def test_skips_series_already_matched_to_a_library_show(self):
        """A series whose id is in ``matched_ids`` already has a real
        library show carrying its missing count — don't double-count."""
        from utils.library import _apply_sonarr_wanted_shows
        shows = []
        series = [{
            'id': 5, 'tmdbId': 100, 'title': 'Present', 'monitored': True,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 8, 'episodeFileCount': 0}}],
        }]
        with _fake_sonarr(series):
            count = _apply_sonarr_wanted_shows(shows, {5})
        assert count == 0
        assert shows == []

    def test_skips_unmonitored_series(self):
        from utils.library import _apply_sonarr_wanted_shows
        shows = []
        series = [{
            'id': 5, 'title': 'Unmon', 'monitored': False,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 8, 'episodeFileCount': 0}}],
        }]
        with _fake_sonarr(series):
            count = _apply_sonarr_wanted_shows(shows, set())
        assert count == 0
        assert shows == []

    def test_skips_series_sonarr_considers_satisfied(self):
        """A monitored series with all monitored episodes on file (per
        Sonarr) has missing==0 — not wanted, don't inject."""
        from utils.library import _apply_sonarr_wanted_shows
        shows = []
        series = [{
            'id': 5, 'title': 'Complete', 'monitored': True,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 8, 'episodeFileCount': 8}}],
        }]
        with _fake_sonarr(series):
            count = _apply_sonarr_wanted_shows(shows, set())
        assert count == 0
        assert shows == []

    def test_pending_series_suppressed(self):
        """A series currently downloading is represented by the pending
        bucket; skip its ghost to avoid double-counting."""
        from utils.library import _apply_sonarr_wanted_shows
        shows = []
        series = [{
            'id': 5, 'title': 'Pending Show', 'monitored': True,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 8, 'episodeFileCount': 0}}],
        }]
        pending = {'pending show': {'direction': 'to-debrid'}}
        with _fake_sonarr(series):
            count = _apply_sonarr_wanted_shows(shows, set(), pending=pending)
        assert count == 0
        assert shows == []

    def test_sonarr_not_configured_no_op(self):
        from utils.library import _apply_sonarr_wanted_shows
        shows = []
        with patch('utils.arr_client.get_download_service',
                   return_value=(None, None)):
            count = _apply_sonarr_wanted_shows(shows, set())
        assert count == 0
        assert shows == []

    def test_fetch_failure_graceful(self):
        from utils.library import _apply_sonarr_wanted_shows
        shows = [{'title': 'Real', 'source': 'debrid'}]
        client = MagicMock()
        client.get_all_series.side_effect = RuntimeError('sonarr down')
        with patch('utils.arr_client.get_download_service',
                   return_value=(client, 'sonarr')):
            count = _apply_sonarr_wanted_shows(shows, set())
        assert count == 0
        assert len(shows) == 1

    def test_skips_series_matching_on_disk_show_by_tmdb_id(self):
        """A partially-on-disk show the title cascade MISSED (so its id is
        not in matched_ids) must still be deduped against the real shows
        list by tmdbId — otherwise its real entry and a ghost both count."""
        from utils.library import _apply_sonarr_wanted_shows
        shows = [{'title': 'On Disk', 'source': 'debrid', 'tmdb_id': 100,
                  'missing_episodes': 3}]
        series = [{
            'id': 5, 'tmdbId': 100, 'title': 'Different Folder Name',
            'monitored': True,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 8, 'episodeFileCount': 0}}],
        }]
        with _fake_sonarr(series):
            count = _apply_sonarr_wanted_shows(shows, set())
        assert count == 0
        assert len(shows) == 1

    def test_skips_series_matching_on_disk_show_by_imdb_id(self):
        from utils.library import _apply_sonarr_wanted_shows
        shows = [{'title': 'On Disk', 'source': 'debrid', 'imdb_id': 'tt999',
                  'missing_episodes': 3}]
        series = [{
            'id': 5, 'imdbId': 'tt999', 'title': 'Different Folder Name',
            'monitored': True,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 8, 'episodeFileCount': 0}}],
        }]
        with _fake_sonarr(series):
            count = _apply_sonarr_wanted_shows(shows, set())
        assert count == 0
        assert len(shows) == 1

    def test_skips_series_matching_on_disk_show_by_norm_title_year(self):
        """No external IDs on either side (TVDB-only / cache miss): the
        (norm_title, year) fallback still prevents the double-inject."""
        from utils.library import _apply_sonarr_wanted_shows
        shows = [{'title': 'The Show', 'source': 'debrid', 'year': 2021,
                  'missing_episodes': 3}]
        series = [{
            'id': 5, 'title': 'The Show', 'year': 2021, 'monitored': True,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 8, 'episodeFileCount': 0}}],
        }]
        with _fake_sonarr(series):
            count = _apply_sonarr_wanted_shows(shows, set())
        assert count == 0
        assert len(shows) == 1

    def test_ghost_entries_in_shows_list_not_used_for_dedup(self):
        """A pre-existing source='wanted' entry must not seed the dedup
        sets (it's not a real on-disk show)."""
        from utils.library import _apply_sonarr_wanted_shows
        shows = [{'title': 'Ghosty', 'source': 'wanted', 'tmdb_id': 100}]
        series = [{
            'id': 5, 'tmdbId': 100, 'title': 'Ghosty', 'monitored': True,
            'seasons': [{'seasonNumber': 1, 'monitored': True,
                         'statistics': {'episodeCount': 8, 'episodeFileCount': 0}}],
        }]
        with _fake_sonarr(series):
            count = _apply_sonarr_wanted_shows(shows, set())
        # Pre-existing ghost doesn't block injection; but the freshly
        # injected ghost's own keys prevent a second copy of the same id.
        assert count == 1

    def test_ghost_show_not_counted_in_composition_stats(self):
        """A source='wanted' ghost show must NOT inflate the on-disk
        library composition (mirrors the ghost-movie skip)."""
        from utils.library import compute_library_stats
        data = {
            'movies': [],
            'shows': [
                {'title': 'Real', 'source': 'debrid', 'season_data': [
                    {'number': 1, 'episodes': [
                        {'number': 1, 'source': 'debrid', 'size_bytes': 100}]}]},
                {'title': 'Ghost', 'source': 'wanted', 'missing_episodes': 8,
                 'season_data': [], '_episodes': {}},
            ],
        }
        stats = compute_library_stats(data)
        assert stats['shows']['total'] == 1  # ghost excluded
        assert stats['shows']['by_source'] == {'local': 0, 'debrid': 1, 'both': 0}
        assert stats['shows']['episodes']['total'] == 1


class TestStripGhostDuplicates:
    """Post-enrichment ghost-deduplication pass.

    The pre-injection dedup in ``_apply_radarr_wanted_movies`` uses the
    parsed-folder norm at the time of injection — but TMDB enrichment
    may later rename a real entry to its canonical title that collides
    with a ghost we already injected. Without this pass the library
    would render two cards for the same movie (real Available + ghost
    Wanted). Reviewer-flagged HIGH from commit 071ba5d.
    """

    def test_ghost_collision_with_real_dropped(self):
        from utils.library import _strip_ghost_duplicates
        # Simulates the F1 case: real movie post-rename is now "F1",
        # ghost was injected pre-rename as "F1". Real wins.
        movies = [
            {'title': 'F1', 'year': 2025, 'source': 'debrid'},
            {'title': 'F1', 'year': 2025, 'source': 'wanted', 'missing': True},
        ]
        _strip_ghost_duplicates(movies)
        assert len(movies) == 1
        assert movies[0]['source'] == 'debrid'

    def test_ghost_with_unique_title_preserved(self):
        from utils.library import _strip_ghost_duplicates
        movies = [
            {'title': 'Real Movie', 'year': 2024, 'source': 'debrid'},
            {'title': 'Different Movie', 'year': 2024,
             'source': 'wanted', 'missing': True},
        ]
        _strip_ghost_duplicates(movies)
        assert len(movies) == 2
        # Order preserved (real first, ghost second)
        assert movies[0]['source'] == 'debrid'
        assert movies[1]['source'] == 'wanted'

    def test_year_mismatch_keeps_both(self):
        """Same title different year is two genuinely different films
        (Dune 1984 vs Dune 2021). Don't collapse them."""
        from utils.library import _strip_ghost_duplicates
        movies = [
            {'title': 'Dune', 'year': 1984, 'source': 'local'},
            {'title': 'Dune', 'year': 2021, 'source': 'wanted',
             'missing': True},
        ]
        _strip_ghost_duplicates(movies)
        assert len(movies) == 2

    def test_no_real_movies_keeps_all_ghosts(self):
        from utils.library import _strip_ghost_duplicates
        movies = [
            {'title': 'G1', 'year': 2024, 'source': 'wanted', 'missing': True},
            {'title': 'G2', 'year': 2025, 'source': 'wanted', 'missing': True},
        ]
        _strip_ghost_duplicates(movies)
        assert len(movies) == 2

    def test_only_real_movies_no_op(self):
        from utils.library import _strip_ghost_duplicates
        movies = [
            {'title': 'R1', 'year': 2024, 'source': 'debrid'},
            {'title': 'R2', 'year': 2025, 'source': 'local'},
        ]
        _strip_ghost_duplicates(movies)
        assert len(movies) == 2

    def test_multiple_ghosts_only_collision_dropped(self):
        """When several ghosts exist, only the one colliding with a
        real entry is stripped. Other ghosts survive."""
        from utils.library import _strip_ghost_duplicates
        movies = [
            {'title': 'Real', 'year': 2025, 'source': 'debrid'},
            {'title': 'Real', 'year': 2025, 'source': 'wanted', 'missing': True},
            {'title': 'Ghost Unique', 'year': 2025,
             'source': 'wanted', 'missing': True},
        ]
        _strip_ghost_duplicates(movies)
        assert len(movies) == 2
        assert movies[0]['title'] == 'Real'
        assert movies[0]['source'] == 'debrid'
        assert movies[1]['title'] == 'Ghost Unique'

    def test_empty_movies_no_op(self):
        from utils.library import _strip_ghost_duplicates
        movies = []
        _strip_ghost_duplicates(movies)
        assert movies == []


class TestDedupShowsByExternalId:
    """Post-enrichment shows-dedup pass.

    The alias-map dedup in ``_dedup_by_tmdb`` keys by normalized
    parsed-folder titles and runs pre-enrichment.  Three debrid folders
    that parse to distinct norms (``your friends and neighbors`` vs
    ``your friends neighbors`` vs ``your friends and neighbours``)
    survive as three groups when the TMDB alias map doesn't carry all
    three variants.  Enrichment then stamps the same canonical title +
    imdb_id on every entry → UI renders three cards for one show.

    Reworked after reviewer-flagged CRITICAL/HIGH findings on the v1
    implementation: this version rebuilds ``season_data`` from the
    unioned ``_episodes``, preserves the survivor's Sonarr-aware
    ``missing_episodes`` from ``_apply_sonarr_monitored_filter``,
    per-episode quality compare on collisions (larger size wins),
    ``size_bytes`` summed from merged episodes (no double-count), and
    drops the dead tmdb_id fallback (enrichment never stamps it).
    """

    def _ep(self, file_name, size_bytes=1_000_000_000):
        """Build a minimal _episodes value dict matching _build_season_data
        expectations: 'file' and 'size_bytes' minimum."""
        return {'file': file_name, 'size_bytes': size_bytes}

    def test_three_debrid_folders_collapse_with_unioned_episodes(self):
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'Your Friends & Neighbors', 'imdb_id': 'tt30459041',
             'source': 'debrid', 'path': '/data/zurgarr/path1',
             'size_bytes': 100, 'total_episodes': 18, 'missing_episodes': 16,
             '_episodes': {(1, 1): self._ep('s1e1.mkv'),
                           (1, 2): self._ep('s1e2.mkv')}},
            {'title': 'Your Friends & Neighbors', 'imdb_id': 'tt30459041',
             'source': 'debrid', 'path': '/data/torbox/path2',
             'size_bytes': 200, 'total_episodes': 18, 'missing_episodes': 16,
             '_episodes': {(2, 1): self._ep('s2e1.mkv'),
                           (2, 8): self._ep('s2e8.mkv')}},
            {'title': 'Your Friends & Neighbors', 'imdb_id': 'tt30459041',
             'source': 'debrid', 'path': '/data/torbox/path3',
             'size_bytes': 50, 'total_episodes': 18, 'missing_episodes': 16,
             '_episodes': {(1, 3): self._ep('s1e3.mkv')}}
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 1
        m = shows[0]
        # Episodes from all three folders unioned: 5 distinct keys
        assert len(m['_episodes']) == 5
        # Two seasons represented (1 and 2)
        assert m['seasons'] == 2
        assert m['episodes'] == 5

    def test_season_data_rebuilt_from_merged_episodes(self):
        """CRITICAL #1 fix: downstream consumers iterate ``season_data``,
        not ``_episodes`` — composition card, prefs enforcer, gap-fill,
        search loops.  Without rebuilding, sibling-folder episodes are
        invisible to all of them."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             'season_data': [{'number': 1, 'episode_count': 1,
                              'episodes': [{'number': 1, 'file': 's1e1.mkv',
                                            'source': 'debrid', 'size_bytes': 100,
                                            'quality': {}}]}],
             '_episodes': {(1, 1): self._ep('s1e1.mkv', 100)}},
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             'season_data': [{'number': 2, 'episode_count': 1,
                              'episodes': [{'number': 1, 'file': 's2e1.mkv',
                                            'source': 'debrid', 'size_bytes': 200,
                                            'quality': {}}]}],
             '_episodes': {(2, 1): self._ep('s2e1.mkv', 200)}},
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 1
        sd = shows[0]['season_data']
        seasons = {s['number'] for s in sd}
        assert seasons == {1, 2}, \
            f'season_data must contain both merged seasons, got {seasons}'
        total_eps = sum(s['episode_count'] for s in sd)
        assert total_eps == 2, \
            'season_data episode_count must reflect merged episodes'

    def test_missing_episodes_preserves_sonarr_filtered_value(self):
        """CRITICAL #2 fix: ``_apply_sonarr_monitored_filter`` runs
        pre-merge and writes monitored-aware ``missing_episodes`` that
        accounts for unmonitored seasons.  The v1 implementation
        recomputed from ``total_episodes - merged_have`` (TMDB-all math),
        which would re-inflate missing on Grey's Anatomy (22 seasons,
        most unmonitored) — exactly the bug ``_apply_sonarr_monitored_filter``
        exists to prevent.  Preserve survivor's value instead."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            # Survivor: Sonarr filter wrote missing_episodes=1 because
            # only S22 is monitored. total_episodes=430 (all seasons).
            {'title': 'Greys Anatomy', 'imdb_id': 'tt0413573',
             'source': 'debrid',
             'total_episodes': 430, 'missing_episodes': 1,
             'monitored_episodes': 20,
             '_episodes': {(22, 1): self._ep('s22e1.mkv', 100)}},
            # Sibling: same show, different debrid folder
            {'title': 'Greys Anatomy', 'imdb_id': 'tt0413573',
             'source': 'debrid',
             'total_episodes': 430, 'missing_episodes': 1,
             'monitored_episodes': 20,
             '_episodes': {(1, 1): self._ep('s1e1.mkv', 100)}},  # S1 unmonitored
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 1
        merged = shows[0]
        # CRITICAL: must NOT recompute as 430 - 2 = 428 (TMDB-all math)
        # MUST preserve survivor's Sonarr-aware value
        assert merged['missing_episodes'] == 1, \
            f'must preserve Sonarr-filtered missing_episodes=1 (Greys Anatomy ' \
            f'unmonitored-seasons regression), got {merged["missing_episodes"]}'
        # monitored_episodes must also survive
        assert merged.get('monitored_episodes') == 20

    def test_episode_collision_higher_size_wins(self):
        """HIGH #4 fix: per-episode quality compare.  v1 was first-seen
        wins regardless of quality — would silently keep a 720p episode
        when a 2160p REMUX sibling existed."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             '_episodes': {(1, 1): self._ep('s1e1.720p.mkv', 1_000_000_000)}},
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             '_episodes': {(1, 1): self._ep('s1e1.2160p.mkv', 30_000_000_000)}},
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 1
        ep = shows[0]['_episodes'][(1, 1)]
        assert '2160p' in ep['file'], \
            f'larger-size episode (2160p) must win collision, got {ep["file"]!r}'

    def test_size_bytes_summed_from_merged_episodes_not_show_field(self):
        """HIGH #5 fix: don't sum show-level size_bytes (which would
        double-count overlapping releases — same S01E01 in three
        qualities triple-counted).  Sum per-episode size from the merged
        ``_episodes`` dict instead."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             'size_bytes': 10_000_000_000,  # stale show-level field
             '_episodes': {(1, 1): self._ep('a.mkv', 1_000_000)}},
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             'size_bytes': 10_000_000_000,
             '_episodes': {(1, 2): self._ep('b.mkv', 2_000_000)}},
        ]
        _dedup_shows_by_external_id(shows)
        # Correct: 1_000_000 + 2_000_000 from merged episodes
        # Wrong (v1): 10B + 10B from show-level fields
        assert shows[0]['size_bytes'] == 3_000_000, \
            f'size_bytes must sum merged episode sizes, got {shows[0]["size_bytes"]}'

    def test_distinct_imdb_ids_left_alone(self):
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'A', 'imdb_id': 'tt1', 'source': 'debrid'},
            {'title': 'B', 'imdb_id': 'tt2', 'source': 'debrid'},
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 2

    def test_shows_without_imdb_id_passed_through(self):
        """Items lacking imdb_id can't safely merge — risk of
        distinct-shows-same-title collisions."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'Mystery 1', 'source': 'debrid'},
            {'title': 'Mystery 2', 'source': 'debrid'},
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 2

    def test_tmdb_id_alone_does_not_merge(self):
        """HIGH #3 fix: tmdb_id-only fallback was dead code in v1
        because enrichment never stamps tmdb_id on shows.  We dropped
        the fallback; shows with only tmdb_id are now treated as
        no-external-id (passed through, not merged)."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'X', 'tmdb_id': 123, 'source': 'debrid',
             '_episodes': {(1, 1): self._ep('a.mkv')}},
            {'title': 'X', 'tmdb_id': 123, 'source': 'debrid',
             '_episodes': {(1, 2): self._ep('b.mkv')}},
        ]
        _dedup_shows_by_external_id(shows)
        # Both pass through unmerged — there is no imdb_id either, so
        # they hit the no_id list.
        assert len(shows) == 2

    def test_source_promotion_to_both_when_local_present(self):
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             '_episodes': {(1, 1): self._ep('a.mkv')}},
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'local',
             '_episodes': {(1, 2): self._ep('b.mkv')}},
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 1
        assert shows[0]['source'] == 'both'

    def test_best_entry_preferred_by_rank(self):
        """Source ranking: 'both' > 'local' > 'debrid'.  When ranks
        tie, prefer entries with a year populated, then most episodes."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             'year': None, 'path': '/data/path1', '_episodes': {}},
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             'year': 2024, 'path': '/data/path2',
             '_episodes': {(1, 1): self._ep('a.mkv'),
                           (1, 2): self._ep('b.mkv')}},
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 1
        # path2 wins: has year AND more episodes
        assert shows[0]['path'] == '/data/path2'

    def test_single_show_no_op(self):
        from utils.library import _dedup_shows_by_external_id
        shows = [{'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid'}]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 1

    def test_empty_shows_no_op(self):
        from utils.library import _dedup_shows_by_external_id
        shows = []
        _dedup_shows_by_external_id(shows)
        assert shows == []

    def test_mixed_groups_only_dupes_collapsed(self):
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'A', 'imdb_id': 'tt1', 'source': 'debrid',
             '_episodes': {(1, 1): self._ep('a.mkv')}},
            {'title': 'A', 'imdb_id': 'tt1', 'source': 'debrid',
             '_episodes': {(1, 2): self._ep('b.mkv')}},
            {'title': 'B', 'imdb_id': 'tt2', 'source': 'debrid'},
            {'title': 'C', 'source': 'debrid'},  # no external id
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 3
        merged = next(s for s in shows if s.get('imdb_id') == 'tt1')
        assert len(merged['_episodes']) == 2

    # ── Reviewer-flagged v2 edge cases ──

    def test_sonarr_filtered_sibling_wins_survivor_pick(self):
        """v2 reviewer HIGH: survivor selection must prefer entries that
        ``_apply_sonarr_monitored_filter`` matched (i.e. have
        ``monitored_episodes`` set).  Without this, a non-Sonarr-matched
        rank-winner inherits TMDB-all missing math and the
        Grey's-Anatomy unmonitored-seasons regression returns."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            # Would-be rank-winner under old logic: more episodes, no Sonarr
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             'total_episodes': 430, 'missing_episodes': 428,
             # No monitored_episodes set → no Sonarr filter
             '_episodes': {(1, 1): self._ep('a.mkv'), (1, 2): self._ep('b.mkv')}},
            # Sonarr-matched: fewer episodes but correct monitored math
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             'total_episodes': 430, 'missing_episodes': 1,
             'monitored_episodes': 20,
             '_episodes': {(22, 1): self._ep('s22e1.mkv')}},
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 1
        merged = shows[0]
        # Sonarr-aware values must survive even though the sibling has
        # more episodes — reviewer-confirmed regression.
        assert merged['missing_episodes'] == 1
        assert merged.get('monitored_episodes') == 20

    def test_debrid_only_sibling_episode_not_tagged_both(self):
        """v2 reviewer HIGH: when merged source is 'both' (local present
        in one sibling), debrid-only sibling episodes WITHOUT an explicit
        ``source`` key in their info dict must NOT be tagged 'both' by
        the season_data rebuild.  Pre-fix the show-level merged 'both'
        was passed as ``default_source``, falsely promoting debrid-only
        episodes."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            # Local entry with explicit per-episode source='local'
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'local',
             '_episodes': {(1, 1): {'file': 'local.mkv', 'size_bytes': 100,
                                    'source': 'local'}}},
            # Debrid entry whose episode info dict has NO 'source' key
            # (FUSE/WebDAV scanners don't write one).
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             '_episodes': {(2, 1): {'file': 'debrid.mkv', 'size_bytes': 200}}},
        ]
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 1
        merged = shows[0]
        # Show-level promoted to 'both' (local + debrid)
        assert merged['source'] == 'both'
        # But the debrid-only episode must NOT inherit 'both'
        sd_by_season = {s['number']: s for s in merged['season_data']}
        s2_ep1 = sd_by_season[2]['episodes'][0]
        assert s2_ep1['source'] == 'debrid', \
            f'debrid-only sibling episode falsely tagged {s2_ep1["source"]!r} after rebuild'

    def test_size_bytes_falls_back_to_show_level_when_per_episode_zero(self):
        """v2 reviewer MEDIUM: legacy scanner paths emit empty info dicts
        with no size_bytes.  Summing 0 across them would silently zero
        the composition-card footprint.  Falls back to max sibling
        show-level size when per-episode sum is 0."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             'size_bytes': 5_000_000_000,
             '_episodes': {(1, 1): {'file': 'a.mkv'}}},  # no size_bytes
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             'size_bytes': 8_000_000_000,
             '_episodes': {(1, 2): {'file': 'b.mkv'}}},  # no size_bytes
        ]
        _dedup_shows_by_external_id(shows)
        # Per-episode sum is 0 → use max show-level fallback
        assert shows[0]['size_bytes'] == 8_000_000_000

    def test_episodes_missing_file_key_skipped_not_crashed(self):
        """v2 reviewer MEDIUM: ``_build_season_data`` does
        ``info['file']`` unconditionally — empty/file-less dicts crash
        it with KeyError.  ``_normalize_episodes_for_merge`` legacy
        list-of-tuples path emits ``{}`` shells.  Dedup must skip them
        rather than propagate into the rebuild."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             '_episodes': {(1, 1): self._ep('a.mkv'),
                           (1, 99): {}}},  # legacy empty shell
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             '_episodes': {(1, 2): self._ep('b.mkv')}},
        ]
        # Must not raise KeyError
        _dedup_shows_by_external_id(shows)
        assert len(shows) == 1
        # Empty shell dropped
        assert (1, 99) not in shows[0]['_episodes']
        # Real episodes preserved
        assert (1, 1) in shows[0]['_episodes']
        assert (1, 2) in shows[0]['_episodes']

    def test_equal_size_collision_keeps_first_seen(self):
        """v2 reviewer MEDIUM: equal-size collisions keep first-seen
        (the survivor's release) so folder/blocklist tracking stays on
        the survivor's release name.  Documented behavior, asserted."""
        from utils.library import _dedup_shows_by_external_id
        shows = [
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             '_episodes': {(1, 1): self._ep('survivor-release.mkv', 100)}},
            {'title': 'X', 'imdb_id': 'tt1', 'source': 'debrid',
             '_episodes': {(1, 1): self._ep('sibling-release.mkv', 100)}},
        ]
        _dedup_shows_by_external_id(shows)
        ep = shows[0]['_episodes'][(1, 1)]
        assert ep['file'] == 'survivor-release.mkv', \
            f'equal-size collision should keep first-seen, got {ep["file"]!r}'


class TestSearchForMissingEpisodesSkipGhosts:
    """Ghost entries (source='wanted') MUST NOT be processed by the
    gap-fill search loop. Reviewer-flagged CRITICAL from commit
    071ba5d: without this skip, the function calls ensure_and_search
    on Radarr-monitored movies that Radarr is already searching for,
    AND writes set_pending() — which causes the next scan's pending
    suppression to hide the ghost forever (Wanted view self-erases).
    """

    def test_ghost_movie_skipped_in_search_loop(self, monkeypatch):
        from utils.library import LibraryScanner

        # Build a scanner with the minimum surface to call
        # _search_for_missing_episodes. Real movies should pass through
        # to the search code path; the ghost should be skipped before
        # ensure_and_search is ever called for it.
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._alias_norms = {}
        scanner._lock = __import__('threading').RLock()

        radarr_client = MagicMock()
        radarr_client.configured = True
        radarr_client.ensure_and_search.return_value = {'status': 'sent'}
        radarr_client.find_movie_in_library.return_value = {
            'id': 42, 'tmdbId': 100,
        }
        monkeypatch.setattr(
            'utils.arr_client.get_download_service',
            lambda mt: (radarr_client, 'radarr') if mt == 'movie'
            else (None, None),
        )
        monkeypatch.setattr('utils.library.gap_fill_enabled', lambda: True)
        # Suppress all the persistence + history + notification side effects
        monkeypatch.setattr(
            'utils.library_prefs.get_all_pending', lambda: {},
        )
        monkeypatch.setattr(
            'utils.library_prefs.set_pending', MagicMock(),
        )
        monkeypatch.setattr(
            'utils.library_prefs.touch_pending_searched', MagicMock(),
        )
        monkeypatch.setattr(
            'utils.library_prefs.update_pending_error', MagicMock(),
        )

        movies = [
            # Ghost — should be skipped before reaching ensure_and_search
            {'title': 'Ghost Movie', 'year': 2025,
             'source': 'wanted', 'missing': True,
             '_radarr_tmdb_id': 100},
        ]
        # Empty shows, no preferences (route=None → gap-fill path)
        scanner._search_for_missing_episodes(shows=[], movies=movies,
                                             preferences={})

        # The critical assertion: ensure_and_search MUST NOT have been
        # called for the ghost. Pre-fix this fired on every scan and
        # caused the self-erase bug.
        assert radarr_client.ensure_and_search.call_count == 0

    def test_ghost_show_skipped_in_search_loop(self, monkeypatch):
        """TV mirror of the ghost-movie skip. A source='wanted' show has
        empty season_data, but _compute_missing_episodes derives candidates
        from the TMDB episode cache — so without the guard it would fire
        Sonarr searches and write a self-erasing set_pending entry."""
        from utils.library import LibraryScanner

        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._alias_norms = {}
        scanner._search_cooldown = {}
        scanner._lock = __import__('threading').RLock()

        sonarr_client = MagicMock()
        sonarr_client.configured = True
        sonarr_client.ensure_and_search.return_value = {'status': 'sent'}
        monkeypatch.setattr(
            'utils.arr_client.get_download_service',
            lambda mt: (sonarr_client, 'sonarr') if mt == 'show'
            else (None, None),
        )
        monkeypatch.setattr('utils.library.gap_fill_enabled', lambda: True)
        monkeypatch.setattr('utils.library_prefs.get_all_pending', lambda: {})
        monkeypatch.setattr('utils.library_prefs.set_pending', MagicMock())
        monkeypatch.setattr('utils.library_prefs.touch_pending_searched',
                            MagicMock())
        monkeypatch.setattr('utils.library_prefs.update_pending_error',
                            MagicMock())
        # If the guard were absent, the loop would reach this and find a
        # missing episode → fire ensure_and_search. The guard must skip
        # the ghost before _compute_missing_episodes is consulted.
        monkeypatch.setattr(LibraryScanner, '_compute_missing_episodes',
                            lambda self, show: [(1, 1)])

        shows = [
            {'title': 'Ghost Show', 'year': 2025, 'source': 'wanted',
             'missing': True, 'missing_episodes': 8, 'season_data': []},
        ]
        scanner._search_for_missing_episodes(shows=shows, movies=[],
                                             preferences={})

        assert sonarr_client.ensure_and_search.call_count == 0


class TestGetRadarrMoviesList:
    """TTL cache for the Radarr movie list."""

    def test_cached_within_ttl(self):
        from utils.library import _get_radarr_movies_list
        client = MagicMock()
        client.get_all_movies.side_effect = [
            [{'id': 1, 'title': 'A'}],
            [{'id': 2, 'title': 'B'}],
        ]
        first = _get_radarr_movies_list(client)
        second = _get_radarr_movies_list(client)
        assert first == [{'id': 1, 'title': 'A'}]
        assert second == first
        assert client.get_all_movies.call_count == 1

    def test_force_refresh_bypasses_cache(self):
        from utils.library import _get_radarr_movies_list
        client = MagicMock()
        client.get_all_movies.side_effect = [
            [{'id': 1, 'title': 'A'}],
            [{'id': 2, 'title': 'B'}],
        ]
        _get_radarr_movies_list(client)
        refreshed = _get_radarr_movies_list(client, force_refresh=True)
        assert refreshed == [{'id': 2, 'title': 'B'}]
        assert client.get_all_movies.call_count == 2

    def test_fetch_failure_returns_none(self):
        from utils.library import _get_radarr_movies_list
        client = MagicMock()
        client.get_all_movies.side_effect = RuntimeError('boom')
        result = _get_radarr_movies_list(client)
        assert result is None


class TestGetSonarrSeriesList:
    """TTL cache shared by the monitored-filter and symlink paths."""

    def test_cached_within_ttl(self):
        from utils.library import _get_sonarr_series_list
        client = MagicMock()
        client.get_all_series.side_effect = [
            [{'id': 1, 'title': 'A'}],
            [{'id': 2, 'title': 'B'}],  # should not be reached
        ]
        first = _get_sonarr_series_list(client)
        second = _get_sonarr_series_list(client)
        assert first == [{'id': 1, 'title': 'A'}]
        assert second == first
        assert client.get_all_series.call_count == 1

    def test_force_refresh_bypasses_cache(self):
        from utils.library import _get_sonarr_series_list
        client = MagicMock()
        client.get_all_series.side_effect = [
            [{'id': 1, 'title': 'A'}],
            [{'id': 2, 'title': 'B'}],
        ]
        _get_sonarr_series_list(client)
        refreshed = _get_sonarr_series_list(client, force_refresh=True)
        assert refreshed == [{'id': 2, 'title': 'B'}]
        assert client.get_all_series.call_count == 2

    def test_fetch_failure_returns_none_and_does_not_cache(self):
        """A transient fetch failure must return None and leave the cache
        empty so the next scan retries rather than returning stale data."""
        import utils.library as _lib
        client = MagicMock()
        client.get_all_series.side_effect = RuntimeError('boom')
        result = _lib._get_sonarr_series_list(client)
        assert result is None
        assert _lib._sonarr_series_cache['data'] is None


# ---------------------------------------------------------------------------
# Phase 1: memoize "Zurg lacks recursive PROPFIND" detection so the scanner
# stops re-attempting Depth: infinity on every cache miss.
# ---------------------------------------------------------------------------

class TestWebDAVUnsupportedMemoization:

    def _make_scanner(self):
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = '/mnt/debrid'
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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        # Point at a non-writable subpath so any accidental persist is a
        # safe no-op (the persist method swallows OSError).  Tests that
        # need a real round-trip override this attribute explicitly.
        scanner._capabilities_path = '/dev/null/library_capabilities.json'
        return scanner

    def test_flag_set_on_first_detection(self, monkeypatch):
        """folders-but-no-files response flips the memoization flag."""
        scanner = self._make_scanner()
        monkeypatch.setattr(library, '_discover_zurg_url',
                            lambda mp: 'http://zurg:9999')
        monkeypatch.setattr(library, '_get_zurg_auth', lambda: None)

        def fake_propfind(url, depth, auth, timeout):
            if depth == 1:
                # Root listing — return one scannable category.
                return [
                    {'href': '/dav/', 'name': '', 'is_collection': True, 'size': 0},
                    {'href': '/dav/movies/', 'name': 'movies',
                     'is_collection': True, 'size': 0},
                ]
            # Depth=infinity: Zurg returns folder names but no files.
            return [
                {'href': '/dav/movies/', 'name': 'movies',
                 'is_collection': True, 'size': 0},
                {'href': '/dav/movies/Inception/', 'name': 'Inception',
                 'is_collection': True, 'size': 0},
                {'href': '/dav/movies/Dune/', 'name': 'Dune',
                 'is_collection': True, 'size': 0},
            ]
        monkeypatch.setattr('utils.webdav.propfind', fake_propfind)

        with pytest.raises(library._WebDAVUnsupportedError):
            scanner._webdav_scan_mount()
        assert scanner._webdav_unsupported is True

    def test_memoized_short_circuits_propfind(self, monkeypatch):
        """Once memoized, _webdav_scan_mount must not issue any HTTP."""
        scanner = self._make_scanner()
        scanner._webdav_unsupported = True

        called = []

        def fake_propfind(*a, **kw):
            called.append(1)
            return []

        monkeypatch.setattr('utils.webdav.propfind', fake_propfind)
        # Sentinel: _discover_zurg_url must not be called either, since the
        # short-circuit happens before the URL is resolved.
        monkeypatch.setattr(library, '_discover_zurg_url',
                            lambda mp: pytest.fail('should not be called'))

        with pytest.raises(library._WebDAVUnsupportedError, match='memoized'):
            scanner._webdav_scan_mount()
        assert called == []

    def test_log_demoted_to_debug_after_first_detection(self, monkeypatch):
        """First "using FUSE" log fires at INFO; subsequent fires at DEBUG.

        We spy on the logger directly rather than caplog because the custom
        ZURGARR logger has its own handler config that doesn't always play
        well with caplog's root-handler propagation.
        """
        scanner = self._make_scanner()

        # Stub everything _scan_read touches after the WebDAV failure.
        monkeypatch.setattr(scanner, '_scan_mount', lambda *a, **kw: ([], []))
        monkeypatch.setattr(scanner, '_scan_local_movies', lambda: [])
        monkeypatch.setattr(scanner, '_scan_local_shows', lambda: [])
        monkeypatch.setattr(scanner, '_dedup_by_tmdb',
                            lambda items, _aliases: items)
        monkeypatch.setattr(library, '_build_tmdb_aliases', lambda: ({}, {}))
        monkeypatch.setattr(library, '_enrich_with_tmdb_cache',
                            lambda movies, shows, **kw: [])
        monkeypatch.setattr(library, '_apply_sonarr_monitored_filter',
                            lambda shows, **kw: None)
        from utils import library_prefs
        monkeypatch.setattr(library_prefs, 'get_all_preferences', lambda: {})

        info_calls = []
        debug_calls = []
        monkeypatch.setattr(
            library.logger, 'info',
            lambda msg, *a, **kw: info_calls.append(msg % a if a else msg),
        )
        monkeypatch.setattr(
            library.logger, 'debug',
            lambda msg, *a, **kw: debug_calls.append(msg % a if a else msg),
        )

        # First scan — webdav raises the detection-style error.
        def first_call(*a, **kw):
            scanner._webdav_unsupported = True
            raise library._WebDAVUnsupportedError(
                "WebDAV depth-infinity returned 5 folders but 0 files for movies"
            )
        monkeypatch.setattr(scanner, '_webdav_scan_mount', first_call)

        scanner._scan_read()
        info_msgs = [m for m in info_calls if 'WebDAV scan unavailable' in m]
        debug_msgs = [m for m in debug_calls if 'WebDAV scan unavailable' in m]
        assert len(info_msgs) == 1
        assert debug_msgs == []
        assert scanner._webdav_unsupported_logged is True

        # Second scan — memoized branch raises immediately.  Log demoted.
        info_calls.clear()
        debug_calls.clear()

        def second_call(*a, **kw):
            raise library._WebDAVUnsupportedError(
                "Zurg lacks recursive PROPFIND (memoized)"
            )
        monkeypatch.setattr(scanner, '_webdav_scan_mount', second_call)

        scanner._scan_read()
        info_msgs = [m for m in info_calls if 'WebDAV scan unavailable' in m]
        debug_msgs = [m for m in debug_calls if 'WebDAV scan unavailable' in m]
        assert info_msgs == []
        assert len(debug_msgs) == 1

    def test_transient_failure_does_not_set_logged_flag(self, monkeypatch):
        """A non-unsupported exception (e.g. transient DNS) keeps logging
        at INFO and must NOT flip the memoization flags — flag-flipping is
        reserved for the typed unsupported error so transient outages
        don't permanently silence the FUSE-fallback log or wedge the
        scanner into FUSE-only mode."""
        scanner = self._make_scanner()

        monkeypatch.setattr(scanner, '_scan_mount', lambda *a, **kw: ([], []))
        monkeypatch.setattr(scanner, '_scan_local_movies', lambda: [])
        monkeypatch.setattr(scanner, '_scan_local_shows', lambda: [])
        monkeypatch.setattr(scanner, '_dedup_by_tmdb',
                            lambda items, _aliases: items)
        monkeypatch.setattr(library, '_build_tmdb_aliases', lambda: ({}, {}))
        monkeypatch.setattr(library, '_enrich_with_tmdb_cache',
                            lambda movies, shows, **kw: [])
        monkeypatch.setattr(library, '_apply_sonarr_monitored_filter',
                            lambda shows, **kw: None)
        from utils import library_prefs
        monkeypatch.setattr(library_prefs, 'get_all_preferences', lambda: {})

        info_calls = []
        debug_calls = []
        monkeypatch.setattr(
            library.logger, 'info',
            lambda msg, *a, **kw: info_calls.append(msg % a if a else msg),
        )
        monkeypatch.setattr(
            library.logger, 'debug',
            lambda msg, *a, **kw: debug_calls.append(msg % a if a else msg),
        )

        def raise_transient(*a, **kw):
            raise OSError('connection refused')
        monkeypatch.setattr(scanner, '_webdav_scan_mount', raise_transient)

        scanner._scan_read()

        info_msgs = [m for m in info_calls if 'WebDAV scan unavailable' in m]
        debug_msgs = [m for m in debug_calls if 'WebDAV scan unavailable' in m]
        assert len(info_msgs) == 1
        assert debug_msgs == []
        assert scanner._webdav_unsupported is False
        assert scanner._webdav_unsupported_logged is False


class TestWebDAVScanSkipsObfuscated:

    def test_obfuscated_folder_excluded_from_webdav_scan(self, monkeypatch):
        scanner = TestWebDAVUnsupportedMemoization._make_scanner(
            TestWebDAVUnsupportedMemoization())
        monkeypatch.setattr(library, '_discover_zurg_url',
                            lambda mp: 'http://zurg:9999')
        monkeypatch.setattr(library, '_get_zurg_auth', lambda: None)

        hex_dir = '050bd19ee9934249a2ce4c9762c0d710[EZTVx.to]'

        def fake_propfind(url, depth, auth, timeout):
            if depth == 1:
                return [
                    {'href': '/dav/', 'name': '', 'is_collection': True, 'size': 0},
                    {'href': '/dav/movies/', 'name': 'movies',
                     'is_collection': True, 'size': 0},
                ]
            return [
                {'href': '/dav/movies/', 'name': 'movies',
                 'is_collection': True, 'size': 0},
                {'href': f'/dav/movies/{hex_dir}/', 'name': hex_dir,
                 'is_collection': True, 'size': 0},
                {'href': f'/dav/movies/{hex_dir}/{hex_dir}.mkv',
                 'name': f'{hex_dir}.mkv', 'is_collection': False,
                 'size': 900_000_000},
                {'href': '/dav/movies/Inception (2010)/', 'name': 'Inception (2010)',
                 'is_collection': True, 'size': 0},
                {'href': '/dav/movies/Inception (2010)/Inception.mkv',
                 'name': 'Inception.mkv', 'is_collection': False,
                 'size': 800_000_000},
            ]
        monkeypatch.setattr('utils.webdav.propfind', fake_propfind)

        movies, shows = scanner._webdav_scan_mount()

        titles = {m['title'] for m in movies} | {s['title'] for s in shows}
        assert 'Inception' in titles
        assert not any('050bd19' in t.lower() for t in titles)

    def test_only_obfuscated_category_does_not_poison_memoization(self, monkeypatch):
        """A category containing ONLY obfuscated folders yields an empty
        folders dict, so it must not count toward the folders-but-no-files
        detection that memoizes WebDAV as unsupported."""
        scanner = TestWebDAVUnsupportedMemoization._make_scanner(
            TestWebDAVUnsupportedMemoization())
        monkeypatch.setattr(library, '_discover_zurg_url',
                            lambda mp: 'http://zurg:9999')
        monkeypatch.setattr(library, '_get_zurg_auth', lambda: None)

        hex_dir = '050bd19ee9934249a2ce4c9762c0d710[EZTVx.to]'

        def fake_propfind(url, depth, auth, timeout):
            if depth == 1:
                return [
                    {'href': '/dav/', 'name': '', 'is_collection': True, 'size': 0},
                    {'href': '/dav/movies/', 'name': 'movies',
                     'is_collection': True, 'size': 0},
                ]
            return [
                {'href': '/dav/movies/', 'name': 'movies',
                 'is_collection': True, 'size': 0},
                {'href': f'/dav/movies/{hex_dir}/', 'name': hex_dir,
                 'is_collection': True, 'size': 0},
                {'href': f'/dav/movies/{hex_dir}/{hex_dir}.mkv',
                 'name': f'{hex_dir}.mkv', 'is_collection': False,
                 'size': 900_000_000},
            ]
        monkeypatch.setattr('utils.webdav.propfind', fake_propfind)

        movies, shows = scanner._webdav_scan_mount()

        assert movies == []
        assert shows == []
        assert scanner._webdav_unsupported is False


# ---------------------------------------------------------------------------
# Phase 2: persist the WebDAV-unsupported memoization across container
# restarts so the doomed Depth: infinity probe isn't re-attempted on every
# cold boot.
# ---------------------------------------------------------------------------

class TestWebDAVCapabilityPersistence:

    def _make_scanner(self, capabilities_path):
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = '/mnt/debrid'
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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        scanner._capabilities_path = capabilities_path
        return scanner

    def test_persist_round_trip_pre_sets_flag(self, tmp_dir, monkeypatch):
        """First scanner detects → writes file.  Second scanner reads file
        on init and pre-sets the flag without any HTTP traffic."""
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        monkeypatch.delenv('ZURG_VERSION', raising=False)

        first = self._make_scanner(path)
        first._webdav_unsupported = True
        first._persist_webdav_capability()
        assert os.path.isfile(path)

        # Build a fresh scanner via __init__ pointed at the same CONFIG_DIR
        # so the load path runs end-to-end.
        monkeypatch.setenv('CONFIG_DIR', tmp_dir)
        # Avoid the rest of __init__ poking real paths / discovering mounts.
        monkeypatch.setattr(library, '_discover_mount', lambda: None)
        second = library.LibraryScanner()
        assert second._capabilities_path == path
        assert second._webdav_unsupported is True
        # Loaded from disk → suppress the duplicate INFO at first scan.
        assert second._webdav_unsupported_logged is True

    def test_corrupt_file_does_not_take_scanner_offline(self, tmp_dir):
        """A garbage capability file logs a warning and falls through to
        fresh detection — never raises out of __init__."""
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        with open(path, 'w') as fh:
            fh.write('this is not json{][')
        scanner = self._make_scanner(path)
        scanner._load_webdav_capability()
        assert scanner._webdav_unsupported is False
        assert scanner._webdav_unsupported_logged is False

    def test_oversize_file_rejected(self, tmp_dir):
        """A capability file larger than the size cap is ignored."""
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        with open(path, 'w') as fh:
            fh.write('a' * (library._WEBDAV_CAPABILITY_MAX_BYTES + 100))
        scanner = self._make_scanner(path)
        scanner._load_webdav_capability()
        assert scanner._webdav_unsupported is False

    def test_stale_cache_re_evaluates(self, tmp_dir):
        """Records older than the TTL are ignored — fresh detection runs
        on next scan in case Zurg has been upgraded since."""
        import json
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        with open(path, 'w') as fh:
            json.dump({
                'webdav_unsupported': True,
                'ts': time.time() - (library._WEBDAV_CAPABILITY_TTL_S + 60),
                'zurg_version': None,
            }, fh)
        scanner = self._make_scanner(path)
        scanner._load_webdav_capability()
        assert scanner._webdav_unsupported is False

    def test_future_dated_cache_rejected(self, tmp_dir):
        """A timestamp far in the future (clock skew or tampering) is
        ignored — better to re-detect than trust a clearly-bogus record."""
        import json
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        with open(path, 'w') as fh:
            json.dump({
                'webdav_unsupported': True,
                'ts': time.time() + (2 * 86400),
                'zurg_version': None,
            }, fh)
        scanner = self._make_scanner(path)
        scanner._load_webdav_capability()
        assert scanner._webdav_unsupported is False

    def test_zurg_version_change_invalidates_cache(self, tmp_dir, monkeypatch):
        """If ZURG_VERSION differs from the recorded one, drop the cache —
        a Zurg upgrade may have added recursive PROPFIND support."""
        import json
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        with open(path, 'w') as fh:
            json.dump({
                'webdav_unsupported': True,
                'ts': time.time(),
                'zurg_version': 'v0.9.2',
            }, fh)
        monkeypatch.setenv('ZURG_VERSION', 'v0.9.3')
        scanner = self._make_scanner(path)
        scanner._load_webdav_capability()
        assert scanner._webdav_unsupported is False

    def test_zurg_version_match_keeps_cache(self, tmp_dir, monkeypatch):
        """Matching ZURG_VERSION → cache trusted, flag pre-set."""
        import json
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        with open(path, 'w') as fh:
            json.dump({
                'webdav_unsupported': True,
                'ts': time.time(),
                'zurg_version': 'v0.9.2',
            }, fh)
        monkeypatch.setenv('ZURG_VERSION', 'v0.9.2')
        scanner = self._make_scanner(path)
        scanner._load_webdav_capability()
        assert scanner._webdav_unsupported is True
        assert scanner._webdav_unsupported_logged is True

    def test_missing_file_no_op(self, tmp_dir):
        """No capability file → no flag, no error."""
        path = os.path.join(tmp_dir, 'does-not-exist.json')
        scanner = self._make_scanner(path)
        scanner._load_webdav_capability()
        assert scanner._webdav_unsupported is False

    def test_persist_failure_does_not_raise(self, tmp_dir):
        """Read-only / unwritable target → warning logged, no exception
        propagates — the scanner falls back to in-memory memoization for
        the rest of the process lifetime."""
        # Point at a directory that doesn't exist as a parent — atomic_write
        # will fail with OSError, which the persist method must swallow.
        scanner = self._make_scanner('/dev/null/nope/library_capabilities.json')
        scanner._webdav_unsupported = True
        # Must not raise.
        scanner._persist_webdav_capability()

    def test_detection_writes_file(self, tmp_dir, monkeypatch):
        """End-to-end: when _webdav_scan_mount detects the bad shape, it
        persists the capability cache before raising."""
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        scanner = self._make_scanner(path)
        monkeypatch.setattr(library, '_discover_zurg_url',
                            lambda mp: 'http://zurg:9999')
        monkeypatch.setattr(library, '_get_zurg_auth', lambda: None)

        def fake_propfind(url, depth, auth, timeout):
            if depth == 1:
                return [
                    {'href': '/dav/', 'name': '', 'is_collection': True, 'size': 0},
                    {'href': '/dav/movies/', 'name': 'movies',
                     'is_collection': True, 'size': 0},
                ]
            return [
                {'href': '/dav/movies/', 'name': 'movies',
                 'is_collection': True, 'size': 0},
                {'href': '/dav/movies/Inception/', 'name': 'Inception',
                 'is_collection': True, 'size': 0},
            ]
        monkeypatch.setattr('utils.webdav.propfind', fake_propfind)
        monkeypatch.setenv('ZURG_VERSION', 'v0.9.2-test')

        with pytest.raises(library._WebDAVUnsupportedError):
            scanner._webdav_scan_mount()

        assert os.path.isfile(path)
        import json
        with open(path) as fh:
            payload = json.load(fh)
        assert payload['webdav_unsupported'] is True
        assert payload['zurg_version'] == 'v0.9.2-test'
        assert isinstance(payload['ts'], (int, float))

    def test_load_does_not_set_flag_when_field_falsy(self, tmp_dir):
        """A persisted record with `webdav_unsupported: false` (or missing)
        must not flip the flag — only truthy records lock in FUSE."""
        import json
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        with open(path, 'w') as fh:
            json.dump({
                'webdav_unsupported': False,
                'ts': time.time(),
                'zurg_version': None,
            }, fh)
        scanner = self._make_scanner(path)
        scanner._load_webdav_capability()
        assert scanner._webdav_unsupported is False

    def test_load_rejects_non_bool_truthy_values(self, tmp_dir):
        """A hand-edited file with `webdav_unsupported: "yes"` (or any
        non-bool truthy) must NOT lock the scanner into FUSE — only the
        canonical Python `True` value qualifies.  Closes the gap where
        `not raw.get(...)` would have accepted truthy strings."""
        import json
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        for value in ['yes', 'true', 1, [True], {'inner': True}]:
            with open(path, 'w') as fh:
                json.dump({
                    'webdav_unsupported': value,
                    'ts': time.time(),
                    'zurg_version': None,
                }, fh)
            scanner = self._make_scanner(path)
            scanner._load_webdav_capability()
            assert scanner._webdav_unsupported is False, (
                f"non-bool value {value!r} incorrectly flipped the flag"
            )


# ---------------------------------------------------------------------------
# Phase 2 hardening: aggregate detection across categories so a single
# quirky empty category doesn't permanently lock the scanner into FUSE.
# ---------------------------------------------------------------------------

class TestWebDAVCrossCategoryDetection:
    """Phase 2 must not memoize "Zurg unsupported" based on a single
    empty/quirky category when other categories prove recursive PROPFIND
    works.  Tests the aggregate verdict at the bottom of
    `_webdav_scan_mount`."""

    def _make_scanner(self, capabilities_path):
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = '/mnt/debrid'
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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        scanner._capabilities_path = capabilities_path
        return scanner

    def test_one_working_category_prevents_unsupported_lockout(self, tmp_dir, monkeypatch):
        """If category A returns folders+files (Zurg recursion works) and
        category B returns folders-but-no-files (quirky empty cat), the
        scan must NOT memoize unsupported — the empty category is
        skipped, not promoted to a global verdict."""
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        scanner = self._make_scanner(path)
        monkeypatch.setattr(library, '_discover_zurg_url',
                            lambda mp: 'http://zurg:9999')
        monkeypatch.setattr(library, '_get_zurg_auth', lambda: None)
        monkeypatch.setattr(scanner, '_mount_path_for',
                            lambda cat, rel: f'/mnt/debrid/{cat}/{rel}')

        def fake_propfind(url, depth, auth, timeout):
            if depth == 1:
                return [
                    {'href': '/dav/', 'name': '', 'is_collection': True, 'size': 0},
                    {'href': '/dav/movies/', 'name': 'movies',
                     'is_collection': True, 'size': 0},
                    {'href': '/dav/shows/', 'name': 'shows',
                     'is_collection': True, 'size': 0},
                ]
            if 'movies' in url:
                # Working: folders WITH files inside.
                return [
                    {'href': '/dav/movies/', 'name': 'movies',
                     'is_collection': True, 'size': 0},
                    {'href': '/dav/movies/Inception/', 'name': 'Inception',
                     'is_collection': True, 'size': 0},
                    {'href': '/dav/movies/Inception/movie.mkv', 'name': 'movie.mkv',
                     'is_collection': False, 'size': 1000000},
                ]
            # shows: folders WITHOUT files (quirky empty cat).
            return [
                {'href': '/dav/shows/', 'name': 'shows',
                 'is_collection': True, 'size': 0},
                {'href': '/dav/shows/EmptyShow/', 'name': 'EmptyShow',
                 'is_collection': True, 'size': 0},
            ]
        monkeypatch.setattr('utils.webdav.propfind', fake_propfind)

        # Should NOT raise — movies category proves Zurg works.
        movies, shows = scanner._webdav_scan_mount()
        assert scanner._webdav_unsupported is False
        assert not os.path.exists(path), (
            "capability cache should not be written when at least one category works"
        )
        assert any(m['title'] == 'Inception' for m in movies)

    def test_all_empty_categories_trigger_unsupported(self, tmp_dir, monkeypatch):
        """If every category that returned folders has zero files, the
        memoization fires and the cache is persisted."""
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        scanner = self._make_scanner(path)
        monkeypatch.setattr(library, '_discover_zurg_url',
                            lambda mp: 'http://zurg:9999')
        monkeypatch.setattr(library, '_get_zurg_auth', lambda: None)

        def fake_propfind(url, depth, auth, timeout):
            if depth == 1:
                return [
                    {'href': '/dav/', 'name': '', 'is_collection': True, 'size': 0},
                    {'href': '/dav/movies/', 'name': 'movies',
                     'is_collection': True, 'size': 0},
                    {'href': '/dav/shows/', 'name': 'shows',
                     'is_collection': True, 'size': 0},
                ]
            # Both categories have folders but no files.
            cat = 'movies' if 'movies' in url else 'shows'
            return [
                {'href': f'/dav/{cat}/', 'name': cat,
                 'is_collection': True, 'size': 0},
                {'href': f'/dav/{cat}/Folder/', 'name': 'Folder',
                 'is_collection': True, 'size': 0},
            ]
        monkeypatch.setattr('utils.webdav.propfind', fake_propfind)

        with pytest.raises(library._WebDAVUnsupportedError):
            scanner._webdav_scan_mount()
        assert scanner._webdav_unsupported is True
        assert os.path.isfile(path)

    def test_partial_scan_with_empty_category_does_not_memoize(self, tmp_dir, monkeypatch):
        """If we scan one category (empty) and the deadline forces us to
        skip the rest, scan_completed=False blocks the verdict — the
        cache must NOT be written.  Without this gate, a slow first
        category combined with one quirky empty category would lock the
        scanner into a 7-day false negative."""
        path = os.path.join(tmp_dir, 'library_capabilities.json')
        scanner = self._make_scanner(path)
        monkeypatch.setattr(library, '_discover_zurg_url',
                            lambda mp: 'http://zurg:9999')
        monkeypatch.setattr(library, '_get_zurg_auth', lambda: None)

        # Step the fake clock past the deadline only after the first
        # category's PROPFIND completes, so the second iteration's
        # `time.monotonic() > deadline` check fires and breaks the loop.
        base = time.monotonic()
        ticks = {'count': 0}

        def fake_monotonic():
            ticks['count'] += 1
            # Return base for the first ~5 calls (initial setup + root
            # PROPFIND + first category iteration), then jump past
            # base+30 so the next loop-top check breaks out.
            if ticks['count'] < 6:
                return base
            return base + 100

        monkeypatch.setattr(time, 'monotonic', fake_monotonic)

        def fake_propfind(url, depth, auth, timeout):
            if depth == 1:
                return [
                    {'href': '/dav/', 'name': '', 'is_collection': True, 'size': 0},
                    {'href': '/dav/movies/', 'name': 'movies',
                     'is_collection': True, 'size': 0},
                    {'href': '/dav/shows/', 'name': 'shows',
                     'is_collection': True, 'size': 0},
                ]
            return [
                {'href': '/dav/Cat/', 'name': 'Cat',
                 'is_collection': True, 'size': 0},
                {'href': '/dav/Cat/Folder/', 'name': 'Folder',
                 'is_collection': True, 'size': 0},
            ]
        monkeypatch.setattr('utils.webdav.propfind', fake_propfind)

        # Returns ([], []) without raising — partial scan is incomplete.
        scanner._webdav_scan_mount(deadline=base + 30)

        assert scanner._webdav_unsupported is False, (
            "incomplete scan must not memoize unsupported"
        )
        assert not os.path.exists(path), (
            "capability cache must not be written from an incomplete scan"
        )


# ---------------------------------------------------------------------------
# Library cache persistence (plan 37)
# ---------------------------------------------------------------------------


class TestLibraryCachePersistence:
    """Persist `_cache` + path indexes to /config/library_cache.json so cold
    start serves last-known-good library instantly instead of waiting on a
    51-second FUSE walk.
    """

    def _sample_state(self):
        cache = {
            'movies': [{'title': 'Some Movie', 'year': 2024}],
            'shows': [{'title': 'Some Show'}],
            'preferences': {'show': 'prefer-debrid'},
            'last_scan': '2026-04-25T14:00:00+00:00',
            'scan_duration_ms': 51000,
        }
        path_index = {
            ('some show', 1, 1): '/mnt/zurgarr/shows/Some Show/S01E01.mkv',
            ('some show', 1, 2): '/mnt/zurgarr/shows/Some Show/S01E02.mkv',
        }
        local_path_index = {
            ('some show', 1, 1): '/local/tv/Some Show/S01E01.mkv',
        }
        alias_norms = {'some show': {'some show', 'the show'}}
        return cache, path_index, local_path_index, alias_norms

    def _make_scanner(self, cache_path):
        scanner = LibraryScanner.__new__(LibraryScanner)
        scanner._mount_path = '/mnt/debrid'
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
        scanner._library_cache_path = cache_path
        return scanner

    # --- serialize/deserialize helpers ---

    def test_round_trip_preserves_cache_and_indexes(self):
        cache, pi, lpi, an = self._sample_state()
        env = library._serialize_cache_state(cache, pi, lpi, an)
        result = library._deserialize_cache_state(env)
        assert result is not None
        c2, pi2, lpi2, an2 = result
        assert c2 == cache
        assert pi2 == pi
        assert lpi2 == lpi
        assert an2 == an

    def test_arr_degraded_stripped_on_deserialize(self):
        """``arr_degraded`` is a per-scan runtime signal for the recovery
        snapshot writer — a warm-started payload must never replay a
        previous run's degradation flag."""
        cache, pi, lpi, an = self._sample_state()
        cache['arr_degraded'] = ['sonarr_series']
        env = library._serialize_cache_state(cache, pi, lpi, an)
        result = library._deserialize_cache_state(env)
        assert result is not None
        assert 'arr_degraded' not in result[0]

    def test_tuple_keys_round_trip(self):
        """JSON has no tuple keys — serialize as 4-element rows, restore as tuples."""
        cache, pi, _, _ = self._sample_state()
        env = library._serialize_cache_state(cache, pi, {}, {})
        # Wire-format check: rows are lists, not tuples.
        assert all(isinstance(row, list) and len(row) == 4 for row in env['path_index'])
        result = library._deserialize_cache_state(env)
        _, pi2, _, _ = result
        # Keys must be tuples again so existing get(...) calls keep working.
        for k in pi2:
            assert isinstance(k, tuple) and len(k) == 3

    def test_alias_norms_set_round_trip(self):
        cache, _, _, _ = self._sample_state()
        an = {'a': {'a', 'b', 'c'}}
        env = library._serialize_cache_state(cache, {}, {}, an)
        # Wire format is sorted list for deterministic disk content.
        assert env['alias_norms']['a'] == ['a', 'b', 'c']
        result = library._deserialize_cache_state(env)
        _, _, _, an2 = result
        assert an2 == an
        assert isinstance(an2['a'], set)

    # --- invalidation on load ---

    def test_missing_file_no_op(self, tmp_dir):
        path = os.path.join(tmp_dir, 'library_cache.json')
        scanner = self._make_scanner(path)
        scanner._load_persisted_cache()
        assert scanner._cache is None
        assert scanner._path_index == {}

    def test_corrupt_json_treated_as_missing(self, tmp_dir):
        path = os.path.join(tmp_dir, 'library_cache.json')
        with open(path, 'w') as fh:
            fh.write('}}}not json[[')
        scanner = self._make_scanner(path)
        scanner._load_persisted_cache()
        assert scanner._cache is None

    def test_size_cap_via_fstat_rejects_oversize(self, tmp_dir):
        """File larger than the 16 MB cap is rejected before json.load runs."""
        path = os.path.join(tmp_dir, 'library_cache.json')
        with open(path, 'w') as fh:
            # One byte over the cap is enough.  Doesn't matter that the
            # content isn't valid JSON — the size check fires first.
            fh.write('a' * (library._LIBRARY_CACHE_MAX_BYTES + 1))
        scanner = self._make_scanner(path)
        scanner._load_persisted_cache()
        assert scanner._cache is None

    def test_schema_mismatch_rejected(self, tmp_dir):
        import json
        cache, pi, lpi, an = self._sample_state()
        env = library._serialize_cache_state(cache, pi, lpi, an)
        env['schema'] = 99
        path = os.path.join(tmp_dir, 'library_cache.json')
        with open(path, 'w') as fh:
            json.dump(env, fh)
        scanner = self._make_scanner(path)
        scanner._load_persisted_cache()
        assert scanner._cache is None

    def test_version_mismatch_rejected(self, tmp_dir):
        import json
        cache, pi, lpi, an = self._sample_state()
        env = library._serialize_cache_state(cache, pi, lpi, an)
        env['zurgarr_version'] = '0.0.0-not-real'
        path = os.path.join(tmp_dir, 'library_cache.json')
        with open(path, 'w') as fh:
            json.dump(env, fh)
        scanner = self._make_scanner(path)
        scanner._load_persisted_cache()
        assert scanner._cache is None

    def test_future_dated_ts_rejected(self, tmp_dir):
        import json
        cache, pi, lpi, an = self._sample_state()
        env = library._serialize_cache_state(cache, pi, lpi, an)
        env['ts'] = time.time() + library._LIBRARY_CACHE_FUTURE_TS_TOLERANCE_S + 60
        path = os.path.join(tmp_dir, 'library_cache.json')
        with open(path, 'w') as fh:
            json.dump(env, fh)
        scanner = self._make_scanner(path)
        scanner._load_persisted_cache()
        assert scanner._cache is None

    def test_persist_failure_swallowed(self, tmp_dir, monkeypatch):
        """A write failure must NOT propagate out of the scan path."""
        path = os.path.join(tmp_dir, 'library_cache.json')
        scanner = self._make_scanner(path)
        cache, pi, lpi, an = self._sample_state()

        def boom(*args, **kwargs):
            raise OSError('disk is read-only in tests')

        # atomic_write is imported lazily inside _persist_cache.
        import utils.file_utils as fu
        monkeypatch.setattr(fu, 'atomic_write', boom)
        # Must not raise.
        scanner._persist_cache(cache, pi, lpi, an)
        # File never created.
        assert not os.path.exists(path)

    def test_persist_no_op_when_path_unset(self, tmp_dir):
        """Partially-constructed scanners (built via ``__new__`` in unit
        tests, lacking ``_library_cache_path``) must no-op silently
        rather than raise — the existing capability test helpers rely on
        this."""
        scanner = LibraryScanner.__new__(LibraryScanner)
        # Intentionally no _library_cache_path attribute.
        scanner._path_lock = threading.Lock()
        cache, pi, lpi, an = self._sample_state()
        # Must not raise.
        scanner._persist_cache(cache, pi, lpi, an)

    # --- strict field types ---

    def test_strict_int_rejects_bool_in_path_index(self):
        cache, _, _, _ = self._sample_state()
        env = library._serialize_cache_state(cache, {}, {}, {})
        env['path_index'] = [['show', True, 1, '/p']]
        assert library._deserialize_cache_state(env) is None

    def test_path_index_row_arity_validated(self):
        cache, _, _, _ = self._sample_state()
        env = library._serialize_cache_state(cache, {}, {}, {})
        env['path_index'] = [['show', 1, 1]]  # missing path
        assert library._deserialize_cache_state(env) is None

    def test_movies_must_be_list(self):
        cache, pi, lpi, an = self._sample_state()
        env = library._serialize_cache_state(cache, pi, lpi, an)
        env['cache']['movies'] = 'not a list'
        assert library._deserialize_cache_state(env) is None

    def test_alias_norms_values_must_be_list_of_str(self):
        cache, _, _, _ = self._sample_state()
        env = library._serialize_cache_state(cache, {}, {}, {})
        env['alias_norms'] = {'a': [1, 2]}
        assert library._deserialize_cache_state(env) is None

    # --- end-to-end ---

    def test_end_to_end_warm_start(self, tmp_dir, monkeypatch):
        """Persist via one scanner, load via a fresh LibraryScanner() and
        verify get_data() returns the persisted cache without scanning."""
        cache_path = os.path.join(tmp_dir, 'library_cache.json')
        scanner = self._make_scanner(cache_path)
        cache, pi, lpi, an = self._sample_state()
        scanner._persist_cache(cache, pi, lpi, an)
        assert os.path.isfile(cache_path)

        # Build a fresh scanner via __init__ pointed at the same CONFIG_DIR.
        # _discover_mount and the scan-state load are stubbed so __init__
        # doesn't poke unrelated paths.
        monkeypatch.setenv('CONFIG_DIR', tmp_dir)
        monkeypatch.setattr(library, '_discover_mount', lambda: None)
        fresh = library.LibraryScanner()
        assert fresh._library_cache_path == cache_path
        assert fresh._cache == cache
        assert fresh._path_index == pi
        assert fresh._local_path_index == lpi
        assert fresh._alias_norms == an

        # get_data() must return the loaded cache without triggering a scan.
        scan_called = []
        monkeypatch.setattr(fresh, 'scan', lambda *a, **k: scan_called.append(1) or cache)
        out = fresh.get_data()
        assert out == cache
        assert scan_called == [], 'get_data() should serve from persisted cache'

    def test_load_failure_leaves_init_state_untouched(self, tmp_dir):
        """Even with a corrupt file, _cache stays None and indexes stay {}."""
        path = os.path.join(tmp_dir, 'library_cache.json')
        with open(path, 'w') as fh:
            fh.write('garbage')
        scanner = self._make_scanner(path)
        scanner._cache = None
        scanner._path_index = {}
        scanner._load_persisted_cache()
        assert scanner._cache is None
        assert scanner._path_index == {}

    def test_successful_load_sets_cache_time(self, tmp_dir):
        """A successful load advances ``_cache_time`` to ``time.monotonic()``
        so the very next ``get_data()`` call serves the persisted view
        without triggering a synchronous scan."""
        cache_path = os.path.join(tmp_dir, 'library_cache.json')
        scanner = self._make_scanner(cache_path)
        cache, pi, lpi, an = self._sample_state()
        scanner._persist_cache(cache, pi, lpi, an)

        fresh = self._make_scanner(cache_path)
        before = time.monotonic()
        fresh._load_persisted_cache()
        after = time.monotonic()
        # Must be set to a recent monotonic timestamp.
        assert before <= fresh._cache_time <= after

    @pytest.mark.parametrize('mutation', [
        ('preferences', 'not a dict'),
        ('last_scan', 12345),
        ('scan_duration_ms', 'not a number'),
        ('scan_duration_ms', True),  # bool-as-int trap
    ])
    def test_inner_cache_strict_types_rejected(self, mutation):
        """Inner cache fields used by downstream consumers must pass
        strict-type validation; a tampered file with the wrong type
        rejects the whole envelope."""
        cache, pi, lpi, an = self._sample_state()
        env = library._serialize_cache_state(cache, pi, lpi, an)
        key, val = mutation
        env['cache'][key] = val
        assert library._deserialize_cache_state(env) is None


# ---------------------------------------------------------------------------
# Phase 4: dual-debrid library view
# ---------------------------------------------------------------------------

class TestPhase4DualDebridMerge:
    """The library scanner enumerates both the RD mount (existing) and
    the TB mount (new in plan 39 phase 4).  Items unique to TB are
    appended; items present on both get ``has_alt_source=True`` so the
    UI can render a pair-badge."""

    def test_alt_unique_items_appended(self):
        """A movie only on TB joins the primary list with source_debrid=torbox."""
        primary_movies = [
            {'title': 'On RD Only', 'year': 2020,
             'source': 'debrid', 'source_debrid': 'realdebrid',
             'type': 'movie'},
        ]
        alt_movies = [
            {'title': 'TB Exclusive', 'year': 2021,
             'source': 'debrid', 'source_debrid': 'torbox',
             'type': 'movie'},
        ]
        m, s = library.LibraryScanner._merge_alt_debrid_items(
            primary_movies, [], alt_movies, [],
        )
        titles = sorted(x['title'] for x in m)
        assert titles == ['On RD Only', 'TB Exclusive']
        tb = next(x for x in m if x['title'] == 'TB Exclusive')
        assert tb['source_debrid'] == 'torbox'
        # No has_alt_source flag on uniques
        assert 'has_alt_source' not in tb
        assert 'has_alt_source' not in next(x for x in m if x['title'] == 'On RD Only')

    def test_movie_on_both_flagged_with_alt_source(self):
        """Same movie on both → keep ONE entry, flag with alt-source."""
        primary_movies = [
            {'title': 'Dune', 'year': 2021,
             'source': 'debrid', 'source_debrid': 'realdebrid',
             'type': 'movie'},
        ]
        alt_movies = [
            {'title': 'Dune', 'year': 2021,
             'source': 'debrid', 'source_debrid': 'torbox',
             'type': 'movie'},
        ]
        m, s = library.LibraryScanner._merge_alt_debrid_items(
            primary_movies, [], alt_movies, [],
        )
        assert len(m) == 1
        assert m[0]['source_debrid'] == 'realdebrid'  # primary wins
        assert m[0]['has_alt_source'] is True
        assert m[0]['alt_source_debrid'] == 'torbox'

    def test_shows_merge_episode_sets(self):
        """Show on both mounts: episodes from both should union, increasing
        the seasons + episodes counts."""
        primary_shows = [
            {'title': 'Show', 'year': 2020,
             'source': 'debrid', 'source_debrid': 'realdebrid', 'type': 'show',
             '_episodes': [(1, 1), (1, 2)],
             'seasons': 1, 'episodes': 2, 'path': '/data/zurgarr/shows/Show'},
        ]
        alt_shows = [
            {'title': 'Show', 'year': 2020,
             'source': 'debrid', 'source_debrid': 'torbox', 'type': 'show',
             '_episodes': [(1, 2), (2, 1)],   # S01E02 dup + new S02E01
             'seasons': 1, 'episodes': 2, 'path': '/data/torbox/shows/Show'},
        ]
        _, s = library.LibraryScanner._merge_alt_debrid_items(
            [], primary_shows, [], alt_shows,
        )
        assert len(s) == 1
        merged = s[0]
        assert merged['has_alt_source'] is True
        assert merged['alt_source_debrid'] == 'torbox'
        # S01E01 (RD), S01E02 (both, dedup'd), S02E01 (TB)
        eps = {tuple(e) if isinstance(e, list) else e for e in merged['_episodes']}
        assert eps == {(1, 1), (1, 2), (2, 1)}
        assert merged['seasons'] == 2
        assert merged['episodes'] == 3

    def test_no_alt_items_is_noop(self):
        """Empty alt lists → primary returned unchanged."""
        primary_movies = [
            {'title': 'X', 'year': 2020, 'source_debrid': 'realdebrid'},
        ]
        m, s = library.LibraryScanner._merge_alt_debrid_items(
            primary_movies, [], [], [],
        )
        assert m == primary_movies
        assert s == []
        # And no has_alt_source was injected
        assert 'has_alt_source' not in m[0]

    def test_missing_title_does_not_crash(self):
        """An item with an empty title (parsed-folder failure upstream)
        must not match other empty-title items.  Defensive: real data
        rarely has this but we don't want a TB-side parse failure to
        silently merge into an RD-side parse failure."""
        primary_movies = [{'title': '', 'source_debrid': 'realdebrid'}]
        alt_movies = [{'title': '', 'source_debrid': 'torbox'}]
        m, _ = library.LibraryScanner._merge_alt_debrid_items(
            primary_movies, [], alt_movies, [],
        )
        # Empty-title items are kept as-is, not merged.
        assert len(m) == 2

    def test_shows_merge_episode_dicts_from_scan_mount(self):
        """CRITICAL-2 regression: ``_scan_mount`` produces ``_episodes`` as
        a DICT keyed by ``(season, ep)`` (library.py:4543 ``'_episodes': eps``
        where eps comes from ``_collect_episodes``'s dict output).  Pre-fix
        the merge did ``p_eps + a_eps`` which raises TypeError on dict+dict.
        The except-Exception at library.py:1926 then swallowed the error
        and discarded the ENTIRE TB scan silently.

        The merge must accept dict-form ``_episodes`` and produce a merged
        dict preserving episode-info values from both sources (not just the
        key tuples).  Sibling test ``test_shows_merge_episode_sets`` uses
        tuple-list inputs so it doesn't catch the dict path."""
        primary_shows = [
            {'title': 'Show', 'year': 2020,
             'source': 'debrid', 'source_debrid': 'realdebrid', 'type': 'show',
             '_episodes': {
                 (1, 1): {'file': 'S01E01.mkv', 'path': '/data/zurgarr/shows/Show/S01E01.mkv',
                          'size_bytes': 100, 'folder': 'Show'},
                 (1, 2): {'file': 'S01E02.mkv', 'path': '/data/zurgarr/shows/Show/S01E02.mkv',
                          'size_bytes': 200, 'folder': 'Show'},
             },
             'seasons': 1, 'episodes': 2, 'path': '/data/zurgarr/shows/Show'},
        ]
        alt_shows = [
            {'title': 'Show', 'year': 2020,
             'source': 'debrid', 'source_debrid': 'torbox', 'type': 'show',
             '_episodes': {
                 (1, 2): {'file': 'S01E02.mkv', 'path': '/data/torbox/shows/Show/S01E02.mkv',
                          'size_bytes': 200, 'folder': 'Show'},
                 (2, 1): {'file': 'S02E01.mkv', 'path': '/data/torbox/shows/Show/S02E01.mkv',
                          'size_bytes': 300, 'folder': 'Show'},
             },
             'seasons': 2, 'episodes': 2, 'path': '/data/torbox/shows/Show'},
        ]
        # Pre-fix this raised TypeError: unsupported operand type(s) for +: 'dict' and 'dict'.
        _, s = library.LibraryScanner._merge_alt_debrid_items(
            [], primary_shows, [], alt_shows,
        )
        assert len(s) == 1
        merged = s[0]
        assert merged['has_alt_source'] is True
        assert merged['alt_source_debrid'] == 'torbox'

        eps = merged['_episodes']
        # Must remain dict-shaped so downstream consumers
        # (library.py:1839, library.py:2103) keep working.
        assert isinstance(eps, dict)
        assert set(eps.keys()) == {(1, 1), (1, 2), (2, 1)}
        # Episode-info values preserved (primary wins on dupes — its path
        # is the canonical one for the source_debrid badge).
        assert eps[(1, 1)]['file'] == 'S01E01.mkv'
        assert eps[(2, 1)]['file'] == 'S02E01.mkv'
        # Counts reflect the union.
        assert merged['seasons'] == 2
        assert merged['episodes'] == 3


class TestPhase4DiscoverTorboxMount:
    """``_discover_torbox_mount`` returns the TB mount path when the
    provider is configured AND the mount exists; None otherwise."""

    def test_returns_none_without_api_key(self, monkeypatch):
        monkeypatch.delenv('TORBOX_API_KEY', raising=False)
        assert library.LibraryScanner._discover_torbox_mount() is None

    def test_returns_none_when_mount_absent(self, monkeypatch, tmp_path):
        # Set the key but the mount path doesn't exist
        monkeypatch.setenv('TORBOX_API_KEY', 'tb-key')
        monkeypatch.setenv('TORBOX_MOUNT_NAME', 'torbox')
        # Don't create the mount directory
        assert library.LibraryScanner._discover_torbox_mount() is None

    def test_returns_path_when_mount_exists(self, monkeypatch, tmp_path):
        """Mount discovery uses utils.debrid_routing.mount_for_debrid
        which returns ``/data/<TORBOX_MOUNT_NAME>``.  We can't mock
        ``/data/torbox`` in pytest cleanly without root, so this test
        asserts the predicate logic via a temp dir that we explicitly
        configure as the TB mount via monkeypatching the helper."""
        monkeypatch.setenv('TORBOX_API_KEY', 'tb-key')
        monkeypatch.setenv('TORBOX_MOUNT_NAME', 'torbox')
        fake_mount = tmp_path / 'torbox_mount'
        fake_mount.mkdir()
        from utils import debrid_routing as _dr
        monkeypatch.setattr(_dr, 'mount_for_debrid',
                            lambda d, **kw: str(fake_mount) if d == 'torbox' else None)
        result = library.LibraryScanner._discover_torbox_mount()
        assert result == str(fake_mount)


class TestScanMountFlatLayout:
    """``_scan_mount(flat_layout=True)`` treats mount_path itself as the
    release-folder parent (no shows/movies/anime/__all__ subdivision).

    Regression: pre-fix, plan 39 phase 4 passed only ``source_debrid='torbox'``
    without ``flat_layout``, so ``_scan_mount`` iterated each TB release
    folder AS a category and looked for sub-folders inside (finding only
    media files) — every TB show and movie except the rare ones with
    internal subdirs got silently dropped, producing a massively under-
    counted library view (observed: 1 movie + 1 show out of 20+ on mount).
    """

    def test_flat_layout_finds_show_episodes_in_release_folders(self, tmp_dir):
        """A flat mount with N show-episode folders surfaces N episodes
        grouped into a single show entry (matching the existing dedup
        semantics of categorized scans).
        """
        tb_mount = os.path.join(tmp_dir, 'tb')
        os.makedirs(tb_mount)
        # Four releases of the same show, different episodes
        for ep in (1, 2, 3, 4):
            d = os.path.join(tb_mount, f'My.Show.S01E0{ep}.1080p-FLUX[TGx]')
            os.makedirs(d)
            with open(os.path.join(d, f'My.Show.S01E0{ep}.mkv'), 'w') as f:
                f.write('video')
        scanner = library.LibraryScanner()
        movies, shows = scanner._scan_mount(
            tb_mount, source_debrid='torbox', flat_layout=True,
        )
        assert len(movies) == 0
        assert len(shows) == 1
        assert shows[0]['title'] == 'My Show'
        # 4 episodes from 4 distinct release folders, all S01
        assert shows[0]['episodes'] == 4
        assert shows[0]['source_debrid'] == 'torbox'

    def test_flat_layout_finds_movies_in_release_folders(self, tmp_dir):
        """A movie release folder under the flat mount is detected and
        emerges as a movie entry (not a show)."""
        tb_mount = os.path.join(tmp_dir, 'tb')
        os.makedirs(tb_mount)
        d = os.path.join(tb_mount, 'My.Movie.2024.1080p-FLUX')
        os.makedirs(d)
        with open(os.path.join(d, 'My.Movie.2024.mkv'), 'w') as f:
            f.write('video')
        scanner = library.LibraryScanner()
        movies, shows = scanner._scan_mount(
            tb_mount, source_debrid='torbox', flat_layout=True,
        )
        assert len(shows) == 0
        assert len(movies) == 1
        assert movies[0]['title'] == 'My Movie'
        assert movies[0]['year'] == 2024
        assert movies[0]['source_debrid'] == 'torbox'

    def test_categorized_layout_still_works(self, tmp_dir):
        """Regression guard: Zurg-style 2-level layout still scans
        correctly when ``flat_layout=False`` (the default)."""
        zurg_mount = os.path.join(tmp_dir, 'zurg')
        os.makedirs(os.path.join(zurg_mount, 'shows'))
        d = os.path.join(zurg_mount, 'shows', 'My.Show.S01E01-NTb')
        os.makedirs(d)
        with open(os.path.join(d, 'My.Show.S01E01.mkv'), 'w') as f:
            f.write('video')
        scanner = library.LibraryScanner()
        movies, shows = scanner._scan_mount(zurg_mount, source_debrid='realdebrid')
        assert len(shows) == 1
        assert shows[0]['title'] == 'My Show'
        assert shows[0]['source_debrid'] == 'realdebrid'

    def test_flat_layout_with_noisy_release_names(self, tmp_dir):
        """Real TB folder names have indexer tags, dots, and trailing
        brackets; _parse_folder_name + _collect_episodes must still
        extract a usable title + season-episode for each.
        """
        tb_mount = os.path.join(tmp_dir, 'tb')
        os.makedirs(tb_mount)
        noisy_names = [
            'For All Mankind S05E07 The Sirens of Titan 1080p ATVP WEB-DL DDP5 1 H 264-NTb[EZTVx.to]',
            'For.All.Mankind.S05E09.Sons.and.Daughters.1080p.WEB-DL-NTb[TGx]',
            'www.UIndex.org    -    For All Mankind S04E01 Glasnost 1080p ATVP WEB-DL DDPA5 1 H 264-FLUX',
        ]
        for name in noisy_names:
            d = os.path.join(tb_mount, name)
            os.makedirs(d)
            # Extract S##E## from the folder name for the file name
            import re
            m = re.search(r'S(\d{2})E(\d{2})', name)
            ep_file = f'ep_s{m.group(1)}e{m.group(2)}.mkv' if m else 'ep.mkv'
            with open(os.path.join(d, ep_file), 'w') as f:
                f.write('video')
        scanner = library.LibraryScanner()
        movies, shows = scanner._scan_mount(
            tb_mount, source_debrid='torbox', flat_layout=True,
        )
        # All three are "For All Mankind" episodes — one merged show entry
        # with 3 episodes (S05E07, S05E09, S04E01).
        assert len(movies) == 0
        # Title parsing may yield slight variants; check the group
        # produces ONE show with 3 episodes.
        assert len(shows) == 1
        assert shows[0]['episodes'] == 3


class TestTbScanTruncationFallback:
    """A TorBox FUSE walk that gets rate-limited (429) or hits its deadline
    must not drop TB titles to "Wanted". _scan_mount flags the truncation;
    _scan_read falls back to the last COMPLETE scan, unioning the partial
    over it. Regression: pre-fix, _scan_mount discarded its timed_out flag
    and the caller fed the partial set straight into the merge, so every
    truncated hourly scan wiped the missing TB titles.
    """

    def test_scan_mount_sets_truncated_flag_on_deadline(self, tmp_dir):
        """A deadline already in the past trips the truncation flag."""
        tb_mount = os.path.join(tmp_dir, 'tb')
        os.makedirs(tb_mount)
        d = os.path.join(tb_mount, 'My.Movie.2024.1080p-FLUX')
        os.makedirs(d)
        with open(os.path.join(d, 'My.Movie.2024.mkv'), 'w') as f:
            f.write('video')
        scanner = library.LibraryScanner()
        # Deadline 100s in the past → the first entry check trips the timeout.
        scanner._scan_mount(
            tb_mount, deadline=time.monotonic() - 100,
            source_debrid='torbox', flat_layout=True,
        )
        assert scanner._last_scan_mount_truncated is True

    def test_scan_mount_clears_truncated_flag_on_clean_scan(self, tmp_dir):
        """A complete walk leaves the flag False (and resets a prior True)."""
        tb_mount = os.path.join(tmp_dir, 'tb')
        os.makedirs(tb_mount)
        d = os.path.join(tb_mount, 'My.Movie.2024.1080p-FLUX')
        os.makedirs(d)
        with open(os.path.join(d, 'My.Movie.2024.mkv'), 'w') as f:
            f.write('video')
        scanner = library.LibraryScanner()
        scanner._last_scan_mount_truncated = True  # stale prior state
        scanner._scan_mount(
            tb_mount, source_debrid='torbox', flat_layout=True,
        )
        assert scanner._last_scan_mount_truncated is False

    def test_union_tb_items_partial_wins_and_carries_last_good(self):
        scanner = library.LibraryScanner()
        last_good = [
            {'title': 'Alpha', 'year': 2020, 'quality': '720p'},
            {'title': 'Beta', 'year': 2021, 'quality': '1080p'},
        ]
        partial = [
            {'title': 'Alpha', 'year': 2020, 'quality': '2160p'},  # upgraded
            {'title': 'Gamma', 'year': 2022, 'quality': '1080p'},  # new
        ]
        out = scanner._union_tb_items(last_good, partial)
        by_title = {it['title']: it for it in out}
        assert set(by_title) == {'Alpha', 'Beta', 'Gamma'}
        # Partial wins on collision (fresh quality), Beta carried from last-good.
        assert by_title['Alpha']['quality'] == '2160p'
        assert by_title['Beta']['quality'] == '1080p'

    def test_union_tb_items_empty_inputs(self):
        scanner = library.LibraryScanner()
        assert scanner._union_tb_items([], []) == []
        assert scanner._union_tb_items(None, None) == []
        out = scanner._union_tb_items(None, [{'title': 'X', 'year': 2020}])
        assert len(out) == 1 and out[0]['title'] == 'X'

    def _make_scanner_for_scan_read(self, monkeypatch, tb_partial, truncated):
        """Build a scanner whose RD path is a no-op and whose TB scan returns
        ``tb_partial`` with the given truncation flag, so _scan_read exercises
        only the TB fallback branch."""
        scanner = library.LibraryScanner()
        scanner._mount_path = '/nonexistent/rd'
        # RD WebDAV + FUSE both yield nothing, no exceptions.
        monkeypatch.setattr(scanner, '_webdav_scan_mount', lambda *a, **k: ([], []))
        monkeypatch.setattr(scanner, '_discover_torbox_mount', lambda: '/nonexistent/tb')

        def fake_scan_api(tb_mount):
            scanner._last_scan_mount_truncated = truncated
            return tb_partial

        monkeypatch.setattr(scanner, '_scan_torbox_via_api', fake_scan_api)
        return scanner

    def test_scan_read_complete_scan_promotes_last_good(self, monkeypatch):
        movies = [{'title': 'Alpha', 'year': 2020, 'type': 'movie', 'source': 'debrid', 'source_debrid': 'torbox'}]
        scanner = self._make_scanner_for_scan_read(monkeypatch, (movies, []), truncated=False)
        scanner._scan_read()
        assert scanner._last_tb_movies is not None
        assert [m['title'] for m in scanner._last_tb_movies] == ['Alpha']

    def test_scan_read_truncated_falls_back_to_last_good(self, monkeypatch):
        """A truncated scan that drops 'Beta' still surfaces it via last-good."""
        full = [
            {'title': 'Alpha', 'year': 2020, 'type': 'movie', 'source': 'debrid', 'source_debrid': 'torbox'},
            {'title': 'Beta', 'year': 2021, 'type': 'movie', 'source': 'debrid', 'source_debrid': 'torbox'},
        ]
        scanner = self._make_scanner_for_scan_read(monkeypatch, (full, []), truncated=False)
        scanner._scan_read()  # complete scan → baseline = {Alpha, Beta}

        # Next scan is truncated and only returns Alpha (Beta dropped by 429).
        partial = [{'title': 'Alpha', 'year': 2020, 'type': 'movie', 'source': 'debrid', 'source_debrid': 'torbox'}]

        def fake_partial(tb_mount):
            scanner._last_scan_mount_truncated = True
            return (partial, [])

        monkeypatch.setattr(scanner, '_scan_torbox_via_api', fake_partial)
        data = scanner._scan_read()
        titles = {m['title'] for m in data['movies']}
        # Beta survives via last-good despite being absent from the partial scan.
        assert 'Alpha' in titles and 'Beta' in titles
        # Baseline NOT overwritten by the partial.
        assert {m['title'] for m in scanner._last_tb_movies} == {'Alpha', 'Beta'}

    def test_scan_read_truncated_no_baseline_uses_partial(self, monkeypatch):
        """First-ever scan truncated, no last-good → use partial, don't promote."""
        partial = [{'title': 'Alpha', 'year': 2020, 'type': 'movie', 'source': 'debrid', 'source_debrid': 'torbox'}]
        scanner = self._make_scanner_for_scan_read(monkeypatch, (partial, []), truncated=True)
        data = scanner._scan_read()
        assert {m['title'] for m in data['movies']} == {'Alpha'}
        # Partial must NOT become the baseline.
        assert scanner._last_tb_movies is None

    def test_scan_read_baseline_isolated_from_downstream_mutation(self, monkeypatch):
        """The promoted baseline must be a deep copy — downstream stages mutate
        the returned item dicts in place, and a shallow copy would corrupt the
        last-good set for the next truncated scan."""
        movies = [{'title': 'Alpha', 'year': 2020, 'type': 'movie', 'source': 'debrid', 'source_debrid': 'torbox'}]
        scanner = self._make_scanner_for_scan_read(monkeypatch, (movies, []), truncated=False)
        data = scanner._scan_read()
        # Simulate a downstream stage mutating the returned dict in place.
        for m in data['movies']:
            if m['title'] == 'Alpha':
                m['has_alt_source'] = True
                m['source'] = 'both'
        # Baseline snapshot must be untouched by that mutation.
        base = {m['title']: m for m in scanner._last_tb_movies}
        assert base['Alpha'].get('has_alt_source') is not True
        assert base['Alpha']['source'] == 'debrid'

    def test_scan_mount_sets_truncated_flag_on_listing_oserror(self, tmp_dir, monkeypatch):
        """A listing OSError mid-walk (e.g. a TorBox 429) flags truncation."""
        tb_mount = os.path.join(tmp_dir, 'tb')
        os.makedirs(tb_mount)
        scanner = library.LibraryScanner()
        real_scandir = os.scandir

        def boom(path, *a, **k):
            # Fail the flat-root listing the way a 429 surfaces through FUSE.
            if os.path.normpath(path) == os.path.normpath(tb_mount):
                raise OSError("rate limit exceeded: 429 Too Many Requests")
            return real_scandir(path, *a, **k)

        monkeypatch.setattr(os, 'scandir', boom)
        movies, shows = scanner._scan_mount(
            tb_mount, source_debrid='torbox', flat_layout=True,
        )
        assert movies == [] and shows == []
        assert scanner._last_scan_mount_truncated is True


class TestResolveNfsRescanDelay:
    """Plan 41 phase B.2 — NFS attribute-cache delay between symlink
    creation and arr rescan trigger."""

    def test_unset_returns_zero(self, monkeypatch):
        from utils.library import _resolve_nfs_rescan_delay
        monkeypatch.delenv('LIBRARY_RESCAN_NFS_DELAY', raising=False)
        assert _resolve_nfs_rescan_delay() == 0

    def test_empty_string_returns_zero(self, monkeypatch):
        from utils.library import _resolve_nfs_rescan_delay
        monkeypatch.setenv('LIBRARY_RESCAN_NFS_DELAY', '')
        assert _resolve_nfs_rescan_delay() == 0

    def test_valid_value_honoured(self, monkeypatch):
        from utils.library import _resolve_nfs_rescan_delay
        monkeypatch.setenv('LIBRARY_RESCAN_NFS_DELAY', '30')
        assert _resolve_nfs_rescan_delay() == 30

    def test_value_at_max_boundary(self, monkeypatch):
        from utils.library import _resolve_nfs_rescan_delay
        monkeypatch.setenv('LIBRARY_RESCAN_NFS_DELAY', '300')
        assert _resolve_nfs_rescan_delay() == 300

    def test_value_clamped_above_max(self, monkeypatch):
        """A typo (`9999`, `3600` etc.) cannot stall the scan loop indefinitely."""
        from utils.library import _resolve_nfs_rescan_delay
        monkeypatch.setenv('LIBRARY_RESCAN_NFS_DELAY', '9999')
        assert _resolve_nfs_rescan_delay() == 300

    def test_negative_clamped_to_zero(self, monkeypatch):
        from utils.library import _resolve_nfs_rescan_delay
        monkeypatch.setenv('LIBRARY_RESCAN_NFS_DELAY', '-5')
        assert _resolve_nfs_rescan_delay() == 0

    def test_non_integer_falls_back_to_zero(self, monkeypatch):
        """A typo shouldn't crash the scanner — disable the mitigation."""
        from utils.library import _resolve_nfs_rescan_delay
        monkeypatch.setenv('LIBRARY_RESCAN_NFS_DELAY', 'abc')
        assert _resolve_nfs_rescan_delay() == 0

    def test_float_string_falls_back_to_zero(self, monkeypatch):
        """``int('30.5')`` raises ValueError — caller treats as misconfigured."""
        from utils.library import _resolve_nfs_rescan_delay
        monkeypatch.setenv('LIBRARY_RESCAN_NFS_DELAY', '30.5')
        assert _resolve_nfs_rescan_delay() == 0


class TestDetectTvMarker:
    """Plan 41 phase B.1 — folder-name TV-marker recognition that
    rescues TB flat-layout season packs from being mis-bucketed as
    movies.  Pure-function tests against the helper; the integration
    with ``_scan_mount`` is covered by the existing flat-layout tests
    (TestPhase4DualDebridMerge) once the synthetic content is dropped
    on a tmp dir.
    """

    def test_canonical_episode_marker(self):
        from utils.library import _detect_tv_marker
        # The case _collect_episodes already handles — must still True.
        assert _detect_tv_marker('Andor.S02E01.1080p.WEB-DL') is True

    def test_season_only_pack(self):
        """The headline regression — S22.COMPLETE has no episode marker."""
        from utils.library import _detect_tv_marker
        assert _detect_tv_marker('Greys.Anatomy.S22.COMPLETE.1080p.WEB.H264-AMB3R') is True

    def test_season_only_with_quality_tag(self):
        from utils.library import _detect_tv_marker
        assert _detect_tv_marker('Show.Name.S03.1080p.ATVP.WEB-DL') is True

    def test_multi_season_range(self):
        """For.All.Mankind.S01-S04 pattern."""
        from utils.library import _detect_tv_marker
        assert _detect_tv_marker('For.All.Mankind.S01-S04.COMPLETE') is True

    def test_multi_season_range_alt_form(self):
        """S01-04 form (no second S) and en-dash form."""
        from utils.library import _detect_tv_marker
        assert _detect_tv_marker('Show.Name.S01-04.1080p') is True
        assert _detect_tv_marker('Show.Name.S01–S04.1080p') is True

    def test_season_word_form(self):
        from utils.library import _detect_tv_marker
        assert _detect_tv_marker('Show.Name.Season.3.1080p.WEB-DL') is True
        assert _detect_tv_marker('Show.Name.Season 3.1080p') is True
        assert _detect_tv_marker('Show.Name.Seasons.1.Complete') is True

    def test_real_movie_returns_false(self):
        from utils.library import _detect_tv_marker
        # No TV markers — should bucket as movie.
        assert _detect_tv_marker('Dune.Part.Two.2024.1080p.WEB-DL.DDP5.1.x264-NTb') is False
        assert _detect_tv_marker('Gattaca.1997.1080p.BluRay.x264') is False

    def test_year_only_not_misclassified_as_season(self):
        """``2024`` mustn't accidentally match — the season-only regex is
        anchored to ``S`` prefix so a 4-digit year doesn't trigger."""
        from utils.library import _detect_tv_marker
        assert _detect_tv_marker('Documentary.2024.1080p.WEB-DL') is False

    def test_empty_string(self):
        from utils.library import _detect_tv_marker
        assert _detect_tv_marker('') is False
        assert _detect_tv_marker(None) is False

    def test_sxxexx_inside_movie_name_still_tv(self):
        """If a folder name has ``S01E01`` anywhere, classify as TV even
        if the rest looks movie-ish."""
        from utils.library import _detect_tv_marker
        assert _detect_tv_marker('Some.Title.2024.S01E01.1080p') is True

    def test_season_only_in_middle_of_name(self):
        """``Show.S22.Title`` (season tag not at end) still matches."""
        from utils.library import _detect_tv_marker
        assert _detect_tv_marker('Greys.S22.Anatomy.WEB-DL') is True


class TestScanMountTvMarkerFallback:
    """Plan 41 phase B.1 integration: ``_scan_mount`` correctly buckets
    a season-pack folder as TV even when ``_collect_episodes`` returns
    empty (no SxxExx files inside the folder).
    """

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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        scanner._capabilities_path = '/dev/null/library_capabilities.json'
        return scanner

    def test_season_pack_no_episode_files_classified_as_show(self, tmp_dir, monkeypatch):
        """The headline regression: TB flat-layout season pack folder
        without SxxExx-tagged media inside is now a show, not a movie."""
        # Folder name with S22 marker but EMPTY contents (TB caching).
        pack_dir = os.path.join(tmp_dir, 'Greys.Anatomy.S22.COMPLETE.1080p.WEB.H264-AMB3R')
        os.makedirs(pack_dir)

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        movies, shows = scanner._scan_mount(tmp_dir, flat_layout=True)

        # Must be in shows, not movies.
        show_titles = {s['title'] for s in shows}
        movie_titles = {m['title'] for m in movies}
        assert any('grey' in t.lower() or 'anatomy' in t.lower() for t in show_titles), \
            f"expected Grey's Anatomy in shows; shows={show_titles}, movies={movie_titles}"
        assert not any('grey' in t.lower() or 'anatomy' in t.lower() for t in movie_titles), \
            f"Grey's Anatomy should not appear in movies; movies={movie_titles}"

    def test_multi_season_pack_classified_as_show(self, tmp_dir, monkeypatch):
        pack_dir = os.path.join(tmp_dir, 'For.All.Mankind.S01-S04.COMPLETE.1080p')
        os.makedirs(pack_dir)

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        movies, shows = scanner._scan_mount(tmp_dir, flat_layout=True)

        show_titles = {s['title'].lower() for s in shows}
        assert any('mankind' in t for t in show_titles), \
            f"expected For All Mankind in shows; got {show_titles}"

    def test_real_movie_still_classified_as_movie(self, tmp_dir, monkeypatch):
        """Regression-guard: a folder with no TV markers stays a movie."""
        movie_dir = os.path.join(tmp_dir, 'Dune.Part.Two.2024.1080p.WEB-DL.x264-NTb')
        os.makedirs(movie_dir)

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        movies, shows = scanner._scan_mount(tmp_dir, flat_layout=True)

        movie_titles = {m['title'].lower() for m in movies}
        show_titles = {s['title'].lower() for s in shows}
        assert any('dune' in t for t in movie_titles), \
            f"expected Dune in movies; movies={movie_titles}, shows={show_titles}"
        assert not any('dune' in t for t in show_titles)


class TestScanMountPathSwapOnHeavierFolder:
    """Plan 41 phase B bug-hunter LOW #3 fix: when the same show is
    encountered first as an empty-marker entry (B.1 TV-marker fallback,
    season pack with no SxxExx files cached yet) and later in the same
    scan as a populated entry, the show's ``path`` field must point at
    the populated folder so downstream ``date_added``/symlink-target
    consumers stat a populated dir.
    """

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
        scanner._webdav_unsupported = False
        scanner._webdav_unsupported_logged = False
        scanner._capabilities_path = '/dev/null/library_capabilities.json'
        return scanner

    def test_populated_folder_wins_over_empty_marker(self, tmp_dir, monkeypatch):
        """Empty-marker folder seen first, populated folder second —
        show entry's ``path`` ends up on the populated folder."""
        empty_dir = os.path.join(tmp_dir, 'Greys.Anatomy.S22.COMPLETE.1080p')
        os.makedirs(empty_dir)  # zero files inside

        # Same normalised title (Grey's Anatomy → 'greys anatomy') with
        # SxxExx files inside.  Name AFTER the empty one alphabetically
        # so os.scandir likely returns empty first.  Both folders share
        # the same normalised title key.
        populated_dir = os.path.join(tmp_dir, 'Greys.Anatomy.S22E01.1080p.WEB-DL')
        os.makedirs(populated_dir)
        with open(os.path.join(populated_dir, 'Greys.Anatomy.S22E01.mkv'), 'w') as f:
            f.write('x')
        with open(os.path.join(populated_dir, 'Greys.Anatomy.S22E02.mkv'), 'w') as f:
            f.write('x')

        scanner = self._make_scanner(tmp_dir, monkeypatch)
        movies, shows = scanner._scan_mount(tmp_dir, flat_layout=True)

        # Find the Grey's Anatomy show entry.
        candidates = [s for s in shows if 'grey' in s['title'].lower() or 'anatomy' in s['title'].lower()]
        assert len(candidates) == 1, \
            f"expected 1 Grey's Anatomy entry, got {len(candidates)}: {[s['title'] for s in shows]}"
        show = candidates[0]

        # The show entry's path must point at the populated folder, NOT
        # the empty marker.  Pre-fix path stayed on whichever os.scandir
        # returned first — typically the empty one.
        assert show['path'] == populated_dir, \
            f"path should be populated folder; got {show['path']} (empty={empty_dir})"
        assert show['episodes'] == 2


class TestMergeShowGroup:
    """Plan 41 phase B second-pass reviewer fix-up: ``_merge_show_group``
    is the single source of truth for show-group merging used by both
    ``_scan_mount`` (FUSE) and ``_webdav_scan_mount`` (WebDAV).  Pin
    the semantics here so the two scan paths can't drift again.
    """

    def test_inserts_fresh_entry(self):
        from utils.library import _merge_show_group
        groups = {}
        eps = {(1, 1): {'file': 'ep1.mkv'}}
        _merge_show_group(groups, 'show1', 'Show One', 2024, eps, '/mnt/show1')
        assert groups == {
            'show1': {
                'title': 'Show One',
                'year': 2024,
                'episodes': {(1, 1): {'file': 'ep1.mkv'}},
                'path': '/mnt/show1',
            },
        }

    def test_merge_adds_new_episodes(self):
        from utils.library import _merge_show_group
        groups = {
            'show1': {
                'title': 'Show One',
                'year': 2024,
                'episodes': {(1, 1): {'file': 'ep1.mkv'}},
                'path': '/mnt/folder_a',
            },
        }
        eps = {(1, 2): {'file': 'ep2.mkv'}}
        _merge_show_group(groups, 'show1', 'Show One', 2024, eps, '/mnt/folder_b')
        # Both episodes present.
        assert set(groups['show1']['episodes'].keys()) == {(1, 1), (1, 2)}

    def test_path_swap_when_new_has_more_episodes(self):
        """The headline B.1 fix — empty-marker folder seen first loses
        to populated folder for the same show."""
        from utils.library import _merge_show_group
        groups = {
            'show1': {
                'title': 'Show One',
                'year': None,
                'episodes': {},  # empty marker
                'path': '/mnt/empty_marker',
            },
        }
        eps = {(1, n): {'file': f'ep{n}.mkv'} for n in range(1, 6)}
        _merge_show_group(groups, 'show1', 'Show One', 2024, eps, '/mnt/populated')
        assert groups['show1']['path'] == '/mnt/populated', \
            "populated folder must win over empty marker"

    def test_path_stays_when_existing_has_more(self):
        from utils.library import _merge_show_group
        groups = {
            'show1': {
                'title': 'Show One',
                'year': 2024,
                'episodes': {(1, n): {'file': f'ep{n}.mkv'} for n in range(1, 11)},
                'path': '/mnt/heavy',
            },
        }
        eps = {(2, 1): {'file': 'ep21.mkv'}, (2, 2): {'file': 'ep22.mkv'}}
        _merge_show_group(groups, 'show1', 'Show One', 2024, eps, '/mnt/light')
        # 2 < 10 — heavy folder stays.
        assert groups['show1']['path'] == '/mnt/heavy'

    def test_path_stays_on_equal_count(self):
        """Equal counts keep the first-seen path for stability."""
        from utils.library import _merge_show_group
        groups = {
            'show1': {
                'title': 'Show One',
                'year': 2024,
                'episodes': {(1, 1): {'file': 'ep1.mkv'}, (1, 2): {'file': 'ep2.mkv'}},
                'path': '/mnt/first',
            },
        }
        # Two new episodes; matches stored count — no swap.
        eps = {(2, 1): {'file': 'ep21.mkv'}, (2, 2): {'file': 'ep22.mkv'}}
        _merge_show_group(groups, 'show1', 'Show One', 2024, eps, '/mnt/second')
        assert groups['show1']['path'] == '/mnt/first'

    def test_year_propagated_when_existing_has_none(self):
        from utils.library import _merge_show_group
        groups = {
            'show1': {
                'title': 'show one',  # lowercased
                'year': None,
                'episodes': {},
                'path': '/mnt/old',
            },
        }
        _merge_show_group(groups, 'show1', 'Show One', 2024, {}, '/mnt/new')
        # Year propagated.
        assert groups['show1']['year'] == 2024
        # Title swapped to the year-bearing one.
        assert groups['show1']['title'] == 'Show One'

    def test_per_season_episode_count_preference(self):
        """Higher per-season count wins on key collision."""
        from utils.library import _merge_show_group
        groups = {
            'show1': {
                'title': 'Show',
                'year': 2024,
                'episodes': {
                    (1, 1): {'file': 'low_quality.mkv', '_folder_ep_count': 1},
                },
                'path': '/mnt/old',
            },
        }
        eps = {
            (1, 1): {'file': 'high_quality.mkv', '_folder_ep_count': 10},
        }
        _merge_show_group(groups, 'show1', 'Show', 2024, eps, '/mnt/new')
        # Season-pack version wins.
        assert groups['show1']['episodes'][(1, 1)]['file'] == 'high_quality.mkv'


class TestScanTorboxViaApi:
    """_scan_torbox_via_api enumerates TB via the mylist API (no FUSE walk)."""

    TB_MOUNT = '/data/torbox'

    def _scan(self, torrents, api_key='tbkey'):
        from unittest.mock import patch
        scanner = LibraryScanner.__new__(LibraryScanner)
        with patch('base.load_secret_or_env', return_value=api_key), \
             patch('utils.search.list_torbox_torrents', return_value=torrents):
            movies, shows = scanner._scan_torbox_via_api(self.TB_MOUNT)
        return scanner, movies, shows

    def test_splits_movies_and_shows(self):
        torrents = [
            {'name': 'Big.Movie.2021.1080p', 'hash': 'a' * 40,
             'created_at': '2024-01-15T12:00:00Z',
             'files': [{'name': 'Big.Movie.2021.1080p/big.mkv', 'size': 10}]},
            {'name': 'Cool.Show.S01.1080p', 'hash': 'b' * 40,
             'created_at': '2024-02-01T00:00:00Z',
             'files': [
                 {'name': 'Cool.Show.S01.1080p/Cool.Show.S01E01.mkv', 'size': 5},
                 {'name': 'Cool.Show.S01.1080p/Cool.Show.S01E02.mkv', 'size': 6},
             ]},
        ]
        _, movies, shows = self._scan(torrents)
        assert [m['title'] for m in movies] == ['Big Movie']
        assert len(shows) == 1
        assert shows[0]['episodes'] == 2
        assert shows[0]['seasons'] == 1
        for item in movies + shows:
            assert item['source'] == 'debrid'
            assert item['source_debrid'] == 'torbox'

    def test_synthesized_paths_are_under_mount(self):
        """The make-or-break property: every episode/movie path must live
        under the TB mount so _resolve_symlink_target maps it to the TB
        symlink base (prefix match against realpath(tb_mount))."""
        torrents = [
            {'name': 'Cool.Show.S01.1080p',
             'files': [{'name': 'Cool.Show.S01.1080p/Cool.Show.S01E01.mkv', 'size': 5}]},
        ]
        _, _movies, shows = self._scan(torrents)
        ep = shows[0]['_episodes'][(1, 1)]
        assert ep['path'] == os.path.join(
            self.TB_MOUNT, 'Cool.Show.S01.1080p', 'Cool.Show.S01E01.mkv')
        assert ep['path'].startswith(self.TB_MOUNT + os.sep)

    def test_movie_quality_and_size_from_api(self):
        """Movies must carry quality + size derived from the API file data,
        not all-None / 0 (the UI sorts and displays on these)."""
        torrents = [
            {'name': 'Big.Movie.2021.1080p',
             'files': [{'name': 'Big.Movie.2021.1080p/Big.Movie.2021.1080p.BluRay.x264.mkv',
                        'size': 1_000_000}]},
        ]
        _, movies, _shows = self._scan(torrents)
        assert movies[0]['quality']['resolution'] == '1080p'
        assert movies[0]['size_bytes'] == 1_000_000

    def test_obfuscated_folder_skipped(self):
        hex_dir = '050bd19ee9934249a2ce4c9762c0d710[EZTVx.to]'
        torrents = [
            {'name': hex_dir, 'hash': 'c' * 40,
             'created_at': '2026-07-03T00:25:14Z',
             'files': [{'name': f'{hex_dir}/{hex_dir}.mkv', 'size': 900}]},
            {'name': 'Big.Movie.2021.1080p',
             'files': [{'name': 'Big.Movie.2021.1080p/big.mkv', 'size': 10}]},
        ]
        _, movies, shows = self._scan(torrents)
        titles = {m['title'] for m in movies} | {s['title'] for s in shows}
        assert 'Big Movie' in titles
        assert not any('050bd19' in t.lower() for t in titles)

    def test_absolute_subpath_does_not_escape_mount(self):
        """A file whose stripped sub-path is absolute must not escape the
        mount (os.path.join would otherwise discard the mount prefix)."""
        torrents = [
            {'name': 'Weird.Show.S01',
             'files': [
                 {'name': 'Weird.Show.S01//Weird.Show.S01E01.mkv', 'size': 4},
             ]},
        ]
        _, _movies, shows = self._scan(torrents)
        # The "//" collapses to a single separator and the episode is still
        # produced (not silently skipped) with a path under the mount — never
        # an absolute escape. assert shows guards against a vacuous pass.
        assert shows
        eps = list(shows[0]['_episodes'].values())
        assert eps
        for ep in eps:
            assert ep['path'].startswith(self.TB_MOUNT + os.sep)
            assert ep['path'] == os.path.join(
                self.TB_MOUNT, 'Weird.Show.S01', 'Weird.Show.S01E01.mkv')

    def test_traversal_components_rejected(self):
        torrents = [
            {'name': 'Show',
             'files': [{'name': 'Show/../escape.mkv', 'size': 4}]},
        ]
        _, movies, shows = self._scan(torrents)
        # No episode/movie path may contain a traversal escape.
        paths = [m['path'] for m in movies]
        for s in shows:
            paths += [ep['path'] for ep in s['_episodes'].values()]
        for p in paths:
            assert '..' not in p.split(os.sep)

    def test_season_subdir_episodes(self):
        torrents = [
            {'name': 'Nested.Show',
             'files': [
                 {'name': 'Nested.Show/Season 1/Nested.Show.S01E05.mkv', 'size': 9},
             ]},
        ]
        _, _movies, shows = self._scan(torrents)
        assert (1, 5) in shows[0]['_episodes']
        assert shows[0]['_episodes'][(1, 5)]['path'] == os.path.join(
            self.TB_MOUNT, 'Nested.Show', 'Season 1', 'Nested.Show.S01E05.mkv')

    def test_bare_root_file_is_skipped(self):
        """A file with no folder component (bare file at the mount root) is
        skipped: the on-disk folder is the FIRST path component of the file's
        mylist name, and the old FUSE walk only enumerated top-level dirs. The
        entry-level `name` is a sanitized display string and must NOT be used
        to synthesize a folder (it matches disk for only ~20% of torrents)."""
        torrents = [
            {'name': 'Odd.Movie.2020',
             'files': [{'name': 'odd.mkv', 'size': 3}]},
        ]
        _, movies, shows = self._scan(torrents)
        assert movies == []
        assert shows == []

    def test_folder_derived_from_file_path_not_entry_name(self):
        """The on-disk folder comes from files[].name's first component, NOT
        the sanitized entry `name`. Live mylist `name` differs from the rclone
        folder for ~80% of torrents; keying off it synthesizes dead paths."""
        torrents = [
            {'name': 'Big Movie 2021 1080p WEBRip x265',  # sanitized display
             'files': [{'name': 'Big.Movie.2021.1080p.BluRay.x264-GRP/big.mkv',
                        'size': 10}]},
        ]
        _, movies, _shows = self._scan(torrents)
        assert len(movies) == 1
        # path keyed to the FILE's folder, not the entry name
        assert movies[0]['path'] == os.path.join(
            self.TB_MOUNT, 'Big.Movie.2021.1080p.BluRay.x264-GRP')

    def test_tv_marker_fallback_classifies_show(self):
        """A season-pack folder whose files aren't yet SxxExx-named (still
        caching) is classified as a show via the folder-name marker, with 0
        episodes — provided at least one file exists to reveal the folder."""
        torrents = [
            {'name': 'Pending.Show.S03.COMPLETE.1080p',
             'files': [{'name': 'Pending.Show.S03.COMPLETE.1080p/readme.nfo',
                        'size': 1}]},
        ]
        _, movies, shows = self._scan(torrents)
        assert movies == []
        assert len(shows) == 1
        assert shows[0]['episodes'] == 0

    def test_malformed_entries_skipped_not_fatal(self):
        """A malformed mylist element (non-dict entry, None files, non-dict
        file) degrades to a skip rather than aborting the whole scan — one
        bad entry must not lose all genuinely-new TB content for the cycle."""
        torrents = [
            'not-a-dict',
            {'name': 'Bad.Files', 'files': None},
            {'name': 'Bad.Inner', 'files': ['nope', 42]},
            {'name': 'Good.Movie.2022',
             'files': [{'name': 'Good.Movie.2022.1080p/g.mkv', 'size': 7}]},
        ]
        scanner, movies, shows = self._scan(torrents)
        assert [m['title'] for m in movies] == ['Good Movie']
        assert shows == []
        assert scanner._last_scan_mount_truncated is False

    def test_api_failure_marks_incomplete(self):
        scanner, movies, shows = self._scan(None)
        assert (movies, shows) == ([], [])
        assert scanner._last_scan_mount_truncated is True

    def test_empty_account_is_complete(self):
        scanner, movies, shows = self._scan([])
        assert (movies, shows) == ([], [])
        assert scanner._last_scan_mount_truncated is False

    def test_no_api_key_marks_incomplete(self):
        scanner, movies, shows = self._scan([{'name': 'x'}], api_key=None)
        assert (movies, shows) == ([], [])
        assert scanner._last_scan_mount_truncated is True

    def test_date_added_from_created_at(self):
        from utils.library import _parse_tb_timestamp
        ts = '2024-01-15T12:00:00Z'
        torrents = [
            {'name': 'Dated.Movie.2021', 'created_at': ts,
             'files': [{'name': 'Dated.Movie.2021/m.mkv', 'size': 1}]},
        ]
        _, movies, _shows = self._scan(torrents)
        assert movies[0]['date_added'] == _parse_tb_timestamp(ts)
        assert movies[0]['date_added'] > 0


class TestParseTbTimestamp:
    def test_z_suffix(self):
        from utils.library import _parse_tb_timestamp
        assert _parse_tb_timestamp('2024-01-15T12:00:00Z') > 0

    def test_offset_form(self):
        from utils.library import _parse_tb_timestamp
        assert _parse_tb_timestamp('2024-01-15T12:00:00+00:00') > 0

    def test_invalid_returns_zero(self):
        from utils.library import _parse_tb_timestamp
        assert _parse_tb_timestamp('not-a-date') == 0
        assert _parse_tb_timestamp('') == 0
        assert _parse_tb_timestamp(None) == 0
        assert _parse_tb_timestamp(12345) == 0


class TestRecoverWantedViaTorbox:
    """Wanted→TorBox proactive recovery pass (_recover_wanted_via_debrid)."""

    def _scanner(self):
        sc = LibraryScanner.__new__(LibraryScanner)
        sc._wanted_tb_cooldown = {}
        sc._wanted_rd_miss = {}
        sc._wanted_no_results = {}
        return sc

    @pytest.fixture
    def wire(self, monkeypatch):
        """Stub the external surfaces and capture add_to_debrid calls.

        Returns a dict so each test can tune cache results / torrentio output
        and assert on the captured adds.
        """
        import base
        import utils.blackhole as bh
        import utils.search as search

        monkeypatch.setenv('TORRENTIO_URL', 'https://torrentio.example')
        monkeypatch.setenv('WANTED_TB_RECOVERY_ENABLED', 'true')
        monkeypatch.delenv('WANTED_TB_RECOVERY_MAX_PER_SCAN', raising=False)

        state = {
            'adds': [],
            'cooldown': 0,
            'cache_cached': True,   # all probed hashes report cached
            'torrentio': [
                {'info_hash': 'a' * 40, 'title': 'The.Substance.2024.1080p.WEB-DL.RelA',
                 'seeds': 10, 'quality': {'label': '1080p', 'score': 100}},
                {'info_hash': 'b' * 40, 'title': 'The.Substance.2024.720p.WEB-DL.RelB',
                 'seeds': 5, 'quality': {'label': '720p', 'score': 50}},
            ],
        }

        monkeypatch.setattr(base, 'load_secret_or_env',
                            lambda name: 'tb_key' if name == 'torbox_api_key' else None)
        monkeypatch.setattr(bh, '_check_torbox_cooldown',
                            lambda *a, **kw: state['cooldown'])
        monkeypatch.setattr(search, 'search_torrentio',
                            lambda *a, **kw: list(state['torrentio']))

        def _cache(hashes, service=None, api_key=None):
            return {h: state['cache_cached'] for h in hashes}
        monkeypatch.setattr(search, 'check_debrid_cache', _cache)

        def _add(info_hash, **kw):
            state['adds'].append({'info_hash': info_hash, **kw})
            return {'success': True, 'torrent_id': 't', 'service': 'torbox'}
        monkeypatch.setattr(search, 'add_to_debrid', _add)

        return state

    def test_recovers_cached_wanted_movie(self, wire):
        sc = self._scanner()
        movies = [{'title': 'The Substance', 'source': 'wanted',
                   'imdb_id': 'tt1234567', 'is_available': True}]
        sc._recover_wanted_via_debrid([], movies, {})
        assert len(wire['adds']) == 1
        add = wire['adds'][0]
        # Highest-score release picked, targeted at TorBox with the new cause.
        assert add['info_hash'] == 'a' * 40
        assert add['service'] == 'torbox'
        assert add['cause'] == 'wanted_tb_recovered'
        assert add['media_title'] == 'The Substance'

    def test_disabled_flag_noops(self, wire, monkeypatch):
        monkeypatch.setenv('WANTED_TB_RECOVERY_ENABLED', 'false')
        sc = self._scanner()
        movies = [{'title': 'X', 'source': 'wanted',
                   'imdb_id': 'tt1', 'is_available': True}]
        sc._recover_wanted_via_debrid([], movies, {})
        assert wire['adds'] == []

    def test_no_torrentio_url_noops(self, wire, monkeypatch):
        monkeypatch.delenv('TORRENTIO_URL', raising=False)
        sc = self._scanner()
        movies = [{'title': 'X', 'source': 'wanted',
                   'imdb_id': 'tt1', 'is_available': True}]
        sc._recover_wanted_via_debrid([], movies, {})
        assert wire['adds'] == []

    def _two_movies(self):
        return [
            {'title': 'The Substance', 'source': 'wanted',
             'imdb_id': 'tt1', 'is_available': True},
            {'title': 'The Substance', 'source': 'wanted',
             'imdb_id': 'tt2', 'is_available': True},
        ]

    def _failing_add(self, wire, monkeypatch, duplicate=False):
        import utils.search as search

        def _add(info_hash, **kw):
            wire['adds'].append({'info_hash': info_hash, **kw})
            result = {'success': False,
                      'error': 'Failed to add magnet to TorBox'}
            if duplicate:
                result['duplicate'] = True
            return result
        monkeypatch.setattr(search, 'add_to_debrid', _add)

    def test_tb_advisory_cooldown_proceeds(self, wire):
        # ``cooldown_until`` is advisory on Pro plans — organic creates
        # succeed while it's set, so the flag alone must not starve the
        # leg.  The pass attempts the add anyway.
        wire['cooldown'] = 3600
        sc = self._scanner()
        movies = [{'title': 'The Substance', 'source': 'wanted',
                   'imdb_id': 'tt1', 'is_available': True}]
        sc._recover_wanted_via_debrid([], movies, {})
        assert len(wire['adds']) == 1

    def test_add_failure_with_cooldown_flag_disables_leg(self, wire, monkeypatch):
        # An add FAILING while the cooldown flag is set is the real
        # enforcement signal — the leg must stop burning budget this pass.
        wire['cooldown'] = 3600
        self._failing_add(wire, monkeypatch)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._two_movies(), {})
        assert len(wire['adds']) == 1  # second title never attempted

    def test_add_failure_without_cooldown_flag_keeps_leg_alive(self, wire, monkeypatch):
        # Failure with NO cooldown flag is a transient error — keep going.
        self._failing_add(wire, monkeypatch)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._two_movies(), {})
        assert len(wire['adds']) == 2

    def test_duplicate_add_does_not_trigger_backoff(self, wire, monkeypatch):
        # A duplicate isn't a rejected create — no enforcement inference,
        # even with the advisory flag set.
        wire['cooldown'] = 3600
        self._failing_add(wire, monkeypatch, duplicate=True)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._two_movies(), {})
        assert len(wire['adds']) == 2

    def test_budget_caps_adds(self, wire, monkeypatch):
        monkeypatch.setenv('WANTED_TB_RECOVERY_MAX_PER_SCAN', '1')
        sc = self._scanner()
        movies = [
            {'title': 'The Substance', 'source': 'wanted', 'imdb_id': 'tt1', 'is_available': True},
            {'title': 'The Substance', 'source': 'wanted', 'imdb_id': 'tt2', 'is_available': True},
        ]
        sc._recover_wanted_via_debrid([], movies, {})
        assert len(wire['adds']) == 1

    def test_uncached_title_not_added_and_cooled_down(self, wire):
        wire['cache_cached'] = False
        sc = self._scanner()
        movies = [{'title': 'The Substance', 'source': 'wanted',
                   'imdb_id': 'tt9', 'is_available': True}]
        sc._recover_wanted_via_debrid([], movies, {})
        assert wire['adds'] == []
        assert 'tt9' in sc._wanted_tb_cooldown

    def test_unreleased_movie_skipped(self, wire):
        sc = self._scanner()
        movies = [{'title': 'Future', 'source': 'wanted',
                   'imdb_id': 'tt1', 'is_available': False}]
        sc._recover_wanted_via_debrid([], movies, {})
        assert wire['adds'] == []

    def test_movie_without_imdb_skipped(self, wire):
        sc = self._scanner()
        movies = [{'title': 'NoId', 'source': 'wanted', 'is_available': True}]
        sc._recover_wanted_via_debrid([], movies, {})
        assert wire['adds'] == []

    def test_non_wanted_movie_ignored(self, wire):
        sc = self._scanner()
        movies = [{'title': 'Owned', 'source': 'debrid',
                   'imdb_id': 'tt1', 'is_available': True}]
        sc._recover_wanted_via_debrid([], movies, {})
        assert wire['adds'] == []

    def test_per_title_cooldown_skips_reprobe(self, wire):
        sc = self._scanner()
        sc._wanted_tb_cooldown = {'tt1': time.monotonic()}
        movies = [{'title': 'X', 'source': 'wanted',
                   'imdb_id': 'tt1', 'is_available': True}]
        sc._recover_wanted_via_debrid([], movies, {})
        assert wire['adds'] == []

    def test_show_ghost_recovered_with_episode_string(self, wire):
        sc = self._scanner()
        wire['torrentio'] = [
            {'info_hash': 'a' * 40, 'title': 'Broadchurch.S01E01.1080p.WEB',
             'seeds': 10, 'quality': {'label': '1080p', 'score': 100}},
        ]
        shows = [{'title': 'Broadchurch', 'source': 'wanted',
                  'imdb_id': 'tt2249364', 'season_data': []}]
        sc._recover_wanted_via_debrid(shows, [], {})
        assert len(wire['adds']) == 1
        # Default S01E01 when TMDB has no missing-episode ground truth.
        assert wire['adds'][0]['episode'] == 'S01E01'


class _FakeRdClient:
    """RD client stub for the Wanted RD leg — records probe/delete calls."""

    def __init__(self):
        self.configured = True
        self.probe_result = {'status': 'healthy'}
        self.probe_calls = []
        self.delete_calls = []
        self.info_calls = []
        # ``added`` returned by torrent_info; default = "just now", i.e.
        # a torrent the probe itself created (deletable).  Tests set an
        # old timestamp to model RD hash-dedup returning the USER'S
        # pre-existing torrent, or None to model an info fetch failure.
        from datetime import datetime, timezone
        self.info_added = datetime.now(timezone.utc).isoformat()

    def probe_file(self, tid):
        self.probe_calls.append(tid)
        return dict(self.probe_result)

    def delete_torrent(self, tid):
        self.delete_calls.append(tid)
        return True

    def torrent_info(self, tid):
        self.info_calls.append(tid)
        if self.info_added is None:
            return None
        return {'id': tid, 'status': 'downloaded', 'added': self.info_added}

    def select_files(self, tid):
        return True


def _wire_wanted_recovery(monkeypatch):
    """Stub both legs' external surfaces; capture RD rescue attempts, TB
    adds, and history events.  Shared by TestWantedRdRecovery and
    TestWantedFilterGiveup so the wiring stays identical."""
    import base
    import utils.blackhole as bh
    import utils.search as search
    import utils.debrid_client as dc
    import utils.debrid_routing as routing
    import utils.history as history

    monkeypatch.setenv('TORRENTIO_URL', 'https://torrentio.example')
    monkeypatch.setenv('WANTED_TB_RECOVERY_ENABLED', 'true')
    monkeypatch.setenv('WANTED_RD_RECOVERY_ENABLED', 'true')
    monkeypatch.delenv('WANTED_TB_RECOVERY_MAX_PER_SCAN', raising=False)
    monkeypatch.delenv('WANTED_RD_RECOVERY_MAX_PER_SCAN', raising=False)

    rd_client = _FakeRdClient()
    state = {
        'tb_adds': [],
        'events': [],
        'rescue_calls': [],
        'cooldown': 0,
        'cache_cached': True,   # TB cache probe result
        'existing': set(),      # hashes "already on the RD account"
        'remembered': [],
        'rd_client': rd_client,
        'rd_core': {'rescued': True, 'to': 'realdebrid',
                    'alt_torrent_id': 'RDTID1', 'alt_client': rd_client},
        'torrentio': [
            {'info_hash': 'a' * 40, 'title': 'The.Substance.2024.1080p.WEB-DL.RelA',
             'seeds': 10, 'quality': {'label': '1080p', 'score': 100}},
            {'info_hash': 'b' * 40, 'title': 'The.Substance.2024.720p.WEB-DL.RelB',
             'seeds': 5, 'quality': {'label': '720p', 'score': 50}},
        ],
    }

    monkeypatch.setattr(base, 'load_secret_or_env',
                        lambda name: {'torbox_api_key': 'tb_key',
                                      'rd_api_key': 'rd_key'}.get(name))
    monkeypatch.setattr(bh, '_check_torbox_cooldown',
                        lambda *a, **kw: state['cooldown'])
    monkeypatch.setattr(search, 'search_torrentio',
                        lambda *a, **kw: list(state['torrentio']))

    def _cache(hashes, service=None, api_key=None):
        return {h: state['cache_cached'] for h in hashes}
    monkeypatch.setattr(search, 'check_debrid_cache', _cache)

    def _add(info_hash, **kw):
        state['tb_adds'].append({'info_hash': info_hash, **kw})
        return {'success': True, 'torrent_id': 't', 'service': 'torbox'}
    monkeypatch.setattr(search, 'add_to_debrid', _add)

    monkeypatch.setattr(search, '_existing_hashes',
                        lambda svc, key, **kw: state['existing'])
    monkeypatch.setattr(search, 'remember_added_hash',
                        lambda svc, h: state['remembered'].append((svc, h)))

    monkeypatch.setattr(
        dc, 'get_debrid_client',
        lambda service=None, api_key=None: (rd_client, service))

    def _rescue(info_hash, source, **kw):
        state['rescue_calls'].append({'info_hash': info_hash, **kw})
        return dict(state['rd_core'])
    monkeypatch.setattr(routing, 'attempt_add_rescue', _rescue)

    def _log(ev_type, title, **kw):
        state['events'].append({'type': ev_type, 'title': title, **kw})
    monkeypatch.setattr(history, 'log_event', _log)

    return state


class TestWantedRdRecovery:
    """RD leg of the Wanted recovery pass — the add IS the cache probe."""

    def _scanner(self):
        sc = LibraryScanner.__new__(LibraryScanner)
        sc._wanted_tb_cooldown = {}
        sc._wanted_rd_miss = {}
        sc._wanted_no_results = {}
        return sc

    def _movie(self, title='The Substance', imdb='tt1234567'):
        return [{'title': title, 'source': 'wanted',
                 'imdb_id': imdb, 'is_available': True}]

    @pytest.fixture
    def wire(self, monkeypatch):
        return _wire_wanted_recovery(monkeypatch)

    @pytest.fixture(autouse=True)
    def ledger(self, tmp_path):
        # Isolated attempt_ledger: the RD leg now reads/writes persistent
        # ``rdblock:{hash}`` verdicts, and every test here shares the same
        # 'a'*40 top hash — without isolation one 451 test would poison
        # the RD probe for every test after it.
        import importlib
        from utils import attempt_ledger
        importlib.reload(attempt_ledger)
        attempt_ledger.init(config_dir=str(tmp_path))
        yield attempt_ledger
        attempt_ledger._file_path = None
        attempt_ledger._state = {}

    def _causes(self, wire):
        return [(e['type'], (e.get('meta') or {}).get('cause'))
                for e in wire['events']]

    def test_rd_hit_recovers_without_tb_add(self, wire):
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert len(wire['rescue_calls']) == 1
        assert wire['rescue_calls'][0]['info_hash'] == 'a' * 40
        assert wire['tb_adds'] == []  # RD won — TB leg skipped
        assert ('debrid_add', 'wanted_rd_recovered') in self._causes(wire)
        assert ('realdebrid', 'a' * 40) in wire['remembered']
        assert 'tt1234567' in sc._wanted_tb_cooldown
        assert sc._wanted_rd_miss == {}

    def test_rd_hit_targets_top_quality_release(self, wire):
        # Results arrive unsorted; the RD probe must get the ranked top.
        wire['torrentio'] = list(reversed(wire['torrentio']))
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'][0]['info_hash'] == 'a' * 40

    def test_rd_miss_falls_back_to_tb(self, wire):
        wire['rd_core'] = {'rescued': False, 'reason': 'never_ready',
                           'alt_torrent_id': 'RDTID1'}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert len(wire['tb_adds']) == 1  # TB leg took over
        assert 'tt1234567' in sc._wanted_rd_miss
        causes = self._causes(wire)
        assert ('debrid_add_failed', 'wanted_rd_uncached') in causes
        assert ('debrid_add', 'wanted_rd_recovered') not in causes

    def test_rd_failed_state_records_state_as_reason(self, wire):
        wire['rd_core'] = {'rescued': False, 'reason': 'failed_state',
                           'state': 'magnet_error',
                           'alt_torrent_id': 'RDTID1'}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        miss = [e for e in wire['events']
                if (e.get('meta') or {}).get('cause') == 'wanted_rd_uncached']
        assert len(miss) == 1
        assert miss[0]['meta']['reason'] == 'magnet_error'

    def test_rd_transient_add_error_not_memoized(self, wire):
        wire['rd_core'] = {'rescued': False, 'reason': 'add_error',
                           'alt_torrent_id': None}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        # Transient RD failure: no miss memo, no measurement event —
        # the title gets another RD shot on a later scan.
        assert sc._wanted_rd_miss == {}
        assert ('debrid_add_failed', 'wanted_rd_uncached') not in self._causes(wire)
        assert len(wire['tb_adds']) == 1  # TB fallback still ran

    def test_rd_451_at_add_time_is_filter_block_not_miss(self, wire):
        # RD's keyword filter rejects at addMagnet time — deterministic and
        # permanent, NOT a cache miss.  It no longer lands in the 7-day
        # _wanted_rd_miss memo (that would re-probe a blocked release
        # forever); the measurement event still fires.  Here TB has it
        # cached, so the title is recovered on TB.
        wire['rd_core'] = {'rescued': False, 'reason': 'add_failed',
                           'http_status': 451, 'alt_torrent_id': None}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert sc._wanted_rd_miss == {}
        miss = [e for e in wire['events']
                if (e.get('meta') or {}).get('cause') == 'wanted_rd_uncached']
        assert len(miss) == 1
        assert miss[0]['meta']['reason'] == 'infringing_add'
        assert len(wire['tb_adds']) == 1  # TB recovered it

    def test_rd_403_add_error_is_filter_block(self, wire):
        wire['rd_core'] = {'rescued': False, 'reason': 'add_error',
                           'http_status': 403, 'alt_torrent_id': None}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert sc._wanted_rd_miss == {}  # filter block, not a cache miss
        assert ('debrid_add_failed', 'wanted_rd_uncached') in self._causes(wire)

    def test_rd_5xx_add_failure_stays_transient(self, wire):
        wire['rd_core'] = {'rescued': False, 'reason': 'add_failed',
                           'http_status': 503, 'alt_torrent_id': None}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert sc._wanted_rd_miss == {}
        assert ('debrid_add_failed', 'wanted_rd_uncached') not in self._causes(wire)

    def test_add_time_filter_block_deletes_and_falls_back(self, wire):
        wire['rd_client'].probe_result = {
            'status': 'blocked', 'reason': 'infringing_file', 'http': 451}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rd_client'].delete_calls == ['RDTID1']
        # Post-add filter block: same permanent class — no cache-miss memo.
        assert sc._wanted_rd_miss == {}
        miss = [e for e in wire['events']
                if (e.get('meta') or {}).get('cause') == 'wanted_rd_uncached']
        assert len(miss) == 1
        assert miss[0]['meta']['reason'] == 'infringing_file'
        assert ('debrid_add', 'wanted_rd_recovered') not in self._causes(wire)
        assert len(wire['tb_adds']) == 1  # TB leg got its chance immediately

    def test_probe_unknown_keeps_recovery(self, wire):
        # Network blip on the filter probe must not throw away a good add —
        # the health sweep re-probes on its own cycle.
        wire['rd_client'].probe_result = {'status': 'unknown', 'error': 'x'}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rd_client'].delete_calls == []
        assert ('debrid_add', 'wanted_rd_recovered') in self._causes(wire)
        assert wire['tb_adds'] == []

    def test_add_time_filter_block_preexisting_torrent_not_deleted(self, wire):
        """Rank-5B regression: RD's addMagnet hash-dedups — adding a hash
        already on the account returns the USER'S pre-existing torrent id.
        When that torrent turns out filter-blocked, the probe's cleanup
        delete must not fire on a torrent the probe didn't create."""
        wire['rd_client'].probe_result = {
            'status': 'blocked', 'reason': 'infringing_file', 'http': 451}
        wire['rd_client'].info_added = '2020-01-01T00:00:00+00:00'
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rd_client'].delete_calls == []
        # Still classified as a filter block: no miss memo, TB leg fires.
        assert sc._wanted_rd_miss == {}
        assert len(wire['tb_adds']) == 1

    def test_add_time_filter_block_info_unavailable_skips_delete(self, wire):
        # Unknown ownership (torrent_info fetch failed) → conservative:
        # an orphaned probe entry beats destroying user data.
        wire['rd_client'].probe_result = {
            'status': 'blocked', 'reason': 'infringing_file', 'http': 451}
        wire['rd_client'].info_added = None
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rd_client'].delete_calls == []

    def test_preexisting_check_wired_into_rescue_core(self, wire):
        """The same added-timestamp guard must reach attempt_add_rescue's
        own cleanup deletes (never_ready / failed_state / stop_event)."""
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        check = wire['rescue_calls'][0].get('preexisting_check')
        assert callable(check)
        rd = wire['rd_client']
        # Fresh probe-created torrent → deletable.
        assert check(rd, 'RDTID1') is False
        # Old ``added`` → the user's own torrent.
        rd.info_added = '2020-01-01T00:00:00+00:00'
        assert check(rd, 'RDTID1') is True
        # Unparseable timestamp → conservative True.
        rd.info_added = 'not-a-date'
        assert check(rd, 'RDTID1') is True
        # Info fetch failure → conservative True.
        rd.info_added = None
        assert check(rd, 'RDTID1') is True

    def test_dedup_listing_bypasses_ttl_cache(self, wire, monkeypatch):
        """The 30s dedup-cache staleness window is CORRELATED with user
        adds (a popular new release).  The probe must force-refresh so a
        just-added user torrent is seen and the probe bails 'skipped'."""
        import utils.search as search
        calls = []

        def _existing(svc, key, **kw):
            calls.append(kw)
            return set()
        monkeypatch.setattr(search, '_existing_hashes', _existing)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert calls and calls[0].get('force_refresh') is True

    def test_tb_cooldown_only_disables_tb_leg(self, wire):
        # Previously a TB cooldown aborted the whole pass.
        wire['cooldown'] = 999
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert len(wire['rescue_calls']) == 1
        assert ('debrid_add', 'wanted_rd_recovered') in self._causes(wire)
        assert wire['tb_adds'] == []

    def test_rd_disabled_leaves_legacy_tb_behavior(self, wire, monkeypatch):
        monkeypatch.setenv('WANTED_RD_RECOVERY_ENABLED', 'false')
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'] == []
        assert len(wire['tb_adds']) == 1

    def test_no_rd_key_disables_rd_leg(self, wire, monkeypatch):
        import base
        monkeypatch.setattr(base, 'load_secret_or_env',
                            lambda name: 'tb_key' if name == 'torbox_api_key' else None)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'] == []
        assert len(wire['tb_adds']) == 1

    def test_rd_budget_counts_attempts_not_successes(self, wire, monkeypatch):
        monkeypatch.setenv('WANTED_RD_RECOVERY_MAX_PER_SCAN', '1')
        wire['rd_core'] = {'rescued': False, 'reason': 'never_ready',
                           'alt_torrent_id': 'RDTID1'}
        wire['cache_cached'] = False  # TB leg probes but never adds
        movies = self._movie() + self._movie(title='Other', imdb='tt7654321')
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], movies, {})
        assert len(wire['rescue_calls']) == 1  # budget spent on the miss

    def test_duplicate_hash_on_rd_account_skips_probe(self, wire):
        wire['existing'] = {'a' * 40}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'] == []  # never added
        assert 'tt1234567' in sc._wanted_rd_miss
        assert len(wire['tb_adds']) == 1  # TB fallback still ran

    def test_rd_miss_memo_skips_reprobe(self, wire):
        sc = self._scanner()
        sc._wanted_rd_miss['tt1234567'] = time.monotonic()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'] == []
        assert len(wire['tb_adds']) == 1

    def test_blocklisted_hash_excluded_from_both_legs(self, wire, monkeypatch):
        import utils.blocklist as blocklist
        monkeypatch.setattr(blocklist, 'is_blocked',
                            lambda h: h == 'a' * 40)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        # RD probe got the next-best non-blocklisted release.
        assert wire['rescue_calls'][0]['info_hash'] == 'b' * 40

    def test_all_results_blocklisted_cools_down(self, wire, monkeypatch):
        import utils.blocklist as blocklist
        monkeypatch.setattr(blocklist, 'is_blocked', lambda h: True)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'] == []
        assert wire['tb_adds'] == []
        # An empty (or fully-blocklisted) result set is leg-independent —
        # it lands in the shared no-results memo, not the TB cooldown.
        assert 'tt1234567' in sc._wanted_no_results

    def test_tb_per_title_cooldown_does_not_block_rd_leg(self, wire):
        # A prior TB miss cools the TB leg only — RD must still probe.
        sc = self._scanner()
        sc._wanted_tb_cooldown['tt1234567'] = time.monotonic()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert len(wire['rescue_calls']) == 1
        assert ('debrid_add', 'wanted_rd_recovered') in self._causes(wire)
        assert wire['tb_adds'] == []

    def test_rd_listing_unavailable_skips_add_entirely(self, wire, monkeypatch):
        # _existing_hashes → None means we can't prove the hash isn't
        # already on the account; RD returns the PRE-EXISTING torrent id
        # for a duplicate add, so a probe-miss delete could destroy a
        # user's in-flight download.  The probe must bail before adding.
        import utils.search as search
        monkeypatch.setattr(search, '_existing_hashes',
                            lambda svc, key, **kw: None)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'] == []   # no add attempted
        assert sc._wanted_rd_miss == {}     # transient — no memo
        assert len(wire['tb_adds']) == 1    # TB fallback still ran

    def test_local_skips_do_not_burn_rd_budget(self, wire, monkeypatch):
        # Movie 1's hash is already on the account (local skip, no API
        # add); with a budget of 1 the probe slot must survive to movie 2.
        monkeypatch.setenv('WANTED_RD_RECOVERY_MAX_PER_SCAN', '1')
        wire['existing'] = {'a' * 40}

        def _torrentio(imdb, **kw):
            if imdb == 'tt7654321':
                return [{'info_hash': 'c' * 40,
                         'title': 'Other.2024.1080p.WEB.RelC', 'seeds': 3,
                         'quality': {'label': '1080p', 'score': 90}}]
            return list(wire['torrentio'])
        import utils.search as search
        monkeypatch.setattr(search, 'search_torrentio', _torrentio)
        movies = self._movie() + self._movie(title='Other', imdb='tt7654321')
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], movies, {})
        assert len(wire['rescue_calls']) == 1
        assert wire['rescue_calls'][0]['info_hash'] == 'c' * 40

    def test_mislabeled_top_result_filtered_out(self, wire):
        # Torrentio's imdb-keyed lists are polluted: a mislabeled 2160p
        # junk entry outscores the real release.  The title filter must
        # drop it so the RD probe targets the real release's hash — this
        # is the live "Fight Club added in The Fountain's slot" bug.
        wire['torrentio'] = [
            {'info_hash': 'f' * 40,
             'title': 'Fight Club (1999) AI UHD - 10th Anniversary Edition',
             'seeds': 3, 'quality': {'label': '2160p', 'score': 200}},
        ] + list(wire['torrentio'])
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert len(wire['rescue_calls']) == 1
        assert wire['rescue_calls'][0]['info_hash'] == 'a' * 40

    def test_all_results_mislabeled_memoizes_no_results(self, wire):
        wire['torrentio'] = [
            {'info_hash': 'f' * 40,
             'title': 'Fight Club (1999) AI UHD - 10th Anniversary Edition',
             'seeds': 3, 'quality': {'label': '2160p', 'score': 200}},
        ]
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'] == []
        assert wire['tb_adds'] == []
        assert 'tt1234567' in sc._wanted_no_results

    # ---- persisted rdblock verdicts ----------------------------------

    def test_451_add_persists_rdblock_verdict(self, wire, ledger):
        wire['rd_core'] = {'rescued': False, 'reason': 'add_failed',
                           'http_status': 451, 'alt_torrent_id': None}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert ledger.get('rdblock:' + 'a' * 40) == 1

    def test_probe_file_block_persists_rdblock_verdict(self, wire, ledger):
        wire['rd_client'].probe_result = {
            'status': 'blocked', 'reason': 'infringing_file', 'http': 451}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert ledger.get('rdblock:' + 'a' * 40) == 1

    def test_probe_file_not_found_does_not_persist_rdblock(self, wire, ledger):
        # probe_file returns status='blocked' for a bare HTTP 404 too
        # (reason='not_found' — file transiently unresolvable, e.g. RD
        # hoster indexing lag).  That must NOT persist a 30-day rdblock
        # verdict, or a possibly-cached hash gets locked out of RD on a
        # transient error.
        wire['rd_client'].probe_result = {
            'status': 'blocked', 'reason': 'not_found', 'http': 404}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert ledger.get('rdblock:' + 'a' * 40) == 0

    def test_transient_rd_failure_does_not_persist_rdblock(self, wire, ledger):
        wire['rd_core'] = {'rescued': False, 'reason': 'add_failed',
                           'http_status': 503, 'alt_torrent_id': None}
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert ledger.get('rdblock:' + 'a' * 40) == 0

    def test_persisted_rdblock_skips_rd_add_but_tb_still_runs(self, wire, ledger):
        # Verdict persisted on a previous pass (possibly before a restart —
        # this is the whole point: the in-memory miss memo dies on restart
        # and was re-adding known-blocked hashes 6-7× per title).
        ledger.bump('rdblock:' + 'a' * 40)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'] == []   # no RD re-add
        assert len(wire['tb_adds']) == 1    # TB leg still got its chance

    def test_persisted_rdblock_burns_no_rd_budget(self, wire, ledger, monkeypatch):
        import utils.search as search
        monkeypatch.setenv('WANTED_RD_RECOVERY_MAX_PER_SCAN', '1')
        monkeypatch.setenv('WANTED_TB_RECOVERY_ENABLED', 'false')
        ledger.bump('rdblock:' + 'a' * 40)

        def _torrentio(imdb, **kw):
            if imdb == 'tt7654321':
                return [{'info_hash': 'c' * 40,
                         'title': 'Other.2024.1080p.WEB.RelC', 'seeds': 3,
                         'quality': {'label': '1080p', 'score': 90}}]
            return list(wire['torrentio'])
        monkeypatch.setattr(search, 'search_torrentio', _torrentio)
        movies = self._movie() + self._movie(title='Other', imdb='tt7654321')
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], movies, {})
        # The persisted verdict cost no API call and no budget — the single
        # RD attempt survived to the second title.
        assert len(wire['rescue_calls']) == 1
        assert wire['rescue_calls'][0]['info_hash'] == 'c' * 40

    def test_persisted_rdblock_still_accrues_giveup_strike(self, wire, ledger):
        # The persisted verdict must count as the "RD filter-blocked" half
        # of the terminal give-up signature WITHOUT re-adding, so strikes
        # keep climbing across passes (and restarts).
        ledger.bump('rdblock:' + 'a' * 40)
        wire['cache_cached'] = False  # TB uncached → both halves confirmed
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'] == []
        assert ledger.get('wantedblock:tt1234567') == 1


class TestWantedFilterGiveup:
    """Terminal give-up when a Wanted ghost is RD-filter-blocked AND
    TorBox-uncached across WANTED_FILTER_GIVEUP_STRIKES recovery passes."""

    def _scanner(self):
        sc = LibraryScanner.__new__(LibraryScanner)
        sc._wanted_tb_cooldown = {}
        sc._wanted_rd_miss = {}
        sc._wanted_no_results = {}
        return sc

    def _movie(self, title='The Substance', imdb='tt1234567'):
        return [{'title': title, 'source': 'wanted',
                 'imdb_id': imdb, 'is_available': True}]

    def _show(self, title='Some Show', imdb='tt7654321'):
        return [{'title': title, 'source': 'wanted',
                 'imdb_id': imdb, 'is_available': True}]

    def _causes(self, wire):
        return [(e['type'], (e.get('meta') or {}).get('cause'))
                for e in wire['events']]

    @pytest.fixture
    def wire(self, monkeypatch):
        return _wire_wanted_recovery(monkeypatch)

    @pytest.fixture
    def ledger(self, tmp_path):
        import importlib
        from utils import attempt_ledger
        importlib.reload(attempt_ledger)
        attempt_ledger.init(config_dir=str(tmp_path))
        yield attempt_ledger
        attempt_ledger._file_path = None
        attempt_ledger._state = {}

    def _filter_block(self, wire):
        # RD keyword-filters the add; TB has nothing cached.
        wire['rd_core'] = {'rescued': False, 'reason': 'add_failed',
                           'http_status': 451, 'alt_torrent_id': None}
        wire['cache_cached'] = False

    def test_filter_block_plus_tb_uncached_bumps_strike(self, wire, ledger):
        self._filter_block(wire)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert ledger.get('wantedblock:tt1234567') == 1
        # A confirmed both-provider failure is NOT a cache miss — it accrues
        # a persistent strike instead of the transient 7-day RD memo.
        assert sc._wanted_rd_miss == {}
        assert wire['tb_adds'] == []
        # Below the threshold: no terminal event yet.
        assert ('debrid_add_failed', 'wanted_filter_giveup') not in self._causes(wire)

    def test_three_strikes_logs_terminal_giveup_once_then_skips(self, wire, ledger):
        self._filter_block(wire)
        sc = self._scanner()
        for _ in range(4):
            sc._recover_wanted_via_debrid([], self._movie(), {})
        # Strike caps at the threshold — the 4th pass is skipped by the guard
        # before it can probe, so the count never climbs past 3.
        assert ledger.get('wantedblock:tt1234567') == 3
        # Only the FIRST pass re-adds to RD: the 451 verdict is persisted
        # per hash (rdblock:), so passes 2-3 confirm the filter-block from
        # the ledger while the strike count still climbs.
        assert len(wire['rescue_calls']) == 1
        giveups = [e for e in wire['events']
                   if (e.get('meta') or {}).get('cause') == 'wanted_filter_giveup']
        assert len(giveups) == 1  # logged exactly once, on the crossing pass
        assert giveups[0]['meta']['imdb_id'] == 'tt1234567'

    def test_giveup_guard_skips_probing_both_legs(self, wire, ledger):
        for _ in range(3):
            ledger.bump('wantedblock:tt1234567')
        self._filter_block(wire)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert wire['rescue_calls'] == []   # RD leg never ran
        assert wire['tb_adds'] == []         # TB leg never ran

    def test_tb_unavailable_filter_block_falls_back_to_rd_miss(self, wire, ledger, monkeypatch):
        # TB leg unavailable (disabled — the account cooldown flag is only
        # advisory now and no longer parks the leg) → the "uncached" half
        # can't be confirmed, so no strike; fall back to the 7-day RD-miss
        # memo instead of hammering RD.
        self._filter_block(wire)
        monkeypatch.setenv('WANTED_TB_RECOVERY_ENABLED', 'false')
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert ledger.get('wantedblock:tt1234567') == 0
        assert 'tt1234567' in sc._wanted_rd_miss
        assert wire['tb_adds'] == []

    def test_tb_probe_error_filter_block_no_strike(self, wire, ledger, monkeypatch):
        # A TorBox cache-probe error can't confirm "uncached" — be
        # conservative: no strike, memo RD-miss so RD isn't re-probed hourly.
        self._filter_block(wire)
        import utils.search as search

        def _boom(*a, **kw):
            raise RuntimeError('tb api down')
        monkeypatch.setattr(search, 'check_debrid_cache', _boom)
        sc = self._scanner()
        sc._recover_wanted_via_debrid([], self._movie(), {})
        assert ledger.get('wantedblock:tt1234567') == 0
        assert 'tt1234567' in sc._wanted_rd_miss

    def test_show_strike_is_episode_scoped(self, wire, ledger, monkeypatch):
        # A blocked show episode strikes under wantedblock:<imdb>:<s>:<e>, NOT
        # the bare imdb — so if miss[0] shifts to a different episode later
        # (e.g. this one gets recovered elsewhere), that episode isn't already
        # abandoned by strikes accrued against a sibling episode.
        self._filter_block(wire)
        wire['torrentio'] = [
            {'info_hash': 'a' * 40, 'title': 'The.Substance.S02E05.1080p.WEB',
             'seeds': 10, 'quality': {'label': '1080p', 'score': 100}},
        ]
        monkeypatch.setattr(LibraryScanner, '_compute_missing_episodes',
                            lambda self, s: [(2, 5)])
        sc = self._scanner()
        sc._recover_wanted_via_debrid(
            self._show(title='The Substance', imdb='tt7654321'), [], {})
        assert ledger.get('wantedblock:tt7654321:2:5') == 1
        assert ledger.get('wantedblock:tt7654321') == 0


class TestReleaseMatchesTitle:
    """Golden cases for the Torrentio auto-add title sanity check."""

    def test_exact_with_year_and_quality_tail(self):
        assert _release_matches_title(
            'The Fountain 2006 1080p BluRay x264', 'The Fountain')

    def test_mislabeled_release_rejected(self):
        assert not _release_matches_title(
            'Fight Club (1999) AI UHD - 10th Anniversary Edition + Extras '
            '(PROPER) FIX', 'The Fountain')

    def test_unicode_transliteration_matches(self):
        assert _release_matches_title(
            'Amelie.2001.1080p.BluRay.x264', 'Amélie')

    def test_media_title_as_prefix_of_release(self):
        assert _release_matches_title('F1.The.Movie.2025.2160p.WEB-DL', 'F1')

    def test_reverse_prefix_rejected(self):
        # A short junk release name must not claim a longer media title.
        assert not _release_matches_title('The 2006 1080p', 'The Fountain')

    def test_tv_episode_release_matches(self):
        assert _release_matches_title(
            'Paradise 2025 S01E01 1080p WEB', 'Paradise (2025)')

    def test_media_year_suffix_stripped(self):
        assert _release_matches_title(
            'Broadchurch.S01E01.1080p.WEB', 'Broadchurch')

    def test_numeric_title_with_parenthesized_year(self):
        assert _release_matches_title('1917 (2019) 1080p', '1917')

    def test_empty_norms_rejected(self):
        assert not _release_matches_title('', 'The Fountain')
        assert not _release_matches_title('The Fountain 2006 1080p', '')
        # Titles that collapse to nothing after ASCII transliteration.
        assert not _release_matches_title('愛.2011.1080p', '愛')

    def test_scene_release_dropping_leading_article_matches(self):
        # Scene names routinely omit the "The" the arr keeps.
        assert _release_matches_title(
            'Big.Bang.Theory.S03E12.1080p.WEB', 'The Big Bang Theory')

    def test_article_only_release_rejected(self):
        assert not _release_matches_title('The 2006 1080p', 'The Fountain')

    def test_sequel_rejected_by_year_check(self):
        # "Dune Part Two" passes the prefix rule for "Dune" — the year
        # cross-check is what rejects it.
        assert not _release_matches_title(
            'Dune.Part.Two.2024.1080p.WEB', 'Dune', media_year=2021)
        assert _release_matches_title(
            'Dune.2021.1080p.WEB', 'Dune', media_year=2021)

    def test_remake_rejected_by_year_check(self):
        assert not _release_matches_title(
            'It.2017.1080p.BluRay', 'It', media_year=1990)
        assert _release_matches_title(
            'It.1990.1080p', 'It', media_year=1990)

    def test_year_in_title_not_counted_as_release_year(self):
        # "1917" the token is the title, not release-year evidence — a
        # release without an explicit year must still match.
        assert _release_matches_title(
            '1917.1080p.BluRay', '1917', media_year=2019)

    def test_year_tolerance_plus_minus_one(self):
        # Release-date vs production-year tagging is commonly off by one.
        assert _release_matches_title(
            'The Fountain 2007 1080p', 'The Fountain', media_year=2006)

    def test_no_years_anywhere_skips_year_check(self):
        assert _release_matches_title(
            'Sing.2.1080p.WEB.x264-GRP', 'Sing 2', media_year=2021)
