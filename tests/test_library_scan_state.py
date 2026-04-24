"""LibraryScanner scan-state (new-import vs upgrade classification) tests.

Covers the persisted ``library_scan_state.json`` loader hardening and the
stash-reset behavior for ``_pending_rescan_prior_ids`` across scans.
"""

import json
import os

import pytest

from utils.library import LibraryScanner


def _scanner(config_dir, monkeypatch):
    """Build a scanner with __init__ targeting the given config_dir."""
    monkeypatch.setenv('CONFIG_DIR', config_dir)
    monkeypatch.delenv('BLACKHOLE_LOCAL_LIBRARY_MOVIES', raising=False)
    monkeypatch.delenv('BLACKHOLE_LOCAL_LIBRARY_TV', raising=False)
    return LibraryScanner()


def test_state_loader_happy_path(tmp_dir, monkeypatch):
    path = os.path.join(tmp_dir, 'library_scan_state.json')
    with open(path, 'w') as f:
        json.dump({'titles': {'Guardians of the Galaxy': ['movie.1080p.mkv']}}, f)
    s = _scanner(tmp_dir, monkeypatch)
    assert s._last_symlinked_files == {
        'Guardians of the Galaxy': {'movie.1080p.mkv'},
    }
    assert s._state_was_bootstrapped is False


def test_state_loader_treats_non_object_root_as_bootstrap(tmp_dir, monkeypatch):
    path = os.path.join(tmp_dir, 'library_scan_state.json')
    with open(path, 'w') as f:
        f.write('[]')  # array not object
    s = _scanner(tmp_dir, monkeypatch)
    # Loader must not crash __init__ — treat malformed state as bootstrap.
    assert s._last_symlinked_files == {}
    assert s._state_was_bootstrapped is True


def test_state_loader_treats_non_object_titles_as_bootstrap(tmp_dir, monkeypatch):
    path = os.path.join(tmp_dir, 'library_scan_state.json')
    with open(path, 'w') as f:
        f.write('{"titles": "not a dict"}')
    s = _scanner(tmp_dir, monkeypatch)
    assert s._last_symlinked_files == {}
    assert s._state_was_bootstrapped is True


def test_state_loader_drops_non_string_basenames(tmp_dir, monkeypatch):
    path = os.path.join(tmp_dir, 'library_scan_state.json')
    with open(path, 'w') as f:
        json.dump({'titles': {'Movie': ['ok.mkv', 42, None, {'x': 1}]}}, f)
    s = _scanner(tmp_dir, monkeypatch)
    # Only the string survives.
    assert s._last_symlinked_files == {'Movie': {'ok.mkv'}}


def test_state_loader_skips_huge_file(tmp_dir, monkeypatch):
    path = os.path.join(tmp_dir, 'library_scan_state.json')
    # Write a 15 MB file — over the 10 MB cap.  Loader must refuse to
    # parse it and bootstrap fresh rather than OOM the container.
    with open(path, 'w') as f:
        f.write('{"titles":{"x":["' + ('a' * (15 * 1024 * 1024)) + '"]}}')
    s = _scanner(tmp_dir, monkeypatch)
    assert s._last_symlinked_files == {}
    assert s._state_was_bootstrapped is True


def test_state_loader_invalid_json_bootstraps(tmp_dir, monkeypatch):
    path = os.path.join(tmp_dir, 'library_scan_state.json')
    with open(path, 'w') as f:
        f.write('{not valid json')
    s = _scanner(tmp_dir, monkeypatch)
    assert s._last_symlinked_files == {}
    assert s._state_was_bootstrapped is True
