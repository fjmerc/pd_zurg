"""Tests for utils/webdav.py — WebDAV PROPFIND client."""

import pytest
from utils.webdav import _parse_multistatus


# --- XML parsing tests ---

MULTISTATUS_BASIC = b"""\
<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dav/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/></D:resourcetype>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/movies/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/></D:resourcetype>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/shows/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/></D:resourcetype>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"""


def test_parse_multistatus_categories():
    entries = _parse_multistatus(MULTISTATUS_BASIC)
    assert len(entries) == 3
    assert entries[0]['href'] == '/dav/'
    assert entries[0]['is_collection'] is True
    assert entries[0]['name'] == 'dav'
    assert entries[1]['name'] == 'movies'
    assert entries[2]['name'] == 'shows'


MULTISTATUS_FILES = b"""\
<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dav/movies/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/movies/Inception%20(2010)/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/movies/Inception%20(2010)/Inception.2010.1080p.mkv</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>4294967296</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"""


def test_parse_multistatus_movies():
    entries = _parse_multistatus(MULTISTATUS_FILES)
    assert len(entries) == 3

    folder = entries[1]
    assert folder['name'] == 'Inception (2010)'
    assert folder['is_collection'] is True
    assert folder['href'] == '/dav/movies/Inception (2010)/'

    f = entries[2]
    assert f['name'] == 'Inception.2010.1080p.mkv'
    assert f['is_collection'] is False
    assert f['size'] == 4294967296


MULTISTATUS_SHOWS = b"""\
<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dav/shows/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/shows/Breaking%20Bad%20(2008)/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/shows/Breaking%20Bad%20(2008)/Season%201/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/shows/Breaking%20Bad%20(2008)/Season%201/S01E01.mkv</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>500000000</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/shows/Breaking%20Bad%20(2008)/Season%201/S01E02.mkv</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>520000000</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/shows/Breaking%20Bad%20(2008)/S01E03.mkv</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>490000000</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"""


def test_parse_multistatus_shows_with_seasons():
    entries = _parse_multistatus(MULTISTATUS_SHOWS)
    files = [e for e in entries if not e['is_collection']]
    assert len(files) == 3

    assert files[0]['name'] == 'S01E01.mkv'
    assert files[0]['href'] == '/dav/shows/Breaking Bad (2008)/Season 1/S01E01.mkv'
    assert files[1]['name'] == 'S01E02.mkv'
    # Flat file in folder root
    assert files[2]['href'] == '/dav/shows/Breaking Bad (2008)/S01E03.mkv'


def test_parse_multistatus_empty():
    xml = b'<?xml version="1.0"?><D:multistatus xmlns:D="DAV:"></D:multistatus>'
    entries = _parse_multistatus(xml)
    assert entries == []


def test_parse_multistatus_no_content_length():
    xml = b"""\
<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dav/movies/test/movie.mkv</D:href>
    <D:propstat>
      <D:prop><D:resourcetype/></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"""
    entries = _parse_multistatus(xml)
    assert len(entries) == 1
    assert entries[0]['size'] == 0
    assert entries[0]['is_collection'] is False


# --- Episode extraction from WebDAV data ---

class TestCollectEpisodesFromWebdav:
    """Test LibraryScanner._collect_episodes_from_webdav static method."""

    @pytest.fixture
    def scanner(self, monkeypatch):
        monkeypatch.setenv('RCLONE_MOUNT_NAME', '')
        from utils.library import LibraryScanner
        return LibraryScanner()

    def test_season_dir_episodes(self, scanner):
        contents = {
            'files': [],
            'season_files': {
                'Season 1': [
                    ('S01E01.mkv', 500, '/data/m/shows/Show/Season 1/S01E01.mkv'),
                    ('S01E02.mkv', 520, '/data/m/shows/Show/Season 1/S01E02.mkv'),
                ],
                'Season 2': [
                    ('S02E01.mkv', 480, '/data/m/shows/Show/Season 2/S02E01.mkv'),
                ],
            },
        }
        eps = scanner._collect_episodes_from_webdav(contents)
        assert (1, 1) in eps
        assert (1, 2) in eps
        assert (2, 1) in eps
        assert eps[(1, 1)]['file'] == 'S01E01.mkv'
        assert eps[(1, 1)]['path'] == '/data/m/shows/Show/Season 1/S01E01.mkv'

    def test_flat_episodes(self, scanner):
        contents = {
            'files': [
                ('S01E01.mkv', 500, '/data/m/shows/Show/S01E01.mkv'),
                ('S01E02.mkv', 520, '/data/m/shows/Show/S01E02.mkv'),
            ],
            'season_files': {},
        }
        eps = scanner._collect_episodes_from_webdav(contents)
        assert (1, 1) in eps
        assert (1, 2) in eps
        assert eps[(1, 1)]['path'] == '/data/m/shows/Show/S01E01.mkv'

    def test_non_media_files_skipped(self, scanner):
        contents = {
            'files': [
                ('S01E01.mkv', 500, '/data/m/shows/Show/S01E01.mkv'),
                ('S01E02.srt', 100, '/data/m/shows/Show/S01E02.srt'),
                ('info.nfo', 50, '/data/m/shows/Show/info.nfo'),
            ],
            'season_files': {},
        }
        eps = scanner._collect_episodes_from_webdav(contents)
        assert len(eps) == 1
        assert (1, 1) in eps

    def test_non_episode_media_skipped_in_flat(self, scanner):
        """Flat files without S##E## pattern are skipped (movies in root)."""
        contents = {
            'files': [
                ('movie.mkv', 5000, '/data/m/shows/Show/movie.mkv'),
            ],
            'season_files': {},
        }
        eps = scanner._collect_episodes_from_webdav(contents)
        assert len(eps) == 0

    def test_non_season_subdirs_skipped(self, scanner):
        contents = {
            'files': [],
            'season_files': {
                'Season 1': [
                    ('S01E01.mkv', 500, '/p'),
                ],
                'Extras': [
                    ('behind_the_scenes.mkv', 200, '/p2'),
                ],
            },
        }
        eps = scanner._collect_episodes_from_webdav(contents)
        assert len(eps) == 1
        assert (1, 1) in eps

    def test_empty_contents(self, scanner):
        contents = {'files': [], 'season_files': {}}
        eps = scanner._collect_episodes_from_webdav(contents)
        assert len(eps) == 0


# --- Zurg URL discovery ---

class TestDiscoverZurgUrl:

    def test_rd_port(self, monkeypatch):
        monkeypatch.setenv('ZURG_PORT_RealDebrid', '9001')
        from utils.library import _discover_zurg_url
        assert _discover_zurg_url('/data/realdebrid') == 'http://localhost:9001'

    def test_ad_port(self, monkeypatch):
        monkeypatch.setenv('ZURG_PORT_AllDebrid', '9002')
        from utils.library import _discover_zurg_url
        assert _discover_zurg_url('/data/alldebrid') == 'http://localhost:9002'

    def test_rd_suffix_priority(self, monkeypatch):
        monkeypatch.setenv('ZURG_PORT_RealDebrid', '9001')
        monkeypatch.setenv('ZURG_PORT_AllDebrid', '9002')
        from utils.library import _discover_zurg_url
        assert _discover_zurg_url('/data/myremote_RD') == 'http://localhost:9001'

    def test_ad_suffix_priority(self, monkeypatch):
        monkeypatch.setenv('ZURG_PORT_RealDebrid', '9001')
        monkeypatch.setenv('ZURG_PORT_AllDebrid', '9002')
        from utils.library import _discover_zurg_url
        assert _discover_zurg_url('/data/myremote_AD') == 'http://localhost:9002'

    def test_no_suffix_prefers_rd(self, monkeypatch):
        monkeypatch.setenv('ZURG_PORT_RealDebrid', '9001')
        monkeypatch.setenv('ZURG_PORT_AllDebrid', '9002')
        from utils.library import _discover_zurg_url
        assert _discover_zurg_url('/data/myremote') == 'http://localhost:9001'

    def test_no_ports_returns_none(self, clean_env):
        from utils.library import _discover_zurg_url
        assert _discover_zurg_url('/data/mount') is None


# --- Zurg auth ---

class TestGetZurgAuth:

    def test_auth_set(self, monkeypatch):
        monkeypatch.setenv('ZURG_USER', 'admin')
        monkeypatch.setenv('ZURG_PASS', 'secret')
        from utils.library import _get_zurg_auth
        assert _get_zurg_auth() == ('admin', 'secret')

    def test_no_auth(self, clean_env):
        from utils.library import _get_zurg_auth
        assert _get_zurg_auth() is None

    def test_user_without_pass(self, monkeypatch, clean_env):
        monkeypatch.setenv('ZURG_USER', 'admin')
        from utils.library import _get_zurg_auth
        assert _get_zurg_auth() is None
