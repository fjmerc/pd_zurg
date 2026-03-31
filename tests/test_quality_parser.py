"""Tests for utils.quality_parser — filename quality extraction."""

import pytest
from utils.quality_parser import parse_quality


class TestResolution:
    def test_1080p_standard(self):
        q = parse_quality("Show.Name.S01E01.1080p.WEB-DL.x265-GROUP.mkv")
        assert q['resolution'] == '1080p'

    def test_2160p(self):
        q = parse_quality("Movie.2024.2160p.BluRay.Remux.HEVC.HDR.DTS-HD.mkv")
        assert q['resolution'] == '2160p'

    def test_4k_label(self):
        q = parse_quality("Movie.2024.4K.WEB-DL.mkv")
        assert q['resolution'] == '2160p'

    def test_uhd(self):
        q = parse_quality("Movie.UHD.BluRay.mkv")
        assert q['resolution'] == '2160p'

    def test_720p(self):
        q = parse_quality("show.s02e03.720p.hdtv.mkv")
        assert q['resolution'] == '720p'

    def test_480p(self):
        q = parse_quality("Show.S01E01.480p.DVDRip.mkv")
        assert q['resolution'] == '480p'

    def test_1080i(self):
        q = parse_quality("Show.S01E01.1080i.HDTV.mkv")
        assert q['resolution'] == '1080p'

    def test_576p_maps_to_480p(self):
        q = parse_quality("Show.S01E01.576p.DVDRip.mkv")
        assert q['resolution'] == '480p'

    def test_no_resolution(self):
        q = parse_quality("Movie (2023).mkv")
        assert q['resolution'] is None

    def test_uppercase_1080P(self):
        q = parse_quality("Show.S01E01.1080P.WEB-DL.mkv")
        assert q['resolution'] == '1080p'


class TestSource:
    def test_web_dl_hyphen(self):
        q = parse_quality("Show.S01E01.1080p.WEB-DL.x265-GROUP.mkv")
        assert q['source'] == 'WEB-DL'

    def test_webdl_no_separator(self):
        q = parse_quality("Show.S01E01.1080p.WEBDL.mkv")
        assert q['source'] == 'WEB-DL'

    def test_web_dl_with_dot(self):
        q = parse_quality("Show.S01E01.1080p.WEB.DL.mkv")
        assert q['source'] == 'WEB-DL'

    def test_webrip(self):
        q = parse_quality("Show.S01E01.1080p.WEBRip.mkv")
        assert q['source'] == 'WEBRip'

    def test_bluray(self):
        q = parse_quality("Movie.2024.1080p.BluRay.mkv")
        assert q['source'] == 'BluRay'

    def test_blu_ray_hyphen(self):
        q = parse_quality("Movie.2024.1080p.Blu-Ray.mkv")
        assert q['source'] == 'BluRay'

    def test_bdrip(self):
        q = parse_quality("Movie.2024.1080p.BDRip.mkv")
        assert q['source'] == 'BluRay'

    def test_brrip(self):
        q = parse_quality("Movie.2024.720p.BRRip.mkv")
        assert q['source'] == 'BluRay'

    def test_remux(self):
        q = parse_quality("Movie.2024.2160p.BluRay.Remux.HEVC.mkv")
        assert q['source'] == 'Remux'

    def test_bdremux(self):
        q = parse_quality("Movie.2024.2160p.BDRemux.HEVC.mkv")
        assert q['source'] == 'Remux'

    def test_hdtv(self):
        q = parse_quality("show.s02e03.720p.hdtv.mkv")
        assert q['source'] == 'HDTV'

    def test_dvdrip(self):
        q = parse_quality("Movie.2020.DVDRip.mkv")
        assert q['source'] == 'DVDRip'

    def test_hdrip(self):
        q = parse_quality("Movie.2020.HDRip.mkv")
        assert q['source'] == 'HDRip'

    def test_bare_web(self):
        q = parse_quality("Show.S01E01.2160p.WEB.H265.mkv")
        assert q['source'] == 'WEB-DL'

    def test_no_source(self):
        q = parse_quality("Movie (2023).mkv")
        assert q['source'] is None


class TestCodec:
    def test_x265(self):
        q = parse_quality("Show.S01E01.1080p.WEB-DL.x265-GROUP.mkv")
        assert q['codec'] == 'x265'

    def test_h265(self):
        q = parse_quality("Show.S01E01.2160p.WEB.H265.mkv")
        assert q['codec'] == 'x265'

    def test_h_dot_265(self):
        q = parse_quality("Show.S01E01.1080p.H.265.mkv")
        assert q['codec'] == 'x265'

    def test_hevc(self):
        q = parse_quality("Movie.2024.2160p.BluRay.Remux.HEVC.mkv")
        assert q['codec'] == 'x265'

    def test_x264(self):
        q = parse_quality("Show.S01E01.720p.HDTV.x264.mkv")
        assert q['codec'] == 'x264'

    def test_h264(self):
        q = parse_quality("Show.S01E01.1080p.H264.mkv")
        assert q['codec'] == 'x264'

    def test_avc(self):
        q = parse_quality("Movie.2024.1080p.BluRay.AVC.mkv")
        assert q['codec'] == 'x264'

    def test_av1(self):
        q = parse_quality("Movie.2024.2160p.WEB-DL.AV1.mkv")
        assert q['codec'] == 'AV1'

    def test_no_codec(self):
        q = parse_quality("Movie (2023).mkv")
        assert q['codec'] is None

    def test_x_dot_265(self):
        q = parse_quality("Show.S01E01.x.265.mkv")
        assert q['codec'] == 'x265'


class TestHDR:
    def test_hdr(self):
        q = parse_quality("Movie.2024.2160p.BluRay.HDR.mkv")
        assert q['hdr'] == 'HDR'

    def test_hdr10_plus(self):
        q = parse_quality("Movie.2024.2160p.WEB-DL.HDR10+.mkv")
        assert q['hdr'] == 'HDR10+'

    def test_hdr10(self):
        q = parse_quality("Movie.2024.2160p.BluRay.HDR10.mkv")
        assert q['hdr'] == 'HDR10'

    def test_dolby_vision_dv(self):
        q = parse_quality("Show.S01E01.DV.HDR10+.2160p.WEB.H265.mkv")
        assert q['hdr'] == 'DV'

    def test_dolby_vision_full(self):
        q = parse_quality("Movie.2024.2160p.Dolby.Vision.mkv")
        assert q['hdr'] == 'DV'

    def test_dovi(self):
        q = parse_quality("Movie.2024.2160p.DoVi.mkv")
        assert q['hdr'] == 'DV'

    def test_no_hdr(self):
        q = parse_quality("Movie.2024.1080p.WEB-DL.mkv")
        assert q['hdr'] is None


class TestLabel:
    def test_source_and_resolution(self):
        q = parse_quality("Show.S01E01.1080p.WEB-DL.x265-GROUP.mkv")
        assert q['label'] == 'WEB-DL 1080p'

    def test_source_resolution_hdr(self):
        q = parse_quality("Movie.2024.2160p.BluRay.Remux.HEVC.HDR.DTS-HD.mkv")
        assert q['label'] == 'Remux 2160p HDR'

    def test_resolution_only(self):
        q = parse_quality("Movie.2024.1080p.mkv")
        assert q['label'] == '1080p'

    def test_source_only(self):
        q = parse_quality("Movie.HDTV.mkv")
        assert q['label'] == 'HDTV'

    def test_no_quality_info(self):
        q = parse_quality("Movie (2023).mkv")
        assert q['label'] is None

    def test_dv_hdr_in_label(self):
        q = parse_quality("Show.S01E01.DV.2160p.WEB.H265.mkv")
        assert q['label'] == 'WEB-DL 2160p DV'


class TestEdgeCases:
    def test_no_extension(self):
        q = parse_quality("Show.S01E01.1080p.WEB-DL.x265-GROUP")
        assert q['resolution'] == '1080p'
        assert q['source'] == 'WEB-DL'

    def test_garbage_filename(self):
        q = parse_quality("random_garbage_file.txt")
        assert q['resolution'] is None
        assert q['source'] is None
        assert q['codec'] is None
        assert q['hdr'] is None
        assert q['label'] is None

    def test_empty_string(self):
        q = parse_quality("")
        assert q['label'] is None

    def test_none_input(self):
        q = parse_quality(None)
        assert q['resolution'] is None
        assert q['source'] is None
        assert q['codec'] is None
        assert q['hdr'] is None
        assert q['label'] is None

    def test_complex_real_filename(self):
        q = parse_quality("The.Movie.2024.2160p.AMZN.WEB-DL.DDP5.1.Atmos.DV.H.265-GROUP.mkv")
        assert q['resolution'] == '2160p'
        assert q['source'] == 'WEB-DL'
        assert q['codec'] == 'x265'
        assert q['hdr'] == 'DV'

    def test_multiple_resolution_tokens_first_wins(self):
        # Weird but possible: first resolution wins
        q = parse_quality("Movie.2160p.1080p.mkv")
        assert q['resolution'] == '2160p'

    def test_bdmv_source(self):
        q = parse_quality("Movie.2024.BDMV.mkv")
        assert q['source'] == 'BluRay'

    def test_lowercase_hevc(self):
        q = parse_quality("movie.s01e01.1080p.hevc.mkv")
        assert q['codec'] == 'x265'

    def test_year_not_confused_with_resolution(self):
        # 2024 should NOT be parsed as resolution
        q = parse_quality("Movie.2024.mkv")
        assert q['resolution'] is None
