"""Golden-file tests for utils.activity_format.format_event.

Every cause slug in utils/history.py CAUSE_* constants must have a
corresponding formatter and a test here — the UI relies on that.
"""

import pytest

from utils.activity_format import format_event


def _ev(cause, **meta_kwargs):
    """Helper: build an event dict with the given cause."""
    return {
        'id': 't',
        'ts': '2026-04-24T12:00:00+00:00',
        'type': 'x',
        'title': 'Thing',
        'meta': {'cause': cause, **meta_kwargs},
    }


def test_library_new_import_with_file():
    ev = _ev('library_new_import', file='Movie.1080p.mkv',
             quality='1080p', size_bytes=4_500_000_000)
    got = format_event(ev)
    assert 'New import: Movie.1080p.mkv' in got['short']
    assert '1080p' in got['short']
    assert '4.2 GB' in got['short']


def test_library_new_import_without_file():
    ev = _ev('library_new_import')
    assert format_event(ev)['short'] == 'New debrid file symlinked'


def test_library_upgrade_replaced_with_both():
    ev = _ev('library_upgrade_replaced', file='Movie.1080p.mkv',
             replaces='Movie.720p.mkv', quality='1080p')
    s = format_event(ev)['short']
    assert 'Upgraded' in s
    assert 'Movie.720p.mkv' in s
    assert 'Movie.1080p.mkv' in s
    assert '1080p' in s


def test_library_state_init_with_file():
    ev = _ev('library_state_init', file='X.mkv')
    assert 'Initial scan linked: X.mkv' in format_event(ev)['short']


def test_post_symlink_rescan_service_cap():
    ev = _ev('post_symlink_rescan', arr_service='radarr')
    assert format_event(ev)['short'] == 'Radarr rescan — new symlink available for import'


def test_routing_audit_retry_cycle_formatting():
    from datetime import datetime, timezone, timedelta
    first_ts = (datetime.now(timezone.utc) - timedelta(days=5, hours=3)).isoformat(
        timespec='seconds')
    ev = _ev('routing_audit_retry', arr_service='radarr',
             cycle_n=14, cycle_first_ts=first_ts)
    s = format_event(ev)['short']
    assert 'Radarr search' in s
    assert 'routing audit retry' in s
    assert 'retry #14' in s
    assert 'first attempt' in s
    # Regression: the d-unit remainder used to compute as "5d 7h" because
    # the divisor was wrongly `unit // 60` (1440) instead of 3600.
    assert '5d 7h' not in s
    # Elapsed spans 5d 3h, give or take a few seconds for test latency.
    assert '5d 3h' in s or '5d 2h' in s


def test_routing_audit_retry_no_cycle_suffix_on_first():
    ev = _ev('routing_audit_retry', arr_service='sonarr', cycle_n=1)
    assert 'retry #' not in format_event(ev)['short']


def test_debrid_unavailable_marked_includes_retries_tail():
    ev = _ev('debrid_unavailable_marked', age_days=3, search_attempts=14)
    s = format_event(ev)['short']
    assert 'Marked unavailable after 3d' in s
    assert '14 searches so far' in s
    assert 'retries continue' in s


def test_debrid_unavailable_marked_without_attempts():
    ev = _ev('debrid_unavailable_marked', age_days=3)
    assert 'retries continue in arr' in format_event(ev)['short']


def test_preference_source_switch_arrow():
    ev = _ev('preference_source_switch', **{'from': 'local', 'to': 'debrid'})
    assert format_event(ev)['short'] == 'Source switch: local → debrid'


def test_task_library_scan_counts():
    ev = _ev('task_library_scan', movies=565, shows=117, duration_ms=80645)
    s = format_event(ev)['short']
    assert '565 movies' in s and '117 shows' in s and '80645ms' in s


def test_task_verify_symlinks_empty_when_no_action():
    ev = _ev('task_verify_symlinks')
    assert 'nothing to do' in format_event(ev)['short']


def test_task_verify_symlinks_with_counts():
    ev = _ev('task_verify_symlinks', repaired=2, searched=5, deleted=1)
    s = format_event(ev)['short']
    assert 'repaired 2' in s and 'searched 5' in s and 'deleted 1' in s


def test_blackhole_new_import_with_count():
    ev = _ev('blackhole_new_import', count=5, release='Big.Pack.2024')
    assert 'Blackhole import: 5 files from Big.Pack.2024' in format_event(ev)['short']


def test_blackhole_cache_hit_lists_provider():
    ev = _ev('blackhole_cache_hit', provider='realdebrid')
    assert 'realdebrid' in format_event(ev)['short']


def test_terminal_error_shows_status():
    ev = _ev('terminal_error', provider='realdebrid', status='magnet_error')
    assert 'Failed on realdebrid: magnet_error' == format_event(ev)['short']


def test_uncached_timeout_deleted_vs_not():
    kept = format_event(_ev('uncached_timeout', deleted=False))['short']
    removed = format_event(_ev('uncached_timeout', deleted=True))['short']
    assert 'debrid cleanup skipped' in kept
    assert 'removed from debrid' in removed


def test_unknown_cause_falls_back_to_detail():
    ev = {'id': 't', 'ts': '2026', 'type': 'x', 'title': 'Y',
          'detail': 'legacy string', 'meta': {'cause': 'not_a_real_cause'}}
    assert format_event(ev)['short'] == 'legacy string'


def test_event_without_cause_uses_detail():
    ev = {'id': 't', 'ts': '2026', 'type': 'x', 'title': 'Y',
          'detail': 'before vocab'}
    assert format_event(ev)['short'] == 'before vocab'


def test_event_without_meta_or_detail_is_empty():
    ev = {'id': 't', 'ts': '2026', 'type': 'x', 'title': 'Y'}
    assert format_event(ev)['short'] == ''


def test_malformed_event_no_crash():
    assert format_event(None)['short'] == ''
    assert format_event('not a dict')['short'] == ''
    assert format_event({})['short'] == ''


def test_group_key_includes_type_source_cause_media():
    ev = {'id': 't', 'ts': '2026', 'type': 'search_triggered',
          'title': 'Radarr movie 217', 'source': 'arr',
          'media_title': 'LEGO Marvel',
          'meta': {'cause': 'routing_audit_retry', 'arr_service': 'radarr'}}
    gk = format_event(ev)['group_key']
    assert gk == ('search_triggered', 'arr', 'routing_audit_retry', 'LEGO Marvel')


def test_every_cause_constant_has_a_formatter():
    """Every CAUSE_* constant must map to a formatter or the UI degrades to the
    raw detail string.  Keep parity — this guards against adding a new slug
    and forgetting to wire the renderer."""
    from utils import history, activity_format
    constants = [v for k, v in vars(history).items()
                 if k.startswith('CAUSE_') and isinstance(v, str)]
    missing = [c for c in constants if c not in activity_format._CAUSE_FORMATTERS]
    assert not missing, f'CAUSE_* constants without formatters: {missing}'
