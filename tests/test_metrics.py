"""Tests for Prometheus metrics formatting."""

import re
import pytest
from utils.metrics import MetricsRegistry, _emit, _sanitize_label, _format_labels


class TestMetricsRegistry:

    def test_increment_counter(self):
        """Counter should increment correctly."""
        m = MetricsRegistry()
        m.inc('test_total', {'status': 'ok'})
        m.inc('test_total', {'status': 'ok'})
        assert m.get_counter('test_total', {'status': 'ok'}) == 2

    def test_separate_labels(self):
        """Different label values should be tracked independently."""
        m = MetricsRegistry()
        m.inc('test_total', {'status': 'ok'})
        m.inc('test_total', {'status': 'fail'})
        assert m.get_counter('test_total', {'status': 'ok'}) == 1
        assert m.get_counter('test_total', {'status': 'fail'}) == 1

    def test_no_labels(self):
        """Counter with no labels should work."""
        m = MetricsRegistry()
        m.inc('simple_total')
        assert m.get_counter('simple_total') == 1

    def test_increment_by_value(self):
        """Counter should support incrementing by arbitrary values."""
        m = MetricsRegistry()
        m.inc('test_total', value=5)
        m.inc('test_total', value=3)
        assert m.get_counter('test_total') == 8

    def test_get_nonexistent_counter(self):
        """Getting a non-existent counter should return 0."""
        m = MetricsRegistry()
        assert m.get_counter('does_not_exist') == 0

    def test_get_nonexistent_labels(self):
        """Getting counter with wrong labels should return 0."""
        m = MetricsRegistry()
        m.inc('test_total', {'status': 'ok'})
        assert m.get_counter('test_total', {'status': 'missing'}) == 0

    def test_label_order_irrelevant(self):
        """Label order should not matter for counter identity."""
        m = MetricsRegistry()
        m.inc('test_total', {'a': '1', 'b': '2'})
        assert m.get_counter('test_total', {'b': '2', 'a': '1'}) == 1


class TestLabelSanitization:

    def test_escapes_quotes(self):
        assert _sanitize_label('path with "quotes"') == 'path with \\"quotes\\"'

    def test_escapes_backslash(self):
        assert _sanitize_label('C:\\Users') == 'C:\\\\Users'

    def test_escapes_newline(self):
        assert _sanitize_label('line1\nline2') == 'line1\\nline2'

    def test_plain_string_unchanged(self):
        assert _sanitize_label('simple_string') == 'simple_string'

    def test_empty_string(self):
        assert _sanitize_label('') == ''

    def test_numeric_value(self):
        """Should handle non-string values via str()."""
        assert _sanitize_label(42) == '42'


class TestFormatLabels:

    def test_single_label(self):
        result = _format_labels((('status', 'ok'),))
        assert result == 'status="ok"'

    def test_multiple_labels(self):
        result = _format_labels((('level', 'info'), ('service', 'zurg')))
        assert 'level="info"' in result
        assert 'service="zurg"' in result

    def test_empty_labels(self):
        result = _format_labels(())
        assert result == ''

    def test_labels_with_special_chars(self):
        result = _format_labels((('path', '/data/"test"'),))
        assert 'path="/data/\\"test\\""' in result


class TestFormatMetrics:
    """Integration-level assertions on format_metrics() output.

    Note: for dual-prefix (pd_zurg_*/zurgarr_*) symmetry assertions, add
    tests to `TestDualEmission` below — that is the authoritative home
    for the 2.19.0 deprecation-window contract. When the deprecation
    window closes in 2.20.0, both classes drop the legacy-prefix
    assertions in one pass.
    """

    def test_output_contains_up_gauge(self):
        """Formatted metrics should always contain pd_zurg_up (legacy prefix
        during the 2.19.0 deprecation window; TestDualEmission asserts the
        zurgarr_ counterpart)."""
        m = MetricsRegistry()
        output = m.format_metrics()
        assert 'pd_zurg_up 1' in output

    def test_output_contains_uptime(self):
        """Formatted metrics should contain uptime."""
        m = MetricsRegistry()
        output = m.format_metrics()
        assert 'pd_zurg_uptime_seconds' in output

    def test_output_contains_event_counters(self):
        """Formatted metrics should contain event counters."""
        m = MetricsRegistry()
        m.inc('events', {'level': 'info'}, 5)
        m.inc('events', {'level': 'error'}, 2)
        output = m.format_metrics()
        assert 'pd_zurg_events_total{level="info"} 5' in output
        assert 'pd_zurg_events_total{level="error"} 2' in output

    def test_output_ends_with_newline(self):
        """Prometheus format requires trailing newline."""
        m = MetricsRegistry()
        output = m.format_metrics()
        assert output.endswith('\n')

    def test_output_has_type_annotations(self):
        """Output should include TYPE and HELP lines."""
        m = MetricsRegistry()
        output = m.format_metrics()
        assert '# TYPE pd_zurg_up gauge' in output
        assert '# HELP pd_zurg_up' in output

    def test_blackhole_counters_included(self):
        """Blackhole counters should appear when set."""
        m = MetricsRegistry()
        m.inc('blackhole_processed', {'status': 'success'}, 10)
        output = m.format_metrics()
        assert 'pd_zurg_blackhole_processed_total{status="success"} 10' in output


class TestDualEmission:
    """Plan 35 Phase 2: during the 2.19.0 → 2.20.0 deprecation window every
    metric is exported under BOTH the legacy `pd_zurg_*` prefix and the new
    `zurgarr_*` prefix. These tests lock down the dual-emission contract —
    sample values and labels must be byte-identical between prefixes;
    only the legacy HELP line carries the DEPRECATED marker.
    """

    def test_up_gauge_emitted_under_both_prefixes(self):
        m = MetricsRegistry()
        output = m.format_metrics()
        assert 'pd_zurg_up 1' in output
        assert 'zurgarr_up 1' in output

    def test_uptime_emitted_under_both_prefixes(self):
        m = MetricsRegistry()
        output = m.format_metrics()
        assert re.search(r'^pd_zurg_uptime_seconds \S+$', output, re.MULTILINE)
        assert re.search(r'^zurgarr_uptime_seconds \S+$', output, re.MULTILINE)

    def test_event_counters_emitted_under_both_prefixes(self):
        m = MetricsRegistry()
        m.inc('events', {'level': 'info'}, 5)
        m.inc('events', {'level': 'error'}, 2)
        output = m.format_metrics()
        assert 'pd_zurg_events_total{level="info"} 5' in output
        assert 'zurgarr_events_total{level="info"} 5' in output
        assert 'pd_zurg_events_total{level="error"} 2' in output
        assert 'zurgarr_events_total{level="error"} 2' in output

    def test_blackhole_counters_emitted_under_both_prefixes(self):
        m = MetricsRegistry()
        m.inc('blackhole_processed', {'status': 'success'}, 10)
        output = m.format_metrics()
        assert 'pd_zurg_blackhole_processed_total{status="success"} 10' in output
        assert 'zurgarr_blackhole_processed_total{status="success"} 10' in output

    def test_type_annotations_present_for_both_prefixes(self):
        m = MetricsRegistry()
        output = m.format_metrics()
        assert '# TYPE pd_zurg_up gauge' in output
        assert '# TYPE zurgarr_up gauge' in output
        assert '# TYPE pd_zurg_events_total counter' in output
        assert '# TYPE zurgarr_events_total counter' in output

    def test_legacy_help_carries_deprecation_marker(self):
        """Every pd_zurg_* HELP line must contain the DEPRECATED marker
        pointing at the new metric name and removal version. This is the
        cue Grafana / alerting users see when hovering the metric in the
        UI."""
        m = MetricsRegistry()
        output = m.format_metrics()
        legacy_help_lines = [
            line for line in output.splitlines()
            if line.startswith('# HELP pd_zurg_')
        ]
        assert legacy_help_lines, 'no pd_zurg_* HELP lines emitted'
        for line in legacy_help_lines:
            assert 'DEPRECATED' in line, f'missing DEPRECATED marker: {line}'
            assert '2.20.0' in line, f'missing removal version: {line}'
            # The suffix should name the replacement metric explicitly.
            name = line.split()[2][len('pd_zurg_'):]
            assert f'zurgarr_{name}' in line, (
                f'HELP line does not name the replacement zurgarr_{name}: {line}'
            )

    def test_new_help_does_not_carry_deprecation_marker(self):
        """The new zurgarr_* side of each pair is the canonical form —
        its HELP must be clean."""
        m = MetricsRegistry()
        output = m.format_metrics()
        new_help_lines = [
            line for line in output.splitlines()
            if line.startswith('# HELP zurgarr_')
        ]
        assert new_help_lines, 'no zurgarr_* HELP lines emitted'
        for line in new_help_lines:
            assert 'DEPRECATED' not in line, f'unexpected DEPRECATED: {line}'

    def test_every_legacy_metric_has_new_counterpart(self):
        """Invariant: each pd_zurg_X metric sample has a byte-identical
        zurgarr_X counterpart (labels + value). Catches cases where a new
        metric is added under only one prefix.

        status_data.to_dict is patched at the class level (like the pattern
        in tests/test_system_stats.py::TestMetricsNewGauges) so the
        normally-gated process/mount/service metric families also fire —
        without this the test only exercises the events/blackhole paths
        and a future one-prefix regression in the gated families would
        slip through.
        """
        from unittest.mock import patch
        from utils.status_server import StatusData

        m = MetricsRegistry()
        m.inc('events', {'level': 'info'}, 3)
        m.inc('blackhole_processed', {'status': 'success'}, 7)
        m.inc('blackhole_retry', value=2)

        fake_status = {
            'version': '0.0.0',
            'uptime_seconds': 12345,
            'processes': [
                {'name': 'zurg', 'running': True, 'restart_count': 0},
                {'name': 'rclone', 'running': False, 'restart_count': 2},
            ],
            'mounts': [
                {'path': '/data/zurgarr', 'mounted': True, 'accessible': True},
            ],
            'services': [
                {'name': 'plex', 'type': 'media', 'status': 'ok'},
                {'name': 'sonarr', 'type': 'arr', 'status': 'down'},
            ],
            'system': {
                'memory_percent': 42.5, 'memory_used_bytes': 1024000,
                'cpu_percent': 7.2,
                'disk_used_bytes': 500000, 'disk_total_bytes': 1000000,
                'disk_percent': 50.0,
                'fd_open': 142, 'fd_max': 1048576,
                'net_rx_bytes': 5000000, 'net_tx_bytes': 2000000,
            },
            'recent_events': [], 'error_count': 0, 'provider_health': {},
        }

        with patch.object(StatusData, 'to_dict', return_value=fake_status):
            output = m.format_metrics()

        legacy_samples = {}
        new_samples = {}
        for line in output.splitlines():
            if line.startswith('#') or not line.strip():
                continue
            if line.startswith('pd_zurg_'):
                rest = line[len('pd_zurg_'):]
                legacy_samples[rest] = line
            elif line.startswith('zurgarr_'):
                rest = line[len('zurgarr_'):]
                new_samples[rest] = line

        assert set(legacy_samples.keys()) == set(new_samples.keys()), (
            f'prefix mismatch\n'
            f'legacy-only: {set(legacy_samples) - set(new_samples)}\n'
            f'new-only:    {set(new_samples) - set(legacy_samples)}'
        )
        # Sanity: every gated metric family fired.
        suffixes = {rest.split('{')[0].split(' ')[0] for rest in legacy_samples}
        for expected in (
            'up', 'uptime_seconds',
            'process_running', 'process_restart_total',
            'mount_mounted', 'mount_accessible',
            'service_up',
            'memory_usage_percent', 'cpu_usage_percent',
            'disk_used_bytes', 'fd_open', 'net_rx_bytes_total',
            'events_total', 'blackhole_processed_total', 'blackhole_retry_total',
        ):
            assert expected in suffixes, f'{expected!r} family did not emit'


class TestEmitHelper:
    """Direct unit tests for the `_emit` helper — isolates dual-emission
    formatting logic from the format_metrics orchestration so the helper
    contract is clear and regressions are caught at the unit level."""

    def test_emits_both_prefixes_for_no_label_metric(self):
        lines = []
        _emit(lines, 'my_metric', 'My help', 'gauge', [('', 42)])
        assert (
            '# HELP pd_zurg_my_metric My help '
            '(DEPRECATED since 2.19.0 — use zurgarr_my_metric; removed in 2.20.0)'
        ) in lines
        assert '# TYPE pd_zurg_my_metric gauge' in lines
        assert 'pd_zurg_my_metric 42' in lines
        assert '# HELP zurgarr_my_metric My help' in lines
        assert '# TYPE zurgarr_my_metric gauge' in lines
        assert 'zurgarr_my_metric 42' in lines

    def test_emits_labeled_samples_under_both_prefixes(self):
        lines = []
        samples = [
            ('status="ok"', 5),
            ('status="fail"', 2),
        ]
        _emit(lines, 'things_total', 'Thing count', 'counter', samples)
        assert 'pd_zurg_things_total{status="ok"} 5' in lines
        assert 'pd_zurg_things_total{status="fail"} 2' in lines
        assert 'zurgarr_things_total{status="ok"} 5' in lines
        assert 'zurgarr_things_total{status="fail"} 2' in lines

    def test_trailing_blank_separator(self):
        lines = []
        _emit(lines, 'm', 'H', 'gauge', [('', 1)])
        assert lines[-1] == '', 'last line must be blank separator'

    def test_sample_values_byte_identical_between_prefixes(self):
        """The legacy and new sample lines for a given (labels, value) must
        differ ONLY in the prefix — no rounding, no label reordering."""
        lines = []
        _emit(lines, 'm', 'H', 'gauge', [('a="1",b="2"', 3.14159)])
        legacy = [l for l in lines if l.startswith('pd_zurg_m')]
        new = [l for l in lines if l.startswith('zurgarr_m')]
        assert len(legacy) == 1 and len(new) == 1
        assert legacy[0] == 'pd_zurg_m{a="1",b="2"} 3.14159'
        assert new[0] == 'zurgarr_m{a="1",b="2"} 3.14159'

    def test_samples_generator_is_materialised(self):
        """_emit must consume the samples iterable only once — if a caller
        passes a generator, both the legacy AND new prefix must still see
        the samples. Earlier versions of _emit iterated `samples` inside
        the two-prefix loop directly; a generator would exhaust on the
        first prefix and the second would silently emit HELP+TYPE with
        no sample lines."""
        def _gen():
            yield 'label="a"', 1
            yield 'label="b"', 2

        lines = []
        _emit(lines, 'g', 'H', 'counter', _gen())
        legacy = [l for l in lines if l.startswith('pd_zurg_g{')]
        new = [l for l in lines if l.startswith('zurgarr_g{')]
        assert len(legacy) == 2, f'legacy prefix missing samples: {lines}'
        assert len(new) == 2, f'new prefix missing samples: {lines}'
