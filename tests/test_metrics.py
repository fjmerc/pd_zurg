"""Tests for Prometheus metrics formatting."""

import pytest
from utils.metrics import MetricsRegistry, _sanitize_label, _format_labels


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

    def test_output_contains_up_gauge(self):
        """Formatted metrics should always contain pd_zurg_up."""
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
