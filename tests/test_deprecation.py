"""Tests for utils.deprecation.warn_once and base._env_dual (plan 35 Phase 1)."""

import logging
import os
import pytest

from utils.deprecation import warn_once, flush_pending, _reset_for_tests


@pytest.fixture(autouse=True)
def _clear_fired():
    """Ensure every test starts with empty dedupe + pending state."""
    _reset_for_tests()
    yield
    _reset_for_tests()


@pytest.fixture
def _with_pdzurg_handler():
    """Attach a StreamHandler to the PDZURG logger so warn_once emits directly
    instead of buffering. Mimics the post-`get_logger()` runtime state."""
    import io
    logger = logging.getLogger('PDZURG')
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.WARNING)
    logger.addHandler(handler)
    original_level = logger.level
    logger.setLevel(logging.WARNING)
    yield
    logger.removeHandler(handler)
    logger.setLevel(original_level)


class TestWarnOnce:

    def test_fires_once(self, caplog, _with_pdzurg_handler):
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            warn_once('OLD_NAME', 'NEW_NAME', 'env var', '2.19.0', '2.20.0')
            warn_once('OLD_NAME', 'NEW_NAME', 'env var', '2.19.0', '2.20.0')
            warn_once('OLD_NAME', 'NEW_NAME', 'env var', '2.19.0', '2.20.0')

        dep_records = [r for r in caplog.records if '[deprecation]' in r.getMessage()]
        assert len(dep_records) == 1

    def test_different_old_names_each_fire(self, caplog, _with_pdzurg_handler):
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            warn_once('OLD_A', 'NEW_A', 'env var', '2.19.0', '2.20.0')
            warn_once('OLD_B', 'NEW_B', 'env var', '2.19.0', '2.20.0')

        dep_records = [r for r in caplog.records if '[deprecation]' in r.getMessage()]
        assert len(dep_records) == 2

    def test_same_old_name_different_surface_each_fire(self, caplog, _with_pdzurg_handler):
        """Surface is part of the dedupe key — metric and env var rename sharing a
        name should both warn."""
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            warn_once('FOO', 'BAR', 'env var', '2.19.0', '2.20.0')
            warn_once('FOO', 'BAR', 'metric', '2.19.0', '2.20.0')

        dep_records = [r for r in caplog.records if '[deprecation]' in r.getMessage()]
        assert len(dep_records) == 2

    def test_message_content(self, caplog, _with_pdzurg_handler):
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            warn_once('PDZURG_LOG_LEVEL', 'ZURGARR_LOG_LEVEL', 'env var', '2.19.0', '2.20.0')

        dep_records = [r for r in caplog.records if '[deprecation]' in r.getMessage()]
        assert len(dep_records) == 1
        msg = dep_records[0].getMessage()
        assert 'PDZURG_LOG_LEVEL' in msg
        assert 'ZURGARR_LOG_LEVEL' in msg
        assert 'env var' in msg
        assert '2.19.0' in msg
        assert '2.20.0' in msg
        assert 'deprecation' in msg.lower()

    def test_level_is_warning(self, caplog, _with_pdzurg_handler):
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            warn_once('OLD', 'NEW', 'env var', '2.19.0', '2.20.0')
        dep_records = [r for r in caplog.records if '[deprecation]' in r.getMessage()]
        assert len(dep_records) == 1
        assert dep_records[0].levelno == logging.WARNING


@pytest.fixture
def _isolated_pdzurg():
    """Simulate the cold-start state: the PDZURG logger has no handlers AND
    propagation to root is disabled, so ``logger.hasHandlers()`` returns
    False and ``warn_once`` takes the buffering path regardless of whether
    the test harness has a root-level handler attached (pytest's caplog
    attaches to root; without this fixture the emit path would run and the
    buffer path would never be exercised).
    """
    logger = logging.getLogger('PDZURG')
    original_handlers = list(logger.handlers)
    original_propagate = logger.propagate
    for h in original_handlers:
        logger.removeHandler(h)
    logger.propagate = False
    yield logger
    logger.propagate = original_propagate
    for h in original_handlers:
        logger.addHandler(h)


class TestPendingBuffer:
    """`warn_once` fired before any handler is attached to the PDZURG logger
    must buffer the warning so ``flush_pending()`` can replay it once the
    rotating file handler is ready. Protects the Phase 1 cold-start path
    in ``get_logger()``, where the first env-var read fires BEFORE handlers
    are attached — without buffering, the deprecation warning lands on
    stderr only (via Python's lastResort) and never reaches the log file.
    """

    def test_pre_handler_warning_buffered_and_flushed(self, caplog, _isolated_pdzurg):
        """No handler attached → warn_once buffers until flush_pending fires."""
        from utils.deprecation import _pending
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            warn_once('OLD_BUF', 'NEW_BUF', 'env var', '2.19.0', '2.20.0')
            # Warning was buffered, not captured.
            pre_flush_records = [r for r in caplog.records if 'OLD_BUF' in r.getMessage()]
            assert pre_flush_records == []
            assert any('OLD_BUF' in entry[0] for entry in _pending)

            # Restore propagate so flush_pending's emission reaches caplog
            # (real `get_logger()` would have attached a real handler here).
            _isolated_pdzurg.propagate = True
            flush_pending()

            post_flush_records = [r for r in caplog.records if 'OLD_BUF' in r.getMessage()]
            assert len(post_flush_records) == 1

    def test_post_handler_warning_emits_immediately(self, caplog, _with_pdzurg_handler):
        """Handler attached → warn_once emits directly, no buffering needed."""
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            warn_once('OLD_DIRECT', 'NEW_DIRECT', 'env var', '2.19.0', '2.20.0')
            dep_records = [r for r in caplog.records if 'OLD_DIRECT' in r.getMessage()]
            assert len(dep_records) == 1
        flush_pending()  # No-op — pending list is empty.
        dep_records = [r for r in caplog.records if 'OLD_DIRECT' in r.getMessage()]
        assert len(dep_records) == 1

    def test_flush_idempotent(self, caplog, _isolated_pdzurg):
        """Flushing twice doesn't double-emit — pending list is cleared on first flush."""
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            warn_once('OLD_IDEM', 'NEW_IDEM', 'env var', '2.19.0', '2.20.0')
            _isolated_pdzurg.propagate = True
            flush_pending()
            flush_pending()
            dep_records = [r for r in caplog.records if 'OLD_IDEM' in r.getMessage()]
            assert len(dep_records) == 1


class TestEnvDual:

    def _env_dual(self):
        from base import _env_dual
        return _env_dual

    def test_neither_set_returns_default(self, clean_env):
        result = self._env_dual()('ZURGARR_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'INFO')
        assert result == 'INFO'

    def test_neither_set_default_empty(self, clean_env):
        result = self._env_dual()('ZURGARR_LOG_LEVEL', 'PDZURG_LOG_LEVEL')
        assert result == ''

    def test_old_only_returns_old_and_warns(self, clean_env, env_vars, caplog):
        env_vars(PDZURG_LOG_LEVEL='DEBUG')
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            result = self._env_dual()('ZURGARR_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'INFO')
        assert result == 'DEBUG'
        dep_records = [r for r in caplog.records if '[deprecation]' in r.getMessage()]
        assert len(dep_records) == 1
        assert 'PDZURG_LOG_LEVEL' in dep_records[0].getMessage()

    def test_new_only_returns_new_no_warn(self, clean_env, env_vars, caplog):
        env_vars(ZURGARR_LOG_LEVEL='DEBUG')
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            result = self._env_dual()('ZURGARR_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'INFO')
        assert result == 'DEBUG'
        dep_records = [r for r in caplog.records if '[deprecation]' in r.getMessage()]
        assert dep_records == []

    def test_both_set_new_wins_no_warn(self, clean_env, env_vars, caplog):
        env_vars(ZURGARR_LOG_LEVEL='DEBUG', PDZURG_LOG_LEVEL='ERROR')
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            result = self._env_dual()('ZURGARR_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'INFO')
        assert result == 'DEBUG'
        dep_records = [r for r in caplog.records if '[deprecation]' in r.getMessage()]
        assert dep_records == []

    def test_old_empty_string_treated_as_unset(self, clean_env, env_vars):
        """Empty string should fall through to the default (matches bash ${VAR:-default})."""
        env_vars(PDZURG_LOG_LEVEL='   ')
        result = self._env_dual()('ZURGARR_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'INFO')
        assert result == 'INFO'

    def test_old_value_stripped(self, clean_env, env_vars):
        env_vars(PDZURG_LOG_LEVEL='  DEBUG  ')
        result = self._env_dual()('ZURGARR_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'INFO')
        assert result == 'DEBUG'

    def test_warning_fires_once_across_calls(self, clean_env, env_vars, caplog):
        """Multiple _env_dual calls for the same old name share one deprecation warning."""
        env_vars(PDZURG_LOG_LEVEL='DEBUG')
        with caplog.at_level(logging.WARNING, logger='PDZURG'):
            self._env_dual()('ZURGARR_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'INFO')
            self._env_dual()('ZURGARR_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'INFO')
            self._env_dual()('ZURGARR_LOG_LEVEL', 'PDZURG_LOG_LEVEL', 'INFO')
        dep_records = [r for r in caplog.records if '[deprecation]' in r.getMessage()]
        assert len(dep_records) == 1
