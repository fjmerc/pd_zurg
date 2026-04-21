"""Per-process deprecation-warning helper for the Zurgarr rename window.

Identifiers retained from the pd_zurg era (env var keys, Prometheus metric
prefixes, localStorage keys, on-disk file extensions) are being renamed
across a two-release deprecation window — dual-read / dual-emit lands in
2.19.0, legacy names are removed in 2.20.0.

``warn_once`` dedupes via a module-level set keyed on ``(surface, old_name)``
so the same deprecation fires at most once per process lifetime, regardless
of how many times the legacy name is consulted at runtime (every
``get_logger()`` call re-reads the env vars, for example).

Emission can happen before the Zurgarr logger's handlers are attached — the
first env-var read fires from inside ``get_logger()`` itself, *before* the
rotating-file handler is installed. To avoid silently dropping deprecation
notices from the log file, warnings fired in that pre-handler window are
buffered on ``_pending`` and flushed by ``flush_pending()`` once handlers
are ready. After flush, subsequent warnings emit directly through the
logging system.

This module intentionally depends only on ``logging`` and ``threading``
from the stdlib so it can be imported from anywhere — including
``base/__init__.py`` — without introducing an import cycle.
"""

import logging
import threading


# The internal logger retains the 'PDZURG' name during the deprecation
# window because ``get_logger()`` still instantiates it under that name
# (the rotating file handler and the console handler are attached there).
# Renaming the logger would orphan existing log viewers and is a separate
# piece of work outside plan 35's scope. The user-visible deprecation
# content is branded Zurgarr via the message text itself.
_LOGGER_NAME = 'PDZURG'

_fired = set()
_fired_lock = threading.Lock()

_pending = []
_pending_lock = threading.Lock()


def warn_once(old_name, new_name, surface, since, removed_in):
    key = (surface, old_name)
    with _fired_lock:
        if key in _fired:
            return
        _fired.add(key)
    payload = (old_name, new_name, surface, since, removed_in)
    logger = logging.getLogger(_LOGGER_NAME)
    # ``hasHandlers()`` walks the propagate chain up to root, so this check
    # returns True both when ``get_logger()`` has attached the rotating file
    # handler directly (production) and when a test harness has attached a
    # handler to root via propagation (pytest's ``caplog`` fixture).
    if logger.hasHandlers():
        _emit(logger, payload)
    else:
        with _pending_lock:
            _pending.append(payload)


def flush_pending():
    """Emit any deprecation warnings that were buffered before the logger's
    handlers were attached.

    Called from ``get_logger()`` after handler installation so early-startup
    warnings (fired while the rotating-file handler did not yet exist)
    still land in the log file.
    """
    logger = logging.getLogger(_LOGGER_NAME)
    with _pending_lock:
        pending = list(_pending)
        _pending.clear()
    for payload in pending:
        _emit(logger, payload)


def _emit(logger, payload):
    old_name, new_name, surface, since, removed_in = payload
    logger.warning(
        "[deprecation] %s '%s' is deprecated since %s; use '%s' instead. "
        "Legacy name will be removed in %s.",
        surface, old_name, since, new_name, removed_in,
    )


def _reset_for_tests():
    """Clear dedupe + pending state. Test-only — do not call from production code."""
    with _fired_lock:
        _fired.clear()
    with _pending_lock:
        _pending.clear()
