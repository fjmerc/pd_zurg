"""Regression tests for env-boolean truthiness (audit findings #3, #5).

Boolean env configs are strings; 'false' is truthy in Python, so any
`if VAR:` / `bool(VAR)` gate silently enables a feature the user turned
off. DUPLICATE_CLEANUP=false with KEEP=zurg deletes local media."""
import re
from pathlib import Path

import pytest

MAIN_PY = Path(__file__).resolve().parent.parent / 'main.py'


@pytest.mark.parametrize('pattern,description', [
    (r'bool\(ZURGUPDATE\)', 'ZURG_UPDATE gate must use str().lower() == "true"'),
    (r'if\s+DUPECLEAN\s*:', 'DUPLICATE_CLEANUP gate must use str().lower() == "true"'),
    (r'if\s+PDUPDATE\s+and\s+PDREPO', 'PD_UPDATE gate must use str().lower() == "true"'),
])
def test_main_py_has_no_truthiness_bool_gates(pattern, description):
    source = MAIN_PY.read_text()
    assert not re.search(pattern, source), description


@pytest.mark.parametrize('dupeclean_value', ['false', 'False', '', None, '0', 'no'])
def test_duplicate_cleanup_setup_disabled(monkeypatch, dupeclean_value):
    from utils import duplicate_cleanup as dc
    monkeypatch.setattr(dc, 'PLEXADD', 'http://plex:32400', raising=False)
    monkeypatch.setattr(dc, 'PLEXTOKEN', 'tok', raising=False)
    monkeypatch.setattr(dc, 'RCLONEMN', 'zurgarr', raising=False)
    monkeypatch.setattr(dc, 'DUPECLEAN', dupeclean_value, raising=False)
    registered = []
    from utils import task_scheduler
    monkeypatch.setattr(task_scheduler.scheduler, 'register',
                        lambda *a, **k: registered.append(a))
    dc.setup()
    assert registered == [], (
        f'DUPLICATE_CLEANUP={dupeclean_value!r} must NOT register the cleanup task')


def test_duplicate_cleanup_setup_enabled(monkeypatch):
    from utils import duplicate_cleanup as dc
    monkeypatch.setattr(dc, 'PLEXADD', 'http://plex:32400', raising=False)
    monkeypatch.setattr(dc, 'PLEXTOKEN', 'tok', raising=False)
    monkeypatch.setattr(dc, 'RCLONEMN', 'zurgarr', raising=False)
    monkeypatch.setattr(dc, 'DUPECLEAN', 'true', raising=False)
    registered = []
    from utils import task_scheduler
    monkeypatch.setattr(task_scheduler.scheduler, 'register',
                        lambda *a, **k: registered.append(a))
    dc.setup()
    assert len(registered) == 1
