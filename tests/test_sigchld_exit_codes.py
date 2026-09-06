"""SIGCHLD disposition regression tests (audit finding #1, CRITICAL).

signal.SIG_IGN on SIGCHLD makes the kernel auto-reap children; CPython's
subprocess then maps waitpid ECHILD to returncode 0 for EVERY child, so
check=True never raises and every exit-status branch in the codebase is
dead (umount fallback, rclone obscure failure, crash exit logging)."""
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

MAIN_PY = Path(__file__).resolve().parent.parent / 'main.py'


def test_main_py_does_not_ignore_sigchld():
    source = MAIN_PY.read_text()
    assert not re.search(r'SIGCHLD\s*,\s*signal\.SIG_IGN', source), (
        'SIG_IGN on SIGCHLD forces every subprocess returncode to 0')
    assert re.search(r'SIGCHLD\s*,\s*signal\.SIG_DFL', source), (
        'main.py must explicitly set SIGCHLD to SIG_DFL')


def test_exit_codes_visible_under_sig_dfl():
    """The behavior SIG_IGN destroyed: nonzero exit codes must be seen."""
    old = signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    try:
        r = subprocess.run([sys.executable, '-c', 'import sys; sys.exit(7)'])
        assert r.returncode == 7
    finally:
        signal.signal(signal.SIGCHLD, old)


def test_reap_orphans_exists_and_never_raises():
    from utils import processes
    old = signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    try:
        # No children at all -> ChildProcessError path must be swallowed
        processes._reap_orphans()
        # An exited child is drained without raising (it may equally be
        # claimed by subprocess internals first — both outcomes are fine)
        p = subprocess.Popen(['true'])
        deadline = time.time() + 5
        while p.poll() is None and time.time() < deadline:
            time.sleep(0.05)
        processes._reap_orphans()
    finally:
        signal.signal(signal.SIGCHLD, old)


def test_write_zurg_remote_logs_error_on_obscure_failure(monkeypatch, tmp_path):
    """With real exit codes, `rclone obscure` failure returns None; the
    config writer must scream, not silently omit user/pass (which yields
    an unexplained 401 mount failure)."""
    from rclone import rclone as rclone_mod
    monkeypatch.setattr(rclone_mod, 'ZURGUSER', 'user', raising=False)
    monkeypatch.setattr(rclone_mod, 'ZURGPASS', 'pass', raising=False)
    monkeypatch.setattr(rclone_mod, 'obscure_password', lambda p: None)
    errors = []
    monkeypatch.setattr(rclone_mod.logger, 'error',
                        lambda msg, *a, **k: errors.append(msg))
    cfg = tmp_path / 'config.yml'
    cfg.write_text('port: 9999\n')
    out = tmp_path / 'rclone.config'
    with open(out, 'w') as f:
        rclone_mod._write_zurg_remote(f, 'zurgarr', str(cfg))
    assert any('obscure' in e.lower() for e in errors), (
        'silent credential omission — user gets an unexplained 401')
    assert 'pass =' not in out.read_text()
