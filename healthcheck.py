from base import *
from utils.logger import *
import time
import urllib.request
import urllib.error


# FUSE liveness probe budget.  A *dead* mount raises ENOTCONN
# instantly; only a slow / rate-limited one (e.g. a TorBox FUSE walk
# being 429-throttled) blocks.  ``_MOUNT_PROBE_TIMEOUT_SEC`` caps any
# single mount's listdir; ``_MOUNT_PROBE_BUDGET_SEC`` caps the SUM
# across all mounts so a 3-mount install (RD+AD+TB) all slow at once
# can't blow Docker's 10s healthcheck timeout (6s mounts + 3s status
# probe = 9s worst case).  The per-mount cap still applies so one slow
# mount can't starve the budget before a later (possibly dead) mount
# gets probed.
_MOUNT_PROBE_TIMEOUT_SEC = 3
_MOUNT_PROBE_BUDGET_SEC = 6
_STATUS_PROBE_TIMEOUT_SEC = 3


def check_processes(process_info):
    found_processes = {key: False for key in process_info.keys()}

    for proc in psutil.process_iter():
        try:
            cmdline = ' '.join(proc.cmdline())
            for process_name, info in process_info.items():
                if info['regex'].search(cmdline):
                    found_processes[process_name] = True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    return found_processes


def _listdir_with_timeout(path, timeout):
    """``os.listdir(path)`` bounded by a wall-clock timeout.

    ``signal.alarm`` is main-thread-only and brittle, so use a daemon
    thread with a bounded join (mirrors
    ``utils/scheduled_tasks._listdir_with_timeout``).  The healthcheck
    is a short-lived process, so a worker leaked on timeout dies with
    the interpreter — no accumulation across probes.
    """
    import threading
    result = {}

    def _run():
        try:
            result['entries'] = os.listdir(path)
        except BaseException as exc:  # noqa: BLE001 - propagate to caller
            result['exc'] = exc

    t = threading.Thread(target=_run, name=f'hc-probe-{os.path.basename(path)}', daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f'listdir({path!r}) did not return within {timeout}s')
    if 'exc' in result:
        raise result['exc']
    return result['entries']


def _mount_alive(mount_path, timeout=None):
    # ``os.path.ismount`` catches an unmounted dir; the bounded
    # ``listdir`` catches a half-stuck FUSE (mount table entry persists,
    # rclone process dead → every traversal returns ENOTCONN).  Without
    # the listdir probe a dead FUSE looks healthy because the kernel
    # still reports the mount in /proc/self/mountinfo.
    if timeout is None:
        timeout = _MOUNT_PROBE_TIMEOUT_SEC
    if not os.path.ismount(mount_path):
        return False, "not a mount point"
    try:
        _listdir_with_timeout(mount_path, timeout)
    except TimeoutError:
        # Slow-but-alive must NOT flap the container to unhealthy.  A
        # dead mount raises ENOTCONN immediately (caught below); a
        # rate-limited TorBox FUSE walk (429 storm) just blocks until the
        # deadline.  Treat the timeout as alive-and-slow so a busy mount
        # doesn't fail the 10s Docker healthcheck — the scheduler's
        # mount_liveness probe still surfaces persistent slowness on the
        # System page.
        print(f"Warning: mount {mount_path} slow (listdir > "
              f"{timeout:.1f}s) — treating as alive", file=sys.stderr)
        return True, ""
    except Exception as exc:  # noqa: BLE001 — any hard failure means mount is unusable
        return False, f"{type(exc).__name__}: {exc}"
    return True, ""


def _status_server_alive(port, timeout):
    """Is the status UI accepting connections on ``port``?

    An HTTP status code (e.g. 401 from the auth-gated UI) IS a response —
    the server is up and serving — so an ``HTTPError`` counts as alive.
    Only a connection-level failure (refused, reset, timeout) means the
    server is genuinely not responding.
    """
    try:
        urllib.request.urlopen(f'http://localhost:{port}/', timeout=timeout)
    except urllib.error.HTTPError:
        return True
    except Exception:  # noqa: BLE001 — URLError / timeout / socket error → down
        return False
    return True


def main():
    try:
        error_messages = []

        # Dual-provider mount name derivation (must match rclone/rclone.py)
        if RDAPIKEY and ADAPIKEY and RCLONEMN:
            RCLONEMN_RD = f"{RCLONEMN}_RD"
            RCLONEMN_AD = f"{RCLONEMN}_AD"
        else:
            RCLONEMN_RD = RCLONEMN_AD = RCLONEMN

        mount_type = "serve nfs" if NFSMOUNT is not None and str(NFSMOUNT).lower() == 'true' else "mount"

        plex_debrid_should_run = str(PLEXDEBRID).lower() == 'true' and (
            os.getenv('PLEX_CONNECTED', 'False') == 'True'
            or bool(os.getenv('JF_API_KEY', '').strip())
        )

        # TorBox mount is set up by rclone/rclone.py iff API key + WebDAV
        # creds are all present — must match _torbox_mount_configured() there.
        torbox_mount_configured = bool(TORBOXAPIKEY and TORBOXWEBDAVUSER and TORBOXWEBDAVPASS)

        process_info = {
            "zurg_rd": {
                "regex": re.compile(r'/zurg/RD/zurg', re.IGNORECASE),
                "error_message": "The Zurg RD process is not running.",
                "should_run": str(ZURG).lower() == 'true' and RDAPIKEY
            },
            "zurg_ad": {
                "regex": re.compile(r'/zurg/AD/zurg', re.IGNORECASE),
                "error_message": "The Zurg AD process is not running.",
                "should_run": str(ZURG).lower() == 'true' and ADAPIKEY
            },
            "plex_debrid": {
                "regex": re.compile(r'python ./plex_debrid/main.py --config-dir /config'),
                "error_message": "The plex_debrid process is not running.",
                "should_run": plex_debrid_should_run
            },
            "rclonemn_rd": {
                "regex": re.compile(rf'rclone {mount_type} {re.escape(RCLONEMN_RD)}:'),
                "error_message": f"The Rclone RD process for {RCLONEMN_RD} is not running.",
                "should_run": str(ZURG).lower() == 'true' and RDAPIKEY and os.path.exists(f'/healthcheck/{RCLONEMN_RD}')
            },
            "rclonemn_ad": {
                "regex": re.compile(rf'rclone {mount_type} {re.escape(RCLONEMN_AD)}:'),
                "error_message": f"The Rclone AD process for {RCLONEMN_AD} is not running.",
                "should_run": str(ZURG).lower() == 'true' and ADAPIKEY and os.path.exists(f'/healthcheck/{RCLONEMN_AD}')
            },
            "rclonemn_torbox": {
                "regex": re.compile(rf'rclone {mount_type} {re.escape(TORBOX_MOUNT_NAME)}:'),
                "error_message": f"The Rclone TorBox process for {TORBOX_MOUNT_NAME} is not running.",
                "should_run": torbox_mount_configured and os.path.exists(f'/healthcheck/{TORBOX_MOUNT_NAME}')
            }
        }

        process_status = check_processes(process_info)

        for process_name, info in process_info.items():
            if info["should_run"] and not process_status[process_name]:
                error_messages.append(info["error_message"])

        # Shared wall-clock budget for ALL mount probes.  The per-mount cap
        # (_MOUNT_PROBE_TIMEOUT_SEC) bounds any single slow mount; this
        # deadline bounds their SUM so a 3-mount install (RD+AD+TB) all slow
        # at once can't stack 3×3s and blow Docker's 10s healthcheck cap.
        mount_probe_deadline = time.monotonic() + _MOUNT_PROBE_BUDGET_SEC

        def _probe_budget():
            # Never below 0.5s: a tiny-but-nonzero window still lets a *dead*
            # mount raise ENOTCONN instantly (the failure we must catch),
            # while a slow one trips the timeout and is treated as alive.
            return max(0.5, min(_MOUNT_PROBE_TIMEOUT_SEC,
                                mount_probe_deadline - time.monotonic()))

        if str(ZURG).lower() == 'true':
            if RDAPIKEY and os.path.exists(f'/healthcheck/{RCLONEMN_RD}'):
                mp = f'/data/{RCLONEMN_RD}'
                alive, why = _mount_alive(mp, _probe_budget())
                if not alive:
                    error_messages.append(f"Rclone mount {mp} is not active ({why}).")
            if ADAPIKEY and os.path.exists(f'/healthcheck/{RCLONEMN_AD}'):
                mp = f'/data/{RCLONEMN_AD}'
                alive, why = _mount_alive(mp, _probe_budget())
                if not alive:
                    error_messages.append(f"Rclone mount {mp} is not active ({why}).")
        # TB mount is NOT under the ZURG guard: TorBox uses its own WebDAV
        # endpoint (webdav.torbox.app) and does not require Zurg to be
        # enabled.  A TB-only setup with ZURG=false is supported.
        if torbox_mount_configured and os.path.exists(f'/healthcheck/{TORBOX_MOUNT_NAME}'):
            mp = f'/data/{TORBOX_MOUNT_NAME}'
            alive, why = _mount_alive(mp, _probe_budget())
            if not alive:
                error_messages.append(f"Rclone mount {mp} is not active ({why}).")

        # Status server responsiveness (non-fatal — log warning but don't
        # fail healthcheck).  Kept short so it can't, together with the
        # bounded mount probes, push the whole check past Docker's 10s cap.
        port = int(os.environ.get('STATUS_UI_PORT', '8080'))
        if not _status_server_alive(port, _STATUS_PROBE_TIMEOUT_SEC):
            print("Warning: Status server is not responding on port " + str(port), file=sys.stderr)

        if error_messages:
            error_message_combined = " | ".join(error_messages)
            raise Exception(error_message_combined)

    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
