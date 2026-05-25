from base import *
from utils.logger import *
import urllib.request


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

    # Mount liveness — verify FUSE mount is active, not just rclone process.
    # ``os.path.ismount`` catches an unmounted dir; ``os.listdir`` inside a
    # try/except catches a half-stuck FUSE (mount table entry persists,
    # rclone process dead → every traversal returns ENOTCONN).  Without the
    # listdir probe a dead FUSE looks healthy because the kernel still
    # reports the mount in /proc/self/mountinfo.  Exception catch is
    # broader than ``OSError`` because libfuse bindings can also surface
    # decode/parse errors on corrupted dentry blocks — any failure here
    # is a real mount problem the operator needs to see.
    def _mount_alive(mount_path):
        if not os.path.ismount(mount_path):
            return False, "not a mount point"
        try:
            os.listdir(mount_path)
        except Exception as exc:  # noqa: BLE001 — any failure means mount is unusable
            return False, f"{type(exc).__name__}: {exc}"
        return True, ""

    if str(ZURG).lower() == 'true':
        if RDAPIKEY and os.path.exists(f'/healthcheck/{RCLONEMN_RD}'):
            mp = f'/data/{RCLONEMN_RD}'
            alive, why = _mount_alive(mp)
            if not alive:
                error_messages.append(f"Rclone mount {mp} is not active ({why}).")
        if ADAPIKEY and os.path.exists(f'/healthcheck/{RCLONEMN_AD}'):
            mp = f'/data/{RCLONEMN_AD}'
            alive, why = _mount_alive(mp)
            if not alive:
                error_messages.append(f"Rclone mount {mp} is not active ({why}).")
    # TB mount is NOT under the ZURG guard: TorBox uses its own WebDAV
    # endpoint (webdav.torbox.app) and does not require Zurg to be
    # enabled.  A TB-only setup with ZURG=false is supported.
    if torbox_mount_configured and os.path.exists(f'/healthcheck/{TORBOX_MOUNT_NAME}'):
        mp = f'/data/{TORBOX_MOUNT_NAME}'
        alive, why = _mount_alive(mp)
        if not alive:
            error_messages.append(f"Rclone mount {mp} is not active ({why}).")

    # Status server responsiveness (non-fatal — log warning but don't fail healthcheck)
    try:
        port = int(os.environ.get('STATUS_UI_PORT', '8080'))
        urllib.request.urlopen(f'http://localhost:{port}/', timeout=5)
    except Exception:
        print("Warning: Status server is not responding on port " + str(port), file=sys.stderr)

    if error_messages:
        error_message_combined = " | ".join(error_messages)
        raise Exception(error_message_combined)

except Exception as e:
    print(str(e), file=sys.stderr)
    exit(1)
