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
        }
    }

    process_status = check_processes(process_info)

    for process_name, info in process_info.items():
        if info["should_run"] and not process_status[process_name]:
            error_messages.append(info["error_message"])

    # Mount liveness — verify FUSE mount is active, not just rclone process
    if str(ZURG).lower() == 'true':
        if RDAPIKEY and os.path.exists(f'/healthcheck/{RCLONEMN_RD}'):
            mount_path = f'/data/{RCLONEMN_RD}'
            if not os.path.ismount(mount_path):
                error_messages.append(f"Rclone mount {mount_path} is not active.")
        if ADAPIKEY and os.path.exists(f'/healthcheck/{RCLONEMN_AD}'):
            mount_path = f'/data/{RCLONEMN_AD}'
            if not os.path.ismount(mount_path):
                error_messages.append(f"Rclone mount {mount_path} is not active.")

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
