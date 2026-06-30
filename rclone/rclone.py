from base import *
from utils.logger import *
from utils.processes import ProcessHandler
from utils.notifications import notify
from utils.network import wait_for_url
from utils.file_utils import atomic_write

logger = get_logger()

# RC (remote control) base port for rclone instances.
# Each mount gets its own RC port: base, base+1, etc.
_RC_BASE_PORT = 5572
# Populated at setup time: {mount_name: rc_url, ...}
_rc_urls = {}

# TorBox WebDAV endpoint — fixed by TorBox.  Exposed as a constant so tests
# can monkeypatch a mock without touching the live URL.
TORBOX_WEBDAV_URL = 'https://webdav.torbox.app/'


def _is_mount_point(path):
    """True iff ``path`` is currently a mount point in this namespace.

    Reads /proc/self/mountinfo directly (field 5 is the mount point) rather
    than ``os.path.ismount``, which can raise on a stale FUSE corpse
    (ENOTCONN) instead of answering.  This is the authoritative signal rclone
    itself keys off — if a line for ``path`` is present, rclone will refuse to
    mount with "directory already mounted".
    """
    try:
        with open("/proc/self/mountinfo") as fh:
            for line in fh:
                fields = line.split()
                if len(fields) > 4 and _unescape_mountinfo(fields[4]) == path:
                    return True
    except OSError:
        pass
    return False


def _unescape_mountinfo(field):
    """Decode the kernel's octal escapes in a mountinfo path field.

    /proc/self/mountinfo escapes space (\\040), tab (\\011), newline (\\012)
    and backslash (\\134) in path fields, so a raw == comparison would miss a
    mount whose name contains any of them.  Backslash is decoded last so a
    literal ``\\134`` in the source isn't re-interpreted.
    """
    return (field.replace("\\040", " ")
                 .replace("\\011", "\t")
                 .replace("\\012", "\n")
                 .replace("\\134", "\\"))


def _force_clear_stale_mount(mount_path, logger, attempts=10, delay=0.5):
    """Detach a stale FUSE corpse at ``mount_path`` and wait until it is gone.

    Only called once a plain ``os.makedirs(exist_ok=True)`` has already failed,
    i.e. the mountpoint is already broken — so a healthy, actively-streaming
    mount is never reached here.

    The corpse is left by a prior container whose rclone was SIGKILLed mid
    shutdown; because the TorBox mount is nested under the ``:shared`` ``/data``
    bind, the dead host-side mount propagates back into the fresh container on
    ``--force-recreate``.  A single lazy unmount detaches asynchronously, so
    the old code's "unmount then immediately remount" raced the deferred
    teardown and rclone hit "directory already mounted".  Here we escalate and
    then *verify* the path is genuinely no longer a mount point (and is a usable
    dir) before returning, retrying a bounded number of times.
    """
    unmount_ladder = (["fusermount3", "-uz", mount_path],
                      ["fusermount", "-uz", mount_path],
                      ["umount", "-l", mount_path])
    for attempt in range(attempts):
        for cmd in unmount_ladder:
            try:
                subprocess.run(cmd, check=False,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except FileNotFoundError:
                # fuse3 image ships only `fusermount3`; tolerate the others
                # being absent and still fall through to `umount -l`.
                pass
        if not _is_mount_point(mount_path):
            try:
                os.makedirs(mount_path, exist_ok=True)
            except OSError:
                # Detached from the mount table but the dir entry is still a
                # corpse (ENOTCONN); give the lazy teardown a moment, retry.
                pass
            else:
                if attempt:
                    logger.info(
                        f"Cleared stale mountpoint {mount_path} after "
                        f"{attempt + 1} unmount attempt(s)")
                return True
        time.sleep(delay)
    logger.warning(
        f"Stale mountpoint {mount_path} still present after {attempts} "
        f"unmount attempts; rclone mount may fail with 'directory already "
        f"mounted'")
    return False


def _torbox_mount_configured():
    """Return True iff all three TorBox WebDAV credentials are present.

    The API key alone is not sufficient — TorBox's WebDAV requires a
    separately configured user + WebDAV-only password set in the TorBox
    dashboard (Settings → Integrations → WebDAV).  We treat the mount as
    unconfigured (warn, skip) rather than failing the container so the
    user can still use the non-mount features (search/cache-check) of
    the existing TORBOX_API_KEY integration.
    """
    return bool(TORBOXAPIKEY and TORBOXWEBDAVUSER and TORBOXWEBDAVPASS)


def _write_torbox_remote(file_handle, remote_name):
    """Emit the ``[<remote_name>]`` section for TorBox WebDAV.

    Caller has already opened ``file_handle`` for write — we just append
    the stanza.  Keeps the password obscure-encoded so the on-disk
    rclone.config doesn't carry it in clear text (matches the existing
    Zurg-user pattern).
    """
    obscured = obscure_password(TORBOXWEBDAVPASS)
    if not obscured:
        logger.error(
            "[rclone] Failed to obscure TorBox WebDAV password — "
            "TorBox mount will be skipped"
        )
        return False
    file_handle.write(f"[{remote_name}]\n")
    file_handle.write("type = webdav\n")
    file_handle.write(f"url = {TORBOX_WEBDAV_URL}\n")
    # vendor=other matches the Zurg remotes; TorBox's WebDAV implements
    # the generic spec, not Nextcloud/Owncloud-specific extensions.
    file_handle.write("vendor = other\n")
    file_handle.write("pacer_min_sleep = 0\n")
    file_handle.write(f"user = {TORBOXWEBDAVUSER}\n")
    file_handle.write(f"pass = {obscured}\n")
    return True

def get_rc_url(mount_name=None):
    """Return the RC URL for a given mount, or the first one if unspecified."""
    if mount_name and mount_name in _rc_urls:
        return _rc_urls[mount_name]
    return next(iter(_rc_urls.values()), None)

def get_all_rc_urls():
    """Return all registered RC URLs."""
    return list(_rc_urls.values())

def get_port_from_config(config_file_path, key_type):
    try:
        with open(config_file_path, 'r') as file:
            for line in file:
                if line.strip().startswith("port:"):
                    port = line.split(':')[1].strip()
                    return port
    except Exception as e:
        logger.error(f"Error reading port from config file: {e}")
    return '9999'

def obscure_password(password):
    """Obscure the password using rclone."""
    try:
        result = subprocess.run(["rclone", "obscure", password], check=True, stdout=subprocess.PIPE)
        return result.stdout.decode().strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Error obscuring password: {e}")
        return None

def regenerate_config():
    """Regenerate rclone.config from current config values.

    Separated from setup() so config_reload can regenerate the config
    file without re-launching processes.
    """
    refresh_globals(globals())

    if not RCLONEMN:
        raise Exception("Please set a name for the rclone mount")
    if not RDAPIKEY and not ADAPIKEY:
        raise Exception("Please set the API Key for the rclone mount")

    config_file_path_rd = '/zurg/RD/config.yml'
    config_file_path_ad = '/zurg/AD/config.yml'

    if RDAPIKEY and ADAPIKEY:
        RCLONEMN_RD = f"{RCLONEMN}_RD"
        RCLONEMN_AD = f"{RCLONEMN}_AD"
    else:
        RCLONEMN_RD = RCLONEMN_AD = RCLONEMN

    rclone_config_path = "/config/rclone.config"
    if os.path.exists(rclone_config_path):
        backup_path = rclone_config_path + ".bak"
        shutil.copy2(rclone_config_path, backup_path)

    with atomic_write(rclone_config_path) as f:
        if RDAPIKEY:
            rd_port = get_port_from_config(config_file_path_rd, 'RDAPIKEY')
            f.write(f"[{RCLONEMN_RD}]\n")
            f.write("type = webdav\n")
            f.write(f"url = http://localhost:{rd_port}/dav\n")
            f.write("vendor = other\n")
            f.write("pacer_min_sleep = 0\n")
            if ZURGUSER and ZURGPASS:
                obscured_password = obscure_password(ZURGPASS)
                if obscured_password:
                    f.write(f"user = {ZURGUSER}\n")
                    f.write(f"pass = {obscured_password}\n")

        if ADAPIKEY:
            ad_port = get_port_from_config(config_file_path_ad, 'ADAPIKEY')
            f.write(f"[{RCLONEMN_AD}]\n")
            f.write("type = webdav\n")
            f.write(f"url = http://localhost:{ad_port}/dav\n")
            f.write("vendor = other\n")
            f.write("pacer_min_sleep = 0\n")
            if ZURGUSER and ZURGPASS:
                obscured_password = obscure_password(ZURGPASS)
                if obscured_password:
                    f.write(f"user = {ZURGUSER}\n")
                    f.write(f"pass = {obscured_password}\n")

        # TorBox co-debrid (plan 39).  Written only when fully configured;
        # incomplete TorBox config logs a warning at setup() time and the
        # mount is skipped.
        if _torbox_mount_configured():
            _write_torbox_remote(f, TORBOX_MOUNT_NAME)

    logger.info("Regenerated rclone.config")


def setup():
    refresh_globals(globals())
    _rc_urls.clear()
    logger.info("Checking rclone flags")

    try:
        if not RCLONEMN:
            raise Exception("Please set a name for the rclone mount")
        logger.info(f"Configuring the rclone mount name to \"{RCLONEMN}\"")

        if not RDAPIKEY and not ADAPIKEY:
            raise Exception("Please set the API Key for the rclone mount")

        if RDAPIKEY and ADAPIKEY:
            RCLONEMN_RD = f"{RCLONEMN}_RD"
            RCLONEMN_AD = f"{RCLONEMN}_AD"
        else:
            RCLONEMN_RD = RCLONEMN_AD = RCLONEMN

        config_file_path_rd = '/zurg/RD/config.yml'
        config_file_path_ad = '/zurg/AD/config.yml'

        rclone_config_path = "/config/rclone.config"
        if os.path.exists(rclone_config_path):
            backup_path = rclone_config_path + ".bak"
            shutil.copy2(rclone_config_path, backup_path)
            logger.info(f"Backed up existing rclone config to {backup_path}")

        with atomic_write(rclone_config_path) as f:
            if RDAPIKEY:
                rd_port = get_port_from_config(config_file_path_rd, 'RDAPIKEY')
                f.write(f"[{RCLONEMN_RD}]\n")
                f.write("type = webdav\n")
                f.write(f"url = http://localhost:{rd_port}/dav\n")
                f.write("vendor = other\n")
                f.write("pacer_min_sleep = 0\n")
                if ZURGUSER and ZURGPASS:
                    obscured_password = obscure_password(ZURGPASS)
                    if obscured_password:
                        f.write(f"user = {ZURGUSER}\n")
                        f.write(f"pass = {obscured_password}\n")

            if ADAPIKEY:
                ad_port = get_port_from_config(config_file_path_ad, 'ADAPIKEY')
                f.write(f"[{RCLONEMN_AD}]\n")
                f.write("type = webdav\n")
                f.write(f"url = http://localhost:{ad_port}/dav\n")
                f.write("vendor = other\n")
                f.write("pacer_min_sleep = 0\n")
                if ZURGUSER and ZURGPASS:
                    obscured_password = obscure_password(ZURGPASS)
                    if obscured_password:
                        f.write(f"user = {ZURGUSER}\n")
                        f.write(f"pass = {obscured_password}\n")

            # TorBox co-debrid (plan 39).
            torbox_remote_written = False
            if _torbox_mount_configured():
                torbox_remote_written = _write_torbox_remote(f, TORBOX_MOUNT_NAME)
            elif TORBOXAPIKEY:
                logger.warning(
                    "[rclone] TORBOX_API_KEY is set but TORBOX_WEBDAV_USER "
                    "and/or TORBOX_WEBDAV_PASS is missing — TorBox mount "
                    "will be skipped.  Generate a WebDAV-only password in "
                    "the TorBox dashboard (Settings → Integrations → WebDAV) "
                    "and set both env vars to enable the mount.  Non-mount "
                    "TorBox features (cache probes, search add) still work "
                    "with just the API key."
                )

        with open("/etc/fuse.conf", "a") as f:
            f.write("user_allow_other\n")

        mount_names = []
        if RDAPIKEY:
            mount_names.append(RCLONEMN_RD)
        if ADAPIKEY:
            mount_names.append(RCLONEMN_AD)
        if torbox_remote_written:
            # Collision check — TORBOX_MOUNT_NAME must not shadow an existing
            # Zurg mount.  If it does, log loudly and skip — silently writing
            # over /data/zurgarr/ with TorBox content would corrupt the
            # symlink machinery's view of RD's catalogue.
            if TORBOX_MOUNT_NAME in mount_names:
                logger.error(
                    f"[rclone] TORBOX_MOUNT_NAME ('{TORBOX_MOUNT_NAME}') "
                    f"collides with an existing Zurg mount.  TorBox mount "
                    f"will be skipped.  Set TORBOX_MOUNT_NAME to a unique "
                    f"value (default is 'torbox')."
                )
            else:
                mount_names.append(TORBOX_MOUNT_NAME)

        process_handler = ProcessHandler(logger)

        def _configure_mount(idx, mn):
            logger.info(f"Configuring rclone for {mn}")
            mount_path = f"/data/{mn}"
            # Plain (non-lazy) umount clears a clean leftover mount but is a
            # no-op (EBUSY) on a healthy, actively-streaming mount — which is
            # exactly what we want, since setup() also runs on SIGHUP reload.
            subprocess.run(["umount", mount_path], check=False,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                os.makedirs(mount_path, exist_ok=True)
            except OSError:
                # A dead/stale FUSE mountpoint left by a prior container can
                # survive a plain umount; the path then exists but isn't a
                # usable directory, so makedirs(exist_ok=True) still raises.
                # Escalate, then *wait* until the corpse is verifiably gone
                # before remounting — a bare lazy unmount detaches async and
                # rclone would otherwise race it and fail "directory already
                # mounted".  Only fires on an already-broken mount, so a
                # healthy active mount is never force-detached.
                if not _force_clear_stale_mount(mount_path, logger):
                    # Surface the real reason in the per-mount error/notify
                    # (the loop's `except Exception` catches this) rather than
                    # letting the retry makedirs raise a generic OSError that
                    # buries the diagnostic warning already logged above.
                    raise OSError(
                        f"Stale FUSE mountpoint {mount_path} could not be "
                        f"cleared; skipping mount to avoid 'directory already "
                        f"mounted'")
                os.makedirs(mount_path, exist_ok=True)

            rc_port = _RC_BASE_PORT + idx

            # TorBox's dir cache must outlive the hourly library scan
            # (TORBOX_RCLONE_DIR_CACHE_TIME, default 2h) — with the shorter
            # shared default it expires between scans, so every scan re-lists
            # all release folders over the throttled (tpslimit) mount, hits
            # the scan deadline, and drops TB titles (they flip to "Wanted").
            # Other mounts keep the shared RCLONE_DIR_CACHE_TIME.  Gate on
            # torbox_remote_written (not just the name) so a Zurg mount that
            # happens to be named "torbox" — RCLONE_MOUNT_NAME with TorBox
            # unconfigured — doesn't wrongly inherit the 2h cache.
            if mn == TORBOX_MOUNT_NAME and torbox_remote_written:
                dir_cache_time = (TORBOX_RCLONE_DIR_CACHE_TIME or '').strip() or '2h'
            else:
                dir_cache_time = (os.environ.get('RCLONE_DIR_CACHE_TIME') or '').strip() or '10s'

            if NFSMOUNT is not None and NFSMOUNT.lower() == "true":
                port = NFSPORT if NFSPORT else find_available_port(8001, 8999)
                logger.info(f"Setting up rclone NFS server for {mn} at 0.0.0.0:{port}")
                vfs_cache_mode = (os.environ.get('RCLONE_VFS_CACHE_MODE') or '').strip() or 'full'
                rclone_command = ["rclone", "serve", "nfs", f"{mn}:", "--config", "/config/rclone.config", "--addr", f"0.0.0.0:{port}", f"--vfs-cache-mode={vfs_cache_mode}", f"--dir-cache-time={dir_cache_time}"]
            else:
                # poll-interval makes rclone actively diff the backend on a
                # schedule and emit FUSE_NOTIFY_INVAL_ENTRY for any entries
                # that changed. Without it, rclone only ever re-reads when
                # dir-cache-time expires or RC refresh is called, and the
                # kernel dentry cache holds ghost entries in between.
                poll_interval = (os.environ.get('RCLONE_POLL_INTERVAL') or '').strip() or '15s'
                rclone_command = ["rclone", "mount", f"{mn}:", f"/data/{mn}", "--config", "/config/rclone.config", "--allow-other", f"--poll-interval={poll_interval}", f"--dir-cache-time={dir_cache_time}"]

            # Enable RC API so Zurgarr can refresh dir cache on demand.
            # --daemon is intentionally omitted: it forks rclone into a new
            # process that discards the RC server.  ProcessHandler manages the
            # lifecycle instead, keeping the RC port alive for vfs/refresh calls.
            rclone_command.extend(["--rc", f"--rc-addr=localhost:{rc_port}", "--rc-no-auth"])

            # Optional VFS cache flags — apply to both NFS and FUSE modes.
            # Rclone also reads these natively from RCLONE_* env vars, but
            # explicit flags ensure they take effect on restarts via SIGHUP.
            for env_key, flag in [('RCLONE_VFS_CACHE_MAX_SIZE', 'vfs-cache-max-size'),
                                  ('RCLONE_VFS_CACHE_MAX_AGE', 'vfs-cache-max-age')]:
                val = (os.environ.get(env_key) or '').strip()
                if val:
                    rclone_command.append(f'--{flag}={val}')

            # Per-mount throttling / retry tuning (plan 41 phase D).
            #
            # FUSE-only — ``rclone serve nfs`` does not accept these
            # flags (it's a server, not a mount), and ``--low-level-retries``
            # specifically would crash the NFS subcommand with an
            # unknown-flag error.  Skip the entire block when NFSMOUNT is
            # active so the NFS path stays exactly as it was before D.
            #
            # TorBox: rate-limits reads aggressively under concurrent
            # Plex/Bazarr/library-scan load — the symptom is rclone vfs
            # cache 429 spam that ends with "too many errors 11/10".
            # --tpslimit caps tokens-per-second issued by rclone so we
            # stay under TB's threshold; --tpslimit-burst lets short
            # bursts (e.g. ffprobe quick-peek) succeed.  Defaults of
            # 5/10 match TB Essential's observed steady-state ceiling.
            # Set either env var to ``0`` to omit the flag entirely.
            #
            # RD/AD (Zurg-backed): RD returns HTTP 423 on freshly-grabbed
            # files while the upstream re-cracks the rar; that resolves
            # in seconds.  Default rclone retry count is 10, which floods
            # the log with retries before giving up.  Cap to 3 retries —
            # plenty for the recovery window, and the rest of the noise
            # disappears.
            nfs_mode = NFSMOUNT is not None and NFSMOUNT.lower() == "true"
            if not nfs_mode:
                if mn == TORBOX_MOUNT_NAME and torbox_remote_written:
                    tps_raw = (os.environ.get('TORBOX_RCLONE_TPSLIMIT') or '5').strip()
                    burst_raw = (os.environ.get('TORBOX_RCLONE_TPSLIMIT_BURST') or '3').strip()
                    try:
                        tps = int(tps_raw)
                        if tps > 0:
                            rclone_command.append(f'--tpslimit={tps}')
                    except ValueError:
                        logger.warning(f"Invalid TORBOX_RCLONE_TPSLIMIT={tps_raw!r}; ignoring")
                    try:
                        burst = int(burst_raw)
                        if burst > 0:
                            rclone_command.append(f'--tpslimit-burst={burst}')
                    except ValueError:
                        logger.warning(f"Invalid TORBOX_RCLONE_TPSLIMIT_BURST={burst_raw!r}; ignoring")
                else:
                    rclone_command.append('--low-level-retries=3')

            # Pick the URL + auth + verb to probe for readiness.  RD/AD
            # point at a local Zurg instance and accept plain GET on
            # /dav/.  TorBox goes direct to webdav.torbox.app and rejects
            # GET at root with 401 but accepts PROPFIND with 207 — so the
            # TorBox branch passes ``method='PROPFIND'`` to wait_for_url.
            if mn == TORBOX_MOUNT_NAME and torbox_remote_written:
                url = TORBOX_WEBDAV_URL.rstrip('/')
                probe_endpoint = "/"
                probe_auth = (TORBOXWEBDAVUSER, TORBOXWEBDAVPASS)
                probe_label = f"TorBox WebDAV ({mn})"
                probe_method = "PROPFIND"
            else:
                url = f"http://localhost:{rd_port if mn == RCLONEMN_RD else ad_port}"
                probe_endpoint = "/dav/"
                probe_auth = (ZURGUSER, ZURGPASS) if ZURGUSER and ZURGPASS else None
                probe_label = f"Zurg WebDAV ({mn})"
                probe_method = "GET"
            if os.path.exists(f"/healthcheck/{mn}"):
                os.rmdir(f"/healthcheck/{mn}")
            if wait_for_url(url, endpoint=probe_endpoint, auth=probe_auth,
                            description=probe_label, method=probe_method):
                os.makedirs(f"/healthcheck/{mn}") # makdir for healthcheck. Don't like it, but it works for now...
                logger.info(f"The {probe_label} URL {url}{probe_endpoint} is accessible. Starting rclone for {mn} (RC on port {rc_port})")
                process_name = "rclone"
                suppress_logging=False
                if str(RCLONELOGLEVEL).lower()=='off':
                    suppress_logging = True
                    logger.info(f"Suppressing {process_name} logging")                     
                rclone_process = process_handler.start_process(process_name, "/config", rclone_command, mn, suppress_logging=suppress_logging)
                if rclone_process:
                    # Register the RC URL only once the mount process is
                    # actually up, so a skipped/failed mount never leaves a
                    # dead localhost URL for the RC-refresh consumers.
                    _rc_urls[mn] = f"http://localhost:{rc_port}"
                notify('mount_success', 'Rclone Mounted', f'Mount {mn} is ready')
            else:
                logger.error(f"The Zurg WebDav URL {url}/dav is not accessible within the timeout period. Skipping rclone setup for {mn}")
                notify('health_error', 'Rclone Mount Failed', f'Mount {mn} failed: Zurg WebDAV timeout', level='error')

        # Per-mount isolation: a single mount failing (stale FUSE mountpoint
        # left by a prior container, an unreachable WebDAV) must not abort
        # setup for the other mounts or starve the rest of startup (scheduler
        # registration runs after this in main()).
        for idx, mn in enumerate(mount_names):
            try:
                _configure_mount(idx, mn)
            except Exception as e:
                logger.error(f"Error configuring rclone mount {mn!r}: {e}", exc_info=True)
                notify('health_error', 'Rclone Mount Failed', f'Mount {mn} failed: {e}', level='error')
                continue

        logger.info("rclone startup complete")

    except Exception as e:
        # Pre-loop config failure (missing key, unwritable config). Don't
        # exit(1) — that raises SystemExit, which main()'s `except Exception`
        # can't catch, aborting startup before the task scheduler is even
        # registered. Re-raise so main.py logs it and the rest of startup
        # (scheduler, blackhole) still runs.
        logger.error(f"rclone setup failed before any mount started: {e}", exc_info=True)
        notify('health_error', 'Rclone Setup Failed',
               f'rclone configuration failed before any mount started: {e}', level='error')
        raise
