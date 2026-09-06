"""File utility functions for safe I/O operations."""

import os
import stat
import tempfile
from contextlib import contextmanager


@contextmanager
def atomic_write(target_path, mode='w', encoding='utf-8', fsync=True):
    """Context manager for crash-safe atomic file writes.

    Writes to a temporary file in the same directory, then atomically
    renames to the target path on success. If an exception occurs,
    the temp file is cleaned up and the original file is untouched.

    By default, the temp file's contents and the containing directory
    are both fsync'd before/after the rename so the write survives a
    crash or power loss, not just concurrent readers. Pass
    ``fsync=False`` only for rebuildable caches where the extra fsync
    cost isn't worth it (e.g. a hot-path TMDB metadata cache that can
    just be re-fetched).

    Usage:
        with atomic_write('/path/to/config.yml') as f:
            f.write('key: value\\n')

    For binary mode:
        with atomic_write('/path/to/file', mode='wb') as f:
            f.write(b'binary data')
    """
    target_dir = os.path.dirname(target_path) or '.'

    # Preserve permissions from existing file if possible
    original_mode = None
    try:
        original_stat = os.stat(target_path)
        original_mode = stat.S_IMODE(original_stat.st_mode)
    except FileNotFoundError:
        pass

    fd, tmp_path = tempfile.mkstemp(dir=target_dir)
    try:
        fdopen_kwargs = {'mode': mode}
        if 'b' not in mode:
            fdopen_kwargs['encoding'] = encoding
        with os.fdopen(fd, **fdopen_kwargs) as tmp_file:
            yield tmp_file
            if fsync:
                # A caller that does `f.write(...); f.close()` inside the
                # `with` block (rather than just writing and letting the
                # context manager close it) already flushed to disk via
                # close() — flush()/fileno() on the now-closed file object
                # raise ValueError, which must not fail a write that has
                # already succeeded. An OSError here is a real fsync
                # failure and stays fatal (controller ruling: audit
                # finding #7 — do not swallow it).
                try:
                    tmp_file.flush()
                    os.fsync(tmp_file.fileno())
                except ValueError:
                    pass

        # Preserve original permissions
        if original_mode is not None:
            os.chmod(tmp_path, original_mode)

        # Atomic rename
        os.replace(tmp_path, target_path)

        # Persist the rename itself — os.replace is atomic for concurrent
        # readers but not durable across power loss without a dir fsync.
        # Best-effort: the rename has already landed on disk by this
        # point, so a dir-fsync failure (e.g. O_DIRECTORY/EINVAL on some
        # CIFS/NFS mounts — /config is a network share in some
        # deployments) must not turn a successful write into a reported
        # failure.
        if fsync:
            try:
                dir_fd = os.open(target_dir, os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
    except BaseException:
        # Clean up temp file on error
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
