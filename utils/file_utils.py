"""File utility functions for safe I/O operations."""

import os
import stat
import tempfile
from contextlib import contextmanager


@contextmanager
def atomic_write(target_path, mode='w', encoding='utf-8'):
    """Context manager for crash-safe atomic file writes.

    Writes to a temporary file in the same directory, then atomically
    renames to the target path on success. If an exception occurs,
    the temp file is cleaned up and the original file is untouched.

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

        # Preserve original permissions
        if original_mode is not None:
            os.chmod(tmp_path, original_mode)

        # Atomic rename
        os.replace(tmp_path, target_path)
    except BaseException:
        # Clean up temp file on error
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
