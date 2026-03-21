"""Tests for atomic file write operations."""

import os
import pytest
from utils.file_utils import atomic_write


class TestAtomicWrite:

    def test_basic_write(self, tmp_dir):
        """Atomic write should create file with correct content."""
        path = os.path.join(tmp_dir, 'test.yml')
        with atomic_write(path) as f:
            f.write('key: value\n')
        assert os.path.exists(path)
        with open(path) as f:
            assert f.read() == 'key: value\n'

    def test_overwrites_existing(self, tmp_dir):
        """Atomic write should replace existing file content."""
        path = os.path.join(tmp_dir, 'test.yml')
        with atomic_write(path) as f:
            f.write('old content\n')
        with atomic_write(path) as f:
            f.write('new content\n')
        with open(path) as f:
            assert f.read() == 'new content\n'

    def test_binary_mode(self, tmp_dir):
        """Atomic write should work in binary mode."""
        path = os.path.join(tmp_dir, 'test.bin')
        with atomic_write(path, mode='wb') as f:
            f.write(b'\x00\x01\x02\x03')
        with open(path, 'rb') as f:
            assert f.read() == b'\x00\x01\x02\x03'

    def test_preserves_permissions(self, tmp_dir):
        """Atomic write should preserve original file permissions."""
        path = os.path.join(tmp_dir, 'test.yml')
        with open(path, 'w') as f:
            f.write('original')
        os.chmod(path, 0o600)

        with atomic_write(path) as f:
            f.write('updated')

        mode = os.stat(path).st_mode & 0o777
        assert mode == 0o600

    def test_no_partial_write_on_error(self, tmp_dir):
        """If write fails, original file should be untouched."""
        path = os.path.join(tmp_dir, 'test.yml')
        with atomic_write(path) as f:
            f.write('original\n')

        with pytest.raises(ValueError):
            with atomic_write(path) as f:
                f.write('partial')
                raise ValueError("simulated crash")

        with open(path) as f:
            assert f.read() == 'original\n'

    def test_temp_file_cleaned_up_on_success(self, tmp_dir):
        """No temp files should remain after successful write."""
        path = os.path.join(tmp_dir, 'test.yml')
        with atomic_write(path) as f:
            f.write('content\n')
        files = os.listdir(tmp_dir)
        assert files == ['test.yml']

    def test_temp_file_cleaned_up_on_error(self, tmp_dir):
        """No temp files should remain after failed write."""
        path = os.path.join(tmp_dir, 'test.yml')
        try:
            with atomic_write(path) as f:
                f.write('data')
                raise RuntimeError("oops")
        except RuntimeError:
            pass
        files = os.listdir(tmp_dir)
        assert files == []

    def test_new_file_default_permissions(self, tmp_dir):
        """New file should be created with default permissions."""
        path = os.path.join(tmp_dir, 'new.yml')
        with atomic_write(path) as f:
            f.write('content')
        assert os.path.exists(path)

    def test_empty_write(self, tmp_dir):
        """Writing empty content should produce an empty file."""
        path = os.path.join(tmp_dir, 'empty.yml')
        with atomic_write(path) as f:
            f.write('')
        with open(path) as f:
            assert f.read() == ''
