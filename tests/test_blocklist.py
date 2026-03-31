"""Tests for utils/blocklist.py."""

import hashlib
import json
import os
from utils import blocklist
from utils.blackhole import _bencode_end


def test_add_and_check(tmp_dir):
    """Add a hash and verify is_blocked returns True."""
    blocklist.init(tmp_dir)

    entry_id = blocklist.add('abc123def456', 'Test Movie', reason='wrong content')
    assert entry_id is not None
    assert blocklist.is_blocked('ABC123DEF456')
    assert blocklist.is_blocked('abc123def456')  # case-insensitive
    assert not blocklist.is_blocked('zzz999')


def test_add_returns_none_without_hash(tmp_dir):
    """Adding with empty hash returns None."""
    blocklist.init(tmp_dir)
    assert blocklist.add('', 'No Hash') is None
    assert blocklist.add(None, 'No Hash') is None


def test_remove(tmp_dir):
    """Add then remove — is_blocked should return False."""
    blocklist.init(tmp_dir)

    entry_id = blocklist.add('hash_to_remove', 'Removable', reason='test')
    assert blocklist.is_blocked('hash_to_remove')

    result = blocklist.remove(entry_id)
    assert result is True
    assert not blocklist.is_blocked('hash_to_remove')


def test_remove_nonexistent(tmp_dir):
    """Removing a non-existent ID returns False."""
    blocklist.init(tmp_dir)
    assert blocklist.remove('fake-uuid') is False


def test_clear(tmp_dir):
    """Add 3 entries, clear, verify all unblocked."""
    blocklist.init(tmp_dir)

    blocklist.add('hash1', 'Title 1')
    blocklist.add('hash2', 'Title 2')
    blocklist.add('hash3', 'Title 3')
    assert blocklist.is_blocked('hash1')
    assert blocklist.is_blocked('hash2')
    assert blocklist.is_blocked('hash3')
    assert len(blocklist.get_all()) == 3

    blocklist.clear()
    assert not blocklist.is_blocked('hash1')
    assert not blocklist.is_blocked('hash2')
    assert not blocklist.is_blocked('hash3')
    assert len(blocklist.get_all()) == 0


def test_persistence(tmp_dir):
    """Add entries, re-init from same file, verify entries survive."""
    blocklist.init(tmp_dir)

    blocklist.add('persist_hash_1', 'Persistent Title 1', reason='corrupt')
    blocklist.add('persist_hash_2', 'Persistent Title 2', reason='wrong language')

    # Verify file was written
    fp = os.path.join(tmp_dir, 'blocklist.json')
    assert os.path.isfile(fp)

    # Re-init from the same directory (simulates restart)
    blocklist.init(tmp_dir)
    assert blocklist.is_blocked('persist_hash_1')
    assert blocklist.is_blocked('persist_hash_2')
    assert len(blocklist.get_all()) == 2


def test_duplicate_add(tmp_dir):
    """Adding the same hash twice should return the existing entry ID."""
    blocklist.init(tmp_dir)

    id1 = blocklist.add('dupe_hash', 'Title A', reason='first')
    id2 = blocklist.add('dupe_hash', 'Title B', reason='second')
    assert id1 == id2
    assert len(blocklist.get_all()) == 1


def test_get_all_sorted_desc(tmp_dir):
    """get_all returns entries sorted by date descending."""
    blocklist.init(tmp_dir)

    blocklist.add('h1', 'First')
    blocklist.add('h2', 'Second')
    blocklist.add('h3', 'Third')

    entries = blocklist.get_all()
    assert len(entries) == 3
    # Most recent should be first
    dates = [e['date'] for e in entries]
    assert dates == sorted(dates, reverse=True)


def test_is_blocked_title(tmp_dir):
    """Title-based blocking works with normalized matching."""
    blocklist.init(tmp_dir)

    blocklist.add('hash_title', 'The.Great.Movie.2024.1080p')
    assert blocklist.is_blocked_title('the great movie 2024 1080p')
    assert blocklist.is_blocked_title('The.Great.Movie.2024.1080p')
    assert not blocklist.is_blocked_title('Some Other Movie')


def test_is_blocked_title_empty(tmp_dir):
    """Empty/None title returns False."""
    blocklist.init(tmp_dir)
    assert not blocklist.is_blocked_title('')
    assert not blocklist.is_blocked_title(None)


def test_entry_structure(tmp_dir):
    """Verify entry dict has all expected fields."""
    blocklist.init(tmp_dir)

    blocklist.add('struct_hash', 'Struct Title', reason='test reason', source='auto')
    entries = blocklist.get_all()
    assert len(entries) == 1

    entry = entries[0]
    assert 'id' in entry
    assert entry['info_hash'] == 'STRUCT_HASH'
    assert entry['title'] == 'Struct Title'
    assert entry['reason'] == 'test reason'
    assert entry['source'] == 'auto'
    assert 'date' in entry


def test_corrupted_file_handled(tmp_dir):
    """Gracefully handles a corrupted JSON file."""
    fp = os.path.join(tmp_dir, 'blocklist.json')
    with open(fp, 'w') as f:
        f.write('not valid json{{{')

    # Should not raise
    blocklist.init(tmp_dir)
    assert len(blocklist.get_all()) == 0
    # Should still be functional
    blocklist.add('after_corrupt', 'After Corrupt')
    assert blocklist.is_blocked('after_corrupt')


def test_title_index_collision_on_remove(tmp_dir):
    """Removing one entry must not un-block another with the same normalized title."""
    blocklist.init(tmp_dir)

    id1 = blocklist.add('hash_a', 'The.Flash.S01.1080p')
    id2 = blocklist.add('hash_b', 'The Flash S01 1080p')
    # Different hashes, but same normalized title — only one in title index
    assert blocklist.is_blocked_title('the flash s01 1080p')

    # Remove the entry that's currently in the title index
    blocklist.remove(id2)
    # The title should still be blocked via the remaining entry
    assert blocklist.is_blocked_title('the flash s01 1080p')


def test_ampersand_normalization(tmp_dir):
    """Verify & is normalized to 'and' in title matching."""
    blocklist.init(tmp_dir)

    blocklist.add('hash_amp', 'Tom & Jerry')
    assert blocklist.is_blocked_title('Tom and Jerry')
    assert blocklist.is_blocked_title('tom & jerry')


def test_bencode_end_dict():
    """_bencode_end correctly finds the end of a bencoded dict."""
    # d3:foo3:bare => dict {"foo": "bar"}
    data = b'd3:foo3:bare'
    assert _bencode_end(data, 0) == len(data)


def test_bencode_end_nested():
    """_bencode_end handles nested structures."""
    # d4:infod4:name4:testeee  => {"info": {"name": "test"}}
    data = b'd4:infod4:name4:testee'
    assert _bencode_end(data, 0) == len(data)


def test_torrent_info_hash_extraction(tmp_dir):
    """Extract info hash from a minimal .torrent file."""
    from utils.blackhole import BlackholeWatcher
    # Build a minimal bencoded torrent: d8:announce3:url4:infod4:name4:test6:lengthi100eee
    info_dict = b'd4:name4:test6:lengthi100ee'
    torrent = b'd8:announce3:url4:info' + info_dict + b'e'

    expected_hash = hashlib.sha1(info_dict).hexdigest().upper()

    torrent_path = os.path.join(tmp_dir, 'test.torrent')
    with open(torrent_path, 'wb') as f:
        f.write(torrent)

    result = BlackholeWatcher._extract_info_hash_from_file(torrent_path)
    assert result == expected_hash
