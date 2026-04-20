"""Tests for blackhole watch folder logic."""

import json
import os
import time
import pytest
from utils.blackhole import (
    RetryMeta, BlackholeWatcher, RETRY_SCHEDULE, MAX_RETRIES,
    MEDIA_EXTENSIONS, MOUNT_CATEGORIES, parse_release_name,
    _is_multi_season_pack, _extract_file_season, _build_season_release_name,
    _enrich_for_history,
    _is_valid_label, iter_release_dirs,
)


class TestRetryMeta:

    def test_read_nonexistent(self, tmp_dir):
        """Reading meta for file without sidecar should return (0, 0)."""
        path = os.path.join(tmp_dir, 'test.torrent')
        retries, last = RetryMeta.read(path)
        assert retries == 0
        assert last == 0

    def test_write_and_read(self, tmp_dir):
        """Should persist retry count and timestamp."""
        path = os.path.join(tmp_dir, 'test.torrent')
        before = time.time()
        RetryMeta.write(path, 3)
        retries, last = RetryMeta.read(path)
        assert retries == 3
        assert last >= before

    def test_incremental_writes(self, tmp_dir):
        """Each write should update the retry count."""
        path = os.path.join(tmp_dir, 'test.torrent')
        for i in range(1, 4):
            RetryMeta.write(path, i)
            retries, _ = RetryMeta.read(path)
            assert retries == i

    def test_remove(self, tmp_dir):
        """Should clean up sidecar meta file."""
        path = os.path.join(tmp_dir, 'test.torrent')
        RetryMeta.write(path, 1)
        assert os.path.exists(path + '.meta')
        RetryMeta.remove(path)
        assert not os.path.exists(path + '.meta')

    def test_remove_nonexistent(self, tmp_dir):
        """Removing meta for file without sidecar should not raise."""
        path = os.path.join(tmp_dir, 'test.torrent')
        RetryMeta.remove(path)  # Should not raise

    def test_corrupt_meta_returns_defaults(self, tmp_dir):
        """Corrupt meta file should return defaults instead of crashing."""
        path = os.path.join(tmp_dir, 'test.torrent')
        meta = path + '.meta'
        with open(meta, 'w') as f:
            f.write('not json')
        retries, last = RetryMeta.read(path)
        assert retries == 0
        assert last == 0

    def test_meta_path(self, tmp_dir):
        """Meta path should be original path + .meta suffix."""
        path = os.path.join(tmp_dir, 'movie.torrent')
        assert RetryMeta.meta_path(path) == path + '.meta'

    def test_write_preserves_other_fields(self, tmp_dir):
        """write() must not wipe unrelated keys like alt_exhausted or tier_state.

        Without this guarantee, the first retry after tier_state is
        seeded would silently reset the compromise state machine —
        the dwell timer would be lost and I3 (dwell before compromise)
        would never actually gate.
        """
        path = os.path.join(tmp_dir, 'test.torrent')
        meta = path + '.meta'
        # Hand-craft a sidecar with extra fields (simulates alt-exhausted
        # path writing raw JSON, as the existing code does).
        with open(meta, 'w') as f:
            json.dump({
                'retries': 1,
                'last_attempt': 100.0,
                'alt_exhausted': True,
                'custom_future_field': 'keep-me',
            }, f)
        RetryMeta.write(path, 2)
        with open(meta, 'r') as f:
            data = json.load(f)
        assert data['retries'] == 2  # bumped
        assert data['last_attempt'] > 100.0  # refreshed
        assert data['alt_exhausted'] is True  # preserved
        assert data['custom_future_field'] == 'keep-me'  # preserved


class TestRetryMetaTierStateV2:
    """Plan 33 Phase 2 — tier_state schema on RetryMeta."""

    def test_arr_url_hash_stable_and_short(self):
        h = RetryMeta.arr_url_hash('http://sonarr:8989')
        assert len(h) == 6
        assert all(c in '0123456789abcdef' for c in h)
        # Deterministic — same input yields the same hash
        assert RetryMeta.arr_url_hash('http://sonarr:8989') == h

    def test_arr_url_hash_differentiates_instances(self):
        # Isolation for sonarr-4k vs sonarr-hd (per plan 33's per-arr keying)
        a = RetryMeta.arr_url_hash('http://sonarr-4k:8989')
        b = RetryMeta.arr_url_hash('http://sonarr-hd:8989')
        assert a != b

    def test_arr_url_hash_empty_url_returns_empty(self):
        assert RetryMeta.arr_url_hash('') == ''
        assert RetryMeta.arr_url_hash(None) == ''

    def test_read_tier_state_returns_none_for_legacy_file(self, tmp_dir):
        """Legacy v1 sidecar (no tier_state) must load as None, not raise.

        Backward compat is the load-bearing promise of the v2 schema —
        a user upgrading pd_zurg mid-retry must not lose retry state
        or crash the blackhole on the first legacy sidecar it reads.
        """
        path = os.path.join(tmp_dir, 'legacy.torrent')
        meta = path + '.meta'
        with open(meta, 'w') as f:
            json.dump({'retries': 2, 'last_attempt': 100.0}, f)
        assert RetryMeta.read_tier_state(path) is None
        # And the legacy retries/last_attempt readers still work
        retries, last = RetryMeta.read(path)
        assert retries == 2
        assert last == 100.0

    def test_read_tier_state_returns_none_for_missing_file(self, tmp_dir):
        path = os.path.join(tmp_dir, 'missing.torrent')
        assert RetryMeta.read_tier_state(path) is None

    def test_init_tier_state_seeds_fresh_file(self, tmp_dir):
        """Fresh init creates a v1 tier_state with current_tier_index=0
        and first_attempted_at=now.  These two fields are load-bearing
        for the dwell check (I3: dwell measured from first attempt)."""
        path = os.path.join(tmp_dir, 'show.torrent')
        now = 1_700_000_000.0
        ts = RetryMeta.init_tier_state(
            path, 'sonarr', 'http://sonarr:8989',
            profile_id=4, tier_order=['2160p', '1080p', '720p'], now=now,
        )
        assert ts['schema_version'] == RetryMeta.TIER_STATE_SCHEMA_VERSION
        assert ts['arr_service'] == 'sonarr'
        assert len(ts['arr_url_hash']) == 6
        assert ts['profile_id'] == 4
        assert ts['tier_order'] == ['2160p', '1080p', '720p']
        assert ts['current_tier_index'] == 0
        assert ts['first_attempted_at'] == now
        assert ts['tier_attempts'] == []
        assert ts['compromise_fired_at'] is None
        assert ts['season_pack_attempted'] is False
        # Round-trip
        loaded = RetryMeta.read_tier_state(path)
        assert loaded == ts

    def test_init_tier_state_is_idempotent(self, tmp_dir):
        """Re-init must NOT reset first_attempted_at — that would let a
        user-initiated retry game the dwell clock (I3)."""
        path = os.path.join(tmp_dir, 'show.torrent')
        t1 = 1_700_000_000.0
        t2 = t1 + 86400  # one day later
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=t1,
        )
        second = RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=t2,
        )
        assert second['first_attempted_at'] == t1

    def test_init_tier_state_preserves_legacy_fields(self, tmp_dir):
        """Seeding tier_state on a sidecar that already carries
        retries/alt_exhausted must keep those top-level fields intact."""
        path = os.path.join(tmp_dir, 'show.torrent')
        meta = path + '.meta'
        with open(meta, 'w') as f:
            json.dump({'retries': 1, 'last_attempt': 100.0, 'alt_exhausted': True}, f)
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p'], now=200.0,
        )
        with open(meta, 'r') as f:
            data = json.load(f)
        assert data['retries'] == 1
        assert data['alt_exhausted'] is True
        assert data['tier_state']['profile_id'] == 4

    def test_record_tier_attempt_creates_entry(self, tmp_dir):
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        assert RetryMeta.record_tier_attempt(
            path, tier_index=0, cached_hits=0, uncached_hits=5,
            outcome='no_cached_alts_exhausted', now=150.0,
        ) is True
        ts = RetryMeta.read_tier_state(path)
        assert len(ts['tier_attempts']) == 1
        entry = ts['tier_attempts'][0]
        assert entry['tier'] == '2160p'
        assert entry['tier_index'] == 0
        assert entry['first_tried_at'] == 150.0
        assert entry['last_tried_at'] == 150.0
        assert entry['attempts'] == 1
        assert entry['cached_hits_found'] == 0
        assert entry['uncached_hits_found'] == 5
        assert entry['outcome'] == 'no_cached_alts_exhausted'

    def test_record_tier_attempt_updates_existing_entry(self, tmp_dir):
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        RetryMeta.record_tier_attempt(path, 0, 0, 3, 'waiting', now=150.0)
        RetryMeta.record_tier_attempt(path, 0, 0, 5, 'no_cached_alts_exhausted', now=200.0)
        ts = RetryMeta.read_tier_state(path)
        assert len(ts['tier_attempts']) == 1  # upserted, not appended
        entry = ts['tier_attempts'][0]
        assert entry['first_tried_at'] == 150.0  # preserved
        assert entry['last_tried_at'] == 200.0  # refreshed
        assert entry['attempts'] == 2  # incremented
        assert entry['uncached_hits_found'] == 5  # latest value
        assert entry['outcome'] == 'no_cached_alts_exhausted'

    def test_record_tier_attempt_without_tier_state_returns_false(self, tmp_dir):
        """Guard: caller must seed tier_state first; recording without it
        is a programming error we refuse rather than silently create an
        orphan tier_attempts list."""
        path = os.path.join(tmp_dir, 'show.torrent')
        assert RetryMeta.record_tier_attempt(
            path, 0, 0, 0, 'test',
        ) is False

    def test_record_tier_attempt_rejects_bool_index(self, tmp_dir):
        """bool is-a int in Python — record_tier_attempt(True, ...) would
        otherwise alias to tier_index=1 and corrupt tier attribution."""
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        assert RetryMeta.record_tier_attempt(path, True, 0, 0, 'x') is False
        assert RetryMeta.record_tier_attempt(path, False, 0, 0, 'x') is False

    def test_advance_tier_monotonic_downward_only(self, tmp_dir):
        """I2 — current_tier_index never decrements and never stays."""
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p', '720p'], now=100.0,
        )
        assert RetryMeta.advance_tier(path, 1, 'dwell_elapsed', now=200.0) is True
        # Can't stay
        assert RetryMeta.advance_tier(path, 1, 'x') is False
        # Can't go back
        assert RetryMeta.advance_tier(path, 0, 'x') is False
        # Can go further down
        assert RetryMeta.advance_tier(path, 2, 'still_no_cached', now=300.0) is True
        ts = RetryMeta.read_tier_state(path)
        assert ts['current_tier_index'] == 2
        # compromise_fired_at set ONLY on first advance (not refreshed
        # on subsequent advances — the history value is the original
        # compromise moment, not the last tier change).
        assert ts['compromise_fired_at'] == 200.0
        assert ts['last_advance_reason'] == 'still_no_cached'

    def test_advance_tier_out_of_range_refused(self, tmp_dir):
        """I1 — never advance outside the profile's tier list."""
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        # tier_order has 2 entries (indices 0, 1); index 2 is out of range
        assert RetryMeta.advance_tier(path, 2, 'x') is False
        ts = RetryMeta.read_tier_state(path)
        assert ts['current_tier_index'] == 0  # unchanged

    def test_advance_tier_rejects_bool_index(self, tmp_dir):
        """bool-as-int guard: True would otherwise advance from 0 to 1."""
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        assert RetryMeta.advance_tier(path, True, 'x') is False
        ts = RetryMeta.read_tier_state(path)
        assert ts['current_tier_index'] == 0

    def test_advance_tier_without_state_returns_false(self, tmp_dir):
        path = os.path.join(tmp_dir, 'show.torrent')
        assert RetryMeta.advance_tier(path, 1, 'x') is False

    def test_mark_season_pack_attempted_flips_flag(self, tmp_dir):
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        assert RetryMeta.mark_season_pack_attempted(path) is True
        ts = RetryMeta.read_tier_state(path)
        assert ts['season_pack_attempted'] is True

    def test_mark_season_pack_attempted_without_state_returns_false(self, tmp_dir):
        path = os.path.join(tmp_dir, 'show.torrent')
        assert RetryMeta.mark_season_pack_attempted(path) is False

    def test_tier_state_survives_retry_count_bump(self, tmp_dir):
        """Regression: the load-bearing behavior of the write() change.
        Seeding tier_state, then bumping retries via RetryMeta.write(),
        must leave the tier_state intact."""
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        RetryMeta.write(path, 2)
        ts = RetryMeta.read_tier_state(path)
        assert ts is not None
        assert ts['profile_id'] == 4
        retries, _ = RetryMeta.read(path)
        assert retries == 2

    def test_per_arr_url_hash_keying_isolates_state(self, tmp_dir):
        """Two arr instances, same filename, separate sidecars
        (filenames already differ by directory in practice — this test
        asserts that the URL hash in tier_state is the disambiguator
        for the compromise engine, not the filename)."""
        path_a = os.path.join(tmp_dir, 'sonarr-4k', 'show.torrent')
        path_b = os.path.join(tmp_dir, 'sonarr-hd', 'show.torrent')
        os.makedirs(os.path.dirname(path_a))
        os.makedirs(os.path.dirname(path_b))
        RetryMeta.init_tier_state(
            path_a, 'sonarr', 'http://sonarr-4k:8989', 4, ['2160p'], now=100.0,
        )
        RetryMeta.init_tier_state(
            path_b, 'sonarr', 'http://sonarr-hd:8989', 2, ['1080p', '720p'], now=200.0,
        )
        ts_a = RetryMeta.read_tier_state(path_a)
        ts_b = RetryMeta.read_tier_state(path_b)
        assert ts_a['arr_url_hash'] != ts_b['arr_url_hash']
        assert ts_a['tier_order'] == ['2160p']
        assert ts_b['tier_order'] == ['1080p', '720p']
        assert ts_a['profile_id'] == 4
        assert ts_b['profile_id'] == 2

    def test_mark_alt_exhausted_preserves_tier_state(self, tmp_dir):
        """Regression: the old raw writers wiped tier_state; the new helper
        must preserve it.  This is the load-bearing fix that prevents
        alt-exhaustion from silently resetting the dwell timer."""
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        RetryMeta.record_tier_attempt(
            path, 0, cached_hits=0, uncached_hits=5,
            outcome='no_cached_alts_exhausted', now=150.0,
        )
        assert RetryMeta.mark_alt_exhausted(path) is True
        # Top-level fields set
        assert RetryMeta.is_alt_exhausted(path) is True
        retries, _ = RetryMeta.read(path)
        assert retries == 1
        # tier_state still intact — first_attempted_at unchanged,
        # tier_attempts history survived
        ts = RetryMeta.read_tier_state(path)
        assert ts is not None
        assert ts['first_attempted_at'] == 100.0
        assert len(ts['tier_attempts']) == 1
        assert ts['tier_attempts'][0]['uncached_hits_found'] == 5

    def test_is_alt_exhausted_default_false(self, tmp_dir):
        path = os.path.join(tmp_dir, 'fresh.torrent')
        assert RetryMeta.is_alt_exhausted(path) is False

    def test_read_tier_state_rejects_future_schema_version(self, tmp_dir):
        """Forward-compat guard: a downgrade to this reader must not act
        on a sidecar written by a newer schema version."""
        path = os.path.join(tmp_dir, 'show.torrent')
        meta = path + '.meta'
        future = {
            'tier_state': {
                'schema_version': RetryMeta.TIER_STATE_SCHEMA_VERSION + 1,
                'tier_order': ['2160p'],
                'current_tier_index': 0,
                'first_attempted_at': 100.0,
                'tier_attempts': [],
            }
        }
        with open(meta, 'w') as f:
            json.dump(future, f)
        assert RetryMeta.read_tier_state(path) is None

    def test_read_tier_state_rejects_pre_fix_v1_schema(self, tmp_dir):
        """Backward-compat guard: v1 sidecars seeded under the inverted-
        tier_order bug must be invalidated so ``init_tier_state`` re-seeds
        with the corrected preferred-first order on the next retry pass.
        The stored ``tier_order`` in a v1 sidecar could have ``tier_order[0]``
        as the LOWEST-quality tier; trusting it would send compromise
        upward in quality."""
        path = os.path.join(tmp_dir, 'show.torrent')
        meta = path + '.meta'
        # Perfectly valid v1 shape — only the version number makes it stale.
        # Note the ascending tier_order that pre-fix get_tier_order would
        # have produced (buggy output shape).
        stale = {
            'tier_state': {
                'schema_version': 1,
                'arr_service': 'sonarr',
                'arr_url_hash': 'a9f2c4',
                'profile_id': 4,
                'tier_order': ['480p', '720p', '1080p'],  # buggy ascending
                'current_tier_index': 0,
                'first_attempted_at': 100.0,
                'tier_attempts': [],
                'compromise_fired_at': None,
                'last_advance_reason': None,
                'season_pack_attempted': False,
            }
        }
        with open(meta, 'w') as f:
            json.dump(stale, f)
        assert RetryMeta.read_tier_state(path) is None
        # And a subsequent init_tier_state call seeds fresh with the new
        # (correct) order, resetting the dwell clock to the new ``now``.
        fresh = RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4,
            ['1080p', '720p', '480p'], now=500.0,
        )
        assert fresh['schema_version'] == RetryMeta.TIER_STATE_SCHEMA_VERSION
        assert fresh['tier_order'] == ['1080p', '720p', '480p']
        assert fresh['first_attempted_at'] == 500.0

    def test_read_tier_state_rejects_malformed_tier_order(self, tmp_dir):
        """A sidecar with tier_order=dict (hand-edit / corruption) must
        degrade to None rather than crash downstream subscripting."""
        path = os.path.join(tmp_dir, 'show.torrent')
        meta = path + '.meta'
        with open(meta, 'w') as f:
            json.dump({
                'tier_state': {
                    'schema_version': RetryMeta.TIER_STATE_SCHEMA_VERSION,
                    'tier_order': {'not': 'a list'},
                    'current_tier_index': 0,
                    'first_attempted_at': 100.0,
                    'tier_attempts': [],
                }
            }, f)
        assert RetryMeta.read_tier_state(path) is None

    def test_read_tier_state_rejects_negative_current_index(self, tmp_dir):
        """Malformed current_tier_index must fail validation."""
        path = os.path.join(tmp_dir, 'show.torrent')
        meta = path + '.meta'
        with open(meta, 'w') as f:
            json.dump({
                'tier_state': {
                    'schema_version': RetryMeta.TIER_STATE_SCHEMA_VERSION,
                    'tier_order': ['2160p'],
                    'current_tier_index': -1,
                    'first_attempted_at': 100.0,
                    'tier_attempts': [],
                }
            }, f)
        assert RetryMeta.read_tier_state(path) is None

    def test_init_tier_state_reseeds_malformed_existing(self, tmp_dir):
        """A malformed pre-existing tier_state is replaced — not silently
        trusted — so future runs have a well-formed baseline."""
        path = os.path.join(tmp_dir, 'show.torrent')
        meta = path + '.meta'
        with open(meta, 'w') as f:
            json.dump({
                'tier_state': {
                    'schema_version': RetryMeta.TIER_STATE_SCHEMA_VERSION,
                    'tier_order': 'bogus',
                }
            }, f)
        ts = RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=300.0,
        )
        assert ts['first_attempted_at'] == 300.0
        assert ts['tier_order'] == ['2160p', '1080p']

    def test_record_tier_attempt_out_of_range_refused(self, tmp_dir):
        """I1 — record_tier_attempt refuses indices outside tier_order so
        a future caller race can't stuff tier=None into the history."""
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        assert RetryMeta.record_tier_attempt(path, 2, 0, 0, 'x') is False
        assert RetryMeta.record_tier_attempt(path, 99, 0, 0, 'x') is False
        ts = RetryMeta.read_tier_state(path)
        assert ts['tier_attempts'] == []

    def test_save_raw_catches_type_error(self, tmp_dir, monkeypatch):
        """A non-serializable value in tier_state must not crash the
        watcher — catches TypeError alongside I/O errors."""
        path = os.path.join(tmp_dir, 'show.torrent')
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        # Inject a non-serializable object via monkeypatched _save_raw call
        # path: pass an un-JSONable set() as the outcome string.
        class Unserializable:
            pass

        # advance_tier goes through _save_raw; stuff a bad advance_reason
        # via a mock that short-circuits the public API.  Easier: call
        # _save_raw directly with a broken payload.
        bad = {'tier_state': {'outcome': Unserializable()}}
        assert RetryMeta._save_raw(path, bad) is False
        # Original sidecar unchanged
        ts = RetryMeta.read_tier_state(path)
        assert ts is not None
        assert ts['first_attempted_at'] == 100.0

    def test_atomic_write_leaves_old_file_intact_on_failure(self, tmp_dir, monkeypatch):
        """Simulated crash during write must leave the prior sidecar
        readable rather than corrupting it — the entire reason for
        routing tier_state through utils.file_utils.atomic_write.

        We patch json.dump to raise mid-flight; atomic_write should
        delete the temp file without replacing the target.
        """
        path = os.path.join(tmp_dir, 'show.torrent')
        # Seed a valid sidecar first
        RetryMeta.init_tier_state(
            path, 'sonarr', 'http://s:8989', 4, ['2160p', '1080p'], now=100.0,
        )
        before = RetryMeta.read_tier_state(path)
        assert before is not None

        # Simulate a crash during the next save
        import utils.blackhole as bh
        real_dump = bh.json.dump

        def exploding_dump(obj, fp, *a, **kw):
            fp.write('{"partial": tr')  # write a torn prefix
            raise OSError('simulated disk failure')

        monkeypatch.setattr(bh.json, 'dump', exploding_dump)
        # advance_tier goes through _save_raw which uses atomic_write
        result = RetryMeta.advance_tier(path, 1, 'test', now=200.0)
        monkeypatch.setattr(bh.json, 'dump', real_dump)

        assert result is False  # write reported failure
        # The target file must still be readable as the original state
        after = RetryMeta.read_tier_state(path)
        assert after is not None
        assert after == before  # unchanged

        # And no leftover temp files should linger next to it
        parent = os.path.dirname(path)
        leftovers = [f for f in os.listdir(parent)
                     if f.startswith('tmp') and f != 'show.torrent.meta']
        assert leftovers == []


class TestBlackholeWatcher:

    def test_supported_extensions(self):
        """Should support .torrent and .magnet extensions."""
        assert '.torrent' in BlackholeWatcher.SUPPORTED_EXTENSIONS
        assert '.magnet' in BlackholeWatcher.SUPPORTED_EXTENSIONS
        assert '.nzb' not in BlackholeWatcher.SUPPORTED_EXTENSIONS

    def test_scan_finds_torrent_files(self, tmp_dir):
        """Scan should detect .torrent files in watch directory."""
        # Create test files
        for name in ['movie.torrent', 'show.torrent', 'readme.txt']:
            path = os.path.join(tmp_dir, name)
            with open(path, 'w') as f:
                f.write('test')
            # Set mtime to past so files aren't skipped as "still being written"
            os.utime(path, (time.time() - 10, time.time() - 10))

        watcher = BlackholeWatcher(tmp_dir, 'fake_key', 'realdebrid')
        found = []
        for filename in os.listdir(tmp_dir):
            ext = os.path.splitext(filename)[1].lower()
            if ext in watcher.SUPPORTED_EXTENSIONS:
                found.append(filename)
        assert len(found) == 2
        assert 'readme.txt' not in found

    def test_scan_ignores_subdirectories(self, tmp_dir):
        """Scan should not process files in subdirectories."""
        subdir = os.path.join(tmp_dir, 'subdir')
        os.makedirs(subdir)
        with open(os.path.join(subdir, 'nested.torrent'), 'w') as f:
            f.write('test')

        # Only files directly in watch_dir should be found
        watcher = BlackholeWatcher(tmp_dir, 'fake_key', 'realdebrid')
        top_files = [
            f for f in os.listdir(tmp_dir)
            if os.path.isfile(os.path.join(tmp_dir, f))
        ]
        assert len(top_files) == 0

    def test_scan_skips_recent_files(self, tmp_dir):
        """Files modified within last 2 seconds should be skipped."""
        path = os.path.join(tmp_dir, 'new.torrent')
        with open(path, 'w') as f:
            f.write('still writing...')
        # File just created — mtime is now

        watcher = BlackholeWatcher(tmp_dir, 'fake_key', 'realdebrid')
        now = time.time()
        mtime = os.path.getmtime(path)
        assert now - mtime < 2.0  # Should be skipped


class TestRetrySchedule:

    def test_schedule_values(self):
        """Retry schedule should have increasing delays."""
        for i in range(1, len(RETRY_SCHEDULE)):
            assert RETRY_SCHEDULE[i] > RETRY_SCHEDULE[i - 1]

    def test_max_retries_matches_schedule(self):
        """MAX_RETRIES should be reasonable relative to schedule length."""
        assert MAX_RETRIES >= 1
        assert MAX_RETRIES <= 10

    def test_schedule_first_retry_reasonable(self):
        """First retry should be at least 60 seconds."""
        assert RETRY_SCHEDULE[0] >= 60


class TestSymlinkConstants:

    def test_media_extensions_include_common_video(self):
        """MEDIA_EXTENSIONS should include common video formats."""
        for ext in ['.mkv', '.mp4', '.avi', '.ts', '.webm']:
            assert ext in MEDIA_EXTENSIONS

    def test_media_extensions_exclude_non_video(self):
        """MEDIA_EXTENSIONS should not include non-video formats."""
        for ext in ['.nfo', '.txt', '.jpg', '.png', '.srt', '.sub']:
            assert ext not in MEDIA_EXTENSIONS

    def test_mount_categories(self):
        """MOUNT_CATEGORIES should include the standard Zurg categories."""
        assert 'shows' in MOUNT_CATEGORIES
        assert 'movies' in MOUNT_CATEGORIES
        assert 'anime' in MOUNT_CATEGORIES


class TestExtractTorrentId:

    def test_realdebrid_string_id(self):
        """RD returns torrent_id as a plain string."""
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        assert watcher._extract_torrent_id('TGLXHJIH2IFL6') == 'TGLXHJIH2IFL6'

    def test_alldebrid_json_response(self):
        """AD returns full JSON; extract magnet ID."""
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        result = {'data': {'magnets': [{'id': 12345}]}}
        assert watcher._extract_torrent_id(result) == '12345'

    def test_torbox_json_response(self):
        """TorBox returns full JSON; extract torrent_id."""
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        result = {'data': {'torrent_id': 67890}}
        assert watcher._extract_torrent_id(result) == '67890'

    def test_torbox_fallback_to_id(self):
        """TorBox should fallback to 'id' if 'torrent_id' is missing."""
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        result = {'data': {'id': 11111}}
        assert watcher._extract_torrent_id(result) == '11111'

    def test_alldebrid_malformed_response(self):
        """Should return None for malformed AD response."""
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        assert watcher._extract_torrent_id({}) is None
        assert watcher._extract_torrent_id({'data': {}}) is None

    def test_torbox_malformed_response(self):
        """Should return None for malformed TorBox response."""
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        # Empty data with no torrent_id or id returns empty string which is falsy
        result = watcher._extract_torrent_id({'data': {}})
        assert not result  # empty string or None


class TestFindOnMount:

    def test_finds_in_shows(self, tmp_dir):
        """Should find content in the shows category."""
        shows_dir = os.path.join(tmp_dir, 'shows', 'My.Show.S01')
        os.makedirs(shows_dir)
        with open(os.path.join(shows_dir, 'ep01.mkv'), 'w') as f:
            f.write('video')

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('My.Show.S01')
        assert path == shows_dir
        assert category == 'shows'
        assert matched == 'My.Show.S01'

    def test_finds_in_movies(self, tmp_dir):
        """Should find content in the movies category."""
        movies_dir = os.path.join(tmp_dir, 'movies', 'My.Movie.2024')
        os.makedirs(movies_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('My.Movie.2024')
        assert path == movies_dir
        assert category == 'movies'
        assert matched == 'My.Movie.2024'

    def test_finds_in_anime(self, tmp_dir):
        """Should find content in the anime category."""
        anime_dir = os.path.join(tmp_dir, 'anime', 'My.Anime.S01')
        os.makedirs(anime_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('My.Anime.S01')
        assert path == anime_dir
        assert category == 'anime'
        assert matched == 'My.Anime.S01'

    def test_fallback_to_all(self, tmp_dir):
        """Should fall back to __all__ if not in categorized dirs."""
        all_dir = os.path.join(tmp_dir, '__all__', 'Random.Content')
        os.makedirs(all_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Random.Content')
        assert path == all_dir
        assert category == '__all__'
        assert matched == 'Random.Content'

    def test_not_found(self, tmp_dir):
        """Should return (None, None, None) when content is not on mount."""
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Nonexistent.Release')
        assert path is None
        assert category is None
        assert matched is None

    def test_prefers_categorized_over_all(self, tmp_dir):
        """Categorized dirs should be checked before __all__."""
        # Create in both shows and __all__
        for cat in ['shows', '__all__']:
            d = os.path.join(tmp_dir, cat, 'My.Show.S01')
            os.makedirs(d)

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('My.Show.S01')
        assert category == 'shows'
        assert matched == 'My.Show.S01'

    def test_strips_video_extension(self, tmp_dir):
        """Should find folder when release name has video extension that Zurg strips."""
        # Zurg creates folder without .mkv extension
        shows_dir = os.path.join(tmp_dir, 'shows', 'Bad.Monkey.S01E01.1080p')
        os.makedirs(shows_dir)
        with open(os.path.join(shows_dir, 'Bad.Monkey.S01E01.1080p.mkv'), 'w') as f:
            f.write('video')

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        # RD returns filename WITH .mkv extension
        path, category, matched = watcher._find_on_mount('Bad.Monkey.S01E01.1080p.mkv')
        assert path == shows_dir
        assert category == 'shows'
        assert matched == 'Bad.Monkey.S01E01.1080p'

    def test_prefers_exact_name_over_stripped(self, tmp_dir):
        """Should prefer exact name match over extension-stripped match."""
        # Both exist: exact match (with .mkv in folder name) and stripped
        exact_dir = os.path.join(tmp_dir, 'shows', 'Release.Name.mkv')
        stripped_dir = os.path.join(tmp_dir, 'shows', 'Release.Name')
        os.makedirs(exact_dir)
        os.makedirs(stripped_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Release.Name.mkv')
        assert path == exact_dir
        assert matched == 'Release.Name.mkv'

    def test_no_strip_for_non_media_extension(self, tmp_dir):
        """Should not strip non-media extensions like .nfo."""
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Release.Name.nfo')
        assert path is None
        assert matched is None


class TestCreateSymlinks:

    def _make_watcher(self, tmp_dir):
        """Create a watcher configured for symlink testing."""
        completed = os.path.join(tmp_dir, 'completed')
        mount = os.path.join(tmp_dir, 'mount')
        os.makedirs(completed)
        os.makedirs(mount)
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed,
            rclone_mount=mount,
            symlink_target_base='/mnt/debrid',
        )
        return watcher, completed, mount

    def test_creates_symlinks_for_media_files(self, tmp_dir):
        """Should create symlinks only for media files."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        # Create mock content on mount
        release = 'My.Show.S01E01'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        for name in ['episode.mkv', 'episode.nfo', 'poster.jpg', 'sample.mkv']:
            with open(os.path.join(release_dir, name), 'w') as f:
                f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        # Only episode.mkv — sample.mkv is skipped, .nfo and .jpg are non-media
        assert count == 1

        symlink = os.path.join(completed, release, 'episode.mkv')
        assert os.path.islink(symlink)
        target = os.readlink(symlink)
        assert target == f'/mnt/debrid/shows/{release}/episode.mkv'

    def test_skips_sample_files(self, tmp_dir):
        """Files with 'sample' in the name should be skipped."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Movie.2024'
        release_dir = os.path.join(mount, 'movies', release)
        os.makedirs(release_dir)
        for name in ['Movie.2024.mkv', 'Sample.mkv', 'movie-sample.mp4']:
            with open(os.path.join(release_dir, name), 'w') as f:
                f.write('data')

        count = watcher._create_symlinks(release, 'movies', release_dir)
        assert count == 1  # Only Movie.2024.mkv

    def test_skips_existing_symlinks(self, tmp_dir):
        """Should not recreate existing symlinks."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Movie.2024'
        release_dir = os.path.join(mount, 'movies', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'movie.mkv'), 'w') as f:
            f.write('data')

        # Create first time
        count1 = watcher._create_symlinks(release, 'movies', release_dir)
        assert count1 == 1

        # Try again — should skip existing
        count2 = watcher._create_symlinks(release, 'movies', release_dir)
        assert count2 == 0

    def test_handles_nested_directories(self, tmp_dir):
        """Should handle files in subdirectories within a release."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01.Complete'
        release_dir = os.path.join(mount, 'shows', release)
        sub = os.path.join(release_dir, 'Season 01')
        os.makedirs(sub)
        with open(os.path.join(sub, 'S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(sub, 'S01E02.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 2

        symlink = os.path.join(completed, release, 'Season 01', 'S01E01.mkv')
        assert os.path.islink(symlink)
        target = os.readlink(symlink)
        assert target == f'/mnt/debrid/shows/{release}/Season 01/S01E01.mkv'

    def test_symlink_target_uses_configured_base(self, tmp_dir):
        """Symlink targets should use the configured target base path."""
        completed = os.path.join(tmp_dir, 'completed')
        mount = os.path.join(tmp_dir, 'mount')
        os.makedirs(completed)
        os.makedirs(mount)
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed,
            rclone_mount=mount,
            symlink_target_base='/custom/path',
        )

        release = 'Movie.2024'
        release_dir = os.path.join(mount, 'movies', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'movie.mp4'), 'w') as f:
            f.write('data')

        watcher._create_symlinks(release, 'movies', release_dir)
        target = os.readlink(os.path.join(completed, release, 'movie.mp4'))
        assert target.startswith('/custom/path/')

    def test_path_traversal_blocked(self, tmp_dir):
        """Release names with path traversal should be handled safely."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Normal.Release'
        release_dir = os.path.join(mount, 'movies', release)
        # Create a file with a path-traversal relative path
        sub = os.path.join(release_dir, '..', '..', 'escape')
        os.makedirs(sub, exist_ok=True)
        with open(os.path.join(sub, 'evil.mkv'), 'w') as f:
            f.write('data')

        # The traversal should be caught by the guard
        count = watcher._create_symlinks(release, 'movies', release_dir)
        # Should not create symlinks outside the completed release dir
        assert not os.path.exists(os.path.join(completed, 'escape'))


class TestPendingMonitors:

    def test_add_and_load_pending(self, tmp_dir):
        """Should persist pending monitors to disk."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )

        watcher._add_pending('torrent123', 'movie.torrent')
        entries = watcher._load_pending()
        assert len(entries) == 1
        assert entries[0]['torrent_id'] == 'torrent123'
        assert entries[0]['filename'] == 'movie.torrent'
        assert entries[0]['service'] == 'realdebrid'

    def test_add_pending_deduplicates(self, tmp_dir):
        """Should not add duplicate torrent IDs."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )

        watcher._add_pending('torrent123', 'movie.torrent')
        watcher._add_pending('torrent123', 'movie.torrent')
        entries = watcher._load_pending()
        assert len(entries) == 1

    def test_remove_pending(self, tmp_dir):
        """Should remove a specific torrent from pending."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )

        watcher._add_pending('torrent1', 'file1.torrent')
        watcher._add_pending('torrent2', 'file2.torrent')
        watcher._remove_pending('torrent1')
        entries = watcher._load_pending()
        assert len(entries) == 1
        assert entries[0]['torrent_id'] == 'torrent2'

    def test_load_pending_missing_file(self, tmp_dir):
        """Should return empty list when no pending file exists."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        assert watcher._load_pending() == []

    def test_load_pending_corrupt_file(self, tmp_dir):
        """Should return empty list for corrupt pending file."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        with open(watcher._pending_file, 'w') as f:
            f.write('not valid json')
        assert watcher._load_pending() == []

    def test_pending_file_in_completed_dir(self, tmp_dir):
        """Pending file should be stored in completed_dir, not watch_dir."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        assert watcher._pending_file.startswith(completed)


class TestSymlinkCleanup:

    def test_removes_broken_symlinks(self, tmp_dir):
        """Should remove symlinks whose targets no longer exist."""
        completed = os.path.join(tmp_dir, 'completed')
        release_dir = os.path.join(completed, 'Old.Release')
        os.makedirs(release_dir)

        # Create a broken symlink
        symlink = os.path.join(release_dir, 'episode.mkv')
        os.symlink('/nonexistent/path/episode.mkv', symlink)
        assert os.path.islink(symlink)
        assert not os.path.exists(symlink)  # broken

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        watcher._cleanup_symlinks()

        # Broken symlink should be removed, and empty dir cleaned up
        assert not os.path.islink(symlink)
        assert not os.path.exists(release_dir)

    def test_preserves_valid_symlinks(self, tmp_dir):
        """Should not remove directories with valid symlinks."""
        completed = os.path.join(tmp_dir, 'completed')
        release_dir = os.path.join(completed, 'Good.Release')
        os.makedirs(release_dir)

        # Create a valid symlink target
        target_file = os.path.join(tmp_dir, 'real_file.mkv')
        with open(target_file, 'w') as f:
            f.write('video data')

        symlink = os.path.join(release_dir, 'episode.mkv')
        os.symlink(target_file, symlink)
        assert os.path.exists(symlink)

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
            symlink_max_age=0,  # Disable age-based cleanup
        )
        watcher._cleanup_symlinks()

        assert os.path.islink(symlink)
        assert os.path.isdir(release_dir)

    def test_age_based_cleanup(self, tmp_dir):
        """Should remove directories older than max age."""
        completed = os.path.join(tmp_dir, 'completed')
        release_dir = os.path.join(completed, 'Old.Release')
        os.makedirs(release_dir)

        # Create a valid symlink
        target_file = os.path.join(tmp_dir, 'real_file.mkv')
        with open(target_file, 'w') as f:
            f.write('data')
        symlink = os.path.join(release_dir, 'ep.mkv')
        os.symlink(target_file, symlink)

        # Set mtime to 100 hours ago
        old_time = time.time() - (100 * 3600)
        os.utime(release_dir, (old_time, old_time))

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
            symlink_max_age=72,  # 72 hours
        )
        watcher._cleanup_symlinks()

        assert not os.path.exists(release_dir)

    def test_age_zero_disables_age_cleanup(self, tmp_dir):
        """symlink_max_age=0 should disable age-based removal."""
        completed = os.path.join(tmp_dir, 'completed')
        release_dir = os.path.join(completed, 'Old.Release')
        os.makedirs(release_dir)

        target_file = os.path.join(tmp_dir, 'real_file.mkv')
        with open(target_file, 'w') as f:
            f.write('data')
        symlink = os.path.join(release_dir, 'ep.mkv')
        os.symlink(target_file, symlink)

        # Set very old mtime
        old_time = time.time() - (1000 * 3600)
        os.utime(release_dir, (old_time, old_time))

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
            symlink_max_age=0,
        )
        watcher._cleanup_symlinks()

        assert os.path.exists(release_dir)

    def test_cleanup_skipped_when_disabled(self, tmp_dir):
        """Cleanup should do nothing when symlinks are disabled."""
        watcher = BlackholeWatcher(tmp_dir, 'key', 'realdebrid', symlink_enabled=False)
        # Should not raise even with no completed_dir
        watcher._cleanup_symlinks()


class TestTorrentStatusHelpers:

    def test_is_torrent_ready_realdebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        assert watcher._is_torrent_ready('downloaded') is True
        assert watcher._is_torrent_ready('downloading') is False
        assert watcher._is_torrent_ready('queued') is False

    def test_is_torrent_ready_alldebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        assert watcher._is_torrent_ready('Ready') is True
        assert watcher._is_torrent_ready('Downloading') is False

    def test_is_torrent_ready_torbox(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        assert watcher._is_torrent_ready('completed') is True
        assert watcher._is_torrent_ready('downloading') is False

    def test_is_terminal_error_realdebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        assert watcher._is_terminal_error('magnet_error') is True
        assert watcher._is_terminal_error('error') is True
        assert watcher._is_terminal_error('virus') is True
        assert watcher._is_terminal_error('dead') is True
        assert watcher._is_terminal_error('downloading') is False

    def test_is_terminal_error_alldebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        assert watcher._is_terminal_error('Error') is True
        assert watcher._is_terminal_error('Ready') is False

    def test_is_terminal_error_torbox(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        assert watcher._is_terminal_error('error') is True
        assert watcher._is_terminal_error('failed') is True
        assert watcher._is_terminal_error('completed') is False


class TestExtractReleaseName:

    def test_realdebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        info = {'filename': 'Landman.S01.1080p'}
        assert watcher._extract_release_name(info) == 'Landman.S01.1080p'

    def test_alldebrid(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        info = {'data': {'magnets': {'filename': 'Movie.2024'}}}
        assert watcher._extract_release_name(info) == 'Movie.2024'

    def test_torbox(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        info = {'data': {'name': 'Show.S02'}}
        assert watcher._extract_release_name(info) == 'Show.S02'

    def test_missing_data(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        assert watcher._extract_release_name({}) == ''


class TestWatcherSymlinkInit:

    def test_default_symlink_disabled(self):
        """Symlink should be disabled by default."""
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        assert watcher.symlink_enabled is False

    def test_symlink_config_passed_through(self, tmp_dir):
        """All symlink config should be stored on the watcher."""
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed,
            rclone_mount='/data',
            symlink_target_base='/mnt/debrid',
            mount_poll_timeout=600,
            mount_poll_interval=15,
            symlink_max_age=48,
        )
        assert watcher.symlink_enabled is True
        assert watcher.completed_dir == completed
        assert watcher.rclone_mount == '/data'
        assert watcher.symlink_target_base == '/mnt/debrid'
        assert watcher.mount_poll_timeout == 600
        assert watcher.mount_poll_interval == 15
        assert watcher.symlink_max_age == 48


class TestParseReleaseName:

    def test_tv_episode(self):
        name, season, is_tv = parse_release_name('Bad.Monkey.S01E01.1080p.ATVP.WEB-DL.DDP5.1.H.264-NTb.torrent')
        assert name == 'Bad Monkey'
        assert season == 1
        assert is_tv is True

    def test_tv_season_pack(self):
        name, season, is_tv = parse_release_name('Fargo.S05.COMPLETE.1080p.torrent')
        assert name == 'Fargo'
        assert season == 5
        assert is_tv is True

    def test_tv_with_year(self):
        name, season, is_tv = parse_release_name('Fargo.2014.S03E01.720p.torrent')
        assert name == 'Fargo'
        assert season == 3
        assert is_tv is True

    def test_movie(self):
        name, season, is_tv = parse_release_name('Gattaca.1997.1080p.BluRay.torrent')
        assert name == 'Gattaca'
        assert season is None
        assert is_tv is False

    def test_movie_no_year(self):
        name, season, is_tv = parse_release_name('Some.Movie.1080p.WEB.torrent')
        assert name == 'Some Movie'
        assert season is None
        assert is_tv is False

    def test_magnet_extension(self):
        name, season, is_tv = parse_release_name('Show.S02E05.720p.magnet')
        assert name == 'Show'
        assert season == 2
        assert is_tv is True

    def test_season_at_end(self):
        """S01 at end of name with no episode number."""
        name, season, is_tv = parse_release_name('Some.Show.S01.torrent')
        assert name == 'Some Show'
        assert season == 1
        assert is_tv is True


class TestCheckLocalLibrary:

    def _make_watcher(self, tmp_dir):
        tv_dir = os.path.join(tmp_dir, 'tv')
        movies_dir = os.path.join(tmp_dir, 'movies')
        os.makedirs(tv_dir)
        os.makedirs(movies_dir)
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            dedup_enabled=True,
            local_library_tv=tv_dir,
            local_library_movies=movies_dir,
        )
        return watcher, tv_dir, movies_dir

    def test_skips_existing_tv_episode(self, tmp_dir):
        """Should skip if the specific episode exists locally."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 05')
        os.makedirs(season_dir)
        with open(os.path.join(season_dir, 'Fargo (2014) - S05E01 - The Tragedy of the Commons.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Fargo.S05E01.1080p.WEB.torrent') is True

    def test_allows_missing_episode(self, tmp_dir):
        """Should allow if the season exists but the specific episode doesn't."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 05')
        os.makedirs(season_dir)
        with open(os.path.join(season_dir, 'Fargo (2014) - S05E01 - The Tragedy of the Commons.mkv'), 'w') as f:
            f.write('data')

        # E03 is not present locally — should NOT skip
        assert watcher._check_local_library('Fargo.S05E03.1080p.WEB.torrent') is False

    def test_allows_missing_season(self, tmp_dir):
        """Should allow if the show exists but the season doesn't."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 01')
        os.makedirs(season_dir)
        with open(os.path.join(season_dir, 'Fargo (2014) - S01E01 - The Crocodiles Dilemma.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Fargo.S05E01.1080p.WEB.torrent') is False

    def test_skips_existing_movie(self, tmp_dir):
        """Should skip if the movie exists locally."""
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        movie_dir = os.path.join(movies_dir, 'Gattaca (1997)')
        os.makedirs(movie_dir)
        with open(os.path.join(movie_dir, 'Gattaca.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Gattaca.1997.1080p.BluRay.torrent') is True

    def test_allows_missing_movie(self, tmp_dir):
        """Should allow if the movie doesn't exist locally."""
        watcher, _, _ = self._make_watcher(tmp_dir)
        assert watcher._check_local_library('Gattaca.1997.1080p.BluRay.torrent') is False

    def test_disabled_by_default(self, tmp_dir):
        """Should always return False when dedup is disabled."""
        watcher = BlackholeWatcher(os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid')
        assert watcher._check_local_library('Fargo.S05E01.torrent') is False

    def test_no_false_positive_substring(self, tmp_dir):
        """Should not match 'Fargo' against 'Wells Fargo Documentary'."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        other_dir = os.path.join(tv_dir, 'Wells Fargo Documentary', 'Season 01')
        os.makedirs(other_dir)
        with open(os.path.join(other_dir, 'ep01.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Fargo.S01E01.torrent') is False

    def test_missing_library_path(self, tmp_dir):
        """Should not crash when library path doesn't exist."""
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            dedup_enabled=True,
            local_library_tv='/nonexistent/path',
            local_library_movies='/nonexistent/path',
        )
        assert watcher._check_local_library('Fargo.S01E01.torrent') is False

    def test_empty_season_dir_not_matched(self, tmp_dir):
        """Should not match a season directory that has no files."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 05')
        os.makedirs(season_dir)
        # Empty season dir

        assert watcher._check_local_library('Fargo.S05E01.1080p.WEB.torrent') is False


class TestCheckLocalLibraryPreferDebridBypass:
    """Dedup bypass when the user has 'prefer-debrid' set for the title.

    The preference key uses the canonical title (potentially with colons,
    apostrophes, ampersands) while the release filename is dot-separated
    and loses that punctuation during parsing.  The bypass must match
    punctuation-insensitively or prefer-debrid force-grabs get dropped.
    """

    @pytest.fixture(autouse=True)
    def _isolate_prefs(self, tmp_dir, monkeypatch):
        import utils.library_prefs as lp
        monkeypatch.setattr(lp, 'PREFS_PATH', os.path.join(tmp_dir, 'library_prefs.json'))

    def _make_watcher(self, tmp_dir):
        tv_dir = os.path.join(tmp_dir, 'tv')
        movies_dir = os.path.join(tmp_dir, 'movies')
        os.makedirs(tv_dir)
        os.makedirs(movies_dir)
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            dedup_enabled=True,
            local_library_tv=tv_dir,
            local_library_movies=movies_dir,
        )
        return watcher, tv_dir, movies_dir

    def _seed_movie(self, movies_dir, folder):
        movie_path = os.path.join(movies_dir, folder)
        os.makedirs(movie_path)
        with open(os.path.join(movie_path, 'movie.mkv'), 'w') as f:
            f.write('data')

    def _seed_show_episode(self, tv_dir, folder, season, episode_basename):
        season_dir = os.path.join(tv_dir, folder, f'Season {season:02d}')
        os.makedirs(season_dir)
        with open(os.path.join(season_dir, episode_basename), 'w') as f:
            f.write('data')

    def test_bypass_movie_with_colon_in_pref_key(self, tmp_dir):
        """Pref key 'lego dc batman: family matters' must match dot-separated release."""
        import utils.library_prefs as lp
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        self._seed_movie(movies_dir, 'Lego DC Batman Family Matters (2019)')
        lp.set_preference('lego dc batman: family matters', 'prefer-debrid')

        assert watcher._check_local_library(
            'LEGO.DC.Batman.Family.Matters.2019.1080p.WEB-DL.DD5.1.H264-CMRG.magnet'
        ) is False

    def test_bypass_movie_with_apostrophe_in_pref_key(self, tmp_dir):
        """Pref key with apostrophe must match release that dropped it."""
        import utils.library_prefs as lp
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        self._seed_movie(movies_dir, "Ocean's Eleven (2001)")
        lp.set_preference("ocean's eleven", 'prefer-debrid')

        assert watcher._check_local_library(
            'Oceans.Eleven.2001.1080p.BluRay.x264.torrent'
        ) is False

    def test_bypass_show_with_colon_in_pref_key(self, tmp_dir):
        """TV parity: colon in canonical show title must still bypass dedup."""
        import utils.library_prefs as lp
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        self._seed_show_episode(
            tv_dir, 'Marvels Agents of SHIELD (2013)', 1,
            'Marvels Agents of SHIELD (2013) - S01E01.mkv',
        )
        lp.set_preference("marvel's agents of s.h.i.e.l.d.", 'prefer-debrid')

        assert watcher._check_local_library(
            'Marvels.Agents.of.SHIELD.S01E01.1080p.WEB.torrent'
        ) is False

    def test_no_bypass_for_prefer_local(self, tmp_dir):
        """Only 'prefer-debrid' bypasses — 'prefer-local' must not."""
        import utils.library_prefs as lp
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        self._seed_movie(movies_dir, 'Lego DC Batman Family Matters (2019)')
        lp.set_preference('lego dc batman: family matters', 'prefer-local')

        assert watcher._check_local_library(
            'LEGO.DC.Batman.Family.Matters.2019.1080p.WEB-DL.torrent'
        ) is True

    def test_no_bypass_without_matching_pref(self, tmp_dir):
        """Different title with prefer-debrid must not bypass unrelated release."""
        import utils.library_prefs as lp
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        self._seed_movie(movies_dir, 'Gattaca (1997)')
        lp.set_preference('some other movie', 'prefer-debrid')

        assert watcher._check_local_library(
            'Gattaca.1997.1080p.BluRay.torrent'
        ) is True

    def test_no_bypass_when_prefs_empty(self, tmp_dir):
        """Baseline: no preferences set, dedup still runs normally."""
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        self._seed_movie(movies_dir, 'Gattaca (1997)')

        assert watcher._check_local_library(
            'Gattaca.1997.1080p.BluRay.torrent'
        ) is True

    def test_bypass_when_release_retains_parenthesized_year(self, tmp_dir):
        """Release filenames with `(YYYY)` parens must still match a pref
        key stored under the _normalize_title-stripped form.

        parse_release_name's year regex requires `[.\\s](\\d{4})[.\\s]` but
        a filename like `Name.(2014).quality.torrent` has `(` before the
        year, so it falls through to the quality-match path and preserves
        `(2014)` in the parsed name.  norm_for_matching keeps digits, so a
        fuzzy-only bypass would miss.  The strict (_normalize_title) branch
        must catch it.
        """
        import utils.library_prefs as lp
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        self._seed_movie(movies_dir, 'Gattaca (1997)')
        lp.set_preference('gattaca', 'prefer-debrid')

        assert watcher._check_local_library(
            'Gattaca.(1997).1080p.BluRay.x264.torrent'
        ) is False

    def test_bypass_for_non_ascii_title(self, tmp_dir):
        """Native-script pref key matching native-script release name.

        norm_for_matching transliterates to ASCII via NFKD+encode-ignore,
        which drops non-decomposable CJK/Arabic/Cyrillic characters to
        empty string.  The strict branch compares _normalize_title
        directly (preserves non-ASCII) so this case still bypasses.
        """
        import utils.library_prefs as lp
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        self._seed_show_episode(
            tv_dir, '鬼滅の刃', 1, 'ep01.mkv',
        )
        lp.set_preference('鬼滅の刃', 'prefer-debrid')

        assert watcher._check_local_library('鬼滅の刃.S01E01.1080p.torrent') is False


class TestIsMultiSeasonPack:

    def test_s01_s05(self):
        is_multi, start, end = _is_multi_season_pack('Show.S01-S05.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 5

    def test_s01_05_bare(self):
        is_multi, start, end = _is_multi_season_pack('Show.S01-05.BluRay')
        assert is_multi is True
        assert start == 1
        assert end == 5

    def test_cross_season_episodes(self):
        is_multi, start, end = _is_multi_season_pack('Show.S01E01-S03E12.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 3

    def test_complete_series(self):
        is_multi, start, end = _is_multi_season_pack('Show.Complete.Series.1080p')
        assert is_multi is True
        assert start is None
        assert end is None

    def test_complete_collection(self):
        is_multi, start, end = _is_multi_season_pack('Show.Complete.Collection.BluRay')
        assert is_multi is True
        assert start is None
        assert end is None

    def test_seasons_range(self):
        is_multi, start, end = _is_multi_season_pack('Show.Seasons.1-3.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 3

    def test_season_singular_range(self):
        is_multi, start, end = _is_multi_season_pack('Show.Season.1-5.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 5

    def test_seasons_ampersand(self):
        is_multi, start, end = _is_multi_season_pack('Show.Seasons.1.&.2.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 2

    def test_seasons_and_separator(self):
        is_multi, start, end = _is_multi_season_pack('Project Blue Book Seasons 1 and 2 Mp4 1080p')
        assert is_multi is True
        assert start == 1
        assert end == 2

    def test_series_range(self):
        is_multi, start, end = _is_multi_season_pack('Show.Series.1-3.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 3

    def test_single_season_not_multi(self):
        is_multi, _, _ = _is_multi_season_pack('Show.S03.1080p')
        assert is_multi is False

    def test_single_episode_not_multi(self):
        is_multi, _, _ = _is_multi_season_pack('Show.S03E01.1080p')
        assert is_multi is False

    def test_movie_not_multi(self):
        is_multi, _, _ = _is_multi_season_pack('Movie.2024.1080p')
        assert is_multi is False

    def test_single_season_episode_range_not_multi(self):
        """S01E01-E05 is a multi-episode single-season pack, NOT multi-season."""
        is_multi, _, _ = _is_multi_season_pack('Show.S01E01-E05.1080p')
        assert is_multi is False

    def test_en_dash_separator(self):
        is_multi, start, end = _is_multi_season_pack('Show.S01\u2013S05.1080p')
        assert is_multi is True
        assert start == 1
        assert end == 5

    def test_encoding_marker_not_multi(self):
        """S05-10bit is an encoding marker, NOT a multi-season range."""
        is_multi, _, _ = _is_multi_season_pack('Show.S05-10bit.HEVC.1080p')
        assert is_multi is False

    def test_3d_marker_not_multi(self):
        is_multi, _, _ = _is_multi_season_pack('Show.S02-3D.BluRay.1080p')
        assert is_multi is False

    def test_same_season_number_not_multi(self):
        is_multi, _, _ = _is_multi_season_pack('Show.S02-S02.1080p')
        assert is_multi is False


class TestExtractFileSeason:

    def test_standard_sxxexx(self):
        assert _extract_file_season('Show.S01E04.1080p.mkv') == 1

    def test_high_season_number(self):
        assert _extract_file_season('Show.S12E01.mkv') == 12

    def test_lowercase(self):
        assert _extract_file_season('show.s3e12.mkv') == 3

    def test_parent_dir_season(self):
        assert _extract_file_season('Season 2/Show.E05.mkv') == 2

    def test_parent_dir_season_dot_format(self):
        assert _extract_file_season('Season.02/Show.E05.mkv') == 2

    def test_no_season_info(self):
        assert _extract_file_season('Show.1080p.mkv') is None

    def test_absolute_episode_only(self):
        assert _extract_file_season('Show.E26.mkv') is None

    def test_s_prefix_dir(self):
        assert _extract_file_season('S03/Show.E01.mkv') == 3

    def test_season_zero_specials(self):
        assert _extract_file_season('Show.S00E01.Special.mkv') == 0

    def test_sxx_without_exx(self):
        """Sxx without episode number should still extract season."""
        assert _extract_file_season('Show.S03.Special.mkv') == 3

    def test_sxx_with_title(self):
        assert _extract_file_season('Show.S02.The.Cats.Meow.mkv') == 2


class TestBuildSeasonReleaseName:

    def test_s_range(self):
        result = _build_season_release_name('Breaking.Bad.S01-S05.1080p.BluRay-GROUP', 3)
        assert result == 'Breaking.Bad.S03.1080p.BluRay-GROUP'

    def test_complete_series(self):
        result = _build_season_release_name('The.Wire.Complete.Series.1080p', 2)
        assert result == 'The.Wire.S02.1080p'

    def test_cross_season_episodes(self):
        result = _build_season_release_name('Show.S01E01-S03E12.1080p', 1)
        assert result == 'Show.S01.1080p'

    def test_seasons_range(self):
        result = _build_season_release_name('Show.Seasons.1-5.BluRay', 4)
        assert result == 'Show.S04.BluRay'

    def test_s_bare_range(self):
        result = _build_season_release_name('Show.S01-05.1080p', 3)
        assert result == 'Show.S03.1080p'

    def test_preserves_group_tag(self):
        result = _build_season_release_name('Show.S01-S03.1080p.WEB-DL-GROUP', 2)
        assert result == 'Show.S02.1080p.WEB-DL-GROUP'

    def test_complete_collection(self):
        result = _build_season_release_name('Show.Complete.Collection.BluRay', 1)
        assert result == 'Show.S01.BluRay'

    def test_no_double_dots(self):
        result = _build_season_release_name('Show.Complete.Series.1080p', 5)
        assert '..' not in result

    def test_fallback_appends_season(self):
        result = _build_season_release_name('Random.Name.1080p', 3)
        assert result == 'Random.Name.1080p.S03'


class TestMultiSeasonSymlinks:

    def _make_watcher(self, tmp_dir):
        completed = os.path.join(tmp_dir, 'completed')
        mount = os.path.join(tmp_dir, 'mount')
        os.makedirs(completed)
        os.makedirs(mount)
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed,
            rclone_mount=mount,
            symlink_target_base='/mnt/debrid',
        )
        return watcher, completed, mount

    def test_splits_multi_season_pack(self, tmp_dir):
        """Multi-season pack should create per-season directories."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S03.1080p.BluRay-GROUP'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)

        for ep in ['Show.S01E01.1080p.mkv', 'Show.S01E02.1080p.mkv',
                    'Show.S02E01.1080p.mkv', 'Show.S03E01.1080p.mkv',
                    'Show.S03E02.1080p.mkv']:
            with open(os.path.join(release_dir, ep), 'w') as f:
                f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 5

        # Verify per-season directories exist
        s1_dir = os.path.join(completed, 'Show.S01.1080p.BluRay-GROUP')
        s2_dir = os.path.join(completed, 'Show.S02.1080p.BluRay-GROUP')
        s3_dir = os.path.join(completed, 'Show.S03.1080p.BluRay-GROUP')
        assert os.path.isdir(s1_dir)
        assert os.path.isdir(s2_dir)
        assert os.path.isdir(s3_dir)

        # Verify file counts per season
        assert len(os.listdir(s1_dir)) == 2
        assert len(os.listdir(s2_dir)) == 1
        assert len(os.listdir(s3_dir)) == 2

        # Verify symlink targets still point to original mount path
        link = os.path.join(s1_dir, 'Show.S01E01.1080p.mkv')
        assert os.path.islink(link)
        target = os.readlink(link)
        assert target == f'/mnt/debrid/shows/{release}/Show.S01E01.1080p.mkv'

    def test_single_season_unchanged(self, tmp_dir):
        """Single-season pack should use original single-dir behavior."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S03.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'Show.S03E01.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 1

        # Should be in the original release name dir, not a constructed one
        assert os.path.isdir(os.path.join(completed, release))
        assert os.path.islink(os.path.join(completed, release, 'Show.S03E01.mkv'))

    def test_no_original_dir_when_split(self, tmp_dir):
        """When split succeeds, the original multi-season dir should NOT be created."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        for ep in ['Show.S01E01.mkv', 'Show.S02E01.mkv']:
            with open(os.path.join(release_dir, ep), 'w') as f:
                f.write('data')

        watcher._create_symlinks(release, 'shows', release_dir)
        assert not os.path.exists(os.path.join(completed, release))

    def test_fallback_when_no_seasons_parseable(self, tmp_dir):
        """Multi-season name with unparseable files falls back to single dir."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.Complete.Series.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        # Files without SxxExx patterns
        with open(os.path.join(release_dir, 'episode1.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'episode2.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 2

        # Should fall back to original single-dir behavior
        assert os.path.isdir(os.path.join(completed, release))

    def test_fallback_when_only_one_season(self, tmp_dir):
        """Multi-season name but all files are one season — use single dir."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S05.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        # All files are season 3
        for ep in ['Show.S03E01.mkv', 'Show.S03E02.mkv']:
            with open(os.path.join(release_dir, ep), 'w') as f:
                f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 2

        # Falls back to single dir since only 1 season found
        assert os.path.isdir(os.path.join(completed, release))

    def test_skips_unparseable_files_in_split(self, tmp_dir):
        """Files without season info should be skipped during splitting."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'Show.S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'Show.S02E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'extras.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        # Only 2 files with parseable seasons, extras skipped
        assert count == 2

    def test_season_zero_specials(self, tmp_dir):
        """Season 0 (specials) should get their own directory."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S00-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'Show.S00E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'Show.S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'Show.S02E01.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 3
        assert os.path.isdir(os.path.join(completed, 'Show.S00.1080p'))
        assert os.path.isdir(os.path.join(completed, 'Show.S01.1080p'))
        assert os.path.isdir(os.path.join(completed, 'Show.S02.1080p'))

    def test_subdirectory_season_extraction(self, tmp_dir):
        """Files in Season subdirs should preserve directory structure in split."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        s1_dir = os.path.join(release_dir, 'Season 01')
        s2_dir = os.path.join(release_dir, 'Season 02')
        os.makedirs(s1_dir)
        os.makedirs(s2_dir)
        with open(os.path.join(s1_dir, 'Show.S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(s2_dir, 'Show.S02E01.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 2

        s1_completed = os.path.join(completed, 'Show.S01.1080p')
        s2_completed = os.path.join(completed, 'Show.S02.1080p')
        assert os.path.isdir(s1_completed)
        assert os.path.isdir(s2_completed)

        # Subdirectory structure should be preserved
        symlink = os.path.join(s1_completed, 'Season 01', 'Show.S01E01.mkv')
        assert os.path.islink(symlink)
        target = os.readlink(symlink)
        assert target == f'/mnt/debrid/shows/{release}/Season 01/Show.S01E01.mkv'

    def test_sample_files_skipped_in_split(self, tmp_dir):
        """Sample files should be skipped during multi-season splitting too."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'Show.S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'Show.S02E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'Sample.S01E01.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 2

    def test_split_idempotency(self, tmp_dir):
        """Calling _create_symlinks twice on a multi-season pack should be idempotent."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        for ep in ['Show.S01E01.mkv', 'Show.S02E01.mkv']:
            with open(os.path.join(release_dir, ep), 'w') as f:
                f.write('data')

        count1 = watcher._create_symlinks(release, 'shows', release_dir)
        assert count1 == 2

        count2 = watcher._create_symlinks(release, 'shows', release_dir)
        assert count2 == 0

    def test_fallback_does_not_create_split_dirs(self, tmp_dir):
        """When falling back to single dir, no per-season directories should exist."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'Show.Complete.Series.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'episode1.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(release_dir, 'episode2.mkv'), 'w') as f:
            f.write('data')

        watcher._create_symlinks(release, 'shows', release_dir)
        # No per-season dirs should be created
        assert not os.path.exists(os.path.join(completed, 'Show.S01.1080p'))


class TestEnrichForHistory:
    """Tests for _enrich_for_history helper that extracts media_title and episode."""

    def test_tv_single_episode(self):
        name, ep = _enrich_for_history('Breaking.Bad.S01E05.1080p.WEB.mkv.torrent')
        assert name == 'Breaking Bad'
        assert ep == 'S01E05'

    def test_tv_multi_episode(self):
        name, ep = _enrich_for_history('Show.Name.S02E03E04.720p.torrent')
        assert name == 'Show Name'
        assert ep == 'S02E03E04'

    def test_tv_season_pack(self):
        name, ep = _enrich_for_history('Show.S03.Complete.1080p.torrent')
        assert name == 'Show'
        assert ep == 'S03'

    def test_movie(self):
        name, ep = _enrich_for_history('The.Dark.Knight.2008.BluRay.1080p.torrent')
        assert name == 'The Dark Knight'
        assert ep is None

    def test_movie_no_year(self):
        name, ep = _enrich_for_history('SomeMovie.1080p.WEB.torrent')
        assert name == 'SomeMovie'
        assert ep is None

    def test_empty_name_returns_none(self):
        name, ep = _enrich_for_history('.torrent')
        assert name is None


class TestDiscRipDetection:
    """Tests for _has_usable_media_files and _extract_filenames_from_info."""

    # ── RealDebrid ────────────────────────────────────────────────────

    def test_rd_mkv_files_usable(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        info = {'files': [
            {'path': '/Movie/Movie.mkv', 'bytes': 5000000, 'selected': 1},
            {'path': '/Movie/Sample.mkv', 'bytes': 50000, 'selected': 1},
        ]}
        assert watcher._has_usable_media_files(info) is True

    def test_rd_m2ts_only_not_usable(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        info = {'files': [
            {'path': '/BDMV/STREAM/00001.m2ts', 'bytes': 30000000000, 'selected': 1},
            {'path': '/BDMV/STREAM/00002.m2ts', 'bytes': 500000000, 'selected': 1},
            {'path': '/BDMV/index.bdmv', 'bytes': 1000, 'selected': 1},
        ]}
        assert watcher._has_usable_media_files(info) is False

    def test_rd_mixed_m2ts_and_mkv_usable(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        info = {'files': [
            {'path': '/BDMV/STREAM/00001.m2ts', 'bytes': 30000000000, 'selected': 1},
            {'path': '/Movie.mkv', 'bytes': 5000000000, 'selected': 1},
        ]}
        assert watcher._has_usable_media_files(info) is True

    def test_rd_only_unselected_files(self):
        """Unselected files should be ignored; no selected files means assume usable."""
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        info = {'files': [
            {'path': '/Movie.mkv', 'bytes': 5000000000, 'selected': 0},
        ]}
        # No selected files → empty filenames → assume usable
        assert watcher._has_usable_media_files(info) is True

    def test_rd_no_files_key(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        info = {'status': 'downloaded', 'id': '123'}
        assert watcher._has_usable_media_files(info) is True

    def test_rd_empty_files_list(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        info = {'files': []}
        assert watcher._has_usable_media_files(info) is True

    # ── AllDebrid ─────────────────────────────────────────────────────

    def test_ad_mkv_in_nested_dirs(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        info = {'data': {'magnets': {'files': [
            {'n': 'Movie', 'e': [
                {'n': 'Movie.mkv', 's': 5000000000},
                {'n': 'Movie.srt', 's': 50000},
            ]},
        ]}}}
        assert watcher._has_usable_media_files(info) is True

    def test_ad_m2ts_only_not_usable(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        info = {'data': {'magnets': {'files': [
            {'n': 'BDMV', 'e': [
                {'n': 'STREAM', 'e': [
                    {'n': '00001.m2ts', 's': 30000000000},
                    {'n': '00002.m2ts', 's': 500000000},
                ]},
                {'n': 'index.bdmv', 's': 1000},
            ]},
        ]}}}
        assert watcher._has_usable_media_files(info) is False

    def test_ad_missing_structure(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        info = {'data': {'magnets': {}}}
        assert watcher._has_usable_media_files(info) is True

    def test_ad_flat_files(self):
        """AD response with no nesting (single-file torrent)."""
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        info = {'data': {'magnets': {'files': [
            {'n': 'Movie.mp4', 's': 5000000000},
        ]}}}
        assert watcher._has_usable_media_files(info) is True

    # ── TorBox ────────────────────────────────────────────────────────

    def test_tb_mp4_usable(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        info = {'data': {'files': [
            {'name': 'Movie.mp4', 'size': 5000000000},
        ]}}
        assert watcher._has_usable_media_files(info) is True

    def test_tb_m2ts_only_not_usable(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        info = {'data': {'files': [
            {'name': '00001.m2ts', 'size': 30000000000},
            {'name': '00002.m2ts', 'size': 500000000},
        ]}}
        assert watcher._has_usable_media_files(info) is False

    def test_tb_missing_files(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        info = {'data': {}}
        assert watcher._has_usable_media_files(info) is True

    # ── _extract_filenames_from_info ──────────────────────────────────

    def test_extract_rd_filenames(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        info = {'files': [
            {'path': '/Movie/Movie.mkv', 'bytes': 5000, 'selected': 1},
            {'path': '/Movie/Extras.mkv', 'bytes': 1000, 'selected': 0},
            {'path': '/Movie/Subs.srt', 'bytes': 100, 'selected': 1},
        ]}
        names = watcher._extract_filenames_from_info(info)
        assert names == ['Movie.mkv', 'Subs.srt']

    def test_extract_ad_filenames_nested(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'alldebrid')
        info = {'data': {'magnets': {'files': [
            {'n': 'BDMV', 'e': [
                {'n': 'STREAM', 'e': [
                    {'n': '00001.m2ts', 's': 30000},
                ]},
            ]},
            {'n': 'readme.txt', 's': 100},
        ]}}}
        names = watcher._extract_filenames_from_info(info)
        assert set(names) == {'00001.m2ts', 'readme.txt'}

    def test_extract_tb_filenames(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        info = {'data': {'files': [
            {'name': 'Movie.avi', 'size': 5000},
            {'name': 'info.nfo', 'size': 100},
        ]}}
        names = watcher._extract_filenames_from_info(info)
        assert names == ['Movie.avi', 'info.nfo']

    def test_extract_unknown_provider(self):
        watcher = BlackholeWatcher('/tmp', 'key', 'unknown_service')
        names = watcher._extract_filenames_from_info({'files': []})
        assert names == []

    def test_empty_info_dict(self):
        """Empty info dict should return empty list (provider can't extract)."""
        for provider in ('realdebrid', 'alldebrid', 'torbox'):
            watcher = BlackholeWatcher('/tmp', 'key', provider)
            names = watcher._extract_filenames_from_info({})
            assert names == [], f"Expected empty list for {provider} with empty info"


# ─── Per-arr label routing ──────────────────────────────────────────────

class TestIsValidLabel:

    def test_accepts_simple_name(self):
        assert _is_valid_label('sonarr') is True
        assert _is_valid_label('radarr') is True
        assert _is_valid_label('sonarr-4k') is True
        assert _is_valid_label('sonarr_hd') is True
        assert _is_valid_label('Readarr') is True
        assert _is_valid_label('arr1') is True

    def test_rejects_reserved(self):
        assert _is_valid_label('failed') is False
        assert _is_valid_label('.alt_pending') is False
        # Case-insensitive reserved match
        assert _is_valid_label('Failed') is False
        assert _is_valid_label('FAILED') is False

    def test_rejects_invalid_chars(self):
        assert _is_valid_label('sonarr; rm -rf /') is False
        assert _is_valid_label('sonarr/radarr') is False
        assert _is_valid_label('sonarr radarr') is False
        assert _is_valid_label('sonarr.arr') is False
        assert _is_valid_label('..') is False

    def test_rejects_path_traversal(self):
        assert _is_valid_label('../../etc') is False
        assert _is_valid_label('..') is False
        assert _is_valid_label('.') is False
        assert _is_valid_label('a/..') is False

    def test_rejects_empty_and_long(self):
        assert _is_valid_label('') is False
        assert _is_valid_label(None) is False
        assert _is_valid_label('a' * 64) is True
        assert _is_valid_label('a' * 65) is False


class TestScanLabelDiscovery:

    def _make_watcher(self, tmp_dir):
        watch = os.path.join(tmp_dir, 'watch')
        os.makedirs(watch)
        return BlackholeWatcher(watch, 'key', 'realdebrid'), watch

    def _old_file(self, path):
        """Backdate mtime so _scan doesn't skip file as in-flight."""
        t = time.time() - 10
        os.utime(path, (t, t))

    def test_scan_discovers_label_from_subdir(self, tmp_dir, monkeypatch):
        """Subdir name should be passed as label to _process_file."""
        watcher, watch = self._make_watcher(tmp_dir)
        sub = os.path.join(watch, 'sonarr')
        os.makedirs(sub)
        f = os.path.join(sub, 'Show.S01E01.torrent')
        with open(f, 'w') as h:
            h.write('x')
        self._old_file(f)

        calls = []
        monkeypatch.setattr(
            watcher, '_process_file',
            lambda fp, label=None: calls.append((fp, label)),
        )
        watcher._scan()
        assert len(calls) == 1
        assert calls[0][1] == 'sonarr'
        assert calls[0][0] == f

    def test_scan_root_file_has_no_label(self, tmp_dir, monkeypatch):
        """Files in watch_dir root should pass label=None (flat mode)."""
        watcher, watch = self._make_watcher(tmp_dir)
        f = os.path.join(watch, 'Movie.2024.torrent')
        with open(f, 'w') as h:
            h.write('x')
        self._old_file(f)

        calls = []
        monkeypatch.setattr(
            watcher, '_process_file',
            lambda fp, label=None: calls.append((fp, label)),
        )
        watcher._scan()
        assert calls == [(f, None)]

    def test_scan_mixed_flat_and_labeled(self, tmp_dir, monkeypatch):
        watcher, watch = self._make_watcher(tmp_dir)
        root_file = os.path.join(watch, 'Flat.torrent')
        with open(root_file, 'w') as h:
            h.write('x')
        self._old_file(root_file)

        sub = os.path.join(watch, 'radarr')
        os.makedirs(sub)
        sub_file = os.path.join(sub, 'Movie.magnet')
        with open(sub_file, 'w') as h:
            h.write('magnet:?xt=x')
        self._old_file(sub_file)

        seen = {}
        monkeypatch.setattr(
            watcher, '_process_file',
            lambda fp, label=None: seen.update({os.path.basename(fp): label}),
        )
        watcher._scan()
        assert seen == {'Flat.torrent': None, 'Movie.magnet': 'radarr'}

    def test_scan_rejects_invalid_label_characters(self, tmp_dir, monkeypatch):
        """Subdirs with invalid label names should be skipped entirely."""
        watcher, watch = self._make_watcher(tmp_dir)
        # '.' in name is not in the whitelist
        sub = os.path.join(watch, 'evil.path')
        os.makedirs(sub)
        f = os.path.join(sub, 'x.torrent')
        with open(f, 'w') as h:
            h.write('x')
        self._old_file(f)

        calls = []
        monkeypatch.setattr(
            watcher, '_process_file',
            lambda fp, label=None: calls.append((fp, label)),
        )
        watcher._scan()
        assert calls == []

    def test_scan_reserved_labels_skipped(self, tmp_dir, monkeypatch):
        """failed/ and .alt_pending/ must never be treated as labels."""
        watcher, watch = self._make_watcher(tmp_dir)
        for name in ('failed', '.alt_pending'):
            sub = os.path.join(watch, name)
            os.makedirs(sub)
            f = os.path.join(sub, 'x.torrent')
            with open(f, 'w') as h:
                h.write('x')
            self._old_file(f)

        calls = []
        monkeypatch.setattr(
            watcher, '_process_file',
            lambda fp, label=None: calls.append((fp, label)),
        )
        watcher._scan()
        assert calls == []

    def test_path_traversal_via_label_rejected(self, tmp_dir, monkeypatch):
        """A crafted '..' subdir must not be accepted as a label."""
        watcher, watch = self._make_watcher(tmp_dir)
        # Can't literally create '..' but invalid chars path is covered by _is_valid_label
        # Create a dir whose name would escape if not validated (uses chars outside whitelist)
        sub = os.path.join(watch, '../escape-attempt')
        try:
            os.makedirs(os.path.normpath(sub))
        except (OSError, FileExistsError):
            pass
        # Also create a dir with a bogus name in the watch tree
        weird = os.path.join(watch, 'sonarr..evil')
        os.makedirs(weird)
        with open(os.path.join(weird, 'x.torrent'), 'w') as h:
            h.write('x')
        self._old_file(os.path.join(weird, 'x.torrent'))

        calls = []
        monkeypatch.setattr(
            watcher, '_process_file',
            lambda fp, label=None: calls.append((fp, label)),
        )
        watcher._scan()
        assert calls == []


class TestCreateSymlinksWithLabel:

    def _make_watcher(self, tmp_dir):
        completed = os.path.join(tmp_dir, 'completed')
        mount = os.path.join(tmp_dir, 'mount')
        os.makedirs(completed)
        os.makedirs(mount)
        watcher = BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed,
            rclone_mount=mount,
            symlink_target_base='/mnt/debrid',
        )
        return watcher, completed, mount

    def test_create_symlinks_with_label_writes_to_label_subdir(self, tmp_dir):
        watcher, completed, mount = self._make_watcher(tmp_dir)
        release = 'My.Show.S01E01'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'ep.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir, label='sonarr')
        assert count == 1
        link = os.path.join(completed, 'sonarr', release, 'ep.mkv')
        assert os.path.islink(link)
        # Flat path must NOT have been created
        assert not os.path.exists(os.path.join(completed, release))

    def test_create_symlinks_without_label_writes_flat(self, tmp_dir):
        """Regression guard: label=None falls through to legacy flat output."""
        watcher, completed, mount = self._make_watcher(tmp_dir)
        release = 'My.Show.S01E01'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'ep.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir)
        assert count == 1
        assert os.path.islink(os.path.join(completed, release, 'ep.mkv'))

    def test_create_split_season_symlinks_with_label(self, tmp_dir):
        watcher, completed, mount = self._make_watcher(tmp_dir)
        release = 'Show.S01-S02.1080p'
        release_dir = os.path.join(mount, 'shows', release)
        os.makedirs(release_dir)
        for ep in ('Show.S01E01.mkv', 'Show.S02E01.mkv'):
            with open(os.path.join(release_dir, ep), 'w') as f:
                f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir, label='sonarr')
        assert count == 2
        assert os.path.isdir(os.path.join(completed, 'sonarr', 'Show.S01.1080p'))
        assert os.path.isdir(os.path.join(completed, 'sonarr', 'Show.S02.1080p'))
        # Make sure flat-mode dirs were NOT created
        assert not os.path.exists(os.path.join(completed, 'Show.S01.1080p'))


class TestPendingMonitorsWithLabel:

    def _make_watcher(self, tmp_dir):
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)
        return BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )

    def test_pending_monitors_persist_label(self, tmp_dir):
        w = self._make_watcher(tmp_dir)
        w._add_pending('torrent1', 'sonarr.torrent', label='sonarr')
        w._add_pending('torrent2', 'radarr.magnet', label='radarr')
        w._add_pending('torrent3', 'flat.torrent')

        entries = {e['torrent_id']: e for e in w._load_pending()}
        assert entries['torrent1']['label'] == 'sonarr'
        assert entries['torrent2']['label'] == 'radarr'
        # label=None should not be persisted, keeping JSON compact
        assert 'label' not in entries['torrent3']

    def test_pending_monitors_load_legacy_without_label(self, tmp_dir):
        """Existing in-flight entries from before the upgrade have no label field."""
        w = self._make_watcher(tmp_dir)
        legacy = [
            {'torrent_id': 't1', 'filename': 'old.torrent',
             'service': 'realdebrid', 'timestamp': time.time()}
        ]
        with open(w._pending_file, 'w') as f:
            json.dump(legacy, f)

        entries = w._load_pending()
        assert len(entries) == 1
        assert entries[0].get('label') is None

    def test_resume_pending_validates_label(self, tmp_dir, monkeypatch):
        """A tampered label value in the JSON must be dropped on resume,
        not piped into os.path.join (directory escape primitive)."""
        w = self._make_watcher(tmp_dir)
        tampered = [
            {'torrent_id': 't1', 'filename': 'x.torrent',
             'service': 'realdebrid', 'timestamp': time.time(),
             'label': '../../etc'},  # path traversal attempt
            {'torrent_id': 't2', 'filename': 'y.torrent',
             'service': 'realdebrid', 'timestamp': time.time(),
             'label': '/etc/cron.d'},  # absolute path attempt
            {'torrent_id': 't3', 'filename': 'z.torrent',
             'service': 'realdebrid', 'timestamp': time.time(),
             'label': ['not', 'a', 'string']},  # wrong type
            {'torrent_id': 't4', 'filename': 'ok.torrent',
             'service': 'realdebrid', 'timestamp': time.time(),
             'label': 'sonarr'},  # valid
        ]
        with open(w._pending_file, 'w') as f:
            json.dump(tampered, f)

        captured = []
        monkeypatch.setattr(
            w, '_start_monitor',
            lambda tid, fn, label=None: captured.append((tid, label)),
        )
        w._resume_pending_monitors()
        by_id = dict(captured)
        assert by_id['t1'] is None  # traversal → sanitized
        assert by_id['t2'] is None  # absolute path → sanitized
        assert by_id['t3'] is None  # wrong type → sanitized
        assert by_id['t4'] == 'sonarr'  # valid passes through

    def test_resume_pending_skips_bad_entries(self, tmp_dir, monkeypatch):
        """A non-dict entry must not crash the resume loop and kill the worker."""
        w = self._make_watcher(tmp_dir)
        bad = [
            'banana',  # not a dict
            42,        # not a dict
            {'torrent_id': 't_ok', 'filename': 'x.torrent',
             'service': 'realdebrid', 'timestamp': time.time()},
        ]
        with open(w._pending_file, 'w') as f:
            json.dump(bad, f)

        captured = []
        monkeypatch.setattr(
            w, '_start_monitor',
            lambda tid, fn, label=None: captured.append(tid),
        )
        w._resume_pending_monitors()  # must not raise
        assert captured == ['t_ok']


class TestFailedRetryPreservesLabel:

    def test_failed_retry_preserves_label(self, tmp_dir):
        """A labeled failed file moves back to /watch/<label>/ for retry."""
        watch = os.path.join(tmp_dir, 'watch')
        os.makedirs(watch)
        watcher = BlackholeWatcher(watch, 'key', 'realdebrid')
        label_dir = os.path.join(watch, 'failed', 'sonarr')
        os.makedirs(label_dir)
        failed_path = os.path.join(label_dir, 'x.torrent')
        with open(failed_path, 'w') as f:
            f.write('data')
        # Write retry meta with old timestamp so backoff has elapsed
        with open(failed_path + '.meta', 'w') as f:
            json.dump({'retries': 0, 'last_attempt': 0}, f)

        watcher._retry_failed()
        assert not os.path.exists(failed_path)
        assert os.path.exists(os.path.join(watch, 'sonarr', 'x.torrent'))

    def test_flat_retry_still_works(self, tmp_dir):
        """Legacy flat failed/ layout must still be retried to watch_dir root."""
        watch = os.path.join(tmp_dir, 'watch')
        os.makedirs(watch)
        watcher = BlackholeWatcher(watch, 'key', 'realdebrid')
        failed_dir = os.path.join(watch, 'failed')
        os.makedirs(failed_dir)
        failed_path = os.path.join(failed_dir, 'y.magnet')
        with open(failed_path, 'w') as f:
            f.write('magnet:?xt=x')
        with open(failed_path + '.meta', 'w') as f:
            json.dump({'retries': 0, 'last_attempt': 0}, f)

        watcher._retry_failed()
        assert not os.path.exists(failed_path)
        assert os.path.exists(os.path.join(watch, 'y.magnet'))

    def test_retry_does_not_clobber_fresh_drop(self, tmp_dir):
        """If the arr has just dropped a same-filename file in the label dir,
        the retry must leave the failed file in place rather than silently
        overwriting the fresh drop."""
        watch = os.path.join(tmp_dir, 'watch')
        os.makedirs(watch)
        watcher = BlackholeWatcher(watch, 'key', 'realdebrid')
        label_dir = os.path.join(watch, 'sonarr')
        os.makedirs(label_dir)

        # Fresh drop from the arr
        fresh = os.path.join(label_dir, 'x.torrent')
        with open(fresh, 'w') as f:
            f.write('FRESH_CONTENT')

        # Failed file from a prior attempt
        failed_dir = os.path.join(watch, 'failed', 'sonarr')
        os.makedirs(failed_dir)
        failed_path = os.path.join(failed_dir, 'x.torrent')
        with open(failed_path, 'w') as f:
            f.write('OLD_CONTENT')
        with open(failed_path + '.meta', 'w') as f:
            json.dump({'retries': 0, 'last_attempt': 0}, f)

        watcher._retry_failed()
        # Fresh drop is preserved, failed file stays in place
        with open(fresh) as f:
            assert f.read() == 'FRESH_CONTENT'
        assert os.path.exists(failed_path)


class TestAltPendingRecoveryPreservesLabel:

    def test_alt_pending_recovery_preserves_label(self, tmp_dir):
        watch = os.path.join(tmp_dir, 'watch')
        os.makedirs(watch)
        watcher = BlackholeWatcher(watch, 'key', 'realdebrid')
        staged_dir = os.path.join(watch, '.alt_pending', 'sonarr')
        os.makedirs(staged_dir)
        stranded = os.path.join(staged_dir, 'stranded.torrent')
        with open(stranded, 'w') as f:
            f.write('data')

        watcher._recover_alt_pending()
        assert not os.path.exists(stranded)
        recovered = os.path.join(watch, 'failed', 'sonarr', 'stranded.torrent')
        assert os.path.exists(recovered)
        # alt_exhausted marked so retry doesn't loop through alts again
        meta = recovered + '.meta'
        assert os.path.exists(meta)
        with open(meta) as f:
            data = json.load(f)
        assert data.get('alt_exhausted') is True

    def test_alt_pending_flat_recovery(self, tmp_dir):
        """Legacy flat .alt_pending/ layout must still be recovered."""
        watch = os.path.join(tmp_dir, 'watch')
        os.makedirs(watch)
        watcher = BlackholeWatcher(watch, 'key', 'realdebrid')
        staged = os.path.join(watch, '.alt_pending')
        os.makedirs(staged)
        stranded = os.path.join(staged, 'flat.torrent')
        with open(stranded, 'w') as f:
            f.write('data')

        watcher._recover_alt_pending()
        assert not os.path.exists(stranded)
        assert os.path.exists(os.path.join(watch, 'failed', 'flat.torrent'))


class TestIterReleaseDirs:

    def test_empty_dir(self, tmp_dir):
        assert list(iter_release_dirs(tmp_dir)) == []

    def test_missing_dir(self, tmp_dir):
        assert list(iter_release_dirs(os.path.join(tmp_dir, 'missing'))) == []

    def test_flat_layout(self, tmp_dir):
        # Release dir containing a file (typical flat release)
        r1 = os.path.join(tmp_dir, 'Show.S01E01')
        os.makedirs(r1)
        with open(os.path.join(r1, 'ep.mkv'), 'w') as f:
            f.write('x')

        got = list(iter_release_dirs(tmp_dir))
        assert len(got) == 1
        label, name, path = got[0]
        assert label is None
        assert name == 'Show.S01E01'
        assert path == r1

    def test_labeled_layout(self, tmp_dir):
        sonarr = os.path.join(tmp_dir, 'sonarr')
        os.makedirs(os.path.join(sonarr, 'Show.S01E01'))
        os.makedirs(os.path.join(sonarr, 'Show.S01E02'))

        radarr = os.path.join(tmp_dir, 'radarr')
        os.makedirs(os.path.join(radarr, 'Movie.2024'))

        got = {(label, name) for label, name, _ in iter_release_dirs(tmp_dir)}
        assert got == {
            ('sonarr', 'Show.S01E01'),
            ('sonarr', 'Show.S01E02'),
            ('radarr', 'Movie.2024'),
        }

    def test_mixed_layout(self, tmp_dir):
        """Flat release dirs and labeled parents coexisting."""
        # Labeled parent (only subdirs, no files)
        sonarr = os.path.join(tmp_dir, 'sonarr')
        os.makedirs(os.path.join(sonarr, 'Show.S01E01'))

        # Flat release dir (has files directly)
        flat = os.path.join(tmp_dir, 'Legacy.Release')
        os.makedirs(flat)
        with open(os.path.join(flat, 'file.mkv'), 'w') as f:
            f.write('x')

        got = {(label, name) for label, name, _ in iter_release_dirs(tmp_dir)}
        assert got == {
            ('sonarr', 'Show.S01E01'),
            (None, 'Legacy.Release'),
        }

    def test_ignores_pending_monitors_file(self, tmp_dir):
        """pending_monitors.json is a file at the top level — must be ignored."""
        pending = os.path.join(tmp_dir, 'pending_monitors.json')
        with open(pending, 'w') as f:
            f.write('[]')
        assert list(iter_release_dirs(tmp_dir)) == []

    def test_empty_label_dir_yields_nothing_and_is_not_flat_release(self, tmp_dir):
        """An empty dir with a label-compatible name must not be treated as a flat release.

        Misclassification would cause _cleanup_symlinks to shutil.rmtree the
        user's label subdir (via 'no valid files' → should_remove=True).
        """
        os.makedirs(os.path.join(tmp_dir, 'sonarr'))  # label-compatible, empty
        assert list(iter_release_dirs(tmp_dir)) == []

    def test_label_dir_with_stray_loose_file_still_classified_as_label(self, tmp_dir):
        """A stray file (e.g. .DS_Store, arr lockfile) inside a label dir must not
        demote the dir to a flat release — that would cause _cleanup_symlinks to
        wipe the entire label tree."""
        sonarr = os.path.join(tmp_dir, 'sonarr')
        os.makedirs(os.path.join(sonarr, 'Show.S01E01'))
        # Loose file alongside the release dir
        with open(os.path.join(sonarr, '.DS_Store'), 'w') as f:
            f.write('noise')

        got = {(label, name) for label, name, _ in iter_release_dirs(tmp_dir)}
        assert got == {('sonarr', 'Show.S01E01')}


class TestCleanupSymlinksLabeled:

    def test_removes_empty_label_dir_after_cleanup(self, tmp_dir):
        """After every release under a label is removed, the label dir itself goes."""
        completed = os.path.join(tmp_dir, 'completed')
        sonarr_dir = os.path.join(completed, 'sonarr')
        release_dir = os.path.join(sonarr_dir, 'Old.Release')
        os.makedirs(release_dir)
        # Broken symlink → release gets removed by _cleanup_symlinks
        os.symlink('/nonexistent/path.mkv', os.path.join(release_dir, 'ep.mkv'))

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        watcher._cleanup_symlinks()

        assert not os.path.exists(release_dir)
        # Empty label parent is also removed
        assert not os.path.exists(sonarr_dir)
        # Top-level completed_dir is preserved
        assert os.path.isdir(completed)

    def test_labeled_broken_symlink_removed(self, tmp_dir):
        completed = os.path.join(tmp_dir, 'completed')
        sonarr_dir = os.path.join(completed, 'sonarr')
        release_dir = os.path.join(sonarr_dir, 'Show.S01E01')
        os.makedirs(release_dir)
        os.symlink('/nonexistent/gone.mkv', os.path.join(release_dir, 'ep.mkv'))

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        watcher._cleanup_symlinks()
        assert not os.path.exists(release_dir)

    def test_empty_label_dir_not_removed_by_cleanup(self, tmp_dir):
        """Regression: cleanup must not misclassify an empty label dir as a
        flat release with no valid files, which would trigger shutil.rmtree."""
        completed = os.path.join(tmp_dir, 'completed')
        sonarr_dir = os.path.join(completed, 'sonarr')
        os.makedirs(sonarr_dir)  # Empty — no releases yet

        watcher = BlackholeWatcher(
            tmp_dir, 'key', 'realdebrid',
            symlink_enabled=True, completed_dir=completed,
        )
        watcher._cleanup_symlinks()
        # Empty label dir must survive cleanup (the user created it for a reason)
        assert os.path.isdir(sonarr_dir)
