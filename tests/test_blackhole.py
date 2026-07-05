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
    _is_rate_limit_response, _check_rate_limit, _mark_rate_limited,
    _rate_limit_until,
    _check_torbox_cooldown, _tb_cooldown_cache,
    _is_resolving_video, _dir_has_video, _local_episodes,
    _coalesced_root_refresh, _ROOT_REFRESH_COALESCE_S,
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
        a user upgrading Zurgarr mid-retry must not lose retry state
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


class TestRateLimitGate:
    """Per-provider rate-limit detection + backoff.

    Regression for observed search-storm behaviour: Sonarr's
    ``MissingEpisodeSearch`` fires dozens of magnet drops within the same
    second.  Each one was being submitted to RD/TB/AD blindly — even after
    the API returned ``rate limit exceeded`` — so the whole batch failed,
    landed in the 5-min retry queue, then hit the rate limit again on the
    next retry pass.  This class verifies the detector + per-provider
    backoff window prevent that loop.
    """

    def setup_method(self):
        """Clear the global rate-limit map so tests don't bleed into each other."""
        _rate_limit_until.clear()

    def teardown_method(self):
        _rate_limit_until.clear()

    @pytest.mark.parametrize('status,body,expected', [
        (429, '', True),
        (429, 'Too Many Requests', True),
        (200, 'rate limit exceeded', True),  # RD returns 200 + error text
        (503, 'rate_limit_exceeded', True),  # underscored variant
        (200, 'success', False),
        (500, 'internal server error', False),
        (404, 'not found', False),
    ])
    def test_is_rate_limit_response(self, status, body, expected):
        class _R:
            def __init__(self, s, b):
                self.status_code = s
                self.text = b
        assert _is_rate_limit_response(_R(status, body)) is expected

    def test_is_rate_limit_response_handles_missing_attrs(self):
        """A response object with no .text attribute must not crash."""
        class _R:
            status_code = 500
        # Should not raise; non-429 with no body falls through to False.
        assert _is_rate_limit_response(_R()) is False

    def test_mark_then_check_sleeps_until_window_expires(self, monkeypatch):
        """``_check_rate_limit`` blocks until the marked window expires."""
        sleeps = []
        monkeypatch.setattr('utils.blackhole.time.sleep', lambda s: sleeps.append(s))
        _mark_rate_limited('realdebrid', seconds=2)
        _check_rate_limit('realdebrid')
        assert len(sleeps) == 1
        # Slept for approximately the remaining window (allow 0.5s test jitter).
        assert 1.0 < sleeps[0] <= 2.0

    def test_check_rate_limit_no_op_when_no_window(self, monkeypatch):
        """No active window → no sleep."""
        sleeps = []
        monkeypatch.setattr('utils.blackhole.time.sleep', lambda s: sleeps.append(s))
        _check_rate_limit('realdebrid')
        assert sleeps == []

    def test_per_provider_isolation(self, monkeypatch):
        """A rate-limit on RD must NOT pause adds to TB (cache_aware still works)."""
        sleeps = []
        monkeypatch.setattr('utils.blackhole.time.sleep', lambda s: sleeps.append(s))
        _mark_rate_limited('realdebrid', seconds=5)
        _check_rate_limit('torbox')
        assert sleeps == []  # TB unaffected
        _check_rate_limit('realdebrid')
        assert len(sleeps) == 1  # RD waited

    def test_window_expires(self, monkeypatch):
        """An expired window doesn't cause a sleep."""
        sleeps = []
        monkeypatch.setattr('utils.blackhole.time.sleep', lambda s: sleeps.append(s))
        # Mark with negative duration → already expired.
        _mark_rate_limited('realdebrid', seconds=-1)
        _check_rate_limit('realdebrid')
        assert sleeps == []


class TestTorboxCooldownProbe:
    """``_check_torbox_cooldown`` converts TB's account-level
    ``cooldown_until`` field into a "seconds remaining" value so a
    failed createtorrent can be gated via the existing rate-limit
    window infrastructure.  TB returns HTTP 400 + generic
    ``DOWNLOAD_SERVER_ERROR`` while the cooldown is active (not the
    standard 429 path), so without this probe every subsequent add
    silently wastes an API call until the cooldown lifts.
    """

    def setup_method(self):
        _tb_cooldown_cache['checked_at'] = 0.0
        _tb_cooldown_cache['seconds_until'] = 0.0

    def teardown_method(self):
        _tb_cooldown_cache['checked_at'] = 0.0
        _tb_cooldown_cache['seconds_until'] = 0.0

    def _mock_response(self, status_code, json_body):
        from unittest.mock import MagicMock
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_body
        return resp

    def test_active_cooldown_returns_seconds_remaining(self, monkeypatch):
        """Cooldown_until 600s in the future → returns ~600.0 (within
        wall-clock jitter)."""
        from datetime import datetime, timedelta, timezone
        future = datetime.now(timezone.utc) + timedelta(seconds=600)
        iso = future.strftime('%Y-%m-%dT%H:%M:%SZ')
        resp = self._mock_response(200, {'data': {'cooldown_until': iso}})
        monkeypatch.setattr('utils.blackhole.requests.get', lambda *a, **kw: resp)
        seconds = _check_torbox_cooldown('tb-key')
        assert 595 <= seconds <= 605

    def test_no_cooldown_returns_zero(self, monkeypatch):
        """``cooldown_until`` absent / null → 0.0."""
        resp = self._mock_response(200, {'data': {'cooldown_until': None}})
        monkeypatch.setattr('utils.blackhole.requests.get', lambda *a, **kw: resp)
        assert _check_torbox_cooldown('tb-key') == 0.0

    def test_expired_cooldown_returns_zero(self, monkeypatch):
        """A cooldown_until in the past must clamp to 0 (not negative)
        so callers can use the raw value as a sleep duration."""
        from datetime import datetime, timedelta, timezone
        past = datetime.now(timezone.utc) - timedelta(seconds=60)
        iso = past.strftime('%Y-%m-%dT%H:%M:%SZ')
        resp = self._mock_response(200, {'data': {'cooldown_until': iso}})
        monkeypatch.setattr('utils.blackhole.requests.get', lambda *a, **kw: resp)
        assert _check_torbox_cooldown('tb-key') == 0.0

    def test_network_failure_degrades_to_zero(self, monkeypatch):
        """A network hiccup on /user/me must NOT wedge the add path —
        treat unknown cooldown state as 'no cooldown' (caller will
        decide based on the original error response)."""
        def _boom(*a, **kw):
            raise OSError('connection refused')
        monkeypatch.setattr('utils.blackhole.requests.get', _boom)
        assert _check_torbox_cooldown('tb-key') == 0.0

    def test_non_200_response_returns_zero(self, monkeypatch):
        """401/500/etc. → treat as unknown → 0.0."""
        resp = self._mock_response(401, {})
        monkeypatch.setattr('utils.blackhole.requests.get', lambda *a, **kw: resp)
        assert _check_torbox_cooldown('tb-key') == 0.0

    def test_missing_api_key_returns_zero_without_network(self, monkeypatch):
        """Empty api_key must not hit /user/me — silently return 0."""
        calls = []
        monkeypatch.setattr('utils.blackhole.requests.get',
                            lambda *a, **kw: calls.append(a) or None)
        assert _check_torbox_cooldown('') == 0.0
        assert _check_torbox_cooldown(None) == 0.0
        assert calls == []

    def test_cache_suppresses_repeat_calls(self, monkeypatch):
        """Within the TTL the helper must NOT re-hit /user/me — a retry
        storm of failed adds would otherwise hammer the cooldown probe."""
        from datetime import datetime, timedelta, timezone
        future = datetime.now(timezone.utc) + timedelta(seconds=300)
        iso = future.strftime('%Y-%m-%dT%H:%M:%SZ')
        resp = self._mock_response(200, {'data': {'cooldown_until': iso}})
        call_count = {'n': 0}
        def _get(*a, **kw):
            call_count['n'] += 1
            return resp
        monkeypatch.setattr('utils.blackhole.requests.get', _get)
        s1 = _check_torbox_cooldown('tb-key')
        s2 = _check_torbox_cooldown('tb-key')
        s3 = _check_torbox_cooldown('tb-key')
        assert call_count['n'] == 1  # cache hit on second + third
        assert s1 > 0 and s2 > 0 and s3 > 0
        # Cached value decays with elapsed time so the caller sees a
        # monotonically non-increasing snapshot, not a stale fixed number.
        assert s2 <= s1 + 0.5  # allow tiny clock jitter
        assert s3 <= s2 + 0.5

    def test_cache_expires_after_ttl(self, monkeypatch):
        """Past the TTL the helper re-fetches /user/me so a manually
        lifted cooldown is picked up promptly."""
        from datetime import datetime, timedelta, timezone
        future = datetime.now(timezone.utc) + timedelta(seconds=300)
        iso = future.strftime('%Y-%m-%dT%H:%M:%SZ')
        resp = self._mock_response(200, {'data': {'cooldown_until': iso}})
        call_count = {'n': 0}
        def _get(*a, **kw):
            call_count['n'] += 1
            return resp
        monkeypatch.setattr('utils.blackhole.requests.get', _get)
        now = time.time()
        _check_torbox_cooldown('tb-key', _now=now)
        # Jump past the cache TTL — should refresh.
        _check_torbox_cooldown('tb-key', _now=now + 999)
        assert call_count['n'] == 2


class TestCoalescedRootRefresh:
    """``_coalesced_root_refresh`` collapses the N-way root-relist burst a
    landing season pack would otherwise produce (one full-root PROPFIND per
    episode monitor) into a single re-list per ``_ROOT_REFRESH_COALESCE_S``
    window — without it the burst trips TorBox's WebDAV listing rate-limit.
    """

    def setup_method(self):
        import utils.blackhole as bh
        bh._root_refresh_ts = 0.0

    def teardown_method(self):
        import utils.blackhole as bh
        bh._root_refresh_ts = 0.0

    def _count_refreshes(self, monkeypatch):
        calls = {'n': 0, 'dirs': []}

        def _fake_refresh_dir(d='', recursive=False):
            calls['n'] += 1
            calls['dirs'].append(d)
        monkeypatch.setattr('utils.rclone_rc.refresh_dir', _fake_refresh_dir)
        return calls

    def test_first_call_fires_refresh(self, monkeypatch):
        calls = self._count_refreshes(monkeypatch)
        assert _coalesced_root_refresh(_now=1000.0) is True
        assert calls['n'] == 1
        assert calls['dirs'] == ['']

    def test_burst_within_window_collapses_to_one(self, monkeypatch):
        """A season pack's N monitors all reaching Phase 2 in the same
        second must trigger exactly one re-list, not N."""
        calls = self._count_refreshes(monkeypatch)
        results = [_coalesced_root_refresh(_now=2000.0 + i * 0.01)
                   for i in range(8)]
        assert results[0] is True
        assert all(r is False for r in results[1:])
        assert calls['n'] == 1

    def test_call_after_window_fires_again(self, monkeypatch):
        calls = self._count_refreshes(monkeypatch)
        assert _coalesced_root_refresh(_now=3000.0) is True
        # Just past the coalesce window — a genuinely later monitor refreshes.
        later = 3000.0 + _ROOT_REFRESH_COALESCE_S + 0.01
        assert _coalesced_root_refresh(_now=later) is True
        assert calls['n'] == 2

    def test_real_threads_collapse_to_one(self, monkeypatch):
        """The actual motivation: N real monitor threads hitting the unseamed
        path concurrently must fire exactly one refresh — this exercises the
        lock, not the ``_now`` simulation."""
        import threading
        calls = self._count_refreshes(monkeypatch)
        barrier = threading.Barrier(8)

        def _worker():
            barrier.wait()  # release all threads as simultaneously as possible
            _coalesced_root_refresh()
        threads = [threading.Thread(target=_worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert calls['n'] == 1

    def test_refresh_failure_is_swallowed_but_claims_window(self, monkeypatch):
        """A failed refresh must not raise, and must still claim the window
        so a retry storm can't bypass the coalesce guard."""
        import utils.blackhole as bh
        def _boom(d='', recursive=False):
            raise RuntimeError('rc down')
        monkeypatch.setattr('utils.rclone_rc.refresh_dir', _boom)
        assert _coalesced_root_refresh(_now=4000.0) is True
        # The window is claimed (timestamp advanced) even though refresh raised.
        assert bh._root_refresh_ts == 4000.0
        assert _coalesced_root_refresh(_now=4000.5) is False


class TestTorboxAddCooldownGate:
    """``_add_to_torbox`` must convert a TB cooldown-shape failure
    (HTTP 400 ``DOWNLOAD_SERVER_ERROR`` while /user/me reports an
    active cooldown) into a rate-limit window so subsequent adds
    block on ``_check_rate_limit('torbox')`` rather than wasting an
    API call each.  Plain non-cooldown failures must still surface as
    a generic error without setting a window.
    """

    def setup_method(self):
        _rate_limit_until.clear()
        _tb_cooldown_cache['checked_at'] = 0.0
        _tb_cooldown_cache['seconds_until'] = 0.0

    def teardown_method(self):
        _rate_limit_until.clear()
        _tb_cooldown_cache['checked_at'] = 0.0
        _tb_cooldown_cache['seconds_until'] = 0.0

    def _make_watcher(self, tmp_dir, monkeypatch):
        monkeypatch.setattr('utils.blackhole.os.path.exists', lambda *_a, **_k: True)
        return BlackholeWatcher(
            watch_dir=tmp_dir, debrid_api_key='tb-key',
            debrid_service='torbox', completed_dir=tmp_dir,
        )

    def _magnet_path(self, tmp_dir):
        path = os.path.join(tmp_dir, 'test.magnet')
        with open(path, 'w') as f:
            f.write('magnet:?xt=urn:btih:' + ('a' * 40))
        return path

    def test_cooldown_failure_marks_rate_limit_window(self, tmp_dir, monkeypatch):
        """A 400 ``DOWNLOAD_SERVER_ERROR`` while cooldown_until is set
        must set the TB rate-limit window so the *next* add blocks
        instead of hitting createtorrent again."""
        from unittest.mock import MagicMock
        from datetime import datetime, timedelta, timezone

        watcher = self._make_watcher(tmp_dir, monkeypatch)
        path = self._magnet_path(tmp_dir)

        # First call: createtorrent returns 400 cooldown error.
        post_resp = MagicMock()
        post_resp.status_code = 400
        post_resp.text = '{"success":false,"error":"DOWNLOAD_SERVER_ERROR"}'
        post_resp.json.return_value = {'success': False, 'error': 'DOWNLOAD_SERVER_ERROR'}
        # /user/me returns an active cooldown.
        future = datetime.now(timezone.utc) + timedelta(seconds=300)
        iso = future.strftime('%Y-%m-%dT%H:%M:%SZ')
        get_resp = MagicMock()
        get_resp.status_code = 200
        get_resp.json.return_value = {'data': {'cooldown_until': iso}}

        monkeypatch.setattr('utils.blackhole.tracked_request',
                            lambda *a, **kw: post_resp)
        monkeypatch.setattr('utils.blackhole.requests.get',
                            lambda *a, **kw: get_resp)

        ok, msg = watcher._add_to_torbox(path)
        assert ok is False
        assert 'cooldown' in msg.lower()
        # Window must be set so the next _check_rate_limit('torbox') blocks.
        assert _rate_limit_until.get('torbox', 0) > time.time() + 250

    def test_plain_failure_does_not_set_window(self, tmp_dir, monkeypatch):
        """A non-cooldown error (e.g. 400 with no active cooldown) must
        propagate without arming a rate-limit window — otherwise a
        single bad torrent would gate the whole TB pipeline for the
        full window duration."""
        from unittest.mock import MagicMock

        watcher = self._make_watcher(tmp_dir, monkeypatch)
        path = self._magnet_path(tmp_dir)

        post_resp = MagicMock()
        post_resp.status_code = 400
        post_resp.text = '{"success":false,"error":"INVALID_MAGNET"}'
        post_resp.json.return_value = {'success': False, 'error': 'INVALID_MAGNET'}
        get_resp = MagicMock()
        get_resp.status_code = 200
        # No cooldown_until → helper returns 0 → no window arming.
        get_resp.json.return_value = {'data': {'cooldown_until': None}}

        monkeypatch.setattr('utils.blackhole.tracked_request',
                            lambda *a, **kw: post_resp)
        monkeypatch.setattr('utils.blackhole.requests.get',
                            lambda *a, **kw: get_resp)

        ok, msg = watcher._add_to_torbox(path)
        assert ok is False
        assert 'cooldown' not in msg.lower()
        assert _rate_limit_until.get('torbox', 0) <= time.time()


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

    def test_torbox_flat_mount(self, tmp_dir):
        """TorBox's WebDAV mount has no category subdirs — search the bare root."""
        # TB mount layout: /<rclone_mount>/torbox/<release_name>
        tb_mount = os.path.join(tmp_dir, 'torbox')
        release_dir = os.path.join(tb_mount, 'Show.S01E01.1080p-FLUX')
        os.makedirs(release_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'torbox', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Show.S01E01.1080p-FLUX', debrid='torbox')
        assert path == release_dir
        assert category == ''  # Flat layout — no category subdivision
        assert matched == 'Show.S01E01.1080p-FLUX'

    def test_torbox_strips_indexer_prefix(self, tmp_dir):
        """TB API ``data.name`` sometimes has a leading ``[indexer.to] ``
        prefix the actual mount folder doesn't — strip it for the lookup.

        Regression: live grab of Landman S02E01 — TB API returned
        ``[bitsearch.to] www.UIndex.org    -    Landman ...`` but mount
        folder was ``www.UIndex.org    -    Landman ...``.
        """
        tb_mount = os.path.join(tmp_dir, 'torbox')
        # Mount folder has NO indexer prefix
        release_dir = os.path.join(
            tb_mount,
            'www.UIndex.org    -    Landman S02E01 Death and a Sunset 1080p AMZN WEB-DL DDP5 1 H 264-FLUX'
        )
        os.makedirs(release_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'torbox', rclone_mount=tmp_dir)
        # API returns name WITH the leading bracket prefix
        api_name = '[bitsearch.to] www.UIndex.org    -    Landman S02E01 Death and a Sunset 1080p AMZN WEB-DL DDP5 1 H 264-FLUX'
        path, category, matched = watcher._find_on_mount(api_name, debrid='torbox')
        assert path == release_dir
        assert category == ''
        # matched is the actual folder name on the mount (without prefix)
        assert matched == 'www.UIndex.org    -    Landman S02E01 Death and a Sunset 1080p AMZN WEB-DL DDP5 1 H 264-FLUX'

    def test_torbox_fuzzy_listdir_fallback(self, tmp_dir):
        """When exact + leading-bracket-strip both miss, walk the mount
        and fuzzy-norm-match against folder names. Handles the inverse
        case: mount has a leading bracket the API name dropped.
        """
        tb_mount = os.path.join(tmp_dir, 'torbox')
        # Mount has leading [indexer] that API name dropped
        release_dir = os.path.join(
            tb_mount,
            '[scraper.to] My Show S01E01 1080p WEB-DL-FLUX [TGx]'
        )
        os.makedirs(release_dir)

        watcher = BlackholeWatcher('/tmp', 'key', 'torbox', rclone_mount=tmp_dir)
        api_name = 'My Show S01E01 1080p WEB-DL-FLUX [TGx]'
        path, category, matched = watcher._find_on_mount(api_name, debrid='torbox')
        assert path == release_dir
        assert category == ''
        # matched is the folder name that exists on the mount
        assert matched == '[scraper.to] My Show S01E01 1080p WEB-DL-FLUX [TGx]'

    def test_torbox_not_found_when_no_match(self, tmp_dir):
        """No match anywhere on TB mount returns (None, None, None)."""
        tb_mount = os.path.join(tmp_dir, 'torbox')
        os.makedirs(tb_mount)
        os.makedirs(os.path.join(tb_mount, 'Other.Release.S01E01-NTb'))

        watcher = BlackholeWatcher('/tmp', 'key', 'torbox', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Something.Else.S05E10-XYZ', debrid='torbox')
        assert path is None
        assert category is None
        assert matched is None

    def test_torbox_fuzzy_match_refuses_ambiguous(self, tmp_dir):
        """If multiple TB mount folders fuzzy-match, refuse to guess."""
        tb_mount = os.path.join(tmp_dir, 'torbox')
        # Two folders that norm to the same fuzzy key after leading-bracket strip
        os.makedirs(os.path.join(tb_mount, '[a.to] Show S01E01 GROUP'))
        os.makedirs(os.path.join(tb_mount, '[b.to] Show S01E01 GROUP'))

        watcher = BlackholeWatcher('/tmp', 'key', 'torbox', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount('Show S01E01 GROUP', debrid='torbox')
        # Exact-path probes miss (both candidates have brackets); fuzzy
        # finds two matches → refuses.
        assert path is None
        assert category is None
        assert matched is None

    @pytest.mark.parametrize('hostile', [
        '/etc',
        '/etc/passwd',
        '../../etc',
        '..',
        '.',
        'foo/../bar',
        'a/b',
        '',
        'name\x00with\x00nul',
    ])
    def test_torbox_rejects_path_traversal_candidates(self, tmp_dir, hostile):
        """Adversarial release-name candidates must NEVER escape mount_path.

        ``os.path.join('/mnt/tb', '/etc')`` collapses to ``'/etc'`` because
        the absolute right-side argument resets the join — a TB ``data.name``
        field that an uploader chose as ``/etc`` would otherwise make
        ``os.path.isdir`` return True for the real ``/etc`` and walk it as
        the symlink-source tree. ``..`` segments are equally dangerous.
        Regression guard for the CRITICAL bug-hunter finding.
        """
        tb_mount = os.path.join(tmp_dir, 'torbox')
        os.makedirs(tb_mount)
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount(hostile, debrid='torbox')
        assert path is None
        assert category is None
        assert matched is None

    @pytest.mark.parametrize('hostile', [
        '/etc',
        '../../etc',
        '..',
        'a/b',
    ])
    def test_rd_mount_rejects_path_traversal_candidates(self, tmp_dir, hostile):
        """Same path-traversal guard applies to the Zurg (RD/AD) branch."""
        os.makedirs(os.path.join(tmp_dir, 'shows'))
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid', rclone_mount=tmp_dir)
        path, category, matched = watcher._find_on_mount(hostile)
        assert path is None
        assert category is None
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


class TestIsObfuscatedName:

    def test_hex_folder_with_tracker_tag_is_obfuscated(self):
        from utils.blackhole import is_obfuscated_name
        assert is_obfuscated_name('1f9da83faaf847949e043d0dae9684aa[eztv.re]')
        assert is_obfuscated_name('050bd19ee9934249a2ce4c9762c0d710[EZTVx.to]')

    def test_hex_media_file_is_obfuscated(self):
        from utils.blackhole import is_obfuscated_name
        assert is_obfuscated_name('1f9da83faaf847949e043d0dae9684aa[eztv.re].mkv')

    def test_hex_magnet_file_is_obfuscated(self):
        from utils.blackhole import is_obfuscated_name
        assert is_obfuscated_name('06bc5039b73b477f83c1e6750991d607[EZTVx.to].magnet')

    def test_bare_hex_is_obfuscated(self):
        from utils.blackhole import is_obfuscated_name
        assert is_obfuscated_name('050bd19ee9934249a2ce4c9762c0d710')

    def test_real_release_is_not_obfuscated(self):
        from utils.blackhole import is_obfuscated_name
        assert not is_obfuscated_name(
            'What We Do in the Shadows S05E03 1080p DSNP WEB-DL DDP5 1 H 264-NTb')
        assert not is_obfuscated_name('Movie.2024.1080p.BluRay.x264-GROUP')
        assert not is_obfuscated_name('My.Show.S01E01.mkv')

    def test_short_hex_is_not_obfuscated(self):
        # Below the 16-char floor — could be a legit short title fragment.
        from utils.blackhole import is_obfuscated_name
        assert not is_obfuscated_name('deadbeef')

    def test_empty_and_none_are_not_obfuscated(self):
        from utils.blackhole import is_obfuscated_name
        assert not is_obfuscated_name('')
        assert not is_obfuscated_name(None)


class TestCreateSymlinksObfuscated:

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

    def test_obfuscated_payload_uses_display_name(self, tmp_dir):
        """Hex mount folder + single hex media file: completed dir and link
        basename take the display name; target keeps the hex mount name."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        matched = '1f9da83faaf847949e043d0dae9684aa[eztv.re]'
        release_dir = os.path.join(mount, matched)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, matched + '.mkv'), 'w') as f:
            f.write('data')

        display = 'What We Do in the Shadows S05E03 1080p DSNP WEB-DL DDP5 1 H 264-NTb'
        count = watcher._create_symlinks(matched, '', release_dir, display_name=display)
        assert count == 1

        symlink = os.path.join(completed, display, display + '.mkv')
        assert os.path.islink(symlink)
        # Target still points at the real (hex) mount folder + file
        target = os.readlink(symlink)
        assert target == f'/mnt/debrid/{matched}/{matched}.mkv'

    def test_non_obfuscated_ignores_display_name(self, tmp_dir):
        """A normal release name must keep its own folder/file names even
        when a display_name is passed."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        release = 'My.Show.S01E01.1080p-GROUP'
        release_dir = os.path.join(mount, release)
        os.makedirs(release_dir)
        with open(os.path.join(release_dir, 'episode.mkv'), 'w') as f:
            f.write('data')

        count = watcher._create_symlinks(release, 'shows', release_dir,
                                         display_name='Something Else')
        assert count == 1
        assert os.path.islink(os.path.join(completed, release, 'episode.mkv'))

    def test_obfuscated_multi_file_keeps_original_basenames(self, tmp_dir):
        """When the payload has >1 media file we can't safely rename any single
        one to the release title — completed dir is renamed, files are not."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        matched = 'abcdef0123456789abcdef0123456789[eztv.re]'
        release_dir = os.path.join(mount, matched)
        os.makedirs(release_dir)
        for name in ['aaaa1111bbbb2222aaaa1111bbbb2222.mkv',
                     'cccc3333dddd4444cccc3333dddd4444.mkv']:
            with open(os.path.join(release_dir, name), 'w') as f:
                f.write('data')

        display = 'Some Show S01 1080p WEB-DL-GRP'
        count = watcher._create_symlinks(matched, '', release_dir, display_name=display)
        assert count == 2
        # Dir renamed to display; files keep their (hex) basenames
        assert os.path.isdir(os.path.join(completed, display))
        assert os.path.islink(os.path.join(
            completed, display, 'aaaa1111bbbb2222aaaa1111bbbb2222.mkv'))

    def test_obfuscated_multiseason_uses_display_name_for_season_dirs(self, tmp_dir):
        """Obfuscated mount folder + season-parseable files + multi-season
        display name: per-season completed dirs must be built from the
        display name (so Sonarr parses them), targets from the hex folder."""
        watcher, completed, mount = self._make_watcher(tmp_dir)

        matched = 'abcdef0123456789abcdef0123456789[eztv.re]'
        release_dir = os.path.join(mount, matched)
        os.makedirs(release_dir)
        # Real season/episode names inside a hex folder (partial obfuscation).
        for name in ['Show.S01E01.mkv', 'Show.S02E01.mkv']:
            with open(os.path.join(release_dir, name), 'w') as f:
                f.write('data')

        display = 'Show.S01-S02.1080p.WEB-DL-GRP'
        count = watcher._create_symlinks(matched, '', release_dir, display_name=display)
        assert count == 2
        # Season dirs derive from the display name, not the hex folder.
        s1 = os.path.join(completed, 'Show.S01.1080p.WEB-DL-GRP', 'Show.S01E01.mkv')
        s2 = os.path.join(completed, 'Show.S02.1080p.WEB-DL-GRP', 'Show.S02E01.mkv')
        assert os.path.islink(s1)
        assert os.path.islink(s2)
        # Target still points at the real (hex) mount folder.
        assert os.readlink(s1) == f'/mnt/debrid/{matched}/Show.S01E01.mkv'


class TestAuditReleaseCompleteness:

    def _make_watcher(self, tmp_dir):
        completed = os.path.join(tmp_dir, 'completed')
        mount = os.path.join(tmp_dir, 'mount')
        os.makedirs(completed)
        os.makedirs(mount)
        return BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed,
            rclone_mount=mount,
            symlink_target_base='/mnt/debrid',
        )

    def test_obfuscated_payload_skips_audit(self, tmp_dir, monkeypatch):
        """An obfuscated payload's hex file names carry no episode info, so a
        parse-derived 'missing' must NOT trigger blocklist/history/re-search."""
        import utils.blackhole as bh
        watcher = self._make_watcher(tmp_dir)

        matched = '1f9da83faaf847949e043d0dae9684aa[eztv.re]'
        mount_path = os.path.join(tmp_dir, 'mount', matched)
        os.makedirs(mount_path)
        with open(os.path.join(mount_path, matched + '.mkv'), 'w') as f:
            f.write('data')

        events = []
        fake_history = type('H', (), {'log_event': lambda self, *a, **k: events.append((a, k))})()
        blocked = []
        fake_blocklist = type('B', (), {'add': lambda self, *a, **k: blocked.append((a, k))})()
        monkeypatch.setattr(bh, '_history', fake_history, raising=False)
        monkeypatch.setattr(bh, '_blocklist', fake_blocklist, raising=False)

        # filename is the REAL release name (claims S05E03)
        filename = 'What.We.Do.in.the.Shadows.S05E03.1080p.mkv'
        watcher._audit_release_completeness(filename, matched, mount_path, {})

        assert events == []
        assert blocked == []


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
        """TB returns ``cached`` for instant cache hits (the dominant
        case under plan 39 cache_aware routing — every TB-routed grab
        is cache-positive at probe time), ``completed`` for torrents
        that went through a full BT cycle, and ``uploading`` for the
        post-download seeding phase.  All three indicate the file is
        on TB storage and reachable via WebDAV, so the blackhole
        should stop polling and proceed to symlink creation.

        Pre-fix this checked only ``status == 'completed'`` — every
        cached TB grab timed out at ``mount_poll_timeout`` (default
        300s) and got auto-blocklisted as 'Uncached on debrid (timed
        out)', even though the file was ready immediately."""
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        # Ready states — all three must be accepted.
        assert watcher._is_torrent_ready('completed') is True
        assert watcher._is_torrent_ready('cached') is True
        assert watcher._is_torrent_ready('uploading') is True
        # Not-ready states — file isn't on TB storage yet.
        assert watcher._is_torrent_ready('downloading') is False
        assert watcher._is_torrent_ready('queued') is False
        assert watcher._is_torrent_ready('metadl') is False
        assert watcher._is_torrent_ready('paused') is False
        # Defensive: unknown/garbage strings don't accidentally pass.
        assert watcher._is_torrent_ready('') is False
        assert watcher._is_torrent_ready('something_new') is False

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


class TestDirHasVideo:
    """Unit tests for the resolving-video helpers underpinning local dedup."""

    def test_is_resolving_video_real_file(self, tmp_dir):
        p = os.path.join(tmp_dir, 'ep.mkv')
        with open(p, 'w') as f:
            f.write('data')
        assert _is_resolving_video(p) is True

    def test_is_resolving_video_subtitle_rejected(self, tmp_dir):
        p = os.path.join(tmp_dir, 'ep.srt')
        with open(p, 'w') as f:
            f.write('sub')
        assert _is_resolving_video(p) is False

    def test_is_resolving_video_broken_symlink_rejected(self, tmp_dir):
        link = os.path.join(tmp_dir, 'ep.mkv')
        os.symlink(os.path.join(tmp_dir, 'gone.mkv'), link)
        assert _is_resolving_video(link) is False

    def test_dir_has_video_flat_true(self, tmp_dir):
        with open(os.path.join(tmp_dir, 'movie.mp4'), 'w') as f:
            f.write('data')
        assert _dir_has_video(tmp_dir) is True

    def test_dir_has_video_subtitle_only_false(self, tmp_dir):
        with open(os.path.join(tmp_dir, 'movie.srt'), 'w') as f:
            f.write('sub')
        assert _dir_has_video(tmp_dir) is False

    def test_dir_has_video_nonrecursive_ignores_subdir(self, tmp_dir):
        sub = os.path.join(tmp_dir, 'Season 01')
        os.makedirs(sub)
        with open(os.path.join(sub, 'ep.mkv'), 'w') as f:
            f.write('data')
        assert _dir_has_video(tmp_dir, recursive=False) is False

    def test_dir_has_video_recursive_finds_subdir(self, tmp_dir):
        sub = os.path.join(tmp_dir, 'Season 01')
        os.makedirs(sub)
        with open(os.path.join(sub, 'ep.mkv'), 'w') as f:
            f.write('data')
        assert _dir_has_video(tmp_dir, recursive=True) is True

    def test_dir_has_video_recursive_subtitle_only_false(self, tmp_dir):
        sub = os.path.join(tmp_dir, 'Season 01')
        os.makedirs(sub)
        with open(os.path.join(sub, 'ep.srt'), 'w') as f:
            f.write('sub')
        assert _dir_has_video(tmp_dir, recursive=True) is False

    def test_dir_has_video_recursive_depth_bounded(self, tmp_dir):
        """Recursion is one level only — video two levels deep is NOT found."""
        deep = os.path.join(tmp_dir, 'Season 01', 'extras')
        os.makedirs(deep)
        with open(os.path.join(deep, 'ep.mkv'), 'w') as f:
            f.write('data')
        assert _dir_has_video(tmp_dir, recursive=True) is False

    def test_dir_has_video_missing_path_false(self, tmp_dir):
        assert _dir_has_video(os.path.join(tmp_dir, 'nope')) is False

    def test_local_episodes_skips_subtitles(self, tmp_dir):
        with open(os.path.join(tmp_dir, 'Show.S01E01.mkv'), 'w') as f:
            f.write('data')
        with open(os.path.join(tmp_dir, 'Show.S01E02.srt'), 'w') as f:
            f.write('sub')
        assert _local_episodes(tmp_dir) == {1}


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

    def test_skips_punctuation_movie(self, tmp_dir):
        """Apostrophe in the on-disk folder must not defeat dedup.

        parse_release_name strips punctuation from the release side
        (dots→spaces), but arr folders keep it — a strict compare misses
        "Whats Eating Gilbert Grape" vs "What's Eating Gilbert Grape (1993)"
        and lets a duplicate import through.
        """
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        movie_dir = os.path.join(movies_dir, "What's Eating Gilbert Grape (1993)")
        os.makedirs(movie_dir)
        with open(os.path.join(movie_dir, 'movie.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library(
            'Whats.Eating.Gilbert.Grape.1993.1080p.BluRay.torrent') is True

    def test_skips_punctuation_tv_episode(self, tmp_dir):
        """Same punctuation tolerance on the TV side."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, "Schitt's Creek (2015)", 'Season 01')
        os.makedirs(season_dir)
        with open(os.path.join(season_dir, "Schitt's Creek - S01E01.mkv"), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Schitts.Creek.S01E01.1080p.WEB.torrent') is True

    def test_dedup_empty_fuzzy_forms_never_match(self, tmp_dir):
        """Two distinct non-ASCII titles that both collapse to '' under
        transliteration must not fuzzy-match each other; identical names
        still match via the strict path."""
        watcher, _, _ = self._make_watcher(tmp_dir)
        assert watcher._dedup_names_match('妖猫伝 (2017)', '悪人', '') is False
        assert watcher._dedup_names_match('悪人 (2010)', '悪人', '') is True

    def test_fuzzy_does_not_conflate_distinct_titles(self, tmp_dir):
        """Fuzzy fallback must not match a genuinely different movie."""
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        movie_dir = os.path.join(movies_dir, 'Gattaca (1997)')
        os.makedirs(movie_dir)
        with open(os.path.join(movie_dir, 'movie.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Attack.1997.1080p.BluRay.torrent') is False

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

    def test_subtitle_only_season_pack_not_matched(self, tmp_dir):
        """A season folder holding only subtitles must NOT block a season pack.

        Regression: an orphan ``.srt`` season made every Sonarr grab skip with
        "Season N exists locally", leaving the show permanently missing.
        """
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Adolescence', 'Season 01')
        os.makedirs(season_dir)
        for ep in range(1, 5):
            with open(os.path.join(season_dir, f'Adolescence.S01E{ep:02d}.srt'), 'w') as f:
                f.write('1\n00:00:00,000 --> 00:00:01,000\nhi\n')

        # Season pack (no specific episodes) — subtitles alone must not count.
        assert watcher._check_local_library('Adolescence.S01.1080p.NF.WEB-DL.magnet') is False

    def test_subtitle_only_does_not_satisfy_episode(self, tmp_dir):
        """A stray ``.srt`` for an episode must not mark that episode present."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 05')
        os.makedirs(season_dir)
        with open(os.path.join(season_dir, 'Fargo.S05E01.srt'), 'w') as f:
            f.write('sub')

        assert watcher._check_local_library('Fargo.S05E01.1080p.WEB.torrent') is False

    def test_subtitle_only_movie_not_matched(self, tmp_dir):
        """A movie folder with only a subtitle must not block the grab."""
        watcher, _, movies_dir = self._make_watcher(tmp_dir)
        movie_dir = os.path.join(movies_dir, 'Gattaca (1997)')
        os.makedirs(movie_dir)
        with open(os.path.join(movie_dir, 'Gattaca.srt'), 'w') as f:
            f.write('sub')

        assert watcher._check_local_library('Gattaca.1997.1080p.BluRay.torrent') is False

    def test_broken_symlink_video_not_matched(self, tmp_dir):
        """A dangling video symlink (dead debrid target) must not count as local."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 05')
        os.makedirs(season_dir)
        link = os.path.join(season_dir, 'Fargo (2014) - S05E01.mkv')
        os.symlink(os.path.join(tmp_dir, 'does-not-exist.mkv'), link)

        assert watcher._check_local_library('Fargo.S05E01.1080p.WEB.torrent') is False

    def test_mixed_video_and_subtitle_still_matches(self, tmp_dir):
        """A real video alongside subtitles must still register as present."""
        watcher, tv_dir, _ = self._make_watcher(tmp_dir)
        season_dir = os.path.join(tv_dir, 'Fargo (2014)', 'Season 05')
        os.makedirs(season_dir)
        with open(os.path.join(season_dir, 'Fargo.S05E01.srt'), 'w') as f:
            f.write('sub')
        with open(os.path.join(season_dir, 'Fargo (2014) - S05E01.mkv'), 'w') as f:
            f.write('data')

        assert watcher._check_local_library('Fargo.S05E01.1080p.WEB.torrent') is True


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


class TestEnrichForHistoryRobustParsing:
    """Tests for robust title parsing in _enrich_for_history.

    Covers cases where the naive parse_release_name fails: parens around
    year, brackets around year, dash separators, and inline-junk
    (actor/genre tags) before the year.  These all fall back to the
    library parser path since no TMDB cache is present in the test env.
    """

    def test_parens_year_movie(self):
        """Filename with `(YYYY)` — naive year regex misses; library parser
        catches it via MID_YEAR_PATTERN."""
        name, ep = _enrich_for_history('Gattaca.(1997).1080p.BluRay.x264.torrent')
        assert name == 'Gattaca'
        assert ep is None

    def test_parens_year_with_actor_and_genre(self):
        """The reported Gattaca regression: actor name + genre tag before
        the parenthesized year. Library parser strips year/quality and
        leaves "Gattaca Ethan Hawke Sci Fi" — still cleaner than the
        naive parser's "Gattaca Ethan Hawke Sci Fi (1997)" output (year
        and quality artifacts gone).  The TMDB-cache lookup recovers the
        canonical title — exercised in TestCanonicalTitleResolution."""
        name, ep = _enrich_for_history(
            'Gattaca.Ethan.Hawke.Sci.Fi.(1997).1080p.BluRay.x264-GROUP.torrent'
        )
        # No cache file in test env → falls back to library parser output.
        # The year MUST be stripped (this is the core regression).
        assert name == 'Gattaca Ethan Hawke Sci Fi'
        assert '(1997)' not in name
        assert '1080p' not in name
        assert ep is None

    def test_bracket_year_movie(self):
        """Bracketed year `[YYYY]` — naive year regex misses; library
        parser's BRACKET_YEAR_PATTERN catches it."""
        name, ep = _enrich_for_history('Gattaca.[1997].1080p.BluRay.torrent')
        assert name == 'Gattaca'
        assert ep is None

    def test_dash_separated_year_movie(self):
        """Dashes around the year — naive regex requires `.`/space."""
        name, ep = _enrich_for_history('Gattaca-1997-1080p-BluRay-GROUP.torrent')
        assert name == 'Gattaca'
        assert ep is None

    def test_edition_tag_stripped(self):
        """Edition/cut tags between title and quality — library parser
        strips via _EDITION_PATTERN."""
        name, ep = _enrich_for_history('Blade.Runner.1982.Final.Cut.1080p.BluRay.torrent')
        # year stripped, quality stripped; "Final Cut" is also an edition tag
        assert name == 'Blade Runner'
        assert ep is None

    def test_site_prefix_stripped(self):
        """Indexer URL prefix at start of filename — library parser
        strips via _SITE_PREFIX_PATTERN."""
        name, ep = _enrich_for_history('www.UIndex.org.Gattaca.1997.1080p.BluRay.torrent')
        assert name == 'Gattaca'
        assert ep is None

    def test_existing_dotted_year_still_works(self):
        """Regression guard: filenames the naive parser already handled
        (dot-separated year) must still resolve."""
        name, ep = _enrich_for_history('Gattaca.1997.1080p.BluRay.torrent')
        assert name == 'Gattaca'
        assert ep is None

    def test_tv_unaffected_by_robust_path(self):
        """Regression guard: TV episode parsing path still produces the
        same media_title and ep_str."""
        name, ep = _enrich_for_history('Bad.Monkey.S01E01.1080p.ATVP.WEB-DL.torrent')
        assert name == 'Bad Monkey'
        assert ep == 'S01E01'

    def test_tv_with_year_prefix(self):
        """TV release with year prefix, no parens — library parser
        strips trailing year after season cut."""
        name, ep = _enrich_for_history('Fargo.2014.S03E01.720p.torrent')
        assert name == 'Fargo'
        assert ep == 'S03E01'

    def test_empty_filename_after_strip(self):
        """Pathological input: only the extension. Must still return
        None, not crash."""
        name, ep = _enrich_for_history('.torrent')
        assert name is None
        assert ep is None


class TestCanonicalTitleResolution:
    """Tests for _resolve_canonical_title and _lookup_canonical_in_tmdb.

    Each test stubs the TMDB cache loader to return a controlled fixture,
    so the lookup logic is exercised deterministically without touching
    the real /config/tmdb_cache.json file.
    """

    def _patch_cache(self, monkeypatch, cache):
        """Patch tmdb._load_cache so the resolver sees the fixture."""
        from utils import tmdb as _tmdb
        monkeypatch.setattr(_tmdb, '_load_cache', lambda: cache)

    def test_direct_year_qualified_hit(self, monkeypatch):
        """Parser produces clean title; cache has a year-qualified entry
        — lookup returns its canonical 'title' field."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'gattaca (1997)': {
                    'title': 'Gattaca',
                    'release_date': '1997-10-24',
                },
            },
        })
        result = _resolve_canonical_title(
            'Gattaca.1997.1080p.BluRay.torrent', 'Gattaca', is_tv=False,
        )
        assert result == 'Gattaca'

    def test_prefix_match_recovers_from_actor_genre_junk(self, monkeypatch):
        """The Gattaca regression case — parser leaves "Gattaca Ethan
        Hawke Sci Fi" and the prefix matcher resolves it to canonical
        "Gattaca" via the cache."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'gattaca (1997)': {
                    'title': 'Gattaca',
                    'release_date': '1997-10-24',
                },
            },
        })
        result = _resolve_canonical_title(
            'Gattaca.Ethan.Hawke.Sci.Fi.(1997).1080p.BluRay.x264-GROUP.torrent',
            'Gattaca Ethan Hawke Sci Fi (1997)', is_tv=False,
        )
        assert result == 'Gattaca'

    def test_year_mismatch_excludes_candidate(self, monkeypatch):
        """When parsed year disagrees with cache entry's year, prefix
        match must NOT fire — no false positive."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'gattaca (2020)': {
                    'title': 'Gattaca',
                    'release_date': '2020-01-01',
                },
            },
        })
        result = _resolve_canonical_title(
            'Gattaca.Ethan.Hawke.Sci.Fi.(1997).1080p.torrent',
            'fallback', is_tv=False,
        )
        # Year mismatch → cache miss → falls back to library parser output.
        assert result == 'Gattaca Ethan Hawke Sci Fi'

    def test_longest_prefix_wins(self, monkeypatch):
        """When multiple cache entries are valid prefixes, the longest
        (most specific) wins. Prevents 'The Dark' from beating 'The
        Dark Knight' for a Dark Knight release."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'the dark (2005)': {
                    'title': 'The Dark',
                    'release_date': '2005-01-01',
                },
                'the dark knight (2008)': {
                    'title': 'The Dark Knight',
                    'release_date': '2008-07-18',
                },
            },
        })
        result = _resolve_canonical_title(
            'The.Dark.Knight.2008.BluRay.1080p.torrent',
            'The Dark Knight', is_tv=False,
        )
        assert result == 'The Dark Knight'

    def test_non_prefix_does_not_match(self, monkeypatch):
        """A cache entry whose tokens appear MID-string (not at start)
        must not match. Real release names put the title first."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'sci fi (2020)': {
                    'title': 'Sci Fi',
                    'release_date': '2020-01-01',
                },
            },
        })
        result = _resolve_canonical_title(
            'Gattaca.Ethan.Hawke.Sci.Fi.(1997).1080p.torrent',
            'fallback', is_tv=False,
        )
        # "sci fi" appears mid-string in parsed tokens, not at start.
        # No prefix match → fall back to library parser output.
        assert result == 'Gattaca Ethan Hawke Sci Fi'

    def test_show_section_used_for_tv(self, monkeypatch):
        """is_tv=True must look in the 'shows' section, not 'movies'."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'breaking bad (2008)': {
                    'title': 'Breaking Bad MOVIE',
                    'release_date': '2008-01-01',
                },
            },
            'shows': {
                'breaking bad': {
                    'title': 'Breaking Bad',
                    'first_air_date': '2008-01-20',
                },
            },
        })
        result = _resolve_canonical_title(
            'Breaking.Bad.S01E05.1080p.torrent',
            'Breaking Bad', is_tv=True,
        )
        assert result == 'Breaking Bad'  # from shows, not the movie spoof

    def test_empty_cache_falls_back_to_library_parser(self, monkeypatch):
        """No cache file — resolver returns library parser output."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {})
        result = _resolve_canonical_title(
            'Gattaca.(1997).1080p.BluRay.torrent', 'fallback', is_tv=False,
        )
        assert result == 'Gattaca'

    def test_load_cache_exception_falls_back(self, monkeypatch):
        """Cache load raises — resolver still produces library parser
        output (never propagates the error)."""
        from utils.blackhole import _resolve_canonical_title
        from utils import tmdb as _tmdb

        def _raise():
            raise OSError("disk error")
        monkeypatch.setattr(_tmdb, '_load_cache', _raise)
        result = _resolve_canonical_title(
            'Gattaca.1997.1080p.torrent', 'Gattaca', is_tv=False,
        )
        assert result == 'Gattaca'

    def test_robust_parser_failure_falls_back_to_naive(self, monkeypatch):
        """If parse_folder_name explodes for any reason, resolver returns
        the original fallback_name (never None, never empty)."""
        from utils.blackhole import _resolve_canonical_title
        from utils import library

        def _raise(name):
            raise RuntimeError("boom")
        monkeypatch.setattr(library, 'parse_folder_name', _raise)
        result = _resolve_canonical_title(
            'Gattaca.1997.torrent', 'naive fallback', is_tv=False,
        )
        assert result == 'naive fallback'

    def test_empty_filename_returns_fallback(self):
        """Defensive: empty input returns the fallback unchanged."""
        from utils.blackhole import _resolve_canonical_title
        assert _resolve_canonical_title('', 'x', is_tv=False) == 'x'
        assert _resolve_canonical_title(None, 'x', is_tv=False) == 'x'

    def test_only_extension_returns_fallback(self):
        """Filename of just `.torrent` strips to empty — fallback
        passthrough, no resolver work."""
        from utils.blackhole import _resolve_canonical_title
        assert _resolve_canonical_title('.torrent', 'x', is_tv=False) == 'x'

    def test_year_missing_does_not_filter(self, monkeypatch):
        """When parsed_year is None, year filtering is skipped entirely."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'somemovie': {
                    'title': 'SomeMovie',
                    'release_date': '2000-01-01',
                },
            },
        })
        # Library parser won't find a year in this filename.
        result = _resolve_canonical_title(
            'SomeMovie.1080p.WEB.torrent', 'SomeMovie', is_tv=False,
        )
        # Direct match: norm "somemovie" hits the bare key.
        assert result == 'SomeMovie'

    def test_multi_token_entry_year_missing_fails_open(self, monkeypatch):
        """Multi-token cache entry without release_date: parsed-year
        filter falls open (legacy entries lack the field; the candidate's
        token-count specificity already protects against false positives).

        Single-token candidates use stricter fail-closed semantics — see
        test_single_token_prefix_no_entry_year_rejected.
        """
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'the dark knight (2008)': {
                    'title': 'The Dark Knight',
                    # no release_date — legacy entry
                },
            },
        })
        # Multi-token prefix ["the","dark","knight"] matches parser
        # output exactly.  Parsed year 2008, entry year missing →
        # fail-open accepts.
        result = _resolve_canonical_title(
            'The.Dark.Knight.Extras.2008.1080p.torrent',
            'fallback', is_tv=False,
        )
        assert result == 'The Dark Knight'

    def test_non_dict_cache_entry_skipped(self, monkeypatch):
        """Defensive: malformed cache shouldn't crash. Non-dict entries
        must be skipped silently."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'gattaca': 'not a dict',  # corrupt entry
                'gattaca (1997)': {
                    'title': 'Gattaca',
                    'release_date': '1997-10-24',
                },
            },
        })
        result = _resolve_canonical_title(
            'Gattaca.Ethan.Hawke.(1997).torrent', 'fallback', is_tv=False,
        )
        assert result == 'Gattaca'

    def test_non_string_title_in_entry_does_not_crash(self, monkeypatch):
        """Defensive: malformed cache entry where 'title' is a dict/list/int
        must not raise AttributeError out of the resolver.  Pre-fix this
        would propagate up to _enrich_for_history's caller and break the
        whole grab-event logging path on a single bad entry."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'gattaca (1997)': {
                    'title': {'unexpected': 'dict'},  # corrupt — not a string
                    'release_date': '1997-10-24',
                },
                'gattaca (2020)': {
                    'title': ['also', 'wrong'],  # corrupt — not a string
                    'release_date': '2020-01-01',
                },
            },
        })
        # Direct hit returns '' (skipped), prefix match also returns '' →
        # falls back to library parser output.  Critically: no exception.
        result = _resolve_canonical_title(
            'Gattaca.1997.1080p.BluRay.torrent', 'fallback', is_tv=False,
        )
        assert result == 'Gattaca'

    def test_non_string_title_during_prefix_scan_skipped(self, monkeypatch):
        """Even when the bad entry would otherwise prefix-match, it must
        be skipped without crashing, and a sibling valid entry must
        still win."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'gattaca (1997)': {
                    'title': 12345,  # int — not a string
                    'release_date': '1997-10-24',
                },
                'gattaca ethan hawke sci fi (1997)': {
                    'title': 'Gattaca',  # the valid one
                    'release_date': '1997-10-24',
                },
            },
        })
        result = _resolve_canonical_title(
            'Gattaca.Ethan.Hawke.Sci.Fi.(1997).1080p.torrent',
            'fallback', is_tv=False,
        )
        assert result == 'Gattaca'

    def test_single_token_prefix_requires_year(self, monkeypatch):
        """Single-word cache entry like 'The' (the 2017 film) must NOT
        prefix-match a multi-word parse without year confirmation —
        otherwise every release starting with 'The' resolves to the
        wrong canonical."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'the': {
                    'title': 'The',
                    'release_date': '2017-01-01',
                },
            },
        })
        result = _resolve_canonical_title(
            'The.Dark.Knight.2008.BluRay.1080p.torrent',
            'fallback', is_tv=False,
        )
        # Year mismatch (2008 vs 2017): single-token guard rejects.
        # Falls back to library parser output.
        assert result == 'The Dark Knight'

    def test_single_token_prefix_no_year_in_filename_rejected(self, monkeypatch):
        """If the filename has no year, single-token cache entries must
        not prefix-match (no way to disambiguate)."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'the': {
                    'title': 'The',
                    'release_date': '2017-01-01',
                },
            },
        })
        # No year-bearing filename → parser yields year=None
        result = _resolve_canonical_title(
            'The.Dark.Knight.BluRay.1080p.torrent',
            'fallback', is_tv=False,
        )
        # Single-token guard fires (year is None) → no match → fallback
        assert result == 'The Dark Knight'

    def test_single_token_prefix_year_match_accepted(self, monkeypatch):
        """Single-token candidate with matching year IS accepted —
        the year provides the disambiguation."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'gattaca': {
                    'title': 'Gattaca',
                    'release_date': '1997-10-24',
                },
            },
        })
        # Single-token candidate "gattaca" matches multi-word parsed
        # output, year 1997 == entry year 1997 → accepted.
        result = _resolve_canonical_title(
            'Gattaca.Ethan.Hawke.(1997).1080p.torrent',
            'fallback', is_tv=False,
        )
        assert result == 'Gattaca'

    def test_single_token_prefix_no_entry_year_rejected(self, monkeypatch):
        """Single-token candidate without an entry year is rejected even
        if the filename has a year — fail-closed for narrow guard."""
        from utils.blackhole import _resolve_canonical_title
        self._patch_cache(monkeypatch, {
            'movies': {
                'the': {
                    'title': 'The',
                    # no release_date
                },
            },
        })
        result = _resolve_canonical_title(
            'The.Dark.Knight.2008.1080p.torrent',
            'fallback', is_tv=False,
        )
        # Single-token + missing entry_year + non-None parsed_year:
        # fail-closed → no match → fallback to library parser.
        assert result == 'The Dark Knight'

    def test_extract_entry_year_release_date(self):
        """_extract_entry_year reads release_date for movies."""
        from utils.blackhole import _extract_entry_year
        assert _extract_entry_year({'release_date': '1997-10-24'}) == 1997
        assert _extract_entry_year({'release_date': '2008-07-18'}) == 2008

    def test_extract_entry_year_first_air_date(self):
        """_extract_entry_year reads first_air_date for shows."""
        from utils.blackhole import _extract_entry_year
        assert _extract_entry_year({'first_air_date': '2008-01-20'}) == 2008
        # release_date takes precedence when both present
        assert _extract_entry_year(
            {'release_date': '1997-10-24', 'first_air_date': '2020-01-01'}
        ) == 1997

    def test_extract_entry_year_missing_returns_none(self):
        """No date field → None."""
        from utils.blackhole import _extract_entry_year
        assert _extract_entry_year({}) is None
        assert _extract_entry_year({'title': 'X'}) is None

    def test_extract_entry_year_malformed_returns_none(self):
        """Malformed date strings, non-string values, short strings →
        None (no crash)."""
        from utils.blackhole import _extract_entry_year
        assert _extract_entry_year({'release_date': ''}) is None
        assert _extract_entry_year({'release_date': '19'}) is None  # too short
        assert _extract_entry_year({'release_date': '19xx-01-01'}) is None
        assert _extract_entry_year({'release_date': None}) is None
        assert _extract_entry_year({'release_date': 1997}) is None  # int, not str
        assert _extract_entry_year({'release_date': ['1997']}) is None  # list

    def test_enrich_uses_canonical_title_end_to_end(self, monkeypatch):
        """End-to-end: _enrich_for_history must surface the canonical
        TMDB title when a hit is found."""
        from utils import tmdb as _tmdb
        monkeypatch.setattr(_tmdb, '_load_cache', lambda: {
            'movies': {
                'gattaca (1997)': {
                    'title': 'Gattaca',
                    'release_date': '1997-10-24',
                },
            },
        })
        name, ep = _enrich_for_history(
            'Gattaca.Ethan.Hawke.Sci.Fi.(1997).1080p.BluRay.x264-GROUP.torrent'
        )
        assert name == 'Gattaca'
        assert ep is None


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
            # **_ swallows the new `debrid` kwarg added in plan 39 phase 2
            # — this test only cares about (tid, label) sanitisation.
            lambda tid, fn, label=None, **_: captured.append((tid, label)),
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
            # **_ swallows the new `debrid` kwarg added in plan 39 phase 2.
            lambda tid, fn, label=None, **_: captured.append(tid),
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


# ---------------------------------------------------------------------------
# Plan 41 phase A — add-time filter-block cross-rescue
# ---------------------------------------------------------------------------

class TestAddTimeFilterBlockRescue:
    """When RD returns infringing_file on a magnet add and TB is configured,
    the same hash is routed to TB before the file is failed.  Data-loss
    bug — regression here means popular Disney/HBO/Apple titles silently
    vanish into /watch/.alt_pending/ instead of landing on TB.
    """

    _HASH = 'A' * 40  # canonical magnet info-hash
    _MAGNET_CONTENT = f'magnet:?xt=urn:btih:{_HASH}&dn=Andor.S02E01'
    _FILENAME = 'Andor.S02E01.1080p.DSNP.WEB-DL.DDP5.1.Atmos.H.264-FLUX.magnet'

    def _make_watcher(self, tmp_dir, monkeypatch, with_tb=True):
        """BlackholeWatcher with both RD and TB configured."""
        watch_dir = os.path.join(tmp_dir, 'watch')
        completed_dir = os.path.join(tmp_dir, 'completed')
        os.makedirs(watch_dir)
        os.makedirs(completed_dir)

        monkeypatch.setenv('RD_API_KEY', 'rd-key')
        if with_tb:
            monkeypatch.setenv('TORBOX_API_KEY', 'tb-key')
        else:
            monkeypatch.delenv('TORBOX_API_KEY', raising=False)

        watcher = BlackholeWatcher(
            watch_dir, 'rd-key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed_dir,
            debrid_api_keys={'realdebrid': 'rd-key', 'torbox': 'tb-key'} if with_tb else {'realdebrid': 'rd-key'},
        )
        return watcher, watch_dir

    def _drop_magnet(self, watch_dir):
        path = os.path.join(watch_dir, self._FILENAME)
        with open(path, 'w') as f:
            f.write(self._MAGNET_CONTENT)
        os.utime(path, (time.time() - 10, time.time() - 10))
        return path

    class _FakeTbClient:
        """Stub TB client matching the surface attempt_add_rescue uses."""

        def __init__(self, configured=True):
            self.configured = configured
            self.delete_calls = []

        def add_magnet(self, h):
            # Should NOT be called — blackhole uses _add_to_torbox instead.
            raise AssertionError('add_magnet should not be called from blackhole rescue')

        def torrent_status(self, tid):
            return 'cached'

        def delete_torrent(self, tid):
            self.delete_calls.append(tid)
            return True

    def test_rescues_to_tb_on_rd_filter_block(self, tmp_dir, monkeypatch):
        watcher, watch_dir = self._make_watcher(tmp_dir, monkeypatch)
        magnet_path = self._drop_magnet(watch_dir)

        # RD says filter-block.  TB cache says yes (probed via search.check_debrid_cache).
        # TB add succeeds.
        rd_calls = []

        def fake_rd_add(file_path, api_key=None):
            rd_calls.append(file_path)
            return False, '{"error":"infringing_file","error_code":35}'

        tb_calls = []

        def fake_tb_add(file_path, api_key=None):
            tb_calls.append(file_path)
            return True, {'data': {'torrent_id': 'tb-123'}}

        monkeypatch.setattr(watcher, '_add_to_realdebrid', fake_rd_add)
        monkeypatch.setattr(watcher, '_add_to_torbox', fake_tb_add)

        def fake_cache(hashes, service=None, api_key=None):
            # Both providers report cached — but RD's filter still hits
            # at the add stage (the cache check is informational, the add
            # is where the filter actually fires).
            return {h.lower(): True for h in hashes}

        monkeypatch.setattr('utils.search.check_debrid_cache', fake_cache)
        # Stub the existing-hashes dedup check to avoid hitting RD's
        # /torrents endpoint during the route decision.
        monkeypatch.setattr('utils.search._existing_hashes', lambda *a, **kw: set())

        fake_tb_client = self._FakeTbClient()
        monkeypatch.setattr(
            'utils.debrid_client.get_debrid_client',
            lambda service=None, api_key=None: (fake_tb_client, service),
        )

        # Force routing to RD as primary so the rescue direction is RD→TB
        # even though the cache probe lies and says both are cached.
        monkeypatch.setenv('BLACKHOLE_DEBRID_PRIMARY', 'realdebrid')

        # Capture history events
        history_events = []
        import utils.history as history_mod
        original_log = history_mod.log_event

        def capture_log(*args, **kwargs):
            history_events.append((args, kwargs))
            return original_log(*args, **kwargs)

        monkeypatch.setattr(history_mod, 'log_event', capture_log)

        # Capture monitor starts
        monitor_calls = []
        monkeypatch.setattr(
            watcher, '_start_monitor',
            lambda tid, fn, label=None, debrid=None, **kw: monitor_calls.append((tid, fn, debrid)),
        )

        watcher._process_file(magnet_path)

        # The watch-dir file should be gone (rescued — moved to staged
        # path during rescue, then removed after success).
        assert not os.path.exists(magnet_path), \
            f"magnet should be removed after rescue, still at {magnet_path}"
        # The unique staged path should also be cleaned up post-rescue.
        staging_dir = os.path.join(watch_dir, '.alt_pending')
        if os.path.exists(staging_dir):
            staged_entries = [e for e in os.listdir(staging_dir) if e.startswith('.rescue-')]
            assert staged_entries == [], \
                f"staged rescue file should be removed after success, found: {staged_entries}"
        # TB add was called once via _add_to_torbox.  Plan-41-phase-A
        # rev-2 stages the file under .alt_pending/.rescue-<uuid8>-<name>
        # BEFORE the rescue wait_ready loop to protect against Sonarr
        # re-grab clobbering file_path during a 60s wait — so the TB
        # add now reads from the staged path, not the original.
        assert len(tb_calls) == 1, f"expected exactly one TB add call, got {tb_calls}"
        assert os.path.basename(tb_calls[0]).startswith('.rescue-'), \
            f"TB add should target the rescue-staged file; got {tb_calls[0]}"
        assert os.path.basename(tb_calls[0]).endswith(self._FILENAME), \
            f"staged filename should preserve original suffix; got {tb_calls[0]}"
        # Monitor entry started on TB
        assert len(monitor_calls) == 1
        tid, fn, debrid = monitor_calls[0]
        assert tid == 'tb-123'
        assert debrid == 'torbox'
        # History event emitted with rescue_stage='add_time'
        rescue_events = [
            (a, kw) for a, kw in history_events
            if kw.get('meta', {}).get('cause') == 'debrid_rescued'
        ]
        assert len(rescue_events) == 1, f"expected 1 debrid_rescued event, got {len(rescue_events)}"
        _args, kw = rescue_events[0]
        meta = kw['meta']
        assert meta['rescue_stage'] == 'add_time'
        assert meta['from'] == 'realdebrid'
        assert meta['to'] == 'torbox'

    def test_no_tb_configured_falls_through_to_alt_release(self, tmp_dir, monkeypatch):
        """When only RD is configured, the rescue helper short-circuits and
        the existing alt-release search path runs."""
        watcher, watch_dir = self._make_watcher(tmp_dir, monkeypatch, with_tb=False)
        magnet_path = self._drop_magnet(watch_dir)

        def fake_rd_add(file_path, api_key=None):
            return False, '{"error":"infringing_file","error_code":35}'

        monkeypatch.setattr(watcher, '_add_to_realdebrid', fake_rd_add)
        monkeypatch.setattr('utils.search.check_debrid_cache', lambda *a, **kw: {})
        monkeypatch.setattr('utils.search._existing_hashes', lambda *a, **kw: set())

        # Track whether the alt-release thread was kicked off (it's the
        # fallback path when cross-rescue isn't available).
        import threading as _threading
        alt_thread_started = []
        original_thread = _threading.Thread

        class _CapturingThread(original_thread):
            def __init__(self, *a, **kw):
                if 'alt-retry' in kw.get('name', ''):
                    alt_thread_started.append(True)
                super().__init__(*a, **kw)

            def start(self):
                pass  # Don't actually run the alt-release search

        monkeypatch.setattr('utils.blackhole.threading.Thread', _CapturingThread)

        watcher._process_file(magnet_path)

        assert alt_thread_started == [True], \
            "alt-release thread should fire when cross-rescue isn't available"

    def test_tb_also_filter_blocks_blocklists_hash(self, tmp_dir, monkeypatch):
        """If TB also returns infringing_file, the hash is filter-blocked on
        BOTH debrids — annotate the blocklist so future re-grabs short-circuit."""
        watcher, watch_dir = self._make_watcher(tmp_dir, monkeypatch)
        magnet_path = self._drop_magnet(watch_dir)

        def fake_rd_add(file_path, api_key=None):
            return False, '{"error":"infringing_file","error_code":35}'

        def fake_tb_add(file_path, api_key=None):
            return False, '{"error":"infringing_file","error_code":35}'

        monkeypatch.setattr(watcher, '_add_to_realdebrid', fake_rd_add)
        monkeypatch.setattr(watcher, '_add_to_torbox', fake_tb_add)
        monkeypatch.setattr('utils.search.check_debrid_cache',
                            lambda h, service=None, api_key=None: {x.lower(): True for x in h})
        monkeypatch.setattr('utils.search._existing_hashes', lambda *a, **kw: set())

        monkeypatch.setattr(
            'utils.debrid_client.get_debrid_client',
            lambda service=None, api_key=None: (self._FakeTbClient(), service),
        )
        monkeypatch.setenv('BLACKHOLE_DEBRID_PRIMARY', 'realdebrid')

        # Mock the blocklist
        blocklist_adds = []
        import utils.blocklist as bl_mod

        class _FakeBlocklist:
            def add(self, h, fn, reason='', source=''):
                blocklist_adds.append((h, fn, reason, source))
                return 'entry-1'

            def is_blocked(self, h):
                return False

        monkeypatch.setattr('utils.blackhole._blocklist', _FakeBlocklist())

        # Disable alt-release fallback (we're testing the blocklist annotation)
        monkeypatch.setattr(watcher, '_alt_exhausted', lambda fp: True)

        watcher._process_file(magnet_path)

        # Blocklist should have the "filter_blocked_everywhere" annotation.
        # One entry from rescue (the add-time double-block) — alt-release
        # was disabled via _alt_exhausted so no second entry from that path.
        rescue_entries = [
            e for e in blocklist_adds if 'filter_blocked_everywhere' in e[2]
        ]
        assert len(rescue_entries) >= 1, \
            f"expected filter_blocked_everywhere blocklist entry, got {blocklist_adds!r}"

    def test_unsupported_source_does_not_rescue(self, tmp_dir, monkeypatch):
        """ALLDEBRID has no rescue partner — _attempt_add_time_rescue
        returns False quickly without contacting the network."""
        watcher, watch_dir = self._make_watcher(tmp_dir, monkeypatch)
        magnet_path = self._drop_magnet(watch_dir)

        cache_calls = []

        def fake_cache(*args, **kwargs):
            cache_calls.append(args)
            return {}

        monkeypatch.setattr('utils.search.check_debrid_cache', fake_cache)

        result = watcher._attempt_add_time_rescue(
            magnet_path, self._FILENAME, self._HASH,
            'alldebrid', label=None, dispatch={'alldebrid': lambda *a, **kw: (True, {})},
        )
        assert result is False
        # No cache probe was issued — short-circuit before hitting the network
        assert cache_calls == []

    def test_invalid_torrent_does_not_trigger_rescue(self, tmp_dir, monkeypatch):
        """RD code 30 (torrent_file_invalid) is a permanent rejection
        for the hash — cross-rescue would just hit the same wall on TB.
        The _process_file gate compares classify_add_failure(result) ==
        'filter_block' so invalid_torrent falls through to alt-release
        search WITHOUT touching the TB pipeline at all.  Regression
        guard for the code-reviewer's missing-integration-test gap."""
        watcher, watch_dir = self._make_watcher(tmp_dir, monkeypatch)
        magnet_path = self._drop_magnet(watch_dir)

        def fake_rd_add(file_path, api_key=None):
            return False, '{"error":"torrent_file_invalid","error_code":30}'

        tb_calls = []

        def fake_tb_add(file_path, api_key=None):
            tb_calls.append(file_path)
            return True, {'data': {'torrent_id': 'tb-shouldnt-be-called'}}

        monkeypatch.setattr(watcher, '_add_to_realdebrid', fake_rd_add)
        monkeypatch.setattr(watcher, '_add_to_torbox', fake_tb_add)
        monkeypatch.setattr('utils.search.check_debrid_cache',
                            lambda h, service=None, api_key=None: {x.lower(): True for x in h})
        monkeypatch.setattr('utils.search._existing_hashes', lambda *a, **kw: set())

        # Stub the alt-release thread so it doesn't fire real work
        import threading as _threading
        original_thread = _threading.Thread

        class _NoOpThread(original_thread):
            def start(self):
                pass

        monkeypatch.setattr('utils.blackhole.threading.Thread', _NoOpThread)

        watcher._process_file(magnet_path)

        # TB add must NOT have been called — invalid_torrent isn't a
        # filter_block, so the rescue gate never opens.
        assert tb_calls == [], \
            f"invalid_torrent must not trigger TB rescue; tb_calls={tb_calls}"

    def test_torrent_file_path_routes_through_rescue(self, tmp_dir, monkeypatch):
        """`.torrent` files (bencoded) should rescue identically to
        `.magnet` files — _add_to_torbox handles both by extension, and
        the rescue closure passes the staged file path through opaquely.
        Regression guard for the code-reviewer's missing-test gap."""
        watcher, watch_dir = self._make_watcher(tmp_dir, monkeypatch)

        # Drop a synthetic .torrent file (content doesn't need to be
        # valid bencoding — _add_to_torbox is mocked).
        torrent_path = os.path.join(watch_dir, 'Andor.S02E01.torrent')
        with open(torrent_path, 'wb') as f:
            f.write(b'd4:infod4:name20:Andor.S02E01.WEB-DL5:filesle4:type5:hashee')
        os.utime(torrent_path, (time.time() - 10, time.time() - 10))

        # Patch the info-hash extractor so we get a deterministic hash
        # without depending on .torrent bencoding.
        monkeypatch.setattr(watcher, '_extract_info_hash_from_file',
                            lambda fp: self._HASH)

        def fake_rd_add(file_path, api_key=None):
            return False, '{"error":"infringing_file","error_code":35}'

        tb_calls = []

        def fake_tb_add(file_path, api_key=None):
            tb_calls.append(file_path)
            return True, {'data': {'torrent_id': 'tb-456'}}

        monkeypatch.setattr(watcher, '_add_to_realdebrid', fake_rd_add)
        monkeypatch.setattr(watcher, '_add_to_torbox', fake_tb_add)
        monkeypatch.setattr('utils.search.check_debrid_cache',
                            lambda h, service=None, api_key=None: {x.lower(): True for x in h})
        monkeypatch.setattr('utils.search._existing_hashes', lambda *a, **kw: set())
        monkeypatch.setattr(
            'utils.debrid_client.get_debrid_client',
            lambda service=None, api_key=None: (self._FakeTbClient(), service),
        )
        monkeypatch.setenv('BLACKHOLE_DEBRID_PRIMARY', 'realdebrid')

        monitor_calls = []
        monkeypatch.setattr(
            watcher, '_start_monitor',
            lambda tid, fn, label=None, debrid=None, **kw: monitor_calls.append((tid, fn, debrid)),
        )

        watcher._process_file(torrent_path)

        assert not os.path.exists(torrent_path), \
            f".torrent should be removed after rescue, still at {torrent_path}"
        assert len(tb_calls) == 1
        # Staging preserves the extension so _add_to_torbox's ext branch
        # picks the .torrent code path.
        assert tb_calls[0].endswith('.torrent'), \
            f"staged path should end with .torrent; got {tb_calls[0]}"
        assert monitor_calls == [('tb-456', 'Andor.S02E01.torrent', 'torbox')]


class TestRescueOrphanRecovery:
    """Plan 41 phase A second-pass reviewer fix-up: rescue orphans
    (files staged under ``.alt_pending/.rescue-<uuid8>-<filename>``
    when the container died mid-rescue) must be recovered to ``failed/``
    under their ORIGINAL filename so Sonarr/Radarr's blackhole import
    recognises them on the next retry cycle.  Pre-fix the recovery
    moved them with the ``.rescue-`` prefix intact and they rotted
    silently."""

    def test_restore_basename_strips_prefix(self):
        """Pure-function pin on the prefix-strip regex."""
        from utils.blackhole import _restore_rescue_basename
        assert _restore_rescue_basename(
            '.rescue-deadbeef-Show.Name.S01E01.1080p.WEB-DL.magnet'
        ) == 'Show.Name.S01E01.1080p.WEB-DL.magnet'

    def test_restore_basename_passes_through_non_prefixed(self):
        """Files staged by the older alt-release path have no prefix —
        must be returned unchanged."""
        from utils.blackhole import _restore_rescue_basename
        assert _restore_rescue_basename('Show.Name.S01E01.torrent') == 'Show.Name.S01E01.torrent'

    def test_restore_basename_only_matches_8_hex_chars(self):
        """The regex is anchored to 8 hex chars exactly — a file that
        coincidentally starts with ``.rescue-`` but has a non-hex segment
        (or different length) is NOT treated as a rescue orphan."""
        from utils.blackhole import _restore_rescue_basename
        # 7 hex chars — too short, regex misses, name unchanged.
        assert _restore_rescue_basename('.rescue-deadbee-name') == '.rescue-deadbee-name'
        # 9 hex chars — too long, regex anchors on 8, but then the next
        # char is hex not '-', so the regex doesn't match. Unchanged.
        assert _restore_rescue_basename('.rescue-deadbeef9-name') == '.rescue-deadbeef9-name'
        # Non-hex chars in the uuid slot — must NOT match.
        assert _restore_rescue_basename('.rescue-xyzzy123-name') == '.rescue-xyzzy123-name'

    def test_restore_basename_empty(self):
        from utils.blackhole import _restore_rescue_basename
        assert _restore_rescue_basename('') == ''
        assert _restore_rescue_basename(None) is None

    def test_recover_alt_pending_strips_rescue_prefix(self, tmp_dir):
        """Integration: a rescue orphan in .alt_pending/ gets moved to
        failed/ under its ORIGINAL filename so Sonarr/Radarr's
        blackhole import can recognise it on the next retry."""
        watch_dir = os.path.join(tmp_dir, 'watch')
        alt_pending = os.path.join(watch_dir, '.alt_pending')
        os.makedirs(alt_pending)

        # Simulate a rescue orphan — file staged with the .rescue-<uuid8>- prefix.
        orphan_path = os.path.join(alt_pending, '.rescue-deadbeef-Andor.S02E01.magnet')
        with open(orphan_path, 'w') as f:
            f.write('magnet:?xt=urn:btih:abc')

        watcher = BlackholeWatcher(watch_dir, 'rd-key', 'realdebrid')
        watcher._recover_alt_pending()

        # The rescue orphan must now be in failed/ as ``Andor.S02E01.magnet``
        # (the prefix stripped), NOT ``.rescue-deadbeef-Andor.S02E01.magnet``.
        failed_dir = os.path.join(watch_dir, 'failed')
        assert os.path.isdir(failed_dir)
        recovered = os.path.join(failed_dir, 'Andor.S02E01.magnet')
        assert os.path.isfile(recovered), \
            f"orphan not recovered under original name; failed/ contents: {os.listdir(failed_dir)}"
        # Mangled name must NOT be present.
        mangled = os.path.join(failed_dir, '.rescue-deadbeef-Andor.S02E01.magnet')
        assert not os.path.exists(mangled), \
            f"orphan recovered with prefix intact — Sonarr would not recognise this"

    def test_recover_alt_pending_with_label(self, tmp_dir):
        """Labeled rescue orphan recovered under the matching failed/label/."""
        watch_dir = os.path.join(tmp_dir, 'watch')
        alt_pending_sonarr = os.path.join(watch_dir, '.alt_pending', 'sonarr')
        os.makedirs(alt_pending_sonarr)

        orphan_path = os.path.join(alt_pending_sonarr, '.rescue-cafebabe-Yellowjackets.S02E09.magnet')
        with open(orphan_path, 'w') as f:
            f.write('magnet:?xt=urn:btih:def')

        watcher = BlackholeWatcher(watch_dir, 'rd-key', 'realdebrid')
        watcher._recover_alt_pending()

        recovered = os.path.join(watch_dir, 'failed', 'sonarr', 'Yellowjackets.S02E09.magnet')
        assert os.path.isfile(recovered), \
            f"labeled orphan not recovered correctly; failed/sonarr/ contents: " \
            f"{os.listdir(os.path.join(watch_dir, 'failed', 'sonarr')) if os.path.isdir(os.path.join(watch_dir, 'failed', 'sonarr')) else 'missing'}"

    def test_recover_alt_pending_legacy_no_prefix_unchanged(self, tmp_dir):
        """Alt-release-path orphans (no .rescue- prefix) keep their
        original behaviour — moved to failed/ with the same name."""
        watch_dir = os.path.join(tmp_dir, 'watch')
        alt_pending = os.path.join(watch_dir, '.alt_pending')
        os.makedirs(alt_pending)

        # No prefix — pre-plan-41 alt-release staging.
        orphan_path = os.path.join(alt_pending, 'Old.Style.Release.torrent')
        with open(orphan_path, 'w') as f:
            f.write('d4:infod4:name3:Olde')

        watcher = BlackholeWatcher(watch_dir, 'rd-key', 'realdebrid')
        watcher._recover_alt_pending()

        recovered = os.path.join(watch_dir, 'failed', 'Old.Style.Release.torrent')
        assert os.path.isfile(recovered), \
            f"non-prefixed orphan should recover unchanged; failed/ contents: " \
            f"{os.listdir(os.path.join(watch_dir, 'failed'))}"

    def test_recover_alt_pending_rescue_orphan_not_alt_exhausted(self, tmp_dir):
        """A rescue orphan never ran an alt-release search, so it must NOT
        be marked alt_exhausted on recovery — that flag makes _retry_failed
        skip the file forever (permanent dead-end). Alt-release orphans
        (no prefix) keep the exhausted marker."""
        from utils.blackhole import RetryMeta
        watch_dir = os.path.join(tmp_dir, 'watch')
        alt_pending = os.path.join(watch_dir, '.alt_pending')
        os.makedirs(alt_pending)

        rescue_orphan = os.path.join(alt_pending, '.rescue-deadbeef-Andor.S02E01.magnet')
        with open(rescue_orphan, 'w') as f:
            f.write('magnet:?xt=urn:btih:' + 'a' * 40)
        legacy_orphan = os.path.join(alt_pending, 'Old.Style.Release.magnet')
        with open(legacy_orphan, 'w') as f:
            f.write('magnet:?xt=urn:btih:' + 'b' * 40)

        watcher = BlackholeWatcher(watch_dir, 'rd-key', 'realdebrid')
        watcher._recover_alt_pending()

        failed_dir = os.path.join(watch_dir, 'failed')
        recovered_rescue = os.path.join(failed_dir, 'Andor.S02E01.magnet')
        recovered_legacy = os.path.join(failed_dir, 'Old.Style.Release.magnet')
        assert os.path.isfile(recovered_rescue)
        assert os.path.isfile(recovered_legacy)
        assert not RetryMeta.is_alt_exhausted(recovered_rescue), \
            "rescue orphan wrongly marked alt_exhausted — _retry_failed would skip it forever"
        assert RetryMeta.is_alt_exhausted(recovered_legacy), \
            "alt-release orphan lost its alt_exhausted marker"


class TestAltReleaseProviderBinding:
    """The alt-release / compromise chain must bind torrent-ID extraction
    and the symlink monitor to the ROUTED provider, not the primary.
    Regression: with RD primary and the handler routed to TorBox,
    _extract_torrent_id defaulted to RD's schema (str(result)) and the
    monitor polled RD for a TorBox torrent — content landed on TorBox
    but was never symlinked."""

    def test_try_releases_binds_routed_debrid(self, tmp_dir, monkeypatch):
        watch_dir = os.path.join(tmp_dir, 'watch')
        os.makedirs(watch_dir)
        watcher = BlackholeWatcher(watch_dir, 'rd-key', 'realdebrid',
                                   symlink_enabled=True)

        orig_path = os.path.join(tmp_dir, 'orig.magnet')
        with open(orig_path, 'w') as f:
            f.write('magnet:?xt=urn:btih:' + 'c' * 40)

        tb_result = {'success': True, 'data': {'torrent_id': 999}}
        captured = {}
        monkeypatch.setattr(
            watcher, '_start_monitor',
            lambda tid, fn, label=None, debrid=None, **_: captured.update(
                {'tid': tid, 'debrid': debrid}),
        )

        releases = [{'guid': 'magnet:?xt=urn:btih:' + 'd' * 40,
                     'title': 'Alt.Release.1080p'}]
        ok = watcher._try_releases(
            releases, lambda path: (True, tb_result),
            'orig.magnet', orig_path, label=None, debrid='torbox',
        )
        assert ok is True
        assert captured['tid'] == '999', \
            f"TorBox result parsed with wrong provider schema: {captured['tid']!r}"
        assert captured['debrid'] == 'torbox', \
            "monitor not bound to the routed provider"


class TestRescueStagingFilenameLength:
    """Plan 41 phase A second-pass reviewer fix-up: long multi-byte
    filenames (Russian-tracker releases with Cyrillic in the .torrent
    name) can push the staged basename over POSIX NAME_MAX (255
    bytes).  Truncation in the staging path keeps ``os.rename`` from
    raising ENAMETOOLONG and silently dropping the rescue.
    """

    _HASH = 'A' * 40

    def _make_watcher(self, tmp_dir, monkeypatch):
        watch_dir = os.path.join(tmp_dir, 'watch')
        completed_dir = os.path.join(tmp_dir, 'completed')
        os.makedirs(watch_dir)
        os.makedirs(completed_dir)
        monkeypatch.setenv('RD_API_KEY', 'rd-key')
        monkeypatch.setenv('TORBOX_API_KEY', 'tb-key')
        return BlackholeWatcher(
            watch_dir, 'rd-key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed_dir,
            debrid_api_keys={'realdebrid': 'rd-key', 'torbox': 'tb-key'},
        ), watch_dir

    def test_long_filename_staged_under_name_max(self, tmp_dir, monkeypatch):
        """A 240-byte filename + .rescue-<uuid8>- prefix would exceed
        NAME_MAX without truncation.  Verify the staged basename is
        capped so ``os.rename`` succeeds."""
        watcher, watch_dir = self._make_watcher(tmp_dir, monkeypatch)
        # 240-char filename — plus the 17-byte prefix overhead would
        # push past 255 if not truncated.
        long_filename = 'A' * 240 + '.magnet'
        magnet_path = os.path.join(watch_dir, long_filename)
        with open(magnet_path, 'w') as f:
            f.write(f'magnet:?xt=urn:btih:{self._HASH}')
        os.utime(magnet_path, (time.time() - 10, time.time() - 10))

        def fake_rd_add(file_path, api_key=None):
            return False, '{"error":"infringing_file","error_code":35}'

        tb_calls = []

        def fake_tb_add(file_path, api_key=None):
            tb_calls.append(file_path)
            return True, {'data': {'torrent_id': 'tb-long-1'}}

        monkeypatch.setattr(watcher, '_add_to_realdebrid', fake_rd_add)
        monkeypatch.setattr(watcher, '_add_to_torbox', fake_tb_add)
        monkeypatch.setattr('utils.search.check_debrid_cache',
                            lambda h, service=None, api_key=None: {x.lower(): True for x in h})
        monkeypatch.setattr('utils.search._existing_hashes', lambda *a, **kw: set())

        class _FakeTbClient:
            configured = True

            def add_magnet(self, h):
                raise AssertionError('should not be called')

            def torrent_status(self, tid):
                return 'cached'

            def delete_torrent(self, tid):
                return True

        monkeypatch.setattr(
            'utils.debrid_client.get_debrid_client',
            lambda service=None, api_key=None: (_FakeTbClient(), service),
        )
        monkeypatch.setenv('BLACKHOLE_DEBRID_PRIMARY', 'realdebrid')
        monkeypatch.setattr(watcher, '_start_monitor', lambda *a, **kw: None)

        # Should not raise (used to raise ENAMETOOLONG inside the staging
        # os.rename before the truncation fix).
        watcher._process_file(magnet_path)

        # Rescue should have run successfully.
        assert len(tb_calls) == 1
        # The staged basename used for the TB add must fit under NAME_MAX.
        assert len(os.path.basename(tb_calls[0]).encode('utf-8')) <= 255, \
            f"staged basename exceeds NAME_MAX: {os.path.basename(tb_calls[0])!r}"


class TestRescueRestoreAtomicity:
    """Plan 41 phase A second-pass reviewer fix-up: the rescue-failure
    restore path uses ``os.link`` + ``os.unlink`` instead of a
    check-then-rename sequence so a fresh Sonarr drop landing at
    ``file_path`` during the rescue wait cannot be silently
    overwritten by the staged file.
    """

    _HASH = 'A' * 40

    def _make_watcher(self, tmp_dir, monkeypatch, with_tb=True):
        watch_dir = os.path.join(tmp_dir, 'watch')
        completed_dir = os.path.join(tmp_dir, 'completed')
        os.makedirs(watch_dir)
        os.makedirs(completed_dir)
        monkeypatch.setenv('RD_API_KEY', 'rd-key')
        if with_tb:
            monkeypatch.setenv('TORBOX_API_KEY', 'tb-key')
        return BlackholeWatcher(
            watch_dir, 'rd-key', 'realdebrid',
            symlink_enabled=True,
            completed_dir=completed_dir,
            debrid_api_keys={'realdebrid': 'rd-key', 'torbox': 'tb-key'} if with_tb else {'realdebrid': 'rd-key'},
        ), watch_dir

    def test_fresh_drop_during_rescue_wait_preserves_both_files(self, tmp_dir, monkeypatch):
        """Simulate Sonarr re-grabbing the same filename while our rescue
        is in flight: the fresh drop at file_path survives, AND the
        rescue's staged content survives under its unique name."""
        watcher, watch_dir = self._make_watcher(tmp_dir, monkeypatch)
        filename = 'Andor.S02E01.magnet'
        original_path = os.path.join(watch_dir, filename)
        with open(original_path, 'w') as f:
            f.write('magnet:?xt=urn:btih:' + self._HASH + '&n=original')
        os.utime(original_path, (time.time() - 10, time.time() - 10))

        def fake_rd_add(file_path, api_key=None):
            return False, '{"error":"infringing_file","error_code":35}'

        # Closure: during the alt_add_fn call, simulate Sonarr dropping
        # a new file at the ORIGINAL file_path while we're "waiting"
        # for the alt add to complete.  The rescue's restore path
        # should then leave the original at the staged path.
        def fake_tb_add(file_path, api_key=None):
            # Fresh Sonarr drop arrives mid-rescue, before we've decided
            # success/failure.
            with open(original_path, 'w') as f:
                f.write('magnet:?xt=urn:btih:' + self._HASH + '&n=fresh-drop')
            # Then we return a failure so the rescue tries to restore.
            return False, '{"error":"rate limit exceeded"}'

        monkeypatch.setattr(watcher, '_add_to_realdebrid', fake_rd_add)
        monkeypatch.setattr(watcher, '_add_to_torbox', fake_tb_add)
        monkeypatch.setattr('utils.search.check_debrid_cache',
                            lambda h, service=None, api_key=None: {x.lower(): True for x in h})
        monkeypatch.setattr('utils.search._existing_hashes', lambda *a, **kw: set())

        class _FakeTbClient:
            configured = True

            def add_magnet(self, h):
                raise AssertionError('unreached')

            def torrent_status(self, tid):
                return ''

            def delete_torrent(self, tid):
                return True

        monkeypatch.setattr(
            'utils.debrid_client.get_debrid_client',
            lambda service=None, api_key=None: (_FakeTbClient(), service),
        )
        monkeypatch.setenv('BLACKHOLE_DEBRID_PRIMARY', 'realdebrid')

        # Disable the existing alt-release fallback so we can observe
        # the rescue-restore behaviour in isolation.
        import threading as _threading

        class _NoOpThread(_threading.Thread):
            def start(self):
                pass

        monkeypatch.setattr('utils.blackhole.threading.Thread', _NoOpThread)

        watcher._process_file(original_path)

        # After _process_file: fresh drop has been processed by the
        # post-rescue rejection-handling path (alt-release staging or
        # failed-dir move).  We don't care WHICH happened — both are
        # legitimate outcomes for an ``infringing_file``-shaped failure
        # after rescue couldn't recover.  We DO care that:
        #   (a) the fresh-drop content survived rather than being
        #       overwritten by the older staged file, and
        #   (b) the rescue's staged file remained at its unique
        #       .rescue-* name for manual recovery.
        # Walk the watch-dir tree looking for the content markers.
        fresh_drop_survived = False
        rescue_orphan_survived = False
        for dp, _dn, files in os.walk(watch_dir):
            for fn in files:
                fpath = os.path.join(dp, fn)
                try:
                    with open(fpath) as f:
                        body = f.read()
                except OSError:
                    continue
                if 'fresh-drop' in body:
                    fresh_drop_survived = True
                if 'n=original' in body and fn.startswith('.rescue-'):
                    rescue_orphan_survived = True

        assert fresh_drop_survived, \
            "Fresh Sonarr drop must survive the rescue-restore path " \
            "(silently overwritten = data loss)"
        assert rescue_orphan_survived, \
            "Rescue's staged file must remain under its unique .rescue-* " \
            "name for manual recovery — collision-with-fresh-drop case"


class TestScannerHandoff:
    """Mount-timeout-but-confirmed-ready hand-off to the library scanner.

    When a torrent is confirmed added + ready on the debrid but doesn't
    surface on the rclone mount within mount_poll_timeout (common under
    TorBox 429 rate-limiting), the worker must NOT treat it as a hard
    failure.  It registers a 'to-debrid' library pending entry so the
    scanner resolves it on a later pass.
    """

    @pytest.fixture(autouse=True)
    def _isolate_prefs(self, tmp_dir, monkeypatch):
        import utils.library_prefs as lp
        monkeypatch.setattr(lp, 'PREFS_PATH', os.path.join(tmp_dir, 'library_prefs.json'))
        monkeypatch.setattr(lp, 'PENDING_PATH', os.path.join(tmp_dir, 'library_pending.json'))

    # ── _register_scanner_handoff unit tests ──────────────────────────

    def test_handoff_movie_registers_pending(self):
        import utils.library_prefs as lp
        from utils.library import normalize_title
        watcher = BlackholeWatcher('/tmp', 'key', 'realdebrid')
        ok = watcher._register_scanner_handoff(
            'Inside.Out.2.2024.1080p.WEB-DL.x264.magnet')
        assert ok is True
        pending = lp.get_all_pending()
        key = normalize_title('Inside Out 2')
        assert key in pending
        entry = pending[key]
        assert entry['direction'] == 'to-debrid'
        assert entry['episodes'] == [{'season': 0, 'episode': 0}]
        assert entry.get('created')  # escalation clock starts

    def test_handoff_show_single_episode(self):
        import utils.library_prefs as lp
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        ok = watcher._register_scanner_handoff(
            'The.Show.S01E05.1080p.WEB.H264-GROUP.torrent')
        assert ok is True
        pending = lp.get_all_pending()
        # exactly one entry, direction to-debrid, the parsed episode
        assert len(pending) == 1
        entry = next(iter(pending.values()))
        assert entry['direction'] == 'to-debrid'
        assert entry['episodes'] == [{'season': 1, 'episode': 5}]

    def test_handoff_show_episode_range(self):
        import utils.library_prefs as lp
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        ok = watcher._register_scanner_handoff(
            'The.Show.S02E01-E03.1080p.WEB.torrent')
        assert ok is True
        entry = next(iter(lp.get_all_pending().values()))
        assert entry['episodes'] == [
            {'season': 2, 'episode': 1},
            {'season': 2, 'episode': 2},
            {'season': 2, 'episode': 3},
        ]

    def test_handoff_season_pack_skips(self):
        """Season pack (no parseable episodes) must NOT register pending —
        an empty episode list would falsely escalate to debrid-unavailable
        even after the scanner creates symlinks."""
        import utils.library_prefs as lp
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        ok = watcher._register_scanner_handoff(
            'The.Show.S03.1080p.WEB.Complete.Season.torrent')
        assert ok is False
        assert lp.get_all_pending() == {}

    def test_handoff_multi_season_pack_skips(self):
        """Multi-season pack (S01-S05) parses as is_tv=False — must NOT
        register a bogus movie (0,0) entry under the show's title (would
        never resolve and falsely escalate to debrid-unavailable)."""
        import utils.library_prefs as lp
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        ok = watcher._register_scanner_handoff(
            'The.Show.S01-S05.1080p.WEB.Complete.torrent')
        assert ok is False
        assert lp.get_all_pending() == {}

    def test_handoff_cross_season_range_skips(self):
        """Cross-season episode range (S01E01-S02E10) can't be one
        (season, episodes) entry — must skip, not register a partial list."""
        import utils.library_prefs as lp
        watcher = BlackholeWatcher('/tmp', 'key', 'torbox')
        ok = watcher._register_scanner_handoff(
            'The.Show.S01E01-S02E10.1080p.WEB.torrent')
        assert ok is False
        assert lp.get_all_pending() == {}

    # ── timeout-branch integration ────────────────────────────────────

    def _make_symlink_watcher(self, tmp_dir, debrid):
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed, exist_ok=True)
        return BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), 'key', debrid,
            symlink_enabled=True, completed_dir=completed,
            rclone_mount=os.path.join(tmp_dir, 'data'),
            mount_poll_timeout=0.3, mount_poll_interval=0.02,
        )

    def _drive_timeout(self, watcher, monkeypatch, torrent_id, filename,
                       debrid, ready_status, label=None):
        """Run _monitor_and_symlink with stubs so it reaches the Phase-2
        mount-timeout branch (ready on debrid, never on mount)."""
        status_attr = {
            'realdebrid': '_check_realdebrid_status',
            'torbox': '_check_torbox_status',
        }[debrid]
        monkeypatch.setattr(
            watcher, status_attr,
            lambda tid, api_key=None: (ready_status, {'filename': filename}))
        monkeypatch.setattr(watcher, '_has_usable_media_files',
                            lambda *a, **k: True)
        monkeypatch.setattr(watcher, '_extract_release_name',
                            lambda info, debrid=None: filename.rsplit('.', 1)[0])
        monkeypatch.setattr(watcher, '_extract_filenames_from_info',
                            lambda *a, **k: [])
        # Never surfaces on the mount → forces the timeout branch.
        monkeypatch.setattr(watcher, '_find_on_mount',
                            lambda *a, **k: (None, None, None))
        # Seed the blackhole monitor entry so we can assert it's removed.
        watcher._add_pending(torrent_id, filename, label=label, debrid=debrid)
        watcher._monitor_and_symlink(torrent_id, filename, label, debrid)

    def test_mount_timeout_movie_hands_off(self, tmp_dir, monkeypatch):
        import utils.library_prefs as lp
        from utils.library import normalize_title
        watcher = self._make_symlink_watcher(tmp_dir, 'realdebrid')
        self._drive_timeout(
            watcher, monkeypatch, 'rd-abc',
            'Inside.Out.2.2024.1080p.WEB-DL.x264.magnet',
            'realdebrid', 'downloaded')
        # Library pending registered for the scanner.
        pending = lp.get_all_pending()
        assert normalize_title('Inside Out 2') in pending
        assert pending[normalize_title('Inside Out 2')]['direction'] == 'to-debrid'
        # Blackhole monitor handed off (removed) — not retried / resumed.
        assert all(e['torrent_id'] != 'rd-abc' for e in watcher._load_pending())

    def test_mount_timeout_show_hands_off(self, tmp_dir, monkeypatch):
        """Sonarr parity: a show grab takes the same hand-off path."""
        import utils.library_prefs as lp
        watcher = self._make_symlink_watcher(tmp_dir, 'torbox')
        self._drive_timeout(
            watcher, monkeypatch, 'tb-xyz',
            'The.Show.S01E05.1080p.WEB.H264-GROUP.torrent',
            'torbox', 'completed', label='sonarr')
        pending = lp.get_all_pending()
        assert len(pending) == 1
        entry = next(iter(pending.values()))
        assert entry['direction'] == 'to-debrid'
        assert entry['episodes'] == [{'season': 1, 'episode': 5}]
        assert all(e['torrent_id'] != 'tb-xyz' for e in watcher._load_pending())


class TestTorboxCachedAlternative:
    """_try_torbox_cached_alternative: when a grabbed release is uncached,
    grab a same-title, same-tier alternative that IS cached on TorBox rather
    than dropping the title to 'Wanted'."""

    REJECTED = 'a' * 40   # the uncached hash the arr picked
    CACHED_ALT = 'b' * 40  # a same-title release cached on TorBox

    @pytest.fixture(autouse=True)
    def _ledger(self):
        """Reset attempt_ledger to the pristine uninitialized state.

        The sibling-grab dedup now mirrors into the module-global
        attempt_ledger, so a ledger initialized by an unrelated earlier
        test would leak tbaltdedup keys across tests in this class.
        Subclasses that WANT a live ledger (TestTorboxAltGiveUpCap)
        override this fixture with one that reloads AND initializes.
        """
        import importlib
        from utils import attempt_ledger
        importlib.reload(attempt_ledger)
        yield

    def _make_watcher(self, tmp_dir, tb_key='tbkey', symlink_enabled=False):
        return BlackholeWatcher(
            os.path.join(tmp_dir, 'watch'), tb_key, 'torbox',
            symlink_enabled=symlink_enabled,
            completed_dir=os.path.join(tmp_dir, 'completed'),
            rclone_mount=os.path.join(tmp_dir, 'data'),
            debrid_api_keys={'torbox': tb_key} if tb_key else None,
        )

    def _make_file(self, tmp_dir, name):
        os.makedirs(tmp_dir, exist_ok=True)
        path = os.path.join(tmp_dir, name)
        with open(path, 'w') as f:
            f.write('magnet:?xt=urn:btih:' + self.REJECTED)
        return path

    def _stub_search(self, monkeypatch, results):
        import utils.search as search
        monkeypatch.setattr(search, 'search_torrentio',
                            lambda *a, **k: results)

    def _stub_cache(self, monkeypatch, cache_map):
        import utils.search as search
        monkeypatch.setattr(search, 'check_debrid_cache',
                            lambda hashes, **k: {h: cache_map.get(h) for h in hashes})
        monkeypatch.setattr(search, 'remember_added_hash', lambda *a, **k: None)

    def _candidate(self, info_hash, tier='1080p', seeds=10, size=8_000_000_000,
                   title=None):
        score = {'2160p': 4, '1080p': 3, '720p': 2}.get(tier, 0)
        return {
            'title': title or f'Sing.2.{tier}.WEB.x264-GRP',
            'info_hash': info_hash,
            'size_bytes': size,
            'seeds': seeds,
            'quality': {'label': tier, 'score': score},
        }

    def test_disabled_via_env_declines_and_keeps_file(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'false')
        w = self._make_watcher(tmp_dir)
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert os.path.exists(fp)

    def test_no_info_hash_declines(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), '', 'realdebrid') is False
        assert os.path.exists(fp)

    def test_no_torbox_key_declines(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir, tb_key='')
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert os.path.exists(fp)

    def test_unparseable_tier_declines(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        # No 1080p/720p/2160p marker -> parse_quality returns 'Unknown'.
        fp = self._make_file(tmp_dir, 'Sing.2.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert os.path.exists(fp)

    def test_no_imdb_declines(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: (None, None, None, None))
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert os.path.exists(fp)

    def test_no_cached_alternative_declines_and_keeps_file(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt1234567', 'movie', None, None))
        self._stub_search(monkeypatch, [self._candidate(self.CACHED_ALT)])
        # The only alternative is uncached on TorBox.
        self._stub_cache(monkeypatch, {self.CACHED_ALT: False})
        add_called = []
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: add_called.append(1) or (True, {}))
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert not add_called
        assert os.path.exists(fp)

    def test_wrong_tier_alternative_declined(self, tmp_dir, monkeypatch):
        """A cached alternative at a DIFFERENT tier than the arr approved
        must not be grabbed (don't silently downgrade/upgrade quality)."""
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt1234567', 'movie', None, None))
        # Rejected release was 1080p; only cached alt is 720p.
        self._stub_search(monkeypatch,
                          [self._candidate(self.CACHED_ALT, tier='720p')])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: (True, {}))
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert os.path.exists(fp)

    def test_mislabeled_same_tier_candidate_excluded(self, tmp_dir, monkeypatch):
        """A same-tier cached candidate whose release name doesn't match the
        arr-approved title is a mislabeled Torrentio upload — grabbing it
        would park the wrong movie under this title."""
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt1234567', 'movie', None, None))
        self._stub_search(monkeypatch, [self._candidate(
            self.CACHED_ALT, title='Fight.Club.1999.1080p.WEB.x264-JUNK')])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        adds = []
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: adds.append(1) or (True, {}))
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert not adds
        assert os.path.exists(fp)

    def test_rejected_hash_excluded_from_candidates(self, tmp_dir, monkeypatch):
        """Even if the rejected hash is reported cached, it is excluded from
        the candidate set (it's the one we already know fails downstream)."""
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt1234567', 'movie', None, None))
        # Search returns ONLY the rejected hash -> nothing left after exclusion.
        self._stub_search(monkeypatch, [self._candidate(self.REJECTED)])
        self._stub_cache(monkeypatch, {self.REJECTED: True})
        monkeypatch.setattr(w, '_add_to_torbox', lambda *a, **k: (True, {}))
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert os.path.exists(fp)

    def test_happy_path_grabs_cached_alt_and_removes_file(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt1234567', 'movie', None, None))
        # One uncached alt + one cached alt, same tier as rejected (1080p).
        self._stub_search(monkeypatch, [
            self._candidate(self.CACHED_ALT, tier='1080p', seeds=50),
            self._candidate('c' * 40, tier='1080p', seeds=5),
        ])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True, 'c' * 40: False})
        added = {}
        def fake_add(path, api_key=None):
            with open(path) as f:
                added['magnet'] = f.read()
            return True, {'data': {'torrent_id': 999}}
        monkeypatch.setattr(w, '_add_to_torbox', fake_add)
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')

        events = []
        import utils.blackhole as bh
        monkeypatch.setattr(bh, '_history', type('H', (), {
            'log_event': staticmethod(lambda *a, **k: events.append((a, k)))})())

        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is True
        # Grabbed the cached alternative (best seeded), not the uncached one.
        assert self.CACHED_ALT in added['magnet']
        # Original watch-dir file removed so the scanner won't re-process it.
        assert not os.path.exists(fp)
        # History event records the recovery with the right cause + provider.
        assert events
        _, kwargs = events[0]
        assert kwargs['meta']['cause'] == 'tb_cached_alt_grabbed'
        assert kwargs['meta']['provider'] == 'torbox'
        assert kwargs['meta']['rejected_provider'] == 'realdebrid'
        assert kwargs['meta']['info_hash'] == self.CACHED_ALT

    def test_torbox_add_failure_keeps_file(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt1234567', 'movie', None, None))
        self._stub_search(monkeypatch, [self._candidate(self.CACHED_ALT)])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: (False, 'rate limit exceeded'))
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        # Add failed -> leave the file for the caller's normal handling.
        assert os.path.exists(fp)

    def test_symlink_mode_starts_monitor_and_removes_file(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir, symlink_enabled=True)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt1234567', 'movie', None, None))
        self._stub_search(monkeypatch, [self._candidate(self.CACHED_ALT)])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: (True, {'data': {'torrent_id': 777}}))
        started = []
        monkeypatch.setattr(w, '_start_monitor',
                            lambda tid, fn, label=None, debrid=None: started.append((tid, debrid)))
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is True
        assert started == [('777', 'torbox')]
        assert not os.path.exists(fp)

    def test_symlink_mode_no_torrent_id_declines_and_keeps_file(self, tmp_dir, monkeypatch):
        """If the torrent id can't be extracted in symlink mode, the alt would
        be orphaned on TorBox with no monitor — decline (and keep the original
        for the caller's normal handling) rather than silently claim success."""
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir, symlink_enabled=True)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt1234567', 'movie', None, None))
        self._stub_search(monkeypatch, [self._candidate(self.CACHED_ALT)])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        # TorBox add "succeeds" but returns a body with no extractable id.
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: (True, {'data': {}}))
        started = []
        monkeypatch.setattr(w, '_start_monitor',
                            lambda *a, **k: started.append(1))
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert not started
        assert os.path.exists(fp)

    def test_dedup_suppresses_second_grab_for_same_season(self, tmp_dir, monkeypatch):
        """One cached pack recovers a whole season: after the first episode of
        a season grabs an alternative, a sibling episode is skipped before any
        search/probe/grab — this is what stops 3+ packs landing for one season
        and overdriving the rclone VFS into a TorBox 429 storm."""
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt999', 'series', 2, 1))
        self._stub_search(monkeypatch, [self._candidate(
            self.CACHED_ALT, title='Show.S02.1080p.WEB.x264-GRP')])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        adds = []
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: adds.append(1) or (True, {}))
        fp1 = self._make_file(tmp_dir, 'Show.S02E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp1, os.path.basename(fp1), self.REJECTED, 'realdebrid') is True
        assert len(adds) == 1
        # Sibling episode of the SAME season: suppressed, file kept, no 2nd add.
        fp2 = self._make_file(tmp_dir, 'Show.S02E02.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp2, os.path.basename(fp2), self.REJECTED, 'realdebrid') is False
        assert len(adds) == 1
        assert os.path.exists(fp2)

    def test_dedup_does_not_suppress_a_different_season(self, tmp_dir, monkeypatch):
        """The dedup key is (imdb_id, season) — a grab for S02 must not block
        recovery of S03."""
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        ident = {'season': 2}
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt999', 'series', ident['season'], 1))
        self._stub_search(monkeypatch, [self._candidate(
            self.CACHED_ALT, title='Show.S02.1080p.WEB.x264-GRP')])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        adds = []
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: adds.append(1) or (True, {}))
        fp1 = self._make_file(tmp_dir, 'Show.S02E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp1, os.path.basename(fp1), self.REJECTED, 'realdebrid') is True
        ident['season'] = 3
        fp2 = self._make_file(tmp_dir, 'Show.S03E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp2, os.path.basename(fp2), self.REJECTED, 'realdebrid') is True
        assert len(adds) == 2

    def test_dedup_not_set_when_grab_fails(self, tmp_dir, monkeypatch):
        """The guard records only on a committed grab — a failed first attempt
        (no cached alt) must not poison the season so a later sibling can still
        recover once an alternative becomes cached."""
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt999', 'series', 2, 1))
        self._stub_search(monkeypatch, [self._candidate(
            self.CACHED_ALT, title='Show.S02.1080p.WEB.x264-GRP')])
        cache = {self.CACHED_ALT: False}  # uncached on first attempt
        import utils.search as search
        monkeypatch.setattr(search, 'check_debrid_cache',
                            lambda hashes, **k: {h: cache.get(h) for h in hashes})
        monkeypatch.setattr(search, 'remember_added_hash', lambda *a, **k: None)
        adds = []
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: adds.append(1) or (True, {}))
        fp1 = self._make_file(tmp_dir, 'Show.S02E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp1, os.path.basename(fp1), self.REJECTED, 'realdebrid') is False
        assert not adds
        # Now the alternative is cached — the season was NOT poisoned, so a
        # sibling episode recovers normally.
        cache[self.CACHED_ALT] = True
        fp2 = self._make_file(tmp_dir, 'Show.S02E02.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp2, os.path.basename(fp2), self.REJECTED, 'realdebrid') is True
        assert len(adds) == 1


class TestTorboxAltGiveUpCap(TestTorboxCachedAlternative):
    """Persistent give-up cap: after BLACKHOLE_TB_ALT_MAX_ATTEMPTS cached-alt
    grabs for one (imdb_id, season), decline so the title falls back to Wanted
    instead of re-arming TorBox's abuse cooldown on every .magnet re-drop."""

    @pytest.fixture(autouse=True)
    def _ledger(self, tmp_dir):
        import importlib
        from utils import attempt_ledger
        importlib.reload(attempt_ledger)
        attempt_ledger.init(config_dir=tmp_dir)
        self.ledger = attempt_ledger
        yield

    def test_successful_grab_bumps_ledger(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt777', 'series', 2, 1))
        self._stub_search(monkeypatch, [self._candidate(
            self.CACHED_ALT, title='Show.S02.1080p.WEB.x264-GRP')])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        monkeypatch.setattr(w, '_add_to_torbox', lambda *a, **k: (True, {}))
        fp = self._make_file(tmp_dir, 'Show.S02E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is True
        assert self.ledger.get('tbalt:tt777:s2') == 1

    def test_cap_declines_and_keeps_file(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        w._tb_alt_max_attempts = 2
        for _ in range(2):
            self.ledger.bump('tbalt:tt777:s2')
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt777', 'series', 2, 1))
        self._stub_search(monkeypatch, [self._candidate(
            self.CACHED_ALT, title='Show.S02.1080p.WEB.x264-GRP')])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        adds = []
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: adds.append(1) or (True, {}))
        fp = self._make_file(tmp_dir, 'Show.S02E03.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert not adds              # never probed/grabbed
        assert os.path.exists(fp)    # caller falls through to its own delete

    def test_dedup_key_for_movie_uses_none_season(self, tmp_dir, monkeypatch):
        """Movies key on (imdb_id, None).  A successful grab records the key,
        so a re-drop of the same movie is suppressed — and the None season
        never collides with a different movie's key."""
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        imdb = {'id': 'tt111'}
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: (imdb['id'], 'movie', None, None))
        alt_b = 'c' * 40
        self._stub_search(monkeypatch, [
            self._candidate(self.CACHED_ALT, title='Movie.A.1080p.WEB.x264-GRP'),
            self._candidate(alt_b, title='Movie.B.1080p.WEB.x264-GRP'),
        ])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True, alt_b: True})
        adds = []
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: adds.append(1) or (True, {}))
        fp1 = self._make_file(tmp_dir, 'Movie.A.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp1, os.path.basename(fp1), self.REJECTED, 'realdebrid') is True
        # Same movie re-dropped -> suppressed (already recovered).
        fp2 = self._make_file(tmp_dir, 'Movie.A.REPACK.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp2, os.path.basename(fp2), self.REJECTED, 'realdebrid') is False
        # A DIFFERENT movie (different imdb) is not collided by the None season.
        imdb['id'] = 'tt222'
        fp3 = self._make_file(tmp_dir, 'Movie.B.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp3, os.path.basename(fp3), self.REJECTED, 'realdebrid') is True
        assert len(adds) == 2

    def test_sibling_dedup_survives_restart(self, tmp_dir):
        """The 6h sibling-grab suppression must survive a container restart.

        Pre-fix it lived only in an in-memory dict, so a restart
        mid-backfill re-enabled sibling season-pack grabs — each redundant
        TB create being the volume event that arms TB Essential's cooldown.
        """
        import importlib
        from utils import attempt_ledger
        w = self._make_watcher(tmp_dir)
        w._remember_tb_alt_grab(('tt777', 2))

        # "Restart": reload the ledger from the same file, fresh watcher
        # (empty in-memory dedup dict).
        importlib.reload(attempt_ledger)
        attempt_ledger.init(config_dir=tmp_dir)
        w2 = self._make_watcher(tmp_dir)
        assert w2._tb_alt_recently_grabbed(('tt777', 2)) is True
        assert w2._tb_alt_recently_grabbed(('tt777', 3)) is False
        assert w2._tb_alt_recently_grabbed(('tt888', 2)) is False

    def test_sibling_dedup_ledger_entry_expires(self, tmp_dir):
        """A ledger-mirrored dedup entry older than the TTL does not suppress."""
        from datetime import datetime, timezone, timedelta
        from utils.blackhole import _TB_ALT_DEDUP_TTL
        w = self._make_watcher(tmp_dir)
        key = ('tt777', 2)
        w._remember_tb_alt_grab(key)
        with w._tb_alt_grabs_lock:
            w._tb_alt_recent_grabs.clear()  # drop the in-memory fast path
        old = (datetime.now(timezone.utc)
               - timedelta(seconds=_TB_ALT_DEDUP_TTL + 60)).isoformat(timespec='seconds')
        self.ledger._state['tbaltdedup:tt777:s2']['last_ts'] = old
        assert w._tb_alt_recently_grabbed(key) is False

class TestTorboxAltRetryLoopBreaker(TestTorboxCachedAlternative):
    """Failed-import loop breaker: a cached alternative that was already
    grabbed (or blocklisted) must never be re-grabbed for the same
    (imdb_id, season).  Pre-fix, an alt that failed to import (mislabeled
    payload, no symlink) was re-grabbed every ~6h — the sibling dedup TTL
    expiring in step with the arr's search cycle — burning a TorBox create
    and re-arming the abuse cooldown each time."""

    @pytest.fixture(autouse=True)
    def _ledger(self, tmp_dir):
        import importlib
        from utils import attempt_ledger
        importlib.reload(attempt_ledger)
        attempt_ledger.init(config_dir=tmp_dir)
        self.ledger = attempt_ledger
        yield

    def test_blocklisted_candidate_excluded(self, tmp_dir, monkeypatch):
        """A blocklisted hash must not be selectable as a cached alternative
        (parity with the legacy alt-retry path and the main grab gate)."""
        import utils.blackhole as bh
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt1234567', 'movie', None, None))
        self._stub_search(monkeypatch, [self._candidate(self.CACHED_ALT)])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        seen = []
        stub = type('B', (), {'is_blocked': staticmethod(
            lambda h: seen.append(h) or h.lower() == self.CACHED_ALT)})()
        monkeypatch.setattr(bh, '_blocklist', stub)
        adds = []
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: adds.append(1) or (True, {}))
        fp = self._make_file(tmp_dir, 'Sing.2.1080p.WEB.x264-CYBER.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is False
        assert not adds
        assert os.path.exists(fp)
        assert seen  # the blocklist was actually consulted

    def test_grab_records_tried_hash_in_ledger(self, tmp_dir, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt777', 'series', 2, 1))
        self._stub_search(monkeypatch, [self._candidate(
            self.CACHED_ALT, title='Show.S02.1080p.WEB.x264-GRP')])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        monkeypatch.setattr(w, '_add_to_torbox', lambda *a, **k: (True, {}))
        fp = self._make_file(tmp_dir, 'Show.S02E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp, os.path.basename(fp), self.REJECTED, 'realdebrid') is True
        assert self.ledger.get(f'tbalttried:tt777:s2:{self.CACHED_ALT}') == 1

    def test_tried_alt_not_regrabbed_next_cycle(self, tmp_dir, monkeypatch):
        """Second recovery cycle for the same (imdb, season) — i.e. the first
        alt failed to import and the arr searched again after the dedup TTL —
        must pick a DIFFERENT candidate, and give up once all are tried."""
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt777', 'series', 2, 1))
        alt_b = 'c' * 40
        self._stub_search(monkeypatch, [
            self._candidate(self.CACHED_ALT, seeds=50,
                            title='Show.S02.1080p.WEB.x264-GRP'),
            self._candidate(alt_b, seeds=5,
                            title='Show.S02.1080p.WEB.x264-OTHER'),
        ])
        self._stub_cache(monkeypatch,
                         {self.CACHED_ALT: True, alt_b: True})
        added = []
        def fake_add(path, api_key=None):
            with open(path) as f:
                added.append(f.read())
            return True, {}
        monkeypatch.setattr(w, '_add_to_torbox', fake_add)

        # Cycle 1: grabs the best-seeded candidate.
        fp1 = self._make_file(tmp_dir, 'Show.S02E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp1, os.path.basename(fp1), self.REJECTED, 'realdebrid') is True
        assert self.CACHED_ALT in added[0]
        # Original removed by the grab — the later cycles' _make_file calls
        # reuse this name, so a leaked file would silently mask cycle 3's
        # file-kept assertion.
        assert not os.path.exists(fp1)

        # The 6h sibling dedup would suppress the next call outright; the
        # real loop fires only after it expires, so neutralize it here to
        # isolate the tried-hash memory.
        monkeypatch.setattr(w, '_tb_alt_recently_grabbed', lambda k: False)

        # Cycle 2: the tried hash is excluded -> the OTHER candidate wins.
        fp2 = self._make_file(tmp_dir, 'Show.S02E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp2, os.path.basename(fp2), self.REJECTED, 'realdebrid') is True
        assert alt_b in added[1]

        # Cycle 3: every candidate tried -> decline, no TorBox create.
        fp3 = self._make_file(tmp_dir, 'Show.S02E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp3, os.path.basename(fp3), self.REJECTED, 'realdebrid') is False
        assert len(added) == 2
        assert os.path.exists(fp3)

    def test_tried_hash_is_season_scoped(self, tmp_dir, monkeypatch):
        """A pack tried for S02 must not be barred for S03 of the same show
        (and vice versa) — the key is (imdb, season, hash)."""
        monkeypatch.setenv('BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')
        w = self._make_watcher(tmp_dir)
        ident = {'season': 2}
        monkeypatch.setattr(w, '_resolve_arr_identity',
                            lambda fn: ('tt777', 'series', ident['season'], 1))
        self._stub_search(monkeypatch, [self._candidate(
            self.CACHED_ALT, title='Show.1080p.WEB.x264-GRP')])
        self._stub_cache(monkeypatch, {self.CACHED_ALT: True})
        adds = []
        monkeypatch.setattr(w, '_add_to_torbox',
                            lambda *a, **k: adds.append(1) or (True, {}))
        fp1 = self._make_file(tmp_dir, 'Show.S02E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp1, os.path.basename(fp1), self.REJECTED, 'realdebrid') is True
        ident['season'] = 3
        fp2 = self._make_file(tmp_dir, 'Show.S03E01.1080p.WEB.x264-GRP.mkv.magnet')
        assert w._try_torbox_cached_alternative(
            fp2, os.path.basename(fp2), self.REJECTED, 'realdebrid') is True
        assert len(adds) == 2
