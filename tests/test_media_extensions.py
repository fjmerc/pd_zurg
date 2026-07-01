"""Guard against drift between the three MEDIA_EXTENSIONS definitions.

CLAUDE.md: the sets in library.py, blackhole.py and scheduled_tasks.py MUST
be identical — a mismatch makes files visible to symlink creation but
invisible to verification (or vice versa). This test is the enforcement.
"""


def test_media_extensions_definitions_are_identical():
    from utils.library import MEDIA_EXTENSIONS as lib_exts
    from utils.blackhole import MEDIA_EXTENSIONS as bh_exts
    from utils.scheduled_tasks import MEDIA_EXTENSIONS as st_exts

    assert lib_exts == bh_exts, (
        "library.py and blackhole.py MEDIA_EXTENSIONS diverged: "
        f"{sorted(set(lib_exts) ^ set(bh_exts))}"
    )
    assert lib_exts == st_exts, (
        "library.py and scheduled_tasks.py MEDIA_EXTENSIONS diverged: "
        f"{sorted(set(lib_exts) ^ set(st_exts))}"
    )
