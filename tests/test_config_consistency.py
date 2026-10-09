"""Guard: every table the pipeline can build must have merge keys registered.

silver_sb_matches was once missing from config.MERGE_KEYS. It still ran, because
each Silver builder returns its own keys, but the generated data dictionary then
showed that table with no primary key - which the spec explicitly requires.
This test makes that mismatch fail loudly instead of hiding in the docs.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.common import config
from pipelines.bronze.raw_to_bronze import TARGET_TABLE
from pipelines.silver.bronze_to_silver import BUILDERS


def main():
    expected = set(TARGET_TABLE.values()) | set(BUILDERS)
    registered = set(config.MERGE_KEYS)
    missing = expected - registered
    extra = registered - expected

    print(f"tables the pipeline builds : {len(expected)}")
    print(f"tables with merge keys     : {len(registered)}")
    if missing:
        print(f"MISSING merge keys for     : {sorted(missing)}")
    if extra:
        print(f"merge keys with no builder : {sorted(extra)}")

    empty = [t for t, k in config.MERGE_KEYS.items() if not k]
    if empty:
        print(f"EMPTY merge keys           : {empty}")

    ok = not missing and not extra and not empty
    print("CONFIG CONSISTENCY:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
