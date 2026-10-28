"""Back up the warehouse and damage it on purpose (`break`); put the backup back (`restore`)."""

import os
import shutil
import sys

from pipeline import db

BACKUP_PATH = db.WAREHOUSE_PATH + ".bak"
WAL_PATH = db.WAREHOUSE_PATH + ".wal"


def break_warehouse() -> int:
    """Damage raw_transactions the three ways the checks must catch: lost rows, a ghost row, a silent edit."""
    if os.path.exists(BACKUP_PATH):
        print(f"{BACKUP_PATH} already exists: run `make restore` first", file=sys.stderr)
        return 2
    if not os.path.exists(db.WAREHOUSE_PATH):
        print(f"{db.WAREHOUSE_PATH} not found: run `make load` first", file=sys.stderr)
        return 2
    wh = db.warehouse()
    # Flush the WAL into the file, so the copy holds every committed change.
    wh.execute("CHECKPOINT")
    wh.close()
    shutil.copy2(db.WAREHOUSE_PATH, BACKUP_PATH)

    wh = db.warehouse()
    active = [r[0] for r in wh.execute(
        "SELECT transaction_id FROM raw_transactions WHERE _deleted_at IS NULL ORDER BY transaction_id").fetchall()]
    oldest, changed, fake_id = active[:3], active[-1], active[-1] + 1_000_000
    wh.execute("BEGIN")
    wh.execute("DELETE FROM raw_transactions WHERE transaction_id IN (SELECT UNNEST(?))", [oldest])
    wh.execute("INSERT INTO raw_transactions SELECT * REPLACE (? AS transaction_id) FROM raw_transactions "
               "WHERE transaction_id = ?", [fake_id, changed])
    wh.execute("UPDATE raw_transactions SET amount = amount + 1 WHERE transaction_id = ?", [changed])
    wh.execute("COMMIT")
    wh.close()
    print(f"break: deleted transactions {oldest}, inserted fake {fake_id}, amount + 1 on {changed}; backup at {BACKUP_PATH}")
    return 0


def restore_warehouse() -> int:
    if not os.path.exists(BACKUP_PATH):
        print(f"{BACKUP_PATH} not found: nothing to restore", file=sys.stderr)
        return 2
    # A WAL left by the damaged file would be replayed onto the restored one.
    if os.path.exists(WAL_PATH):
        os.remove(WAL_PATH)
    os.replace(BACKUP_PATH, db.WAREHOUSE_PATH)
    print(f"restore: {db.WAREHOUSE_PATH} restored from backup")
    return 0


if __name__ == "__main__":
    actions = {"break": break_warehouse, "restore": restore_warehouse}
    if len(sys.argv) != 2 or sys.argv[1] not in actions:
        sys.exit("usage: python -m pipeline.break_restore break|restore")
    sys.exit(actions[sys.argv[1]]())
