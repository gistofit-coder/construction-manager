r"""
One-time migration: adds is_transfer column to work_categories,
flags transfer/equity categories, and UNFLAG any that shouldn't be transfers.

Run this ONCE against your existing database before starting the app:
    python migrate_db.py

Or specify your db path:
    python migrate_db.py C:\path\to\your.db
"""
import sqlite3
import sys
import os

# ── Locate the database ───────────────────────────────────────────────────────
if len(sys.argv) > 1:
    db_path = sys.argv[1]
else:
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'construction.db')

if not os.path.exists(db_path):
    print(f"ERROR: Database not found at: {db_path}")
    print("Usage: python migrate_db.py [path/to/your.db]")
    sys.exit(1)

print(f"Migrating: {db_path}")

# Categories that ARE transfers (excluded from P&L income/expense totals)
TRANSFER_CATEGORIES = (
    'Credit',
    'Credit Card Payment',
    'Distribution',
    'Distributions',
    'Contribution',
    'Loan Proceeds',
    'Owner Draw',
    'Previous',
    'KB',
    'Memo',
)

# Categories that should explicitly NOT be transfers
# (corrects any previous mis-flagging)
NOT_TRANSFER_CATEGORIES = (
    'WRITE OFF',
    'Bad Debt / Write-Off',
    'Income Received',
    'ACCOUNT CREDIT',
)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

try:
    # ── 1. Add column if missing ──────────────────────────────────────────────
    existing_cols = [row[1] for row in conn.execute("PRAGMA table_info(work_categories)").fetchall()]
    if 'is_transfer' in existing_cols:
        print("  ✓ is_transfer column already exists")
    else:
        conn.execute("ALTER TABLE work_categories ADD COLUMN is_transfer INTEGER DEFAULT 0")
        print("  ✓ Added is_transfer column")

    # ── 2. Flag transfer categories ───────────────────────────────────────────
    ph = ','.join('?' * len(TRANSFER_CATEGORIES))
    r = conn.execute(
        f"UPDATE work_categories SET is_transfer=1 WHERE category_name IN ({ph})",
        list(TRANSFER_CATEGORIES)
    )
    print(f"  ✓ Flagged {r.rowcount} categories as transfers")

    # ── 3. Un-flag any that were incorrectly set ──────────────────────────────
    ph2 = ','.join('?' * len(NOT_TRANSFER_CATEGORIES))
    r2 = conn.execute(
        f"UPDATE work_categories SET is_transfer=0 WHERE category_name IN ({ph2})",
        list(NOT_TRANSFER_CATEGORIES)
    )
    if r2.rowcount:
        print(f"  ✓ Corrected {r2.rowcount} categories (un-flagged from transfer)")

    # ── 4. Insert any missing transfer categories ─────────────────────────────
    for cat in TRANSFER_CATEGORIES:
        conn.execute("""
            INSERT OR IGNORE INTO work_categories
                (category_name, is_cogs, is_tax_deductible, is_transfer)
            VALUES (?, 0, 0, 1)
        """, [cat])

    # ── Also add is_pending column if missing ────────────────────────────────
    existing_ledger = [row[1] for row in conn.execute("PRAGMA table_info(ledger)").fetchall()]
    if 'is_pending' in existing_ledger:
        print("  ✓ is_pending column already exists on ledger")
    else:
        conn.execute("ALTER TABLE ledger ADD COLUMN is_pending INTEGER DEFAULT 0")
        print("  ✓ Added is_pending column to ledger")

    # ── Flag existing rows imported with pending date ─────────────────────────
    # Rows with status=Pending and entry_date=today were likely imported from
    # a CSV with "Pending" in the date column — backfill is_pending=1
    from datetime import date as _date
    today_str = _date.today().isoformat()
    r3 = conn.execute(
        "UPDATE ledger SET is_pending=1 WHERE status='Pending' AND entry_date=? AND is_deleted=0 AND is_pending=0",
        [today_str]
    )
    if r3.rowcount:
        print(f"  ✓ Flagged {r3.rowcount} existing rows as pending (status=Pending, date=today)")

    conn.commit()

    # ── 5. Report final state ─────────────────────────────────────────────────
    flagged = conn.execute(
        "SELECT category_name FROM work_categories WHERE is_transfer=1 ORDER BY category_name"
    ).fetchall()
    print(f"\n  Categories flagged as transfers (excluded from P&L):")
    for row in flagged:
        print(f"    • {row['category_name']}")

    # Confirm WRITE OFF is NOT flagged
    wo = conn.execute(
        "SELECT is_transfer FROM work_categories WHERE category_name='WRITE OFF'"
    ).fetchone()
    if wo:
        status = "NOT flagged ✓" if wo['is_transfer'] == 0 else "FLAGGED (unexpected)"
        print(f"\n  WRITE OFF: {status}")

    print("\n✅ Migration complete. You can now start the app normally.")

except Exception as e:
    conn.rollback()
    print(f"\n❌ Migration failed: {e}")
    sys.exit(1)
finally:
    conn.close()
