"""
Business logic automations — all computed server-side, never on the frontend.
These replace spreadsheet formula logic.
"""
import re
import os
import json
import sqlite3
from datetime import datetime, timedelta
from database import db, get_connection


# ============================================================
# 1. CUSTOMER ID AUTO-GENERATION
# ============================================================

def generate_customer_id(full_name: str, year_acquired: int, conn=None) -> str:
    """
    Generate customer ID from name + year.
    'Bob and Barb Smith', 2024 → 'BSm24'
    Handles duplicates by appending _2, _3, etc.
    """
    if not full_name:
        return ""
    year_suffix = str(year_acquired)[-2:] if year_acquired else str(datetime.now().year)[-2:]
    parts = full_name.strip().split()
    # Extract meaningful name parts (skip "and", "&", etc.)
    skip_words = {'and', '&', 'the', 'or'}
    name_parts = [p for p in parts if p.lower() not in skip_words]
    if not name_parts:
        return f"XX{year_suffix}"
    first_name_initial = name_parts[0][0].upper()
    # Last meaningful word = last name
    last_name = name_parts[-1]
    last_initial = last_name[0].upper()
    last_two = last_name[1:3] if len(last_name) > 1 else ''
    base_id = f"{first_name_initial}{last_initial}{last_two[:1].lower()}{year_suffix}"
    # Check uniqueness
    close = close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        existing = conn.execute(
            "SELECT customer_id FROM clients WHERE customer_id LIKE ? AND is_deleted=0",
            (base_id + '%',)
        ).fetchall()
        existing_ids = {row['customer_id'] for row in existing}
        if base_id not in existing_ids:
            return base_id
        counter = 2
        while f"{base_id}_{counter}" in existing_ids:
            counter += 1
        return f"{base_id}_{counter}"
    finally:
        if close_conn:
            conn.close()


def extract_last_name(full_name: str) -> str:
    """Extract last name from full name string."""
    if not full_name:
        return ""
    skip = {'and', '&', 'the'}
    parts = [p for p in full_name.strip().split() if p.lower() not in skip]
    return parts[-1] if parts else ""


# ============================================================
# 2. RATE LOOKUP (critical — store at entry time)
# ============================================================

def get_rate_for_date(emp_id: int, entry_date: str, conn=None) -> dict:
    """
    Find the most recent rate effective on or before entry_date.
    Returns dict with bill_rate_per_hour, cost_rate_per_hour.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        row = conn.execute("""
            SELECT bill_rate_per_hour, cost_rate_per_hour
            FROM employee_rates
            WHERE emp_id = ?
              AND effective_date <= ?
              AND is_deleted = 0
            ORDER BY effective_date DESC
            LIMIT 1
        """, [emp_id, entry_date]).fetchone()
        if row:
            return {
                'bill_rate': row['bill_rate_per_hour'],
                'cost_rate': row['cost_rate_per_hour']
            }
        return {'bill_rate': 0.0, 'cost_rate': 0.0}
    finally:
        if close_conn:
            conn.close()


def get_person_label(emp_id: int, conn=None) -> str:
    """Return 'FirstName LastName' for an employee."""
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        row = conn.execute(
            "SELECT first_name, last_name FROM employees WHERE emp_id=? AND is_deleted=0",
            [emp_id]
        ).fetchone()
        if row:
            return f"{row['first_name']} {row['last_name']}"
        return ""
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 3. RECEIPT FILENAME AUTO-GENERATION
# ============================================================

def generate_receipt_filename(entry_date: str, job_code: str, vendor: str, amount: float) -> str:
    """
    Auto-generate a receipt filename.
    Format: YYYY-MM-DD.JobCode.VendorName.Amount.pdf
    """
    if not entry_date or not vendor:
        return ""
    try:
        d = datetime.strptime(entry_date, '%Y-%m-%d') if isinstance(entry_date, str) else entry_date
        date_str = d.strftime('%Y-%m-%d')
    except Exception:
        date_str = str(entry_date)[:10]
    vendor_clean = re.sub(r'[^a-zA-Z0-9]', '', vendor)[:20]
    job_clean = re.sub(r'[^a-zA-Z0-9\-]', '', str(job_code))[:20]
    amount_str = f"{abs(float(amount)):.2f}"  # always positive in filename; 44.00 not -44.00
    return f"{date_str}.{job_clean}.{vendor_clean}.{amount_str}.pdf"


# ============================================================
# 4. RECEIPT EXISTENCE VERIFICATION
# ============================================================

def verify_receipt(receipt_filename: str, receipts_folder: str) -> bool:
    """Check if receipt file exists on disk.
    Tries exact match first, then prefix match (for files stored without extension).
    """
    if not receipt_filename or not receipts_folder:
        return False
    folder = receipts_folder.replace('/', os.sep)
    # Exact match
    path = os.path.join(folder, receipt_filename)
    if os.path.isfile(path):
        return True
    # Prefix match — file may have .pdf/.jpg/.png extension added
    base = receipt_filename.replace('/', '').replace('\\', '')
    try:
        if os.path.isdir(folder):
            for f in os.listdir(folder):
                name_no_ext = os.path.splitext(f)[0]
                if name_no_ext == base or f == base or f.startswith(base + '.'):
                    return True
    except Exception:
        pass
    return False


def verify_all_receipts(conn=None):
    """Run receipt verification across all ledger entries."""
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        cfg = conn.execute("SELECT receipts_folder_path FROM company_config WHERE id=1").fetchone()
        folder = cfg['receipts_folder_path'] if cfg else ''
        rows = conn.execute(
            "SELECT id, receipt_filename FROM ledger WHERE receipt_filename != '' AND is_deleted=0"
        ).fetchall()
        for row in rows:
            verified = verify_receipt(row['receipt_filename'], folder)
            conn.execute(
                "UPDATE ledger SET receipt_verified=?, updated_at=datetime('now') WHERE id=?",
                [1 if verified else 0, row['id']]
            )
        conn.commit()
        return len(rows)
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 5 & 6. INVOICE CALCULATIONS
# ============================================================

def compute_invoice_dates_and_balance(invoice_date: str, amount: float, amount_paid: float) -> dict:
    """Compute due_date and balance_due for an invoice."""
    try:
        d = datetime.strptime(invoice_date, '%Y-%m-%d')
        due = d + timedelta(days=30)
        due_str = due.strftime('%Y-%m-%d')
    except Exception:
        due_str = ''
    balance = round(float(amount) - float(amount_paid), 2)
    return {'due_date': due_str, 'balance_due': balance}


def update_invoice_status(invoice_id: int, conn=None):
    """Recompute and update invoice status based on balance and dates."""
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        inv = conn.execute(
            "SELECT amount, amount_paid, due_date FROM invoices WHERE id=? AND is_deleted=0",
            [invoice_id]
        ).fetchone()
        if not inv:
            return
        balance = round(float(inv['amount']) - float(inv['amount_paid']), 2)
        today = datetime.now().strftime('%Y-%m-%d')
        if balance <= 0:
            status = 'Paid'
        elif inv['amount_paid'] > 0:
            status = 'Partial' if inv['due_date'] >= today else 'Overdue'
        elif inv['due_date'] and inv['due_date'] < today:
            status = 'Overdue'
        else:
            status = 'Pending'
        conn.execute(
            "UPDATE invoices SET balance_due=?, status=?, updated_at=datetime('now') WHERE id=?",
            [balance, status, invoice_id]
        )
        conn.commit()
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 7 & 8. PAYROLL TAX CALCULATIONS
# ============================================================

def get_ss_wage_cap(year: int, conn=None) -> float:
    """Look up SS wage cap for a given year."""
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        row = conn.execute(
            "SELECT cap_amount FROM ss_wage_caps WHERE year=? AND is_deleted=0",
            [year]
        ).fetchone()
        return row['cap_amount'] if row else 160200.0
    finally:
        if close_conn:
            conn.close()


def get_ytd_wages(emp_id: int, year: int, exclude_run_id: int = None, conn=None) -> float:
    """Get YTD gross wages for an employee in a year (for wage base tracking)."""
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        query = """
            SELECT COALESCE(SUM(gross_pay), 0) as ytd
            FROM payroll_runs
            WHERE emp_id=? AND year=? AND is_deleted=0
        """
        params = [emp_id, year]
        if exclude_run_id:
            query += " AND id != ?"
            params.append(exclude_run_id)
        row = conn.execute(query, params).fetchone()
        return float(row['ytd'])
    finally:
        if close_conn:
            conn.close()


def calculate_payroll_taxes(emp_id: int, year: int, gross_pay: float,
                             config: dict = None, conn=None) -> dict:
    """
    Calculate all payroll taxes for a given gross pay amount.
    Returns dict with all withheld/employer amounts.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        if config is None:
            cfg = conn.execute("SELECT * FROM company_config WHERE id=1").fetchone()
            config = dict(cfg) if cfg else {}
        fica_emp = config.get('fica_rate_employee', 0.062)
        fica_er = config.get('fica_rate_employer', 0.062)
        med_emp = config.get('medicare_rate_employee', 0.0145)
        med_er = config.get('medicare_rate_employer', 0.0145)
        futa_rate = config.get('futa_rate', 0.006)
        futa_base = config.get('futa_wage_base', 7000.0)
        suta_rate = config.get('suta_rate_il', 0.035)
        suta_base = config.get('suta_wage_base_il', 13590.0)
        ss_cap = get_ss_wage_cap(year, conn)
        ytd = get_ytd_wages(emp_id, year, conn=conn)
        # SS — apply wage cap
        ss_room = max(0.0, ss_cap - ytd)
        ss_wages = min(gross_pay, ss_room)
        ss_withheld = round(ss_wages * fica_emp, 2)
        employer_ss = round(ss_wages * fica_er, 2)
        # Medicare (no cap)
        medicare_withheld = round(gross_pay * med_emp, 2)
        employer_medicare = round(gross_pay * med_er, 2)
        # FUTA
        futa_room = max(0.0, futa_base - ytd)
        futa_wages = min(gross_pay, futa_room)
        futa_amount = round(futa_wages * futa_rate, 2)
        # SUTA (IL)
        suta_room = max(0.0, suta_base - ytd)
        suta_wages = min(gross_pay, suta_room)
        suta_amount = round(suta_wages * suta_rate, 2)
        return {
            'ss_withheld': ss_withheld,
            'medicare_withheld': medicare_withheld,
            'employer_ss': employer_ss,
            'employer_medicare': employer_medicare,
            'futa_amount': futa_amount,
            'suta_amount': suta_amount,
            'ss_wage_cap': ss_cap,
            'ytd_before': ytd,
            'ss_wages_applied': ss_wages,
        }
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 9. VENDOR CATEGORY AUTO-FILL
# ============================================================

def get_vendor_category(vendor_name: str, conn=None) -> str:
    """Look up default category for a vendor."""
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        if not vendor_name:
            return ""
        row = conn.execute(
            "SELECT default_category FROM vendor_categories WHERE vendor_name=? AND is_deleted=0",
            [vendor_name]
        ).fetchone()
        if row:
            return row['default_category']
        # Fuzzy fallback — case-insensitive contains
        row = conn.execute(
            "SELECT default_category FROM vendor_categories WHERE LOWER(vendor_name) LIKE ? AND is_deleted=0 LIMIT 1",
            [f"%{vendor_name.lower()[:10]}%"]
        ).fetchone()
        return row['default_category'] if row else ""
    finally:
        if close_conn:
            conn.close()


def save_vendor_category(vendor_name: str, category: str, conn=None):
    """Upsert vendor→category mapping."""
    if not vendor_name or not category:
        return
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        conn.execute("""
            INSERT INTO vendor_categories (vendor_name, default_category)
            VALUES (?, ?)
            ON CONFLICT(vendor_name) DO UPDATE SET
                default_category=excluded.default_category,
                updated_at=datetime('now')
        """, [vendor_name, category])
        conn.commit()
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 13. CERT EXPIRATION STATUS
# ============================================================

def get_cert_status(end_date_str: str) -> dict:
    """Return cert expiration status dict."""
    if not end_date_str:
        return {'status': 'Unknown', 'color': 'gray', 'days': None}
    try:
        end = datetime.strptime(end_date_str, '%Y-%m-%d')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        days = (end - today).days
        if days < -365:
            return {'status': 'Archived', 'color': 'gray', 'days': days}
        elif days < 0:
            return {'status': 'Expired', 'color': 'red', 'days': days}
        elif days <= 60:
            return {'status': f'Expiring in {days}d', 'color': 'yellow', 'days': days}
        else:
            return {'status': 'Current', 'color': 'green', 'days': days}
    except Exception:
        return {'status': 'Unknown', 'color': 'gray', 'days': None}


def parse_cert_filename(filename: str) -> dict:
    """
    Parse cert filename in format: 'CompanyName MM-DD-YY_MM-DD-YY' or
    'CompanyName M-D-YYYY_M-D-YYYY'. Handles 1-2 digit months/days and
    2 or 4 digit years.
    Returns dict with company_name, start_date, end_date.
    """
    base = os.path.splitext(filename)[0]
    # Match 1-2 digit month, day, and 2 OR 4 digit year
    pattern = r'^(.+?)\s+(\d{1,2}-\d{1,2}-\d{2,4})_(\d{1,2}-\d{1,2}-\d{2,4})$'
    m = re.match(pattern, base)
    if m:
        company = m.group(1).strip()
        def parse_date(s):
            parts = s.split('-')
            if len(parts) != 3:
                return ''
            mm, dd, yy = parts
            try:
                yr = int(yy)
                if yr < 100:          # 2-digit year
                    yr += 2000 if yr < 50 else 1900
                return f'{yr:04d}-{int(mm):02d}-{int(dd):02d}'
            except (ValueError, TypeError):
                return ''
        return {
            'company_name': company,
            'start_date': parse_date(m.group(2)),
            'end_date': parse_date(m.group(3))
        }
    return {'company_name': base, 'start_date': '', 'end_date': ''}


# ============================================================
# QUICK QUOTE CALCULATOR
# ============================================================

def calculate_quick_quote(total_amount: float, overhead_pct: float,
                           insurance_pct: float, owner_wages_pct: float,
                           profit_pct: float) -> dict:
    """
    Back-calculate direct costs from total + percentages.
    All percentages expressed as 0–100.
    """
    total_pct = (overhead_pct + insurance_pct + owner_wages_pct + profit_pct) / 100.0
    if total_pct >= 1.0:
        return {'error': 'Percentages cannot total 100% or more'}
    direct_costs = total_amount * (1.0 - total_pct)
    return {
        'total': round(total_amount, 2),
        'direct_costs': round(direct_costs, 2),
        'overhead': round(total_amount * overhead_pct / 100, 2),
        'insurance': round(total_amount * insurance_pct / 100, 2),
        'owner_wages': round(total_amount * owner_wages_pct / 100, 2),
        'profit': round(total_amount * profit_pct / 100, 2),
        'overhead_pct': overhead_pct,
        'insurance_pct': insurance_pct,
        'owner_wages_pct': owner_wages_pct,
        'profit_pct': profit_pct,
    }


# ============================================================
# REMINDER STATUS
# ============================================================

def get_reminder_status(due_date_str: str, status: str) -> dict:
    """Return display color/badge for a reminder."""
    if status == 'Done':
        return {'color': 'green', 'label': 'Done'}
    if not due_date_str:
        return {'color': 'gray', 'label': 'Pending'}
    try:
        due = datetime.strptime(due_date_str, '%Y-%m-%d')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        days = (due - today).days
        if days < 0:
            return {'color': 'red', 'label': f'Overdue {abs(days)}d'}
        elif days <= 7:
            return {'color': 'orange', 'label': f'Due in {days}d'}
        else:
            return {'color': 'gray', 'label': f'Due {due.strftime("%b %d")}'}
    except Exception:
        return {'color': 'gray', 'label': 'Pending'}


# ============================================================
# UNDO LOG
# ============================================================

def log_action(conn, table: str, record_id: int, action: str,
               old_data: dict = None, new_data: dict = None,
               session_id: str = '', user_label: str = '', field_name: str = ''):
    """Record an action in the undo log with optional session tracking."""
    # Auto-generate user_label if not provided
    if not user_label:
        nd = new_data or {}
        od = old_data or {}
        if action == 'INSERT':
            vendor = nd.get('vendor') or nd.get('name') or nd.get('description', '')
            amt    = nd.get('amount')
            user_label = f"Added {vendor}" + (f" ${abs(float(amt)):.2f}" if amt is not None else '')
        elif action == 'DELETE':
            vendor = od.get('vendor') or od.get('name') or od.get('description', '')
            user_label = f"Deleted {vendor}" if vendor else f"Deleted #{record_id}"
        elif action == 'UPDATE':
            if field_name:
                old_v = od.get(field_name, '')
                new_v = nd.get(field_name, '')
                # Truncate long values
                def trunc(v): return str(v)[:30] + ('…' if len(str(v)) > 30 else '')
                user_label = f"{field_name}: {trunc(old_v)} → {trunc(new_v)}"
            else:
                changed = [k for k in nd if k in od and str(nd[k]) != str(od.get(k,''))]
                user_label = 'Updated ' + (', '.join(changed[:3]) or table)
    conn.execute("""
        INSERT INTO undo_log
            (table_name, record_id, action, old_data, new_data,
             session_id, user_label, field_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        table, record_id, action,
        json.dumps(old_data or {}),
        json.dumps(new_data or {}),
        session_id or '', user_label or '', field_name or ''
    ])


def soft_delete(conn, table: str, record_id: int):
    """Soft-delete a record and log it."""
    row = conn.execute(f"SELECT * FROM {table} WHERE id=?", [record_id]).fetchone()
    if row:
        old = dict(row)
        conn.execute(
            f"UPDATE {table} SET is_deleted=1, updated_at=datetime('now') WHERE id=?",
            [record_id]
        )
        log_action(conn, table, record_id, 'DELETE', old_data=old)


# ============================================================
# DUPLICATE DETECTION
# ============================================================

def check_duplicate_client(full_name: str, conn=None) -> list:
    """Check for potential duplicate clients."""
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        rows = conn.execute("""
            SELECT id, full_name, customer_id FROM clients
            WHERE LOWER(full_name) LIKE ? AND is_deleted=0 LIMIT 5
        """, [f"%{full_name.lower()[:20]}%"]).fetchall()
        return [dict(r) for r in rows]
    finally:
        if close_conn:
            conn.close()
