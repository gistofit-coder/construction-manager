"""
Construction Business Management App — Phase 1 + 2
Flask application: Settings, Config, Migration, Dashboard, Core Data (Clients/Employees/Contractors/Jobs)
"""
import os
import sys
import json
import uuid
import tempfile
import re
import shutil
import zipfile
import glob
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, send_file, abort, Response, stream_with_context

from database import init_db, db, get_connection, DB_PATH
from automations import (
    generate_customer_id, extract_last_name,
    get_rate_for_date, get_person_label,
    generate_receipt_filename, verify_receipt, verify_all_receipts,
    compute_invoice_dates_and_balance, update_invoice_status,
    get_ss_wage_cap, calculate_payroll_taxes,
    get_vendor_category, save_vendor_category,
    get_cert_status, parse_cert_filename,
    calculate_quick_quote, get_reminder_status,
    log_action, soft_delete, check_duplicate_client
)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'construction-app-dev-key-change-in-prod')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB — large enough for any CSV import
app.jinja_env.globals['now'] = datetime.now

# Phone number Jinja2 filters
import re as _re

def _digits_only(phone):
    return _re.sub(r'\D', '', str(phone or ''))

def _fmt_phone(phone):
    digits = _re.sub(r'\D', '', str(phone or ''))
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f'({digits[:3]}) {digits[3:6]}-{digits[6:]}'
    return digits or str(phone or '')

app.jinja_env.filters['fmt_phone']   = _fmt_phone
app.jinja_env.filters['digits_only'] = _digits_only

@app.context_processor
def inject_globals():
    return {'db_name': os.path.basename(DB_PATH)}

# Register Phase 2 blueprint
from routes_phase2 import phase2
app.register_blueprint(phase2)

# Register Phase 3 blueprint
from routes_phase3 import phase3
app.register_blueprint(phase3)

# Register Phase 4 blueprint
from routes_phase4 import phase4
app.register_blueprint(phase4)

# Register Phase 5 blueprint
from routes_phase5 import phase5
app.register_blueprint(phase5)

# Register Phase 6 blueprint
from routes_phase6 import phase6
app.register_blueprint(phase6)

# Register Phase 7 blueprint
from routes_phase7 import phase7
app.register_blueprint(phase7)

# Register Phase 8 blueprint
from routes_phase8 import phase8
app.register_blueprint(phase8)

# Register Phase 9 blueprint
from routes_phase9 import phase9
app.register_blueprint(phase9)

# Register Phase 10 blueprint
from routes_phase10 import phase10
app.register_blueprint(phase10)

# Register Phase 11 blueprint
from routes_phase11 import phase11
app.register_blueprint(phase11)


def get_config():
    """Fetch company config as dict."""
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM company_config WHERE id=1").fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def get_nav_badges():
    """Get badge counts for sidebar navigation."""
    conn = get_connection()
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        unmatched = conn.execute(
            "SELECT COUNT(*) FROM bank_transactions WHERE match_status='Unmatched' AND is_deleted=0"
        ).fetchone()[0]
        overdue_reminders = conn.execute(
            "SELECT COUNT(*) FROM reminders WHERE due_date < ? AND status='Pending' AND is_deleted=0",
            [today]
        ).fetchone()[0]
        expiring_certs = conn.execute(
            "SELECT COUNT(*) FROM certificates WHERE end_date <= date(?, '+60 days') AND end_date >= ? AND is_deleted=0",
            [today, today]
        ).fetchone()[0]
        expired_certs = conn.execute(
            "SELECT COUNT(*) FROM certificates WHERE end_date < ? AND is_deleted=0",
            [today]
        ).fetchone()[0]
        # Badge counts EXACT duplicates only (near-matches are informational)
        try:
            from routes_phase3 import _find_duplicate_groups as _fdg
            duplicate_groups = sum(1 for g in _fdg(conn) if g['match_type'] == 'exact')
        except Exception:
            duplicate_groups = 0

        # Unverified receipts
        unverified_receipts = conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE receipt_filename != '' AND receipt_verified=0 AND is_deleted=0"
        ).fetchone()[0]

        return {
            'unmatched':          unmatched,
            'overdue_reminders':  overdue_reminders,
            'expiring_certs':     expiring_certs + expired_certs,
            'duplicate_groups':   duplicate_groups,
            'unverified_receipts': unverified_receipts,
        }
    finally:
        conn.close()


# ============================================================
# DASHBOARD
# ============================================================

@app.route('/')
def dashboard():
    config = get_config()
    badges = get_nav_badges()
    conn = get_connection()
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        year = datetime.now().year

        # KPIs
        # Income = any ledger row where income was recorded (income col set and positive)
        # This catches all categories, not just 'Income Received'
        # Use canonical signed-amount expression
        _AMT = "COALESCE(income, CASE WHEN expense IS NOT NULL THEN -expense ELSE amount END, 0)"
        ledger_income = conn.execute(f"""
            SELECT COALESCE(SUM({_AMT}), 0) FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE entry_date >= ? AND l.is_deleted=0
              AND {_AMT} > 0
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
        """, [f"{year}-01-01"]).fetchone()[0]

        invoice_revenue = conn.execute("""
            SELECT COALESCE(SUM(amount_paid), 0) FROM invoices
            WHERE invoice_date >= ? AND status IN ('Paid','Partial') AND is_deleted=0
        """, [f"{year}-01-01"]).fetchone()[0]

        # Use ledger income if populated, otherwise fall back to invoice revenue
        ytd_revenue = ledger_income if ledger_income > 0 else invoice_revenue

        # Expenses = ledger expense entries (signed amount negative), excluding transfers
        ytd_expenses = conn.execute(f"""
            SELECT COALESCE(SUM(ABS({_AMT})), 0) FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE entry_date >= ? AND l.is_deleted=0
              AND {_AMT} < 0
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
        """, [f"{year}-01-01"]).fetchone()[0]

        outstanding_sum = conn.execute("""
            SELECT COALESCE(SUM(balance_due), 0) FROM invoices
            WHERE status IN ('Pending','Partial','Overdue') AND is_deleted=0
        """).fetchone()[0]

        outstanding_count = conn.execute("""
            SELECT COUNT(*) FROM invoices
            WHERE status IN ('Pending','Partial','Overdue') AND is_deleted=0
        """).fetchone()[0]

        active_jobs = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE status='Active' AND is_deleted=0"
        ).fetchone()[0]

        # Upcoming reminders (next 3)
        reminders = conn.execute("""
            SELECT * FROM reminders
            WHERE status='Pending' AND is_deleted=0
            ORDER BY due_date ASC LIMIT 3
        """).fetchall()
        reminders = [dict(r) for r in reminders]
        for r in reminders:
            r['display'] = get_reminder_status(r['due_date'], r['status'])

        # Expiring certs — exactly matches insurance page logic: BETWEEN today AND today+60
        expiring = conn.execute("""
            SELECT * FROM certificates
            WHERE end_date BETWEEN ? AND date(?, '+60 days')
              AND is_deleted=0
            ORDER BY end_date ASC LIMIT 8
        """, [today, today]).fetchall()
        expiring = [dict(c) for c in expiring]
        for c in expiring:
            c['cert_status'] = get_cert_status(c['end_date'])

        # Recent ledger entries
        recent = conn.execute("""
            SELECT * FROM ledger WHERE is_deleted=0
            ORDER BY entry_date DESC, id DESC LIMIT 10
        """).fetchall()
        recent = [dict(r) for r in recent]

        unverified_receipts = conn.execute("""
            SELECT COUNT(*) FROM ledger
            WHERE receipt_filename != '' AND receipt_verified=0 AND is_deleted=0
        """).fetchone()[0]

        # Active jobs with budget health for dashboard widget
        jobs_health = conn.execute("""
            SELECT j.id, j.job_code, j.description, j.contract_amount, j.budget_amount,
                   j.start_date, j.end_date, j.status,
                   c.full_name AS client_name,
                   COALESCE((SELECT SUM(ABS(l.amount)) FROM ledger l
                             WHERE l.job_code=j.job_code AND l.amount<0 AND l.is_deleted=0),0) AS total_cost,
                   COALESCE((SELECT SUM(i.balance_due) FROM invoices i
                             WHERE i.job_code=j.job_code AND i.is_deleted=0
                               AND i.status NOT IN ('Paid','Void','Draft')),0) AS ar
            FROM jobs j
            LEFT JOIN clients c ON j.client_id=c.id
            WHERE j.status='Active' AND j.is_deleted=0
            ORDER BY j.start_date DESC
            LIMIT 8
        """).fetchall()
        jobs_health = [dict(j) for j in jobs_health]

        return render_template('dashboard.html',
            config=config,
            badges=badges,
            today=today,
            ytd_revenue=ytd_revenue,
            ytd_expenses=ytd_expenses,
            net_income=ytd_revenue - ytd_expenses,
            outstanding_sum=outstanding_sum,
            outstanding_count=outstanding_count,
            active_jobs=active_jobs,
            reminders=reminders,
            expiring=expiring,
            recent=recent,
            unmatched=badges['unmatched'],
            unverified_receipts=unverified_receipts,
            jobs_health=jobs_health,
            duplicate_groups=badges.get('duplicate_groups', 0),
        )
    finally:
        conn.close()


# ============================================================
# SETTINGS
# ============================================================

@app.route('/settings')
def settings():
    config = get_config()
    badges = get_nav_badges()
    conn = get_connection()
    try:
        ss_caps = conn.execute(
            "SELECT * FROM ss_wage_caps WHERE is_deleted=0 ORDER BY year DESC"
        ).fetchall()
        bank_accounts = conn.execute(
            "SELECT * FROM bank_accounts WHERE is_deleted=0 ORDER BY account_name"
        ).fetchall()
        categories = conn.execute(
            "SELECT * FROM work_categories WHERE is_deleted=0 ORDER BY category_name"
        ).fetchall()
        vendor_cats = conn.execute(
            "SELECT * FROM vendor_categories WHERE is_deleted=0 ORDER BY vendor_name"
        ).fetchall()
        return render_template('settings.html',
            config=config, badges=badges,
            ss_caps=[dict(r) for r in ss_caps],
            bank_accounts=[dict(r) for r in bank_accounts],
            categories=[dict(r) for r in categories],
            vendor_cats=[dict(r) for r in vendor_cats],
        )
    finally:
        conn.close()




@app.route('/files/logo')
def serve_logo():
    """Serve the company logo uploaded in settings."""
    config = get_config()
    path = config.get('company_logo_path', '')
    if path and os.path.isfile(path):
        return send_file(path)
    return ('No logo configured', 404)


@app.route('/settings/logo', methods=['POST'])
def settings_logo_upload():
    """Upload/replace company logo."""
    f = request.files.get('logo_file')
    if not f or not f.filename:
        flash('No file selected.', 'error')
        return redirect(url_for('settings'))
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in ('.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg'):
        flash('Unsupported file type. Use PNG, JPG, GIF, WebP or SVG.', 'error')
        return redirect(url_for('settings'))
    # Store logo in static/uploads/ next to the app
    upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'uploads')
    os.makedirs(upload_dir, exist_ok=True)
    logo_path = os.path.join(upload_dir, f'company_logo{ext}')
    f.save(logo_path)
    with db() as conn:
        conn.execute("UPDATE company_config SET company_logo_path=?, updated_at=datetime('now') WHERE id=1",
                     [logo_path])
    flash('Logo uploaded successfully.', 'success')
    return redirect(url_for('settings'))



@app.route('/api/settings/patch', methods=['POST'])
def api_settings_patch():
    """AJAX endpoint: update one or more company_config fields instantly.
    Used by timeclock toggle, auto-save, and any in-page settings widget."""
    ALLOWED = {
        'time_tracker_enabled','continuous_scroll','auto_backup_mode',
        'backup_keep_count','confirm_on_exit',
        'default_overhead_pct','default_insurance_pct',
        'default_owner_wages_pct','default_profit_pct','default_markup_pct',
    }
    data = request.get_json(silent=True) or {}
    fields = {k: v for k, v in data.items() if k in ALLOWED}
    if not fields:
        return jsonify({'success': False, 'error': 'No valid fields'}), 400
    with db() as conn:
        set_clause = ', '.join(f"{k}=?" for k in fields)
        conn.execute(
            f"UPDATE company_config SET {set_clause}, updated_at=datetime('now') WHERE id=1",
            list(fields.values())
        )
    return jsonify({'success': True, 'updated': list(fields.keys())})

@app.route('/settings/company', methods=['POST'])
def settings_company_save():
    data = request.form.to_dict()
    with db() as conn:
        fields = ['company_name','owner_name','address','city_state_zip','phone','email',
                  'website','license_number','ein','receipts_folder_path','invoices_folder_path',
                  'certs_folder_path','backup_folder_path','active_jobs_folder_path','payroll_year',
                  'time_tracker_enabled',
                  'backup_keep_count','continuous_scroll','auto_backup_mode','confirm_on_exit',
                  'fica_rate_employee','medicare_rate_employee','fica_rate_employer',
                  'medicare_rate_employer','futa_rate','futa_wage_base','suta_rate_il',
                  'suta_wage_base_il','prior_year_withholding_carryforward',
                  'prior_year_trade_adjustment','opening_bank_balance']
        set_clause = ', '.join(f"{f}=?" for f in fields if f in data)
        values = [data[f] for f in fields if f in data]
        if set_clause:
            conn.execute(
                f"UPDATE company_config SET {set_clause}, updated_at=datetime('now') WHERE id=1",
                values
            )
    flash('Company settings saved.', 'success')
    return redirect(url_for('settings'))


@app.route('/settings/ss-cap', methods=['POST'])
def settings_ss_cap_save():
    year = request.form.get('year')
    cap = request.form.get('cap_amount')
    source = request.form.get('irs_source', '')
    if not year or not cap:
        flash('Year and cap amount required.', 'error')
        return redirect(url_for('settings'))
    with db() as conn:
        conn.execute("""
            INSERT INTO ss_wage_caps (year, cap_amount, irs_source)
            VALUES (?, ?, ?)
            ON CONFLICT(year) DO UPDATE SET cap_amount=excluded.cap_amount,
            irs_source=excluded.irs_source, updated_at=datetime('now')
        """, [int(year), float(cap), source])
    flash(f'SS wage cap for {year} saved.', 'success')
    return redirect(url_for('settings'))


@app.route('/settings/bank-account', methods=['POST'])
def settings_bank_account_save():
    data = request.form.to_dict()
    acct_id = data.get('id')
    with db() as conn:
        if acct_id:
            conn.execute("""
                UPDATE bank_accounts SET account_name=?,account_type=?,institution_name=?,
                last_four=?,current_balance=?,notes=?,statements_folder_path=?,
                updated_at=datetime('now')
                WHERE id=?
            """, [data.get('account_name',''), data.get('account_type','Checking'),
                  data.get('institution_name',''), data.get('last_four',''),
                  float(data.get('current_balance',0)), data.get('notes',''),
                  data.get('statements_folder_path','').strip(), acct_id])
        else:
            conn.execute("""
                INSERT INTO bank_accounts (account_name,account_type,institution_name,
                    last_four,current_balance,notes,statements_folder_path)
                VALUES (?,?,?,?,?,?,?)
            """, [data.get('account_name',''), data.get('account_type','Checking'),
                  data.get('institution_name',''), data.get('last_four',''),
                  float(data.get('current_balance',0)), data.get('notes',''),
                  data.get('statements_folder_path','').strip()])
    flash('Bank account saved.', 'success')
    return redirect(url_for('settings'))




@app.route('/api/recon/account/<int:acct_id>/match-stats', methods=['GET'])
def api_recon_account_match_stats(acct_id):
    """Return current match statistics for a bank account (for the confirm dialog)."""
    with db() as conn:
        acct = conn.execute(
            "SELECT id, account_name FROM bank_accounts WHERE id=? AND is_deleted=0", [acct_id]
        ).fetchone()
        if not acct:
            return jsonify({'error': 'Account not found'}), 404

        matched = conn.execute("""
            SELECT COUNT(*) FROM bank_transactions
            WHERE bank_account_id=? AND is_deleted=0
              AND match_status IN ('Auto-Matched', 'Manual-Matched')
        """, [acct_id]).fetchone()[0]

        ledger_freed = conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE bank_account_id=? AND is_deleted=0", [acct_id]
        ).fetchone()[0]

    return jsonify({'matched': matched, 'ledger_freed': ledger_freed})


@app.route('/api/recon/account/<int:acct_id>/clear-matches', methods=['POST'])
def api_recon_account_clear_matches(acct_id):
    """
    Reset ALL match results for a single bank account (Auto-Matched AND Manual-Matched).

    What this does:
      1. Resets bank_transactions.match_status → 'Unmatched' for all matched txns
      2. Clears bank_transactions.matched_ledger_id → NULL
      3. Clears bank_transactions.notes
      4. Resets ledger entries linked to this account:
           - status → 'Pending'
           - bank_account_id → NULL
           - reconciliation_id → NULL
      (Both auto-matched AND manually matched / reconciled entries are cleared.)

    What this does NOT do:
      - Does not delete any transactions or ledger entries
      - Does not touch 'Excluded' bank transactions
      - Does not affect other bank accounts

    Use this to fully reset and re-run auto-match.
    """
    with db() as conn:
        acct = conn.execute(
            "SELECT id, account_name FROM bank_accounts WHERE id=? AND is_deleted=0", [acct_id]
        ).fetchone()
        if not acct:
            return jsonify({'error': 'Account not found'}), 404

        # Count before reset (for response / log)
        matched_before = conn.execute("""
            SELECT COUNT(*) FROM bank_transactions
            WHERE bank_account_id=? AND is_deleted=0
              AND match_status IN ('Auto-Matched', 'Manual-Matched')
        """, [acct_id]).fetchone()[0]

        ledger_before = conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE bank_account_id=? AND is_deleted=0", [acct_id]
        ).fetchone()[0]

        # 1. Collect all ledger IDs linked via matched bank transactions (for full status reset)
        linked_ledger_ids = [
            r[0] for r in conn.execute("""
                SELECT DISTINCT matched_ledger_id FROM bank_transactions
                WHERE bank_account_id=? AND is_deleted=0
                  AND match_status IN ('Auto-Matched', 'Manual-Matched')
                  AND matched_ledger_id IS NOT NULL
            """, [acct_id]).fetchall()
        ]

        # 2. Reset bank transactions — leave 'Excluded' alone
        conn.execute("""
            UPDATE bank_transactions
            SET match_status      = 'Unmatched',
                matched_ledger_id = NULL,
                notes             = '',
                updated_at        = datetime('now')
            WHERE bank_account_id = ?
              AND is_deleted = 0
              AND match_status IN ('Auto-Matched', 'Manual-Matched')
        """, [acct_id])

        # 3. Reset all ledger entries assigned to this account:
        #    - Clear status back to Pending
        #    - Unlink bank_account_id
        #    - Clear reconciliation_id (handles manually reconciled entries)
        conn.execute("""
            UPDATE ledger
            SET status            = 'Pending',
                bank_account_id   = NULL,
                reconciliation_id = NULL,
                updated_at        = datetime('now')
            WHERE bank_account_id = ?
              AND is_deleted = 0
        """, [acct_id])

        # 4. Also reset any ledger entries that were linked via matched_ledger_id
        #    but may have a different bank_account_id (edge case: orphaned links)
        if linked_ledger_ids:
            placeholders = ','.join('?' * len(linked_ledger_ids))
            conn.execute(f"""
                UPDATE ledger
                SET status            = 'Pending',
                    bank_account_id   = NULL,
                    reconciliation_id = NULL,
                    updated_at        = datetime('now')
                WHERE id IN ({placeholders}) AND is_deleted = 0
            """, linked_ledger_ids)

        # Log the action
        from automations import log_action
        log_action(conn, 'bank_transactions', 0, 'UPDATE', new_data={
            'action': 'clear_matches',
            'account_id': acct_id,
            'account_name': acct['account_name'],
            'transactions_reset': matched_before,
            'ledger_entries_freed': ledger_before,
        })

    return jsonify({
        'success': True,
        'reset': matched_before,
        'ledger_freed': ledger_before,
    })


@app.route('/settings/bank-account/<int:acct_id>/delete', methods=['POST'])
def settings_bank_account_delete(acct_id):
    """Soft-delete a bank account."""
    with db() as conn:
        conn.execute("UPDATE bank_accounts SET is_deleted=1 WHERE id=?", [acct_id])
    flash('Bank account deleted.', 'success')
    return redirect(url_for('settings') + '#accounts')

@app.route('/settings/category', methods=['POST'])
def settings_category_save():
    data = request.form.to_dict()
    cat_id = data.get('id')
    with db() as conn:
        if cat_id:
            conn.execute("""
                UPDATE work_categories SET category_name=?,is_cogs=?,is_tax_deductible=?,
                is_transfer=?,schedule_c_line=?,updated_at=datetime('now') WHERE id=?
            """, [data.get('category_name',''),
                  1 if data.get('is_cogs') else 0,
                  1 if data.get('is_tax_deductible') else 0,
                  1 if data.get('is_transfer') else 0,
                  data.get('schedule_c_line',''), cat_id])
        else:
            conn.execute("""
                INSERT OR IGNORE INTO work_categories
                    (category_name,is_cogs,is_tax_deductible,is_transfer,schedule_c_line)
                VALUES (?,?,?,?,?)
            """, [data.get('category_name',''),
                  1 if data.get('is_cogs') else 0,
                  1 if data.get('is_tax_deductible') else 0,
                  1 if data.get('is_transfer') else 0,
                  data.get('schedule_c_line','')])
    flash('Category saved.', 'success')
    return redirect(url_for('settings'))


@app.route('/settings/vendor-category', methods=['POST'])
def settings_vendor_cat_save():
    data = request.form.to_dict()
    vendor = data.get('vendor_name', '').strip()
    cat = data.get('default_category', '').strip()
    if vendor and cat:
        with db() as conn:
            save_vendor_category(vendor, cat, conn)
        flash('Vendor category saved.', 'success')
    return redirect(url_for('settings'))


def _do_backup(label='manual'):
    """
    ZIP-based backup with rotation.
    Creates kbweb_backup_{label}_{ts}.zip in backup_folder_path containing:
      - db/ (database)
      - receipts/ (all receipt PDFs if configured)
      - certs/ (all cert PDFs if configured)
      - manifest.txt
    Prunes old startup/auto backups beyond backup_keep_count.
    Returns (success: bool, message: str).
    """
    config = get_config()
    backup_root = config.get('backup_folder_path', '')
    if not backup_root:
        return False, 'Backup folder not configured in Settings.'
    os.makedirs(backup_root, exist_ok=True)

    ts       = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_name = f"kbweb_backup_{label}_{ts}.zip"
    zip_path = os.path.join(backup_root, zip_name)

    receipts_folder = config.get('receipts_folder_path', '')
    certs_folder    = config.get('certs_folder_path', '')
    keep_count      = int(config.get('backup_keep_count') or 30)
    receipt_count   = 0
    cert_count      = 0

    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            # 1. Database
            if os.path.isfile(DB_PATH):
                zf.write(DB_PATH, f'db/{os.path.basename(DB_PATH)}')

            # 2. Receipts
            if receipts_folder and os.path.isdir(receipts_folder):
                for fpath in glob.glob(os.path.join(receipts_folder, '**', '*'), recursive=True):
                    if os.path.isfile(fpath):
                        rel = os.path.relpath(fpath, receipts_folder)
                        zf.write(fpath, f'receipts/{rel}')
                        receipt_count += 1

            # 3. Certs
            if certs_folder and os.path.isdir(certs_folder):
                for fpath in glob.glob(os.path.join(certs_folder, '**', '*'), recursive=True):
                    if os.path.isfile(fpath):
                        rel = os.path.relpath(fpath, certs_folder)
                        zf.write(fpath, f'certs/{rel}')
                        cert_count += 1

            # 4. Manifest
            zf.writestr('manifest.txt',
                f"KB Construction Manager — Backup\n"
                f"Created  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Label    : {label}\n"
                f"DB       : {os.path.basename(DB_PATH)}\n"
                f"Receipts : {receipt_count} files\n"
                f"Certs    : {cert_count} files\n"
            )

        # 5. Rotate: keep only last N startup/auto backups
        all_zips = sorted(glob.glob(os.path.join(backup_root, 'kbweb_backup_*.zip')), reverse=True)
        auto_zips = [z for z in all_zips if os.path.basename(z).startswith(('kbweb_backup_startup_', 'kbweb_backup_auto_'))]
        for old_zip in auto_zips[keep_count:]:
            try: os.remove(old_zip)
            except OSError: pass

        size_mb = os.path.getsize(zip_path) / (1024 * 1024)
        return True, f"{zip_name} ({size_mb:.1f} MB, {receipt_count} receipts, {cert_count} certs)"

    except Exception as e:
        return False, str(e)


@app.route('/settings/backup', methods=['POST'])
def settings_backup():
    ok, msg = _do_backup('manual')
    if ok:
        flash(f'Backup saved to: {msg}', 'success')
    else:
        flash(f'Backup failed: {msg}', 'error')
    return redirect(url_for('settings'))



@app.route('/api/backup/manual', methods=['POST'])
def api_backup_manual():
    """AJAX endpoint so the UI can show the backup overlay while backing up."""
    ok, msg = _do_backup('manual')
    if ok:
        return jsonify({'success': True, 'filename': os.path.basename(msg)})
    return jsonify({'success': False, 'error': msg}), 500


@app.route('/api/backup/list')
def api_backup_list():
    """List all backups with size and date."""
    config = get_config()
    backup_root = config.get('backup_folder_path', '')
    if not backup_root or not os.path.isdir(backup_root):
        return jsonify({'backups': [], 'error': 'Backup folder not configured'})
    zips = sorted(
        [f for f in os.listdir(backup_root) if f.endswith('.zip')],
        reverse=True
    )
    result = []
    for z in zips:
        full = os.path.join(backup_root, z)
        stat = os.stat(full)
        result.append({
            'name':     z,
            'path':     full,
            'size_mb':  round(stat.st_size / 1024 / 1024, 1),
            'created':  datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M'),
            'is_auto':  z.startswith(('kbweb_backup_startup_', 'kbweb_backup_auto_')),
        })
    return jsonify({'backups': result})




@app.route('/api/backup/delete', methods=['POST'])
def api_backup_delete():
    """Delete a specific backup file from disk.
    Accepts 'name' (basename only) or 'path' (full path — basename is extracted).
    The file is always resolved relative to the configured backup_folder_path."""
    data = request.get_json() or {}
    # Accept either 'name' (preferred, safe) or 'path' (legacy — extract basename)
    name = data.get('name', '').strip()
    if not name:
        raw_path = data.get('path', '').strip()
        name = os.path.basename(raw_path)
    if not name:
        return jsonify({'error': 'No filename provided'}), 400

    # Block path traversal in the filename itself
    if os.sep in name or '/' in name or '..' in name:
        return jsonify({'error': 'Invalid filename'}), 400

    # Only allow deleting files that match our backup naming convention
    if not name.startswith('kbweb_backup_') or not name.endswith('.zip'):
        return jsonify({'error': 'Not a recognised backup file'}), 400

    try:
        config = get_config()
        backup_folder = config.get('backup_folder_path', '').strip()
        if not backup_folder or not os.path.isdir(backup_folder):
            return jsonify({'error': 'Backup folder not configured or missing'}), 400

        target = os.path.join(backup_folder, name)
        if not os.path.isfile(target):
            return jsonify({'error': 'File not found'}), 404

        os.remove(target)
        return jsonify({'success': True, 'message': f'Deleted: {name}'})
    except PermissionError:
        return jsonify({'error': 'Permission denied — cannot delete file'}), 403
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/backup/restore', methods=['POST'])
def api_backup_restore():
    """
    Restore selected tables from a backup ZIP.
    Body: { zip_path: str, tables: [str] }  
    tables can include: 'ledger','clients','contractors','certificates','timesheet','invoices','jobs'
    The current DB tables are replaced; everything else is untouched.
    A rescue backup is created first.
    """
    import zipfile as _zipfile, shutil as _shutil, tempfile as _tmpdir
    data = request.json or {}
    zip_path = data.get('zip_path', '').strip()
    tables   = data.get('tables', [])

    if not zip_path or not os.path.isfile(zip_path):
        return jsonify({'error': 'Backup file not found'}), 400
    if not tables:
        return jsonify({'error': 'No tables selected'}), 400

    # Validate table names against whitelist
    ALLOWED = {'ledger','clients','contractors','certificates','timesheet',
               'invoices','jobs','employees','payroll_runs','bank_accounts',
               'vendor_categories','work_categories'}
    tables = [t for t in tables if t in ALLOWED]
    if not tables:
        return jsonify({'error': 'No valid tables selected'}), 400

    db_path = _get_db_path()
    config  = get_config()

    # Step 1: Create rescue backup of current state
    rescue_ok, rescue_msg = _do_backup('pre-restore')
    if not rescue_ok:
        return jsonify({'error': f'Could not create rescue backup: {rescue_msg}'}), 500

    try:
        # Step 2: Extract the backup DB to a temp dir
        with _tmpdir.TemporaryDirectory() as tmp:
            with _zipfile.ZipFile(zip_path, 'r') as zf:
                # Find the DB file in the zip
                db_entries = [n for n in zf.namelist() if n.endswith('.db') or n.endswith('.sqlite')]
                if not db_entries:
                    return jsonify({'error': 'No database file found in backup ZIP'}), 400
                zf.extract(db_entries[0], tmp)
                backup_db = os.path.join(tmp, db_entries[0])

            # Step 3: Connect to both DBs and copy requested tables
            import sqlite3 as _sqlite3
            src = _sqlite3.connect(backup_db)
            src.row_factory = _sqlite3.Row
            dst = _sqlite3.connect(db_path)
            dst.row_factory = _sqlite3.Row

            restored = {}
            for table in tables:
                try:
                    # Get source rows
                    src_rows = src.execute(f"SELECT * FROM {table} WHERE is_deleted=0").fetchall()
                    cols     = [d[0] for d in src_rows[0].description] if src_rows else []

                    if not cols:
                        # Try without is_deleted filter
                        src_rows = src.execute(f"SELECT * FROM {table}").fetchall()
                        cols = [d[0] for d in src_rows[0].description] if src_rows else []

                    if not src_rows:
                        restored[table] = 0
                        continue

                    # Soft-delete all current rows
                    dst.execute(f"UPDATE {table} SET is_deleted=1")

                    # Insert source rows (skip if id already exists)
                    ph = ','.join(['?']*len(cols))
                    col_list = ','.join(cols)
                    count = 0
                    for row in src_rows:
                        try:
                            dst.execute(
                                f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({ph})",
                                list(row)
                            )
                            count += 1
                        except Exception:
                            pass
                    restored[table] = count
                except Exception as e:
                    restored[table] = f'error: {e}'

            dst.commit()
            src.close(); dst.close()

            return jsonify({
                'success': True,
                'restored': restored,
                'rescue_backup': rescue_msg,
                'message': f'Restored {sum(v for v in restored.values() if isinstance(v, int))} rows across {len(tables)} tables. A rescue backup was saved: {rescue_msg}'
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _get_db_path():
    """Return the current database path."""
    return os.environ.get('CONSTRUCTION_DB', 'construction.db')

@app.route('/api/backup/status')
def api_backup_status():
    """Return info about recent backups."""
    config = get_config()
    backup_root = config.get('backup_folder_path', '')
    backups = []
    if backup_root and os.path.isdir(backup_root):
        zips = sorted(glob.glob(os.path.join(backup_root, 'kbweb_backup_*.zip')), reverse=True)
        for z in zips[:10]:
            try:
                stat = os.stat(z)
                backups.append({
                    'name': os.path.basename(z),
                    'size_mb': round(stat.st_size / (1024*1024), 1),
                    'created': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M'),
                })
            except OSError:
                pass
    return jsonify({
        'backups': backups,
        'last_backup': backups[0]['name'] if backups else None,
        'backup_folder': backup_root,
    })


@app.route('/settings/verify-receipts', methods=['POST'])
def settings_verify_receipts():
    count = verify_all_receipts()
    flash(f'Verified {count} receipt entries.', 'success')
    return redirect(url_for('settings'))


# ============================================================
# IMPORT / MIGRATION WIZARD
# ============================================================

@app.route('/settings/import')
def import_wizard():
    config = get_config()
    badges = get_nav_badges()
    conn = get_connection()
    try:
        batches = conn.execute(
            "SELECT * FROM import_batches ORDER BY import_date DESC LIMIT 10"
        ).fetchall()
        return render_template('import_wizard.html',
            config=config, badges=badges,
            batches=[dict(b) for b in batches]
        )
    finally:
        conn.close()


@app.route('/settings/import/upload', methods=['POST'])
def import_upload():
    """Step 1: Upload file and detect sheets."""
    from migration import read_xlsx_sheets, read_ods_sheets, detect_sheet_mapping
    f = request.files.get('file')
    if not f:
        return jsonify({'error': 'No file uploaded'}), 400
    filename = f.filename
    ext = os.path.splitext(filename)[1].lower()
    batch_id = str(uuid.uuid4())[:12]
    tmp_path = os.path.join(tempfile.gettempdir(), f"import_{batch_id}{ext}")
    f.save(tmp_path)
    try:
        if ext == '.xlsx':
            sheets = read_xlsx_sheets(tmp_path)
        elif ext in ('.ods', '.ods'):
            sheets = read_ods_sheets(tmp_path)
        else:
            return jsonify({'error': f'Unsupported file type: {ext}'}), 400
        # Detect mappings and build preview
        preview = {}
        for name, data in sheets.items():
            target = detect_sheet_mapping(name, data.get('headers', []))
            preview[name] = {
                'target': target,
                'headers': data.get('headers', []),
                'row_count': len(data.get('rows', [])),
                'sample': data.get('rows', [])[:5]
            }
        # Save batch record
        with db() as conn:
            conn.execute("""
                INSERT INTO import_batches (batch_id, source_filename, status)
                VALUES (?, ?, 'Preview')
            """, [batch_id, filename])
        # Store sheets data temporarily
        import pickle
        with open(os.path.join(tempfile.gettempdir(), f"sheets_{batch_id}.pkl"), 'wb') as pf:
            pickle.dump(sheets, pf)
        return jsonify({'batch_id': batch_id, 'preview': preview})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


@app.route('/settings/import/validate', methods=['POST'])
def import_validate():
    """Step 2: Validate rows and return errors."""
    from migration import (read_xlsx_sheets, detect_sheet_mapping,
                           map_row_to_ledger, map_row_to_timesheet,
                           validate_ledger_rows, validate_timesheet_rows)
    data = request.json
    batch_id = data.get('batch_id')
    import pickle
    pkl_path = os.path.join(tempfile.gettempdir(), f"sheets_{batch_id}.pkl")
    if not os.path.exists(pkl_path):
        return jsonify({'error': 'Session expired. Please re-upload.'}), 400
    with open(pkl_path, 'rb') as f:
        sheets = pickle.load(f)
    errors_by_sheet = {}
    totals = {}
    for name, sheet_data in sheets.items():
        target = detect_sheet_mapping(name, sheet_data.get('headers', []))
        rows = sheet_data.get('rows', [])
        if target == 'ledger':
            mapped = [map_row_to_ledger(r) for r in rows]
            _, errs = validate_ledger_rows(mapped)
            errors_by_sheet[name] = errs
            totals[name] = {'total': len(rows), 'errors': len(errs), 'valid': len(rows)-len(errs)}
        elif target == 'timesheet':
            mapped = [map_row_to_timesheet(r) for r in rows]
            _, errs = validate_timesheet_rows(mapped)
            errors_by_sheet[name] = errs
            totals[name] = {'total': len(rows), 'errors': len(errs), 'valid': len(rows)-len(errs)}
        else:
            errors_by_sheet[name] = []
            totals[name] = {'total': len(rows), 'errors': 0, 'valid': len(rows)}
    return jsonify({'errors': errors_by_sheet, 'totals': totals})


@app.route('/settings/import/execute', methods=['POST'])
def import_execute():
    """Step 3: Execute the import."""
    from migration import run_full_import
    data = request.json
    batch_id = data.get('batch_id')
    import pickle
    pkl_path = os.path.join(tempfile.gettempdir(), f"sheets_{batch_id}.pkl")
    if not os.path.exists(pkl_path):
        return jsonify({'error': 'Session expired. Please re-upload.'}), 400
    with open(pkl_path, 'rb') as f:
        sheets = pickle.load(f)
    try:
        with db() as conn:
            summary = run_full_import(batch_id, sheets, conn)
        os.remove(pkl_path)
        return jsonify({'success': True, 'summary': summary})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# API ENDPOINTS (used by JS in later phases)
# ============================================================

@app.route('/api/config')
def api_config():
    return jsonify(get_config())


@app.route('/api/autocomplete/vendors')
def api_vendors():
    q = request.args.get('q', '')
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT DISTINCT vendor FROM ledger
            WHERE vendor LIKE ? AND is_deleted=0
            UNION
            SELECT vendor_name FROM vendor_categories WHERE vendor_name LIKE ? AND is_deleted=0
            LIMIT 20
        """, [f"%{q}%", f"%{q}%"]).fetchall()
        return jsonify([r[0] for r in rows if r[0]])
    finally:
        conn.close()


@app.route('/api/autocomplete/categories')
def api_categories():
    q = request.args.get('q', '')
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT category_name FROM work_categories
            WHERE category_name LIKE ? AND is_deleted=0
            ORDER BY category_name LIMIT 20
        """, [f"%{q}%"]).fetchall()
        return jsonify([r[0] for r in rows])
    finally:
        conn.close()


@app.route('/api/autocomplete/jobs')
def api_jobs():
    q = request.args.get('q', '')
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT job_code, description FROM jobs
            WHERE (job_code LIKE ? OR description LIKE ?) AND is_deleted=0
            ORDER BY job_code LIMIT 20
        """, [f"%{q}%", f"%{q}%"]).fetchall()
        return jsonify([{'job_code': r[0], 'description': r[1]} for r in rows])
    finally:
        conn.close()


@app.route('/api/autocomplete/clients')
def api_clients():
    q = request.args.get('q', '')
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, full_name, customer_id, address, city_state_zip
            FROM clients WHERE full_name LIKE ? AND is_deleted=0
            ORDER BY full_name LIMIT 20
        """, [f"%{q}%"]).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route('/api/autocomplete/employees')
def api_employees():
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT emp_id, first_name, last_name FROM employees
            WHERE status='Active' AND is_deleted=0 ORDER BY last_name
        """).fetchall()
        return jsonify([{
            'emp_id': r['emp_id'],
            'name': f"{r['first_name']} {r['last_name']}"
        } for r in rows])
    finally:
        conn.close()


@app.route('/api/employee-rate')
def api_employee_rate():
    emp_id = request.args.get('emp_id')
    entry_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    if not emp_id:
        return jsonify({'error': 'emp_id required'}), 400
    rates = get_rate_for_date(int(emp_id), entry_date)
    return jsonify(rates)


@app.route('/api/vendor-category')
def api_vendor_category():
    vendor = request.args.get('vendor', '')
    cat = get_vendor_category(vendor)
    return jsonify({'category': cat})


@app.route('/api/receipt-filename')
def api_receipt_filename():
    date = request.args.get('date', '')
    job = request.args.get('job', '')
    vendor = request.args.get('vendor', '')
    amount = request.args.get('amount', '0')
    fname = generate_receipt_filename(date, job, vendor, float(amount or 0))
    return jsonify({'filename': fname})


@app.route('/api/quick-quote')
def api_quick_quote():
    total = float(request.args.get('total', 0))
    overhead = float(request.args.get('overhead', 15))
    insurance = float(request.args.get('insurance', 0.9))
    owner_wages = float(request.args.get('owner_wages', 0))
    profit = float(request.args.get('profit', 10))
    result = calculate_quick_quote(total, overhead, insurance, owner_wages, profit)
    return jsonify(result)


# ============================================================
# UNDO / REDO  (Phase 5: unlimited with session tracking)
# ============================================================

def _apply_undo_entry(conn, entry):
    """
    Reverse a single undo_log entry.
    Returns (success, message) tuple.
    Handles UPDATE, DELETE, INSERT reversals.
    """
    old   = json.loads(entry['old_data']) if entry.get('old_data') else {}
    new   = json.loads(entry['new_data']) if entry.get('new_data') else {}
    action = entry['action']
    table  = entry['table_name']
    rid    = entry['record_id']

    SAFE_TABLES = {'ledger', 'contractors', 'clients', 'jobs', 'invoices',
                   'employees', 'timesheet', 'certificates', 'bank_accounts'}
    if table not in SAFE_TABLES:
        return False, f"Table {table!r} not reversible"

    try:
        if action == 'DELETE':
            conn.execute(
                f"UPDATE {table} SET is_deleted=0, updated_at=datetime('now') WHERE id=?",
                [rid]
            )
        elif action == 'UPDATE' and old:
            safe_old = {k: v for k, v in old.items() if k != 'id'}
            if safe_old:
                set_clause = ', '.join(f"{k}=?" for k in safe_old)
                conn.execute(
                    f"UPDATE {table} SET {set_clause}, updated_at=datetime('now') WHERE id=?",
                    list(safe_old.values()) + [rid]
                )
        elif action == 'INSERT':
            conn.execute(
                f"UPDATE {table} SET is_deleted=1, updated_at=datetime('now') WHERE id=?",
                [rid]
            )
        else:
            return False, "Nothing to reverse"
        return True, entry.get('user_label') or f"{action} on {table} #{rid}"
    except Exception as e:
        return False, str(e)


def _apply_redo_entry(conn, entry):
    """
    Re-apply a previously undone entry.
    Returns (success, message) tuple.
    """
    new   = json.loads(entry['new_data']) if entry.get('new_data') else {}
    action = entry['action']
    table  = entry['table_name']
    rid    = entry['record_id']

    SAFE_TABLES = {'ledger', 'contractors', 'clients', 'jobs', 'invoices',
                   'employees', 'timesheet', 'certificates', 'bank_accounts'}
    if table not in SAFE_TABLES:
        return False, f"Table {table!r} not redoable"

    try:
        if action == 'INSERT':
            # Re-insert means un-delete
            conn.execute(
                f"UPDATE {table} SET is_deleted=0, updated_at=datetime('now') WHERE id=?",
                [rid]
            )
        elif action == 'DELETE':
            conn.execute(
                f"UPDATE {table} SET is_deleted=1, updated_at=datetime('now') WHERE id=?",
                [rid]
            )
        elif action == 'UPDATE' and new:
            safe_new = {k: v for k, v in new.items() if k != 'id'}
            if safe_new:
                set_clause = ', '.join(f"{k}=?" for k in safe_new)
                conn.execute(
                    f"UPDATE {table} SET {set_clause}, updated_at=datetime('now') WHERE id=?",
                    list(safe_new.values()) + [rid]
                )
        else:
            return False, "Nothing to re-apply"
        return True, entry.get('user_label') or f"Redo {action} on {table} #{rid}"
    except Exception as e:
        return False, str(e)


@app.route('/api/undo', methods=['POST'])
def api_undo():
    """Undo the most recent non-reversed action."""
    conn = get_connection()
    try:
        entry = conn.execute("""
            SELECT * FROM undo_log
            WHERE reversed IN (0, 2)
            ORDER BY id DESC LIMIT 1
        """).fetchone()
        if not entry:
            return jsonify({'error': 'Nothing to undo'}), 400

        entry = dict(entry)
        ok, msg = _apply_undo_entry(conn, entry)
        if ok:
            conn.execute("UPDATE undo_log SET reversed=1 WHERE id=?", [entry['id']])
            conn.commit()
            return jsonify({
                'success': True,
                'message': f"Undid: {msg}",
                'action': entry['action'],
                'table': entry['table_name'],
                'record_id': entry['record_id'],
                'entry_id': entry['id'],
            })
        else:
            conn.rollback()
            return jsonify({'error': msg}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/api/redo', methods=['POST'])
def api_redo():
    """Redo the most recently undone action (reversed=1, not yet redone=2)."""
    conn = get_connection()
    try:
        entry = conn.execute("""
            SELECT * FROM undo_log
            WHERE reversed = 1
            ORDER BY id DESC LIMIT 1
        """).fetchone()
        if not entry:
            return jsonify({'error': 'Nothing to redo'}), 400

        entry = dict(entry)
        ok, msg = _apply_redo_entry(conn, entry)
        if ok:
            conn.execute("UPDATE undo_log SET reversed=2 WHERE id=?", [entry['id']])
            conn.commit()
            return jsonify({
                'success': True,
                'message': f"Redid: {msg}",
                'action': entry['action'],
                'table': entry['table_name'],
                'record_id': entry['record_id'],
                'entry_id': entry['id'],
            })
        else:
            conn.rollback()
            return jsonify({'error': msg}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/api/undo/history')
def api_undo_history():
    """
    Return the undo history for the current session (or recent N entries).
    Query params: n (default 100), session_id, table (filter by table name).
    """
    n           = min(int(request.args.get('n', 100)), 500)
    table_filter = request.args.get('table', '')
    reversed_filter = request.args.get('reversed', '')  # '0', '1', '2', or '' for all

    where = []
    params = []
    if table_filter:
        where.append("table_name=?"); params.append(table_filter)
    if reversed_filter != '':
        where.append("reversed=?"); params.append(int(reversed_filter))

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    conn = get_connection()
    try:
        rows = conn.execute(f"""
            SELECT id, action_time, table_name, record_id, action,
                   old_data, new_data, reversed, user_label, field_name, session_id
            FROM undo_log
            {where_sql}
            ORDER BY id DESC LIMIT ?
        """, params + [n]).fetchall()

        # Group entries by session_id (batch ops appear as one item)
        seen_sessions = {}
        entries = []
        for r in rows:
            d = dict(r)
            try:
                old = json.loads(d.get('old_data') or '{}')
                new = json.loads(d.get('new_data') or '{}')
            except Exception:
                old, new = {}, {}

            # Build a readable label if not stored
            label = d.get('user_label') or ''
            if not label:
                if d['action'] == 'INSERT':
                    v = new.get('vendor') or new.get('name', '')
                    label = f"Added {v}" if v else f"Added to {d['table_name']}"
                elif d['action'] == 'DELETE':
                    v = old.get('vendor') or old.get('name', '')
                    label = f"Deleted {v}" if v else f"Deleted from {d['table_name']}"
                elif d['action'] == 'UPDATE':
                    fn = d.get('field_name') or ''
                    if fn:
                        ov = str(old.get(fn,''))[:20]
                        nv = str(new.get(fn,''))[:20]
                        label = f"{fn}: {ov} → {nv}"
                    else:
                        label = f"Updated {d['table_name']} #{d['record_id']}"

            status = {0: 'active', 1: 'undone', 2: 'redone'}.get(d['reversed'], 'active')
            sid = d.get('session_id', '')

            if sid and sid in seen_sessions:
                # Fold into existing batch entry
                batch = seen_sessions[sid]
                batch['batch_count'] += 1
                batch['ids'].append(d['id'])
                continue

            entry = {
                'id':          d['id'],
                'time':        d['action_time'],
                'table':       d['table_name'],
                'record_id':   d['record_id'],
                'action':      d['action'],
                'label':       label,
                'status':      status,
                'field_name':  d.get('field_name',''),
                'can_undo':    d['reversed'] == 0,
                'can_redo':    d['reversed'] == 1,
                'session_id':  sid,
                'batch_count': 1,
                'ids':         [d['id']],
            }
            if sid:
                seen_sessions[sid] = entry
            entries.append(entry)

        # Stats
        total       = conn.execute("SELECT COUNT(*) FROM undo_log").fetchone()[0]
        undoable    = conn.execute("SELECT COUNT(*) FROM undo_log WHERE reversed=0").fetchone()[0]
        redoable    = conn.execute("SELECT COUNT(*) FROM undo_log WHERE reversed=1").fetchone()[0]

        return jsonify({
            'entries': entries,
            'total': total,
            'undoable': undoable,
            'redoable': redoable,
        })
    finally:
        conn.close()


@app.route('/api/undo/entry/<int:entry_id>', methods=['POST'])
def api_undo_entry(entry_id):
    """Undo a specific undo_log entry by ID (not just the last one)."""
    conn = get_connection()
    try:
        entry = conn.execute(
            "SELECT * FROM undo_log WHERE id=? AND reversed=0", [entry_id]
        ).fetchone()
        if not entry:
            return jsonify({'error': 'Entry not found or already undone'}), 404

        entry = dict(entry)
        ok, msg = _apply_undo_entry(conn, entry)
        if ok:
            conn.execute("UPDATE undo_log SET reversed=1 WHERE id=?", [entry_id])
            conn.commit()
            return jsonify({'success': True, 'message': f"Undid: {msg}"})
        else:
            conn.rollback()
            return jsonify({'error': msg}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/api/undo/export-log')
def api_undo_export_log():
    """Export the full action history as CSV."""
    import csv as csv_mod
    import io as io_mod

    n = min(int(request.args.get('n', 1000)), 5000)
    table_filter = request.args.get('table', '')

    where = []
    params = []
    if table_filter:
        where.append("table_name=?"); params.append(table_filter)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    conn = get_connection()
    try:
        rows = conn.execute(f"""
            SELECT id, action_time, table_name, record_id, action,
                   user_label, field_name, reversed, old_data, new_data
            FROM undo_log {where_sql}
            ORDER BY id DESC LIMIT ?
        """, params + [n]).fetchall()

        def generate():
            buf = io_mod.StringIO()
            w = csv_mod.writer(buf)
            w.writerow(['#', 'Time', 'Table', 'Record ID', 'Action',
                        'Description', 'Field', 'Status'])
            yield buf.getvalue(); buf.seek(0); buf.truncate()
            for r in rows:
                status = {0: 'Active', 1: 'Undone', 2: 'Redone'}.get(r['reversed'], '')
                w.writerow([
                    r['id'], r['action_time'], r['table_name'], r['record_id'],
                    r['action'], r['user_label'] or '', r['field_name'] or '', status
                ])
                yield buf.getvalue(); buf.seek(0); buf.truncate()

        from datetime import datetime as dt
        fname = f"session_log_{dt.now().strftime('%Y%m%d_%H%M')}.csv"
        return Response(
            stream_with_context(generate()),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={fname}'}
        )
    finally:
        conn.close()


@app.route('/api/balances')
def api_balances():
    """
    Top-bar balance summary.
    Returns YTD income/expense/net, current month, AR balance, and bank account totals.
    All figures come directly from ledger + invoices tables.
    """
    from datetime import date as _date
    today  = _date.today()
    year   = today.strftime('%Y')
    month  = today.strftime('%Y-%m')

    conn = get_connection()
    try:
        # YTD totals from ledger
        ytd = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN amount>0 THEN amount  ELSE 0 END), 0) AS income,
                COALESCE(SUM(CASE WHEN amount<0 THEN ABS(amount) ELSE 0 END), 0) AS expense,
                COALESCE(SUM(amount), 0) AS net
            FROM ledger
            WHERE is_deleted=0
              AND substr(entry_date,1,4) = ?
        """, [year]).fetchone()

        # Current month totals
        mtd = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN amount>0 THEN amount  ELSE 0 END), 0) AS income,
                COALESCE(SUM(CASE WHEN amount<0 THEN ABS(amount) ELSE 0 END), 0) AS expense,
                COALESCE(SUM(amount), 0) AS net
            FROM ledger
            WHERE is_deleted=0
              AND substr(entry_date,1,7) = ?
        """, [month]).fetchone()

        # AR: outstanding invoices
        ar = conn.execute("""
            SELECT COALESCE(SUM(balance_due), 0) AS total
            FROM invoices
            WHERE is_deleted=0
              AND status NOT IN ('Paid','Void','Draft')
        """).fetchone()

        # Bank account totals
        banks = conn.execute("""
            SELECT account_name, account_type, current_balance
            FROM bank_accounts
            WHERE is_deleted=0
            ORDER BY account_type, account_name
        """).fetchall()

        bank_total = sum(float(b['current_balance'] or 0) for b in banks)

        return jsonify({
            'year':  year,
            'month': month,
            'ytd': {
                'income':  round(float(ytd['income']),  2),
                'expense': round(float(ytd['expense']), 2),
                'net':     round(float(ytd['net']),     2),
            },
            'mtd': {
                'income':  round(float(mtd['income']),  2),
                'expense': round(float(mtd['expense']), 2),
                'net':     round(float(mtd['net']),     2),
            },
            'ar_outstanding': round(float(ar['total']), 2),
            'bank_total':     round(bank_total, 2),
            'banks': [
                {'name':    b['account_name'],
                 'type':    b['account_type'],
                 'balance': round(float(b['current_balance'] or 0), 2)}
                for b in banks
            ],
        })
    finally:
        conn.close()


@app.route('/api/undo/clear', methods=['POST'])
def api_undo_clear():
    """Soft-clear all undo history (mark all as reversed=1)."""
    conn = get_connection()
    try:
        conn.execute("UPDATE undo_log SET reversed=1 WHERE reversed=0")
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


# ============================================================
# PLACEHOLDER ROUTES (stubs for Phases 2–10)
# ============================================================

@app.route('/ledger')
def ledger():
    return redirect(url_for('phase3.ledger'))

@app.route('/timesheet')
def timesheet():
    return redirect(url_for('phase4.timesheet'))

@app.route('/invoices')
def invoices():
    return redirect(url_for('phase5.invoices'))

@app.route('/clients')
def clients():
    return redirect(url_for('phase2.clients'))

@app.route('/employees')
def employees():
    return redirect(url_for('phase2.employees'))

@app.route('/contractors')
def contractors():
    return redirect(url_for('phase2.contractors'))

@app.route('/payroll')
def payroll():
    return redirect(url_for('phase6.payroll'))

@app.route('/reconciliation')
def reconciliation():
    return redirect(url_for('phase8.reconciliation'))

@app.route('/reports')
def reports_redirect():
    return redirect(url_for('phase10.reports'))

@app.route('/tax')
def tax():
    return redirect(url_for('phase7.tax'))

@app.route('/certs')
def certs_redirect():
    return redirect(url_for('phase11.certs'))

@app.route('/quote')
def quote_redirect():
    return redirect(url_for('phase11.quick_quote'))



# ============================================================
# PHASE 12 — FILE SERVING (Receipts & Certs)
# ============================================================

@app.route('/files/receipts/<path:filename>')
def serve_receipt(filename):
    """Serve a receipt PDF/image from the configured receipts folder.
    Tries exact match, then prefix match (file stored without extension).
    """
    config = get_config()
    folder = config.get('receipts_folder_path', '')
    if not folder or not os.path.isdir(folder):
        return ('Receipt folder not configured in Settings.', 404)
    # Exact match
    filepath = os.path.join(folder, filename)
    if os.path.isfile(filepath):
        return send_file(filepath)
    # Prefix match — find file with any extension
    base = os.path.basename(filename)
    try:
        for f in os.listdir(folder):
            name_no_ext = os.path.splitext(f)[0]
            if name_no_ext == base or f == base or f.startswith(base + '.'):
                return send_file(os.path.join(folder, f))
    except Exception:
        pass
    return (f'Receipt file not found: {filename}', 404)


@app.route('/files/certs/<path:filename>')
def serve_cert_file(filename):
    """Serve a cert PDF from the configured certs folder."""
    config = get_config()
    folder = config.get('certs_folder_path', '')
    if not folder or not os.path.isdir(folder):
        return ('Certs folder not configured.', 404)
    filepath = os.path.join(folder, filename)
    if not os.path.isfile(filepath):
        return (f'Cert file not found: {filename}', 404)
    return send_file(filepath)


# ============================================================
# PHASE 12 — CERT PDF SCAN (match PDFs in certs folder to cert records)
# ============================================================

def _scan_cert_pdfs():
    """
    Scan certs_folder_path for PDF files.

    Two operations:
    1. For PDFs matching an existing contractor name: link PDF to cert record.
    2. For PDFs with format "CompanyName MM-DD-YY_MM-DD-YY.pdf": if a matching contractor
       exists but has no cert, AUTO-CREATE a cert record with the parsed dates.
    """
    import re as _re
    config = get_config()
    folder = config.get('certs_folder_path', '')
    if not folder or not os.path.isdir(folder):
        return 0

    pdfs = []
    for root, dirs, files in os.walk(folder):
        for fname in files:
            if fname.lower().endswith('.pdf'):
                rel = os.path.relpath(os.path.join(root, fname), folder)
                pdfs.append((rel, fname))
    if not pdfs:
        return 0

    conn = get_connection()
    try:
        def normalize(s):
            return _re.sub(r'[^a-z0-9]', '', (s or '').lower())

        def parse_cert_dates(stem):
            """Parse 'CompanyName MM-DD-YY_MM-DD-YY' → (company, start, end).
            Handles 1-2 digit months/days, 2 OR 4 digit years."""
            m = _re.search(r'(.+?)\s+(\d{1,2}-\d{1,2}-\d{2,4})_(\d{1,2}-\d{1,2}-\d{2,4})$', stem)
            if m:
                def pd(s):
                    pts = s.split('-')
                    if len(pts) != 3:
                        return ''
                    mm, dd, yy = pts
                    try:
                        yr = int(yy)
                        if yr < 100:   # 2-digit → 4-digit year
                            yr += 2000 if yr < 50 else 1900
                        return f'{yr:04d}-{int(mm):02d}-{int(dd):02d}'
                    except (ValueError, TypeError):
                        return ''
                return m.group(1).strip(), pd(m.group(2)), pd(m.group(3))
            return stem, '', ''

        contractors = conn.execute(
            "SELECT id, company_name FROM contractors WHERE is_deleted=0"
        ).fetchall()

        certs = conn.execute("""
            SELECT c.id, c.contractor_id, c.cert_pdf_filename, con.company_name
            FROM certificates c
            LEFT JOIN contractors con ON c.contractor_id = con.id
            WHERE c.is_deleted=0
        """).fetchall()

        # Build normalized contractor name → id lookup
        con_lookup = {}
        for con in contractors:
            n = normalize(con['company_name'] or '')
            if n: con_lookup[n] = con['id']
            first = normalize((con['company_name'] or '').split()[0])
            if first and first not in con_lookup:
                con_lookup[first] = con['id']

        cert_by_con = {}
        for cert in certs:
            cid = cert['contractor_id']
            cert_by_con.setdefault(cid, []).append(cert)

        matched = created = 0
        unmatched_names = []

        for rel_path, filename in pdfs:
            stem = os.path.splitext(filename)[0]
            company_part, start_date, end_date = parse_cert_dates(stem)
            norm_company = normalize(company_part)

            # Find matching contractor
            con_id = con_lookup.get(norm_company)
            if con_id is None:
                for norm_name, cid in con_lookup.items():
                    if len(norm_company) >= 4 and len(norm_name) >= 4:
                        if norm_company in norm_name or norm_name in norm_company:
                            con_id = cid
                            break

            if con_id is None:
                # Auto-create contractor from PDF filename if not found
                if company_part and start_date:
                    try:
                        cur = conn.execute("""
                            INSERT INTO contractors
                                (company_name, vendor_type, is_deleted, created_at, updated_at)
                            VALUES (?, 'Subcontractor', 0, datetime('now'), datetime('now'))
                        """, [company_part])
                        con_id = cur.lastrowid
                        # Add to lookup so subsequent PDFs for same company match
                        n = normalize(company_part)
                        if n: con_lookup[n] = con_id
                        first_word = normalize(company_part.split()[0])
                        if first_word and first_word not in con_lookup:
                            con_lookup[first_word] = con_id
                        # Refresh cert_by_con entry
                        cert_by_con.setdefault(con_id, [])
                    except Exception:
                        unmatched_names.append(company_part)
                        continue
                else:
                    unmatched_names.append(company_part)
                    continue

            existing = cert_by_con.get(con_id, [])

            # Skip if this exact PDF is already linked
            if any(c['cert_pdf_filename'] == rel_path for c in existing):
                matched += 1
                continue

            # Skip if a cert with the same end_date already exists for this contractor
            # (prevents duplicate COIs from repeated scans; same company can have
            # multiple COIs but each must have a distinct end_date)
            if end_date and any(c['end_date'] == end_date for c in existing):
                matched += 1
                continue

            # Try to fill an unlinked cert record first (user pre-created cert with no PDF)
            # Only fill if the dates are compatible (both blank, or matching)
            unlinked = [c for c in existing if not c['cert_pdf_filename']
                        and (not c.get('end_date') or not end_date or c.get('end_date') == end_date)]
            if unlinked:
                cert_id = unlinked[0]['id']
                sets, vals = ["cert_pdf_filename=?", "cert_verified=1"], [rel_path]
                if start_date: sets.append("start_date=?"); vals.append(start_date)
                if end_date:   sets.append("end_date=?");   vals.append(end_date)
                conn.execute(
                    f"UPDATE certificates SET {', '.join(sets)}, updated_at=datetime('now') WHERE id=?",
                    vals + [cert_id]
                )
                # Update in-memory cache so later PDFs for same contractor don't re-fill
                unlinked[0]['cert_pdf_filename'] = rel_path
                unlinked[0]['end_date'] = end_date
                matched += 1
            else:
                con_row = conn.execute("SELECT company_name FROM contractors WHERE id=?", [con_id]).fetchone()
                con_name = con_row['company_name'] if con_row else company_part
                conn.execute("""
                    INSERT INTO certificates
                        (contractor_id, company_name, cert_type, cert_pdf_filename,
                         start_date, end_date, cert_verified, created_at, updated_at)
                    VALUES (?, ?, 'COI', ?, ?, ?, 1, datetime('now'), datetime('now'))
                """, [con_id, con_name, rel_path, start_date, end_date])
                # Add to in-memory cache so this PDF won't be re-processed this run
                cert_by_con.setdefault(con_id, []).append({
                    'cert_pdf_filename': rel_path,
                    'end_date': end_date,
                    'id': None  # rowid not tracked here, that's fine
                })
                created += 1
                matched += 1

        # Re-verify already-linked PDFs exist on disk
        for cert in certs:
            if cert['cert_pdf_filename']:
                exists = os.path.isfile(os.path.join(folder, cert['cert_pdf_filename']))
                conn.execute("UPDATE certificates SET cert_verified=? WHERE id=?",
                             [1 if exists else 0, cert['id']])

        conn.commit()
        return {'matched': matched, 'created': created, 'unmatched': unmatched_names}
    finally:
        conn.close()


@app.route('/api/certs/scan-pdfs', methods=['POST'])
def api_scan_cert_pdfs():
    result = _scan_cert_pdfs()
    if isinstance(result, dict):
        return jsonify({
            'success': True,
            'matched': result['matched'],
            'created': result['created'],
            'unmatched': result.get('unmatched', []),
        })
    return jsonify({'success': True, 'matched': result, 'created': 0, 'unmatched': []})


@app.route('/api/certs/<int:cert_id>/set-pdf', methods=['POST'])
def api_cert_set_pdf(cert_id):
    """Manually assign a PDF filename to a cert record."""
    data     = request.json or {}
    filename = (data.get('filename') or '').strip()
    config   = get_config()
    folder   = config.get('certs_folder_path', '')
    verified = 1 if folder and os.path.isfile(os.path.join(folder, filename)) else 0
    with db() as conn:
        conn.execute(
            "UPDATE certificates SET cert_pdf_filename=?, cert_verified=? WHERE id=? AND is_deleted=0",
            [filename, verified, cert_id]
        )
    return jsonify({'success': True, 'verified': bool(verified)})


@app.route('/api/certs/available-pdfs')
def api_available_cert_pdfs():
    """Return list of PDF files in the certs folder."""
    config = get_config()
    folder = config.get('certs_folder_path', '')
    if not folder or not os.path.isdir(folder):
        return jsonify({'pdfs': [], 'folder': folder})
    pdfs = sorted([f for f in os.listdir(folder) if f.lower().endswith('.pdf')])
    return jsonify({'pdfs': pdfs, 'folder': folder})


# ============================================================
# PHASE 12 — HISTORY / UNDO
# ============================================================

@app.route('/history')
def history():
    config = get_config()
    badges = get_nav_badges()
    conn   = get_connection()
    try:
        table_f  = request.args.get('table', '')
        action_f = request.args.get('action', '')
        page     = max(1, int(request.args.get('page', 1)))
        per_page = 50

        where  = ["reversed=0"]
        params = []
        if table_f:
            where.append("table_name=?"); params.append(table_f)
        if action_f:
            where.append("action=?"); params.append(action_f)

        total = conn.execute(
            f"SELECT COUNT(*) FROM undo_log WHERE {' AND '.join(where)}", params
        ).fetchone()[0]

        rows = conn.execute(f"""
            SELECT * FROM undo_log
            WHERE {' AND '.join(where)}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
        """, params + [per_page, (page-1)*per_page]).fetchall()

        entries = []
        for r in rows:
            d        = dict(r)
            old_data = json.loads(d.get('old_data') or '{}')
            new_data = json.loads(d.get('new_data') or '{}')
            # Build a human-readable summary of what changed
            summary  = _history_summary(d['table_name'], d['action'], old_data, new_data)
            d['summary']  = summary
            d['old_data'] = old_data
            d['new_data'] = new_data
            entries.append(d)

        tables = conn.execute(
            "SELECT DISTINCT table_name FROM undo_log ORDER BY table_name"
        ).fetchall()

        return render_template('history.html',
            config=config, badges=badges,
            entries=entries,
            total=total, page=page, per_page=per_page,
            pages=max(1, (total + per_page - 1) // per_page),
            table_f=table_f, action_f=action_f,
            tables=[r['table_name'] for r in tables],
        )
    finally:
        conn.close()


def _history_summary(table, action, old, new):
    """Generate a detailed human-readable description of a change.
    For UPDATEs, shows each changed field with its before→after value inline.
    """
    # Human-friendly field name overrides
    FIELD_LABELS = {
        'vendor': 'Vendor', 'date': 'Date', 'amount': 'Amount',
        'income': 'Income', 'expense': 'Expense', 'description': 'Description',
        'memo': 'Memo', 'job_id': 'Job', 'category': 'Category',
        'project': 'Project', 'payment_method': 'Payment',
        'invoice_number': 'Invoice #', 'invoice_date': 'Date',
        'total_amount': 'Total', 'balance_due': 'Balance',
        'cert_type': 'Cert Type', 'end_date': 'Expiry', 'start_date': 'Start',
        'company_name': 'Company', 'trade_type': 'Trade',
        'full_name': 'Name', 'email': 'Email', 'phone': 'Phone',
        'job_code': 'Job Code', 'first_name': 'First', 'last_name': 'Last',
        'is_deleted': 'Deleted', 'status': 'Status',
    }
    # Fields that identify the record for the label prefix
    label_map = {
        'ledger':       ('date', 'vendor', 'amount'),
        'invoices':     ('invoice_date', 'invoice_number', 'amount'),
        'certificates': ('cert_type', 'end_date', None),
        'job_estimates':('estimate_date', 'estimate_number', 'total_estimate'),
        'clients':      ('full_name', 'customer_id', None),
        'contractors':  ('company_name', 'trade_type', None),
        'jobs':         ('job_code', 'description', None),
        'employees':    ('first_name', 'last_name', None),
    }
    # Fields never worth showing in diffs
    SKIP = {'updated_at', 'created_at', 'id', 'session_id', 'user_label',
            'duplicate_flag', 'receipt_verified', 'coi_verified'}

    fields = label_map.get(table, (None, None, None))
    src    = new if action == 'INSERT' else old
    parts  = [f for f in fields if f and src.get(f)]
    label  = " — ".join(str(src[f]) for f in parts if src.get(f)) if parts else f"Record #{src.get('id', '?')}"

    if action == 'INSERT':
        # Show key non-null fields
        highlights = []
        for k, v in new.items():
            if k in SKIP or v is None or v == '' or v == 0: continue
            hl = FIELD_LABELS.get(k, k)
            highlights.append(f"{hl}: {v}")
            if len(highlights) >= 4: break
        detail = f" ({', '.join(highlights)})" if highlights else ''
        return f"Created {label}{detail}"

    elif action == 'DELETE':
        return f"Deleted {label}"

    else:  # UPDATE
        changed = []
        for k in sorted(set(list(old.keys()) + list(new.keys()))):
            if k in SKIP: continue
            ov, nv = old.get(k), new.get(k)
            if ov == nv: continue
            # Skip null→null or empty→empty
            if (not ov and ov != 0) and (not nv and nv != 0): continue
            fname = FIELD_LABELS.get(k, k)
            ov_s = str(ov) if ov is not None else '—'
            nv_s = str(nv) if nv is not None else '—'
            # Truncate long values
            if len(ov_s) > 40: ov_s = ov_s[:37] + '…'
            if len(nv_s) > 40: nv_s = nv_s[:37] + '…'
            changed.append(f"{fname}: {ov_s} → {nv_s}")
        if changed:
            return f"{label} · " + "; ".join(changed[:5]) + ("…" if len(changed) > 5 else "")
        return f"Updated {label} (no data changes recorded)"


@app.route('/api/history/<int:log_id>/revert', methods=['POST'])
def api_history_revert(log_id):
    """Revert a single undo_log entry by restoring old_data."""
    conn = get_connection()
    try:
        entry = conn.execute(
            "SELECT * FROM undo_log WHERE id=? AND reversed=0", [log_id]
        ).fetchone()
        if not entry:
            return jsonify({'error': 'Log entry not found or already reversed'}), 404

        table    = entry['table_name']
        record_id = entry['record_id']
        action   = entry['action']
        old_data = json.loads(entry['old_data'] or '{}')

        # Whitelist of revertable tables
        REVERTABLE = {'ledger', 'invoices', 'certificates', 'job_estimates', 'clients',
                      'contractors', 'jobs', 'employees', 'payroll_records',
                      'bank_transactions', 'timesheet', 'job_milestones',
                      'program_sessions'}
        if table not in REVERTABLE:
            return jsonify({'error': f'Revert not supported for table: {table}'}), 400

        with db() as wconn:
            if action == 'DELETE' and old_data:
                # Restore the row
                old_data.pop('id', None)
                old_data['is_deleted'] = 0
                old_data['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cols = ', '.join(old_data.keys())
                placeholders = ', '.join(['?'] * len(old_data))
                wconn.execute(
                    f"INSERT OR REPLACE INTO {table} (id, {cols}) VALUES (?, {placeholders})",
                    [record_id] + list(old_data.values())
                )

            elif action in ('UPDATE', 'INSERT') and old_data:
                # Restore field values from old_data
                safe_fields = {k: v for k, v in old_data.items()
                               if k not in ('id', 'created_at')}
                safe_fields['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if safe_fields:
                    set_clause = ', '.join(f"{k}=?" for k in safe_fields)
                    wconn.execute(
                        f"UPDATE {table} SET {set_clause} WHERE id=?",
                        list(safe_fields.values()) + [record_id]
                    )

            # Mark as reversed
            wconn.execute(
                "UPDATE undo_log SET reversed=1 WHERE id=?", [log_id]
            )
            # Log the revert itself
            log_action(wconn, table, record_id, 'UPDATE',
                       old_data={'reverted_log': log_id},
                       new_data={'revert_action': action})

        return jsonify({'success': True, 'table': table, 'record_id': record_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


# ============================================================
# PHASE 12 — SETTINGS: continuous_scroll + backup_keep_count
# ============================================================

@app.route('/api/settings/continuous-scroll', methods=['POST'])
def api_toggle_continuous_scroll():
    data  = request.json or {}
    value = 1 if data.get('enabled') else 0
    with db() as conn:
        conn.execute(
            "UPDATE company_config SET continuous_scroll=? WHERE id=1", [value]
        )
    return jsonify({'success': True, 'continuous_scroll': bool(value)})

@app.route('/api/toggle-scroll', methods=['POST'])
def api_toggle_scroll_simple():
    """Toggle continuous scroll on/off (flips current state)."""
    conn = get_connection()
    try:
        cfg = conn.execute("SELECT continuous_scroll FROM company_config WHERE id=1").fetchone()
        current = cfg['continuous_scroll'] if cfg else 0
        new_val = 0 if current else 1
        with db() as c:
            c.execute("UPDATE company_config SET continuous_scroll=? WHERE id=1", [new_val])
        return jsonify({'success': True, 'continuous_scroll': bool(new_val)})
    finally:
        conn.close()

# ERROR HANDLERS
# ============================================================




# ════════════════════════════════════════════════════════════════
#  PROGRAM TIME CLOCK
# ════════════════════════════════════════════════════════════════

_SESSION_ID = None  # current open session id

@app.route('/api/timeclock/start', methods=['POST'])
def api_timeclock_start():
    """Called on page load / activity ping to start/continue a session."""
    global _SESSION_ID
    config = get_config()
    if not config.get('time_tracker_enabled', 0):
        return jsonify({'enabled': False})
    
    conn = get_connection()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if _SESSION_ID is None:
            cur = conn.execute(
                "INSERT INTO program_sessions (start_time) VALUES (?)", [now]
            )
            _SESSION_ID = cur.lastrowid
            conn.commit()
        return jsonify({'enabled': True, 'session_id': _SESSION_ID, 'start_time': now})
    finally:
        conn.close()


@app.route('/api/timeclock/ping', methods=['POST'])
def api_timeclock_ping():
    """Activity ping — updates active_minutes for current session."""
    global _SESSION_ID
    config = get_config()
    if not config.get('time_tracker_enabled', 0) or _SESSION_ID is None:
        return jsonify({'ok': True})
    
    data = request.json or {}
    active_minutes = float(data.get('active_minutes', 0))
    
    conn = get_connection()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute(
            "UPDATE program_sessions SET active_minutes=?, end_time=? WHERE id=?",
            [active_minutes, now, _SESSION_ID]
        )
        conn.commit()
        return jsonify({'ok': True})
    finally:
        conn.close()


@app.route('/api/timeclock/end', methods=['POST'])
def api_timeclock_end():
    """Called when user exits — saves description and finalizes session."""
    global _SESSION_ID
    config = get_config()
    if not config.get('time_tracker_enabled', 0):
        return jsonify({'ok': True})
    
    data = request.json or {}
    description   = data.get('description', '').strip()
    tags          = data.get('tags', '').strip()
    active_minutes = float(data.get('active_minutes', 0))
    
    if _SESSION_ID is None:
        return jsonify({'ok': True})
    
    conn = get_connection()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute("""
            UPDATE program_sessions 
            SET end_time=?, active_minutes=?, description=?, tags=?
            WHERE id=?
        """, [now, active_minutes, description, tags, _SESSION_ID])
        conn.commit()
        _SESSION_ID = None
        return jsonify({'ok': True})
    finally:
        conn.close()




@app.route('/api/timeclock/session/<int:session_id>/edit', methods=['POST'])
def api_timeclock_session_edit(session_id):
    """Edit a program_sessions record."""
    data = request.get_json(silent=True) or {}
    fields, vals = [], []
    for col in ('start_time','end_time','description','tags','active_minutes'):
        if col in data:
            fields.append(f'{col}=?')
            vals.append(data[col] if data[col] != '' else None if col == 'end_time' else data[col])
    if not fields:
        return jsonify({'success': False, 'error': 'Nothing to update'}), 400
    with db() as conn:
        old_row = conn.execute("SELECT * FROM program_sessions WHERE id=?", [session_id]).fetchone()
        conn.execute(
            f"UPDATE program_sessions SET {', '.join(fields)} WHERE id=?",
            vals + [session_id]
        )
        log_action(conn, 'program_sessions', session_id, 'UPDATE',
                   old_data=dict(old_row) if old_row else {},
                   user_label=f'Edited timeclock session #{session_id}')
    return jsonify({'success': True})


@app.route('/api/timeclock/session/<int:session_id>/delete', methods=['POST'])
def api_timeclock_session_delete(session_id):
    """Hard-delete a program_sessions record."""
    with db() as conn:
        old_row = conn.execute("SELECT * FROM program_sessions WHERE id=?", [session_id]).fetchone()
        conn.execute("DELETE FROM program_sessions WHERE id=?", [session_id])
        log_action(conn, 'program_sessions', session_id, 'DELETE',
                   old_data=dict(old_row) if old_row else {},
                   user_label=f'Deleted timeclock session #{session_id}')
    return jsonify({'success': True})

@app.route('/timeclock')
def timeclock_log():
    """View time clock log."""
    config = get_config()
    badges = get_nav_badges()
    conn = get_connection()
    try:
        sessions = conn.execute("""
            SELECT *, 
                   ROUND(active_minutes / 60.0, 2) AS active_hours
            FROM program_sessions 
            ORDER BY start_time DESC 
            LIMIT 200
        """).fetchall()
        total_hours = conn.execute(
            "SELECT COALESCE(SUM(active_minutes),0)/60.0 FROM program_sessions"
        ).fetchone()[0]
        return render_template('timeclock.html',
            config=config, badges=badges,
            sessions=[dict(s) for s in sessions],
            total_hours=total_hours,
        )
    finally:
        conn.close()

@app.route('/api/open-folder', methods=['POST'])
def api_open_folder():
    """Open a folder path in the OS file explorer (Windows Explorer / macOS Finder)."""
    import subprocess, platform
    data = request.json or {}
    path = data.get('path', '').strip()
    if not path:
        return jsonify({'error': 'No path provided'}), 400
    if not os.path.isdir(path):
        return jsonify({'error': f'Folder not found: {path}'}), 404
    try:
        system = platform.system()
        if system == 'Windows':
            subprocess.Popen(['explorer', path])
        elif system == 'Darwin':
            subprocess.Popen(['open', path])
        else:
            subprocess.Popen(['xdg-open', path])
        return jsonify({'success': True, 'path': path})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/job-folder-path')
def api_job_folder_path():
    """Return the expected folder path for a job code."""
    job_code = request.args.get('job_code', '')
    config = get_config()
    base = config.get('active_jobs_folder_path', '')
    if not base:
        return jsonify({'error': 'Jobs folder not configured in Settings'}), 400
    import glob
    # Try exact match first, then fuzzy (job_code prefix)
    exact = os.path.join(base, job_code)
    if os.path.isdir(exact):
        return jsonify({'path': exact, 'exists': True})
    # Search for any folder starting with job_code
    matches = [d for d in os.listdir(base)
               if os.path.isdir(os.path.join(base, d))
               and (d.startswith(job_code) or job_code in d)]
    if matches:
        return jsonify({'path': os.path.join(base, matches[0]), 'exists': True})
    return jsonify({'path': exact, 'exists': False,
                    'message': f'Folder {job_code} not found in {base}'})


@app.route('/api/data/clear-table', methods=['POST'])
def api_clear_table():
    """Clear a specific table (soft-delete all rows) after creating a backup."""
    data  = request.json or {}
    table = data.get('table', '').strip()
    ALLOWED = {'ledger', 'timesheet', 'bank_transactions', 'invoices', 'payroll_runs', 'clients_soft'}
    if table not in ALLOWED:
        return jsonify({'error': f'Cannot clear table: {table}'}), 400
    # Attempt backup (warns but proceeds if backup folder not configured)
    ok, msg = _do_backup('pre-clear')
    backup_note = msg if ok else f'⚠️ No backup created: {msg}'
    real_table = 'clients' if table == 'clients_soft' else table
    with db() as conn:
        count = conn.execute(f"SELECT COUNT(*) FROM {real_table} WHERE is_deleted=0").fetchone()[0]
        conn.execute(f"UPDATE {real_table} SET is_deleted=1 WHERE is_deleted=0")
        log_action(conn, real_table, 0, 'DELETE', old_data={'bulk_clear': count})
    return jsonify({'success': True, 'cleared': count, 'backup': backup_note})


# ══════════════════════════════════════════════════════════════
#  DATA MANAGEMENT — per-table export, import, clear
# ══════════════════════════════════════════════════════════════

def _dm_ledger_import(content: str, mode: str = 'append'):
    """
    Import ledger CSV from the Data Management tab.
    For 'append': routes through the same preview+duplicate-check flow as /ledger/import.
    For 'replace': executes immediately (destructive, no preview needed — backup taken first).
    """
    import uuid as _uuid, tempfile as _tf
    from routes_phase3 import (
        _parse_import_csv, _auto_receipt,
        _check_import_row_duplicate, _get_recurring_patterns,
        _get_bank_accounts,
    )
    from automations import verify_receipt
    from database import get_connection as _gc

    rows, errors = _parse_import_csv(
        content_raw=content,
        skip_header=True,
        default_job='',
        default_acct=None,
        sign_convention='auto',
    )

    if not rows and errors:
        return jsonify({'error': 'CSV parse errors: ' + '; '.join(errors[:3])}), 400

    # ── REPLACE mode: no preview, execute immediately ──────────────────────
    if mode == 'replace':
        _do_backup('pre-import')
        config = get_config()
        receipts_folder = config.get('receipts_folder_path', '')
        imported = skipped = 0
        with db() as conn:
            conn.execute('UPDATE ledger SET is_deleted=1 WHERE is_deleted=0')
            for row in rows:
                try:
                    dm_income  = row.get('income')
                    dm_expense = row.get('expense')
                    dm_amount  = row.get('amount', 0) or 0

                    # Only derive from amount when neither income nor expense set from CSV
                    if dm_income is None and dm_expense is None:
                        if dm_amount > 0:
                            dm_income = dm_amount
                        elif dm_amount < 0:
                            dm_expense = dm_amount  # negative = refund, keep sign

                    # Recompute net amount for consistency
                    if dm_income is not None or dm_expense is not None:
                        dm_amount = (dm_income or 0) - (dm_expense or 0)

                    dm_is_pend = row.get('is_pending', 0)
                    cur = conn.execute("""
                        INSERT INTO ledger
                            (entry_date, job_code, invoice_number, category,
                             description, vendor, is_cogs, amount, income, expense,
                             bank_account_id, notes, status,
                             nickname, memo, type_of_payment, is_pending)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, [
                        row['entry_date'], row.get('job_code',''), row.get('invoice_number',''),
                        row.get('category',''), row.get('description',''), row.get('vendor',''),
                        row.get('is_cogs',0), dm_amount, dm_income, dm_expense,
                        row.get('bank_account_id'), row.get('notes',''),
                        'Pending' if dm_is_pend else (row.get('status','').strip() or 'Pending'),
                        row.get('nickname',''), row.get('memo',''), row.get('type_of_payment',''),
                        dm_is_pend,
                    ])
                    new_id = cur.lastrowid
                    csv_receipt = row.get('receipt_filename','').strip()
                    if csv_receipt:
                        verified = verify_receipt(csv_receipt, receipts_folder)
                        conn.execute("UPDATE ledger SET receipt_filename=?, receipt_verified=? WHERE id=?",
                                     [csv_receipt, 1 if verified else 0, new_id])
                    else:
                        _auto_receipt(conn, new_id, row['entry_date'],
                                      row.get('job_code',''), row.get('vendor',''), row.get('amount',0))
                    imported += 1
                except Exception:
                    skipped += 1
        msg = f"Replaced ledger: imported {imported} entries"
        if skipped: msg += f", skipped {skipped}"
        return jsonify({'success': True, 'inserted': imported, 'skipped': skipped, 'message': msg})

    # ── APPEND mode: run duplicate check then show ledger preview page ─────
    conn = _gc()
    try:
        patterns = _get_recurring_patterns(conn)
        for row in rows:
            row['dup_status'] = _check_import_row_duplicate(conn, row, patterns)
    finally:
        conn.close()

    # Save to temp file (same mechanism as /ledger/import)
    token    = str(_uuid.uuid4())
    tmp_path = os.path.join(_tf.gettempdir(), f'kbweb_import_{token}.json')
    with open(tmp_path, 'w', encoding='utf-8') as tmp:
        import json as _json
        _json.dump(rows, tmp)

    config        = get_config()
    badges        = _get_badges()
    bank_accounts = _get_bank_accounts()

    # Render the same preview page as the ledger import
    from flask import render_template as _rt
    from routes_phase3 import _get_config as _rc
    return _rt('ledger_import_preview.html',
        config=config, badges=badges,
        rows=rows,
        errors=errors,
        default_job='',
        default_acct=None,
        sign_convention='auto',
        bank_accounts=bank_accounts,
        filename='data-management-import.csv',
        import_token=token,
    )



# Maps logical table key → (db_table, label, exportable_cols, has_soft_delete)

# ─────────────────────────────────────────────────────────────────────────────
# Contractor / Supplier import — maps user-friendly CSV columns to DB columns
# ─────────────────────────────────────────────────────────────────────────────
# Map from CSV Tax Category → vendor_type DB value
_TAX_CAT_TO_VENDOR_TYPE = {
    'contract labor':       'Subcontractor',
    'subcontractor':        'Subcontractor',
    'materials & supplies': 'Supplier',
    'materials and supplies':'Supplier',
    'supplier':             'Supplier',
    'professional services':'Service Provider',
    'office expense':       'Service Provider',
    'repairs & maintenance':'Subcontractor',
    'utilities':            'Service Provider',
    'licenses & permits':   'Government/Tax',
    'licenses and permits': 'Government/Tax',
    'insurance':            'Service Provider',
    'other expenses':       'Other',
}

# CSV column → DB column aliases for contractors import
_CONTRACTOR_COL_ALIASES = {
    'type':              'trade_type',
    'trade type':        'trade_type',
    'trade_type':        'trade_type',
    'tax category':      None,          # handled separately → vendor_type
    'tax_category':      None,
    'name':              'company_name',
    'company':           'company_name',
    'company name':      'company_name',
    'company_name':      'company_name',
    'contact person':    'contact_person',
    'contact':           'contact_person',
    'contact_person':    'contact_person',
    'phone':             'phone',
    'cell / alt phone':  'cell',
    'cell/alt phone':    'cell',
    'cell':              'cell',
    'alt phone':         'cell',
    'email':             'email',
    'website':           'website',
    'address':           'address',
    'labels':            'notes',
    'notes':             'notes',
    'license_number':    'license_number',
    'license number':    'license_number',
    'vendor_type':       'vendor_type',
    'requires_1099':     'requires_1099',
    'rank_preference':   'rank_preference',
}


def _dm_contractors_import(content: str, mode: str = 'append'):
    """Import contractors CSV with column alias mapping.
    Handles the user's workbook export format:
    Type, Tax Category, Name, Contact Person, Phone, Cell / Alt Phone, Email, Website, Address, Labels
    """
    import csv as _csv, io as _io
    reader = _csv.DictReader(_io.StringIO(content))
    if not reader.fieldnames:
        return jsonify({'error': 'Empty or invalid CSV'}), 400

    # Normalise fieldnames for lookup
    def norm_key(k): return (k or '').strip().lower()
    csv_fields = {norm_key(f): f for f in reader.fieldnames}

    rows = list(reader)
    if not rows:
        return jsonify({'inserted': 0, 'skipped': 0, 'message': 'No data rows'}), 200

    conn = get_connection()
    try:
        if mode == 'replace':
            _do_backup('pre-import')
            conn.execute('UPDATE contractors SET is_deleted=1 WHERE is_deleted=0')

        inserted = skipped = 0
        for row in rows:
            # Map CSV columns to DB columns using aliases
            mapped = {}
            vendor_type = None

            for csv_col_norm, csv_col_orig in csv_fields.items():
                val = (row.get(csv_col_orig) or '').strip()
                db_col = _CONTRACTOR_COL_ALIASES.get(csv_col_norm)

                if csv_col_norm in ('tax category', 'tax_category'):
                    # Convert tax category → vendor_type
                    vendor_type = _TAX_CAT_TO_VENDOR_TYPE.get(val.lower(), 'Subcontractor')
                    continue

                if db_col is None:
                    continue  # explicitly skipped
                if val:
                    mapped[db_col] = val

            # company_name is required
            if not mapped.get('company_name'):
                skipped += 1
                continue

            # Apply vendor_type — either from Tax Category or default
            if 'vendor_type' not in mapped:
                mapped['vendor_type'] = vendor_type or 'Subcontractor'

            # Validate vendor_type against allowed values
            allowed = {'Subcontractor', 'Supplier', 'Service Provider', 'Government/Tax', 'Other'}
            if mapped['vendor_type'] not in allowed:
                mapped['vendor_type'] = 'Other'

            # Check for duplicate by company_name (avoid re-inserting same contractor)
            if mode == 'append':
                exists = conn.execute(
                    'SELECT id FROM contractors WHERE company_name=? AND is_deleted=0',
                    [mapped['company_name']]
                ).fetchone()
                if exists:
                    skipped += 1
                    continue

            mapped['is_deleted'] = 0

            cols = [c for c in mapped if c not in ('created_at', 'updated_at')]
            vals = [mapped[c] for c in cols]
            col_sql = ', '.join(cols + ['created_at', 'updated_at', 'is_deleted'])
            ph = ', '.join(['?'] * len(vals) + ["datetime('now')", "datetime('now')", '?'])

            try:
                conn.execute(f'INSERT INTO contractors ({col_sql}) VALUES ({ph})', vals + [0])
                inserted += 1
            except Exception:
                skipped += 1

        conn.commit()
        return jsonify({
            'success': True,
            'inserted': inserted,
            'skipped': skipped,
            'message': f'Imported {inserted} contractor(s). {skipped} skipped (duplicates or missing name).'
        })
    except Exception as e:
        return jsonify({'error': f'Import failed: {str(e)}'}), 500
    finally:
        conn.close()

DATA_TABLES = {
    'ledger':          ('ledger',          'Ledger Entries',      True,  True),
    'clients':         ('clients',         'Clients',             True,  True),
    'jobs':            ('jobs',            'Projects/Jobs',       True,  True),
    'contractors':     ('contractors',     'Contractors/Vendors', True,  True),
    'employees':       ('employees',       'Employees',           True,  True),
    'timesheet':       ('timesheet',       'Timesheet',           True,  True),
    'payroll_runs':    ('payroll_runs',    'Payroll Runs',        True,  True),
    'invoices':        ('invoices',        'Invoices',            True,  True),
    'bank_accounts':   ('bank_accounts',   'Bank Accounts',       True,  True),
    'bank_transactions':('bank_transactions','Bank Transactions', True,  True),
    'certificates':    ('certificates',    'COI / Certificates',  True,  True),
    'reminders':       ('reminders',       'Reminders',           True,  True),
    'vendor_categories':('vendor_categories','Vendor Categories', True,  True),
    'work_categories': ('work_categories', 'Work Categories',     True,  True),
    'reconciliation':  ('reconciliation_sessions','Bank Reconciliation', True, True),
}


@app.route('/api/data/table-export')
def api_data_table_export():
    """Export a single table as CSV. Pass empty=1 for headers-only template."""
    key = request.args.get('table', '').strip()
    include_deleted = request.args.get('include_deleted', '0') == '1'
    empty_template  = request.args.get('empty', '0') == '1'
    if key not in DATA_TABLES:
        return jsonify({'error': f'Unknown table: {key}'}), 400

    db_table, label, _, has_delete = DATA_TABLES[key]
    conn = get_connection()
    try:
        cols_info = conn.execute(f'PRAGMA table_info({db_table})').fetchall()
        col_names = [c[1] for c in cols_info]

        if empty_template:
            # Return headers-only CSV as an import template
            def generate_empty():
                yield ','.join(f'"{c}"' for c in col_names) + '\n'
            fname = f'{key}_template.csv'
            return Response(
                stream_with_context(generate_empty()),
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment; filename={fname}'}
            )

        if has_delete and not include_deleted:
            rows = conn.execute(
                f'SELECT * FROM {db_table} WHERE is_deleted=0 ORDER BY id'
            ).fetchall()
        else:
            rows = conn.execute(f'SELECT * FROM {db_table} ORDER BY id').fetchall()

        def generate():
            yield ','.join(f'"{c}"' for c in col_names) + '\n'
            for row in rows:
                yield ','.join(
                    f'"{str(v).replace(chr(34), chr(34)*2)}"' if v is not None else '""'
                    for v in row
                ) + '\n'

        ts = datetime.now().strftime('%Y%m%d_%H%M')
        fname = f'{key}_export_{ts}.csv'
        return Response(
            stream_with_context(generate()),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={fname}'}
        )
    finally:
        conn.close()


@app.route('/api/data/table-import', methods=['POST'])
def api_data_table_import():
    """Import CSV into a table. Inserts all rows, skipping id column to avoid conflicts."""
    import csv as _csv, io as _io
    key = request.form.get('table', '').strip()
    mode = request.form.get('mode', 'append').strip()  # append | replace
    f = request.files.get('file')
    if key not in DATA_TABLES:
        return jsonify({'error': f'Unknown table: {key}'}), 400
    if not f:
        return jsonify({'error': 'No file uploaded'}), 400

    db_table = DATA_TABLES[key][0]
    has_delete = DATA_TABLES[key][3]

    raw = f.stream.read()
    for enc in ('utf-8-sig', 'latin-1', 'cp1252'):
        try:
            content = raw.decode(enc); break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        content = raw.decode('latin-1', errors='replace')

    reader = _csv.DictReader(_io.StringIO(content))
    if not reader.fieldnames:
        return jsonify({'error': 'Empty or invalid CSV'}), 400

    # For the ledger table, delegate to the dedicated ledger import pipeline
    # which handles COL_ALIASES, receipt filenames, auto-generation, etc.
    if key == 'ledger' and db_table == 'ledger':
        return _dm_ledger_import(content, mode)

    # For contractors, delegate to the dedicated import that handles column aliases
    # (Type, Tax Category, Name, Contact Person, etc.)
    if key == 'contractors' and db_table == 'contractors':
        return _dm_contractors_import(content, mode)

    conn = get_connection()
    try:
        # Get actual DB columns
        db_cols_info = conn.execute(f'PRAGMA table_info({db_table})').fetchall()
        db_col_names = [c[1] for c in db_cols_info]
        db_col_set = set(db_col_names)

        # Only import columns that actually exist in the table (skip id, unknown cols)
        import_cols = [c for c in reader.fieldnames
                       if c in db_col_set and c != 'id']

        if not import_cols:
            return jsonify({'error': 'No matching columns found in CSV'}), 400

        if mode == 'replace' and has_delete:
            _do_backup('pre-import')
            conn.execute(f'UPDATE {db_table} SET is_deleted=1 WHERE is_deleted=0')

        inserted = skipped = 0
        placeholder = ','.join('?' * len(import_cols))
        col_list = ','.join(import_cols)
        # INSERT OR REPLACE handles UNIQUE constraint conflicts by updating the
        # existing row rather than skipping it (upsert behaviour)
        sql = f'INSERT OR REPLACE INTO {db_table} ({col_list}) VALUES ({placeholder})'

        first_error = None
        for row in reader:
            vals = []
            for col in import_cols:
                v = row.get(col, '')
                col_type = next((c[2] for c in db_cols_info if c[1] == col), 'TEXT')
                if v == '' or v is None:
                    vals.append(None)
                elif col_type in ('INTEGER', 'REAL'):
                    try:
                        vals.append(int(v) if col_type == 'INTEGER' else float(v))
                    except (ValueError, TypeError):
                        vals.append(None)
                else:
                    vals.append(v)
            try:
                conn.execute(sql, vals)
                inserted += 1
            except Exception as e:
                skipped += 1
                if first_error is None:
                    first_error = str(e)

        conn.commit()
        result = {'success': True, 'inserted': inserted, 'skipped': skipped}
        if skipped and first_error:
            result['skip_reason'] = first_error
        return jsonify(result)
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()




@app.route('/api/data/table-purge', methods=['POST'])
def api_data_table_purge():
    """Permanently hard-delete all soft-deleted (is_deleted=1) rows from a table.
    This frees space and clears the deleted count shown in Data Management.
    A backup is created first."""
    data = request.json or {}
    key  = data.get('table', '').strip()
    if key not in DATA_TABLES:
        return jsonify({'error': f'Unknown table: {key}'}), 400

    db_table, label, _, has_delete = DATA_TABLES[key]
    if not has_delete:
        return jsonify({'error': 'Table does not support soft-delete'}), 400

    # Count deleted rows first (outside transaction)
    conn_check = get_connection()
    try:
        count = conn_check.execute(
            f'SELECT COUNT(*) FROM {db_table} WHERE is_deleted=1'
        ).fetchone()[0]
    finally:
        conn_check.close()

    if count == 0:
        return jsonify({'success': True, 'purged': 0, 'label': label,
                        'message': f'No deleted rows in {label} to purge.'})

    # Backup before destructive operation (caller may opt out)
    skip_backup = data.get('skip_backup', False)
    if skip_backup:
        ok, msg = True, 'skipped'
    else:
        ok, msg = _do_backup('pre-purge')

    # Hard-delete in its own transaction
    # First: null out ALL FK references that point to rows we're about to delete,
    # to avoid FOREIGN KEY constraint violations on any table.
    try:
        with db() as conn:
            # Disable FK enforcement during nulling so we can clean up in any order
            conn.execute('PRAGMA foreign_keys = OFF')
            try:
                if db_table == 'ledger':
                    conn.execute("""
                        UPDATE bank_transactions
                        SET matched_ledger_id = NULL,
                            match_status = CASE WHEN match_status='Matched' THEN 'Unmatched' ELSE match_status END
                        WHERE matched_ledger_id IN (
                            SELECT id FROM ledger WHERE is_deleted=1
                        )
                    """)
                elif db_table == 'employees':
                    conn.execute("""UPDATE payroll_runs       SET emp_id=NULL WHERE emp_id IN (SELECT emp_id FROM employees WHERE is_deleted=1)""")
                elif db_table == 'clients':
                    conn.execute("""UPDATE jobs               SET client_id=NULL WHERE client_id IN (SELECT id FROM clients WHERE is_deleted=1)""")
                    conn.execute("""UPDATE invoices           SET client_id=NULL WHERE client_id IN (SELECT id FROM clients WHERE is_deleted=1)""")
                elif db_table == 'jobs':
                    conn.execute("""UPDATE ledger             SET job_id=NULL    WHERE job_id    IN (SELECT id FROM jobs    WHERE is_deleted=1)""")
                    conn.execute("""UPDATE invoices           SET job_id=NULL    WHERE job_id    IN (SELECT id FROM jobs    WHERE is_deleted=1)""")
                    conn.execute("""UPDATE program_sessions   SET job_id=NULL    WHERE job_id    IN (SELECT id FROM jobs    WHERE is_deleted=1)""")
                elif db_table == 'contractors':
                    conn.execute("""UPDATE certificates       SET contractor_id=NULL WHERE contractor_id IN (SELECT id FROM contractors WHERE is_deleted=1)""")
                elif db_table == 'bank_accounts':
                    conn.execute("""UPDATE bank_transactions  SET bank_account_id=NULL WHERE bank_account_id IN (SELECT id FROM bank_accounts WHERE is_deleted=1)""")
                    conn.execute("""UPDATE bank_statements    SET bank_account_id=NULL WHERE bank_account_id IN (SELECT id FROM bank_accounts WHERE is_deleted=1)""")
                conn.execute(f'DELETE FROM {db_table} WHERE is_deleted=1')
            finally:
                conn.execute('PRAGMA foreign_keys = ON')
    except Exception as e:
        return jsonify({'error': f'Purge failed: {str(e)}'}), 500

    backup_note = '' if ok else f' (⚠ no backup: {msg})'
    return jsonify({'success': True, 'purged': count, 'label': label,
                    'message': f'Permanently deleted {count} row(s) from {label}.{backup_note}'})

@app.route('/api/data/table-clear', methods=['POST'])
def api_data_table_clear():
    """Soft-delete all rows in a table (or hard-delete for tables without soft-delete)."""
    data = request.json or {}
    key = data.get('table', '').strip()
    hard = data.get('hard', False)
    if key not in DATA_TABLES:
        return jsonify({'error': f'Unknown table: {key}'}), 400

    db_table, label, _, has_delete = DATA_TABLES[key]
    _do_backup('pre-clear')

    with db() as conn:
        if has_delete and not hard:
            count = conn.execute(
                f'SELECT COUNT(*) FROM {db_table} WHERE is_deleted=0'
            ).fetchone()[0]
            conn.execute(f'UPDATE {db_table} SET is_deleted=1 WHERE is_deleted=0')
        else:
            count = conn.execute(f'SELECT COUNT(*) FROM {db_table}').fetchone()[0]
            conn.execute(f'DELETE FROM {db_table}')

    return jsonify({'success': True, 'cleared': count, 'label': label})


@app.route('/api/data/table-restore', methods=['POST'])
def api_data_table_restore():
    """Restore soft-deleted rows (set is_deleted=0)."""
    data = request.json or {}
    key = data.get('table', '').strip()
    if key not in DATA_TABLES:
        return jsonify({'error': f'Unknown table: {key}'}), 400
    db_table, label, _, has_delete = DATA_TABLES[key]
    if not has_delete:
        return jsonify({'error': 'Table does not support soft-delete'}), 400

    with db() as conn:
        count = conn.execute(
            f'SELECT COUNT(*) FROM {db_table} WHERE is_deleted=1'
        ).fetchone()[0]
        conn.execute(f'UPDATE {db_table} SET is_deleted=0 WHERE is_deleted=1')

    return jsonify({'success': True, 'restored': count, 'label': label})


@app.route('/api/data/table-stats')
def api_data_table_stats():
    """Return row counts for all managed tables."""
    conn = get_connection()
    try:
        stats = {}
        for key, (db_table, label, _, has_delete) in DATA_TABLES.items():
            try:
                total = conn.execute(f'SELECT COUNT(*) FROM {db_table}').fetchone()[0]
                active = conn.execute(
                    f'SELECT COUNT(*) FROM {db_table} WHERE is_deleted=0'
                ).fetchone()[0] if has_delete else total
                deleted = total - active if has_delete else 0
            except Exception:
                total = active = deleted = 0
            stats[key] = {
                'label': label, 'total': total,
                'active': active, 'deleted': deleted
            }
        return jsonify(stats)
    finally:
        conn.close()


@app.route('/api/backup/selective', methods=['POST'])
def api_backup_selective():
    """Create a backup ZIP with user-selected tables exported as CSVs."""
    data = request.json or {}
    selected_tables = data.get('tables', list(DATA_TABLES.keys()))
    include_db = data.get('include_db', True)
    include_receipts = data.get('include_receipts', False)
    include_certs = data.get('include_certs', False)
    label = data.get('label', 'selective')

    config = get_config()
    backup_root = config.get('backup_folder_path', '')
    if not backup_root:
        return jsonify({'error': 'Backup folder not configured in Settings.'}), 400

    os.makedirs(backup_root, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_name = f'kbweb_backup_{label}_{ts}.zip'
    zip_path = os.path.join(backup_root, zip_name)

    import csv as _csv, io as _io
    conn = get_connection()
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Database file
            if include_db and os.path.isfile(DB_PATH):
                zf.write(DB_PATH, f'db/{os.path.basename(DB_PATH)}')

            # Selected table CSVs
            for key in selected_tables:
                if key not in DATA_TABLES:
                    continue
                db_table, tbl_label, _, has_delete = DATA_TABLES[key]
                try:
                    cols_info = conn.execute(f'PRAGMA table_info({db_table})').fetchall()
                    col_names = [c[1] for c in cols_info]
                    rows = conn.execute(
                        f'SELECT * FROM {db_table} WHERE is_deleted=0 ORDER BY id'
                        if has_delete else
                        f'SELECT * FROM {db_table} ORDER BY id'
                    ).fetchall()
                    buf = _io.StringIO()
                    w = _csv.writer(buf)
                    w.writerow(col_names)
                    for r in rows:
                        w.writerow([v if v is not None else '' for v in r])
                    zf.writestr(f'tables/{key}.csv', buf.getvalue())
                except Exception as e:
                    zf.writestr(f'tables/{key}_error.txt', str(e))

            # Receipts
            receipts_folder = config.get('receipts_folder_path', '')
            rcpt_count = 0
            if include_receipts and receipts_folder and os.path.isdir(receipts_folder):
                for fpath in glob.glob(os.path.join(receipts_folder, '**', '*'), recursive=True):
                    if os.path.isfile(fpath):
                        zf.write(fpath, 'receipts/' + os.path.relpath(fpath, receipts_folder))
                        rcpt_count += 1

            # Certs
            certs_folder = config.get('certs_folder_path', '')
            cert_count = 0
            if include_certs and certs_folder and os.path.isdir(certs_folder):
                for fpath in glob.glob(os.path.join(certs_folder, '**', '*'), recursive=True):
                    if os.path.isfile(fpath):
                        zf.write(fpath, 'certs/' + os.path.relpath(fpath, certs_folder))
                        cert_count += 1

            # Manifest
            zf.writestr('manifest.txt',
                f"KB Construction Manager — Selective Backup\n"
                f"Created  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Label    : {label}\n"
                f"Tables   : {', '.join(selected_tables)}\n"
                f"DB file  : {'yes' if include_db else 'no'}\n"
                f"Receipts : {rcpt_count} files\n"
                f"Certs    : {cert_count} files\n"
            )

        size_mb = os.path.getsize(zip_path) / (1024 * 1024)
        return jsonify({
            'success': True,
            'filename': zip_name,
            'size_mb': round(size_mb, 2),
            'tables': selected_tables,
        })
    except Exception as e:
        try: os.unlink(zip_path)
        except OSError: pass
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/shutdown', methods=['POST'])
def api_shutdown():
    """Graceful shutdown — save and exit."""
    import threading
    def _shutdown():
        import time; time.sleep(0.5)
        import os, signal
        os.kill(os.getpid(), signal.SIGTERM)
    threading.Thread(target=_shutdown, daemon=True).start()
    return jsonify({'success': True, 'message': 'Shutting down...'})

@app.errorhandler(404)
def not_found(e):
    config = get_config()
    try:
        badges = get_nav_badges()
    except Exception:
        badges = {}
    return render_template('error.html', config=config, badges=badges,
                           error=404, message="Page not found"), 404

@app.errorhandler(500)
def server_error(e):
    import traceback, logging
    logging.error("500 error: %s\n%s", e, traceback.format_exc())
    config = get_config()
    try:
        badges = get_nav_badges()
    except Exception:
        badges = {}
    return render_template('error.html', config=config, badges=badges,
                           error=500, message=str(e)), 500


if __name__ == '__main__':
    db_path = os.environ.get('CONSTRUCTION_DB', 'construction.db')
    init_db(db_path)
    print("=" * 60)
    print("  Construction Business Management App")
    print(f"  Database: {db_path}")
    print("  Open: http://localhost:5000")

    # Auto-backup on startup — only if auto_backup_mode allows it
    try:
        _ab_conn = get_connection()
        _ab_mode = (_ab_conn.execute(
            "SELECT auto_backup_mode FROM company_config LIMIT 1"
        ).fetchone() or {})  
        _ab_mode = (_ab_mode['auto_backup_mode'] if hasattr(_ab_mode, '__getitem__') and 'auto_backup_mode' in _ab_mode.keys() else None) or 'startup'
        _ab_conn.close()
    except Exception:
        _ab_mode = 'startup'
    if _ab_mode in ('startup', 'both'):
        ok, msg = _do_backup('startup')
        if ok:  print(f"  ✅ Auto-backup: {msg}")
        else:   print(f"  ⚠️  Auto-backup skipped: {msg}")
    else:
        print(f"  ℹ️  Auto-backup skipped (mode: {_ab_mode})")

    # Auto-verify receipts (scan folder vs DB)
    try:
        verified = verify_all_receipts()
        print(f"  📎 Receipt scan: {verified} entries checked")
    except Exception as e:
        print(f"  ⚠️  Receipt scan skipped: {e}")

    # COI scan removed from startup — use Settings → Scan COI PDFs button instead

    print("=" * 60)
    # use_reloader=False prevents Flask from running startup code twice
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
