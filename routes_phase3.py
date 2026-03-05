"""
Phase 3 Routes — Ledger
Full general ledger with:
  - Paginated, filterable list
  - Inline row editing (all fields editable without leaving the page)
  - Auto-fill: vendor→category, receipt filename, receipt verification
  - Bulk import via CSV (spending/income)
  - Monthly summary + job-cost breakdown
  - Export to CSV
  - YTD income/expense totals in header
  - Undo on every write
"""
import csv
import io
import json
import os
import uuid
from datetime import datetime, date
from flask import (Blueprint, render_template, request, jsonify,
                   redirect, url_for, flash, Response, stream_with_context)

from database import db, get_connection
from automations import (
    generate_receipt_filename, verify_receipt, verify_all_receipts,
    get_vendor_category, save_vendor_category,
    log_action, soft_delete,
)

phase3 = Blueprint('phase3', __name__)

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _get_config(conn=None):
    close = False
    if conn is None:
        conn = get_connection(); close = True
    try:
        row = conn.execute("SELECT * FROM company_config WHERE id=1").fetchone()
        return dict(row) if row else {}
    finally:
        if close: conn.close()

def _get_badges():
    from app import get_nav_badges
    return get_nav_badges()

def _receipts_folder():
    cfg = _get_config()
    return cfg.get('receipts_folder_path', '')

def _auto_receipt(conn, row_id: int, entry_date: str, job_code: str,
                  vendor: str, amount: float):
    """Generate receipt filename + verify it exists. Updates the ledger row."""
    fname = generate_receipt_filename(entry_date, job_code, vendor, amount)
    verified = verify_receipt(fname, _receipts_folder()) if fname else False
    conn.execute("""
        UPDATE ledger SET receipt_filename=?, receipt_verified=?,
                          updated_at=datetime('now')
        WHERE id=?
    """, [fname, 1 if verified else 0, row_id])
    return fname, verified

def _is_cogs_for_category(category: str, conn) -> int:
    row = conn.execute(
        "SELECT is_cogs FROM work_categories WHERE category_name=? AND is_deleted=0 LIMIT 1",
        [category]
    ).fetchone()
    return row['is_cogs'] if row else 0


# ════════════════════════════════════════════════════════════════
#  LEDGER LIST
# ════════════════════════════════════════════════════════════════

def _build_ledger_where(args):
    """
    Build WHERE clause + params from request args.
    Supports multi-value params: year=2024&year=2025 etc.
    Returns (where_list, params, active_filters_dict)
    active_filters_dict maps filter_key → list of active values (for UI state).
    """
    where = ["l.is_deleted=0"]
    params = []
    active = {}

    q = args.get('q', '').strip()
    if q:
        where.append("(l.vendor LIKE ? OR l.description LIKE ? OR l.invoice_number LIKE ? OR l.job_code LIKE ? OR l.nickname LIKE ?)")
        params += [f"%{q}%"] * 5

    # Multi-value helpers
    def multi(key, col_expr):
        vals = [v.strip() for v in args.getlist(key) if v.strip()]
        if vals:
            active[key] = vals
            ph = ','.join('?' * len(vals))
            where.append(f"{col_expr} IN ({ph})")
            params.extend(vals)

    multi('year',    "substr(l.entry_date,1,4)")
    multi('job',     "l.job_code")
    multi('cat',     "l.category")
    multi('vendor',  "l.vendor")
    multi('payment', "l.type_of_payment")
    multi('acct',    "CAST(l.bank_account_id AS TEXT)")

    # New filters
    multi('nickname', "l.nickname")
    multi('invoice',  "l.invoice_number")

    # Description: partial match multi
    desc_vals = [v.strip() for v in args.getlist('desc') if v.strip()]
    if desc_vals:
        active['desc'] = desc_vals
        clauses = ' OR '.join(['l.description LIKE ?' for _ in desc_vals])
        where.append(f"({clauses})")
        params.extend([f'%{v}%' for v in desc_vals])

    sign = args.get('sign', '').strip()
    if sign == 'income':
        active['sign'] = ['income']
        where.append("l.amount > 0")
    elif sign == 'expense':
        active['sign'] = ['expense']
        where.append("l.amount < 0")

    receipt = args.get('receipt', '').strip()
    if receipt:
        active['receipt'] = [receipt]
        if receipt == 'missing':
            where.append("(l.receipt_filename='' OR l.receipt_filename IS NULL)")
        elif receipt == 'unverified':
            where.append("l.receipt_filename!='' AND l.receipt_verified=0")
        elif receipt == 'needs_receipt':
            where.append("(l.receipt_filename='' OR l.receipt_filename IS NULL OR l.receipt_verified=0)")
        elif receipt == 'verified':
            where.append("l.receipt_verified=1")

    # Receipt verified filter
    rcpt_verified_vals = [v.strip() for v in args.getlist('rcpt_verified') if v.strip()]
    if rcpt_verified_vals:
        active['rcpt_verified'] = rcpt_verified_vals
        ph = ','.join('?' * len(rcpt_verified_vals))
        where.append(f"CAST(l.receipt_verified AS TEXT) IN ({ph})")
        params.extend(rcpt_verified_vals)

    # COI verified filter
    coi_vals = [v.strip() for v in args.getlist('coi_verified') if v.strip()]
    if coi_vals:
        active['coi_verified'] = coi_vals
        ph = ','.join('?' * len(coi_vals))
        where.append(f"CAST(l.coi_verified AS TEXT) IN ({ph})")
        params.extend(coi_vals)

    return where, params, active, q


@phase3.route('/ledger')
def ledger():
    config = _get_config()
    badges = _get_badges()
    conn = get_connection()
    try:
        # ── Filter params (multi-value aware) ──────────────────
        where, params, active_filters, q = _build_ledger_where(request.args)
        # Remove duplicate year clause (added by both helper and manual block — clean up)
        # The _build_ledger_where already handles everything; we just removed the old multi() call for year
        where_sql = " AND ".join(where)

        page     = max(1, int(request.args.get('page', 1)))
        per_page = int(request.args.get('per_page', 50))
        sort_col = request.args.get('sort', 'entry_date')
        sort_dir = request.args.get('dir', 'desc')
        _SORTABLE = {'entry_date','vendor','category','amount','job_code','description',
                     'status','invoice_number','type_of_payment','nickname','memo',
                     'receipt_verified','coi_verified'}
        if sort_col not in _SORTABLE: sort_col = 'entry_date'
        if sort_dir not in ('asc','desc'): sort_dir = 'desc'

        # ── Counts & totals ────────────────────────────────────
        total = conn.execute(
            f"SELECT COUNT(*) FROM ledger l WHERE {where_sql}", params
        ).fetchone()[0]
        totals = conn.execute(f"""
            SELECT
                COALESCE(SUM(CASE WHEN amount>0 THEN amount ELSE 0 END),0) AS total_income,
                COALESCE(SUM(CASE WHEN amount<0 THEN amount ELSE 0 END),0) AS total_expense,
                COALESCE(SUM(amount),0) AS net
            FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE {where_sql}
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
        """, params).fetchone()

        # ── Rows ───────────────────────────────────────────────
        rows = conn.execute(f"""
            SELECT l.*,
                   ba.account_name,
                   ba.institution_name,
                   COALESCE(wc.is_transfer, 0) AS is_transfer
            FROM ledger l
            LEFT JOIN bank_accounts ba ON l.bank_account_id = ba.id
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE {where_sql}
            ORDER BY l.{sort_col} {sort_dir}, l.id {sort_dir}
            LIMIT ? OFFSET ?
        """, params + [per_page, (page-1)*per_page]).fetchall()

        # ── Dropdown / filter option data ──────────────────────
        ledger_years = conn.execute("""
            SELECT DISTINCT substr(entry_date,1,4) AS yr FROM ledger
            WHERE is_deleted=0 AND entry_date!='' ORDER BY yr DESC
        """).fetchall()

        all_jobs_rows = conn.execute("""
            SELECT DISTINCT job_code FROM (
                SELECT job_code FROM jobs WHERE is_deleted=0 AND job_code != ''
                UNION
                SELECT DISTINCT job_code FROM ledger WHERE job_code != '' AND is_deleted=0
            ) ORDER BY job_code
        """).fetchall()

        categories = conn.execute(
            "SELECT category_name FROM work_categories WHERE is_deleted=0 ORDER BY category_name"
        ).fetchall()

        bank_accounts = conn.execute(
            "SELECT id, account_name, institution_name FROM bank_accounts WHERE is_deleted=0 ORDER BY account_name"
        ).fetchall()

        months_rows = conn.execute("""
            SELECT DISTINCT substr(entry_date,1,7) AS ym
            FROM ledger WHERE is_deleted=0
            ORDER BY ym DESC LIMIT 48
        """).fetchall()

        vendors_rows = conn.execute("""
            SELECT DISTINCT vendor FROM ledger
            WHERE is_deleted=0 AND vendor!='' ORDER BY vendor LIMIT 500
        """).fetchall()

        pay_types_rows = conn.execute("""
            SELECT DISTINCT type_of_payment FROM ledger
            WHERE is_deleted=0 AND type_of_payment!='' ORDER BY type_of_payment
        """).fetchall()

        # ── YTD header totals ──────────────────────────────────
        yr = datetime.now().year
        ytd = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN amount>0 THEN amount ELSE 0 END),0) AS ytd_income,
                COALESCE(SUM(CASE WHEN amount<0 THEN amount ELSE 0 END),0) AS ytd_expense,
                COALESCE(SUM(amount),0) AS ytd_net
            FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE l.is_deleted=0 AND l.entry_date >= ? AND l.entry_date < ?
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
        """, [f"{yr}-01-01", f"{yr+1}-01-01"]).fetchone()

        return render_template('ledger.html',
            config=config, badges=badges,
            rows=[dict(r) for r in rows],
            total=total,
            totals=dict(totals),
            ytd=dict(ytd),
            page=page, per_page=per_page,
            pages=(total + per_page - 1) // per_page if per_page else 1,
            sort_col=sort_col, sort_dir=sort_dir,
            # active filter state (multi-value aware)
            q=q,
            active_filters=active_filters,
            # backwards-compat single-value refs for simple checks
            year_filter=','.join(active_filters.get('year',[])),
            job_filter=','.join(active_filters.get('job',[])),
            cat_filter=','.join(active_filters.get('cat',[])),
            month_filter=','.join(active_filters.get('month',[])),
            sign_filter=','.join(active_filters.get('sign',[])),
            receipt_filter=','.join(active_filters.get('receipt',[])),
            acct_filter=','.join(active_filters.get('acct',[])),
            vendor_filter=active_filters.get('vendor',[]),
            payment_filter=active_filters.get('payment',[]),
            # option lists for filter panels
            all_jobs=[r['job_code'] for r in all_jobs_rows],
            categories=[r['category_name'] for r in categories],
            bank_accounts=[dict(b) for b in bank_accounts],
            months=[r['ym'] for r in months_rows],
            ledger_years=[r['yr'] for r in ledger_years],
            all_vendors=[r['vendor'] for r in vendors_rows],
            all_pay_types=[r['type_of_payment'] for r in pay_types_rows],
            year=yr,
            today=datetime.now().strftime('%Y-%m-%d'),
        )
    finally:
        conn.close()


@phase3.route('/api/ledger/filter-options')
def api_ledger_filter_options():
    """
    Return available filter values for all columns.
    Used by the multi-select filter panels to populate options dynamically.
    Accepts same filter params as /ledger so options reflect current context.
    """
    conn = get_connection()
    try:
        # Build base where from current filters (so options stay contextual)
        where, params, _, _ = _build_ledger_where(request.args)
        # For the requested column, remove that column's own filter so all its values show
        col = request.args.get('col', '')
        col_to_clause = {
            'year':         'substr(l.entry_date,1,4)',
            'nickname':     'l.nickname',
            'job':          'l.job_code',
            'invoice':      'l.invoice_number',
            'cat':          'l.category',
            'desc':         'l.description',
            'vendor':       'l.vendor',
            'payment':      'l.type_of_payment',
            'acct':         'CAST(l.bank_account_id AS TEXT)',
        }
        col_expr = col_to_clause.get(col)
        if not col_expr:
            return jsonify({'values': []})

        where_sql = ' AND '.join(where)
        rows = conn.execute(f"""
            SELECT DISTINCT {col_expr} AS val, COUNT(*) AS cnt
            FROM ledger l
            WHERE {where_sql} AND {col_expr} IS NOT NULL AND {col_expr} != ''
            GROUP BY {col_expr}
            ORDER BY {col_expr}
        """, params).fetchall()

        return jsonify({'col': col, 'values': [{'v': r['val'], 'n': r['cnt']} for r in rows]})
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CREATE / EDIT / DELETE
# ════════════════════════════════════════════════════════════════

@phase3.route('/ledger/new', methods=['POST'])
def ledger_new():
    """Create a new ledger entry. Accepts JSON or form data."""
    data = request.json if request.is_json else request.form.to_dict()
    return _ledger_save(None, data)


@phase3.route('/ledger/<int:row_id>/edit', methods=['POST'])
def ledger_edit(row_id):
    """Update an existing ledger entry."""
    data = request.json if request.is_json else request.form.to_dict()
    return _ledger_save(row_id, data)


def _ledger_save(row_id, data):
    entry_date_raw = (data.get('entry_date') or '').strip()
    vendor     = (data.get('vendor') or '').strip()
    amount_raw = (data.get('amount') or '').strip()
    category   = (data.get('category') or '').strip()

    # Pending entries: store today's date internally so NOT NULL is satisfied
    is_pending = 1 if entry_date_raw.lower() in ('pending','pend','tbd','') else 0
    entry_date = datetime.now().strftime('%Y-%m-%d') if is_pending else entry_date_raw

    errors = []
    if not entry_date:
        errors.append('entry_date is required')
    if amount_raw == '':
        errors.append('amount is required')
    if errors:
        if request.is_json:
            return jsonify({'error': '; '.join(errors)}), 400
        flash('; '.join(errors), 'error')
        return redirect(url_for('phase3.ledger'))

    try:
        amount = float(str(amount_raw).replace(',','').replace('$',''))
    except ValueError:
        if request.is_json:
            return jsonify({'error': 'Invalid amount'}), 400
        flash('Invalid amount', 'error')
        return redirect(url_for('phase3.ledger'))

    job_code      = (data.get('job_code') or '').strip()
    description   = (data.get('description') or '').strip()
    invoice_number= (data.get('invoice_number') or '').strip()
    notes         = (data.get('notes') or '').strip()
    nickname      = (data.get('nickname') or '').strip()
    memo          = (data.get('memo') or '').strip()
    type_of_payment = (data.get('type_of_payment') or '').strip()
    bank_acct_id  = data.get('bank_account_id') or None
    status        = (data.get('status') or 'Pending').strip()
    if is_pending:
        status = 'Pending'  # can't clear/reconcile until a real date is set
    # Allow manual override of receipt filename
    manual_receipt= (data.get('receipt_filename') or '').strip()

    with db() as conn:
        # Auto-fill category from vendor
        if not category and vendor:
            category = get_vendor_category(vendor, conn)

        # Learn vendor→category mapping
        if vendor and category:
            save_vendor_category(vendor, category, conn)

        is_cogs = _is_cogs_for_category(category, conn)

        if row_id:
            old = conn.execute("SELECT * FROM ledger WHERE id=?", [row_id]).fetchone()
            old_dict = dict(old) if old else {}
            conn.execute("""
                UPDATE ledger SET
                    entry_date=?, nickname=?, job_code=?, invoice_number=?, status=?,
                    category=?, description=?, vendor=?, is_cogs=?,
                    amount=?, type_of_payment=?, memo=?, bank_account_id=?, notes=?,
                    is_pending=?,
                    updated_at=datetime('now')
                WHERE id=?
            """, [entry_date, nickname, job_code, invoice_number, status,
                  category, description, vendor, is_cogs,
                  amount, type_of_payment, memo, bank_acct_id, notes,
                  is_pending, row_id])

            # Re-generate receipt filename if key fields changed
            if manual_receipt:
                conn.execute("UPDATE ledger SET receipt_filename=?, updated_at=datetime('now') WHERE id=?",
                             [manual_receipt, row_id])
            else:
                _auto_receipt(conn, row_id, entry_date, job_code, vendor, amount)

            new = conn.execute("SELECT * FROM ledger WHERE id=?", [row_id]).fetchone()
            log_action(conn, 'ledger', row_id, 'UPDATE', old_data=old_dict, new_data=dict(new))
            if request.is_json:
                return jsonify({'success': True, 'row': dict(new)})
            flash('Entry updated.', 'success')
        else:
            cur = conn.execute("""
                INSERT INTO ledger
                    (entry_date, nickname, job_code, invoice_number, status,
                     category, description, vendor, is_cogs,
                     amount, type_of_payment, memo, bank_account_id, notes,
                     is_pending)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, [entry_date, nickname, job_code, invoice_number, status,
                  category, description, vendor, is_cogs,
                  amount, type_of_payment, memo, bank_acct_id, notes,
                  is_pending])
            new_id = cur.lastrowid

            # Generate receipt filename
            if manual_receipt:
                conn.execute("UPDATE ledger SET receipt_filename=? WHERE id=?",
                             [manual_receipt, new_id])
                fname, verified = manual_receipt, False
            else:
                fname, verified = _auto_receipt(conn, new_id, entry_date, job_code, vendor, amount)

            log_action(conn, 'ledger', new_id, 'INSERT',
                       new_data={'vendor': vendor, 'amount': amount, 'entry_date': entry_date})

            if request.is_json:
                row = conn.execute("SELECT * FROM ledger WHERE id=?", [new_id]).fetchone()
                return jsonify({'success': True, 'row': dict(row)})
            flash('Entry added.', 'success')

    return redirect(request.referrer or url_for('phase3.ledger'))


@phase3.route('/ledger/<int:row_id>/delete', methods=['POST'])
def ledger_delete(row_id):
    with db() as conn:
        soft_delete(conn, 'ledger', row_id)
    if request.is_json:
        return jsonify({'success': True})
    flash('Entry deleted (use Undo to restore).', 'success')
    return redirect(request.referrer or url_for('phase3.ledger'))


# ════════════════════════════════════════════════════════════════
#  INLINE FIELD PATCH  (single-cell save without page reload)
# ════════════════════════════════════════════════════════════════

LEDGER_EDITABLE = {
    'entry_date', 'nickname', 'job_code', 'invoice_number', 'status',
    'category', 'description', 'vendor', 'amount',
    'type_of_payment', 'memo', 'bank_account_id', 'notes', 'receipt_filename',
    'receipt_verified', 'coi_verified', 'duplicate_flag', 'is_cogs'
}

# Fields that are safe to bulk-edit (exclude amount and dates to prevent accidents)
LEDGER_BULK_SAFE = {
    'vendor', 'category', 'job_code', 'type_of_payment', 'status',
    'memo', 'notes', 'bank_account_id', 'nickname', 'description',
    'receipt_verified', 'coi_verified', 'duplicate_flag'
}


@phase3.route('/api/ledger/bulk-patch', methods=['POST'])
def api_ledger_bulk_patch():
    """
    Apply field changes to multiple ledger rows atomically.
    Body: { "ids": [1,2,3], "changes": {"field": "value", ...} }
    Returns per-row results and total success/error counts.
    """
    data = request.json or {}
    ids     = [int(i) for i in (data.get('ids') or []) if str(i).isdigit()]
    changes = {k: v for k, v in (data.get('changes') or {}).items()
               if k in LEDGER_BULK_SAFE}

    if not ids:
        return jsonify({'error': 'No row IDs provided'}), 400
    if not changes:
        return jsonify({'error': 'No valid fields to change'}), 400
    if len(ids) > 500:
        return jsonify({'error': 'Too many rows (max 500)'}), 400

    results = {'updated': 0, 'errors': [], 'skipped': 0}

    with db() as conn:
        for row_id in ids:
            row = conn.execute(
                "SELECT * FROM ledger WHERE id=? AND is_deleted=0", [row_id]
            ).fetchone()
            if not row:
                results['skipped'] += 1
                continue

            old_dict = dict(row)
            set_clauses = ', '.join(f"{f}=?" for f in changes)
            vals        = list(changes.values()) + [row_id]
            try:
                conn.execute(
                    f"UPDATE ledger SET {set_clauses}, updated_at=datetime('now') WHERE id=?",
                    vals
                )
                # Side-effect: if category changed, update is_cogs
                if 'category' in changes and changes['category']:
                    is_cogs = _is_cogs_for_category(changes['category'], conn)
                    conn.execute("UPDATE ledger SET is_cogs=? WHERE id=?", [is_cogs, row_id])
                # Side-effect: vendor + category mapping learning
                new_vendor   = changes.get('vendor')   or old_dict.get('vendor')
                new_category = changes.get('category') or old_dict.get('category')
                if new_vendor and new_category:
                    save_vendor_category(new_vendor, new_category, conn)

                log_action(conn, 'ledger', row_id, 'UPDATE',
                           old_data={f: old_dict.get(f) for f in changes},
                           new_data=changes)
                results['updated'] += 1
            except Exception as e:
                results['errors'].append({'id': row_id, 'error': str(e)})

    return jsonify({
        'success': True,
        'updated': results['updated'],
        'skipped': results['skipped'],
        'errors':  results['errors'],
        'message': f"{results['updated']} row{'s' if results['updated']!=1 else ''} updated"
                   + (f", {results['skipped']} not found" if results['skipped'] else '')
                   + (f", {len(results['errors'])} errors" if results['errors'] else '')
    })


@phase3.route('/api/ledger/<int:row_id>/patch', methods=['POST'])
def api_ledger_patch(row_id):
    """Patch a single field on a ledger row (inline edit)."""
    data = request.json or {}
    field = data.get('field', '')
    value = data.get('value')

    if field not in LEDGER_EDITABLE:
        return jsonify({'error': f'Field {field!r} is not editable'}), 403

    with db() as conn:
        row = conn.execute(
            "SELECT * FROM ledger WHERE id=? AND is_deleted=0", [row_id]
        ).fetchone()
        if not row:
            return jsonify({'error': 'Not found'}), 404

        old_val = dict(row).get(field)
        conn.execute(
            f"UPDATE ledger SET {field}=?, updated_at=datetime('now') WHERE id=?",
            [value, row_id]
        )

        # Side-effects when key fields change
        r = dict(row)
        r[field] = value     # apply update locally for re-computation

        if field == 'vendor' and value:
            # Re-fill category if category is empty
            if not r.get('category'):
                cat = get_vendor_category(value, conn)
                if cat:
                    conn.execute("UPDATE ledger SET category=? WHERE id=?", [cat, row_id])
                    r['category'] = cat

        if field == 'entry_date' and value:
            # If a real date is set on a pending row, clear the pending flag
            parsed = _parse_date(value)
            if parsed and dict(row).get('is_pending'):
                conn.execute(
                    "UPDATE ledger SET is_pending=0, entry_date=? WHERE id=?",
                    [parsed, row_id]
                )
                r['is_pending'] = 0
                r['entry_date'] = parsed

        if field in ('vendor', 'amount', 'entry_date', 'job_code'):
            # Regenerate receipt filename
            fname, verified = _auto_receipt(
                conn, row_id,
                r.get('entry_date',''), r.get('job_code',''),
                r.get('vendor',''), float(r.get('amount',0) or 0)
            )
            r['receipt_filename'] = fname
            r['receipt_verified'] = verified

        if field == 'vendor' and value and r.get('category'):
            save_vendor_category(value, r['category'], conn)

        if field == 'category' and value and r.get('vendor'):
            save_vendor_category(r['vendor'], value, conn)

        log_action(conn, 'ledger', row_id, 'UPDATE',
                   old_data={field: old_val}, new_data={field: value},
                   field_name=field)

        updated = conn.execute("SELECT * FROM ledger WHERE id=?", [row_id]).fetchone()
        return jsonify({'success': True, 'row': dict(updated)})


@phase3.route('/api/ledger/row/<int:row_id>')
def api_ledger_row(row_id):
    """Return a single ledger row as JSON (used for DOM refresh after bulk edit)."""
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT l.*, ba.account_name
            FROM ledger l
            LEFT JOIN bank_accounts ba ON l.bank_account_id = ba.id
            WHERE l.id=? AND l.is_deleted=0
        """, [row_id]).fetchone()
        if not row:
            return jsonify({'error': 'Not found'}), 404
        return jsonify({'row': dict(row)})
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  BULK CSV IMPORT
# ════════════════════════════════════════════════════════════════

@phase3.route('/ledger/import', methods=['POST'])
def ledger_import():
    """
    Import ledger entries from a CSV file.
    Two-step using a server-side temp file to avoid passing large JSON through forms.
    Step 1 (preview): parse CSV → save rows to temp file → show preview with token.
    Step 2 (confirm): load rows from temp file using token → insert → delete temp file.
    """
    # ── CONFIRM STEP: user approved the preview ─────────────────
    if request.form.get('confirmed') == '1':
        return _ledger_import_commit()

    # ── PREVIEW STEP: parse CSV and show preview page ───────────
    f = request.files.get('csv_file')
    if not f or not f.filename:
        flash('No file selected.', 'error')
        return redirect(url_for('phase3.ledger'))

    default_job     = request.form.get('default_job', '').strip()
    default_acct    = request.form.get('default_bank_account_id') or None
    sign_convention = request.form.get('sign_convention', 'negative_expense')
    skip_header     = request.form.get('skip_header', '1') == '1'

    # Support both utf-8-sig and latin-1 encoded CSVs
    raw_bytes = f.stream.read()
    for enc in ('utf-8-sig', 'latin-1', 'cp1252'):
        try:
            content_raw = raw_bytes.decode(enc)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        content_raw = raw_bytes.decode('latin-1', errors='replace')

    parsed, errors_list = _parse_import_csv(
        content_raw, skip_header, sign_convention, default_job, default_acct
    )

    # Check each row against existing ledger for duplicates
    conn = get_connection()
    try:
        patterns = _get_recurring_patterns(conn)
        for row in parsed:
            row['dup_status'] = _check_import_row_duplicate(conn, row, patterns)
    finally:
        conn.close()

    # Save parsed rows to a temp file — avoids sending ~1MB of JSON through the form
    import tempfile as _tf
    token = str(uuid.uuid4())
    tmp_path = os.path.join(_tf.gettempdir(), f'kbweb_import_{token}.json')
    with open(tmp_path, 'w', encoding='utf-8') as tmp:
        json.dump(parsed, tmp)

    config = _get_config()
    badges = _get_badges()
    bank_accounts = _get_bank_accounts()

    return render_template('ledger_import_preview.html',
        config=config, badges=badges,
        rows=parsed,
        errors=errors_list,
        default_job=default_job,
        default_acct=default_acct,
        sign_convention=sign_convention,
        bank_accounts=bank_accounts,
        filename=f.filename,
        import_token=token,
    )


def _get_bank_accounts():
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, account_name FROM bank_accounts WHERE is_deleted=0 ORDER BY account_name"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _parse_import_csv(content_raw, skip_header, sign_convention, default_job, default_acct):
    """Parse CSV content into a list of row dicts. Returns (rows, errors)."""
    COL_ALIASES = {
        'date': 'entry_date', 'transaction date': 'entry_date',
        'post date': 'entry_date', 'trans date': 'entry_date',
        'vendor': 'vendor', 'payee': 'vendor', 'merchant': 'vendor',
        'vendor/sub': 'vendor', 'vendor/subcontractor': 'vendor',
        'subcontractor': 'vendor', 'sub': 'vendor', 'company': 'vendor',
        'description': 'description', 'desc': 'description',
        'amount': 'amount', 'debit': 'amount', 'charge': 'amount',
        'category': 'category', 'type': 'category', 'acct': 'category',
        'job': 'job_code', 'job code': 'job_code', 'project': 'job_code',
        'job #': 'job_code', 'job#': 'job_code',
        'notes': 'notes', 'note': 'notes', 'internal notes': 'notes',
        'invoice': 'invoice_number', 'invoice number': 'invoice_number',
        'invoice #': 'invoice_number', 'inv #': 'invoice_number', 'inv#': 'invoice_number',
        'receipt': 'receipt_filename', 'receipt file': 'receipt_filename',
        'receipt name': 'receipt_filename', 'receipt filename': 'receipt_filename',
        'nickname': 'nickname', 'client': 'nickname', 'client name': 'nickname',
        'memo': 'memo',
        'transaction type': 'type_of_payment', 'trans type': 'type_of_payment',
        'payment type': 'type_of_payment', 'payment method': 'type_of_payment',
        'type of payment': 'type_of_payment',
        'receipt verified': 'receipt_verified', 'rcpt verified': 'receipt_verified',
        'coi verified': 'coi_verified', 'coi_verified': 'coi_verified',
        # income / expense split columns (handled specially below)
        'income': '_income', 'expense': '_expense',
        'credit': '_income', 'withdrawal': '_expense',
    }

    reader = csv.DictReader(io.StringIO(content_raw)) if skip_header else              csv.reader(io.StringIO(content_raw))

    rows, errors = [], []

    conn = get_connection()
    try:
        for i, row in enumerate(reader, start=2 if skip_header else 1):
            if isinstance(row, dict):
                norm = {COL_ALIASES.get(k.strip().lower(), k.strip().lower()): v
                        for k, v in row.items()}
            else:
                keys = ['entry_date', 'vendor', 'amount', 'description', 'category']
                norm = dict(zip(keys, row))

            raw_date = norm.get('entry_date', '').strip()
            csv_is_pending = raw_date.lower() in ('pending', 'pend', 'tbd', '')                              or 'pending' in raw_date.lower()
            entry_date = _parse_date(raw_date)
            if not entry_date:
                if csv_is_pending:
                    entry_date = datetime.now().strftime('%Y-%m-%d')
                else:
                    errors.append(f"Row {i}: can't parse date '{raw_date}'")
                    continue

            # Parse amount — supports: amount column OR income+expense split columns
            amount = None
            has_income_exp = '_income' in norm or '_expense' in norm

            if has_income_exp:
                # income/expense split takes priority (handles re-import of exported ledger)
                try:
                    def _clean_num(s):
                        return (s or '').strip().replace(',', '').replace('$', '') or '0'
                    inc = float(_clean_num(norm.get('_income')))
                    exp = float(_clean_num(norm.get('_expense')))
                    if inc and exp:
                        # Both filled — income wins; shouldn't happen in well-formed export
                        amount = inc
                    elif inc:
                        amount = inc
                    elif exp:
                        amount = -abs(exp)
                    else:
                        amount = 0.0
                except (ValueError, TypeError):
                    pass

            if amount is None:
                # Fall back to amount column
                raw_amt = norm.get('amount', '').strip()
                try:
                    amount = _parse_amount(raw_amt, sign_convention)
                except ValueError as e:
                    if has_income_exp:
                        amount = 0.0  # income/expense both empty
                    else:
                        errors.append(f"Row {i}: {e}")
                        continue

            vendor      = norm.get('vendor', '').strip()
            description = norm.get('description', '').strip()
            category    = norm.get('category', '').strip()
            job_code    = norm.get('job_code', default_job).strip()
            notes       = norm.get('notes', '').strip()
            inv_num     = norm.get('invoice_number', '').strip()
            nickname    = norm.get('nickname', '').strip()
            memo        = norm.get('memo', '').strip()
            pay_type    = norm.get('type_of_payment', '').strip()

            if not category and vendor:
                category = get_vendor_category(vendor, conn) or ''

            is_cogs = _is_cogs_for_category(category, conn)

            # Preserve receipt_filename from CSV if present
            csv_receipt = norm.get('receipt_filename', '').strip()
            csv_rcpt_verified = 0
            try:
                csv_rcpt_verified = int(norm.get('receipt_verified', 0) or 0)
            except (ValueError, TypeError):
                pass

            rows.append({
                '_row': i,
                'entry_date':       entry_date,
                'is_pending':       1 if csv_is_pending else 0,
                'vendor':           vendor,
                'description':      description,
                'amount':           amount,
                'category':         category,
                'job_code':         job_code,
                'notes':            notes,
                'invoice_number':   inv_num,
                'nickname':         nickname,
                'memo':             memo,
                'type_of_payment':  pay_type,
                'is_cogs':          is_cogs,
                'bank_account_id':  default_acct,
                'receipt_filename': csv_receipt,
                'receipt_verified': csv_rcpt_verified,
                'import': True,  # default: include
            })
    finally:
        conn.close()

    return rows, errors


def _check_import_row_duplicate(conn, row, patterns=None):
    """
    Check if an incoming CSV row likely already exists in the ledger.
    Returns one of: 'exact', 'near', 'recurring', or '' (clean).
    """
    if patterns and _matches_recurring(row.get('vendor',''), row.get('amount',0), patterns):
        return 'recurring'

    vendor = row.get('vendor', '')
    amount = row.get('amount', 0)
    date   = row.get('entry_date', '')

    if not vendor or not date:
        return ''

    # Exact: same date + vendor + amount
    exact = conn.execute("""
        SELECT id FROM ledger
        WHERE is_deleted=0
          AND vendor=? AND ABS(amount-?)< 0.01
          AND entry_date=?
        LIMIT 1
    """, [vendor, amount, date]).fetchone()
    if exact:
        return 'exact'

    # Near: same vendor + amount within 3 days
    near = conn.execute("""
        SELECT id FROM ledger
        WHERE is_deleted=0
          AND vendor=? AND ABS(amount-?)<0.01
          AND ABS(julianday(entry_date)-julianday(?))<= 3
        LIMIT 1
    """, [vendor, amount, date]).fetchone()
    if near:
        return 'near'

    return ''


def _ledger_import_commit():
    """
    Second step of import: load rows from server-side temp file using token.
    Only the token and skip list travel through the form — no large JSON payload.
    """
    token     = request.form.get('import_token', '').strip()
    skip_ids  = set(request.form.getlist('skip_row'))  # row indices to skip

    if not token:
        flash('Import session expired or missing. Please re-upload the file.', 'error')
        return redirect(url_for('phase3.ledger'))

    import tempfile as _tf
    tmp_path = os.path.join(_tf.gettempdir(), f'kbweb_import_{token}.json')
    if not os.path.exists(tmp_path):
        flash('Import session expired. Please re-upload the CSV file.', 'error')
        return redirect(url_for('phase3.ledger'))

    try:
        with open(tmp_path, 'r', encoding='utf-8') as tmp:
            rows = json.load(tmp)
    except (json.JSONDecodeError, OSError):
        flash('Could not read import data. Please re-upload the CSV file.', 'error')
        return redirect(url_for('phase3.ledger'))

    imported = skipped = 0

    with db() as conn:
        for row in rows:
            row_idx = str(row.get('_row', ''))
            if row_idx in skip_ids:
                skipped += 1
                continue

            try:
                is_pend = row.get('is_pending', 0)
                cur = conn.execute("""
                    INSERT INTO ledger
                        (entry_date, job_code, invoice_number, category,
                         description, vendor, is_cogs, amount,
                         bank_account_id, notes, status,
                         nickname, memo, type_of_payment, is_pending)
                    VALUES (?,?,?,?,?,?,?,?,?,?,'Pending',?,?,?,?)
                """, [
                    row['entry_date'],
                    row.get('job_code',''),
                    row.get('invoice_number',''),
                    row.get('category',''),
                    row.get('description',''),
                    row.get('vendor',''),
                    row.get('is_cogs', 0),
                    row['amount'],
                    row.get('bank_account_id'),
                    row.get('notes',''),
                    row.get('nickname',''),
                    row.get('memo',''),
                    row.get('type_of_payment',''),
                    is_pend,
                ])
                new_id = cur.lastrowid
                csv_receipt = row.get('receipt_filename', '').strip()
                if csv_receipt:
                    # Use receipt filename from CSV directly
                    verified = verify_receipt(csv_receipt, _receipts_folder())
                    conn.execute(
                        "UPDATE ledger SET receipt_filename=?, receipt_verified=? WHERE id=?",
                        [csv_receipt, 1 if verified else 0, new_id]
                    )
                else:
                    # Auto-generate receipt filename from entry data
                    _auto_receipt(conn, new_id,
                                  row['entry_date'],
                                  row.get('job_code',''),
                                  row.get('vendor',''),
                                  row['amount'])
                log_action(conn, 'ledger', new_id, 'INSERT',
                           new_data=row,
                           user_label=f"CSV import: {row.get('vendor','')} {row['entry_date']}")
                imported += 1
            except Exception as e:
                skipped += 1

    # Clean up the temp file regardless of outcome
    try:
        os.unlink(tmp_path)
    except OSError:
        pass

    msg = f"Imported {imported} entries"
    if skipped:
        msg += f", skipped {skipped}"
    flash(msg + '.', 'success' if imported else 'warning')
    return redirect(url_for('phase3.ledger'))


def _parse_date(s: str) -> str:
    """Try multiple date formats, return YYYY-MM-DD or ''."""
    if not s:
        return ''
    formats = ['%Y-%m-%d','%m/%d/%Y','%m/%d/%y','%m-%d-%Y',
               '%m-%d-%y','%d-%b-%Y','%d/%m/%Y','%b %d, %Y']
    for fmt in formats:
        try:
            return datetime.strptime(s.strip(), fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return ''


def _parse_amount(s: str, convention: str = 'negative_expense') -> float:
    """Parse a dollar amount string. Convention: negative_expense = expenses are already negative."""
    if not s:
        raise ValueError(f"Empty amount")
    s = s.strip().replace(',','').replace('$','').replace(' ','')
    negative = s.startswith('(') and s.endswith(')')
    s = s.strip('()')
    try:
        val = float(s)
        if negative:
            val = -abs(val)
        return val
    except ValueError:
        raise ValueError(f"Can't parse amount '{s}'")




# ════════════════════════════════════════════════════════════════
#  DUPLICATE DETECTION
# ════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════
#  DUPLICATE DETECTION & MANAGEMENT  (Phase 7)
# ════════════════════════════════════════════════════════════════

def _get_recurring_patterns(conn):
    """Return list of recurring pattern dicts."""
    rows = conn.execute(
        "SELECT * FROM recurring_patterns WHERE is_deleted=0"
    ).fetchall()
    return [dict(r) for r in rows]


def _matches_recurring(vendor, amount, patterns):
    """Return True if this vendor+amount matches any recurring pattern (exempt from dup detection)."""
    vl = vendor.lower()
    for p in patterns:
        if p['vendor'].lower() != vl:
            continue
        amt = float(amount or 0)
        lo = p['amount_min']
        hi = p['amount_max']
        if lo is not None and hi is not None:
            if lo <= abs(amt) <= hi:
                return True
        elif lo is None and hi is None:
            return True  # matches any amount for this vendor
    return False


def _find_duplicate_groups(conn, tolerance_days=3, min_score=0.7):
    """
    Find groups of likely duplicate ledger entries.
    Uses three tiers of matching:
      - Exact:  same date + vendor + amount  (score 1.0)
      - Near:   date within tolerance_days + same vendor + same amount  (score 0.85)
      - Fuzzy:  date within tolerance_days + same vendor + amount within 1%  (score 0.70)

    Returns list of groups, each group is a dict:
      {score, match_type, entries: [list of row dicts]}

    Excludes rows with duplicate_flag='dismissed' and recurring patterns.
    """
    patterns = _get_recurring_patterns(conn)

    # Pull all non-deleted, non-dismissed rows with vendor+amount
    rows = conn.execute("""
        SELECT id, entry_date, vendor, amount, category, description,
               job_code, invoice_number, duplicate_flag, type_of_payment,
               receipt_filename, receipt_verified
        FROM ledger
        WHERE is_deleted=0
          AND (duplicate_flag IS NULL OR duplicate_flag != 'dismissed')
          AND vendor != '' AND amount != 0
        ORDER BY vendor, amount, entry_date
    """).fetchall()
    rows = [dict(r) for r in rows]

    # Filter out recurring patterns
    rows = [r for r in rows if not _matches_recurring(r['vendor'], r['amount'], patterns)]

    groups = []
    used_ids = set()

    for i, a in enumerate(rows):
        if a['id'] in used_ids:
            continue
        group_entries = [a]
        group_score   = 1.0
        match_type    = 'exact'

        for b in rows[i+1:]:
            if b['id'] in used_ids:
                continue
            if b['vendor'].lower() != a['vendor'].lower():
                break  # sorted by vendor, so no more matches

            # Amount check: within 1%
            amt_a = abs(float(a['amount']))
            amt_b = abs(float(b['amount']))
            if amt_a == 0 and amt_b == 0:
                amt_match = True; score = 1.0
            elif amt_a == 0 or amt_b == 0:
                continue
            else:
                ratio = abs(amt_a - amt_b) / max(amt_a, amt_b)
                if ratio > 0.01:
                    continue
                score = 1.0 if ratio == 0 else 0.85

            # Date check
            try:
                from datetime import date as _date
                da = _date.fromisoformat(a['entry_date'])
                db_ = _date.fromisoformat(b['entry_date'])
                day_diff = abs((da - db_).days)
            except (ValueError, TypeError):
                continue

            if day_diff == 0:
                tier_score = 1.0
                tier_type  = 'exact'
            elif day_diff <= tolerance_days:
                tier_score = 0.85
                tier_type  = 'near'
            else:
                continue

            final_score = min(score, tier_score)
            if final_score >= min_score:
                group_entries.append(b)
                group_score = min(group_score, final_score)
                if tier_type != 'exact' or match_type == 'fuzzy':
                    match_type = tier_type

        if len(group_entries) > 1:
            for e in group_entries:
                used_ids.add(e['id'])
            groups.append({
                'score':      round(group_score, 2),
                'match_type': match_type,
                'count':      len(group_entries),
                'vendor':     a['vendor'],
                'amount':     a['amount'],
                'entries':    group_entries,
                # Suggested keep: entry with receipt, or oldest
                'keep_id':    next(
                    (e['id'] for e in sorted(group_entries, key=lambda x: x['id'])
                     if e.get('receipt_filename')),
                    min(e['id'] for e in group_entries)
                ),
            })

    # Sort: exact matches first, then by score desc
    groups.sort(key=lambda g: (-g['score'], g['vendor']))
    return groups


@phase3.route('/ledger/duplicates')
def ledger_duplicates():
    """Full duplicate management page."""
    config  = _get_config()
    badges  = _get_badges()
    conn    = get_connection()
    try:
        groups   = _find_duplicate_groups(conn)
        patterns = _get_recurring_patterns(conn)

        # Stats
        exact_count = sum(1 for g in groups if g['match_type'] == 'exact')
        near_count  = sum(1 for g in groups if g['match_type'] == 'near')
        total_extra = sum(g['count'] - 1 for g in groups)  # entries that could be removed

        # Dismissed count
        dismissed = conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE duplicate_flag='dismissed' AND is_deleted=0"
        ).fetchone()[0]

        all_vendors = [r['vendor'] for r in conn.execute(
            "SELECT DISTINCT vendor FROM ledger WHERE is_deleted=0 AND vendor!='' ORDER BY vendor LIMIT 300"
        ).fetchall()]

        return render_template('ledger_duplicates.html',
            config=config, badges=badges,
            groups=groups,
            patterns=patterns,
            exact_count=exact_count,
            near_count=near_count,
            total_extra=total_extra,
            dismissed_count=dismissed,
            all_vendors=all_vendors,
        )
    finally:
        conn.close()


@phase3.route('/api/ledger/flag-duplicate', methods=['POST'])
def api_flag_duplicate():
    """
    Flag or dismiss a ledger entry's duplicate status.
    Body: { "id": 123, "flag": "flagged"|"dismissed"|"" }
    """
    data  = request.json or {}
    row_id = int(data.get('id', 0))
    flag   = (data.get('flag') or '').strip()
    if flag not in ('flagged', 'dismissed', ''):
        return jsonify({'error': 'Invalid flag value'}), 400

    with db() as conn:
        old = conn.execute("SELECT duplicate_flag FROM ledger WHERE id=?", [row_id]).fetchone()
        if not old:
            return jsonify({'error': 'Not found'}), 404
        conn.execute(
            "UPDATE ledger SET duplicate_flag=?, updated_at=datetime('now') WHERE id=?",
            [flag, row_id]
        )
        log_action(conn, 'ledger', row_id, 'UPDATE',
                   old_data={'duplicate_flag': dict(old).get('duplicate_flag','')},
                   new_data={'duplicate_flag': flag},
                   field_name='duplicate_flag')
    return jsonify({'success': True, 'id': row_id, 'flag': flag})


@phase3.route('/api/ledger/dismiss-group', methods=['POST'])
def api_dismiss_group():
    """
    Dismiss all but the keep_id in a duplicate group.
    Body: { "keep_id": 5, "dismiss_ids": [6, 7] }
    Returns count dismissed.
    """
    data        = request.json or {}
    keep_id     = int(data.get('keep_id', 0))
    dismiss_ids = [int(i) for i in (data.get('dismiss_ids') or []) if str(i).isdigit()]

    if not dismiss_ids:
        return jsonify({'error': 'No dismiss_ids provided'}), 400

    with db() as conn:
        for rid in dismiss_ids:
            if rid == keep_id:
                continue
            old = conn.execute("SELECT duplicate_flag FROM ledger WHERE id=?", [rid]).fetchone()
            conn.execute(
                "UPDATE ledger SET duplicate_flag='dismissed', updated_at=datetime('now') WHERE id=?",
                [rid]
            )
            if old:
                log_action(conn, 'ledger', rid, 'UPDATE',
                           old_data={'duplicate_flag': dict(old).get('duplicate_flag','')},
                           new_data={'duplicate_flag': 'dismissed'},
                           field_name='duplicate_flag')

    return jsonify({'success': True, 'dismissed': len(dismiss_ids)})


@phase3.route('/api/ledger/delete-dupe', methods=['POST'])
def api_ledger_delete_dupe():
    """Soft-delete a ledger row identified as a duplicate."""
    data   = request.json or {}
    row_id = data.get('row_id')
    if not row_id:
        return jsonify({'error': 'row_id required'}), 400
    with db() as conn:
        row = conn.execute("SELECT * FROM ledger WHERE id=? AND is_deleted=0", [row_id]).fetchone()
        if not row:
            return jsonify({'error': 'Not found'}), 404
        soft_delete(conn, 'ledger', row_id)
    return jsonify({'success': True})


@phase3.route('/api/ledger/find-duplicates')
def api_find_duplicates():
    """Return duplicate groups as JSON (for the ledger page inline dupe banner)."""
    conn = get_connection()
    try:
        tolerance = int(request.args.get('days', 3))
        groups = _find_duplicate_groups(conn, tolerance_days=tolerance)
        # Flatten for backwards-compat: also return simple summary
        summary = [{
            'vendor':      g['vendor'],
            'amount':      g['amount'],
            'count':       g['count'],
            'ids':         [e['id'] for e in g['entries']],
            'keep_id':     g['keep_id'],
            'match_type':  g['match_type'],
            'score':       g['score'],
        } for g in groups]
        return jsonify({
            'groups':      summary,
            'total_groups': len(groups),
            'total_extra':  sum(g['count']-1 for g in groups),
        })
    finally:
        conn.close()


@phase3.route('/api/ledger/remove-duplicates', methods=['POST'])
def api_remove_duplicates():
    """
    Soft-delete duplicates based on provided groups, or auto-detect and remove all.
    Body: { groups: [{keep_id, remove_ids: [...]}] }  OR  {}  for auto-mode.
    """
    data = request.json or {}
    conn = get_connection()
    try:
        removed = 0
        if data.get('groups'):
            for group in data['groups']:
                for rid in (group.get('remove_ids') or []):
                    conn.execute(
                        "UPDATE ledger SET is_deleted=1, updated_at=datetime('now') WHERE id=?",
                        [rid]
                    )
                    log_action(conn, 'ledger', rid, 'DELETE',
                               old_data={'id': rid}, user_label=f"Auto-removed duplicate #{rid}")
                    removed += 1
        else:
            # Auto-mode via _find_duplicate_groups
            groups = _find_duplicate_groups(conn)
            for g in groups:
                keep = g['keep_id']
                for e in g['entries']:
                    if e['id'] != keep:
                        conn.execute(
                            "UPDATE ledger SET is_deleted=1, updated_at=datetime('now') WHERE id=?",
                            [e['id']]
                        )
                        log_action(conn, 'ledger', e['id'], 'DELETE',
                                   old_data={'id': e['id']},
                                   user_label=f"Auto-removed duplicate #{e['id']}")
                        removed += 1
        conn.commit()
        return jsonify({'success': True, 'removed': removed,
                        'message': f'{removed} duplicate entries removed. Use Undo to recover.'})
    finally:
        conn.close()


# ── Recurring patterns ───────────────────────────────────────

@phase3.route('/api/recurring-patterns', methods=['GET'])
def api_recurring_patterns_list():
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM recurring_patterns WHERE is_deleted=0 ORDER BY vendor, amount_min"
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@phase3.route('/api/recurring-patterns', methods=['POST'])
def api_recurring_patterns_add():
    """
    Add a recurring pattern to exclude from duplicate detection.
    Body: { vendor, amount_min, amount_max, day_of_month, notes }
    """
    data = request.json or {}
    vendor = (data.get('vendor') or '').strip()
    if not vendor:
        return jsonify({'error': 'vendor required'}), 400

    amt_min = float(data['amount_min']) if data.get('amount_min') not in (None, '') else None
    amt_max = float(data['amount_max']) if data.get('amount_max') not in (None, '') else None
    dom     = int(data['day_of_month']) if data.get('day_of_month') not in (None, '') else None
    notes   = (data.get('notes') or '').strip()

    with db() as conn:
        cur = conn.execute(
            "INSERT INTO recurring_patterns (vendor, amount_min, amount_max, day_of_month, notes) VALUES (?,?,?,?,?)",
            [vendor, amt_min, amt_max, dom, notes]
        )
        new_id = cur.lastrowid
        new_row = conn.execute("SELECT * FROM recurring_patterns WHERE id=?", [new_id]).fetchone()
    return jsonify({'success': True, 'pattern': dict(new_row)})


@phase3.route('/api/recurring-patterns/<int:pattern_id>', methods=['DELETE'])
def api_recurring_patterns_delete(pattern_id):
    with db() as conn:
        conn.execute(
            "UPDATE recurring_patterns SET is_deleted=1 WHERE id=?", [pattern_id]
        )
    return jsonify({'success': True})


# ════════════════════════════════════════════════════════════════
#  EXPORT
# ════════════════════════════════════════════════════════════════

@phase3.route('/ledger/export')
def ledger_export():
    """Export filtered ledger to CSV. Respects 'cols' param for column selection/order."""
    conn = get_connection()
    try:
        # Reuse the same multi-value filter logic
        where, params, _, _ = _build_ledger_where(request.args)
        cols_param = request.args.get('cols', '')

        rows = conn.execute(
            f"SELECT * FROM ledger l WHERE {' AND '.join(where)} ORDER BY l.entry_date DESC",
            params
        ).fetchall()

        # Column key → DB column name mapping (income/expense are derived from amount)
        ALL_COL_KEYS = [
            'date', 'nickname', 'job_code', 'invoice_number', 'category',
            'description', 'vendor', 'income', 'expense',
            'type_of_payment', 'memo', 'receipt', 'receipt_verified', 'coi_verified',
            # extra cols always available
            'id', 'status', 'notes', 'amount', 'bank_account_id', 'is_cogs',
        ]
        COL_TO_DB = {
            'date': 'entry_date', 'nickname': 'nickname', 'job_code': 'job_code',
            'invoice_number': 'invoice_number', 'category': 'category',
            'description': 'description', 'vendor': 'vendor',
            'income': None, 'expense': None,  # derived
            'type_of_payment': 'type_of_payment', 'memo': 'memo',
            'receipt': 'receipt_filename', 'receipt_verified': 'receipt_verified',
            'coi_verified': 'coi_verified', 'id': 'id', 'status': 'status',
            'notes': 'notes', 'amount': 'amount', 'bank_account_id': 'bank_account_id',
            'is_cogs': 'is_cogs',
        }
        COL_LABELS = {
            'date': 'Date', 'nickname': 'Nickname', 'job_code': 'Job Code',
            'invoice_number': 'Invoice #', 'category': 'Category',
            'description': 'Description', 'vendor': 'Vendor/Sub',
            'income': 'Income', 'expense': 'Expense',
            'type_of_payment': 'Transaction Type', 'memo': 'Memo',
            'receipt': 'Receipt', 'receipt_verified': 'Receipt Verified',
            'coi_verified': 'COI Verified', 'id': 'ID', 'status': 'Status',
            'notes': 'Notes', 'amount': 'Amount', 'bank_account_id': 'Bank Account ID',
            'is_cogs': 'COGS',
        }

        if cols_param:
            # Use the exact user-specified order (comes from localStorage column order)
            export_keys = [k for k in cols_param.split(',') if k in COL_TO_DB]
        else:
            # Default: all standard columns in default display order
            export_keys = ['date','nickname','job_code','invoice_number','category',
                          'description','vendor','income','expense',
                          'type_of_payment','memo','receipt','receipt_verified','coi_verified']

        def cell_value(row_dict, key):
            if key == 'income':
                amt = float(row_dict.get('amount', 0) or 0)
                return f"{amt:.2f}" if amt > 0 else ''
            elif key == 'expense':
                amt = float(row_dict.get('amount', 0) or 0)
                return f"{abs(amt):.2f}" if amt < 0 else ''
            elif key == 'date':
                return row_dict.get('entry_date', '')
            elif key == 'receipt':
                return row_dict.get('receipt_filename', '')
            else:
                db_col = COL_TO_DB.get(key, key)
                return str(row_dict.get(db_col, '') or '')

        def generate():
            header = [COL_LABELS.get(k, k) for k in export_keys]
            yield ','.join(f'"{h}"' for h in header) + '\n'
            for r in rows:
                d = dict(r)
                yield ','.join(
                    f'"{cell_value(d, k).replace(chr(34), chr(34)*2)}"'
                    for k in export_keys
                ) + '\n'

        fname = f"ledger_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        return Response(
            stream_with_context(generate()),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={fname}'}
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  RECEIPT MANAGEMENT
# ════════════════════════════════════════════════════════════════

@phase3.route('/api/ledger/verify-receipt/<int:row_id>', methods=['POST'])
def api_verify_single_receipt(row_id):
    """Re-check if receipt file exists for one row."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT receipt_filename FROM ledger WHERE id=?", [row_id]
        ).fetchone()
        if not row:
            return jsonify({'error': 'Not found'}), 404
        verified = verify_receipt(row['receipt_filename'], _receipts_folder())
        conn.execute(
            "UPDATE ledger SET receipt_verified=?, updated_at=datetime('now') WHERE id=?",
            [1 if verified else 0, row_id]
        )
        conn.commit()
        return jsonify({'verified': verified, 'filename': row['receipt_filename']})
    finally:
        conn.close()


@phase3.route('/api/ledger/preview-receipt')
def api_preview_receipt():
    """Preview what receipt filename would be generated."""
    entry_date = request.args.get('date', '')
    job_code   = request.args.get('job', '')
    vendor     = request.args.get('vendor', '')
    amount     = request.args.get('amount', '0')
    try:
        fname = generate_receipt_filename(entry_date, job_code, vendor, float(amount or 0))
        verified = verify_receipt(fname, _receipts_folder()) if fname else False
    except Exception:
        fname, verified = '', False
    return jsonify({'filename': fname, 'verified': verified})


@phase3.route('/api/ledger/assign-receipt/<int:row_id>', methods=['POST'])
def api_assign_receipt(row_id):
    """
    Assign (or rename) a receipt filename to a ledger row.
    Body: { "filename": "2025-01-15.KB001.AcmeCorp.250_00.pdf" }
    Verifies existence, updates row, returns verified status.
    """
    data     = request.json or {}
    filename = (data.get('filename') or '').strip()
    # Allow empty filename to clear the receipt link
    # (filename='' clears the receipt)

    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM ledger WHERE id=? AND is_deleted=0", [row_id]
        ).fetchone()
        if not row:
            return jsonify({'error': 'Row not found'}), 404

        folder   = _receipts_folder()
        verified = verify_receipt(filename, folder) if folder else False

        old_fn = dict(row).get('receipt_filename', '')
        conn.execute(
            "UPDATE ledger SET receipt_filename=?, receipt_verified=?, updated_at=datetime('now') WHERE id=?",
            [filename, 1 if verified else 0, row_id]
        )
        log_action(conn, 'ledger', row_id, 'UPDATE',
                   old_data={'receipt_filename': old_fn},
                   new_data={'receipt_filename': filename},
                   field_name='receipt_filename')
        conn.commit()

        return jsonify({
            'success':   True,
            'filename':  filename,
            'verified':  verified,
            'message':   ('✅ Receipt linked and verified.' if verified
                          else f'⚠️ Filename saved but file not found in receipts folder. Save the PDF as "{filename}" to verify.'),
        })
    finally:
        conn.close()


@phase3.route('/api/ledger/reverify-all', methods=['POST'])
def api_reverify_all_receipts():
    """
    Re-scan the receipts folder and update receipt_verified for all ledger rows.
    Returns counts of verified, unverified, and missing.
    """
    folder = _receipts_folder()
    if not folder:
        return jsonify({'error': 'Receipts folder not configured in Settings'}), 400

    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, receipt_filename FROM ledger WHERE receipt_filename != '' AND is_deleted=0"
        ).fetchall()

        verified_count = unverified_count = skipped = 0
        for row in rows:
            fname = row['receipt_filename']
            if not fname:
                skipped += 1
                continue
            v = verify_receipt(fname, folder)
            conn.execute(
                "UPDATE ledger SET receipt_verified=?, updated_at=datetime('now') WHERE id=?",
                [1 if v else 0, row['id']]
            )
            if v:
                verified_count += 1
            else:
                unverified_count += 1

        no_receipt = conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE (receipt_filename='' OR receipt_filename IS NULL) AND is_deleted=0"
        ).fetchone()[0]

        conn.commit()
        return jsonify({
            'success':    True,
            'verified':   verified_count,
            'unverified': unverified_count,
            'no_receipt': no_receipt,
            'message':    f"Re-verified {verified_count + unverified_count} receipts: "
                          f"{verified_count} found, {unverified_count} missing on disk, "
                          f"{no_receipt} entries have no receipt filename.",
        })
    finally:
        conn.close()


@phase3.route('/api/ledger/coi-batch')
def api_coi_batch():
    """
    Return COI status for a batch of vendor names.
    Query: ?vendors=AcmeCorp,Beta+LLC,Gamma+Inc
    Returns dict of vendor → {status, message, end_date, is_sub}
    """
    vendors_raw = request.args.get('vendors', '')
    vendors     = [v.strip() for v in vendors_raw.split(',') if v.strip()]
    if not vendors:
        return jsonify({})

    conn = get_connection()
    try:
        from datetime import date as _date, timedelta as _td

        result = {}
        for vname in vendors[:100]:  # cap at 100
            row = conn.execute("""
                SELECT c.vendor_type,
                       cert.end_date AS coi_end_date
                FROM contractors c
                LEFT JOIN (
                    SELECT contractor_id, end_date
                    FROM certificates
                    WHERE is_deleted=0
                      AND (cert_type LIKE '%liability%' OR cert_type LIKE '%COI%'
                           OR cert_type LIKE '%insurance%' OR cert_type='')
                    ORDER BY end_date DESC LIMIT 1
                ) cert ON cert.contractor_id = c.id
                WHERE c.company_name=? AND c.is_deleted=0
                LIMIT 1
            """, [vname]).fetchone()

            if not row:
                result[vname] = {'status': 'unknown', 'message': '', 'end_date': '', 'is_sub': False}
                continue

            d = dict(row)
            is_sub = d.get('vendor_type') == 'Subcontractor'

            if not is_sub:
                result[vname] = {'status': 'not_required', 'message': '', 'end_date': '', 'is_sub': False}
                continue

            end = d.get('coi_end_date') or ''
            if not end:
                result[vname] = {'status': 'missing',
                                 'message': f'No COI on file for {vname}',
                                 'end_date': '', 'is_sub': True}
                continue

            try:
                exp   = _date.fromisoformat(end)
                today = _date.today()
                if exp < today:
                    st  = 'expired'
                    msg = f'COI expired {end}'
                elif exp <= today + _td(days=30):
                    st  = 'expiring_soon'
                    msg = f'COI expires {end} (≤30 days)'
                else:
                    st  = 'valid'
                    msg = f'COI valid through {end}'
            except (ValueError, TypeError):
                st, msg = 'missing', f'COI date invalid for {vname}'

            result[vname] = {'status': st, 'message': msg, 'end_date': end, 'is_sub': True}

        return jsonify(result)
    finally:
        conn.close()


@phase3.route('/api/ledger/receipts-folder-contents')
def api_receipts_folder_contents():
    """
    Return list of files in the receipts folder for the drag-assign picker.
    Supports ?q= for filtering.
    """
    folder = _receipts_folder()
    if not folder or not os.path.isdir(folder):
        return jsonify({'error': 'Receipts folder not configured', 'files': []})

    q = request.args.get('q', '').lower()
    try:
        files = []
        for fname in sorted(os.listdir(folder)):
            if fname.startswith('.'): continue
            if q and q not in fname.lower(): continue
            fpath = os.path.join(folder, fname)
            size  = os.path.getsize(fpath) if os.path.isfile(fpath) else 0
            files.append({'name': fname, 'size': size})
        return jsonify({'files': files[:200], 'folder': folder})
    except Exception as e:
        return jsonify({'error': str(e), 'files': []})


# ════════════════════════════════════════════════════════════════
#  MONTHLY SUMMARY  (JSON for dashboard chart)
# ════════════════════════════════════════════════════════════════

@phase3.route('/api/ledger/monthly-summary')
def api_monthly_summary():
    year = int(request.args.get('year', datetime.now().year))
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                substr(l.entry_date,1,7) AS ym,
                COALESCE(SUM(CASE WHEN l.amount>0 THEN l.amount ELSE 0 END),0) AS income,
                COALESCE(SUM(CASE WHEN l.amount<0 THEN ABS(l.amount) ELSE 0 END),0) AS expense,
                COALESCE(SUM(l.amount),0) AS net
            FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE l.is_deleted=0 AND l.entry_date >= ? AND l.entry_date < ?
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
            GROUP BY ym ORDER BY ym
        """, [f"{year}-01-01", f"{year+1}-01-01"]).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@phase3.route('/api/ledger/job-summary')
def api_job_summary():
    """Cost/income breakdown per job for a given year."""
    year = int(request.args.get('year', datetime.now().year))
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                l.job_code,
                j.description AS job_desc,
                COALESCE(SUM(CASE WHEN l.amount>0 THEN l.amount ELSE 0 END),0) AS income,
                COALESCE(SUM(CASE WHEN l.amount<0 THEN ABS(l.amount) ELSE 0 END),0) AS expense,
                COALESCE(SUM(l.amount),0) AS net,
                COUNT(*) AS entry_count
            FROM ledger l
            LEFT JOIN jobs j ON l.job_code = j.job_code
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE l.is_deleted=0 AND l.job_code!=''
              AND l.entry_date >= ? AND l.entry_date < ?
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
            GROUP BY l.job_code
            ORDER BY ABS(net) DESC
        """, [f"{year}-01-01", f"{year+1}-01-01"]).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@phase3.route('/api/ledger/category-summary')
def api_category_summary():
    """Expense breakdown by category."""
    year = int(request.args.get('year', datetime.now().year))
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                l.category,
                wc.schedule_c_line,
                wc.is_cogs,
                COALESCE(SUM(ABS(l.amount)),0) AS total,
                COUNT(*) AS entry_count
            FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE l.is_deleted=0 AND l.amount < 0
              AND l.entry_date >= ? AND l.entry_date < ?
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
            GROUP BY l.category
            ORDER BY total DESC
        """, [f"{year}-01-01", f"{year+1}-01-01"]).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  RECEIPT NAME LOOKUP  (for clipboard copy when PDF missing)
# ════════════════════════════════════════════════════════════════

@phase3.route('/api/ledger/receipt-name/<int:row_id>')
def api_ledger_receipt_name(row_id):
    """Return the receipt filename for a ledger row (auto-generate if blank)."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM ledger WHERE id=? AND is_deleted=0", [row_id]
        ).fetchone()
        if not row:
            return jsonify({'error': 'Not found'}), 404
        r = dict(row)
        filename = r.get('receipt_filename', '')
        if not filename:
            # Auto-generate the name without saving it
            _, _ = _auto_receipt(conn, row_id,
                                  r.get('entry_date',''), r.get('job_code',''),
                                  r.get('vendor',''), float(r.get('amount', 0) or 0))
            row2 = conn.execute("SELECT receipt_filename FROM ledger WHERE id=?", [row_id]).fetchone()
            filename = row2['receipt_filename'] if row2 else ''
        return jsonify({'filename': filename})
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  VENDOR AUTOCOMPLETE (enhanced for Ledger)
# ════════════════════════════════════════════════════════════════

@phase3.route('/api/ledger/vendors')
def api_ledger_vendors():
    """Return recent vendors + their default categories."""
    q = request.args.get('q', '').strip()
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT vendor, MAX(entry_date) AS last_used, COUNT(*) AS use_count
            FROM ledger
            WHERE vendor LIKE ? AND vendor!='' AND is_deleted=0
            GROUP BY vendor
            ORDER BY use_count DESC, last_used DESC
            LIMIT 20
        """, [f"%{q}%"]).fetchall()
        result = []
        for r in rows:
            cat = get_vendor_category(r['vendor'], conn)
            result.append({
                'vendor': r['vendor'],
                'last_used': r['last_used'],
                'use_count': r['use_count'],
                'default_category': cat,
            })
        return jsonify(result)
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  VENDOR COI STATUS (Phase 1 — for ledger entry COI warnings)
# ════════════════════════════════════════════════════════════════

@phase3.route('/api/vendor/coi-status')
def api_vendor_coi_status():
    """Return COI/type info for a vendor name — used by ledger entry form."""
    vendor_name = request.args.get('vendor', '').strip()
    if not vendor_name:
        return jsonify({'found': False})
    conn = get_connection()
    try:
        contractor = conn.execute("""
            SELECT c.id, c.company_name, c.vendor_type, c.requires_1099,
                   cert.end_date AS coi_end_date,
                   cert.cert_type
            FROM contractors c
            LEFT JOIN (
                SELECT contractor_id, end_date, cert_type
                FROM certificates
                WHERE is_deleted=0
                  AND (cert_type LIKE '%liability%' OR cert_type LIKE '%COI%'
                       OR cert_type LIKE '%insurance%' OR cert_type = '')
                ORDER BY end_date DESC
                LIMIT 1
            ) cert ON cert.contractor_id = c.id
            WHERE c.company_name = ? AND c.is_deleted=0
            LIMIT 1
        """, [vendor_name]).fetchone()

        if not contractor:
            return jsonify({'found': False, 'vendor': vendor_name})

        d = dict(contractor)
        is_sub = d.get('vendor_type') == 'Subcontractor'

        # Determine COI status
        coi_status = 'not_required'
        coi_message = ''
        coi_end = d.get('coi_end_date', '')

        if is_sub:
            if not coi_end:
                coi_status = 'missing'
                coi_message = f'⚠️ {vendor_name} is a Subcontractor with no COI on file.'
            else:
                from datetime import date, timedelta
                try:
                    exp = date.fromisoformat(coi_end)
                    today = date.today()
                    if exp < today:
                        coi_status = 'expired'
                        coi_message = f'🚫 COI for {vendor_name} expired {coi_end}. Do not pay until renewed.'
                    elif exp <= today + timedelta(days=30):
                        coi_status = 'expiring_soon'
                        coi_message = f'⚠️ COI for {vendor_name} expires {coi_end} (within 30 days).'
                    else:
                        coi_status = 'valid'
                        coi_message = f'✅ COI valid through {coi_end}.'
                except (ValueError, TypeError):
                    coi_status = 'missing'
                    coi_message = f'⚠️ {vendor_name} COI date is invalid. Please verify.'

        return jsonify({
            'found': True,
            'vendor': vendor_name,
            'vendor_type': d.get('vendor_type', 'Other'),
            'is_subcontractor': is_sub,
            'requires_1099': bool(d.get('requires_1099')),
            'coi_status': coi_status,
            'coi_end_date': coi_end,
            'coi_message': coi_message,
        })
    finally:
        conn.close()
@phase3.route('/<int:row_id>/patch', methods=['POST'])
def patch_ledger(row_id):
    data = request.get_json()
    field = data.get('field')
    value = data.get('value')
    sign_hint = data.get('sign')  # 'positive' or 'negative' from frontend

    if not field:
        return jsonify({"error": "Field required"}), 400

    conn = get_connection()
    try:
        # Fetch current row
        row = conn.execute("SELECT * FROM ledger WHERE id = ?", (row_id,)).fetchone()
        if not row:
            return jsonify({"error": "Row not found"}), 404

        # Special handling for amount field
        if field == 'amount':
            try:
                new_amount = float(value)
                # If user edited the opposite column → flip sign automatically
                if sign_hint == 'positive' and new_amount < 0:
                    new_amount = abs(new_amount)
                elif sign_hint == 'negative' and new_amount > 0:
                    new_amount = -new_amount
                value = new_amount
            except ValueError:
                return jsonify({"error": "Invalid amount"}), 400

        # Update the field
        conn.execute(f"UPDATE ledger SET {field} = ?, updated_at = datetime('now') WHERE id = ?",
                     (value, row_id))
        conn.commit()

        # Return updated row for frontend refresh
        updated = conn.execute("SELECT * FROM ledger WHERE id = ?", (row_id,)).fetchone()
        return jsonify({"success": True, "row": dict(updated)})

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@phase3.route('/<int:row_id>/duplicate', methods=['POST'])
def duplicate_ledger(row_id):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM ledger WHERE id = ?", (row_id,)).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        
        # Copy row, exclude id and timestamps
        data = dict(row)
        data.pop('id', None)
        data.pop('created_at', None)
        data.pop('updated_at', None)
        # Clear potentially unique/confusing fields
        data['receipt_filename'] = None
        data['invoice_number'] = None
        data['receipt_verified'] = 0
        
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        conn.execute(f"INSERT INTO ledger ({columns}) VALUES ({placeholders})", list(data.values()))
        conn.commit()
        
        return jsonify({"success": True, "message": "Entry duplicated"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()
