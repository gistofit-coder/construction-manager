"""
Phase 5 Routes — Invoices
Full invoice management:
  - List with status badges, aging column, client/job filters
  - Create/Edit form: auto-next invoice number, due date computed (+30d), status auto-set
  - Detail page: line items pulled from timesheet, payment history, aging
  - Record Payment modal (partial or full)
  - PDF generation via WeasyPrint (or plain-HTML fallback)
  - Aging report API (0-30, 31-60, 61-90, 90+ buckets)
  - Overdue badges wired into nav badge count
  - Mark Overdue batch job (run from settings or auto on page load)
  - Export to CSV
  - Undo on every write
"""
import json
import io
import csv
from datetime import datetime, date, timedelta
from flask import (Blueprint, render_template, request, jsonify,
                   redirect, url_for, flash, Response, stream_with_context,
                   make_response)

from database import db, get_connection
from automations import (
    compute_invoice_dates_and_balance, update_invoice_status,
    log_action, soft_delete,
)

phase5 = Blueprint('phase5', __name__)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _cfg(conn=None):
    close = False
    if conn is None:
        conn = get_connection(); close = True
    try:
        r = conn.execute("SELECT * FROM company_config WHERE id=1").fetchone()
        return dict(r) if r else {}
    finally:
        if close: conn.close()

def _badges():
    from app import get_nav_badges
    return get_nav_badges()

def _next_invoice_number(conn) -> int:
    row = conn.execute(
        "SELECT MAX(invoice_number) AS mx FROM invoices"  # include deleted to avoid UNIQUE collision
    ).fetchone()
    mx = row['mx'] if row and row['mx'] else 0
    return int(mx) + 1

def _aging_bucket(due_date_str: str, today_str: str) -> str:
    """Return '0-30', '31-60', '61-90', or '90+'."""
    if not due_date_str:
        return '0-30'
    try:
        due  = datetime.strptime(due_date_str, '%Y-%m-%d').date()
        td   = datetime.strptime(today_str, '%Y-%m-%d').date()
        days = (td - due).days
        if days <= 0:   return '0-30'
        elif days <= 30: return '0-30'
        elif days <= 60: return '31-60'
        elif days <= 90: return '61-90'
        else:            return '90+'
    except Exception:
        return '0-30'

def _days_overdue(due_date_str: str, today_str: str) -> int:
    if not due_date_str:
        return 0
    try:
        due = datetime.strptime(due_date_str, '%Y-%m-%d').date()
        td  = datetime.strptime(today_str, '%Y-%m-%d').date()
        return max(0, (td - due).days)
    except Exception:
        return 0

def _mark_overdue_batch(conn):
    """Set status=Overdue for any unpaid/partial invoices past due_date."""
    today = datetime.now().strftime('%Y-%m-%d')
    conn.execute("""
        UPDATE invoices
        SET status='Overdue', updated_at=datetime('now')
        WHERE status IN ('Pending','Partial')
          AND due_date < ?
          AND balance_due > 0
          AND is_deleted=0
    """, [today])


# ════════════════════════════════════════════════════════════════
#  INVOICES LIST
# ════════════════════════════════════════════════════════════════

@phase5.route('/invoices')
def invoices():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        # Run batch overdue update on every list load
        _mark_overdue_batch(conn)
        conn.commit()

        today = datetime.now().strftime('%Y-%m-%d')

        # ── Filters ───────────────────────────────────────────
        status_filter  = request.args.get('status', '').strip()
        client_filter  = request.args.get('client', '').strip()
        job_filter     = request.args.get('job', '').strip()
        q              = request.args.get('q', '').strip()
        year_filter    = request.args.get('year', '').strip()
        page           = max(1, int(request.args.get('page', 1)))
        per_page       = int(request.args.get('per_page', 50))

        where  = ["i.is_deleted=0"]
        params = []

        if status_filter:
            where.append("i.status=?"); params.append(status_filter)
        if client_filter:
            where.append("i.client_id=?"); params.append(client_filter)
        if job_filter:
            where.append("i.job_code=?"); params.append(job_filter)
        if year_filter:
            where.append("substr(i.invoice_date,1,4)=?"); params.append(year_filter)
        if q:
            where.append("(i.invoice_number LIKE ? OR c.full_name LIKE ? OR i.description_of_work LIKE ? OR i.job_code LIKE ?)")
            params += [f"%{q}%"]*4

        where_sql = " AND ".join(where)

        total_count = conn.execute(
            f"SELECT COUNT(*) FROM invoices i LEFT JOIN clients c ON i.client_id=c.id WHERE {where_sql}",
            params
        ).fetchone()[0]

        rows = conn.execute(f"""
            SELECT i.*,
                   c.full_name AS client_name,
                   c.customer_id,
                   j.description AS job_desc
            FROM invoices i
            LEFT JOIN clients c ON i.client_id = c.id
            LEFT JOIN jobs    j ON i.job_code  = j.job_code
            WHERE {where_sql}
            ORDER BY i.invoice_number DESC
            LIMIT ? OFFSET ?
        """, params + [per_page, (page-1)*per_page]).fetchall()

        # ── Summary tiles ─────────────────────────────────────
        summary = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN status='Paid' THEN amount ELSE 0 END),0)       AS paid_total,
                COALESCE(SUM(CASE WHEN status!='Paid' THEN balance_due ELSE 0 END),0) AS outstanding,
                COALESCE(SUM(CASE WHEN status='Overdue' THEN balance_due ELSE 0 END),0) AS overdue,
                COUNT(CASE WHEN status NOT IN ('Paid') THEN 1 END)                    AS open_count,
                COUNT(CASE WHEN status='Overdue' THEN 1 END)                          AS overdue_count
            FROM invoices WHERE is_deleted=0
        """).fetchone()

        # ── Dropdown data ─────────────────────────────────────
        clients = conn.execute(
            "SELECT id, full_name, customer_id FROM clients WHERE is_deleted=0 ORDER BY full_name"
        ).fetchall()
        jobs = conn.execute(
            "SELECT job_code FROM jobs WHERE is_deleted=0 ORDER BY job_code"
        ).fetchall()
        years = conn.execute("""
            SELECT DISTINCT substr(invoice_date,1,4) AS yr FROM invoices
            WHERE is_deleted=0 ORDER BY yr DESC
        """).fetchall()

        # Add aging info per row
        rows_dicts = []
        for r in rows:
            d = dict(r)
            d['days_overdue'] = _days_overdue(d.get('due_date',''), today)
            d['aging_bucket'] = _aging_bucket(d.get('due_date',''), today)
            rows_dicts.append(d)

        return render_template('invoices.html',
            config=config, badges=badges, today=today,
            rows=rows_dicts,
            total_count=total_count,
            summary=dict(summary),
            page=page, per_page=per_page,
            pages=(total_count + per_page - 1) // per_page if per_page else 1,
            # filters
            status_filter=status_filter, client_filter=client_filter,
            job_filter=job_filter, q=q, year_filter=year_filter,
            # dropdowns
            clients=[dict(c) for c in clients],
            jobs=[r['job_code'] for r in jobs],
            years=[r['yr'] for r in years],
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CREATE
# ════════════════════════════════════════════════════════════════

@phase5.route('/invoices/new', methods=['GET', 'POST'])
def invoice_new():
    if request.method == 'GET':
        config = _cfg()
        badges = _badges()
        conn   = get_connection()
        try:
            next_num = _next_invoice_number(conn)
            clients  = conn.execute(
                "SELECT id, full_name, customer_id FROM clients WHERE is_deleted=0 AND status='Active' ORDER BY full_name"
            ).fetchall()
            all_jobs = conn.execute(
                "SELECT job_code, client_id FROM jobs WHERE is_deleted=0 ORDER BY job_code"
            ).fetchall()
            today = date.today().strftime('%Y-%m-%d')
            due   = (date.today() + timedelta(days=30)).strftime('%Y-%m-%d')
            prefill_client = request.args.get('client_id', '')
            prefill_job    = request.args.get('job_code', '')
            return render_template('invoice_form.html',
                config=config, badges=badges,
                invoice=None, next_num=next_num,
                clients=[dict(c) for c in clients],
                all_jobs=[dict(j) for j in all_jobs],
                today=today, due=due,
                prefill_client=prefill_client, prefill_job=prefill_job,
            )
        finally:
            conn.close()

    # POST
    return _invoice_save(None, request.form.to_dict())


@phase5.route('/invoices/<int:inv_id>/edit', methods=['GET', 'POST'])
def invoice_edit(inv_id):
    if request.method == 'GET':
        config = _cfg()
        badges = _badges()
        conn   = get_connection()
        try:
            invoice = conn.execute(
                "SELECT * FROM invoices WHERE id=? AND is_deleted=0", [inv_id]
            ).fetchone()
            if not invoice:
                flash('Invoice not found.', 'error')
                return redirect(url_for('phase5.invoices'))
            clients = conn.execute(
                "SELECT id, full_name, customer_id FROM clients WHERE is_deleted=0 ORDER BY full_name"
            ).fetchall()
            all_jobs = conn.execute(
                "SELECT job_code, client_id FROM jobs WHERE is_deleted=0 ORDER BY job_code"
            ).fetchall()
            return render_template('invoice_form.html',
                config=config, badges=badges,
                invoice=dict(invoice),
                next_num=invoice['invoice_number'],
                clients=[dict(c) for c in clients],
                all_jobs=[dict(j) for j in all_jobs],
                today='', due='',
                prefill_client='', prefill_job='',
            )
        finally:
            conn.close()

    return _invoice_save(inv_id, request.form.to_dict())


def _invoice_save(inv_id, data):
    invoice_date = (data.get('invoice_date') or '').strip()
    amount_raw   = (data.get('amount') or '').strip()
    client_id    = data.get('client_id') or None

    errors = []
    if not invoice_date: errors.append('Invoice date required')
    if not amount_raw:   errors.append('Amount required')
    if errors:
        for e in errors: flash(e, 'error')
        return redirect(request.referrer or url_for('phase5.invoice_new'))

    try:
        amount = float(str(amount_raw).replace(',', '').replace('$', ''))
    except ValueError:
        flash('Invalid amount', 'error')
        return redirect(request.referrer or url_for('phase5.invoice_new'))

    job_code    = (data.get('job_code') or '').strip()
    description = (data.get('description_of_work') or '').strip()
    notes       = (data.get('notes') or '').strip()
    amount_paid       = float((data.get('amount_paid') or '0').replace(',','').replace('$',''))
    adjustment_amount = float((data.get('adjustment_amount') or '0').replace(',','').replace('$',''))
    adjustment_note   = (data.get('adjustment_note') or '').strip()
    due_date          = (data.get('due_date') or '').strip()

    # Auto-compute due_date if not supplied
    if not due_date:
        computed = compute_invoice_dates_and_balance(invoice_date, amount, amount_paid)
        due_date = computed['due_date']

    balance_due = round(amount + adjustment_amount - amount_paid, 2)

    with db() as conn:
        if inv_id:
            old = conn.execute("SELECT * FROM invoices WHERE id=?", [inv_id]).fetchone()
            old_dict = dict(old) if old else {}
            conn.execute("""
                UPDATE invoices SET
                    invoice_date=?, due_date=?, job_code=?, client_id=?,
                    description_of_work=?, amount=?, amount_paid=?, balance_due=?,
                    adjustment_amount=?, adjustment_note=?,
                    notes=?, updated_at=datetime('now')
                WHERE id=?
            """, [invoice_date, due_date, job_code, client_id,
                  description, amount, amount_paid, balance_due,
                  adjustment_amount, adjustment_note, notes, inv_id])
            update_invoice_status(inv_id, conn)
            log_action(conn, 'invoices', inv_id, 'UPDATE', old_data=old_dict)
            flash(f'Invoice updated.', 'success')
            return redirect(url_for('phase5.invoice_detail', inv_id=inv_id))
        else:
            inv_num = int(data.get('invoice_number') or _next_invoice_number(conn))
            # Resolve any UNIQUE collision (including soft-deleted rows)
            while conn.execute(
                "SELECT 1 FROM invoices WHERE invoice_number=?", [inv_num]
            ).fetchone():
                inv_num += 1
            cur = conn.execute("""
                INSERT INTO invoices
                    (invoice_number, invoice_date, due_date, job_code, client_id,
                     description_of_work, amount, amount_paid, balance_due,
                     adjustment_amount, adjustment_note, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, [inv_num, invoice_date, due_date, job_code, client_id,
                  description, amount, amount_paid, balance_due,
                  adjustment_amount, adjustment_note, notes])
            new_id = cur.lastrowid
            update_invoice_status(new_id, conn)
            log_action(conn, 'invoices', new_id, 'INSERT',
                       new_data={'invoice_number': inv_num, 'amount': amount})
            flash(f'Invoice #{inv_num} created.', 'success')
            return redirect(url_for('phase5.invoice_detail', inv_id=new_id))


# ════════════════════════════════════════════════════════════════
#  DETAIL
# ════════════════════════════════════════════════════════════════

@phase5.route('/invoices/<int:inv_id>')
def invoice_detail(inv_id):
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        invoice = conn.execute("""
            SELECT i.*, c.full_name AS client_name, c.customer_id,
                   c.address, c.city_state_zip, c.phone1, c.email1,
                   j.description AS job_desc
            FROM invoices i
            LEFT JOIN clients c ON i.client_id = c.id
            LEFT JOIN jobs    j ON i.job_code  = j.job_code
            WHERE i.id=? AND i.is_deleted=0
        """, [inv_id]).fetchone()
        if not invoice:
            flash('Invoice not found.', 'error')
            return redirect(url_for('phase5.invoices'))

        today = datetime.now().strftime('%Y-%m-%d')
        inv   = dict(invoice)
        inv['days_overdue'] = _days_overdue(inv.get('due_date',''), today)

        # Linked timesheet entries (by job_code + invoice_number)
        ts_rows = conn.execute("""
            SELECT t.*, e.first_name || ' ' || e.last_name AS emp_name
            FROM timesheet t
            LEFT JOIN employees e ON t.emp_id = e.emp_id
            WHERE t.is_deleted=0
              AND (t.job_code=? OR t.invoice_number=?)
            ORDER BY t.entry_date
        """, [inv['job_code'] or '__NONE__',
              str(inv['invoice_number'])]).fetchall()

        ts_summary = {
            'total_hours':  sum(r['hours'] for r in ts_rows),
            'total_bill':   sum(r['bill_amount'] for r in ts_rows),
            'total_cost':   sum(r['cost_amount'] for r in ts_rows),
        }

        # Payment history (ledger entries referencing this invoice)
        payments = conn.execute("""
            SELECT * FROM ledger
            WHERE invoice_number=? AND amount > 0 AND is_deleted=0
            ORDER BY entry_date DESC
        """, [str(inv['invoice_number'])]).fetchall()

        return render_template('invoice_detail.html',
            config=config, badges=badges, today=today,
            invoice=inv,
            ts_rows=[dict(r) for r in ts_rows],
            ts_summary=ts_summary,
            payments=[dict(p) for p in payments],
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  RECORD PAYMENT
# ════════════════════════════════════════════════════════════════

@phase5.route('/invoices/<int:inv_id>/payment', methods=['POST'])
def invoice_payment(inv_id):
    data           = request.json if request.is_json else request.form.to_dict()
    raw_amt        = data.get('payment_amount', 0)
    if isinstance(raw_amt, (int, float)):
        payment_amount = float(raw_amt)
    else:
        payment_amount = float(str(raw_amt).replace(',','').replace('$','') or 0)
    payment_date   = (data.get('payment_date') or datetime.now().strftime('%Y-%m-%d')).strip()
    notes          = (data.get('notes') or '').strip()

    if payment_amount <= 0:
        if request.is_json:
            return jsonify({'error': 'Payment amount must be positive'}), 400
        flash('Payment amount must be positive', 'error')
        return redirect(url_for('phase5.invoice_detail', inv_id=inv_id))

    with db() as conn:
        inv = conn.execute(
            "SELECT * FROM invoices WHERE id=? AND is_deleted=0", [inv_id]
        ).fetchone()
        if not inv:
            if request.is_json:
                return jsonify({'error': 'Invoice not found'}), 404
            flash('Invoice not found', 'error')
            return redirect(url_for('phase5.invoices'))

        new_paid    = round(float(inv['amount_paid']) + payment_amount, 2)
        new_balance = round(float(inv['amount']) - new_paid, 2)

        conn.execute("""
            UPDATE invoices
            SET amount_paid=?, balance_due=?, updated_at=datetime('now')
            WHERE id=?
        """, [new_paid, new_balance, inv_id])

        update_invoice_status(inv_id, conn)

        # Also create a ledger entry for this payment
        inv_num = inv['invoice_number']
        job_code = inv['job_code'] or ''
        conn.execute("""
            INSERT INTO ledger
                (entry_date, job_code, invoice_number, category,
                 description, amount, status)
            VALUES (?,?,?,'Income',?,?,?)
        """, [payment_date, job_code, str(inv_num),
              notes or f'Payment on Invoice #{inv_num}',
              payment_amount, 'Cleared'])

        log_action(conn, 'invoices', inv_id, 'UPDATE',
                   old_data={'amount_paid': inv['amount_paid']},
                   new_data={'amount_paid': new_paid, 'payment_amount': payment_amount})

    if request.is_json:
        conn2 = get_connection()
        updated = conn2.execute("SELECT * FROM invoices WHERE id=?", [inv_id]).fetchone()
        conn2.close()
        return jsonify({'success': True, 'invoice': dict(updated)})

    flash(f'Payment of ${payment_amount:,.2f} recorded.', 'success')
    return redirect(url_for('phase5.invoice_detail', inv_id=inv_id))


# ════════════════════════════════════════════════════════════════
#  DELETE
# ════════════════════════════════════════════════════════════════

@phase5.route('/invoices/<int:inv_id>/delete', methods=['POST'])
def invoice_delete(inv_id):
    with db() as conn:
        soft_delete(conn, 'invoices', inv_id)
    if request.is_json:
        return jsonify({'success': True})
    flash('Invoice deleted (Undo to restore).', 'success')
    return redirect(url_for('phase5.invoices'))


# ════════════════════════════════════════════════════════════════
#  PDF GENERATION
# ════════════════════════════════════════════════════════════════

@phase5.route('/invoices/<int:inv_id>/pdf')
def invoice_pdf(inv_id):
    """Generate a print-ready invoice PDF via WeasyPrint; fall back to HTML."""
    conn = get_connection()
    try:
        invoice = conn.execute("""
            SELECT i.*, c.full_name AS client_name, c.customer_id,
                   c.address, c.city_state_zip, c.phone1, c.email1,
                   j.description AS job_desc
            FROM invoices i
            LEFT JOIN clients c ON i.client_id = c.id
            LEFT JOIN jobs    j ON i.job_code  = j.job_code
            WHERE i.id=? AND i.is_deleted=0
        """, [inv_id]).fetchone()
        if not invoice:
            flash('Invoice not found.', 'error')
            return redirect(url_for('phase5.invoices'))

        config = _cfg(conn)
        ts_rows = conn.execute("""
            SELECT t.*, e.first_name || ' ' || e.last_name AS emp_name
            FROM timesheet t
            LEFT JOIN employees e ON t.emp_id = e.emp_id
            WHERE t.is_deleted=0 AND (t.job_code=? OR t.invoice_number=?)
            ORDER BY t.entry_date
        """, [invoice['job_code'] or '__NONE__',
              str(invoice['invoice_number'])]).fetchall()

        today = datetime.now().strftime('%Y-%m-%d')
        inv   = dict(invoice)
        inv['days_overdue'] = _days_overdue(inv.get('due_date',''), today)

        html_str = render_template('invoice_pdf.html',
            config=config, invoice=inv,
            ts_rows=[dict(r) for r in ts_rows],
            today=today,
        )

        # Try WeasyPrint
        try:
            from weasyprint import HTML as WHP
            pdf_bytes = WHP(string=html_str).write_pdf()
            response = make_response(pdf_bytes)
            response.headers['Content-Type'] = 'application/pdf'
            response.headers['Content-Disposition'] = \
                f'inline; filename=invoice_{invoice["invoice_number"]}.pdf'
            return response
        except ImportError:
            # WeasyPrint not installed — serve printable HTML
            response = make_response(html_str)
            response.headers['Content-Type'] = 'text/html'
            return response
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  AGING REPORT API
# ════════════════════════════════════════════════════════════════

@phase5.route('/api/invoices/aging')
def api_aging():
    """Return aging buckets for all open invoices."""
    conn = get_connection()
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        rows  = conn.execute("""
            SELECT i.id, i.invoice_number, i.invoice_date, i.due_date,
                   i.amount, i.balance_due, i.status,
                   c.full_name AS client_name
            FROM invoices i
            LEFT JOIN clients c ON i.client_id = c.id
            WHERE i.is_deleted=0 AND i.status != 'Paid'
            ORDER BY i.due_date
        """).fetchall()

        buckets = {'0-30': [], '31-60': [], '61-90': [], '90+': []}
        for r in rows:
            d = dict(r)
            d['days_overdue'] = _days_overdue(d.get('due_date',''), today)
            bucket = _aging_bucket(d.get('due_date',''), today)
            buckets[bucket].append(d)

        totals = {
            k: {'count': len(v), 'total': round(sum(r['balance_due'] for r in v), 2)}
            for k, v in buckets.items()
        }
        return jsonify({'buckets': buckets, 'totals': totals, 'today': today})
    finally:
        conn.close()


@phase5.route('/api/invoices/summary')
def api_invoice_summary():
    """YTD + all-time invoice summary stats."""
    conn = get_connection()
    try:
        year = int(request.args.get('year', datetime.now().year))
        ytd  = conn.execute("""
            SELECT
                COUNT(*)                                                     AS count,
                COALESCE(SUM(amount),0)                                      AS invoiced,
                COALESCE(SUM(amount_paid),0)                                 AS collected,
                COALESCE(SUM(CASE WHEN status='Paid' THEN amount ELSE 0 END),0) AS paid_total,
                COALESCE(SUM(CASE WHEN status!='Paid' THEN balance_due ELSE 0 END),0) AS outstanding,
                COUNT(CASE WHEN status='Overdue' THEN 1 END)                 AS overdue_count
            FROM invoices
            WHERE is_deleted=0 AND substr(invoice_date,1,4)=?
        """, [str(year)]).fetchone()
        return jsonify(dict(ytd))
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  EXPORT
# ════════════════════════════════════════════════════════════════

@phase5.route('/invoices/export')
def invoices_export():
    status_filter = request.args.get('status', '')
    client_filter = request.args.get('client', '')
    year_filter   = request.args.get('year', '')

    conn = get_connection()
    try:
        where  = ["i.is_deleted=0"]
        params = []
        if status_filter:
            where.append("i.status=?"); params.append(status_filter)
        if client_filter:
            where.append("i.client_id=?"); params.append(client_filter)
        if year_filter:
            where.append("substr(i.invoice_date,1,4)=?"); params.append(year_filter)

        rows = conn.execute(f"""
            SELECT i.*, c.full_name AS client_name
            FROM invoices i
            LEFT JOIN clients c ON i.client_id=c.id
            WHERE {' AND '.join(where)}
            ORDER BY i.invoice_number DESC
        """, params).fetchall()

        def generate():
            cols = ['invoice_number','invoice_date','due_date','client_name',
                    'job_code','description_of_work','amount','amount_paid',
                    'balance_due','status','notes']
            yield ','.join(cols) + '\n'
            for r in rows:
                d = dict(r)
                yield ','.join(
                    f'"{str(d.get(c,"")).replace(chr(34),chr(34)*2)}"'
                    for c in cols
                ) + '\n'

        fname = f"invoices_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        return Response(
            stream_with_context(generate()),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={fname}'}
        )
    finally:
        conn.close()
