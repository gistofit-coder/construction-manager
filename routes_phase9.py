"""
Phase 9 Routes — Job Estimates
Full estimate workflow:
  - List view with status badges, totals, filter by status/job/client
  - Estimate builder: header (job, client, overhead/insurance/profit %) + dynamic line items
  - Line items: category, labor hrs × rate, materials, subs, other, markup %, sell price
  - Live totals sidebar: direct costs → overhead → profit → total sell price
  - Auto-number: EST-YYYY-NNN
  - Status workflow: Draft → Sent → Accepted / Rejected
  - Revise: clone to new version (keeps history)
  - Convert accepted estimate to Invoice
  - Actuals vs Estimate report: per-category budget vs real spend from ledger + timesheet
  - PDF generation: professional estimate document
  - Export CSV
"""
import json
from datetime import date, datetime
from flask import (Blueprint, render_template, request, jsonify,
                   redirect, url_for, flash, Response, stream_with_context,
                   make_response)

from database import db, get_connection
from automations import log_action, soft_delete

phase9 = Blueprint('phase9', __name__)

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

def _next_estimate_number(conn, prefix='EST'):
    """Generate next estimate number: EST-YYYY-NNN"""
    year = date.today().year
    last = conn.execute("""
        SELECT estimate_number FROM job_estimates
        WHERE estimate_number LIKE ? AND is_deleted=0
        ORDER BY id DESC LIMIT 1
    """, [f"{prefix}-{year}-%"]).fetchone()
    if last:
        try:
            seq = int(last['estimate_number'].split('-')[-1]) + 1
        except (ValueError, IndexError):
            seq = 1
    else:
        seq = 1
    return f"{prefix}-{year}-{seq:03d}"

def _compute_totals(line_items: list, overhead_pct: float, insurance_pct: float,
                    owner_wages_pct: float, profit_pct: float) -> dict:
    """Compute estimate totals from line items and overhead percentages."""
    total_labor       = sum(li.get('labor_cost', 0) or 0 for li in line_items)
    total_materials   = sum(li.get('materials_cost', 0) or 0 for li in line_items)
    total_subs        = sum(li.get('subcontractor_cost', 0) or 0 for li in line_items)
    total_other       = sum(li.get('other_cost', 0) or 0 for li in line_items)
    total_direct      = round(total_labor + total_materials + total_subs + total_other, 2)

    overhead_amt      = round(total_direct * (overhead_pct / 100), 2)
    insurance_amt     = round(total_direct * (insurance_pct / 100), 2)
    owner_wages_amt   = round(total_direct * (owner_wages_pct / 100), 2)
    subtotal_w_burden = round(total_direct + overhead_amt + insurance_amt + owner_wages_amt, 2)
    profit_amt        = round(subtotal_w_burden * (profit_pct / 100), 2)
    total_estimate    = round(subtotal_w_burden + profit_amt, 2)

    # Also sum sell prices if set at line-item level
    total_sell_lines  = sum(li.get('sell_price', 0) or 0 for li in line_items)

    return {
        'total_labor':       total_labor,
        'total_materials':   total_materials,
        'total_subs':        total_subs,
        'total_other':       total_other,
        'total_direct':      total_direct,
        'overhead_amt':      overhead_amt,
        'insurance_amt':     insurance_amt,
        'owner_wages_amt':   owner_wages_amt,
        'subtotal_w_burden': subtotal_w_burden,
        'profit_amt':        profit_amt,
        'total_estimate':    total_estimate,
        'total_sell_lines':  total_sell_lines,
    }

def _load_line_items(estimate_id, conn) -> list:
    rows = conn.execute("""
        SELECT * FROM estimate_line_items
        WHERE estimate_id=? AND is_deleted=0
        ORDER BY id
    """, [estimate_id]).fetchall()
    return [dict(r) for r in rows]

def _save_line_items(estimate_id, items_data: list, conn):
    """Delete existing and reinsert all line items for an estimate."""
    conn.execute("UPDATE estimate_line_items SET is_deleted=1 WHERE estimate_id=?", [estimate_id])
    for item in items_data:
        labor_hrs  = float(item.get('labor_hours_estimated') or 0)
        labor_rate = float(item.get('labor_rate') or 0)
        labor_cost = round(labor_hrs * labor_rate, 2)
        mat_cost   = float(item.get('materials_cost') or 0)
        sub_cost   = float(item.get('subcontractor_cost') or 0)
        oth_cost   = float(item.get('other_cost') or 0)
        total_cost = round(labor_cost + mat_cost + sub_cost + oth_cost, 2)
        markup_pct = float(item.get('markup_pct') or 0)
        sell_price = round(total_cost * (1 + markup_pct / 100), 2) if markup_pct else float(item.get('sell_price') or total_cost)

        category = (item.get('category') or '').strip()
        if not category:
            continue  # skip blank rows

        conn.execute("""
            INSERT INTO estimate_line_items
                (estimate_id, category, labor_hours_estimated, labor_rate, labor_cost,
                 materials_cost, subcontractor_cost, other_cost, total_cost,
                 markup_pct, sell_price, notes, is_deleted)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)
        """, [estimate_id, category, labor_hrs, labor_rate, labor_cost,
              mat_cost, sub_cost, oth_cost, total_cost,
              markup_pct, sell_price, (item.get('notes') or '').strip()])


# ════════════════════════════════════════════════════════════════
#  ESTIMATES LIST
# ════════════════════════════════════════════════════════════════

@phase9.route('/estimates')
def estimates():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        status_f = request.args.get('status', '')
        job_f    = request.args.get('job', '')
        client_f = request.args.get('client', '')
        year_f   = request.args.get('year', '')

        where  = ["je.is_deleted=0"]
        params = []
        if status_f:
            where.append("je.status=?"); params.append(status_f)
        if job_f:
            where.append("j.job_code LIKE ?"); params.append(f"%{job_f}%")
        if client_f:
            where.append("c.full_name LIKE ?"); params.append(f"%{client_f}%")
        if year_f:
            where.append("substr(je.estimate_date,1,4)=?"); params.append(year_f)

        estimates_rows = conn.execute(f"""
            SELECT je.*,
                   j.job_code, j.description AS job_desc,
                   c.full_name AS client_name
            FROM job_estimates je
            LEFT JOIN jobs j ON je.job_id = j.id
            LEFT JOIN clients c ON je.client_id = c.id
            WHERE {' AND '.join(where)}
            ORDER BY je.estimate_date DESC, je.id DESC
        """, params).fetchall()

        estimates_list = [dict(e) for e in estimates_rows]

        # Summary tiles
        all_counts = conn.execute("""
            SELECT status, COUNT(*) AS cnt, SUM(total_estimate) AS total
            FROM job_estimates WHERE is_deleted=0
            GROUP BY status
        """).fetchall()
        status_summary = {r['status']: {'count': r['cnt'], 'total': r['total'] or 0}
                          for r in all_counts}

        # For filters
        jobs    = conn.execute("SELECT job_code, description FROM jobs WHERE is_deleted=0 ORDER BY job_code").fetchall()
        clients = conn.execute("SELECT id, full_name FROM clients WHERE is_deleted=0 ORDER BY full_name").fetchall()
        years   = conn.execute("""
            SELECT DISTINCT substr(estimate_date,1,4) AS yr FROM job_estimates
            WHERE is_deleted=0 ORDER BY yr DESC
        """).fetchall()

        return render_template('estimates.html',
            config=config, badges=badges,
            estimates=estimates_list,
            status_summary=status_summary,
            jobs=[dict(j) for j in jobs],
            clients=[dict(c) for c in clients],
            years=[r['yr'] for r in years],
            status_f=status_f, job_f=job_f, client_f=client_f, year_f=year_f,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CREATE ESTIMATE
# ════════════════════════════════════════════════════════════════

@phase9.route('/estimates/new', methods=['GET', 'POST'])
def estimate_new():
    config = _cfg()
    badges = _badges()

    if request.method == 'GET':
        conn = get_connection()
        try:
            jobs    = conn.execute("SELECT id, job_code, description FROM jobs WHERE is_deleted=0 ORDER BY job_code").fetchall()
            clients = conn.execute("SELECT id, full_name FROM clients WHERE is_deleted=0 ORDER BY full_name").fetchall()
            cats    = conn.execute("SELECT category_name FROM work_categories WHERE is_deleted=0 ORDER BY category_name").fetchall()
            # Prefill from URL params
            prefill_job_id    = request.args.get('job_id', '')
            prefill_client_id = request.args.get('client_id', '')
            today = date.today().strftime('%Y-%m-%d')
            return render_template('estimate_form.html',
                config=config, badges=badges,
                estimate=None, line_items=[],
                jobs=[dict(j) for j in jobs],
                clients=[dict(c) for c in clients],
                categories=[dict(c) for c in cats],
                today=today,
                prefill_job_id=prefill_job_id,
                prefill_client_id=prefill_client_id,
            )
        finally:
            conn.close()
    return _estimate_save(None, request.form, request.form.getlist('li_category[]'))


@phase9.route('/estimates/<int:est_id>/edit', methods=['GET', 'POST'])
def estimate_edit(est_id):
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        est = conn.execute(
            "SELECT * FROM job_estimates WHERE id=? AND is_deleted=0", [est_id]
        ).fetchone()
        if not est:
            flash('Estimate not found.', 'error')
            return redirect(url_for('phase9.estimates'))

        if request.method == 'GET':
            est_d = dict(est)
            line_items = _load_line_items(est_id, conn)
            jobs    = conn.execute("SELECT id, job_code, description FROM jobs WHERE is_deleted=0 ORDER BY job_code").fetchall()
            clients = conn.execute("SELECT id, full_name FROM clients WHERE is_deleted=0 ORDER BY full_name").fetchall()
            today   = date.today().strftime('%Y-%m-%d')
            cats    = conn.execute("SELECT category_name FROM work_categories WHERE is_deleted=0 ORDER BY category_name").fetchall()
            return render_template('estimate_form.html',
                config=config, badges=badges,
                estimate=est_d, line_items=line_items,
                jobs=[dict(j) for j in jobs],
                clients=[dict(c) for c in clients],
                categories=[dict(c) for c in cats],
                today=today,
                prefill_job_id='', prefill_client_id='',
            )
    finally:
        conn.close()
    return _estimate_save(est_id, request.form, request.form.getlist('li_category[]'))


def _estimate_save(est_id, form, _unused=None):
    """Save (create or update) an estimate with its line items."""
    job_id        = form.get('job_id') or None
    client_id     = form.get('client_id') or None
    est_date      = (form.get('estimate_date') or date.today().strftime('%Y-%m-%d')).strip()
    status        = form.get('status', 'Draft').strip()
    notes         = (form.get('notes') or '').strip()

    try:
        overhead_pct    = float(form.get('overhead_pct') or 0)
        insurance_pct   = float(form.get('insurance_pct') or 0)
        owner_wages_pct = float(form.get('owner_wages_pct') or 0)
        profit_pct      = float(form.get('profit_pct') or 0)
    except ValueError:
        flash('Invalid percentage value.', 'error')
        return redirect(request.referrer or url_for('phase9.estimate_new'))

    # Collect line items from parallel form arrays
    categories   = form.getlist('li_category[]')
    descriptions = form.getlist('li_description[]')
    labor_hrs   = form.getlist('li_labor_hours[]')
    labor_rates = form.getlist('li_labor_rate[]')
    mat_costs   = form.getlist('li_materials[]')
    sub_costs   = form.getlist('li_subs[]')
    oth_costs   = form.getlist('li_other[]')
    markup_pcts = form.getlist('li_markup[]')
    sell_prices = form.getlist('li_sell_price[]')
    li_notes    = form.getlist('li_notes[]')

    items = []
    for i, cat in enumerate(categories):
        if not cat.strip():
            continue
        hrs = _f(labor_hrs, i)
        rate = _f(labor_rates, i)
        labor_cost = round(hrs * rate, 2)
        # materials may be a formula result string — evaluate safely
        mat_raw = _s(mat_costs, i)
        if mat_raw.startswith('='):
            try:
                expr = mat_raw[1:].replace(' ', '')
                mat_val = float(eval(compile(expr, '<string>', 'eval'), {"__builtins__": {}}, {}))
            except Exception:
                mat_val = 0.0
        else:
            mat_val = _f(mat_costs, i)

        desc = _s(descriptions, i) if descriptions else _s(li_notes, i)
        items.append({
            'category':              cat.strip(),
            'labor_hours_estimated': hrs,
            'labor_rate':            rate,
            'labor_cost':            labor_cost,
            'materials_cost':        mat_val,
            'subcontractor_cost':    _f(sub_costs, i),
            'other_cost':            _f(oth_costs, i),
            'markup_pct':            _f(markup_pcts, i),
            'sell_price':            _f(sell_prices, i),
            'notes':                 desc,
        })

    totals = _compute_totals(items, overhead_pct, insurance_pct, owner_wages_pct, profit_pct)

    with db() as conn:
        prefix = _cfg(conn).get('estimate_prefix') or 'EST'
        if est_id:
            old = conn.execute("SELECT * FROM job_estimates WHERE id=?", [est_id]).fetchone()
            conn.execute("""
                UPDATE job_estimates SET
                    job_id=?, client_id=?, estimate_date=?, status=?,
                    overhead_pct=?, insurance_pct=?, owner_wages_pct=?, profit_pct=?,
                    total_direct_costs=?, total_estimate=?, notes=?,
                    updated_at=datetime('now')
                WHERE id=?
            """, [job_id, client_id, est_date, status,
                  overhead_pct, insurance_pct, owner_wages_pct, profit_pct,
                  totals['total_direct'], totals['total_estimate'], notes, est_id])
            log_action(conn, 'job_estimates', est_id, 'UPDATE',
                       old_data=dict(old) if old else {})
            _save_line_items(est_id, items, conn)
            flash('Estimate updated.', 'success')
            return redirect(url_for('phase9.estimate_detail', est_id=est_id))
        else:
            est_num = _next_estimate_number(conn, prefix)
            cur = conn.execute("""
                INSERT INTO job_estimates
                    (job_id, client_id, estimate_date, estimate_number, status,
                     overhead_pct, insurance_pct, owner_wages_pct, profit_pct,
                     total_direct_costs, total_estimate, version_number, notes)
                VALUES (?,?,?,?,'Draft',?,?,?,?,?,?,1,?)
            """, [job_id, client_id, est_date, est_num,
                  overhead_pct, insurance_pct, owner_wages_pct, profit_pct,
                  totals['total_direct'], totals['total_estimate'], notes])
            new_id = cur.lastrowid
            log_action(conn, 'job_estimates', new_id, 'INSERT',
                       new_data={'estimate_number': est_num})
            _save_line_items(new_id, items, conn)
            flash(f'Estimate {est_num} created.', 'success')
            return redirect(url_for('phase9.estimate_detail', est_id=new_id))


def _f(lst, i, default=0.0):
    try:
        return float(lst[i]) if i < len(lst) and lst[i] else default
    except (ValueError, TypeError):
        return default

def _s(lst, i, default=''):
    return lst[i].strip() if i < len(lst) else default


# ════════════════════════════════════════════════════════════════
#  ESTIMATE DETAIL
# ════════════════════════════════════════════════════════════════

@phase9.route('/estimates/<int:est_id>')
def estimate_detail(est_id):
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        est = conn.execute("""
            SELECT je.*,
                   j.job_code, j.description AS job_desc, j.status AS job_status,
                   c.full_name AS client_name, c.address, c.phone1, c.email1
            FROM job_estimates je
            LEFT JOIN jobs j ON je.job_id = j.id
            LEFT JOIN clients c ON je.client_id = c.id
            WHERE je.id=? AND je.is_deleted=0
        """, [est_id]).fetchone()
        if not est:
            flash('Estimate not found.', 'error')
            return redirect(url_for('phase9.estimates'))

        est_d      = dict(est)
        line_items = _load_line_items(est_id, conn)
        totals     = _compute_totals(
            line_items,
            float(est_d.get('overhead_pct') or 0),
            float(est_d.get('insurance_pct') or 0),
            float(est_d.get('owner_wages_pct') or 0),
            float(est_d.get('profit_pct') or 0),
        )

        # Version history
        versions = conn.execute("""
            SELECT id, estimate_number, version_number, status, estimate_date,
                   total_estimate, created_at
            FROM job_estimates
            WHERE (job_id=? OR id=?) AND is_deleted=0
            ORDER BY version_number
        """, [est_d.get('job_id'), est_id]).fetchall()

        # Actuals vs estimate data (from view)
        actuals = conn.execute("""
            SELECT * FROM job_actuals_vs_estimate
            WHERE estimate_id=?
        """, [est_id]).fetchall() if est_d.get('job_id') else []

        # Actual labor from timesheet
        actual_labor = 0.0
        if est_d.get('job_code'):
            row = conn.execute("""
                SELECT COALESCE(SUM(cost_amount),0) AS total
                FROM timesheet WHERE job_code=? AND is_deleted=0
            """, [est_d['job_code']]).fetchone()
            actual_labor = float(row['total'])

        return render_template('estimate_detail.html',
            config=config, badges=badges,
            est=est_d, line_items=line_items, totals=totals,
            versions=[dict(v) for v in versions],
            actuals=[dict(a) for a in actuals],
            actual_labor=actual_labor,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  STATUS ACTIONS
# ════════════════════════════════════════════════════════════════

@phase9.route('/api/estimates/<int:est_id>/status', methods=['POST'])
def estimate_status(est_id):
    data      = request.json or {}
    new_status = data.get('status', '').strip()
    valid = ('Draft', 'Sent', 'Accepted', 'Rejected', 'Revised')
    if new_status not in valid:
        return jsonify({'error': f'Invalid status. Must be one of: {valid}'}), 400

    with db() as conn:
        old = conn.execute("SELECT * FROM job_estimates WHERE id=? AND is_deleted=0", [est_id]).fetchone()
        if not old:
            return jsonify({'error': 'Not found'}), 404
        conn.execute("""
            UPDATE job_estimates SET status=?, updated_at=datetime('now') WHERE id=?
        """, [new_status, est_id])
        log_action(conn, 'job_estimates', est_id, 'UPDATE',
                   old_data=dict(old), new_data={'status': new_status})
    return jsonify({'success': True, 'status': new_status})


# ════════════════════════════════════════════════════════════════
#  REVISE — clone estimate as next version
# ════════════════════════════════════════════════════════════════

@phase9.route('/api/estimates/<int:est_id>/revise', methods=['POST'])
def estimate_revise(est_id):
    """Clone the estimate as a new revision (version_number + 1), mark original as Revised."""
    conn = get_connection()
    try:
        old = conn.execute(
            "SELECT * FROM job_estimates WHERE id=? AND is_deleted=0", [est_id]
        ).fetchone()
        if not old:
            conn.close()
            return jsonify({'error': 'Not found'}), 404
        old = dict(old)
        old_items = _load_line_items(est_id, conn)
    finally:
        conn.close()

    with db() as conn:
        prefix   = _cfg(conn).get('estimate_prefix') or 'EST'
        new_num  = _next_estimate_number(conn, prefix)
        new_ver  = (old.get('version_number') or 1) + 1

        cur = conn.execute("""
            INSERT INTO job_estimates
                (job_id, client_id, estimate_date, estimate_number, status,
                 overhead_pct, insurance_pct, owner_wages_pct, profit_pct,
                 total_direct_costs, total_estimate, version_number, notes)
            VALUES (?,?,date('now'),?,?,?,?,?,?,?,?,?,?)
        """, [old['job_id'], old['client_id'], new_num, 'Draft',
              old['overhead_pct'], old['insurance_pct'],
              old['owner_wages_pct'], old['profit_pct'],
              old['total_direct_costs'], old['total_estimate'],
              new_ver, old['notes']])
        new_id = cur.lastrowid

        # Clone line items
        for li in old_items:
            conn.execute("""
                INSERT INTO estimate_line_items
                    (estimate_id, category, labor_hours_estimated, labor_rate, labor_cost,
                     materials_cost, subcontractor_cost, other_cost, total_cost,
                     markup_pct, sell_price, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, [new_id, li['category'], li['labor_hours_estimated'], li['labor_rate'],
                  li['labor_cost'], li['materials_cost'], li['subcontractor_cost'],
                  li['other_cost'], li['total_cost'], li['markup_pct'], li['sell_price'],
                  li['notes']])

        # Mark original as Revised
        conn.execute("""
            UPDATE job_estimates SET status='Revised', updated_at=datetime('now') WHERE id=?
        """, [est_id])
        log_action(conn, 'job_estimates', new_id, 'INSERT',
                   new_data={'revised_from': est_id, 'version': new_ver})

    return jsonify({'success': True, 'new_estimate_id': new_id, 'estimate_number': new_num})


# ════════════════════════════════════════════════════════════════
#  CONVERT TO INVOICE
# ════════════════════════════════════════════════════════════════

@phase9.route('/api/estimates/<int:est_id>/convert-to-invoice', methods=['POST'])
def estimate_to_invoice(est_id):
    """Create a draft invoice from an accepted estimate."""
    conn = get_connection()
    try:
        est = conn.execute(
            "SELECT * FROM job_estimates WHERE id=? AND is_deleted=0", [est_id]
        ).fetchone()
        if not est:
            conn.close()
            return jsonify({'error': 'Not found'}), 404
        est = dict(est)
    finally:
        conn.close()

    # Get job info for job_code
    conn = get_connection()
    try:
        job = conn.execute("SELECT job_code FROM jobs WHERE id=?", [est['job_id']]).fetchone() if est['job_id'] else None
        job_code = job['job_code'] if job else ''
        # Get next invoice number
        last_inv = conn.execute(
            "SELECT MAX(invoice_number) AS mx FROM invoices WHERE is_deleted=0"
        ).fetchone()
        inv_num = (last_inv['mx'] or 999) + 1
    finally:
        conn.close()

    with db() as conn:
        due_date = ''
        cur = conn.execute("""
            INSERT INTO invoices
                (invoice_number, invoice_date, due_date, job_code, client_id,
                 description_of_work, amount, amount_paid, balance_due, status, notes)
            VALUES (?,date('now'),?,?,?,?,?,0,?,  'Pending',?)
        """, [inv_num, due_date, job_code, est['client_id'],
              f"Per Estimate {est['estimate_number']}",
              est['total_estimate'], est['total_estimate'],
              f"Converted from estimate {est['estimate_number']}"])
        inv_id = cur.lastrowid
        log_action(conn, 'invoices', inv_id, 'INSERT',
                   new_data={'from_estimate': est_id, 'estimate_number': est['estimate_number']})

    return jsonify({'success': True, 'invoice_id': inv_id, 'invoice_number': inv_num})


# ════════════════════════════════════════════════════════════════
#  DELETE
# ════════════════════════════════════════════════════════════════

@phase9.route('/api/estimates/<int:est_id>/delete', methods=['POST'])
def estimate_delete(est_id):
    with db() as conn:
        soft_delete(conn, 'job_estimates', est_id)
        conn.execute(
            "UPDATE estimate_line_items SET is_deleted=1 WHERE estimate_id=?", [est_id]
        )
    return jsonify({'success': True})


# ════════════════════════════════════════════════════════════════
#  LIVE TOTALS API  (called by form JS on field change)
# ════════════════════════════════════════════════════════════════

@phase9.route('/api/estimates/preview-totals', methods=['POST'])
def preview_totals():
    """Compute live totals from line item data without saving."""
    data = request.json or {}
    try:
        overhead_pct    = float(data.get('overhead_pct') or 0)
        insurance_pct   = float(data.get('insurance_pct') or 0)
        owner_wages_pct = float(data.get('owner_wages_pct') or 0)
        profit_pct      = float(data.get('profit_pct') or 0)
        items           = data.get('line_items', [])
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid input'}), 400

    totals = _compute_totals(items, overhead_pct, insurance_pct, owner_wages_pct, profit_pct)
    return jsonify(totals)


# ════════════════════════════════════════════════════════════════
#  ACTUALS VS ESTIMATE REPORT
# ════════════════════════════════════════════════════════════════

@phase9.route('/estimates/<int:est_id>/actuals')
def estimate_actuals(est_id):
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        est = conn.execute("""
            SELECT je.*, j.job_code, j.description AS job_desc, c.full_name AS client_name
            FROM job_estimates je
            LEFT JOIN jobs j ON je.job_id = j.id
            LEFT JOIN clients c ON je.client_id = c.id
            WHERE je.id=? AND je.is_deleted=0
        """, [est_id]).fetchone()
        if not est:
            flash('Estimate not found.', 'error')
            return redirect(url_for('phase9.estimates'))

        est_d      = dict(est)
        line_items = _load_line_items(est_id, conn)
        totals     = _compute_totals(
            line_items,
            float(est_d.get('overhead_pct') or 0),
            float(est_d.get('insurance_pct') or 0),
            float(est_d.get('owner_wages_pct') or 0),
            float(est_d.get('profit_pct') or 0),
        )
        job_code = est_d.get('job_code') or ''

        # Actual labor from timesheet
        actual_labor = 0.0
        actual_labor_hours = 0.0
        if job_code:
            row = conn.execute("""
                SELECT COALESCE(SUM(cost_amount),0) AS total, COALESCE(SUM(hours),0) AS hrs
                FROM timesheet WHERE job_code=? AND is_deleted=0
            """, [job_code]).fetchone()
            actual_labor       = float(row['total'])
            actual_labor_hours = float(row['hrs'])

        # Actual costs per category from ledger
        actual_by_category = {}
        if job_code:
            rows = conn.execute("""
                SELECT category, SUM(amount) AS total
                FROM ledger WHERE job_code=? AND is_cogs=1 AND is_deleted=0
                GROUP BY category
            """, [job_code]).fetchall()
            actual_by_category = {r['category']: float(r['total']) for r in rows}

        # Build comparison rows per line item
        comparison = []
        for li in line_items:
            cat = li['category']
            est_labor_cost = float(li.get('labor_cost') or 0)
            est_mat        = float(li.get('materials_cost') or 0)
            est_subs       = float(li.get('subcontractor_cost') or 0)
            est_other      = float(li.get('other_cost') or 0)
            est_total      = float(li.get('total_cost') or 0)

            # Apportion actual labor proportionally
            total_est_labor = totals['total_labor'] or 1
            act_labor_share = round(actual_labor * (est_labor_cost / total_est_labor), 2) if total_est_labor else 0
            act_category    = actual_by_category.get(cat, 0)
            act_total       = round(act_labor_share + act_category, 2)

            variance      = round(est_total - act_total, 2)
            variance_pct  = round((variance / est_total * 100), 1) if est_total else 0

            comparison.append({
                'category':      cat,
                'est_labor':     est_labor_cost,
                'est_materials': est_mat,
                'est_subs':      est_subs,
                'est_other':     est_other,
                'est_total':     est_total,
                'act_labor':     act_labor_share,
                'act_materials': act_category,
                'act_total':     act_total,
                'variance':      variance,
                'variance_pct':  variance_pct,
                'over_budget':   variance < 0,
            })

        total_est_all = sum(r['est_total'] for r in comparison)
        total_act_all = sum(r['act_total'] for r in comparison)
        total_variance = round(total_est_all - total_act_all, 2)

        return render_template('estimate_actuals.html',
            config=config, badges=badges,
            est=est_d, totals=totals,
            comparison=comparison,
            total_est_all=total_est_all,
            total_act_all=total_act_all,
            total_variance=total_variance,
            actual_labor=actual_labor,
            actual_labor_hours=actual_labor_hours,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  PDF GENERATION
# ════════════════════════════════════════════════════════════════

@phase9.route('/estimates/<int:est_id>/pdf')
def estimate_pdf(est_id):
    """Return a print-ready HTML page (browser prints to PDF)."""
    conn = get_connection()
    try:
        est = conn.execute("""
            SELECT je.*,
                   j.job_code, j.description AS job_desc,
                   c.full_name AS client_name, c.address, c.city_state_zip,
                   c.phone1, c.email1
            FROM job_estimates je
            LEFT JOIN jobs j ON je.job_id = j.id
            LEFT JOIN clients c ON je.client_id = c.id
            WHERE je.id=? AND je.is_deleted=0
        """, [est_id]).fetchone()
        if not est:
            flash('Estimate not found.', 'error')
            return redirect(url_for('phase9.estimates'))
        est_d      = dict(est)
        line_items = _load_line_items(est_id, conn)
        config     = _cfg(conn)
        totals     = _compute_totals(
            line_items,
            float(est_d.get('overhead_pct') or 0),
            float(est_d.get('insurance_pct') or 0),
            float(est_d.get('owner_wages_pct') or 0),
            float(est_d.get('profit_pct') or 0),
        )
        customer_view = request.args.get('customer', '0') == '1'
        return render_template('estimate_pdf.html',
            config=config, est=est_d, line_items=line_items, totals=totals,
            customer_view=customer_view)
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  EXPORT CSV
# ════════════════════════════════════════════════════════════════

@phase9.route('/estimates/export')
def estimates_export():
    status_f = request.args.get('status', '')
    conn     = get_connection()
    try:
        where  = ["je.is_deleted=0"]
        params = []
        if status_f:
            where.append("je.status=?"); params.append(status_f)

        rows = conn.execute(f"""
            SELECT je.estimate_number, je.estimate_date, je.status, je.version_number,
                   j.job_code, c.full_name AS client_name,
                   je.total_direct_costs, je.total_estimate,
                   je.overhead_pct, je.profit_pct, je.notes
            FROM job_estimates je
            LEFT JOIN jobs j ON je.job_id = j.id
            LEFT JOIN clients c ON je.client_id = c.id
            WHERE {' AND '.join(where)}
            ORDER BY je.estimate_date DESC
        """, params).fetchall()

        COLS = ['estimate_number','estimate_date','status','version_number','job_code',
                'client_name','total_direct_costs','total_estimate',
                'overhead_pct','profit_pct','notes']

        def generate():
            yield ','.join(COLS) + '\n'
            for r in rows:
                d = dict(r)
                yield ','.join(
                    f'"{str(d.get(c,"")).replace(chr(34),chr(34)*2)}"' for c in COLS
                ) + '\n'

        fname = f"estimates_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        return Response(stream_with_context(generate()), mimetype='text/csv',
                        headers={'Content-Disposition': f'attachment; filename={fname}'})
    finally:
        conn.close()
