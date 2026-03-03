"""
Phase 2 Routes — Core Data Entry
Covers: Clients, Employees (+ Rates), Contractors, Jobs
All CRUD operations with full automation wiring.
"""
import json
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash, Response

from database import db, get_connection
from automations import (
    generate_customer_id, extract_last_name,
    get_rate_for_date, get_person_label,
    log_action, soft_delete, check_duplicate_client,
    get_cert_status,
)

def _normalise_phone(phone: str) -> str:
    """Store phones as digits-only (10 digits) for consistent formatting."""
    import re
    digits = re.sub(r'\D', '', str(phone or ''))
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    return digits  # store raw digits; display formatting done in template

phase2 = Blueprint('phase2', __name__)


# ─────────────────────────────────────────────
# Helpers shared across Phase 2 routes
# ─────────────────────────────────────────────

def _get_config():
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM company_config WHERE id=1").fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()

def _get_badges():
    from app import get_nav_badges
    return get_nav_badges()

def _next_emp_id(conn):
    row = conn.execute("SELECT MAX(emp_id) FROM employees").fetchone()
    return (row[0] or 0) + 1


# ════════════════════════════════════════════════════════════════
#  CLIENTS
# ════════════════════════════════════════════════════════════════

@phase2.route('/clients')
def clients():
    config = _get_config()
    badges = _get_badges()
    conn = get_connection()
    try:
        q = request.args.get('q', '').strip()
        status_filter = request.args.get('status', '')
        page = int(request.args.get('page', 1))
        per_page = 999999 if _get_config().get('continuous_scroll') else 50

        where = ["c.is_deleted=0"]
        params = []
        if q:
            where.append("(c.full_name LIKE ? OR c.customer_id LIKE ? OR c.phone1 LIKE ? OR c.email1 LIKE ?)")
            params += [f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"]
        if status_filter:
            where.append("c.status=?")
            params.append(status_filter)

        where_sql = " AND ".join(where)
        total = conn.execute(f"SELECT COUNT(*) FROM clients c WHERE {where_sql}", params).fetchone()[0]

        rows = conn.execute(f"""
            SELECT c.*,
                (SELECT COUNT(*) FROM jobs j WHERE j.client_id=c.id AND j.is_deleted=0) AS job_count,
                (SELECT COALESCE(SUM(i.amount),0) FROM invoices i WHERE i.client_id=c.id AND i.is_deleted=0) AS total_invoiced,
                (SELECT COALESCE(SUM(i.amount_paid),0) FROM invoices i WHERE i.client_id=c.id AND i.is_deleted=0) AS total_paid,
                (SELECT MAX(i.invoice_date) FROM invoices i WHERE i.client_id=c.id AND i.is_deleted=0) AS last_invoice_date
            FROM clients c
            WHERE {where_sql}
            ORDER BY c.last_name, c.full_name
            LIMIT ? OFFSET ?
        """, params + [per_page, (page-1)*per_page]).fetchall()

        return render_template('clients.html',
            config=config, badges=badges,
            clients=[dict(r) for r in rows],
            q=q, status_filter=status_filter,
            total=total, page=page, per_page=per_page,
            pages=(total + per_page - 1) // per_page,
        )
    finally:
        conn.close()


@phase2.route('/clients/new', methods=['GET', 'POST'])
def client_new():
    config = _get_config()
    badges = _get_badges()
    if request.method == 'POST':
        return _client_save(None)
    year = datetime.now().year
    return render_template('client_form.html',
        config=config, badges=badges,
        client={}, mode='new',
        default_year=year,
    )


@phase2.route('/clients/<int:client_id>', methods=['GET'])
def client_detail(client_id):
    config = _get_config()
    badges = _get_badges()
    conn = get_connection()
    try:
        client = conn.execute(
            "SELECT * FROM clients WHERE id=? AND is_deleted=0", [client_id]
        ).fetchone()
        if not client:
            flash('Client not found.', 'error')
            return redirect(url_for('phase2.clients'))
        client = dict(client)

        jobs = conn.execute(
            "SELECT * FROM jobs WHERE client_id=? AND is_deleted=0 ORDER BY start_date DESC",
            [client_id]
        ).fetchall()
        invoices = conn.execute(
            "SELECT * FROM invoices WHERE client_id=? AND is_deleted=0 ORDER BY invoice_date DESC",
            [client_id]
        ).fetchall()
        # Activity summary
        total_invoiced = sum(i['amount'] for i in invoices)
        total_paid = sum(i['amount_paid'] for i in invoices)

        return render_template('client_detail.html',
            config=config, badges=badges,
            client=client,
            jobs=[dict(j) for j in jobs],
            invoices=[dict(i) for i in invoices],
            total_invoiced=total_invoiced,
            total_paid=total_paid,
            balance_outstanding=total_invoiced - total_paid,
        )
    finally:
        conn.close()


@phase2.route('/clients/<int:client_id>/edit', methods=['GET', 'POST'])
def client_edit(client_id):
    config = _get_config()
    badges = _get_badges()
    if request.method == 'POST':
        return _client_save(client_id)
    conn = get_connection()
    try:
        client = conn.execute(
            "SELECT * FROM clients WHERE id=? AND is_deleted=0", [client_id]
        ).fetchone()
        if not client:
            flash('Client not found.', 'error')
            return redirect(url_for('phase2.clients'))
        return render_template('client_form.html',
            config=config, badges=badges,
            client=dict(client), mode='edit',
            default_year=datetime.now().year,
        )
    finally:
        conn.close()


def _client_save(client_id):
    """Shared save logic for new and edit client."""
    data = request.form.to_dict()
    full_name = data.get('full_name', '').strip()
    if not full_name:
        flash('Full name is required.', 'error')
        return redirect(request.referrer or url_for('phase2.clients'))

    year = int(data.get('year_acquired') or datetime.now().year)
    last_name = extract_last_name(full_name)

    with db() as conn:
        if client_id:
            old = conn.execute("SELECT * FROM clients WHERE id=?", [client_id]).fetchone()
            # Keep existing customer_id unless it's empty
            cid = data.get('customer_id', '').strip() or (dict(old)['customer_id'] if old else '')
            if not cid:
                cid = generate_customer_id(full_name, year, conn)
            conn.execute("""
                UPDATE clients SET
                    year_acquired=?, customer_id=?, last_name=?, full_name=?,
                    address=?, city_state_zip=?, phone1=?, phone2=?,
                    email1=?, email2=?, status=?, notes=?,
                    updated_at=datetime('now')
                WHERE id=?
            """, [year, cid, last_name, full_name,
                  data.get('address',''), data.get('city_state_zip',''),
                  _normalise_phone(data.get('phone1','')),
                  _normalise_phone(data.get('phone2','')),
                  data.get('email1',''), data.get('email2',''),
                  data.get('status','Active'), data.get('notes',''),
                  client_id])
            new = conn.execute("SELECT * FROM clients WHERE id=?", [client_id]).fetchone()
            log_action(conn, 'clients', client_id, 'UPDATE',
                       old_data=dict(old) if old else {}, new_data=dict(new))
            flash(f'Client "{full_name}" updated.', 'success')
            return redirect(url_for('phase2.client_detail', client_id=client_id))
        else:
            # Duplicate check
            dupes = check_duplicate_client(full_name, conn)
            if dupes:
                names = ', '.join(d['full_name'] for d in dupes[:3])
                flash(f'Warning: Similar clients exist: {names}. Saved anyway.', 'warning')
            cid = generate_customer_id(full_name, year, conn)
            cur = conn.execute("""
                INSERT INTO clients
                    (year_acquired, customer_id, last_name, full_name, address,
                     city_state_zip, phone1, phone2, email1, email2, status, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, [year, cid, last_name, full_name,
                  data.get('address',''), data.get('city_state_zip',''),
                  _normalise_phone(data.get('phone1','')),
                  _normalise_phone(data.get('phone2','')),
                  data.get('email1',''), data.get('email2',''),
                  data.get('status','Active'), data.get('notes','')])
            new_id = cur.lastrowid
            log_action(conn, 'clients', new_id, 'INSERT',
                       new_data={'full_name': full_name, 'customer_id': cid})
            flash(f'Client "{full_name}" created. ID: {cid}', 'success')
            return redirect(url_for('phase2.client_detail', client_id=new_id))


@phase2.route('/clients/<int:client_id>/delete', methods=['POST'])
def client_delete(client_id):
    with db() as conn:
        client = conn.execute("SELECT full_name FROM clients WHERE id=?", [client_id]).fetchone()
        soft_delete(conn, 'clients', client_id)
        name = client['full_name'] if client else str(client_id)
    flash(f'Client "{name}" archived. Use undo to restore.', 'success')
    return redirect(url_for('phase2.clients'))


# API: check for duplicate client name (used by JS)
@phase2.route('/api/clients/check-duplicate')
def api_client_duplicate():
    name = request.args.get('name', '')
    exclude_id = request.args.get('exclude_id')
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, full_name, customer_id FROM clients
            WHERE LOWER(full_name) LIKE ? AND is_deleted=0
            LIMIT 5
        """, [f"%{name.lower()[:30]}%"]).fetchall()
        results = [dict(r) for r in rows if str(r['id']) != str(exclude_id)]
        return jsonify(results)
    finally:
        conn.close()


# API: generate preview of customer ID
@phase2.route('/api/clients/preview-id')
def api_client_preview_id():
    name = request.args.get('name', '')
    year = int(request.args.get('year', datetime.now().year))
    if not name:
        return jsonify({'id': ''})
    conn = get_connection()
    try:
        cid = generate_customer_id(name, year, conn)
        return jsonify({'id': cid})
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  EMPLOYEES
# ════════════════════════════════════════════════════════════════

@phase2.route('/employees')
def employees():
    config = _get_config()
    badges = _get_badges()
    conn = get_connection()
    try:
        q = request.args.get('q', '').strip()
        status_filter = request.args.get('status', '')
        where = ["e.is_deleted=0"]
        params = []
        if q:
            where.append("(e.first_name LIKE ? OR e.last_name LIKE ? OR e.occupation LIKE ?)")
            params += [f"%{q}%", f"%{q}%", f"%{q}%"]
        if status_filter:
            where.append("e.status=?")
            params.append(status_filter)
        where_sql = " AND ".join(where)

        rows = conn.execute(f"""
            SELECT e.*,
                (SELECT er.bill_rate_per_hour FROM employee_rates er
                 WHERE er.emp_id=e.emp_id AND er.is_deleted=0
                 ORDER BY er.effective_date DESC LIMIT 1) AS current_bill_rate,
                (SELECT er.cost_rate_per_hour FROM employee_rates er
                 WHERE er.emp_id=e.emp_id AND er.is_deleted=0
                 ORDER BY er.effective_date DESC LIMIT 1) AS current_cost_rate,
                (SELECT COALESCE(SUM(pr.gross_pay),0) FROM payroll_runs pr
                 WHERE pr.emp_id=e.emp_id AND pr.year=? AND pr.is_deleted=0) AS ytd_gross
            FROM employees e
            WHERE {where_sql}
            ORDER BY e.last_name, e.first_name
        """, [datetime.now().year] + params).fetchall()

        return render_template('employees.html',
            config=config, badges=badges,
            employees=[dict(r) for r in rows],
            q=q, status_filter=status_filter,
        )
    finally:
        conn.close()


@phase2.route('/employees/new', methods=['GET', 'POST'])
def employee_new():
    config = _get_config()
    badges = _get_badges()
    if request.method == 'POST':
        return _employee_save(None)
    conn = get_connection()
    try:
        next_id = _next_emp_id(conn)
        return render_template('employee_form.html',
            config=config, badges=badges,
            employee={}, mode='new',
            next_emp_id=next_id,
        )
    finally:
        conn.close()


@phase2.route('/employees/<int:emp_id>')
def employee_detail(emp_id):
    config = _get_config()
    badges = _get_badges()
    conn = get_connection()
    try:
        emp = conn.execute(
            "SELECT * FROM employees WHERE emp_id=? AND is_deleted=0", [emp_id]
        ).fetchone()
        if not emp:
            flash('Employee not found.', 'error')
            return redirect(url_for('phase2.employees'))
        emp = dict(emp)

        rates = conn.execute("""
            SELECT * FROM employee_rates WHERE emp_id=? AND is_deleted=0
            ORDER BY effective_date DESC
        """, [emp_id]).fetchall()

        # YTD payroll summary
        year = datetime.now().year
        ytd = conn.execute("""
            SELECT
                COALESCE(SUM(gross_pay),0) AS ytd_gross,
                COALESCE(SUM(net_pay),0) AS ytd_net,
                COALESCE(SUM(ss_withheld),0) AS ytd_ss,
                COALESCE(SUM(medicare_withheld),0) AS ytd_medicare,
                COALESCE(SUM(fed_withholding),0) AS ytd_fed,
                COUNT(*) AS payroll_count
            FROM payroll_runs
            WHERE emp_id=? AND year=? AND is_deleted=0
        """, [emp_id, year]).fetchone()

        # Timesheet hours this year
        hours = conn.execute("""
            SELECT COALESCE(SUM(hours),0) AS ytd_hours
            FROM timesheet
            WHERE emp_id=? AND entry_date >= ? AND is_deleted=0
        """, [emp_id, f"{year}-01-01"]).fetchone()

        # Recent timesheet
        recent_ts = conn.execute("""
            SELECT * FROM timesheet WHERE emp_id=? AND is_deleted=0
            ORDER BY entry_date DESC LIMIT 10
        """, [emp_id]).fetchall()

        return render_template('employee_detail.html',
            config=config, badges=badges,
            emp=emp,
            rates=[dict(r) for r in rates],
            ytd=dict(ytd) if ytd else {},
            ytd_hours=float(hours['ytd_hours']) if hours else 0,
            recent_ts=[dict(r) for r in recent_ts],
            year=year,
        )
    finally:
        conn.close()


@phase2.route('/employees/<int:emp_id>/edit', methods=['GET', 'POST'])
def employee_edit(emp_id):
    config = _get_config()
    badges = _get_badges()
    if request.method == 'POST':
        return _employee_save(emp_id)
    conn = get_connection()
    try:
        emp = conn.execute(
            "SELECT * FROM employees WHERE emp_id=? AND is_deleted=0", [emp_id]
        ).fetchone()
        if not emp:
            flash('Employee not found.', 'error')
            return redirect(url_for('phase2.employees'))
        return render_template('employee_form.html',
            config=config, badges=badges,
            employee=dict(emp), mode='edit',
            next_emp_id=emp_id,
        )
    finally:
        conn.close()


def _employee_save(emp_id):
    data = request.form.to_dict()
    first = data.get('first_name', '').strip()
    last = data.get('last_name', '').strip()
    if not first or not last:
        flash('First and last name are required.', 'error')
        return redirect(request.referrer or url_for('phase2.employees'))

    with db() as conn:
        if emp_id:
            old = conn.execute("SELECT * FROM employees WHERE emp_id=?", [emp_id]).fetchone()
            conn.execute("""
                UPDATE employees SET
                    first_name=?, last_name=?, phone=?, email=?,
                    address=?, city_state_zip=?, gender=?, occupation=?,
                    hire_date=?, status=?, notes=?, updated_at=datetime('now')
                WHERE emp_id=?
            """, [first, last, data.get('phone',''), data.get('email',''),
                  data.get('address',''), data.get('city_state_zip',''),
                  data.get('gender',''), data.get('occupation',''),
                  data.get('hire_date',''), data.get('status','Active'),
                  data.get('notes',''), emp_id])
            log_action(conn, 'employees', emp_id, 'UPDATE', old_data=dict(old) if old else {})
            flash(f'{first} {last} updated.', 'success')
            return redirect(url_for('phase2.employee_detail', emp_id=emp_id))
        else:
            new_emp_id = int(data.get('emp_id') or _next_emp_id(conn))
            # Check for dup emp_id
            exists = conn.execute(
                "SELECT 1 FROM employees WHERE emp_id=? AND is_deleted=0", [new_emp_id]
            ).fetchone()
            if exists:
                new_emp_id = _next_emp_id(conn)
            conn.execute("""
                INSERT INTO employees
                    (emp_id, first_name, last_name, phone, email,
                     address, city_state_zip, gender, occupation,
                     hire_date, status, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, [new_emp_id, first, last,
                  data.get('phone',''), data.get('email',''),
                  data.get('address',''), data.get('city_state_zip',''),
                  data.get('gender',''), data.get('occupation',''),
                  data.get('hire_date',''), data.get('status','Active'),
                  data.get('notes','')])
            log_action(conn, 'employees', new_emp_id, 'INSERT',
                       new_data={'emp_id': new_emp_id, 'name': f'{first} {last}'})
            # Auto-add initial rate if provided
            bill_rate = data.get('initial_bill_rate','').strip()
            cost_rate = data.get('initial_cost_rate','').strip()
            if bill_rate or cost_rate:
                eff_date = data.get('hire_date') or datetime.now().strftime('%Y-%m-%d')
                conn.execute("""
                    INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour, notes)
                    VALUES (?,?,?,?,?)
                """, [new_emp_id, eff_date,
                      float(bill_rate or 0), float(cost_rate or 0),
                      'Initial rate at hire'])
            flash(f'Employee {first} {last} created. ID: {new_emp_id}', 'success')
            return redirect(url_for('phase2.employee_detail', emp_id=new_emp_id))


@phase2.route('/employees/<int:emp_id>/delete', methods=['POST'])
def employee_delete(emp_id):
    with db() as conn:
        emp = conn.execute(
            "SELECT first_name, last_name FROM employees WHERE emp_id=?", [emp_id]
        ).fetchone()
        conn.execute(
            "UPDATE employees SET is_deleted=1, updated_at=datetime('now') WHERE emp_id=?", [emp_id]
        )
        log_action(conn, 'employees', emp_id, 'DELETE', old_data=dict(emp) if emp else {})
    name = f"{emp['first_name']} {emp['last_name']}" if emp else str(emp_id)
    flash(f'Employee "{name}" deactivated. Use undo to restore.', 'success')
    return redirect(url_for('phase2.employees'))


# ── RATES ─────────────────────────────────────────────────────

@phase2.route('/employees/<int:emp_id>/rates/add', methods=['POST'])
def rate_add(emp_id):
    data = request.form.to_dict()
    eff_date = data.get('effective_date','').strip()
    bill = data.get('bill_rate_per_hour','').strip()
    cost = data.get('cost_rate_per_hour','').strip()
    if not eff_date or not bill:
        flash('Effective date and bill rate are required.', 'error')
        return redirect(url_for('phase2.employee_detail', emp_id=emp_id))
    with db() as conn:
        # Verify employee exists
        emp = conn.execute(
            "SELECT 1 FROM employees WHERE emp_id=? AND is_deleted=0", [emp_id]
        ).fetchone()
        if not emp:
            flash('Employee not found.', 'error')
            return redirect(url_for('phase2.employees'))
        # Check for existing rate on same date
        existing = conn.execute(
            "SELECT id FROM employee_rates WHERE emp_id=? AND effective_date=? AND is_deleted=0",
            [emp_id, eff_date]
        ).fetchone()
        if existing:
            conn.execute("""
                UPDATE employee_rates
                SET bill_rate_per_hour=?, cost_rate_per_hour=?, notes=?, updated_at=datetime('now')
                WHERE id=?
            """, [float(bill), float(cost or 0), data.get('notes',''), existing['id']])
            flash(f'Rate for {eff_date} updated.', 'success')
        else:
            conn.execute("""
                INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour, notes)
                VALUES (?,?,?,?,?)
            """, [emp_id, eff_date, float(bill), float(cost or 0), data.get('notes','')])
            flash(f'Rate effective {eff_date} added.', 'success')
    return redirect(url_for('phase2.employee_detail', emp_id=emp_id))


@phase2.route('/employees/rates/<int:rate_id>/delete', methods=['POST'])
def rate_delete(rate_id):
    with db() as conn:
        rate = conn.execute("SELECT emp_id FROM employee_rates WHERE id=?", [rate_id]).fetchone()
        emp_id = rate['emp_id'] if rate else None
        conn.execute(
            "UPDATE employee_rates SET is_deleted=1, updated_at=datetime('now') WHERE id=?",
            [rate_id]
        )
    flash('Rate record deleted.', 'success')
    return redirect(url_for('phase2.employee_detail', emp_id=emp_id) if emp_id else url_for('phase2.employees'))


# API for live rate preview
@phase2.route('/api/employees/<int:emp_id>/rate-on-date')
def api_rate_on_date(emp_id):
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    rates = get_rate_for_date(emp_id, date)
    label = get_person_label(emp_id)
    return jsonify({**rates, 'person_label': label})


# ════════════════════════════════════════════════════════════════
#  CONTRACTORS
# ════════════════════════════════════════════════════════════════

@phase2.route('/contractors')
def contractors():
    config = _get_config()
    badges = _get_badges()
    conn = get_connection()
    try:
        q = request.args.get('q', '').strip()
        trade_filter = request.args.get('trade', '')
        only_1099 = request.args.get('needs_1099', '')
        ctype_filter = request.args.get('ctype', '')  # 'contractor' | 'supplier' | ''
        page = int(request.args.get('page', 1))
        per_page = 999999  # full scroll always on contractors

        where = ["c.is_deleted=0"]
        params = []
        if q:
            where.append("(c.company_name LIKE ? OR c.contact_person LIKE ? OR c.trade_type LIKE ? OR c.website LIKE ?)")
            params += [f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"]
        if trade_filter:
            where.append("c.trade_type=?")
            params.append(trade_filter)
        if only_1099 == '1':
            where.append("c.requires_1099=1")
        if ctype_filter == 'supplier':
            where.append("c.vendor_type='Supplier'")
        elif ctype_filter == 'contractor':
            where.append("c.vendor_type='Subcontractor'")
        elif ctype_filter in ('Subcontractor', 'Supplier', 'Service Provider', 'Government/Tax', 'Other'):
            where.append("c.vendor_type=?")
            params.append(ctype_filter)
        where_sql = " AND ".join(where)

        total = conn.execute(f"SELECT COUNT(*) FROM contractors c WHERE {where_sql}", params).fetchone()[0]
        rows = conn.execute(f"""
            SELECT c.*,
                (SELECT COALESCE(SUM(l.amount),0) FROM ledger l
                 WHERE l.vendor=c.company_name AND l.is_deleted=0
                   AND l.entry_date >= date('now','start of year')) AS ytd_payments,
                (SELECT COUNT(*) FROM certificates cert
                 WHERE cert.contractor_id=c.id AND cert.is_deleted=0) AS cert_count,
                (SELECT MIN(cert.end_date) FROM certificates cert
                 WHERE cert.contractor_id=c.id AND cert.is_deleted=0
                   AND cert.end_date IS NOT NULL AND cert.end_date != '') AS next_cert_expiry
            FROM contractors c
            WHERE {where_sql}
            ORDER BY c.rank_preference DESC, c.company_name
            LIMIT ? OFFSET ?
        """, params + [per_page, (page-1)*per_page]).fetchall()

        # Get distinct trades for filter dropdown
        trades = conn.execute(
            "SELECT DISTINCT trade_type FROM contractors WHERE trade_type!='' AND is_deleted=0 ORDER BY trade_type"
        ).fetchall()

        # Add cert status to each row
        rows_out = []
        for r in rows:
            d = dict(r)
            d['cert_status'] = get_cert_status(d.get('next_cert_expiry','')) if d.get('next_cert_expiry') else None
            # Look up vendor→category mapping
            vc = conn.execute(
                "SELECT default_category FROM vendor_categories WHERE vendor_name=? AND is_deleted=0 LIMIT 1",
                [d.get('company_name','')]
            ).fetchone()
            d['vendor_category'] = vc['default_category'] if vc else ''
            rows_out.append(d)

        return render_template('contractors.html',
            config=config, badges=badges,
            contractors=rows_out,
            q=q, trade_filter=trade_filter, only_1099=only_1099,
            ctype_filter=ctype_filter,
            trades=[t[0] for t in trades],
            total=total, page=page, per_page=per_page,
            pages=1,
        )
    finally:
        conn.close()




@phase2.route('/clients/export')
def clients_export():
    """Export all clients as CSV."""
    import csv
    import io
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM clients WHERE is_deleted=0 ORDER BY full_name"
        ).fetchall()
        output = io.StringIO()
        if rows:
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            for r in rows:
                writer.writerow(dict(r))
        csv_data = output.getvalue()
        return Response(csv_data, mimetype='text/csv',
                        headers={'Content-Disposition': 'attachment; filename=clients_export.csv'})
    finally:
        conn.close()


@phase2.route('/clients/import-csv', methods=['POST'])
def clients_import_csv():
    """Import clients from CSV file."""
    import csv
    import io
    f = request.files.get('file')
    if not f:
        return jsonify({'error': 'No file provided'}), 400
    content = f.read().decode('utf-8-sig', errors='replace')
    reader = csv.DictReader(io.StringIO(content))
    conn = get_connection()
    try:
        added = skipped = 0
        year = datetime.now().year
        for row in reader:
            name = (row.get('full_name') or '').strip()
            if not name:
                continue
            exists = conn.execute(
                "SELECT id FROM clients WHERE full_name=? AND is_deleted=0", [name]
            ).fetchone()
            if exists:
                skipped += 1
                continue
            cid = row.get('customer_id') or generate_customer_id(name, year, conn)
            conn.execute("""
                INSERT INTO clients (full_name, last_name, customer_id, address, city_state_zip,
                                     phone1, phone2, email1, email2, notes, year_acquired)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, [name, extract_last_name(name), cid,
                  row.get('address',''), row.get('city_state_zip',''),
                  row.get('phone1',''), row.get('phone2',''),
                  row.get('email1',''), row.get('email2',''),
                  row.get('notes',''),
                  int(row.get('year_acquired', year) or year)])
            added += 1
        conn.commit()
        return jsonify({'success': True, 'added': added, 'skipped': skipped})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


@phase2.route('/contractors/<int:contractor_id>/patch', methods=['POST'])
def contractor_patch(contractor_id):
    """Patch a single field on a contractor (AJAX inline edit)."""
    data = request.get_json(silent=True) or {}
    ALLOWED = {'rank_preference', 'trade_type', 'notes', 'requires_1099', 'vendor_type', 'website'}
    conn = get_connection()
    try:
        for field, value in data.items():
            if field not in ALLOWED:
                continue
            conn.execute(f"UPDATE contractors SET {field}=? WHERE id=?", [value, contractor_id])
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    finally:
        conn.close()

@phase2.route('/contractors/new', methods=['GET', 'POST'])
def contractor_new():
    config = _get_config()
    badges = _get_badges()
    if request.method == 'POST':
        return _contractor_save(None)
    conn = get_connection()
    try:
        trades = conn.execute(
            "SELECT DISTINCT trade_type FROM contractors WHERE trade_type!='' AND is_deleted=0 ORDER BY trade_type"
        ).fetchall()
        return render_template('contractor_form.html',
            config=config, badges=badges,
            contractor={}, mode='new',
            trades=[t[0] for t in trades],
        )
    finally:
        conn.close()


@phase2.route('/contractors/<int:contractor_id>', methods=['GET'])
def contractor_detail(contractor_id):
    config = _get_config()
    badges = _get_badges()
    conn = get_connection()
    try:
        contractor = conn.execute(
            "SELECT * FROM contractors WHERE id=? AND is_deleted=0", [contractor_id]
        ).fetchone()
        if not contractor:
            flash('Contractor not found.', 'error')
            return redirect(url_for('phase2.contractors'))
        contractor = dict(contractor)

        # Certs
        certs = conn.execute("""
            SELECT * FROM certificates WHERE contractor_id=? AND is_deleted=0
            ORDER BY end_date DESC
        """, [contractor_id]).fetchall()
        certs_out = []
        for c in certs:
            d = dict(c)
            d['status_info'] = get_cert_status(d.get('end_date',''))
            certs_out.append(d)

        # Payments from ledger
        payments = conn.execute("""
            SELECT entry_date, job_code, description, amount, invoice_number
            FROM ledger
            WHERE vendor=? AND is_deleted=0
            ORDER BY entry_date DESC LIMIT 20
        """, [contractor['company_name']]).fetchall()

        ytd_pay = conn.execute("""
            SELECT COALESCE(SUM(amount),0) FROM ledger
            WHERE vendor=? AND is_deleted=0 AND entry_date >= date('now','start of year')
        """, [contractor['company_name']]).fetchone()[0]

        all_time_pay = conn.execute(
            "SELECT COALESCE(SUM(amount),0) FROM ledger WHERE vendor=? AND is_deleted=0",
            [contractor['company_name']]
        ).fetchone()[0]

        trades = conn.execute(
            "SELECT DISTINCT trade_type FROM contractors WHERE trade_type!='' AND is_deleted=0 ORDER BY trade_type"
        ).fetchall()

        return render_template('contractor_detail.html',
            config=config, badges=badges,
            contractor=contractor,
            certs=certs_out,
            payments=[dict(p) for p in payments],
            ytd_pay=ytd_pay,
            all_time_pay=all_time_pay,
            trades=[t[0] for t in trades],
        )
    finally:
        conn.close()


@phase2.route('/contractors/<int:contractor_id>/edit', methods=['GET', 'POST'])
def contractor_edit(contractor_id):
    config = _get_config()
    badges = _get_badges()
    if request.method == 'POST':
        return _contractor_save(contractor_id)
    conn = get_connection()
    try:
        contractor = conn.execute(
            "SELECT * FROM contractors WHERE id=? AND is_deleted=0", [contractor_id]
        ).fetchone()
        if not contractor:
            flash('Contractor not found.', 'error')
            return redirect(url_for('phase2.contractors'))
        trades = conn.execute(
            "SELECT DISTINCT trade_type FROM contractors WHERE trade_type!='' AND is_deleted=0 ORDER BY trade_type"
        ).fetchall()
        return render_template('contractor_form.html',
            config=config, badges=badges,
            contractor=dict(contractor), mode='edit',
            trades=[t[0] for t in trades],
        )
    finally:
        conn.close()


def _contractor_save(contractor_id):
    data = request.form.to_dict()
    company = data.get('company_name', '').strip()
    if not company:
        flash('Company name is required.', 'error')
        return redirect(request.referrer or url_for('phase2.contractors'))
    with db() as conn:
        if contractor_id:
            old = conn.execute("SELECT * FROM contractors WHERE id=?", [contractor_id]).fetchone()
            conn.execute("""
                UPDATE contractors SET
                    rank_preference=?, trade_type=?, company_name=?, contact_person=?,
                    phone=?, cell=?, email=?, website=?, address=?,
                    license_number=?, vendor_type=?, requires_1099=?, notes=?,
                    updated_at=datetime('now')
                WHERE id=?
            """, [
                int(data.get('rank_preference',0) or 0),
                data.get('trade_type',''), company, data.get('contact_person',''),
                data.get('phone',''), data.get('cell',''), data.get('email',''),
                data.get('website',''), data.get('address',''),
                data.get('license_number',''),
                data.get('vendor_type','Subcontractor'),
                1 if data.get('requires_1099') else 0,
                data.get('notes',''), contractor_id
            ])
            log_action(conn, 'contractors', contractor_id, 'UPDATE',
                       old_data=dict(old) if old else {})
            flash(f'"{company}" updated.', 'success')
            return redirect(url_for('phase2.contractor_detail', contractor_id=contractor_id))
        else:
            cur = conn.execute("""
                INSERT INTO contractors
                    (rank_preference, trade_type, company_name, contact_person,
                     phone, cell, email, website, address,
                     license_number, vendor_type, requires_1099, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, [
                int(data.get('rank_preference',0) or 0),
                data.get('trade_type',''), company, data.get('contact_person',''),
                data.get('phone',''), data.get('cell',''), data.get('email',''),
                data.get('website',''), data.get('address',''),
                data.get('license_number',''),
                data.get('vendor_type','Subcontractor'),
                1 if data.get('requires_1099') else 0,
                data.get('notes','')
            ])
            new_id = cur.lastrowid
            log_action(conn, 'contractors', new_id, 'INSERT',
                       new_data={'company_name': company})
            flash(f'Contractor "{company}" created.', 'success')
            return redirect(url_for('phase2.contractor_detail', contractor_id=new_id))


@phase2.route('/contractors/<int:contractor_id>/delete', methods=['POST'])
def contractor_delete(contractor_id):
    with db() as conn:
        c = conn.execute("SELECT company_name FROM contractors WHERE id=?", [contractor_id]).fetchone()
        soft_delete(conn, 'contractors', contractor_id)
        name = c['company_name'] if c else str(contractor_id)
    flash(f'Contractor "{name}" archived.', 'success')
    return redirect(url_for('phase2.contractors'))


# ── CERTIFICATES (under contractors) ─────────────────────────

@phase2.route('/contractors/<int:contractor_id>/certs/add', methods=['POST'])
def cert_add(contractor_id):
    data = request.form.to_dict()
    with db() as conn:
        conn.execute("""
            INSERT INTO certificates
                (contractor_id, company_name, cert_filename, cert_type,
                 start_date, end_date, notes)
            VALUES (?,?,?,?,?,?,?)
        """, [
            contractor_id, data.get('company_name',''),
            data.get('cert_filename',''), data.get('cert_type',''),
            data.get('start_date',''), data.get('end_date',''),
            data.get('notes','')
        ])
    flash('Certificate added.', 'success')
    return redirect(url_for('phase2.contractor_detail', contractor_id=contractor_id))


@phase2.route('/certs/<int:cert_id>/delete', methods=['POST'])
def cert_delete(cert_id):
    with db() as conn:
        cert = conn.execute("SELECT contractor_id FROM certificates WHERE id=?", [cert_id]).fetchone()
        contractor_id = cert['contractor_id'] if cert else None
        conn.execute(
            "UPDATE certificates SET is_deleted=1, updated_at=datetime('now') WHERE id=?", [cert_id]
        )
    flash('Certificate removed.', 'success')
    return redirect(url_for('phase2.contractor_detail', contractor_id=contractor_id) if contractor_id else url_for('phase2.contractors'))


# ════════════════════════════════════════════════════════════════
#  JOBS
# ════════════════════════════════════════════════════════════════

@phase2.route('/projects')
def projects_redirect():
    """Alias: /projects → /jobs (Projects page)."""
    from flask import redirect
    args = request.query_string.decode()
    return redirect(f'/jobs{"?" + args if args else ""}')


@phase2.route('/jobs')
def jobs():
    config = _get_config()
    badges = _get_badges()
    conn = get_connection()
    try:
        q = request.args.get('q', '').strip()
        status_filter = request.args.get('status', 'Active')
        page = int(request.args.get('page', 1))
        per_page = 999999 if _get_config().get('continuous_scroll') else 50

        where = ["j.is_deleted=0"]
        params = []
        if q:
            where.append("(j.job_code LIKE ? OR j.description LIKE ?)")
            params += [f"%{q}%", f"%{q}%"]
        if status_filter and status_filter != 'All':
            where.append("j.status=?")
            params.append(status_filter)
        where_sql = " AND ".join(where)

        total = conn.execute(f"SELECT COUNT(*) FROM jobs j WHERE {where_sql}", params).fetchone()[0]
        rows = conn.execute(f"""
            SELECT j.*,
                c.full_name AS client_name,
                c.customer_id,
                (SELECT COALESCE(SUM(ABS(l.amount)),0) FROM ledger l
                 WHERE l.job_code=j.job_code AND l.amount < 0 AND l.is_deleted=0) AS total_cost,
                (SELECT COALESCE(SUM(i.amount),0) FROM invoices i
                 WHERE i.job_code=j.job_code AND i.is_deleted=0) AS total_invoiced,
                (SELECT COALESCE(SUM(i.balance_due),0) FROM invoices i
                 WHERE i.job_code=j.job_code AND i.is_deleted=0
                   AND i.status NOT IN ('Paid','Void','Draft')) AS ar_outstanding,
                (SELECT COALESCE(SUM(t.hours),0) FROM timesheet t
                 WHERE t.job_code=j.job_code AND t.is_deleted=0) AS total_hours,
                (SELECT COUNT(*) FROM job_milestones m
                 WHERE m.job_id=j.id AND m.status='Pending') AS pending_milestones
            FROM jobs j
            LEFT JOIN clients c ON j.client_id=c.id
            WHERE {where_sql}
            ORDER BY j.status, j.start_date DESC
            LIMIT ? OFFSET ?
        """, params + [per_page, (page-1)*per_page]).fetchall()

        clients_list = conn.execute(
            "SELECT id, full_name, customer_id FROM clients WHERE is_deleted=0 ORDER BY last_name"
        ).fetchall()

        return render_template('jobs.html',
            config=config, badges=badges,
            jobs=[dict(r) for r in rows],
            clients=[dict(c) for c in clients_list],
            q=q, status_filter=status_filter,
            total=total, page=page, per_page=per_page,
            pages=(total + per_page - 1) // per_page,
        )
    finally:
        conn.close()


@phase2.route('/jobs/new', methods=['GET', 'POST'])
def job_new():
    config = _get_config()
    badges = _get_badges()
    if request.method == 'POST':
        return _job_save(None)
    conn = get_connection()
    try:
        clients_list = conn.execute(
            "SELECT id, full_name, customer_id FROM clients WHERE is_deleted=0 ORDER BY last_name"
        ).fetchall()
        # Suggest next job number
        row = conn.execute("SELECT MAX(job_number) FROM jobs").fetchone()
        next_num = (row[0] or 0) + 1
        return render_template('job_form.html',
            config=config, badges=badges,
            job={}, mode='new',
            clients=[dict(c) for c in clients_list],
            next_job_number=next_num,
            year=datetime.now().year,
        )
    finally:
        conn.close()


@phase2.route('/jobs/<int:job_id>/edit', methods=['GET', 'POST'])
def job_edit(job_id):
    config = _get_config()
    badges = _get_badges()
    if request.method == 'POST':
        return _job_save(job_id)
    conn = get_connection()
    try:
        job = conn.execute(
            "SELECT * FROM jobs WHERE id=? AND is_deleted=0", [job_id]
        ).fetchone()
        if not job:
            flash('Job not found.', 'error')
            return redirect(url_for('phase2.jobs'))
        clients_list = conn.execute(
            "SELECT id, full_name, customer_id FROM clients WHERE is_deleted=0 ORDER BY last_name"
        ).fetchall()
        return render_template('job_form.html',
            config=config, badges=badges,
            job=dict(job), mode='edit',
            clients=[dict(c) for c in clients_list],
            next_job_number=job['job_number'],
            year=datetime.now().year,
        )
    finally:
        conn.close()


def _job_save(job_id):
    data = request.form.to_dict()
    job_code = data.get('job_code', '').strip()
    if not job_code:
        flash('Job code is required.', 'error')
        return redirect(request.referrer or url_for('phase2.jobs'))
    with db() as conn:
        if job_id:
            old = conn.execute("SELECT * FROM jobs WHERE id=?", [job_id]).fetchone()
            conn.execute("""
                UPDATE jobs SET
                    job_code=?, job_number=?, client_id=?, description=?,
                    status=?, start_date=?, end_date=?, contract_amount=?,
                    budget_amount=?, notes=?, notes_internal=?,
                    updated_at=datetime('now')
                WHERE id=?
            """, [
                job_code, data.get('job_number') or None,
                data.get('client_id') or None, data.get('description',''),
                data.get('status','Active'),
                data.get('start_date',''), data.get('end_date',''),
                float(data.get('contract_amount',0) or 0),
                float(data.get('budget_amount',0) or 0),
                data.get('notes',''), data.get('notes_internal',''), job_id
            ])
            log_action(conn, 'jobs', job_id, 'UPDATE', old_data=dict(old) if old else {})
            flash(f'Job "{job_code}" updated.', 'success')
            return redirect(url_for('phase2.jobs'))
        else:
            # Check job_code uniqueness
            exists = conn.execute(
                "SELECT 1 FROM jobs WHERE job_code=? AND is_deleted=0", [job_code]
            ).fetchone()
            if exists:
                flash(f'Job code "{job_code}" already exists.', 'error')
                return redirect(request.referrer or url_for('phase2.jobs'))
            cur = conn.execute("""
                INSERT INTO jobs
                    (job_code, job_number, client_id, description,
                     status, start_date, end_date, contract_amount,
                     budget_amount, notes, notes_internal)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, [
                job_code, data.get('job_number') or None,
                data.get('client_id') or None, data.get('description',''),
                data.get('status','Active'),
                data.get('start_date',''), data.get('end_date',''),
                float(data.get('contract_amount',0) or 0),
                float(data.get('budget_amount',0) or 0),
                data.get('notes',''), data.get('notes_internal','')
            ])
            log_action(conn, 'jobs', cur.lastrowid, 'INSERT',
                       new_data={'job_code': job_code})
            flash(f'Job "{job_code}" created.', 'success')
            return redirect(url_for('phase2.jobs'))


@phase2.route('/jobs/<int:job_id>')
def job_detail(job_id):
    """
    Full job dashboard: P&L breakdown by category, invoices, timesheet hours,
    milestones, and profitability vs contract/budget.
    """
    config = _get_config()
    badges = _get_badges()
    conn = get_connection()
    try:
        job = conn.execute("""
            SELECT j.*, c.full_name AS client_name, c.customer_id, c.id AS client_id
            FROM jobs j
            LEFT JOIN clients c ON j.client_id = c.id
            WHERE j.id=? AND j.is_deleted=0
        """, [job_id]).fetchone()
        if not job:
            flash('Job not found.', 'error')
            return redirect(url_for('phase2.jobs'))
        job = dict(job)

        # Expense breakdown by category
        expenses = conn.execute("""
            SELECT category,
                   COUNT(*) AS entry_count,
                   COALESCE(SUM(ABS(amount)),0) AS total
            FROM ledger
            WHERE job_code=? AND amount<0 AND is_deleted=0
            GROUP BY category
            ORDER BY total DESC
        """, [job['job_code']]).fetchall()

        # Income entries
        income_rows = conn.execute("""
            SELECT SUM(amount) AS total, COUNT(*) AS count
            FROM ledger
            WHERE job_code=? AND amount>0 AND is_deleted=0
        """, [job['job_code']]).fetchone()

        # Recent ledger entries (last 50)
        ledger_entries = conn.execute("""
            SELECT id, entry_date, vendor, description, amount, category, receipt_verified
            FROM ledger
            WHERE job_code=? AND is_deleted=0
            ORDER BY entry_date DESC LIMIT 50
        """, [job['job_code']]).fetchall()

        # Invoices
        invoices = conn.execute("""
            SELECT id, invoice_number, invoice_date, due_date, amount,
                   amount_paid, balance_due, status
            FROM invoices
            WHERE job_code=? AND is_deleted=0
            ORDER BY invoice_date DESC
        """, [job['job_code']]).fetchall()

        # Timesheet summary
        timesheet = conn.execute("""
            SELECT COALESCE(t.person_label, e.first_name || ' ' || e.last_name, 'Unknown') AS full_name,
                   SUM(t.hours) AS hours,
                   SUM(COALESCE(t.cost_amount, t.bill_amount, 0)) AS cost
            FROM timesheet t
            LEFT JOIN employees e ON t.emp_id = e.id
            WHERE t.job_code=? AND t.is_deleted=0
            GROUP BY COALESCE(t.person_label, t.emp_id)
            ORDER BY hours DESC
        """, [job['job_code']]).fetchall()

        # Milestones
        milestones = conn.execute("""
            SELECT * FROM job_milestones
            WHERE job_id=?
            ORDER BY sort_order, due_date, id
        """, [job_id]).fetchall()

        # Compute P&L summary
        total_cost     = sum(float(e['total']) for e in expenses)
        total_income   = float(income_rows['total'] or 0)
        total_invoiced = sum(float(i['amount']) for i in invoices)
        ar_outstanding = sum(float(i['balance_due']) for i in invoices
                             if i['status'] not in ('Paid','Void','Draft'))
        total_hours    = sum(float(t['hours'] or 0) for t in timesheet)

        contract_amt   = float(job.get('contract_amount') or 0)
        budget_amt     = float(job.get('budget_amount') or 0)
        gross_profit   = contract_amt - total_cost if contract_amt else total_income - total_cost
        margin_pct     = (gross_profit / contract_amt * 100) if contract_amt > 0 else None
        budget_used_pct = (total_cost / budget_amt * 100) if budget_amt > 0 else None

        return render_template('job_detail.html',
            config=config, badges=badges,
            job=job,
            expenses=[dict(e) for e in expenses],
            ledger_entries=[dict(e) for e in ledger_entries],
            invoices=[dict(i) for i in invoices],
            timesheet=[dict(t) for t in timesheet],
            milestones=[dict(m) for m in milestones],
            total_cost=total_cost,
            total_income=total_income,
            total_invoiced=total_invoiced,
            ar_outstanding=ar_outstanding,
            total_hours=total_hours,
            gross_profit=gross_profit,
            margin_pct=margin_pct,
            budget_used_pct=budget_used_pct,
            contract_amt=contract_amt,
            budget_amt=budget_amt,
        )
    finally:
        conn.close()


@phase2.route('/jobs/<int:job_id>/patch', methods=['POST'])
def job_patch(job_id):
    """
    Quick-patch a single job field (status, budget_amount, notes_internal, etc.)
    Body: { "field": "status", "value": "Archived" }
    """
    data  = request.json or {}
    field = data.get('field','').strip()
    value = data.get('value')

    SAFE_FIELDS = {'status','budget_amount','notes_internal','notes',
                   'contract_amount','start_date','end_date','description'}
    if field not in SAFE_FIELDS:
        return jsonify({'error': f'Field {field!r} not patchable'}), 400

    VALID_STATUS = ('Active','Bidding','Archived','Cancelled')
    if field == 'status' and value not in VALID_STATUS:
        return jsonify({'error': f'Invalid status {value!r}'}), 400

    with db() as conn:
        old = conn.execute(f"SELECT {field} FROM jobs WHERE id=?", [job_id]).fetchone()
        if not old:
            return jsonify({'error': 'Job not found'}), 404
        conn.execute(
            f"UPDATE jobs SET {field}=?, updated_at=datetime('now') WHERE id=?",
            [value, job_id]
        )
    return jsonify({'success': True, 'field': field, 'value': value})


# ── Milestones API ──────────────────────────────────────────

@phase2.route('/api/jobs/<int:job_id>/milestones', methods=['GET'])
def api_job_milestones(job_id):
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM job_milestones WHERE job_id=? ORDER BY sort_order, due_date, id",
            [job_id]
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@phase2.route('/api/jobs/<int:job_id>/milestones', methods=['POST'])
def api_job_milestone_add(job_id):
    data   = request.json or {}
    title  = (data.get('title') or '').strip()
    if not title:
        return jsonify({'error': 'title required'}), 400
    with db() as conn:
        # Verify job exists
        if not conn.execute("SELECT id FROM jobs WHERE id=?", [job_id]).fetchone():
            return jsonify({'error': 'Job not found'}), 404
        cur = conn.execute(
            "INSERT INTO job_milestones (job_id, title, due_date, notes, sort_order) VALUES (?,?,?,?,?)",
            [job_id, title, data.get('due_date',''), data.get('notes',''), data.get('sort_order',0)]
        )
        row = conn.execute("SELECT * FROM job_milestones WHERE id=?", [cur.lastrowid]).fetchone()
    return jsonify({'success': True, 'milestone': dict(row)})


@phase2.route('/api/milestones/<int:milestone_id>', methods=['PATCH'])
def api_milestone_patch(milestone_id):
    data = request.json or {}
    SAFE = {'title','due_date','completed_date','status','notes','sort_order'}
    updates = {k: v for k, v in data.items() if k in SAFE}
    if not updates:
        return jsonify({'error': 'No valid fields'}), 400
    with db() as conn:
        for field, value in updates.items():
            conn.execute(
                f"UPDATE job_milestones SET {field}=? WHERE id=?",
                [value, milestone_id]
            )
    return jsonify({'success': True})


@phase2.route('/api/milestones/<int:milestone_id>', methods=['DELETE'])
def api_milestone_delete(milestone_id):
    with db() as conn:
        conn.execute("DELETE FROM job_milestones WHERE id=?", [milestone_id])
    return jsonify({'success': True})


@phase2.route('/jobs/<int:job_id>/delete', methods=['POST'])
def job_delete(job_id):
    with db() as conn:
        j = conn.execute("SELECT job_code FROM jobs WHERE id=?", [job_id]).fetchone()
        soft_delete(conn, 'jobs', job_id)
        code = j['job_code'] if j else str(job_id)
    flash(f'Job "{code}" archived.', 'success')
    return redirect(url_for('phase2.jobs'))


# API: suggest job code from client name + year
@phase2.route('/api/jobs/suggest-code')
def api_suggest_job_code():
    client_id = request.args.get('client_id')
    year = request.args.get('year', datetime.now().year)
    if not client_id:
        return jsonify({'code': ''})
    conn = get_connection()
    try:
        client = conn.execute(
            "SELECT last_name, full_name FROM clients WHERE id=? AND is_deleted=0", [client_id]
        ).fetchone()
        if not client:
            return jsonify({'code': ''})
        last = client['last_name'] or client['full_name'].split()[-1]
        base = f"{last}-{str(year)[-2:]}"
        # Check if exists and increment
        existing = conn.execute(
            "SELECT job_code FROM jobs WHERE job_code LIKE ? AND is_deleted=0",
            [f"{base}%"]
        ).fetchall()
        codes = {r['job_code'] for r in existing}
        if base not in codes:
            return jsonify({'code': base})
        i = 2
        while f"{base}-{i}" in codes:
            i += 1
        return jsonify({'code': f"{base}-{i}"})
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  INLINE EDIT API (for table cells)
# ════════════════════════════════════════════════════════════════

@phase2.route('/api/inline-edit', methods=['POST'])
def api_inline_edit():
    """Generic inline edit endpoint for simple field updates."""
    data = request.json
    table = data.get('table')
    record_id = data.get('id')
    field = data.get('field')
    value = data.get('value')

    ALLOWED = {
        'clients': ['status', 'notes', 'phone1', 'email1', 'address'],
        'employees': ['status', 'occupation', 'notes', 'phone'],
        'contractors': ['rank_preference', 'trade_type', 'notes', 'requires_1099'],
        'jobs': ['status', 'description', 'notes', 'contract_amount', 'end_date'],
    }
    if table not in ALLOWED or field not in ALLOWED.get(table, []):
        return jsonify({'error': 'Not allowed'}), 403

    with db() as conn:
        old = conn.execute(f"SELECT {field} FROM {table} WHERE id=?", [record_id]).fetchone()
        conn.execute(
            f"UPDATE {table} SET {field}=?, updated_at=datetime('now') WHERE id=?",
            [value, record_id]
        )
        log_action(conn, table, record_id, 'UPDATE',
                   old_data={field: dict(old)[field] if old else None},
                   new_data={field: value})
    return jsonify({'success': True, 'value': value})


# ════════════════════════════════════════════════════════════════
#  JOB EXPORT  (CSV + standalone HTML summary)
# ════════════════════════════════════════════════════════════════

@phase2.route('/jobs/<int:job_id>/export-csv')
def job_export_csv(job_id):
    """Export all ledger entries for a job as CSV."""
    conn = get_connection()
    try:
        job = conn.execute("SELECT * FROM jobs WHERE id=?", [job_id]).fetchone()
        if not job:
            flash("Job not found", "error")
            return redirect(url_for("phase2.jobs"))
        job = dict(job)

        rows = conn.execute("""
            SELECT entry_date, vendor, category, description, amount,
                   invoice_number, status, job_code, notes, receipt_filename
            FROM ledger
            WHERE job_code=? AND is_deleted=0
            ORDER BY entry_date
        """, [job['job_code']]).fetchall()

        import csv, io
        buf = io.StringIO()
        w   = csv.writer(buf)
        w.writerow(['Date','Vendor','Category','Description','Amount',
                    'Invoice','Status','Job','Notes','Receipt'])
        for r in rows:
            w.writerow([r['entry_date'], r['vendor'], r['category'],
                        r['description'], r['amount'], r['invoice_number'],
                        r['status'], r['job_code'], r['notes'], r['receipt_filename']])

        fname = f"job_{job['job_code']}_ledger.csv"
        return Response(
            buf.getvalue(), mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={fname}'}
        )
    finally:
        conn.close()


@phase2.route('/jobs/<int:job_id>/export-pdf')
def job_export_pdf(job_id):
    """Export a standalone HTML job summary (print-to-PDF friendly)."""
    conn = get_connection()
    try:
        job = conn.execute("""
            SELECT j.*, c.full_name AS client_name, c.email AS client_email,
                   c.phone AS client_phone, c.address AS client_address
            FROM jobs j
            LEFT JOIN clients c ON j.client_id=c.id
            WHERE j.id=?
        """, [job_id]).fetchone()
        if not job:
            flash("Job not found", "error")
            return redirect(url_for("phase2.jobs"))
        job = dict(job)

        ledger = conn.execute("""
            SELECT entry_date, vendor, category, description, amount, invoice_number
            FROM ledger WHERE job_code=? AND is_deleted=0 ORDER BY entry_date
        """, [job['job_code']]).fetchall()

        invoices = conn.execute("""
            SELECT invoice_number, invoice_date, amount, amount_paid, status, description
            FROM invoices WHERE job_code=? AND is_deleted=0 ORDER BY invoice_date
        """, [job['job_code']]).fetchall()

        timesheet = conn.execute("""
            SELECT entry_date, person_label, work_type, billable, hours,
                   bill_rate, bill_amount, description
            FROM timesheet WHERE job_code=? AND is_deleted=0 ORDER BY entry_date
        """, [job['job_code']]).fetchall()

        estimates = conn.execute("""
            SELECT estimate_number, estimate_date, status, total_estimate
            FROM job_estimates WHERE job_id=? AND is_deleted=0 ORDER BY estimate_date
        """, [job_id]).fetchall()

        # Totals
        total_cost    = sum(float(r['amount']) for r in ledger if float(r['amount'] or 0) > 0)
        total_invoiced = sum(float(r['amount']) for r in invoices)
        total_paid    = sum(float(r['amount_paid'] or 0) for r in invoices)
        total_hours   = sum(float(r['hours'] or 0) for r in timesheet)

        config = _get_config()
        from flask import render_template_string
        now_str = datetime.now().strftime('%m/%d/%Y')

        return render_template('job_export_pdf.html',
            job=job, config=config,
            ledger=[dict(r) for r in ledger],
            invoices=[dict(r) for r in invoices],
            timesheet=[dict(r) for r in timesheet],
            estimates=[dict(r) for r in estimates],
            total_cost=total_cost,
            total_invoiced=total_invoiced,
            total_paid=total_paid,
            total_hours=total_hours,
            now_str=now_str,
        )
    finally:
        conn.close()

