"""
Phase 4 Routes — Timesheet
Full timesheet with:
  - List view: paginated, filtered by employee / job / date range / week
  - Weekly grid view: one row per employee, columns Mon–Sun
  - Add/Edit form with rate lock-in at save time (get_rate_for_date)
  - Bulk entry: enter multiple employees for same job/date in one form
  - Inline patch: single-cell saves without page reload
  - Summary tiles: hours, bill amount, cost amount, margin %
  - Per-employee weekly totals
  - Per-job hours/cost/bill rollup API
  - Export to CSV
  - Undo on every write
"""
import json
import csv
import io
from datetime import datetime, date, timedelta
from flask import (Blueprint, render_template, request, jsonify,
                   redirect, url_for, flash, Response, stream_with_context)

from database import db, get_connection
from automations import (get_rate_for_date, get_person_label,
                          log_action, soft_delete)

phase4 = Blueprint('phase4', __name__)


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

def _week_bounds(ref: date):
    """Return (monday, sunday) for the week containing ref."""
    mon = ref - timedelta(days=ref.weekday())
    sun = mon + timedelta(days=6)
    return mon, sun

def _compute_amounts(hours: float, bill_rate: float, cost_rate: float, expenses: float = 0.0):
    """Return bill_amount, cost_amount."""
    return round(hours * bill_rate, 2), round(hours * cost_rate + expenses, 2)


# ════════════════════════════════════════════════════════════════
#  TIMESHEET LIST
# ════════════════════════════════════════════════════════════════

@phase4.route('/timesheet')
def timesheet():
    config = _cfg()
    badges = _badges()
    conn = get_connection()
    try:
        # ── Params ────────────────────────────────────────────
        view      = request.args.get('view', 'list')   # list | week
        q         = request.args.get('q', '').strip()
        emp_filter= request.args.get('emp', '').strip()
        job_filter= request.args.get('job', '').strip()
        date_from = request.args.get('from', '').strip()
        date_to   = request.args.get('to', '').strip()
        week_str  = request.args.get('week', '')       # YYYY-MM-DD of week's Monday
        page      = max(1, int(request.args.get('page', 1)))
        per_page  = int(request.args.get('per_page', 50))

        # Week view defaults to current week
        if view == 'week':
            if week_str:
                try:
                    week_mon = datetime.strptime(week_str, '%Y-%m-%d').date()
                    week_mon = week_mon - timedelta(days=week_mon.weekday())
                except ValueError:
                    week_mon = date.today() - timedelta(days=date.today().weekday())
            else:
                week_mon = date.today() - timedelta(days=date.today().weekday())
            week_sun = week_mon + timedelta(days=6)
            prev_week = (week_mon - timedelta(days=7)).strftime('%Y-%m-%d')
            next_week = (week_mon + timedelta(days=7)).strftime('%Y-%m-%d')

        # ── Build WHERE ────────────────────────────────────────
        where = ["t.is_deleted=0"]
        params: list = []

        if view == 'week':
            where.append("t.entry_date >= ? AND t.entry_date <= ?")
            params += [week_mon.strftime('%Y-%m-%d'), week_sun.strftime('%Y-%m-%d')]
        else:
            if date_from:
                where.append("t.entry_date >= ?"); params.append(date_from)
            if date_to:
                where.append("t.entry_date <= ?"); params.append(date_to)

        if emp_filter:
            where.append("t.emp_id=?"); params.append(emp_filter)
        if job_filter:
            where.append("t.job_code=?"); params.append(job_filter)
        if q:
            where.append("(t.description LIKE ? OR t.notes LIKE ? OR t.person_label LIKE ? OR t.job_code LIKE ?)")
            params += [f"%{q}%"]*4

        where_sql = " AND ".join(where)

        # ── Summary totals ────────────────────────────────────
        totals = conn.execute(f"""
            SELECT
                COALESCE(SUM(t.hours),0)       AS total_hours,
                COALESCE(SUM(t.bill_amount),0)  AS total_bill,
                COALESCE(SUM(t.cost_amount),0)  AS total_cost,
                COALESCE(SUM(t.expenses),0)     AS total_expenses
            FROM timesheet t WHERE {where_sql}
        """, params).fetchone()

        total_count = conn.execute(
            f"SELECT COUNT(*) FROM timesheet t WHERE {where_sql}", params
        ).fetchone()[0]

        # ── Rows (list view) ──────────────────────────────────
        rows = []
        if view == 'list':
            rows = conn.execute(f"""
                SELECT t.*,
                       e.first_name || ' ' || e.last_name AS emp_name,
                       j.description AS job_desc
                FROM timesheet t
                LEFT JOIN employees e ON t.emp_id = e.emp_id
                LEFT JOIN jobs j ON t.job_code = j.job_code
                WHERE {where_sql}
                ORDER BY t.entry_date DESC, t.emp_id, t.id DESC
                LIMIT ? OFFSET ?
            """, params + [per_page, (page-1)*per_page]).fetchall()

        # ── Weekly grid data ──────────────────────────────────
        week_data = {}    # {emp_id: {day_str: [entries]}}
        week_employees = []
        if view == 'week':
            week_rows = conn.execute(f"""
                SELECT t.*,
                       e.first_name || ' ' || e.last_name AS emp_name
                FROM timesheet t
                LEFT JOIN employees e ON t.emp_id = e.emp_id
                WHERE {where_sql}
                ORDER BY t.emp_id, t.entry_date
            """, params).fetchall()

            seen_emps = {}
            for r in week_rows:
                eid = r['emp_id']
                if eid not in seen_emps:
                    seen_emps[eid] = r['emp_name'] or r['person_label'] or f"Emp {eid}"
                    week_data[eid] = {}
                day = r['entry_date']
                week_data[eid].setdefault(day, []).append(dict(r))

            week_employees = [{'emp_id': eid, 'emp_name': name}
                              for eid, name in seen_emps.items()]

        # ── Dropdown data ──────────────────────────────────────
        work_types = conn.execute(
            "SELECT category_name FROM work_categories WHERE is_deleted=0 ORDER BY is_cogs DESC, category_name"
        ).fetchall()

        employees = conn.execute("""
            SELECT emp_id, first_name || ' ' || last_name AS name
            FROM employees WHERE is_deleted=0 AND status='Active'
            ORDER BY last_name, first_name
        """).fetchall()
        all_employees = conn.execute("""
            SELECT emp_id, first_name || ' ' || last_name AS name
            FROM employees WHERE is_deleted=0 ORDER BY last_name, first_name
        """).fetchall()
        # Include jobs from jobs table PLUS any job codes that appear in ledger
        # (handles imported data where jobs weren't explicitly created)
        jobs = conn.execute("""
            SELECT job_code, description FROM (
                SELECT job_code, description FROM jobs WHERE is_deleted=0
                UNION
                SELECT DISTINCT job_code, job_code as description FROM ledger
                WHERE job_code != '' AND is_deleted=0
                  AND job_code NOT IN (SELECT job_code FROM jobs WHERE is_deleted=0)
                UNION
                SELECT DISTINCT job_code, job_code as description FROM timesheet
                WHERE job_code != '' AND is_deleted=0
                  AND job_code NOT IN (SELECT job_code FROM jobs WHERE is_deleted=0)
            ) ORDER BY job_code
        """).fetchall()
        all_jobs = jobs

        # ── Week days list ────────────────────────────────────
        week_days = []
        if view == 'week':
            for i in range(7):
                d = week_mon + timedelta(days=i)
                week_days.append({'date': d.strftime('%Y-%m-%d'),
                                  'label': d.strftime('%a') + ' ' + str(d.day)})

        today = date.today().strftime('%Y-%m-%d')

        return render_template('timesheet.html',
            config=config, badges=badges,
            view=view, today=today,
            rows=[dict(r) for r in rows],
            totals=dict(totals),
            total_count=total_count,
            page=page, per_page=per_page,
            pages=(total_count + per_page - 1) // per_page if per_page else 1,
            # filters
            q=q, emp_filter=emp_filter, job_filter=job_filter,
            date_from=date_from, date_to=date_to,
            # week
            week_days=week_days,
            week_data=week_data,
            week_employees=week_employees,
            week_str=week_str or (week_mon.strftime('%Y-%m-%d') if view=='week' else ''),
            prev_week=prev_week if view=='week' else '',
            next_week=next_week if view=='week' else '',
            week_mon=(week_mon.strftime('%b') + ' ' + str(week_mon.day)) if view=='week' else '',
            week_sun=(week_sun.strftime('%b') + ' ' + str(week_sun.day) + week_sun.strftime(', %Y')) if view=='week' else '',
            # dropdowns
            employees=[dict(e) for e in employees],
            all_employees=[dict(e) for e in all_employees],
            jobs=[dict(j) for j in jobs],
            all_jobs=[r['job_code'] for r in all_jobs],
            all_work_types=[r['category_name'] for r in work_types],
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CREATE / EDIT
# ════════════════════════════════════════════════════════════════

@phase4.route('/timesheet/new', methods=['POST'])
def timesheet_new():
    data = request.json if request.is_json else request.form.to_dict()
    return _ts_save(None, data)


@phase4.route('/timesheet/<int:row_id>/edit', methods=['POST'])
def timesheet_edit(row_id):
    data = request.json if request.is_json else request.form.to_dict()
    return _ts_save(row_id, data)


def _ts_save(row_id, data):
    entry_date = (data.get('entry_date') or '').strip()
    emp_id_raw = (data.get('emp_id') or '').strip()
    hours_raw  = (data.get('hours') or '').strip()

    errors = []
    if not entry_date:
        errors.append('entry_date required')
    if not emp_id_raw:
        errors.append('emp_id required')
    if hours_raw == '':
        errors.append('hours required')

    if errors:
        if request.is_json:
            return jsonify({'error': '; '.join(errors)}), 400
        flash('; '.join(errors), 'error')
        return redirect(url_for('phase4.timesheet'))

    try:
        emp_id = int(emp_id_raw)
        hours  = float(str(hours_raw).replace(',',''))
    except (ValueError, TypeError):
        if request.is_json:
            return jsonify({'error': 'Invalid emp_id or hours'}), 400
        flash('Invalid emp_id or hours', 'error')
        return redirect(url_for('phase4.timesheet'))

    job_code      = (data.get('job_code') or '').strip()
    invoice_number= (data.get('invoice_number') or '').strip()
    description   = (data.get('description') or '').strip()
    work_type     = (data.get('work_type') or '').strip()
    billable      = (data.get('billable') or 'Billable').strip()
    notes         = (data.get('notes') or '').strip()
    expenses      = float((data.get('expenses') or '0').replace(',',''))

    # ── Rate lock-in: always look up rate at entry_date ───────
    with db() as conn:
        rates = get_rate_for_date(emp_id, entry_date, conn)
        bill_rate = float(data.get('bill_rate') or rates['bill_rate'] or 0)
        cost_rate = float(data.get('cost_rate') or rates['cost_rate'] or 0)

        bill_amount, cost_amount = _compute_amounts(hours, bill_rate, cost_rate, expenses)
        person_label = get_person_label(emp_id, conn)

        if row_id:
            old = conn.execute("SELECT * FROM timesheet WHERE id=?", [row_id]).fetchone()
            old_dict = dict(old) if old else {}
            conn.execute("""
                UPDATE timesheet SET
                    entry_date=?, job_code=?, invoice_number=?, emp_id=?,
                    hours=?, bill_rate=?, cost_rate=?,
                    bill_amount=?, cost_amount=?, expenses=?,
                    description=?, work_type=?, billable=?, notes=?, person_label=?,
                    updated_at=datetime('now')
                WHERE id=?
            """, [entry_date, job_code, invoice_number, emp_id,
                  hours, bill_rate, cost_rate,
                  bill_amount, cost_amount, expenses,
                  description, work_type, billable, notes, person_label, row_id])
            new = conn.execute("SELECT * FROM timesheet WHERE id=?", [row_id]).fetchone()
            log_action(conn, 'timesheet', row_id, 'UPDATE', old_data=old_dict, new_data=dict(new))
            if request.is_json:
                return jsonify({'success': True, 'row': dict(new)})
            flash('Entry updated.', 'success')
        else:
            cur = conn.execute("""
                INSERT INTO timesheet
                    (entry_date, job_code, invoice_number, emp_id,
                     hours, bill_rate, cost_rate,
                     bill_amount, cost_amount, expenses,
                     description, work_type, billable, notes, person_label)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, [entry_date, job_code, invoice_number, emp_id,
                  hours, bill_rate, cost_rate,
                  bill_amount, cost_amount, expenses,
                  description, work_type, billable, notes, person_label])
            new_id = cur.lastrowid
            log_action(conn, 'timesheet', new_id, 'INSERT',
                       new_data={'emp_id': emp_id, 'hours': hours, 'entry_date': entry_date})
            if request.is_json:
                row = conn.execute("SELECT * FROM timesheet WHERE id=?", [new_id]).fetchone()
                return jsonify({'success': True, 'row': dict(row)})
            flash('Entry added.', 'success')

    return redirect(request.referrer or url_for('phase4.timesheet'))


# ════════════════════════════════════════════════════════════════
#  BULK ENTRY  — multiple employees, same date + job
# ════════════════════════════════════════════════════════════════

@phase4.route('/timesheet/bulk', methods=['POST'])
def timesheet_bulk():
    """
    Accepts a JSON payload:
    {
      "entry_date": "2025-03-15",
      "job_code": "TST-24",
      "invoice_number": "",
      "description": "Framing work",
      "entries": [
        {"emp_id": 1, "hours": 8, "expenses": 0},
        {"emp_id": 2, "hours": 6, "expenses": 25.00}
      ]
    }
    Returns list of created rows.
    """
    data = request.json or {}
    entry_date    = (data.get('entry_date') or '').strip()
    job_code      = (data.get('job_code') or '').strip()
    invoice_number= (data.get('invoice_number') or '').strip()
    description   = (data.get('description') or '').strip()
    notes         = (data.get('notes') or '').strip()
    entries       = data.get('entries', [])

    if not entry_date:
        return jsonify({'error': 'entry_date required'}), 400
    if not entries:
        return jsonify({'error': 'No entries provided'}), 400

    created = []
    errors  = []

    with db() as conn:
        for i, e in enumerate(entries):
            try:
                emp_id = int(e.get('emp_id') or 0)
                hours  = float(e.get('hours') or 0)
            except (ValueError, TypeError):
                errors.append(f"Entry {i}: invalid emp_id or hours")
                continue

            if not emp_id or hours <= 0:
                continue   # silently skip blank rows

            expenses = float(e.get('expenses') or 0)
            rates    = get_rate_for_date(emp_id, entry_date, conn)
            bill_rate= float(e.get('bill_rate') or rates['bill_rate'] or 0)
            cost_rate= float(e.get('cost_rate') or rates['cost_rate'] or 0)
            bill_amount, cost_amount = _compute_amounts(hours, bill_rate, cost_rate, expenses)
            person_label = get_person_label(emp_id, conn)

            cur = conn.execute("""
                INSERT INTO timesheet
                    (entry_date, job_code, invoice_number, emp_id,
                     hours, bill_rate, cost_rate,
                     bill_amount, cost_amount, expenses,
                     description, work_type, billable, notes, person_label)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, [entry_date, job_code, invoice_number, emp_id,
                  hours, bill_rate, cost_rate,
                  bill_amount, cost_amount, expenses,
                  description, work_type, billable, notes, person_label])
            new_id = cur.lastrowid
            log_action(conn, 'timesheet', new_id, 'INSERT',
                       new_data={'emp_id': emp_id, 'hours': hours, 'entry_date': entry_date,
                                 'job_code': job_code})
            row = conn.execute("SELECT * FROM timesheet WHERE id=?", [new_id]).fetchone()
            created.append(dict(row))

    result = {'success': True, 'created': len(created), 'rows': created}
    if errors:
        result['errors'] = errors
    return jsonify(result)


# ════════════════════════════════════════════════════════════════
#  INLINE PATCH
# ════════════════════════════════════════════════════════════════

TS_EDITABLE = {
    'entry_date', 'job_code', 'invoice_number', 'hours',
    'bill_rate', 'cost_rate', 'expenses', 'description', 'notes'
}

@phase4.route('/api/timesheet/<int:row_id>/patch', methods=['POST'])
def api_ts_patch(row_id):
    data  = request.json or {}
    field = data.get('field', '')
    value = data.get('value')

    if field not in TS_EDITABLE:
        return jsonify({'error': f'Field {field!r} not editable'}), 403

    with db() as conn:
        row = conn.execute(
            "SELECT * FROM timesheet WHERE id=? AND is_deleted=0", [row_id]
        ).fetchone()
        if not row:
            return jsonify({'error': 'Not found'}), 404

        old_val = dict(row).get(field)
        conn.execute(
            f"UPDATE timesheet SET {field}=?, updated_at=datetime('now') WHERE id=?",
            [value, row_id]
        )

        # Recompute bill_amount / cost_amount whenever rates or hours change
        r = dict(row)
        r[field] = value
        if field in ('hours', 'bill_rate', 'cost_rate', 'expenses', 'entry_date', 'emp_id'):
            # If entry_date changed, re-look up rates (unless manually overridden)
            if field == 'entry_date':
                auto = get_rate_for_date(r['emp_id'], value, conn)
                r['bill_rate'] = auto['bill_rate']
                r['cost_rate'] = auto['cost_rate']
                conn.execute(
                    "UPDATE timesheet SET bill_rate=?, cost_rate=? WHERE id=?",
                    [r['bill_rate'], r['cost_rate'], row_id]
                )
            hours      = float(r.get('hours') or 0)
            bill_rate  = float(r.get('bill_rate') or 0)
            cost_rate  = float(r.get('cost_rate') or 0)
            expenses   = float(r.get('expenses') or 0)
            ba, ca = _compute_amounts(hours, bill_rate, cost_rate, expenses)
            conn.execute(
                "UPDATE timesheet SET bill_amount=?, cost_amount=? WHERE id=?",
                [ba, ca, row_id]
            )

        log_action(conn, 'timesheet', row_id, 'UPDATE',
                   old_data={field: old_val}, new_data={field: value})

        updated = conn.execute("SELECT * FROM timesheet WHERE id=?", [row_id]).fetchone()
        return jsonify({'success': True, 'row': dict(updated)})


# ════════════════════════════════════════════════════════════════
#  DELETE
# ════════════════════════════════════════════════════════════════

@phase4.route('/timesheet/<int:row_id>/delete', methods=['POST'])
def timesheet_delete(row_id):
    with db() as conn:
        soft_delete(conn, 'timesheet', row_id)
    if request.is_json:
        return jsonify({'success': True})
    flash('Entry deleted (Undo to restore).', 'success')
    return redirect(request.referrer or url_for('phase4.timesheet'))


# ════════════════════════════════════════════════════════════════
#  RATE LOOKUP  (used by frontend when employee/date changes)
# ════════════════════════════════════════════════════════════════

@phase4.route('/api/timesheet/rate')
def api_ts_rate():
    """Return bill/cost rate for an employee on a given date."""
    emp_id = request.args.get('emp_id', '')
    entry_date = request.args.get('date', '')
    if not emp_id or not entry_date:
        return jsonify({'bill_rate': 0, 'cost_rate': 0})
    conn = get_connection()
    try:
        rates = get_rate_for_date(int(emp_id), entry_date, conn)
        label = get_person_label(int(emp_id), conn)
        return jsonify({'bill_rate': rates['bill_rate'],
                        'cost_rate': rates['cost_rate'],
                        'person_label': label})
    except (ValueError, TypeError):
        return jsonify({'bill_rate': 0, 'cost_rate': 0, 'person_label': ''})
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  SUMMARY APIs
# ════════════════════════════════════════════════════════════════

@phase4.route('/api/timesheet/job-summary')
def api_ts_job_summary():
    """Hours, bill, cost, margin per job for a date range."""
    date_from = request.args.get('from', '')
    date_to   = request.args.get('to', '')
    year      = request.args.get('year', str(date.today().year))

    conn = get_connection()
    try:
        if not date_from:
            date_from = f"{year}-01-01"
        if not date_to:
            date_to = f"{year}-12-31"

        rows = conn.execute("""
            SELECT
                t.job_code,
                j.description AS job_desc,
                j.contract_amount,
                COALESCE(SUM(t.hours),0)        AS total_hours,
                COALESCE(SUM(t.bill_amount),0)   AS total_bill,
                COALESCE(SUM(t.cost_amount),0)   AS total_cost,
                COALESCE(SUM(t.expenses),0)      AS total_expenses,
                COUNT(DISTINCT t.emp_id)         AS employee_count,
                COUNT(*)                         AS entry_count
            FROM timesheet t
            LEFT JOIN jobs j ON t.job_code = j.job_code
            WHERE t.is_deleted=0 AND t.job_code!=''
              AND t.entry_date >= ? AND t.entry_date <= ?
            GROUP BY t.job_code
            ORDER BY total_bill DESC
        """, [date_from, date_to]).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@phase4.route('/api/timesheet/employee-summary')
def api_ts_emp_summary():
    """Hours, bill, cost per employee for a date range."""
    date_from = request.args.get('from', '')
    date_to   = request.args.get('to', '')
    year      = request.args.get('year', str(date.today().year))

    conn = get_connection()
    try:
        if not date_from:
            date_from = f"{year}-01-01"
        if not date_to:
            date_to = f"{year}-12-31"

        rows = conn.execute("""
            SELECT
                t.emp_id,
                e.first_name || ' ' || e.last_name AS emp_name,
                e.occupation,
                COALESCE(SUM(t.hours),0)        AS total_hours,
                COALESCE(SUM(t.bill_amount),0)   AS total_bill,
                COALESCE(SUM(t.cost_amount),0)   AS total_cost,
                COUNT(DISTINCT t.job_code)       AS job_count,
                COUNT(*)                         AS entry_count
            FROM timesheet t
            LEFT JOIN employees e ON t.emp_id = e.emp_id
            WHERE t.is_deleted=0
              AND t.entry_date >= ? AND t.entry_date <= ?
            GROUP BY t.emp_id
            ORDER BY total_hours DESC
        """, [date_from, date_to]).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@phase4.route('/api/timesheet/weekly-totals')
def api_ts_weekly_totals():
    """Per-employee hours totals for a given week."""
    week_str = request.args.get('week', date.today().strftime('%Y-%m-%d'))
    try:
        week_mon = datetime.strptime(week_str, '%Y-%m-%d').date()
        week_mon = week_mon - timedelta(days=week_mon.weekday())
    except ValueError:
        week_mon = date.today() - timedelta(days=date.today().weekday())
    week_sun = week_mon + timedelta(days=6)

    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                t.emp_id,
                e.first_name || ' ' || e.last_name AS emp_name,
                t.entry_date,
                COALESCE(SUM(t.hours),0)       AS day_hours,
                COALESCE(SUM(t.bill_amount),0)  AS day_bill,
                COALESCE(SUM(t.cost_amount),0)  AS day_cost
            FROM timesheet t
            LEFT JOIN employees e ON t.emp_id = e.emp_id
            WHERE t.is_deleted=0
              AND t.entry_date >= ? AND t.entry_date <= ?
            GROUP BY t.emp_id, t.entry_date
            ORDER BY t.emp_id, t.entry_date
        """, [week_mon.strftime('%Y-%m-%d'), week_sun.strftime('%Y-%m-%d')]).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  EXPORT
# ════════════════════════════════════════════════════════════════

@phase4.route('/timesheet/export')
def timesheet_export():
    emp_filter = request.args.get('emp', '')
    job_filter = request.args.get('job', '')
    date_from  = request.args.get('from', '')
    date_to    = request.args.get('to', '')

    conn = get_connection()
    try:
        where = ["is_deleted=0"]
        params = []
        if emp_filter:
            where.append("emp_id=?"); params.append(emp_filter)
        if job_filter:
            where.append("job_code=?"); params.append(job_filter)
        if date_from:
            where.append("entry_date>=?"); params.append(date_from)
        if date_to:
            where.append("entry_date<=?"); params.append(date_to)

        rows = conn.execute(
            f"SELECT * FROM timesheet WHERE {' AND '.join(where)} ORDER BY entry_date DESC, emp_id",
            params
        ).fetchall()

        def generate():
            cols = ['id','entry_date','emp_id','person_label','job_code',
                    'hours','bill_rate','cost_rate','bill_amount','cost_amount',
                    'expenses','invoice_number','description','notes']
            yield ','.join(cols) + '\n'
            for r in rows:
                d = dict(r)
                yield ','.join(
                    f'"{str(d.get(c,"")).replace(chr(34),chr(34)*2)}"'
                    for c in cols
                ) + '\n'

        fname = f"timesheet_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        return Response(
            stream_with_context(generate()),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={fname}'}
        )
    finally:
        conn.close()
