"""
Phase 6 Routes — Payroll
Full payroll processing:
  - List view: all runs with filters by year/employee/week, YTD summary
  - New Run form: pick pay period (auto-populates from timesheet), enter hours
  - OT automation: 40h/week threshold → standard vs overtime hours split
  - Tax automation: SS (with wage cap), Medicare, FUTA, SUTA — all from config
  - Federal/state withholding entered manually per run (W-4 based)
  - Computed fields: gross_pay, total_withheld, net_pay, employer tax totals
  - Check printing view: formatted check stub ready to print
  - YTD per-employee wage and tax summary
  - Employer tax liability report: SS, Medicare, FUTA, SUTA totals
  - Soft-delete with undo
  - Export to CSV
"""
import json
from datetime import datetime, date, timedelta
from flask import (Blueprint, render_template, request, jsonify,
                   redirect, url_for, flash, Response, stream_with_context)

from database import db, get_connection
from automations import (
    calculate_payroll_taxes, get_ytd_wages, get_ss_wage_cap,
    get_rate_for_date, get_person_label,
    log_action, soft_delete,
)

phase6 = Blueprint('phase6', __name__)

OT_THRESHOLD = 40.0   # hours/week before overtime kicks in


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

def _week_of(d: date) -> int:
    return d.isocalendar()[1]

def _week_bounds(d: date):
    mon = d - timedelta(days=d.weekday())
    sun = mon + timedelta(days=6)
    return mon, sun

def _split_ot(total_hours: float, ot_threshold: float = OT_THRESHOLD):
    """Return (standard_hours, overtime_hours)."""
    standard = min(total_hours, ot_threshold)
    overtime = max(0.0, total_hours - ot_threshold)
    return round(standard, 2), round(overtime, 2)

def _compute_run(hours_input: float, std_rate: float, ot_rate: float,
                  holiday_hours: float, holiday_rate: float) -> dict:
    """Compute standard/OT split and gross pay from raw hours."""
    standard_h, overtime_h = _split_ot(hours_input)
    standard_pay  = round(standard_h * std_rate, 2)
    overtime_pay  = round(overtime_h * ot_rate,  2)
    holiday_pay   = round(holiday_hours * holiday_rate, 2)
    gross_pay     = round(standard_pay + overtime_pay + holiday_pay, 2)
    return {
        'standard_hours': standard_h,
        'overtime_hours': overtime_h,
        'standard_pay':   standard_pay,
        'overtime_pay':   overtime_pay,
        'holiday_hours':  holiday_hours,
        'holiday_pay':    holiday_pay,
        'gross_pay':      gross_pay,
    }


# ════════════════════════════════════════════════════════════════
#  PAYROLL LIST
# ════════════════════════════════════════════════════════════════

@phase6.route('/payroll')
def payroll():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        today = date.today()
        cur_year = today.year

        # ── Filters ───────────────────────────────────────────
        year_filter = int(request.args.get('year', cur_year))
        emp_filter  = request.args.get('emp', '').strip()
        week_filter = request.args.get('week', '').strip()
        page        = max(1, int(request.args.get('page', 1)))
        per_page    = int(request.args.get('per_page', 50))

        where  = ["pr.is_deleted=0"]
        params = []
        where.append("pr.year=?"); params.append(year_filter)
        if emp_filter:
            where.append("pr.emp_id=?"); params.append(emp_filter)
        if week_filter:
            where.append("pr.week_number=?"); params.append(week_filter)

        where_sql = " AND ".join(where)

        total_count = conn.execute(
            f"SELECT COUNT(*) FROM payroll_runs pr WHERE {where_sql}", params
        ).fetchone()[0]

        runs = conn.execute(f"""
            SELECT pr.*,
                   e.first_name || ' ' || e.last_name AS emp_name,
                   e.occupation
            FROM payroll_runs pr
            LEFT JOIN employees e ON pr.emp_id = e.emp_id
            WHERE {where_sql}
            ORDER BY pr.pay_period_start DESC, pr.emp_id
            LIMIT ? OFFSET ?
        """, params + [per_page, (page-1)*per_page]).fetchall()

        # ── YTD summary tiles ─────────────────────────────────
        ytd = conn.execute("""
            SELECT
                COALESCE(SUM(gross_pay),0)        AS total_gross,
                COALESCE(SUM(net_pay),0)           AS total_net,
                COALESCE(SUM(ss_withheld),0)       AS total_ss,
                COALESCE(SUM(medicare_withheld),0) AS total_medicare,
                COALESCE(SUM(fed_withholding),0)   AS total_fed,
                COALESCE(SUM(state_withholding),0) AS total_state,
                COALESCE(SUM(employer_ss),0)       AS total_er_ss,
                COALESCE(SUM(employer_medicare),0) AS total_er_med,
                COALESCE(SUM(futa_amount),0)       AS total_futa,
                COALESCE(SUM(suta_amount),0)       AS total_suta,
                COALESCE(SUM(standard_hours),0)    AS total_std_hrs,
                COALESCE(SUM(overtime_hours),0)    AS total_ot_hrs,
                COUNT(DISTINCT emp_id)             AS employee_count,
                COUNT(*)                           AS run_count
            FROM payroll_runs
            WHERE is_deleted=0 AND year=?
        """, [year_filter]).fetchone()

        # ── Dropdowns ─────────────────────────────────────────
        employees = conn.execute("""
            SELECT emp_id, first_name || ' ' || last_name AS name
            FROM employees WHERE is_deleted=0 ORDER BY last_name, first_name
        """).fetchall()
        years = conn.execute("""
            SELECT DISTINCT year FROM payroll_runs WHERE is_deleted=0 ORDER BY year DESC
        """).fetchall()
        if not any(r['year'] == cur_year for r in years):
            years = [{'year': cur_year}] + [dict(r) for r in years]

        weeks = conn.execute("""
            SELECT DISTINCT week_number, pay_period_start, pay_period_end
            FROM payroll_runs WHERE is_deleted=0 AND year=?
            ORDER BY week_number DESC LIMIT 52
        """, [year_filter]).fetchall()

        return render_template('payroll.html',
            config=config, badges=badges,
            runs=[dict(r) for r in runs],
            ytd=dict(ytd),
            total_count=total_count,
            page=page, per_page=per_page,
            pages=(total_count + per_page - 1) // per_page if per_page else 1,
            year_filter=year_filter, emp_filter=emp_filter, week_filter=week_filter,
            employees=[dict(e) for e in employees],
            all_employees=[dict(e) for e in employees],
            years=[r['year'] if isinstance(r, dict) else r['year'] for r in years],
            weeks=[dict(w) for w in weeks],
            today=today.strftime('%Y-%m-%d'),
            cur_year=cur_year,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  NEW / EDIT RUN
# ════════════════════════════════════════════════════════════════

@phase6.route('/payroll/new', methods=['GET', 'POST'])
def payroll_new():
    if request.method == 'GET':
        return _payroll_form(None)
    return _payroll_save(None, request.form.to_dict())


@phase6.route('/payroll/<int:run_id>/edit', methods=['GET', 'POST'])
def payroll_edit(run_id):
    if request.method == 'GET':
        return _payroll_form(run_id)
    return _payroll_save(run_id, request.form.to_dict())


def _payroll_form(run_id):
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        run = None
        if run_id:
            run = conn.execute(
                "SELECT * FROM payroll_runs WHERE id=? AND is_deleted=0", [run_id]
            ).fetchone()
            if not run:
                flash('Payroll run not found.', 'error')
                return redirect(url_for('phase6.payroll'))
            run = dict(run)

        employees = conn.execute("""
            SELECT emp_id, first_name || ' ' || last_name AS name, occupation
            FROM employees WHERE is_deleted=0 AND status='Active' ORDER BY last_name, first_name
        """).fetchall()

        # Auto-suggest pay period: current week Mon-Sun
        today = date.today()
        mon, sun = _week_bounds(today)
        default_start = mon.strftime('%Y-%m-%d')
        default_end   = sun.strftime('%Y-%m-%d')
        default_week  = _week_of(today)
        default_year  = today.year

        # Prefill from query params (coming from timesheet)
        prefill_emp  = request.args.get('emp_id', '')
        prefill_start= request.args.get('start', default_start)
        prefill_end  = request.args.get('end',   default_end)

        # Pull timesheet hours for employee in pay period
        ts_hours = {}
        if prefill_emp and prefill_start and prefill_end:
            rows = conn.execute("""
                SELECT COALESCE(SUM(hours),0) AS hrs
                FROM timesheet
                WHERE emp_id=? AND entry_date >= ? AND entry_date <= ? AND is_deleted=0
            """, [prefill_emp, prefill_start, prefill_end]).fetchone()
            ts_hours = {'hrs': float(rows['hrs'])} if rows else {}

        return render_template('payroll_form.html',
            config=config, badges=badges, run=run,
            employees=[dict(e) for e in employees],
            default_start=prefill_start, default_end=prefill_end,
            default_week=default_week, default_year=default_year,
            prefill_emp=prefill_emp, ts_hours=ts_hours,
        )
    finally:
        conn.close()


def _payroll_save(run_id, data):
    # ── Parse inputs ──────────────────────────────────────────
    emp_id_raw      = (data.get('emp_id') or '').strip()
    start           = (data.get('pay_period_start') or '').strip()
    end             = (data.get('pay_period_end') or '').strip()
    total_hours_raw = (data.get('total_hours') or '0').strip()
    holiday_h_raw   = (data.get('holiday_hours') or '0').strip()
    fed_wh_raw      = (data.get('fed_withholding') or '0').strip()
    state_wh_raw    = (data.get('state_withholding') or '0').strip()
    std_rate_raw    = (data.get('standard_pay_rate') or '').strip()
    ot_rate_raw     = (data.get('overtime_pay_rate') or '').strip()
    check_number    = (data.get('check_number') or '').strip()
    notes           = (data.get('notes') or '').strip()

    # Validate
    errors = []
    if not emp_id_raw: errors.append('Employee required')
    if not start:      errors.append('Pay period start required')
    if not end:        errors.append('Pay period end required')
    if errors:
        for e in errors: flash(e, 'error')
        return redirect(request.referrer or url_for('phase6.payroll_new'))

    try:
        emp_id      = int(emp_id_raw)
        total_hours = float(total_hours_raw)
        holiday_h   = float(holiday_h_raw)
        fed_wh      = float(fed_wh_raw)
        state_wh    = float(state_wh_raw)
    except (ValueError, TypeError):
        flash('Invalid numeric value.', 'error')
        return redirect(request.referrer or url_for('phase6.payroll_new'))

    try:
        period_start_d = datetime.strptime(start, '%Y-%m-%d').date()
    except ValueError:
        flash('Invalid date format.', 'error')
        return redirect(request.referrer or url_for('phase6.payroll_new'))

    year    = period_start_d.year
    week_num= _week_of(period_start_d)
    run_date= date.today().strftime('%Y-%m-%d')

    with db() as conn:
        # ── Look up pay rate ──────────────────────────────────
        rates = get_rate_for_date(emp_id, start, conn)
        std_rate = float(std_rate_raw) if std_rate_raw else float(rates['cost_rate'] or 0)
        ot_mult  = 1.5
        ot_rate  = float(ot_rate_raw) if ot_rate_raw else round(std_rate * ot_mult, 4)

        # ── OT split & gross pay ──────────────────────────────
        comp = _compute_run(total_hours, std_rate, ot_rate, holiday_h, std_rate)
        gross_pay = comp['gross_pay']

        # ── Payroll taxes ─────────────────────────────────────
        cfg  = dict(conn.execute("SELECT * FROM company_config WHERE id=1").fetchone() or {})
        # Exclude current run from YTD if editing
        exclude_id = run_id if run_id else None
        taxes = calculate_payroll_taxes(emp_id, year, gross_pay, config=cfg, conn=conn)

        total_withheld = round(taxes['ss_withheld'] + taxes['medicare_withheld'] +
                                fed_wh + state_wh, 2)
        net_pay = round(gross_pay - total_withheld, 2)

        person_label = get_person_label(emp_id, conn)

        row_data = [
            run_date, start, end, week_num, year, emp_id,
            comp['standard_hours'], std_rate, comp['standard_pay'],
            comp['overtime_hours'], comp['overtime_pay'],
            comp['holiday_hours'],  comp['holiday_pay'],
            gross_pay,
            taxes['ss_withheld'], taxes['medicare_withheld'],
            fed_wh, state_wh, total_withheld, net_pay,
            taxes['employer_ss'], taxes['employer_medicare'],
            taxes['futa_amount'], taxes['suta_amount'],
            check_number, notes,
        ]

        if run_id:
            old = conn.execute("SELECT * FROM payroll_runs WHERE id=?", [run_id]).fetchone()
            conn.execute("""
                UPDATE payroll_runs SET
                    run_date=?, pay_period_start=?, pay_period_end=?,
                    week_number=?, year=?, emp_id=?,
                    standard_hours=?, standard_pay_rate=?, standard_pay=?,
                    overtime_hours=?, overtime_pay=?,
                    holiday_hours=?,  holiday_pay=?,
                    gross_pay=?,
                    ss_withheld=?, medicare_withheld=?,
                    fed_withholding=?, state_withholding=?, total_withheld=?, net_pay=?,
                    employer_ss=?, employer_medicare=?,
                    futa_amount=?, suta_amount=?,
                    check_number=?, notes=?,
                    updated_at=datetime('now')
                WHERE id=?
            """, row_data + [run_id])
            log_action(conn, 'payroll_runs', run_id, 'UPDATE',
                       old_data=dict(old) if old else {})
            flash(f'Payroll run updated — {person_label}.', 'success')
            return redirect(url_for('phase6.payroll_detail', run_id=run_id))
        else:
            cur = conn.execute("""
                INSERT INTO payroll_runs (
                    run_date, pay_period_start, pay_period_end,
                    week_number, year, emp_id,
                    standard_hours, standard_pay_rate, standard_pay,
                    overtime_hours, overtime_pay,
                    holiday_hours,  holiday_pay,
                    gross_pay,
                    ss_withheld, medicare_withheld,
                    fed_withholding, state_withholding, total_withheld, net_pay,
                    employer_ss, employer_medicare,
                    futa_amount, suta_amount,
                    check_number, check_printed, notes
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)
            """, row_data)
            new_id = cur.lastrowid
            log_action(conn, 'payroll_runs', new_id, 'INSERT',
                       new_data={'emp_id': emp_id, 'gross_pay': gross_pay, 'week': week_num})
            flash(f'Payroll run created — {person_label}, Week {week_num}, ${gross_pay:,.2f} gross.', 'success')
            return redirect(url_for('phase6.payroll_detail', run_id=new_id))


# ════════════════════════════════════════════════════════════════
#  DETAIL / CHECK STUB
# ════════════════════════════════════════════════════════════════

@phase6.route('/payroll/<int:run_id>')
def payroll_detail(run_id):
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        run = conn.execute("""
            SELECT pr.*,
                   e.first_name || ' ' || e.last_name AS emp_name,
                   e.first_name, e.last_name,
                   e.address AS emp_address, e.city_state_zip AS emp_csz,
                   e.ssn_encrypted, e.occupation
            FROM payroll_runs pr
            LEFT JOIN employees e ON pr.emp_id = e.emp_id
            WHERE pr.id=? AND pr.is_deleted=0
        """, [run_id]).fetchone()
        if not run:
            flash('Payroll run not found.', 'error')
            return redirect(url_for('phase6.payroll'))

        run = dict(run)
        year = run['year']
        emp_id = run['emp_id']

        # YTD totals for this employee (including this run)
        ytd = conn.execute("""
            SELECT
                COALESCE(SUM(gross_pay),0)        AS ytd_gross,
                COALESCE(SUM(net_pay),0)           AS ytd_net,
                COALESCE(SUM(ss_withheld),0)       AS ytd_ss,
                COALESCE(SUM(medicare_withheld),0) AS ytd_medicare,
                COALESCE(SUM(fed_withholding),0)   AS ytd_fed,
                COALESCE(SUM(state_withholding),0) AS ytd_state,
                COALESCE(SUM(standard_hours),0)    AS ytd_std_hrs,
                COALESCE(SUM(overtime_hours),0)    AS ytd_ot_hrs
            FROM payroll_runs
            WHERE emp_id=? AND year=? AND is_deleted=0
        """, [emp_id, year]).fetchone()

        # SS wage cap info
        ss_cap = get_ss_wage_cap(year, conn)
        ytd_before_this = get_ytd_wages(emp_id, year, exclude_run_id=run_id, conn=conn)

        return render_template('payroll_detail.html',
            config=config, badges=badges, run=run,
            ytd=dict(ytd), ss_cap=ss_cap,
            ytd_before_this=ytd_before_this,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  MARK CHECK PRINTED
# ════════════════════════════════════════════════════════════════

@phase6.route('/payroll/<int:run_id>/mark-printed', methods=['POST'])
def payroll_mark_printed(run_id):
    data         = request.json or {}
    check_number = (data.get('check_number') or '').strip()
    with db() as conn:
        conn.execute("""
            UPDATE payroll_runs
            SET check_printed=1, check_number=?, updated_at=datetime('now')
            WHERE id=?
        """, [check_number, run_id])
        log_action(conn, 'payroll_runs', run_id, 'UPDATE',
                   new_data={'check_printed': 1, 'check_number': check_number})
    return jsonify({'success': True})


# ════════════════════════════════════════════════════════════════
#  DELETE
# ════════════════════════════════════════════════════════════════

@phase6.route('/payroll/<int:run_id>/delete', methods=['POST'])
def payroll_delete(run_id):
    with db() as conn:
        soft_delete(conn, 'payroll_runs', run_id)
    if request.is_json:
        return jsonify({'success': True})
    flash('Payroll run deleted (Undo to restore).', 'success')
    return redirect(url_for('phase6.payroll'))


# ════════════════════════════════════════════════════════════════
#  RATE LOOKUP API  (for form JS)
# ════════════════════════════════════════════════════════════════

@phase6.route('/api/payroll/rates')
def api_payroll_rates():
    emp_id     = request.args.get('emp_id', '')
    start_date = request.args.get('start', date.today().strftime('%Y-%m-%d'))
    if not emp_id:
        return jsonify({'std_rate': 0, 'ot_rate': 0, 'person_label': ''})
    conn = get_connection()
    try:
        rates = get_rate_for_date(int(emp_id), start_date, conn)
        label = get_person_label(int(emp_id), conn)
        std   = float(rates['cost_rate'] or 0)
        ot    = round(std * 1.5, 4)
        return jsonify({'std_rate': std, 'ot_rate': ot, 'person_label': label})
    except (ValueError, TypeError):
        return jsonify({'std_rate': 0, 'ot_rate': 0, 'person_label': ''})
    finally:
        conn.close()


@phase6.route('/api/payroll/timesheet-hours')
def api_payroll_ts_hours():
    """Return total hours for an employee in a date range (from timesheet)."""
    emp_id = request.args.get('emp_id', '')
    start  = request.args.get('start', '')
    end    = request.args.get('end', '')
    if not (emp_id and start and end):
        return jsonify({'hours': 0})
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT COALESCE(SUM(hours),0) AS hrs
            FROM timesheet
            WHERE emp_id=? AND entry_date >= ? AND entry_date <= ? AND is_deleted=0
        """, [emp_id, start, end]).fetchone()
        return jsonify({'hours': float(row['hrs'])})
    finally:
        conn.close()


@phase6.route('/api/payroll/preview')
def api_payroll_preview():
    """Live tax + gross preview for the form."""
    try:
        emp_id      = int(request.args.get('emp_id', 0))
        total_hours = float(request.args.get('hours', 0))
        holiday_h   = float(request.args.get('holiday_hours', 0))
        std_rate    = float(request.args.get('std_rate', 0))
        ot_rate     = float(request.args.get('ot_rate', 0))
        fed_wh      = float(request.args.get('fed_wh', 0))
        state_wh    = float(request.args.get('state_wh', 0))
        year        = int(request.args.get('year', date.today().year))
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid params'}), 400

    comp  = _compute_run(total_hours, std_rate, ot_rate, holiday_h, std_rate)
    conn  = get_connection()
    try:
        cfg   = dict(conn.execute("SELECT * FROM company_config WHERE id=1").fetchone() or {})
        taxes = calculate_payroll_taxes(emp_id, year, comp['gross_pay'], config=cfg, conn=conn)
        total_withheld = round(taxes['ss_withheld'] + taxes['medicare_withheld'] + fed_wh + state_wh, 2)
        net_pay        = round(comp['gross_pay'] - total_withheld, 2)
        return jsonify({
            **comp,
            **taxes,
            'fed_withholding':    fed_wh,
            'state_withholding':  state_wh,
            'total_withheld':     total_withheld,
            'net_pay':            net_pay,
        })
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  YTD REPORT API
# ════════════════════════════════════════════════════════════════

@phase6.route('/api/payroll/ytd')
def api_payroll_ytd():
    """Per-employee YTD wage and tax summary for a year."""
    year = int(request.args.get('year', date.today().year))
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                pr.emp_id,
                e.first_name || ' ' || e.last_name AS emp_name,
                e.occupation,
                COALESCE(SUM(pr.gross_pay),0)        AS ytd_gross,
                COALESCE(SUM(pr.net_pay),0)           AS ytd_net,
                COALESCE(SUM(pr.standard_hours + pr.overtime_hours),0) AS ytd_hours,
                COALESCE(SUM(pr.overtime_hours),0)    AS ytd_ot_hours,
                COALESCE(SUM(pr.ss_withheld),0)       AS ytd_ss_withheld,
                COALESCE(SUM(pr.medicare_withheld),0) AS ytd_medicare_withheld,
                COALESCE(SUM(pr.fed_withholding),0)   AS ytd_fed_withheld,
                COALESCE(SUM(pr.state_withholding),0) AS ytd_state_withheld,
                COALESCE(SUM(pr.employer_ss),0)       AS ytd_er_ss,
                COALESCE(SUM(pr.employer_medicare),0) AS ytd_er_medicare,
                COALESCE(SUM(pr.futa_amount),0)       AS ytd_futa,
                COALESCE(SUM(pr.suta_amount),0)       AS ytd_suta,
                COUNT(*)                              AS run_count
            FROM payroll_runs pr
            LEFT JOIN employees e ON pr.emp_id = e.emp_id
            WHERE pr.is_deleted=0 AND pr.year=?
            GROUP BY pr.emp_id
            ORDER BY ytd_gross DESC
        """, [year]).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@phase6.route('/api/payroll/employer-liability')
def api_employer_liability():
    """Employer payroll tax liability report by quarter."""
    year = int(request.args.get('year', date.today().year))
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                CASE
                    WHEN CAST(substr(pay_period_start,6,2) AS INTEGER) BETWEEN 1 AND 3  THEN 'Q1'
                    WHEN CAST(substr(pay_period_start,6,2) AS INTEGER) BETWEEN 4 AND 6  THEN 'Q2'
                    WHEN CAST(substr(pay_period_start,6,2) AS INTEGER) BETWEEN 7 AND 9  THEN 'Q3'
                    ELSE 'Q4'
                END AS quarter,
                COALESCE(SUM(gross_pay),0)        AS gross,
                COALESCE(SUM(employer_ss),0)      AS er_ss,
                COALESCE(SUM(employer_medicare),0)AS er_medicare,
                COALESCE(SUM(ss_withheld),0)      AS ee_ss,
                COALESCE(SUM(medicare_withheld),0)AS ee_medicare,
                COALESCE(SUM(futa_amount),0)      AS futa,
                COALESCE(SUM(suta_amount),0)      AS suta,
                COUNT(DISTINCT emp_id)            AS employee_count
            FROM payroll_runs
            WHERE is_deleted=0 AND year=?
            GROUP BY quarter
            ORDER BY quarter
        """, [year]).fetchall()

        # Total row
        total = conn.execute("""
            SELECT
                COALESCE(SUM(gross_pay),0)        AS gross,
                COALESCE(SUM(employer_ss),0)      AS er_ss,
                COALESCE(SUM(employer_medicare),0)AS er_medicare,
                COALESCE(SUM(ss_withheld),0)      AS ee_ss,
                COALESCE(SUM(medicare_withheld),0)AS ee_medicare,
                COALESCE(SUM(futa_amount),0)      AS futa,
                COALESCE(SUM(suta_amount),0)      AS suta
            FROM payroll_runs WHERE is_deleted=0 AND year=?
        """, [year]).fetchone()

        return jsonify({
            'year': year,
            'by_quarter': [dict(r) for r in rows],
            'total': dict(total),
        })
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  EXPORT
# ════════════════════════════════════════════════════════════════

@phase6.route('/payroll/export')
def payroll_export():
    year_filter = int(request.args.get('year', date.today().year))
    emp_filter  = request.args.get('emp', '')
    conn = get_connection()
    try:
        where  = ["pr.is_deleted=0", "pr.year=?"]
        params = [year_filter]
        if emp_filter:
            where.append("pr.emp_id=?"); params.append(emp_filter)

        rows = conn.execute(f"""
            SELECT pr.*, e.first_name || ' ' || e.last_name AS emp_name
            FROM payroll_runs pr
            LEFT JOIN employees e ON pr.emp_id = e.emp_id
            WHERE {' AND '.join(where)}
            ORDER BY pr.pay_period_start DESC, pr.emp_id
        """, params).fetchall()

        COLS = ['emp_name','pay_period_start','pay_period_end','week_number','year',
                'standard_hours','overtime_hours','holiday_hours',
                'standard_pay','overtime_pay','holiday_pay','gross_pay',
                'ss_withheld','medicare_withheld','fed_withholding','state_withholding',
                'total_withheld','net_pay',
                'employer_ss','employer_medicare','futa_amount','suta_amount',
                'check_number','check_printed','run_date']

        def generate():
            yield ','.join(COLS) + '\n'
            for r in rows:
                d = dict(r)
                yield ','.join(
                    f'"{str(d.get(c,"")).replace(chr(34),chr(34)*2)}"' for c in COLS
                ) + '\n'

        fname = f"payroll_{year_filter}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        return Response(
            stream_with_context(generate()),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={fname}'}
        )
    finally:
        conn.close()
