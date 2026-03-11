# routes_phase7.py  v1.0.2 – 2026-03-11 – fixed year=all crash, dashboard KPI columns, chart yearly grouping
"""
Phase 7 Routes — Tax Reminders & Deadlines
Full tax deadline management:
  - List view: all reminders grouped by status with urgency badges
  - Calendar view: month grid with dot indicators per deadline
  - Create / Edit reminder (one-time or recurring)
  - Done: mark paid with payment date + amount
  - RemindLater: snooze by N days
  - Advance year: clone all recurring reminders for next year (+1yr)
  - Nav badge count driven from overdue Pending reminders
  - Export CSV of all deadlines
  - Quick-add standard IRS/IL deadlines for any year
  - API: upcoming reminders JSON (for dashboard widget)
"""
import json
from datetime import datetime, date, timedelta
from calendar import monthcalendar, month_name
from flask import (Blueprint, render_template, request, jsonify,
                   redirect, url_for, flash, Response, stream_with_context)

from database import db, get_connection
from automations import get_reminder_status, log_action, soft_delete

phase7 = Blueprint('phase7', __name__)

# ─────────────────────────────────────────────
# Standard tax deadlines template (used by advance-year)
# ─────────────────────────────────────────────
STANDARD_DEADLINES = [
    # (month, day, description, frequency, url)
    (1,  15, 'Q4 {prev_year} Estimated Tax Payment (Federal)',  'Quarterly',
     'https://www.irs.gov/payments'),
    (1,  15, 'IL Q4 {prev_year} Estimated Tax Payment',         'Quarterly',
     'https://mytax.illinois.gov'),
    (1,  31, 'Deposit FUTA tax (if >$500)',                     'Quarterly',
     'https://www.eftps.gov'),
    (1,  31, 'Form 941 - Q4 {prev_year}',                       'Quarterly',
     'https://www.irs.gov/forms-pubs/about-form-941'),
    (1,  31, 'W-2s to employees',                               'Yearly',
     'https://www.irs.gov/forms-pubs/about-form-w-2'),
    (1,  31, '1099-NEC to contractors',                         'Yearly',
     'https://www.irs.gov/forms-pubs/about-form-1099-nec'),
    (2,  28, 'W-2/1099 copies to SSA/IRS',                      'Yearly',
     'https://www.ssa.gov/employer'),
    (4,  15, 'Q1 {year} Estimated Tax Payment (Federal)',        'Quarterly',
     'https://www.irs.gov/payments'),
    (4,  15, 'IL Q1 {year} Estimated Tax Payment',              'Quarterly',
     'https://mytax.illinois.gov'),
    (4,  30, 'Form 941 - Q1 {year}',                            'Quarterly',
     'https://www.irs.gov/forms-pubs/about-form-941'),
    (6,  16, 'Q2 {year} Estimated Tax Payment (Federal)',        'Quarterly',
     'https://www.irs.gov/payments'),
    (6,  16, 'IL Q2 {year} Estimated Tax Payment',              'Quarterly',
     'https://mytax.illinois.gov'),
    (7,  31, 'Form 941 - Q2 {year}',                            'Quarterly',
     'https://www.irs.gov/forms-pubs/about-form-941'),
    (7,  31, 'Deposit FUTA if >$500 (mid-year)',                 'Quarterly',
     'https://www.eftps.gov'),
    (9,  15, 'Q3 {year} Estimated Tax Payment (Federal)',        'Quarterly',
     'https://www.irs.gov/payments'),
    (9,  15, 'IL Q3 {year} Estimated Tax Payment',              'Quarterly',
     'https://mytax.illinois.gov'),
    (10, 31, 'Form 941 - Q3 {year}',                            'Quarterly',
     'https://www.irs.gov/forms-pubs/about-form-941'),
    (10, 31, 'Deposit FUTA if >$500 (Q3)',                       'Quarterly',
     'https://www.eftps.gov'),
    (1,  31, 'Form 940 - Annual FUTA Return',                   'Yearly',
     'https://www.irs.gov/forms-pubs/about-form-940'),
]


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

def _days_until(due_str: str, today_str: str) -> int:
    try:
        due   = datetime.strptime(due_str, '%Y-%m-%d').date()
        today = datetime.strptime(today_str, '%Y-%m-%d').date()
        return (due - today).days
    except Exception:
        return 999

def _urgency(days: int, status: str) -> str:
    """Return CSS class name for urgency."""
    if status == 'Done':        return 'done'
    if status == 'RemindLater': return 'snoozed'
    if days < 0:                return 'overdue'
    if days <= 7:               return 'urgent'
    if days <= 30:              return 'soon'
    return 'upcoming'

def _urgency_label(days: int, status: str) -> str:
    if status == 'Done':        return 'Done'
    if status == 'RemindLater': return 'Snoozed'
    if days < 0:                return f'{abs(days)}d overdue'
    if days == 0:               return 'Due today'
    if days == 1:               return 'Due tomorrow'
    if days <= 7:               return f'Due in {days}d'
    return ''


# ════════════════════════════════════════════════════════════════
#  TAX REMINDERS LIST
# ════════════════════════════════════════════════════════════════

@phase7.route('/tax')
def tax():
    config  = _cfg()
    badges  = _badges()
    conn    = get_connection()
    try:
        today     = date.today()
        today_str = today.strftime('%Y-%m-%d')
        year_raw  = request.args.get('year', str(today.year))
        year      = 'all' if year_raw == 'all' else int(year_raw)
        view      = request.args.get('view', 'list')   # 'list' or 'calendar'
        status_f  = request.args.get('status', '')

        where  = ["is_deleted=0"]
        params = []
        if year == 'all':
            # No year restriction — show everything, with optional status filter
            if status_f:
                where.append("status=?"); params.append(status_f)
        elif not status_f:
            # Year filter: due_date in selected year (but show overdue from prior years too)
            where.append("(substr(due_date,1,4)=? OR (status='Pending' AND due_date < ?))")
            params += [str(year), f"{year}-01-01"]
        else:
            where.append("substr(due_date,1,4)=?"); params.append(str(year))
            where.append("status=?");              params.append(status_f)

        rows = conn.execute(f"""
            SELECT * FROM reminders
            WHERE {' AND '.join(where)}
            ORDER BY due_date, id
        """, params).fetchall()

        reminders_list = []
        for r in rows:
            d = dict(r)
            days = _days_until(d['due_date'], today_str)
            d['days_until']     = days
            d['urgency']        = _urgency(days, d['status'])
            d['urgency_label']  = _urgency_label(days, d['status'])
            d['display']        = get_reminder_status(d['due_date'], d['status'])
            reminders_list.append(d)

        # Summary counts
        pending  = [r for r in reminders_list if r['status'] == 'Pending']
        done     = [r for r in reminders_list if r['status'] == 'Done']
        snoozed  = [r for r in reminders_list if r['status'] == 'RemindLater']
        overdue  = [r for r in pending if r['days_until'] < 0]
        upcoming7= [r for r in pending if 0 <= r['days_until'] <= 7]

        # Calendar data: for the current month view (only relevant when not 'all')
        _cal_year_default = today.year if year == 'all' else year
        cal_month = int(request.args.get('month', today.month))
        cal_year  = int(request.args.get('cal_year', _cal_year_default))
        cal_rows  = conn.execute("""
            SELECT id, due_date, task_description, status
            FROM reminders
            WHERE substr(due_date,1,7)=? AND is_deleted=0
            ORDER BY due_date
        """, [f"{cal_year:04d}-{cal_month:02d}"]).fetchall()
        # Map day → list of reminders
        cal_map = {}
        for r in cal_rows:
            day = int(r['due_date'].split('-')[2])
            cal_map.setdefault(day, []).append(dict(r))

        # Available years
        years = conn.execute("""
            SELECT DISTINCT substr(due_date,1,4) AS yr
            FROM reminders WHERE is_deleted=0 ORDER BY yr
        """).fetchall()
        year_list = [r['yr'] for r in years]
        if year != 'all' and str(year) not in year_list:
            year_list.append(str(year))
        year_list = sorted(set(year_list))

        # Pre-group by urgency for the list view
        sections = [
            ('overdue', '🔴 Overdue',            [r for r in reminders_list if r['urgency'] == 'overdue']),
            ('urgent',  '🟠 Due This Week',       [r for r in reminders_list if r['urgency'] == 'urgent']),
            ('soon',    '🟡 Due Within 30 Days',  [r for r in reminders_list if r['urgency'] == 'soon']),
            ('upcoming','📅 Upcoming',             [r for r in reminders_list if r['urgency'] == 'upcoming']),
            ('snoozed', '💤 Snoozed',             [r for r in reminders_list if r['urgency'] == 'snoozed']),
            ('done',    '✅ Done',                [r for r in reminders_list if r['urgency'] == 'done']),
        ]

        return render_template('tax.html',
            config=config, badges=badges, today=today_str,
            reminders=reminders_list, sections=sections,
            year=year, view=view, status_f=status_f,
            pending=pending, done=done, snoozed=snoozed,
            overdue=overdue, upcoming7=upcoming7,
            year_list=year_list,
            # Calendar
            cal_month=cal_month, cal_year=cal_year,
            cal_map=cal_map,
            month_name=month_name[cal_month],
            month_grid=monthcalendar(cal_year, cal_month),
            prev_month=(cal_month-1) if cal_month > 1 else 12,
            prev_cal_year=(cal_year if cal_month > 1 else cal_year-1),
            next_month=(cal_month+1) if cal_month < 12 else 1,
            next_cal_year=(cal_year if cal_month < 12 else cal_year+1),
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CREATE / EDIT
# ════════════════════════════════════════════════════════════════

@phase7.route('/tax/new', methods=['GET', 'POST'])
def tax_new():
    if request.method == 'GET':
        config = _cfg(); badges = _badges()
        today = date.today().strftime('%Y-%m-%d')
        prefill_desc = request.args.get('desc', '')
        prefill_date = request.args.get('date', today)
        return render_template('tax_form.html',
            config=config, badges=badges,
            reminder=None, today=today,
            prefill_desc=prefill_desc, prefill_date=prefill_date)
    return _reminder_save(None, request.form.to_dict())


@phase7.route('/tax/<int:rem_id>/edit', methods=['GET', 'POST'])
def tax_edit(rem_id):
    if request.method == 'GET':
        config = _cfg(); badges = _badges()
        conn = get_connection()
        try:
            r = conn.execute(
                "SELECT * FROM reminders WHERE id=? AND is_deleted=0", [rem_id]
            ).fetchone()
            if not r:
                flash('Reminder not found.', 'error')
                return redirect(url_for('phase7.tax'))
            return render_template('tax_form.html',
                config=config, badges=badges,
                reminder=dict(r), today=date.today().strftime('%Y-%m-%d'),
                prefill_desc='', prefill_date='')
        finally:
            conn.close()
    return _reminder_save(rem_id, request.form.to_dict())


def _reminder_save(rem_id, data):
    due_date = (data.get('due_date') or '').strip()
    desc     = (data.get('task_description') or '').strip()

    if not due_date or not desc:
        flash('Due date and description are required.', 'error')
        return redirect(request.referrer or url_for('phase7.tax_new'))

    is_recurring = 1 if data.get('is_recurring') else 0
    frequency    = (data.get('frequency') or '').strip()
    url          = (data.get('url') or '').strip()
    amount_raw   = (data.get('amount') or '0').replace(',', '').replace('$', '')
    notes        = (data.get('notes') or '').strip()
    try:
        amount = float(amount_raw)
    except ValueError:
        amount = 0.0

    if not is_recurring:
        frequency = ''

    with db() as conn:
        if rem_id:
            old = conn.execute("SELECT * FROM reminders WHERE id=?", [rem_id]).fetchone()
            conn.execute("""
                UPDATE reminders SET
                    due_date=?, task_description=?, is_recurring=?,
                    frequency=?, url=?, amount=?, notes=?,
                    updated_at=datetime('now')
                WHERE id=?
            """, [due_date, desc, is_recurring, frequency, url, amount, notes, rem_id])
            log_action(conn, 'reminders', rem_id, 'UPDATE', old_data=dict(old) if old else {})
            flash('Reminder updated.', 'success')
            return redirect(url_for('phase7.tax'))
        else:
            cur = conn.execute("""
                INSERT INTO reminders
                    (due_date, task_description, is_recurring, frequency, url, amount, notes, status)
                VALUES (?,?,?,?,?,?,?,'Pending')
            """, [due_date, desc, is_recurring, frequency, url, amount, notes])
            log_action(conn, 'reminders', cur.lastrowid, 'INSERT',
                       new_data={'due_date': due_date, 'description': desc})
            flash(f'Reminder added: {desc}.', 'success')
            return redirect(url_for('phase7.tax'))


# ════════════════════════════════════════════════════════════════
#  STATUS ACTIONS  (JSON API — called from list-view JS)
# ════════════════════════════════════════════════════════════════

@phase7.route('/api/reminders/<int:rem_id>/done', methods=['POST'])
def reminder_done(rem_id):
    data         = request.json or {}
    payment_date = (data.get('payment_date') or date.today().strftime('%Y-%m-%d')).strip()
    amount_raw   = data.get('amount', 0)
    amount       = float(amount_raw) if isinstance(amount_raw, (int, float)) else \
                   float(str(amount_raw).replace(',','').replace('$','') or 0)

    with db() as conn:
        old = conn.execute("SELECT * FROM reminders WHERE id=? AND is_deleted=0", [rem_id]).fetchone()
        if not old:
            return jsonify({'error': 'Not found'}), 404
        conn.execute("""
            UPDATE reminders
            SET status='Done', payment_date=?, amount=?, updated_at=datetime('now')
            WHERE id=?
        """, [payment_date, amount, rem_id])
        log_action(conn, 'reminders', rem_id, 'UPDATE',
                   old_data=dict(old),
                   new_data={'status': 'Done', 'payment_date': payment_date, 'amount': amount})
    return jsonify({'success': True, 'status': 'Done'})


@phase7.route('/api/reminders/<int:rem_id>/snooze', methods=['POST'])
def reminder_snooze(rem_id):
    data   = request.json or {}
    days   = int(data.get('days', 7))
    days   = max(1, min(days, 365))  # clamp 1–365

    conn = get_connection()
    try:
        rem = conn.execute(
            "SELECT * FROM reminders WHERE id=? AND is_deleted=0", [rem_id]
        ).fetchone()
        if not rem:
            conn.close()
            return jsonify({'error': 'Not found'}), 404
        try:
            old_due = datetime.strptime(rem['due_date'], '%Y-%m-%d').date()
        except Exception:
            old_due = date.today()
        new_due = (old_due + timedelta(days=days)).strftime('%Y-%m-%d')
    finally:
        conn.close()

    with db() as conn:
        conn.execute("""
            UPDATE reminders
            SET status='RemindLater', due_date=?, updated_at=datetime('now')
            WHERE id=?
        """, [new_due, rem_id])
        log_action(conn, 'reminders', rem_id, 'UPDATE',
                   old_data={'due_date': rem['due_date'], 'status': rem['status']},
                   new_data={'status': 'RemindLater', 'due_date': new_due})
    return jsonify({'success': True, 'new_due_date': new_due})


@phase7.route('/api/reminders/<int:rem_id>/reopen', methods=['POST'])
def reminder_reopen(rem_id):
    """Set status back to Pending (undo Done or snooze)."""
    with db() as conn:
        old = conn.execute("SELECT * FROM reminders WHERE id=? AND is_deleted=0", [rem_id]).fetchone()
        if not old:
            return jsonify({'error': 'Not found'}), 404
        conn.execute("""
            UPDATE reminders SET status='Pending', updated_at=datetime('now') WHERE id=?
        """, [rem_id])
        log_action(conn, 'reminders', rem_id, 'UPDATE',
                   old_data=dict(old), new_data={'status': 'Pending'})
    return jsonify({'success': True})


@phase7.route('/api/reminders/<int:rem_id>/delete', methods=['POST'])
def reminder_delete(rem_id):
    with db() as conn:
        soft_delete(conn, 'reminders', rem_id)
    return jsonify({'success': True})


# ════════════════════════════════════════════════════════════════
#  ADVANCE YEAR  (clone recurring → next year)
# ════════════════════════════════════════════════════════════════

@phase7.route('/api/reminders/advance-year', methods=['POST'])
def advance_year():
    """
    Clone all recurring reminders from `from_year` into `to_year`.
    Skips any that already exist (same description + same YYYY).
    Returns count of newly created reminders.
    """
    data      = request.json or {}
    from_year = int(data.get('from_year', date.today().year))
    to_year   = int(data.get('to_year',   from_year + 1))

    if to_year <= from_year:
        return jsonify({'error': 'to_year must be after from_year'}), 400

    diff = to_year - from_year

    with db() as conn:
        source_rows = conn.execute("""
            SELECT * FROM reminders
            WHERE is_recurring=1 AND is_deleted=0
              AND substr(due_date,1,4)=?
        """, [str(from_year)]).fetchall()

        # Existing descriptions in to_year (to avoid duplication)
        existing = conn.execute("""
            SELECT task_description FROM reminders
            WHERE substr(due_date,1,4)=? AND is_deleted=0
        """, [str(to_year)]).fetchall()
        existing_descs = {r['task_description'] for r in existing}

        created = 0
        for r in source_rows:
            # Advance due_date by diff years
            try:
                old_due = datetime.strptime(r['due_date'], '%Y-%m-%d').date()
                new_due = old_due.replace(year=old_due.year + diff)
                new_due_str = new_due.strftime('%Y-%m-%d')
            except ValueError:
                # Feb 29 edge case → Feb 28
                new_due_str = r['due_date'].replace(str(from_year), str(to_year), 1)

            # Update year references in description
            desc = r['task_description']
            desc = desc.replace(str(from_year),   str(to_year))
            desc = desc.replace(str(from_year-1), str(to_year-1))

            if desc in existing_descs:
                continue  # skip duplicate

            conn.execute("""
                INSERT INTO reminders
                    (due_date, task_description, is_recurring, frequency,
                     url, amount, notes, status)
                VALUES (?,?,?,?,?,?,?,'Pending')
            """, [new_due_str, desc, r['is_recurring'], r['frequency'],
                  r['url'], 0.0, r['notes']])
            created += 1

    return jsonify({'success': True, 'created': created, 'to_year': to_year})


@phase7.route('/api/reminders/seed-year', methods=['POST'])
def seed_year():
    """
    Seed all STANDARD_DEADLINES for a given year (skips existing).
    """
    data = request.json or {}
    year = int(data.get('year', date.today().year))

    with db() as conn:
        existing = {r['task_description'] for r in conn.execute(
            "SELECT task_description FROM reminders WHERE substr(due_date,1,4)=? AND is_deleted=0",
            [str(year)]
        ).fetchall()}

        created = 0
        for month, day, desc_tpl, freq, url in STANDARD_DEADLINES:
            desc = desc_tpl.format(year=year, prev_year=year-1)
            if desc in existing:
                continue
            try:
                due = date(year, month, day).strftime('%Y-%m-%d')
            except ValueError:
                continue
            conn.execute("""
                INSERT INTO reminders (due_date, task_description, is_recurring, frequency, url, status)
                VALUES (?,?,1,?,?,'Pending')
            """, [due, desc, freq, url])
            created += 1

    return jsonify({'success': True, 'created': created, 'year': year})


# ════════════════════════════════════════════════════════════════
#  UPCOMING API  (used by dashboard widget)
# ════════════════════════════════════════════════════════════════

@phase7.route('/api/reminders/upcoming')
def api_upcoming():
    days_ahead = int(request.args.get('days', 30))
    limit      = int(request.args.get('limit', 10))
    today      = date.today()
    cutoff     = (today + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
    today_str  = today.strftime('%Y-%m-%d')

    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT * FROM reminders
            WHERE status='Pending' AND is_deleted=0
              AND due_date <= ?
            ORDER BY due_date LIMIT ?
        """, [cutoff, limit]).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            days = _days_until(d['due_date'], today_str)
            d['days_until']    = days
            d['urgency']       = _urgency(days, d['status'])
            d['urgency_label'] = _urgency_label(days, d['status'])
            result.append(d)
        return jsonify(result)
    finally:
        conn.close()


@phase7.route('/api/reminders/counts')
def api_counts():
    """Badge counts: overdue + due within 7 days."""
    today = date.today().strftime('%Y-%m-%d')
    week  = (date.today() + timedelta(days=7)).strftime('%Y-%m-%d')
    conn  = get_connection()
    try:
        overdue = conn.execute(
            "SELECT COUNT(*) FROM reminders WHERE status='Pending' AND due_date < ? AND is_deleted=0",
            [today]
        ).fetchone()[0]
        soon = conn.execute(
            "SELECT COUNT(*) FROM reminders WHERE status='Pending' AND due_date BETWEEN ? AND ? AND is_deleted=0",
            [today, week]
        ).fetchone()[0]
        return jsonify({'overdue': overdue, 'due_this_week': soon, 'total_attention': overdue + soon})
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  EXPORT
# ════════════════════════════════════════════════════════════════

@phase7.route('/tax/export')
def tax_export():
    year   = request.args.get('year', date.today().year)
    conn   = get_connection()
    try:
        rows = conn.execute("""
            SELECT * FROM reminders
            WHERE substr(due_date,1,4)=? AND is_deleted=0
            ORDER BY due_date
        """, [str(year)]).fetchall()

        COLS = ['due_date','task_description','frequency','status',
                'payment_date','amount','is_recurring','url','notes']

        def generate():
            yield ','.join(COLS) + '\n'
            for r in rows:
                d = dict(r)
                yield ','.join(
                    f'"{str(d.get(c,"")).replace(chr(34),chr(34)*2)}"' for c in COLS
                ) + '\n'

        fname = f"tax_reminders_{year}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        return Response(
            stream_with_context(generate()),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={fname}'}
        )
    finally:
        conn.close()
