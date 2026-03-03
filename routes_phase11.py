"""
Phase 11 — Certs Tracker & Quick Quote

CERTS TRACKER (/certs):
  - List all contractor certificates (COI, general liability, workers comp, etc.)
  - Status badges: Active / Expiring Soon (≤60 days) / Expired
  - Filter by contractor, cert type, status
  - Add / Edit / Delete certs per contractor
  - Bulk-view: all certs for a given contractor on one page
  - Alert tiles: expired count, expiring-soon count, upcoming expirations
  - Export CSV

QUICK QUOTE (/quote):
  - Zero-friction ballpark calculator — no record saved unless user wants
  - Input: total job price OR build up from labor/materials/subs
  - Overhead / insurance / owner wages / profit % sliders with live math
  - Output: full cost breakdown, suggested price, margin
  - Optionally save as a draft estimate (POST to /estimates/new)
  - Print-friendly quote sheet
"""
from datetime import date, datetime, timedelta
from flask import (Blueprint, render_template, request, jsonify,
                   redirect, url_for, flash, Response, stream_with_context)

from database import get_connection, db
from automations import log_action, soft_delete, calculate_quick_quote

phase11 = Blueprint('phase11', __name__)

TODAY = date.today().strftime('%Y-%m-%d')
# Subcontractor COI types
CERT_TYPES = [
    # --- Subcontractor Certificates ---
    'General Liability',
    'Workers Compensation',
    'Certificate of Insurance (COI)',
    'Professional Liability / E&O',
    'Auto Liability',
    'Umbrella / Excess Liability',
    'Builders Risk',
    'Contractors License',
    'Business License',
    'Bond / Surety',
    # --- Business Insurance Policies ---
    'Business — General Liability',
    'Business — Workers Comp',
    'Business — Commercial Auto',
    'Business — Umbrella',
    'Business — Builders Risk',
    'Business — Professional Liability',
    'Business — Business Owner Policy (BOP)',
    'Business — Commercial Property',
    'Business — Life / Key Person',
    'Other',
]


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _cfg():
    conn = get_connection()
    try:
        r = conn.execute("SELECT * FROM company_config WHERE id=1").fetchone()
        return dict(r) if r else {}
    finally:
        conn.close()

def _certs_folder():
    return _cfg().get('certs_folder_path', '')

def _verify_cert_file(cert_filename: str) -> bool:
    """Check if cert PDF exists in the configured certs folder."""
    folder = _certs_folder()
    if not cert_filename or not folder:
        return False
    import os as _os
    path = _os.path.join(folder.replace('/', _os.sep), cert_filename)
    return _os.path.isfile(path)

def _verify_all_certs():
    """Re-scan the certs folder and update cert_verified flags for all certs."""
    conn = get_connection()
    try:
        folder = _certs_folder()
        rows = conn.execute(
            "SELECT id, cert_filename FROM certificates WHERE cert_filename!='' AND is_deleted=0"
        ).fetchall()
        count = 0
        for row in rows:
            import os as _os
            verified = _verify_cert_file(row['cert_filename'])
            conn.execute(
                "UPDATE certificates SET cert_verified=?, updated_at=datetime('now') WHERE id=?",
                [1 if verified else 0, row['id']]
            )
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()

def _badges():
    from app import get_nav_badges
    return get_nav_badges()

def _cert_status(end_date_str: str) -> str:
    """Return 'Expired', 'Expiring Soon', or 'Active'."""
    if not end_date_str:
        return 'Unknown'
    try:
        end = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        return 'Unknown'
    today = date.today()
    if end < today:
        return 'Expired'
    if end <= today + timedelta(days=60):
        return 'Expiring Soon'
    return 'Active'

def _days_until(end_date_str: str) -> int | None:
    if not end_date_str:
        return None
    try:
        end = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        return (end - date.today()).days
    except ValueError:
        return None


# ════════════════════════════════════════════════════════════════
#  CERTS TRACKER — LIST
# ════════════════════════════════════════════════════════════════

@phase11.route('/api/certs/<int:cert_id>/verify', methods=['POST'])
def cert_verify(cert_id):
    """Check if a cert file exists on disk and update verified flag."""
    conn = get_connection()
    try:
        cert = conn.execute(
            "SELECT * FROM certificates WHERE id=? AND is_deleted=0", [cert_id]
        ).fetchone()
        if not cert:
            return jsonify({'error': 'Not found'}), 404
        verified = _verify_cert_file(cert['cert_filename'])
        with db() as wconn:
            wconn.execute(
                "UPDATE certificates SET cert_verified=?, updated_at=datetime('now') WHERE id=?",
                [1 if verified else 0, cert_id]
            )
        return jsonify({
            'success': True,
            'verified': verified,
            'filename': cert['cert_filename'],
        })
    finally:
        conn.close()


@phase11.route('/api/certs/verify-all', methods=['POST'])
def certs_verify_all():
    """Scan certs folder and update all cert_verified flags."""
    count = _verify_all_certs()
    folder = _certs_folder()
    return jsonify({'success': True, 'checked': count, 'folder': folder})


# ════════════════════════════════════════════════════════════════
#  CERTS TRACKER — LIST
# ════════════════════════════════════════════════════════════════

@phase11.route('/certs')
def certs():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        today     = date.today().strftime('%Y-%m-%d')
        soon_date = (date.today() + timedelta(days=60)).strftime('%Y-%m-%d')

        status_f     = request.args.get('status', '')
        contractor_f = request.args.get('contractor', '')
        cert_type_f  = request.args.get('cert_type', '')

        # Summary counts
        expired_count  = conn.execute(
            "SELECT COUNT(*) FROM certificates WHERE end_date < ? AND is_deleted=0", [today]
        ).fetchone()[0]
        expiring_count = conn.execute(
            "SELECT COUNT(*) FROM certificates WHERE end_date BETWEEN ? AND ? AND is_deleted=0",
            [today, soon_date]
        ).fetchone()[0]
        active_count   = conn.execute(
            "SELECT COUNT(*) FROM certificates WHERE end_date > ? AND is_deleted=0", [soon_date]
        ).fetchone()[0]
        unknown_count  = conn.execute(
            "SELECT COUNT(*) FROM certificates WHERE (end_date = '' OR end_date IS NULL) AND is_deleted=0"
        ).fetchone()[0]

        # Build query
        where  = ["c.is_deleted=0"]
        params = []
        if contractor_f:
            where.append("(con.company_name LIKE ? OR con.contact_person LIKE ?)")
            params += [f"%{contractor_f}%", f"%{contractor_f}%"]
        if cert_type_f:
            where.append("c.cert_type=?"); params.append(cert_type_f)
        if status_f == 'Expired':
            where.append("c.end_date < ?"); params.append(today)
        elif status_f == 'Expiring Soon':
            where.append("c.end_date BETWEEN ? AND ?"); params += [today, soon_date]
        elif status_f == 'Active':
            where.append("c.end_date > ?"); params.append(soon_date)
        elif status_f == 'Unknown':
            where.append("(c.end_date = '' OR c.end_date IS NULL)")

        rows = conn.execute(f"""
            SELECT c.*, con.company_name AS contractor_name,
                   con.contact_person, con.phone, con.email, con.trade_type
            FROM certificates c
            LEFT JOIN contractors con ON c.contractor_id = con.id
            WHERE {' AND '.join(where)}
            ORDER BY c.end_date ASC NULLS LAST, con.company_name
        """, params).fetchall()

        certs_list = []
        for r in rows:
            d = dict(r)
            d['status']    = _cert_status(d.get('end_date') or '')
            d['days_left'] = _days_until(d.get('end_date') or '')
            certs_list.append(d)

        # For filter dropdowns
        contractors = conn.execute(
            "SELECT id, company_name FROM contractors WHERE is_deleted=0 ORDER BY company_name"
        ).fetchall()

        # Upcoming expirations (next 90 days) for alert panel
        upcoming = conn.execute("""
            SELECT c.id, c.cert_type, c.end_date, con.company_name AS contractor_name
            FROM certificates c
            LEFT JOIN contractors con ON c.contractor_id = con.id
            WHERE c.end_date BETWEEN ? AND date(?, '+90 days')
              AND c.is_deleted=0
            ORDER BY c.end_date ASC
        """, [today, today]).fetchall()

        return render_template('certs.html',
            config=config, badges=badges,
            certs=certs_list,
            expired_count=expired_count,
            expiring_count=expiring_count,
            active_count=active_count,
            unknown_count=unknown_count,
            upcoming=[dict(r) for r in upcoming],
            contractors=[dict(c) for c in contractors],
            cert_types=CERT_TYPES,
            status_f=status_f,
            contractor_f=contractor_f,
            cert_type_f=cert_type_f,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CERTS — CREATE
# ════════════════════════════════════════════════════════════════

@phase11.route('/certs/new', methods=['GET', 'POST'])
def cert_new():
    config = _cfg()
    badges = _badges()

    if request.method == 'GET':
        conn = get_connection()
        try:
            contractors = conn.execute(
                "SELECT id, company_name, trade_type FROM contractors WHERE is_deleted=0 ORDER BY company_name"
            ).fetchall()
            prefill_contractor = request.args.get('contractor_id', '')
            return render_template('cert_form.html',
                config=config, badges=badges,
                cert=None,
                contractors=[dict(c) for c in contractors],
                cert_types=CERT_TYPES,
                prefill_contractor=prefill_contractor,
                today=TODAY,
            )
        finally:
            conn.close()

    # POST — save
    return _cert_save(None, request.form)


@phase11.route('/certs/<int:cert_id>/edit', methods=['GET', 'POST'])
def cert_edit(cert_id):
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        cert = conn.execute(
            "SELECT * FROM certificates WHERE id=? AND is_deleted=0", [cert_id]
        ).fetchone()
        if not cert:
            flash('Certificate not found.', 'error')
            return redirect(url_for('phase11.certs'))

        if request.method == 'GET':
            contractors = conn.execute(
                "SELECT id, company_name, trade_type FROM contractors WHERE is_deleted=0 ORDER BY company_name"
            ).fetchall()
            return render_template('cert_form.html',
                config=config, badges=badges,
                cert=dict(cert),
                contractors=[dict(c) for c in contractors],
                cert_types=CERT_TYPES,
                prefill_contractor='',
                today=TODAY,
            )
    finally:
        conn.close()

    return _cert_save(cert_id, request.form)


def _cert_save(cert_id, form):
    contractor_id = form.get('contractor_id') or None
    company_name  = (form.get('company_name') or '').strip()
    cert_type     = (form.get('cert_type') or '').strip()
    start_date    = (form.get('start_date') or '').strip()
    end_date      = (form.get('end_date') or '').strip()
    cert_filename = (form.get('cert_filename') or '').strip()
    notes         = (form.get('notes') or '').strip()

    if not cert_type:
        flash('Certificate type is required.', 'error')
        return redirect(request.referrer or url_for('phase11.cert_new'))

    with db() as conn:
        if cert_id:
            old = conn.execute("SELECT * FROM certificates WHERE id=?", [cert_id]).fetchone()
            conn.execute("""
                UPDATE certificates SET
                    contractor_id=?, company_name=?, cert_type=?,
                    start_date=?, end_date=?, cert_filename=?, notes=?,
                    updated_at=datetime('now')
                WHERE id=?
            """, [contractor_id, company_name, cert_type,
                  start_date, end_date, cert_filename, notes, cert_id])
            log_action(conn, 'certificates', cert_id, 'UPDATE', old_data=dict(old) if old else {})
            flash('Certificate updated.', 'success')
        else:
            cur = conn.execute("""
                INSERT INTO certificates
                    (contractor_id, company_name, cert_type, start_date, end_date,
                     cert_filename, notes)
                VALUES (?,?,?,?,?,?,?)
            """, [contractor_id, company_name, cert_type,
                  start_date, end_date, cert_filename, notes])
            log_action(conn, 'certificates', cur.lastrowid, 'INSERT',
                       new_data={'cert_type': cert_type})
            flash('Certificate added.', 'success')

    return redirect(url_for('phase11.certs'))


# ════════════════════════════════════════════════════════════════
#  CERTS — DELETE
# ════════════════════════════════════════════════════════════════

@phase11.route('/api/certs/<int:cert_id>/delete', methods=['POST'])
def cert_delete(cert_id):
    with db() as conn:
        cert = conn.execute(
            "SELECT * FROM certificates WHERE id=? AND is_deleted=0", [cert_id]
        ).fetchone()
        if not cert:
            return jsonify({'error': 'Not found'}), 404
        soft_delete(conn, 'certificates', cert_id)
    return jsonify({'success': True})


# ════════════════════════════════════════════════════════════════
#  CERTS — CONTRACTOR VIEW (all certs for one contractor)
# ════════════════════════════════════════════════════════════════

@phase11.route('/certs/contractor/<int:contractor_id>')
def certs_for_contractor(contractor_id):
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        contractor = conn.execute(
            "SELECT * FROM contractors WHERE id=? AND is_deleted=0", [contractor_id]
        ).fetchone()
        if not contractor:
            flash('Contractor not found.', 'error')
            return redirect(url_for('phase11.certs'))

        certs_rows = conn.execute("""
            SELECT * FROM certificates
            WHERE contractor_id=? AND is_deleted=0
            ORDER BY end_date ASC
        """, [contractor_id]).fetchall()

        certs_list = []
        for r in certs_rows:
            d = dict(r)
            d['status']    = _cert_status(d.get('end_date') or '')
            d['days_left'] = _days_until(d.get('end_date') or '')
            certs_list.append(d)

        return render_template('certs_contractor.html',
            config=config, badges=badges,
            contractor=dict(contractor),
            certs=certs_list,
            cert_types=CERT_TYPES,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CERTS — EXPORT CSV
# ════════════════════════════════════════════════════════════════

@phase11.route('/certs/export')
def certs_export():
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT c.cert_type, c.start_date, c.end_date, c.cert_filename, c.notes,
                   con.company_name AS contractor, con.contact_person, con.phone, con.email
            FROM certificates c
            LEFT JOIN contractors con ON c.contractor_id = con.id
            WHERE c.is_deleted=0
            ORDER BY c.end_date ASC
        """).fetchall()

        COLS = ['cert_type','start_date','end_date','cert_filename','notes',
                'contractor','contact_person','phone','email','status']

        def generate():
            yield ','.join(COLS) + '\n'
            for r in rows:
                d = dict(r)
                d['status'] = _cert_status(d.get('end_date') or '')
                yield ','.join(
                    f'"{str(d.get(c,"")).replace(chr(34), chr(34)*2)}"'
                    for c in COLS
                ) + '\n'

        fname = f"certificates_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        return Response(stream_with_context(generate()), mimetype='text/csv',
                        headers={'Content-Disposition': f'attachment; filename={fname}'})
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CERTS — STATUS UPDATE (renew: set new end date)
# ════════════════════════════════════════════════════════════════

@phase11.route('/api/certs/<int:cert_id>/renew', methods=['POST'])
def cert_renew(cert_id):
    data     = request.json or {}
    new_end  = (data.get('end_date') or '').strip()
    new_start = (data.get('start_date') or '').strip()

    if not new_end:
        return jsonify({'error': 'end_date is required'}), 400

    with db() as conn:
        cert = conn.execute(
            "SELECT * FROM certificates WHERE id=? AND is_deleted=0", [cert_id]
        ).fetchone()
        if not cert:
            return jsonify({'error': 'Not found'}), 404
        conn.execute("""
            UPDATE certificates
            SET end_date=?, start_date=CASE WHEN ?!='' THEN ? ELSE start_date END,
                updated_at=datetime('now')
            WHERE id=?
        """, [new_end, new_start, new_start, cert_id])
        log_action(conn, 'certificates', cert_id, 'UPDATE',
                   old_data=dict(cert),
                   new_data={'end_date': new_end, 'start_date': new_start})

    new_status = _cert_status(new_end)
    return jsonify({'success': True, 'status': new_status, 'end_date': new_end})


# ════════════════════════════════════════════════════════════════
#  QUICK QUOTE
# ════════════════════════════════════════════════════════════════

@phase11.route('/quote')
def quick_quote():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        jobs    = conn.execute(
            "SELECT id, job_code, description FROM jobs WHERE is_deleted=0 ORDER BY job_code"
        ).fetchall()
        clients = conn.execute(
            "SELECT id, full_name FROM clients WHERE is_deleted=0 ORDER BY full_name"
        ).fetchall()
        return render_template('quick_quote.html',
            config=config, badges=badges,
            jobs=[dict(j) for j in jobs],
            clients=[dict(c) for c in clients],
            default_overhead   = config.get('default_overhead_pct')   or 15.0,
            default_insurance  = config.get('default_insurance_pct')  or 5.0,
            default_owner_wages= config.get('default_owner_wages_pct')or 10.0,
            default_profit     = config.get('default_profit_pct')     or 10.0,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  QUICK QUOTE — CALCULATE API
# ════════════════════════════════════════════════════════════════

@phase11.route('/api/quick-quote/calculate', methods=['POST'])
def api_quick_quote_calc():
    """
    Two modes:
      mode=buildUp — user enters labor/materials/subs/other, we compute sell price
      mode=topDown — user enters desired sell price, we back-calculate direct costs
    """
    data = request.json or {}
    mode = data.get('mode', 'buildUp')

    try:
        overhead_pct    = float(data.get('overhead_pct')    or 0)
        insurance_pct   = float(data.get('insurance_pct')   or 0)
        owner_wages_pct = float(data.get('owner_wages_pct') or 0)
        profit_pct      = float(data.get('profit_pct')      or 0)
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid percentage values'}), 400

    if mode == 'buildUp':
        try:
            labor       = float(data.get('labor')       or 0)
            materials   = float(data.get('materials')   or 0)
            subs        = float(data.get('subs')        or 0)
            other       = float(data.get('other')       or 0)
        except (ValueError, TypeError):
            return jsonify({'error': 'Invalid cost values'}), 400

        direct = labor + materials + subs + other
        overhead_amt    = round(direct * overhead_pct    / 100, 2)
        insurance_amt   = round(direct * insurance_pct   / 100, 2)
        owner_wages_amt = round(direct * owner_wages_pct / 100, 2)
        subtotal        = round(direct + overhead_amt + insurance_amt + owner_wages_amt, 2)
        profit_amt      = round(subtotal * profit_pct / 100, 2)
        sell_price      = round(subtotal + profit_amt, 2)
        margin          = round((sell_price - direct) / sell_price * 100, 1) if sell_price else 0

        return jsonify({
            'mode': 'buildUp',
            'labor': labor, 'materials': materials, 'subs': subs, 'other': other,
            'direct': direct,
            'overhead_amt': overhead_amt,
            'insurance_amt': insurance_amt,
            'owner_wages_amt': owner_wages_amt,
            'subtotal': subtotal,
            'profit_amt': profit_amt,
            'sell_price': sell_price,
            'margin': margin,
        })

    elif mode == 'topDown':
        try:
            sell_price = float(data.get('sell_price') or 0)
        except (ValueError, TypeError):
            return jsonify({'error': 'Invalid sell_price'}), 400

        result = calculate_quick_quote(sell_price, overhead_pct, insurance_pct,
                                       owner_wages_pct, profit_pct)
        if 'error' in result:
            return jsonify(result), 400

        direct   = result['direct_costs']
        overhead = result['overhead']
        insur    = result['insurance']
        ow       = result['owner_wages']
        profit   = result['profit']
        margin   = round((sell_price - direct) / sell_price * 100, 1) if sell_price else 0

        return jsonify({
            'mode': 'topDown',
            'sell_price': sell_price,
            'direct': direct,
            'overhead_amt': overhead,
            'insurance_amt': insur,
            'owner_wages_amt': ow,
            'profit_amt': profit,
            'margin': margin,
        })

    return jsonify({'error': f'Unknown mode: {mode}'}), 400


# ════════════════════════════════════════════════════════════════
#  QUICK QUOTE — SAVE AS ESTIMATE
# ════════════════════════════════════════════════════════════════

@phase11.route('/api/quick-quote/save-as-estimate', methods=['POST'])
def api_save_as_estimate():
    """Convert a quick quote into a saved draft estimate."""
    data = request.json or {}

    job_id        = data.get('job_id') or None
    client_id     = data.get('client_id') or None
    quote_name    = (data.get('quote_name') or 'Quick Quote').strip()
    notes         = (data.get('notes') or '').strip()
    overhead_pct  = float(data.get('overhead_pct')    or 0)
    insurance_pct = float(data.get('insurance_pct')   or 0)
    owner_wages   = float(data.get('owner_wages_pct') or 0)
    profit_pct    = float(data.get('profit_pct')      or 0)
    sell_price    = float(data.get('sell_price')       or 0)
    direct        = float(data.get('direct')           or 0)
    labor         = float(data.get('labor')            or 0)
    materials     = float(data.get('materials')        or 0)
    subs          = float(data.get('subs')             or 0)
    other         = float(data.get('other')            or 0)

    with db() as conn:
        from routes_phase9 import _next_estimate_number
        prefix  = conn.execute("SELECT estimate_prefix FROM company_config WHERE id=1").fetchone()
        prefix  = (prefix['estimate_prefix'] if prefix else None) or 'EST'
        est_num = _next_estimate_number(conn, prefix)

        cur = conn.execute("""
            INSERT INTO job_estimates
                (job_id, client_id, estimate_date, estimate_number, status,
                 overhead_pct, insurance_pct, owner_wages_pct, profit_pct,
                 total_direct_costs, total_estimate, version_number, notes)
            VALUES (?,?,date('now'),?,'Draft',?,?,?,?,?,?,1,?)
        """, [job_id, client_id, est_num,
              overhead_pct, insurance_pct, owner_wages, profit_pct,
              direct, sell_price, f"Created from Quick Quote: {quote_name}. {notes}".strip()])
        est_id = cur.lastrowid

        # Add line items for each cost type that has a value
        for cat, amt, labor_h in [
            ('Labor',          labor,     0),
            ('Materials',      materials, 0),
            ('Subcontractors', subs,      0),
            ('Other',          other,     0),
        ]:
            if amt:
                conn.execute("""
                    INSERT INTO estimate_line_items
                        (estimate_id, category, labor_cost, materials_cost,
                         subcontractor_cost, other_cost, total_cost, sell_price)
                    VALUES (?,?,?,?,?,?,?,?)
                """, [est_id,
                      cat,
                      amt if cat == 'Labor' else 0,
                      amt if cat == 'Materials' else 0,
                      amt if cat == 'Subcontractors' else 0,
                      amt if cat == 'Other' else 0,
                      amt, amt])

        log_action(conn, 'job_estimates', est_id, 'INSERT',
                   new_data={'from_quick_quote': quote_name})

    return jsonify({'success': True, 'estimate_id': est_id, 'estimate_number': est_num})
