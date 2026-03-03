"""
Phase 8 Routes — Bank Reconciliation
Full bank reconciliation workflow:
  - Account list + quick balance overview
  - CSV import: parse bank statement (flexible column detection)
  - Auto-match: amount + date proximity, amount + description similarity
  - Manual match: drag ledger entry onto bank transaction
  - Mark excluded (bank fees, transfers, etc.)
  - Reconciliation session: set statement end date + ending balance
  - Session summary: cleared total, outstanding total, difference
  - Complete reconciliation: lock cleared entries, update ledger status
  - History: past reconciliation sessions per account
  - Export unmatched items to CSV
"""
import csv
import io
import json
import re
from datetime import datetime, date, timedelta
from flask import (Blueprint, render_template, request, jsonify,
                   redirect, url_for, flash, Response, stream_with_context)

from database import db, get_connection
from automations import log_action, soft_delete

phase8 = Blueprint('phase8', __name__)


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

def _acct(acct_id, conn):
    r = conn.execute(
        "SELECT * FROM bank_accounts WHERE id=? AND is_deleted=0", [acct_id]
    ).fetchone()
    return dict(r) if r else None

def _amount_match(a: float, b: float, tol: float = 0.01) -> bool:
    return abs(abs(a) - abs(b)) <= tol

def _date_proximity(d1: str, d2: str, days: int = 3) -> bool:
    try:
        dt1 = datetime.strptime(d1, '%Y-%m-%d').date()
        dt2 = datetime.strptime(d2, '%Y-%m-%d').date()
        return abs((dt1 - dt2).days) <= days
    except Exception:
        return False

def _normalize_desc(s: str) -> str:
    """Strip numbers and punctuation for fuzzy description matching."""
    s = re.sub(r'[^a-zA-Z\s]', ' ', (s or '').upper())
    return ' '.join(s.split())[:40]

def _desc_similarity(a: str, b: str) -> float:
    """Simple word-overlap ratio between two descriptions."""
    wa = set(_normalize_desc(a).split())
    wb = set(_normalize_desc(b).split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


# ─────────────────────────────────────────────
# CSV parsing
# ─────────────────────────────────────────────

def _parse_bank_csv(file_text: str) -> list[dict]:
    """
    Flexible CSV parser. Detects columns by common header names.
    Returns list of dicts: {transaction_date, description, amount, transaction_type, reference_number}
    """
    reader = csv.DictReader(io.StringIO(file_text.strip()))
    rows   = []
    headers = [h.strip().lower() for h in (reader.fieldnames or [])]

    # Column detection
    def _col(*candidates):
        for c in candidates:
            for h in headers:
                if c in h:
                    return h
        return None

    date_col  = _col('date', 'posted', 'trans date', 'transaction date')
    desc_col  = _col('description', 'memo', 'narration', 'payee', 'name', 'details')
    amt_col   = _col('amount', 'debit', 'credit', 'value')
    debit_col = _col('debit', 'withdrawal', 'charge')
    credit_col= _col('credit', 'deposit', 'payment')
    ref_col   = _col('reference', 'check', 'ref', 'transaction id', 'id', 'number')

    fieldnames_lower = {h: h for h in headers}
    for row in reader:
        row_lower = {k.strip().lower(): v for k, v in row.items()}

        # Date
        raw_date = row_lower.get(date_col, '') if date_col else ''
        txn_date = _parse_date(raw_date.strip())
        if not txn_date:
            continue  # skip rows without a parseable date

        # Description
        description = row_lower.get(desc_col, '').strip() if desc_col else ''

        # Amount + type
        if debit_col and credit_col:
            debit  = _parse_amount(row_lower.get(debit_col, ''))
            credit = _parse_amount(row_lower.get(credit_col, ''))
            if debit:
                amount, txn_type = debit, 'Debit'
            else:
                amount, txn_type = credit, 'Credit'
        elif amt_col:
            raw = _parse_amount_signed(row_lower.get(amt_col, ''))
            if raw < 0:
                amount, txn_type = abs(raw), 'Debit'
            else:
                amount, txn_type = raw, 'Credit'
        else:
            continue

        if amount == 0:
            continue

        # Reference
        reference = row_lower.get(ref_col, '').strip() if ref_col else ''

        rows.append({
            'transaction_date': txn_date,
            'description':      description,
            'amount':           round(amount, 2),
            'transaction_type': txn_type,
            'reference_number': reference,
        })

    return rows


def _parse_date(s: str) -> str:
    """Try multiple date formats, return YYYY-MM-DD or ''."""
    formats = ['%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%d/%m/%Y',
               '%m-%d-%Y', '%m-%d-%y', '%b %d, %Y', '%B %d, %Y',
               '%d-%b-%Y', '%Y/%m/%d']
    s = s.strip().replace('"', '').replace("'", '')
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return ''


def _parse_amount(s) -> float:
    if not s:
        return 0.0
    try:
        return abs(float(str(s).replace(',', '').replace('$', '').replace('(', '-').replace(')', '')))
    except (ValueError, TypeError):
        return 0.0


def _parse_amount_signed(s) -> float:
    if not s:
        return 0.0
    try:
        cleaned = str(s).replace(',', '').replace('$', '').strip()
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


# ─────────────────────────────────────────────
# Auto-matching engine
# ─────────────────────────────────────────────

def _run_auto_match(acct_id: int, conn) -> int:
    """
    Try to auto-match unmatched bank transactions against uncleared ledger entries.
    Returns count of new matches made.
    """
    # Fetch unmatched bank transactions for this account
    bank_rows = conn.execute("""
        SELECT * FROM bank_transactions
        WHERE bank_account_id=? AND match_status='Unmatched' AND is_deleted=0
    """, [acct_id]).fetchall()

    # Fetch uncleared ledger entries for this account (or any account if not assigned)
    ledger_rows = conn.execute("""
        SELECT * FROM ledger
        WHERE (bank_account_id=? OR bank_account_id IS NULL)
          AND status != 'Cleared' AND is_deleted=0
    """, [acct_id]).fetchall()

    matched = 0
    used_ledger_ids = set()

    for bt in bank_rows:
        bt = dict(bt)
        best_ledger_id  = None
        best_score      = 0.0

        for le in ledger_rows:
            le = dict(le)
            if le['id'] in used_ledger_ids:
                continue

            # Amount must match within $0.01
            if not _amount_match(bt['amount'], le['amount']):
                continue

            # Date proximity (within 5 days)
            if not _date_proximity(bt['transaction_date'], le['entry_date'], days=5):
                continue

            # Description similarity tiebreaker
            score = _desc_similarity(bt['description'], le.get('description', ''))
            # Strong date match boosts score
            date_diff = abs((datetime.strptime(bt['transaction_date'], '%Y-%m-%d').date()
                            - datetime.strptime(le['entry_date'], '%Y-%m-%d').date()).days)
            score += (5 - date_diff) * 0.1  # closer date = higher score

            if score > best_score:
                best_score     = score
                best_ledger_id = le['id']

        if best_ledger_id and best_score >= 0.0:
            conn.execute("""
                UPDATE bank_transactions
                SET matched_ledger_id=?, match_status='Auto-Matched', updated_at=datetime('now')
                WHERE id=?
            """, [best_ledger_id, bt['id']])
            conn.execute("""
                UPDATE ledger SET status='Cleared', reconciliation_id=?,
                    bank_account_id=?, updated_at=datetime('now')
                WHERE id=?
            """, [None, acct_id, best_ledger_id])
            used_ledger_ids.add(best_ledger_id)
            matched += 1

    return matched


# ════════════════════════════════════════════════════════════════
#  RECONCILIATION HOME — account list
# ════════════════════════════════════════════════════════════════

@phase8.route('/reconciliation')
def reconciliation():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        accounts = conn.execute("""
            SELECT ba.*,
                   (SELECT COUNT(*) FROM bank_transactions
                    WHERE bank_account_id=ba.id AND is_deleted=0) AS txn_count,
                   (SELECT COUNT(*) FROM bank_transactions
                    WHERE bank_account_id=ba.id AND match_status='Unmatched' AND is_deleted=0) AS unmatched_count,
                   (SELECT session_date FROM reconciliation_sessions
                    WHERE bank_account_id=ba.id AND status='Complete' AND is_deleted=0
                    ORDER BY session_date DESC LIMIT 1) AS last_reconciled
            FROM bank_accounts ba
            WHERE ba.is_deleted=0
            ORDER BY ba.account_name
        """).fetchall()

        # Uncleared ledger count (across all accounts)
        uncleared = conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE status != 'Cleared' AND is_deleted=0"
        ).fetchone()[0]

        return render_template('reconciliation.html',
            config=config, badges=badges,
            accounts=[dict(a) for a in accounts],
            uncleared=uncleared,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  ACCOUNT DETAIL — transactions + matching workspace
# ════════════════════════════════════════════════════════════════

@phase8.route('/reconciliation/<int:acct_id>')
def recon_account(acct_id):
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        acct = _acct(acct_id, conn)
        if not acct:
            flash('Bank account not found.', 'error')
            return redirect(url_for('phase8.reconciliation'))

        # Filters
        status_f   = request.args.get('status', 'Unmatched')   # Unmatched / Auto-Matched / Manual-Matched / All
        start_date = request.args.get('start', '')
        end_date   = request.args.get('end',   '')

        where  = ["bt.bank_account_id=?", "bt.is_deleted=0"]
        params = [acct_id]
        if status_f and status_f != 'All':
            where.append("bt.match_status=?"); params.append(status_f)
        if start_date:
            where.append("bt.transaction_date >= ?"); params.append(start_date)
        if end_date:
            where.append("bt.transaction_date <= ?"); params.append(end_date)

        transactions = conn.execute(f"""
            SELECT bt.*,
                   l.entry_date AS ledger_date, l.description AS ledger_desc,
                   l.amount AS ledger_amount, l.category, l.job_code, l.vendor
            FROM bank_transactions bt
            LEFT JOIN ledger l ON bt.matched_ledger_id = l.id
            WHERE {' AND '.join(where)}
            ORDER BY bt.transaction_date DESC, bt.id DESC
        """, params).fetchall()

        # Summary stats for this account
        stats = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN match_status='Unmatched' THEN 1 ELSE 0 END) AS unmatched,
                SUM(CASE WHEN match_status IN ('Auto-Matched','Manual-Matched') THEN 1 ELSE 0 END) AS matched,
                SUM(CASE WHEN match_status='Excluded' THEN 1 ELSE 0 END) AS excluded,
                SUM(CASE WHEN transaction_type='Credit' THEN amount ELSE 0 END) AS total_credits,
                SUM(CASE WHEN transaction_type='Debit'  THEN amount ELSE 0 END) AS total_debits
            FROM bank_transactions
            WHERE bank_account_id=? AND is_deleted=0
        """, [acct_id]).fetchone()

        # Uncleared ledger entries eligible for matching
        uncleared_ledger = conn.execute("""
            SELECT l.*, c.category_name
            FROM ledger l
            LEFT JOIN work_categories c ON l.category = c.category_name
            WHERE (l.bank_account_id=? OR l.bank_account_id IS NULL)
              AND l.status != 'Cleared' AND l.is_deleted=0
            ORDER BY l.entry_date DESC
            LIMIT 200
        """, [acct_id]).fetchall()

        # Active/recent reconciliation sessions
        sessions = conn.execute("""
            SELECT * FROM reconciliation_sessions
            WHERE bank_account_id=? AND is_deleted=0
            ORDER BY session_date DESC LIMIT 10
        """, [acct_id]).fetchall()

        return render_template('recon_account.html',
            config=config, badges=badges, acct=acct,
            transactions=[dict(t) for t in transactions],
            stats=dict(stats),
            uncleared_ledger=[dict(l) for l in uncleared_ledger],
            sessions=[dict(s) for s in sessions],
            status_f=status_f, start_date=start_date, end_date=end_date,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CSV IMPORT
# ════════════════════════════════════════════════════════════════

@phase8.route('/reconciliation/<int:acct_id>/import', methods=['GET', 'POST'])
def recon_import(acct_id):
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        acct = _acct(acct_id, conn)
        if not acct:
            flash('Bank account not found.', 'error')
            return redirect(url_for('phase8.reconciliation'))
    finally:
        conn.close()

    if request.method == 'GET':
        return render_template('recon_import.html',
            config=config, badges=badges, acct=acct)

    # POST — process uploaded CSV
    f = request.files.get('csv_file')
    if not f or not f.filename:
        flash('No file uploaded.', 'error')
        return redirect(request.referrer or url_for('phase8.recon_import', acct_id=acct_id))

    try:
        raw_text = f.read().decode('utf-8-sig')  # handle BOM
    except UnicodeDecodeError:
        try:
            f.seek(0)
            raw_text = f.read().decode('latin-1')
        except Exception:
            flash('Could not decode CSV file. Try saving as UTF-8.', 'error')
            return redirect(url_for('phase8.recon_import', acct_id=acct_id))

    try:
        rows = _parse_bank_csv(raw_text)
    except Exception as e:
        flash(f'CSV parse error: {e}', 'error')
        return redirect(url_for('phase8.recon_import', acct_id=acct_id))

    if not rows:
        flash('No valid transactions found in the CSV.', 'error')
        return redirect(url_for('phase8.recon_import', acct_id=acct_id))

    batch_id = f"import_{acct_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    skipped = inserted = 0

    with db() as conn:
        for row in rows:
            # Deduplicate: same account + date + amount + type + reference
            existing = conn.execute("""
                SELECT id FROM bank_transactions
                WHERE bank_account_id=? AND transaction_date=?
                  AND amount=? AND transaction_type=?
                  AND reference_number=? AND is_deleted=0
            """, [acct_id, row['transaction_date'], row['amount'],
                  row['transaction_type'], row['reference_number']]).fetchone()
            if existing:
                skipped += 1
                continue
            conn.execute("""
                INSERT INTO bank_transactions
                    (bank_account_id, transaction_date, description, amount,
                     transaction_type, reference_number, import_batch_id, match_status, source)
                VALUES (?,?,?,?,?,?,?,'Unmatched','CSV')
            """, [acct_id, row['transaction_date'], row['description'], row['amount'],
                  row['transaction_type'], row['reference_number'], batch_id])
            inserted += 1

        # Auto-match after import
        auto_matched = _run_auto_match(acct_id, conn)

    flash(f'Imported {inserted} transactions ({skipped} duplicates skipped). '
          f'Auto-matched {auto_matched}.', 'success')
    return redirect(url_for('phase8.recon_account', acct_id=acct_id))


# ════════════════════════════════════════════════════════════════
#  MATCH ACTIONS (JSON API)
# ════════════════════════════════════════════════════════════════

@phase8.route('/api/recon/match', methods=['POST'])
def api_match():
    """Manually match a bank transaction to a ledger entry."""
    data       = request.json or {}
    bt_id      = data.get('bank_transaction_id')
    ledger_id  = data.get('ledger_id')
    if not bt_id or not ledger_id:
        return jsonify({'error': 'bank_transaction_id and ledger_id required'}), 400

    with db() as conn:
        bt = conn.execute(
            "SELECT * FROM bank_transactions WHERE id=? AND is_deleted=0", [bt_id]
        ).fetchone()
        le = conn.execute(
            "SELECT * FROM ledger WHERE id=? AND is_deleted=0", [ledger_id]
        ).fetchone()
        if not bt or not le:
            return jsonify({'error': 'Record not found'}), 404

        conn.execute("""
            UPDATE bank_transactions
            SET matched_ledger_id=?, match_status='Manual-Matched', updated_at=datetime('now')
            WHERE id=?
        """, [ledger_id, bt_id])
        conn.execute("""
            UPDATE ledger SET status='Cleared',
                bank_account_id=?, updated_at=datetime('now')
            WHERE id=?
        """, [bt['bank_account_id'], ledger_id])
        log_action(conn, 'bank_transactions', bt_id, 'UPDATE',
                   new_data={'match_status': 'Manual-Matched', 'matched_ledger_id': ledger_id})
    return jsonify({'success': True})


@phase8.route('/api/recon/unmatch', methods=['POST'])
def api_unmatch():
    """Remove a match, set transaction back to Unmatched."""
    data  = request.json or {}
    bt_id = data.get('bank_transaction_id')
    if not bt_id:
        return jsonify({'error': 'bank_transaction_id required'}), 400

    with db() as conn:
        bt = conn.execute(
            "SELECT * FROM bank_transactions WHERE id=? AND is_deleted=0", [bt_id]
        ).fetchone()
        if not bt:
            return jsonify({'error': 'Not found'}), 404
        old_ledger_id = bt['matched_ledger_id']
        conn.execute("""
            UPDATE bank_transactions
            SET matched_ledger_id=NULL, match_status='Unmatched', updated_at=datetime('now')
            WHERE id=?
        """, [bt_id])
        if old_ledger_id:
            conn.execute("""
                UPDATE ledger SET status='Pending', reconciliation_id=NULL, updated_at=datetime('now') WHERE id=?
            """, [old_ledger_id])
        log_action(conn, 'bank_transactions', bt_id, 'UPDATE',
                   new_data={'match_status': 'Unmatched'})
    return jsonify({'success': True})


@phase8.route('/api/recon/exclude', methods=['POST'])
def api_exclude():
    """Mark a bank transaction as excluded (bank fee, transfer, etc.)."""
    data  = request.json or {}
    bt_id = data.get('bank_transaction_id')
    notes = data.get('notes', 'Excluded')
    if not bt_id:
        return jsonify({'error': 'bank_transaction_id required'}), 400

    with db() as conn:
        conn.execute("""
            UPDATE bank_transactions
            SET match_status='Excluded', matched_ledger_id=NULL, notes=?,
                updated_at=datetime('now')
            WHERE id=?
        """, [notes, bt_id])
    return jsonify({'success': True})


@phase8.route('/api/recon/auto-match', methods=['POST'])
def api_auto_match():
    """Re-run auto-matching for an account."""
    data    = request.json or {}
    acct_id = data.get('account_id')
    if not acct_id:
        return jsonify({'error': 'account_id required'}), 400
    with db() as conn:
        count = _run_auto_match(int(acct_id), conn)
    return jsonify({'success': True, 'matched': count})


@phase8.route('/api/recon/create-ledger', methods=['POST'])
def api_create_ledger():
    """Create a new ledger entry from a bank transaction (for unmatched items)."""
    data      = request.json or {}
    bt_id     = data.get('bank_transaction_id')
    category  = (data.get('category') or '').strip()
    job_code  = (data.get('job_code') or '').strip()
    vendor    = (data.get('vendor') or '').strip()
    notes     = (data.get('notes') or '').strip()
    if not bt_id:
        return jsonify({'error': 'bank_transaction_id required'}), 400

    with db() as conn:
        bt = conn.execute(
            "SELECT * FROM bank_transactions WHERE id=? AND is_deleted=0", [bt_id]
        ).fetchone()
        if not bt:
            return jsonify({'error': 'Not found'}), 404

        bt = dict(bt)
        # Create ledger entry
        cur = conn.execute("""
            INSERT INTO ledger
                (entry_date, description, amount, status, category, job_code, vendor,
                 bank_account_id, notes)
            VALUES (?,?,?,'Cleared',?,?,?,?,?)
        """, [bt['transaction_date'], bt['description'], bt['amount'],
              category, job_code, vendor, bt['bank_account_id'], notes])
        new_ledger_id = cur.lastrowid

        # Link bank transaction to new ledger entry
        conn.execute("""
            UPDATE bank_transactions
            SET matched_ledger_id=?, match_status='Manual-Matched', updated_at=datetime('now')
            WHERE id=?
        """, [new_ledger_id, bt_id])
        log_action(conn, 'ledger', new_ledger_id, 'INSERT',
                   new_data={'from_bank_import': True, 'bt_id': bt_id})

    return jsonify({'success': True, 'ledger_id': new_ledger_id})


# ════════════════════════════════════════════════════════════════
#  RECONCILIATION SESSION
# ════════════════════════════════════════════════════════════════

@phase8.route('/reconciliation/<int:acct_id>/session', methods=['GET', 'POST'])
def recon_session(acct_id):
    """Start or view a reconciliation session."""
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        acct = _acct(acct_id, conn)
        if not acct:
            flash('Bank account not found.', 'error')
            return redirect(url_for('phase8.reconciliation'))

        if request.method == 'POST':
            stmt_end    = (request.form.get('statement_end_date') or '').strip()
            stmt_balance_raw = (request.form.get('statement_ending_balance') or '0').replace(',','').replace('$','')
            notes       = (request.form.get('notes') or '').strip()
            try:
                stmt_balance = float(stmt_balance_raw)
            except ValueError:
                flash('Invalid balance.', 'error')
                return redirect(request.referrer)

            with db() as conn2:
                cur = conn2.execute("""
                    INSERT INTO reconciliation_sessions
                        (bank_account_id, session_date, statement_end_date,
                         statement_ending_balance, status, notes)
                    VALUES (?,date('now'),?,?,'In-Progress',?)
                """, [acct_id, stmt_end, stmt_balance, notes])
                sess_id = cur.lastrowid
            return redirect(url_for('phase8.recon_session_detail', acct_id=acct_id, sess_id=sess_id))

        # GET — list sessions with enriched stats
        sessions_raw = conn.execute("""
            SELECT rs.*,
                   (SELECT COUNT(*) FROM bank_transactions bt
                    WHERE bt.bank_account_id = rs.bank_account_id
                      AND bt.transaction_date <= rs.statement_end_date
                      AND bt.is_deleted=0) AS tx_count,
                   (SELECT COUNT(*) FROM bank_transactions bt
                    WHERE bt.bank_account_id = rs.bank_account_id
                      AND bt.transaction_date <= rs.statement_end_date
                      AND bt.match_status IN ('Auto-Matched','Manual-Matched')
                      AND bt.is_deleted=0) AS tx_matched
            FROM reconciliation_sessions rs
            WHERE rs.bank_account_id=? AND rs.is_deleted=0
            ORDER BY rs.session_date DESC
        """, [acct_id]).fetchall()

        sessions = []
        for s in sessions_raw:
            sd = dict(s)
            # Match pct
            cnt = sd.get('tx_count') or 0
            matched = sd.get('tx_matched') or 0
            sd['match_pct'] = round(matched / cnt * 100) if cnt else None
            sessions.append(sd)

        # Running balance trend for sparkline
        balance_history = [
            {'date': s['statement_end_date'] or s['session_date'],
             'balance': s['statement_ending_balance'],
             'status': s['status']}
            for s in sessions if s.get('statement_ending_balance')
        ]
        balance_history.reverse()  # chronological

        return render_template('recon_session.html',
            config=config, badges=badges, acct=acct,
            sessions=sessions,
            balance_history=balance_history,
            today=date.today().strftime('%Y-%m-%d'),
        )
    finally:
        conn.close()


@phase8.route('/reconciliation/<int:acct_id>/session/<int:sess_id>')
def recon_session_detail(acct_id, sess_id):
    """Reconciliation session detail — balance check, complete button."""
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        acct = _acct(acct_id, conn)
        sess = conn.execute(
            "SELECT * FROM reconciliation_sessions WHERE id=? AND is_deleted=0", [sess_id]
        ).fetchone()
        if not acct or not sess:
            flash('Session not found.', 'error')
            return redirect(url_for('phase8.recon_account', acct_id=acct_id))

        sess = dict(sess)
        stmt_end = sess['statement_end_date']

        # Cleared bank transactions up to statement end date
        cleared_txns = conn.execute("""
            SELECT * FROM bank_transactions
            WHERE bank_account_id=? AND is_deleted=0
              AND match_status IN ('Auto-Matched','Manual-Matched')
              AND (? = '' OR transaction_date <= ?)
            ORDER BY transaction_date
        """, [acct_id, stmt_end, stmt_end]).fetchall()

        # Outstanding (unmatched) up to statement end date
        outstanding_txns = conn.execute("""
            SELECT * FROM bank_transactions
            WHERE bank_account_id=? AND is_deleted=0
              AND match_status='Unmatched'
              AND (? = '' OR transaction_date <= ?)
            ORDER BY transaction_date
        """, [acct_id, stmt_end, stmt_end]).fetchall()

        # Compute balance
        cleared_credits = sum(t['amount'] for t in cleared_txns if t['transaction_type'] == 'Credit')
        cleared_debits  = sum(t['amount'] for t in cleared_txns if t['transaction_type'] == 'Debit')
        cleared_net     = round(cleared_credits - cleared_debits, 2)

        opening_balance = acct.get('current_balance', 0) or 0
        computed_balance = round(float(opening_balance) + cleared_net, 2)
        difference       = round(computed_balance - float(sess['statement_ending_balance']), 2)

        outstanding_credits = sum(t['amount'] for t in outstanding_txns if t['transaction_type'] == 'Credit')
        outstanding_debits  = sum(t['amount'] for t in outstanding_txns if t['transaction_type'] == 'Debit')

        return render_template('recon_session_detail.html',
            config=config, badges=badges, acct=acct, sess=sess,
            cleared_txns=[dict(t) for t in cleared_txns],
            outstanding_txns=[dict(t) for t in outstanding_txns],
            cleared_credits=cleared_credits, cleared_debits=cleared_debits,
            cleared_net=cleared_net, computed_balance=computed_balance,
            difference=difference,
            outstanding_credits=outstanding_credits, outstanding_debits=outstanding_debits,
            sess_id=sess_id,
        )
    finally:
        conn.close()


@phase8.route('/api/recon/session/<int:sess_id>/complete', methods=['POST'])
def api_session_complete(sess_id):
    """Mark a reconciliation session as complete, lock cleared entries."""
    data     = request.json or {}
    comp_by  = (data.get('completed_by') or 'Owner').strip()

    with db() as conn:
        sess = conn.execute(
            "SELECT * FROM reconciliation_sessions WHERE id=? AND is_deleted=0", [sess_id]
        ).fetchone()
        if not sess:
            return jsonify({'error': 'Session not found'}), 404

        # Compute final difference
        acct_id  = sess['bank_account_id']
        stmt_end = sess['statement_end_date']

        cleared_txns = conn.execute("""
            SELECT * FROM bank_transactions
            WHERE bank_account_id=? AND is_deleted=0
              AND match_status IN ('Auto-Matched','Manual-Matched')
              AND (? = '' OR transaction_date <= ?)
        """, [acct_id, stmt_end, stmt_end]).fetchall()

        acct = conn.execute("SELECT * FROM bank_accounts WHERE id=?", [acct_id]).fetchone()
        opening = float(acct['current_balance'] or 0)
        cleared_net = sum(
            t['amount'] if t['transaction_type'] == 'Credit' else -t['amount']
            for t in cleared_txns
        )
        computed = round(opening + cleared_net, 2)
        difference = round(computed - float(sess['statement_ending_balance']), 2)

        # Stamp session as complete
        conn.execute("""
            UPDATE reconciliation_sessions SET
                status='Complete', computed_balance=?, difference=?,
                completed_by=?, updated_at=datetime('now')
            WHERE id=?
        """, [computed, difference, comp_by, sess_id])

        # Stamp reconciliation_id on cleared ledger entries
        for t in cleared_txns:
            if t['matched_ledger_id']:
                conn.execute("""
                    UPDATE ledger SET reconciliation_id=?, updated_at=datetime('now')
                    WHERE id=?
                """, [sess_id, t['matched_ledger_id']])

        log_action(conn, 'reconciliation_sessions', sess_id, 'UPDATE',
                   new_data={'status': 'Complete', 'difference': difference})

    return jsonify({'success': True, 'difference': difference, 'computed_balance': computed})


# ════════════════════════════════════════════════════════════════
#  LEDGER SEARCH API  (for manual match picker)
# ════════════════════════════════════════════════════════════════

@phase8.route('/api/recon/ledger-search')
def api_ledger_search():
    """Search uncleared ledger entries for manual match."""
    q       = request.args.get('q', '').strip()
    acct_id = request.args.get('acct_id', '')
    amount  = request.args.get('amount', '')
    limit   = int(request.args.get('limit', 20))

    conn = get_connection()
    try:
        where  = ["l.status != 'Cleared'", "l.is_deleted=0"]
        params = []
        if acct_id:
            where.append("(l.bank_account_id=? OR l.bank_account_id IS NULL)")
            params.append(int(acct_id))
        if q:
            where.append("(l.description LIKE ? OR l.vendor LIKE ? OR l.job_code LIKE ?)")
            params += [f'%{q}%', f'%{q}%', f'%{q}%']
        if amount:
            try:
                amt = float(amount)
                where.append("ABS(l.amount - ?) < 0.02")
                params.append(amt)
            except ValueError:
                pass

        rows = conn.execute(f"""
            SELECT l.id, l.entry_date, l.description, l.amount, l.category,
                   l.job_code, l.vendor, l.status
            FROM ledger l
            WHERE {' AND '.join(where)}
            ORDER BY l.entry_date DESC
            LIMIT ?
        """, params + [limit]).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  BANK TRANSACTION DELETE
# ════════════════════════════════════════════════════════════════

@phase8.route('/api/recon/transaction/<int:bt_id>/delete', methods=['POST'])
def api_txn_delete(bt_id):
    with db() as conn:
        bt = conn.execute(
            "SELECT matched_ledger_id FROM bank_transactions WHERE id=? AND is_deleted=0",
            [bt_id]
        ).fetchone()
        if bt and bt['matched_ledger_id']:
            conn.execute("""
                UPDATE ledger SET status='Pending', updated_at=datetime('now')
                WHERE id=?
            """, [bt['matched_ledger_id']])
        soft_delete(conn, 'bank_transactions', bt_id)
    return jsonify({'success': True})


# ════════════════════════════════════════════════════════════════
#  EXPORT  — unmatched transactions
# ════════════════════════════════════════════════════════════════

@phase8.route('/reconciliation/<int:acct_id>/export')
def recon_export(acct_id):
    status_f = request.args.get('status', 'Unmatched')
    conn     = get_connection()
    try:
        where  = ["bank_account_id=?", "is_deleted=0"]
        params = [acct_id]
        if status_f and status_f != 'All':
            where.append("match_status=?"); params.append(status_f)

        rows = conn.execute(f"""
            SELECT * FROM bank_transactions
            WHERE {' AND '.join(where)}
            ORDER BY transaction_date DESC
        """, params).fetchall()

        COLS = ['transaction_date','description','amount','transaction_type',
                'reference_number','match_status','notes']

        def generate():
            yield ','.join(COLS) + '\n'
            for r in rows:
                d = dict(r)
                yield ','.join(
                    f'"{str(d.get(c,"")).replace(chr(34),chr(34)*2)}"' for c in COLS
                ) + '\n'

        fname = f"bank_txns_{acct_id}_{status_f}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        return Response(
            stream_with_context(generate()),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={fname}'}
        )
    finally:
        conn.close()
