"""
Phase 10 — Reports & Analytics

Reports available:
  1. Reports Hub          /reports                — card grid of all reports
  2. P&L Summary          /reports/pl             — income vs expenses by month/year
  3. Job Profitability    /reports/jobs           — revenue, cost, margin per job
  4. Cash Flow            /reports/cashflow       — monthly inflows vs outflows
  5. Payroll Tax Ledger   /reports/payroll-tax    — quarterly 941 / FUTA / SUTA liabilities
  6. Estimate Win Rate    /reports/estimates      — bid vs win analysis
  7. Monthly Snapshot     /reports/snapshot       — one-page summary for any month
  8. Category Breakdown   /reports/categories     — spending by category (ledger)
  9. Accounts Receivable  /reports/ar             — invoice aging summary
 10. Print any report     /reports/<name>/print   — printer-friendly version
"""
from datetime import date, datetime, timedelta
from collections import defaultdict
from flask import (Blueprint, render_template, request, jsonify,
                   redirect, url_for, Response, stream_with_context)

from database import get_connection

phase10 = Blueprint('phase10', __name__)

MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

# Canonical signed-amount expression for the ledger table.
# income rows:  income=positive, expense=NULL  → positive (revenue)
# expense rows: expense=positive, income=NULL  → negated → negative (cost)
# legacy rows:  both NULL → falls back to legacy `amount` column
_AMT = "COALESCE(l.income, CASE WHEN l.expense IS NOT NULL THEN -l.expense ELSE l.amount END, 0)"
# Same but without table alias (for queries that don't alias ledger as 'l')
_AMT_RAW = "COALESCE(income, CASE WHEN expense IS NOT NULL THEN -expense ELSE amount END, 0)"

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

def _badges():
    from app import get_nav_badges
    return get_nav_badges()

def _year_param(default=None):
    v = request.args.get('year', str(default or date.today().year))
    if v == 'all':
        return 'all'
    return int(v) if v and v.isdigit() else date.today().year

def _year_where(year, col='entry_date', alias=None):
    """Return (where_fragment, params) for a year filter.
    year='all' → no date restriction.
    Returns e.g. ("substr(l.entry_date,1,4)=?", ["2024"])
    """
    full_col = f"{alias}.{col}" if alias else col
    if year == 'all':
        return ('1=1', [])
    return (f"substr({full_col},1,4)=?", [str(year)])

def _month_param(default=None):
    return int(request.args.get('month', default or date.today().month))

def _available_years(conn, table='ledger', date_col='entry_date'):
    rows = conn.execute(f"""
        SELECT DISTINCT substr({date_col},1,4) AS yr
        FROM {table} WHERE is_deleted=0 AND {date_col}!=''
        ORDER BY yr DESC
    """).fetchall()
    return [int(r['yr']) for r in rows if r['yr'] and r['yr'].isdigit()]

def _fmt(v):
    """Format float as currency string."""
    return f"${v:,.2f}"

def _pct(num, den):
    if not den:
        return 0.0
    return round(num / den * 100, 1)


# ════════════════════════════════════════════════════════════════
#  REPORTS HUB
# ════════════════════════════════════════════════════════════════

@phase10.route('/reports')
def reports():
    config = _cfg()
    badges = _badges()
    year   = date.today().year
    month  = date.today().month

    conn = get_connection()
    try:
        # Quick stats for hub cards
        ytd_revenue = conn.execute("""
            SELECT COALESCE(SUM(amount),0) FROM invoices
            WHERE substr(invoice_date,1,4)=? AND status IN ('Paid','Partial') AND is_deleted=0
        """, [str(year)]).fetchone()[0]

        ytd_expenses = conn.execute(f"""
            SELECT COALESCE(SUM(CASE WHEN {_AMT} < 0 THEN ABS({_AMT}) ELSE 0 END), 0)
            FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE substr(l.entry_date,1,4)=? AND l.is_deleted=0
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
        """, [str(year)]).fetchone()[0]

        ytd_payroll = conn.execute("""
            SELECT COALESCE(SUM(gross_pay),0) FROM payroll_runs
            WHERE year=? AND is_deleted=0
        """, [year]).fetchone()[0]

        active_jobs = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE status='Active' AND is_deleted=0"
        ).fetchone()[0]

        overdue_inv = conn.execute("""
            SELECT COUNT(*), COALESCE(SUM(balance_due),0) FROM invoices
            WHERE status='Overdue' AND is_deleted=0
        """).fetchone()

        open_estimates = conn.execute(
            "SELECT COUNT(*) FROM job_estimates WHERE status IN ('Draft','Sent') AND is_deleted=0"
        ).fetchone()[0]

        return render_template('reports_hub.html',
            config=config, badges=badges,
            year=year, month=month,
            ytd_revenue=ytd_revenue,
            ytd_expenses=ytd_expenses,
            ytd_payroll=ytd_payroll,
            ytd_profit=ytd_revenue - ytd_expenses,
            active_jobs=active_jobs,
            overdue_inv_count=overdue_inv[0],
            overdue_inv_amount=overdue_inv[1],
            open_estimates=open_estimates,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  P&L SUMMARY
# ════════════════════════════════════════════════════════════════

@phase10.route('/reports/pl')
def report_pl():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        year    = _year_param()
        years   = _available_years(conn, 'ledger', 'entry_date') or [date.today().year]
        if year != 'all' and year not in years: years = [year] + years

        yw, yp = _year_where(year, 'entry_date')  # for non-aliased queries
        ywl, ypl = _year_where(year, 'entry_date', 'l')  # aliased as l

        INCOME_CATS = ("'Income Received','ACCOUNT CREDIT'")

        if year == 'all':
            # ── Group by YEAR ─────────────────────────────────
            grp = "substr(entry_date,1,4)"
            rev_rows = conn.execute(f"""
                SELECT {grp} AS lbl, SUM({_AMT_RAW}) AS total
                FROM ledger WHERE is_deleted=0 AND {_AMT_RAW} > 0
                  AND COALESCE(l.is_deleted,0)=0
                GROUP BY lbl ORDER BY lbl
            """.replace('l.is_deleted', 'is_deleted')).fetchall()
            # Simpler: just query without alias
            rev_rows = conn.execute(f"""
                SELECT substr(entry_date,1,4) AS lbl, SUM({_AMT_RAW}) AS total
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE l.is_deleted=0 AND {_AMT_RAW} > 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY lbl ORDER BY lbl
            """).fetchall()
            exp_rows = conn.execute(f"""
                SELECT substr(l.entry_date,1,4) AS lbl, SUM(ABS({_AMT})) AS total
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE l.is_deleted=0 AND {_AMT} < 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY lbl ORDER BY lbl
            """).fetchall()
            pay_rows = conn.execute("""
                SELECT CAST(year AS TEXT) AS lbl, SUM(gross_pay) AS total
                FROM payroll_runs WHERE is_deleted=0
                GROUP BY lbl ORDER BY lbl
            """).fetchall()

            all_labels = sorted(set(
                [r['lbl'] for r in rev_rows] +
                [r['lbl'] for r in exp_rows] +
                [r['lbl'] for r in pay_rows]
            ))
            rev_by = {r['lbl']: float(r['total']) for r in rev_rows}
            exp_by = {r['lbl']: float(r['total']) for r in exp_rows}
            pay_by = {r['lbl']: float(r['total']) for r in pay_rows}

            months_data = []
            ytd_rev = ytd_exp = ytd_pay = 0.0
            for lbl in all_labels:
                rev  = rev_by.get(lbl, 0.0)
                exp  = exp_by.get(lbl, 0.0)
                pay  = pay_by.get(lbl, 0.0)
                total_cost = exp + pay
                profit = rev - total_cost
                margin = _pct(profit, rev)
                ytd_rev += rev; ytd_exp += exp; ytd_pay += pay
                months_data.append({
                    'month': lbl, 'label': lbl,
                    'revenue': rev, 'expenses': exp, 'payroll': pay,
                    'total_cost': total_cost, 'profit': profit, 'margin': margin,
                    'future': False,
                })
        else:
            # ── Group by MONTH (single year) ──────────────────
            rev_rows = conn.execute(f"""
                SELECT CAST(substr(entry_date,6,2) AS INTEGER) AS mo,
                       SUM({_AMT_RAW}) AS total
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE {ywl} AND l.is_deleted=0 AND {_AMT_RAW} > 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY mo ORDER BY mo
            """, ypl).fetchall()
            revenue_by_month = {r['mo']: float(r['total']) for r in rev_rows}

            if not revenue_by_month:
                inv_yw, inv_yp = _year_where(year, 'invoice_date')
                rev_rows2 = conn.execute(f"""
                    SELECT CAST(substr(invoice_date,6,2) AS INTEGER) AS mo,
                           SUM(amount_paid) AS total
                    FROM invoices
                    WHERE {inv_yw} AND status IN ('Paid','Partial') AND is_deleted=0
                    GROUP BY mo ORDER BY mo
                """, inv_yp).fetchall()
                revenue_by_month = {r['mo']: float(r['total']) for r in rev_rows2}

            exp_rows = conn.execute(f"""
                SELECT CAST(substr(l.entry_date,6,2) AS INTEGER) AS mo,
                       SUM(ABS({_AMT})) AS total
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE {ywl} AND l.is_deleted=0
                  AND {_AMT} < 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY mo ORDER BY mo
            """, ypl).fetchall()
            expenses_by_month = {r['mo']: float(r['total']) for r in exp_rows}

            pay_yw, pay_yp = _year_where(year, 'run_date')
            pay_rows = conn.execute(f"""
                SELECT CAST(substr(run_date,6,2) AS INTEGER) AS mo,
                       SUM(gross_pay) AS total
                FROM payroll_runs
                WHERE {pay_yw} AND is_deleted=0
                GROUP BY mo ORDER BY mo
            """, pay_yp).fetchall()
            payroll_by_month = {r['mo']: float(r['total']) for r in pay_rows}

            months_data = []
            ytd_rev = ytd_exp = ytd_pay = 0.0
            for m in range(1, 13):
                rev  = revenue_by_month.get(m, 0.0)
                exp  = expenses_by_month.get(m, 0.0)
                pay  = payroll_by_month.get(m, 0.0)
                total_cost = exp + pay
                profit = rev - total_cost
                margin = _pct(profit, rev)
                ytd_rev += rev; ytd_exp += exp; ytd_pay += pay
                months_data.append({
                    'month': m, 'label': MONTHS[m-1],
                    'revenue': rev, 'expenses': exp, 'payroll': pay,
                    'total_cost': total_cost, 'profit': profit, 'margin': margin,
                    'future': m > date.today().month and year == date.today().year,
                })

        ytd_total_cost = ytd_exp + ytd_pay
        ytd_profit     = ytd_rev - ytd_total_cost
        ytd_margin     = _pct(ytd_profit, ytd_rev)

        # Expense breakdown by category (YTD, excluding income/transfer)
        cat_rows = conn.execute(f"""
            SELECT l.category, SUM(ABS({_AMT})) AS total
            FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE {ywl} AND l.is_deleted=0
              AND {_AMT} < 0
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
            GROUP BY l.category ORDER BY total DESC LIMIT 20
        """, ypl).fetchall()

        return render_template('report_pl.html',
            config=config, badges=badges,
            year=year, years=years,
            months_data=months_data,
            ytd_rev=ytd_rev, ytd_exp=ytd_exp, ytd_pay=ytd_pay,
            ytd_total_cost=ytd_total_cost, ytd_profit=ytd_profit, ytd_margin=ytd_margin,
            categories=[dict(r) for r in cat_rows],
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  JOB PROFITABILITY
# ════════════════════════════════════════════════════════════════

@phase10.route('/reports/jobs')
def report_jobs():
    config  = _cfg()
    badges  = _badges()
    conn    = get_connection()
    try:
        year    = _year_param()
        years   = _available_years(conn, 'ledger', 'entry_date') or [year]
        if year not in years: years = [year] + years
        status_f = request.args.get('status', '')

        where  = ["j.is_deleted=0"]
        params = []
        if status_f:
            where.append("j.status=?"); params.append(status_f)

        jobs = conn.execute(f"""
            SELECT j.id, j.job_code, j.description, j.status, j.contract_amount,
                   c.full_name AS client_name
            FROM jobs j
            LEFT JOIN clients c ON j.client_id = c.id
            WHERE {' AND '.join(where)}
            ORDER BY j.job_code
        """, params).fetchall()

        job_data = []
        for j in jobs:
            jc = j['job_code']
            # Revenue: paid invoices for this job
            revenue = conn.execute("""
                SELECT COALESCE(SUM(amount_paid),0) FROM invoices
                WHERE job_code=? AND status IN ('Paid','Partial') AND is_deleted=0
            """, [jc]).fetchone()[0]

            # COGS from ledger
            cogs = conn.execute(f"""
                SELECT COALESCE(SUM(ABS({_AMT})),0) FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE l.job_code=? AND l.is_cogs=1 AND l.is_deleted=0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
            """, [jc]).fetchone()[0]

            # Labor cost from timesheet
            labor = conn.execute("""
                SELECT COALESCE(SUM(cost_amount),0) FROM timesheet
                WHERE job_code=? AND is_deleted=0
            """, [jc]).fetchone()[0]

            # Estimated total (latest accepted/sent estimate)
            est = conn.execute("""
                SELECT total_estimate FROM job_estimates
                WHERE job_id=? AND is_deleted=0
                ORDER BY version_number DESC LIMIT 1
            """, [j['id']]).fetchone()
            estimated = float(est['total_estimate']) if est else 0.0

            total_cost = float(cogs) + float(labor)
            profit     = float(revenue) - total_cost
            margin     = _pct(profit, float(revenue))
            variance   = estimated - float(revenue) if estimated else None

            job_data.append({
                'job_code':    jc,
                'description': j['description'],
                'status':      j['status'],
                'client':      j['client_name'],
                'contract':    float(j['contract_amount'] or 0),
                'estimated':   estimated,
                'revenue':     float(revenue),
                'cogs':        float(cogs),
                'labor':       float(labor),
                'total_cost':  total_cost,
                'profit':      profit,
                'margin':      margin,
                'variance':    variance,
            })

        # Sort by profit desc
        job_data.sort(key=lambda r: r['profit'], reverse=True)

        total_rev    = sum(j['revenue'] for j in job_data)
        total_cost   = sum(j['total_cost'] for j in job_data)
        total_profit = total_rev - total_cost
        total_margin = _pct(total_profit, total_rev)

        return render_template('report_jobs.html',
            config=config, badges=badges,
            year=year, years=years, status_f=status_f,
            jobs=job_data,
            total_rev=total_rev, total_cost=total_cost,
            total_profit=total_profit, total_margin=total_margin,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CASH FLOW
# ════════════════════════════════════════════════════════════════

@phase10.route('/reports/cashflow')
def report_cashflow():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        year  = _year_param()
        years = _available_years(conn, 'ledger', 'entry_date') or [date.today().year]

        yw, yp   = _year_where(year, 'entry_date')
        ywl, ypl = _year_where(year, 'entry_date', 'l')

        if year == 'all':
            # Group by year
            in_rows = conn.execute(f"""
                SELECT substr(l.entry_date,1,4) AS lbl, SUM({_AMT_RAW}) AS total
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE l.is_deleted=0 AND {_AMT_RAW} > 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY lbl ORDER BY lbl
            """.replace('l.is_deleted', 'is_deleted')).fetchall()
            in_rows = conn.execute(f"""
                SELECT substr(l.entry_date,1,4) AS lbl, SUM({_AMT_RAW}) AS total
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE l.is_deleted=0 AND {_AMT_RAW} > 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY lbl ORDER BY lbl
            """).fetchall()
            out_rows = conn.execute(f"""
                SELECT substr(l.entry_date,1,4) AS lbl, SUM(ABS({_AMT})) AS total
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE l.is_deleted=0 AND {_AMT} < 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY lbl ORDER BY lbl
            """).fetchall()
            pay_rows = conn.execute("""
                SELECT CAST(year AS TEXT) AS lbl, SUM(net_pay) AS total
                FROM payroll_runs WHERE is_deleted=0
                GROUP BY lbl ORDER BY lbl
            """).fetchall()

            all_labels = sorted(set(
                [r['lbl'] for r in in_rows] +
                [r['lbl'] for r in out_rows] +
                [r['lbl'] for r in pay_rows]
            ))
            in_by  = {r['lbl']: float(r['total']) for r in in_rows}
            out_by = {r['lbl']: float(r['total']) for r in out_rows}
            pay_by = {r['lbl']: float(r['total']) for r in pay_rows}

            months_data = []
            running_balance = 0.0
            for lbl in all_labels:
                inflow  = in_by.get(lbl, 0.0)
                outflow = out_by.get(lbl, 0.0) + pay_by.get(lbl, 0.0)
                net     = inflow - outflow
                running_balance += net
                months_data.append({
                    'month': lbl, 'label': lbl,
                    'inflow': inflow, 'outflow': outflow,
                    'net': net, 'running': running_balance,
                    'future': False,
                })
        else:
            # Group by month — use income column as inflow signal
            in_rows = conn.execute(f"""
                SELECT CAST(substr(l.entry_date,6,2) AS INTEGER) AS mo,
                       SUM({_AMT_RAW}) AS total
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE {ywl} AND l.is_deleted=0 AND {_AMT_RAW} > 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY mo
            """, ypl).fetchall()
            inflows = {r['mo']: float(r['total']) for r in in_rows}

            # If no ledger income at all, fall back to invoice payments
            if not inflows:
                inv_yw, inv_yp = _year_where(year, 'invoice_date')
                in_rows2 = conn.execute(f"""
                    SELECT CAST(substr(invoice_date,6,2) AS INTEGER) AS mo,
                           SUM(amount_paid) AS total
                    FROM invoices
                    WHERE {inv_yw} AND amount_paid > 0 AND is_deleted=0
                    GROUP BY mo
                """, inv_yp).fetchall()
                inflows = {r['mo']: float(r['total']) for r in in_rows2}

            out_rows = conn.execute(f"""
                SELECT CAST(substr(l.entry_date,6,2) AS INTEGER) AS mo,
                       SUM(ABS({_AMT})) AS total
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE {ywl} AND l.is_deleted=0
                  AND {_AMT} < 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY mo
            """, ypl).fetchall()
            outflows_ledger = {r['mo']: float(r['total']) for r in out_rows}

            pay_yw, pay_yp = _year_where(year, 'run_date')
            pay_rows = conn.execute(f"""
                SELECT CAST(substr(run_date,6,2) AS INTEGER) AS mo,
                       SUM(net_pay) AS total
                FROM payroll_runs WHERE {pay_yw} AND is_deleted=0
                GROUP BY mo
            """, pay_yp).fetchall()
            outflows_payroll = {r['mo']: float(r['total']) for r in pay_rows}

            months_data = []
            running_balance = 0.0
            for m in range(1, 13):
                inflow  = inflows.get(m, 0.0)
                outflow = outflows_ledger.get(m, 0.0) + outflows_payroll.get(m, 0.0)
                net     = inflow - outflow
                running_balance += net
                months_data.append({
                    'month': m, 'label': MONTHS[m-1],
                    'inflow': inflow, 'outflow': outflow,
                    'net': net, 'running': running_balance,
                    'future': m > date.today().month and year == date.today().year,
                })

        total_in  = sum(r['inflow']  for r in months_data)
        total_out = sum(r['outflow'] for r in months_data)

        return render_template('report_cashflow.html',
            config=config, badges=badges,
            year=year, years=years,
            months_data=months_data,
            total_in=total_in, total_out=total_out,
            net_cashflow=total_in - total_out,
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  PAYROLL TAX LEDGER
# ════════════════════════════════════════════════════════════════

@phase10.route('/reports/payroll-tax')
def report_payroll_tax():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        year  = _year_param()
        years = conn.execute(
            "SELECT DISTINCT year FROM payroll_runs WHERE is_deleted=0 ORDER BY year DESC"
        ).fetchall()
        years = [r['year'] for r in years] or [year]

        # Monthly totals
        monthly = conn.execute("""
            SELECT CAST(substr(run_date,6,2) AS INTEGER) AS mo,
                   SUM(gross_pay)          AS gross,
                   SUM(ss_withheld)        AS ee_ss,
                   SUM(medicare_withheld)  AS ee_med,
                   SUM(fed_withholding)    AS fed_wh,
                   SUM(state_withholding)  AS state_wh,
                   SUM(total_withheld)     AS total_wh,
                   SUM(net_pay)            AS net,
                   SUM(employer_ss)        AS er_ss,
                   SUM(employer_medicare)  AS er_med,
                   SUM(futa_amount)        AS futa,
                   SUM(suta_amount)        AS suta
            FROM payroll_runs
            WHERE year=? AND is_deleted=0
            GROUP BY mo ORDER BY mo
        """, [year]).fetchall()
        monthly_data = {r['mo']: dict(r) for r in monthly}

        # Quarterly rollup
        quarters = []
        for q, months_in_q in enumerate([(1,2,3),(4,5,6),(7,8,9),(10,11,12)], 1):
            agg = defaultdict(float)
            for m in months_in_q:
                md = monthly_data.get(m, {})
                for k in ['gross','ee_ss','ee_med','fed_wh','state_wh','total_wh',
                          'net','er_ss','er_med','futa','suta']:
                    agg[k] += float(md.get(k) or 0)
            agg['quarter'] = q
            # 941 liability = employee withholding + employer match
            agg['liability_941'] = (
                agg['ee_ss'] + agg['ee_med'] + agg['fed_wh'] +
                agg['er_ss'] + agg['er_med']
            )
            quarters.append(dict(agg))

        # YTD totals
        ytd = conn.execute("""
            SELECT SUM(gross_pay) AS gross, SUM(ss_withheld) AS ee_ss,
                   SUM(medicare_withheld) AS ee_med, SUM(fed_withholding) AS fed_wh,
                   SUM(state_withholding) AS state_wh, SUM(total_withheld) AS total_wh,
                   SUM(net_pay) AS net, SUM(employer_ss) AS er_ss,
                   SUM(employer_medicare) AS er_med, SUM(futa_amount) AS futa,
                   SUM(suta_amount) AS suta
            FROM payroll_runs WHERE year=? AND is_deleted=0
        """, [year]).fetchone()
        ytd_data = dict(ytd) if ytd else {}

        # Per-employee YTD
        emp_rows = conn.execute("""
            SELECT (e.first_name || ' ' || e.last_name) AS emp_name,
                   SUM(p.gross_pay) AS gross, SUM(p.net_pay) AS net,
                   SUM(p.ss_withheld+p.employer_ss) AS total_ss,
                   SUM(p.medicare_withheld+p.employer_medicare) AS total_med,
                   SUM(p.futa_amount) AS futa,
                   COUNT(*) AS checks
            FROM payroll_runs p
            JOIN employees e ON p.emp_id = e.emp_id
            WHERE p.year=? AND p.is_deleted=0
            GROUP BY p.emp_id ORDER BY gross DESC
        """, [year]).fetchall()

        return render_template('report_payroll_tax.html',
            config=config, badges=badges,
            year=year, years=years,
            monthly_data=monthly_data,
            quarters=quarters,
            ytd=ytd_data,
            employees=[dict(e) for e in emp_rows],
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  ESTIMATE WIN RATE
# ════════════════════════════════════════════════════════════════

@phase10.route('/reports/estimates')
def report_estimates():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        year  = _year_param()
        years = _available_years(conn, 'job_estimates', 'estimate_date') or [year]

        rows = conn.execute("""
            SELECT je.status, COUNT(*) AS cnt, SUM(je.total_estimate) AS total,
                   AVG(je.total_estimate) AS avg_val
            FROM job_estimates je
            WHERE substr(je.estimate_date,1,4)=? AND je.is_deleted=0
            GROUP BY je.status
        """, [str(year)]).fetchall()
        by_status = {r['status']: dict(r) for r in rows}

        total_count = sum(r['cnt'] for r in by_status.values())
        total_value = sum(float(r['total'] or 0) for r in by_status.values())
        won         = by_status.get('Accepted', {})
        won_count   = int(won.get('cnt', 0))
        won_value   = float(won.get('total') or 0)
        win_rate_count = _pct(won_count, total_count)
        win_rate_value = _pct(won_value, total_value)

        # Monthly trend
        trend_rows = conn.execute("""
            SELECT CAST(substr(estimate_date,6,2) AS INTEGER) AS mo,
                   status, COUNT(*) AS cnt, SUM(total_estimate) AS total
            FROM job_estimates
            WHERE substr(estimate_date,1,4)=? AND is_deleted=0
            GROUP BY mo, status ORDER BY mo
        """, [str(year)]).fetchall()

        monthly_trend = defaultdict(lambda: defaultdict(float))
        for r in trend_rows:
            monthly_trend[r['mo']][r['status']] += float(r['total'] or 0)
            monthly_trend[r['mo']][r['status']+'_cnt'] += int(r['cnt'])

        trend_months = []
        for m in range(1, 13):
            md = monthly_trend.get(m, {})
            trend_months.append({
                'label':    MONTHS[m-1],
                'sent':     md.get('Sent', 0) + md.get('Draft', 0),
                'accepted': md.get('Accepted', 0),
                'rejected': md.get('Rejected', 0),
                'cnt_sent': md.get('Sent_cnt', 0) + md.get('Draft_cnt', 0),
                'cnt_won':  md.get('Accepted_cnt', 0),
                'cnt_lost': md.get('Rejected_cnt', 0),
            })

        # Recent estimates list
        recent = conn.execute("""
            SELECT je.estimate_number, je.estimate_date, je.status,
                   je.total_estimate, j.job_code, c.full_name AS client_name
            FROM job_estimates je
            LEFT JOIN jobs j ON je.job_id = j.id
            LEFT JOIN clients c ON je.client_id = c.id
            WHERE substr(je.estimate_date,1,4)=? AND je.is_deleted=0
            ORDER BY je.estimate_date DESC LIMIT 30
        """, [str(year)]).fetchall()

        return render_template('report_estimates.html',
            config=config, badges=badges,
            year=year, years=years,
            by_status=by_status,
            total_count=total_count, total_value=total_value,
            won_count=won_count, won_value=won_value,
            win_rate_count=win_rate_count, win_rate_value=win_rate_value,
            trend_months=trend_months,
            recent=[dict(r) for r in recent],
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  CATEGORY BREAKDOWN
# ════════════════════════════════════════════════════════════════

@phase10.route('/reports/categories')
def report_categories():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        year         = _year_param()
        years        = _available_years(conn) or [date.today().year]
        job_filter   = request.args.get('job', '').strip()
        month_filter = request.args.get('month', '').strip()

        yw, yp = _year_where(year, 'entry_date')
        where  = [yw, "is_deleted=0"]
        params = yp[:]
        if job_filter:
            where.append("job_code=?"); params.append(job_filter)
        if month_filter:
            where.append("CAST(substr(entry_date,6,2) AS INTEGER)=?"); params.append(int(month_filter))
        where_sql = " AND ".join(where)

        rows = conn.execute(f"""
            SELECT category,
                   COUNT(*)            AS txn_count,
                   SUM({_AMT_RAW})     AS total,
                   AVG({_AMT_RAW})     AS avg_amount,
                   MIN(entry_date)     AS first_date,
                   MAX(entry_date)     AS last_date
            FROM ledger
            WHERE {where_sql}
            GROUP BY category
            ORDER BY total DESC
        """, params).fetchall()

        total_spend = sum(float(r['total'] or 0) for r in rows if float(r['total'] or 0) > 0)
        categories  = []
        for r in rows:
            d = dict(r)
            d['total']      = float(d['total'] or 0)
            d['pct']        = _pct(abs(d['total']), total_spend)
            d['avg_amount'] = float(d['avg_amount'] or 0)
            categories.append(d)

        # Jobs for filter dropdown
        job_list = conn.execute("""
            SELECT DISTINCT job_code FROM (
                SELECT job_code FROM jobs WHERE is_deleted=0 AND job_code!=''
                UNION
                SELECT DISTINCT job_code FROM ledger WHERE job_code!='' AND is_deleted=0
            ) ORDER BY job_code
        """).fetchall()

        return render_template('report_categories.html',
            config=config, badges=badges,
            year=year, years=years,
            job_filter=job_filter, month_filter=month_filter,
            job_list=[r[0] for r in job_list],
            categories=categories,
            total_spend=total_spend,
        )
    finally:
        conn.close()


@phase10.route('/api/reports/category-detail')
def api_category_detail():
    """Return individual transactions for a category (for drilldown)."""
    conn = get_connection()
    try:
        category = request.args.get('category', '')
        year     = request.args.get('year', str(date.today().year))
        job      = request.args.get('job', '')
        month    = request.args.get('month', '')

        where  = ["substr(entry_date,1,4)=?", "is_deleted=0", "category=?"]
        params = [str(year), category]
        if job:
            where.append("job_code=?"); params.append(job)
        if month:
            where.append("CAST(substr(entry_date,6,2) AS INTEGER)=?"); params.append(int(month))

        rows = conn.execute(
            f"SELECT entry_date, vendor, job_code, description, "
            f"{_AMT_RAW} AS amount, "
            f"receipt_filename, receipt_verified "
            f"FROM ledger WHERE {' AND '.join(where)} ORDER BY entry_date DESC LIMIT 200",
            params
        ).fetchall()

        return jsonify({'rows': [dict(r) for r in rows]})
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  ACCOUNTS RECEIVABLE AGING
# ════════════════════════════════════════════════════════════════

@phase10.route('/reports/ar')
def report_ar():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        today = date.today()

        invoices = conn.execute("""
            SELECT i.*, c.full_name AS client_name
            FROM invoices i
            LEFT JOIN clients c ON i.client_id = c.id
            WHERE i.status IN ('Pending','Partial','Overdue') AND i.is_deleted=0
            ORDER BY i.due_date ASC
        """).fetchall()

        # Aging buckets
        current = []     # not yet due
        bucket_30 = []   # 1-30 days overdue
        bucket_60 = []   # 31-60
        bucket_90 = []   # 61-90
        bucket_90p = []  # 90+

        for inv in invoices:
            inv_d = dict(inv)
            try:
                due = datetime.strptime(inv_d['due_date'], '%Y-%m-%d').date() if inv_d['due_date'] else today
            except ValueError:
                due = today
            days_overdue = (today - due).days
            inv_d['days_overdue'] = max(0, days_overdue)
            balance = float(inv_d.get('balance_due') or 0)

            if days_overdue <= 0:
                current.append(inv_d)
            elif days_overdue <= 30:
                bucket_30.append(inv_d)
            elif days_overdue <= 60:
                bucket_60.append(inv_d)
            elif days_overdue <= 90:
                bucket_90.append(inv_d)
            else:
                bucket_90p.append(inv_d)

        def _sum(lst):
            return sum(float(i.get('balance_due') or 0) for i in lst)

        return render_template('report_ar.html',
            config=config, badges=badges,
            today=today.strftime('%Y-%m-%d'),
            current=current,     current_total=_sum(current),
            bucket_30=bucket_30, bucket_30_total=_sum(bucket_30),
            bucket_60=bucket_60, bucket_60_total=_sum(bucket_60),
            bucket_90=bucket_90, bucket_90_total=_sum(bucket_90),
            bucket_90p=bucket_90p, bucket_90p_total=_sum(bucket_90p),
            grand_total=_sum(current)+_sum(bucket_30)+_sum(bucket_60)+_sum(bucket_90)+_sum(bucket_90p),
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  MONTHLY SNAPSHOT
# ════════════════════════════════════════════════════════════════

@phase10.route('/reports/snapshot')
def report_snapshot():
    config = _cfg()
    badges = _badges()
    conn   = get_connection()
    try:
        year  = _year_param()
        month = _month_param()
        years = _available_years(conn) or [year]

        mo_str = f"{year}-{month:02d}"

        # Revenue
        revenue = conn.execute("""
            SELECT COALESCE(SUM(amount_paid),0) FROM invoices
            WHERE substr(invoice_date,1,7)=? AND is_deleted=0
        """, [mo_str]).fetchone()[0]

        # Invoiced (billed this month)
        invoiced = conn.execute("""
            SELECT COALESCE(SUM(amount),0) FROM invoices
            WHERE substr(invoice_date,1,7)=? AND is_deleted=0
        """, [mo_str]).fetchone()[0]

        # Expenses
        expenses = conn.execute(f"""
            SELECT COALESCE(SUM(ABS({_AMT})),0) FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE substr(l.entry_date,1,7)=? AND l.is_deleted=0
              AND {_AMT} < 0
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
        """, [mo_str]).fetchone()[0]

        # Payroll
        payroll = conn.execute("""
            SELECT COALESCE(SUM(gross_pay),0) FROM payroll_runs
            WHERE substr(run_date,1,7)=? AND is_deleted=0
        """, [mo_str]).fetchone()[0]

        # Labor hours
        labor_hrs = conn.execute("""
            SELECT COALESCE(SUM(hours),0) FROM timesheet
            WHERE substr(entry_date,1,7)=? AND is_deleted=0
        """, [mo_str]).fetchone()[0]

        # Top expenses this month
        top_exp = conn.execute(f"""
            SELECT l.category, l.vendor, SUM({_AMT}) AS total, COUNT(*) AS cnt
            FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE substr(l.entry_date,1,7)=? AND l.is_deleted=0
              AND {_AMT} < 0
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
            GROUP BY l.category ORDER BY total ASC LIMIT 8
        """, [mo_str]).fetchall()

        # Jobs with activity
        active_jobs_mo = conn.execute("""
            SELECT DISTINCT l.job_code, j.description
            FROM ledger l
            LEFT JOIN jobs j ON l.job_code = j.job_code
            WHERE substr(l.entry_date,1,7)=? AND l.job_code!='' AND l.is_deleted=0
            UNION
            SELECT DISTINCT t.job_code, j.description
            FROM timesheet t
            LEFT JOIN jobs j ON t.job_code = j.job_code
            WHERE substr(t.entry_date,1,7)=? AND t.job_code!='' AND t.is_deleted=0
        """, [mo_str, mo_str]).fetchall()

        # Invoices issued this month
        invoices_mo = conn.execute("""
            SELECT i.invoice_number, i.amount, i.status, c.full_name AS client
            FROM invoices i LEFT JOIN clients c ON i.client_id = c.id
            WHERE substr(i.invoice_date,1,7)=? AND i.is_deleted=0
            ORDER BY i.invoice_date
        """, [mo_str]).fetchall()

        # Tax deadlines this month
        tax_due = conn.execute("""
            SELECT task_description, due_date, status FROM reminders
            WHERE substr(due_date,1,7)=? AND is_deleted=0
            ORDER BY due_date
        """, [mo_str]).fetchall()

        net_profit = float(revenue) - float(expenses) - float(payroll)

        return render_template('report_snapshot.html',
            config=config, badges=badges,
            year=year, month=month, months=MONTHS, years=years,
            month_label=f"{MONTHS[month-1]} {year}",
            revenue=float(revenue), invoiced=float(invoiced),
            expenses=float(expenses), payroll=float(payroll),
            labor_hrs=float(labor_hrs), net_profit=net_profit,
            top_exp=[dict(r) for r in top_exp],
            active_jobs=[dict(r) for r in active_jobs_mo],
            invoices_mo=[dict(r) for r in invoices_mo],
            tax_due=[dict(r) for r in tax_due],
        )
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  JSON DATA APIs (for charts)
# ════════════════════════════════════════════════════════════════

@phase10.route('/api/reports/pl-chart')
def api_pl_chart():
    year = _year_param()
    conn = get_connection()
    try:
        if year == 'all':
            # Group by year instead of month
            rev_rows = conn.execute(f"""
                SELECT substr(l.entry_date,1,4) AS lbl, SUM({_AMT_RAW}) AS v
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE l.is_deleted=0 AND {_AMT_RAW} > 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY lbl ORDER BY lbl
            """).fetchall()
            exp_rows = conn.execute(f"""
                SELECT substr(l.entry_date,1,4) AS lbl, SUM(ABS({_AMT})) AS v
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE l.is_deleted=0 AND {_AMT} < 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY lbl ORDER BY lbl
            """).fetchall()
            all_years = sorted(set(
                [r['lbl'] for r in rev_rows] + [r['lbl'] for r in exp_rows]
            ))
            rev = {r['lbl']: float(r['v']) for r in rev_rows}
            exp = {r['lbl']: float(r['v']) for r in exp_rows}
            return jsonify({
                'labels':   all_years,
                'revenue':  [rev.get(y, 0) for y in all_years],
                'expenses': [exp.get(y, 0) for y in all_years],
                'profit':   [rev.get(y,0) - exp.get(y,0) for y in all_years],
            })

        # Single year — group by month
        yw, yp = _year_where(year, 'invoice_date')
        rev_rows = conn.execute(f"""
            SELECT CAST(substr(invoice_date,6,2) AS INTEGER) AS mo, SUM(amount_paid) AS v
            FROM invoices WHERE {yw} AND is_deleted=0 GROUP BY mo
        """, yp).fetchall()
        ywl, ypl = _year_where(year, 'entry_date', 'l')
        exp_rows = conn.execute(f"""
            SELECT CAST(substr(l.entry_date,6,2) AS INTEGER) AS mo, SUM(ABS({_AMT})) AS v
            FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE {ywl} AND l.is_deleted=0
              AND {_AMT} < 0
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
            GROUP BY mo
        """, ypl).fetchall()

        rev = {r['mo']: float(r['v']) for r in rev_rows}
        exp = {r['mo']: float(r['v']) for r in exp_rows}

        return jsonify({
            'labels':   MONTHS,
            'revenue':  [rev.get(m, 0) for m in range(1,13)],
            'expenses': [exp.get(m, 0) for m in range(1,13)],
            'profit':   [rev.get(m,0) - exp.get(m,0) for m in range(1,13)],
        })
    finally:
        conn.close()


@phase10.route('/api/reports/cashflow-chart')
def api_cashflow_chart():
    year = _year_param()
    conn = get_connection()
    try:
        if year == 'all':
            in_rows = conn.execute(f"""
                SELECT substr(l.entry_date,1,4) AS lbl, SUM({_AMT_RAW}) AS v
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE l.is_deleted=0 AND {_AMT_RAW} > 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY lbl ORDER BY lbl
            """).fetchall()
            out_rows = conn.execute(f"""
                SELECT substr(l.entry_date,1,4) AS lbl, SUM(ABS({_AMT})) AS v
                FROM ledger l
                LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
                WHERE l.is_deleted=0 AND {_AMT} < 0
                  AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
                GROUP BY lbl ORDER BY lbl
            """).fetchall()
            all_years = sorted(set(
                [r['lbl'] for r in in_rows] + [r['lbl'] for r in out_rows]
            ))
            inflows  = {r['lbl']: float(r['v']) for r in in_rows}
            outflows = {r['lbl']: float(r['v']) for r in out_rows}
            return jsonify({
                'labels':   all_years,
                'inflows':  [inflows.get(y, 0)  for y in all_years],
                'outflows': [outflows.get(y, 0) for y in all_years],
            })

        # Single year — group by month
        ywl, ypl = _year_where(year, 'entry_date', 'l')
        in_rows = conn.execute(f"""
            SELECT CAST(substr(l.entry_date,6,2) AS INTEGER) AS mo, SUM({_AMT_RAW}) AS v
            FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE {ywl} AND l.is_deleted=0 AND {_AMT_RAW} > 0
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
            GROUP BY mo
        """, ypl).fetchall()
        out_rows = conn.execute(f"""
            SELECT CAST(substr(l.entry_date,6,2) AS INTEGER) AS mo, SUM(ABS({_AMT})) AS v
            FROM ledger l
            LEFT JOIN work_categories wc ON LOWER(l.category)=LOWER(wc.category_name)
            WHERE {ywl} AND l.is_deleted=0
              AND {_AMT} < 0
              AND (wc.is_transfer IS NULL OR wc.is_transfer=0)
            GROUP BY mo
        """, ypl).fetchall()

        inflows  = {r['mo']: float(r['v']) for r in in_rows}
        outflows = {r['mo']: float(r['v']) for r in out_rows}
        return jsonify({
            'labels':   MONTHS,
            'inflows':  [inflows.get(m, 0)  for m in range(1,13)],
            'outflows': [outflows.get(m, 0) for m in range(1,13)],
        })
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  INCOME STATEMENT  (Schedule C / Quarterly format)
# ════════════════════════════════════════════════════════════════

# Income statement expense line definitions
# Maps income statement display label → list of matching category names in DB
# Matches on EXACT category name OR schedule_c_line from work_categories table
IS_EXPENSE_LINES = [
    ('Advertising',              ['Advertising'],                                           'Line 8'),
    ('Vehicle',                  ['Vehicle/Fuel', 'Vehicle', 'Fuel', 'Gas', 'Auto'],       'Line 9'),
    ('Insurance',                ['Insurance', 'Business Insurance'],                       'Line 15'),
    ('Accounting',               ['Professional Fees', 'Accounting', 'Legal/Accounting'],  'Line 17'),
    ('Office',                   ['Office Supplies', 'Office', 'Supplies'],                'Line 18'),
    ('Tools',                    ['Tools', 'Equipment Rental', 'Tool Purchase'],           'Line 22'),
    ('Payroll taxes',            ['Payroll taxes', 'Payroll Taxes', 'FICA', 'Federal Tax', 'State Tax', 'Payroll Tax'], 'Line 23b'),
    ('Fees',                     ['Bank Fees', 'Fees', 'Permits and Fees'],                'Line 23'),
    ('Meals',                    ['Meals (50%)', 'Meals', 'Entertainment'],                'Line 24b'),
    ('Phone',                    ['Phone', 'Cell Phone', 'Internet'],                      ''),
    ('Clothing',                 ['Clothing', 'Uniforms', 'Work Clothes'],                 ''),
    ('Wages',                    ['Wages', 'Payroll', 'Salary', 'Owner Wages',
                                  'Employee Wages', 'Gross Pay'],                          'Line 26'),
    ('Charitable Contributions', ['Contribution', 'Charitable Contributions', 'Donations', 'Charity'], 'Line 19'),
    ('Rent',                     ['Rent', 'Storage', 'Office Rent'],                       'Line 20b'),
    ('Research and development', ['Research and development', 'R&D'],                      ''),
    ('Software',                 ['Software', 'Dues/Subscriptions', 'Subscriptions',
                                  'SaaS', 'App Subscription'],                             'Line 27a'),
    ('Travel',                   ['Travel', 'Lodging', 'Airfare'],                         'Line 24a'),
    ('Utilities',                ['Utilities', 'Electric', 'Gas Utility', 'Water'],        'Line 25'),
    ('Web hosting and domains',  ['Web hosting and domains', 'Web Hosting', 'Domain',
                                  'Website', 'Marketing'],                                 ''),
    ('Other',                    ['Other Expense', 'Miscellaneous', 'Other',
                                  'Misc', 'General'],                                      'Line 27a'),
]

# Build lookup: category_name → IS expense line label (for schedule_c_line fallback)
def _build_is_lookup(conn):
    """Return dict: category_name → label for IS expense bucketing."""
    lookup = {}
    # Direct category name matches
    for label, cats, _ in IS_EXPENSE_LINES:
        for cat in cats:
            lookup[cat.lower().strip()] = label
    # Also load schedule_c_line mappings from DB
    sc_map = {
        'Line 8': 'Advertising', 'Line 9': 'Vehicle', 'Line 15': 'Insurance',
        'Line 17': 'Accounting', 'Line 18': 'Office', 'Line 19': 'Charitable Contributions',
        'Line 20b': 'Rent', 'Line 22': 'Tools', 'Line 23': 'Fees',
        'Line 23b': 'Payroll taxes', 'Line 24a': 'Travel', 'Line 24b': 'Meals',
        'Line 25': 'Utilities', 'Line 26': 'Wages', 'Line 27a': 'Other',
    }
    try:
        rows = conn.execute("SELECT category_name, schedule_c_line FROM work_categories WHERE is_deleted=0").fetchall()
        for row in rows:
            cat = (row['category_name'] or '').lower().strip()
            sc  = (row['schedule_c_line'] or '').strip()
            if cat not in lookup and sc in sc_map:
                lookup[cat] = sc_map[sc]
    except Exception:
        pass
    return lookup

@phase10.route('/reports/income-statement')
def report_income_statement():
    config  = _cfg()
    badges  = _badges()
    conn    = get_connection()
    try:
        year  = _year_param()
        years = _available_years(conn, 'ledger', 'entry_date') or [year]

        INCOME_CATS  = "'Income Received','ACCOUNT CREDIT'"
        EXCLUDE_CATS = ("'Income Received','ACCOUNT CREDIT','Credit Card Payment',"
                        "'Contribution','Distribution','Credit','Previous','Memo','KB','WRITE OFF'")

        def _qsum(q_months, extra_where='', params_extra=None):
            """Sum signed amounts for a list of months (1-based)."""
            if not q_months:
                return 0.0
            placeholders = ','.join('?' * len(q_months))
            p = [str(year)] + q_months + (params_extra or [])
            row = conn.execute(f"""
                SELECT COALESCE(SUM({_AMT_RAW}), 0)
                FROM ledger
                WHERE substr(entry_date,1,4)=?
                  AND CAST(substr(entry_date,6,2) AS INTEGER) IN ({placeholders})
                  AND is_deleted=0
                  {extra_where}
            """, p).fetchone()
            return float(row[0]) if row else 0.0

        # Quarters: Q1=Jan-Mar(1-3), Q2=Apr-Jun(4-6), Q3=Jul-Sep(7-9), Q4=Oct-Dec(10-12)
        QUARTERS = [(1,[1,2,3]), (2,[4,5,6]), (3,[7,8,9]), (4,[10,11,12])]

        # ── Revenue ──────────────────────────────────────────
        def rev_q(months):
            return _qsum(months, f"AND category IN ({INCOME_CATS}) AND {_AMT_RAW} > 0")
        def rev_yr():
            r = conn.execute(f"""
                SELECT COALESCE(SUM({_AMT_RAW}),0) FROM ledger
                WHERE substr(entry_date,1,4)=? AND is_deleted=0
                  AND category IN ({INCOME_CATS}) AND {_AMT_RAW} > 0
            """, [str(year)]).fetchone()
            return float(r[0]) if r else 0.0

        gross_sales   = [rev_q([1,2,3]), rev_q([4,5,6]), rev_q([7,8,9]), rev_q([10,11,12])]
        gross_sales_yr = rev_yr()

        # ── COGS ─────────────────────────────────────────────
        def cogs_q(months):
            if not months:
                return 0.0
            placeholders = ','.join('?' * len(months))
            p = [str(year)] + months
            row = conn.execute(f"""
                SELECT COALESCE(SUM(ABS({_AMT_RAW})), 0)
                FROM ledger
                WHERE substr(entry_date,1,4)=?
                  AND CAST(substr(entry_date,6,2) AS INTEGER) IN ({placeholders})
                  AND is_deleted=0
                  AND {_AMT_RAW} < 0 AND is_cogs=1
            """, p).fetchone()
            return float(row[0]) if row else 0.0
        def cogs_yr():
            r = conn.execute(f"""
                SELECT COALESCE(SUM(ABS({_AMT_RAW})),0) FROM ledger
                WHERE substr(entry_date,1,4)=? AND is_deleted=0
                  AND {_AMT_RAW} < 0 AND is_cogs=1
            """, [str(year)]).fetchone()
            return float(r[0]) if r else 0.0

        goods_purchased   = [cogs_q([1,2,3]), cogs_q([4,5,6]), cogs_q([7,8,9]), cogs_q([10,11,12])]
        goods_purchased_yr = cogs_yr()

        # ── Operating Expenses ────────────────────────────────
        # Build dynamic lookup: category_name → IS expense line label
        is_lookup = _build_is_lookup(conn)

        EXCLUDE_FROM_OPEX = frozenset([
            'income received', 'account credit', 'credit card payment',
            'contribution', 'distribution', 'credit', 'previous', 'memo', 'kb',
            'write off', 'write-off', 'owner draw', 'loan proceeds',
        ])

        # Load ALL non-COGS, non-income expense amounts bucketed by label
        opex_rows_raw = conn.execute(f"""
            SELECT category,
                   CAST(substr(entry_date,6,2) AS INTEGER) AS mo,
                   SUM(ABS({_AMT_RAW})) AS total
            FROM ledger
            WHERE substr(entry_date,1,4)=? AND is_deleted=0
              AND {_AMT_RAW} < 0 AND is_cogs=0
            GROUP BY category, mo ORDER BY mo
        """, [str(year)]).fetchall()

        # Build per-label, per-quarter accumulator
        label_order = [row[0] for row in IS_EXPENSE_LINES]
        label_q  = {lbl: [0.0, 0.0, 0.0, 0.0] for lbl in label_order}
        label_yr = {lbl: 0.0 for lbl in label_order}
        unmatched_q  = [0.0, 0.0, 0.0, 0.0]
        unmatched_yr = 0.0

        QUARTER_MAP = {1:0, 2:0, 3:0, 4:1, 5:1, 6:1, 7:2, 8:2, 9:2, 10:3, 11:3, 12:3}

        for row in opex_rows_raw:
            cat   = (row['category'] or '').strip()
            mo    = row['mo']
            total = float(row['total'])
            qi    = QUARTER_MAP.get(mo, 0)

            # Skip excluded categories
            if cat.lower() in EXCLUDE_FROM_OPEX:
                continue

            lbl = is_lookup.get(cat.lower())
            if lbl and lbl in label_q:
                label_q[lbl][qi]  += total
                label_yr[lbl]     += total
            else:
                # Unmapped → "Other"
                unmatched_q[qi]  += total
                unmatched_yr     += total

        # Add unmatched to "Other"
        if 'Other' in label_q:
            for i in range(4): label_q['Other'][i] += unmatched_q[i]
            label_yr['Other'] += unmatched_yr

        expense_rows = []
        for label, cats, sc in IS_EXPENSE_LINES:
            q_vals = label_q.get(label, [0.0]*4)
            yr_val = label_yr.get(label, 0.0)
            expense_rows.append({'label': label, 'q': q_vals, 'yr': yr_val})

        # Build summary numbers
        def _yr(vals): return sum(vals)

        net_sales          = [g for g in gross_sales]
        net_sales_yr       = gross_sales_yr

        total_goods        = goods_purchased[:]
        total_goods_yr     = goods_purchased_yr

        cogs               = goods_purchased[:]
        cogs_yr            = goods_purchased_yr

        gross_profit       = [net_sales[i] - cogs[i] for i in range(4)]
        gross_profit_yr    = net_sales_yr - cogs_yr

        total_opex         = [sum(r['q'][i] for r in expense_rows) for i in range(4)]
        total_opex_yr      = sum(r['yr'] for r in expense_rows)

        operating_income   = [gross_profit[i] - total_opex[i] for i in range(4)]
        operating_income_yr = gross_profit_yr - total_opex_yr

        # Non-operating items (interest, tax) — from ledger if categories exist
        def _simple_yr_sum(cat_list):
            if not cat_list: return 0.0
            ph = ','.join('?'*len(cat_list))
            r = conn.execute(f"""
                SELECT COALESCE(SUM(ABS({_AMT_RAW})),0) FROM ledger
                WHERE substr(entry_date,1,4)=? AND is_deleted=0
                  AND {_AMT_RAW} < 0 AND category IN ({ph})
            """, [str(year)] + cat_list).fetchone()
            return float(r[0]) if r else 0.0

        interest_expense   = [0.0]*4
        interest_expense_yr = _simple_yr_sum(['Interest Expense','Interest'])

        income_before_tax  = [operating_income[i] - interest_expense[i] for i in range(4)]
        income_before_tax_yr = operating_income_yr - interest_expense_yr

        income_tax         = [0.0]*4
        income_tax_yr      = 0.0

        net_income         = [income_before_tax[i] - income_tax[i] for i in range(4)]
        net_income_yr      = income_before_tax_yr - income_tax_yr

        today_str = date.today().strftime('%m/%d/%y')
        return render_template('report_income_statement.html',
            config=config, badges=badges,
            year=year, years=years, today_str=today_str,
            # Revenue
            gross_sales=gross_sales, gross_sales_yr=gross_sales_yr,
            net_sales=net_sales, net_sales_yr=net_sales_yr,
            # COGS
            goods_purchased=goods_purchased, goods_purchased_yr=goods_purchased_yr,
            total_goods=total_goods, total_goods_yr=total_goods_yr,
            cogs=cogs, cogs_yr=cogs_yr,
            gross_profit=gross_profit, gross_profit_yr=gross_profit_yr,
            # Expenses
            expense_rows=expense_rows,
            total_opex=total_opex, total_opex_yr=total_opex_yr,
            # Bottom
            operating_income=operating_income, operating_income_yr=operating_income_yr,
            interest_expense=interest_expense, interest_expense_yr=interest_expense_yr,
            income_before_tax=income_before_tax, income_before_tax_yr=income_before_tax_yr,
            income_tax=income_tax, income_tax_yr=income_tax_yr,
            net_income=net_income, net_income_yr=net_income_yr,
        )
    finally:
        conn.close()
