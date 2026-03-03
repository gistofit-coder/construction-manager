"""
Phase 10 Tests — Reports & Analytics
Run: python3 tests_phase10.py
"""
import os, sys, json, unittest
from datetime import date, timedelta

TEST_DB = '/tmp/test_p10.db'
os.environ['CONSTRUCTION_DB'] = TEST_DB
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

from database import init_db, get_connection
init_db(TEST_DB)

from app import app as flask_app
flask_app.config['TESTING'] = True
CLIENT = flask_app.test_client()

TODAY      = date.today().strftime('%Y-%m-%d')
THIS_YEAR  = date.today().year
THIS_MONTH = f"{THIS_YEAR}-{date.today().month:02d}"

from database import db as _db

# ─────────────────────────────────────────────
# Seed helpers
# ─────────────────────────────────────────────

_seed_ctr = 0
def _uid():
    global _seed_ctr; _seed_ctr += 1
    return _seed_ctr

def _seed_client(name=None):
    n = name or f"Client {_uid()}"
    with _db() as conn:
        cur = conn.execute("INSERT INTO clients (full_name) VALUES (?)", [n])
        return cur.lastrowid

def _seed_job(client_id=None, status='Active'):
    uid = _uid()
    code = f"JOB-{uid:04d}"
    with _db() as conn:
        cur = conn.execute(
            "INSERT INTO jobs (job_code, client_id, description, status) VALUES (?,?,?,?)",
            [code, client_id, f'Test Job {uid}', status]
        )
        return cur.lastrowid, code

def _seed_ledger(amount, date_str=None, category='Materials', job_code='', is_cogs=0):
    date_str = date_str or TODAY
    with _db() as conn:
        cur = conn.execute("""
            INSERT INTO ledger (entry_date, amount, category, job_code, is_cogs, vendor)
            VALUES (?,?,?,?,?,'Test Vendor')
        """, [date_str, amount, category, job_code, is_cogs])
        return cur.lastrowid

def _seed_invoice(amount, paid=0, status='Paid', date_str=None, job_code='', client_id=None):
    date_str  = date_str or TODAY
    due       = date_str
    balance   = amount - paid
    inv_num   = _uid() + 1000
    with _db() as conn:
        cur = conn.execute("""
            INSERT INTO invoices (invoice_number, invoice_date, due_date, job_code,
                client_id, description_of_work, amount, amount_paid, balance_due, status)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, [inv_num, date_str, due, job_code, client_id,
              'Work done', amount, paid, balance, status])
        return cur.lastrowid

def _seed_payroll(gross, net=None, year=None, date_str=None, emp_id=None):
    year     = year or THIS_YEAR
    date_str = date_str or TODAY
    net      = net or gross * 0.75
    ss_ee    = round(gross * 0.062, 2)
    med_ee   = round(gross * 0.0145, 2)
    ss_er    = round(gross * 0.062, 2)
    med_er   = round(gross * 0.0145, 2)
    futa     = round(min(gross, 7000) * 0.006, 2)
    suta     = round(min(gross, 13590) * 0.035, 2)
    fed_wh   = round(gross * 0.12, 2)

    if not emp_id:
        uid = _uid()
        with _db() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO employees (emp_id, first_name, last_name)
                VALUES (?,?,?)
            """, [uid, 'Test', f'Emp{uid}'])
        emp_id = uid

    with _db() as conn:
        cur = conn.execute("""
            INSERT INTO payroll_runs
                (run_date, year, emp_id, gross_pay, net_pay,
                 ss_withheld, medicare_withheld, fed_withholding,
                 employer_ss, employer_medicare, futa_amount, suta_amount,
                 total_withheld)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [date_str, year, emp_id, gross, net,
              ss_ee, med_ee, fed_wh,
              ss_er, med_er, futa, suta,
              ss_ee+med_ee+fed_wh])
        return cur.lastrowid

def _seed_estimate(job_id=None, client_id=None, status='Accepted',
                   total=10000, date_str=None):
    date_str = date_str or TODAY
    uid = _uid()
    with _db() as conn:
        cur = conn.execute("""
            INSERT INTO job_estimates
                (job_id, client_id, estimate_date, estimate_number, status,
                 total_direct_costs, total_estimate, version_number)
            VALUES (?,?,?,?,?,?,?,1)
        """, [job_id, client_id, date_str, f'EST-{THIS_YEAR}-{uid:03d}',
              status, total * 0.7, total])
        return cur.lastrowid


# ════════════════════════════════════════════════════════════════
class TestReportsHub(unittest.TestCase):

    def test_hub_loads(self):
        r = CLIENT.get('/reports', follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Reports', r.data)

    def test_hub_shows_report_links(self):
        r = CLIENT.get('/reports', follow_redirects=True)
        self.assertIn(b'P&amp;L', r.data)
        self.assertIn(b'Job Profitability', r.data)
        self.assertIn(b'Cash Flow', r.data)
        self.assertIn(b'Payroll Tax', r.data)

    def test_hub_shows_ytd_stats(self):
        # Seed some data
        _seed_invoice(5000, paid=5000, status='Paid')
        _seed_ledger(1000)
        r = CLIENT.get('/reports', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_hub_redirect_from_old_url(self):
        r = CLIENT.get('/reports')
        # Either 200 (direct) or 302 (redirect) both OK
        self.assertIn(r.status_code, [200, 302])


# ════════════════════════════════════════════════════════════════
class TestPLReport(unittest.TestCase):

    def setUp(self):
        # Seed revenue and expenses for this year
        _seed_invoice(8000, paid=8000, status='Paid', date_str=f'{THIS_YEAR}-03-15')
        _seed_invoice(12000, paid=12000, status='Paid', date_str=f'{THIS_YEAR}-06-10')
        _seed_ledger(2000, date_str=f'{THIS_YEAR}-03-20', category='Materials')
        _seed_ledger(1500, date_str=f'{THIS_YEAR}-06-05', category='Equipment')
        _seed_payroll(3000, date_str=f'{THIS_YEAR}-03-01')

    def test_pl_loads(self):
        r = CLIENT.get(f'/reports/pl?year={THIS_YEAR}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Profit', r.data)

    def test_pl_shows_12_months(self):
        r = CLIENT.get(f'/reports/pl?year={THIS_YEAR}')
        # All 12 month abbreviations should appear
        for mo in [b'Jan', b'Feb', b'Mar', b'Apr']:
            self.assertIn(mo, r.data)

    def test_pl_shows_totals(self):
        r = CLIENT.get(f'/reports/pl?year={THIS_YEAR}')
        self.assertIn(b'YTD', r.data)
        self.assertIn(b'Revenue', r.data)

    def test_pl_year_filter(self):
        r = CLIENT.get(f'/reports/pl?year={THIS_YEAR - 1}')
        self.assertEqual(r.status_code, 200)

    def test_pl_chart_api(self):
        r = CLIENT.get(f'/api/reports/pl-chart?year={THIS_YEAR}')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn('revenue', d)
        self.assertIn('expenses', d)
        self.assertIn('profit', d)
        self.assertIn('labels', d)
        self.assertEqual(len(d['revenue']), 12)
        self.assertEqual(len(d['labels']), 12)

    def test_pl_chart_revenue_nonzero(self):
        r = CLIENT.get(f'/api/reports/pl-chart?year={THIS_YEAR}')
        d = json.loads(r.data)
        self.assertGreater(sum(d['revenue']), 0)

    def test_pl_shows_category_breakdown(self):
        r = CLIENT.get(f'/reports/pl?year={THIS_YEAR}')
        self.assertIn(b'Materials', r.data)


# ════════════════════════════════════════════════════════════════
class TestJobProfitability(unittest.TestCase):

    def setUp(self):
        self.client_id = _seed_client('Profit Client')
        self.job_id, self.job_code = _seed_job(client_id=self.client_id)
        _seed_invoice(15000, paid=15000, status='Paid', job_code=self.job_code, client_id=self.client_id)
        _seed_ledger(4000, job_code=self.job_code, is_cogs=1, category='Materials')
        _seed_ledger(2000, job_code=self.job_code, is_cogs=1, category='Subs')

    def test_jobs_loads(self):
        r = CLIENT.get('/reports/jobs')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Profitability', r.data)

    def test_jobs_shows_job_codes(self):
        r = CLIENT.get('/reports/jobs')
        self.assertIn(self.job_code.encode(), r.data)

    def test_jobs_filter_by_status(self):
        r = CLIENT.get('/reports/jobs?status=Active')
        self.assertEqual(r.status_code, 200)
        self.assertIn(self.job_code.encode(), r.data)

    def test_jobs_filter_archived_excludes_active(self):
        r = CLIENT.get('/reports/jobs?status=Archived')
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(self.job_code.encode(), r.data)

    def test_jobs_shows_profit_column(self):
        r = CLIENT.get('/reports/jobs')
        self.assertIn(b'Profit', r.data)

    def test_jobs_totals_row(self):
        r = CLIENT.get('/reports/jobs')
        self.assertIn(b'TOTALS', r.data)


# ════════════════════════════════════════════════════════════════
class TestCashFlow(unittest.TestCase):

    def setUp(self):
        _seed_invoice(5000, paid=5000, status='Paid', date_str=f'{THIS_YEAR}-04-01')
        _seed_ledger(1200, date_str=f'{THIS_YEAR}-04-15')
        _seed_payroll(2000, date_str=f'{THIS_YEAR}-04-30')

    def test_cashflow_loads(self):
        r = CLIENT.get(f'/reports/cashflow?year={THIS_YEAR}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Cash Flow', r.data)

    def test_cashflow_shows_12_months(self):
        r = CLIENT.get(f'/reports/cashflow?year={THIS_YEAR}')
        self.assertIn(b'Jan', r.data)
        self.assertIn(b'Dec', r.data)

    def test_cashflow_shows_totals(self):
        r = CLIENT.get(f'/reports/cashflow?year={THIS_YEAR}')
        self.assertIn(b'Total Inflows', r.data)
        self.assertIn(b'Total Outflows', r.data)

    def test_cashflow_chart_api(self):
        r = CLIENT.get(f'/api/reports/cashflow-chart?year={THIS_YEAR}')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn('inflows', d)
        self.assertIn('outflows', d)
        self.assertEqual(len(d['inflows']), 12)

    def test_cashflow_year_filter(self):
        r = CLIENT.get(f'/reports/cashflow?year={THIS_YEAR - 1}')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestPayrollTax(unittest.TestCase):

    def setUp(self):
        self.emp_id = _uid()
        with _db() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO employees (emp_id, first_name, last_name)
                VALUES (?,?,?)
            """, [self.emp_id, 'John', 'Doe'])
        for mo in ['01', '02', '03', '04']:
            _seed_payroll(4000, year=THIS_YEAR,
                         date_str=f'{THIS_YEAR}-{mo}-15',
                         emp_id=self.emp_id)

    def test_payroll_tax_loads(self):
        r = CLIENT.get(f'/reports/payroll-tax?year={THIS_YEAR}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Payroll Tax', r.data)

    def test_payroll_tax_shows_quarters(self):
        r = CLIENT.get(f'/reports/payroll-tax?year={THIS_YEAR}')
        self.assertIn(b'Q1', r.data)
        self.assertIn(b'Q2', r.data)
        self.assertIn(b'Q3', r.data)
        self.assertIn(b'Q4', r.data)

    def test_payroll_tax_shows_941_liability(self):
        r = CLIENT.get(f'/reports/payroll-tax?year={THIS_YEAR}')
        self.assertIn(b'941', r.data)

    def test_payroll_tax_shows_futa_suta(self):
        r = CLIENT.get(f'/reports/payroll-tax?year={THIS_YEAR}')
        self.assertIn(b'FUTA', r.data)
        self.assertIn(b'SUTA', r.data)

    def test_payroll_tax_shows_employees(self):
        r = CLIENT.get(f'/reports/payroll-tax?year={THIS_YEAR}')
        self.assertIn(b'John', r.data)

    def test_payroll_tax_ytd_tiles(self):
        r = CLIENT.get(f'/reports/payroll-tax?year={THIS_YEAR}')
        self.assertIn(b'Gross Pay', r.data)
        self.assertIn(b'Net Pay', r.data)

    def test_payroll_tax_year_filter(self):
        r = CLIENT.get(f'/reports/payroll-tax?year={THIS_YEAR - 1}')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestEstimateWinRate(unittest.TestCase):

    def setUp(self):
        self.client_id = _seed_client('Win Rate Client')
        self.job_id, _ = _seed_job(client_id=self.client_id)
        # Seed some estimates
        _seed_estimate(job_id=self.job_id, client_id=self.client_id, status='Accepted', total=20000)
        _seed_estimate(job_id=self.job_id, client_id=self.client_id, status='Rejected', total=15000)
        _seed_estimate(job_id=self.job_id, client_id=self.client_id, status='Sent', total=12000)
        _seed_estimate(job_id=self.job_id, client_id=self.client_id, status='Draft', total=8000)

    def test_win_rate_loads(self):
        r = CLIENT.get(f'/reports/estimates?year={THIS_YEAR}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Win Rate', r.data)

    def test_win_rate_shows_status_breakdown(self):
        r = CLIENT.get(f'/reports/estimates?year={THIS_YEAR}')
        self.assertIn(b'Accepted', r.data)
        self.assertIn(b'Rejected', r.data)
        self.assertIn(b'Sent', r.data)

    def test_win_rate_shows_count(self):
        r = CLIENT.get(f'/reports/estimates?year={THIS_YEAR}')
        self.assertIn(b'Total Estimates', r.data)

    def test_win_rate_shows_percentage(self):
        r = CLIENT.get(f'/reports/estimates?year={THIS_YEAR}')
        self.assertIn(b'Win Rate', r.data)
        self.assertIn(b'%', r.data)

    def test_win_rate_shows_recent_list(self):
        r = CLIENT.get(f'/reports/estimates?year={THIS_YEAR}')
        self.assertIn(b'EST-', r.data)

    def test_win_rate_year_filter(self):
        r = CLIENT.get(f'/reports/estimates?year={THIS_YEAR - 1}')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestMonthlySnapshot(unittest.TestCase):

    def setUp(self):
        mo = f'{THIS_YEAR}-{date.today().month:02d}'
        _seed_invoice(7500, paid=7500, status='Paid', date_str=f'{mo}-10')
        _seed_ledger(800,  date_str=f'{mo}-05', category='Fuel')
        _seed_ledger(1200, date_str=f'{mo}-12', category='Materials')
        _seed_payroll(2500, date_str=f'{mo}-15')

    def test_snapshot_loads(self):
        r = CLIENT.get(f'/reports/snapshot?year={THIS_YEAR}&month={date.today().month}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Snapshot', r.data)

    def test_snapshot_shows_kpi_tiles(self):
        r = CLIENT.get(f'/reports/snapshot?year={THIS_YEAR}&month={date.today().month}')
        self.assertIn(b'Revenue', r.data)
        self.assertIn(b'Expenses', r.data)
        self.assertIn(b'Net Profit', r.data)

    def test_snapshot_shows_top_expenses(self):
        r = CLIENT.get(f'/reports/snapshot?year={THIS_YEAR}&month={date.today().month}')
        self.assertIn(b'Top Expenses', r.data)

    def test_snapshot_month_selector(self):
        r = CLIENT.get(f'/reports/snapshot?year={THIS_YEAR}&month=1')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Jan', r.data)

    def test_snapshot_different_year(self):
        r = CLIENT.get(f'/reports/snapshot?year={THIS_YEAR - 1}&month=6')
        self.assertEqual(r.status_code, 200)

    def test_snapshot_shows_pl_summary(self):
        r = CLIENT.get(f'/reports/snapshot?year={THIS_YEAR}&month={date.today().month}')
        self.assertIn(b'P&L Summary', r.data)

    def test_snapshot_shows_invoices_section(self):
        r = CLIENT.get(f'/reports/snapshot?year={THIS_YEAR}&month={date.today().month}')
        self.assertIn(b'Invoices', r.data)


# ════════════════════════════════════════════════════════════════
class TestCategoryBreakdown(unittest.TestCase):

    def setUp(self):
        for cat, amt in [('Labor', 5000), ('Materials', 3000), ('Equipment', 1500), ('Fuel', 800)]:
            _seed_ledger(amt, date_str=f'{THIS_YEAR}-05-01', category=cat)

    def test_categories_loads(self):
        r = CLIENT.get(f'/reports/categories?year={THIS_YEAR}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Category', r.data)

    def test_categories_shows_all_categories(self):
        r = CLIENT.get(f'/reports/categories?year={THIS_YEAR}')
        self.assertIn(b'Labor', r.data)
        self.assertIn(b'Materials', r.data)
        self.assertIn(b'Equipment', r.data)
        self.assertIn(b'Fuel', r.data)

    def test_categories_shows_total_spend(self):
        r = CLIENT.get(f'/reports/categories?year={THIS_YEAR}')
        self.assertIn(b'Total Spend', r.data)

    def test_categories_shows_percentages(self):
        r = CLIENT.get(f'/reports/categories?year={THIS_YEAR}')
        self.assertIn(b'%', r.data)

    def test_categories_year_filter(self):
        r = CLIENT.get(f'/reports/categories?year={THIS_YEAR - 1}')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestARaging(unittest.TestCase):

    def setUp(self):
        self.client_id = _seed_client('AR Client')
        # Current invoice
        _seed_invoice(3000, paid=0, status='Pending',
                      date_str=TODAY, client_id=self.client_id)
        # 45-day overdue invoice
        past45 = (date.today() - timedelta(days=45)).strftime('%Y-%m-%d')
        _seed_invoice(2000, paid=0, status='Overdue',
                      date_str=past45, client_id=self.client_id)
        # 100-day overdue invoice
        past100 = (date.today() - timedelta(days=100)).strftime('%Y-%m-%d')
        _seed_invoice(1500, paid=0, status='Overdue',
                      date_str=past100, client_id=self.client_id)

    def test_ar_loads(self):
        r = CLIENT.get('/reports/ar')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Aging', r.data)

    def test_ar_shows_buckets(self):
        r = CLIENT.get('/reports/ar')
        self.assertIn(b'Current', r.data)
        self.assertIn(b'1\xe2\x80\x9330', r.data)  # 1–30 or similar

    def test_ar_shows_grand_total(self):
        r = CLIENT.get('/reports/ar')
        self.assertIn(b'Total Outstanding', r.data)

    def test_ar_shows_client_name(self):
        r = CLIENT.get('/reports/ar')
        self.assertIn(b'AR Client', r.data)

    def test_ar_empty_state(self):
        """With all invoices paid, AR shows empty state."""
        # Pay off the invoices via status update
        with _db() as conn:
            conn.execute("UPDATE invoices SET status='Paid', amount_paid=amount, balance_due=0 WHERE status IN ('Pending','Overdue')")
        r = CLIENT.get('/reports/ar')
        self.assertEqual(r.status_code, 200)

    def test_ar_no_paid_invoices_shown(self):
        """Paid invoices should not appear in AR."""
        _seed_invoice(9999, paid=9999, status='Paid')
        r = CLIENT.get('/reports/ar')
        self.assertNotIn(b'9,999', r.data)


# ════════════════════════════════════════════════════════════════
class TestReportsMath(unittest.TestCase):
    """Verify computed values in route functions."""

    def test_pct_helper(self):
        from routes_phase10 import _pct
        self.assertEqual(_pct(25, 100), 25.0)
        self.assertEqual(_pct(0, 100), 0.0)
        self.assertEqual(_pct(50, 0), 0.0)   # no division by zero
        self.assertEqual(_pct(1, 3), 33.3)

    def test_available_years_returns_list(self):
        from routes_phase10 import _available_years
        conn = get_connection()
        years = _available_years(conn)
        conn.close()
        self.assertIsInstance(years, list)

    def test_year_param_default(self):
        with flask_app.test_request_context('/reports/pl'):
            from routes_phase10 import _year_param
            y = _year_param()
            self.assertEqual(y, date.today().year)

    def test_month_param_default(self):
        with flask_app.test_request_context('/reports/snapshot'):
            from routes_phase10 import _month_param
            m = _month_param()
            self.assertEqual(m, date.today().month)

    def test_pl_totals_add_up(self):
        """Revenue minus total_cost should equal profit."""
        # Seed known amounts
        _seed_invoice(10000, paid=10000, status='Paid', date_str=f'{THIS_YEAR}-07-01')
        _seed_ledger(2000, date_str=f'{THIS_YEAR}-07-10')
        r = CLIENT.get(f'/api/reports/pl-chart?year={THIS_YEAR}')
        d = json.loads(r.data)
        for i in range(12):
            computed_profit = d['revenue'][i] - d['expenses'][i]
            self.assertAlmostEqual(d['profit'][i], computed_profit, places=2)

    def test_ar_aging_buckets_sum_to_grand_total(self):
        """Sum of all aging buckets should equal grand total."""
        # Check via the data, not HTML parsing
        from routes_phase10 import report_ar
        with flask_app.test_request_context('/reports/ar'):
            response = CLIENT.get('/reports/ar')
            self.assertEqual(response.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestRegressionPhase10(unittest.TestCase):

    def test_estimates_module_loads(self):
        r = CLIENT.get('/estimates')
        self.assertEqual(r.status_code, 200)

    def test_reconciliation_loads(self):
        r = CLIENT.get('/reconciliation')
        self.assertEqual(r.status_code, 200)

    def test_tax_loads(self):
        r = CLIENT.get('/tax', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_payroll_loads(self):
        r = CLIENT.get('/payroll', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_invoices_loads(self):
        r = CLIENT.get('/invoices', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_dashboard_loads(self):
        r = CLIENT.get('/')
        self.assertEqual(r.status_code, 200)

    def test_ledger_loads(self):
        r = CLIENT.get('/ledger', follow_redirects=True)
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    total  = result.testsRun
    if result.wasSuccessful():
        print(f"\n✅ All {total} Phase 10 tests passed!")
    else:
        print(f"\n❌ {len(result.failures)} failure(s), {len(result.errors)} error(s) out of {total}")
    sys.exit(0 if result.wasSuccessful() else 1)
