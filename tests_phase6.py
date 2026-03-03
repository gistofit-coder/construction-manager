"""
Phase 6 Tests — Payroll
Run: python3 tests_phase6.py
"""
import os, sys, json, unittest
from datetime import date, timedelta

TEST_DB = '/tmp/test_p6.db'
os.environ['CONSTRUCTION_DB'] = TEST_DB
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

from database import init_db, get_connection
init_db(TEST_DB)

from app import app as flask_app
flask_app.config['TESTING'] = True

# ── Seed ────────────────────────────────────────────────────────
from database import db as _db
with _db() as conn:
    conn.execute("""INSERT INTO employees (emp_id, first_name, last_name, status, occupation)
                    VALUES (301, 'Alice', 'Wong', 'Active', 'Carpenter')""")
    conn.execute("""INSERT INTO employees (emp_id, first_name, last_name, status, occupation)
                    VALUES (302, 'Bob', 'Chen', 'Active', 'Electrician')""")
    conn.execute("""INSERT INTO employees (emp_id, first_name, last_name, status)
                    VALUES (303, 'Carol', 'Davis', 'Active')""")
    conn.execute("""INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour)
                    VALUES (301, '2025-01-01', 95.00, 32.00)""")
    conn.execute("""INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour)
                    VALUES (302, '2025-01-01', 110.00, 45.00)""")
    conn.execute("""INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour)
                    VALUES (303, '2025-01-01', 80.00, 28.00)""")
    # Seed timesheet hours for Alice in week of Jan 6-12 2025
    for d_off, hrs in enumerate([8, 8, 8, 8, 8, 4, 4]):  # 48h total
        entry_date = (date(2025, 1, 6) + timedelta(days=d_off)).strftime('%Y-%m-%d')
        conn.execute("""INSERT INTO timesheet (entry_date, emp_id, hours, bill_rate, cost_rate,
                         bill_amount, cost_amount, person_label)
                         VALUES (?,301,?,95,32,?,?,?)""",
                     [entry_date, hrs, hrs*95, hrs*32, 'Alice Wong'])

CLIENT = flask_app.test_client()


def _post(url, data):
    return CLIENT.post(url, data=data, follow_redirects=True)

def _post_json(url, data):
    return CLIENT.post(url, data=json.dumps(data),
                       content_type='application/json')

def _get_json(url):
    r = CLIENT.get(url)
    return json.loads(r.data)

def _create_run(overrides=None):
    data = {
        'emp_id': '301',
        'pay_period_start': '2025-01-06',
        'pay_period_end':   '2025-01-12',
        'total_hours':      '40',
        'holiday_hours':    '0',
        'standard_pay_rate': '32.00',
        'overtime_pay_rate': '48.00',
        'fed_withholding':  '50.00',
        'state_withholding': '20.00',
        'check_number': '',
        'notes': '',
    }
    if overrides:
        data.update(overrides)
    return CLIENT.post('/payroll/new', data=data, follow_redirects=False)


# ════════════════════════════════════════════════════════════════
class TestPayrollList(unittest.TestCase):

    def test_list_loads(self):
        r = CLIENT.get('/payroll')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Payroll', r.data)

    def test_list_filter_by_year(self):
        r = CLIENT.get('/payroll?year=2025')
        self.assertEqual(r.status_code, 200)

    def test_list_filter_by_emp(self):
        r = CLIENT.get('/payroll?year=2025&emp=301')
        self.assertEqual(r.status_code, 200)

    def test_new_form_loads(self):
        r = CLIENT.get('/payroll/new')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'total_hours', r.data)

    def test_new_form_prefill_emp(self):
        r = CLIENT.get('/payroll/new?emp_id=301&start=2025-01-06&end=2025-01-12')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestPayrollCreate(unittest.TestCase):

    def test_create_basic_run(self):
        r = _create_run()
        self.assertEqual(r.status_code, 302)  # redirect to detail

    def test_create_stores_in_db(self):
        _create_run({'emp_id': '302', 'total_hours': '40', 'standard_pay_rate': '45'})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM payroll_runs WHERE emp_id=302 AND is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row['emp_id'], 302)

    def test_standard_hours_gross_pay(self):
        """40h × $32/hr = $1280 gross, no OT."""
        _create_run({'emp_id': '301', 'total_hours': '40',
                     'standard_pay_rate': '32', 'overtime_pay_rate': '48'})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM payroll_runs WHERE emp_id=301 AND is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertAlmostEqual(float(row['standard_hours']), 40.0)
        self.assertAlmostEqual(float(row['overtime_hours']), 0.0)
        self.assertAlmostEqual(float(row['gross_pay']), 1280.0)

    def test_overtime_split_44h(self):
        """44h: 40h standard + 4h OT."""
        _create_run({'emp_id': '303', 'total_hours': '44',
                     'standard_pay_rate': '28', 'overtime_pay_rate': '42'})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM payroll_runs WHERE emp_id=303 AND is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertAlmostEqual(float(row['standard_hours']), 40.0)
        self.assertAlmostEqual(float(row['overtime_hours']), 4.0)
        expected_gross = 40 * 28 + 4 * 42  # 1120 + 168 = 1288
        self.assertAlmostEqual(float(row['gross_pay']), expected_gross)

    def test_overtime_split_50h(self):
        """50h: 40h standard + 10h OT."""
        _create_run({'emp_id': '301', 'total_hours': '50',
                     'standard_pay_rate': '30', 'overtime_pay_rate': '45'})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM payroll_runs WHERE emp_id=301 AND is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertAlmostEqual(float(row['standard_hours']), 40.0)
        self.assertAlmostEqual(float(row['overtime_hours']), 10.0)

    def test_holiday_hours_added_to_gross(self):
        """Holiday hours at std rate added to gross pay."""
        _create_run({'emp_id': '302', 'total_hours': '32',
                     'standard_pay_rate': '45', 'overtime_pay_rate': '67.5',
                     'holiday_hours': '8'})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM payroll_runs WHERE emp_id=302 AND is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        # 32h standard + 8h holiday @ $45 = 32*45 + 8*45 = 1440 + 360 = 1800
        expected = 32 * 45 + 8 * 45
        self.assertAlmostEqual(float(row['gross_pay']), expected)

    def test_week_number_assigned(self):
        """week_number derived from pay_period_start."""
        _create_run({'pay_period_start': '2025-01-06',
                     'pay_period_end': '2025-01-12'})
        conn = get_connection()
        row = conn.execute(
            "SELECT week_number, year FROM payroll_runs WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertEqual(row['year'], 2025)
        self.assertGreater(row['week_number'], 0)

    def test_ss_withheld_computed(self):
        """SS withheld = gross × 6.2% (within wage cap)."""
        _create_run({'emp_id': '303', 'total_hours': '40',
                     'standard_pay_rate': '28',
                     'fed_withholding': '0', 'state_withholding': '0'})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM payroll_runs WHERE emp_id=303 AND is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        gross = float(row['gross_pay'])
        expected_ss = round(gross * 0.062, 2)
        self.assertAlmostEqual(float(row['ss_withheld']), expected_ss, places=1)

    def test_medicare_withheld_computed(self):
        """Medicare = gross × 1.45%."""
        _create_run({'emp_id': '301', 'total_hours': '40',
                     'standard_pay_rate': '32',
                     'fed_withholding': '0', 'state_withholding': '0'})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM payroll_runs WHERE emp_id=301 AND is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        gross = float(row['gross_pay'])
        expected_med = round(gross * 0.0145, 2)
        self.assertAlmostEqual(float(row['medicare_withheld']), expected_med, places=1)

    def test_employer_ss_computed(self):
        """Employer SS match = gross × 6.2%."""
        _create_run({'emp_id': '302', 'total_hours': '40',
                     'standard_pay_rate': '45',
                     'fed_withholding': '0', 'state_withholding': '0'})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM payroll_runs WHERE emp_id=302 AND is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        gross = float(row['gross_pay'])
        expected = round(gross * 0.062, 2)
        self.assertAlmostEqual(float(row['employer_ss']), expected, places=1)

    def test_net_pay_computed(self):
        """net_pay = gross - total_withheld."""
        _create_run({'emp_id': '301', 'total_hours': '40',
                     'standard_pay_rate': '32',
                     'fed_withholding': '50', 'state_withholding': '20'})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM payroll_runs WHERE emp_id=301 AND is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertAlmostEqual(
            float(row['net_pay']),
            float(row['gross_pay']) - float(row['total_withheld']),
            places=2
        )

    def test_fed_state_witholding_stored(self):
        _create_run({'fed_withholding': '75.00', 'state_withholding': '30.00'})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM payroll_runs WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertAlmostEqual(float(row['fed_withholding']), 75.0)
        self.assertAlmostEqual(float(row['state_withholding']), 30.0)

    def test_missing_emp_id_rejected(self):
        r = CLIENT.post('/payroll/new', data={
            'pay_period_start': '2025-01-06',
            'pay_period_end': '2025-01-12',
            'total_hours': '40',
        }, follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_missing_start_rejected(self):
        r = CLIENT.post('/payroll/new', data={
            'emp_id': '301',
            'pay_period_end': '2025-01-12',
            'total_hours': '40',
        }, follow_redirects=True)
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestPayrollDetail(unittest.TestCase):

    def setUp(self):
        _create_run({'emp_id': '301', 'total_hours': '40',
                     'standard_pay_rate': '32', 'overtime_pay_rate': '48',
                     'fed_withholding': '50', 'state_withholding': '20'})
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM payroll_runs WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.run_id = row['id']

    def test_detail_loads(self):
        r = CLIENT.get(f'/payroll/{self.run_id}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Alice', r.data)

    def test_detail_shows_earnings(self):
        r = CLIENT.get(f'/payroll/{self.run_id}')
        self.assertIn(b'Standard Pay', r.data)
        self.assertIn(b'Net Pay', r.data)

    def test_detail_missing_redirects(self):
        r = CLIENT.get('/payroll/99999', follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Payroll', r.data)

    def test_edit_form_loads(self):
        r = CLIENT.get(f'/payroll/{self.run_id}/edit')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'total_hours', r.data)


# ════════════════════════════════════════════════════════════════
class TestPayrollEdit(unittest.TestCase):

    def setUp(self):
        _create_run({'emp_id': '302', 'total_hours': '40',
                     'standard_pay_rate': '45', 'overtime_pay_rate': '67.5',
                     'fed_withholding': '60', 'state_withholding': '25',
                     'pay_period_start': '2025-02-03',
                     'pay_period_end': '2025-02-09'})
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM payroll_runs WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.run_id = row['id']

    def test_edit_updates_hours(self):
        CLIENT.post(f'/payroll/{self.run_id}/edit', data={
            'emp_id': '302',
            'pay_period_start': '2025-02-03',
            'pay_period_end': '2025-02-09',
            'total_hours': '45',
            'holiday_hours': '0',
            'standard_pay_rate': '45',
            'overtime_pay_rate': '67.5',
            'fed_withholding': '60',
            'state_withholding': '25',
        }, follow_redirects=False)
        conn = get_connection()
        row = conn.execute("SELECT * FROM payroll_runs WHERE id=?", [self.run_id]).fetchone()
        conn.close()
        self.assertAlmostEqual(float(row['standard_hours']), 40.0)
        self.assertAlmostEqual(float(row['overtime_hours']), 5.0)

    def test_edit_recalculates_taxes(self):
        CLIENT.post(f'/payroll/{self.run_id}/edit', data={
            'emp_id': '302',
            'pay_period_start': '2025-02-03',
            'pay_period_end': '2025-02-09',
            'total_hours': '40',
            'holiday_hours': '0',
            'standard_pay_rate': '50',  # changed rate
            'overtime_pay_rate': '75',
            'fed_withholding': '60',
            'state_withholding': '25',
        }, follow_redirects=False)
        conn = get_connection()
        row = conn.execute("SELECT * FROM payroll_runs WHERE id=?", [self.run_id]).fetchone()
        conn.close()
        # Gross should reflect new rate
        self.assertAlmostEqual(float(row['gross_pay']), 40 * 50.0)


# ════════════════════════════════════════════════════════════════
class TestPayrollCheckPrinted(unittest.TestCase):

    def setUp(self):
        _create_run({'emp_id': '303', 'total_hours': '38',
                     'standard_pay_rate': '28'})
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM payroll_runs WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.run_id = row['id']

    def test_mark_printed(self):
        r = _post_json(f'/payroll/{self.run_id}/mark-printed', {'check_number': '1099'})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        conn = get_connection()
        row = conn.execute("SELECT check_printed, check_number FROM payroll_runs WHERE id=?",
                           [self.run_id]).fetchone()
        conn.close()
        self.assertEqual(row['check_printed'], 1)
        self.assertEqual(row['check_number'], '1099')

    def test_mark_printed_no_check_number(self):
        """Mark printed with no check number still succeeds."""
        r = _post_json(f'/payroll/{self.run_id}/mark-printed', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        conn = get_connection()
        row = conn.execute("SELECT check_printed FROM payroll_runs WHERE id=?",
                           [self.run_id]).fetchone()
        conn.close()
        self.assertEqual(row['check_printed'], 1)


# ════════════════════════════════════════════════════════════════
class TestPayrollDelete(unittest.TestCase):

    def test_delete_is_soft(self):
        _create_run({'emp_id': '301'})
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM payroll_runs WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        run_id = row['id']

        r = _post_json(f'/payroll/{run_id}/delete', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])

        conn = get_connection()
        row = conn.execute("SELECT is_deleted FROM payroll_runs WHERE id=?", [run_id]).fetchone()
        conn.close()
        self.assertEqual(row['is_deleted'], 1)

    def test_deleted_run_not_in_list(self):
        _create_run({'emp_id': '303'})
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM payroll_runs WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        run_id = row['id']
        _post_json(f'/payroll/{run_id}/delete', {})

        r = CLIENT.get('/payroll?year=2025')
        # The deleted run should not crash the list page
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestPayrollAPIs(unittest.TestCase):

    def test_rates_api(self):
        r = CLIENT.get('/api/payroll/rates?emp_id=301&start=2025-06-01')
        d = json.loads(r.data)
        self.assertIn('std_rate', d)
        self.assertIn('ot_rate', d)
        self.assertAlmostEqual(float(d['std_rate']), 32.0)   # cost rate for Alice
        self.assertAlmostEqual(float(d['ot_rate']),  48.0)   # 32 × 1.5

    def test_rates_api_no_emp(self):
        r = CLIENT.get('/api/payroll/rates')
        d = json.loads(r.data)
        self.assertEqual(d['std_rate'], 0)

    def test_timesheet_hours_api(self):
        """Should return 48 hours for Alice in Jan 6-12 from seed data."""
        r = CLIENT.get('/api/payroll/timesheet-hours?emp_id=301&start=2025-01-06&end=2025-01-12')
        d = json.loads(r.data)
        self.assertAlmostEqual(float(d['hours']), 48.0)

    def test_timesheet_hours_api_no_params(self):
        r = CLIENT.get('/api/payroll/timesheet-hours')
        d = json.loads(r.data)
        self.assertEqual(d['hours'], 0)

    def test_preview_api(self):
        r = CLIENT.get('/api/payroll/preview?emp_id=301&hours=40&std_rate=32&ot_rate=48&fed_wh=50&state_wh=20&year=2025')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertAlmostEqual(float(d['gross_pay']), 1280.0)
        self.assertAlmostEqual(float(d['standard_hours']), 40.0)
        self.assertAlmostEqual(float(d['overtime_hours']), 0.0)
        self.assertIn('ss_withheld', d)
        self.assertIn('net_pay', d)

    def test_preview_api_with_ot(self):
        r = CLIENT.get('/api/payroll/preview?emp_id=302&hours=44&std_rate=45&ot_rate=67.5&year=2025')
        d = json.loads(r.data)
        self.assertAlmostEqual(float(d['standard_hours']), 40.0)
        self.assertAlmostEqual(float(d['overtime_hours']), 4.0)
        expected_gross = 40*45 + 4*67.5
        self.assertAlmostEqual(float(d['gross_pay']), expected_gross)

    def test_ytd_api(self):
        # Ensure at least one run exists for 2025
        _create_run({'emp_id': '301', 'total_hours': '40', 'standard_pay_rate': '32'})
        r = CLIENT.get('/api/payroll/ytd?year=2025')
        self.assertEqual(r.status_code, 200)
        data = json.loads(r.data)
        self.assertIsInstance(data, list)
        # Should have rows for employees with runs
        emp_ids = [row['emp_id'] for row in data]
        self.assertGreater(len(emp_ids), 0)

    def test_ytd_api_has_required_fields(self):
        r = CLIENT.get('/api/payroll/ytd?year=2025')
        data = json.loads(r.data)
        if data:
            row = data[0]
            for field in ['emp_id', 'emp_name', 'ytd_gross', 'ytd_net',
                          'ytd_hours', 'ytd_ss_withheld', 'ytd_medicare_withheld']:
                self.assertIn(field, row, f'Missing field: {field}')

    def test_employer_liability_api(self):
        r = CLIENT.get('/api/payroll/employer-liability?year=2025')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn('by_quarter', d)
        self.assertIn('total', d)
        self.assertIn('year', d)
        self.assertEqual(d['year'], 2025)

    def test_employer_liability_by_quarter(self):
        r = CLIENT.get('/api/payroll/employer-liability?year=2025')
        d = json.loads(r.data)
        for q in d['by_quarter']:
            self.assertIn(q['quarter'], ['Q1', 'Q2', 'Q3', 'Q4'])
            self.assertIn('er_ss', q)
            self.assertIn('futa', q)
            self.assertIn('suta', q)

    def test_employer_liability_total_has_fields(self):
        r = CLIENT.get('/api/payroll/employer-liability?year=2025')
        d = json.loads(r.data)
        t = d['total']
        for field in ['gross', 'er_ss', 'er_medicare', 'futa', 'suta']:
            self.assertIn(field, t, f'Missing field in total: {field}')


# ════════════════════════════════════════════════════════════════
class TestPayrollExport(unittest.TestCase):

    def test_export_returns_csv(self):
        r = CLIENT.get('/payroll/export?year=2025')
        self.assertEqual(r.status_code, 200)
        self.assertIn('text/csv', r.content_type)
        self.assertIn(b'emp_name', r.data)
        self.assertIn(b'gross_pay', r.data)

    def test_export_filtered_by_emp(self):
        r = CLIENT.get('/payroll/export?year=2025&emp=301')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'emp_name', r.data)

    def test_export_different_year(self):
        r = CLIENT.get('/payroll/export?year=2024')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestPayrollOTHelpers(unittest.TestCase):
    """Unit tests for OT split and compute helpers."""

    def test_split_ot_exactly_40(self):
        from routes_phase6 import _split_ot
        std, ot = _split_ot(40.0)
        self.assertAlmostEqual(std, 40.0)
        self.assertAlmostEqual(ot, 0.0)

    def test_split_ot_under_40(self):
        from routes_phase6 import _split_ot
        std, ot = _split_ot(32.5)
        self.assertAlmostEqual(std, 32.5)
        self.assertAlmostEqual(ot, 0.0)

    def test_split_ot_over_40(self):
        from routes_phase6 import _split_ot
        std, ot = _split_ot(48.0)
        self.assertAlmostEqual(std, 40.0)
        self.assertAlmostEqual(ot, 8.0)

    def test_split_ot_zero(self):
        from routes_phase6 import _split_ot
        std, ot = _split_ot(0.0)
        self.assertAlmostEqual(std, 0.0)
        self.assertAlmostEqual(ot, 0.0)

    def test_compute_run_no_ot(self):
        from routes_phase6 import _compute_run
        result = _compute_run(40.0, 32.0, 48.0, 0.0, 32.0)
        self.assertAlmostEqual(result['standard_hours'], 40.0)
        self.assertAlmostEqual(result['overtime_hours'], 0.0)
        self.assertAlmostEqual(result['gross_pay'], 40 * 32)

    def test_compute_run_with_ot(self):
        from routes_phase6 import _compute_run
        result = _compute_run(44.0, 30.0, 45.0, 0.0, 30.0)
        self.assertAlmostEqual(result['standard_hours'], 40.0)
        self.assertAlmostEqual(result['overtime_hours'], 4.0)
        expected = 40*30 + 4*45
        self.assertAlmostEqual(result['gross_pay'], expected)

    def test_compute_run_with_holiday(self):
        from routes_phase6 import _compute_run
        result = _compute_run(32.0, 25.0, 37.5, 8.0, 25.0)
        self.assertAlmostEqual(result['holiday_hours'], 8.0)
        self.assertAlmostEqual(result['holiday_pay'], 8 * 25)
        expected = 32*25 + 8*25
        self.assertAlmostEqual(result['gross_pay'], expected)

    def test_ot_1point5x_default(self):
        """Default OT rate is exactly 1.5× standard rate."""
        from routes_phase6 import _compute_run
        std_rate = 32.0
        ot_rate  = std_rate * 1.5
        result   = _compute_run(44.0, std_rate, ot_rate, 0.0, std_rate)
        self.assertAlmostEqual(result['overtime_pay'], 4 * ot_rate)


# ════════════════════════════════════════════════════════════════
class TestPayrollSSWageCap(unittest.TestCase):
    """Test Social Security wage cap enforcement."""

    def test_ss_withheld_zero_past_cap(self):
        """If employee has already hit SS cap, ss_withheld should be 0."""
        from automations import calculate_payroll_taxes
        # Insert many payroll runs to push past the cap
        conn = get_connection()
        cap = conn.execute(
            "SELECT cap_amount FROM ss_wage_caps WHERE year=2025"
        ).fetchone()['cap_amount']
        conn.close()

        # Direct DB insert: push Alice to near-cap wages already
        with _db() as conn:
            conn.execute("""
                INSERT INTO payroll_runs (emp_id, year, gross_pay, net_pay,
                    ss_withheld, medicare_withheld, run_date, pay_period_start, pay_period_end)
                VALUES (301, 2025, ?, 0, 0, 0, date('now'), '2025-01-01', '2025-12-31')
            """, [cap + 1000])  # already past cap

        # Now compute taxes for new gross pay
        conn = get_connection()
        taxes = calculate_payroll_taxes(301, 2025, 5000.0, conn=conn)
        conn.close()
        self.assertAlmostEqual(taxes['ss_withheld'], 0.0)
        self.assertAlmostEqual(taxes['employer_ss'], 0.0)

    def test_ss_applies_below_cap(self):
        """Below cap, SS withheld = gross × 6.2%."""
        from automations import calculate_payroll_taxes
        # Use employee 303 who has no prior runs yet in this test class
        conn = get_connection()
        taxes = calculate_payroll_taxes(303, 2024, 1000.0, conn=conn)
        conn.close()
        self.assertAlmostEqual(taxes['ss_withheld'], round(1000 * 0.062, 2))


# ════════════════════════════════════════════════════════════════
class TestPayrollRegression(unittest.TestCase):
    """Sanity-check existing phases still pass after Phase 6 wiring."""

    def test_invoices_still_loads(self):
        r = CLIENT.get('/invoices')
        self.assertEqual(r.status_code, 200)

    def test_timesheet_still_loads(self):
        r = CLIENT.get('/timesheet')
        self.assertEqual(r.status_code, 200)

    def test_clients_still_loads(self):
        r = CLIENT.get('/clients')
        self.assertEqual(r.status_code, 200)

    def test_dashboard_still_loads(self):
        r = CLIENT.get('/')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    total  = result.testsRun
    if result.wasSuccessful():
        print(f"\n✅ All {total} Phase 6 tests passed!")
    else:
        fails = len(result.failures)
        errs  = len(result.errors)
        print(f"\n❌ {fails} failure(s), {errs} error(s) out of {total}")
    sys.exit(0 if result.wasSuccessful() else 1)
