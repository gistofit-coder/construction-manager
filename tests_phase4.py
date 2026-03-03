"""
Phase 4 Tests — Timesheet
Run: python3 tests_phase4.py
"""
import os, sys, json, unittest
from datetime import date, timedelta

TEST_DB = '/tmp/test_p4.db'
os.environ['CONSTRUCTION_DB'] = TEST_DB
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

from database import init_db, get_connection
init_db(TEST_DB)

from app import app as flask_app
flask_app.config['TESTING'] = True

# ── Seed test data ────────────────────────────────────────────
from database import db as _db
with _db() as conn:
    conn.execute("""
        INSERT INTO clients (customer_id, full_name, last_name, status)
        VALUES ('TST24','Test Client','Client','Active')
    """)
    conn.execute("""
        INSERT INTO jobs (job_code, client_id, description, status)
        VALUES ('TST-24', 1, 'Test job', 'Active')
    """)
    conn.execute("""
        INSERT INTO employees (emp_id, first_name, last_name, status, occupation)
        VALUES (101, 'Alice', 'Smith', 'Active', 'Carpenter')
    """)
    conn.execute("""
        INSERT INTO employees (emp_id, first_name, last_name, status, occupation)
        VALUES (102, 'Bob', 'Jones', 'Active', 'Laborer')
    """)
    # Rates effective Jan 1 2025
    conn.execute("""
        INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour)
        VALUES (101, '2025-01-01', 95.00, 45.00)
    """)
    conn.execute("""
        INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour)
        VALUES (102, '2025-01-01', 75.00, 35.00)
    """)
    # Rate change for Alice in July 2025
    conn.execute("""
        INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour)
        VALUES (101, '2025-07-01', 100.00, 50.00)
    """)


class TestTimesheetList(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_list_loads(self):
        r = self.client.get('/timesheet')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Timesheet', r.data)

    def test_week_view_loads(self):
        r = self.client.get('/timesheet?view=week')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Week of', r.data)

    def test_week_view_with_specific_week(self):
        r = self.client.get('/timesheet?view=week&week=2025-03-10')
        self.assertEqual(r.status_code, 200)

    def test_list_filter_by_employee(self):
        r = self.client.get('/timesheet?view=list&emp=101')
        self.assertEqual(r.status_code, 200)

    def test_list_filter_by_job(self):
        r = self.client.get('/timesheet?view=list&job=TST-24')
        self.assertEqual(r.status_code, 200)

    def test_list_filter_by_date_range(self):
        r = self.client.get('/timesheet?view=list&from=2025-01-01&to=2025-12-31')
        self.assertEqual(r.status_code, 200)


class TestTimesheetCreate(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_create_basic_entry(self):
        r = self.client.post('/timesheet/new',
            data=json.dumps({
                'entry_date': '2025-03-15',
                'emp_id': '101',
                'hours': '8',
                'job_code': 'TST-24',
                'description': 'Framing work',
            }),
            content_type='application/json')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        row = d['row']
        self.assertEqual(row['emp_id'], 101)
        self.assertEqual(float(row['hours']), 8.0)

    def test_rate_locked_at_entry_date_jan(self):
        """Jan 2025 entry should use Alice's Jan rate ($95 bill, $45 cost)."""
        r = self.client.post('/timesheet/new',
            data=json.dumps({
                'entry_date': '2025-03-15',
                'emp_id': '101',
                'hours': '8',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertEqual(float(d['row']['bill_rate']), 95.00)
        self.assertEqual(float(d['row']['cost_rate']), 45.00)

    def test_rate_locked_at_entry_date_july(self):
        """July 2025 entry should use Alice's July rate ($100 bill, $50 cost)."""
        r = self.client.post('/timesheet/new',
            data=json.dumps({
                'entry_date': '2025-08-01',
                'emp_id': '101',
                'hours': '8',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertEqual(float(d['row']['bill_rate']), 100.00)
        self.assertEqual(float(d['row']['cost_rate']), 50.00)

    def test_bill_amount_computed_correctly(self):
        """8 hrs * $95 = $760 bill, 8 hrs * $45 = $360 cost."""
        r = self.client.post('/timesheet/new',
            data=json.dumps({
                'entry_date': '2025-04-10',
                'emp_id': '101',
                'hours': '8',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertAlmostEqual(float(d['row']['bill_amount']), 760.00)
        self.assertAlmostEqual(float(d['row']['cost_amount']), 360.00)

    def test_expenses_added_to_cost(self):
        """6 hrs * $45 cost + $50 expenses = $320 cost."""
        r = self.client.post('/timesheet/new',
            data=json.dumps({
                'entry_date': '2025-04-11',
                'emp_id': '101',
                'hours': '6',
                'expenses': '50',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertAlmostEqual(float(d['row']['cost_amount']), 6 * 45 + 50)

    def test_person_label_stored(self):
        """person_label should be set to 'Alice Smith'."""
        r = self.client.post('/timesheet/new',
            data=json.dumps({
                'entry_date': '2025-04-12',
                'emp_id': '101',
                'hours': '4',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertEqual(d['row']['person_label'], 'Alice Smith')

    def test_manual_rate_override(self):
        """Caller can override the auto-looked-up rate."""
        r = self.client.post('/timesheet/new',
            data=json.dumps({
                'entry_date': '2025-04-13',
                'emp_id': '101',
                'hours': '4',
                'bill_rate': '200',
                'cost_rate': '100',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertEqual(float(d['row']['bill_rate']), 200.00)
        self.assertEqual(float(d['row']['cost_rate']), 100.00)

    def test_missing_emp_id_returns_error(self):
        r = self.client.post('/timesheet/new',
            data=json.dumps({'entry_date': '2025-04-15', 'hours': '8'}),
            content_type='application/json')
        self.assertEqual(r.status_code, 400)

    def test_missing_hours_returns_error(self):
        r = self.client.post('/timesheet/new',
            data=json.dumps({'entry_date': '2025-04-15', 'emp_id': '101'}),
            content_type='application/json')
        self.assertEqual(r.status_code, 400)

    def test_missing_date_returns_error(self):
        r = self.client.post('/timesheet/new',
            data=json.dumps({'emp_id': '101', 'hours': '8'}),
            content_type='application/json')
        self.assertEqual(r.status_code, 400)


class TestTimesheetBulkEntry(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_bulk_creates_multiple_entries(self):
        r = self.client.post('/timesheet/bulk',
            data=json.dumps({
                'entry_date': '2025-05-01',
                'job_code': 'TST-24',
                'description': 'Bulk test',
                'entries': [
                    {'emp_id': 101, 'hours': 8},
                    {'emp_id': 102, 'hours': 7.5},
                ]
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertEqual(d['created'], 2)
        self.assertEqual(len(d['rows']), 2)

    def test_bulk_uses_correct_rates_per_employee(self):
        r = self.client.post('/timesheet/bulk',
            data=json.dumps({
                'entry_date': '2025-05-02',
                'entries': [
                    {'emp_id': 101, 'hours': 8},  # Alice: $95 bill
                    {'emp_id': 102, 'hours': 8},  # Bob: $75 bill
                ]
            }),
            content_type='application/json')
        d = json.loads(r.data)
        rows_by_emp = {r['emp_id']: r for r in d['rows']}
        self.assertEqual(float(rows_by_emp[101]['bill_rate']), 95.00)
        self.assertEqual(float(rows_by_emp[102]['bill_rate']), 75.00)

    def test_bulk_skips_zero_hours(self):
        r = self.client.post('/timesheet/bulk',
            data=json.dumps({
                'entry_date': '2025-05-03',
                'entries': [
                    {'emp_id': 101, 'hours': 8},
                    {'emp_id': 102, 'hours': 0},   # should be skipped
                ]
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertEqual(d['created'], 1)

    def test_bulk_skips_blank_emp_id(self):
        r = self.client.post('/timesheet/bulk',
            data=json.dumps({
                'entry_date': '2025-05-04',
                'entries': [
                    {'emp_id': 101, 'hours': 6},
                    {'emp_id': None, 'hours': 8},   # should be skipped
                ]
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertEqual(d['created'], 1)

    def test_bulk_no_entries_returns_error(self):
        r = self.client.post('/timesheet/bulk',
            data=json.dumps({'entry_date': '2025-05-05', 'entries': []}),
            content_type='application/json')
        self.assertEqual(r.status_code, 400)

    def test_bulk_no_date_returns_error(self):
        r = self.client.post('/timesheet/bulk',
            data=json.dumps({'entries': [{'emp_id': 101, 'hours': 8}]}),
            content_type='application/json')
        self.assertEqual(r.status_code, 400)


class TestTimesheetInlinePatch(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()
        # Create a row to patch
        r = self.client.post('/timesheet/new',
            data=json.dumps({
                'entry_date': '2025-06-01',
                'emp_id': '101',
                'hours': '8',
                'job_code': 'TST-24',
            }),
            content_type='application/json')
        self.row_id = json.loads(r.data)['row']['id']

    def test_patch_description(self):
        r = self.client.post(f'/api/timesheet/{self.row_id}/patch',
            data=json.dumps({'field': 'description', 'value': 'Updated desc'}),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertEqual(d['row']['description'], 'Updated desc')

    def test_patch_hours_recomputes_amounts(self):
        """Patching hours should recompute bill_amount and cost_amount."""
        r = self.client.post(f'/api/timesheet/{self.row_id}/patch',
            data=json.dumps({'field': 'hours', 'value': '10'}),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        # 10 hrs * $95 = $950
        self.assertAlmostEqual(float(d['row']['bill_amount']), 950.00)
        self.assertAlmostEqual(float(d['row']['cost_amount']), 450.00)

    def test_patch_bill_rate_recomputes_amounts(self):
        r = self.client.post(f'/api/timesheet/{self.row_id}/patch',
            data=json.dumps({'field': 'bill_rate', 'value': '120'}),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        # 10 hrs * $120 (after previous patch)
        self.assertAlmostEqual(float(d['row']['bill_amount']),
                               float(d['row']['hours']) * 120.0)

    def test_patch_job_code(self):
        r = self.client.post(f'/api/timesheet/{self.row_id}/patch',
            data=json.dumps({'field': 'job_code', 'value': 'TST-24'}),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertTrue(d['success'])

    def test_patch_blocked_field(self):
        r = self.client.post(f'/api/timesheet/{self.row_id}/patch',
            data=json.dumps({'field': 'is_deleted', 'value': '1'}),
            content_type='application/json')
        self.assertEqual(r.status_code, 403)

    def test_patch_nonexistent_row(self):
        r = self.client.post('/api/timesheet/99999/patch',
            data=json.dumps({'field': 'description', 'value': 'test'}),
            content_type='application/json')
        self.assertEqual(r.status_code, 404)


class TestTimesheetDelete(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_delete_is_soft(self):
        # Create a row
        r = self.client.post('/timesheet/new',
            data=json.dumps({'entry_date': '2025-07-01', 'emp_id': '102', 'hours': '4'}),
            content_type='application/json')
        row_id = json.loads(r.data)['row']['id']
        # Delete it
        dr = self.client.post(f'/timesheet/{row_id}/delete',
            content_type='application/json')
        d = json.loads(dr.data)
        self.assertTrue(d['success'])
        # Verify soft-deleted
        conn = get_connection()
        row = conn.execute("SELECT is_deleted FROM timesheet WHERE id=?", [row_id]).fetchone()
        conn.close()
        self.assertEqual(row['is_deleted'], 1)


class TestTimesheetRateAPI(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_rate_lookup_jan(self):
        r = self.client.get('/api/timesheet/rate?emp_id=101&date=2025-03-15')
        d = json.loads(r.data)
        self.assertEqual(float(d['bill_rate']), 95.00)
        self.assertEqual(float(d['cost_rate']), 45.00)
        self.assertEqual(d['person_label'], 'Alice Smith')

    def test_rate_lookup_july(self):
        r = self.client.get('/api/timesheet/rate?emp_id=101&date=2025-08-01')
        d = json.loads(r.data)
        self.assertEqual(float(d['bill_rate']), 100.00)
        self.assertEqual(float(d['cost_rate']), 50.00)

    def test_rate_lookup_bob(self):
        r = self.client.get('/api/timesheet/rate?emp_id=102&date=2025-06-01')
        d = json.loads(r.data)
        self.assertEqual(float(d['bill_rate']), 75.00)
        self.assertEqual(d['person_label'], 'Bob Jones')

    def test_rate_before_any_rate_returns_zero(self):
        r = self.client.get('/api/timesheet/rate?emp_id=101&date=2020-01-01')
        d = json.loads(r.data)
        self.assertEqual(float(d['bill_rate']), 0.0)

    def test_rate_missing_params(self):
        r = self.client.get('/api/timesheet/rate')
        d = json.loads(r.data)
        self.assertEqual(float(d['bill_rate']), 0.0)


class TestTimesheetSummaryAPIs(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_job_summary(self):
        r = self.client.get('/api/timesheet/job-summary?year=2025')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIsInstance(d, list)
        if d:
            self.assertIn('job_code', d[0])
            self.assertIn('total_hours', d[0])
            self.assertIn('total_bill', d[0])
            self.assertIn('total_cost', d[0])

    def test_employee_summary(self):
        r = self.client.get('/api/timesheet/employee-summary?year=2025')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIsInstance(d, list)
        if d:
            self.assertIn('emp_id', d[0])
            self.assertIn('total_hours', d[0])

    def test_weekly_totals(self):
        r = self.client.get('/api/timesheet/weekly-totals?week=2025-05-01')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIsInstance(d, list)

    def test_job_summary_custom_date_range(self):
        r = self.client.get('/api/timesheet/job-summary?from=2025-05-01&to=2025-05-31')
        self.assertEqual(r.status_code, 200)

    def test_employee_summary_contains_alice(self):
        r = self.client.get('/api/timesheet/employee-summary?year=2025')
        d = json.loads(r.data)
        names = [e.get('emp_name', '') for e in d]
        # Alice should appear since we created entries for emp 101
        alice_found = any('Alice' in name for name in names)
        self.assertTrue(alice_found, f"Alice not found in {names}")


class TestTimesheetExport(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_export_returns_csv(self):
        r = self.client.get('/timesheet/export')
        self.assertEqual(r.status_code, 200)
        self.assertIn('text/csv', r.content_type)
        self.assertIn(b'entry_date', r.data)

    def test_export_filtered_by_employee(self):
        r = self.client.get('/timesheet/export?emp=101')
        self.assertEqual(r.status_code, 200)
        content = r.data.decode()
        # All data rows should be for emp_id 101
        for line in content.strip().split('\n')[1:]:
            if line.strip():
                self.assertIn('101', line)

    def test_export_filtered_by_job(self):
        r = self.client.get('/timesheet/export?job=TST-24')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'TST-24', r.data)


class TestRateLockInvariant(unittest.TestCase):
    """
    Verify that the rate lock-in invariant holds:
    Historical entries should not change when new rates are added.
    """
    def setUp(self):
        self.client = flask_app.test_client()

    def test_historical_rate_unchanged_after_new_rate(self):
        # Create an entry in March 2025 (should use $95 rate)
        r = self.client.post('/timesheet/new',
            data=json.dumps({
                'entry_date': '2025-03-20',
                'emp_id': '101',
                'hours': '8',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        stored_bill_rate = float(d['row']['bill_rate'])
        stored_bill_amount = float(d['row']['bill_amount'])
        row_id = d['row']['id']

        # Add a new rate effective in the far future (2030) so it doesn't
        # contaminate any other test entries in 2025
        conn = get_connection()
        conn.execute("""
            INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour)
            VALUES (101, '2030-01-01', 999.00, 500.00)
        """)
        conn.commit()
        conn.close()

        # Fetch the stored row — bill_rate should still be $95 (locked at save time)
        conn = get_connection()
        row = conn.execute("SELECT bill_rate, bill_amount FROM timesheet WHERE id=?", [row_id]).fetchone()
        conn.close()
        self.assertEqual(float(row['bill_rate']), stored_bill_rate,
                         "Rate lock violated: stored rate changed after new rate added")
        self.assertEqual(float(row['bill_amount']), stored_bill_amount,
                         "Rate lock violated: stored amount changed after new rate added")


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    total  = result.testsRun
    if result.wasSuccessful():
        print(f"\n✅ All {total} Phase 4 tests passed!")
    else:
        print(f"\n❌ {len(result.failures)} failure(s), {len(result.errors)} error(s) out of {total}")
    sys.exit(0 if result.wasSuccessful() else 1)
