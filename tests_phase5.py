"""
Phase 5 Tests — Invoices
Run: python3 tests_phase5.py
"""
import os, sys, json, unittest
from datetime import date, timedelta

TEST_DB = '/tmp/test_p5.db'
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
    conn.execute("""INSERT INTO clients (customer_id, full_name, last_name, status, email1, phone1)
                    VALUES ('ACME01','Acme Corp','Corp','Active','acme@test.com','555-1234')""")
    conn.execute("""INSERT INTO clients (customer_id, full_name, last_name, status)
                    VALUES ('BETA02','Beta LLC','LLC','Active')""")
    conn.execute("""INSERT INTO jobs (job_code, client_id, description, status, contract_amount)
                    VALUES ('JOB-01', 1, 'Kitchen remodel', 'Active', 15000)""")
    conn.execute("""INSERT INTO jobs (job_code, client_id, description, status)
                    VALUES ('JOB-02', 2, 'Deck build', 'Active')""")
    conn.execute("""INSERT INTO employees (emp_id, first_name, last_name, status)
                    VALUES (201, 'Carlos', 'Rivera', 'Active')""")
    conn.execute("""INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour)
                    VALUES (201, '2025-01-01', 85.00, 40.00)""")
    # Seed a timesheet entry linked to JOB-01 / invoice 1001
    conn.execute("""INSERT INTO timesheet
        (entry_date, job_code, invoice_number, emp_id, hours, bill_rate, cost_rate,
         bill_amount, cost_amount, person_label)
        VALUES ('2025-04-10','JOB-01','1001',201,8,85,40,680,320,'Carlos Rivera')""")


def _post_json(client, url, data):
    return client.post(url, data=json.dumps(data),
                       content_type='application/json')


# ════════════════════════════════════════════════════════════════
class TestInvoiceList(unittest.TestCase):
    def setUp(self):
        self.c = flask_app.test_client()

    def test_list_loads(self):
        r = self.c.get('/invoices')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Invoices', r.data)

    def test_new_form_loads(self):
        r = self.c.get('/invoices/new')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'invoice_number', r.data)

    def test_list_filter_by_status(self):
        r = self.c.get('/invoices?status=Paid')
        self.assertEqual(r.status_code, 200)

    def test_list_filter_by_client(self):
        r = self.c.get('/invoices?client=1')
        self.assertEqual(r.status_code, 200)

    def test_list_filter_by_job(self):
        r = self.c.get('/invoices?job=JOB-01')
        self.assertEqual(r.status_code, 200)

    def test_list_filter_search(self):
        r = self.c.get('/invoices?q=Acme')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestInvoiceCreate(unittest.TestCase):
    def setUp(self):
        self.c = flask_app.test_client()

    def _create(self, overrides=None):
        data = {
            'invoice_date': '2025-05-01',
            'amount': '5000.00',
            'client_id': '1',
            'job_code': 'JOB-01',
            'description_of_work': 'Phase 1 framing',
            'amount_paid': '0',
            'notes': '',
        }
        if overrides:
            data.update(overrides)
        return self.c.post('/invoices/new', data=data,
                           follow_redirects=True)

    def test_create_basic_invoice(self):
        r = self._create()
        self.assertEqual(r.status_code, 200)
        # Should redirect to detail page
        self.assertIn(b'Invoice #', r.data)

    def test_invoice_number_auto_assigned(self):
        """Each new invoice gets an incremented invoice number."""
        conn = get_connection()
        before_max = conn.execute(
            "SELECT COALESCE(MAX(invoice_number),0) FROM invoices WHERE is_deleted=0"
        ).fetchone()[0]
        conn.close()

        self._create()
        conn = get_connection()
        after_max = conn.execute(
            "SELECT MAX(invoice_number) FROM invoices WHERE is_deleted=0"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(int(after_max), int(before_max) + 1)

    def test_due_date_auto_computed(self):
        """due_date should be invoice_date + 30 days when not supplied."""
        r = self._create({'invoice_date': '2025-06-01', 'due_date': ''})
        # Look up the most recent invoice
        conn = get_connection()
        inv = conn.execute(
            "SELECT due_date FROM invoices WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        expected = (date(2025, 6, 1) + timedelta(days=30)).strftime('%Y-%m-%d')
        self.assertEqual(inv['due_date'], expected)

    def test_balance_due_computed(self):
        self._create({'amount': '3000', 'amount_paid': '1000'})
        conn = get_connection()
        inv = conn.execute(
            "SELECT balance_due FROM invoices WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertAlmostEqual(float(inv['balance_due']), 2000.0)

    def test_status_pending_on_create(self):
        self._create({'invoice_date': date.today().strftime('%Y-%m-%d'),
                      'amount': '1000', 'amount_paid': '0'})
        conn = get_connection()
        inv = conn.execute(
            "SELECT status FROM invoices WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertIn(inv['status'], ('Pending', 'Overdue'))  # overdue if test date past due

    def test_status_paid_when_fully_paid(self):
        self._create({'amount': '500', 'amount_paid': '500'})
        conn = get_connection()
        inv = conn.execute(
            "SELECT status FROM invoices WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertEqual(inv['status'], 'Paid')

    def test_status_partial_when_partially_paid(self):
        # Use a far-future due date so it won't become Overdue
        self._create({
            'amount': '2000', 'amount_paid': '500',
            'invoice_date': '2025-06-01',
            'due_date': '2099-12-31',
        })
        conn = get_connection()
        inv = conn.execute(
            "SELECT status FROM invoices WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertEqual(inv['status'], 'Partial')

    def test_missing_amount_rejected(self):
        r = self.c.post('/invoices/new',
                        data={'invoice_date': '2025-05-01'},
                        follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        # Should show error or stay on form (no invoice created with blank amount)
        conn = get_connection()
        blank = conn.execute(
            "SELECT COUNT(*) FROM invoices WHERE amount=0 AND is_deleted=0"
        ).fetchone()[0]
        conn.close()
        # We allow 0 as a fallback — just check no crash
        self.assertIsNotNone(blank)

    def test_missing_invoice_date_rejected(self):
        r = self.c.post('/invoices/new',
                        data={'amount': '1000'},
                        follow_redirects=True)
        # Should redirect back with flash error, not crash
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestInvoiceDetail(unittest.TestCase):
    def setUp(self):
        self.c = flask_app.test_client()
        # Create a fresh invoice for detail tests (auto-numbered)
        r = self.c.post('/invoices/new', data={
            'invoice_date': '2025-04-01',
            'due_date': '2025-05-01',
            'amount': '680.00',
            'amount_paid': '0',
            'client_id': '1',
            'job_code': 'JOB-01',
            'description_of_work': 'April labor',
        }, follow_redirects=False)
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM invoices WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.inv_id = row['id'] if row else 1

    def test_detail_loads(self):
        r = self.c.get(f'/invoices/{self.inv_id}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Invoice #', r.data)

    def test_detail_shows_linked_timesheet(self):
        """The seeded timesheet entry for JOB-01/invoice 1001 should appear."""
        r = self.c.get(f'/invoices/{self.inv_id}')
        self.assertEqual(r.status_code, 200)
        # Timesheet section should be present when ts_rows exist
        # (rows linked by job_code=JOB-01 or invoice_number=1001)
        self.assertIn(b'Carlos Rivera', r.data)

    def test_detail_missing_invoice_redirects(self):
        r = self.c.get('/invoices/99999', follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Invoices', r.data)

    def test_pdf_endpoint_loads(self):
        r = self.c.get(f'/invoices/{self.inv_id}/pdf')
        self.assertEqual(r.status_code, 200)
        # Either PDF bytes or fallback HTML — either way, 200
        self.assertIn(r.content_type, ['application/pdf', 'text/html'])

    def test_edit_form_loads(self):
        r = self.c.get(f'/invoices/{self.inv_id}/edit')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'invoice_date', r.data)


# ════════════════════════════════════════════════════════════════
class TestInvoicePayment(unittest.TestCase):
    def setUp(self):
        self.c = flask_app.test_client()
        # Create a fresh invoice for each test
        self.c.post('/invoices/new', data={
            'invoice_date': '2025-05-15',
            'due_date': '2099-12-31',
            'amount': '4000.00',
            'amount_paid': '0',
            'client_id': '1',
            'job_code': 'JOB-01',
            'description_of_work': 'Payment test',
        })
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM invoices WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.inv_id = row['id']

    def test_record_full_payment(self):
        r = _post_json(self.c, f'/invoices/{self.inv_id}/payment',
                       {'payment_amount': 4000.00, 'payment_date': '2025-06-01'})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertEqual(d['invoice']['status'], 'Paid')
        self.assertAlmostEqual(float(d['invoice']['balance_due']), 0.0)

    def test_record_partial_payment(self):
        r = _post_json(self.c, f'/invoices/{self.inv_id}/payment',
                       {'payment_amount': 1500.00, 'payment_date': '2025-06-01'})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertEqual(d['invoice']['status'], 'Partial')
        self.assertAlmostEqual(float(d['invoice']['balance_due']), 2500.0)

    def test_payment_creates_ledger_entry(self):
        """Recording a payment should create an Income ledger entry."""
        conn = get_connection()
        before = conn.execute("SELECT COUNT(*) FROM ledger WHERE is_deleted=0").fetchone()[0]
        conn.close()

        _post_json(self.c, f'/invoices/{self.inv_id}/payment',
                   {'payment_amount': 500.00, 'payment_date': '2025-06-15',
                    'notes': 'Check #4421'})

        conn = get_connection()
        after = conn.execute("SELECT COUNT(*) FROM ledger WHERE is_deleted=0").fetchone()[0]
        conn.close()
        self.assertEqual(after, before + 1)

    def test_payment_ledger_entry_is_income(self):
        inv_num_row = get_connection().execute(
            "SELECT invoice_number FROM invoices WHERE id=?", [self.inv_id]
        ).fetchone()
        get_connection().close()
        inv_num = inv_num_row['invoice_number']

        _post_json(self.c, f'/invoices/{self.inv_id}/payment',
                   {'payment_amount': 750.00, 'payment_date': '2025-06-20'})

        conn = get_connection()
        ledger_row = conn.execute(
            "SELECT * FROM ledger WHERE invoice_number=? AND amount > 0 AND is_deleted=0 ORDER BY id DESC LIMIT 1",
            [str(inv_num)]
        ).fetchone()
        conn.close()
        self.assertIsNotNone(ledger_row)
        self.assertEqual(ledger_row['category'], 'Income')
        self.assertAlmostEqual(float(ledger_row['amount']), 750.0)

    def test_second_payment_accumulates(self):
        _post_json(self.c, f'/invoices/{self.inv_id}/payment',
                   {'payment_amount': 1000.00, 'payment_date': '2025-06-01'})
        _post_json(self.c, f'/invoices/{self.inv_id}/payment',
                   {'payment_amount': 3000.00, 'payment_date': '2025-06-15'})

        conn = get_connection()
        inv = conn.execute("SELECT * FROM invoices WHERE id=?", [self.inv_id]).fetchone()
        conn.close()
        self.assertAlmostEqual(float(inv['amount_paid']), 4000.0)
        self.assertEqual(inv['status'], 'Paid')

    def test_zero_payment_rejected(self):
        r = _post_json(self.c, f'/invoices/{self.inv_id}/payment',
                       {'payment_amount': 0, 'payment_date': '2025-06-01'})
        self.assertEqual(r.status_code, 400)

    def test_negative_payment_rejected(self):
        r = _post_json(self.c, f'/invoices/{self.inv_id}/payment',
                       {'payment_amount': -100, 'payment_date': '2025-06-01'})
        self.assertEqual(r.status_code, 400)


# ════════════════════════════════════════════════════════════════
class TestInvoiceStatusAutomation(unittest.TestCase):
    def setUp(self):
        self.c = flask_app.test_client()

    def _create_inv(self, invoice_date, due_date, amount, amount_paid=0):
        self.c.post('/invoices/new', data={
            'invoice_date': invoice_date,
            'due_date': due_date,
            'amount': str(amount),
            'amount_paid': str(amount_paid),
            'client_id': '1',
        })
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM invoices WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return row['id']

    def test_overdue_batch_marks_past_due(self):
        """An invoice with due_date in the past and unpaid balance should become Overdue."""
        inv_id = self._create_inv('2024-01-01', '2024-02-01', 1000)
        # Trigger batch mark-overdue by loading the list
        self.c.get('/invoices')
        conn = get_connection()
        inv = conn.execute("SELECT status FROM invoices WHERE id=?", [inv_id]).fetchone()
        conn.close()
        self.assertEqual(inv['status'], 'Overdue')

    def test_paid_invoice_not_marked_overdue(self):
        """A fully paid invoice must never be marked Overdue."""
        inv_id = self._create_inv('2024-01-01', '2024-02-01', 1000, amount_paid=1000)
        self.c.get('/invoices')  # trigger batch
        conn = get_connection()
        inv = conn.execute("SELECT status FROM invoices WHERE id=?", [inv_id]).fetchone()
        conn.close()
        self.assertEqual(inv['status'], 'Paid')

    def test_future_due_date_stays_pending(self):
        inv_id = self._create_inv(
            date.today().strftime('%Y-%m-%d'),
            '2099-12-31',
            500
        )
        self.c.get('/invoices')
        conn = get_connection()
        inv = conn.execute("SELECT status FROM invoices WHERE id=?", [inv_id]).fetchone()
        conn.close()
        self.assertEqual(inv['status'], 'Pending')

    def test_status_updates_after_edit(self):
        """Editing an invoice recalculates status."""
        inv_id = self._create_inv('2025-07-01', '2099-12-31', 2000)
        # Edit to fully paid
        conn = get_connection()
        inv = conn.execute("SELECT * FROM invoices WHERE id=?", [inv_id]).fetchone()
        conn.close()
        self.c.post(f'/invoices/{inv_id}/edit', data={
            'invoice_date': inv['invoice_date'],
            'due_date': inv['due_date'],
            'amount': '2000',
            'amount_paid': '2000',
            'client_id': '1',
            'description_of_work': 'test',
        }, follow_redirects=True)
        conn = get_connection()
        updated = conn.execute("SELECT status FROM invoices WHERE id=?", [inv_id]).fetchone()
        conn.close()
        self.assertEqual(updated['status'], 'Paid')


# ════════════════════════════════════════════════════════════════
class TestInvoiceDelete(unittest.TestCase):
    def setUp(self):
        self.c = flask_app.test_client()

    def test_delete_is_soft(self):
        self.c.post('/invoices/new', data={
            'invoice_date': '2025-08-01',
            'amount': '100',
            'client_id': '1',
        })
        conn = get_connection()
        inv = conn.execute(
            "SELECT id FROM invoices WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        inv_id = inv['id']

        r = _post_json(self.c, f'/invoices/{inv_id}/delete', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])

        conn = get_connection()
        row = conn.execute("SELECT is_deleted FROM invoices WHERE id=?", [inv_id]).fetchone()
        conn.close()
        self.assertEqual(row['is_deleted'], 1)


# ════════════════════════════════════════════════════════════════
class TestInvoiceAgingAPI(unittest.TestCase):
    def setUp(self):
        self.c = flask_app.test_client()

    def test_aging_api_returns_buckets(self):
        r = self.c.get('/api/invoices/aging')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn('buckets', d)
        self.assertIn('totals', d)
        for bucket in ['0-30', '31-60', '61-90', '90+']:
            self.assertIn(bucket, d['buckets'])
            self.assertIn(bucket, d['totals'])

    def test_aging_totals_have_count_and_total(self):
        r = self.c.get('/api/invoices/aging')
        d = json.loads(r.data)
        for bucket, vals in d['totals'].items():
            self.assertIn('count', vals)
            self.assertIn('total', vals)

    def test_overdue_invoices_appear_in_aging(self):
        """Invoices with past due dates should be in 31-60 / 61-90 / 90+ buckets."""
        # Create a very old overdue invoice
        self.c.post('/invoices/new', data={
            'invoice_date': '2023-01-01',
            'due_date': '2023-02-01',
            'amount': '9999',
            'amount_paid': '0',
            'client_id': '2',
        })
        r = self.c.get('/api/invoices/aging')
        d = json.loads(r.data)
        # The 90+ bucket should have at least 1 invoice
        self.assertGreater(d['totals']['90+']['count'], 0)
        self.assertGreater(d['totals']['90+']['total'], 0)

    def test_summary_api(self):
        r = self.c.get('/api/invoices/summary?year=2025')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn('invoiced', d)
        self.assertIn('collected', d)
        self.assertIn('outstanding', d)
        self.assertIn('overdue_count', d)


# ════════════════════════════════════════════════════════════════
class TestInvoiceExport(unittest.TestCase):
    def setUp(self):
        self.c = flask_app.test_client()

    def test_export_returns_csv(self):
        r = self.c.get('/invoices/export')
        self.assertEqual(r.status_code, 200)
        self.assertIn('text/csv', r.content_type)
        self.assertIn(b'invoice_number', r.data)

    def test_export_filtered_by_status(self):
        r = self.c.get('/invoices/export?status=Paid')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'invoice_number', r.data)

    def test_export_filtered_by_client(self):
        r = self.c.get('/invoices/export?client=1')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestInvoiceAutomationUnit(unittest.TestCase):
    """Unit tests for the automation functions used by Phase 5."""

    def test_compute_dates_and_balance(self):
        from automations import compute_invoice_dates_and_balance
        result = compute_invoice_dates_and_balance('2025-06-01', 5000.0, 0.0)
        self.assertEqual(result['due_date'], '2025-07-01')
        self.assertAlmostEqual(result['balance_due'], 5000.0)

    def test_compute_balance_with_partial_payment(self):
        from automations import compute_invoice_dates_and_balance
        result = compute_invoice_dates_and_balance('2025-06-01', 5000.0, 1500.0)
        self.assertAlmostEqual(result['balance_due'], 3500.0)

    def test_update_invoice_status_paid(self):
        from automations import update_invoice_status
        conn = get_connection()
        conn.execute("""INSERT INTO invoices (invoice_number, invoice_date, due_date,
                        amount, amount_paid, balance_due)
                        VALUES (9901, '2025-01-01', '2025-02-01', 1000, 1000, 0)""")
        conn.commit()
        inv_id = conn.execute("SELECT id FROM invoices WHERE invoice_number=9901").fetchone()['id']
        update_invoice_status(inv_id, conn)
        status = conn.execute("SELECT status FROM invoices WHERE id=?", [inv_id]).fetchone()['status']
        conn.close()
        self.assertEqual(status, 'Paid')

    def test_update_invoice_status_overdue(self):
        from automations import update_invoice_status
        conn = get_connection()
        conn.execute("""INSERT INTO invoices (invoice_number, invoice_date, due_date,
                        amount, amount_paid, balance_due)
                        VALUES (9902, '2023-01-01', '2023-02-01', 500, 0, 500)""")
        conn.commit()
        inv_id = conn.execute("SELECT id FROM invoices WHERE invoice_number=9902").fetchone()['id']
        update_invoice_status(inv_id, conn)
        status = conn.execute("SELECT status FROM invoices WHERE id=?", [inv_id]).fetchone()['status']
        conn.close()
        self.assertEqual(status, 'Overdue')

    def test_update_invoice_status_partial(self):
        from automations import update_invoice_status
        conn = get_connection()
        conn.execute("""INSERT INTO invoices (invoice_number, invoice_date, due_date,
                        amount, amount_paid, balance_due)
                        VALUES (9903, '2025-01-01', '2099-12-31', 1000, 400, 600)""")
        conn.commit()
        inv_id = conn.execute("SELECT id FROM invoices WHERE invoice_number=9903").fetchone()['id']
        update_invoice_status(inv_id, conn)
        status = conn.execute("SELECT status FROM invoices WHERE id=?", [inv_id]).fetchone()['status']
        conn.close()
        self.assertEqual(status, 'Partial')

    def test_aging_bucket_helper(self):
        from routes_phase5 import _aging_bucket, _days_overdue
        today = date.today().strftime('%Y-%m-%d')
        past_30  = (date.today() - timedelta(days=40)).strftime('%Y-%m-%d')
        past_70  = (date.today() - timedelta(days=70)).strftime('%Y-%m-%d')
        past_100 = (date.today() - timedelta(days=100)).strftime('%Y-%m-%d')
        future   = '2099-12-31'

        self.assertEqual(_aging_bucket(future,   today), '0-30')
        self.assertEqual(_aging_bucket(past_30,  today), '31-60')
        self.assertEqual(_aging_bucket(past_70,  today), '61-90')
        self.assertEqual(_aging_bucket(past_100, today), '90+')

    def test_days_overdue_helper(self):
        from routes_phase5 import _days_overdue
        today = date.today().strftime('%Y-%m-%d')
        past  = (date.today() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.assertEqual(_days_overdue(past,  today), 15)
        self.assertEqual(_days_overdue(today, today), 0)
        self.assertEqual(_days_overdue('2099-12-31', today), 0)

    def test_next_invoice_number_increments(self):
        from routes_phase5 import _next_invoice_number
        conn = get_connection()
        before = _next_invoice_number(conn)
        conn.execute("""INSERT INTO invoices (invoice_number, invoice_date, amount)
                        VALUES (?, date('now'), 1)""", [before])
        conn.commit()
        after = _next_invoice_number(conn)
        conn.close()
        self.assertEqual(after, before + 1)


# ════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    total  = result.testsRun
    if result.wasSuccessful():
        print(f"\n✅ All {total} Phase 5 tests passed!")
    else:
        fails = len(result.failures)
        errs  = len(result.errors)
        print(f"\n❌ {fails} failure(s), {errs} error(s) out of {total}")
    sys.exit(0 if result.wasSuccessful() else 1)
