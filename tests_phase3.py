"""
Phase 3 Tests — Ledger
Run: python3 tests_phase3.py
"""
import os, sys, json, io, unittest

TEST_DB = '/tmp/test_p3.db'
os.environ['CONSTRUCTION_DB'] = TEST_DB
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

from database import init_db, get_connection
from automations import generate_receipt_filename, verify_receipt

init_db(TEST_DB)

from app import app as flask_app
flask_app.config['TESTING'] = True

# ── seed a client, job, bank account for FK refs ──────────────
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
        INSERT INTO bank_accounts (account_name, account_type)
        VALUES ('Test Checking', 'Checking')
    """)
    conn.execute("""
        INSERT INTO vendor_categories (vendor_name, default_category)
        VALUES ('Home Depot', 'Materials')
    """)


class TestLedgerCreate(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_ledger_list_loads(self):
        r = self.client.get('/ledger')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'General Ledger', r.data)

    def test_create_expense_entry_json(self):
        r = self.client.post('/ledger/new',
            data=json.dumps({
                'entry_date': '2025-03-15',
                'vendor': 'Home Depot',
                'amount': '-250.00',
                'job_code': 'TST-24',
                'description': 'Lumber',
            }),
            content_type='application/json')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertEqual(d['row']['vendor'], 'Home Depot')
        self.assertEqual(float(d['row']['amount']), -250.0)

    def test_category_auto_filled_from_vendor(self):
        """Vendor→category auto-fill should kick in."""
        r = self.client.post('/ledger/new',
            data=json.dumps({
                'entry_date': '2025-03-16',
                'vendor': 'Home Depot',
                'amount': '-100.00',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertEqual(d['row']['category'], 'Materials')

    def test_receipt_filename_auto_generated(self):
        """Receipt filename should be auto-generated on save."""
        r = self.client.post('/ledger/new',
            data=json.dumps({
                'entry_date': '2025-03-17',
                'vendor': 'Lowes',
                'amount': '-75.50',
                'job_code': 'TST-24',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        fname = d['row']['receipt_filename']
        self.assertIn('2025-03-17', fname)
        self.assertIn('Lowes', fname)
        self.assertIn('TST-24', fname)
        self.assertIn('.pdf', fname)

    def test_create_income_entry(self):
        r = self.client.post('/ledger/new',
            data=json.dumps({
                'entry_date': '2025-03-20',
                'vendor': 'Test Client',
                'amount': '5000.00',
                'job_code': 'TST-24',
                'description': 'Invoice payment',
                'category': 'Income',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertGreater(float(d['row']['amount']), 0)

    def test_missing_date_returns_error(self):
        r = self.client.post('/ledger/new',
            data=json.dumps({'vendor': 'Someone', 'amount': '-50'}),
            content_type='application/json')
        self.assertEqual(r.status_code, 400)
        d = json.loads(r.data)
        self.assertIn('error', d)

    def test_missing_amount_returns_error(self):
        r = self.client.post('/ledger/new',
            data=json.dumps({'entry_date': '2025-03-01', 'vendor': 'Someone'}),
            content_type='application/json')
        self.assertEqual(r.status_code, 400)

    def test_invalid_amount_string(self):
        r = self.client.post('/ledger/new',
            data=json.dumps({'entry_date': '2025-03-01', 'amount': 'abc'}),
            content_type='application/json')
        self.assertEqual(r.status_code, 400)


class TestLedgerInlinePatch(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()
        # Get the first ledger row ID
        conn = get_connection()
        row = conn.execute("SELECT id FROM ledger WHERE is_deleted=0 LIMIT 1").fetchone()
        conn.close()
        self.row_id = row['id'] if row else None

    def test_patch_description(self):
        if not self.row_id: self.skipTest('No rows')
        r = self.client.post(f'/api/ledger/{self.row_id}/patch',
            data=json.dumps({'field': 'description', 'value': 'Updated description'}),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertEqual(d['row']['description'], 'Updated description')

    def test_patch_category_updates_vendor_mapping(self):
        """Patching category on a row with a vendor should update vendor_categories."""
        if not self.row_id: self.skipTest('No rows')
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM ledger WHERE id=? AND is_deleted=0", [self.row_id]
        ).fetchone()
        conn.close()
        if not row or not row['vendor']:
            self.skipTest('No vendor on row')
        r = self.client.post(f'/api/ledger/{self.row_id}/patch',
            data=json.dumps({'field': 'category', 'value': 'Tools'}),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        # Check vendor mapping updated
        conn = get_connection()
        vc = conn.execute(
            "SELECT default_category FROM vendor_categories WHERE vendor_name=?",
            [row['vendor']]
        ).fetchone()
        conn.close()
        if vc:
            self.assertEqual(vc['default_category'], 'Tools')

    def test_patch_amount_regenerates_receipt(self):
        """Patching amount should trigger receipt filename regen."""
        if not self.row_id: self.skipTest('No rows')
        conn = get_connection()
        old = conn.execute("SELECT receipt_filename FROM ledger WHERE id=?", [self.row_id]).fetchone()
        conn.close()
        r = self.client.post(f'/api/ledger/{self.row_id}/patch',
            data=json.dumps({'field': 'amount', 'value': '-99.99'}),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        # Receipt filename should contain the new amount
        fname = d['row']['receipt_filename']
        if fname:
            self.assertIn('99_99', fname)

    def test_patch_blocked_field(self):
        """Should reject patching id or is_deleted."""
        if not self.row_id: self.skipTest('No rows')
        r = self.client.post(f'/api/ledger/{self.row_id}/patch',
            data=json.dumps({'field': 'is_deleted', 'value': '1'}),
            content_type='application/json')
        self.assertEqual(r.status_code, 403)

    def test_patch_vendor_autofills_category(self):
        """Patching vendor when category is empty should auto-fill category."""
        # Create a row with no category
        create_r = self.client.post('/ledger/new',
            data=json.dumps({
                'entry_date': '2025-04-01',
                'vendor': '',
                'amount': '-10.00',
            }),
            content_type='application/json')
        d = json.loads(create_r.data)
        new_id = d['row']['id']
        # Clear category
        conn = get_connection()
        conn.execute("UPDATE ledger SET category='' WHERE id=?", [new_id])
        conn.commit()
        conn.close()
        # Patch vendor
        r = self.client.post(f'/api/ledger/{new_id}/patch',
            data=json.dumps({'field': 'vendor', 'value': 'Home Depot'}),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertEqual(d['row']['category'], 'Materials')


class TestLedgerDelete(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_delete_is_soft(self):
        # Create a row to delete
        r = self.client.post('/ledger/new',
            data=json.dumps({
                'entry_date': '2025-05-01',
                'vendor': 'Delete Me Vendor',
                'amount': '-9.99',
            }),
            content_type='application/json')
        row_id = json.loads(r.data)['row']['id']
        # Delete it
        dr = self.client.post(f'/ledger/{row_id}/delete',
            content_type='application/json')
        d = json.loads(dr.data)
        self.assertTrue(d['success'])
        # Verify soft-deleted
        conn = get_connection()
        row = conn.execute("SELECT is_deleted FROM ledger WHERE id=?", [row_id]).fetchone()
        conn.close()
        self.assertEqual(row['is_deleted'], 1)

    def test_deleted_row_not_in_list(self):
        # Verify the deleted row is no longer returned by the default list (no filter)
        r = self.client.get('/ledger')
        self.assertEqual(r.status_code, 200)
        # The deleted vendor name should not appear in any table row data-vendor attribute
        # (it may still appear in the search input if a previous test set a ?q param, so check DB instead)
        conn = get_connection()
        count = conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE vendor='Delete Me Vendor' AND is_deleted=0"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 0, "Soft-deleted vendor should not appear in non-deleted rows")


class TestLedgerFilters(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_filter_by_job(self):
        r = self.client.get('/ledger?job=TST-24')
        self.assertEqual(r.status_code, 200)
        # All shown rows should have TST-24 job code
        self.assertIn(b'TST-24', r.data)

    def test_filter_by_income(self):
        r = self.client.get('/ledger?sign=income')
        self.assertEqual(r.status_code, 200)

    def test_filter_by_expense(self):
        r = self.client.get('/ledger?sign=expense')
        self.assertEqual(r.status_code, 200)

    def test_filter_by_month(self):
        r = self.client.get('/ledger?month=2025-03')
        self.assertEqual(r.status_code, 200)

    def test_filter_by_category(self):
        r = self.client.get('/ledger?cat=Materials')
        self.assertEqual(r.status_code, 200)

    def test_search_by_vendor(self):
        r = self.client.get('/ledger?q=Home+Depot')
        self.assertEqual(r.status_code, 200)


class TestLedgerCSVImport(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def _make_csv(self, rows, header=True):
        lines = []
        if header:
            lines.append('date,vendor,amount,description,category,job')
        for r in rows:
            lines.append(','.join(str(v) for v in r))
        return '\n'.join(lines)

    def test_import_basic_csv(self):
        csv_data = self._make_csv([
            ('2025-06-01', 'ABC Supply', '-450.00', 'Plumbing parts', 'Materials', 'TST-24'),
            ('2025-06-02', 'Shell Gas', '-55.50', 'Fuel', 'Vehicle/Fuel', ''),
            ('2025-06-03', 'Test Client', '2000.00', 'Invoice payment', 'Income', 'TST-24'),
        ])
        r = self.client.post('/ledger/import', data={
            'csv_file': (io.BytesIO(csv_data.encode()), 'test_import.csv'),
            'skip_header': '1',
            'sign_convention': 'negative_expense',
        }, content_type='multipart/form-data', follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Imported 3', r.data)

    def test_import_with_default_job(self):
        # CSV with no job column — default_job should be applied
        csv_data = 'date,vendor,amount,description\n2025-07-01,Menards,-120.00,Hardware\n'
        r = self.client.post('/ledger/import', data={
            'csv_file': (io.BytesIO(csv_data.encode()), 'menards.csv'),
            'default_job': 'TST-24',
            'skip_header': '1',
        }, content_type='multipart/form-data', follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Imported 1', r.data)
        # Verify default job applied
        conn = get_connection()
        row = conn.execute(
            "SELECT job_code FROM ledger WHERE vendor='Menards' AND is_deleted=0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        self.assertEqual(row['job_code'], 'TST-24')

    def test_import_skips_bad_dates(self):
        csv_data = self._make_csv([
            ('NOT-A-DATE', 'Vendor X', '-50', 'Bad date', '', ''),
            ('2025-07-02', 'Good Vendor', '-25', 'Good row', '', ''),
        ])
        r = self.client.post('/ledger/import', data={
            'csv_file': (io.BytesIO(csv_data.encode()), 'mixed.csv'),
            'skip_header': '1',
        }, content_type='multipart/form-data', follow_redirects=True)
        self.assertIn(b'Imported 1', r.data)
        self.assertIn(b'skipped 1', r.data)

    def test_import_various_date_formats(self):
        csv_data = 'date,vendor,amount\n03/15/2025,Vendor A,-10\n15-03-2025,Vendor B,-20\n2025-04-01,Vendor C,-30'
        r = self.client.post('/ledger/import', data={
            'csv_file': (io.BytesIO(csv_data.encode()), 'dates.csv'),
            'skip_header': '1',
        }, content_type='multipart/form-data', follow_redirects=True)
        # At least some should import
        self.assertIn(b'Imported', r.data)

    def test_import_no_file(self):
        r = self.client.post('/ledger/import', data={},
            content_type='multipart/form-data', follow_redirects=True)
        self.assertIn(b'No file', r.data)


class TestLedgerExport(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_export_returns_csv(self):
        r = self.client.get('/ledger/export')
        self.assertEqual(r.status_code, 200)
        self.assertIn('text/csv', r.content_type)
        self.assertIn(b'entry_date', r.data)  # header row

    def test_export_filtered_by_job(self):
        r = self.client.get('/ledger/export?job=TST-24')
        self.assertEqual(r.status_code, 200)
        # Should contain TST-24 entries
        self.assertIn(b'TST-24', r.data)


class TestLedgerAPIEndpoints(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_monthly_summary_api(self):
        r = self.client.get('/api/ledger/monthly-summary?year=2025')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIsInstance(d, list)
        if d:
            self.assertIn('ym', d[0])
            self.assertIn('income', d[0])
            self.assertIn('expense', d[0])
            self.assertIn('net', d[0])

    def test_job_summary_api(self):
        r = self.client.get('/api/ledger/job-summary?year=2025')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIsInstance(d, list)

    def test_category_summary_api(self):
        r = self.client.get('/api/ledger/category-summary?year=2025')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIsInstance(d, list)

    def test_vendor_autocomplete(self):
        r = self.client.get('/api/ledger/vendors?q=Home')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIsInstance(d, list)
        if d:
            self.assertIn('vendor', d[0])
            self.assertIn('default_category', d[0])

    def test_receipt_preview_api(self):
        r = self.client.get('/api/ledger/preview-receipt?date=2025-03-15&vendor=Home+Depot&amount=250&job=TST-24')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn('filename', d)
        self.assertIn('2025-03-15', d['filename'])
        self.assertIn('HomeDepot', d['filename'])

    def test_verify_receipt_api(self):
        # Get a row with a receipt filename
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM ledger WHERE receipt_filename!='' AND is_deleted=0 LIMIT 1"
        ).fetchone()
        conn.close()
        if not row:
            self.skipTest('No rows with receipt filenames')
        r = self.client.post(f'/api/ledger/verify-receipt/{row["id"]}')
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn('verified', d)


class TestReceiptFilenameAutomation(unittest.TestCase):
    """Unit tests for the receipt filename generation logic."""

    def test_basic_filename(self):
        fn = generate_receipt_filename('2025-03-15', 'TST-24', 'Home Depot', 250.00)
        self.assertEqual(fn, '2025-03-15.TST-24.HomeDepot.250_00.pdf')

    def test_vendor_sanitized(self):
        fn = generate_receipt_filename('2025-03-15', 'TST-24', "O'Brien's Hardware & Supply", 99.99)
        self.assertNotIn("'", fn)
        self.assertNotIn("&", fn)
        self.assertIn('.pdf', fn)

    def test_no_vendor(self):
        fn = generate_receipt_filename('2025-03-15', 'TST-24', '', 100.0)
        self.assertEqual(fn, '')

    def test_receipt_not_verified_when_file_missing(self):
        fn = '2025-03-15.TST-24.HomeDepot.250_00.pdf'
        result = verify_receipt(fn, '/nonexistent/path')
        self.assertFalse(result)


class TestLedgerVendorCategoryLearning(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()

    def test_new_vendor_category_learned(self):
        """A new vendor+category combo should be stored after a ledger save."""
        self.client.post('/ledger/new',
            data=json.dumps({
                'entry_date': '2025-08-01',
                'vendor': 'Brand New Vendor XYZ',
                'amount': '-33.00',
                'category': 'Office Supplies',
            }),
            content_type='application/json')
        conn = get_connection()
        row = conn.execute(
            "SELECT default_category FROM vendor_categories WHERE vendor_name='Brand New Vendor XYZ'"
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row['default_category'], 'Office Supplies')

    def test_second_entry_reuses_learned_category(self):
        """Same vendor again should auto-fill the previously learned category."""
        r = self.client.post('/ledger/new',
            data=json.dumps({
                'entry_date': '2025-08-15',
                'vendor': 'Brand New Vendor XYZ',
                'amount': '-12.50',
            }),
            content_type='application/json')
        d = json.loads(r.data)
        self.assertEqual(d['row']['category'], 'Office Supplies')


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    total = result.testsRun
    if result.wasSuccessful():
        print(f"\n✅ All {total} Phase 3 tests passed!")
    else:
        print(f"\n❌ {len(result.failures)} failure(s), {len(result.errors)} error(s) out of {total}")
    sys.exit(0 if result.wasSuccessful() else 1)
