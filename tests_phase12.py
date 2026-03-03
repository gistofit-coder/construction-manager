"""
Phase 12 Tests — Auto-Backup, File Serving, Cert PDF Scan, History/Undo, Column Sort
Run: python3 tests_phase12.py
"""
import os, sys, json, zipfile, tempfile, unittest
from datetime import date, timedelta
from pathlib import Path

TEST_DB = '/tmp/test_p12.db'
os.environ['CONSTRUCTION_DB'] = TEST_DB
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

from database import init_db, get_connection
init_db(TEST_DB)

from app import app as flask_app
flask_app.config['TESTING'] = True
CLIENT = flask_app.test_client()

from database import db as _db
from automations import verify_all_receipts, log_action

TODAY = date.today().strftime('%Y-%m-%d')

# ─────────────────────────────────────────────
# Seed helpers
# ─────────────────────────────────────────────
_ctr = 0
def _uid():
    global _ctr; _ctr += 1; return _ctr

def _post_json(url, data):
    return CLIENT.post(url, data=json.dumps(data), content_type='application/json')

def _seed_config(**kwargs):
    with _db() as conn:
        for k, v in kwargs.items():
            conn.execute(f"UPDATE company_config SET {k}=? WHERE id=1", [v])

def _seed_ledger(receipt_filename=''):
    with _db() as conn:
        cur = conn.execute("""
            INSERT INTO ledger (entry_date, vendor, amount, category, receipt_filename)
            VALUES (?, ?, ?, ?, ?)
        """, [TODAY, f'Vendor {_uid()}', 100.0, 'Materials', receipt_filename])
        return cur.lastrowid

def _seed_contractor(name=None):
    name = name or f'Contractor {_uid()}'
    with _db() as conn:
        cur = conn.execute(
            "INSERT INTO contractors (company_name) VALUES (?)", [name]
        )
        return cur.lastrowid

def _seed_cert(contractor_id, cert_type='General Liability', end_date=None):
    end_date = end_date or (date.today() + timedelta(days=180)).strftime('%Y-%m-%d')
    with _db() as conn:
        cur = conn.execute("""
            INSERT INTO certificates (contractor_id, cert_type, end_date)
            VALUES (?, ?, ?)
        """, [contractor_id, cert_type, end_date])
        return cur.lastrowid


# ════════════════════════════════════════════════════════════════
class TestAutoBackupZip(unittest.TestCase):
    """Test ZIP-based backup creation and rotation."""

    def setUp(self):
        self.backup_dir = tempfile.mkdtemp(prefix='test_backup_')
        _seed_config(backup_folder_path=self.backup_dir, backup_keep_count=3)

    def test_backup_creates_zip(self):
        from app import _do_backup
        ok, msg = _do_backup('test')
        self.assertTrue(ok, msg)
        zips = list(Path(self.backup_dir).glob('kbweb_backup_*.zip'))
        self.assertGreater(len(zips), 0)

    def test_zip_contains_db(self):
        from app import _do_backup
        ok, msg = _do_backup('test')
        zips = sorted(Path(self.backup_dir).glob('kbweb_backup_*.zip'))
        with zipfile.ZipFile(zips[-1]) as zf:
            names = zf.namelist()
        self.assertTrue(any('db/' in n for n in names))

    def test_zip_contains_manifest(self):
        from app import _do_backup
        ok, _ = _do_backup('test')
        zips = sorted(Path(self.backup_dir).glob('kbweb_backup_*.zip'))
        with zipfile.ZipFile(zips[-1]) as zf:
            self.assertIn('manifest.txt', zf.namelist())

    def test_zip_manifest_readable(self):
        from app import _do_backup
        ok, _ = _do_backup('test')
        zips = sorted(Path(self.backup_dir).glob('kbweb_backup_*.zip'))
        with zipfile.ZipFile(zips[-1]) as zf:
            manifest = zf.read('manifest.txt').decode()
        self.assertIn('KB Construction Manager', manifest)
        self.assertIn('Label', manifest)

    def test_backup_includes_receipts(self):
        receipts_dir = tempfile.mkdtemp(prefix='test_receipts_')
        # Write a fake PDF
        fake_pdf = os.path.join(receipts_dir, 'test_receipt.pdf')
        with open(fake_pdf, 'wb') as f:
            f.write(b'%PDF-1.4 fake')
        _seed_config(receipts_folder_path=receipts_dir)
        from app import _do_backup
        ok, msg = _do_backup('receipt_test')
        zips = sorted(Path(self.backup_dir).glob('kbweb_backup_*.zip'))
        with zipfile.ZipFile(zips[-1]) as zf:
            names = zf.namelist()
        self.assertTrue(any('receipts/' in n for n in names), f"No receipts in zip. Names: {names}")

    def test_backup_includes_certs_folder(self):
        certs_dir = tempfile.mkdtemp(prefix='test_certs_')
        fake_coi = os.path.join(certs_dir, 'AcmeCOI.pdf')
        with open(fake_coi, 'wb') as f:
            f.write(b'%PDF-1.4 fake coi')
        _seed_config(certs_folder_path=certs_dir)
        from app import _do_backup
        ok, msg = _do_backup('cert_test')
        zips = sorted(Path(self.backup_dir).glob('kbweb_backup_*.zip'))
        with zipfile.ZipFile(zips[-1]) as zf:
            names = zf.namelist()
        self.assertTrue(any('certs/' in n for n in names), f"No certs in zip. Names: {names}")

    def test_rotation_keeps_n_backups(self):
        from app import _do_backup
        # Create 5 startup backups, limit is 3
        for _ in range(5):
            _do_backup('startup')
        startup_zips = list(Path(self.backup_dir).glob('kbweb_backup_startup_*.zip'))
        self.assertLessEqual(len(startup_zips), 3)

    def test_manual_backups_not_pruned(self):
        from app import _do_backup
        # Create startup backups to trigger rotation
        for _ in range(5):
            _do_backup('startup')
        # Manual backup stays
        _do_backup('manual')
        manual_zips = list(Path(self.backup_dir).glob('kbweb_backup_manual_*.zip'))
        self.assertEqual(len(manual_zips), 1)

    def test_backup_no_folder_configured(self):
        _seed_config(backup_folder_path='')
        from app import _do_backup
        ok, msg = _do_backup('test')
        self.assertFalse(ok)
        self.assertIn('configured', msg.lower())

    def test_backup_status_api(self):
        from app import _do_backup
        _do_backup('startup')
        r = CLIENT.get('/api/backup/status')
        d = json.loads(r.data)
        self.assertIn('backups', d)
        self.assertIsInstance(d['backups'], list)

    def test_backup_status_shows_recent(self):
        from app import _do_backup
        _do_backup('startup')
        r = CLIENT.get('/api/backup/status')
        d = json.loads(r.data)
        if d['backups']:
            self.assertIn('name', d['backups'][0])
            self.assertIn('size_mb', d['backups'][0])


# ════════════════════════════════════════════════════════════════
class TestFileServing(unittest.TestCase):
    """Test /files/receipts/<name> and /files/certs/<name> routes."""

    def setUp(self):
        self.receipts_dir = tempfile.mkdtemp(prefix='test_receipts_')
        self.certs_dir    = tempfile.mkdtemp(prefix='test_certs_')
        self.fake_receipt = os.path.join(self.receipts_dir, 'test_receipt.pdf')
        self.fake_cert    = os.path.join(self.certs_dir,    'AcmeCOI.pdf')
        with open(self.fake_receipt, 'wb') as f:
            f.write(b'%PDF-1.4 receipt')
        with open(self.fake_cert, 'wb') as f:
            f.write(b'%PDF-1.4 cert coi')
        _seed_config(
            receipts_folder_path=self.receipts_dir,
            certs_folder_path=self.certs_dir,
        )

    def test_serve_receipt_200(self):
        r = CLIENT.get('/files/receipts/test_receipt.pdf')
        self.assertEqual(r.status_code, 200)

    def test_serve_receipt_content(self):
        r = CLIENT.get('/files/receipts/test_receipt.pdf')
        self.assertIn(b'%PDF', r.data)

    def test_serve_receipt_not_found_404(self):
        r = CLIENT.get('/files/receipts/nonexistent_file.pdf')
        self.assertEqual(r.status_code, 404)

    def test_serve_cert_200(self):
        r = CLIENT.get('/files/certs/AcmeCOI.pdf')
        self.assertEqual(r.status_code, 200)

    def test_serve_cert_content(self):
        r = CLIENT.get('/files/certs/AcmeCOI.pdf')
        self.assertIn(b'%PDF', r.data)

    def test_serve_cert_not_found_404(self):
        r = CLIENT.get('/files/certs/missing_cert.pdf')
        self.assertEqual(r.status_code, 404)

    def test_serve_no_folder_configured_404(self):
        _seed_config(receipts_folder_path='')
        r = CLIENT.get('/files/receipts/test_receipt.pdf')
        self.assertEqual(r.status_code, 404)
        # Restore
        _seed_config(receipts_folder_path=self.receipts_dir)


# ════════════════════════════════════════════════════════════════
class TestReceiptVerification(unittest.TestCase):
    """Test that verify_all_receipts correctly marks found/missing."""

    def setUp(self):
        self.receipts_dir = tempfile.mkdtemp(prefix='test_verify_')
        _seed_config(receipts_folder_path=self.receipts_dir)
        # Create a real PDF
        self.real_pdf = 'real_receipt_2025.pdf'
        with open(os.path.join(self.receipts_dir, self.real_pdf), 'wb') as f:
            f.write(b'%PDF-1.4')

    def test_found_receipt_marks_verified(self):
        row_id = _seed_ledger(receipt_filename=self.real_pdf)
        count  = verify_all_receipts()
        conn   = get_connection()
        row    = conn.execute("SELECT receipt_verified FROM ledger WHERE id=?", [row_id]).fetchone()
        conn.close()
        self.assertEqual(row['receipt_verified'], 1)

    def test_missing_receipt_marks_unverified(self):
        row_id = _seed_ledger(receipt_filename='does_not_exist.pdf')
        verify_all_receipts()
        conn   = get_connection()
        row    = conn.execute("SELECT receipt_verified FROM ledger WHERE id=?", [row_id]).fetchone()
        conn.close()
        self.assertEqual(row['receipt_verified'], 0)

    def test_empty_receipt_filename_skipped(self):
        row_id = _seed_ledger(receipt_filename='')
        # Should not error
        verify_all_receipts()
        conn   = get_connection()
        row    = conn.execute("SELECT receipt_verified FROM ledger WHERE id=?", [row_id]).fetchone()
        conn.close()
        # Empty filename entries are not updated (stays 0)
        self.assertEqual(row['receipt_verified'], 0)

    def test_verify_returns_count(self):
        _seed_ledger(receipt_filename=self.real_pdf)
        _seed_ledger(receipt_filename='missing.pdf')
        count = verify_all_receipts()
        self.assertGreaterEqual(count, 2)


# ════════════════════════════════════════════════════════════════
class TestCertPdfScan(unittest.TestCase):
    """Test _scan_cert_pdfs matching logic."""

    def setUp(self):
        self.certs_dir = tempfile.mkdtemp(prefix='test_cert_scan_')
        _seed_config(certs_folder_path=self.certs_dir)

    def _make_pdf(self, name):
        path = os.path.join(self.certs_dir, name)
        with open(path, 'wb') as f:
            f.write(b'%PDF-1.4 COI')
        return name

    def test_scan_endpoint_ok(self):
        r = _post_json('/api/certs/scan-pdfs', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertIn('matched', d)

    def test_scan_matches_by_company_name(self):
        con_id  = _seed_contractor('Acme Plumbing')
        cert_id = _seed_cert(con_id)
        pdf     = self._make_pdf('AcmePlumbing_COI_2025.pdf')
        from app import _scan_cert_pdfs
        matched = _scan_cert_pdfs()
        conn    = get_connection()
        cert    = conn.execute("SELECT cert_pdf_filename, cert_verified FROM certificates WHERE id=?", [cert_id]).fetchone()
        conn.close()
        self.assertEqual(cert['cert_pdf_filename'], pdf)
        self.assertEqual(cert['cert_verified'], 1)

    def test_scan_no_match_stays_unverified(self):
        con_id  = _seed_contractor('XYZ Roofing')
        cert_id = _seed_cert(con_id)
        self._make_pdf('TotallyDifferent.pdf')  # Won't match XYZ Roofing
        from app import _scan_cert_pdfs
        _scan_cert_pdfs()
        conn = get_connection()
        cert = conn.execute("SELECT cert_verified FROM certificates WHERE id=?", [cert_id]).fetchone()
        conn.close()
        self.assertEqual(cert['cert_verified'], 0)

    def test_available_pdfs_api(self):
        self._make_pdf('TestCOI.pdf')
        r = CLIENT.get('/api/certs/available-pdfs')
        d = json.loads(r.data)
        self.assertIn('pdfs', d)
        self.assertIn('TestCOI.pdf', d['pdfs'])

    def test_available_pdfs_no_folder(self):
        _seed_config(certs_folder_path='')
        r = CLIENT.get('/api/certs/available-pdfs')
        d = json.loads(r.data)
        self.assertEqual(d['pdfs'], [])
        _seed_config(certs_folder_path=self.certs_dir)

    def test_set_pdf_manually(self):
        con_id  = _seed_contractor('Manual Link Co')
        cert_id = _seed_cert(con_id)
        pdf     = self._make_pdf('ManualCOI.pdf')
        r = _post_json(f'/api/certs/{cert_id}/set-pdf', {'filename': pdf})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        conn = get_connection()
        cert = conn.execute("SELECT cert_pdf_filename FROM certificates WHERE id=?", [cert_id]).fetchone()
        conn.close()
        self.assertEqual(cert['cert_pdf_filename'], pdf)

    def test_set_pdf_verifies_existence(self):
        con_id  = _seed_contractor('Verify Exist Co')
        cert_id = _seed_cert(con_id)
        pdf     = self._make_pdf('ExistingFile.pdf')
        r = _post_json(f'/api/certs/{cert_id}/set-pdf', {'filename': pdf})
        d = json.loads(r.data)
        self.assertTrue(d['verified'])

    def test_set_pdf_nonexistent_not_verified(self):
        con_id  = _seed_contractor('No File Co')
        cert_id = _seed_cert(con_id)
        r = _post_json(f'/api/certs/{cert_id}/set-pdf', {'filename': 'ghost.pdf'})
        d = json.loads(r.data)
        self.assertFalse(d['verified'])


# ════════════════════════════════════════════════════════════════
class TestHistory(unittest.TestCase):
    """Test history/undo log view and revert API."""

    def _log(self, table='ledger', record_id=1, action='UPDATE',
             old=None, new=None):
        with _db() as conn:
            log_action(conn, table, record_id, action,
                       old_data=old or {'amount': 100},
                       new_data=new or {'amount': 200})
            conn.execute("SELECT last_insert_rowid()").fetchone()
        # Get the log id
        conn = get_connection()
        row = conn.execute(
            "SELECT id FROM undo_log WHERE table_name=? AND record_id=? ORDER BY id DESC LIMIT 1",
            [table, record_id]
        ).fetchone()
        conn.close()
        return row['id'] if row else None

    def test_history_page_loads(self):
        r = CLIENT.get('/history')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Change History', r.data)

    def test_history_shows_entries(self):
        self._log(table='ledger', record_id=999, action='UPDATE',
                  old={'amount': 50}, new={'amount': 75})
        r = CLIENT.get('/history')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'UPDATE', r.data)

    def test_history_filter_by_table(self):
        r = CLIENT.get('/history?table=ledger')
        self.assertEqual(r.status_code, 200)

    def test_history_filter_by_action(self):
        r = CLIENT.get('/history?action=UPDATE')
        self.assertEqual(r.status_code, 200)

    def test_history_pagination(self):
        r = CLIENT.get('/history?page=1')
        self.assertEqual(r.status_code, 200)

    def test_revert_not_found(self):
        r = _post_json('/api/history/99999/revert', {})
        self.assertEqual(r.status_code, 404)

    def test_revert_update_restores_field(self):
        # Create a ledger entry, then log an update to it
        row_id = _seed_ledger()
        # Manually set amount to 500
        with _db() as conn:
            conn.execute("UPDATE ledger SET amount=500 WHERE id=?", [row_id])
        log_id = self._log(table='ledger', record_id=row_id, action='UPDATE',
                           old={'amount': 100}, new={'amount': 500})
        r = _post_json(f'/api/history/{log_id}/revert', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])

    def test_revert_marks_reversed(self):
        row_id = _seed_ledger()
        log_id = self._log(table='ledger', record_id=row_id, action='UPDATE',
                           old={'amount': 100}, new={'amount': 999})
        _post_json(f'/api/history/{log_id}/revert', {})
        conn = get_connection()
        entry = conn.execute("SELECT reversed FROM undo_log WHERE id=?", [log_id]).fetchone()
        conn.close()
        self.assertEqual(entry['reversed'], 1)

    def test_revert_already_reversed(self):
        row_id = _seed_ledger()
        log_id = self._log(table='ledger', record_id=row_id, action='UPDATE',
                           old={'amount': 100}, new={'amount': 999})
        _post_json(f'/api/history/{log_id}/revert', {})
        # Second revert should return 404 (already reversed)
        r = _post_json(f'/api/history/{log_id}/revert', {})
        self.assertEqual(r.status_code, 404)

    def test_revert_nonrevertable_table(self):
        with _db() as conn:
            log_action(conn, 'company_config', 1, 'UPDATE',
                       old_data={'company_name': 'Old'}, new_data={'company_name': 'New'})
        conn = get_connection()
        log_id = conn.execute(
            "SELECT id FROM undo_log WHERE table_name='company_config' ORDER BY id DESC LIMIT 1"
        ).fetchone()['id']
        conn.close()
        r = _post_json(f'/api/history/{log_id}/revert', {})
        # company_config is not in whitelist → 400
        self.assertEqual(r.status_code, 400)

    def test_history_shows_diff_data(self):
        row_id = _seed_ledger()
        self._log(table='ledger', record_id=row_id, action='UPDATE',
                  old={'amount': 100, 'vendor': 'Before'},
                  new={'amount': 200, 'vendor': 'After'})
        r = CLIENT.get('/history')
        # Diff data is rendered as JSON in button attrs
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestColumnSorting(unittest.TestCase):
    """Test that sortable tables render correctly and JS is present."""

    def test_ledger_has_sortable_table(self):
        r = CLIENT.get('/ledger', follow_redirects=True)
        self.assertIn(b'ledger-table', r.data)

    def test_certs_has_sortable_table(self):
        r = CLIENT.get('/certs', follow_redirects=True)
        self.assertIn(b'certsTable', r.data)

    def test_base_has_sort_function(self):
        r = CLIENT.get('/', follow_redirects=True)
        self.assertIn(b'initSortableTable', r.data)

    def test_base_has_sort_arrow_css(self):
        r = CLIENT.get('/')
        self.assertIn(b'sort-arrow', r.data)

    def test_invoices_has_sortable_table(self):
        r = CLIENT.get('/invoices', follow_redirects=True)
        self.assertIn(b'invoices-table', r.data)

    def test_sort_url_params_persist(self):
        # Sort params should be accepted without error
        r = CLIENT.get('/ledger?sort=0&dir=asc', follow_redirects=True)
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestContinuousScroll(unittest.TestCase):
    """Test continuous scroll toggle."""

    def test_toggle_on(self):
        r = _post_json('/api/settings/continuous-scroll', {'enabled': True})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertTrue(d['continuous_scroll'])

    def test_toggle_off(self):
        r = _post_json('/api/settings/continuous-scroll', {'enabled': False})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertFalse(d['continuous_scroll'])

    def test_toggle_persists_to_db(self):
        _post_json('/api/settings/continuous-scroll', {'enabled': True})
        conn = get_connection()
        cfg  = conn.execute("SELECT continuous_scroll FROM company_config WHERE id=1").fetchone()
        conn.close()
        self.assertEqual(cfg['continuous_scroll'], 1)

    def test_settings_has_toggle(self):
        r = CLIENT.get('/settings', follow_redirects=True)
        self.assertIn(b'continuous_scroll', r.data)


# ════════════════════════════════════════════════════════════════
class TestLedgerReceiptLinks(unittest.TestCase):
    """Test that receipt file links render in ledger template."""

    def setUp(self):
        # Seed a ledger entry with a receipt filename so the link renders
        _seed_ledger(receipt_filename='test_receipt_link.pdf')

    def test_ledger_has_receipt_route_in_page(self):
        r = CLIENT.get('/ledger', follow_redirects=True)
        # The route should be referenced in the page (in JS or HTML)
        self.assertIn(b'files/receipts', r.data)

    def test_history_link_in_nav(self):
        r = CLIENT.get('/ledger', follow_redirects=True)
        self.assertIn(b'History', r.data)


# ════════════════════════════════════════════════════════════════
class TestRegressionPhase12(unittest.TestCase):
    """Verify all previous phases still work."""

    def test_dashboard(self):
        self.assertEqual(CLIENT.get('/').status_code, 200)

    def test_certs_list(self):
        r = CLIENT.get('/certs', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_reports(self):
        r = CLIENT.get('/reports', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_estimates(self):
        r = CLIENT.get('/estimates')
        self.assertEqual(r.status_code, 200)

    def test_payroll(self):
        r = CLIENT.get('/payroll', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_tax(self):
        r = CLIENT.get('/tax', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_quick_quote(self):
        r = CLIENT.get('/quote', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_reconciliation(self):
        r = CLIENT.get('/reconciliation')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    total  = result.testsRun
    if result.wasSuccessful():
        print(f"\n✅ All {total} Phase 12 tests passed!")
    else:
        fails = len(result.failures)
        errs  = len(result.errors)
        print(f"\n❌ {fails} failure(s), {errs} error(s) out of {total}")
        for t, m in result.failures + result.errors:
            print(f"\n--- {t} ---\n{m}")
    sys.exit(0 if result.wasSuccessful() else 1)
