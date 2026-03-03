"""
Phase 8 Tests — Bank Reconciliation
Run: python3 tests_phase8.py
"""
import os, sys, json, io, unittest
from datetime import date, timedelta

TEST_DB = '/tmp/test_p8.db'
os.environ['CONSTRUCTION_DB'] = TEST_DB
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

from database import init_db, get_connection
init_db(TEST_DB)

from app import app as flask_app
flask_app.config['TESTING'] = True
CLIENT = flask_app.test_client()

TODAY = date.today().strftime('%Y-%m-%d')


# ─────────────────────────────────────────────
# Seed helpers
# ─────────────────────────────────────────────
from database import db as _db

def _seed_account(name='Test Checking', acct_type='Checking', balance=5000.0):
    with _db() as conn:
        cur = conn.execute(
            "INSERT INTO bank_accounts (account_name, account_type, current_balance) VALUES (?,?,?)",
            [name, acct_type, balance]
        )
        return cur.lastrowid

def _seed_ledger(entry_date=None, amount=100.0, desc='Test Expense',
                 status='Pending', category='Materials', job_code='', bank_acct_id=None):
    with _db() as conn:
        cur = conn.execute("""
            INSERT INTO ledger (entry_date, description, amount, status, category, job_code, bank_account_id)
            VALUES (?,?,?,?,?,?,?)
        """, [entry_date or TODAY, desc, amount, status, category, job_code, bank_acct_id])
        return cur.lastrowid

def _seed_bank_txn(acct_id, amount=100.0, desc='Test Txn',
                   txn_date=None, txn_type='Debit', match_status='Unmatched', ref=''):
    with _db() as conn:
        cur = conn.execute("""
            INSERT INTO bank_transactions
                (bank_account_id, transaction_date, description, amount,
                 transaction_type, match_status, reference_number)
            VALUES (?,?,?,?,?,?,?)
        """, [acct_id, txn_date or TODAY, desc, amount, txn_type, match_status, ref])
        return cur.lastrowid

def _post_json(url, data):
    return CLIENT.post(url, data=json.dumps(data), content_type='application/json')

def _get_json(url):
    return json.loads(CLIENT.get(url).data)


# ════════════════════════════════════════════════════════════════
class TestReconciliationHome(unittest.TestCase):

    def test_home_loads(self):
        r = CLIENT.get('/reconciliation')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Bank', r.data)

    def test_home_shows_account(self):
        _seed_account('My Test Bank')
        r = CLIENT.get('/reconciliation')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'My Test Bank', r.data)


# ════════════════════════════════════════════════════════════════
class TestReconAccount(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('Acct Workspace')

    def test_account_view_loads(self):
        r = CLIENT.get(f'/reconciliation/{self.acct_id}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Acct Workspace', r.data)

    def test_account_view_404(self):
        r = CLIENT.get('/reconciliation/99999', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_account_filter_by_status(self):
        r = CLIENT.get(f'/reconciliation/{self.acct_id}?status=Unmatched')
        self.assertEqual(r.status_code, 200)

    def test_account_filter_by_dates(self):
        r = CLIENT.get(f'/reconciliation/{self.acct_id}?start=2025-01-01&end=2025-12-31')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestCSVParsing(unittest.TestCase):

    def _parse(self, text):
        from routes_phase8 import _parse_bank_csv
        return _parse_bank_csv(text)

    def test_basic_csv(self):
        csv = "Date,Description,Amount\n2025-01-15,ACH Payment,250.00\n2025-01-16,Check #1001,100.00"
        rows = self._parse(csv)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['transaction_date'], '2025-01-15')
        self.assertAlmostEqual(rows[0]['amount'], 250.0)

    def test_debit_credit_columns(self):
        csv = "Date,Description,Debit,Credit\n01/15/2025,Payroll,1500.00,\n01/16/2025,Payment,,2000.00"
        rows = self._parse(csv)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['transaction_type'], 'Debit')
        self.assertEqual(rows[1]['transaction_type'], 'Credit')

    def test_amount_negative_is_debit(self):
        csv = "Date,Description,Amount\n2025-02-01,Withdrawal,-500.00"
        rows = self._parse(csv)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]['amount'], 500.0)
        self.assertEqual(rows[0]['transaction_type'], 'Debit')

    def test_amount_positive_is_credit(self):
        csv = "Date,Description,Amount\n2025-02-01,Deposit,2500.00"
        rows = self._parse(csv)
        self.assertEqual(rows[0]['transaction_type'], 'Credit')

    def test_slash_date_format(self):
        csv = "Date,Description,Amount\n01/15/2025,Test,100.00"
        rows = self._parse(csv)
        self.assertEqual(rows[0]['transaction_date'], '2025-01-15')

    def test_mm_dd_yy_format(self):
        csv = "Date,Description,Amount\n01/15/25,Test,50.00"
        rows = self._parse(csv)
        self.assertEqual(rows[0]['transaction_date'], '2025-01-15')

    def test_comma_in_amount(self):
        csv = 'Date,Description,Amount\n2025-01-01,Big Payment,"1,500.00"'
        rows = self._parse(csv)
        self.assertAlmostEqual(rows[0]['amount'], 1500.0)

    def test_dollar_sign_stripped(self):
        csv = "Date,Description,Amount\n2025-01-01,Test,$350.00"
        rows = self._parse(csv)
        self.assertAlmostEqual(rows[0]['amount'], 350.0)

    def test_empty_rows_skipped(self):
        csv = "Date,Description,Amount\n\n2025-01-01,Good,100.00\n\n"
        rows = self._parse(csv)
        self.assertEqual(len(rows), 1)

    def test_zero_amount_skipped(self):
        csv = "Date,Description,Amount\n2025-01-01,Zero,0.00"
        rows = self._parse(csv)
        self.assertEqual(len(rows), 0)

    def test_invalid_date_skipped(self):
        csv = "Date,Description,Amount\nnot-a-date,Bad Row,100.00\n2025-01-01,Good,50.00"
        rows = self._parse(csv)
        self.assertEqual(len(rows), 1)

    def test_reference_number_captured(self):
        csv = "Date,Description,Amount,Reference\n2025-01-01,Payment,200.00,CHK1042"
        rows = self._parse(csv)
        self.assertEqual(rows[0]['reference_number'], 'CHK1042')

    def test_chase_format(self):
        """Simulate Chase bank CSV format."""
        csv = 'Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n01/15/2025,01/16/2025,VENMO PAYMENT,Transfer,Sale,-75.50,'
        rows = self._parse(csv)
        self.assertGreater(len(rows), 0)
        self.assertAlmostEqual(rows[0]['amount'], 75.5)

    def test_wellsfargo_format(self):
        """Simulate Wells Fargo format with debit/credit columns."""
        csv = '"Date","Description","Deposits","Withdrawals","Balance"\n"01/15/2025","ACH CREDIT","1500.00","","8500.00"\n"01/16/2025","CHECK #101","","250.00","8250.00"'
        rows = self._parse(csv)
        self.assertGreater(len(rows), 0)


# ════════════════════════════════════════════════════════════════
class TestCSVImport(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('Import Test Account')

    def _upload_csv(self, content):
        data = {'csv_file': (io.BytesIO(content.encode()), 'statement.csv')}
        return CLIENT.post(
            f'/reconciliation/{self.acct_id}/import',
            data=data, content_type='multipart/form-data',
            follow_redirects=True
        )

    def test_import_loads_form(self):
        r = CLIENT.get(f'/reconciliation/{self.acct_id}/import')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Import', r.data)

    def test_import_creates_transactions(self):
        csv = "Date,Description,Amount\n2025-03-01,Lumber Supply,450.00\n2025-03-02,Tool Rental,125.00"
        self._upload_csv(csv)
        conn = get_connection()
        count = conn.execute(
            "SELECT COUNT(*) FROM bank_transactions WHERE bank_account_id=? AND is_deleted=0",
            [self.acct_id]
        ).fetchone()[0]
        conn.close()
        self.assertGreaterEqual(count, 2)

    def test_import_deduplicates(self):
        csv = "Date,Description,Amount\n2025-03-10,Duplicate Entry,300.00"
        self._upload_csv(csv)
        self._upload_csv(csv)  # second import should skip duplicate
        conn = get_connection()
        count = conn.execute(
            "SELECT COUNT(*) FROM bank_transactions "
            "WHERE bank_account_id=? AND amount=300 AND is_deleted=0",
            [self.acct_id]
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)

    def test_import_empty_csv(self):
        r = self._upload_csv("Date,Description,Amount\n")
        self.assertEqual(r.status_code, 200)  # handled gracefully

    def test_import_no_file(self):
        r = CLIENT.post(
            f'/reconciliation/{self.acct_id}/import',
            data={}, content_type='multipart/form-data',
            follow_redirects=True
        )
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestAutoMatch(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('AutoMatch Account')

    def test_auto_match_exact_amount_date(self):
        """Bank txn and ledger entry with same amount and same date should auto-match."""
        led_id = _seed_ledger(amount=250.0, desc='Lumber', entry_date='2025-04-01',
                              bank_acct_id=self.acct_id, status='Pending')
        bt_id  = _seed_bank_txn(self.acct_id, amount=250.0, desc='LUMBER CO',
                                txn_date='2025-04-01', match_status='Unmatched')

        from routes_phase8 import _run_auto_match
        with _db() as conn:
            matched = _run_auto_match(self.acct_id, conn)
        self.assertGreater(matched, 0)

        conn = get_connection()
        bt  = conn.execute("SELECT match_status, matched_ledger_id FROM bank_transactions WHERE id=?", [bt_id]).fetchone()
        led = conn.execute("SELECT status FROM ledger WHERE id=?", [led_id]).fetchone()
        conn.close()
        self.assertIn(bt['match_status'], ['Auto-Matched'])
        self.assertEqual(bt['matched_ledger_id'], led_id)
        self.assertEqual(led['status'], 'Cleared')

    def test_auto_match_within_5_days(self):
        """Auto-match should work within 5-day date proximity."""
        led_id = _seed_ledger(amount=500.0, desc='Contractor', entry_date='2025-05-01',
                              bank_acct_id=self.acct_id, status='Pending')
        bt_id  = _seed_bank_txn(self.acct_id, amount=500.0, desc='CONTRACTOR PMT',
                                txn_date='2025-05-04', match_status='Unmatched')  # 3 days off

        from routes_phase8 import _run_auto_match
        with _db() as conn:
            matched = _run_auto_match(self.acct_id, conn)
        self.assertGreater(matched, 0)

    def test_auto_match_no_match_outside_date(self):
        """Should NOT match if date is > 5 days apart."""
        led_id = _seed_ledger(amount=750.0, desc='Far Date', entry_date='2025-06-01',
                              bank_acct_id=self.acct_id, status='Pending')
        bt_id  = _seed_bank_txn(self.acct_id, amount=750.0, desc='FAR DATE TXN',
                                txn_date='2025-06-10', match_status='Unmatched')  # 9 days off

        from routes_phase8 import _run_auto_match
        with _db() as conn:
            matched = _run_auto_match(self.acct_id, conn)

        conn = get_connection()
        bt = conn.execute("SELECT match_status FROM bank_transactions WHERE id=?", [bt_id]).fetchone()
        conn.close()
        self.assertEqual(bt['match_status'], 'Unmatched')

    def test_auto_match_api(self):
        r = _post_json('/api/recon/auto-match', {'account_id': self.acct_id})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertIn('matched', d)

    def test_auto_match_api_missing_acct(self):
        r = _post_json('/api/recon/auto-match', {})
        self.assertEqual(r.status_code, 400)


# ════════════════════════════════════════════════════════════════
class TestManualMatch(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('Manual Match Account')

    def test_manual_match(self):
        led_id = _seed_ledger(amount=175.0, bank_acct_id=self.acct_id)
        bt_id  = _seed_bank_txn(self.acct_id, amount=175.0)

        r = _post_json('/api/recon/match', {
            'bank_transaction_id': bt_id,
            'ledger_id': led_id,
        })
        d = json.loads(r.data)
        self.assertTrue(d['success'])

        conn = get_connection()
        bt  = conn.execute("SELECT match_status, matched_ledger_id FROM bank_transactions WHERE id=?", [bt_id]).fetchone()
        led = conn.execute("SELECT status FROM ledger WHERE id=?", [led_id]).fetchone()
        conn.close()
        self.assertEqual(bt['match_status'], 'Manual-Matched')
        self.assertEqual(bt['matched_ledger_id'], led_id)
        self.assertEqual(led['status'], 'Cleared')

    def test_manual_match_missing_params(self):
        r = _post_json('/api/recon/match', {'bank_transaction_id': 1})
        self.assertEqual(r.status_code, 400)

    def test_manual_match_not_found(self):
        r = _post_json('/api/recon/match', {'bank_transaction_id': 99999, 'ledger_id': 99999})
        self.assertEqual(r.status_code, 404)


# ════════════════════════════════════════════════════════════════
class TestUnmatch(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('Unmatch Account')

    def test_unmatch(self):
        led_id = _seed_ledger(amount=200.0, bank_acct_id=self.acct_id)
        bt_id  = _seed_bank_txn(self.acct_id, amount=200.0)
        _post_json('/api/recon/match', {'bank_transaction_id': bt_id, 'ledger_id': led_id})

        r = _post_json('/api/recon/unmatch', {'bank_transaction_id': bt_id})
        d = json.loads(r.data)
        self.assertTrue(d['success'])

        conn = get_connection()
        bt  = conn.execute("SELECT match_status, matched_ledger_id FROM bank_transactions WHERE id=?", [bt_id]).fetchone()
        led = conn.execute("SELECT status FROM ledger WHERE id=?", [led_id]).fetchone()
        conn.close()
        self.assertEqual(bt['match_status'], 'Unmatched')
        self.assertIsNone(bt['matched_ledger_id'])
        self.assertEqual(led['status'], 'Pending')

    def test_unmatch_missing_param(self):
        r = _post_json('/api/recon/unmatch', {})
        self.assertEqual(r.status_code, 400)


# ════════════════════════════════════════════════════════════════
class TestExclude(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('Exclude Account')

    def test_exclude(self):
        bt_id = _seed_bank_txn(self.acct_id, amount=15.0, desc='Bank Fee')
        r = _post_json('/api/recon/exclude', {
            'bank_transaction_id': bt_id,
            'notes': 'Monthly bank fee'
        })
        d = json.loads(r.data)
        self.assertTrue(d['success'])

        conn = get_connection()
        bt = conn.execute("SELECT match_status, notes FROM bank_transactions WHERE id=?", [bt_id]).fetchone()
        conn.close()
        self.assertEqual(bt['match_status'], 'Excluded')
        self.assertEqual(bt['notes'], 'Monthly bank fee')

    def test_exclude_missing_param(self):
        r = _post_json('/api/recon/exclude', {})
        self.assertEqual(r.status_code, 400)


# ════════════════════════════════════════════════════════════════
class TestCreateLedger(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('Create Ledger Account')

    def test_create_ledger_from_bank_txn(self):
        bt_id = _seed_bank_txn(self.acct_id, amount=89.99, desc='Office Depot')
        r = _post_json('/api/recon/create-ledger', {
            'bank_transaction_id': bt_id,
            'category': 'Office Supplies',
            'vendor': 'Office Depot',
        })
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertIn('ledger_id', d)

        conn = get_connection()
        led = conn.execute("SELECT * FROM ledger WHERE id=?", [d['ledger_id']]).fetchone()
        bt  = conn.execute("SELECT match_status FROM bank_transactions WHERE id=?", [bt_id]).fetchone()
        conn.close()
        self.assertIsNotNone(led)
        self.assertAlmostEqual(float(led['amount']), 89.99)
        self.assertEqual(led['category'], 'Office Supplies')
        self.assertEqual(led['status'], 'Cleared')
        self.assertEqual(bt['match_status'], 'Manual-Matched')

    def test_create_ledger_missing_bt(self):
        r = _post_json('/api/recon/create-ledger', {'category': 'Test'})
        self.assertEqual(r.status_code, 400)

    def test_create_ledger_bt_not_found(self):
        r = _post_json('/api/recon/create-ledger', {'bank_transaction_id': 99999})
        self.assertEqual(r.status_code, 404)


# ════════════════════════════════════════════════════════════════
class TestDeleteTransaction(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('Delete Txn Account')

    def test_delete_is_soft(self):
        bt_id = _seed_bank_txn(self.acct_id, amount=50.0)
        r = _post_json(f'/api/recon/transaction/{bt_id}/delete', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])

        conn = get_connection()
        bt = conn.execute("SELECT is_deleted FROM bank_transactions WHERE id=?", [bt_id]).fetchone()
        conn.close()
        self.assertEqual(bt['is_deleted'], 1)

    def test_delete_unclears_ledger(self):
        led_id = _seed_ledger(amount=300.0, bank_acct_id=self.acct_id)
        bt_id  = _seed_bank_txn(self.acct_id, amount=300.0)
        _post_json('/api/recon/match', {'bank_transaction_id': bt_id, 'ledger_id': led_id})
        _post_json(f'/api/recon/transaction/{bt_id}/delete', {})

        conn = get_connection()
        led = conn.execute("SELECT status FROM ledger WHERE id=?", [led_id]).fetchone()
        conn.close()
        self.assertEqual(led['status'], 'Pending')


# ════════════════════════════════════════════════════════════════
class TestLedgerSearch(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('Search Account')
        _seed_ledger(amount=500.0, desc='Plumbing Materials', category='Materials',
                     bank_acct_id=self.acct_id, entry_date='2025-08-01')

    def test_search_by_desc(self):
        d = _get_json(f'/api/recon/ledger-search?acct_id={self.acct_id}&q=Plumbing')
        self.assertIsInstance(d, list)
        self.assertTrue(any('Plumbing' in r['description'] for r in d))

    def test_search_by_amount(self):
        d = _get_json(f'/api/recon/ledger-search?acct_id={self.acct_id}&amount=500.0')
        self.assertTrue(any(abs(float(r['amount']) - 500.0) < 0.02 for r in d))

    def test_search_returns_list(self):
        d = _get_json('/api/recon/ledger-search')
        self.assertIsInstance(d, list)

    def test_search_limit(self):
        for i in range(5):
            _seed_ledger(amount=100+i, desc=f'Search Limit Test {i}', bank_acct_id=self.acct_id)
        d = _get_json(f'/api/recon/ledger-search?acct_id={self.acct_id}&limit=3')
        self.assertLessEqual(len(d), 3)

    def test_cleared_entries_excluded(self):
        _seed_ledger(amount=999.0, desc='Already Cleared', bank_acct_id=self.acct_id, status='Cleared')
        d = _get_json(f'/api/recon/ledger-search?acct_id={self.acct_id}&q=Already+Cleared')
        # Cleared entries should not appear
        self.assertFalse(any(r['description'] == 'Already Cleared' for r in d))


# ════════════════════════════════════════════════════════════════
class TestReconciliationSession(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('Session Account', balance=10000.0)

    def test_session_list_loads(self):
        r = CLIENT.get(f'/reconciliation/{self.acct_id}/session')
        self.assertEqual(r.status_code, 200)

    def test_session_create(self):
        r = CLIENT.post(f'/reconciliation/{self.acct_id}/session', data={
            'statement_end_date': '2025-09-30',
            'statement_ending_balance': '9500.00',
            'notes': 'September reconciliation',
        }, follow_redirects=False)
        # Should redirect to session detail
        self.assertEqual(r.status_code, 302)
        conn = get_connection()
        sess = conn.execute(
            "SELECT * FROM reconciliation_sessions WHERE bank_account_id=? AND is_deleted=0 ORDER BY id DESC LIMIT 1",
            [self.acct_id]
        ).fetchone()
        conn.close()
        self.assertIsNotNone(sess)
        self.assertEqual(sess['statement_end_date'], '2025-09-30')
        self.assertAlmostEqual(float(sess['statement_ending_balance']), 9500.0)

    def test_session_detail_loads(self):
        r = CLIENT.post(f'/reconciliation/{self.acct_id}/session', data={
            'statement_end_date': '2025-10-31',
            'statement_ending_balance': '12000.00',
        }, follow_redirects=False)
        # Get the new session id from redirect
        loc = r.headers.get('Location', '')
        sess_id = int(loc.split('/')[-1]) if loc.endswith(tuple('0123456789')) else None
        if sess_id:
            r2 = CLIENT.get(f'/reconciliation/{self.acct_id}/session/{sess_id}')
            self.assertEqual(r2.status_code, 200)

    def test_session_complete(self):
        # Create session
        CLIENT.post(f'/reconciliation/{self.acct_id}/session', data={
            'statement_end_date': '2025-11-30',
            'statement_ending_balance': '10000.00',
        })
        conn = get_connection()
        sess = conn.execute(
            "SELECT id FROM reconciliation_sessions WHERE bank_account_id=? ORDER BY id DESC LIMIT 1",
            [self.acct_id]
        ).fetchone()
        conn.close()
        sess_id = sess['id']

        r = _post_json(f'/api/recon/session/{sess_id}/complete', {'completed_by': 'Test User'})
        d = json.loads(r.data)
        self.assertTrue(d['success'])

        conn = get_connection()
        sess = conn.execute("SELECT status, completed_by FROM reconciliation_sessions WHERE id=?", [sess_id]).fetchone()
        conn.close()
        self.assertEqual(sess['status'], 'Complete')
        self.assertEqual(sess['completed_by'], 'Test User')


# ════════════════════════════════════════════════════════════════
class TestExport(unittest.TestCase):

    def setUp(self):
        self.acct_id = _seed_account('Export Account')
        _seed_bank_txn(self.acct_id, amount=100.0, desc='Export Test 1')
        _seed_bank_txn(self.acct_id, amount=200.0, desc='Export Test 2', match_status='Excluded')

    def test_export_unmatched(self):
        r = CLIENT.get(f'/reconciliation/{self.acct_id}/export?status=Unmatched')
        self.assertEqual(r.status_code, 200)
        self.assertIn('text/csv', r.content_type)
        self.assertIn(b'transaction_date', r.data)
        self.assertIn(b'Export Test 1', r.data)

    def test_export_all(self):
        r = CLIENT.get(f'/reconciliation/{self.acct_id}/export?status=All')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Export Test', r.data)

    def test_export_excluded(self):
        r = CLIENT.get(f'/reconciliation/{self.acct_id}/export?status=Excluded')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Export Test 2', r.data)


# ════════════════════════════════════════════════════════════════
class TestHelpers(unittest.TestCase):

    def test_amount_match_exact(self):
        from routes_phase8 import _amount_match
        self.assertTrue(_amount_match(100.0, 100.0))

    def test_amount_match_within_tolerance(self):
        from routes_phase8 import _amount_match
        self.assertTrue(_amount_match(100.0, 100.005))

    def test_amount_match_outside_tolerance(self):
        from routes_phase8 import _amount_match
        self.assertFalse(_amount_match(100.0, 101.0))

    def test_date_proximity_same(self):
        from routes_phase8 import _date_proximity
        self.assertTrue(_date_proximity('2025-01-01', '2025-01-01'))

    def test_date_proximity_within_3(self):
        from routes_phase8 import _date_proximity
        self.assertTrue(_date_proximity('2025-01-01', '2025-01-03'))

    def test_date_proximity_outside(self):
        from routes_phase8 import _date_proximity
        self.assertFalse(_date_proximity('2025-01-01', '2025-01-10'))

    def test_desc_similarity_identical(self):
        from routes_phase8 import _desc_similarity
        self.assertAlmostEqual(_desc_similarity('LUMBER CO', 'LUMBER CO'), 1.0)

    def test_desc_similarity_partial(self):
        from routes_phase8 import _desc_similarity
        s = _desc_similarity('LUMBER SUPPLY CO', 'LUMBER CO')
        self.assertGreater(s, 0.4)

    def test_desc_similarity_no_match(self):
        from routes_phase8 import _desc_similarity
        s = _desc_similarity('PAYROLL', 'OFFICE SUPPLIES')
        self.assertLess(s, 0.3)

    def test_parse_date_formats(self):
        from routes_phase8 import _parse_date
        self.assertEqual(_parse_date('2025-01-15'), '2025-01-15')
        self.assertEqual(_parse_date('01/15/2025'), '2025-01-15')
        self.assertEqual(_parse_date('01/15/25'),   '2025-01-15')
        self.assertEqual(_parse_date('not-a-date'), '')

    def test_parse_amount(self):
        from routes_phase8 import _parse_amount, _parse_amount_signed
        self.assertAlmostEqual(_parse_amount('$1,500.00'), 1500.0)
        self.assertAlmostEqual(_parse_amount('-250.00'), 250.0)
        self.assertAlmostEqual(_parse_amount_signed('-300.00'), -300.0)
        self.assertAlmostEqual(_parse_amount_signed('(400.00)'), -400.0)
        self.assertAlmostEqual(_parse_amount_signed('500.00'), 500.0)


# ════════════════════════════════════════════════════════════════
class TestRegressionPhase8(unittest.TestCase):

    def test_tax_still_loads(self):
        r = CLIENT.get('/tax')
        self.assertEqual(r.status_code, 200)

    def test_payroll_still_loads(self):
        r = CLIENT.get('/payroll')
        self.assertEqual(r.status_code, 200)

    def test_invoices_still_loads(self):
        r = CLIENT.get('/invoices')
        self.assertEqual(r.status_code, 200)

    def test_dashboard_still_loads(self):
        r = CLIENT.get('/')
        self.assertEqual(r.status_code, 200)

    def test_ledger_still_loads(self):
        r = CLIENT.get('/ledger')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    total  = result.testsRun
    if result.wasSuccessful():
        print(f"\n✅ All {total} Phase 8 tests passed!")
    else:
        fails = len(result.failures)
        errs  = len(result.errors)
        print(f"\n❌ {fails} failure(s), {errs} error(s) out of {total}")
    sys.exit(0 if result.wasSuccessful() else 1)
