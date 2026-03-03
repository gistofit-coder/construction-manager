"""
Phase 11 Tests — Certs Tracker & Quick Quote
Run: python3 tests_phase11.py
"""
import os, sys, json, unittest
from datetime import date, timedelta

TEST_DB = '/tmp/test_p11.db'
os.environ['CONSTRUCTION_DB'] = TEST_DB
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

from database import init_db, get_connection
init_db(TEST_DB)

from app import app as flask_app
flask_app.config['TESTING'] = True
CLIENT = flask_app.test_client()

TODAY      = date.today().strftime('%Y-%m-%d')
FUTURE_60  = (date.today() + timedelta(days=60)).strftime('%Y-%m-%d')
FUTURE_90  = (date.today() + timedelta(days=90)).strftime('%Y-%m-%d')
FUTURE_180 = (date.today() + timedelta(days=180)).strftime('%Y-%m-%d')
PAST_30    = (date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
PAST_90    = (date.today() - timedelta(days=90)).strftime('%Y-%m-%d')

from database import db as _db

# ─────────────────────────────────────────────
# Seed helpers
# ─────────────────────────────────────────────

_ctr = 0
def _uid():
    global _ctr; _ctr += 1
    return _ctr

def _seed_contractor(name=None, trade='Plumbing'):
    name = name or f"Contractor {_uid()}"
    with _db() as conn:
        cur = conn.execute(
            "INSERT INTO contractors (company_name, trade_type) VALUES (?,?)",
            [name, trade]
        )
        return cur.lastrowid

def _seed_cert(contractor_id=None, cert_type='General Liability',
               start_date=None, end_date=None):
    start_date = start_date or TODAY
    end_date   = end_date or FUTURE_180
    with _db() as conn:
        cur = conn.execute("""
            INSERT INTO certificates
                (contractor_id, cert_type, start_date, end_date, notes)
            VALUES (?,?,?,?,'Test cert')
        """, [contractor_id, cert_type, start_date, end_date])
        return cur.lastrowid

def _post_json(url, data):
    return CLIENT.post(url, data=json.dumps(data),
                       content_type='application/json')


# ════════════════════════════════════════════════════════════════
class TestCertsList(unittest.TestCase):

    def setUp(self):
        self.con_id = _seed_contractor('List Contractor')
        self.cert_active    = _seed_cert(self.con_id, end_date=FUTURE_180)
        self.cert_expiring  = _seed_cert(self.con_id, end_date=FUTURE_60)
        self.cert_expired   = _seed_cert(self.con_id, end_date=PAST_30)

    def test_list_loads(self):
        r = CLIENT.get('/certs', follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Certs Tracker', r.data)

    def test_list_shows_contractor(self):
        r = CLIENT.get('/certs')
        self.assertIn(b'List Contractor', r.data)

    def test_filter_expired(self):
        r = CLIENT.get('/certs?status=Expired')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Expired', r.data)

    def test_filter_expiring_soon(self):
        r = CLIENT.get('/certs?status=Expiring Soon')
        self.assertEqual(r.status_code, 200)

    def test_filter_active(self):
        r = CLIENT.get('/certs?status=Active')
        self.assertEqual(r.status_code, 200)

    def test_filter_cert_type(self):
        r = CLIENT.get('/certs?cert_type=General Liability')
        self.assertEqual(r.status_code, 200)

    def test_filter_contractor_name(self):
        r = CLIENT.get('/certs?contractor=List+Contractor')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'List Contractor', r.data)

    def test_kpi_tiles_present(self):
        r = CLIENT.get('/certs')
        self.assertIn(b'Expired', r.data)
        self.assertIn(b'Active', r.data)


# ════════════════════════════════════════════════════════════════
class TestCertCreate(unittest.TestCase):

    def setUp(self):
        self.con_id = _seed_contractor('Create Contractor')

    def test_new_form_loads(self):
        r = CLIENT.get('/certs/new')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'contractor_id', r.data)

    def test_create_cert(self):
        r = CLIENT.post('/certs/new', data={
            'contractor_id': str(self.con_id),
            'cert_type':     'Workers Compensation',
            'start_date':    TODAY,
            'end_date':      FUTURE_180,
            'notes':         'Policy #12345',
        }, follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        conn = get_connection()
        cert = conn.execute(
            "SELECT * FROM certificates WHERE contractor_id=? AND cert_type='Workers Compensation' AND is_deleted=0",
            [self.con_id]
        ).fetchone()
        conn.close()
        self.assertIsNotNone(cert)

    def test_create_saves_notes(self):
        CLIENT.post('/certs/new', data={
            'contractor_id': str(self.con_id),
            'cert_type':     'Bond',
            'end_date':      FUTURE_90,
            'notes':         'Coverage: $1M',
        }, follow_redirects=True)
        conn = get_connection()
        cert = conn.execute(
            "SELECT * FROM certificates WHERE cert_type='Bond' AND contractor_id=? AND is_deleted=0",
            [self.con_id]
        ).fetchone()
        conn.close()
        self.assertIsNotNone(cert)
        self.assertIn('Coverage', cert['notes'])

    def test_create_missing_cert_type_rejected(self):
        r = CLIENT.post('/certs/new', data={
            'contractor_id': str(self.con_id),
            'cert_type':     '',
            'end_date':      FUTURE_90,
        }, follow_redirects=True)
        # Should redirect back without saving
        conn = get_connection()
        count = conn.execute(
            "SELECT COUNT(*) FROM certificates WHERE contractor_id=? AND cert_type='' AND is_deleted=0",
            [self.con_id]
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

    def test_prefill_contractor(self):
        r = CLIENT.get(f'/certs/new?contractor_id={self.con_id}')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestCertEdit(unittest.TestCase):

    def setUp(self):
        self.con_id  = _seed_contractor('Edit Contractor')
        self.cert_id = _seed_cert(self.con_id, cert_type='Auto Liability',
                                  end_date=FUTURE_90)

    def test_edit_form_loads(self):
        r = CLIENT.get(f'/certs/{self.cert_id}/edit')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Auto Liability', r.data)

    def test_edit_updates_end_date(self):
        new_end = (date.today() + timedelta(days=365)).strftime('%Y-%m-%d')
        CLIENT.post(f'/certs/{self.cert_id}/edit', data={
            'contractor_id': str(self.con_id),
            'cert_type':     'Auto Liability',
            'start_date':    TODAY,
            'end_date':      new_end,
            'notes':         '',
        }, follow_redirects=True)
        conn = get_connection()
        cert = conn.execute("SELECT end_date FROM certificates WHERE id=?", [self.cert_id]).fetchone()
        conn.close()
        self.assertEqual(cert['end_date'], new_end)

    def test_edit_missing_redirects(self):
        r = CLIENT.get('/certs/99999/edit', follow_redirects=True)
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestCertDelete(unittest.TestCase):

    def test_delete_soft(self):
        con_id  = _seed_contractor('Delete Contractor')
        cert_id = _seed_cert(con_id)
        r = _post_json(f'/api/certs/{cert_id}/delete', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        conn = get_connection()
        cert = conn.execute("SELECT is_deleted FROM certificates WHERE id=?", [cert_id]).fetchone()
        conn.close()
        self.assertEqual(cert['is_deleted'], 1)

    def test_delete_not_found(self):
        r = _post_json('/api/certs/99999/delete', {})
        self.assertEqual(r.status_code, 404)

    def test_delete_disappears_from_list(self):
        con_id  = _seed_contractor('Disappear Contractor')
        cert_id = _seed_cert(con_id)
        _post_json(f'/api/certs/{cert_id}/delete', {})
        conn = get_connection()
        cert = conn.execute(
            "SELECT * FROM certificates WHERE id=? AND is_deleted=0", [cert_id]
        ).fetchone()
        conn.close()
        self.assertIsNone(cert)


# ════════════════════════════════════════════════════════════════
class TestCertRenew(unittest.TestCase):

    def setUp(self):
        con_id        = _seed_contractor('Renew Contractor')
        self.cert_id  = _seed_cert(con_id, end_date=PAST_30)  # expired

    def test_renew_updates_end_date(self):
        new_end = FUTURE_180
        r = _post_json(f'/api/certs/{self.cert_id}/renew', {'end_date': new_end})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertEqual(d['end_date'], new_end)

    def test_renew_returns_new_status(self):
        r = _post_json(f'/api/certs/{self.cert_id}/renew', {'end_date': FUTURE_180})
        d = json.loads(r.data)
        self.assertEqual(d['status'], 'Active')

    def test_renew_expiring_soon_status(self):
        r = _post_json(f'/api/certs/{self.cert_id}/renew', {'end_date': FUTURE_60})
        d = json.loads(r.data)
        self.assertIn(d['status'], ['Expiring Soon', 'Active'])

    def test_renew_persists_to_db(self):
        new_end = FUTURE_180
        _post_json(f'/api/certs/{self.cert_id}/renew', {'end_date': new_end})
        conn = get_connection()
        cert = conn.execute("SELECT end_date FROM certificates WHERE id=?", [self.cert_id]).fetchone()
        conn.close()
        self.assertEqual(cert['end_date'], new_end)

    def test_renew_missing_end_date(self):
        r = _post_json(f'/api/certs/{self.cert_id}/renew', {})
        self.assertEqual(r.status_code, 400)

    def test_renew_not_found(self):
        r = _post_json('/api/certs/99999/renew', {'end_date': FUTURE_180})
        self.assertEqual(r.status_code, 404)

    def test_renew_with_start_date(self):
        r = _post_json(f'/api/certs/{self.cert_id}/renew',
                       {'end_date': FUTURE_180, 'start_date': TODAY})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        conn = get_connection()
        cert = conn.execute("SELECT start_date FROM certificates WHERE id=?", [self.cert_id]).fetchone()
        conn.close()
        self.assertEqual(cert['start_date'], TODAY)


# ════════════════════════════════════════════════════════════════
class TestCertStatus(unittest.TestCase):
    """Test the _cert_status helper function."""

    def test_expired(self):
        from routes_phase11 import _cert_status
        self.assertEqual(_cert_status(PAST_90), 'Expired')

    def test_expiring_soon(self):
        from routes_phase11 import _cert_status
        soon = (date.today() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.assertEqual(_cert_status(soon), 'Expiring Soon')

    def test_active(self):
        from routes_phase11 import _cert_status
        self.assertEqual(_cert_status(FUTURE_180), 'Active')

    def test_unknown_empty(self):
        from routes_phase11 import _cert_status
        self.assertEqual(_cert_status(''), 'Unknown')

    def test_unknown_none(self):
        from routes_phase11 import _cert_status
        self.assertEqual(_cert_status(None), 'Unknown')

    def test_boundary_today_expired(self):
        from routes_phase11 import _cert_status
        yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.assertEqual(_cert_status(yesterday), 'Expired')

    def test_boundary_60_days_expiring(self):
        from routes_phase11 import _cert_status
        sixty = FUTURE_60
        self.assertEqual(_cert_status(sixty), 'Expiring Soon')

    def test_boundary_61_days_active(self):
        from routes_phase11 import _cert_status
        sixty_one = (date.today() + timedelta(days=61)).strftime('%Y-%m-%d')
        self.assertEqual(_cert_status(sixty_one), 'Active')


# ════════════════════════════════════════════════════════════════
class TestCertContractorView(unittest.TestCase):

    def setUp(self):
        self.con_id = _seed_contractor('Contractor View Inc')
        _seed_cert(self.con_id, cert_type='General Liability', end_date=FUTURE_180)
        _seed_cert(self.con_id, cert_type='Workers Compensation', end_date=FUTURE_90)

    def test_contractor_view_loads(self):
        r = CLIENT.get(f'/certs/contractor/{self.con_id}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Contractor View Inc', r.data)

    def test_contractor_view_shows_certs(self):
        r = CLIENT.get(f'/certs/contractor/{self.con_id}')
        self.assertIn(b'General Liability', r.data)
        self.assertIn(b'Workers Compensation', r.data)

    def test_contractor_view_missing(self):
        r = CLIENT.get('/certs/contractor/99999', follow_redirects=True)
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
class TestCertExport(unittest.TestCase):

    def setUp(self):
        con_id = _seed_contractor('Export Contractor')
        _seed_cert(con_id)

    def test_export_csv(self):
        r = CLIENT.get('/certs/export')
        self.assertEqual(r.status_code, 200)
        self.assertIn('text/csv', r.content_type)
        self.assertIn(b'cert_type', r.data)

    def test_export_has_data(self):
        r = CLIENT.get('/certs/export')
        lines = r.data.decode().split('\n')
        self.assertGreater(len(lines), 1)

    def test_export_has_status_column(self):
        r = CLIENT.get('/certs/export')
        self.assertIn(b'status', r.data)


# ════════════════════════════════════════════════════════════════
class TestQuickQuotePage(unittest.TestCase):

    def test_page_loads(self):
        r = CLIENT.get('/quote', follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Quick Quote', r.data)

    def test_page_shows_mode_tabs(self):
        r = CLIENT.get('/quote', follow_redirects=True)
        self.assertIn(b'Build Up', r.data)
        self.assertIn(b'Top Down', r.data)

    def test_page_shows_rate_sliders(self):
        r = CLIENT.get('/quote', follow_redirects=True)
        self.assertIn(b'Overhead', r.data)
        self.assertIn(b'Insurance', r.data)
        self.assertIn(b'Profit', r.data)

    def test_page_shows_save_button(self):
        r = CLIENT.get('/quote', follow_redirects=True)
        self.assertIn(b'Save as Draft Estimate', r.data)


# ════════════════════════════════════════════════════════════════
class TestQuickQuoteCalculate(unittest.TestCase):
    """Test /api/quick-quote/calculate in both modes."""

    def _calc(self, payload):
        return json.loads(_post_json('/api/quick-quote/calculate', payload).data)

    def test_buildup_basic(self):
        d = self._calc({
            'mode': 'buildUp',
            'labor': 1000, 'materials': 500, 'subs': 0, 'other': 0,
            'overhead_pct': 0, 'insurance_pct': 0,
            'owner_wages_pct': 0, 'profit_pct': 0,
        })
        self.assertAlmostEqual(d['direct'], 1500.0)
        self.assertAlmostEqual(d['sell_price'], 1500.0)

    def test_buildup_with_overhead(self):
        d = self._calc({
            'mode': 'buildUp',
            'labor': 1000, 'materials': 0, 'subs': 0, 'other': 0,
            'overhead_pct': 15, 'insurance_pct': 0,
            'owner_wages_pct': 0, 'profit_pct': 0,
        })
        self.assertAlmostEqual(d['overhead_amt'], 150.0)
        self.assertAlmostEqual(d['sell_price'], 1150.0)

    def test_buildup_with_profit(self):
        d = self._calc({
            'mode': 'buildUp',
            'labor': 1000, 'materials': 0, 'subs': 0, 'other': 0,
            'overhead_pct': 0, 'insurance_pct': 0,
            'owner_wages_pct': 0, 'profit_pct': 10,
        })
        self.assertAlmostEqual(d['profit_amt'], 100.0)
        self.assertAlmostEqual(d['sell_price'], 1100.0)

    def test_buildup_full_stack(self):
        """$10k direct, 15% OH, 5% ins, 10% OW, 10% profit → known result."""
        d = self._calc({
            'mode': 'buildUp',
            'labor': 6000, 'materials': 4000, 'subs': 0, 'other': 0,
            'overhead_pct': 15, 'insurance_pct': 5,
            'owner_wages_pct': 10, 'profit_pct': 10,
        })
        self.assertAlmostEqual(d['direct'], 10000.0)
        self.assertAlmostEqual(d['overhead_amt'], 1500.0)
        self.assertAlmostEqual(d['insurance_amt'], 500.0)
        self.assertAlmostEqual(d['owner_wages_amt'], 1000.0)
        subtotal = 10000 + 1500 + 500 + 1000  # 13000
        self.assertAlmostEqual(d['subtotal'], subtotal)
        self.assertAlmostEqual(d['profit_amt'], 1300.0)
        self.assertAlmostEqual(d['sell_price'], 14300.0)

    def test_buildup_margin_computed(self):
        d = self._calc({
            'mode': 'buildUp',
            'labor': 1000, 'materials': 0, 'subs': 0, 'other': 0,
            'overhead_pct': 0, 'insurance_pct': 0,
            'owner_wages_pct': 0, 'profit_pct': 20,
        })
        self.assertIn('margin', d)
        self.assertGreater(d['margin'], 0)

    def test_topdown_basic(self):
        d = self._calc({
            'mode': 'topDown',
            'sell_price': 10000,
            'overhead_pct': 0, 'insurance_pct': 0,
            'owner_wages_pct': 0, 'profit_pct': 0,
        })
        self.assertAlmostEqual(d['sell_price'], 10000.0)
        self.assertAlmostEqual(d['direct'], 10000.0)

    def test_topdown_with_profit(self):
        d = self._calc({
            'mode': 'topDown',
            'sell_price': 10000,
            'overhead_pct': 0, 'insurance_pct': 0,
            'owner_wages_pct': 0, 'profit_pct': 10,
        })
        self.assertAlmostEqual(d['profit_amt'], 1000.0)
        self.assertAlmostEqual(d['direct'], 9000.0, places=1)

    def test_topdown_full_stack(self):
        d = self._calc({
            'mode': 'topDown',
            'sell_price': 14300,
            'overhead_pct': 15, 'insurance_pct': 5,
            'owner_wages_pct': 10, 'profit_pct': 10,
        })
        # total_pct = 40%, so direct = 14300 * 0.6 = 8580
        self.assertAlmostEqual(d['direct'], 8580.0, places=1)
        self.assertAlmostEqual(d['sell_price'], 14300.0)

    def test_invalid_mode(self):
        r = _post_json('/api/quick-quote/calculate', {'mode': 'bogus'})
        self.assertEqual(r.status_code, 400)

    def test_zero_values(self):
        d = self._calc({
            'mode': 'buildUp',
            'labor': 0, 'materials': 0, 'subs': 0, 'other': 0,
            'overhead_pct': 15, 'insurance_pct': 5,
            'owner_wages_pct': 10, 'profit_pct': 10,
        })
        self.assertEqual(d['sell_price'], 0.0)

    def test_response_has_all_fields(self):
        d = self._calc({
            'mode': 'buildUp',
            'labor': 500, 'materials': 0, 'subs': 0, 'other': 0,
            'overhead_pct': 10, 'insurance_pct': 5,
            'owner_wages_pct': 8, 'profit_pct': 12,
        })
        for key in ['direct', 'overhead_amt', 'insurance_amt', 'owner_wages_amt',
                    'subtotal', 'profit_amt', 'sell_price', 'margin']:
            self.assertIn(key, d, f"Missing field: {key}")


# ════════════════════════════════════════════════════════════════
class TestSaveAsEstimate(unittest.TestCase):

    def _save(self, **kwargs):
        payload = {
            'sell_price':      20000,
            'direct':          12000,
            'labor':           8000,
            'materials':       4000,
            'subs':            0,
            'other':           0,
            'overhead_pct':    15,
            'insurance_pct':   5,
            'owner_wages_pct': 10,
            'profit_pct':      10,
            'overhead_amt':    1800,
            'insurance_amt':   600,
            'owner_wages_amt': 1200,
            'profit_amt':      1800,
            'subtotal':        15600,
            'quote_name':      'Test Quote',
        }
        payload.update(kwargs)
        return _post_json('/api/quick-quote/save-as-estimate', payload)

    def test_save_creates_estimate(self):
        r = self._save()
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertIn('estimate_id', d)
        self.assertIn('estimate_number', d)

    def test_saved_estimate_in_db(self):
        r = self._save(quote_name='DB Test Quote')
        d = json.loads(r.data)
        conn = get_connection()
        est = conn.execute(
            "SELECT * FROM job_estimates WHERE id=?", [d['estimate_id']]
        ).fetchone()
        conn.close()
        self.assertIsNotNone(est)
        self.assertEqual(est['status'], 'Draft')

    def test_saved_estimate_number_format(self):
        r = self._save()
        d = json.loads(r.data)
        self.assertIn('EST-', d['estimate_number'])

    def test_saved_estimate_sell_price(self):
        r = self._save(sell_price=25000, direct=15000)
        d = json.loads(r.data)
        conn = get_connection()
        est = conn.execute(
            "SELECT total_estimate FROM job_estimates WHERE id=?", [d['estimate_id']]
        ).fetchone()
        conn.close()
        self.assertAlmostEqual(float(est['total_estimate']), 25000.0, places=2)

    def test_saved_estimate_has_line_items(self):
        r = self._save(labor=5000, materials=3000, subs=0, other=0)
        d = json.loads(r.data)
        conn = get_connection()
        items = conn.execute(
            "SELECT * FROM estimate_line_items WHERE estimate_id=? AND is_deleted=0",
            [d['estimate_id']]
        ).fetchall()
        conn.close()
        # Should have Labor and Materials line items (non-zero ones)
        cats = [i['category'] for i in items]
        self.assertIn('Labor', cats)
        self.assertIn('Materials', cats)

    def test_saved_estimate_with_job(self):
        with _db() as conn:
            cur = conn.execute(
                "INSERT INTO jobs (job_code, description) VALUES (?,?)",
                ['QJOB-001', 'Quote Job']
            )
            job_id = cur.lastrowid
        r = self._save(job_id=job_id)
        d = json.loads(r.data)
        conn = get_connection()
        est = conn.execute(
            "SELECT job_id FROM job_estimates WHERE id=?", [d['estimate_id']]
        ).fetchone()
        conn.close()
        self.assertEqual(est['job_id'], job_id)

    def test_save_notes_included(self):
        r = self._save(notes='Special client requirements')
        d = json.loads(r.data)
        conn = get_connection()
        est = conn.execute(
            "SELECT notes FROM job_estimates WHERE id=?", [d['estimate_id']]
        ).fetchone()
        conn.close()
        self.assertIn('Special client requirements', est['notes'])


# ════════════════════════════════════════════════════════════════
class TestRegressionPhase11(unittest.TestCase):
    """Verify all previous phases still work."""

    def test_reports_hub(self):
        r = CLIENT.get('/reports', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_estimates_list(self):
        r = CLIENT.get('/estimates')
        self.assertEqual(r.status_code, 200)

    def test_reconciliation(self):
        r = CLIENT.get('/reconciliation')
        self.assertEqual(r.status_code, 200)

    def test_tax(self):
        r = CLIENT.get('/tax', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_payroll(self):
        r = CLIENT.get('/payroll', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_invoices(self):
        r = CLIENT.get('/invoices', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_dashboard(self):
        r = CLIENT.get('/')
        self.assertEqual(r.status_code, 200)

    def test_ledger(self):
        r = CLIENT.get('/ledger', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_pl_report(self):
        r = CLIENT.get('/reports/pl')
        self.assertEqual(r.status_code, 200)

    def test_job_profitability(self):
        r = CLIENT.get('/reports/jobs')
        self.assertEqual(r.status_code, 200)

    def test_ar_aging(self):
        r = CLIENT.get('/reports/ar')
        self.assertEqual(r.status_code, 200)


# ════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    total  = result.testsRun
    if result.wasSuccessful():
        print(f"\n✅ All {total} Phase 11 tests passed!")
    else:
        print(f"\n❌ {len(result.failures)} failure(s), {len(result.errors)} error(s) out of {total}")
    sys.exit(0 if result.wasSuccessful() else 1)
