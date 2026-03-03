"""
Phase 9 Tests — Job Estimates
Run: python3 tests_phase9.py
"""
import os, sys, json, unittest
from datetime import date

TEST_DB = '/tmp/test_p9.db'
os.environ['CONSTRUCTION_DB'] = TEST_DB
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

from database import init_db, get_connection
init_db(TEST_DB)

from app import app as flask_app
flask_app.config['TESTING'] = True
CLIENT = flask_app.test_client()

TODAY = date.today().strftime('%Y-%m-%d')

from database import db as _db

# ─────────────────────────────────────────────
# Seed helpers
# ─────────────────────────────────────────────

def _seed_client(name='Test Client'):
    with _db() as conn:
        cur = conn.execute(
            "INSERT INTO clients (full_name) VALUES (?)", [name]
        )
        return cur.lastrowid

_jc = 0
def _seed_job(code=None, client_id=None):
    global _jc; _jc += 1
    final = f"{code or 'JOB'}-{_jc}"
    with _db() as conn:
        cur = conn.execute(
            "INSERT INTO jobs (job_code, client_id, description) VALUES (?,?,?)",
            [final, client_id, 'Test Job']
        )
        return cur.lastrowid, final

def _create_estimate(job_id=None, client_id=None,
                     overhead=15.0, insurance=5.0, owner_wages=10.0, profit=10.0,
                     line_items=None):
    """POST to /estimates/new and return new estimate id."""
    if line_items is None:
        line_items = [{'category': 'Labor', 'labor_hours': 40, 'labor_rate': 50,
                       'materials': 0, 'subs': 0, 'other': 0, 'markup': 0}]

    data = {
        'job_id':           str(job_id or ''),
        'client_id':        str(client_id or ''),
        'estimate_date':    TODAY,
        'status':           'Draft',
        'overhead_pct':     str(overhead),
        'insurance_pct':    str(insurance),
        'owner_wages_pct':  str(owner_wages),
        'profit_pct':       str(profit),
        'notes':            'Test estimate',
    }
    # Parallel arrays
    data['li_category[]']    = [li['category']   for li in line_items]
    data['li_labor_hours[]'] = [str(li.get('labor_hours', 0)) for li in line_items]
    data['li_labor_rate[]']  = [str(li.get('labor_rate', 0))  for li in line_items]
    data['li_materials[]']   = [str(li.get('materials', 0))   for li in line_items]
    data['li_subs[]']        = [str(li.get('subs', 0))        for li in line_items]
    data['li_other[]']       = [str(li.get('other', 0))       for li in line_items]
    data['li_markup[]']      = [str(li.get('markup', 0))      for li in line_items]
    data['li_sell_price[]']  = [str(li.get('sell_price', 0))  for li in line_items]
    data['li_notes[]']       = [li.get('notes', '')           for li in line_items]

    r = CLIENT.post('/estimates/new', data=data, follow_redirects=True)
    assert r.status_code == 200, f"Create failed {r.status_code}"

    conn = get_connection()
    row = conn.execute(
        "SELECT id FROM job_estimates WHERE is_deleted=0 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return row['id'] if row else None

def _post_json(url, data):
    return CLIENT.post(url, data=json.dumps(data), content_type='application/json')

def _get_json(url):
    return json.loads(CLIENT.get(url).data)


# ════════════════════════════════════════════════════════════════
class TestEstimatesList(unittest.TestCase):

    def test_list_loads(self):
        r = CLIENT.get('/estimates')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Estimate', r.data)

    def test_list_filter_status(self):
        r = CLIENT.get('/estimates?status=Draft')
        self.assertEqual(r.status_code, 200)

    def test_list_filter_year(self):
        r = CLIENT.get(f'/estimates?year={date.today().year}')
        self.assertEqual(r.status_code, 200)

    def test_list_filter_client(self):
        r = CLIENT.get('/estimates?client=Smith')
        self.assertEqual(r.status_code, 200)

    def test_new_form_loads(self):
        r = CLIENT.get('/estimates/new')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'estimate_date', r.data)


# ════════════════════════════════════════════════════════════════
class TestEstimateCreate(unittest.TestCase):

    def setUp(self):
        self.client_id = _seed_client('Create Client')
        self.job_id, self.job_code = _seed_job('CREATE-001', self.client_id)

    def test_create_basic_estimate(self):
        est_id = _create_estimate(job_id=self.job_id, client_id=self.client_id)
        self.assertIsNotNone(est_id)
        conn = get_connection()
        est = conn.execute("SELECT * FROM job_estimates WHERE id=?", [est_id]).fetchone()
        conn.close()
        self.assertIsNotNone(est)
        self.assertEqual(est['status'], 'Draft')

    def test_auto_numbered(self):
        est_id = _create_estimate(job_id=self.job_id)
        conn = get_connection()
        est = conn.execute("SELECT estimate_number FROM job_estimates WHERE id=?", [est_id]).fetchone()
        conn.close()
        self.assertIn('EST-', est['estimate_number'])

    def test_version_number_starts_at_1(self):
        est_id = _create_estimate(job_id=self.job_id)
        conn = get_connection()
        est = conn.execute("SELECT version_number FROM job_estimates WHERE id=?", [est_id]).fetchone()
        conn.close()
        self.assertEqual(est['version_number'], 1)

    def test_line_items_saved(self):
        est_id = _create_estimate(
            job_id=self.job_id,
            line_items=[
                {'category': 'Labor',     'labor_hours': 20, 'labor_rate': 60},
                {'category': 'Materials', 'materials': 1500},
            ]
        )
        conn = get_connection()
        items = conn.execute(
            "SELECT * FROM estimate_line_items WHERE estimate_id=? AND is_deleted=0",
            [est_id]
        ).fetchall()
        conn.close()
        self.assertEqual(len(items), 2)
        cats = [i['category'] for i in items]
        self.assertIn('Labor', cats)
        self.assertIn('Materials', cats)

    def test_labor_cost_computed(self):
        """labor_cost = labor_hours × labor_rate"""
        est_id = _create_estimate(
            job_id=self.job_id,
            line_items=[{'category': 'Labor', 'labor_hours': 10, 'labor_rate': 75}]
        )
        conn = get_connection()
        item = conn.execute(
            "SELECT labor_cost FROM estimate_line_items WHERE estimate_id=? AND is_deleted=0",
            [est_id]
        ).fetchone()
        conn.close()
        self.assertAlmostEqual(float(item['labor_cost']), 750.0, places=2)

    def test_total_direct_costs_stored(self):
        """total_direct_costs on estimate = sum of all line item totals"""
        est_id = _create_estimate(
            job_id=self.job_id,
            line_items=[
                {'category': 'Labor',     'labor_hours': 20, 'labor_rate': 50},   # 1000
                {'category': 'Materials', 'materials': 500},                        # 500
            ]
        )
        conn = get_connection()
        est = conn.execute("SELECT total_direct_costs FROM job_estimates WHERE id=?", [est_id]).fetchone()
        conn.close()
        self.assertAlmostEqual(float(est['total_direct_costs']), 1500.0, places=2)

    def test_blank_category_rows_skipped(self):
        """Rows with no category should not be saved."""
        est_id = _create_estimate(
            job_id=self.job_id,
            line_items=[
                {'category': 'Labor', 'labor_hours': 5, 'labor_rate': 50},
                {'category': '',      'labor_hours': 10, 'labor_rate': 50},  # blank
            ]
        )
        conn = get_connection()
        count = conn.execute(
            "SELECT COUNT(*) FROM estimate_line_items WHERE estimate_id=? AND is_deleted=0",
            [est_id]
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)

    def test_estimate_number_sequential(self):
        """Two estimates in the same year should have sequential numbers."""
        id1 = _create_estimate()
        id2 = _create_estimate()
        conn = get_connection()
        n1 = conn.execute("SELECT estimate_number FROM job_estimates WHERE id=?", [id1]).fetchone()['estimate_number']
        n2 = conn.execute("SELECT estimate_number FROM job_estimates WHERE id=?", [id2]).fetchone()['estimate_number']
        conn.close()
        seq1 = int(n1.split('-')[-1])
        seq2 = int(n2.split('-')[-1])
        self.assertGreater(seq2, seq1)


# ════════════════════════════════════════════════════════════════
class TestEstimateTotalsComputation(unittest.TestCase):
    """Test _compute_totals math in isolation."""

    def _totals(self, items, oh=0, ins=0, ow=0, profit=0):
        from routes_phase9 import _compute_totals
        return _compute_totals(items, oh, ins, ow, profit)

    def test_direct_costs_sum(self):
        items = [
            {'labor_cost': 1000, 'materials_cost': 500, 'subcontractor_cost': 200, 'other_cost': 100},
        ]
        t = self._totals(items)
        self.assertAlmostEqual(t['total_direct'], 1800.0)

    def test_overhead_applied_to_direct(self):
        items = [{'labor_cost': 1000, 'materials_cost': 0, 'subcontractor_cost': 0, 'other_cost': 0}]
        t = self._totals(items, oh=15)
        self.assertAlmostEqual(t['overhead_amt'], 150.0)

    def test_profit_applied_to_subtotal_with_burden(self):
        """Profit applies to (direct + overhead + insurance + owner wages)."""
        items = [{'labor_cost': 1000, 'materials_cost': 0, 'subcontractor_cost': 0, 'other_cost': 0}]
        t = self._totals(items, oh=10, profit=10)  # subtotal = 1100, profit = 110
        self.assertAlmostEqual(t['profit_amt'], 110.0)
        self.assertAlmostEqual(t['total_estimate'], 1210.0)

    def test_zero_overhead(self):
        items = [{'labor_cost': 500, 'materials_cost': 0, 'subcontractor_cost': 0, 'other_cost': 0}]
        t = self._totals(items)
        self.assertAlmostEqual(t['overhead_amt'], 0.0)
        self.assertAlmostEqual(t['total_estimate'], 500.0)

    def test_multiple_line_items(self):
        items = [
            {'labor_cost': 800, 'materials_cost': 300, 'subcontractor_cost': 0, 'other_cost': 0},
            {'labor_cost': 0,   'materials_cost': 700, 'subcontractor_cost': 0, 'other_cost': 0},
        ]
        t = self._totals(items)
        self.assertAlmostEqual(t['total_labor'], 800.0)
        self.assertAlmostEqual(t['total_materials'], 1000.0)
        self.assertAlmostEqual(t['total_direct'], 1800.0)

    def test_sell_price_sum(self):
        items = [
            {'labor_cost': 0, 'materials_cost': 0, 'subcontractor_cost': 0, 'other_cost': 0, 'sell_price': 1500},
            {'labor_cost': 0, 'materials_cost': 0, 'subcontractor_cost': 0, 'other_cost': 0, 'sell_price': 800},
        ]
        t = self._totals(items)
        self.assertAlmostEqual(t['total_sell_lines'], 2300.0)

    def test_full_real_example(self):
        """Replicate real estimate math: $10k direct, 15% OH, 5% ins, 10% OW, 10% profit"""
        items = [{'labor_cost': 6000, 'materials_cost': 4000, 'subcontractor_cost': 0, 'other_cost': 0}]
        t = self._totals(items, oh=15, ins=5, ow=10, profit=10)
        self.assertAlmostEqual(t['total_direct'],      10000.0)
        self.assertAlmostEqual(t['overhead_amt'],       1500.0)
        self.assertAlmostEqual(t['insurance_amt'],       500.0)
        self.assertAlmostEqual(t['owner_wages_amt'],    1000.0)
        self.assertAlmostEqual(t['subtotal_w_burden'], 13000.0)
        self.assertAlmostEqual(t['profit_amt'],         1300.0)
        self.assertAlmostEqual(t['total_estimate'],    14300.0)


# ════════════════════════════════════════════════════════════════
class TestPreviewTotalsAPI(unittest.TestCase):

    def test_preview_empty(self):
        r = _post_json('/api/estimates/preview-totals', {
            'overhead_pct': 15, 'insurance_pct': 5,
            'owner_wages_pct': 10, 'profit_pct': 10,
            'line_items': []
        })
        d = json.loads(r.data)
        self.assertEqual(d['total_estimate'], 0.0)

    def test_preview_with_items(self):
        r = _post_json('/api/estimates/preview-totals', {
            'overhead_pct': 0, 'insurance_pct': 0,
            'owner_wages_pct': 0, 'profit_pct': 0,
            'line_items': [
                {'labor_cost': 1000, 'materials_cost': 500,
                 'subcontractor_cost': 0, 'other_cost': 0}
            ]
        })
        d = json.loads(r.data)
        self.assertAlmostEqual(d['total_direct'], 1500.0)
        self.assertAlmostEqual(d['total_estimate'], 1500.0)

    def test_preview_with_overhead(self):
        r = _post_json('/api/estimates/preview-totals', {
            'overhead_pct': 20, 'insurance_pct': 0,
            'owner_wages_pct': 0, 'profit_pct': 0,
            'line_items': [
                {'labor_cost': 1000, 'materials_cost': 0,
                 'subcontractor_cost': 0, 'other_cost': 0}
            ]
        })
        d = json.loads(r.data)
        self.assertAlmostEqual(d['overhead_amt'], 200.0)
        self.assertAlmostEqual(d['total_estimate'], 1200.0)

    def test_preview_returns_all_fields(self):
        r = _post_json('/api/estimates/preview-totals', {
            'overhead_pct': 10, 'insurance_pct': 5,
            'owner_wages_pct': 8, 'profit_pct': 12,
            'line_items': []
        })
        d = json.loads(r.data)
        for key in ['total_labor','total_materials','total_subs','total_other',
                    'total_direct','overhead_amt','insurance_amt','owner_wages_amt',
                    'subtotal_w_burden','profit_amt','total_estimate','total_sell_lines']:
            self.assertIn(key, d)


# ════════════════════════════════════════════════════════════════
class TestEstimateDetail(unittest.TestCase):

    def setUp(self):
        self.client_id = _seed_client('Detail Client')
        self.job_id, self.job_code = _seed_job('DETAIL-001', self.client_id)
        self.est_id = _create_estimate(
            job_id=self.job_id, client_id=self.client_id,
            line_items=[
                {'category': 'Framing',  'labor_hours': 40, 'labor_rate': 55, 'materials': 1200},
                {'category': 'Drywall',  'labor_hours': 20, 'labor_rate': 45, 'materials': 400},
            ]
        )

    def test_detail_loads(self):
        r = CLIENT.get(f'/estimates/{self.est_id}')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Framing', r.data)

    def test_detail_shows_totals(self):
        r = CLIENT.get(f'/estimates/{self.est_id}')
        self.assertIn(b'Direct Costs', r.data)

    def test_detail_missing_redirects(self):
        r = CLIENT.get('/estimates/99999', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_pdf_loads(self):
        r = CLIENT.get(f'/estimates/{self.est_id}/pdf')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'ESTIMATE', r.data)

    def test_actuals_loads(self):
        r = CLIENT.get(f'/estimates/{self.est_id}/actuals')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Actuals', r.data)

    def test_edit_form_loads(self):
        r = CLIENT.get(f'/estimates/{self.est_id}/edit')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Framing', r.data)


# ════════════════════════════════════════════════════════════════
class TestEstimateEdit(unittest.TestCase):

    def setUp(self):
        self.est_id = _create_estimate(
            line_items=[{'category': 'Excavation', 'labor_hours': 8, 'labor_rate': 80}]
        )

    def test_edit_updates_notes(self):
        CLIENT.post(f'/estimates/{self.est_id}/edit', data={
            'estimate_date':    TODAY,
            'status':           'Draft',
            'overhead_pct':     '15',
            'insurance_pct':    '5',
            'owner_wages_pct':  '10',
            'profit_pct':       '10',
            'notes':            'Updated scope notes',
            'li_category[]':    ['Excavation'],
            'li_labor_hours[]': ['8'],
            'li_labor_rate[]':  ['80'],
            'li_materials[]':   ['0'],
            'li_subs[]':        ['0'],
            'li_other[]':       ['0'],
            'li_markup[]':      ['0'],
            'li_sell_price[]':  ['640'],
            'li_notes[]':       [''],
        }, follow_redirects=True)
        conn = get_connection()
        est = conn.execute("SELECT notes FROM job_estimates WHERE id=?", [self.est_id]).fetchone()
        conn.close()
        self.assertEqual(est['notes'], 'Updated scope notes')

    def test_edit_recalculates_totals(self):
        CLIENT.post(f'/estimates/{self.est_id}/edit', data={
            'estimate_date':    TODAY,
            'status':           'Draft',
            'overhead_pct':     '0',
            'insurance_pct':    '0',
            'owner_wages_pct':  '0',
            'profit_pct':       '0',
            'li_category[]':    ['Labor'],
            'li_labor_hours[]': ['100'],
            'li_labor_rate[]':  ['50'],
            'li_materials[]':   ['0'],
            'li_subs[]':        ['0'],
            'li_other[]':       ['0'],
            'li_markup[]':      ['0'],
            'li_sell_price[]':  ['5000'],
            'li_notes[]':       [''],
        }, follow_redirects=True)
        conn = get_connection()
        est = conn.execute("SELECT total_direct_costs, total_estimate FROM job_estimates WHERE id=?", [self.est_id]).fetchone()
        conn.close()
        self.assertAlmostEqual(float(est['total_direct_costs']), 5000.0, places=2)

    def test_edit_replaces_line_items(self):
        """Editing should replace old line items with new ones."""
        CLIENT.post(f'/estimates/{self.est_id}/edit', data={
            'estimate_date':    TODAY,
            'status':           'Draft',
            'overhead_pct':     '0',
            'insurance_pct':    '0',
            'owner_wages_pct':  '0',
            'profit_pct':       '0',
            'li_category[]':    ['Roofing', 'Gutters'],
            'li_labor_hours[]': ['20', '5'],
            'li_labor_rate[]':  ['60', '50'],
            'li_materials[]':   ['800', '200'],
            'li_subs[]':        ['0', '0'],
            'li_other[]':       ['0', '0'],
            'li_markup[]':      ['0', '0'],
            'li_sell_price[]':  ['0', '0'],
            'li_notes[]':       ['', ''],
        }, follow_redirects=True)
        conn = get_connection()
        items = conn.execute(
            "SELECT category FROM estimate_line_items WHERE estimate_id=? AND is_deleted=0 ORDER BY id",
            [self.est_id]
        ).fetchall()
        conn.close()
        cats = [i['category'] for i in items]
        self.assertIn('Roofing', cats)
        self.assertIn('Gutters', cats)
        self.assertNotIn('Excavation', cats)


# ════════════════════════════════════════════════════════════════
class TestStatusWorkflow(unittest.TestCase):

    def setUp(self):
        self.est_id = _create_estimate()

    def test_mark_sent(self):
        r = _post_json(f'/api/estimates/{self.est_id}/status', {'status': 'Sent'})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        conn = get_connection()
        est = conn.execute("SELECT status FROM job_estimates WHERE id=?", [self.est_id]).fetchone()
        conn.close()
        self.assertEqual(est['status'], 'Sent')

    def test_mark_accepted(self):
        _post_json(f'/api/estimates/{self.est_id}/status', {'status': 'Sent'})
        r = _post_json(f'/api/estimates/{self.est_id}/status', {'status': 'Accepted'})
        d = json.loads(r.data)
        self.assertTrue(d['success'])

    def test_mark_rejected(self):
        r = _post_json(f'/api/estimates/{self.est_id}/status', {'status': 'Rejected'})
        d = json.loads(r.data)
        self.assertTrue(d['success'])

    def test_invalid_status_rejected(self):
        r = _post_json(f'/api/estimates/{self.est_id}/status', {'status': 'Bogus'})
        self.assertEqual(r.status_code, 400)

    def test_status_not_found(self):
        r = _post_json('/api/estimates/99999/status', {'status': 'Sent'})
        self.assertEqual(r.status_code, 404)

    def test_all_valid_statuses(self):
        for s in ['Draft', 'Sent', 'Accepted', 'Rejected', 'Revised']:
            r = _post_json(f'/api/estimates/{self.est_id}/status', {'status': s})
            d = json.loads(r.data)
            self.assertTrue(d['success'], f"Failed for status: {s}")


# ════════════════════════════════════════════════════════════════
class TestRevise(unittest.TestCase):

    def setUp(self):
        self.job_id, _ = _seed_job('REVISE-001')
        self.est_id = _create_estimate(
            job_id=self.job_id,
            line_items=[{'category': 'Plumbing', 'labor_hours': 10, 'labor_rate': 90, 'materials': 500}]
        )

    def test_revise_creates_new_estimate(self):
        r = _post_json(f'/api/estimates/{self.est_id}/revise', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertIn('new_estimate_id', d)
        self.assertNotEqual(d['new_estimate_id'], self.est_id)

    def test_revise_increments_version(self):
        r = _post_json(f'/api/estimates/{self.est_id}/revise', {})
        d = json.loads(r.data)
        conn = get_connection()
        new_est = conn.execute("SELECT version_number FROM job_estimates WHERE id=?", [d['new_estimate_id']]).fetchone()
        conn.close()
        self.assertEqual(new_est['version_number'], 2)

    def test_revise_marks_original_as_revised(self):
        _post_json(f'/api/estimates/{self.est_id}/revise', {})
        conn = get_connection()
        orig = conn.execute("SELECT status FROM job_estimates WHERE id=?", [self.est_id]).fetchone()
        conn.close()
        self.assertEqual(orig['status'], 'Revised')

    def test_revise_clones_line_items(self):
        r = _post_json(f'/api/estimates/{self.est_id}/revise', {})
        d = json.loads(r.data)
        conn = get_connection()
        items = conn.execute(
            "SELECT category FROM estimate_line_items WHERE estimate_id=? AND is_deleted=0",
            [d['new_estimate_id']]
        ).fetchall()
        conn.close()
        cats = [i['category'] for i in items]
        self.assertIn('Plumbing', cats)

    def test_revise_not_found(self):
        r = _post_json('/api/estimates/99999/revise', {})
        self.assertEqual(r.status_code, 404)


# ════════════════════════════════════════════════════════════════
class TestConvertToInvoice(unittest.TestCase):

    def setUp(self):
        self.client_id = _seed_client('Invoice Client')
        self.job_id, self.job_code = _seed_job('INVOICE-001', self.client_id)
        self.est_id = _create_estimate(
            job_id=self.job_id, client_id=self.client_id,
            line_items=[{'category': 'HVAC', 'labor_hours': 16, 'labor_rate': 85, 'materials': 2000}]
        )
        _post_json(f'/api/estimates/{self.est_id}/status', {'status': 'Accepted'})

    def test_convert_creates_invoice(self):
        r = _post_json(f'/api/estimates/{self.est_id}/convert-to-invoice', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertIn('invoice_id', d)
        self.assertIn('invoice_number', d)

    def test_converted_invoice_in_db(self):
        r = _post_json(f'/api/estimates/{self.est_id}/convert-to-invoice', {})
        d = json.loads(r.data)
        conn = get_connection()
        inv = conn.execute("SELECT * FROM invoices WHERE id=?", [d['invoice_id']]).fetchone()
        conn.close()
        self.assertIsNotNone(inv)
        self.assertEqual(inv['status'], 'Pending')

    def test_converted_invoice_amount_matches_estimate(self):
        conn = get_connection()
        est = conn.execute("SELECT total_estimate FROM job_estimates WHERE id=?", [self.est_id]).fetchone()
        conn.close()
        r = _post_json(f'/api/estimates/{self.est_id}/convert-to-invoice', {})
        d = json.loads(r.data)
        conn = get_connection()
        inv = conn.execute("SELECT amount FROM invoices WHERE id=?", [d['invoice_id']]).fetchone()
        conn.close()
        self.assertAlmostEqual(float(inv['amount']), float(est['total_estimate']), places=2)

    def test_convert_not_found(self):
        r = _post_json('/api/estimates/99999/convert-to-invoice', {})
        self.assertEqual(r.status_code, 404)


# ════════════════════════════════════════════════════════════════
class TestDelete(unittest.TestCase):

    def test_delete_is_soft(self):
        est_id = _create_estimate()
        r = _post_json(f'/api/estimates/{est_id}/delete', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        conn = get_connection()
        est = conn.execute("SELECT is_deleted FROM job_estimates WHERE id=?", [est_id]).fetchone()
        conn.close()
        self.assertEqual(est['is_deleted'], 1)

    def test_deleted_not_in_list(self):
        est_id = _create_estimate()
        _post_json(f'/api/estimates/{est_id}/delete', {})
        conn = get_connection()
        est = conn.execute("SELECT * FROM job_estimates WHERE id=? AND is_deleted=0", [est_id]).fetchone()
        conn.close()
        self.assertIsNone(est)

    def test_delete_also_removes_line_items(self):
        est_id = _create_estimate(
            line_items=[{'category': 'Electrical', 'materials': 800}]
        )
        _post_json(f'/api/estimates/{est_id}/delete', {})
        conn = get_connection()
        items = conn.execute(
            "SELECT * FROM estimate_line_items WHERE estimate_id=? AND is_deleted=0", [est_id]
        ).fetchall()
        conn.close()
        self.assertEqual(len(items), 0)


# ════════════════════════════════════════════════════════════════
class TestActualsReport(unittest.TestCase):

    def setUp(self):
        self.client_id = _seed_client('Actuals Client')
        self.job_id, self.job_code = _seed_job('ACTUAL-001', self.client_id)
        self.est_id = _create_estimate(
            job_id=self.job_id, client_id=self.client_id,
            line_items=[
                {'category': 'Labor',     'labor_hours': 50, 'labor_rate': 60},
                {'category': 'Materials', 'materials': 3000},
            ]
        )

    def test_actuals_page_loads(self):
        r = CLIENT.get(f'/estimates/{self.est_id}/actuals')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Actuals vs Estimate', r.data)

    def test_actuals_shows_categories(self):
        r = CLIENT.get(f'/estimates/{self.est_id}/actuals')
        self.assertIn(b'Labor', r.data)
        self.assertIn(b'Materials', r.data)

    def test_actuals_missing_redirects(self):
        r = CLIENT.get('/estimates/99999/actuals', follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_actuals_with_timesheet_data(self):
        """Seed timesheet entries and verify they appear in actuals."""
        with _db() as conn:
            conn.execute("""
                INSERT INTO timesheet (entry_date, job_code, hours, cost_amount)
                VALUES (?,?,?,?)
            """, [TODAY, self.job_code, 10, 600.0])
        r = CLIENT.get(f'/estimates/{self.est_id}/actuals')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'600', r.data)


# ════════════════════════════════════════════════════════════════
class TestExport(unittest.TestCase):

    def setUp(self):
        _create_estimate()

    def test_export_csv(self):
        r = CLIENT.get('/estimates/export')
        self.assertEqual(r.status_code, 200)
        self.assertIn('text/csv', r.content_type)
        self.assertIn(b'estimate_number', r.data)

    def test_export_filtered_by_status(self):
        r = CLIENT.get('/estimates/export?status=Draft')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'estimate_number', r.data)

    def test_export_contains_data(self):
        r = CLIENT.get('/estimates/export')
        lines = r.data.decode().split('\n')
        self.assertGreater(len(lines), 1)  # header + at least one row


# ════════════════════════════════════════════════════════════════
class TestHelpers(unittest.TestCase):

    def test_f_helper(self):
        from routes_phase9 import _f
        lst = ['10.5', '20.0', '']
        self.assertAlmostEqual(_f(lst, 0), 10.5)
        self.assertAlmostEqual(_f(lst, 2), 0.0)
        self.assertAlmostEqual(_f(lst, 5), 0.0)  # out of bounds

    def test_s_helper(self):
        from routes_phase9 import _s
        lst = ['hello', '  world  ', '']
        self.assertEqual(_s(lst, 0), 'hello')
        self.assertEqual(_s(lst, 1), 'world')
        self.assertEqual(_s(lst, 5), '')  # out of bounds

    def test_next_estimate_number(self):
        from routes_phase9 import _next_estimate_number
        conn = get_connection()
        num = _next_estimate_number(conn, 'EST')
        conn.close()
        self.assertIn('EST-', num)
        parts = num.split('-')
        self.assertEqual(len(parts), 3)
        self.assertEqual(int(parts[1]), date.today().year)
        self.assertGreater(int(parts[2]), 0)


# ════════════════════════════════════════════════════════════════
class TestRegressionPhase9(unittest.TestCase):

    def test_reconciliation_loads(self):
        r = CLIENT.get('/reconciliation')
        self.assertEqual(r.status_code, 200)

    def test_tax_loads(self):
        r = CLIENT.get('/tax')
        self.assertEqual(r.status_code, 200)

    def test_payroll_loads(self):
        r = CLIENT.get('/payroll')
        self.assertEqual(r.status_code, 200)

    def test_invoices_loads(self):
        r = CLIENT.get('/invoices')
        self.assertEqual(r.status_code, 200)

    def test_dashboard_loads(self):
        r = CLIENT.get('/')
        self.assertEqual(r.status_code, 200)

    def test_ledger_loads(self):
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
        print(f"\n✅ All {total} Phase 9 tests passed!")
    else:
        fails = len(result.failures)
        errs  = len(result.errors)
        print(f"\n❌ {fails} failure(s), {errs} error(s) out of {total}")
    sys.exit(0 if result.wasSuccessful() else 1)
