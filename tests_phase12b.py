"""
Phase 12B Tests — Job Estimates / Job Tracker
Tests: 28 tests covering estimates list, CRUD, status, revise, actuals, PDF, export, convert
"""
import os
import sys
import json
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('CONSTRUCTION_DB', '/tmp/test_p12b_full.db')

import app as A
from database import init_db

DB_PATH = '/tmp/test_p12b_full.db'


@pytest.fixture(scope='module')
def client():
    A.app.config['TESTING'] = True
    A.app.config['WTF_CSRF_ENABLED'] = False
    init_db(DB_PATH)
    os.environ['CONSTRUCTION_DB'] = DB_PATH
    with A.app.test_client() as c:
        yield c


def make_estimate(client, **kwargs):
    """Helper: create a draft estimate, return its id."""
    data = {
        'estimate_date': '2026-02-23', 'status': 'Draft',
        'overhead_pct': '0', 'insurance_pct': '0.9',
        'owner_wages_pct': '0', 'profit_pct': '30',
        'notes': 'test estimate',
        'li_category[]': ['Demolition', 'Plumbing'],
        'li_labor_hours[]': ['8', '4'],
        'li_labor_rate[]': ['45', '75'],
        'li_labor_cost[]': ['360', '300'],
        'li_materials[]': ['0', '475'],
        'li_subs[]': ['0', '0'],
        'li_other[]': ['0', '0'],
        'li_total[]': ['360', '775'],
        'li_markup[]': ['30', '30'],
        'li_sell_price[]': ['468', '1008'],
        'li_notes[]': ['', ''],
    }
    data.update(kwargs)
    r = client.post('/estimates/new', data=data, follow_redirects=True)
    assert r.status_code == 200
    import sqlite3, app as A2
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute('SELECT id FROM job_estimates ORDER BY id DESC LIMIT 1').fetchone()
    conn.close()
    return row['id']


class TestEstimatesList:
    def test_list_page_loads(self, client):
        r = client.get('/estimates')
        assert r.status_code == 200

    def test_list_contains_table(self, client):
        r = client.get('/estimates')
        assert b'Estimates' in r.data or b'estimate' in r.data.lower()

    def test_list_filter_by_status(self, client):
        r = client.get('/estimates?status=Draft')
        assert r.status_code == 200

    def test_list_filter_by_year(self, client):
        r = client.get('/estimates?year=2026')
        assert r.status_code == 200

    def test_list_new_button_present(self, client):
        r = client.get('/estimates')
        assert b'New Estimate' in r.data or b'/estimates/new' in r.data

    def test_estimates_in_nav(self, client):
        r = client.get('/')
        assert b'/estimates' in r.data


class TestEstimateCreate:
    def test_new_form_loads(self, client):
        r = client.get('/estimates/new')
        assert r.status_code == 200

    def test_new_form_has_category_fields(self, client):
        r = client.get('/estimates/new')
        assert b'li_category' in r.data

    def test_new_form_has_profit_field(self, client):
        r = client.get('/estimates/new')
        assert b'profit_pct' in r.data

    def test_create_estimate_redirects(self, client):
        r = client.post('/estimates/new', data={
            'estimate_date': '2026-02-23', 'status': 'Draft', 'profit_pct': '30',
            'li_category[]': 'Framing-Labor', 'li_labor_hours[]': '16',
            'li_labor_rate[]': '50', 'li_labor_cost[]': '800',
            'li_materials[]': '400', 'li_subs[]': '0', 'li_other[]': '0',
            'li_total[]': '1200', 'li_markup[]': '30', 'li_sell_price[]': '1560',
            'li_notes[]': '',
        }, follow_redirects=True)
        assert r.status_code == 200

    def test_create_stores_in_db(self, client):
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM job_estimates ORDER BY id DESC LIMIT 1").fetchone()
        conn.close()
        assert row is not None
        assert row['status'] == 'Draft'

    def test_line_items_saved(self, client):
        eid = make_estimate(client)
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        lines = conn.execute(
            "SELECT * FROM estimate_line_items WHERE estimate_id=? AND is_deleted=0", [eid]
        ).fetchall()
        conn.close()
        assert len(lines) == 2
        assert lines[0]['category'] == 'Demolition'


class TestEstimateDetail:
    def test_detail_loads(self, client):
        eid = make_estimate(client)
        r = client.get(f'/estimates/{eid}')
        assert r.status_code == 200

    def test_detail_shows_line_items(self, client):
        eid = make_estimate(client)
        r = client.get(f'/estimates/{eid}')
        assert b'Demolition' in r.data or b'Plumbing' in r.data

    def test_detail_shows_sell_price(self, client):
        eid = make_estimate(client)
        r = client.get(f'/estimates/{eid}')
        assert b'468' in r.data or b'1008' in r.data

    def test_detail_404_for_missing(self, client):
        r = client.get('/estimates/99999')
        assert r.status_code == 404

    def test_edit_form_loads(self, client):
        eid = make_estimate(client)
        r = client.get(f'/estimates/{eid}/edit')
        assert r.status_code == 200

    def test_edit_save(self, client):
        eid = make_estimate(client)
        r = client.post(f'/estimates/{eid}/edit', data={
            'estimate_date': '2026-02-24', 'status': 'Draft', 'profit_pct': '25',
            'li_category[]': 'Painting', 'li_labor_hours[]': '4',
            'li_labor_rate[]': '40', 'li_labor_cost[]': '160',
            'li_materials[]': '200', 'li_subs[]': '0', 'li_other[]': '0',
            'li_total[]': '360', 'li_markup[]': '25', 'li_sell_price[]': '450',
            'li_notes[]': '',
        }, follow_redirects=True)
        assert r.status_code == 200


class TestEstimateActions:
    def test_set_status_sent(self, client):
        eid = make_estimate(client)
        r = client.post(f'/estimates/{eid}/status', json={'status': 'Sent'})
        assert r.status_code == 200
        assert r.get_json()['success'] is True

    def test_set_invalid_status(self, client):
        eid = make_estimate(client)
        r = client.post(f'/estimates/{eid}/status', json={'status': 'Bogus'})
        assert r.status_code == 400

    def test_revise_creates_new(self, client):
        eid = make_estimate(client)
        r = client.post(f'/estimates/{eid}/revise', json={})
        assert r.status_code == 200
        data = r.get_json()
        assert data['success'] is True
        assert 'new_id' in data
        assert data['new_id'] != eid

    def test_revise_increments_version(self, client):
        eid = make_estimate(client)
        r = client.post(f'/estimates/{eid}/revise', json={})
        assert '-v2' in r.get_json().get('estimate_number', '')

    def test_convert_to_invoice(self, client):
        eid = make_estimate(client)
        r = client.post(f'/estimates/{eid}/convert-invoice', json={})
        assert r.status_code == 200
        data = r.get_json()
        assert data['success'] is True
        assert 'invoice_id' in data

    def test_delete_estimate(self, client):
        eid = make_estimate(client)
        r = client.post(f'/estimates/{eid}/delete', follow_redirects=True)
        assert r.status_code == 200


class TestEstimateActualsAndExport:
    def test_actuals_page_loads(self, client):
        eid = make_estimate(client)
        r = client.get(f'/estimates/{eid}/actuals')
        assert r.status_code == 200

    def test_actuals_has_comparison_table(self, client):
        eid = make_estimate(client)
        r = client.get(f'/estimates/{eid}/actuals')
        assert b'Category' in r.data

    def test_pdf_loads(self, client):
        eid = make_estimate(client)
        r = client.get(f'/estimates/{eid}/pdf')
        assert r.status_code == 200

    def test_export_csv(self, client):
        r = client.get('/estimates/export')
        assert r.status_code == 200
        assert b'Estimate' in r.data or r.content_type == 'text/csv'

    def test_api_summary(self, client):
        eid = make_estimate(client)
        r = client.get(f'/api/estimates/{eid}/summary')
        assert r.status_code == 200
        data = r.get_json()
        assert 'estimate' in data
        assert 'line_items' in data
        assert 'total_sell' in data
