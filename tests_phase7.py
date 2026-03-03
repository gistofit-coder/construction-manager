"""
Phase 7 Tests — Tax Reminders & Deadlines
Run: python3 tests_phase7.py
"""
import os, sys, json, unittest
from datetime import date, timedelta

TEST_DB = '/tmp/test_p7.db'
os.environ['CONSTRUCTION_DB'] = TEST_DB
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

from database import init_db, get_connection
init_db(TEST_DB)

from app import app as flask_app
flask_app.config['TESTING'] = True
CLIENT = flask_app.test_client()

TODAY = date.today().strftime('%Y-%m-%d')
TOMORROW = (date.today() + timedelta(days=1)).strftime('%Y-%m-%d')
NEXT_WEEK = (date.today() + timedelta(days=7)).strftime('%Y-%m-%d')
NEXT_MONTH = (date.today() + timedelta(days=35)).strftime('%Y-%m-%d')
YESTERDAY = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
LAST_WEEK = (date.today() - timedelta(days=8)).strftime('%Y-%m-%d')


def _post_json(url, data):
    return CLIENT.post(url, data=json.dumps(data),
                       content_type='application/json')

def _get_json(url):
    return json.loads(CLIENT.get(url).data)

def _create_reminder(due_date=None, desc='Test Deadline', recurring=False,
                     freq='', url='', amount=0, notes=''):
    """POST to /tax/new and return the created reminder's id."""
    due_date = due_date or NEXT_MONTH
    r = CLIENT.post('/tax/new', data={
        'due_date': due_date,
        'task_description': desc,
        'is_recurring': '1' if recurring else '',
        'frequency': freq,
        'url': url,
        'amount': str(amount),
        'notes': notes,
    }, follow_redirects=True)
    assert r.status_code == 200, f"Create failed: {r.status_code}"
    conn = get_connection()
    row = conn.execute(
        "SELECT id FROM reminders WHERE task_description=? AND is_deleted=0 ORDER BY id DESC LIMIT 1",
        [desc]
    ).fetchone()
    conn.close()
    return row['id'] if row else None


# ════════════════════════════════════════════════════════════════
class TestTaxList(unittest.TestCase):

    def test_list_loads(self):
        r = CLIENT.get('/tax')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Tax Reminder', r.data)

    def test_list_shows_seeded_deadlines(self):
        """The DB is seeded with 2025 deadlines from schema."""
        r = CLIENT.get('/tax?year=2025')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'941', r.data)

    def test_list_year_filter(self):
        r = CLIENT.get('/tax?year=2025')
        self.assertEqual(r.status_code, 200)

    def test_list_status_filter_pending(self):
        r = CLIENT.get('/tax?status=Pending')
        self.assertEqual(r.status_code, 200)

    def test_list_status_filter_done(self):
        r = CLIENT.get('/tax?status=Done')
        self.assertEqual(r.status_code, 200)

    def test_calendar_view(self):
        r = CLIENT.get('/tax?view=calendar')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Mon', r.data)

    def test_calendar_month_navigation(self):
        r = CLIENT.get('/tax?view=calendar&month=3&cal_year=2025')
        self.assertEqual(r.status_code, 200)

    def test_new_form_loads(self):
        r = CLIENT.get('/tax/new')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'due_date', r.data)

    def test_new_form_prefill_params(self):
        r = CLIENT.get('/tax/new?desc=Form+941&date=2025-04-30')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Form 941', r.data)


# ════════════════════════════════════════════════════════════════
class TestReminderCreate(unittest.TestCase):

    def test_create_basic(self):
        r = CLIENT.post('/tax/new', data={
            'due_date': NEXT_MONTH,
            'task_description': 'Quarterly Test Reminder',
        }, follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM reminders WHERE task_description='Quarterly Test Reminder' AND is_deleted=0"
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row['status'], 'Pending')

    def test_create_recurring(self):
        r = CLIENT.post('/tax/new', data={
            'due_date': NEXT_MONTH,
            'task_description': 'Recurring Quarterly',
            'is_recurring': '1',
            'frequency': 'Quarterly',
        }, follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM reminders WHERE task_description='Recurring Quarterly' AND is_deleted=0"
        ).fetchone()
        conn.close()
        self.assertEqual(row['is_recurring'], 1)
        self.assertEqual(row['frequency'], 'Quarterly')

    def test_create_with_amount_and_url(self):
        _create_reminder(desc='With Amount', amount=1500.00,
                         url='https://www.irs.gov', recurring=True, freq='Quarterly')
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM reminders WHERE task_description='With Amount' AND is_deleted=0"
        ).fetchone()
        conn.close()
        self.assertAlmostEqual(float(row['amount']), 1500.0)
        self.assertEqual(row['url'], 'https://www.irs.gov')

    def test_create_missing_due_date_rejected(self):
        r = CLIENT.post('/tax/new', data={
            'task_description': 'No date reminder',
        }, follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM reminders WHERE task_description='No date reminder' AND is_deleted=0"
        ).fetchone()
        conn.close()
        self.assertIsNone(row)

    def test_create_missing_description_rejected(self):
        r = CLIENT.post('/tax/new', data={
            'due_date': NEXT_MONTH,
        }, follow_redirects=True)
        self.assertEqual(r.status_code, 200)

    def test_new_reminder_status_is_pending(self):
        rem_id = _create_reminder(desc='Status Check Reminder')
        conn = get_connection()
        row = conn.execute("SELECT status FROM reminders WHERE id=?", [rem_id]).fetchone()
        conn.close()
        self.assertEqual(row['status'], 'Pending')


# ════════════════════════════════════════════════════════════════
class TestReminderEdit(unittest.TestCase):

    def setUp(self):
        self.rem_id = _create_reminder(
            desc='Edit Target', due_date=NEXT_MONTH, recurring=True, freq='Yearly'
        )

    def test_edit_form_loads(self):
        r = CLIENT.get(f'/tax/{self.rem_id}/edit')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Edit Target', r.data)

    def test_edit_updates_description(self):
        CLIENT.post(f'/tax/{self.rem_id}/edit', data={
            'due_date': NEXT_MONTH,
            'task_description': 'Updated Description',
            'is_recurring': '1',
            'frequency': 'Yearly',
        }, follow_redirects=True)
        conn = get_connection()
        row = conn.execute("SELECT task_description FROM reminders WHERE id=?", [self.rem_id]).fetchone()
        conn.close()
        self.assertEqual(row['task_description'], 'Updated Description')

    def test_edit_updates_due_date(self):
        new_date = (date.today() + timedelta(days=60)).strftime('%Y-%m-%d')
        CLIENT.post(f'/tax/{self.rem_id}/edit', data={
            'due_date': new_date,
            'task_description': 'Edit Target',
        }, follow_redirects=True)
        conn = get_connection()
        row = conn.execute("SELECT due_date FROM reminders WHERE id=?", [self.rem_id]).fetchone()
        conn.close()
        self.assertEqual(row['due_date'], new_date)

    def test_edit_missing_redirects(self):
        r = CLIENT.get('/tax/99999/edit', follow_redirects=True)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Tax Reminder', r.data)


# ════════════════════════════════════════════════════════════════
class TestReminderDone(unittest.TestCase):

    def setUp(self):
        self.rem_id = _create_reminder(
            desc=f'Done Test {date.today().isoformat()}', due_date=TOMORROW
        )

    def test_mark_done(self):
        r = _post_json(f'/api/reminders/{self.rem_id}/done', {
            'payment_date': TODAY,
            'amount': 500.00,
        })
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertEqual(d['status'], 'Done')

    def test_mark_done_stored_in_db(self):
        _post_json(f'/api/reminders/{self.rem_id}/done', {
            'payment_date': TODAY,
            'amount': 1200.00,
        })
        conn = get_connection()
        row = conn.execute("SELECT * FROM reminders WHERE id=?", [self.rem_id]).fetchone()
        conn.close()
        self.assertEqual(row['status'], 'Done')
        self.assertEqual(row['payment_date'], TODAY)
        self.assertAlmostEqual(float(row['amount']), 1200.0)

    def test_mark_done_no_amount(self):
        r = _post_json(f'/api/reminders/{self.rem_id}/done', {
            'payment_date': TODAY,
        })
        d = json.loads(r.data)
        self.assertTrue(d['success'])

    def test_mark_done_missing_id_404(self):
        r = _post_json('/api/reminders/99999/done', {'payment_date': TODAY})
        self.assertEqual(r.status_code, 404)


# ════════════════════════════════════════════════════════════════
class TestReminderSnooze(unittest.TestCase):

    def setUp(self):
        self.rem_id = _create_reminder(
            desc=f'Snooze Test {date.today().isoformat()}', due_date=TOMORROW
        )

    def test_snooze_7_days(self):
        r = _post_json(f'/api/reminders/{self.rem_id}/snooze', {'days': 7})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        expected = (date.fromisoformat(TOMORROW) + timedelta(days=7)).strftime('%Y-%m-%d')
        self.assertEqual(d['new_due_date'], expected)

    def test_snooze_sets_remind_later(self):
        _post_json(f'/api/reminders/{self.rem_id}/snooze', {'days': 3})
        conn = get_connection()
        row = conn.execute("SELECT status FROM reminders WHERE id=?", [self.rem_id]).fetchone()
        conn.close()
        self.assertEqual(row['status'], 'RemindLater')

    def test_snooze_updates_due_date(self):
        _post_json(f'/api/reminders/{self.rem_id}/snooze', {'days': 14})
        conn = get_connection()
        row = conn.execute("SELECT due_date FROM reminders WHERE id=?", [self.rem_id]).fetchone()
        conn.close()
        expected = (date.fromisoformat(TOMORROW) + timedelta(days=14)).strftime('%Y-%m-%d')
        self.assertEqual(row['due_date'], expected)

    def test_snooze_default_7_days(self):
        r = _post_json(f'/api/reminders/{self.rem_id}/snooze', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        expected = (date.fromisoformat(TOMORROW) + timedelta(days=7)).strftime('%Y-%m-%d')
        self.assertEqual(d['new_due_date'], expected)

    def test_snooze_clamped_max(self):
        """Snooze days clamped to 365 max."""
        r = _post_json(f'/api/reminders/{self.rem_id}/snooze', {'days': 9999})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        expected = (date.fromisoformat(TOMORROW) + timedelta(days=365)).strftime('%Y-%m-%d')
        self.assertEqual(d['new_due_date'], expected)


# ════════════════════════════════════════════════════════════════
class TestReminderReopen(unittest.TestCase):

    def setUp(self):
        self.rem_id = _create_reminder(desc=f'Reopen Test {date.today().isoformat()}')
        _post_json(f'/api/reminders/{self.rem_id}/done', {'payment_date': TODAY})

    def test_reopen_sets_pending(self):
        r = _post_json(f'/api/reminders/{self.rem_id}/reopen', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        conn = get_connection()
        row = conn.execute("SELECT status FROM reminders WHERE id=?", [self.rem_id]).fetchone()
        conn.close()
        self.assertEqual(row['status'], 'Pending')

    def test_reopen_after_snooze(self):
        snooze_id = _create_reminder(desc=f'Snooze Reopen {date.today().isoformat()}')
        _post_json(f'/api/reminders/{snooze_id}/snooze', {'days': 7})
        _post_json(f'/api/reminders/{snooze_id}/reopen', {})
        conn = get_connection()
        row = conn.execute("SELECT status FROM reminders WHERE id=?", [snooze_id]).fetchone()
        conn.close()
        self.assertEqual(row['status'], 'Pending')


# ════════════════════════════════════════════════════════════════
class TestReminderDelete(unittest.TestCase):

    def test_delete_is_soft(self):
        rem_id = _create_reminder(desc=f'Delete Test {date.today().isoformat()}')
        r = _post_json(f'/api/reminders/{rem_id}/delete', {})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        conn = get_connection()
        row = conn.execute("SELECT is_deleted FROM reminders WHERE id=?", [rem_id]).fetchone()
        conn.close()
        self.assertEqual(row['is_deleted'], 1)

    def test_deleted_not_in_list(self):
        rem_id = _create_reminder(desc=f'Hidden After Delete {date.today().isoformat()}')
        _post_json(f'/api/reminders/{rem_id}/delete', {})
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM reminders WHERE id=? AND is_deleted=0", [rem_id]
        ).fetchone()
        conn.close()
        self.assertIsNone(row)


# ════════════════════════════════════════════════════════════════
class TestUrgencyLogic(unittest.TestCase):
    """Test that urgency classification is correct."""

    def _get_urgency(self, rem_id):
        conn = get_connection()
        row = conn.execute("SELECT due_date, status FROM reminders WHERE id=?", [rem_id]).fetchone()
        conn.close()
        from routes_phase7 import _urgency, _days_until
        days = _days_until(row['due_date'], TODAY)
        return _urgency(days, row['status'])

    def test_overdue_urgency(self):
        rem_id = _create_reminder(desc=f'Overdue {date.today().isoformat()}', due_date=LAST_WEEK)
        self.assertEqual(self._get_urgency(rem_id), 'overdue')

    def test_urgent_tomorrow(self):
        rem_id = _create_reminder(desc=f'Urgent {date.today().isoformat()}', due_date=TOMORROW)
        self.assertEqual(self._get_urgency(rem_id), 'urgent')

    def test_urgent_today(self):
        rem_id = _create_reminder(desc=f'Today {date.today().isoformat()}', due_date=TODAY)
        self.assertEqual(self._get_urgency(rem_id), 'urgent')

    def test_soon_30_days(self):
        soon_date = (date.today() + timedelta(days=25)).strftime('%Y-%m-%d')
        rem_id = _create_reminder(desc=f'Soon {date.today().isoformat()}', due_date=soon_date)
        self.assertEqual(self._get_urgency(rem_id), 'soon')

    def test_upcoming_far_future(self):
        far = (date.today() + timedelta(days=60)).strftime('%Y-%m-%d')
        rem_id = _create_reminder(desc=f'Far Future {date.today().isoformat()}', due_date=far)
        self.assertEqual(self._get_urgency(rem_id), 'upcoming')

    def test_done_urgency(self):
        rem_id = _create_reminder(desc=f'Done Urgency {date.today().isoformat()}', due_date=LAST_WEEK)
        _post_json(f'/api/reminders/{rem_id}/done', {'payment_date': TODAY})
        self.assertEqual(self._get_urgency(rem_id), 'done')

    def test_snoozed_urgency(self):
        rem_id = _create_reminder(desc=f'Snoozed Urgency {date.today().isoformat()}', due_date=TOMORROW)
        _post_json(f'/api/reminders/{rem_id}/snooze', {'days': 7})
        self.assertEqual(self._get_urgency(rem_id), 'snoozed')


# ════════════════════════════════════════════════════════════════
class TestAdvanceYear(unittest.TestCase):

    def setUp(self):
        """Create a set of recurring 2025 reminders to advance."""
        from database import db as _db
        with _db() as conn:
            conn.execute("""DELETE FROM reminders WHERE substr(due_date,1,4) IN ('2030','2031')
                            AND task_description LIKE 'Advance Test%' AND is_deleted=0""")
        for i in range(3):
            CLIENT.post('/tax/new', data={
                'due_date': f'2030-0{i+1}-15',
                'task_description': f'Advance Test {i+1}',
                'is_recurring': '1',
                'frequency': 'Quarterly',
            }, follow_redirects=True)

    def test_advance_year_creates_reminders(self):
        r = _post_json('/api/reminders/advance-year', {'from_year': 2030, 'to_year': 2031})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertGreater(d['created'], 0)
        self.assertEqual(d['to_year'], 2031)

    def test_advance_year_dates_incremented(self):
        _post_json('/api/reminders/advance-year', {'from_year': 2030, 'to_year': 2031})
        conn = get_connection()
        rows = conn.execute(
            "SELECT due_date FROM reminders WHERE substr(due_date,1,4)='2031' AND is_deleted=0"
        ).fetchall()
        conn.close()
        self.assertGreater(len(rows), 0)
        for row in rows:
            self.assertTrue(row['due_date'].startswith('2031'))

    def test_advance_year_skips_duplicates(self):
        """Running advance-year twice should not double-create."""
        _post_json('/api/reminders/advance-year', {'from_year': 2030, 'to_year': 2031})
        r2 = _post_json('/api/reminders/advance-year', {'from_year': 2030, 'to_year': 2031})
        d = json.loads(r2.data)
        self.assertEqual(d['created'], 0)

    def test_advance_year_only_recurring(self):
        """Non-recurring reminders should not be cloned."""
        CLIENT.post('/tax/new', data={
            'due_date': '2030-06-15',
            'task_description': 'One-time 2030 reminder',
        }, follow_redirects=True)
        conn_before = get_connection()
        count_before = conn_before.execute(
            "SELECT COUNT(*) FROM reminders WHERE substr(due_date,1,4)='2031' "
            "AND task_description='One-time 2030 reminder' AND is_deleted=0"
        ).fetchone()[0]
        conn_before.close()

        _post_json('/api/reminders/advance-year', {'from_year': 2030, 'to_year': 2031})

        conn_after = get_connection()
        count_after = conn_after.execute(
            "SELECT COUNT(*) FROM reminders WHERE substr(due_date,1,4)='2031' "
            "AND task_description='One-time 2030 reminder' AND is_deleted=0"
        ).fetchone()[0]
        conn_after.close()
        self.assertEqual(count_after, count_before)

    def test_advance_year_invalid_to_year(self):
        r = _post_json('/api/reminders/advance-year', {'from_year': 2030, 'to_year': 2029})
        self.assertEqual(r.status_code, 400)


# ════════════════════════════════════════════════════════════════
class TestSeedYear(unittest.TestCase):

    def test_seed_creates_deadlines(self):
        r = _post_json('/api/reminders/seed-year', {'year': 2040})
        d = json.loads(r.data)
        self.assertTrue(d['success'])
        self.assertGreater(d['created'], 0)
        self.assertEqual(d['year'], 2040)

    def test_seed_creates_941_forms(self):
        _post_json('/api/reminders/seed-year', {'year': 2041})
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM reminders WHERE substr(due_date,1,4)='2041' AND is_deleted=0"
        ).fetchall()
        conn.close()
        descs = [r['task_description'] for r in rows]
        self.assertTrue(any('941' in d for d in descs), '941 deadlines not seeded')

    def test_seed_creates_w2_deadline(self):
        _post_json('/api/reminders/seed-year', {'year': 2042})
        conn = get_connection()
        rows = conn.execute(
            "SELECT task_description FROM reminders WHERE substr(due_date,1,4)='2042' AND is_deleted=0"
        ).fetchall()
        conn.close()
        descs = [r['task_description'] for r in rows]
        self.assertTrue(any('W-2' in d for d in descs), 'W-2 deadline not seeded')

    def test_seed_skips_duplicates(self):
        _post_json('/api/reminders/seed-year', {'year': 2043})
        r2 = _post_json('/api/reminders/seed-year', {'year': 2043})
        d = json.loads(r2.data)
        self.assertEqual(d['created'], 0)

    def test_seed_sets_recurring(self):
        _post_json('/api/reminders/seed-year', {'year': 2044})
        conn = get_connection()
        rows = conn.execute(
            "SELECT is_recurring FROM reminders WHERE substr(due_date,1,4)='2044' AND is_deleted=0"
        ).fetchall()
        conn.close()
        self.assertTrue(all(r['is_recurring'] == 1 for r in rows))


# ════════════════════════════════════════════════════════════════
class TestReminderAPIs(unittest.TestCase):

    def test_upcoming_api_returns_list(self):
        d = _get_json('/api/reminders/upcoming?days=365')
        self.assertIsInstance(d, list)

    def test_upcoming_api_only_pending(self):
        """Upcoming API should only return Pending reminders."""
        # Mark one as done
        rem_id = _create_reminder(desc=f'Done for API {date.today().isoformat()}', due_date=TOMORROW)
        _post_json(f'/api/reminders/{rem_id}/done', {'payment_date': TODAY})
        d = _get_json('/api/reminders/upcoming?days=30&limit=100')
        for item in d:
            self.assertEqual(item['status'], 'Pending')

    def test_upcoming_api_has_urgency_fields(self):
        _create_reminder(desc=f'API Fields Test {date.today().isoformat()}', due_date=NEXT_WEEK)
        d = _get_json('/api/reminders/upcoming?days=30')
        if d:
            item = d[0]
            self.assertIn('days_until', item)
            self.assertIn('urgency', item)
            self.assertIn('urgency_label', item)

    def test_upcoming_api_limit(self):
        d = _get_json('/api/reminders/upcoming?limit=3')
        self.assertLessEqual(len(d), 3)

    def test_counts_api(self):
        d = _get_json('/api/reminders/counts')
        self.assertIn('overdue', d)
        self.assertIn('due_this_week', d)
        self.assertIn('total_attention', d)

    def test_counts_api_overdue_increases(self):
        """Creating an overdue reminder increases the overdue count."""
        before = _get_json('/api/reminders/counts')['overdue']
        _create_reminder(desc=f'Count Overdue {date.today().isoformat()}', due_date=LAST_WEEK)
        after = _get_json('/api/reminders/counts')['overdue']
        self.assertGreater(after, before)

    def test_counts_total_attention(self):
        d = _get_json('/api/reminders/counts')
        self.assertEqual(d['total_attention'], d['overdue'] + d['due_this_week'])


# ════════════════════════════════════════════════════════════════
class TestExport(unittest.TestCase):

    def test_export_returns_csv(self):
        r = CLIENT.get('/tax/export?year=2025')
        self.assertEqual(r.status_code, 200)
        self.assertIn('text/csv', r.content_type)
        self.assertIn(b'due_date', r.data)
        self.assertIn(b'task_description', r.data)

    def test_export_contains_seeded_data(self):
        r = CLIENT.get('/tax/export?year=2025')
        self.assertIn(b'941', r.data)

    def test_export_different_year(self):
        _post_json('/api/reminders/seed-year', {'year': 2050})
        r = CLIENT.get('/tax/export?year=2050')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'due_date', r.data)


# ════════════════════════════════════════════════════════════════
class TestHelperFunctions(unittest.TestCase):

    def test_days_until_future(self):
        from routes_phase7 import _days_until
        future = (date.today() + timedelta(days=10)).strftime('%Y-%m-%d')
        self.assertEqual(_days_until(future, TODAY), 10)

    def test_days_until_past(self):
        from routes_phase7 import _days_until
        past = (date.today() - timedelta(days=5)).strftime('%Y-%m-%d')
        self.assertEqual(_days_until(past, TODAY), -5)

    def test_days_until_today(self):
        from routes_phase7 import _days_until
        self.assertEqual(_days_until(TODAY, TODAY), 0)

    def test_urgency_overdue(self):
        from routes_phase7 import _urgency
        self.assertEqual(_urgency(-1, 'Pending'), 'overdue')
        self.assertEqual(_urgency(-30, 'Pending'), 'overdue')

    def test_urgency_urgent(self):
        from routes_phase7 import _urgency
        self.assertEqual(_urgency(0, 'Pending'), 'urgent')
        self.assertEqual(_urgency(7, 'Pending'), 'urgent')

    def test_urgency_soon(self):
        from routes_phase7 import _urgency
        self.assertEqual(_urgency(8, 'Pending'), 'soon')
        self.assertEqual(_urgency(30, 'Pending'), 'soon')

    def test_urgency_upcoming(self):
        from routes_phase7 import _urgency
        self.assertEqual(_urgency(31, 'Pending'), 'upcoming')
        self.assertEqual(_urgency(365, 'Pending'), 'upcoming')

    def test_urgency_done_overrides(self):
        from routes_phase7 import _urgency
        self.assertEqual(_urgency(-10, 'Done'), 'done')

    def test_urgency_snoozed_overrides(self):
        from routes_phase7 import _urgency
        self.assertEqual(_urgency(-10, 'RemindLater'), 'snoozed')

    def test_urgency_label_overdue(self):
        from routes_phase7 import _urgency_label
        self.assertIn('overdue', _urgency_label(-5, 'Pending'))
        self.assertIn('5', _urgency_label(-5, 'Pending'))

    def test_urgency_label_today(self):
        from routes_phase7 import _urgency_label
        self.assertIn('today', _urgency_label(0, 'Pending').lower())

    def test_urgency_label_tomorrow(self):
        from routes_phase7 import _urgency_label
        self.assertIn('tomorrow', _urgency_label(1, 'Pending').lower())

    def test_urgency_label_days(self):
        from routes_phase7 import _urgency_label
        label = _urgency_label(5, 'Pending')
        self.assertIn('5', label)


# ════════════════════════════════════════════════════════════════
class TestRegressionPhase7(unittest.TestCase):

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
        print(f"\n✅ All {total} Phase 7 tests passed!")
    else:
        fails = len(result.failures)
        errs  = len(result.errors)
        print(f"\n❌ {fails} failure(s), {errs} error(s) out of {total}")
    sys.exit(0 if result.wasSuccessful() else 1)
