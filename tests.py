"""
Tests for all Phase 1 automation logic.
Run: python3 tests.py
"""
import os
import sys
import unittest
import sqlite3
from datetime import datetime

# Set up test database
TEST_DB = '/tmp/test_construction.db'
os.environ['CONSTRUCTION_DB'] = TEST_DB
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

from database import init_db, get_connection
from automations import (
    generate_customer_id, extract_last_name,
    get_rate_for_date, generate_receipt_filename,
    compute_invoice_dates_and_balance, update_invoice_status,
    get_ss_wage_cap, calculate_payroll_taxes,
    get_vendor_category, save_vendor_category,
    get_cert_status, parse_cert_filename,
    calculate_quick_quote, get_reminder_status,
)
from migration import (
    excel_serial_to_date, parse_flexible_date, parse_amount,
    detect_bank_format, normalize_bank_csv
)

init_db(TEST_DB)


class TestCustomerID(unittest.TestCase):
    def test_basic_name(self):
        # 'Bob and Barb Smith', 2024 → 'BSm24'
        cid = generate_customer_id('Bob and Barb Smith', 2024)
        self.assertEqual(cid, 'BSm24')

    def test_single_name(self):
        cid = generate_customer_id('Madonna', 2024)
        self.assertIsNotNone(cid)

    def test_business_name(self):
        cid = generate_customer_id('Acme Corp', 2023)
        self.assertIn('23', cid)

    def test_extract_last_name(self):
        self.assertEqual(extract_last_name('Bob and Barb Smith'), 'Smith')
        self.assertEqual(extract_last_name('John Doe'), 'Doe')
        self.assertEqual(extract_last_name('Madonna'), 'Madonna')


class TestRateLookup(unittest.TestCase):
    def setUp(self):
        self.conn = get_connection()
        # Insert test employee
        self.conn.execute("""
            INSERT OR IGNORE INTO employees (emp_id, first_name, last_name, status)
            VALUES (99, 'Test', 'Worker', 'Active')
        """)
        # Insert rates
        self.conn.execute("""
            INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour)
            VALUES (99, '2024-01-01', 75.0, 45.0)
        """)
        self.conn.execute("""
            INSERT INTO employee_rates (emp_id, effective_date, bill_rate_per_hour, cost_rate_per_hour)
            VALUES (99, '2024-06-01', 80.0, 50.0)
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_rate_before_first_effective(self):
        """No rate before first effective date"""
        rates = get_rate_for_date(99, '2023-12-31', self.conn)
        self.assertEqual(rates['bill_rate'], 0.0)

    def test_rate_on_effective_date(self):
        rates = get_rate_for_date(99, '2024-01-01', self.conn)
        self.assertEqual(rates['bill_rate'], 75.0)

    def test_rate_between_changes(self):
        rates = get_rate_for_date(99, '2024-03-15', self.conn)
        self.assertEqual(rates['bill_rate'], 75.0)
        self.assertEqual(rates['cost_rate'], 45.0)

    def test_rate_after_change(self):
        rates = get_rate_for_date(99, '2024-07-01', self.conn)
        self.assertEqual(rates['bill_rate'], 80.0)
        self.assertEqual(rates['cost_rate'], 50.0)


class TestReceiptFilename(unittest.TestCase):
    def test_basic_generation(self):
        fn = generate_receipt_filename('2024-03-15', 'Smith-2024', 'Home Depot', 250.50)
        self.assertEqual(fn, '2024-03-15.Smith-2024.HomeDepot.250_50.pdf')

    def test_vendor_sanitization(self):
        fn = generate_receipt_filename('2024-01-01', 'Job1', 'Ace Hardware & Supply Co.!', 100.0)
        self.assertNotIn('&', fn)
        self.assertNotIn('!', fn)

    def test_empty_vendor(self):
        fn = generate_receipt_filename('2024-01-01', 'Job1', '', 100.0)
        self.assertEqual(fn, '')


class TestInvoiceCalculations(unittest.TestCase):
    def test_due_date(self):
        result = compute_invoice_dates_and_balance('2024-03-01', 5000.0, 0.0)
        self.assertEqual(result['due_date'], '2024-03-31')
        self.assertEqual(result['balance_due'], 5000.0)

    def test_partial_payment(self):
        result = compute_invoice_dates_and_balance('2024-01-01', 1000.0, 400.0)
        self.assertEqual(result['balance_due'], 600.0)

    def test_full_payment(self):
        result = compute_invoice_dates_and_balance('2024-01-01', 1000.0, 1000.0)
        self.assertEqual(result['balance_due'], 0.0)


class TestPayrollTaxes(unittest.TestCase):
    def setUp(self):
        self.conn = get_connection()
        self.conn.execute("""
            INSERT OR IGNORE INTO employees (emp_id, first_name, last_name)
            VALUES (88, 'Pay', 'Employee')
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_basic_payroll(self):
        result = calculate_payroll_taxes(88, 2025, 1000.0, conn=self.conn)
        # SS: 6.2% of 1000
        self.assertAlmostEqual(result['ss_withheld'], 62.0, places=2)
        # Medicare: 1.45%
        self.assertAlmostEqual(result['medicare_withheld'], 14.50, places=2)

    def test_ss_wage_cap_lookup(self):
        cap = get_ss_wage_cap(2024, self.conn)
        self.assertEqual(cap, 168600.0)

        cap2025 = get_ss_wage_cap(2025, self.conn)
        self.assertEqual(cap2025, 176100.0)

    def test_futa_wage_base_limit(self):
        """FUTA only applies to first $7,000"""
        result = calculate_payroll_taxes(88, 2025, 1000.0, conn=self.conn)
        # 0.006 * 1000 = $6.00
        self.assertAlmostEqual(result['futa_amount'], 6.0, places=2)


class TestCertStatus(unittest.TestCase):
    def test_expired(self):
        result = get_cert_status('2020-01-01')
        self.assertEqual(result['status'], 'Expired')
        self.assertEqual(result['color'], 'red')

    def test_current(self):
        result = get_cert_status('2030-01-01')
        self.assertEqual(result['status'], 'Current')
        self.assertEqual(result['color'], 'green')

    def test_expiring_soon(self):
        from datetime import timedelta
        soon = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        result = get_cert_status(soon)
        self.assertEqual(result['color'], 'yellow')

    def test_parse_cert_filename(self):
        result = parse_cert_filename('ABC Insurance 01-15-24_01-15-25.pdf')
        self.assertEqual(result['company_name'], 'ABC Insurance')
        self.assertEqual(result['start_date'], '2024-01-15')
        self.assertEqual(result['end_date'], '2025-01-15')


class TestQuickQuote(unittest.TestCase):
    def test_basic_quote(self):
        result = calculate_quick_quote(100000, 15, 0.9, 0, 10)
        self.assertAlmostEqual(result['total'], 100000.0, places=0)
        self.assertAlmostEqual(result['overhead'], 15000.0, places=0)
        self.assertAlmostEqual(result['insurance'], 900.0, places=0)

    def test_over_100_pct_error(self):
        result = calculate_quick_quote(100000, 50, 50, 10, 10)
        self.assertIn('error', result)

    def test_zero_total(self):
        result = calculate_quick_quote(0, 15, 0.9, 0, 10)
        self.assertEqual(result['total'], 0.0)


class TestVendorCategory(unittest.TestCase):
    def setUp(self):
        self.conn = get_connection()
        save_vendor_category('Home Depot', 'Materials', self.conn)

    def tearDown(self):
        self.conn.close()

    def test_exact_match(self):
        cat = get_vendor_category('Home Depot', self.conn)
        self.assertEqual(cat, 'Materials')

    def test_no_match(self):
        cat = get_vendor_category('Unknown Vendor XYZ', self.conn)
        self.assertEqual(cat, '')


class TestMigrationHelpers(unittest.TestCase):
    def test_excel_serial(self):
        # Excel serial 45000 = 2023-03-15 (1899-12-30 + 45000 days)
        result = excel_serial_to_date(45000)
        self.assertEqual(result, '2023-03-15')

    def test_parse_date_formats(self):
        self.assertEqual(parse_flexible_date('2024-03-15'), '2024-03-15')
        self.assertEqual(parse_flexible_date('03/15/2024'), '2024-03-15')
        self.assertEqual(parse_flexible_date('3/15/24'), '2024-03-15')

    def test_parse_amount(self):
        self.assertEqual(parse_amount('$1,234.56'), 1234.56)
        self.assertEqual(parse_amount('(500.00)'), -500.0)
        self.assertEqual(parse_amount(''), 0.0)
        self.assertEqual(parse_amount(None), 0.0)

    def test_bank_csv_parse(self):
        chase_csv = """Transaction Date,Post Date,Description,Category,Type,Amount,Memo
01/15/2024,01/16/2024,HOME DEPOT,Shopping,Sale,-125.43,
01/16/2024,01/17/2024,PAYROLL,Transfer,Credit,3500.00,"""
        rows = normalize_bank_csv(chase_csv)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['transaction_date'], '2024-01-15')
        self.assertEqual(rows[0]['amount'], -125.43)


class TestReminderStatus(unittest.TestCase):
    def test_done(self):
        r = get_reminder_status('2024-01-01', 'Done')
        self.assertEqual(r['color'], 'green')

    def test_overdue(self):
        r = get_reminder_status('2020-01-01', 'Pending')
        self.assertEqual(r['color'], 'red')
        self.assertIn('Overdue', r['label'])


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    if result.wasSuccessful():
        print("\n✅ All tests passed!")
    else:
        print(f"\n❌ {len(result.failures)} failure(s), {len(result.errors)} error(s)")
    sys.exit(0 if result.wasSuccessful() else 1)
