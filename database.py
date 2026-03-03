"""
Database initialization and connection management.
All tables defined here. SQLite with WAL mode for concurrent access.
"""
import sqlite3
import os
from contextlib import contextmanager

DB_PATH = os.environ.get("CONSTRUCTION_DB", "construction.db")

def get_db_path():
    return DB_PATH

def set_db_path(path):
    global DB_PATH
    DB_PATH = path

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

@contextmanager
def db():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

SCHEMA_SQL = """
-- ============================================================
-- COMPANY CONFIG (single row)
-- ============================================================
CREATE TABLE IF NOT EXISTS company_config (
    id INTEGER PRIMARY KEY DEFAULT 1,
    company_name TEXT DEFAULT '',
    owner_name TEXT DEFAULT '',
    address TEXT DEFAULT '',
    city_state_zip TEXT DEFAULT '',
    phone TEXT DEFAULT '',
    email TEXT DEFAULT '',
    website TEXT DEFAULT '',
    license_number TEXT DEFAULT '',
    ein TEXT DEFAULT '',
    receipts_folder_path TEXT DEFAULT '',
    invoices_folder_path TEXT DEFAULT '',
    certs_folder_path TEXT DEFAULT '',
    backup_folder_path TEXT DEFAULT '',
    backup_keep_count INTEGER DEFAULT 30,
    continuous_scroll INTEGER DEFAULT 0,
    time_tracker_enabled INTEGER DEFAULT 0,
    active_jobs_folder_path TEXT DEFAULT '',
    payroll_year INTEGER DEFAULT 2025,
    fica_rate_employee REAL DEFAULT 0.062,
    medicare_rate_employee REAL DEFAULT 0.0145,
    fica_rate_employer REAL DEFAULT 0.062,
    medicare_rate_employer REAL DEFAULT 0.0145,
    futa_rate REAL DEFAULT 0.006,
    futa_wage_base REAL DEFAULT 7000.0,
    suta_rate_il REAL DEFAULT 0.0350,
    suta_wage_base_il REAL DEFAULT 13590.0,
    prior_year_withholding_carryforward REAL DEFAULT 0.0,
    prior_year_trade_adjustment REAL DEFAULT 0.0,
    opening_bank_balance REAL DEFAULT 0.0,
    default_overhead_pct REAL DEFAULT 5.0,
    default_insurance_pct REAL DEFAULT 1.0,
    default_owner_wages_pct REAL DEFAULT 11.0,
    default_profit_pct REAL DEFAULT 20.0,
    default_markup_pct REAL DEFAULT 0.20,
    estimate_prefix TEXT DEFAULT 'EST',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- Ensure one config row always exists
INSERT OR IGNORE INTO company_config (id) VALUES (1);

-- ============================================================
-- SS WAGE CAPS
-- ============================================================
CREATE TABLE IF NOT EXISTS ss_wage_caps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL UNIQUE,
    cap_amount REAL NOT NULL,
    irs_source TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

INSERT OR IGNORE INTO ss_wage_caps (year, cap_amount, irs_source) VALUES
    (2021, 142800, 'IRS Rev. Proc. 2020-45'),
    (2022, 147000, 'IRS Rev. Proc. 2021-45'),
    (2023, 160200, 'IRS Rev. Proc. 2022-38'),
    (2024, 168600, 'IRS Rev. Proc. 2023-34'),
    (2025, 176100, 'IRS Notice 2024-80');

-- ============================================================
-- CLIENTS
-- ============================================================
CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year_acquired INTEGER,
    customer_id TEXT UNIQUE,
    last_name TEXT DEFAULT '',
    full_name TEXT NOT NULL DEFAULT '',
    address TEXT DEFAULT '',
    city_state_zip TEXT DEFAULT '',
    phone1 TEXT DEFAULT '',
    phone2 TEXT DEFAULT '',
    email1 TEXT DEFAULT '',
    email2 TEXT DEFAULT '',
    status TEXT DEFAULT 'Active' CHECK(status IN ('Active','Archived','Prospect')),
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- EMPLOYEES
-- ============================================================
CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    emp_id INTEGER UNIQUE NOT NULL,
    first_name TEXT NOT NULL DEFAULT '',
    last_name TEXT NOT NULL DEFAULT '',
    phone TEXT DEFAULT '',
    email TEXT DEFAULT '',
    address TEXT DEFAULT '',
    city_state_zip TEXT DEFAULT '',
    gender TEXT DEFAULT '',
    occupation TEXT DEFAULT '',
    hire_date TEXT DEFAULT '',
    status TEXT DEFAULT 'Active' CHECK(status IN ('Active','Inactive')),
    ssn_encrypted TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- EMPLOYEE RATES (effective-date based)
-- ============================================================
CREATE TABLE IF NOT EXISTS employee_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    emp_id INTEGER NOT NULL REFERENCES employees(emp_id),
    effective_date TEXT NOT NULL,
    bill_rate_per_hour REAL NOT NULL DEFAULT 0.0,
    cost_rate_per_hour REAL NOT NULL DEFAULT 0.0,
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_emp_rates ON employee_rates(emp_id, effective_date);

-- ============================================================
-- CONTRACTORS / VENDORS
-- ============================================================
CREATE TABLE IF NOT EXISTS contractors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rank_preference INTEGER DEFAULT 0,
    trade_type TEXT DEFAULT '',
    company_name TEXT NOT NULL DEFAULT '',
    contact_person TEXT DEFAULT '',
    phone TEXT DEFAULT '',
    cell TEXT DEFAULT '',
    email TEXT DEFAULT '',
    website TEXT DEFAULT '',
    address TEXT DEFAULT '',
    license_number TEXT DEFAULT '',
    vendor_type TEXT DEFAULT 'Subcontractor' CHECK(vendor_type IN ('Subcontractor','Supplier','Service Provider','Government/Tax','Other')),
    is_supplier INTEGER DEFAULT 0,
    requires_1099 INTEGER DEFAULT 0,
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- VENDOR → CATEGORY MAPPINGS
-- ============================================================
CREATE TABLE IF NOT EXISTS vendor_categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_name TEXT NOT NULL UNIQUE,
    default_category TEXT NOT NULL DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- WORK CATEGORIES
-- ============================================================
CREATE TABLE IF NOT EXISTS work_categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_name TEXT NOT NULL UNIQUE,
    is_cogs INTEGER DEFAULT 0,
    is_tax_deductible INTEGER DEFAULT 1,
    is_transfer INTEGER DEFAULT 0,   -- 1 = account transfer / equity (exclude from P&L)
    schedule_c_line TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- Seed default categories — full KB category list
INSERT OR IGNORE INTO work_categories (category_name, is_cogs, is_tax_deductible, is_transfer, schedule_c_line) VALUES
    ('ACCOUNT CREDIT',       0, 0, 0, ''),
    ('Appliances',           1, 1, 0, 'Line 38'),
    ('Cabinetry',            1, 1, 0, 'Line 38'),
    ('Carpet',               1, 1, 0, 'Line 38'),
    ('Cleaning',             1, 1, 0, 'Line 26'),
    ('Closets',              1, 1, 0, 'Line 38'),
    ('Concrete',             1, 1, 0, 'Line 38'),
    ('Contribution',         0, 1, 1, 'Line 19'),
    ('Counter-tops',         1, 1, 0, 'Line 38'),
    ('Credit',               0, 0, 1, ''),
    ('Credit Card Payment',  0, 0, 1, ''),
    ('Decking',              1, 1, 0, 'Line 38'),
    ('Demolition',           1, 1, 0, 'Line 26'),
    ('Disposal',             1, 1, 0, 'Line 26'),
    ('Distribution',         0, 0, 1, ''),
    ('Driveway',             1, 1, 0, 'Line 38'),
    ('Drywall',              1, 1, 0, 'Line 38'),
    ('Electrical',           1, 1, 0, 'Line 11'),
    ('Electrical-Trim',      1, 1, 0, 'Line 11'),
    ('Excavation',           1, 1, 0, 'Line 38'),
    ('Fencing',              1, 1, 0, 'Line 38'),
    ('Fireplace',            1, 1, 0, 'Line 38'),
    ('Flooring-Carpet',      1, 1, 0, 'Line 38'),
    ('Flooring-Epoxy',       1, 1, 0, 'Line 38'),
    ('Flooring-Tile',        1, 1, 0, 'Line 38'),
    ('Flooring-Vinyl',       1, 1, 0, 'Line 38'),
    ('Flooring-Wood',        1, 1, 0, 'Line 38'),
    ('Framing-Labor',        1, 1, 0, 'Line 26'),
    ('Framing-Materials',    1, 1, 0, 'Line 38'),
    ('Garage Doors',         1, 1, 0, 'Line 38'),
    ('GC',                   1, 1, 0, 'Line 11'),
    ('Glass and Mirror',     1, 1, 0, 'Line 38'),
    ('Golf Sim',             1, 1, 0, 'Line 38'),
    ('Gutters',              1, 1, 0, 'Line 38'),
    ('Hardware',             1, 1, 0, 'Line 38'),
    ('HVAC',                 1, 1, 0, 'Line 11'),
    ('HVAC-Trim',            1, 1, 0, 'Line 11'),
    ('Income Received',      0, 0, 0, ''),
    ('Insulation',           1, 1, 0, 'Line 38'),
    ('KB',                   0, 0, 1, ''),
    ('Labor',                1, 1, 0, 'Line 26'),
    ('Landscaping',          1, 1, 0, 'Line 38'),
    ('Low Voltage',          1, 1, 0, 'Line 11'),
    ('Masonry',              1, 1, 0, 'Line 38'),
    ('Materials',            1, 1, 0, 'Line 38'),
    ('Memo',                 0, 0, 1, ''),
    ('Millwork',             1, 1, 0, 'Line 38'),
    ('Miscellaneous',        1, 1, 0, 'Line 27a'),
    ('Painting',             1, 1, 0, 'Line 11'),
    ('Permits and Fees',     0, 1, 0, 'Line 23'),
    ('Plumbing',             1, 1, 0, 'Line 11'),
    ('Plumbing-Trim',        1, 1, 0, 'Line 11'),
    ('Pool',                 1, 1, 0, 'Line 38'),
    ('Preliminary',          1, 1, 0, 'Line 26'),
    ('Previous',             0, 0, 1, ''),
    ('Protection',           1, 1, 0, 'Line 38'),
    ('Roofing',              1, 1, 0, 'Line 38'),
    ('Siding',               1, 1, 0, 'Line 38'),
    ('Stairs',               1, 1, 0, 'Line 38'),
    ('Steel',                1, 1, 0, 'Line 38'),
    ('Subcontractors',       1, 1, 0, 'Line 11'),
    ('Surveyor',             1, 1, 0, 'Line 38'),
    ('T&M',                  1, 1, 0, 'Line 26'),
    ('Tools',                0, 1, 0, 'Line 22'),
    ('Waterproofing',        1, 1, 0, 'Line 38'),
    ('Window Wells',         1, 1, 0, 'Line 38'),
    ('Windows and Doors',    1, 1, 0, 'Line 38'),
    ('WRITE OFF',            0, 0, 0, ''),
    ('Advertising',          0, 1, 0, 'Line 8'),
    ('Equipment Rental',     1, 1, 0, 'Line 20a'),
    ('Vehicle/Fuel',         0, 1, 0, 'Line 9'),
    ('Insurance',            0, 1, 0, 'Line 15'),
    ('Office Supplies',      0, 1, 0, 'Line 18'),
    ('Utilities',            0, 1, 0, 'Line 25'),
    ('Professional Fees',    0, 1, 0, 'Line 17'),
    ('Dues/Subscriptions',   0, 1, 0, 'Line 27a'),
    ('Bank Fees',            0, 1, 0, 'Line 27a'),
    ('Meals (50%)',          0, 1, 0, 'Line 24b'),
    ('Travel',               0, 1, 0, 'Line 24a'),
    ('Depreciation',         0, 1, 0, 'Line 13'),
    ('Other Expense',        0, 1, 0, 'Line 27a'),
    ('Loan Proceeds',        0, 0, 1, ''),
    ('Owner Draw',           0, 0, 1, '');

INSERT OR IGNORE INTO work_categories (category_name, is_cogs, is_tax_deductible, is_transfer, schedule_c_line) VALUES
    ('Subcontractor Labor',  1, 1, 0, 'Line 11'),
    ('Materials & Supplies', 1, 1, 0, 'Line 38'),
    ('Vehicle & Travel',     0, 1, 0, 'Line 9'),
    ('Wages - W2',           0, 1, 0, 'Line 26'),
    ('Bad Debt / Write-Off', 0, 1, 0, 'Line 27a'),
    ('Taxes & Licenses',     0, 1, 0, 'Line 23');

-- ============================================================
-- JOBS / PROJECTS
-- ============================================================
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_code TEXT NOT NULL UNIQUE,
    job_number INTEGER,
    client_id INTEGER REFERENCES clients(id),
    description TEXT DEFAULT '',
    status TEXT DEFAULT 'Active' CHECK(status IN ('Active','Bidding','Archived','Cancelled')),
    start_date TEXT DEFAULT '',
    end_date TEXT DEFAULT '',
    contract_amount REAL DEFAULT 0.0,
    budget_amount REAL DEFAULT 0.0,       -- internal cost budget (Phase 9)
    notes TEXT DEFAULT '',
    notes_internal TEXT DEFAULT '',       -- internal-only notes (Phase 9)
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- JOB MILESTONES  (Phase 9)
-- ============================================================
CREATE TABLE IF NOT EXISTS job_milestones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    title TEXT NOT NULL DEFAULT '',
    due_date TEXT DEFAULT '',
    completed_date TEXT DEFAULT '',
    status TEXT DEFAULT 'Pending' CHECK(status IN ('Pending','Complete','Skipped')),
    sort_order INTEGER DEFAULT 0,
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);


-- ============================================================
-- PROGRAM TIME CLOCK (tracks app usage for accounting time)
-- ============================================================
CREATE TABLE IF NOT EXISTS program_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time TEXT NOT NULL,
    end_time TEXT DEFAULT '',
    active_minutes REAL DEFAULT 0.0,
    description TEXT DEFAULT '',
    tags TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now'))
);

-- Default Write-Off job entry (for categorizing personal/business write-offs)
INSERT OR IGNORE INTO jobs (job_code, description, status, job_number)
VALUES ('WRITE-OFF', 'Tax Write-Offs & Business Expenses', 'Active', 0);

-- ============================================================
-- BANK ACCOUNTS
-- ============================================================
CREATE TABLE IF NOT EXISTS bank_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_name TEXT NOT NULL DEFAULT '',
    account_type TEXT DEFAULT 'Checking' CHECK(account_type IN ('Checking','CreditCard','Savings')),
    institution_name TEXT DEFAULT '',
    last_four TEXT DEFAULT '',
    routing_number_encrypted TEXT DEFAULT '',
    account_number_encrypted TEXT DEFAULT '',
    current_balance REAL DEFAULT 0.0,
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- TIMESHEET
-- ============================================================
CREATE TABLE IF NOT EXISTS timesheet (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_date TEXT NOT NULL,
    job_code TEXT DEFAULT '',
    invoice_number TEXT DEFAULT '',
    emp_id INTEGER REFERENCES employees(emp_id),
    hours REAL DEFAULT 0.0,
    bill_rate REAL DEFAULT 0.0,
    cost_rate REAL DEFAULT 0.0,
    bill_amount REAL DEFAULT 0.0,
    cost_amount REAL DEFAULT 0.0,
    expenses REAL DEFAULT 0.0,
    description TEXT DEFAULT '',
    work_type TEXT DEFAULT '',
    billable TEXT DEFAULT 'Billable',
    notes TEXT DEFAULT '',
    person_label TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_timesheet_date ON timesheet(entry_date);
CREATE INDEX IF NOT EXISTS idx_timesheet_emp ON timesheet(emp_id);
CREATE INDEX IF NOT EXISTS idx_timesheet_job ON timesheet(job_code);

-- ============================================================
-- LEDGER
-- ============================================================
CREATE TABLE IF NOT EXISTS ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_date TEXT NOT NULL,
    nickname TEXT DEFAULT '',
    job_code TEXT DEFAULT '',
    job_number TEXT DEFAULT '',
    invoice_number TEXT DEFAULT '',
    status TEXT DEFAULT 'Pending',
    category TEXT DEFAULT '',
    description TEXT DEFAULT '',
    vendor TEXT DEFAULT '',
    is_cogs INTEGER DEFAULT 0,
    amount REAL DEFAULT 0.0,
    type_of_payment TEXT DEFAULT '',
    memo TEXT DEFAULT '',
    receipt_filename TEXT DEFAULT '',
    receipt_verified INTEGER DEFAULT 0,
    coi_verified INTEGER DEFAULT 0,
    duplicate_flag TEXT DEFAULT '',
    is_pending INTEGER DEFAULT 0,
    bank_account_id INTEGER REFERENCES bank_accounts(id),
    reconciliation_id INTEGER,
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_ledger_date ON ledger(entry_date);
CREATE INDEX IF NOT EXISTS idx_ledger_job ON ledger(job_code, category);
CREATE INDEX IF NOT EXISTS idx_ledger_vendor ON ledger(vendor);

-- ============================================================
-- BANK TRANSACTIONS (imported)
-- ============================================================
CREATE TABLE IF NOT EXISTS bank_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_account_id INTEGER REFERENCES bank_accounts(id),
    transaction_date TEXT NOT NULL,
    description TEXT DEFAULT '',
    amount REAL DEFAULT 0.0,
    transaction_type TEXT DEFAULT 'Debit' CHECK(transaction_type IN ('Debit','Credit')),
    reference_number TEXT DEFAULT '',
    import_batch_id TEXT DEFAULT '',
    matched_ledger_id INTEGER REFERENCES ledger(id),
    match_status TEXT DEFAULT 'Unmatched' CHECK(match_status IN ('Unmatched','Auto-Matched','Manual-Matched','Excluded','Needs-Review')),
    source TEXT DEFAULT 'CSV' CHECK(source IN ('CSV','Plaid','Manual')),
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- RECONCILIATION SESSIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS reconciliation_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_account_id INTEGER REFERENCES bank_accounts(id),
    session_date TEXT DEFAULT (date('now')),
    statement_end_date TEXT DEFAULT '',
    statement_ending_balance REAL DEFAULT 0.0,
    computed_balance REAL DEFAULT 0.0,
    difference REAL DEFAULT 0.0,
    status TEXT DEFAULT 'In-Progress' CHECK(status IN ('In-Progress','Complete')),
    completed_by TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- INVOICES
-- ============================================================
CREATE TABLE IF NOT EXISTS invoices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_number INTEGER UNIQUE,
    invoice_date TEXT NOT NULL,
    due_date TEXT DEFAULT '',
    job_code TEXT DEFAULT '',
    client_id INTEGER REFERENCES clients(id),
    description_of_work TEXT DEFAULT '',
    amount REAL DEFAULT 0.0,
    amount_paid REAL DEFAULT 0.0,
    balance_due REAL DEFAULT 0.0,
    status TEXT DEFAULT 'Pending' CHECK(status IN ('Pending','Paid','Partial','Overdue')),
    pdf_path TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_invoices_status ON invoices(status);

-- ============================================================
-- PAYROLL RUNS
-- ============================================================
CREATE TABLE IF NOT EXISTS payroll_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT DEFAULT (date('now')),
    pay_period_start TEXT DEFAULT '',
    pay_period_end TEXT DEFAULT '',
    week_number INTEGER DEFAULT 0,
    year INTEGER DEFAULT 2025,
    emp_id INTEGER REFERENCES employees(emp_id),
    standard_hours REAL DEFAULT 0.0,
    standard_pay_rate REAL DEFAULT 0.0,
    standard_pay REAL DEFAULT 0.0,
    overtime_hours REAL DEFAULT 0.0,
    overtime_pay REAL DEFAULT 0.0,
    holiday_hours REAL DEFAULT 0.0,
    holiday_pay REAL DEFAULT 0.0,
    gross_pay REAL DEFAULT 0.0,
    ss_withheld REAL DEFAULT 0.0,
    medicare_withheld REAL DEFAULT 0.0,
    fed_withholding REAL DEFAULT 0.0,
    state_withholding REAL DEFAULT 0.0,
    total_withheld REAL DEFAULT 0.0,
    net_pay REAL DEFAULT 0.0,
    employer_ss REAL DEFAULT 0.0,
    employer_medicare REAL DEFAULT 0.0,
    futa_amount REAL DEFAULT 0.0,
    suta_amount REAL DEFAULT 0.0,
    check_number TEXT DEFAULT '',
    check_printed INTEGER DEFAULT 0,
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_payroll_emp_year ON payroll_runs(emp_id, year);

-- ============================================================
-- CERTIFICATES (COI tracker)
-- ============================================================
CREATE TABLE IF NOT EXISTS certificates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contractor_id INTEGER REFERENCES contractors(id),
    company_name TEXT DEFAULT '',
    cert_filename TEXT DEFAULT '',
    cert_verified INTEGER DEFAULT 0,
    cert_date_modified TEXT DEFAULT '',
    cert_type TEXT DEFAULT '',
    start_date TEXT DEFAULT '',
    end_date TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- TAX REMINDERS
-- ============================================================
CREATE TABLE IF NOT EXISTS reminders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    due_date TEXT NOT NULL,
    payment_date TEXT DEFAULT '',
    task_description TEXT NOT NULL DEFAULT '',
    is_recurring INTEGER DEFAULT 0,
    frequency TEXT DEFAULT '' CHECK(frequency IN ('','Monthly','Quarterly','Yearly')),
    status TEXT DEFAULT 'Pending' CHECK(status IN ('Pending','Done','RemindLater')),
    url TEXT DEFAULT '',
    amount REAL DEFAULT 0.0,
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- Seed standard tax deadlines for 2025
INSERT OR IGNORE INTO reminders (id, due_date, task_description, is_recurring, frequency, status) VALUES
    (1, '2025-01-15', 'Q4 2024 Estimated Tax Payment (Federal)', 1, 'Quarterly', 'Pending'),
    (2, '2025-01-31', 'Deposit FUTA tax (if >$500)', 1, 'Quarterly', 'Pending'),
    (3, '2025-01-31', 'Form 941 - Q4 2024', 1, 'Quarterly', 'Pending'),
    (4, '2025-01-31', 'W-2s to employees', 1, 'Yearly', 'Pending'),
    (5, '2025-01-31', '1099-NEC to contractors', 1, 'Yearly', 'Pending'),
    (6, '2025-02-28', 'W-2/1099 copies to SSA/IRS', 1, 'Yearly', 'Pending'),
    (7, '2025-04-15', 'Q1 2025 Estimated Tax Payment (Federal)', 1, 'Quarterly', 'Pending'),
    (8, '2025-04-30', 'Form 941 - Q1 2025', 1, 'Quarterly', 'Pending'),
    (9, '2025-06-16', 'Q2 2025 Estimated Tax Payment (Federal)', 1, 'Quarterly', 'Pending'),
    (10, '2025-07-31', 'Form 941 - Q2 2025', 1, 'Quarterly', 'Pending'),
    (11, '2025-09-15', 'Q3 2025 Estimated Tax Payment (Federal)', 1, 'Quarterly', 'Pending'),
    (12, '2025-10-31', 'Form 941 - Q3 2025', 1, 'Quarterly', 'Pending'),
    (13, '2025-01-15', 'IL Q4 2024 Estimated Tax Payment', 1, 'Quarterly', 'Pending'),
    (14, '2025-04-15', 'IL Q1 2025 Estimated Tax Payment', 1, 'Quarterly', 'Pending'),
    (15, '2025-06-16', 'IL Q2 2025 Estimated Tax Payment', 1, 'Quarterly', 'Pending'),
    (16, '2025-09-15', 'IL Q3 2025 Estimated Tax Payment', 1, 'Quarterly', 'Pending');

-- ============================================================
-- JOB ESTIMATES (Phase 9 — schema ready)
-- ============================================================
CREATE TABLE IF NOT EXISTS job_estimates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER REFERENCES jobs(id),
    estimate_number TEXT DEFAULT '',
    estimate_date TEXT DEFAULT (date('now')),
    client_id INTEGER REFERENCES clients(id),
    status TEXT DEFAULT 'Draft' CHECK(status IN ('Draft','Sent','Accepted','Rejected','Revised')),
    overhead_pct REAL DEFAULT 0.0,
    insurance_pct REAL DEFAULT 0.0,
    owner_wages_pct REAL DEFAULT 0.0,
    profit_pct REAL DEFAULT 0.0,
    total_direct_costs REAL DEFAULT 0.0,
    total_estimate REAL DEFAULT 0.0,
    version_number INTEGER DEFAULT 1,
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS estimate_line_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    estimate_id INTEGER REFERENCES job_estimates(id),
    category TEXT DEFAULT '',
    labor_hours_estimated REAL DEFAULT 0.0,
    labor_rate REAL DEFAULT 0.0,
    labor_cost REAL DEFAULT 0.0,
    materials_cost REAL DEFAULT 0.0,
    subcontractor_cost REAL DEFAULT 0.0,
    other_cost REAL DEFAULT 0.0,
    total_cost REAL DEFAULT 0.0,
    markup_pct REAL DEFAULT 0.0,
    sell_price REAL DEFAULT 0.0,
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- UNDO LOG
-- ============================================================
CREATE TABLE IF NOT EXISTS undo_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_time TEXT DEFAULT (datetime('now')),
    table_name TEXT NOT NULL,
    record_id INTEGER NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('INSERT','UPDATE','DELETE')),
    old_data TEXT DEFAULT '',
    new_data TEXT DEFAULT '',
    reversed INTEGER DEFAULT 0,
    session_id TEXT DEFAULT '',
    user_label TEXT DEFAULT '',
    field_name TEXT DEFAULT ''
);

-- Keep only last 2000 undo entries
CREATE TRIGGER IF NOT EXISTS trim_undo_log
AFTER INSERT ON undo_log
BEGIN
    DELETE FROM undo_log WHERE id NOT IN (
        SELECT id FROM undo_log ORDER BY id DESC LIMIT 2000
    );
END;

-- ============================================================
-- RECURRING PATTERNS (exclude from duplicate detection)
-- ============================================================
CREATE TABLE IF NOT EXISTS recurring_patterns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor TEXT NOT NULL,
    amount_min REAL DEFAULT NULL,   -- NULL = match any amount
    amount_max REAL DEFAULT NULL,
    day_of_month INTEGER DEFAULT NULL,  -- NULL = any day
    notes TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    is_deleted INTEGER DEFAULT 0
);

-- ============================================================
-- IMPORT BATCHES (for migration tracking)
-- ============================================================
CREATE TABLE IF NOT EXISTS import_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT UNIQUE NOT NULL,
    source_filename TEXT DEFAULT '',
    import_date TEXT DEFAULT (datetime('now')),
    status TEXT DEFAULT 'Pending' CHECK(status IN ('Pending','Preview','Validated','Imported','Failed')),
    summary TEXT DEFAULT '',
    error_log TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- ============================================================
-- JOB ACTUALS VS ESTIMATE VIEW
-- ============================================================
CREATE VIEW IF NOT EXISTS job_actuals_vs_estimate AS
SELECT
    eli.estimate_id,
    eli.id AS line_item_id,
    je.job_id,
    j.job_code,
    eli.category,
    eli.labor_hours_estimated,
    eli.labor_cost AS estimated_labor,
    eli.materials_cost AS estimated_materials,
    eli.subcontractor_cost AS estimated_subs,
    eli.other_cost AS estimated_other,
    eli.total_cost AS estimated_total,
    eli.sell_price AS estimated_sell,
    COALESCE((
        SELECT SUM(t.cost_amount)
        FROM timesheet t
        WHERE t.job_code = j.job_code
          AND t.is_deleted = 0
    ), 0) AS actual_labor_cost,
    COALESCE((
        SELECT SUM(l.amount)
        FROM ledger l
        WHERE l.job_code = j.job_code
          AND l.category = eli.category
          AND l.is_cogs = 1
          AND l.is_deleted = 0
    ), 0) AS actual_category_cost
FROM estimate_line_items eli
JOIN job_estimates je ON eli.estimate_id = je.id
LEFT JOIN jobs j ON je.job_id = j.id
WHERE eli.is_deleted = 0 AND je.is_deleted = 0;
"""

def init_db(db_path=None):
    """Initialize the database with full schema."""
    if db_path:
        set_db_path(db_path)
    conn = get_connection()
    try:
        conn.executescript(SCHEMA_SQL)
        # Run migrations for existing databases (ADD COLUMN IF NOT EXISTS equivalent)
        migrations = [
            "ALTER TABLE company_config ADD COLUMN certs_folder_path TEXT DEFAULT ''",
            "ALTER TABLE certificates ADD COLUMN cert_verified INTEGER DEFAULT 0",
            # Phase 12 migrations
            "ALTER TABLE company_config ADD COLUMN backup_keep_count INTEGER DEFAULT 30",
            "ALTER TABLE company_config ADD COLUMN continuous_scroll INTEGER DEFAULT 0",
            "ALTER TABLE company_config ADD COLUMN time_tracker_enabled INTEGER DEFAULT 0",
            "ALTER TABLE certificates ADD COLUMN cert_pdf_filename TEXT DEFAULT ''",
            # Phase 13 migrations
            "ALTER TABLE ledger ADD COLUMN type_of_payment TEXT DEFAULT ''",
            "ALTER TABLE timesheet ADD COLUMN work_type TEXT DEFAULT ''",
            "ALTER TABLE timesheet ADD COLUMN billable TEXT DEFAULT 'Billable'",
            "ALTER TABLE invoices ADD COLUMN adjustment_amount REAL DEFAULT 0.0",
            "ALTER TABLE invoices ADD COLUMN adjustment_note TEXT DEFAULT ''",
            "ALTER TABLE contractors ADD COLUMN website TEXT DEFAULT ''",
            "ALTER TABLE contractors ADD COLUMN is_supplier INTEGER DEFAULT 0",
            # Phase 1 (ledger rebuild) migrations
            "ALTER TABLE ledger ADD COLUMN nickname TEXT DEFAULT ''",
            "ALTER TABLE ledger ADD COLUMN memo TEXT DEFAULT ''",
            "ALTER TABLE ledger ADD COLUMN coi_verified INTEGER DEFAULT 0",
            "ALTER TABLE ledger ADD COLUMN duplicate_flag TEXT DEFAULT ''",
            "ALTER TABLE contractors ADD COLUMN vendor_type TEXT DEFAULT 'Subcontractor'",
            # Update old fractional rate defaults to percentage values
            """UPDATE company_config SET
               default_overhead_pct=5.0 WHERE default_overhead_pct < 1.0 AND default_overhead_pct > 0""",
            """UPDATE company_config SET
               default_insurance_pct=1.0 WHERE default_insurance_pct < 1.0 AND default_insurance_pct > 0""",
            """UPDATE company_config SET
               default_owner_wages_pct=11.0 WHERE default_owner_wages_pct < 1.0 AND default_owner_wages_pct > 0""",
            """UPDATE company_config SET
               default_profit_pct=20.0 WHERE default_profit_pct < 1.0 AND default_profit_pct > 0""",
            # Phase 5: undo_log session tracking columns
            "ALTER TABLE undo_log ADD COLUMN session_id TEXT DEFAULT ''",
            "ALTER TABLE undo_log ADD COLUMN user_label TEXT DEFAULT ''",
            "ALTER TABLE undo_log ADD COLUMN field_name TEXT DEFAULT ''",
            # Phase 7: recurring patterns (table created via CREATE TABLE IF NOT EXISTS)
            # Also add fuzzy_score column to help UI show match confidence
            "ALTER TABLE ledger ADD COLUMN fuzzy_score REAL DEFAULT 0.0",
            # Phase 9: job enhancements
            "ALTER TABLE jobs ADD COLUMN budget_amount REAL DEFAULT 0.0",
            "ALTER TABLE jobs ADD COLUMN notes_internal TEXT DEFAULT ''",
            # Phase 14: transfer/equity category flag (excludes CC payments, draws from P&L)
            "ALTER TABLE work_categories ADD COLUMN is_transfer INTEGER DEFAULT 0",
            # Phase 15: pending transactions
            "ALTER TABLE ledger ADD COLUMN is_pending INTEGER DEFAULT 0",
        ]
        for sql in migrations:
            try:
                conn.execute(sql)
            except Exception:
                pass  # Column already exists
        # Backfill is_transfer flag for existing work_categories rows
        TRANSFER_NAMES = (
            'Credit', 'Credit Card Payment', 'Distribution', 'Distributions',
            'Contribution', 'Loan Proceeds', 'Owner Draw', 'Previous',
            'KB', 'Memo',
        )
        try:
            placeholders = ','.join('?' * len(TRANSFER_NAMES))
            conn.execute(
                f"UPDATE work_categories SET is_transfer=1 WHERE category_name IN ({placeholders})",
                list(TRANSFER_NAMES)
            )
        except Exception:
            pass

        # Backfill type_of_payment from notes for existing imported rows
        # Notes contain "payment:CHECK#1234" style entries
        try:
            conn.execute("""
                UPDATE ledger
                SET type_of_payment = TRIM(SUBSTR(
                    notes,
                    INSTR(notes, 'payment:') + 8,
                    CASE
                        WHEN INSTR(SUBSTR(notes, INSTR(notes, 'payment:') + 8), ' |') > 0
                        THEN INSTR(SUBSTR(notes, INSTR(notes, 'payment:') + 8), ' |') - 1
                        ELSE 100
                    END
                ))
                WHERE notes LIKE '%payment:%' AND (type_of_payment = '' OR type_of_payment IS NULL)
            """)
        except Exception:
            pass

        conn.commit()
        print(f"Database initialized at: {DB_PATH}")

        # Backfill vendor_type from is_supplier for existing records
        try:
            # is_supplier=1 → Supplier, requires_1099=1 → Subcontractor, else → Other
            conn.execute("""
                UPDATE contractors
                SET vendor_type = CASE
                    WHEN is_supplier = 1 THEN 'Supplier'
                    WHEN requires_1099 = 1 THEN 'Subcontractor'
                    ELSE 'Other'
                END
                WHERE (vendor_type = '' OR vendor_type = 'Subcontractor')
                  AND (is_supplier = 1 OR requires_1099 = 1 OR is_supplier = 0)
                  AND id IN (SELECT id FROM contractors WHERE vendor_type IS NULL OR vendor_type = '')
            """)
            # Simpler fallback: update all records that still have empty vendor_type
            conn.execute("""
                UPDATE contractors
                SET vendor_type = CASE
                    WHEN is_supplier = 1 THEN 'Supplier'
                    WHEN requires_1099 = 1 THEN 'Subcontractor'
                    ELSE 'Subcontractor'
                END
                WHERE vendor_type IS NULL OR vendor_type = ''
            """)
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
