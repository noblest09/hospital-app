# -*- coding: utf-8 -*-
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).with_name("hospital.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    if not _table_exists(conn, table_name):
        return []
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [r["name"] for r in rows]


def _add_column_if_missing(conn: sqlite3.Connection, table_name: str, column_sql: str, column_name: str):
    cols = _table_columns(conn, table_name)
    if column_name not in cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


def init_db(force_recreate: bool = False):
    """
    DB jadvallarini yaratadi.
    force_recreate=False bo'lsa hech narsani o'chirmaydi.
    """
    conn = get_conn()
    cur = conn.cursor()

    # =========================================================
    # ASOSIY IMPORT / SOZLAMALAR
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS import_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        filename TEXT,
        rows_count INTEGER NOT NULL,
        imported_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS column_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module TEXT NOT NULL,
        map_name TEXT NOT NULL,
        field_key TEXT NOT NULL,
        excel_column TEXT NOT NULL,
        UNIQUE(module, map_name, field_key)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS main_groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module TEXT NOT NULL,
        name TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1,
        UNIQUE(module, name)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS service_main_group (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module TEXT NOT NULL,
        service_name TEXT NOT NULL,
        main_group_name TEXT NOT NULL,
        UNIQUE(module, service_name)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ungrouped_services (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module TEXT NOT NULL,
        service_name TEXT NOT NULL,
        first_seen_year INTEGER,
        first_seen_month INTEGER,
        UNIQUE(module, service_name)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS group_synonyms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module TEXT NOT NULL,
        raw_name TEXT NOT NULL,
        canonical_name TEXT NOT NULL,
        UNIQUE(module, raw_name)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS service_synonyms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module TEXT NOT NULL,
        raw_name TEXT NOT NULL,
        canonical_name TEXT NOT NULL,
        UNIQUE(module, raw_name)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS module_settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    # =========================================================
    # BO'LIMLAR RO'YXATI
    # =========================================================
    # Eslatma:
    # Sizda bo'limlar oldin statsionar module bilan saqlangan.
    # Biz shuni umumiy bo'lim ro'yxati sifatida ishlatamiz.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS department_whitelist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module TEXT NOT NULL,          -- 'statsionar' / keyinchalik common sifatida ham ishlatish mumkin
        department TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1,
        UNIQUE(module, department)
    )
    """)

    # =========================================================
    # AMBULATOR RAW
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ambulator_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        amb_no TEXT,
        fio TEXT,
        country TEXT,
        order_type TEXT,
        service_group TEXT,
        service_name TEXT,
        qty REAL DEFAULT 0,
        price REAL DEFAULT 0,
        amount REAL DEFAULT 0
    )
    """)

    # =========================================================
    # STATSIONAR RAW / HISOB
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS statsionar_patient (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        payment_type TEXT NOT NULL,   -- 'order' / 'pullik'
        patient_id TEXT NOT NULL,
        fio TEXT,
        country TEXT,
        department TEXT,
        admission_date TEXT,
        discharge_date TEXT,
        tulov REAL DEFAULT 0,
        akt_sum REAL DEFAULT 0,
        drug_sum REAL DEFAULT 0,
        UNIQUE(year, month, payment_type, patient_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS statsionar_service_amount (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        payment_type TEXT NOT NULL,
        patient_id TEXT NOT NULL,
        department TEXT,
        service_name TEXT NOT NULL,
        amount REAL DEFAULT 0
    )
    """)

    # Eski placeholder jadval bo'lsa o'chiramiz
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='statsionar_raw'")
    if cur.fetchone():
        cur.execute("DROP TABLE IF EXISTS statsionar_raw")

    # =========================================================
    # FOIZ QOIDALARI
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS foiz_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      module TEXT NOT NULL,                 -- 'statsionar' / 'ambulator'
      payment_type TEXT NOT NULL,           -- 'order' / 'pullik' / 'all'
      group_name TEXT NOT NULL,
      to_department TEXT NOT NULL,
      percent REAL NOT NULL,
      UNIQUE(module, payment_type, group_name, to_department)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS foiz_manual (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      report_type TEXT NOT NULL,            -- 'order' / 'pullik' / 'all'
      department TEXT NOT NULL,
      avans REAL NOT NULL DEFAULT 0,
      rentabillik REAL NOT NULL DEFAULT 0,
      UNIQUE(year, month, report_type, department)
    )
    """)

    # =========================================================
    # POLIKLINIKA(OPD) MAPPING
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS doctor_excel_mapping (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_key TEXT NOT NULL,             -- 'amb' / 'stat'
      field_key TEXT NOT NULL,              -- header_row / group / service / qty / amount / exec_doc / ref_doc / department
      excel_column TEXT NOT NULL,
      UNIQUE(source_key, field_key)
    )
    """)

    # =========================================================
    # POLIKLINIKA(OPD) TANLANGAN XIZMATLAR
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS poliklinika_selected_services (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      source_key TEXT NOT NULL,             -- 'amb_exec' / 'amb_ref' / 'stat_exec'
      service_name TEXT NOT NULL,
      UNIQUE(year, month, source_key, service_name)
    )
    """)

    # =========================================================
    # POLIKLINIKA(OPD) VRACH FOIZLARI
    # =========================================================
    # ESKI versiyada role yo'q bo'lishi mumkin, shuning uchun migratsiya qilamiz
    if not _table_exists(conn, "doctor_percent_rules"):
        cur.execute("""
        CREATE TABLE IF NOT EXISTS doctor_percent_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          year INTEGER NOT NULL,
          month INTEGER NOT NULL,
          source_key TEXT NOT NULL,         -- 'amb_exec' / 'amb_ref' / 'stat_exec'
          doctor_name TEXT NOT NULL,
          percent REAL NOT NULL DEFAULT 0,
          UNIQUE(year, month, source_key, doctor_name)
        )
        """)
    else:
        cols = _table_columns(conn, "doctor_percent_rules")

        # Agar eski juda eski jadval bo'lsa va kerakli ustunlar yo'q bo'lsa
        if "year" not in cols:
            _add_column_if_missing(conn, "doctor_percent_rules", "year INTEGER DEFAULT 2026", "year")
        if "month" not in cols:
            _add_column_if_missing(conn, "doctor_percent_rules", "month INTEGER DEFAULT 1", "month")
        if "source_key" not in cols:
            _add_column_if_missing(conn, "doctor_percent_rules", "source_key TEXT DEFAULT 'amb_exec'", "source_key")
        if "doctor_name" not in cols:
            _add_column_if_missing(conn, "doctor_percent_rules", "doctor_name TEXT", "doctor_name")
        if "percent" not in cols:
            _add_column_if_missing(conn, "doctor_percent_rules", "percent REAL DEFAULT 0", "percent")

    # =========================================================
    # POLIKLINIKA(OPD) HISOBLANGAN SUMMALAR
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS poliklinika_add_amount (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      target_module TEXT NOT NULL,         -- 'ambulator' / 'statsionar'
      department TEXT NOT NULL,            -- 'Poliklinika(OPD)' / 'MARKAZ'
      amount REAL NOT NULL DEFAULT 0,
      UNIQUE(year, month, target_module, department)
    )
    """)

    # =========================================================
    # JAMI PROTOKOL MANUAL
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jami_manual (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      department TEXT NOT NULL,
      avans REAL NOT NULL DEFAULT 0,
      rentabillik REAL NOT NULL DEFAULT 0,
      UNIQUE(year, month, department)
    )
    """)

    # =========================================================
    # FORCE RECREATE (faqat user xohlasa)
    # =========================================================
    if force_recreate:
        cur.execute("DELETE FROM ambulator_raw")
        cur.execute("DELETE FROM statsionar_patient")
        cur.execute("DELETE FROM statsionar_service_amount")
        cur.execute("DELETE FROM import_history")
        # Eslatma: mapping / qoidalar / manual ma'lumotlarni atay o'chirmaymiz

    conn.commit()
    conn.close()