# -*- coding: utf-8 -*-
import json
import re
from io import BytesIO

import pandas as pd
import streamlit as st

from database import get_conn
from utils import fmt_uzs

POLIKLINIKA_DEPT = "Poliklinika(OPD)"
CENTER_NAME = "MARKAZ"

SS_STD_DF = "pol_std_df"
SS_META = "pol_meta"
SS_CALC = "pol_calc"


# =========================================================
# NAME HELPERS
# =========================================================
def _normalize_text(s: str) -> str:
    s = str(s or "").strip()
    s = s.replace("’", "'").replace("`", "'").replace("ʻ", "'")
    s = s.replace("O‘", "O").replace("G‘", "G").replace("o‘", "o").replace("g‘", "g")
    s = s.replace("O'", "O").replace("G'", "G").replace("o'", "o").replace("g'", "g")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_name_token(s: str) -> str:
    s = _normalize_text(s).lower()
    s = re.sub(r"[^a-zа-яёқғҳў]", "", s, flags=re.IGNORECASE)
    return s.strip()


def _extract_name_parts(name: str) -> dict:
    raw = str(name or "").strip()
    if raw == "" or raw.lower() in {"nan", "none"}:
        return {
            "raw": raw,
            "kind": "other",
            "surname": "",
            "surname4": "",
            "i1": "",
            "i2": "",
            "display": "",
        }

    norm = _normalize_text(raw)

    # B.J.Akbarxonov / B J Akbarxonov
    m = re.match(
        r"^\s*([A-Za-zА-Яа-яЁёҚқҒғҲҳЎў])\.?\s*([A-Za-zА-Яа-яЁёҚқҒғҲҳЎў])\.?\s*([A-Za-zА-Яа-яЁёҚқҒғҲҳЎў\-]+)\s*$",
        norm
    )
    if m:
        i1 = _clean_name_token(m.group(1))[:1]
        i2 = _clean_name_token(m.group(2))[:1]
        surname = _clean_name_token(m.group(3))
        return {
            "raw": raw,
            "kind": "abbr",
            "surname": surname,
            "surname4": surname[:4],
            "i1": i1,
            "i2": i2,
            "display": raw,
        }

    parts = [p for p in norm.split() if p.strip()]
    parts_clean = [_clean_name_token(p) for p in parts if _clean_name_token(p)]

    # Akbarxonov Bunyod Jamoliddin ...
    if len(parts_clean) >= 3:
        surname = parts_clean[0]
        i1 = parts_clean[1][:1] if parts_clean[1] else ""
        i2 = parts_clean[2][:1] if parts_clean[2] else ""
        return {
            "raw": raw,
            "kind": "full",
            "surname": surname,
            "surname4": surname[:4],
            "i1": i1,
            "i2": i2,
            "display": raw,
        }

    if len(parts_clean) == 2:
        surname = parts_clean[0]
        i1 = parts_clean[1][:1] if parts_clean[1] else ""
        return {
            "raw": raw,
            "kind": "full",
            "surname": surname,
            "surname4": surname[:4],
            "i1": i1,
            "i2": "",
            "display": raw,
        }

    if len(parts_clean) == 1:
        surname = parts_clean[0]
        return {
            "raw": raw,
            "kind": "other",
            "surname": surname,
            "surname4": surname[:4],
            "i1": "",
            "i2": "",
            "display": raw,
        }

    return {
        "raw": raw,
        "kind": "other",
        "surname": "",
        "surname4": "",
        "i1": "",
        "i2": "",
        "display": raw,
    }


def _is_same_doctor(a: dict, b: dict) -> bool:
    if not a["surname"] or not b["surname"]:
        return False
    if not a["surname4"] or not b["surname4"]:
        return False
    if a["surname4"] != b["surname4"]:
        return False

    # familiya boshi mos bo‘lishi kerak
    if not (a["surname"].startswith(b["surname4"]) or b["surname"].startswith(a["surname4"])):
        return False

    # bosh harflar mosligi
    if a["i1"] and b["i1"] and a["i1"] != b["i1"]:
        return False
    if a["i2"] and b["i2"] and a["i2"] != b["i2"]:
        return False

    return True


def _choose_best_display(names: list[str]) -> str:
    cleaned = [str(x).strip() for x in names if str(x).strip()]
    if not cleaned:
        return ""
    # eng uzunini afzal ko'ramiz
    return sorted(cleaned, key=lambda x: (-len(x), x))[0]


def _build_doctor_registry(df_all: pd.DataFrame) -> dict:
    """
    raw_name -> {"doctor_key": ..., "doctor_display": ...}
    Agar to'liq ism topilsa, qisqartirilgan ham shu to'liq ismga ulanadi.
    Topilmasa qisqartirilgan o'z holicha qoladi.
    """
    names = []

    if "ijrochi_vrach" in df_all.columns:
        names.extend(df_all["ijrochi_vrach"].dropna().astype(str).tolist())
    if "yonaltirgan_vrach" in df_all.columns:
        names.extend(df_all["yonaltirgan_vrach"].dropna().astype(str).tolist())

    names = [str(x).strip() for x in names if str(x).strip() and str(x).strip().lower() not in {"nan", "none"}]
    unique_names = list(dict.fromkeys(names))

    parts_map = {nm: _extract_name_parts(nm) for nm in unique_names}

    full_names = [nm for nm in unique_names if parts_map[nm]["kind"] == "full"]
    abbr_names = [nm for nm in unique_names if parts_map[nm]["kind"] == "abbr"]
    other_names = [nm for nm in unique_names if parts_map[nm]["kind"] not in {"full", "abbr"}]

    registry = {}
    canonical_groups = []
    used_full = set()

    # to'liq ismli group
    for nm in full_names:
        if nm in used_full:
            continue

        base = parts_map[nm]
        group = [nm]
        used_full.add(nm)

        for nm2 in full_names:
            if nm2 in used_full:
                continue
            if _is_same_doctor(base, parts_map[nm2]):
                group.append(nm2)
                used_full.add(nm2)

        canonical_display = _choose_best_display(group)
        canonical_key = f"{base['surname4']}|{base['i1']}|{base['i2']}"
        canonical_groups.append((canonical_key, canonical_display, group, base))

    # qisqartirilganlarni full groupga bog'lash
    for nm in abbr_names:
        p = parts_map[nm]
        matched = None

        for canonical_key, canonical_display, _group, base in canonical_groups:
            if _is_same_doctor(p, base):
                matched = (canonical_key, canonical_display)
                break

        if matched is None:
            # moslik topilmadi -> o'zicha qoladi
            canonical_key = f"{p['surname4']}|{p['i1']}|{p['i2']}"
            canonical_display = nm
            canonical_groups.append((canonical_key, canonical_display, [nm], p))
            matched = (canonical_key, canonical_display)

        registry[nm] = {
            "doctor_key": matched[0],
            "doctor_display": matched[1],
        }

    # full larni yozamiz
    for canonical_key, canonical_display, group, _base in canonical_groups:
        for nm in group:
            registry[nm] = {
                "doctor_key": canonical_key,
                "doctor_display": canonical_display,
            }

    # boshqa nomlar
    for nm in other_names:
        p = parts_map[nm]
        key = f"{p['surname4']}|{p['i1']}|{p['i2']}|{_clean_name_token(nm)[:10]}"
        registry[nm] = {
            "doctor_key": key,
            "doctor_display": nm,
        }

    return registry


# =========================================================
# DB HELPERS
# =========================================================
def _ensure_tables():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS doctor_excel_mapping (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_key TEXT NOT NULL,
      field_key TEXT NOT NULL,
      excel_column TEXT NOT NULL,
      UNIQUE(source_key, field_key)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS doctor_percent_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      source_key TEXT NOT NULL,
      doctor_key TEXT NOT NULL,
      doctor_display TEXT NOT NULL,
      percent REAL NOT NULL DEFAULT 0,
      UNIQUE(year, month, source_key, doctor_key)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS doctor_manual_extra (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      doctor_key TEXT NOT NULL,
      doctor_display TEXT NOT NULL,
      avans REAL NOT NULL DEFAULT 0,
      rentabillik REAL NOT NULL DEFAULT 0,
      UNIQUE(year, month, doctor_key)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS poliklinika_selected_services (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      source_key TEXT NOT NULL,
      service_name TEXT NOT NULL,
      UNIQUE(year, month, source_key, service_name)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS poliklinika_add_amount (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      target_module TEXT NOT NULL,
      department TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      UNIQUE(year, month, target_module, department)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS poliklinika_cached_std (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      payload_json TEXT NOT NULL,
      UNIQUE(year, month)
    )
    """)

    conn.commit()
    conn.close()


def _save_mapping(source_key: str, field_key: str, excel_column: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO doctor_excel_mapping(source_key, field_key, excel_column)
        VALUES(?,?,?)
        ON CONFLICT(source_key, field_key) DO UPDATE SET excel_column=excluded.excel_column
        """,
        (source_key, field_key, str(excel_column))
    )
    conn.commit()
    conn.close()


def _load_mapping(source_key: str) -> dict:
    conn = get_conn()
    rows = conn.execute(
        "SELECT field_key, excel_column FROM doctor_excel_mapping WHERE source_key=?",
        (source_key,)
    ).fetchall()
    conn.close()
    return {r["field_key"]: r["excel_column"] for r in rows}


def _load_rules(year: int, month: int, source_key: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT doctor_key, doctor_display, percent
        FROM doctor_percent_rules
        WHERE year=? AND month=? AND source_key=?
        ORDER BY doctor_display
        """,
        conn,
        params=(year, month, source_key),
    )
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["doctor_key", "doctor_display", "percent"])

    df["doctor_key"] = df["doctor_key"].astype(str).str.strip()
    df["doctor_display"] = df["doctor_display"].astype(str).str.strip()
    df["percent"] = pd.to_numeric(df["percent"], errors="coerce").fillna(0.0)

    return (
        df.groupby("doctor_key", as_index=False)
        .agg({"doctor_display": lambda x: _choose_best_display(list(x)), "percent": "max"})
        .sort_values("doctor_display")
        .reset_index(drop=True)
    )


def _save_rules(year: int, month: int, source_key: str, df: pd.DataFrame):
    df2 = df.copy()
    df2["doctor_key"] = df2["doctor_key"].astype(str).str.strip()
    df2["doctor_display"] = df2["doctor_display"].astype(str).str.strip()
    df2["percent"] = pd.to_numeric(df2["percent"], errors="coerce").fillna(0.0)
    df2 = df2[(df2["doctor_key"] != "") & (df2["doctor_display"] != "")].copy()

    df2 = (
        df2.groupby("doctor_key", as_index=False)
        .agg({"doctor_display": lambda x: _choose_best_display(list(x)), "percent": "max"})
        .sort_values("doctor_display")
        .reset_index(drop=True)
    )

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM doctor_percent_rules WHERE year=? AND month=? AND source_key=?", (year, month, source_key))

    rows = [
        (year, month, source_key, str(r["doctor_key"]).strip(), str(r["doctor_display"]).strip(), float(r["percent"]))
        for _, r in df2.iterrows()
    ]
    if rows:
        cur.executemany(
            """
            INSERT INTO doctor_percent_rules
            (year, month, source_key, doctor_key, doctor_display, percent)
            VALUES(?,?,?,?,?,?)
            """,
            rows
        )

    conn.commit()
    conn.close()


def _load_manual_extra(year: int, month: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT doctor_key, doctor_display, avans, rentabillik
        FROM doctor_manual_extra
        WHERE year=? AND month=?
        ORDER BY doctor_display
        """,
        conn,
        params=(year, month),
    )
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["doctor_key", "doctor_display", "avans", "rentabillik"])

    df["doctor_key"] = df["doctor_key"].astype(str).str.strip()
    df["doctor_display"] = df["doctor_display"].astype(str).str.strip()
    df["avans"] = pd.to_numeric(df["avans"], errors="coerce").fillna(0.0)
    df["rentabillik"] = pd.to_numeric(df["rentabillik"], errors="coerce").fillna(0.0)

    return (
        df.groupby("doctor_key", as_index=False)
        .agg({
            "doctor_display": lambda x: _choose_best_display(list(x)),
            "avans": "sum",
            "rentabillik": "sum"
        })
        .sort_values("doctor_display")
        .reset_index(drop=True)
    )


def _save_manual_extra(year: int, month: int, df: pd.DataFrame):
    df2 = df.copy()
    df2["doctor_key"] = df2["doctor_key"].astype(str).str.strip()
    df2["doctor_display"] = df2["doctor_display"].astype(str).str.strip()
    df2["avans"] = pd.to_numeric(df2["avans"], errors="coerce").fillna(0.0)
    df2["rentabillik"] = pd.to_numeric(df2["rentabillik"], errors="coerce").fillna(0.0)
    df2 = df2[(df2["doctor_key"] != "") & (df2["doctor_display"] != "")].copy()

    df2 = (
        df2.groupby("doctor_key", as_index=False)
        .agg({
            "doctor_display": lambda x: _choose_best_display(list(x)),
            "avans": "sum",
            "rentabillik": "sum"
        })
        .sort_values("doctor_display")
        .reset_index(drop=True)
    )

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM doctor_manual_extra WHERE year=? AND month=?", (year, month))

    rows = [
        (
            year,
            month,
            str(r["doctor_key"]).strip(),
            str(r["doctor_display"]).strip(),
            float(r["avans"]),
            float(r["rentabillik"]),
        )
        for _, r in df2.iterrows()
    ]
    if rows:
        cur.executemany(
            """
            INSERT INTO doctor_manual_extra
            (year, month, doctor_key, doctor_display, avans, rentabillik)
            VALUES(?,?,?,?,?,?)
            """,
            rows
        )

    conn.commit()
    conn.close()


def _load_selected_services(year: int, month: int, source_key: str) -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT service_name
        FROM poliklinika_selected_services
        WHERE year=? AND month=? AND source_key=?
        ORDER BY service_name
        """,
        (year, month, source_key)
    ).fetchall()
    conn.close()
    return [r["service_name"] for r in rows]


def _save_selected_services(year: int, month: int, source_key: str, services: list[str]):
    services = [str(x).strip() for x in (services or []) if str(x).strip()]
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM poliklinika_selected_services WHERE year=? AND month=? AND source_key=?",
        (year, month, source_key)
    )
    if services:
        cur.executemany(
            "INSERT INTO poliklinika_selected_services(year, month, source_key, service_name) VALUES(?,?,?,?)",
            [(year, month, source_key, s) for s in services]
        )
    conn.commit()
    conn.close()


def _upsert_add_amount(year: int, month: int, target_module: str, department: str, amount: float):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE poliklinika_add_amount
        SET amount=?
        WHERE year=? AND month=? AND target_module=? AND department=?
        """,
        (float(amount), year, month, target_module, department)
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO poliklinika_add_amount(year, month, target_module, department, amount)
            VALUES(?,?,?,?,?)
            """,
            (year, month, target_module, department, float(amount))
        )
    conn.commit()
    conn.close()


def _save_cached_std(year: int, month: int, df: pd.DataFrame):
    payload_json = df.to_json(orient="records", force_ascii=False)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO poliklinika_cached_std(year, month, payload_json)
        VALUES(?,?,?)
        ON CONFLICT(year, month) DO UPDATE SET payload_json=excluded.payload_json
        """,
        (year, month, payload_json)
    )
    conn.commit()
    conn.close()


def _load_cached_std(year: int, month: int) -> pd.DataFrame:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT payload_json
        FROM poliklinika_cached_std
        WHERE year=? AND month=?
        """,
        (year, month)
    ).fetchone()
    conn.close()

    if not row:
        return pd.DataFrame()

    try:
        payload = row["payload_json"]
        data = json.loads(payload)
        df = pd.DataFrame(data)
        return _clean_std(df) if not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _clear_cached_std(year: int, month: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM poliklinika_cached_std WHERE year=? AND month=?", (year, month))
    conn.commit()
    conn.close()


# =========================================================
# EXCEL HELPERS
# =========================================================
def _find_header_row(df_raw: pd.DataFrame, must_contain: list[str], max_scan: int = 25) -> int:
    for i in range(min(max_scan, len(df_raw))):
        row = df_raw.iloc[i].astype(str).fillna("").str.strip().tolist()
        joined = " | ".join(row).lower()
        ok = True
        for key in must_contain:
            if key.lower() not in joined:
                ok = False
                break
        if ok:
            return i
    return 0


def _read_with_header(file, header_row: int) -> pd.DataFrame:
    try:
        file.seek(0)
    except Exception:
        pass

    df = pd.read_excel(file, header=header_row)
    df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed", regex=True)]
    df.columns = (
        pd.Index(df.columns)
        .astype(str)
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )
    return df


def _safe_defaults(options: list[str], defaults: list[str]) -> list[str]:
    opts = set(options or [])
    return [d for d in (defaults or []) if d in opts]


def _clean_std(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for c in ["guruh", "xizmat", "ijrochi_vrach", "yonaltirgan_vrach", "bolim", "manba"]:
        if c not in out.columns:
            out[c] = ""
        out[c] = out[c].astype(str).fillna("").str.strip()
        out.loc[out[c].isin(["nan", "None"]), c] = ""

    if "soni" not in out.columns:
        out["soni"] = 0.0
    if "summa" not in out.columns:
        out["summa"] = 0.0

    out["soni"] = pd.to_numeric(out["soni"], errors="coerce").fillna(0.0)
    out["summa"] = pd.to_numeric(out["summa"], errors="coerce").fillna(0.0)

    out["xizmat"] = out["xizmat"].astype(str).str.strip()
    out["ijrochi_vrach"] = out["ijrochi_vrach"].astype(str).str.strip()
    out["yonaltirgan_vrach"] = out["yonaltirgan_vrach"].astype(str).str.strip()
    out["bolim"] = out["bolim"].astype(str).str.strip()
    out["manba"] = out["manba"].astype(str).str.strip()

    out = out[out["xizmat"] != ""].copy()
    return out.reset_index(drop=True)


def _parse_ambulator(file, mapping: dict) -> pd.DataFrame:
    header_row = int(mapping.get("header_row", 0))
    df = _read_with_header(file, header_row)

    out = pd.DataFrame({
        "guruh": df[mapping["group"]],
        "xizmat": df[mapping["service"]],
        "soni": df[mapping["qty"]],
        "summa": df[mapping["amount"]],
        "ijrochi_vrach": df[mapping["exec_doc"]],
        "yonaltirgan_vrach": df[mapping["ref_doc"]],
        "bolim": "",
        "manba": "Ambulator",
    })
    return _clean_std(out)


def _parse_statsionar(file, mapping: dict) -> pd.DataFrame:
    header_row = int(mapping.get("header_row", 0))
    df = _read_with_header(file, header_row)

    out = pd.DataFrame({
        "guruh": df[mapping["service"]],
        "xizmat": df[mapping["service"]],
        "soni": df[mapping["qty"]],
        "summa": df[mapping["amount"]],
        "ijrochi_vrach": df[mapping["exec_doc"]],
        "yonaltirgan_vrach": "",
        "bolim": df[mapping["department"]] if "department" in mapping else "",
        "manba": "Statsionar",
    })
    return _clean_std(out)


# =========================================================
# PREVIEW TABLES
# =========================================================
def _preview_exec_amb(df: pd.DataFrame, registry: dict) -> pd.DataFrame:
    d = df[df["manba"] == "Ambulator"].copy()
    if d.empty:
        return pd.DataFrame(columns=["Xizmat", "doctor_key", "doctor_display", "Jami soni", "Jami summa"])

    d["raw_name"] = d["ijrochi_vrach"].astype(str).str.strip()
    d = d[d["raw_name"] != ""].copy()
    d["doctor_key"] = d["raw_name"].map(lambda x: registry.get(x, {}).get("doctor_key", ""))
    d["doctor_display"] = d["raw_name"].map(lambda x: registry.get(x, {}).get("doctor_display", x))
    d = d[d["doctor_key"] != ""].copy()

    g = d.groupby(["xizmat", "doctor_key"], as_index=False).agg(
        **{
            "Jami soni": ("soni", "sum"),
            "Jami summa": ("summa", "sum"),
            "doctor_display_raw": ("doctor_display", lambda x: _choose_best_display(list(x))),
        }
    )
    g = g.rename(columns={"xizmat": "Xizmat", "doctor_display_raw": "doctor_display"})
    return g.sort_values(["doctor_display", "Xizmat"]).reset_index(drop=True)


def _preview_ref_amb(df: pd.DataFrame, registry: dict) -> pd.DataFrame:
    d = df[df["manba"] == "Ambulator"].copy()
    if d.empty:
        return pd.DataFrame(columns=["Xizmat", "doctor_key", "doctor_display", "Jami soni", "Jami summa"])

    d["raw_name"] = d["yonaltirgan_vrach"].astype(str).str.strip()
    d = d[d["raw_name"] != ""].copy()
    d["doctor_key"] = d["raw_name"].map(lambda x: registry.get(x, {}).get("doctor_key", ""))
    d["doctor_display"] = d["raw_name"].map(lambda x: registry.get(x, {}).get("doctor_display", x))
    d = d[d["doctor_key"] != ""].copy()

    g = d.groupby(["xizmat", "doctor_key"], as_index=False).agg(
        **{
            "Jami soni": ("soni", "sum"),
            "Jami summa": ("summa", "sum"),
            "doctor_display_raw": ("doctor_display", lambda x: _choose_best_display(list(x))),
        }
    )
    g = g.rename(columns={"xizmat": "Xizmat", "doctor_display_raw": "doctor_display"})
    return g.sort_values(["doctor_display", "Xizmat"]).reset_index(drop=True)


def _preview_exec_stat(df: pd.DataFrame, registry: dict) -> pd.DataFrame:
    d = df[df["manba"] == "Statsionar"].copy()
    if d.empty:
        return pd.DataFrame(columns=["Xizmat", "doctor_key", "doctor_display", "Jami soni", "Jami summa"])

    d["raw_name"] = d["ijrochi_vrach"].astype(str).str.strip()
    d = d[d["raw_name"] != ""].copy()
    d["doctor_key"] = d["raw_name"].map(lambda x: registry.get(x, {}).get("doctor_key", ""))
    d["doctor_display"] = d["raw_name"].map(lambda x: registry.get(x, {}).get("doctor_display", x))
    d = d[d["doctor_key"] != ""].copy()

    g = d.groupby(["xizmat", "doctor_key"], as_index=False).agg(
        **{
            "Jami soni": ("soni", "sum"),
            "Jami summa": ("summa", "sum"),
            "doctor_display_raw": ("doctor_display", lambda x: _choose_best_display(list(x))),
        }
    )
    g = g.rename(columns={"xizmat": "Xizmat", "doctor_display_raw": "doctor_display"})
    return g.sort_values(["doctor_display", "Xizmat"]).reset_index(drop=True)


def _service_summary_from_preview(df_preview: pd.DataFrame, selected: list[str]) -> pd.DataFrame:
    if df_preview.empty:
        return pd.DataFrame(columns=["Tanlash", "Xizmat", "Jami soni", "Jami summa"])

    g = df_preview.groupby("Xizmat", as_index=False).agg(
        **{"Jami soni": ("Jami soni", "sum"), "Jami summa": ("Jami summa", "sum")}
    )
    g.insert(0, "Tanlash", g["Xizmat"].isin(selected))
    return g.sort_values("Xizmat").reset_index(drop=True)


def _detail_for_one_source(df_preview: pd.DataFrame, selected_services: list[str], rules_df: pd.DataFrame, selected_doctor_key: str = "(Hammasi)") -> pd.DataFrame:
    if df_preview.empty or not selected_services:
        return pd.DataFrame(columns=["Vrach", "Xizmat", "Jami soni", "Jami summa", "Foiz (%)", "Protokol summasi", "Markazda qoladi"])

    d = df_preview.copy()
    d = d[d["Xizmat"].isin(selected_services)].copy()

    if selected_doctor_key != "(Hammasi)":
        d = d[d["doctor_key"] == selected_doctor_key].copy()

    if d.empty:
        return pd.DataFrame(columns=["Vrach", "Xizmat", "Jami soni", "Jami summa", "Foiz (%)", "Protokol summasi", "Markazda qoladi"])

    if rules_df.empty:
        d["Foiz (%)"] = 0.0
    else:
        r = rules_df[["doctor_key", "percent"]].copy()
        r["doctor_key"] = r["doctor_key"].astype(str).str.strip()
        r["percent"] = pd.to_numeric(r["percent"], errors="coerce").fillna(0.0)
        d = d.merge(r, on="doctor_key", how="left")
        d["percent"] = d["percent"].fillna(0.0)
        d = d.rename(columns={"percent": "Foiz (%)"})

    d["Protokol summasi"] = d["Jami summa"] * d["Foiz (%)"] / 100.0
    d["Markazda qoladi"] = d["Jami summa"] - d["Protokol summasi"]

    out = d[[
        "doctor_display", "Xizmat", "Jami soni", "Jami summa",
        "Foiz (%)", "Protokol summasi", "Markazda qoladi"
    ]].rename(columns={"doctor_display": "Vrach"})
    return out.sort_values(["Vrach", "Xizmat"]).reset_index(drop=True)


def _render_detail_block(title: str, df_preview: pd.DataFrame, selected_services: list[str], existing_rules: pd.DataFrame, key_prefix: str):
    with st.expander(title):
        base = _detail_for_one_source(df_preview, selected_services, existing_rules, "(Hammasi)")
        if base.empty:
            st.info("Ma’lumot yo‘q.")
            return

        doctor_options_df = (
            df_preview[df_preview["Xizmat"].isin(selected_services)][["doctor_key", "doctor_display"]]
            .drop_duplicates()
            .sort_values("doctor_display")
        )
        labels = ["(Hammasi)"] + doctor_options_df["doctor_display"].tolist()
        keys = ["(Hammasi)"] + doctor_options_df["doctor_key"].tolist()
        label_to_key = dict(zip(labels, keys))

        selected_label = st.selectbox("Vrach tanlang", labels, key=f"{key_prefix}_doctor_filter")
        selected_key = label_to_key[selected_label]

        det = _detail_for_one_source(df_preview, selected_services, existing_rules, selected_key)
        st.dataframe(det, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                label="📥 Ko‘rinib turgan detalni Excelga yuklash",
                data=_df_to_excel_bytes(det, sheet_name="Detal"),
                file_name=f"{key_prefix}_detal.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{key_prefix}_download_filtered"
            )
        with c2:
            st.download_button(
                label="📥 Hamma vrachlar detali Excel",
                data=_df_to_excel_bytes(base, sheet_name="Detal"),
                file_name=f"{key_prefix}_detal_hammasi.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{key_prefix}_download_all"
            )


# =========================================================
# CALCULATION
# =========================================================
def _build_source_summary(preview_df: pd.DataFrame, selected_services: list[str], rules_df: pd.DataFrame, source_col_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if preview_df.empty or not selected_services:
        empty_sum = pd.DataFrame(columns=["doctor_key", "doctor_display", source_col_name, "Protokol summasi", "Markazda qoladi"])
        return empty_sum, pd.DataFrame()

    d = preview_df.copy()
    d = d[d["Xizmat"].isin(selected_services)].copy()
    if d.empty:
        empty_sum = pd.DataFrame(columns=["doctor_key", "doctor_display", source_col_name, "Protokol summasi", "Markazda qoladi"])
        return empty_sum, pd.DataFrame()

    if rules_df.empty:
        d["Foiz (%)"] = 0.0
    else:
        r = rules_df[["doctor_key", "percent"]].copy()
        r["doctor_key"] = r["doctor_key"].astype(str).str.strip()
        r["percent"] = pd.to_numeric(r["percent"], errors="coerce").fillna(0.0)
        d = d.merge(r, on="doctor_key", how="left")
        d["percent"] = d["percent"].fillna(0.0)
        d = d.rename(columns={"percent": "Foiz (%)"})

    d["Protokol summasi"] = d["Jami summa"] * d["Foiz (%)"] / 100.0
    d["Markazda qoladi"] = d["Jami summa"] - d["Protokol summasi"]

    detail = d.copy()

    summ = (
        d.groupby("doctor_key", as_index=False)
        .agg(
            doctor_display=("doctor_display", lambda x: _choose_best_display(list(x))),
            xizmat_sum=("Jami summa", "sum"),
            prot_sum=("Protokol summasi", "sum"),
            markaz_sum=("Markazda qoladi", "sum"),
        )
    )
    summ = summ.rename(columns={"xizmat_sum": source_col_name, "prot_sum": "Protokol summasi", "markaz_sum": "Markazda qoladi"})
    return summ, detail


def _merge_three_sources(sum_amb_exec: pd.DataFrame, sum_amb_ref: pd.DataFrame, sum_stat_exec: pd.DataFrame) -> pd.DataFrame:
    keys_frames = []
    for df in [sum_amb_exec, sum_amb_ref, sum_stat_exec]:
        if df is not None and not df.empty:
            keys_frames.append(df[["doctor_key", "doctor_display"]])

    if not keys_frames:
        return pd.DataFrame(columns=[
            "doctor_key", "doctor_display",
            "Ambulator ijrochi", "Ambulator yo‘naltirgan", "Statsionar ijrochi",
            "Jami xizmat summasi", "Jami protokol summasi", "Markazda qoladi"
        ])

    base = pd.concat(keys_frames, ignore_index=True)
    base = (
        base.groupby("doctor_key", as_index=False)
        .agg(doctor_display=("doctor_display", lambda x: _choose_best_display(list(x))))
    )

    for col in ["Ambulator ijrochi", "Ambulator yo‘naltirgan", "Statsionar ijrochi"]:
        base[col] = 0.0
    base["Jami protokol summasi"] = 0.0
    base["Markazda qoladi"] = 0.0

    for df, col in [
        (sum_amb_exec, "Ambulator ijrochi"),
        (sum_amb_ref, "Ambulator yo‘naltirgan"),
        (sum_stat_exec, "Statsionar ijrochi"),
    ]:
        if df is not None and not df.empty:
            temp = df[["doctor_key", col, "Protokol summasi", "Markazda qoladi"]].copy()
            temp = temp.groupby("doctor_key", as_index=False).agg({
                col: "sum",
                "Protokol summasi": "sum",
                "Markazda qoladi": "sum"
            })

            temp = temp.rename(columns={"Protokol summasi": "Jami protokol summasi"})
            base = base.merge(temp, on="doctor_key", how="left", suffixes=("", "_new"))

            if f"{col}_new" in base.columns:
                base[col] = base[f"{col}_new"].fillna(base[col])
                base = base.drop(columns=[f"{col}_new"])

            if "Jami protokol summasi_new" in base.columns:
                base["Jami protokol summasi"] = base["Jami protokol summasi"].fillna(0.0) + base["Jami protokol summasi_new"].fillna(0.0)
                base = base.drop(columns=["Jami protokol summasi_new"])

            if "Markazda qoladi_new" in base.columns:
                base["Markazda qoladi"] = base["Markazda qoladi"].fillna(0.0) + base["Markazda qoladi_new"].fillna(0.0)
                base = base.drop(columns=["Markazda qoladi_new"])

    for col in ["Ambulator ijrochi", "Ambulator yo‘naltirgan", "Statsionar ijrochi", "Jami protokol summasi", "Markazda qoladi"]:
        base[col] = pd.to_numeric(base[col], errors="coerce").fillna(0.0)

    base["Jami xizmat summasi"] = base["Ambulator ijrochi"] + base["Ambulator yo‘naltirgan"] + base["Statsionar ijrochi"]

    return base.sort_values("doctor_display").reset_index(drop=True)


def _apply_manual_extra(summary_df: pd.DataFrame, manual_df: pd.DataFrame) -> pd.DataFrame:
    out = summary_df.copy()
    if out.empty:
        out["Avans"] = pd.Series(dtype=float)
        out["Rentabillik"] = pd.Series(dtype=float)
        out["Yakuniy protokol"] = pd.Series(dtype=float)
        return out

    if manual_df is None or manual_df.empty:
        out["Avans"] = 0.0
        out["Rentabillik"] = 0.0
    else:
        m = manual_df[["doctor_key", "avans", "rentabillik"]].copy()
        m["doctor_key"] = m["doctor_key"].astype(str).str.strip()
        m["avans"] = pd.to_numeric(m["avans"], errors="coerce").fillna(0.0)
        m["rentabillik"] = pd.to_numeric(m["rentabillik"], errors="coerce").fillna(0.0)
        out = out.merge(m, on="doctor_key", how="left")
        out["avans"] = out["avans"].fillna(0.0)
        out["rentabillik"] = out["rentabillik"].fillna(0.0)
        out = out.rename(columns={"avans": "Avans", "rentabillik": "Rentabillik"})

    out["Yakuniy protokol"] = out["Jami protokol summasi"] - out["Avans"] + out["Rentabillik"]
    return out


def _manual_editor_from_summary(summary_df: pd.DataFrame, manual_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame(columns=["doctor_key", "doctor_display", "Jami xizmat summasi", "avans", "rentabillik"])

    out = summary_df[[
        "doctor_key", "doctor_display",
        "Ambulator ijrochi", "Ambulator yo‘naltirgan", "Statsionar ijrochi",
        "Jami xizmat summasi", "Jami protokol summasi", "Markazda qoladi"
    ]].copy()

    if manual_df is not None and not manual_df.empty:
        out = out.merge(manual_df[["doctor_key", "avans", "rentabillik"]], on="doctor_key", how="left")
    else:
        out["avans"] = 0.0
        out["rentabillik"] = 0.0

    out["avans"] = pd.to_numeric(out["avans"], errors="coerce").fillna(0.0)
    out["rentabillik"] = pd.to_numeric(out["rentabillik"], errors="coerce").fillna(0.0)
    return out


def _df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


# =========================================================
# UI
# =========================================================
def render_poliklinika_doctor(selected_year: int, selected_month_name: str, uz_months: list[str]):
    _ensure_tables()
    month = uz_months.index(selected_month_name) + 1
    meta_now = {"year": selected_year, "month": month}

    st.header("👨‍⚕️ Poliklinika(OPD) — Umumiy vrach protokoli")
    st.caption("Mapping → Excel yuklash → Xizmat tanlash → 3 ta alohida foiz → Vrach bo‘yicha avans/rentabillik → Hisoblash")

    if SS_STD_DF not in st.session_state or SS_META not in st.session_state or st.session_state.get(SS_META) != meta_now:
        cached_df = _load_cached_std(selected_year, month)
        if not cached_df.empty:
            st.session_state[SS_STD_DF] = cached_df
            st.session_state[SS_META] = meta_now

    tabs = st.tabs(["1) Mapping", "2) Excel yuklash", "3) Xizmat tanlash", "4) Vrach foizlari", "5) Hisoblash + Excel"])

    # ---------- 1) Mapping ----------
    with tabs[0]:
        st.subheader("Mapping")

        c1, c2 = st.columns(2)

        with c1:
            st.markdown("### Ambulator mapping")
            f_map_amb = st.file_uploader("Ambulator excel (mapping uchun)", type=["xlsx"], key="map_amb")
            if f_map_amb is not None:
                try:
                    raw = pd.read_excel(f_map_amb, header=None)
                    guessed = _find_header_row(raw, ["Группа", "Медицин"])
                    header_row = st.number_input(
                        "Ambulator header qatori (0 dan boshlanadi)",
                        min_value=0,
                        max_value=min(50, len(raw) - 1),
                        value=int(guessed),
                        step=1,
                        key="amb_header_row"
                    )
                    df_map = _read_with_header(f_map_amb, int(header_row))
                    cols = df_map.columns.tolist()

                    m = _load_mapping("amb")
                    default_map = lambda key: m.get(key) if m.get(key) in cols else cols[0]

                    group_col = st.selectbox("Guruh ustuni", cols, index=cols.index(default_map("group")))
                    service_col = st.selectbox("Xizmat ustuni", cols, index=cols.index(default_map("service")))
                    qty_col = st.selectbox("Soni ustuni", cols, index=cols.index(default_map("qty")))
                    amount_col = st.selectbox("Summa ustuni", cols, index=cols.index(default_map("amount")))
                    exec_col = st.selectbox("Ijrochi vrach ustuni", cols, index=cols.index(default_map("exec_doc")))
                    ref_col = st.selectbox("Yo‘naltirgan vrach ustuni", cols, index=cols.index(default_map("ref_doc")))

                    if st.button("💾 Ambulator mappingni saqlash", key="save_map_amb"):
                        _save_mapping("amb", "header_row", str(int(header_row)))
                        _save_mapping("amb", "group", group_col)
                        _save_mapping("amb", "service", service_col)
                        _save_mapping("amb", "qty", qty_col)
                        _save_mapping("amb", "amount", amount_col)
                        _save_mapping("amb", "exec_doc", exec_col)
                        _save_mapping("amb", "ref_doc", ref_col)
                        st.success("Ambulator mapping saqlandi ✅")
                except Exception as e:
                    st.error(str(e))

        with c2:
            st.markdown("### Statsionar mapping")
            f_map_stat = st.file_uploader("Statsionar excel (mapping uchun)", type=["xlsx"], key="map_stat")
            if f_map_stat is not None:
                try:
                    raw = pd.read_excel(f_map_stat, header=None)
                    guessed = _find_header_row(raw, ["Наименование услуги"])
                    header_row = st.number_input(
                        "Statsionar header qatori (0 dan boshlanadi)",
                        min_value=0,
                        max_value=min(50, len(raw) - 1),
                        value=int(guessed),
                        step=1,
                        key="stat_header_row"
                    )
                    df_map = _read_with_header(f_map_stat, int(header_row))
                    cols = df_map.columns.tolist()

                    m = _load_mapping("stat")
                    default_map = lambda key: m.get(key) if m.get(key) in cols else cols[0]

                    dept_col = st.selectbox("Bo‘lim ustuni", cols, index=cols.index(default_map("department")))
                    service_col = st.selectbox("Xizmat ustuni", cols, index=cols.index(default_map("service")))
                    qty_col = st.selectbox("Soni ustuni", cols, index=cols.index(default_map("qty")))
                    amount_col = st.selectbox("Summa ustuni", cols, index=cols.index(default_map("amount")))
                    exec_col = st.selectbox("Ijrochi vrach ustuni", cols, index=cols.index(default_map("exec_doc")))

                    if st.button("💾 Statsionar mappingni saqlash", key="save_map_stat"):
                        _save_mapping("stat", "header_row", str(int(header_row)))
                        _save_mapping("stat", "department", dept_col)
                        _save_mapping("stat", "service", service_col)
                        _save_mapping("stat", "qty", qty_col)
                        _save_mapping("stat", "amount", amount_col)
                        _save_mapping("stat", "exec_doc", exec_col)
                        st.success("Statsionar mapping saqlandi ✅")
                except Exception as e:
                    st.error(str(e))

    # ---------- 2) Excel yuklash ----------
    with tabs[1]:
        st.subheader("Excel yuklash")

        amb_map = _load_mapping("amb")
        stat_map = _load_mapping("stat")

        f_amb = st.file_uploader("Ambulator excel", type=["xlsx"], key="upload_amb")
        f_stat = st.file_uploader("Statsionar excel", type=["xlsx"], key="upload_stat")

        c_load1, c_load2 = st.columns([2, 1])

        with c_load1:
            if st.button("📥 Excelni o‘qish", key="read_pol_excel"):
                frames = []
                try:
                    if f_amb is not None:
                        if not {"header_row", "group", "service", "qty", "amount", "exec_doc", "ref_doc"}.issubset(set(amb_map.keys())):
                            st.error("Avval Ambulator mappingni to‘liq saqlang.")
                            return
                        frames.append(_parse_ambulator(f_amb, amb_map))

                    if f_stat is not None:
                        if not {"header_row", "department", "service", "qty", "amount", "exec_doc"}.issubset(set(stat_map.keys())):
                            st.error("Avval Statsionar mappingni to‘liq saqlang.")
                            return
                        frames.append(_parse_statsionar(f_stat, stat_map))

                    if not frames:
                        st.warning("Kamida bitta excel yuklang.")
                        return

                    df_all = pd.concat(frames, ignore_index=True)
                    df_all = _clean_std(df_all)

                    st.session_state[SS_STD_DF] = df_all
                    st.session_state[SS_META] = meta_now
                    _save_cached_std(selected_year, month, df_all)

                    st.success(f"O‘qildi va saqlandi ✅ Jami qatorlar: {len(df_all)}")

                    show = df_all.rename(columns={
                        "guruh": "Guruh",
                        "xizmat": "Xizmat",
                        "soni": "Soni",
                        "summa": "Summa",
                        "ijrochi_vrach": "Ijrochi vrach",
                        "yonaltirgan_vrach": "Yo‘naltirgan vrach",
                        "bolim": "Bo‘lim",
                        "manba": "Manba",
                    }).copy()
                    show["Summa"] = show["Summa"].apply(lambda x: fmt_uzs(float(x)))
                    st.dataframe(show.head(50), use_container_width=True)

                except Exception as e:
                    st.error(str(e))
                    return

        with c_load2:
            if st.button("🗑 Keshni tozalash", key="clear_pol_cache"):
                _clear_cached_std(selected_year, month)
                st.session_state.pop(SS_STD_DF, None)
                st.session_state.pop(SS_META, None)
                st.session_state.pop(SS_CALC, None)
                st.success("Saqlangan excel kesh tozalandi ✅")

        cached_now = _load_cached_std(selected_year, month)
        if not cached_now.empty:
            st.info(f"Saqlangan excel topildi: {len(cached_now)} qator. Qayta yuklamasdan ishlatishingiz mumkin.")

    if SS_STD_DF not in st.session_state or SS_META not in st.session_state:
        return

    if st.session_state[SS_META] != meta_now:
        st.warning("Yil/Oy o‘zgargan. Excelni qayta o‘qing.")
        return

    df_all = st.session_state[SS_STD_DF]

    doctor_registry = _build_doctor_registry(df_all)

    amb_exec_preview = _preview_exec_amb(df_all, doctor_registry)
    amb_ref_preview = _preview_ref_amb(df_all, doctor_registry)
    stat_exec_preview = _preview_exec_stat(df_all, doctor_registry)

    # ---------- 3) Xizmat tanlash ----------
    with tabs[2]:
        st.subheader("Xizmat tanlash")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Ambulator — Ijrochi vrachlar kesimi")
            prev_sel = _load_selected_services(selected_year, month, "amb_exec")
            options = amb_exec_preview["Xizmat"].dropna().astype(str).unique().tolist() if not amb_exec_preview.empty else []
            prev_sel = _safe_defaults(options, prev_sel)
            svc_df = _service_summary_from_preview(amb_exec_preview, prev_sel)

            edited = st.data_editor(
                svc_df,
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "Tanlash": st.column_config.CheckboxColumn("Tanlash"),
                    "Xizmat": st.column_config.TextColumn("Xizmat", disabled=True),
                    "Jami soni": st.column_config.NumberColumn("Jami soni", disabled=True),
                    "Jami summa": st.column_config.NumberColumn("Jami summa", disabled=True),
                },
                key="svc_amb_exec"
            )

            if st.button("💾 Ambulator ijrochi xizmatlarini saqlash", key="save_svc_amb_exec"):
                selected = edited.loc[edited["Tanlash"] == True, "Xizmat"].astype(str).tolist()
                _save_selected_services(selected_year, month, "amb_exec", selected)
                st.success("Saqlandi ✅")

        with col2:
            st.markdown("### Ambulator — Yo‘naltirgan vrachlar kesimi")
            prev_sel = _load_selected_services(selected_year, month, "amb_ref")
            options = amb_ref_preview["Xizmat"].dropna().astype(str).unique().tolist() if not amb_ref_preview.empty else []
            prev_sel = _safe_defaults(options, prev_sel)
            svc_df = _service_summary_from_preview(amb_ref_preview, prev_sel)

            edited = st.data_editor(
                svc_df,
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "Tanlash": st.column_config.CheckboxColumn("Tanlash"),
                    "Xizmat": st.column_config.TextColumn("Xizmat", disabled=True),
                    "Jami soni": st.column_config.NumberColumn("Jami soni", disabled=True),
                    "Jami summa": st.column_config.NumberColumn("Jami summa", disabled=True),
                },
                key="svc_amb_ref"
            )

            if st.button("💾 Ambulator yo‘naltirgan xizmatlarini saqlash", key="save_svc_amb_ref"):
                selected = edited.loc[edited["Tanlash"] == True, "Xizmat"].astype(str).tolist()
                _save_selected_services(selected_year, month, "amb_ref", selected)
                st.success("Saqlandi ✅")

        st.divider()

        st.markdown("### Statsionar — Ijrochi vrachlar kesimi")
        prev_sel = _load_selected_services(selected_year, month, "stat_exec")
        options = stat_exec_preview["Xizmat"].dropna().astype(str).unique().tolist() if not stat_exec_preview.empty else []
        prev_sel = _safe_defaults(options, prev_sel)
        svc_df = _service_summary_from_preview(stat_exec_preview, prev_sel)

        edited = st.data_editor(
            svc_df,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "Tanlash": st.column_config.CheckboxColumn("Tanlash"),
                "Xizmat": st.column_config.TextColumn("Xizmat", disabled=True),
                "Jami soni": st.column_config.NumberColumn("Jami soni", disabled=True),
                "Jami summa": st.column_config.NumberColumn("Jami summa", disabled=True),
            },
            key="svc_stat_exec"
        )

        if st.button("💾 Statsionar ijrochi xizmatlarini saqlash", key="save_svc_stat_exec"):
            selected = edited.loc[edited["Tanlash"] == True, "Xizmat"].astype(str).tolist()
            _save_selected_services(selected_year, month, "stat_exec", selected)
            st.success("Saqlandi ✅")

    # ---------- 4) Vrach foizlari ----------
    with tabs[3]:
        st.subheader("Vrach foizlari")

        sel_amb_exec = _load_selected_services(selected_year, month, "amb_exec")
        sel_amb_ref = _load_selected_services(selected_year, month, "amb_ref")
        sel_stat_exec = _load_selected_services(selected_year, month, "stat_exec")

        c1, c2 = st.columns(2)

        with c1:
            st.markdown("### Ambulator — Ijrochi vrachlar")
            existing = _load_rules(selected_year, month, "amb_exec")

            base = amb_exec_preview[amb_exec_preview["Xizmat"].isin(sel_amb_exec)].copy()
            if not base.empty:
                base = (
                    base.groupby("doctor_key", as_index=False)
                    .agg({
                        "doctor_display": lambda x: _choose_best_display(list(x)),
                        "Jami soni": "sum",
                        "Jami summa": "sum"
                    })
                )
            else:
                base = pd.DataFrame(columns=["doctor_key", "doctor_display", "Jami soni", "Jami summa"])

            base = base.merge(existing[["doctor_key", "percent"]], on="doctor_key", how="left")
            base["percent"] = pd.to_numeric(base["percent"], errors="coerce").fillna(0.0)

            edited = st.data_editor(
                base.rename(columns={"doctor_display": "Vrach", "percent": "Foiz (%)"}),
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "doctor_key": None,
                    "Vrach": st.column_config.TextColumn("Vrach", disabled=True),
                    "Jami soni": st.column_config.NumberColumn("Jami soni", disabled=True),
                    "Jami summa": st.column_config.NumberColumn("Jami summa", disabled=True),
                    "Foiz (%)": st.column_config.NumberColumn("Foiz (%)", min_value=0.0, max_value=100.0, step=1.0),
                },
                key="rules_amb_exec"
            )

            if st.button("💾 Ambulator ijrochi foizlarini saqlash", key="save_rules_amb_exec"):
                save_df = edited.rename(columns={"Vrach": "doctor_display", "Foiz (%)": "percent"})[
                    ["doctor_key", "doctor_display", "percent"]
                ]
                _save_rules(selected_year, month, "amb_exec", save_df)
                st.success("Saqlandi ✅")
                st.rerun()

            _render_detail_block("🔎 Xizmatlar kesimida detal", amb_exec_preview, sel_amb_exec, existing, "amb_exec")

        with c2:
            st.markdown("### Ambulator — Yo‘naltirgan vrachlar")
            existing = _load_rules(selected_year, month, "amb_ref")

            base = amb_ref_preview[amb_ref_preview["Xizmat"].isin(sel_amb_ref)].copy()
            if not base.empty:
                base = (
                    base.groupby("doctor_key", as_index=False)
                    .agg({
                        "doctor_display": lambda x: _choose_best_display(list(x)),
                        "Jami soni": "sum",
                        "Jami summa": "sum"
                    })
                )
            else:
                base = pd.DataFrame(columns=["doctor_key", "doctor_display", "Jami soni", "Jami summa"])

            base = base.merge(existing[["doctor_key", "percent"]], on="doctor_key", how="left")
            base["percent"] = pd.to_numeric(base["percent"], errors="coerce").fillna(0.0)

            edited = st.data_editor(
                base.rename(columns={"doctor_display": "Vrach", "percent": "Foiz (%)"}),
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "doctor_key": None,
                    "Vrach": st.column_config.TextColumn("Vrach", disabled=True),
                    "Jami soni": st.column_config.NumberColumn("Jami soni", disabled=True),
                    "Jami summa": st.column_config.NumberColumn("Jami summa", disabled=True),
                    "Foiz (%)": st.column_config.NumberColumn("Foiz (%)", min_value=0.0, max_value=100.0, step=1.0),
                },
                key="rules_amb_ref"
            )

            if st.button("💾 Ambulator yo‘naltirgan foizlarini saqlash", key="save_rules_amb_ref"):
                save_df = edited.rename(columns={"Vrach": "doctor_display", "Foiz (%)": "percent"})[
                    ["doctor_key", "doctor_display", "percent"]
                ]
                _save_rules(selected_year, month, "amb_ref", save_df)
                st.success("Saqlandi ✅")
                st.rerun()

            _render_detail_block("🔎 Xizmatlar kesimida detal", amb_ref_preview, sel_amb_ref, existing, "amb_ref")

        st.divider()

        st.markdown("### Statsionar — Ijrochi vrachlar")
        existing = _load_rules(selected_year, month, "stat_exec")

        base = stat_exec_preview[stat_exec_preview["Xizmat"].isin(sel_stat_exec)].copy()
        if not base.empty:
            base = (
                base.groupby("doctor_key", as_index=False)
                .agg({
                    "doctor_display": lambda x: _choose_best_display(list(x)),
                    "Jami soni": "sum",
                    "Jami summa": "sum"
                })
            )
        else:
            base = pd.DataFrame(columns=["doctor_key", "doctor_display", "Jami soni", "Jami summa"])

        base = base.merge(existing[["doctor_key", "percent"]], on="doctor_key", how="left")
        base["percent"] = pd.to_numeric(base["percent"], errors="coerce").fillna(0.0)

        edited = st.data_editor(
            base.rename(columns={"doctor_display": "Vrach", "percent": "Foiz (%)"}),
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "doctor_key": None,
                "Vrach": st.column_config.TextColumn("Vrach", disabled=True),
                "Jami soni": st.column_config.NumberColumn("Jami soni", disabled=True),
                "Jami summa": st.column_config.NumberColumn("Jami summa", disabled=True),
                "Foiz (%)": st.column_config.NumberColumn("Foiz (%)", min_value=0.0, max_value=100.0, step=1.0),
            },
            key="rules_stat_exec"
        )

        if st.button("💾 Statsionar ijrochi foizlarini saqlash", key="save_rules_stat_exec"):
            save_df = edited.rename(columns={"Vrach": "doctor_display", "Foiz (%)": "percent"})[
                ["doctor_key", "doctor_display", "percent"]
            ]
            _save_rules(selected_year, month, "stat_exec", save_df)
            st.success("Saqlandi ✅")
            st.rerun()

        _render_detail_block("🔎 Xizmatlar kesimida detal", stat_exec_preview, sel_stat_exec, existing, "stat_exec")

    # ---------- 5) Hisoblash + Excel ----------
    with tabs[4]:
        st.subheader("Hisoblash + Excel")

        sel_amb_exec = _load_selected_services(selected_year, month, "amb_exec")
        sel_amb_ref = _load_selected_services(selected_year, month, "amb_ref")
        sel_stat_exec = _load_selected_services(selected_year, month, "stat_exec")

        rules_amb_exec = _load_rules(selected_year, month, "amb_exec")
        rules_amb_ref = _load_rules(selected_year, month, "amb_ref")
        rules_stat_exec = _load_rules(selected_year, month, "stat_exec")

        manual_df = _load_manual_extra(selected_year, month)

        sum_ae, det_ae = _build_source_summary(amb_exec_preview, sel_amb_exec, rules_amb_exec, "Ambulator ijrochi")
        sum_ar, det_ar = _build_source_summary(amb_ref_preview, sel_amb_ref, rules_amb_ref, "Ambulator yo‘naltirgan")
        sum_se, det_se = _build_source_summary(stat_exec_preview, sel_stat_exec, rules_stat_exec, "Statsionar ijrochi")

        unified_summary = _merge_three_sources(sum_ae, sum_ar, sum_se)

        st.markdown("### Vrachlar bo‘yicha Avans va Rentabillik")
        if unified_summary.empty:
            st.info("Hisoblash uchun tanlangan xizmatlar bo‘yicha vrachlar topilmadi.")
        else:
            manual_editor = _manual_editor_from_summary(unified_summary, manual_df)

            edited_manual = st.data_editor(
                manual_editor.rename(columns={"doctor_display": "Vrach", "avans": "Avans", "rentabillik": "Rentabillik"}),
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "doctor_key": None,
                    "Vrach": st.column_config.TextColumn("Vrach", disabled=True),
                    "Ambulator ijrochi": st.column_config.NumberColumn("Ambulator ijrochi", disabled=True),
                    "Ambulator yo‘naltirgan": st.column_config.NumberColumn("Ambulator yo‘naltirgan", disabled=True),
                    "Statsionar ijrochi": st.column_config.NumberColumn("Statsionar ijrochi", disabled=True),
                    "Jami xizmat summasi": st.column_config.NumberColumn("Jami xizmat summasi", disabled=True),
                    "Jami protokol summasi": st.column_config.NumberColumn("Jami protokol summasi", disabled=True),
                    "Markazda qoladi": st.column_config.NumberColumn("Markazda qoladi", disabled=True),
                    "Avans": st.column_config.NumberColumn("Avans", min_value=0.0, step=1000.0),
                    "Rentabillik": st.column_config.NumberColumn("Rentabillik", step=1000.0),
                },
                key="unified_manual_editor"
            )

            if st.button("💾 Vrach bo‘yicha Avans/Rentabillikni saqlash", key="save_unified_manual"):
                save_df = edited_manual.rename(columns={"Vrach": "doctor_display", "Avans": "avans", "Rentabillik": "rentabillik"})[
                    ["doctor_key", "doctor_display", "avans", "rentabillik"]
                ]
                _save_manual_extra(selected_year, month, save_df)
                st.success("Saqlandi ✅")
                st.rerun()

        st.divider()

        if st.button("✅ Hisoblash", key="doctor_calc"):
            manual_now = _load_manual_extra(selected_year, month)

            sum_ae_now, det_ae_now = _build_source_summary(amb_exec_preview, sel_amb_exec, rules_amb_exec, "Ambulator ijrochi")
            sum_ar_now, det_ar_now = _build_source_summary(amb_ref_preview, sel_amb_ref, rules_amb_ref, "Ambulator yo‘naltirgan")
            sum_se_now, det_se_now = _build_source_summary(stat_exec_preview, sel_stat_exec, rules_stat_exec, "Statsionar ijrochi")

            unified_now = _merge_three_sources(sum_ae_now, sum_ar_now, sum_se_now)
            unified_now = _apply_manual_extra(unified_now, manual_now)

            amb_protocol_total = float(sum_ae_now["Protokol summasi"].sum()) + float(sum_ar_now["Protokol summasi"].sum())
            stat_protocol_total = float(sum_se_now["Protokol summasi"].sum())

            amb_center_total = float(sum_ae_now["Markazda qoladi"].sum()) + float(sum_ar_now["Markazda qoladi"].sum())
            stat_center_total = float(sum_se_now["Markazda qoladi"].sum())

            total_protocol = amb_protocol_total + stat_protocol_total
            total_final = float(unified_now["Yakuniy protokol"].sum())

            if total_protocol > 0:
                amb_final_total = total_final * amb_protocol_total / total_protocol
                stat_final_total = total_final * stat_protocol_total / total_protocol
            else:
                amb_final_total = 0.0
                stat_final_total = 0.0

            st.session_state[SS_CALC] = {
                "summary": unified_now,
                "det_amb_exec": det_ae_now,
                "det_amb_ref": det_ar_now,
                "det_stat_exec": det_se_now,
                "amb_final_total": amb_final_total,
                "stat_final_total": stat_final_total,
                "amb_center_total": amb_center_total,
                "stat_center_total": stat_center_total,
            }

            _upsert_add_amount(selected_year, month, "ambulator", POLIKLINIKA_DEPT, amb_final_total)
            _upsert_add_amount(selected_year, month, "ambulator", CENTER_NAME, amb_center_total)

            _upsert_add_amount(selected_year, month, "statsionar", POLIKLINIKA_DEPT, stat_final_total)
            _upsert_add_amount(selected_year, month, "statsionar", CENTER_NAME, stat_center_total)

            st.success("Hisoblandi ✅")

        if SS_CALC not in st.session_state:
            st.info("Natijani ko‘rish uchun 'Hisoblash' tugmasini bosing.")
            return

        calc = st.session_state[SS_CALC]

        st.markdown("### 1) Protokolga qo‘shiladigan summalar")
        info_df = pd.DataFrame([
            {"Qayerga qo‘shiladi": "Ambulator protokol → Poliklinika(OPD)", "Summa": fmt_uzs(calc["amb_final_total"])},
            {"Qayerga qo‘shiladi": "Ambulator protokol → MARKAZ", "Summa": fmt_uzs(calc["amb_center_total"])},
            {"Qayerga qo‘shiladi": "Statsionar protokol → Poliklinika(OPD)", "Summa": fmt_uzs(calc["stat_final_total"])},
            {"Qayerga qo‘shiladi": "Statsionar protokol → MARKAZ", "Summa": fmt_uzs(calc["stat_center_total"])},
        ])
        st.dataframe(info_df, use_container_width=True)

        st.markdown("### 2) Umumiy vrach jadvali")
        if calc["summary"].empty:
            st.info("Ma’lumot yo‘q.")
        else:
            show = calc["summary"].copy().rename(columns={"doctor_display": "Vrach"})
            for col in [
                "Ambulator ijrochi", "Ambulator yo‘naltirgan", "Statsionar ijrochi",
                "Jami xizmat summasi", "Jami protokol summasi", "Markazda qoladi",
                "Avans", "Rentabillik", "Yakuniy protokol"
            ]:
                if col in show.columns:
                    show[col] = show[col].apply(lambda x: fmt_uzs(float(x)))

            st.dataframe(
                show[[
                    "Vrach",
                    "Ambulator ijrochi",
                    "Ambulator yo‘naltirgan",
                    "Statsionar ijrochi",
                    "Jami xizmat summasi",
                    "Jami protokol summasi",
                    "Avans",
                    "Rentabillik",
                    "Yakuniy protokol",
                    "Markazda qoladi",
                ]],
                use_container_width=True
            )

            st.download_button(
                label="📥 Umumiy vrach protokolini Excelga yuklash",
                data=_df_to_excel_bytes(calc["summary"], sheet_name="Vrach_Protokol"),
                file_name=f"Poliklinika_Umumiy_Vrach_Protokol_{selected_year}_{month}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_unified_protocol"
            )

        st.markdown("### 3) Detal jadvallar")
        with st.expander("🔎 Ambulator ijrochi detal"):
            det = calc["det_amb_exec"].copy()
            if det.empty:
                st.info("Ma’lumot yo‘q.")
            else:
                det_show = det.copy()
                for col in ["Jami summa", "Protokol summasi", "Markazda qoladi"]:
                    det_show[col] = det_show[col].apply(lambda x: fmt_uzs(float(x)))
                st.dataframe(det_show, use_container_width=True)
                st.download_button(
                    "📥 Excel",
                    data=_df_to_excel_bytes(det, sheet_name="Amb_Exec_Detal"),
                    file_name="ambulator_ijrochi_detal.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_det_ae"
                )

        with st.expander("🔎 Ambulator yo‘naltirgan detal"):
            det = calc["det_amb_ref"].copy()
            if det.empty:
                st.info("Ma’lumot yo‘q.")
            else:
                det_show = det.copy()
                for col in ["Jami summa", "Protokol summasi", "Markazda qoladi"]:
                    det_show[col] = det_show[col].apply(lambda x: fmt_uzs(float(x)))
                st.dataframe(det_show, use_container_width=True)
                st.download_button(
                    "📥 Excel",
                    data=_df_to_excel_bytes(det, sheet_name="Amb_Ref_Detal"),
                    file_name="ambulator_yonaltirgan_detal.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_det_ar"
                )

        with st.expander("🔎 Statsionar ijrochi detal"):
            det = calc["det_stat_exec"].copy()
            if det.empty:
                st.info("Ma’lumot yo‘q.")
            else:
                det_show = det.copy()
                for col in ["Jami summa", "Protokol summasi", "Markazda qoladi"]:
                    det_show[col] = det_show[col].apply(lambda x: fmt_uzs(float(x)))
                st.dataframe(det_show, use_container_width=True)
                st.download_button(
                    "📥 Excel",
                    data=_df_to_excel_bytes(det, sheet_name="Stat_Exec_Detal"),
                    file_name="statsionar_ijrochi_detal.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_det_se"
                )

        st.markdown("### 4) Barchasini bir Excelga yuklash")
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            calc["summary"].to_excel(writer, index=False, sheet_name="Vrach_Protokol")
            calc["det_amb_exec"].to_excel(writer, index=False, sheet_name="Amb_Exec_Detal")
            calc["det_amb_ref"].to_excel(writer, index=False, sheet_name="Amb_Ref_Detal")
            calc["det_stat_exec"].to_excel(writer, index=False, sheet_name="Stat_Exec_Detal")

        st.download_button(
            label="Excel yuklab olish",
            data=output.getvalue(),
            file_name=f"Poliklinika_Vrach_Protokol_{selected_year}_{month}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )