# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from io import BytesIO

from database import get_conn
from utils import fmt_uzs

EXCLUDE_GROUPS = ["Dori-darmon", "Oziq-ovqat", "Koyka"]

MODULE_NAME = "statsionar"
CENTER_NAME = "MARKAZ"
SOURCE_ALIAS = "ASOSIY_BO'LIM"
POLIK_DEPT = "Poliklinika(OPD)"
TOL = 0.01

SS_KEY_PROTOCOL = "foiz_protocol_table"
SS_KEY_DETAIL = "foiz_detail_table"
SS_KEY_META = "foiz_meta"


def _prev_year_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _ensure_tables():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS foiz_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      module TEXT NOT NULL,
      payment_type TEXT NOT NULL,
      group_name TEXT NOT NULL,
      to_department TEXT NOT NULL,
      percent REAL NOT NULL,
      UNIQUE(module, payment_type, group_name, to_department)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS foiz_manual (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      report_type TEXT NOT NULL,
      department TEXT NOT NULL,
      avans REAL NOT NULL DEFAULT 0,
      rentabillik REAL NOT NULL DEFAULT 0,
      UNIQUE(year, month, report_type, department)
    );
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

    # Guruh bo'yicha dori
    cur.execute("""
    CREATE TABLE IF NOT EXISTS foiz_group_drug (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      module TEXT NOT NULL,
      payment_type TEXT NOT NULL,
      group_name TEXT NOT NULL,
      drug_amount REAL NOT NULL DEFAULT 0,
      UNIQUE(year, month, module, payment_type, group_name)
    )
    """)

    conn.commit()
    conn.close()


def _load_polik_additions(year: int, month: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT department, amount
        FROM poliklinika_add_amount
        WHERE year=? AND month=? AND target_module=?
        """,
        conn,
        params=(year, month, MODULE_NAME),
    )
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["department", "amount"])

    df["department"] = df["department"].astype(str).str.strip()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    return df


def _safe_upsert_manual(year: int, month: int, report_type: str, department: str, avans: float, rent: float):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE foiz_manual SET avans=?, rentabillik=? WHERE year=? AND month=? AND report_type=? AND department=?",
        (avans, rent, year, month, report_type, department)
    )
    if cur.rowcount == 0:
        cur.execute(
            "INSERT INTO foiz_manual(year, month, report_type, department, avans, rentabillik) VALUES(?,?,?,?,?,?)",
            (year, month, report_type, department, avans, rent)
        )
    conn.commit()
    conn.close()


def _list_departments(module: str) -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT department FROM department_whitelist WHERE module=? AND is_active=1 ORDER BY department",
        (module,)
    ).fetchall()
    conn.close()
    return [r["department"] for r in rows]


def _add_department(module: str, name: str):
    name = (name or "").strip()
    if not name:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE department_whitelist SET is_active=1 WHERE module=? AND department=?",
        (module, name)
    )
    if cur.rowcount == 0:
        cur.execute(
            "INSERT INTO department_whitelist(module, department, is_active) VALUES(?, ?, 1)",
            (module, name)
        )
    conn.commit()
    conn.close()


def _deactivate_department(module: str, name: str):
    conn = get_conn()
    conn.execute(
        "UPDATE department_whitelist SET is_active=0 WHERE module=? AND department=?",
        (module, name)
    )
    conn.commit()
    conn.close()


def _list_group_totals_by_source(year: int, month: int, payment_type_db: str | None) -> pd.DataFrame:
    conn = get_conn()
    where_pt = ""
    params = [year, month]
    if payment_type_db in ("order", "pullik"):
        where_pt = "AND s.payment_type=?"
        params.append(payment_type_db)

    q = f"""
    SELECT
        COALESCE(NULLIF(TRIM(s.department), ''), 'Bo''lim ko''rsatilmagan') AS source_department,
        COALESCE(m.main_group_name, 'Guruhlanmagan') AS group_name,
        SUM(s.amount) AS tushum
    FROM statsionar_service_amount s
    LEFT JOIN service_main_group m
        ON m.module='statsionar' AND m.service_name = s.service_name
    WHERE s.year=? AND s.month=?
      {where_pt}
      AND s.amount > 0
    GROUP BY source_department, group_name
    """
    df = pd.read_sql_query(q, conn, params=params)
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["source_department", "group_name", "tushum"])

    df = df[~df["group_name"].isin(EXCLUDE_GROUPS)].copy()
    df["source_department"] = df["source_department"].astype(str).str.strip()
    df["group_name"] = df["group_name"].astype(str).str.strip()
    df["tushum"] = pd.to_numeric(df["tushum"], errors="coerce").fillna(0.0)
    return df


def _list_groups_in_month(year: int, month: int, payment_type_db: str | None) -> pd.DataFrame:
    base = _list_group_totals_by_source(year, month, payment_type_db)
    if base.empty:
        return pd.DataFrame(columns=["group_name", "tushum"])
    return (
        base.groupby("group_name", as_index=False)["tushum"]
        .sum()
        .sort_values("tushum", ascending=False)
        .reset_index(drop=True)
    )


def _load_rules(module: str, payment_type_db: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT group_name, to_department, percent
        FROM foiz_rules
        WHERE module=? AND payment_type=?
        ORDER BY group_name, to_department
        """,
        conn,
        params=(module, payment_type_db),
    )
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["group_name", "to_department", "percent"])

    df["group_name"] = df["group_name"].astype(str).str.strip()
    df["to_department"] = df["to_department"].astype(str).str.strip()
    df["percent"] = pd.to_numeric(df["percent"], errors="coerce").fillna(0.0)
    return df


def _clean_rules(df: pd.DataFrame, allowed_targets: list[str]) -> pd.DataFrame:
    df2 = df.copy()
    df2["group_name"] = df2["group_name"].astype(str).str.strip()
    df2["to_department"] = df2["to_department"].astype(str).str.strip()
    df2["percent"] = pd.to_numeric(df2["percent"], errors="coerce").fillna(0.0)
    df2 = df2[(df2["group_name"] != "") & (df2["to_department"] != "") & (df2["percent"] > 0)].copy()
    if allowed_targets:
        df2 = df2[df2["to_department"].isin(allowed_targets)].copy()
    return df2


def _validate_rules(df_rules: pd.DataFrame) -> pd.DataFrame:
    if df_rules.empty:
        return pd.DataFrame(columns=["group_name", "sum_percent", "status"])
    s = df_rules.groupby("group_name", as_index=False)["percent"].sum().rename(columns={"percent": "sum_percent"})

    def status(x: float) -> str:
        if abs(x - 100.0) <= TOL:
            return "OK"
        if x > 100.0 + TOL:
            return "OVER"
        return "UNDER"

    s["status"] = s["sum_percent"].apply(status)
    return s.sort_values(["status", "sum_percent"], ascending=[True, False])


def _save_rules(module: str, payment_type_db: str, df: pd.DataFrame, allowed_targets: list[str]) -> tuple[bool, str]:
    df2 = _clean_rules(df, allowed_targets)
    v = _validate_rules(df2)
    if not v.empty and (v["status"] == "OVER").any():
        return False, "Saqlanmadi: ba’zi guruhlarda foiz 100% dan oshib ketgan (OVER)."

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM foiz_rules WHERE module=? AND payment_type=?", (module, payment_type_db))
    rows = [(module, payment_type_db, r["group_name"], r["to_department"], float(r["percent"])) for _, r in df2.iterrows()]
    if rows:
        cur.executemany(
            """
            INSERT INTO foiz_rules(module, payment_type, group_name, to_department, percent)
            VALUES(?,?,?,?,?)
            """,
            rows
        )
    conn.commit()
    conn.close()
    return True, "Saqlandi ✅"


def _load_group_drugs(year: int, month: int, payment_type_db: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT group_name, drug_amount
        FROM foiz_group_drug
        WHERE year=? AND month=? AND module=? AND payment_type=?
        ORDER BY group_name
        """,
        conn,
        params=(year, month, MODULE_NAME, payment_type_db),
    )
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["group_name", "drug_amount"])

    df["group_name"] = df["group_name"].astype(str).str.strip()
    df["drug_amount"] = pd.to_numeric(df["drug_amount"], errors="coerce").fillna(0.0)
    return df


def _save_group_drugs(year: int, month: int, payment_type_db: str, df: pd.DataFrame):
    df2 = df.copy()
    df2["group_name"] = df2["group_name"].astype(str).str.strip()
    df2["drug_amount"] = pd.to_numeric(df2["drug_amount"], errors="coerce").fillna(0.0)
    df2 = df2[df2["group_name"] != ""].copy()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM foiz_group_drug WHERE year=? AND month=? AND module=? AND payment_type=?",
        (year, month, MODULE_NAME, payment_type_db)
    )

    rows = [
        (year, month, MODULE_NAME, payment_type_db, r["group_name"], float(r["drug_amount"]))
        for _, r in df2.iterrows()
        if float(r["drug_amount"]) > 0
    ]
    if rows:
        cur.executemany(
            """
            INSERT INTO foiz_group_drug(year, month, module, payment_type, group_name, drug_amount)
            VALUES(?,?,?,?,?,?)
            """,
            rows
        )

    conn.commit()
    conn.close()


def _rules_for_protocol(rules: pd.DataFrame) -> pd.DataFrame:
    if rules.empty:
        return rules.copy()
    return rules[rules["group_name"] != POLIK_DEPT].copy()


def _effective_owner_department(source_department: str, to_department: str) -> str:
    if str(to_department).strip() == SOURCE_ALIAS:
        return str(source_department).strip()
    return str(to_department).strip()


def _determine_group_owner(source_department: str, group_name: str, rules: pd.DataFrame) -> str:
    if group_name == POLIK_DEPT:
        return POLIK_DEPT

    rr = rules[rules["group_name"] == group_name].copy() if not rules.empty else pd.DataFrame()
    if rr.empty:
        return CENTER_NAME

    rr["percent"] = pd.to_numeric(rr["percent"], errors="coerce").fillna(0.0)
    rr["effective_department"] = rr["to_department"].apply(lambda x: _effective_owner_department(source_department, x))
    rr = rr.sort_values(["percent", "effective_department"], ascending=[False, True]).reset_index(drop=True)
    return str(rr.iloc[0]["effective_department"]).strip()


def _build_group_editor_df(year: int, month: int, payment_type_db: str) -> pd.DataFrame:
    groups = _list_groups_in_month(year, month, payment_type_db)
    drugs = _load_group_drugs(year, month, payment_type_db)

    df = groups.merge(drugs, on="group_name", how="left")
    df["tushum"] = pd.to_numeric(df["tushum"], errors="coerce").fillna(0.0)
    df["drug_amount"] = pd.to_numeric(df["drug_amount"], errors="coerce").fillna(0.0)
    df["sof_tushum"] = (df["tushum"] - df["drug_amount"]).clip(lower=0.0)
    return df


def _apply_group_drug_to_source_totals(totals_by_source: pd.DataFrame, year: int, month: int, payment_type_db: str) -> pd.DataFrame:
    """
    Har bir group uchun kiritilgan dori summasini source bo'limlarga ulushiga qarab proporsional ayiramiz.
    """
    if totals_by_source.empty:
        return totals_by_source.copy()

    out = totals_by_source.copy()
    out["tushum"] = pd.to_numeric(out["tushum"], errors="coerce").fillna(0.0)

    drugs = _load_group_drugs(year, month, payment_type_db)
    if drugs.empty:
        return out

    group_total = out.groupby("group_name", as_index=False)["tushum"].sum().rename(columns={"tushum": "group_sum"})
    out = out.merge(group_total, on="group_name", how="left")
    out = out.merge(drugs, on="group_name", how="left")
    out["drug_amount"] = pd.to_numeric(out["drug_amount"], errors="coerce").fillna(0.0)
    out["group_sum"] = pd.to_numeric(out["group_sum"], errors="coerce").fillna(0.0)

    out["allocated_drug"] = 0.0
    mask = out["group_sum"] > 0
    out.loc[mask, "allocated_drug"] = out.loc[mask, "tushum"] / out.loc[mask, "group_sum"] * out.loc[mask, "drug_amount"]

    out["tushum"] = (out["tushum"] - out["allocated_drug"]).clip(lower=0.0)

    return out[["source_department", "group_name", "tushum"]].copy()


def _apply_rules(totals_by_source: pd.DataFrame, rules: pd.DataFrame, payment_type_db: str) -> pd.DataFrame:
    """
    Protokol uchun foiz taqsimoti.
    Poliklinika(OPD) guruhi BU YERDA hisoblanmaydi.
    totals_by_source bu yerga dori AYIRILGAN holatda keladi.
    """
    if totals_by_source.empty:
        return pd.DataFrame(columns=[
            "payment_type", "source_department", "group_name",
            "to_department", "percent", "group_total", "ulush"
        ])

    base = totals_by_source[totals_by_source["group_name"] != POLIK_DEPT].copy()
    if base.empty:
        return pd.DataFrame(columns=[
            "payment_type", "source_department", "group_name",
            "to_department", "percent", "group_total", "ulush"
        ])

    base = base.rename(columns={"tushum": "group_total"}).copy()
    base["group_total"] = pd.to_numeric(base["group_total"], errors="coerce").fillna(0.0)

    if rules.empty:
        out = base.copy()
        out["payment_type"] = payment_type_db
        out["to_department"] = CENTER_NAME
        out["percent"] = 100.0
        out["ulush"] = out["group_total"]
        return out[["payment_type", "source_department", "group_name", "to_department", "percent", "group_total", "ulush"]]

    merged = base.merge(rules, on="group_name", how="left")
    merged["percent"] = pd.to_numeric(merged["percent"], errors="coerce").fillna(0.0)
    merged["to_department"] = merged["to_department"].fillna("")

    alloc = merged[merged["percent"] > 0].copy()
    alloc["payment_type"] = payment_type_db
    alloc.loc[alloc["to_department"] == SOURCE_ALIAS, "to_department"] = alloc["source_department"]
    alloc["ulush"] = alloc["group_total"] * alloc["percent"] / 100.0

    pct_sum = (
        alloc.groupby(["source_department", "group_name"], as_index=False)["percent"]
        .sum()
        .rename(columns={"percent": "pct_sum"})
    )

    rest = base.merge(pct_sum, on=["source_department", "group_name"], how="left")
    rest["pct_sum"] = rest["pct_sum"].fillna(0.0)
    rest["pct_center"] = (100.0 - rest["pct_sum"]).clip(lower=0.0)
    rest["ulush"] = rest["group_total"] * rest["pct_center"] / 100.0
    rest = rest[rest["ulush"] > 0].copy()
    rest["payment_type"] = payment_type_db
    rest["to_department"] = CENTER_NAME
    rest["percent"] = rest["pct_center"]

    out = pd.concat(
        [
            alloc[["payment_type", "source_department", "group_name", "to_department", "percent", "group_total", "ulush"]],
            rest[["payment_type", "source_department", "group_name", "to_department", "percent", "group_total", "ulush"]],
        ],
        ignore_index=True
    ).sort_values(["group_name", "source_department", "to_department"])

    return out.reset_index(drop=True)


def _calc_detail(year: int, month: int, report_type: str) -> pd.DataFrame:
    if report_type == "order":
        totals = _list_group_totals_by_source(year, month, "order")
        totals = _apply_group_drug_to_source_totals(totals, year, month, "order")
        rules = _rules_for_protocol(_load_rules(MODULE_NAME, "order"))
        return _apply_rules(totals, rules, "order")

    if report_type == "pullik":
        totals = _list_group_totals_by_source(year, month, "pullik")
        totals = _apply_group_drug_to_source_totals(totals, year, month, "pullik")
        rules = _rules_for_protocol(_load_rules(MODULE_NAME, "pullik"))
        return _apply_rules(totals, rules, "pullik")

    totals_order = _list_group_totals_by_source(year, month, "order")
    totals_order = _apply_group_drug_to_source_totals(totals_order, year, month, "order")
    det1 = _apply_rules(totals_order, _rules_for_protocol(_load_rules(MODULE_NAME, "order")), "order")

    totals_pullik = _list_group_totals_by_source(year, month, "pullik")
    totals_pullik = _apply_group_drug_to_source_totals(totals_pullik, year, month, "pullik")
    det2 = _apply_rules(totals_pullik, _rules_for_protocol(_load_rules(MODULE_NAME, "pullik")), "pullik")

    if det1.empty and det2.empty:
        return pd.DataFrame()
    return pd.concat([det1, det2], ignore_index=True)


def _summary_i_by_department(detail: pd.DataFrame) -> pd.DataFrame:
    if detail.empty:
        return pd.DataFrame(columns=["department", "I_jami_foiz"])
    out = detail.groupby("to_department", as_index=False)["ulush"].sum().rename(
        columns={"to_department": "department", "ulush": "I_jami_foiz"}
    )
    return out


def _build_work_and_drug_summary(year: int, month: int, report_type: str) -> pd.DataFrame:
    """
    Jami qilgan ish o'zgarmaydi.
    Dori alohida ustunga yoziladi.
    Dori eng katta foiz olgan bo'limga biriktiriladi.
    """
    if report_type == "order":
        totals = _list_group_totals_by_source(year, month, "order")
        rules = _load_rules(MODULE_NAME, "order")
        drugs = _load_group_drugs(year, month, "order")
    elif report_type == "pullik":
        totals = _list_group_totals_by_source(year, month, "pullik")
        rules = _load_rules(MODULE_NAME, "pullik")
        drugs = _load_group_drugs(year, month, "pullik")
    else:
        totals = _list_group_totals_by_source(year, month, None)

        rules_order = _load_rules(MODULE_NAME, "order")
        rules_pullik = _load_rules(MODULE_NAME, "pullik")
        rules = pd.concat([rules_order, rules_pullik], ignore_index=True)
        if not rules.empty:
            rules = (
                rules.groupby(["group_name", "to_department"], as_index=False)
                .agg({"percent": "max"})
            )

        drug_order = _load_group_drugs(year, month, "order")
        drug_pullik = _load_group_drugs(year, month, "pullik")
        drugs = pd.concat([drug_order, drug_pullik], ignore_index=True)
        if not drugs.empty:
            drugs = drugs.groupby("group_name", as_index=False)["drug_amount"].sum()

    if totals.empty:
        return pd.DataFrame(columns=["department", "jami_qilgan_ish", "dori_darmon"])

    rows = []
    for _, r in totals.iterrows():
        source_department = str(r["source_department"]).strip()
        group_name = str(r["group_name"]).strip()
        group_total = float(r["tushum"])

        owner_department = _determine_group_owner(source_department, group_name, rules)

        rows.append({
            "department": owner_department,
            "jami_qilgan_ish": group_total,
            "dori_darmon": 0.0
        })

    work_df = pd.DataFrame(rows)
    work_df = work_df.groupby("department", as_index=False)[["jami_qilgan_ish", "dori_darmon"]].sum()

    if not drugs.empty:
        drug_rows = []
        drug_map = drugs.set_index("group_name")["drug_amount"].to_dict()

        unique_groups = totals[["source_department", "group_name"]].drop_duplicates()
        done_groups = set()

        for _, r in unique_groups.iterrows():
            source_department = str(r["source_department"]).strip()
            group_name = str(r["group_name"]).strip()

            if group_name in done_groups:
                continue
            done_groups.add(group_name)

            drug_amount = float(drug_map.get(group_name, 0.0))
            if drug_amount <= 0:
                continue

            owner_department = _determine_group_owner(source_department, group_name, rules)
            drug_rows.append({
                "department": owner_department,
                "jami_qilgan_ish": 0.0,
                "dori_darmon": drug_amount
            })

        if drug_rows:
            drug_df = pd.DataFrame(drug_rows).groupby("department", as_index=False)[["jami_qilgan_ish", "dori_darmon"]].sum()
            work_df = pd.concat([work_df, drug_df], ignore_index=True)
            work_df = work_df.groupby("department", as_index=False)[["jami_qilgan_ish", "dori_darmon"]].sum()

    return work_df


def _load_manual(year: int, month: int, report_type: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT department, avans, rentabillik FROM foiz_manual WHERE year=? AND month=? AND report_type=?",
        conn,
        params=(year, month, report_type),
    )
    conn.close()
    return df


def _save_manual(year: int, month: int, report_type: str, df: pd.DataFrame):
    df2 = df.copy()
    df2["department"] = df2["department"].astype(str).str.strip()
    df2["avans"] = pd.to_numeric(df2["avans"], errors="coerce").fillna(0.0)
    df2["rentabillik"] = pd.to_numeric(df2["rentabillik"], errors="coerce").fillna(0.0)

    for _, r in df2.iterrows():
        _safe_upsert_manual(year, month, report_type, r["department"], float(r["avans"]), float(r["rentabillik"]))


def _build_protocol_table(year: int, month: int, report_type: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    detail = _calc_detail(year, month, report_type)
    summ = _summary_i_by_department(detail)
    work = _build_work_and_drug_summary(year, month, report_type)

    add_df = _load_polik_additions(year, month)
    if not add_df.empty:
        for _, r in add_df.iterrows():
            dep = str(r["department"]).strip()
            val = float(r["amount"])
            if (summ["department"] == dep).any():
                summ.loc[summ["department"] == dep, "I_jami_foiz"] += val
            else:
                summ = pd.concat([summ, pd.DataFrame([{
                    "department": dep,
                    "I_jami_foiz": val
                }])], ignore_index=True)

    df = pd.merge(work, summ, on="department", how="outer")
    df["jami_qilgan_ish"] = pd.to_numeric(df["jami_qilgan_ish"], errors="coerce").fillna(0.0)
    df["dori_darmon"] = pd.to_numeric(df["dori_darmon"], errors="coerce").fillna(0.0)
    df["I_jami_foiz"] = pd.to_numeric(df["I_jami_foiz"], errors="coerce").fillna(0.0)

    manual = _load_manual(year, month, report_type)
    df = df.merge(manual, on="department", how="left")
    df["avans"] = df["avans"].fillna(0.0)
    df["rentabillik"] = df["rentabillik"].fillna(0.0)

    df["sof_ish"] = (df["jami_qilgan_ish"] - df["dori_darmon"]).clip(lower=0.0)

    center_i = float(df.loc[df["department"] == CENTER_NAME, "I_jami_foiz"].sum())
    rent_sum_non_center = float(df.loc[df["department"] != CENTER_NAME, "rentabillik"].sum())

    if rent_sum_non_center > center_i + 0.01:
        raise ValueError(
            f"Rentabillik yig‘indisi MARKAZ foydasidan oshib ketdi! "
            f"Rentabillik={rent_sum_non_center:,.2f}, MARKAZ foydasi={center_i:,.2f}"
        )

    if (df["department"] == CENTER_NAME).any():
        df.loc[df["department"] == CENTER_NAME, "rentabillik"] = -rent_sum_non_center

    df["G_ustama_qolgan"] = df["I_jami_foiz"] - df["avans"]
    df["E_protokol"] = df["G_ustama_qolgan"] + df["rentabillik"]
    df["markazda_qoladi"] = (df["sof_ish"] - df["E_protokol"]).clip(lower=0.0)

    py, pm = _prev_year_month(year, month)
    prev_manual = _load_manual(py, pm, report_type)
    prev_detail = _calc_detail(py, pm, report_type)
    prev_summ = _summary_i_by_department(prev_detail)

    prev = prev_summ.merge(prev_manual, on="department", how="left")
    prev["avans"] = prev["avans"].fillna(0.0)
    prev["rentabillik"] = prev["rentabillik"].fillna(0.0)

    prev_center_i = float(prev.loc[prev["department"] == CENTER_NAME, "I_jami_foiz"].sum())
    prev_rent_sum_non_center = float(prev.loc[prev["department"] != CENTER_NAME, "rentabillik"].sum())
    if prev_rent_sum_non_center <= prev_center_i + 0.01 and (prev["department"] == CENTER_NAME).any():
        prev.loc[prev["department"] == CENTER_NAME, "rentabillik"] = -prev_rent_sum_non_center

    prev["C_prev_protokol"] = prev["I_jami_foiz"] - prev["avans"] + prev["rentabillik"]
    prev = prev[["department", "C_prev_protokol"]]

    df = df.merge(prev, on="department", how="left")
    df["C_prev_protokol"] = df["C_prev_protokol"].fillna(0.0)

    df["_sort"] = df["department"].apply(lambda x: 999999 if str(x).strip().upper() == CENTER_NAME else 0)
    df = df.sort_values(["_sort", "I_jami_foiz"], ascending=[True, False]).drop(columns=["_sort"])
    df = df.reset_index(drop=True)
    df.insert(0, "№", range(1, len(df) + 1))

    protocol_table = pd.DataFrame({
        "№": df["№"],
        "Бўлимлар номи ": df["department"],
        "ЖАМИ ҚИЛГАН ИШИ": df["jami_qilgan_ish"],
        "ДОРИ-ДАРМОН": df["dori_darmon"],
        "СОФ ИШ": df["sof_ish"],
        "ОЛДИНГИ ОЙ ПРОТАКОЛ СУММАСИ": df["C_prev_protokol"],
        "ПРОТАКОЛ СУММА": df["E_protokol"],
        "МАРКАЗДА ҚОЛАДИ": df["markazda_qoladi"],
        "РЕНТАБИЛЛИК ХИСОБИДАН": df["rentabillik"],
        "УСТАМА У/Н ҚОЛГАН МАБЛАҒ": df["G_ustama_qolgan"],
        "ИШ ХАҚҚИ ОКЛАД (АВАНС)": df["avans"],
        "жами ФОИЗдан тушган маблағ": df["I_jami_foiz"],
    })

    return protocol_table, detail


def _excel_bytes(protocol_table: pd.DataFrame, detail: pd.DataFrame, sheet_name="Лист1") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        protocol_table.to_excel(writer, index=False, sheet_name=sheet_name)
        detail.to_excel(writer, index=False, sheet_name="Detail")

        from openpyxl.styles import Font, Alignment
        ws = writer.sheets[sheet_name]
        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center", vertical="center")

        for col in ws.columns:
            max_len = 0
            col_letter = col[0].column_letter
            for cell in col:
                v = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, len(v))
            ws.column_dimensions[col_letter].width = min(max_len + 2, 55)

    return output.getvalue()


def _format_group_table_for_show(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["tushum", "drug_amount", "sof_tushum"]:
        if c in out.columns:
            out[c] = out[c].apply(lambda x: fmt_uzs(float(x)))
    rename_map = {
        "group_name": "Guruh",
        "tushum": "Tushum",
        "drug_amount": "Dori-darmon",
        "sof_tushum": "Sof tushum",
    }
    return out.rename(columns=rename_map)


def render_foiz(selected_year: int, selected_month_name: str, uz_months: list[str]):
    _ensure_tables()
    month = uz_months.index(selected_month_name) + 1

    st.header("💰 Statsionar — Foiz taqsimoti (Protokol + Excel)")

    tabs = st.tabs(["Bo‘limlar", "Qoidalar", "Validatsiya", "Hisoblash / Excel"])

    report_type = st.session_state.get("foiz_report_type", "pullik")
    ui_pt = "Pullik"
    if report_type == "order":
        ui_pt = "Order"
    elif report_type == "all":
        ui_pt = "Hammasi"

    meta_now = {"year": selected_year, "month": month, "report_type": report_type}

    st.caption(
        "Har bir guruh summasi jami qilgan ishda faqat bitta bo‘limga yoziladi — "
        "eng katta foiz olgan bo‘limga. "
        "Dori-darmon alohida kiritiladi va avval tushumdan ayrilib, keyin foiz hisoblanadi. "
        "Poliklinika(OPD) guruhi vrach protokoli orqali keladi."
    )

    with tabs[0]:
        st.subheader("Foiz oluvchi bo‘limlar (qo‘lda)")
        depts = _list_departments(MODULE_NAME)
        st.write("Faol bo‘limlar:", depts if depts else "Hali yo‘q")

        c1, c2 = st.columns([3, 1])
        with c1:
            new_dept = st.text_input("Yangi bo‘lim nomi", key="new_dept_wh")
        with c2:
            if st.button("Qo‘shish", key="add_dept_wh_btn"):
                if new_dept.strip():
                    _add_department(MODULE_NAME, new_dept.strip())
                    st.success("Qo‘shildi ✅")
                    st.rerun()
                else:
                    st.error("Nom kiriting.")

        if depts:
            del_name = st.selectbox("Faolsizlantirish", depts, key="deact_dept_wh")
            if st.button("Faolsizlantirish", key="deact_dept_wh_btn"):
                _deactivate_department(MODULE_NAME, del_name)
                st.success("Faolsizlandi ✅")
                st.rerun()

    with tabs[1]:
        st.subheader("Qoidalar (guruh → bo‘lim → foiz)")

        rule_pt = st.radio("Qoidalar qaysi tur uchun?", ["Order", "Pullik"], horizontal=True, key="rule_pt")
        rule_payment_type = "order" if rule_pt == "Order" else "pullik"

        groups_editor = _build_group_editor_df(selected_year, month, rule_payment_type)
        if groups_editor.empty:
            st.info("Bu oyda guruhlar topilmadi.")
        else:
            st.write("### Guruhlar bo‘yicha tushum")
            st.dataframe(
                _format_group_table_for_show(groups_editor),
                use_container_width=True
            )

            st.write("### Dori-darmon kiritish")
            drug_editor = groups_editor[["group_name", "tushum", "drug_amount", "sof_tushum"]].copy()

            edited_drugs = st.data_editor(
                drug_editor,
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "group_name": st.column_config.TextColumn("Guruh", disabled=True),
                    "tushum": st.column_config.NumberColumn("Tushum", disabled=True),
                    "drug_amount": st.column_config.NumberColumn("Dori-darmon", min_value=0.0, step=1000.0),
                    "sof_tushum": st.column_config.NumberColumn("Sof tushum", disabled=True),
                },
                key=f"drug_editor_{rule_payment_type}"
            )

            if st.button("💾 Dori-darmonni saqlash", key=f"save_drug_{rule_payment_type}"):
                _save_group_drugs(selected_year, month, rule_payment_type, edited_drugs[["group_name", "drug_amount"]])
                st.success("Dori-darmon saqlandi ✅")
                st.rerun()

        depts = _list_departments(MODULE_NAME)
        if not depts:
            st.warning("Avval 'Bo‘limlar' tabida bo‘limlarni kiriting.")
        else:
            allowed_targets = [SOURCE_ALIAS] + depts
            existing = _load_rules(MODULE_NAME, rule_payment_type)
            existing = existing[existing["group_name"] != POLIK_DEPT].copy()

            if existing.empty and not groups_editor.empty:
                g0 = groups_editor[groups_editor["group_name"] != POLIK_DEPT]
                if not g0.empty:
                    existing = pd.DataFrame([{
                        "group_name": g0["group_name"].iloc[0],
                        "to_department": SOURCE_ALIAS,
                        "percent": 5.0
                    }])

            edited = st.data_editor(
                existing,
                num_rows="dynamic",
                use_container_width=True,
                column_config={
                    "group_name": st.column_config.SelectboxColumn(
                        "Guruh",
                        options=[g for g in groups_editor["group_name"].tolist() if g != POLIK_DEPT]
                    ),
                    "to_department": st.column_config.SelectboxColumn("Bo‘lim", options=allowed_targets),
                    "percent": st.column_config.NumberColumn("Foiz (%)", min_value=0.0, max_value=100.0, step=1.0),
                },
                key=f"rules_editor_{rule_payment_type}",
            )

            if st.button("💾 Saqlash", key=f"save_rules_{rule_payment_type}"):
                ok, msg = _save_rules(MODULE_NAME, rule_payment_type, edited, allowed_targets)
                st.success(msg) if ok else st.error(msg)
                if ok:
                    st.rerun()

            st.markdown("#### 📌 Saqlangan qoidalar ro‘yxati")
            st.dataframe(_load_rules(MODULE_NAME, rule_payment_type), use_container_width=True)

    with tabs[2]:
        st.subheader("Validatsiya")

        val_pt = st.radio("Validatsiya qaysi tur uchun?", ["Order", "Pullik"], horizontal=True, key="val_pt")
        val_payment_type = "order" if val_pt == "Order" else "pullik"

        depts = _list_departments(MODULE_NAME)
        rules = _clean_rules(_load_rules(MODULE_NAME, val_payment_type), [SOURCE_ALIAS] + depts)
        rules = rules[rules["group_name"] != POLIK_DEPT].copy()

        if rules.empty:
            st.info("Hali qoida yo‘q.")
        else:
            val = _validate_rules(rules)
            st.dataframe(val, use_container_width=True)

            if (val["status"] == "OVER").any():
                st.error("❌ OVER: foiz 100% dan oshgan guruhlar bor.")
            elif (val["status"] == "UNDER").any():
                st.warning("⚠️ UNDER: foiz 100% emas. Qoldiq MARKAZga ketadi.")
            else:
                st.success("✅ Hammasi OK (100%).")

    with tabs[3]:
        st.subheader("Hisoblash / Excel")

        ui_pt = st.radio(
            "Hisobot turi",
            ["Order", "Pullik", "Hammasi"],
            horizontal=True,
            key="foiz_pt"
        )
        report_type = "order" if ui_pt == "Order" else ("pullik" if ui_pt == "Pullik" else "all")
        st.session_state["foiz_report_type"] = report_type

        meta_now = {"year": selected_year, "month": month, "report_type": report_type}

        calc_clicked = st.button("✅ Hisoblash", key="calc_btn")
        if calc_clicked:
            try:
                protocol_table, detail = _build_protocol_table(selected_year, month, report_type)
                st.session_state[SS_KEY_PROTOCOL] = protocol_table
                st.session_state[SS_KEY_DETAIL] = detail
                st.session_state[SS_KEY_META] = meta_now
            except Exception as e:
                st.error(str(e))
                return

        has_saved = SS_KEY_PROTOCOL in st.session_state and SS_KEY_META in st.session_state
        if not has_saved:
            st.info("Natijani ko‘rish uchun 'Hisoblash' tugmasini bosing.")
            return

        old = st.session_state[SS_KEY_META]
        if old != meta_now:
            st.warning("Oy/Yil/Hisobot turi o‘zgargan. Yangilash uchun 'Hisoblash' tugmasini bosing.")

        protocol_table = st.session_state[SS_KEY_PROTOCOL]
        detail = st.session_state.get(SS_KEY_DETAIL, pd.DataFrame())

        st.markdown("### ✍️ H (Avans) va F (Rentabillik) — qo‘lda kiritiladi")
        base_manual = protocol_table[["Бўлимлар номи ", "ИШ ХАҚҚИ ОКЛАД (АВАНС)", "РЕНТАБИЛЛИК ХИСОБИДАН"]].copy()
        base_manual = base_manual.rename(columns={
            "Бўлимлар номи ": "department",
            "ИШ ХАҚҚИ ОКЛАД (АВАНС)": "avans",
            "РЕНТАБИЛЛИК ХИСОБИДАН": "rentabillik"
        })

        edited_manual = st.data_editor(
            base_manual,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "department": st.column_config.TextColumn("Bo‘lim", disabled=True),
                "avans": st.column_config.NumberColumn("Avans (H)", min_value=0.0, step=1000.0),
                "rentabillik": st.column_config.NumberColumn("Rentabillik (F)", step=1000.0),
            },
            key="manual_editor"
        )

        if st.button("💾 Avans/Rentabillikni saqlash", key="save_manual_btn"):
            try:
                _save_manual(selected_year, month, report_type, edited_manual)
                protocol_table, detail = _build_protocol_table(selected_year, month, report_type)
                st.session_state[SS_KEY_PROTOCOL] = protocol_table
                st.session_state[SS_KEY_DETAIL] = detail
                st.session_state[SS_KEY_META] = meta_now
                st.success("Saqlandi ✅ Jadval yangilandi.")
                st.rerun()
            except Exception as e:
                st.error(str(e))
                return

        st.markdown("### 📋 Natija jadvali (Protokol)")
        pretty = protocol_table.copy()

        money_cols = [
            "ЖАМИ ҚИЛГАН ИШИ",
            "ДОРИ-ДАРМОН",
            "СОФ ИШ",
            "ОЛДИНГИ ОЙ ПРОТАКОЛ СУММАСИ",
            "ПРОТАКОЛ СУММА",
            "МАРКАЗДА ҚОЛАДИ",
            "РЕНТАБИЛЛИК ХИСОБИДАН",
            "УСТАМА У/Н ҚОЛГАН МАБЛАҒ",
            "ИШ ХАҚҚИ ОКЛАД (АВАНС)",
            "жами ФОИЗдан тушган маблағ",
        ]
        for c in money_cols:
            if c in pretty.columns:
                pretty[c] = pretty[c].apply(lambda x: fmt_uzs(float(x)) if x is not None else fmt_uzs(0.0)).astype(str)

        st.dataframe(pretty, use_container_width=True)

        st.markdown("### ⬇️ Excelga yuklab olish")
        excel_bytes = _excel_bytes(protocol_table, detail, sheet_name="Лист1")
        file_name = f"Foiz_Protocol_{selected_year}_{month}_{ui_pt}.xlsx"
        st.download_button(
            label="Excel yuklab olish",
            data=excel_bytes,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )