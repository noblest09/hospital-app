# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
from io import BytesIO
import json
import re

from database import get_conn
from utils import UZ_MONTHS, now_iso, fmt_uzs

REQUIRED_KEYS = [
    "patient_id", "fio", "country", "department",
    "admission_date", "discharge_date",
    "tulov", "akt_sum", "drug_sum"
]

# -------------------- helpers --------------------
def _excel_bytes(df: pd.DataFrame, sheet_name: str = "Report") -> bytes:
    out = BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        wb = writer.book
        ws = writer.sheets[sheet_name]
        fmt_int = wb.add_format({"num_format": "# ##0"})
        fmt_txt = wb.add_format({"text_wrap": True})

        for i, col in enumerate(df.columns):
            low = str(col).lower()
            width = max(12, min(45, len(str(col)) + 6))
            if any(x in low for x in ["tulov", "akt", "dori", "summa", "amount", "tushum"]):
                ws.set_column(i, i, 18, fmt_int)
            else:
                ws.set_column(i, i, width, fmt_txt)
    return out.getvalue()

def _load_mapping(module: str, map_name: str = "default") -> dict:
    conn = get_conn()
    rows = conn.execute(
        "SELECT field_key, excel_column FROM column_mapping WHERE module=? AND map_name=?",
        (module, map_name),
    ).fetchall()
    conn.close()
    return {r["field_key"]: r["excel_column"] for r in rows}

def _get_setting(key: str, default: str = "") -> str:
    conn = get_conn()
    row = conn.execute("SELECT value FROM module_settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default

def _save_ungrouped_services(year: int, month: int, service_names: list[str]):
    conn = get_conn()
    cur = conn.cursor()
    for name in service_names:
        cur.execute("""
            INSERT INTO ungrouped_services(module, service_name, first_seen_year, first_seen_month)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(module, service_name) DO NOTHING
        """, ("statsionar", name, year, month))
    conn.commit()
    conn.close()

def _get_ungrouped(service_names: list[str]) -> list[str]:
    if not service_names:
        return []
    conn = get_conn()
    mapped = pd.read_sql_query(
        "SELECT service_name FROM service_main_group WHERE module='statsionar'",
        conn
    )["service_name"].tolist()
    conn.close()
    ms = set(mapped)
    return sorted([s for s in set(service_names) if s not in ms])

def _attach_main_group(df_services: pd.DataFrame) -> pd.DataFrame:
    conn = get_conn()
    m = pd.read_sql_query(
        "SELECT service_name, main_group_name FROM service_main_group WHERE module='statsionar'",
        conn
    )
    conn.close()
    if m.empty:
        df_services["main_group"] = "Guruhsiz"
        return df_services
    d2 = df_services.merge(m, on="service_name", how="left")
    d2["main_group"] = d2["main_group_name"].fillna("Guruhsiz")
    d2.drop(columns=["main_group_name"], inplace=True, errors="ignore")
    return d2

def _letter_to_idx(letter: str) -> int:
    letter = (letter or "").strip().upper()
    n = 0
    for ch in letter:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
    return max(1, n) - 1

def _prev_month(y: int, m: int):
    if m == 1:
        return y - 1, 12
    return y, m - 1

# -------------------- PRIORITY department logic --------------------
def _load_dept_priority_settings():
    primary_json = _get_setting("stats_primary_departments", "[]")
    try:
        primary = json.loads(primary_json)
        if not isinstance(primary, list):
            primary = []
    except:
        primary = []
    primary = [str(x).strip() for x in primary if str(x).strip()]

    gem = _get_setting("stats_gemodializ_name", "Gemodializ").strip()
    re1 = _get_setting("stats_reanim_neo_name", "Neonatal va kardioreanimatsiya").strip()
    re2 = _get_setting("stats_reanim_umumiy_name", "Umumiy reanimatsiya va intensiv davo").strip()
    return primary, gem, re1, re2

def _is_fake_department(name: str) -> bool:
    s = (name or "").strip()
    if not s:
        return True
    if re.match(r"^\d+\s*-\s*", s):  # 7-nurse ...
        return True
    low = s.lower()
    if "nurse" in low or "anesth" in low or "operating" in low:
        return True
    return False

def _pick_department_by_priority(dept_list: list[str]) -> str:
    primary12, gem, re1, re2 = _load_dept_priority_settings()

    depts = []
    for d in dept_list:
        d = (d or "").strip()
        if not d or d == "nan":
            continue
        if _is_fake_department(d):
            continue
        depts.append(d)

    # 1) 12 asosiy
    for d in depts:
        if d in primary12:
            return d

    # 2) Gemodializ
    if gem and gem in depts:
        return gem

    # 3) Reanim
    if re1 and re1 in depts:
        return re1
    if re2 and re2 in depts:
        return re2

    return "Boshqa"

# -------------------- read & import --------------------
def _read_stats_excel(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_excel(uploaded_file)

    mapping = _load_mapping("statsionar", "default")
    missing_keys = [k for k in REQUIRED_KEYS if k not in mapping]
    if missing_keys:
        raise ValueError("Sozlamalar → Statsionar mapping to'liq emas: " + ", ".join(missing_keys))

    missing_cols = [mapping[k] for k in REQUIRED_KEYS if mapping[k] not in df.columns]
    if missing_cols:
        raise ValueError("Excel faylda mappingdagi ustun topilmadi: " + ", ".join(missing_cols))

    fixed = df[[mapping[k] for k in REQUIRED_KEYS]].copy()
    fixed.columns = REQUIRED_KEYS

    for c in ["patient_id", "fio", "country", "department", "admission_date", "discharge_date"]:
        fixed[c] = fixed[c].astype(str).fillna("").str.strip()

    for c in ["tulov", "akt_sum", "drug_sum"]:
        fixed[c] = pd.to_numeric(fixed[c], errors="coerce").fillna(0)

    start_letter = _get_setting("statsionar_service_start_col", "W")
    cols = list(df.columns)
    start_idx = _letter_to_idx(start_letter)
    if start_idx >= len(cols):
        raise ValueError(f"Xizmatlar boshlanish ustuni noto'g'ri: {start_letter}")

    svc_cols = cols[start_idx:]
    svc_df = df[svc_cols].copy()
    svc_df.columns = [str(c).strip() for c in svc_df.columns]
    svc_df = svc_df.apply(pd.to_numeric, errors="coerce").fillna(0)

    long = pd.concat([
        fixed[["patient_id", "department"]].reset_index(drop=True),
        svc_df.reset_index(drop=True)
    ], axis=1)

    long = long.melt(
        id_vars=["patient_id", "department"],
        var_name="service_name",
        value_name="amount"
    )
    long["service_name"] = long["service_name"].astype(str).str.strip()
    long["amount"] = pd.to_numeric(long["amount"], errors="coerce").fillna(0)
    long = long[long["amount"] != 0]

    return fixed, long

def _aggregate_by_patient(year: int, month: int, payment_type: str, df_pat: pd.DataFrame, df_svc: pd.DataFrame):
    dept_map = (
        df_pat.groupby("patient_id")["department"]
        .apply(lambda s: [x for x in s.astype(str).tolist()])
        .to_dict()
    )

    agg_pat = df_pat.groupby("patient_id", as_index=False).agg(
        fio=("fio", "first"),
        country=("country", "first"),
        admission_date=("admission_date", "first"),
        discharge_date=("discharge_date", "first"),
        tulov=("tulov", "sum"),
        akt_sum=("akt_sum", "sum"),
        drug_sum=("drug_sum", "sum"),
    )

    agg_pat["department"] = agg_pat["patient_id"].astype(str).map(
        lambda pid: _pick_department_by_priority(dept_map.get(pid, []))
    )

    agg_pat["year"] = year
    agg_pat["month"] = month
    agg_pat["payment_type"] = payment_type

    agg_svc = df_svc.groupby(["patient_id", "service_name"], as_index=False)["amount"].sum()

    dept_lookup = dict(zip(agg_pat["patient_id"].astype(str), agg_pat["department"].astype(str)))
    agg_svc["department"] = agg_svc["patient_id"].astype(str).map(lambda x: dept_lookup.get(x, "Boshqa"))

    agg_svc["year"] = year
    agg_svc["month"] = month
    agg_svc["payment_type"] = payment_type

    return agg_pat, agg_svc

def _insert_statsionar(year: int, month: int, payment_type: str,
                      df_pat_agg: pd.DataFrame, df_svc_agg: pd.DataFrame,
                      filename: str | None):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM statsionar_patient WHERE year=? AND month=? AND payment_type=?", (year, month, payment_type))
    cur.execute("DELETE FROM statsionar_service_amount WHERE year=? AND month=? AND payment_type=?", (year, month, payment_type))

    pat_rows = []
    for _, r in df_pat_agg.iterrows():
        pat_rows.append((
            int(r["year"]), int(r["month"]), r["payment_type"],
            str(r["patient_id"]),
            str(r.get("fio", "")),
            str(r.get("country", "")),
            str(r.get("department", "")),
            str(r.get("admission_date", "")),
            str(r.get("discharge_date", "")),
            float(r.get("tulov", 0) or 0),
            float(r.get("akt_sum", 0) or 0),
            float(r.get("drug_sum", 0) or 0),
        ))

    cur.executemany("""
        INSERT INTO statsionar_patient
        (year, month, payment_type, patient_id, fio, country, department, admission_date, discharge_date, tulov, akt_sum, drug_sum)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, pat_rows)

    svc_rows = []
    for _, r in df_svc_agg.iterrows():
        svc_rows.append((
            int(r["year"]), int(r["month"]), r["payment_type"],
            str(r["patient_id"]),
            str(r.get("department", "")),
            str(r.get("service_name", "")),
            float(r.get("amount", 0) or 0)
        ))

    cur.executemany("""
        INSERT INTO statsionar_service_amount
        (year, month, payment_type, patient_id, department, service_name, amount)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, svc_rows)

    cur.execute("""
        INSERT INTO import_history (module, year, month, filename, rows_count, imported_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, ("statsionar_" + payment_type, year, month, filename, len(df_pat_agg), now_iso()))

    conn.commit()
    conn.close()

    ungrouped = _get_ungrouped(df_svc_agg["service_name"].dropna().astype(str).tolist())
    if ungrouped:
        _save_ungrouped_services(year, month, ungrouped)
    return ungrouped

def _load_patients(year: int, month: int, payment_filter: str) -> pd.DataFrame:
    conn = get_conn()
    where = "WHERE year=? AND month=?"
    params = [year, month]
    if payment_filter in ("order", "pullik"):
        where += " AND payment_type=?"
        params.append(payment_filter)
    df = pd.read_sql_query(f"SELECT * FROM statsionar_patient {where}", conn, params=tuple(params))
    conn.close()
    return df

def _load_services(year: int, month: int, payment_filter: str) -> pd.DataFrame:
    conn = get_conn()
    where = "WHERE year=? AND month=?"
    params = [year, month]
    if payment_filter in ("order", "pullik"):
        where += " AND payment_type=?"
        params.append(payment_filter)
    df = pd.read_sql_query(f"SELECT * FROM statsionar_service_amount {where}", conn, params=tuple(params))
    conn.close()
    return df

def _delete_month_statsionar(year: int, month: int, payment_type: str | None):
    conn = get_conn()
    cur = conn.cursor()

    if payment_type is None:
        cur.execute("DELETE FROM statsionar_patient WHERE year=? AND month=?", (year, month))
        cur.execute("DELETE FROM statsionar_service_amount WHERE year=? AND month=?", (year, month))
        cur.execute(
            "DELETE FROM import_history WHERE year=? AND month=? AND module IN ('statsionar_order','statsionar_pullik')",
            (year, month)
        )
    else:
        cur.execute("DELETE FROM statsionar_patient WHERE year=? AND month=? AND payment_type=?", (year, month, payment_type))
        cur.execute("DELETE FROM statsionar_service_amount WHERE year=? AND month=? AND payment_type=?", (year, month, payment_type))
        cur.execute("DELETE FROM import_history WHERE year=? AND month=? AND module=?", (year, month, "statsionar_" + payment_type))

    conn.commit()
    conn.close()

# -------------------- UI --------------------
def render_statsionar(selected_year: int, selected_month_name: str):
    st.subheader("🛏️ Statsionar")
    month = UZ_MONTHS.index(selected_month_name) + 1

    st.write("## 1) Hisobot import (Order / Pullik)")
    pay_ui = st.radio("Hisobot turi", ["Order (imtiyozli)", "Pullik"], horizontal=True)
    payment_type = "order" if pay_ui.startswith("Order") else "pullik"

    uploaded = st.file_uploader("Statsionar Excel faylni tanlang (.xlsx)", type=["xlsx"], key=f"st_file_{payment_type}")
    st.write(f"**Tanlangan oy:** {selected_year}-{month:02d} ({selected_month_name})")

    cA, cB, cC = st.columns([1, 1, 1])
    with cA:
        if st.button("📥 Import qilish", key=f"st_import_{payment_type}"):
            if uploaded is None:
                st.error("Iltimos Excel faylni tanlang.")
            else:
                try:
                    df_pat, df_svc = _read_stats_excel(uploaded)
                    df_pat_agg, df_svc_agg = _aggregate_by_patient(selected_year, month, payment_type, df_pat, df_svc)
                    ungrouped = _insert_statsionar(selected_year, month, payment_type, df_pat_agg, df_svc_agg, getattr(uploaded, "name", None))
                    st.success(f"Import muvaffaqiyatli ✅ Bemorlar: {len(df_pat_agg)} | Xizmat yozuvlari: {len(df_svc_agg)}")
                    if ungrouped:
                        st.warning("⚠️ Yangi xizmat(lar) guruhlanmagan:")
                        st.write(ungrouped)
                        st.info("Sozlamalar → Xizmatlarni guruhlash bo'limida guruhlab chiqing.")
                except Exception as e:
                    st.error(f"Import xatosi: {e}")

    with cB:
        if st.button("🗑 Shu oy (faqat tanlangan tur) o'chirish", key=f"del_one_{payment_type}"):
            _delete_month_statsionar(selected_year, month, payment_type)
            st.success("O'chirildi ✅ Endi qayta import qilishingiz mumkin.")

    with cC:
        if st.button("🗑 Shu oy (Order+Pullik) hammasini o'chirish", key="del_all_month"):
            _delete_month_statsionar(selected_year, month, None)
            st.success("O'chirildi ✅ Endi qayta import qilishingiz mumkin.")

    st.divider()

    st.write("## 2) Dashboard (tanlangan oy)")
    payment_filter_ui = st.radio("Ko'rsatish", ["Hammasi", "Faqat Order", "Faqat Pullik"], horizontal=True)
    payment_filter = "all"
    if payment_filter_ui == "Faqat Order":
        payment_filter = "order"
    elif payment_filter_ui == "Faqat Pullik":
        payment_filter = "pullik"

    patients = _load_patients(selected_year, month, payment_filter)
    services = _load_services(selected_year, month, payment_filter)

    if patients.empty:
        st.info("Tanlangan oyda ma'lumot yo'q. Avval import qiling.")
        return

    jami_tulov = float(patients["tulov"].sum())
    jami_akt = float(patients["akt_sum"].sum())
    jami_dori = float(patients["drug_sum"].sum())
    bemor_soni = int(patients["patient_id"].nunique())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Jami to'lov (UZS)", fmt_uzs(jami_tulov))
    c2.metric("Jami bajarilgan ish (Akt summa)", fmt_uzs(jami_akt))
    c3.metric("Jami dori-darmon", fmt_uzs(jami_dori))
    c4.metric("Bemorlar soni", str(bemor_soni))

    # ✅ Bo'lim bo'yicha PIE (Akt summa)
    dept_act = patients.groupby("department", as_index=False)["akt_sum"].sum()
    dept_act = dept_act[dept_act["akt_sum"] > 0].sort_values("akt_sum", ascending=False)
    if not dept_act.empty:
        fig = px.pie(dept_act, names="department", values="akt_sum", title="Bo'limlar ulushi (Akt summa bo'yicha)")
        st.plotly_chart(fig, use_container_width=True)

    # ✅ Oldingi oy bilan taqqoslash (bo‘lim bo‘yicha Akt summa)
    py, pm = _prev_month(selected_year, month)
    prev_patients = _load_patients(py, pm, payment_filter)

    if not prev_patients.empty:

        cur_dept = patients.groupby("department", as_index=False)["akt_sum"].sum().rename(columns={"akt_sum": "akt_hozir"})
        prev_dept = prev_patients.groupby("department", as_index=False)["akt_sum"].sum().rename(columns={"akt_sum": "akt_oldingi"})

        cmp = cur_dept.merge(prev_dept, on="department", how="outer").fillna(0)
        cmp = cmp.sort_values("akt_hozir", ascending=False)

        topN = cmp.head(15).copy()

        # 🔹 Farq va foiz hisoblash
        topN["farq"] = topN["akt_hozir"] - topN["akt_oldingi"]

        def hisobla_pct(row):
            if row["akt_oldingi"] == 0:
                return 100.0 if row["akt_hozir"] > 0 else 0.0
            return (row["farq"] / row["akt_oldingi"]) * 100

        topN["foiz"] = topN.apply(hisobla_pct, axis=1)

        # 🔹 Umumiy o‘sish %
        jami_hozir = topN["akt_hozir"].sum()
        jami_oldingi = topN["akt_oldingi"].sum()

        if jami_oldingi == 0:
            umumiy_pct = 100.0 if jami_hozir > 0 else 0.0
        else:
            umumiy_pct = ((jami_hozir - jami_oldingi) / jami_oldingi) * 100

        st.markdown(f"### 📊 Umumiy o‘zgarish (Top 15 bo‘lim): **{umumiy_pct:.1f}%**")

        # 🔹 Diagramma uchun tayyorlash
        melt = topN.melt(
            id_vars=["department"],
            value_vars=["akt_oldingi", "akt_hozir"],
            var_name="davr",
            value_name="akt_summa"
        )

        melt["davr"] = melt["davr"].map({
            "akt_oldingi": f"{py}-{pm:02d} (oldingi oy)",
            "akt_hozir": f"{selected_year}-{month:02d} (hozirgi oy)"
        })

        fig_cmp = px.bar(
            melt,
            x="department",
            y="akt_summa",
            color="davr",
            barmode="group",
            title="Oldingi oy bilan taqqoslash (Top 15 bo‘lim) — Akt summa"
        )

        # 🔹 Foiz yozuvini ustiga chiqarish (yashil/qizil)
        for _, row in topN.iterrows():
            y_top = max(row["akt_hozir"], row["akt_oldingi"])
            rang = "green" if row["foiz"] >= 0 else "red"

            fig_cmp.add_annotation(
                x=row["department"],
                y=y_top,
                text=f"{row['foiz']:.1f}%",
                showarrow=False,
                yshift=10,
                font=dict(color=rang, size=12)
            )

        fig_cmp.update_layout(
            xaxis_tickangle=-25,
            legend_title_text="Davr"
        )

        st.plotly_chart(fig_cmp, use_container_width=True)
    else:
        st.info(f"Oldingi oyda ({py}-{pm:02d}) ma’lumot topilmadi, taqqoslash chiqmaydi.")

    st.divider()

    st.write("## 3) 2-oyna: Bo'limlar kesimi (To'lov / Akt / Dori + guruhlar)")
    dept_base = patients.groupby("department", as_index=False).agg(
        tulov=("tulov", "sum"),
        akt_sum=("akt_sum", "sum"),
        drug_sum=("drug_sum", "sum"),
        bemorlar=("patient_id", "nunique"),
    )

    if not services.empty:
        svc_mg = _attach_main_group(services)
        mg_pivot = svc_mg.pivot_table(
            index="department",
            columns="main_group",
            values="amount",
            aggfunc="sum",
            fill_value=0
        ).reset_index()
        dept = dept_base.merge(mg_pivot, on="department", how="left").fillna(0)
    else:
        dept = dept_base

    dept = dept.sort_values("akt_sum", ascending=False)

    show = dept.copy()
    for col in show.columns:
        if col in ["department", "bemorlar"]:
            continue
        show[col] = show[col].map(fmt_uzs)

    show.rename(columns={
        "department": "Bo'lim",
        "tulov": "To'lov",
        "akt_sum": "Bajarilgan ish (Akt summa)",
        "drug_sum": "Dori-darmon",
        "bemorlar": "Bemorlar soni",
    }, inplace=True)

    st.dataframe(show, use_container_width=True)

    st.download_button(
        "⬇️ Bo'limlar Excel",
        data=_excel_bytes(dept, sheet_name="Bolimlar"),
        file_name=f"statsionar_{selected_year}_{month:02d}_{payment_filter}_bolimlar.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.divider()

    st.write("## 4) 3-oyna: Bo'lim ichidagi bemorlar + ID tanlab xizmatlar")

    dept_list = sorted([d for d in patients["department"].dropna().astype(str).unique().tolist() if d.strip() != ""])
    dept_list = ["(Hammasi)"] + dept_list
    sel_dept = st.selectbox("Bo'lim tanlang", dept_list, key="sel_dept_stats")

    if sel_dept == "(Hammasi)":
        pat_dept = patients.copy()
    else:
        pat_dept = patients[patients["department"].astype(str) == str(sel_dept)].copy()

    if pat_dept.empty:
        st.info("Bu bo'limda bemor topilmadi.")
        return

    # main_group kolonkalari bemorlarga (ixtiyoriy)
    if not services.empty:
        svc_mg = _attach_main_group(services)
        if sel_dept != "(Hammasi)":
            svc_mg = svc_mg[svc_mg["department"].astype(str) == str(sel_dept)].copy()

        pat_mg = svc_mg.groupby(["patient_id", "main_group"], as_index=False)["amount"].sum()
        pat_wide = pat_mg.pivot_table(
            index="patient_id",
            columns="main_group",
            values="amount",
            aggfunc="sum",
            fill_value=0
        ).reset_index()
        pat_dept = pat_dept.merge(pat_wide, on="patient_id", how="left").fillna(0)

    pat_dept = pat_dept.sort_values("akt_sum", ascending=False)

    pat_show = pat_dept.copy()
    pat_show.rename(columns={
        "patient_id": "Tarix raqami (ID)",
        "fio": "Bemor F.I.O.",
        "country": "Respublika/Davlat",
        "department": "Bo'lim",
        "admission_date": "Kelgan sana",
        "discharge_date": "Chiqqan sana",
        "tulov": "To'lov",
        "akt_sum": "Bajarilgan ish (Akt summa)",
        "drug_sum": "Dori-darmon",
    }, inplace=True)

    skip_cols = {"Tarix raqami (ID)", "Bemor F.I.O.", "Respublika/Davlat", "Bo'lim", "Kelgan sana", "Chiqqan sana", "payment_type", "year", "month", "id"}
    for col in pat_show.columns:
        if col not in skip_cols:
            try:
                pat_show[col] = pat_show[col].map(fmt_uzs)
            except:
                pass

    st.dataframe(pat_show.drop(columns=["id"], errors="ignore"), use_container_width=True)

    st.download_button(
        "⬇️ Bemorlar Excel",
        data=_excel_bytes(pat_dept, sheet_name="Bemorlar"),
        file_name=f"statsionar_{selected_year}_{month:02d}_{payment_filter}_{sel_dept}_bemorlar.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.write("### Tanlangan bo‘lim ichidan bitta bemorni tanlab xizmatlarini ko‘rish")
    pid_list = pat_dept["patient_id"].astype(str).tolist()
    pid = st.selectbox("Tarix raqami (ID)", pid_list, key="pid_in_dept")

    if sel_dept == "(Hammasi)":
        svc_one = services[services["patient_id"].astype(str) == str(pid)].copy()
    else:
        svc_one = services[
            (services["patient_id"].astype(str) == str(pid)) &
            (services["department"].astype(str) == str(sel_dept))
        ].copy()

    if svc_one.empty:
        st.info("Bu ID uchun xizmat topilmadi.")
    else:
        svc_one = _attach_main_group(svc_one)
        svc_one = svc_one.groupby(["main_group", "service_name"], as_index=False)["amount"].sum()
        svc_one = svc_one.sort_values("amount", ascending=False)

        svc_show = svc_one.copy()
        svc_show["amount"] = svc_show["amount"].map(fmt_uzs)
        svc_show.rename(columns={
            "main_group": "Asosiy guruh",
            "service_name": "Xizmat nomi",
            "amount": "Summa"
        }, inplace=True)
        st.dataframe(svc_show, use_container_width=True)