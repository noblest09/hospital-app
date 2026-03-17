# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
from io import BytesIO

from database import get_conn
from utils import now_iso, UZ_MONTHS, fmt_uzs

REQUIRED_KEYS = ["amb_no","fio","country","order_type","service_group","service_name","qty","price","amount"]

# ---------------- Excel export ----------------
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
            if any(x in low for x in ["tushum", "amount", "summa", "narx", "qty", "soni"]):
                ws.set_column(i, i, 18, fmt_int)
            else:
                ws.set_column(i, i, width, fmt_txt)
    return out.getvalue()

# ---------------- Mapping ----------------
def _load_mapping(module: str, map_name: str="default") -> dict:
    conn = get_conn()
    rows = conn.execute(
        "SELECT field_key, excel_column FROM column_mapping WHERE module=? AND map_name=?",
        (module, map_name),
    ).fetchall()
    conn.close()
    return {r["field_key"]: r["excel_column"] for r in rows}

# ---------------- Grouping helpers ----------------
def _attach_main_group(df: pd.DataFrame) -> pd.DataFrame:
    """
    service_name -> main_group_name mappingini DB dan olib keladi.
    Agar mapping bo'lmasa: 'Guruhsiz' qiladi.
    """
    conn = get_conn()
    m = pd.read_sql_query(
        "SELECT service_name, main_group_name FROM service_main_group WHERE module='ambulator'",
        conn
    )
    conn.close()

    if m.empty:
        df["main_group"] = "Guruhsiz"
        return df

    df2 = df.merge(m, on="service_name", how="left")
    df2["main_group"] = df2["main_group_name"].fillna("Guruhsiz")
    df2.drop(columns=["main_group_name"], inplace=True, errors="ignore")
    return df2

# ---------------- Ungrouped services ----------------
def _save_ungrouped_services(year: int, month: int, service_names: list[str]):
    conn = get_conn()
    cur = conn.cursor()
    for name in sorted(set(service_names)):
        cur.execute("""
            INSERT INTO ungrouped_services(module, service_name, first_seen_year, first_seen_month)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(module, service_name) DO NOTHING
        """, ("ambulator", name, year, month))
    conn.commit()
    conn.close()

def _get_ungrouped(service_names: list[str]) -> list[str]:
    if not service_names:
        return []
    conn = get_conn()
    mapped = pd.read_sql_query(
        "SELECT service_name FROM service_main_group WHERE module='ambulator'",
        conn
    )["service_name"].tolist()
    conn.close()
    mapped_set = set(mapped)
    return sorted([s for s in set(service_names) if s not in mapped_set])

# ---------------- Read OPD excel ----------------
def _read_opd_excel(uploaded_file) -> pd.DataFrame:
    df = pd.read_excel(uploaded_file)

    mapping = _load_mapping("ambulator", "default")
    missing_keys = [k for k in REQUIRED_KEYS if k not in mapping]
    if missing_keys:
        raise ValueError("Sozlamalar → Ambulator mapping to‘liq emas: " + ", ".join(missing_keys))

    missing_cols = [mapping[k] for k in REQUIRED_KEYS if mapping[k] not in df.columns]
    if missing_cols:
        raise ValueError("Excel faylda mappingdagi ustun topilmadi: " + ", ".join(missing_cols))

    out = df[[mapping[k] for k in REQUIRED_KEYS]].copy()
    out.columns = REQUIRED_KEYS

    for c in ["qty","price","amount"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    for c in ["amb_no","fio","country","order_type","service_group","service_name"]:
        out[c] = out[c].astype(str).fillna("").str.strip()

    return out

# ---------------- DB write / read ----------------
def _insert_ambulator(year: int, month: int, df: pd.DataFrame, filename: str | None):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM ambulator_raw WHERE year=? AND month=?", (year, month))

    rows = []
    for _, r in df.iterrows():
        rows.append((
            year, month,
            str(r.get("amb_no","")).strip(),
            str(r.get("fio","")).strip(),
            str(r.get("country","")).strip(),
            str(r.get("order_type","")).strip(),
            str(r.get("service_group","")).strip(),
            str(r.get("service_name","")).strip(),
            float(r.get("qty",0) or 0),
            float(r.get("price",0) or 0),
            float(r.get("amount",0) or 0),
        ))

    cur.executemany("""
        INSERT INTO ambulator_raw
        (year, month, amb_no, fio, country, order_type, service_group, service_name, qty, price, amount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    cur.execute("""
        INSERT INTO import_history (module, year, month, filename, rows_count, imported_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, ("ambulator", year, month, filename, len(df), now_iso()))

    conn.commit()
    conn.close()

def _history_table(module: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT year, month, rows_count, imported_at, filename "
        "FROM import_history WHERE module=? ORDER BY year DESC, month DESC",
        conn, params=(module,)
    )
    conn.close()
    return df

def _load_month_df(year: int, month: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT * FROM ambulator_raw WHERE year=? AND month=?",
        conn, params=(year, month)
    )
    conn.close()
    return df

def _delete_month_ambulator(year: int, month: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM ambulator_raw WHERE year=? AND month=?", (year, month))
    cur.execute("DELETE FROM import_history WHERE module='ambulator' AND year=? AND month=?", (year, month))
    conn.commit()
    conn.close()

def _prev_month(y: int, m: int):
    if m == 1:
        return y - 1, 12
    return y, m - 1

# ---------------- UI ----------------
def render_ambulator(selected_year: int, selected_month_name: str):
    st.subheader("🏃 Ambulator (OPD)")
    month = UZ_MONTHS.index(selected_month_name) + 1

    st.write("## 1) Hisobot import")
    st.caption("Import qilishdan oldin Sozlamalar → Ambulator mappingni sozlab qo‘ying.")
    uploaded = st.file_uploader("OPD Excel faylni tanlang (ambulator)", type=["xlsx"], key="opd_import_file")

    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        st.write(f"**Tanlangan oy:** {selected_year}-{month:02d} ({selected_month_name})")
    with col2:
        import_btn = st.button("📥 Import qilish (shu oyga)", key="opd_import_btn")
    with col3:
        if st.button("🗑 Shu oy ma’lumotini o‘chirish", key="opd_delete_month"):
            _delete_month_ambulator(selected_year, month)
            st.success("O‘chirildi ✅ Endi qayta import qilishingiz mumkin.")

    if import_btn:
        if uploaded is None:
            st.error("Iltimos Excel faylni tanlang.")
        else:
            try:
                df = _read_opd_excel(uploaded)

                # yangi xizmatlarni aniqlash
                ungrouped = _get_ungrouped(df["service_name"].dropna().astype(str).tolist())
                if ungrouped:
                    _save_ungrouped_services(selected_year, month, ungrouped)

                _insert_ambulator(selected_year, month, df, getattr(uploaded, "name", None))

                st.success(f"Import muvaffaqiyatli ✅ Qatorlar: {len(df)}")

                if ungrouped:
                    st.warning("⚠️ Yangi xizmat(lar) topildi, guruhlanmagan:")
                    st.write(ungrouped)
                    st.info("Sozlamalar → Xizmatlarni guruhlash bo‘limida guruhlab chiqing.")

            except Exception as e:
                st.error(f"Import xatosi: {e}")

    st.divider()

    st.write("## 2) Yuklangan oylar (Ambulator)")
    hist = _history_table("ambulator")
    if hist.empty:
        st.info("Hali ambulator hisobot yuklanmagan.")
    else:
        show = hist.copy()
        show["oy"] = show["month"].apply(lambda m: UZ_MONTHS[m-1] if 1 <= int(m) <= 12 else str(m))
        show = show[["year","oy","rows_count","imported_at","filename"]]
        st.dataframe(show, use_container_width=True)

    st.divider()

    st.write("## 3) Dashboard (tanlangan oy)")
    dfm = _load_month_df(selected_year, month)
    if dfm.empty:
        st.info("Tanlangan oyda ma’lumot yo‘q. Avval import qiling.")
        return

    dfm = _attach_main_group(dfm)

    total_amount = float(dfm["amount"].sum())
    total_qty = float(dfm["qty"].sum())
    rows_cnt = int(len(dfm))

    c1, c2, c3 = st.columns(3)
    c1.metric("Jami tushum (UZS)", fmt_uzs(total_amount))
    c2.metric("Jami xizmat soni", f"{total_qty:,.0f}".replace(",", " "))
    c3.metric("Qatorlar", str(rows_cnt))

    # PIE: asosiy guruh bo'yicha
    g = dfm.groupby("main_group", as_index=False)["amount"].sum()
    g = g[g["amount"] != 0].sort_values("amount", ascending=False)
    if not g.empty:
        fig = px.pie(g, names="main_group", values="amount", title="Asosiy guruh bo‘yicha ulush")
        st.plotly_chart(fig, use_container_width=True)

    # ✅ Oldingi oy bilan taqqoslash (asosiy guruh bo‘yicha) — chiroyli variant
    py, pm = _prev_month(selected_year, month)
    df_prev = _load_month_df(py, pm)

    if not df_prev.empty:
        df_prev = _attach_main_group(df_prev)

        cur_g = dfm.groupby("main_group", as_index=False)  ["amount"].sum().rename(columns={"amount": "tushum_hozir"})
        prev_g = df_prev.groupby("main_group", as_index=False)   ["amount"].sum().rename(columns={"amount": "tushum_oldingi"})

        cmp = cur_g.merge(prev_g, on="main_group", how="outer").fillna(0)
        cmp = cmp.sort_values("tushum_hozir", ascending=False)

        # Top 15 guruh
        topN = cmp.head(15).copy()

        # % o'zgarish
        topN["diff"] = topN["tushum_hozir"] - topN["tushum_oldingi"]

        def _pct(row):
            if row["tushum_oldingi"] == 0:
                return 100.0 if row["tushum_hozir"] > 0 else 0.0
            return (row["diff"] / row["tushum_oldingi"]) * 100

        topN["pct"] = topN.apply(_pct, axis=1)

        # Umumiy % o'zgarish (Top15 bo'yicha)
        total_hozir = topN["tushum_hozir"].sum()
        total_oldingi = topN["tushum_oldingi"].sum()
        if total_oldingi == 0:
            total_pct = 100.0 if total_hozir > 0 else 0.0
        else:
            total_pct = ((total_hozir - total_oldingi) / total_oldingi) * 100

        st.markdown(f"## Umumiy o‘zgarish (Top 15 guruh): **{total_pct:.1f}%**")

        # Chart uchun melt
        melt = topN.melt(
            id_vars=["main_group"],
            value_vars=["tushum_oldingi", "tushum_hozir"],
            var_name="davr",
            value_name="tushum"
        )
        melt["davr"] = melt["davr"].map({
            "tushum_oldingi": f"{py}-{pm:02d} (oldingi oy)",
            "tushum_hozir": f"{selected_year}-{month:02d} (hozirgi oy)"
        })

        fig_cmp = px.bar(
            melt,
            x="main_group",
            y="tushum",
            color="davr",
            barmode="group",
            title="Oldingi oy bilan taqqoslash (Top 15 guruh) — Tushum",
        )

        # % yozuvlarini (annotation) qo‘shamiz
        for _, r in topN.iterrows():
            y_top = max(r["tushum_hozir"], r["tushum_oldingi"])
            color = "green" if r["pct"] >= 0 else "red"
            fig_cmp.add_annotation(
                x=r["main_group"],
                y=y_top,
                text=f"{r['pct']:.1f}%",
                showarrow=False,
                yshift=10,
                font=dict(color=color, size=12)
            )

        # X o‘qdagi yozuvlar ko‘p bo‘lsa o‘qilishi uchun
        fig_cmp.update_layout(
            xaxis_tickangle=-25,
            legend_title_text="Davr"
        )

        st.plotly_chart(fig_cmp, use_container_width=True)
    else:
        st.info(f"Oldingi oyda ({py}-{pm:02d}) ambulator ma’lumot topilmadi, taqqoslash chiqmaydi.")

    st.divider()

    st.write("## 4) 2-oyna: Xizmat guruhi bo‘yicha")
    g2 = dfm.groupby("main_group", as_index=False).agg(
        xizmat_soni=("qty","sum"),
        tushum=("amount","sum"),
    )
    g2 = g2[g2["tushum"] != 0].sort_values("tushum", ascending=False)

    g2_show = g2.copy()
    g2_show["tushum"] = g2_show["tushum"].map(fmt_uzs)
    g2_show["xizmat_soni"] = g2_show["xizmat_soni"].map(lambda x: f"{x:,.0f}".replace(",", " "))
    st.dataframe(g2_show, use_container_width=True)

    st.download_button(
        "⬇️ 2-oyna Excel",
        data=_excel_bytes(g2, sheet_name="Guruhlar"),
        file_name=f"ambulator_{selected_year}_{month:02d}_guruhlar.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.divider()

    st.write("## 5) 3-oyna: Xizmat nomi bo‘yicha")
    s3 = dfm.groupby(["main_group","service_name"], as_index=False).agg(
        xizmat_soni=("qty","sum"),
        tushum=("amount","sum"),
    )
    s3 = s3[s3["tushum"] != 0].sort_values("tushum", ascending=False)

    groups = ["(Hammasi)"] + sorted([x for x in dfm["main_group"].dropna().unique().tolist()])
    sel_group = st.selectbox("Asosiy guruh filtri", groups, index=0, key="main_group_filter")

    if sel_group != "(Hammasi)":
        s3 = s3[s3["main_group"] == sel_group]

    s3_show = s3.copy()
    s3_show["tushum"] = s3_show["tushum"].map(fmt_uzs)
    s3_show["xizmat_soni"] = s3_show["xizmat_soni"].map(lambda x: f"{x:,.0f}".replace(",", " "))
    st.dataframe(s3_show, use_container_width=True)

    st.download_button(
        "⬇️ 3-oyna Excel",
        data=_excel_bytes(s3, sheet_name="Xizmatlar"),
        file_name=f"ambulator_{selected_year}_{month:02d}_xizmatlar.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )