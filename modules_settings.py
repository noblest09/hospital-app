# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import json

from database import get_conn
from utils import UZ_MONTHS

# --------- UI label helpers (100% Uzbek) ---------
AMB_FIELDS = [
    ("amb_no",        "Ambulator karta raqami"),
    ("fio",           "Bemor F.I.O."),
    ("country",       "Yashash joyi (Respublika/Davlat)"),
    ("order_type",    "Hisobot turi (Order/Pullik)"),
    ("service_group", "Xizmat guruhi (Excel ustuni)"),
    ("service_name",  "Xizmat nomi"),
    ("qty",           "Xizmat soni"),
    ("price",         "Xizmat narxi"),
    ("amount",        "Xizmat summasi"),
]

STAT_FIELDS = [
    ("patient_id",     "Tarix raqami / ID (kasallik tarixi)"),
    ("fio",            "Bemor F.I.O."),
    ("country",        "Respublika/Davlat"),
    ("department",     "Bo'lim"),
    ("admission_date", "Davolanishga kelgan sana"),
    ("discharge_date", "Chiqqan sana"),
    ("tulov",          "To'lov summasi"),
    ("akt_sum",        "Bajarilgan ish (Akt summa)"),
    ("drug_sum",       "Dori-darmon summasi"),
]


# -------------------- DB helpers --------------------
def _load_mapping(module: str, map_name: str = "default") -> dict:
    conn = get_conn()
    rows = conn.execute(
        "SELECT field_key, excel_column FROM column_mapping WHERE module=? AND map_name=?",
        (module, map_name)
    ).fetchall()
    conn.close()
    return {r["field_key"]: r["excel_column"] for r in rows}


def _save_mapping(module: str, mapping: dict, map_name: str = "default"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM column_mapping WHERE module=? AND map_name=?", (module, map_name))
    cur.executemany(
        "INSERT INTO column_mapping (module, map_name, field_key, excel_column) VALUES (?, ?, ?, ?)",
        [(module, map_name, k, v) for k, v in mapping.items()]
    )
    conn.commit()
    conn.close()


def _list_main_groups(module: str) -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT name FROM main_groups WHERE module=? AND is_active=1 ORDER BY name",
        (module,)
    ).fetchall()
    conn.close()
    return [r["name"] for r in rows]


def _add_main_group(module: str, name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO main_groups(module, name, is_active) VALUES (?, ?, 1) "
        "ON CONFLICT(module, name) DO UPDATE SET is_active=1",
        (module, name.strip())
    )
    conn.commit()
    conn.close()


def _deactivate_main_group(module: str, name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE main_groups SET is_active=0 WHERE module=? AND name=?", (module, name))
    conn.commit()
    conn.close()


def _services_from_db(module: str) -> pd.DataFrame:
    conn = get_conn()
    if module == "ambulator":
        q = """
        SELECT DISTINCT service_name
        FROM ambulator_raw
        WHERE service_name IS NOT NULL AND TRIM(service_name)<>'' 
        ORDER BY service_name
        """
    else:
        q = """
        SELECT DISTINCT service_name
        FROM statsionar_service_amount
        WHERE service_name IS NOT NULL AND TRIM(service_name)<>'' 
        ORDER BY service_name
        """
    df = pd.read_sql_query(q, conn)
    conn.close()
    if df.empty:
        return pd.DataFrame({"service_name": []})
    df["service_name"] = df["service_name"].astype(str)
    return df


def _services_from_excel_amb(upl, col_name: str) -> pd.DataFrame:
    df = pd.read_excel(upl)
    if col_name not in df.columns:
        return pd.DataFrame({"service_name": []})
    s = df[col_name].dropna().astype(str).str.strip()
    s = s[s != ""].drop_duplicates().sort_values()
    return pd.DataFrame({"service_name": s.tolist()})


def _services_from_excel_stats_by_letter(upl, start_letter: str) -> pd.DataFrame:
    df = pd.read_excel(upl)
    cols = list(df.columns)

    def letter_to_idx(letter: str) -> int:
        n = 0
        for ch in (letter or "").strip().upper():
            if "A" <= ch <= "Z":
                n = n * 26 + (ord(ch) - ord("A") + 1)
        return max(1, n) - 1

    start_idx = letter_to_idx(start_letter)
    if start_idx >= len(cols):
        return pd.DataFrame({"service_name": []})
    svc_cols = cols[start_idx:]
    s = pd.Series(svc_cols).dropna().astype(str).str.strip()
    s = s[s != ""].drop_duplicates().sort_values()
    return pd.DataFrame({"service_name": s.tolist()})


def _load_service_group_map(module: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT service_name, main_group_name FROM service_main_group WHERE module=? ORDER BY service_name",
        conn, params=(module,)
    )
    conn.close()
    if df.empty:
        return pd.DataFrame({"service_name": [], "main_group_name": []})
    return df


def _save_service_group_map(module: str, df: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM service_main_group WHERE module=?", (module,))
    rows = []
    for _, r in df.iterrows():
        s = str(r["service_name"]).strip()
        g = str(r["main_group_name"]).strip()
        if s and g:
            rows.append((module, s, g))
    cur.executemany(
        "INSERT INTO service_main_group(module, service_name, main_group_name) VALUES (?, ?, ?) "
        "ON CONFLICT(module, service_name) DO UPDATE SET main_group_name=excluded.main_group_name",
        rows
    )
    conn.commit()
    conn.close()


def _list_ungrouped(module: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT service_name, first_seen_year, first_seen_month "
        "FROM ungrouped_services WHERE module=? "
        "ORDER BY first_seen_year DESC, first_seen_month DESC, service_name",
        conn, params=(module,)
    )
    conn.close()
    return df


def _get_setting(key: str, default: str = "") -> str:
    conn = get_conn()
    row = conn.execute("SELECT value FROM module_settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def _set_setting(key: str, value: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO module_settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )
    conn.commit()
    conn.close()


# -------------------- MAIN UI --------------------
def render_settings(selected_year: int, selected_month_name: str, uz_months: list[str] = UZ_MONTHS):
    st.subheader("⚙️ Sozlamalar")

    tabs = st.tabs([
        "1) Ambulator mapping",
        "2) Statsionar mapping",
        "3) Asosiy guruhlar",
        "4) Xizmatlarni guruhlash",
        "5) Guruhlanmagan yangi xizmatlar",
        "6) Statsionar: bo'limlar (12 + Gemodializ + 2 Reanim)"
    ])

    # ---------------- TAB 1: Ambulator mapping ----------------
    with tabs[0]:
        st.write("### Ambulator: Excel ustunlarini moslash (mapping)")
        current = _load_mapping("ambulator", "default")

        upl = st.file_uploader("OPD Excel tanlang (ustunlarni ko'rish uchun)", type=["xlsx"], key="opd_map_file")
        cols = []
        if upl is not None:
            df = pd.read_excel(upl, nrows=3)
            cols = list(df.columns)

        if not cols:
            st.info("Mapping qilish uchun avval OPD Excel fayl tanlang.")
        else:
            mapping_new = {}
            for key, label in AMB_FIELDS:
                default_val = current.get(key)
                idx = cols.index(default_val) if default_val in cols else 0
                mapping_new[key] = st.selectbox(label, options=cols, index=idx, key=f"amb_map_{key}")

            if st.button("💾 Ambulator mappingni saqlash"):
                _save_mapping("ambulator", mapping_new, "default")
                st.success("Saqlandi ✅")

    # ---------------- TAB 2: Statsionar mapping ----------------
    with tabs[1]:
        st.write("### Statsionar: Excel ustunlarini moslash (mapping)")
        st.caption("Xizmatlar ustunlari: faqat boshlanish ustuni harfi (masalan W). Tugash ustuni kerak emas.")

        current = _load_mapping("statsionar", "default")

        # xizmat boshlanish harfi
        start_letter = _get_setting("statsionar_service_start_col", "W")
        new_letter = st.text_input("Xizmatlar boshlanish ustuni (harf)", value=start_letter, max_chars=3)
        if st.button("💾 Xizmatlar boshlanish harfini saqlash"):
            _set_setting("statsionar_service_start_col", new_letter.strip().upper())
            st.success("Saqlandi ✅")

        st.divider()

        upl = st.file_uploader("Statsionar Excel tanlang (ustunlarni ko'rish uchun)", type=["xlsx"], key="st_map_file")
        cols = []
        if upl is not None:
            df = pd.read_excel(upl, nrows=3)
            cols = list(df.columns)

        if not cols:
            st.info("Mapping qilish uchun avval Statsionar Excel fayl tanlang.")
        else:
            mapping_new = {}
            for key, label in STAT_FIELDS:
                default_val = current.get(key)
                idx = cols.index(default_val) if default_val in cols else 0
                mapping_new[key] = st.selectbox(label, options=cols, index=idx, key=f"st_map_{key}")

            if st.button("💾 Statsionar mappingni saqlash"):
                _save_mapping("statsionar", mapping_new, "default")
                st.success("Saqlandi ✅")

    # ---------------- TAB 3: Main groups ----------------
    with tabs[2]:
        st.write("### Asosiy guruhlar (Ambulator va Statsionar uchun alohida)")
        colA, colB = st.columns(2)

        with colA:
            st.write("#### Ambulator")
            groups = _list_main_groups("ambulator")
            st.write(groups if groups else "Hozircha yo'q")
            new_name = st.text_input("Yangi guruh (Ambulator)", key="amb_new_group")
            if st.button("Qo'shish (Ambulator)"):
                if new_name.strip():
                    _add_main_group("ambulator", new_name.strip())
                    st.success("Qo'shildi ✅")
            if groups:
                del_name = st.selectbox("Faolsizlantirish (Ambulator)", groups, key="amb_deact")
                if st.button("Faolsizlantirish (Ambulator)"):
                    _deactivate_main_group("ambulator", del_name)
                    st.success("Faolsizlandi ✅")

        with colB:
            st.write("#### Statsionar")
            groups = _list_main_groups("statsionar")
            st.write(groups if groups else "Hozircha yo'q")
            new_name = st.text_input("Yangi guruh (Statsionar)", key="st_new_group")
            if st.button("Qo'shish (Statsionar)"):
                if new_name.strip():
                    _add_main_group("statsionar", new_name.strip())
                    st.success("Qo'shildi ✅")
            if groups:
                del_name = st.selectbox("Faolsizlantirish (Statsionar)", groups, key="st_deact")
                if st.button("Faolsizlantirish (Statsionar)"):
                    _deactivate_main_group("statsionar", del_name)
                    st.success("Faolsizlandi ✅")

    # ---------------- TAB 4: Service -> main group mapping ----------------
    with tabs[3]:
        st.write("### Xizmatlarni asosiy guruhlarga biriktirish")
        module = st.radio("Modul", ["ambulator", "statsionar"], horizontal=True)

        groups = _list_main_groups(module)
        if not groups:
            st.warning("Avval 3-tabda shu modul uchun asosiy guruhlarni kiriting.")
        else:
            source = st.radio("Xizmatlar manbai", ["Bazadan (import qilingan)", "Excel fayldan"], horizontal=True, key=f"{module}_src")

            services_df = pd.DataFrame({"service_name": []})
            if source.startswith("Baza"):
                services_df = _services_from_db(module)
            else:
                upl = st.file_uploader("Excel tanlang", type=["xlsx"], key=f"{module}_svc_excel")

                if module == "ambulator":
                    colname = st.text_input("Excelda xizmat ustuni nomi (Ambulator)", key="amb_svc_col")
                    if upl is not None and colname.strip():
                        services_df = _services_from_excel_amb(upl, colname.strip())
                else:
                    start = _get_setting("statsionar_service_start_col", "W")
                    st.info(f"Statsionar xizmatlar boshlanish ustuni: {start}")
                    if upl is not None:
                        services_df = _services_from_excel_stats_by_letter(upl, start)

            if services_df.empty:
                st.info("Xizmatlar ro'yxati bo'sh.")
            else:
                current_map = _load_service_group_map(module)
                merged = services_df.merge(current_map, on="service_name", how="left")
                merged["main_group_name"] = merged["main_group_name"].fillna("")

                edited = st.data_editor(
                    merged,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "service_name": st.column_config.TextColumn("Xizmat nomi", disabled=True),
                        "main_group_name": st.column_config.SelectboxColumn("Asosiy guruh", options=[""] + groups),
                    },
                )

                if st.button("💾 Saqlash (xizmat → guruh)", key=f"save_{module}_svcmap"):
                    _save_service_group_map(module, edited)
                    st.success("Saqlandi ✅")

    # ---------------- TAB 5: Ungrouped services ----------------
    with tabs[4]:
        st.write("### Guruhlanmagan yangi xizmatlar")
        module = st.radio("Modul (ungrouped)", ["ambulator", "statsionar"], horizontal=True, key="ung_module")
        dfu = _list_ungrouped(module)
        if dfu.empty:
            st.success("Hozircha hammasi guruhlangan ✅")
        else:
            st.dataframe(dfu, use_container_width=True)
            st.info("4-tabda bu xizmatlarga asosiy guruh biriktirib chiqing.")

    # ---------------- TAB 6: Statsionar department priority settings ----------------
    with tabs[5]:
        st.write("### Statsionar: bo'limlarni prioritet bo'yicha belgilash")
        st.caption("Qoida: 12 asosiy bo'lim → Gemodializ → 2 ta reanimatsiya → Boshqa. "
                   "Shu yerda aynan nomlarini belgilaysiz.")

        upl = st.file_uploader("Statsionar Excel tanlang (bo'limlarni ko'rish uchun)", type=["xlsx"], key="st_dept_helper")
        excel_depts = []
        if upl is not None:
            try:
                df_tmp = pd.read_excel(upl, nrows=500)
                mp = _load_mapping("statsionar", "default")
                dept_col = mp.get("department")
                if dept_col and dept_col in df_tmp.columns:
                    excel_depts = sorted(df_tmp[dept_col].dropna().astype(str).str.strip().unique().tolist())
            except:
                excel_depts = []

        if excel_depts:
            st.info("Excelda uchragan bo'limlar ro'yxati:")
            st.write(excel_depts)

        # saved settings
        primary_json = _get_setting("stats_primary_departments", "[]")
        try:
            primary_saved = json.loads(primary_json)
            if not isinstance(primary_saved, list):
                primary_saved = []
        except:
            primary_saved = []

        gem_saved = _get_setting("stats_gemodializ_name", "Gemodializ")
        re1_saved = _get_setting("stats_reanim_neo_name", "Neonatal va kardioreanimatsiya")
        re2_saved = _get_setting("stats_reanim_umumiy_name", "Umumiy reanimatsiya va intensiv davo")

        options = excel_depts if excel_depts else primary_saved

        st.write("#### 1) 12 ta asosiy bo'lim")
        primary_new = st.multiselect(
            "12 ta asosiy bo'limlarni belgilang",
            options=options,
            default=[x for x in primary_saved if x],
            key="primary_depts_multi"
        )

        st.write("#### 2) Gemodializ")
        gem_new = st.text_input("Gemodializ bo'lim nomi", value=gem_saved, key="gem_name")

        st.write("#### 3) Reanimatsiyalar")
        reanim1_new = st.text_input("Reanimatsiya #1 nomi", value=re1_saved, key="reanim1_name")
        reanim2_new = st.text_input("Reanimatsiya #2 nomi", value=re2_saved, key="reanim2_name")

        if st.button("💾 Saqlash (Statsionar bo'lim prioritetlari)", key="save_stats_dept_priority"):
            _set_setting("stats_primary_departments", json.dumps(primary_new, ensure_ascii=False))
            _set_setting("stats_gemodializ_name", gem_new.strip())
            _set_setting("stats_reanim_neo_name", reanim1_new.strip())
            _set_setting("stats_reanim_umumiy_name", reanim2_new.strip())
            st.success("Saqlandi ✅ Endi Statsionar importni qayta qilsangiz, bo'limlar shu qoida bilan aniqlanadi.")