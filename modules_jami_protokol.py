# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from io import BytesIO

from database import get_conn
from utils import fmt_uzs

from modules_foiz import _build_protocol_table as _build_stats_protocol
from modules_foiz_ambulator import _build_protocol_table as _build_amb_protocol

CENTER_NAME = "MARKAZ"


def _ensure_tables():
    conn = get_conn()
    cur = conn.cursor()

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

    conn.commit()
    conn.close()


def _load_manual(year: int, month: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT department, avans, rentabillik FROM jami_manual WHERE year=? AND month=?",
        conn,
        params=(year, month),
    )
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["department", "avans", "rentabillik"])

    df["department"] = df["department"].astype(str).str.strip()
    df["avans"] = pd.to_numeric(df["avans"], errors="coerce").fillna(0.0)
    df["rentabillik"] = pd.to_numeric(df["rentabillik"], errors="coerce").fillna(0.0)
    return df


def _save_manual(year: int, month: int, df: pd.DataFrame):
    df2 = df.copy()
    df2["department"] = df2["department"].astype(str).str.strip()
    df2["avans"] = pd.to_numeric(df2["avans"], errors="coerce").fillna(0.0)
    df2["rentabillik"] = pd.to_numeric(df2["rentabillik"], errors="coerce").fillna(0.0)

    conn = get_conn()
    cur = conn.cursor()

    for _, r in df2.iterrows():
        cur.execute(
            """
            UPDATE jami_manual
            SET avans=?, rentabillik=?
            WHERE year=? AND month=? AND department=?
            """,
            (float(r["avans"]), float(r["rentabillik"]), year, month, r["department"])
        )
        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT INTO jami_manual(year, month, department, avans, rentabillik)
                VALUES(?,?,?,?,?)
                """,
                (year, month, r["department"], float(r["avans"]), float(r["rentabillik"]))
            )

    conn.commit()
    conn.close()


def _prev_year_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _extract_protocol_work_drug(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["department", "proto_sum", "jami_qilgan_ish", "dori_darmon"])

    dep_col = "Бўлимлар номи "
    proto_col = "ПРОТАКОЛ СУММА"
    work_col = "ЖАМИ ҚИЛГАН ИШИ"
    drug_col = "ДОРИ-ДАРМОН"

    cols = [dep_col, proto_col, work_col]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        return pd.DataFrame(columns=["department", "proto_sum", "jami_qilgan_ish", "dori_darmon"])

    out = df[[c for c in [dep_col, proto_col, work_col, drug_col] if c in df.columns]].copy()
    out = out.rename(columns={
        dep_col: "department",
        proto_col: "proto_sum",
        work_col: "jami_qilgan_ish",
        drug_col: "dori_darmon",
    })

    if "dori_darmon" not in out.columns:
        out["dori_darmon"] = 0.0

    out["department"] = out["department"].astype(str).str.strip()
    out["proto_sum"] = pd.to_numeric(out["proto_sum"], errors="coerce").fillna(0.0)
    out["jami_qilgan_ish"] = pd.to_numeric(out["jami_qilgan_ish"], errors="coerce").fillna(0.0)
    out["dori_darmon"] = pd.to_numeric(out["dori_darmon"], errors="coerce").fillna(0.0)
    return out


def _apply_center_rent_logic(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if out.empty:
        return out

    out["department"] = out["department"].astype(str).str.strip()
    out["rentabillik"] = pd.to_numeric(out["rentabillik"], errors="coerce").fillna(0.0)

    non_center_rent = float(out.loc[out["department"] != CENTER_NAME, "rentabillik"].sum())

    if (out["department"] == CENTER_NAME).any():
        out.loc[out["department"] == CENTER_NAME, "rentabillik"] = -non_center_rent
    else:
        new_row = {c: 0.0 for c in out.columns if c != "department"}
        new_row["department"] = CENTER_NAME
        new_row["rentabillik"] = -non_center_rent
        out = pd.concat([out, pd.DataFrame([new_row])], ignore_index=True)

    return out


def _recalculate_center_protocols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if out.empty:
        return out

    out["department"] = out["department"].astype(str).str.strip()

    numeric_cols = [
        "statsionar_sum",
        "ambulator_sum",
        "statsionar_ish",
        "ambulator_ish",
        "statsionar_dori",
        "ambulator_dori",
    ]
    for c in numeric_cols:
        if c not in out.columns:
            out[c] = 0.0
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    mask_center = out["department"].str.upper() == CENTER_NAME
    mask_non_center = ~mask_center

    total_statsionar_ish = float(out["statsionar_ish"].sum())
    total_ambulator_ish = float(out["ambulator_ish"].sum())

    non_center_statsionar_proto = float(out.loc[mask_non_center, "statsionar_sum"].sum())
    non_center_ambulator_proto = float(out.loc[mask_non_center, "ambulator_sum"].sum())

    center_statsionar_proto = total_statsionar_ish - non_center_statsionar_proto
    center_ambulator_proto = total_ambulator_ish - non_center_ambulator_proto

    center_statsionar_proto = max(center_statsionar_proto, 0.0)
    center_ambulator_proto = max(center_ambulator_proto, 0.0)

    if mask_center.any():
        out.loc[mask_center, "statsionar_sum"] = center_statsionar_proto
        out.loc[mask_center, "ambulator_sum"] = center_ambulator_proto
    else:
        new_row = {c: 0.0 for c in out.columns if c != "department"}
        new_row["department"] = CENTER_NAME
        new_row["statsionar_sum"] = center_statsionar_proto
        new_row["ambulator_sum"] = center_ambulator_proto
        out = pd.concat([out, pd.DataFrame([new_row])], ignore_index=True)

    return out


def _merge_prev_month(year: int, month: int) -> pd.DataFrame:
    py, pm = _prev_year_month(year, month)

    stats_prev, _ = _build_stats_protocol(py, pm, "all")
    amb_prev, _ = _build_amb_protocol(py, pm)

    stats_p = _extract_protocol_work_drug(stats_prev).rename(columns={
        "proto_sum": "stats_prev",
        "jami_qilgan_ish": "stats_prev_ish",
        "dori_darmon": "stats_prev_drug",
    })
    amb_p = _extract_protocol_work_drug(amb_prev).rename(columns={
        "proto_sum": "amb_prev",
        "jami_qilgan_ish": "amb_prev_ish",
        "dori_darmon": "amb_prev_drug",
    })

    prev = stats_p.merge(amb_p, on="department", how="outer")
    prev["department"] = prev["department"].astype(str).str.strip()

    for c in ["stats_prev", "amb_prev", "stats_prev_ish", "amb_prev_ish", "stats_prev_drug", "amb_prev_drug"]:
        prev[c] = pd.to_numeric(prev[c], errors="coerce").fillna(0.0)

    prev["jami_prev"] = prev["stats_prev"] + prev["amb_prev"]

    prev_manual = _load_manual(py, pm)
    prev = prev.merge(prev_manual, on="department", how="left")
    prev["avans"] = pd.to_numeric(prev["avans"], errors="coerce").fillna(0.0)
    prev["rentabillik"] = pd.to_numeric(prev["rentabillik"], errors="coerce").fillna(0.0)

    prev = _apply_center_rent_logic(prev)
    prev["oldingi_yakuniy"] = prev["jami_prev"] - prev["avans"] + prev["rentabillik"]

    return prev[["department", "oldingi_yakuniy"]]


def _build_jami_table(year: int, month: int) -> pd.DataFrame:
    stats_protocol, _ = _build_stats_protocol(year, month, "all")
    amb_protocol, _ = _build_amb_protocol(year, month)

    stats = _extract_protocol_work_drug(stats_protocol).rename(columns={
        "proto_sum": "statsionar_sum",
        "jami_qilgan_ish": "statsionar_ish",
        "dori_darmon": "statsionar_dori",
    })
    amb = _extract_protocol_work_drug(amb_protocol).rename(columns={
        "proto_sum": "ambulator_sum",
        "jami_qilgan_ish": "ambulator_ish",
        "dori_darmon": "ambulator_dori",
    })

    df = stats.merge(amb, on="department", how="outer")
    df["department"] = df["department"].astype(str).str.strip()

    for c in [
        "statsionar_sum", "ambulator_sum",
        "statsionar_ish", "ambulator_ish",
        "statsionar_dori", "ambulator_dori"
    ]:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df = _recalculate_center_protocols(df)

    df["jami_qilgan_ish"] = df["statsionar_ish"] + df["ambulator_ish"]
    df["dori_darmon"] = df["statsionar_dori"] + df["ambulator_dori"]
    df["sof_ish"] = (df["jami_qilgan_ish"] - df["dori_darmon"]).clip(lower=0.0)
    df["jami_sum"] = df["statsionar_sum"] + df["ambulator_sum"]

    manual = _load_manual(year, month)
    df = df.merge(manual, on="department", how="left")
    df["avans"] = pd.to_numeric(df["avans"], errors="coerce").fillna(0.0)
    df["rentabillik"] = pd.to_numeric(df["rentabillik"], errors="coerce").fillna(0.0)

    df = _apply_center_rent_logic(df)
    df["yakuniy_protokol"] = df["jami_sum"] - df["avans"] + df["rentabillik"]

    prev = _merge_prev_month(year, month)
    df = df.merge(prev, on="department", how="left")
    df["oldingi_yakuniy"] = pd.to_numeric(df["oldingi_yakuniy"], errors="coerce").fillna(0.0)

    df["_sort"] = df["department"].apply(lambda x: 999999 if str(x).strip().upper() == CENTER_NAME else 0)
    df = df.sort_values(["_sort", "sof_ish"], ascending=[True, False]).drop(columns=["_sort"])
    df = df.reset_index(drop=True)
    df.insert(0, "№", range(1, len(df) + 1))

    final = pd.DataFrame({
        "№": df["№"],
        "Бўлимлар номи ": df["department"],
        "ЖАМИ ҚИЛГАН ИШИ": df["jami_qilgan_ish"],
        "ДОРИ-ДАРМОН": df["dori_darmon"],
        "СОФ ИШ": df["sof_ish"],
        "СТАЦИОНАР ЖАМИ ИШИ": df["statsionar_ish"],
        "АМБУЛАТОР ЖАМИ ИШИ": df["ambulator_ish"],
        "ОЛДИНГИ ОЙ ЯКУНИЙ ПРОТОКОЛ": df["oldingi_yakuniy"],
        "СТАЦИОНАР ПРОТОКОЛ СУММАСИ": df["statsionar_sum"],
        "АМБУЛАТОР ПРОТОКОЛ СУММАСИ": df["ambulator_sum"],
        "ЖАМИ ПРОТОКОЛ СУММАСИ": df["jami_sum"],
        "РЕНТАБИЛЛИК ХИСОБИДАН": df["rentabillik"],
        "ИШ ХАҚҚИ ОКЛАД (АВАНС)": df["avans"],
        "ЯКУНИЙ ПРОТОКОЛ": df["yakuniy_protokol"],
    })

    return final


def _excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Jami_Protokol")
    return output.getvalue()


def render_jami_protokol(selected_year: int, selected_month_name: str, uz_months: list[str]):
    _ensure_tables()
    month = uz_months.index(selected_month_name) + 1

    st.header("📊 Jami protokol")
    st.caption(
        "Bu yerda Statsionar + Ambulator protokollari bo‘limlar kesimida qo‘shiladi. "
        "Dori-darmon bitta ustunda jamlanadi. "
        "Rentabillik non-center bo‘limlarga berilsa, MARKAZdan ayriladi."
    )

    if st.button("✅ Hisoblash", key="jami_calc"):
        try:
            st.session_state["jami_protocol_df"] = _build_jami_table(selected_year, month)
            st.session_state["jami_protocol_meta"] = {"year": selected_year, "month": month}
            st.success("Hisoblandi ✅")
        except Exception as e:
            st.error(str(e))
            return

    if "jami_protocol_df" not in st.session_state or "jami_protocol_meta" not in st.session_state:
        st.info("Natijani ko‘rish uchun 'Hisoblash' tugmasini bosing.")
        return

    if st.session_state["jami_protocol_meta"] != {"year": selected_year, "month": month}:
        st.warning("Oy/Yil o‘zgargan. Qayta Hisoblash bosing.")
        return

    protocol_df = st.session_state["jami_protocol_df"]

    st.markdown("### ✍️ Avans va Rentabillik")
    manual_df = protocol_df[["Бўлимлар номи ", "ИШ ХАҚҚИ ОКЛАД (АВАНС)", "РЕНТАБИЛЛИК ХИСОБИДАН"]].copy()
    manual_df = manual_df.rename(columns={
        "Бўлимлар номи ": "department",
        "ИШ ХАҚҚИ ОКЛАД (АВАНС)": "avans",
        "РЕНТАБИЛЛИК ХИСОБИДАН": "rentabillik",
    })

    edited = st.data_editor(
        manual_df,
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "department": st.column_config.TextColumn("Bo‘lim", disabled=True),
            "avans": st.column_config.NumberColumn("Avans", min_value=0.0, step=1000.0),
            "rentabillik": st.column_config.NumberColumn("Rentabillik", step=1000.0),
        },
        key="jami_manual_editor"
    )

    if st.button("💾 Avans/Rentabillikni saqlash", key="jami_save_manual"):
        try:
            _save_manual(selected_year, month, edited)
            st.session_state["jami_protocol_df"] = _build_jami_table(selected_year, month)
            st.session_state["jami_protocol_meta"] = {"year": selected_year, "month": month}
            st.success("Saqlandi ✅")
            st.rerun()
        except Exception as e:
            st.error(str(e))
            return

    st.markdown("### 📋 Jami protokol jadvali")
    pretty = protocol_df.copy()

    money_cols = [
        "ЖАМИ ҚИЛГАН ИШИ",
        "ДОРИ-ДАРМОН",
        "СОФ ИШ",
        "СТАЦИОНАР ЖАМИ ИШИ",
        "АМБУЛАТОР ЖАМИ ИШИ",
        "ОЛДИНГИ ОЙ ЯКУНИЙ ПРОТОКОЛ",
        "СТАЦИОНАР ПРОТОКОЛ СУММАСИ",
        "АМБУЛАТОР ПРОТОКОЛ СУММАСИ",
        "ЖАМИ ПРОТОКОЛ СУММАСИ",
        "РЕНТАБИЛЛИК ХИСОБИДАН",
        "ИШ ХАҚҚИ ОКЛАД (АВАНС)",
        "ЯКУНИЙ ПРОТОКОЛ",
    ]
    for c in money_cols:
        pretty[c] = pretty[c].apply(lambda x: fmt_uzs(float(x)))

    st.dataframe(pretty, use_container_width=True)

    st.markdown("### ⬇️ Excelga yuklab olish")
    st.download_button(
        label="Excel yuklab olish",
        data=_excel_bytes(protocol_df),
        file_name=f"Jami_Protokol_{selected_year}_{month}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )