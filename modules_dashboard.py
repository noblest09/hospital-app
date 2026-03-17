# -*- coding: utf-8 -*-
from io import BytesIO

import pandas as pd
import streamlit as st

from database import get_conn
from utils import fmt_uzs
from modules_jami_protokol import _build_jami_table

from modules_poliklinika_doctor import (
    _load_cached_std,
    _build_doctor_registry,
    _preview_exec_amb,
    _preview_ref_amb,
    _preview_exec_stat,
    _load_selected_services,
    _load_rules,
    _build_source_summary,
    _merge_three_sources,
    _load_manual_extra,
    _apply_manual_extra,
)

try:
    import plotly.graph_objects as go
    PLOTLY_OK = True
except Exception:
    PLOTLY_OK = False


# =========================================================
# STYLE
# =========================================================
def _inject_css():
    st.markdown(
        """
        <style>
        .db-title {
            font-size: 32px;
            font-weight: 900;
            color: #0f172a;
            margin-bottom: 4px;
            letter-spacing: -0.3px;
        }
        .db-subtitle {
            color: #64748b;
            font-size: 14px;
            margin-bottom: 18px;
        }
        .kpi-card {
            position: relative;
            overflow: hidden;
            border-radius: 24px;
            padding: 18px 18px 14px 18px;
            box-shadow: 0 14px 34px rgba(15, 23, 42, 0.08);
            min-height: 128px;
            border: 1px solid rgba(255,255,255,0.50);
        }
        .kpi-card:before {
            content: "";
            position: absolute;
            right: -20px;
            top: -20px;
            width: 90px;
            height: 90px;
            border-radius: 50%;
            background: rgba(255,255,255,0.25);
            filter: blur(2px);
        }
        .kpi-blue { background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 52%, #bfdbfe 100%); }
        .kpi-green { background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 52%, #a7f3d0 100%); }
        .kpi-orange { background: linear-gradient(135deg, #fff7ed 0%, #ffedd5 52%, #fed7aa 100%); }
        .kpi-red { background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 52%, #fecaca 100%); }
        .kpi-slate { background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 52%, #cbd5e1 100%); }
        .kpi-violet { background: linear-gradient(135deg, #f5f3ff 0%, #ede9fe 52%, #ddd6fe 100%); }

        .kpi-title {
            font-size: 12px;
            font-weight: 800;
            color: #475569;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 0.7px;
        }
        .kpi-value {
            font-size: 30px;
            font-weight: 900;
            color: #0f172a;
            line-height: 1.12;
            margin-bottom: 6px;
            letter-spacing: -0.5px;
        }
        .kpi-note {
            font-size: 12px;
            color: #475569;
        }
        .panel {
            background: #ffffff;
            border: 1px solid rgba(15, 23, 42, 0.06);
            border-radius: 24px;
            padding: 16px;
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
            margin-bottom: 18px;
        }
        .summary-box {
            border-radius: 24px;
            padding: 18px;
            background: linear-gradient(135deg, #eff6ff 0%, #f0fdf4 100%);
            border: 1px solid rgba(34, 197, 94, 0.18);
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
        }
        .insight-card {
            background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
            border: 1px solid rgba(15, 23, 42, 0.06);
            border-radius: 18px;
            padding: 14px 16px;
            margin-bottom: 10px;
        }
        .insight-title {
            font-size: 12px;
            font-weight: 800;
            color: #64748b;
            text-transform: uppercase;
            margin-bottom: 4px;
            letter-spacing: 0.6px;
        }
        .insight-value {
            font-size: 16px;
            font-weight: 800;
            color: #0f172a;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _kpi_card(title: str, value, note: str = "", tone: str = "kpi-slate"):
    if isinstance(value, (int, float)):
        value = fmt_uzs(value)

    st.markdown(
        f"""
        <div class="kpi-card {tone}">
            <div class="kpi-title">{title}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# HELPERS
# =========================================================
def _safe_sum(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def _safe_num(x) -> float:
    return float(pd.to_numeric(x, errors="coerce"))


def _shorten(x, n=24):
    x = str(x)
    return x if len(x) <= n else x[: n - 1] + "…"


def _render_insight(title: str, value: str):
    st.markdown(
        f"""
        <div class="insight-card">
            <div class="insight-title">{title}</div>
            <div class="insight-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _norm_dept_name(x: str) -> str:
    s = str(x).strip()
    if not s:
        return ""
    up = s.upper().replace("’", "'")
    mapping = {
        "КУНДУЗГИ СТАЦИОНАР": "Kunduzgi statsionar",
        "КУНДУЗГИ СТАТСИОНАР": "Kunduzgi statsionar",
        "KUNDUZGI STATSIONAR": "Kunduzgi statsionar",
    }
    return mapping.get(up, s)


def _load_support_finance(year: int, month: int) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM support_finance WHERE year=? AND month=?",
            conn,
            params=(year, month),
        )
    except Exception:
        conn.close()
        return pd.DataFrame(columns=["department", "avans", "protokol"])
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["department", "avans", "protokol"])

    if "protokol" not in df.columns and "protokol_summa" in df.columns:
        df["protokol"] = df["protokol_summa"]

    if "department" not in df.columns:
        df["department"] = ""

    for col in ["avans", "protokol"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["department"] = df["department"].astype(str).str.strip()
    return df[["department", "avans", "protokol"]]


def _load_tax_settings(year: int, month: int) -> dict:
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM tax_expenses WHERE year=? AND month=?",
            conn,
            params=(year, month),
        )
    except Exception:
        conn.close()
        return {
            "avans_tax": 0.0,
            "protokol_tax": 0.0,
            "markaz_tax": 0.0,
            "kommunal": 0.0,
            "other": 0.0,
        }
    conn.close()

    if df.empty:
        return {
            "avans_tax": 0.0,
            "protokol_tax": 0.0,
            "markaz_tax": 0.0,
            "kommunal": 0.0,
            "other": 0.0,
        }

    row = df.iloc[0].to_dict()
    return {
        "avans_tax": _safe_num(row.get("avans_tax", row.get("avans_soliq_foizi", 0.0))),
        "protokol_tax": _safe_num(row.get("protokol_tax", row.get("protokol_soliq_foizi", 0.0))),
        "markaz_tax": _safe_num(row.get("markaz_tax", row.get("markaz_soliq_foizi", 0.0))),
        "kommunal": _safe_num(row.get("kommunal", 0.0)),
        "other": _safe_num(row.get("other", row.get("boshqa_xarajat", 0.0))),
    }


# =========================================================
# DORI MANBALARI
# =========================================================
def _load_statsionar_drug_by_department(year: int, month: int) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            """
            SELECT department, SUM(drug_sum) AS dori_sum
            FROM statsionar_patient
            WHERE year=? AND month=?
            GROUP BY department
            """,
            conn,
            params=(year, month),
        )
    except Exception:
        conn.close()
        return pd.DataFrame(columns=["Bo‘lim", "Dori-darmon"])
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["Bo‘lim", "Dori-darmon"])

    df["department"] = df["department"].astype(str).apply(_norm_dept_name)
    df["dori_sum"] = pd.to_numeric(df["dori_sum"], errors="coerce").fillna(0.0)
    df = df[df["department"].str.strip() != ""].copy()
    df = df[df["dori_sum"] > 0].copy()

    df = df.rename(columns={"department": "Bo‘lim", "dori_sum": "Dori-darmon"})
    return df.groupby("Bo‘lim", as_index=False)["Dori-darmon"].sum()


def _load_opd_drug_for_kunduzgi(year: int, month: int) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            """
            SELECT service_name, SUM(amount) AS jami_summa
            FROM ambulator_raw
            WHERE year=? AND month=?
            GROUP BY service_name
            """,
            conn,
            params=(year, month),
        )
    except Exception:
        conn.close()
        return pd.DataFrame(columns=["Bo‘lim", "Dori-darmon"])
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["Bo‘lim", "Dori-darmon"])

    df["service_name"] = df["service_name"].astype(str).str.strip().str.upper()
    df["jami_summa"] = pd.to_numeric(df["jami_summa"], errors="coerce").fillna(0.0)

    masks = [
        "DORI DARMON",
        "DORI-DARMON",
        "ДОРИ ДАРМОН",
        "ДОРИ-ДАРМОН",
    ]
    drug_sum = float(df.loc[df["service_name"].isin(masks), "jami_summa"].sum())

    if drug_sum <= 0:
        return pd.DataFrame(columns=["Bo‘lim", "Dori-darmon"])

    return pd.DataFrame({
        "Bo‘lim": ["Kunduzgi statsionar"],
        "Dori-darmon": [drug_sum],
    })


def _load_jami_other_drugs(jami_df: pd.DataFrame, exclude_depts: list[str]) -> pd.DataFrame:
    if jami_df is None or jami_df.empty:
        return pd.DataFrame(columns=["Bo‘lim", "Dori-darmon"])

    dept_col = "Бўлимлар номи "
    drug_col = "ДОРИ-ДАРМОН"

    if dept_col not in jami_df.columns or drug_col not in jami_df.columns:
        return pd.DataFrame(columns=["Bo‘lim", "Dori-darmon"])

    df = jami_df[[dept_col, drug_col]].copy()
    df.columns = ["Bo‘lim", "Dori-darmon"]

    df["Bo‘lim"] = df["Bo‘lim"].astype(str).apply(_norm_dept_name)
    df["Dori-darmon"] = pd.to_numeric(df["Dori-darmon"], errors="coerce").fillna(0.0)

    exclude_set = {_norm_dept_name(x) for x in exclude_depts}
    df = df[~df["Bo‘lim"].isin(exclude_set)].copy()
    df = df[df["Bo‘lim"].str.upper() != "MARKAZ"].copy()
    df = df[df["Dori-darmon"] > 0].copy()

    return df.groupby("Bo‘lim", as_index=False)["Dori-darmon"].sum()


def _build_final_drug_dashboard_df(year: int, month: int, jami_df: pd.DataFrame):
    stats_df = _load_statsionar_drug_by_department(year, month)
    opd_df = _load_opd_drug_for_kunduzgi(year, month)

    exclude_depts = []
    if not stats_df.empty:
        exclude_depts.extend(stats_df["Bo‘lim"].tolist())
    if not opd_df.empty:
        exclude_depts.extend(opd_df["Bo‘lim"].tolist())

    jami_other_df = _load_jami_other_drugs(jami_df, exclude_depts)

    final_df = pd.concat([stats_df, opd_df, jami_other_df], ignore_index=True)

    if final_df.empty:
        return (
            pd.DataFrame(columns=["Bo‘lim", "Dori-darmon", "Ulushi %"]),
            0.0,
            0.0,
            0.0,
        )

    final_df["Dori-darmon"] = pd.to_numeric(final_df["Dori-darmon"], errors="coerce").fillna(0.0)
    final_df = final_df.groupby("Bo‘lim", as_index=False)["Dori-darmon"].sum()
    final_df = final_df.sort_values("Dori-darmon", ascending=False).reset_index(drop=True)

    total = float(final_df["Dori-darmon"].sum())
    final_df["Ulushi %"] = (final_df["Dori-darmon"] / total * 100).round(1) if total > 0 else 0.0

    stats_sum = float(stats_df["Dori-darmon"].sum()) if not stats_df.empty else 0.0
    opd_sum = float(opd_df["Dori-darmon"].sum()) if not opd_df.empty else 0.0
    other_sum = float(jami_other_df["Dori-darmon"].sum()) if not jami_other_df.empty else 0.0

    return final_df, stats_sum, opd_sum, other_sum


# =========================================================
# CHARTS
# =========================================================
def _plot_vbar(df: pd.DataFrame, x_col: str, y_col: str, title: str = "", top_n: int = 12):
    if df is None or df.empty:
        st.info("Ma’lumot yo‘q.")
        return

    plot_df = df.copy()
    plot_df[y_col] = pd.to_numeric(plot_df[y_col], errors="coerce").fillna(0)
    plot_df = plot_df.sort_values(y_col, ascending=False).head(top_n).copy()
    plot_df["_label"] = plot_df[x_col].apply(lambda x: _shorten(x, 22))

    if PLOTLY_OK:
        fig = go.Figure(
            go.Bar(
                x=plot_df["_label"],
                y=plot_df[y_col],
                text=[fmt_uzs(v) for v in plot_df[y_col]],
                textposition="outside",
                cliponaxis=False,
                customdata=plot_df[[x_col]],
                hovertemplate="<b>%{customdata[0]}</b><br>Summa: %{y:,.0f}<extra></extra>",
                marker=dict(
                    color=plot_df[y_col],
                    colorscale="Blues",
                    line=dict(color="rgba(59,130,246,0.20)", width=1.2),
                ),
            )
        )

        fig.update_layout(
            title=dict(text=title, x=0.01, xanchor="left"),
            height=440,
            margin=dict(l=10, r=10, t=50, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="", tickangle=-24, showgrid=False, zeroline=False),
            yaxis=dict(title="", showgrid=True, gridcolor="rgba(148,163,184,0.15)", zeroline=False, tickformat=","),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.bar_chart(plot_df.set_index("_label")[y_col], use_container_width=True)


def _plot_hbar(df: pd.DataFrame, x_col: str, y_col: str, title: str = "", top_n: int = 10):
    if df is None or df.empty:
        st.info("Ma’lumot yo‘q.")
        return

    plot_df = df.copy()
    plot_df[y_col] = pd.to_numeric(plot_df[y_col], errors="coerce").fillna(0)
    plot_df = plot_df.sort_values(y_col, ascending=False).head(top_n).copy()
    plot_df[x_col] = plot_df[x_col].astype(str).apply(lambda x: _shorten(x, 28))
    plot_df = plot_df.iloc[::-1].copy()

    if PLOTLY_OK:
        fig = go.Figure(
            go.Bar(
                y=plot_df[x_col],
                x=plot_df[y_col],
                orientation="h",
                text=[fmt_uzs(v) for v in plot_df[y_col]],
                textposition="outside",
                cliponaxis=False,
                hovertemplate="<b>%{y}</b><br>Summa: %{x:,.0f}<extra></extra>",
                marker=dict(
                    color=plot_df[y_col],
                    colorscale="Tealgrn",
                    line=dict(color="rgba(16,185,129,0.22)", width=1),
                ),
            )
        )

        fig.update_layout(
            title=dict(text=title, x=0.01, xanchor="left"),
            height=430,
            margin=dict(l=10, r=30, t=50, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="", showgrid=True, gridcolor="rgba(148,163,184,0.15)", zeroline=False, tickformat=","),
            yaxis=dict(title="", showgrid=False, zeroline=False),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.bar_chart(plot_df.set_index(x_col)[y_col], use_container_width=True)


def _plot_donut(labels, values, title: str = ""):
    clean_values = [float(pd.to_numeric(v, errors="coerce")) for v in values]
    total = sum(clean_values)

    if total <= 0:
        st.info("Diagramma uchun ma’lumot yo‘q.")
        return

    if PLOTLY_OK:
        fig = go.Figure(
            go.Pie(
                labels=labels,
                values=clean_values,
                hole=0.64,
                textinfo="percent",
                hovertemplate="<b>%{label}</b><br>Summa: %{value:,.0f}<br>Ulush: %{percent}<extra></extra>",
            )
        )
        fig.update_layout(
            title=dict(text=title, x=0.01, xanchor="left"),
            height=380,
            margin=dict(l=10, r=10, t=50, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            annotations=[dict(
                text=f"<b>{fmt_uzs(total)}</b><br><span style='font-size:11px;color:#64748b'>Jami</span>",
                x=0.5, y=0.5, showarrow=False, font=dict(size=16, color="#0f172a")
            )],
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.18, x=0.5, xanchor="center"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write(pd.DataFrame({"Tur": labels, "Summa": clean_values}))


# =========================================================
# AMBULATOR TOP XIZMATLAR
# =========================================================
def _attach_main_group_amb(df: pd.DataFrame) -> pd.DataFrame:
    conn = get_conn()
    try:
        m = pd.read_sql_query(
            "SELECT service_name, main_group_name FROM service_main_group WHERE module='ambulator'",
            conn
        )
    except Exception:
        conn.close()
        df["main_group"] = "Guruhsiz"
        return df
    conn.close()

    if m.empty:
        df["main_group"] = "Guruhsiz"
        return df

    df2 = df.merge(m, on="service_name", how="left")
    df2["main_group"] = df2["main_group_name"].fillna("Guruhsiz")
    df2.drop(columns=["main_group_name"], inplace=True, errors="ignore")
    return df2


def _load_ambulator_top_services(year: int, month: int) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM ambulator_raw WHERE year=? AND month=?",
            conn,
            params=(year, month)
        )
    except Exception:
        conn.close()
        return pd.DataFrame(columns=["service_name", "main_group", "qty", "amount"])
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["service_name", "main_group", "qty", "amount"])

    for c in ["service_name", "qty", "amount"]:
        if c not in df.columns:
            df[c] = "" if c == "service_name" else 0.0

    df["service_name"] = df["service_name"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0.0)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    df = _attach_main_group_amb(df)

    out = df.groupby(["main_group", "service_name"], as_index=False).agg(
        xizmat_soni=("qty", "sum"),
        tushum=("amount", "sum"),
    )
    out = out.sort_values("tushum", ascending=False).reset_index(drop=True)
    return out


# =========================================================
# POLIKLINIKA TOP VRACHLAR
# =========================================================
def _build_poliklinika_dashboard_data(year: int, month: int):
    df_all = _load_cached_std(year, month)
    if df_all is None or df_all.empty:
        return pd.DataFrame(columns=["doctor_display", "Jami xizmat summasi", "Jami protokol summasi", "Yakuniy protokol"])

    registry = _build_doctor_registry(df_all)

    amb_exec_preview = _preview_exec_amb(df_all, registry)
    amb_ref_preview = _preview_ref_amb(df_all, registry)
    stat_exec_preview = _preview_exec_stat(df_all, registry)

    sel_amb_exec = _load_selected_services(year, month, "amb_exec")
    sel_amb_ref = _load_selected_services(year, month, "amb_ref")
    sel_stat_exec = _load_selected_services(year, month, "stat_exec")

    rules_amb_exec = _load_rules(year, month, "amb_exec")
    rules_amb_ref = _load_rules(year, month, "amb_ref")
    rules_stat_exec = _load_rules(year, month, "stat_exec")

    manual_df = _load_manual_extra(year, month)

    sum_ae, _ = _build_source_summary(amb_exec_preview, sel_amb_exec, rules_amb_exec, "Ambulator ijrochi")
    sum_ar, _ = _build_source_summary(amb_ref_preview, sel_amb_ref, rules_amb_ref, "Ambulator yo‘naltirgan")
    sum_se, _ = _build_source_summary(stat_exec_preview, sel_stat_exec, rules_stat_exec, "Statsionar ijrochi")

    unified = _merge_three_sources(sum_ae, sum_ar, sum_se)
    unified = _apply_manual_extra(unified, manual_df)

    if not unified.empty:
        unified = unified[pd.to_numeric(unified["Jami protokol summasi"], errors="coerce").fillna(0) > 0].copy()

    doctors_df = unified[[
        "doctor_display",
        "Jami xizmat summasi",
        "Jami protokol summasi",
        "Yakuniy protokol",
    ]].copy() if not unified.empty else pd.DataFrame(columns=[
        "doctor_display", "Jami xizmat summasi", "Jami protokol summasi", "Yakuniy protokol"
    ])

    doctors_df = doctors_df.sort_values("Jami protokol summasi", ascending=False).reset_index(drop=True)
    return doctors_df


# =========================================================
# EXCEL
# =========================================================
def _export_excel(
    summary_df: pd.DataFrame,
    main_tax_df: pd.DataFrame,
    support_df: pd.DataFrame,
    doctors_df: pd.DataFrame,
    services_df: pd.DataFrame,
    dept_rank_df: pd.DataFrame,
    drug_df: pd.DataFrame,
) -> bytes:
    output = BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book

        header_fmt = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#D9EAF7",
        })

        money_fmt = workbook.add_format({
            "num_format": "# ##0",
            "border": 1,
            "align": "right",
        })

        text_fmt = workbook.add_format({
            "border": 1,
            "align": "left",
        })

        center_fmt = workbook.add_format({
            "border": 1,
            "align": "center",
        })

        sheets = [
            ("Moliyaviy_xulosa", summary_df),
            ("Asosiy_bolimlar_soliq", main_tax_df),
            ("Yordamchi_bolimlar", support_df),
            ("Bolimlar_reytingi", dept_rank_df),
            ("Dori_tahlili", drug_df),
            ("Top_vrachlar", doctors_df),
            ("Top_xizmatlar", services_df),
        ]

        for sheet_name, df in sheets:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            ws = writer.sheets[sheet_name]

            for col_num, col_name in enumerate(df.columns):
                ws.write(0, col_num, col_name, header_fmt)

                col_lower = str(col_name).lower()
                if any(x in col_lower for x in ["summa", "tushum", "avans", "protokol", "oylik", "soliq", "yakuniy", "ish", "dori"]):
                    ws.set_column(col_num, col_num, 18, money_fmt)
                elif any(x in col_lower for x in ["soni", "qty", "%"]):
                    ws.set_column(col_num, col_num, 14, center_fmt)
                else:
                    ws.set_column(col_num, col_num, 28, text_fmt)

    return output.getvalue()


# =========================================================
# MAIN
# =========================================================
def render_dashboard(year: int, month_name: str, uz_months: list[str]):
    _inject_css()

    month = uz_months.index(month_name) + 1

    st.markdown('<div class="db-title">📊 Boshqaruv paneli</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="db-subtitle">Asosiy moliyaviy ko‘rsatkichlar, bo‘limlar reytingi, dori tahlili, top xizmatlar va vrachlar</div>',
        unsafe_allow_html=True,
    )

    try:
        df = _build_jami_table(year, month)
    except Exception as e:
        st.error(f"Dashboard ma’lumotini olishda xatolik: {e}")
        return

    if df.empty:
        st.warning("Tanlangan oy uchun ma’lumot topilmadi.")
        return

    df["Бўлимлар номи "] = df["Бўлимлар номи "].astype(str).str.strip()

    markaz_row = df[df["Бўлимлар номи "].str.upper() == "MARKAZ"].copy()
    main_depts = df[df["Бўлимлар номи "].str.upper() != "MARKAZ"].copy()

    for c in [
        "ЖАМИ ҚИЛГАН ИШИ", "ЖАМИ ПРОТОКОЛ СУММАСИ", "ЯКУНИЙ ПРОТОКОЛ",
        "ИШ ХАҚҚИ ОКЛАД (АВАНС)"
    ]:
        if c not in df.columns:
            df[c] = 0.0
        if c not in main_depts.columns:
            main_depts[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        main_depts[c] = pd.to_numeric(main_depts[c], errors="coerce").fillna(0.0)

    tax = _load_tax_settings(year, month)
    support = _load_support_finance(year, month)

    jami_ish = _safe_sum(df, "ЖАМИ ҚИЛГАН ИШИ")
    asosiy_yakuniy_protokol = _safe_sum(main_depts, "ЯКУНИЙ ПРОТОКОЛ")
    yordamchi_protokol = _safe_sum(support, "protokol")
    jami_protokol = asosiy_yakuniy_protokol + yordamchi_protokol
    markaz_baza = _safe_sum(markaz_row, "ЯКУНИЙ ПРОТОКОЛ")

    drug_df, stats_dori_sum, opd_dori_sum, other_dori_sum = _build_final_drug_dashboard_df(year, month, df)
    jami_dori = _safe_sum(drug_df, "Dori-darmon")

    main_tax_df = main_depts[["Бўлимлар номи ", "ИШ ХАҚҚИ ОКЛАД (АВАНС)", "ЯКУНИЙ ПРОТОКОЛ", "ЖАМИ ПРОТОКОЛ СУММАСИ"]].copy()
    main_tax_df.columns = ["Bo‘lim", "Avans", "Yakuniy protokol", "Jami protokol"]
    main_tax_df["Oylik"] = main_tax_df["Avans"] + main_tax_df["Yakuniy protokol"]
    main_tax_df["Soliq"] = (
        main_tax_df["Avans"] * tax["avans_tax"] / 100
        + main_tax_df["Yakuniy protokol"] * tax["protokol_tax"] / 100
    )
    asosiy_soliq = _safe_sum(main_tax_df, "Soliq")

    if support.empty:
        support_df = pd.DataFrame(columns=["Bo‘lim", "Avans", "Protokol", "Oylik", "Soliq"])
        yordamchi_oylik = 0.0
        yordamchi_soliq = 0.0
    else:
        support_df = support.copy()
        support_df.columns = ["Bo‘lim", "Avans", "Protokol"]
        support_df["Oylik"] = support_df["Avans"] + support_df["Protokol"]
        support_df["Soliq"] = (
            support_df["Avans"] * tax["avans_tax"] / 100
            + support_df["Protokol"] * tax["protokol_tax"] / 100
        )
        yordamchi_oylik = _safe_sum(support_df, "Oylik")
        yordamchi_soliq = _safe_sum(support_df, "Soliq")

    jami_soliq = asosiy_soliq + yordamchi_soliq
    qolgan_summa = markaz_baza - yordamchi_oylik - jami_soliq - tax["kommunal"] - tax["other"]
    markaz_soliq = max(qolgan_summa, 0.0) * tax["markaz_tax"] / 100
    sof_foyda = qolgan_summa - markaz_soliq

    doctors_df = _build_poliklinika_dashboard_data(year, month)
    services_df = _load_ambulator_top_services(year, month)

    dept_rank_df = main_depts[["Бўлимлар номи ", "ЖАМИ ҚИЛГАН ИШИ", "ЯКУНИЙ ПРОТОКОЛ"]].copy()
    dept_rank_df.columns = ["Bo‘lim", "Jami ish", "Yakuniy protokol"]
    dept_rank_df = dept_rank_df.sort_values("Jami ish", ascending=False).reset_index(drop=True)

    # KPI
    r1c1, r1c2, r1c3, r1c4 = st.columns(4)
    with r1c1:
        _kpi_card("Jami qilingan ish", jami_ish, f"{month_name} {year}", "kpi-blue")
    with r1c2:
        _kpi_card("Jami protokol", jami_protokol, "Yakuniy protokol + qo‘shimcha bo‘limlar", "kpi-slate")
    with r1c3:
        _kpi_card("Jami dori-darmon", jami_dori, "3 manbadan yig‘ilgan", "kpi-red")
    with r1c4:
        _kpi_card("Sof foyda", sof_foyda, "Hamma ayirmalardan keyin", "kpi-green")

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    with r2c1:
        _kpi_card("MARKAZ yakuniy protokoli", markaz_baza, "Asosiy baza summa", "kpi-orange")
    with r2c2:
        _kpi_card("Yordamchi bo‘limlar oyligi", yordamchi_oylik, "Avans + protokol", "kpi-violet")
    with r2c3:
        _kpi_card("Jami soliq", jami_soliq, "Asosiy + yordamchi bo‘limlar", "kpi-red")
    with r2c4:
        _kpi_card("Boshqa xarajat", tax["other"], "Moliyaviy sozlamalardan", "kpi-orange")

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    r3c1, r3c2, r3c3 = st.columns(3)
    with r3c1:
        _kpi_card("Kommunal", tax["kommunal"], "Moliyaviy sozlamalardan", "kpi-orange")
    with r3c2:
        _kpi_card("Qolgan summa", qolgan_summa, "MARKAZ baza - ayirmalar", "kpi-slate")
    with r3c3:
        _kpi_card("Markaz solig‘i", markaz_soliq, f"{tax['markaz_tax']:.1f}% bo‘yicha", "kpi-red")

    st.divider()

    c1, c2 = st.columns(2)

    with c1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Dori-darmon manbalari")

        if jami_dori > 0:
            _plot_donut(
                ["Statsionar", "OPD → Kunduzgi statsionar", "Qolgan bo‘limlar"],
                [stats_dori_sum, opd_dori_sum, other_dori_sum],
                "Dori manbalari taqsimoti"
            )
            k1, k2, k3 = st.columns(3)
            with k1:
                _kpi_card("Statsionar dori", stats_dori_sum, "Statsionar bo‘limlar", "kpi-blue")
            with k2:
                _kpi_card("OPD dori", opd_dori_sum, "Kunduzgi statsionarga", "kpi-violet")
            with k3:
                _kpi_card("Qolgan bo‘limlar", other_dori_sum, "Jami protokoldan", "kpi-orange")
        else:
            st.warning("Bu oy uchun dori ma’lumoti topilmadi.")
        st.markdown('</div>', unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Bo‘limlar reytingi")
        metric = st.radio("Reyting ko‘rsatkichi", ["Jami ish", "Yakuniy protokol"], horizontal=True, key="dept_rank_metric")
        _plot_hbar(dept_rank_df, "Bo‘lim", metric, f"Bo‘limlar reytingi — {metric}", top_n=10)

        rank_show = dept_rank_df.sort_values(metric, ascending=False).copy()
        rank_show["Jami ish"] = rank_show["Jami ish"].apply(fmt_uzs)
        rank_show["Yakuniy protokol"] = rank_show["Yakuniy protokol"].apply(fmt_uzs)
        st.dataframe(rank_show, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("🧾 Bo‘limlar bo‘yicha dori-darmon")
    if not drug_df.empty:
        _plot_hbar(drug_df, "Bo‘lim", "Dori-darmon", "Dori-darmon xarajati reytingi", top_n=12)

        show_drug = drug_df.copy()
        show_drug["Dori-darmon"] = show_drug["Dori-darmon"].apply(fmt_uzs)
        show_drug["Ulushi %"] = show_drug["Ulushi %"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(show_drug, use_container_width=True, hide_index=True)
    else:
        st.info("Bo‘limlar bo‘yicha dori ma’lumoti topilmadi.")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("🧪 Top xizmatlar")
    if not services_df.empty:
        _plot_vbar(services_df, "service_name", "tushum", "Ambulator bo‘yicha top xizmatlar", top_n=12)
        show = services_df.copy().rename(columns={
            "main_group": "Asosiy guruh",
            "service_name": "Xizmat",
            "xizmat_soni": "Xizmat soni",
            "tushum": "Tushum",
        })
        show["Xizmat soni"] = show["Xizmat soni"].apply(lambda x: f"{float(x):,.0f}".replace(",", " "))
        show["Tushum"] = show["Tushum"].apply(fmt_uzs)
        st.dataframe(show, use_container_width=True, hide_index=True)
    else:
        st.info("Top xizmatlar uchun ambulator ma’lumot topilmadi.")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("👨‍⚕️ Top vrachlar")
    if not doctors_df.empty:
        _plot_vbar(doctors_df, "doctor_display", "Jami protokol summasi", "Faqat foiz berilgan vrachlar", top_n=12)
        show = doctors_df.copy().rename(columns={"doctor_display": "Vrach"})
        for col in ["Jami xizmat summasi", "Jami protokol summasi", "Yakuniy protokol"]:
            show[col] = show[col].apply(fmt_uzs)
        st.dataframe(show, use_container_width=True, hide_index=True)
    else:
        st.info("Foiz berilgan vrachlar topilmadi.")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("💡 Smart insights")
    i1, i2 = st.columns(2)
    with i1:
        if not dept_rank_df.empty:
            top_dept = dept_rank_df.sort_values("Jami ish", ascending=False).iloc[0]
            _render_insight("Eng kuchli bo‘lim", f"{top_dept['Bo‘lim']} — {fmt_uzs(top_dept['Jami ish'])}")
        if not services_df.empty:
            top_service = services_df.sort_values("tushum", ascending=False).iloc[0]
            _render_insight("Eng kuchli xizmat", f"{top_service['service_name']} — {fmt_uzs(top_service['tushum'])}")
        if not drug_df.empty:
            top_drug = drug_df.iloc[0]
            _render_insight("Eng katta dori xarajati", f"{top_drug['Bo‘lim']} — {fmt_uzs(top_drug['Dori-darmon'])}")
    with i2:
        if not doctors_df.empty:
            top_doc = doctors_df.sort_values("Jami protokol summasi", ascending=False).iloc[0]
            _render_insight("Eng kuchli vrach", f"{top_doc['doctor_display']} — {fmt_uzs(top_doc['Jami protokol summasi'])}")
        _render_insight("Sof foyda holati", fmt_uzs(sof_foyda))
        _render_insight("Jami protokol formulasi", "Asosiy yakuniy protokol + qo‘shimcha bo‘limlar protokoli")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("💸 Asosiy bo‘limlar: oylik va soliq")
    show = main_tax_df.copy()
    for col in ["Avans", "Yakuniy protokol", "Jami protokol", "Oylik", "Soliq"]:
        show[col] = show[col].apply(fmt_uzs)
    st.dataframe(show, use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("🏢 Yordamchi bo‘limlar")
    if support_df.empty:
        st.info("Yordamchi bo‘limlar kiritilmagan.")
    else:
        show = support_df.copy()
        for col in ["Avans", "Protokol", "Oylik", "Soliq"]:
            show[col] = show[col].apply(fmt_uzs)
        st.dataframe(show, use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="summary-box">', unsafe_allow_html=True)
    st.subheader("🧾 Qisqa moliyaviy xulosa")
    parts = [
        f"Joriy oyda jami qilingan ish **{fmt_uzs(jami_ish)}** bo‘ldi.",
        f"Dashboarddagi jami protokol **{fmt_uzs(jami_protokol)}** bo‘ldi.",
        f"Jami dori-darmon **{fmt_uzs(jami_dori)}** bo‘ldi.",
        f"Bu dori 3 manbadan yig‘ildi: statsionar, OPD va qolgan bo‘limlar.",
        f"MARKAZ yakuniy protokoli **{fmt_uzs(markaz_baza)}**, qolgan summa **{fmt_uzs(qolgan_summa)}**, sof foyda esa **{fmt_uzs(sof_foyda)}** bo‘ldi.",
    ]
    st.markdown("\n\n".join(parts))
    st.markdown('</div>', unsafe_allow_html=True)

    st.divider()
    st.subheader("⬇️ Excel eksport")

    summary_df = pd.DataFrame({
        "Ko‘rsatkich": [
            "Jami qilingan ish",
            "Asosiy bo‘limlar yakuniy protokoli",
            "Yordamchi bo‘limlar protokoli",
            "Jami protokol",
            "Jami dori-darmon",
            "MARKAZ yakuniy protokoli",
            "Yordamchi bo‘limlar oyligi",
            "Asosiy bo‘limlar solig‘i",
            "Yordamchi bo‘limlar solig‘i",
            "Jami soliq",
            "Kommunal",
            "Boshqa xarajat",
            "Qolgan summa",
            "Markaz solig‘i",
            "Sof foyda",
        ],
        "Summa": [
            jami_ish,
            asosiy_yakuniy_protokol,
            yordamchi_protokol,
            jami_protokol,
            jami_dori,
            markaz_baza,
            yordamchi_oylik,
            asosiy_soliq,
            yordamchi_soliq,
            jami_soliq,
            tax["kommunal"],
            tax["other"],
            qolgan_summa,
            markaz_soliq,
            sof_foyda,
        ]
    })

    excel_bytes = _export_excel(
        summary_df=summary_df,
        main_tax_df=main_tax_df,
        support_df=support_df,
        doctors_df=doctors_df,
        services_df=services_df,
        dept_rank_df=dept_rank_df,
        drug_df=drug_df,
    )

    st.download_button(
        label="📥 Boshqaruv panelini Excelga yuklab olish",
        data=excel_bytes,
        file_name=f"Boshqaruv_paneli_{year}_{month}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
