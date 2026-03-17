# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd

from database import get_conn
from utils import fmt_uzs


# ================================
# DATABASE TABLES
# ================================
def ensure_tables():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS support_departments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS support_finance (
        year INTEGER,
        month INTEGER,
        department TEXT,
        avans REAL DEFAULT 0,
        protokol REAL DEFAULT 0,
        UNIQUE(year,month,department)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tax_expenses (
        year INTEGER,
        month INTEGER,
        avans_tax REAL DEFAULT 0,
        protokol_tax REAL DEFAULT 0,
        markaz_tax REAL DEFAULT 0,
        kommunal REAL DEFAULT 0,
        other REAL DEFAULT 0,
        UNIQUE(year,month)
    )
    """)

    conn.commit()
    conn.close()


# ================================
# LOAD DEPARTMENTS
# ================================
def load_departments():

    conn = get_conn()

    df = pd.read_sql_query(
        "SELECT * FROM support_departments ORDER BY name",
        conn
    )

    conn.close()

    return df


# ================================
# RENDER PAGE
# ================================
def render_extra_settings(selected_year, selected_month_name, uz_months):

    ensure_tables()

    month = uz_months.index(selected_month_name) + 1

    st.header("⚙️ Moliyaviy sozlamalar")

    tab1, tab2, tab3 = st.tabs(
        [
            "🏢 Yordamchi bo‘limlar",
            "💰 Bo‘limlar summasi",
            "💳 Soliq va xarajatlar"
        ]
    )

# =====================================================
# TAB 1 — YORDAMCHI BO‘LIMLAR
# =====================================================

    with tab1:

        st.subheader("Bo‘limlarni boshqarish")

        df = load_departments()

        edited = st.data_editor(
            df,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "id": st.column_config.NumberColumn(
                    "ID",
                    disabled=True
                ),
                "name": st.column_config.TextColumn(
                    "Bo‘lim nomi"
                )
            }
        )

        if st.button("💾 Bo‘limlarni saqlash"):

            conn = get_conn()
            cur = conn.cursor()

            cur.execute("DELETE FROM support_departments")

            for _, r in edited.iterrows():

                name = str(r["name"]).strip()

                if name != "":

                    cur.execute(
                        "INSERT INTO support_departments(name) VALUES(?)",
                        (name,)
                    )

            conn.commit()
            conn.close()

            st.success("Bo‘limlar saqlandi")
            st.rerun()

# =====================================================
# TAB 2 — BO‘LIMLAR SUMMASI
# =====================================================

    with tab2:

        st.subheader("Yordamchi bo‘limlar summasi")

        conn = get_conn()

        depts = pd.read_sql_query(
            "SELECT name FROM support_departments",
            conn
        )

        finance = pd.read_sql_query(
            """
            SELECT department,avans,protokol
            FROM support_finance
            WHERE year=? AND month=?
            """,
            conn,
            params=(selected_year, month)
        )

        conn.close()

        df = depts.rename(columns={"name": "department"}).merge(
            finance,
            on="department",
            how="left"
        )

        df["avans"] = df["avans"].fillna(0)
        df["protokol"] = df["protokol"].fillna(0)

        edited = st.data_editor(
            df,
            use_container_width=True
        )

        col1, col2 = st.columns(2)

        with col1:

            if st.button("💾 Summalarni saqlash"):

                conn = get_conn()
                cur = conn.cursor()

                for _, r in edited.iterrows():

                    cur.execute("""
                    INSERT OR REPLACE INTO support_finance
                    (year,month,department,avans,protokol)
                    VALUES (?,?,?,?,?)
                    """, (
                        selected_year,
                        month,
                        r["department"],
                        float(r["avans"]),
                        float(r["protokol"])
                    ))

                conn.commit()
                conn.close()

                st.success("Saqlandi")
                st.rerun()

        with col2:

            jami_avans = edited["avans"].sum()
            jami_protokol = edited["protokol"].sum()

            st.metric("Jami avans", fmt_uzs(jami_avans))
            st.metric("Jami protokol", fmt_uzs(jami_protokol))

# =====================================================
# TAB 3 — SOLIQ VA XARAJATLAR
# =====================================================

    with tab3:

        st.subheader("Soliq va xarajatlar")

        conn = get_conn()

        tax = pd.read_sql_query(
            """
            SELECT *
            FROM tax_expenses
            WHERE year=? AND month=?
            """,
            conn,
            params=(selected_year, month)
        )

        conn.close()

        if tax.empty:

            avans_tax = 12.0
            protokol_tax = 12.0
            markaz_tax = 12.0
            kommunal = 0.0
            other = 0.0

        else:

            row = tax.iloc[0]

            avans_tax = row["avans_tax"]
            protokol_tax = row["protokol_tax"]
            markaz_tax = row["markaz_tax"]
            kommunal = row["kommunal"]
            other = row["other"]

        col1, col2, col3 = st.columns(3)

        with col1:
            avans_tax = st.number_input(
                "Avans solig‘i %",
                value=float(avans_tax)
            )

        with col2:
            protokol_tax = st.number_input(
                "Protokol solig‘i %",
                value=float(protokol_tax)
            )

        with col3:
            markaz_tax = st.number_input(
                "Markaz solig‘i %",
                value=float(markaz_tax)
            )

        col4, col5 = st.columns(2)

        with col4:
            kommunal = st.number_input(
                "Kommunal",
                step=1000.0,
                value=float(kommunal)
            )

        with col5:
            other = st.number_input(
                "Boshqa xarajat",
                step=1000.0,
                value=float(other)
            )

        if st.button("💾 Soliq va xarajatlarni saqlash"):

            conn = get_conn()
            cur = conn.cursor()

            cur.execute("""
            INSERT OR REPLACE INTO tax_expenses
            (year,month,avans_tax,protokol_tax,markaz_tax,kommunal,other)
            VALUES (?,?,?,?,?,?,?)
            """, (
                selected_year,
                month,
                avans_tax,
                protokol_tax,
                markaz_tax,
                kommunal,
                other
            ))

            conn.commit()
            conn.close()

            st.success("Saqlandi")
            st.rerun()