# -*- coding: utf-8 -*-
import streamlit as st
from pathlib import Path

from database import init_db
from utils import UZ_MONTHS

# Asosiy modullar
from modules_settings import render_settings
from modules_ambulator import render_ambulator
from modules_statsionar import render_statsionar
from modules_dashboard import render_dashboard

# Foiz modullari
from modules_foiz import render_foiz
from modules_foiz_ambulator import render_foiz_ambulator

# Yangi modullar
from modules_poliklinika_doctor import render_poliklinika_doctor
from modules_jami_protokol import render_jami_protokol

# MUHIM
st.set_page_config(page_title="Kasalxona Hisobot Tizimi", layout="wide")

# DB init
init_db(force_recreate=False)

# =========================
# HEADER
# =========================
st.caption("Statsionar + Ambulator (OPD) | by Tibliyev")

col1, col2 = st.columns([1, 12])

with col1:
    logo_path = Path("assets/logo.png")
    if logo_path.exists():
        st.image(str(logo_path), width=90)
    else:
        st.warning("Logo topilmadi: assets/logo.png")

with col2:
    st.markdown(
        "<h1 style='margin-bottom:0px; padding-top:18px;'>Bolalar Milliy Tibbiyot Markazi</h1>",
        unsafe_allow_html=True
    )

st.divider()

# =========================
# SIDEBAR
# =========================
st.sidebar.header("📅 Hisobot oyi")

year = st.sidebar.selectbox(
    "Yil",
    options=list(range(2024, 2031)),
    index=list(range(2024, 2031)).index(2026) if 2026 in range(2024, 2031) else 0
)

month_name = st.sidebar.selectbox(
    "Oy",
    options=UZ_MONTHS,
    index=0
)

st.sidebar.divider()
st.sidebar.header("📌 Menyu")

menu = st.sidebar.radio(
    "Bo'lim",
    [
        "Dashboard",
        "Statsionar",
        "Statsionar — Foiz",
        "Ambulator (OPD)",
        "Ambulator — Foiz",
        "Poliklinika(OPD)",
        "Jami protokol",
        "Sozlamalar",
    ],
    index=1,
    key="menu"
)

# =========================
# PAGES
# =========================
if menu == "Dashboard":
    render_dashboard(year, month_name, UZ_MONTHS)

elif menu == "Statsionar":
    render_statsionar(year, month_name)

elif menu == "Statsionar — Foiz":
    render_foiz(year, month_name, UZ_MONTHS)

elif menu == "Ambulator (OPD)":
    render_ambulator(year, month_name)

elif menu == "Ambulator — Foiz":
    render_foiz_ambulator(year, month_name, UZ_MONTHS)

elif menu == "Poliklinika(OPD)":
    render_poliklinika_doctor(year, month_name, UZ_MONTHS)

elif menu == "Jami protokol":
    render_jami_protokol(year, month_name, UZ_MONTHS)

elif menu == "Sozlamalar":
    render_settings(year, month_name, UZ_MONTHS)
