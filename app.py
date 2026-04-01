# -*- coding: utf-8 -*-
import streamlit as st
from pathlib import Path
import time

from database import init_db
from utils import UZ_MONTHS
from modules_dashboard import render_dashboard
from modules_settings import render_settings
from modules_ambulator import render_ambulator
from modules_statsionar import render_statsionar
from modules_foiz import render_foiz
from modules_foiz_ambulator import render_foiz_ambulator
from modules_poliklinika_doctor import render_poliklinika_doctor
from modules_jami_protokol import render_jami_protokol
from modules_extra_settings import render_extra_settings


# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="Bolalar Milliy Tibbiyot Markazi",
    page_icon="assets/logo.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================
# DB INIT (faqat bir marta)
# =========================
init_db(force_recreate=False)


# =========================
# SESSION STATE INIT - Render uchun muhim!
# =========================
if "menu" not in st.session_state:
    st.session_state.menu = "Dashboard"

if "selected_year" not in st.session_state:
    st.session_state.selected_year = 2026

if "selected_month" not in st.session_state:
    st.session_state.selected_month = 0

if "page_loaded" not in st.session_state:
    st.session_state.page_loaded = True


# =========================
# HEADER (tezlashtirilgan)
# =========================
st.caption("Statsionar + Ambulator (OPD) | by Tibliyev")

col1, col2 = st.columns([1, 12])

with col1:
    logo_path = Path("assets/logo.png")
    if logo_path.exists():
        st.image(str(logo_path), width=90)

with col2:
    st.markdown(
        "<h1 style='margin-bottom:0px; padding-top:18px;'>Bolalar Milliy Tibbiyot Markazi</h1>",
        unsafe_allow_html=True
    )

st.divider()


# =========================
# SIDEBAR - Render uchun tezlashtirilgan
# =========================
with st.sidebar:
    st.header("📅 Hisobot oyi")
    
    # Yil tanlash
    year = st.selectbox(
        "Yil",
        options=list(range(2024, 2031)),
        index=list(range(2024, 2031)).index(st.session_state.selected_year),
        key="year_select"
    )
    
    # Oy tanlash
    month_name = st.selectbox(
        "Oy",
        options=UZ_MONTHS,
        index=st.session_state.selected_month,
        key="month_select"
    )
    
    # O'zgarishlarni saqlash
    if year != st.session_state.selected_year:
        st.session_state.selected_year = year
        # Yil o'zgarganda keshni tozalash
        for key in list(st.session_state.keys()):
            if key.startswith("dashboard_") or key.startswith("preview_"):
                del st.session_state[key]
        st.rerun()
    
    month_idx = UZ_MONTHS.index(month_name)
    if month_idx != st.session_state.selected_month:
        st.session_state.selected_month = month_idx
        # Oy o'zgarganda keshni tozalash
        for key in list(st.session_state.keys()):
            if key.startswith("dashboard_") or key.startswith("preview_"):
                del st.session_state[key]
        st.rerun()
    
    st.divider()
    st.header("📌 Menyu")
    
    # ===== MUHIM: Buttonlar bilan menyu =====
    menu_items = {
        "Dashboard": "📊",
        "Statsionar": "🏥",
        "Statsionar — Foiz": "📈",
        "Ambulator (OPD)": "🚑",
        "Ambulator — Foiz": "📊",
        "Poliklinika(OPD)": "👨‍⚕️",
        "Jami protokol": "📑",
        "Sozlamalar": "⚙️",
        "Moliyaviy sozlamalar": "💰"
    }
    
    for menu_name, icon in menu_items.items():
        if st.button(f"{icon} {menu_name}", use_container_width=True, key=f"btn_{menu_name.replace(' ', '_')}"):
            st.session_state.menu = menu_name
            st.rerun()
    
    # Hozirgi menyuni ko'rsatish
    st.divider()
    st.info(f"**Hozirgi:** {st.session_state.menu}")


# =========================
# PAGES - session state orqali
# =========================
menu = st.session_state.menu
year = st.session_state.selected_year
month_idx = st.session_state.selected_month
month_name = UZ_MONTHS[month_idx]

# ===== Render uchun progress bar YO'Q! =====
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
elif menu == "Moliyaviy sozlamalar":
    render_extra_settings(year, month_name, UZ_MONTHS)
