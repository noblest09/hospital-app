# -*- coding: utf-8 -*-
import streamlit as st
from pathlib import Path
from functools import lru_cache

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
    layout="wide"
)

# =========================
# DB INIT (faqat bir marta)
# =========================
init_db(force_recreate=False)


# =========================
# SESSION STATE INIT
# =========================
# Menyuni session state da saqlash
if "menu" not in st.session_state:
    st.session_state.menu = "Dashboard"

# Yil va oyni session state da saqlash
if "selected_year" not in st.session_state:
    st.session_state.selected_year = 2026

if "selected_month" not in st.session_state:
    st.session_state.selected_month = 0  # Yanvar


# =========================
# HEADER (o'zgarmaydi)
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
# SIDEBAR - TEZLASHTIRILGAN
# =========================
with st.sidebar:
    st.header("📅 Hisobot oyi")
    
    # Yil tanlash (session state bilan)
    year = st.selectbox(
        "Yil",
        options=list(range(2024, 2031)),
        index=list(range(2024, 2031)).index(st.session_state.selected_year),
        key="year_select"
    )
    
    # Oy tanlash (session state bilan)
    month_name = st.selectbox(
        "Oy",
        options=UZ_MONTHS,
        index=st.session_state.selected_month,
        key="month_select"
    )
    
    # O'zgarishlarni session state ga saqlash
    if year != st.session_state.selected_year:
        st.session_state.selected_year = year
        st.rerun()
    
    if month_name != UZ_MONTHS[st.session_state.selected_month]:
        st.session_state.selected_month = UZ_MONTHS.index(month_name)
        st.rerun()
    
    st.divider()
    st.header("📌 Menyu")
    
    # ===== MUHIM: Buttonlar radio'dan TEZROQ =====
    # Dashboard
    if st.button("📊 Dashboard", use_container_width=True, key="btn_dashboard"):
        st.session_state.menu = "Dashboard"
        st.rerun()
    
    # Statsionar
    if st.button("🏥 Statsionar", use_container_width=True, key="btn_statsionar"):
        st.session_state.menu = "Statsionar"
        st.rerun()
    
    # Statsionar — Foiz
    if st.button("📈 Statsionar — Foiz", use_container_width=True, key="btn_foiz"):
        st.session_state.menu = "Statsionar — Foiz"
        st.rerun()
    
    st.divider()
    
    # Ambulator (OPD)
    if st.button("🚑 Ambulator (OPD)", use_container_width=True, key="btn_ambulator"):
        st.session_state.menu = "Ambulator (OPD)"
        st.rerun()
    
    # Ambulator — Foiz
    if st.button("📊 Ambulator — Foiz", use_container_width=True, key="btn_foiz_amb"):
        st.session_state.menu = "Ambulator — Foiz"
        st.rerun()
    
    st.divider()
    
    # Poliklinika(OPD)
    if st.button("👨‍⚕️ Poliklinika(OPD)", use_container_width=True, key="btn_poliklinika"):
        st.session_state.menu = "Poliklinika(OPD)"
        st.rerun()
    
    # Jami protokol
    if st.button("📑 Jami protokol", use_container_width=True, key="btn_jami"):
        st.session_state.menu = "Jami protokol"
        st.rerun()
    
    st.divider()
    
    # Sozlamalar
    if st.button("⚙️ Sozlamalar", use_container_width=True, key="btn_settings"):
        st.session_state.menu = "Sozlamalar"
        st.rerun()
    
    # Moliyaviy sozlamalar
    if st.button("💰 Moliyaviy sozlamalar", use_container_width=True, key="btn_extra"):
        st.session_state.menu = "Moliyaviy sozlamalar"
        st.rerun()
    
    # Hozirgi menyuni ko'rsatish
    st.divider()
    st.info(f"**Hozirgi:** {st.session_state.menu}")


# =========================
# PAGES - SESSION STATE ORQALI
# =========================
# Session state dan menyuni olish
menu = st.session_state.menu

# Session state dan yil va oyni olish
year = st.session_state.selected_year
month_idx = st.session_state.selected_month
month_name = UZ_MONTHS[month_idx]

# ===== MUHIM: Progress bar YO'Q, spinner YO'Q =====
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
