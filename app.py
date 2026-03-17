# -*- coding: utf-8 -*-
import json
import streamlit as st

from utils import UZ_MONTHS

from modules_dashboard import render_dashboard
from modules_statsionar import render_statsionar
from modules_ambulator import render_ambulator
from modules_jami_protokol import render_jami_protokol

try:
    from modules_foiz import render_foiz
except Exception:
    render_foiz = None

try:
    from modules_foiz_ambulator import render_foiz_ambulator
except Exception:
    render_foiz_ambulator = None

try:
    from modules_poliklinika_doctor import render_poliklinika_doctor
except Exception:
    render_poliklinika_doctor = None

try:
    from modules_settings import render_settings
except Exception:
    render_settings = None

try:
    from modules_extra_settings import render_extra_settings
except Exception:
    render_extra_settings = None


st.set_page_config(
    page_title="Hospital App",
    page_icon="🏥",
    layout="wide"
)

CONFIG_FILE = "config.json"


def load_config():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            if not isinstance(cfg, dict):
                cfg = {}
    except Exception:
        cfg = {}

    if "password" not in cfg or not str(cfg["password"]).strip():
        cfg["password"] = "1234"

    return cfg


def save_config(cfg: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


cfg = load_config()
import os
PASSWORD = os.getenv("APP_PASSWORD", "1234")

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False


def password_gate():
    c1, c2, c3 = st.columns([1, 1.05, 1])

    with c2:
        st.markdown("## 🔐 Tizimga kirish")
        st.caption("Parolni kiriting")

        pwd = st.text_input("Parol", type="password")

        if st.button("Kirish", use_container_width=True):
            if pwd == PASSWORD:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Noto‘g‘ri parol")

    st.stop()


if not st.session_state["authenticated"]:
    password_gate()


def render_password_change_compact():
    with st.sidebar.expander("🔑 Parol", expanded=False):
        old_pwd = st.text_input("Eski", type="password", key="old_pwd")
        new_pwd = st.text_input("Yangi", type="password", key="new_pwd")
        new_pwd2 = st.text_input("Tasdiq", type="password", key="new_pwd2")

        if st.button("Saqlash", key="save_pwd_small", use_container_width=True):
            if old_pwd != PASSWORD:
                st.error("Eski parol noto‘g‘ri")
            elif not str(new_pwd).strip():
                st.error("Yangi parol bo‘sh bo‘lmasin")
            elif new_pwd != new_pwd2:
                st.error("Yangi parollar bir xil emas")
            else:
                cfg["password"] = str(new_pwd).strip()
                save_config(cfg)
                st.success("Parol o‘zgartirildi ✅")


st.sidebar.markdown("### 🏥 Hospital App")
selected_year = st.sidebar.number_input("Yil", min_value=2020, max_value=2100, value=2026, step=1)
selected_month_name = st.sidebar.selectbox("Oy", UZ_MONTHS, index=0)

menu = [
    "Dashboard",
    "Statsionar",
    "Ambulator",
    "Jami protokol",
]

if render_foiz is not None:
    menu.append("Foiz (Statsionar)")

if render_foiz_ambulator is not None:
    menu.append("Foiz (Ambulator)")

if render_poliklinika_doctor is not None:
    menu.append("Poliklinika doctor")

if render_settings is not None:
    menu.append("Sozlamalar")

if render_extra_settings is not None:
    menu.append("Extra settings")

page = st.sidebar.selectbox("Bo‘lim", menu)

# sidebar pastiga tushirish uchun spacer
for _ in range(12):
    st.sidebar.markdown("&nbsp;", unsafe_allow_html=True)

st.sidebar.divider()
render_password_change_compact()

if st.sidebar.button("🚪 Chiqish", use_container_width=True):
    st.session_state["authenticated"] = False
    st.rerun()


if page == "Dashboard":
    render_dashboard(selected_year, selected_month_name, UZ_MONTHS)

elif page == "Statsionar":
    render_statsionar(selected_year, selected_month_name)

elif page == "Ambulator":
    render_ambulator(selected_year, selected_month_name)

elif page == "Jami protokol":
    render_jami_protokol(selected_year, selected_month_name, UZ_MONTHS)

elif page == "Foiz (Statsionar)":
    if render_foiz is None:
        st.info("Bu modul topilmadi")
    else:
        render_foiz(selected_year, selected_month_name, UZ_MONTHS)

elif page == "Foiz (Ambulator)":
    if render_foiz_ambulator is None:
        st.info("Bu modul topilmadi")
    else:
        render_foiz_ambulator(selected_year, selected_month_name, UZ_MONTHS)

elif page == "Poliklinika doctor":
    if render_poliklinika_doctor is None:
        st.info("Bu modul topilmadi")
    else:
        render_poliklinika_doctor(selected_year, selected_month_name, UZ_MONTHS)

elif page == "Sozlamalar":
    if render_settings is None:
        st.info("Bu modul topilmadi")
    else:
        try:
            render_settings(selected_year, selected_month_name)
        except TypeError:
            try:
                render_settings(selected_year, selected_month_name, UZ_MONTHS)
            except TypeError:
                render_settings()

elif page == "Extra settings":
    if render_extra_settings is None:
        st.info("Bu modul topilmadi")
    else:
        try:
            render_extra_settings(selected_year, selected_month_name)
        except TypeError:
            try:
                render_extra_settings(selected_year, selected_month_name, UZ_MONTHS)
            except TypeError:
                render_extra_settings()
