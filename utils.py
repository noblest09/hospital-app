# -*- coding: utf-8 -*-
from datetime import datetime, timezone

UZ_MONTHS = [
    "Yanvar","Fevral","Mart","Aprel","May","Iyun",
    "Iyul","Avgust","Sentyabr","Oktyabr","Noyabr","Dekabr"
]

def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().replace(microsecond=0).isoformat()

def fmt_uzs(x) -> str:
    try:
        return f"{float(x):,.0f}".replace(",", " ")
    except Exception:
        return "0"
