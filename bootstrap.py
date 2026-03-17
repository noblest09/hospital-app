# -*- coding: utf-8 -*-
"""
Ishga tushirishdan oldin (bir marta) DB ni tayyorlash uchun:
python bootstrap.py
"""
from database import init_db

if __name__ == "__main__":
    init_db(force_recreate=False)
    print("OK: DB tayyor.")
