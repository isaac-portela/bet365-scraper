# betws/odds.py
from __future__ import annotations

def odds_to_decimal(od: str | None) -> float | None:
    if not od:
        return None
    od = od.strip().upper()
    if od in ("EVS", "EVENS"):
        return 2.0
    if od == "0/0":
        return None
    if "/" in od:
        a, b = od.split("/", 1)
        try:
            a = float(a); b = float(b)
            if b == 0:
                return None
            return round(1.0 + (a / b), 4)
        except Exception:
            return None
    try:
        return float(od)
    except Exception:
        return None
