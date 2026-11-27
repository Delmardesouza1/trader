"""Step 2/2 — submit the freshly-arrived PIN code using state from step 1.

Usage:
    PYTHONPATH=. python scripts/seed_session_step2.py <CODE>

The temporary state from step 1 is deleted as soon as it is read.
If the code is wrong, run step 1 again to get a new one.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

from curl_cffi import requests

from pyquotex.config import (
    credentials,
    load_all_sessions,
    resource_path,
    write_private_file,
)

# Mesmo host do bot/cliente: cookies e SSID valem só para o domínio onde foram obtidos.
HOST = os.getenv("QUOTEX_HOST", "quotex.com")
BASE = f"https://{HOST}"
LANG = "en"
IMPERSONATE = "firefox133"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:127.0) Gecko/20100101 Firefox/127.0"
STATE_PATH = Path(resource_path("settings/seed_state.json"))


def _cookies_to_header(jar: dict[str, str]) -> str:
    return "; ".join(f"{k}={v}" for k, v in jar.items())


def main(code: str) -> int:
    if not STATE_PATH.exists():
        print("  ❌ Run scripts/seed_session_step1.py first.")
        return 1

    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    STATE_PATH.unlink(missing_ok=True)  # apaga o estado temporário assim que é lido
    email = state["email"]
    cookies = state["cookies"]
    token = state["token"]

    s = requests.Session(impersonate=IMPERSONATE)
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.5"})
    for k, v in cookies.items():
        s.cookies.set(k, v, domain=f".{HOST}")

    pwd = credentials()[1]
    r = s.post(
        f"{BASE}/{LANG}/sign-in/modal",
        data={
            "_token": token,
            "email": email,
            "password": pwd,
            "remember": "1",
            "keep_code": "1",
            "code": code,
        },
        headers={
            "Referer": f"{BASE}/{LANG}/sign-in/modal",
            "Origin": BASE,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    print(f"  POST /sign-in/modal (with code) -> {r.status_code} final_url={r.url}")
    if 'name="keep_code"' in r.text:
        m = re.search(
            r'<div[^>]*class="[^"]*error[^"]*"[^>]*>(.*?)</div>',
            r.text,
            re.S,
        )
        if m:
            print(f"  Error: {m.group(1).strip()[:200]}")
        print("  ❌ Still on PIN form — code rejected or expired. Run step 1 again.")
        return 2

    # We should now be at /trade
    if "/trade" not in str(r.url):
        r = s.get(f"{BASE}/{LANG}/trade")
        print(f"  GET /trade -> {r.status_code}")

    ssid: str | None = None
    m = re.search(r"window\.settings\s*=\s*(\{.*?\});", r.text, re.S)
    if m:
        try:
            data_settings = json.loads(m.group(1))
            ssid = data_settings.get("token")
        except Exception as e:
            print(f"  window.settings parse failed: {e}")
    if not ssid:
        r2 = s.get(
            f"{BASE}/api/v1/cabinets/digest",
            headers={"Referer": f"{BASE}/{LANG}/trade"},
        )
        print(f"  GET /api/v1/cabinets/digest -> {r2.status_code}")
        if r2.status_code == 200:
            try:
                ssid = r2.json().get("data", {}).get("token")
            except Exception:
                pass

    if not ssid:
        print("  ❌ Login passed but SSID not found.")
        return 3

    # Mostra só o início do token, para não expor o valor completo em logs.
    print(f"  ✅ SSID obtido ({ssid[:4]}…)")

    cookie_jar = s.cookies.get_dict()
    session_path = Path(resource_path("session.json"))
    out = load_all_sessions()
    out[email] = {
        "cookies": _cookies_to_header(cookie_jar),
        "token": ssid,
        "user_agent": UA,
        "host": HOST,
    }
    write_private_file(session_path, json.dumps(out, indent=4))
    print(f"  ✅ Wrote session.json ({len(cookie_jar)} cookies)")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: seed_session_step2.py <CODE>")
        sys.exit(1)
    sys.exit(main(sys.argv[1].strip()))
