"""Step 1/2 — fire credentials POST so Quotex emails a fresh 2FA code,
then persist the cookies + CSRF token for step 2 to resume.

The state file is written to settings/ (git-ignored) with restricted
permissions, and step 2 deletes it right after reading it.
Credentials come from QUOTEX_EMAIL / QUOTEX_PASSWORD (or an interactive prompt).
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

from curl_cffi import requests

from pyquotex.config import credentials, resource_path, write_private_file

# Mesmo host do bot/cliente: cookies e SSID valem só para o domínio onde foram obtidos.
HOST = os.getenv("QUOTEX_HOST", "quotex.com")
BASE = f"https://{HOST}"
LANG = "en"
IMPERSONATE = "firefox133"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:127.0) Gecko/20100101 Firefox/127.0"
STATE_PATH = Path(resource_path("settings/seed_state.json"))


def main() -> int:
    email, password = credentials()
    s = requests.Session(impersonate=IMPERSONATE)
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.5"})

    r = s.get(f"{BASE}/{LANG}")
    print(f"  GET /{LANG} -> {r.status_code}")
    if r.status_code != 200:
        return 1

    r = s.get(f"{BASE}/{LANG}/sign-in/modal/")
    print(f"  GET /sign-in/modal/ -> {r.status_code}")
    m = re.search(
        r'<input[^>]*name=["\']_token["\'][^>]*value=["\']([^"\']+)["\']',
        r.text,
    )
    if not m:
        print("  ❌ No _token in modal")
        return 1
    token = m.group(1)

    r = s.post(
        f"{BASE}/{LANG}/sign-in/",
        data={
            "_token": token,
            "email": email,
            "password": password,
            "remember": "1",  # formulário HTTP envia texto; mantém os tipos consistentes
        },
        headers={
            "Referer": f"{BASE}/{LANG}/sign-in",
            "Origin": BASE,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    print(f"  POST /sign-in/ -> {r.status_code}")
    if 'name="keep_code"' not in r.text:
        # Não grava a resposta em /tmp: ela pode conter token e dados de sessão.
        print(f"  ⚠️  No 2FA form returned (status {r.status_code}) — login may have already passed or failed")
        return 2

    write_private_file(
        STATE_PATH,
        json.dumps(
            {
                "cookies": s.cookies.get_dict(),
                "token": token,
                "email": email,
            },
            indent=2,
        ),
    )
    print("  ✅ Temporary state saved to settings/ (step 2 deletes it after use)")
    print("     → check the e-mail of your Quotex account for the LATEST 6-digit code,")
    print("       then run scripts/seed_session_step2.py <CODE>.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
