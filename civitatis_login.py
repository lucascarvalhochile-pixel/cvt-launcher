"""Civitatis Partner Login — auto-refresh cookie via env creds.

Usage:
    from civitatis_login import civitatis_login
    cookie_header = civitatis_login()

Env vars:
    CIVITATIS_LOGIN_EMAIL
    CIVITATIS_LOGIN_PASSWORD
"""
import os
import requests

CIVITATIS_BASE = "https://www.civitatis.com"
CIVITATIS_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


def civitatis_login():
    """Faz login no partner Civitatis usando env vars.

    Returns:
        (cookie_header_string, session) ou (None, None) se falhou.
    """
    email = os.environ.get("CIVITATIS_LOGIN_EMAIL", "").strip()
    pwd = os.environ.get("CIVITATIS_LOGIN_PASSWORD", "").strip()
    if not email or not pwd:
        print("[CVT-LOGIN] CIVITATIS_LOGIN_EMAIL ou PASSWORD ausente")
        return None, None

    s = requests.Session()
    s.headers.update({
        "User-Agent": CIVITATIS_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    })

    try:
        # 1) GET landing page pra pegar cookies iniciais (CSRF, session id)
        r0 = s.get(f"{CIVITATIS_BASE}/br/fornecedores/", timeout=15,
                   allow_redirects=True)
        # 2) POST credenciais no MESMO path (form id=user-login action=/br/fornecedores/)
        payload = {
            "user_name": email,
            "password": pwd,
            "typology": "6",
            "twofactorauth": "true",
            "remember_me": "on",
        }
        r = s.post(f"{CIVITATIS_BASE}/br/fornecedores/", data=payload,
                   timeout=15, allow_redirects=True,
                   headers={"Referer": f"{CIVITATIS_BASE}/br/fornecedores/"})

        # 3) Verificar sucesso
        body_lower = (r.text or "")[:5000].lower()
        if "twofactor" in body_lower or "codigo" in body_lower and "2fa" in body_lower:
            print("[CVT-LOGIN] 2FA required — cannot handle programmatically")
            return None, None
        if "erro" in body_lower and "senha" in body_lower:
            print("[CVT-LOGIN] Credenciais recusadas (senha errada?)")
            return None, None

        # 4) Se conseguiu, testar rota autenticada
        test = s.get(f"{CIVITATIS_BASE}/br/fornecedores/v2/reservas/", timeout=15,
                     allow_redirects=False)
        if test.status_code >= 500 or test.status_code in (401, 403):
            print(f"[CVT-LOGIN] Test route retornou {test.status_code} — login falhou")
            return None, None
        if test.status_code == 302 and "login" in (test.headers.get("Location", "") or "").lower():
            print("[CVT-LOGIN] Test route redirecionou pra login — auth falhou")
            return None, None

        # 5) Serializar cookies como Cookie header
        cookie_header = "; ".join(f"{c.name}={c.value}" for c in s.cookies)
        print(f"[CVT-LOGIN] Login OK ({len(s.cookies)} cookies, {len(cookie_header)} chars)")
        return cookie_header, s

    except Exception as e:
        print(f"[CVT-LOGIN] Exception: {e}")
        return None, None


if __name__ == "__main__":
    cookie, _ = civitatis_login()
    print("---")
    print(f"cookie_len: {len(cookie) if cookie else 0}")
