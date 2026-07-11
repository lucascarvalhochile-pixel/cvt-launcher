"""
CVT Launcher — Civitatis → LCX Automatic Sales Launcher
Microservice to parse Civitatis new booking emails and create sales in LCX.
"""

import os
import re
import json
import imaplib
import smtplib
import email as email_lib
from email.header import decode_header
from email.utils import parsedate_to_datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText as MIMETextPart
from datetime import datetime, timedelta
import traceback
import unicodedata
import threading
import time
import requests
from bs4 import BeautifulSoup
# Bokun integration
try:
    from bokun_integration import BokunClient, BOKUN_TO_LCX
    _BOKUN_IMPORT_OK = True
except Exception as _e:
    print(f"[BOKUN] import failed: {_e}")
    _BOKUN_IMPORT_OK = False
from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════
LCX_BASE = "https://app.lucascarvalhoturismo.com.br"
LCX_EMAIL = os.environ.get("LCX_EMAIL", "b2b@lucascarvalhoturismo.com.br")
LCX_PASSWORD = os.environ.get("LCX_PASSWORD", "")

GMAIL_EMAIL = os.environ.get("GMAIL_EMAIL", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
GMAIL_IMAP_HOST = "imap.gmail.com"

GSHEET_ID = os.environ.get("GSHEET_ID", "1dgMKZ31puupdU5VbzjfAPg8O_gdZfO7DDMaoMP5PAqQ")
TRACKER_SHEET_ID = "1qDnTjA2vySipZy-xy-NBC-TB1Snf1TTjAakJt0_0Dzw"
GSHEET_CREDS_JSON = os.environ.get("GSHEET_CREDS_JSON", "")

# Civitatis Partners (para extrair telefone do voucher PDF)
CIVITATIS_BASE = "https://www.civitatis.com"
CIVITATIS_COOKIE = os.environ.get("CIVITATIS_COOKIE", "")
CIVITATIS_USER_AGENT = os.environ.get(
    "CIVITATIS_USER_AGENT",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
# Provider hash do LC TURISMO (identifica conta na API partners da Civitatis)
CIVITATIS_PROVIDER_HASH = os.environ.get(
    "CIVITATIS_PROVIDER_HASH",
    "MW9adTg3LytDc2ZXVE9MWG56QVlOQT09OjozaXFVQzJXd3NM"
)

# Server action IDs (reverse-engineered from LCX)
ACTION_CREATE_SALE = "608856f3650364be4a3c41a01d619d9a1d6d726137"
ACTION_GET_TOURS = "40d19810cd726ab0a39d2f54d5f75443249c9055dd"
ACTION_UPDATE_SALE_STATUS = "60a040b364ccb723e6d0f26d97aae14bd0a9809a09"
ACTION_UPDATE_SALE_ITEM_STATUS = "70d1714f4559c474142d1d2e60c56c7cfb6cb5c177"
ACTION_UPDATE_SALE = "7092bb726196689cb64a916b21f82484d8325caf46"  # Edit full sale; accepts partial payload like {notes: "..."}

# Auto-scan config
AUTO_SCAN_INTERVAL = int(os.environ.get("AUTO_SCAN_INTERVAL", "300"))  # 5 min
BOKUN_ACCESS_KEY = os.environ.get("BOKUN_ACCESS_KEY", "")
BOKUN_SECRET_KEY = os.environ.get("BOKUN_SECRET_KEY", "")
BOKUN_ENABLED = bool(BOKUN_ACCESS_KEY and BOKUN_SECRET_KEY and _BOKUN_IMPORT_OK)
GO_LIVE_DATE = os.environ.get("GO_LIVE_DATE", "2026-04-18")  # Dedup now via LCX search (no Google Sheets needed)
LAUNCH_CUTOFF = os.environ.get("LAUNCH_CUTOFF", "2026-05-26T17:45:00")  # Skip old emails before this deploy
auto_scan_status = {"last_run": None, "last_result": None, "running": False}

# Error alerting config
CONSECUTIVE_ERROR_THRESHOLD = 3  # Alert after N consecutive scan errors
_consecutive_errors = 0

# Daily summary config
DAILY_SUMMARY_HOUR = 8  # Send summary at 8 AM (America/Sao_Paulo)

# ═══════════════════════════════════════════════════════
# CITY → COUNTRY MAPPING
# ═══════════════════════════════════════════════════════
CITY_COUNTRY = {
    "santiago": ("Chile", "Santiago"),
    "santiago de chile": ("Chile", "Santiago"),
    "san pedro de atacama": ("Chile", "Atacama"),
    "atacama": ("Chile", "Atacama"),
    "valparaíso": ("Chile", "Santiago"),
    "viña del mar": ("Chile", "Santiago"),
    "cartagena": ("Colômbia", "Cartagena"),
    "cartagena de indias": ("Colômbia", "Cartagena"),
    "san andrés": ("Colômbia", "San Andres"),
    "san andres": ("Colômbia", "San Andres"),
    "lima": ("Peru", "Lima"),
    "cusco": ("Peru", "Cusco"),
    "cuzco": ("Peru", "Cusco"),
    "uyuni": ("Chile", "Uyuni"),
}


def resolve_country_city(raw_city):
    """Map Civitatis city name to LCX country + city."""
    key = raw_city.strip().lower()
    if key in CITY_COUNTRY:
        return CITY_COUNTRY[key]
    # Fallback: try partial match
    for k, v in CITY_COUNTRY.items():
        if k in key or key in k:
            return v
    return ("", raw_city)


# ═══════════════════════════════════════════════════════
# TOUR CODE → DESTINO (country + city)
# Source of truth: the LCX tour code prefix (first 6 chars).
# Used by validate_tour_country() to detect mismatch
# between email destino and matched tour destino.
# ═══════════════════════════════════════════════════════
CODE_PREFIX_DESTINO = {
    "CHISAN": ("Chile", "Santiago"),
    "CHIATA": ("Chile", "Atacama"),
    "CHIUYU": ("Chile", "Uyuni"),
    "COLCAR": ("Colômbia", "Cartagena"),
    "COLSAO": ("Colômbia", "San Andres"),
    "PERLIM": ("Peru", "Lima"),
    "PERCUS": ("Peru", "Cusco"),
    "MEXCAN": ("México", "Cancún"),
    "DOMPUN": ("República Dominicana", "Punta Cana"),
    "ARGBUE": ("Argentina", "Buenos Aires"),
    "USAMIA": ("Estados Unidos", "Miami"),
    "USAORL": ("Estados Unidos", "Orlando"),
}


def tour_code_destino(codigo_lcx):
    """Extract (country, city) from LCX tour code prefix (first 6 chars).
    Returns (None, None) for unknown prefixes."""
    if not codigo_lcx or len(codigo_lcx) < 6:
        return None, None
    prefix = codigo_lcx[:6].upper()
    return CODE_PREFIX_DESTINO.get(prefix, (None, None))


def _norm_text(s):
    """Lowercase + strip accents for safe comparison."""
    return _strip_accents((s or "").strip().lower())


def validate_tour_country(codigo_lcx, email_country, email_city):
    """Validate that the matched LCX tour belongs to the same country/city
    as the email. Returns True if OK, False if mismatch detected.

    Fail-open if code prefix is unknown or email destino is missing
    (so we never block on legacy codes / parser gaps).
    """
    tour_country, tour_city = tour_code_destino(codigo_lcx)
    if not tour_country:
        return True
    if not email_country or not email_city:
        return True
    if _norm_text(email_country) != _norm_text(tour_country):
        return False
    if _norm_text(email_city) != _norm_text(tour_city):
        return False
    return True


# ═══════════════════════════════════════════════════════
# GOOGLE SHEETS — READ MAPPING TABLE
# ═══════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════
# HARDCODED MAPPING (fallback when Google Sheets is unavailable)
# Source: planilha mapeamento-civitatis-lcx da Karina
# ═══════════════════════════════════════════════════════
HARDCODED_MAPPING = {
    # Santiago (19+)
    "estação de esqui portillo e laguna del inca": {"codigo_lcx": "CHISAN067", "nome_lcx": "Portillo e Laguna del Inca"},
    "excursión a las termas de colina y el embalse el yeso": {"codigo_lcx": "CHISAN040", "nome_lcx": "Cajón Del Maipo, Embalse El Yeso e Termas de Colina"},
    "excursión al parque safari de rancagua": {"codigo_lcx": "CHISAN071", "nome_lcx": "Safári Rancagua"},
    "excursão ao parque safári de rancagua": {"codigo_lcx": "CHISAN071", "nome_lcx": "Safári Rancagua"},
    "trilha pelo vulcão cerro toco": {"codigo_lcx": "CHIATA024", "nome_lcx": "Vulcão Cerro Toco"},
    "excursión al valle nevado al atardecer": {"codigo_lcx": "CHISAN059", "nome_lcx": "Cordilheira Sunset - Verão"},
    "excursión al viñedo alyan al atardecer": {"codigo_lcx": "CHISAN107", "nome_lcx": "Vinícola Alyan"},
    "excursão à vinícola alyan ao entardecer": {"codigo_lcx": "CHISAN107", "nome_lcx": "Vinícola Alyan"},
    "excursão a isla negra, algarrobo e viña undurraga": {"codigo_lcx": "CHISAN061", "nome_lcx": "Isla Negra, Algarrobo e Undurraga"},
    "excursão a valparaíso e viña del mar": {"codigo_lcx": "CHISAN106", "nome_lcx": "Viña del Mar e Valparaiso"},
    "excursão ao cajón del maipo de moto de neve": {"codigo_lcx": "CHISAN062", "nome_lcx": "Moto Neve em Cajón Del Maipo"},
    "excursão ao parque de farellones": {"codigo_lcx": "CHISAN034", "nome_lcx": "Andes Full Day - Farellones"},
    "excursão ao valle nevado": {"codigo_lcx": "CHISAN1682", "nome_lcx": "Andes Full Day - Valle Nevado"},
    "excursão à vinícola undurraga": {"codigo_lcx": "CHISAN116", "nome_lcx": "Vinícola Undurraga - Tarde"},
    "excursão à estação de esqui el colorado": {"codigo_lcx": "CHISAN6706", "nome_lcx": "Andes Full Day - El Colorado"},
    "excursão às termas valle de colina": {"codigo_lcx": "CHISAN039", "nome_lcx": "Cajón Del Maipo e Termas de Colina"},
    "tour de neve por farellones e valle nevado": {"codigo_lcx": "CHISAN035", "nome_lcx": "Andes Panorâmico"},
    "tour do vinho casillero del diablo na vinícola concha y toro": {"codigo_lcx": "CHISAN111", "nome_lcx": "Vinícola Concha y Toro Noturno"},
    "visita guiada pelo centro histórico de santiago": {"codigo_lcx": "CHISAN055", "nome_lcx": "City Tour Santiago"},
    "visita à vinícola haras de pirque": {"codigo_lcx": "CHISAN113", "nome_lcx": "Vinícola Haras de Pirque Sunset"},
    # Concha y Toro — multiple tiers (matched by código interno)
    "experiência centro do vinho concha y toro": {"codigo_lcx": "CHISAN109", "nome_lcx": "Centro del Vinho Concha y Toro - Manhã"},
    "experiência marqués de casa concha": {"codigo_lcx": "CHISAN110", "nome_lcx": "Vinícola Concha y Toro Tour do Marqués - Manhã"},
    # Amor y Pastas
    "experiencia gastronómica amor y pastas": {"codigo_lcx": "CHISAN033", "nome_lcx": "Amor e Pasta - Tradicional"},
    # Atacama (15)
    "excursão ao vale do arco-íris": {"codigo_lcx": "CHIATA020", "nome_lcx": "Vale do Arco-Íris"},
    "excursão ao valle de la luna": {"codigo_lcx": "CHIATA021", "nome_lcx": "Valle de la Luna e Pedra do Coyote"},
    "excursão aos géiseres de el tatio": {"codigo_lcx": "CHIATA007", "nome_lcx": "Geyser del Tatio"},
    "excursão de 4 dias ao salar de uyuni": {"codigo_lcx": "CHIUYU128", "nome_lcx": "Uyuni Compartilhado (4D3N)"},
    "excursão à cordilheira do sal": {"codigo_lcx": "CHIATA023", "nome_lcx": "Vallecito"},
    "excursão às lagunas escondidas de baltinache": {"codigo_lcx": "CHIATA010", "nome_lcx": "Lagunas Escondidas de Baltinache - Manhã"},
    "excursão às termas de puritama": {"codigo_lcx": "CHIATA014", "nome_lcx": "Termas de Puritama - Manhã"},
    "observação de estrelas no deserto de atacama": {"codigo_lcx": "CHIATA016", "nome_lcx": "Tour Astronômico"},
    "passeio de balão por san pedro de atacama": {"codigo_lcx": "CHIATA017", "nome_lcx": "Tour de Balão"},
    "rota dos salares": {"codigo_lcx": "CHIATA012", "nome_lcx": "Ruta de los Salares"},
    "sandboarding en el valle de la muerte": {"codigo_lcx": "CHIATA013", "nome_lcx": "Sandboard"},
    "tour en bicicleta por la garganta del diablo": {"codigo_lcx": "CHIATA018", "nome_lcx": "Tour de Bike - Manhã"},
    "trekking por el volcán cerro toco": {"codigo_lcx": "CHIATA024", "nome_lcx": "Vulcão Cerro Toco"},
    "trilha por purilibre": {"codigo_lcx": "CHIATA019", "nome_lcx": "Trekking de Purilibre - Manhã"},
    # Cartagena (9)
    "excursão ao isla lizamar beach club": {"codigo_lcx": "COLCAR025", "nome_lcx": "Lizamar Beach Club"},
    "excursão ao mangata ocean club": {"codigo_lcx": "COLCAR034", "nome_lcx": "Mangata Beach Club"},
    "excursão ao palmarito beach": {"codigo_lcx": "COLCAR040", "nome_lcx": "Palmarito Beach – Tierra Bomba"},
    "excursão ao vulcão el totumo": {"codigo_lcx": "COLCAR055", "nome_lcx": "Volcán del Totumo"},
    "excursão à ilha múcura": {"codigo_lcx": "COLCAR000", "nome_lcx": "3 lslas + San Bernardo"},
    "excursão às ilhas de cartagena + plâncton luminescente": {"codigo_lcx": "COLCAR002", "nome_lcx": "5 Islas Vip + Plancton"},
    "festa noturna de barco por cartagena": {"codigo_lcx": "COLCAR036", "nome_lcx": "Noche Blanca"},
    "tour de barco pirata pela baía de cartagena": {"codigo_lcx": "COLCAR003", "nome_lcx": "Barco Pirata"},
    "tour de chiva rumbera por cartagena das índias": {"codigo_lcx": "COLCAR012", "nome_lcx": "City Tour no Ônibus Chiva - Manhã"},
    # San Andrés (8)
    "excursão a johnny cay + aquário natural": {"codigo_lcx": "COLSAO079", "nome_lcx": "Passeio do Barco Johnny Cay e Aquário Natural"},
    "festa no bar flutuante ibiza": {"codigo_lcx": "COLSAO064", "nome_lcx": "Bar Ibiza Sai"},
    "parasailing em san andrés": {"codigo_lcx": "COLSAO077", "nome_lcx": "Parasail - Manhã"},
    "passeio de barco semisubmarino por san andrés": {"codigo_lcx": "COLSAO081", "nome_lcx": "Semisubmarino - Manhã"},
    "seawalker em san andrés": {"codigo_lcx": "COLSAO063", "nome_lcx": "Aquanautas - Manhã"},
    "snorkel em san andrés": {"codigo_lcx": "COLSAO072", "nome_lcx": "Mergulho com Snorkel - Manhã"},
    "tour de caiaque transparente pelos manguezais de san andrés": {"codigo_lcx": "COLSAO068", "nome_lcx": "ECOFIWI Caiaque Transparente - Manhã"},
    # Lima (1)
    "excursão a ica e huacachina + ilhas ballestas": {"codigo_lcx": "PERLIM024", "nome_lcx": "Islas Ballestas y Desierto Huacachina"},
    # Cusco (3)
    "excursão ao vale sagrado dos incas + maras, moray e ollantaytambo": {"codigo_lcx": "PERCUS020", "nome_lcx": "Valle Sagrado + Moray e Maras"},
    "excursão à lagoa humantay": {"codigo_lcx": "PERCUS005", "nome_lcx": "Laguna Humantay"},
    "excursão ao lago humantay": {"codigo_lcx": "PERCUS005", "nome_lcx": "Laguna Humantay"},
    "visita guiada por cusco e suas 4 ruínas": {"codigo_lcx": "PERCUS002", "nome_lcx": "City Tour em Cusco - Manhã"},
    "montaña 7 colores con ticket": {"codigo_lcx": "PERCUS010", "nome_lcx": "Montanha 7 Cores"},
    "montanha 7 cores com ticket": {"codigo_lcx": "PERCUS010", "nome_lcx": "Montanha 7 Cores"},
    "montaña 7 colores": {"codigo_lcx": "PERCUS010", "nome_lcx": "Montanha 7 Cores"},
    "montanha 7 cores": {"codigo_lcx": "PERCUS010", "nome_lcx": "Montanha 7 Cores"},
    "excursión a la montaña 7 colores con ticket": {"codigo_lcx": "PERCUS010", "nome_lcx": "Montanha 7 Cores"},
    "excursão à montanha 7 cores com ticket": {"codigo_lcx": "PERCUS010", "nome_lcx": "Montanha 7 Cores"},
}


_mapping_cache = {"data": None, "ts": None}

def load_mapping():
    """Load Civitatis→LCX mapping. Tries Google Sheets first, falls back to hardcoded."""
    now = datetime.now()
    if _mapping_cache["data"] and _mapping_cache["ts"] and (now - _mapping_cache["ts"]).seconds < 300:
        return _mapping_cache["data"]

    mapping = dict(HARDCODED_MAPPING)

    # Try to overlay with live Google Sheets data
    try:
        if GSHEET_CREDS_JSON:
            creds_dict = json.loads(GSHEET_CREDS_JSON)
            creds = Credentials.from_service_account_info(creds_dict, scopes=[
                "https://www.googleapis.com/auth/spreadsheets"
            ])
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(GSHEET_ID)
            ws = sh.sheet1
            rows = ws.get_all_values()

            for row in rows:
                if len(row) >= 5 and row[2] and row[3]:
                    nome_cvt = row[2].strip()
                    codigo_lcx = row[3].strip()
                    nome_lcx = row[4].strip() if len(row) > 4 else ""
                    if nome_cvt and codigo_lcx and not codigo_lcx.startswith("▸"):
                        # Support multiple names per cell separated by "/"
                        names = [n.strip() for n in nome_cvt.split("/")]
                        for name in names:
                            if name:
                                clean = re.sub(r'\s*-\s*tour\s+.*$', '', unicodedata.normalize("NFC", name.lower()), flags=re.IGNORECASE).strip()
                                mapping[clean] = {
                                    "codigo_lcx": codigo_lcx,
                                    "nome_lcx": nome_lcx,
                                }
            print(f"[SHEETS] Loaded {len(rows)} rows from Google Sheets")
    except Exception as e:
        print(f"[SHEETS] Using hardcoded mapping (Sheets error: {e})")

    _mapping_cache["data"] = mapping
    _mapping_cache["ts"] = now
    return mapping


def _strip_accents(text):
    """Remove diacritics/accents from text for fuzzy matching.
    e.g. 'Excursão safári' → 'Excursao safari', 'Excursión' → 'Excursion'
    """
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def find_lcx_tour(atividade, codigo_interno):
    """Find LCX tour code from Civitatis activity name or internal code."""
    mapping = load_mapping()
    if not mapping:
        return None, None

    key = unicodedata.normalize("NFC", atividade.strip().lower())
    # Remove language/tier suffix: " - Tour em português", " - Tour com retirada + ..."
    key_clean = re.sub(r'\s*-\s*tour\s+.*$', '', key, flags=re.IGNORECASE).strip()

    # 1. Exact match on clean activity name
    if key_clean in mapping:
        m = mapping[key_clean]
        return m["codigo_lcx"], m["nome_lcx"]

    # 1b. Exact match WITHOUT accents (handles PT↔ES: excursão↔excursión, safári↔safari)
    key_no_accent = _strip_accents(key_clean)
    for k, v in mapping.items():
        if _strip_accents(k) == key_no_accent:
            return v["codigo_lcx"], v["nome_lcx"]

    # 2. Match via código interno (accent-insensitive)
    if codigo_interno:
        cod_lower = _strip_accents(codigo_interno.strip().lower())
        for k, v in mapping.items():
            k_na = _strip_accents(k)
            if cod_lower in k_na or k_na in cod_lower:
                return v["codigo_lcx"], v["nome_lcx"]

    # 3. Partial match on activity name (accent-insensitive)
    for k, v in mapping.items():
        k_na = _strip_accents(k)
        if key_no_accent in k_na or k_na in key_no_accent:
            return v["codigo_lcx"], v["nome_lcx"]

    # 4. Word overlap match (at least 3 significant words, accent-insensitive)
    key_words = set(w for w in key_no_accent.split() if len(w) > 3)
    best_match = None
    best_score = 0
    for k, v in mapping.items():
        k_words = set(w for w in _strip_accents(k).split() if len(w) > 3)
        overlap = len(key_words & k_words)
        if overlap > best_score and overlap >= 3:
            best_score = overlap
            best_match = v
    if best_match:
        return best_match["codigo_lcx"], best_match["nome_lcx"]

    return None, None

# ═══════════════════════════════════════════════════════
# EMAIL PARSER — CIVITATIS NEW BOOKING
# ═══════════════════════════════════════════════════════
def parse_civitatis_email(msg):
    """Parse a Civitatis new booking email into structured data."""
    subject = ""
    for part, encoding in decode_header(msg["Subject"] or ""):
        if isinstance(part, bytes):
            subject += part.decode(encoding or "utf-8", errors="replace")
        else:
            subject += part

    # Only process "New booking" emails
    if "New booking" not in subject:
        return None

    # Extract booking number from subject
    booking_match = re.search(r'A(\d+)', subject)
    booking_number = booking_match.group(1) if booking_match else ""

    # Get email body (prefer HTML, fallback to text)
    body_html = ""
    body_text = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/html":
                payload = part.get_payload(decode=True)
                body_html = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
            elif ct == "text/plain":
                payload = part.get_payload(decode=True)
                body_text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        ct = msg.get_content_type()
        decoded = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
        if ct == "text/html":
            body_html = decoded
        else:
            body_text = decoded

    # Parse from HTML if available, else from text
    if body_html:
        return parse_html_body(body_html, booking_number, subject)
    elif body_text:
        return parse_text_body(body_text, booking_number, subject)
    return None


def parse_html_body(html, booking_number, subject):
    """Parse HTML body of Civitatis email."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    return parse_text_body(text, booking_number, subject)


def _clean_text(text):
    """Collapse excessive whitespace from HTML-to-text conversion.

    Civitatis emails generate dozens of blank lines between labels and values.
    This normalizes the text so regex patterns work reliably:
    - Collapse 3+ consecutive newlines into 2
    - Collapse runs of spaces/tabs on a single line into 1 space
    - Strip leading/trailing whitespace from each line
    """
    # Remove lines that are only whitespace
    lines = text.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        cleaned.append(stripped)
    text = "\n".join(cleaned)
    # Collapse 3+ blank lines into 1 blank line
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text


def parse_text_body(text, booking_number, subject):
    """Parse text content of Civitatis booking email."""
    data = {
        "booking_number": booking_number,
        "subject": subject,
    }

    # Clean the text first — Civitatis HTML produces massive whitespace
    text = _clean_text(text)
    data["raw_text"] = text[:3000]

    # Check it's a "Nova reserva" (not modification or cancellation)
    if "Nova reserva" not in text:
        if "reserva foi cancelada" in text.lower() or "cancelamento" in text.lower():
            data["type"] = "CANCELAMENTO"
            return data
        if "modificação" in text.lower() or "modificada" in text.lower():
            data["type"] = "MODIFICACAO"
            return data

    data["type"] = "NOVA_RESERVA"

    def extract(label):
        """Extract value after a label, tolerating newlines between label and value."""
        # Try same-line first: "Label: value"
        m = re.search(rf'{label}:\s*\n?\s*(.+)', text, re.IGNORECASE)
        return m.group(1).strip() if m else ""

    data["atividade"] = extract("Atividade")
    data["cidade"] = extract("Cidade")
    data["idioma"] = extract("Idioma")
    data["codigo_interno"] = extract("Código interno")
    data["data_tour"] = extract("Data")
    data["hora"] = extract("Hora")
    data["ponto_retirada"] = extract("Ponto de retirada")

    # Booking number from "Número da reserva:" if not already set
    if not booking_number:
        nr = extract("Número da reserva")
        if nr:
            data["booking_number"] = nr.strip()

    # Nome completo (different from passenger Nome)
    nome_completo = extract("Nome completo")
    if nome_completo:
        data["nome_completo"] = nome_completo

    # Parse pessoas (people breakdown)
    # Formats: "2 adultos x R$290", "2 Por pessoa x US$126", "1 adulto + 1 criança"
    pessoas_section = re.search(r'Pessoas\s*\n(.+?)(?:Dados|Preço|$)', text, re.DOTALL | re.IGNORECASE)
    if pessoas_section:
        pessoas_text = pessoas_section.group(1)
        data["pessoas_raw"] = pessoas_text.strip()

        adults = re.search(r'(\d+)\s*adult', pessoas_text, re.IGNORECASE)
        children = re.search(r'(\d+)\s*(?:crian|niñ|child)', pessoas_text, re.IGNORECASE)
        seniors = re.search(r'(\d+)\s*(?:senior|idoso)', pessoas_text, re.IGNORECASE)
        # "N Por pessoa" = all adults (generic per-person pricing)
        por_pessoa = re.search(r'(\d+)\s*[Pp]or pessoa', pessoas_text)

        data["num_adults"] = int(adults.group(1)) if adults else (int(por_pessoa.group(1)) if por_pessoa else 0)
        data["num_children"] = int(children.group(1)) if children else 0
        data["num_seniors"] = int(seniors.group(1)) if seniors else 0
        data["num_total"] = data["num_adults"] + data["num_children"] + data["num_seniors"]

    # Prices — tolerate newlines between label and R$
    preco_venda = re.search(r'Preço de venda\s*\n?\s*R\$\s*\n?\s*([\d.,]+)', text)
    preco_liquido = re.search(r'Preço líquido\s*\n?\s*R\$\s*\n?\s*([\d.,]+)', text)
    # Also try "Preço total" as fallback (some email formats)
    preco_total = re.search(r'Preço total\s*\n?\s*R\$\s*\n?\s*([\d.,]+)', text)

    def parse_brl(match):
        if not match:
            return ""
        val = match.group(1).strip()
        # Handle BR format: "1.234,56" → "1234.56", "1.260" → "1260", "502,50" → "502.50"
        if "," in val:
            # Has comma = decimal separator. Dots are thousands.
            val = val.replace(".", "").replace(",", ".")
        elif "." in val:
            # Only dots, no comma. If format is "X.XXX" (thousands), remove dot.
            # "1.260" = 1260 (thousands), "502.50" = 502.50 (decimal)
            parts = val.split(".")
            if len(parts) == 2 and len(parts[1]) == 3:
                # "1.260" → thousands separator
                val = val.replace(".", "")
            # else keep as-is (already decimal format like "502.50")
        return val

    data["preco_venda"] = parse_brl(preco_venda) or parse_brl(preco_total)
    data["preco_liquido"] = parse_brl(preco_liquido)

    # Customer data (from "Dados do cliente" section)
    # Accept value on same line OR after blank line(s) until first non-empty line.
    # BeautifulSoup converts HTML tables with \n between cells, sometimes adds blank lines.
    # _OTHER_LABEL guard: if captured value is another known field label, treat as empty.
    _OTHER_LABEL = re.compile(r'^(Sobrenomes?|Hora|Data|Documento|Idade|Telefone|Lugar|Restri|Coment|Pre[çc]o|N[úu]mero|C[óo]digo|Atividade|Cidade|Idioma|Email|E-mail|Dados|\(Dados)\b', re.IGNORECASE)
    _TIME_TRAIL = re.compile(r'\s+\d{1,2}:\d{2}\s*(?:[ap]\.?\s*m\.?)?\s*$', re.IGNORECASE)

    def _extract_owner_field(label):
        # Try multiple gaps: 0 newlines (same line), 1, 2, 3 newlines (blank lines between).
        # Use lookahead to match first non-empty line after label.
        for gap_pattern in [r'[ \t]+', r'[ \t]*\n[ \t]*', r'[ \t]*\n[ \t]*\n[ \t]*', r'[ \t]*\n[ \t]*\n[ \t]*\n[ \t]*']:
            m = re.search(rf'{label}:{gap_pattern}([^\n]+)', text)
            if m:
                v = m.group(1).strip()
                if v and not _OTHER_LABEL.match(v):
                    v = _TIME_TRAIL.sub("", v).strip()
                    if v:
                        return v
        return ""

    # Also support "Nome e sobrenomes" / "Nome y apellidos" (new template, combined field)
    combined_m = re.search(r'Nome\s+e\s+sobrenomes[ \t]*(?:\n[ \t]*)*([^\n]+)', text, re.IGNORECASE)
    if combined_m:
        v = combined_m.group(1).strip()
        if v and not _OTHER_LABEL.match(v):
            data["nome"] = _TIME_TRAIL.sub("", v).strip()
            data["sobrenomes"] = ""
        else:
            data["nome"] = _extract_owner_field("Nome")
            data["sobrenomes"] = _extract_owner_field("Sobrenomes?")
    else:
        data["nome"] = _extract_owner_field("Nome")
        data["sobrenomes"] = _extract_owner_field("Sobrenomes?")

    # Comentários
    comentario = re.search(r'Coment[áa]rios?:\s*\n?\s*(.+?)(?:\n\n|$)', text, re.DOTALL | re.IGNORECASE)
    data["comentario"] = comentario.group(1).strip() if comentario else ""

    # Parse passengers — "Dados passageiro N:" blocks
    data["passageiros"] = []
    passenger_blocks = re.split(r'Dados\s+passageiro\s*\d+:', text, flags=re.IGNORECASE)
    for block in passenger_blocks[1:]:
        p = {}
        def pext(label, blk=block):
            m = re.search(rf'{label}\s*\n\s*(.+)', blk, re.IGNORECASE)
            return m.group(1).strip() if m else ""

        nome = pext("Nome")
        sobrenome = pext("Sobrenome")
        p["name"] = f"{nome} {sobrenome}".strip()
        p["cpfPassport"] = pext("Documento.*?(?:Passaporte)?")
        idade = re.search(r'Idade\s*\n\s*(\d+)', block, re.IGNORECASE)
        p["age"] = int(idade.group(1)) if idade else None
        p["whatsapp"] = pext("Telefone")
        p["hotel"] = pext("Lugar de retirada")
        p["dietaryRestriction"] = pext(r"Restri[çc][õo]es?\s+alimentar\w*")

        if p["name"]:
            data["passageiros"].append(p)

    # Fallback: if no passengers found, build from client data
    if not data["passageiros"]:
        client_name = ""
        if data.get("nome_completo"):
            client_name = data["nome_completo"]
        elif data.get("nome") or data.get("sobrenomes"):
            client_name = f"{data.get('nome', '')} {data.get('sobrenomes', '')}".strip()
        if client_name:
            data["passageiros"].append({
                "name": client_name,
                "cpfPassport": "",
                "age": None,
                "whatsapp": "",
                "hotel": data.get("ponto_retirada", ""),
                "dietaryRestriction": "",
            })

    # Parse date to ISO format
    data["data_iso"] = parse_date_pt(data.get("data_tour", ""))

    return data


def parse_date_pt(date_str):
    """Parse Portuguese date like 'Domingo, 7 de fevereiro de 2027' to ISO."""
    months = {
        "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
        "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
        "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12
    }
    m = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', date_str)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = months.get(month_name, 0)
        if month:
            return f"{year}-{month:02d}-{day:02d}"
    return ""


# ═══════════════════════════════════════════════════════
# LCX INTEGRATION
# ═══════════════════════════════════════════════════════
class LCXClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 CVTLauncher/1.0",
            "Accept": "text/x-component",
            "Content-Type": "text/plain;charset=UTF-8",
        })
        self.logged_in = False
        self.csrf_token = None

    def login(self):
        """Login to LCX using NextAuth credentials flow."""
        self.last_login_error = None
        try:
            # Step 1: Get CSRF token from NextAuth endpoint
            r = self.session.get(f"{LCX_BASE}/api/auth/csrf", timeout=15)
            csrf_data = r.json()
            self.csrf_token = csrf_data.get("csrfToken", "")

            # Step 2: POST credentials
            login_data = {
                "email": LCX_EMAIL,
                "password": LCX_PASSWORD,
                "csrfToken": self.csrf_token,
                "callbackUrl": "/dashboard",
                "json": "true",
            }
            r = self.session.post(
                f"{LCX_BASE}/api/auth/callback/credentials",
                data=login_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=15,
                allow_redirects=True
            )

            # Step 3: Verify login via session endpoint
            r = self.session.get(f"{LCX_BASE}/api/auth/session", timeout=15)
            session_data = r.json()
            self.logged_in = bool(session_data.get("user"))
            if self.logged_in:
                print(f"[LCX] Logged in as {session_data['user'].get('name', '?')}")
            return self.logged_in
        except Exception as e:
            self.last_login_error = str(e)
            print(f"[LCX LOGIN ERROR] {e}")
            return False

    def create_sale(self, sale_data):
        """Create a sale in LCX using the server action."""
        if not self.logged_in:
            if not self.login():
                return {"success": False, "error": "Login failed"}

        try:
            payload = json.dumps([sale_data])
            r = self.session.post(
                f"{LCX_BASE}/dashboard/vendas/nova",
                data=payload,
                headers={
                    "Next-Action": ACTION_CREATE_SALE,
                    "Content-Type": "text/plain;charset=UTF-8",
                    "Accept": "text/x-component",
                },
                timeout=30
            )

            if r.status_code == 200:
                # Parse RSC response
                text = r.text
                if '"success"' in text or '"id"' in text:
                    # Try to extract sale ID
                    id_match = re.search(r'"id"\s*:\s*"([^"]+)"', text)
                    sale_id = id_match.group(1) if id_match else "unknown"
                    return {"success": True, "sale_id": sale_id, "response": text[:500]}
                elif "error" in text.lower():
                    return {"success": False, "error": text[:500]}
                else:
                    return {"success": True, "sale_id": "pending", "response": text[:500]}
            else:
                return {"success": False, "error": f"HTTP {r.status_code}: {r.text[:300]}"}
        except Exception as e:
            return {"success": False, "error": str(e)}


    def update_sale_status(self, sale_id, status="CONFIRMED"):
        """Update sale status (PENDING → CONFIRMED)."""
        if not self.logged_in:
            if not self.login():
                return {"success": False, "error": "Login failed"}
        try:
            payload = json.dumps([{"saleId": sale_id, "status": status}])
            r = self.session.post(
                f"{LCX_BASE}/dashboard/vendas/{sale_id}",
                data=payload,
                headers={
                    "Next-Action": ACTION_UPDATE_SALE_STATUS,
                    "Content-Type": "text/plain;charset=UTF-8",
                    "Accept": "text/x-component",
                },
                timeout=30,
            )
            if r.status_code == 200 and "success" in r.text:
                return {"success": True}
            return {"success": False, "error": r.text[:300]}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_sale_notes(self, sale_id):
        """Read current internal notes by parsing the SSR payload of the DETAIL page.
        The /editar page is CSR-only (no inline data). The detail page /dashboard/vendas/{id}
        inlines the RSC payload with escaped JSON, e.g. \\"notes\\":\\"VALUE\\"."""
        if not self.logged_in:
            if not self.login():
                return None
        try:
            r = self.session.get(
                f"{LCX_BASE}/dashboard/vendas/{sale_id}",  # DETAIL page, not /editar
                headers={"Accept": "text/html"},
                timeout=15,
            )
            if r.status_code == 200:
                # Match \"notes\":\"VALUE\" — escaped JSON inside RSC payload
                # Group 1 captures value chars: anything that isn't \ or " literal, OR an escape sequence \X
                m = re.search(r'\\"notes\\":\\"((?:[^\\"]|\\.)*?)\\"', r.text)
                if m:
                    raw = m.group(1)
                    # Unescape JSON-style escapes back to real chars
                    notes = raw.replace('\\"', '"').replace('\\n', '\n').replace('\\/', '/').replace('\\\\', '\\')
                    return notes
            return ""
        except Exception as e:
            print(f"[LCX] Error reading notes for {sale_id}: {e}")
            return None

    def update_sale_notes(self, sale_id, new_notes):
        """Update internal notes of a sale via ACTION_UPDATE_SALE with minimal payload.
        IMPORTANT: this REPLACES the notes field — caller must concat with existing notes if append desired."""
        if not self.logged_in:
            if not self.login():
                return {"success": False, "error": "Login failed"}
        try:
            payload = json.dumps([sale_id, {"notes": new_notes}])
            r = self.session.post(
                f"{LCX_BASE}/dashboard/vendas/{sale_id}/editar",
                data=payload.encode("utf-8"),
                headers={
                    "Next-Action": ACTION_UPDATE_SALE,
                    "Content-Type": "text/plain;charset=UTF-8",
                    "Accept": "text/x-component",
                },
                timeout=30,
            )
            if r.status_code == 200:
                return {"success": True}
            return {"success": False, "error": f"HTTP {r.status_code}: {r.text[:300]}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def cancel_sale_full(self, sale_id, booking_number, email_date_str):
        """Orchestrate cancellation: append note + change status → CANCELED.
        email_date_str: human-readable date/time from the cancellation email (e.g. "19/06/2026 14:32")."""
        # 1. Read current notes
        current = self.get_sale_notes(sale_id)
        if current is None:
            current = ""

        # 2. Append cancellation observation (idempotent — skip if already present)
        cancel_marker = f"Cancelada via email Civitatis em {email_date_str}"
        if cancel_marker in current:
            print(f"[CANCEL] Sale {sale_id} already has cancellation note — skipping notes update")
        else:
            new_notes = (current.rstrip() + " | " + cancel_marker) if current.strip() else cancel_marker
            notes_res = self.update_sale_notes(sale_id, new_notes)
            if not notes_res.get("success"):
                print(f"[CANCEL] WARN: failed to update notes for {sale_id}: {notes_res.get('error')}")

        # 3. Change status to CANCELED
        status_res = self.update_sale_status(sale_id, "CANCELED")
        if not status_res.get("success"):
            return {"success": False, "error": f"Status update failed: {status_res.get('error')}", "sale_id": sale_id}

        return {"success": True, "sale_id": sale_id, "booking_number": booking_number, "cancel_marker": cancel_marker}

    def find_sale_id(self, booking_number):
        """Search LCX for a sale with *cvt* #BOOKING and return its sale_id.
        STRICT MATCHING (fix 29/06/2026):
          - Word boundary regex impede prefix collision (#39208 != #392084433)
          - Para cada href de venda na página, valida que o booking EXATO está
            no contexto próximo (mesma "row" ~500 chars) E que não há outros
            bookings diferentes nessa janela.
        Sem isso, o LCX search retorna múltiplas vendas (busca fuzzy/prefix em
        nome do cliente, notes, obs interna) e o regex pega o PRIMEIRO href —
        que pode ser de OUTRA venda. Resultado: cancela/edita a venda errada.
        """
        try:
            r = self.session.get(
                f"{LCX_BASE}/dashboard/vendas?search={booking_number}",
                headers={"Accept": "text/html"},
                timeout=15,
            )
            if r.status_code != 200:
                return None
            booking_pattern = re.compile(
                rf'\*cvt\*\s*#{re.escape(booking_number)}(?!\d)'
            )
            for m in re.finditer(r'href="/dashboard/vendas/(cm[a-z0-9]+)"', r.text):
                sale_id_candidate = m.group(1)
                start = max(0, m.start() - 500)
                end = min(len(r.text), m.end() + 500)
                context = r.text[start:end]
                if not booking_pattern.search(context):
                    continue
                other_bookings = set(re.findall(r'\*cvt\*\s*#(\d+)', context))
                # Aceita só se o ÚNICO booking *cvt* na janela for o esperado
                if other_bookings == {booking_number}:
                    print(f"[LCX] Found sale_id {sale_id_candidate} for booking #{booking_number} (row validated)")
                    return sale_id_candidate
            print(f"[LCX] No sale found with EXACT *cvt* #{booking_number} match in row context")
            return None
        except Exception as e:
            print(f"[LCX] Error finding sale_id for #{booking_number}: {e}")
            return None

    def booking_exists(self, booking_number, max_retries=3):
        """Check if a CVT booking already exists in LCX by searching for *cvt* #BOOKING.
        Word boundary impede prefix collision (#39208 != #392084433) mas NAO exige row context strict
        (a UI da listagem trunca customer.name; strict match gerava falsos negativos -> duplicatas).
        Retries up to max_retries times on transient errors (timeout, HTTP 5xx).
        FAIL-CLOSED after all retries exhausted: assumes exists to prevent duplicates."""
        if not self.logged_in:
            if not self.login():
                print(f"[LCX-DEDUP] Cannot check booking #{booking_number} - login failed, BLOCKING launch")
                return True

        booking_pattern = re.compile(
            rf'\*cvt\*\s*#{re.escape(booking_number)}(?!\d)'
        )

        for attempt in range(1, max_retries + 1):
            try:
                r = self.session.get(
                    f"{LCX_BASE}/dashboard/vendas?search={booking_number}",
                    headers={"Accept": "text/html"},
                    timeout=20,
                )
                if r.status_code == 200:
                    has_sale = bool(re.search(r'href="/dashboard/vendas/cm[a-z0-9]+"', r.text))
                    has_booking = bool(booking_pattern.search(r.text))
                    exists = has_sale and has_booking
                    if exists:
                        print(f"[LCX-DEDUP] Booking #{booking_number} ALREADY EXISTS in LCX - skipping")
                    else:
                        print(f"[LCX-DEDUP] Booking #{booking_number} NOT found in LCX - will create")
                    return exists

                if r.status_code < 500:
                    print(f"[LCX-DEDUP] HTTP {r.status_code} checking #{booking_number} - BLOCKING launch")
                    return True

                print(f"[LCX-DEDUP] HTTP {r.status_code} checking #{booking_number} - attempt {attempt}/{max_retries}")

            except requests.exceptions.Timeout:
                print(f"[LCX-DEDUP] Timeout checking #{booking_number} - attempt {attempt}/{max_retries}")
            except requests.exceptions.ConnectionError:
                print(f"[LCX-DEDUP] Connection error checking #{booking_number} - attempt {attempt}/{max_retries}")
            except Exception as e:
                print(f"[LCX-DEDUP] Error checking #{booking_number}: {e} - attempt {attempt}/{max_retries}")

            if attempt < max_retries:
                wait = 2 ** attempt
                print(f"[LCX-DEDUP] Retrying in {wait}s...")
                time.sleep(wait)
                if not self.logged_in or attempt >= 2:
                    print(f"[LCX-DEDUP] Re-login before retry {attempt + 1}")
                    self.logged_in = False
                    if not self.login():
                        print(f"[LCX-DEDUP] Re-login failed - BLOCKING launch for #{booking_number}")
                        return True

        print(f"[LCX-DEDUP] All {max_retries} attempts failed for #{booking_number} - BLOCKING launch (fail-closed)")
        return True


lcx_client = LCXClient()


# ═══════════════════════════════════════════════════════
# CIVITATIS PARTNERS — Extração de telefone do voucher PDF
# ═══════════════════════════════════════════════════════
# A Civitatis bloqueia o telefone do cliente até 48h antes do tour
# na interface web. MAS o voucher PDF contém o telefone do guest
# desde o momento da reserva. Este módulo:
#   1. Usa a sessão logada do gerencia@ (cookie em env var)
#   2. Localiza o hash do bookingId via listagem
#   3. Baixa o voucher PDF
#   4. Extrai o telefone via regex
#
# MVP: cookie é renovado manualmente pelo Lucas a cada ~30 dias.
# Em caso de falha (cookie expirou), a venda sobe SEM telefone
# e um alerta é registrado.

_civitatis_session_cache = {"session": None, "ts": None}
_civitatis_hash_cache = {}  # booking_number → (hash1, hash2)


def _build_civitatis_session():
    """Constrói sessão requests com cookie + headers do Civitatis Partners."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": CIVITATIS_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,pt-BR;q=0.8,en;q=0.7",
    })
    if CIVITATIS_COOKIE:
        s.headers["Cookie"] = CIVITATIS_COOKIE
    return s


def _get_civitatis_session():
    """Retorna sessão cacheada (reutilizada por até 10 min)."""
    now = datetime.now()
    if (_civitatis_session_cache["session"] and _civitatis_session_cache["ts"]
            and (now - _civitatis_session_cache["ts"]).seconds < 600):
        return _civitatis_session_cache["session"]
    s = _build_civitatis_session()
    _civitatis_session_cache["session"] = s
    _civitatis_session_cache["ts"] = now
    return s


def civitatis_find_booking_id_hash(booking_number, max_pages=20):
    """Localiza idHash de um booking_number via API JSON do partners Civitatis.

    Estratégia (API JSON, não scraping):
      1. GET /api/providers/bookings/<PROVIDER_HASH>?lang=es&page=N
      2. Para cada página, varre json.values procurando o booking.id == booking_number
      3. Retorna booking.idHash quando encontrar
    """
    cache_key = str(booking_number)
    if cache_key in _civitatis_hash_cache:
        return _civitatis_hash_cache[cache_key]

    if not CIVITATIS_COOKIE or not CIVITATIS_PROVIDER_HASH:
        print("[CVT-PARTNERS] CIVITATIS_COOKIE ou CIVITATIS_PROVIDER_HASH não configurado — pulando")
        return None

    s = _get_civitatis_session()
    target = int(str(booking_number))
    list_url = f"{CIVITATIS_BASE}/api/providers/bookings/{CIVITATIS_PROVIDER_HASH}"

    try:
        for page in range(1, max_pages + 1):
            today = datetime.now()
            date_from = (today - timedelta(days=30)).strftime("%Y-%m-%d")
            date_to = today.strftime("%Y-%m-%d")
            params = {
                "lang": "es",
                "dateFrom": date_from,
                "dateTo": date_to,
                "destinations": "0",
                "filterType": "r",
                "grouped": "false",
                "limit": "20",
                "page": str(page),
                "services": "0",
                "short": "rd",
                "status": "",
            }
            r = s.get(list_url, params=params, timeout=20,
                      headers={"Accept": "application/json"},
                      allow_redirects=False)

            if r.status_code in (301, 302, 303, 307, 308):
                location = r.headers.get("Location", "")
                if "login" in location.lower() or "auth" in location.lower():
                    print(f"[CVT-PARTNERS] Cookie expirou (redirect → {location[:60]}). Renovar CIVITATIS_COOKIE.")
                    try:
                        record_alert("CIVITATIS_COOKIE_EXPIRED",
                                     "Cookie do partners.civitatis.com expirou",
                                     "Faça login novamente e atualize CIVITATIS_COOKIE no Railway")
                    except Exception:
                        pass
                    return None

            if r.status_code != 200:
                print(f"[CVT-PARTNERS] API bookings retornou {r.status_code} na página {page}")
                return None

            try:
                data = r.json()
            except Exception as e:
                print(f"[CVT-PARTNERS] Resposta não é JSON na página {page}: {e}")
                return None

            values = data.get("values") or []
            count = data.get("count", 0)
            for b in values:
                if b.get("id") == target:
                    h = b.get("idHash")
                    if h:
                        _civitatis_hash_cache[cache_key] = h
                        print(f"[CVT-PARTNERS] Booking #{booking_number} encontrado na página {page} (idHash {len(h)} chars)")
                        return h

            # Se não há mais reservas, para
            if not values or page * 20 >= count:
                break

        print(f"[CVT-PARTNERS] Booking #{booking_number} não encontrado em {max_pages} páginas (total na conta: {count})")
        return None

    except Exception as e:
        print(f"[CVT-PARTNERS] Erro ao buscar idHash do booking #{booking_number}: {e}")
        return None


def civitatis_get_phone_from_api(id_hash):
    """Busca telefone do cliente via API JSON de detalhe da reserva.

    GET /api/providers/booking/<PROVIDER_HASH>/<idHash>?lang=es
    Retorna o valor de travellerPhone, ou None.
    """
    if not id_hash or not CIVITATIS_COOKIE or not CIVITATIS_PROVIDER_HASH:
        return None
    s = _get_civitatis_session()
    url = f"{CIVITATIS_BASE}/api/providers/booking/{CIVITATIS_PROVIDER_HASH}/{id_hash}"
    try:
        r = s.get(url, params={"lang": "es", "type": "1"}, timeout=15,
                  headers={"Accept": "application/json"})
        if r.status_code != 200:
            print(f"[CVT-PARTNERS] API booking detalhe retornou {r.status_code}")
            return None
        data = r.json()
        phone = data.get("travellerPhone")
        if phone and isinstance(phone, str):
            phone = phone.strip()
            if not phone.startswith("+"):
                prefix = data.get("phonePrefix") or ""
                if prefix and not phone.startswith(prefix):
                    phone = f"+{prefix} {phone}" if not prefix.startswith("+") else f"{prefix} {phone}"
                else:
                    phone = "+" + phone.lstrip("0")
            return phone
        return None
    except Exception as e:
        print(f"[CVT-PARTNERS] Erro ao buscar detalhe da reserva: {e}")
        return None


def civitatis_get_phone_by_booking_id(booking_number):
    """FAST PATH: tenta pegar telefone direto com booking ID numérico.
    Endpoint /api/providers/booking/{HASH}/{ID_NUMERIC}?lang=es&type=1 aceita ID numérico
    diretamente — sem precisar paginar lista pra achar idHash. Retorna phone ou None.
    """
    if not booking_number or not CIVITATIS_COOKIE or not CIVITATIS_PROVIDER_HASH:
        return None
    url = f"{CIVITATIS_BASE}/api/providers/booking/{CIVITATIS_PROVIDER_HASH}/{booking_number}"
    headers = {
        "Cookie": CIVITATIS_COOKIE,
        "User-Agent": CIVITATIS_USER_AGENT,
        "Accept": "application/json",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Referer": f"{CIVITATIS_BASE}/es/proveedores/v2/reservas/",
    }
    try:
        r = requests.get(url, headers=headers, params={"lang": "es", "type": "1"}, timeout=15, allow_redirects=False)
        if r.status_code != 200:
            return None
        try:
            data = r.json()
        except Exception:
            return None
        phone = data.get("travellerPhone") or data.get("phone")
        if phone:
            return str(phone).strip()
        return None
    except Exception as e:
        print(f"[CVT-PARTNERS] Fast-path phone error for #{booking_number}: {e}")
        return None


def civitatis_get_customer_phone(booking_number):
    """Pipeline completo: booking_number → telefone do cliente via API JSON.

    Estratégia:
    1. FAST PATH (1 chamada): endpoint detail direto com ID numérico
    2. FALLBACK: paginar lista → achar idHash → buscar detail (mais lento)

    Retorna telefone formatado (+CC NUMERO) ou None. Não levanta exception.
    """
    if not booking_number or not CIVITATIS_COOKIE:
        return None

    # 1. Fast path
    phone = civitatis_get_phone_by_booking_id(booking_number)
    if phone:
        print(f"[CVT-PARTNERS] Telefone (fast-path) pro #{booking_number}: {phone}")
        return phone

    # 2. Fallback: idHash via paginação
    id_hash = civitatis_find_booking_id_hash(booking_number)
    if not id_hash:
        return None
    phone = civitatis_get_phone_from_api(id_hash)
    if phone:
        print(f"[CVT-PARTNERS] Telefone (idHash fallback) pro #{booking_number}: {phone}")
    return phone


def _resolve_tour_id(codigo_lcx):
    """Search LCX tours by code (e.g. CHISAN067) and return the tourId."""
    if not lcx_client.logged_in:
        if not lcx_client.login():
            return None
    try:
        payload = json.dumps([{"search": codigo_lcx}])
        r = lcx_client.session.post(
            f"{LCX_BASE}/dashboard/passeios",
            data=payload,
            headers={
                "Next-Action": ACTION_GET_TOURS,
                "Content-Type": "text/plain;charset=UTF-8",
                "Accept": "text/x-component",
            },
            timeout=15,
        )
        if r.status_code == 200:
            # Parse RSC response for tour ID
            id_match = re.search(r'"id"\s*:\s*"([^"]+)"', r.text)
            if id_match:
                return id_match.group(1)
    except Exception as e:
        print(f"[LCX GET_TOURS ERROR] {e}")
    return None


def build_lcx_sale(parsed_email):
    """Build LCX createSale payload from parsed Civitatis email data.

    Returns (sale_payload, codigo_lcx). If país/cidade mismatch is detected
    between the email destino and the matched LCX tour, returns
    (None, codigo_lcx) so the caller can reject the sale and alert.
    """
    data = parsed_email
    country, city = resolve_country_city(data.get("cidade", ""))

    # Find LCX tour mapping
    codigo_lcx, nome_lcx = find_lcx_tour(
        data.get("atividade", ""),
        data.get("codigo_interno", "")
    )

    # ─────────────────────────────────────────────
    # CRITICAL: country/city cross-validation
    # If the matched tour's code prefix says it belongs to a destino
    # different from the email destino, REJECT — don't launch garbage.
    # Example: email Peru/Cusco "Montanha 7 Cores" but matcher returned
    # CHIATA020 (Atacama). validate_tour_country() catches this.
    # ─────────────────────────────────────────────
    if codigo_lcx and not validate_tour_country(codigo_lcx, country, city):
        tour_country, tour_city = tour_code_destino(codigo_lcx)
        booking_num = data.get("booking_number", "")
        atividade = data.get("atividade", "")
        print(f"[VALIDATION] MISMATCH booking #{booking_num}: tour {codigo_lcx} "
              f"pertence a {tour_country}/{tour_city} mas email é {country}/{city}. "
              f"Atividade: {atividade}")
        try:
            record_alert(
                "PAIS_CIDADE_MISMATCH",
                f"Tour {codigo_lcx} não pertence ao destino do email",
                f"Booking #{booking_num} | Email destino: {country}/{city} | "
                f"Tour destino: {tour_country}/{tour_city} | "
                f"Atividade: {atividade}"
            )
        except Exception as e:
            print(f"[VALIDATION] Falha ao registrar alerta: {e}")
        return None, codigo_lcx

    # Customer name: booking OWNER (Dados do cliente) + *cvt* + booking number
    # Priority: 1) Dados do cliente (nome + sobrenomes), 2) Passenger 1 fallback
    owner_name = f"{data.get('nome', '')} {data.get('sobrenomes', '')}".strip()
    # Defense in depth: strip any time pattern that leaked through parser
    owner_name = re.sub(r'\s+\d{1,2}:\d{2}\s*(?:[ap]\.?\s*m\.?)?\s*$', '', owner_name, flags=re.IGNORECASE).strip()
    if owner_name:
        customer_name = owner_name
    elif data.get("passageiros") and data["passageiros"][0].get("name"):
        customer_name = data["passageiros"][0]["name"].strip()
    else:
        customer_name = "Cliente Civitatis"
    booking_num = data.get("booking_number", "")
    customer_name += f" *cvt* #{booking_num}" if booking_num else " *cvt*"

    # CIVITATIS: extrair telefone do cliente via voucher PDF
    # (a tela web só mostra o telefone 48h antes do tour, mas o
    # voucher PDF tem desde sempre)
    customer_phone = ""
    try:
        if booking_num:
            customer_phone = civitatis_get_customer_phone(booking_num) or ""
    except Exception as e:
        print(f"[CVT-PARTNERS] Falha silenciosa ao buscar telefone do booking #{booking_num}: {e}")

    tour_name = nome_lcx or data.get("atividade", "Tour Civitatis")
    tour_date = data.get("data_iso", "")
    num_people = data.get("num_total", 1) or 1

    # Use preço LÍQUIDO (net price), not preço de venda
    preco = 0
    try:
        preco = float(data.get("preco_liquido", "0") or "0")
        if preco == 0:
            preco = float(data.get("preco_venda", "0") or "0")
    except:
        pass

    # Meeting point: use passenger hotel/pickup or general ponto_retirada
    meeting_point = ""
    if data.get("passageiros"):
        for p in data["passageiros"]:
            if p.get("hotel"):
                meeting_point = p["hotel"]
                break
    if not meeting_point and data.get("ponto_retirada"):
        meeting_point = data["ponto_retirada"]

    # Notes
    notes_parts = [
        f"Reserva Civitatis #{data.get('booking_number', '')}",
        f"{codigo_lcx or ''}",
        f"Código interno: {data.get('codigo_interno', '')}",
        f"Preço venda: R$ {data.get('preco_venda', '0')}",
        f"Preço líquido: R$ {data.get('preco_liquido', '0')}",
        f"Horário: {data.get('hora', '')}",
    ]
    if data.get("comentario"):
        notes_parts.append(f"Comentário cliente: {data['comentario']}")
    notes = " | ".join([p for p in notes_parts if p])

    # Build items array — 1 item per priceTier, price = total for that tier
    items = []
    num_adults = data.get("num_adults", 0) or num_people
    num_children = data.get("num_children", 0)

    if num_adults > 0:
        # Proportional price: adults get their share of total
        adult_price = preco * (num_adults / num_people) if num_people > 0 else preco
        items.append({
            "country": country,
            "city": city,
            "tourName": tour_name,
            "priceTier": "ADULT",
            "numberOfPeople": num_adults,
            "tourDate": tour_date,
            "price": round(adult_price, 2),
            "isGift": False,
        })

    if num_children > 0:
        child_price = preco * (num_children / num_people) if num_people > 0 else 0
        items.append({
            "country": country,
            "city": city,
            "tourName": tour_name,
            "priceTier": "CHILD",
            "numberOfPeople": num_children,
            "tourDate": tour_date,
            "price": round(child_price, 2),
            "isGift": False,
        })

    # Fallback: at least one item
    if not items:
        items.append({
            "country": country,
            "city": city,
            "tourName": tour_name,
            "priceTier": "ADULT",
            "numberOfPeople": num_people,
            "tourDate": tour_date,
            "price": round(preco, 2),
            "isGift": False,
        })

    # Build payment — DINHEIRO (CASH) + PAGO (paid)
    payments = [{
        "method": "CASH",
        "amount": round(preco, 2),
        "status": "paid",
    }]

    # Build participants from detailed passenger data
    participants = []
    if data.get("passageiros"):
        for p in data["passageiros"]:
            diet_labels = []
            if p.get("dietaryRestriction") and p["dietaryRestriction"].lower() not in ("nao", "não", "no", "none", ""):
                diet_labels.append(p["dietaryRestriction"])
            participants.append({
                "name": p.get("name", "Participante"),
                "email": "",
                "cpfPassport": p.get("cpfPassport", ""),
                "whatsapp": customer_phone or p.get("whatsapp", ""),  # voucher prioridade (telefone do cliente real, não da agência)
                "dietaryRestrictionLabel": diet_labels,
            })
    # Fallback: use customer data if no detailed passengers
    if not participants:
        participants.append({
            "name": customer_name.replace(" *cvt*", ""),
            "email": data.get("email_cliente", ""),
            "cpfPassport": data.get("documento", ""),
            "whatsapp": customer_phone or data.get("telefone", ""),  # voucher prioridade
        })

    # Resolve tourId from LCX via getTours API if we have a code
    tour_id = None
    if codigo_lcx:
        tour_id = _resolve_tour_id(codigo_lcx)

    # If we got a tourId, add it to each item
    if tour_id:
        for item in items:
            item["tourId"] = tour_id

    final_customer_whatsapp = customer_phone or data.get("telefone", "")

    sale = {
        "customer": {
            "name": customer_name,
            "email": data.get("email_cliente", ""),
            "cpfPassport": data.get("documento", ""),
            "whatsapp": final_customer_whatsapp,  # voucher prioridade
        },
        "tripCountry": country,
        "tripCity": city,
        "meetingPoint": meeting_point,
        "tripStartDate": tour_date,
        "tripEndDate": tour_date,
        "numberOfPeople": num_people,
        "status": "PENDING",
        "items": items,
        "payments": payments,
        "participants": participants,
        "notes": notes,
    }

    # DEBUG: log build details so we can inspect what was sent to create_sale
    _add_build_debug({
        "ts": datetime.now().isoformat(),
        "booking": booking_num,
        "customer_phone_from_voucher": customer_phone,
        "data_telefone_from_email": data.get("telefone", ""),
        "passageiros_whatsapp": [p.get("whatsapp", "") for p in (data.get("passageiros") or [])],
        "FINAL_customer_whatsapp_sent": final_customer_whatsapp,
        "FINAL_participants_whatsapp_sent": [p.get("whatsapp", "") for p in participants],
        "codigo_lcx": codigo_lcx,
        "tour_name": tour_name,
    })

    return sale, codigo_lcx


# ═══════════════════════════════════════════════════════
# GMAIL IMAP — FETCH NEW BOOKING EMAILS
# ═══════════════════════════════════════════════════════
def fetch_new_booking_emails(max_results=10, since_hours=24):
    """Fetch recent Civitatis new booking emails via IMAP."""
    if not GMAIL_EMAIL or not GMAIL_APP_PASSWORD:
        return []

    try:
        mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST)
        mail.login(GMAIL_EMAIL, GMAIL_APP_PASSWORD)
        mail.select("INBOX")

        since_date = (datetime.now() - timedelta(hours=since_hours)).strftime("%d-%b-%Y")
        search_criteria = f'(FROM "civitatis.com" SUBJECT "New booking" SINCE {since_date})'

        status, messages = mail.search(None, search_criteria)
        if status != "OK":
            return []

        email_ids = messages[0].split()
        if not email_ids:
            return []

        # Get latest N emails
        email_ids = email_ids[-max_results:]

        results = []
        for eid in email_ids:
            status, msg_data = mail.fetch(eid, "(RFC822)")
            if status == "OK":
                raw_email = msg_data[0][1]
                msg = email_lib.message_from_bytes(raw_email)
                parsed = parse_civitatis_email(msg)
                if parsed:
                    parsed["email_id"] = eid.decode()
                    # IMAP Message-ID header (RFC822) — único por email, sobrevive restart
                    parsed["message_id"] = (msg.get("Message-ID") or msg.get("Message-Id") or "").strip()
                    # Extract email received date for filtering
                    try:
                        parsed["email_date"] = parsedate_to_datetime(msg["Date"]).replace(tzinfo=None)
                    except Exception:
                        parsed["email_date"] = None
                    results.append(parsed)

        mail.close()
        mail.logout()
        return results
    except Exception as e:
        print(f"[IMAP ERROR] {e}")
        traceback.print_exc()
        return []


# ═══════════════════════════════════════════════════════
# PERSISTENT DEDUPLICATION VIA GOOGLE SHEETS
# ═══════════════════════════════════════════════════════
# Uses a "Launch Log" worksheet in the same spreadsheet to persist
# launched booking numbers. Survives Railway deploys (no more duplicates).

_launched_bookings_cache = {"data": set(), "ts": None}
# Debug log: últimas 20 builds de venda — pra inspecionar o que foi enviado ao create_sale
_build_debug_log = []


def _add_build_debug(entry):
    """Push debug entry to in-memory log (keeps last 20)."""
    _build_debug_log.append(entry)
    while len(_build_debug_log) > 20:
        _build_debug_log.pop(0)
# Cache de bookings já cancelados — usa Launch Log filtrando status="CANCELADO".
# Em-memória pra evitar reprocessar cancelamento toda hora; recarrega do Sheets a cada 2 min.
_cancelled_bookings_cache = {"data": set(), "ts": None}
# Cache de Message-IDs já processados — dedup robusto que SOBREVIVE restart.
# Cada email IMAP tem Message-ID único; se já vimos esse Message-ID, NÃO reprocessar
# (independente de booking_exists no LCX, que tem lag de search).
_processed_msgids_cache = {"data": set(), "ts": None}


def _get_or_create_processed_emails_sheet(gc):
    """Get or create the 'Processed Emails' worksheet."""
    sh = gc.open_by_key(GSHEET_ID)
    try:
        ws = sh.worksheet("Processed Emails")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Processed Emails", rows=2000, cols=4)
        ws.update("A1:D1", [["Message-ID", "Booking #", "Timestamp", "Outcome"]])
        print("[SHEETS] Created 'Processed Emails' worksheet")
    return ws


def load_processed_message_ids():
    """Load set of already-processed IMAP Message-IDs from Sheets."""
    now = datetime.now()
    if _processed_msgids_cache["data"] and _processed_msgids_cache["ts"] and (now - _processed_msgids_cache["ts"]).seconds < 120:
        return _processed_msgids_cache["data"]
    try:
        gc = _get_sheets_client()
        if not gc:
            return _processed_msgids_cache["data"]
        ws = _get_or_create_processed_emails_sheet(gc)
        all_rows = ws.get_all_values()
        mids = set()
        for row in all_rows[1:]:
            if len(row) >= 1 and row[0].strip():
                # FIX 24/06/2026: ignorar outcomes ERRO/CANCEL_ERR/MODIFY_ERR para permitir retry
                outcome = row[3].strip().upper() if len(row) >= 4 else ""
                if outcome == "ERRO" or outcome.endswith("ERR"):
                    continue
                mids.add(row[0].strip())
        mids = mids | _processed_msgids_cache["data"]
        _processed_msgids_cache["data"] = mids
        _processed_msgids_cache["ts"] = now
        print(f"[SHEETS] Loaded {len(mids)} processed Message-IDs")
        return mids
    except Exception as e:
        print(f"[SHEETS] load_processed_message_ids error: {e}")
        return _processed_msgids_cache["data"]


def record_processed_message_id(message_id, booking_number="", outcome="OK"):
    """Record an IMAP Message-ID as processed in Sheets (idempotent dedup)."""
    if not message_id:
        return
    # In-memory cache always updated first (immediate dedup within same scan)
    (_processed_msgids_cache["data"].add(message_id) if not ((outcome or "").upper() == "ERRO" or (outcome or "").upper().endswith("ERR")) else None)
    try:
        gc = _get_sheets_client()
        if not gc:
            return
        ws = _get_or_create_processed_emails_sheet(gc)
        ws.append_row([message_id, booking_number, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), outcome], value_input_option="USER_ENTERED")
    except Exception as e:
        print(f"[SHEETS] record_processed_message_id error: {e}")


def load_cancelled_bookings():
    """Load set of already-cancelled booking numbers from Launch Log (status=CANCELADO)."""
    now = datetime.now()
    if _cancelled_bookings_cache["data"] and _cancelled_bookings_cache["ts"] and (now - _cancelled_bookings_cache["ts"]).seconds < 120:
        return _cancelled_bookings_cache["data"]
    try:
        gc = _get_sheets_client()
        if not gc:
            return _cancelled_bookings_cache["data"]
        ws = _get_or_create_log_sheet(gc)
        all_rows = ws.get_all_values()
        cancelled = set()
        for row in all_rows[1:]:
            if len(row) >= 4 and row[0].strip() and row[3].strip().upper() == "CANCELADO":
                cancelled.add(row[0].strip())
        cancelled = cancelled | _cancelled_bookings_cache["data"]
        _cancelled_bookings_cache["data"] = cancelled
        _cancelled_bookings_cache["ts"] = now
        return cancelled
    except Exception as e:
        print(f"[SHEETS] load_cancelled_bookings error: {e}")
        return _cancelled_bookings_cache["data"]

def _get_sheets_client():
    """Get authenticated gspread client with read/write access."""
    if not GSHEET_CREDS_JSON:
        return None
    try:
        creds_dict = json.loads(GSHEET_CREDS_JSON)
        creds = Credentials.from_service_account_info(creds_dict, scopes=[
            "https://www.googleapis.com/auth/spreadsheets"
        ])
        return gspread.authorize(creds)
    except Exception as e:
        print(f"[SHEETS] Auth error: {e}")
        return None


def _get_or_create_log_sheet(gc):
    """Get or create the 'Launch Log' worksheet."""
    sh = gc.open_by_key(GSHEET_ID)
    try:
        ws = sh.worksheet("Launch Log")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Launch Log", rows=500, cols=6)
        ws.update("A1:F1", [["Booking #", "Timestamp", "Código LCX", "Status", "Sale ID", "Atividade"]])
        print("[SHEETS] Created 'Launch Log' worksheet")
    return ws


def load_launched_bookings():
    """Load set of already-launched booking numbers from Google Sheets."""
    now = datetime.now()
    # Cache for 2 minutes
    if _launched_bookings_cache["data"] and _launched_bookings_cache["ts"] and (now - _launched_bookings_cache["ts"]).seconds < 120:
        return _launched_bookings_cache["data"]

    try:
        gc = _get_sheets_client()
        if not gc:
            print("[SHEETS] No client — using in-memory cache only")
            return _launched_bookings_cache["data"]
        ws = _get_or_create_log_sheet(gc)
        all_rows = ws.get_all_values()
        # Only dedup OK launches — ERRO should be retried
        bookings = set()
        for row in all_rows[1:]:
            if len(row) >= 4 and row[0].strip():
                if row[3].strip().upper() != "ERRO":
                    bookings.add(row[0].strip())
        # Merge with in-memory cache (keeps bookings from failed writes)
        bookings = bookings | _launched_bookings_cache["data"]
        _launched_bookings_cache["data"] = bookings
        _launched_bookings_cache["ts"] = now
        print(f"[SHEETS] Loaded {len(bookings)} launched bookings from log")
        return bookings
    except Exception as e:
        print(f"[SHEETS] Error loading launch log: {e}")
        return _launched_bookings_cache["data"]


def record_launch(booking_number, codigo_lcx, status, sale_id, atividade):
    """Record a launch in the Google Sheets log (persistent)."""
    # Only cache successful launches — ERRO should be retried
    if status == "OK":
        _launched_bookings_cache["data"].add(booking_number)

    try:
        gc = _get_sheets_client()
        if not gc:
            print(f"[SHEETS] No client — cached only: #{booking_number} → {status}")
            return
        ws = _get_or_create_log_sheet(gc)
        ws.append_row([
            booking_number,
            datetime.now().isoformat(),
            codigo_lcx or "",
            status,
            sale_id or "",
            atividade or "",
        ])
        print(f"[SHEETS] Recorded launch: #{booking_number} → {status}")
    except Exception as e:
        print(f"[SHEETS] Error recording launch (cached in memory): {e}")


# ═══════════════════════════════════════════════════════
# ROUTES (minimal — no panel, only health check + status)
# ═══════════════════════════════════════════════════════

def record_to_tracker(em, codigo_lcx, sale_id):
    """Write launched sale to Lucas's tracker Google Sheet in real-time."""
    try:
        gc = _get_sheets_client()
        if not gc:
            print("[TRACKER] No Sheets client")
            return
        sh = gc.open_by_key(TRACKER_SHEET_ID)
        try:
            ws = sh.worksheet("Lançamentos CVT")
        except Exception:
            ws = sh.add_worksheet(title="Lançamentos CVT", rows=500, cols=13)
            ws.update("A1:M1", [["Booking #", "Email Recebido", "Cliente (Dono Reserva)", "Passeio", "Cidade", "Data Tour", "Hora", "Pax", "Preco Venda (R$)", "Preco Liquido (R$)", "Codigo LCX", "Sale ID", "Status"]])
            ws.format("A1:M1", {"textFormat": {"bold": True}})
        bn = em.get("booking_number", "")
        nome = em.get("nome", "")
        sobrenomes = em.get("sobrenomes", "")
        cliente = f"{nome} {sobrenomes}".strip() or em.get("nome_completo", "")
        pax = f"{em.get('num_adults', 0)}A"
        if em.get("num_children", 0) > 0:
            pax += f"+{em['num_children']}C"
        email_dt = em.get("email_date")
        email_str = email_dt.strftime("%Y-%m-%d %H:%M") if email_dt else ""
        ws.append_row([
            bn, email_str, cliente,
            em.get("atividade", ""), em.get("cidade", ""),
            em.get("data_tour", ""), em.get("hora", ""),
            pax,
            em.get("preco_venda", 0), em.get("preco_liquido", 0),
            codigo_lcx or "", sale_id or "", "OK"
        ])
        print(f"[TRACKER] Recorded #{bn} to tracker sheet")
    except Exception as e:
        print(f"[TRACKER] Error writing to tracker: {e}")


def record_alert(alert_type, message, details=""):
    """Write an alert to the 'Alertas' tab of the tracker sheet."""
    try:
        gc = _get_sheets_client()
        if not gc:
            print(f"[ALERT] No Sheets client — alert lost: {message}")
            return
        sh = gc.open_by_key(TRACKER_SHEET_ID)
        try:
            ws = sh.worksheet("Alertas")
        except Exception:
            ws = sh.add_worksheet(title="Alertas", rows=500, cols=5)
            ws.update("A1:E1", [["Data/Hora", "Tipo", "Mensagem", "Detalhes", "Resolvido?"]])
            ws.format("A1:E1", {"textFormat": {"bold": True}})
        ws.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            alert_type,
            message,
            details[:500],
            "NAO"
        ])
        print(f"[ALERT] Recorded: {alert_type} — {message}")
    except Exception as e:
        print(f"[ALERT] Error writing alert: {e}")


def send_urgent_booking_email(em, sale_id, codigo_lcx):
    """Send urgent email when a booking has less than 24h until tour date."""
    try:
        booking_num = em.get("booking_number", "")
        atividade = em.get("atividade", "N/A")
        cidade = em.get("cidade", "N/A")
        data_tour = em.get("data_tour", em.get("data_iso", "N/A"))
        hora = em.get("hora", "N/A")
        num_total = em.get("num_total", em.get("num_adults", "?"))
        num_adults = em.get("num_adults", "?")
        num_children = em.get("num_children", 0)
        preco_liquido = em.get("preco_liquido", "N/A")
        preco_venda = em.get("preco_venda", "N/A")
        ponto_retirada = em.get("ponto_retirada", "N/A")
        comentario = em.get("comentario", "")

        nome = em.get("nome", "")
        sobrenomes = em.get("sobrenomes", "")
        nome_completo = em.get("nome_completo", f"{nome} {sobrenomes}".strip() or "N/A")

        lcx_link = f"https://app.lucascarvalhoturismo.com.br/dashboard/vendas/{sale_id}" if sale_id else "N/A"

        subject = f"URGÊNCIA LC CIVITATIS — #{booking_num} — {atividade} — {data_tour} {hora}"

        lines = [
            "!!! RESERVA COM MENOS DE 24H PARA EXECUCAO !!!",
            "",
            f"Booking:     #{booking_num}",
            f"Tour:        {atividade} ({codigo_lcx})",
            f"Cidade:      {cidade}",
            f"Data/Hora:   {data_tour} às {hora}",
            f"Pax:         {num_total} ({num_adults} adultos, {num_children} crianças)",
            f"Cliente:     {nome_completo}",
            f"Retirada:    {ponto_retirada}",
            f"Preço venda: R$ {preco_venda}",
            f"Preço líq.:  R$ {preco_liquido}",
        ]
        if comentario:
            lines.append(f"Comentário:  {comentario}")
        lines += [
            "",
            f"Ver no LCX: {lcx_link}",
        ]

        body = "\n".join(lines)

        msg = MIMEMultipart()
        msg["From"] = GMAIL_EMAIL
        msg["To"] = "b2b@lucascarvalhoturismo.com.br"
        msg["Subject"] = subject
        msg.attach(MIMETextPart(body, "plain", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_EMAIL, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_EMAIL, "b2b@lucascarvalhoturismo.com.br", msg.as_string())

        print(f"[URGENTE] Email enviado — Booking #{booking_num} em menos de 24h ({data_tour} {hora})")
    except Exception as e:
        print(f"[URGENTE] Erro ao enviar email urgente #{em.get('booking_number', '?')}: {e}")


def write_daily_summary():
    """Write a daily summary row to the 'Resumo Diario' tab of the tracker sheet."""
    try:
        gc = _get_sheets_client()
        if not gc:
            print("[SUMMARY] No Sheets client")
            return
        sh = gc.open_by_key(TRACKER_SHEET_ID)

        # Read today's launches from tracker tab
        try:
            tracker_ws = sh.worksheet("Lançamentos CVT")
            all_rows = tracker_ws.get_all_values()
        except Exception:
            all_rows = []

        today_str = datetime.now().strftime("%Y-%m-%d")
        today_launches = []
        total_liquido = 0
        total_venda = 0
        for row in all_rows[1:]:  # skip header
            if len(row) >= 13 and row[1].startswith(today_str):
                today_launches.append(row)
                try:
                    total_venda += float(row[8]) if row[8] else 0
                except (ValueError, IndexError):
                    pass
                try:
                    total_liquido += float(row[9]) if row[9] else 0
                except (ValueError, IndexError):
                    pass

        # Read today's alerts
        try:
            alerts_ws = sh.worksheet("Alertas")
            alert_rows = alerts_ws.get_all_values()
            today_alerts = [r for r in alert_rows[1:] if len(r) >= 2 and r[0].startswith(today_str)]
        except Exception:
            today_alerts = []

        # Write to Resumo Diario tab
        try:
            summary_ws = sh.worksheet("Resumo Diario")
        except Exception:
            summary_ws = sh.add_worksheet(title="Resumo Diario", rows=500, cols=8)
            summary_ws.update("A1:H1", [["Data", "Total Lancamentos", "Total Venda (R$)", "Total Liquido (R$)", "Total Pax", "Erros/Alertas", "Destinos", "Detalhes"]])
            summary_ws.format("A1:H1", {"textFormat": {"bold": True}})

        # Calculate total pax
        total_pax = 0
        destinos = set()
        for row in today_launches:
            try:
                pax_str = row[7] if len(row) > 7 else "0"
                # Parse "2A+1C" format
                adults = int(re.search(r'(\d+)A', pax_str).group(1)) if 'A' in pax_str else 0
                children = int(re.search(r'(\d+)C', pax_str).group(1)) if 'C' in pax_str else 0
                total_pax += adults + children
            except Exception:
                pass
            if len(row) > 4 and row[4]:
                destinos.add(row[4])

        summary_ws.append_row([
            today_str,
            len(today_launches),
            f"{total_venda:.2f}",
            f"{total_liquido:.2f}",
            total_pax,
            len(today_alerts),
            ", ".join(sorted(destinos)) or "—",
            f"{len(today_launches)} vendas lancadas, {len(today_alerts)} alertas"
        ])
        print(f"[SUMMARY] Daily summary written: {len(today_launches)} launches, R$ {total_liquido:.2f} liquido, {len(today_alerts)} alerts")
    except Exception as e:
        print(f"[SUMMARY] Error writing daily summary: {e}")


def daily_summary_worker():
    """Background thread: write daily summary at DAILY_SUMMARY_HOUR."""
    last_summary_date = None
    while True:
        try:
            now = datetime.now()
            today = now.date()
            # Check yesterday's data at DAILY_SUMMARY_HOUR (e.g. 8 AM)
            if now.hour >= DAILY_SUMMARY_HOUR and last_summary_date != today:
                print(f"[SUMMARY] Generating daily summary for {today}")
                write_daily_summary()
                last_summary_date = today
        except Exception as e:
            print(f"[SUMMARY WORKER] Error: {e}")
        time.sleep(300)  # Check every 5 min


@app.route("/")
def index():
    """Health check — confirms the service is running."""
    return jsonify({
        "service": "CVT Launcher",
        "status": "running",
        "auto_scan": auto_scan_status,
        "go_live": GO_LIVE_DATE,
        "launched_count": len(_launched_bookings_cache["data"]),
    })

# ═══════════════════════════════════════════════════════
# AUTO-SCAN BACKGROUND WORKER
# ═══════════════════════════════════════════════════════

def auto_scan_worker():
    """Background thread: scan every AUTO_SCAN_INTERVAL seconds."""
    go_live = datetime.strptime(GO_LIVE_DATE, "%Y-%m-%d")

    # Wait until go-live date
    while datetime.now() < go_live:
        wait_secs = (go_live - datetime.now()).total_seconds()
        print(f"[AUTO-SCAN] Waiting for go-live {GO_LIVE_DATE}. {wait_secs/3600:.1f}h remaining.")
        time.sleep(min(wait_secs + 60, 3600))

    print(f"[AUTO-SCAN] GO LIVE! Scanning every {AUTO_SCAN_INTERVAL}s")

    while True:
        try:
            auto_scan_status["running"] = True
            auto_scan_status["last_run"] = datetime.now().isoformat()

            hours_since_live = min((datetime.now() - go_live).total_seconds() / 3600, 48)
            hours_since_live = max(hours_since_live, 1)

            emails = fetch_new_booking_emails(max_results=50, since_hours=int(hours_since_live) + 1)
            # SAFETY: only process emails received AFTER go-live datetime
            # This prevents old emails from being launched on first run
            emails = [e for e in emails if e.get("email_date") and e["email_date"] >= go_live]
            # Skip emails before LAUNCH_CUTOFF (prevent relaunching old bookings after redeploy)
            cutoff = datetime.strptime(LAUNCH_CUTOFF, "%Y-%m-%dT%H:%M:%S")
            emails = [e for e in emails if e.get("email_date") and e["email_date"] >= cutoff]
            new_bookings = [e for e in emails if e.get("type") == "NOVA_RESERVA"]
            launched = 0
            skipped = 0
            errors = 0

            # Load already-launched bookings from in-memory cache
            launched_bookings = load_launched_bookings()
            # Load already-processed Message-IDs (dedup robusto que sobrevive restart)
            processed_msgids = load_processed_message_ids()

            # CRITICAL: ensure LCX login BEFORE processing any bookings
            if not lcx_client.logged_in:
                if not lcx_client.login():
                    print("[AUTO-SCAN] LCX login failed — aborting this scan to prevent duplicates")
                    auto_scan_status["last_result"] = f"found={len(new_bookings)} ABORTED: LCX login failed"
                    auto_scan_status["running"] = False
                    record_alert("LOGIN_FALHOU", "LCX login falhou — scan abortado", f"found={len(new_bookings)} bookings pendentes")
                    time.sleep(AUTO_SCAN_INTERVAL)
                    continue

            for em in new_bookings:
                booking_num = em.get("booking_number", "")
                msg_id = em.get("message_id", "")

                # DEDUP ROBUSTO: Message-ID já processado (sobrevive restart, sem lag de LCX search)
                if msg_id and msg_id in processed_msgids:
                    skipped += 1
                    continue

                # DEDUP: check if booking already exists in LCX (searches vendas page)
                if booking_num in launched_bookings:
                    # Mesmo que já lançado, marcar msg_id como processado pra evitar nova checagem
                    if msg_id:
                        record_processed_message_id(msg_id, booking_num, "DEDUP_BY_BOOKING")
                    skipped += 1
                    continue
                if lcx_client.booking_exists(booking_num):
                    # Also add to in-memory cache so we don't re-check next cycle
                    _launched_bookings_cache["data"].add(booking_num)
                    skipped += 1
                    continue

                sale_payload, codigo_lcx = build_lcx_sale(em)

                if not codigo_lcx:
                    # DON'T cache SEM_CODIGO — allow retry when mapping is added later
                    print(f"[AUTO-SCAN] Booking #{'{'}booking_num{'}'} has no tour mapping: {'{'}em.get('atividade', ''){'}'} — will retry next scan")
                    errors += 1
                    continue

                # MISMATCH: codigo_lcx existe mas país/cidade do tour ≠ país/cidade do email.
                # build_lcx_sale já registrou alerta. Aqui marcamos como MISMATCH (não-ERRO)
                # pra dedup bloquear retry infinito até Lucas corrigir o mapeamento.
                if sale_payload is None:
                    print(f"[AUTO-SCAN] Booking #{booking_num}: REJEITADO por mismatch país/cidade (tour {codigo_lcx})")
                    record_launch(booking_num, codigo_lcx, "MISMATCH", "", em.get("atividade", ""))
                    _launched_bookings_cache["data"].add(booking_num)
                    errors += 1
                    continue

                result = lcx_client.create_sale(sale_payload)

                # Ensure we have a valid sale_id for status update
                actual_sale_id = result.get("sale_id", "")
                if result.get("success") and actual_sale_id in ("unknown", "pending", ""):
                    # RSC response didn't return clear sale_id — search LCX for it
                    time.sleep(2)  # Small delay to let LCX persist the sale
                    found_id = lcx_client.find_sale_id(booking_num)
                    if found_id:
                        actual_sale_id = found_id
                        result["sale_id"] = found_id
                        print(f"[AUTO-SCAN] Recovered sale_id {found_id} for booking #{booking_num}")

                if result.get("success") and actual_sale_id and actual_sale_id not in ("unknown", "pending"):
                    # CONFIRMED removido - cliente valida manualmente
                    # lcx_client.update_sale_status(actual_sale_id, "CONFIRMED")
                    record_to_tracker(em, codigo_lcx, actual_sale_id)

                status = "OK" if result.get("success") else "ERRO"
                sale_id = result.get("sale_id", "")

                # Record in Google Sheets (survives deploys!)
                record_launch(booking_num, codigo_lcx, status, sale_id, em.get("atividade", ""))

                # URGÊNCIA: se lançou com sucesso e o tour é em menos de 24h, envia email
                if result.get("success") and sale_id and sale_id not in ("unknown", "pending"):
                    data_iso = em.get("data_iso", "")
                    hora_tour = em.get("hora", "00:00")
                    if data_iso:
                        try:
                            hora_clean = re.sub(r'[^\d:]', '', hora_tour)[:5] or "00:00"
                            tour_dt = datetime.strptime(f"{data_iso} {hora_clean}", "%Y-%m-%d %H:%M")
                            hours_until = (tour_dt - datetime.now()).total_seconds() / 3600
                            if hours_until < 24:
                                print(f"[URGENTE] Booking #{booking_num} em {hours_until:.1f}h — disparando email")
                                send_urgent_booking_email(em, sale_id, codigo_lcx)
                        except Exception as e_urg:
                            print(f"[URGENTE] Erro ao calcular horas até o tour: {e_urg}")

                # Marcar Message-ID como processado (sucesso OU erro — evita reprocessar mesmo email no próximo scan)
                if msg_id:
                    record_processed_message_id(msg_id, booking_num, status)

                if result.get("success"):
                    launched += 1
                else:
                    errors += 1

            # ===== CANCELAMENTOS =====
            # Processar emails type=CANCELAMENTO: muda status LCX → CANCELED + nota com data/hora do email
            cancellations = [e for e in emails if e.get("type") == "CANCELAMENTO"]
            cancelled_count = 0
            cancel_skipped = 0
            cancel_errors = 0
            cancelled_cache = load_cancelled_bookings()

            for em in cancellations:
                booking_num = em.get("booking_number", "")
                msg_id = em.get("message_id", "")
                if not booking_num:
                    continue
                # DEDUP ROBUSTO por Message-ID
                if msg_id and msg_id in processed_msgids:
                    cancel_skipped += 1
                    continue
                if booking_num in cancelled_cache:
                    if msg_id:
                        record_processed_message_id(msg_id, booking_num, "DEDUP_CANCEL")
                    cancel_skipped += 1
                    continue
                # Find sale_id pelo *cvt* #BOOKING
                sale_id = lcx_client.find_sale_id(booking_num)
                if not sale_id:
                    print(f"[CANCEL] Booking #{booking_num} não tem venda no LCX — ignorando cancelamento")
                    _cancelled_bookings_cache["data"].add(booking_num)  # Cache pra não reprocessar
                    cancel_skipped += 1
                    continue
                # Data/hora do email pra observação
                email_dt = em.get("email_date")
                email_date_str = email_dt.strftime("%d/%m/%Y %H:%M") if email_dt else "data desconhecida"
                # Executar cancelamento
                res = lcx_client.cancel_sale_full(sale_id, booking_num, email_date_str)
                if res.get("success"):
                    cancelled_count += 1
                    _cancelled_bookings_cache["data"].add(booking_num)
                    record_launch(booking_num, "", "CANCELADO", sale_id, em.get("atividade", ""))
                    print(f"[CANCEL] Booking #{booking_num} cancelado no LCX (sale {sale_id})")
                else:
                    cancel_errors += 1
                    print(f"[CANCEL] Booking #{booking_num} falhou: {res.get('error')}")
                    record_alert("CANCEL_FALHOU", f"Cancelamento #{booking_num} falhou", res.get("error", ""))
                # Marcar processado independente de sucesso pra não reprocessar
                if msg_id:
                    record_processed_message_id(msg_id, booking_num, "CANCEL_OK" if res.get("success") else "CANCEL_ERR")

            # ===== MODIFICAÇÕES =====
            # Processar emails type=MODIFICACAO: NÃO muda status, só anota na obs interna
            # "Em DD/MM/AAAA HH:MM cliente alterou via email Civitatis"
            modifications = [e for e in emails if e.get("type") == "MODIFICACAO"]
            modified_count = 0
            mod_skipped = 0
            mod_errors = 0

            for em in modifications:
                booking_num = em.get("booking_number", "")
                msg_id = em.get("message_id", "")
                if not booking_num:
                    continue
                # DEDUP por Message-ID (cada modificação é um email único)
                if msg_id and msg_id in processed_msgids:
                    mod_skipped += 1
                    continue
                # Find sale no LCX
                sale_id = lcx_client.find_sale_id(booking_num)
                if not sale_id:
                    print(f"[MOD] Booking #{booking_num} não tem venda no LCX — ignorando modificação")
                    if msg_id:
                        record_processed_message_id(msg_id, booking_num, "MOD_NO_SALE")
                    mod_skipped += 1
                    continue
                # Data/hora do email
                email_dt = em.get("email_date")
                email_date_str = email_dt.strftime("%d/%m/%Y %H:%M") if email_dt else "data desconhecida"
                # Ler current notes
                current = lcx_client.get_sale_notes(sale_id)
                if current is None:
                    current = ""
                mod_marker = f"Modificada via email Civitatis em {email_date_str}"
                # Idempotência: se essa marca exata já está, skip
                if mod_marker in current:
                    if msg_id:
                        record_processed_message_id(msg_id, booking_num, "MOD_DUPLICATE")
                    mod_skipped += 1
                    continue
                new_notes = (current.rstrip() + " | " + mod_marker) if current.strip() else mod_marker
                res = lcx_client.update_sale_notes(sale_id, new_notes)
                if res.get("success"):
                    # Volta status pra PENDING pra B2B revalidar a modificacao
                    try:
                        lcx_client.update_sale_status(sale_id, "PENDING")
                    except Exception as _e:
                        print(f"[MOD] WARN: falha ao setar PENDING em {sale_id}: {_e}")
                    modified_count += 1
                    record_launch(booking_num, "", "MODIFICADO", sale_id, em.get("atividade", ""))
                    print(f"[MOD] Booking #{booking_num} modificação anotada no LCX (sale {sale_id})")
                else:
                    mod_errors += 1
                    print(f"[MOD] Booking #{booking_num} falhou: {res.get('error')}")
                    record_alert("MOD_FALHOU", f"Modificação #{booking_num} falhou", res.get("error", ""))
                if msg_id:
                    record_processed_message_id(msg_id, booking_num, "MOD_OK" if res.get("success") else "MOD_ERR")

            summary = f"found={len(new_bookings)} launched={launched} skipped={skipped} errors={errors} | cancelations={len(cancellations)} cancelled={cancelled_count} cancel_skip={cancel_skipped} cancel_err={cancel_errors} | mods={len(modifications)} modified={modified_count} mod_skip={mod_skipped} mod_err={mod_errors}"
            auto_scan_status["last_result"] = summary
            auto_scan_status["running"] = False
            print(f"[AUTO-SCAN] {summary}")

            # Error alerting: track consecutive scan errors
            global _consecutive_errors
            if errors > 0 and launched == 0:
                _consecutive_errors += 1
                if _consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                    error_details = f"Scan {_consecutive_errors}x consecutivo com erros. Ultimo: {summary}"
                    record_alert("ERROS_CONSECUTIVOS", f"{_consecutive_errors} scans seguidos com erros, 0 lançamentos", error_details)
                    _consecutive_errors = 0  # Reset after alerting
            else:
                _consecutive_errors = 0  # Reset on success

            # Alert on LCX login failure
            if not lcx_client.logged_in:
                record_alert("LOGIN_FALHOU", "LCX login falhou durante auto-scan", "Verificar credenciais")

        except Exception as e:
            auto_scan_status["running"] = False
            auto_scan_status["last_result"] = f"ERROR: {e}"
            print(f"[AUTO-SCAN ERROR] {e}")
            traceback.print_exc()
            record_alert("EXCEPTION", f"Auto-scan crash: {type(e).__name__}", str(e)[:500])

        # BOKUN poller: check new bookings via Bokun API (Viator/GYG/Despegar etc)
        if BOKUN_ENABLED:
            try:
                bokun = BokunClient(BOKUN_ACCESS_KEY, BOKUN_SECRET_KEY)
                items = bokun.list_recent_bookings(since_minutes=15)
                bk_found = len(items)
                bk_launched = bk_skipped = bk_errors = 0
                for bk in items:
                    cc = bk.get("confirmationCode", "")
                    key = f"BKN{cc}"
                    cached = _launched_bookings_cache.get("data", set())
                    if key in cached:
                        bk_skipped += 1
                        continue
                    try:
                        if lcx_client.booking_exists(key):
                            bk_skipped += 1
                            cached.add(key)
                            continue
                    except Exception:
                        bk_skipped += 1
                        continue
                    try:
                        first_pb = (bk.get("productBookings") or [{}])[0]
                        activity = first_pb.get("activity", {}) or {}
                        bk_activity_id = str(activity.get("id", ""))
                        codigo_lcx = BOKUN_TO_LCX.get(bk_activity_id)
                        if not codigo_lcx:
                            print(f"[BOKUN] no LCX mapping for activity {bk_activity_id}")
                            bk_skipped += 1
                            continue
                        tour_id = _resolve_tour_id(codigo_lcx)
                        customer = bk.get("customer", {}) or {}
                        customer_name = (str(customer.get("firstName", "")) + " " + str(customer.get("lastName", ""))).strip() or "Cliente Bokun"
                        passengers = first_pb.get("passengers", []) or []
                        n_pax = len(passengers) or int(first_pb.get("totalParticipants", 1))
                        net_price = float(first_pb.get("vendorPayoutAmount") or first_pb.get("netPrice") or first_pb.get("totalPrice", 0))
                        tour_date = first_pb.get("startDate") or first_pb.get("startDateTime", "")[:10]
                        channel = (bk.get("channel") or {}).get("title", "BOKUN")
                        sale_data = {
                            "customer": {
                                "name": f"{customer_name} *bkn* #{cc}",
                                "email": customer.get("email", ""),
                                "cpfPassport": customer.get("passportId", "") or customer.get("nationalId", ""),
                                "whatsapp": customer.get("phoneNumber", "") or customer.get("mobile", ""),
                            },
                            "tripCountry": "Chile",
                            "tripCity": "Santiago",
                            "meetingPoint": first_pb.get("meetingPoint", "") or first_pb.get("pickupPlace", ""),
                            "tripStartDate": tour_date,
                            "tripEndDate": tour_date,
                            "numberOfPeople": n_pax,
                            "status": "PENDING",
                            "items": [{
                                "country": "Chile",
                                "city": "Santiago",
                                "tourName": activity.get("title", codigo_lcx),
                                "tourId": tour_id,
                                "priceTier": "ADULT",
                                "numberOfPeople": n_pax,
                                "tourDate": tour_date,
                                "price": net_price,
                                "isGift": False,
                            }],
                            "payments": [{"method": "CASH", "amount": net_price, "status": "paid"}],
                            "participants": [{
                                "name": ((str(p.get("firstName", "")) + " " + str(p.get("lastName", ""))).strip() or customer_name),
                                "cpfPassport": p.get("passportId", "") or p.get("nationalId", ""),
                                "whatsapp": p.get("phoneNumber", "") or customer.get("phoneNumber", ""),
                                "dietaryRestrictionLabel": [],
                            } for p in passengers] or [{"name": customer_name, "cpfPassport": "", "whatsapp": "", "dietaryRestrictionLabel": []}],
                            "notes": f"Reserva Bokun #{cc} | Canal: {channel} | Liquido: R$ {net_price:.2f} | Pax: {n_pax}",
                        }
                        res = lcx_client.create_sale(sale_data)
                        if res and res.get("ok"):
                            bk_launched += 1
                            cached.add(key)
                        else:
                            bk_errors += 1
                    except Exception as bk_e:
                        print(f"[BOKUN] error processing #{cc}: {bk_e}")
                        bk_errors += 1
                print(f"[BOKUN] found={bk_found} launched={bk_launched} skipped={bk_skipped} errors={bk_errors}")
            except Exception as bk_exc:
                print(f"[BOKUN] scan failed: {bk_exc}")

        time.sleep(AUTO_SCAN_INTERVAL)


@app.route("/api/test-civitatis-debug")
def test_civitatis_debug():
    """Debug: chama a API JSON com 1 request e retorna detalhes."""
    s = _get_civitatis_session()
    url = f"{CIVITATIS_BASE}/api/providers/bookings/{CIVITATIS_PROVIDER_HASH}"
    try:
        r = s.get(url, params={"lang": "es", "page": "1"}, timeout=20, headers={"Accept": "application/json"}, allow_redirects=False)
        ct = r.headers.get("content-type", "")
        body_head = r.text[:300] if r.text else ""
        try:
            j = r.json()
            return jsonify({"status": r.status_code, "ct": ct, "json_keys": list(j.keys())[:10] if isinstance(j, dict) else type(j).__name__, "count": j.get("count") if isinstance(j, dict) else None, "values_len": len(j.get("values", [])) if isinstance(j, dict) else None, "first_ids": [b.get("id") for b in (j.get("values") or [])[:5]] if isinstance(j, dict) else None})
        except Exception as e:
            return jsonify({"status": r.status_code, "ct": ct, "not_json": str(e), "body_head": body_head, "redirect_to": r.headers.get("Location", "")[:200], "cookie_len": len(CIVITATIS_COOKIE), "provider_hash_len": len(CIVITATIS_PROVIDER_HASH)})
    except Exception as e:
        return jsonify({"err": str(e)})


@app.route("/api/test-civitatis-phone")
def test_civitatis_phone():
    booking = request.args.get("booking", "").strip()
    if not booking:
        return jsonify({"error": "missing booking query param"}), 400
    # Try fast-path first (direct numeric ID)
    phone_fast = civitatis_get_phone_by_booking_id(booking)
    if phone_fast:
        return jsonify({"booking": booking, "step": "fast_path", "phone": phone_fast})
    # Fallback: id_hash via pagination
    id_hash = civitatis_find_booking_id_hash(booking)
    if not id_hash:
        return jsonify({"booking": booking, "step": "find_id_hash", "result": "NOT FOUND",
                        "has_cookie": bool(CIVITATIS_COOKIE),
                        "has_provider_hash": bool(CIVITATIS_PROVIDER_HASH)})
    phone = civitatis_get_phone_from_api(id_hash)
    return jsonify({"booking": booking, "step": "idhash_fallback", "id_hash_len": len(id_hash), "phone": phone or "NOT EXTRACTED"})


@app.route("/api/test-cancel", methods=["POST"])
def test_cancel():
    """Manual cancellation test endpoint.
    POST {"booking": "12345678", "email_date": "DD/MM/AAAA HH:MM"}
    Finds *cvt* #BOOKING in LCX, appends cancellation note + sets status CANCELED."""
    body = request.get_json(silent=True) or {}
    booking = (body.get("booking") or "").strip()
    email_date = (body.get("email_date") or datetime.now().strftime("%d/%m/%Y %H:%M")).strip()
    dry_run = bool(body.get("dry_run", False))
    if not booking:
        return jsonify({"error": "missing 'booking' in JSON body"}), 400
    if not lcx_client.logged_in and not lcx_client.login():
        return jsonify({"error": "LCX login failed"}), 502
    sale_id = lcx_client.find_sale_id(booking)
    if not sale_id:
        return jsonify({"booking": booking, "found": False, "msg": "Não existe venda *cvt* no LCX pra esse booking"})
    if dry_run:
        current = lcx_client.get_sale_notes(sale_id)
        return jsonify({"booking": booking, "sale_id": sale_id, "dry_run": True, "current_notes": current,
                        "would_append": f"Cancelada via email Civitatis em {email_date}"})
    res = lcx_client.cancel_sale_full(sale_id, booking, email_date)
    return jsonify(res)


@app.route("/api/auto-scan-status")
def api_auto_scan_status():
    """Check auto-scan status."""
    return jsonify(auto_scan_status)


@app.route("/api/build-debug")
def api_build_debug():
    """Return last 20 build_lcx_sale debug entries (telefone tracing)."""
    return jsonify(_build_debug_log[-20:])


@app.route("/api/daily-summary", methods=["POST"])
def api_daily_summary():
    """Manually trigger daily summary."""
    write_daily_summary()
    return jsonify({"status": "ok", "message": "Daily summary written"})


# Start background threads on app boot
_scan_thread = threading.Thread(target=auto_scan_worker, daemon=True)
_scan_thread.start()
print(f"[AUTO-SCAN] Thread started. Go-live: {GO_LIVE_DATE}, interval: {AUTO_SCAN_INTERVAL}s")

_summary_thread = threading.Thread(target=daily_summary_worker, daemon=True)
_summary_thread.start()
print(f"[SUMMARY] Daily summary thread started. Report at {DAILY_SUMMARY_HOUR}:00")



@app.route("/api/set_civitatis_cookie", methods=["POST"])
def api_set_civitatis_cookie():
    """Atualiza CIVITATIS_COOKIE em runtime (perde em restart, ok pra hotfix)."""
    if request.args.get("key") != "lc-cvt-hotfix-2026":
        return jsonify({"error": "unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    cookie = body.get("cookie", "").strip()
    if not cookie or len(cookie) < 50:
        return jsonify({"error": "cookie too short"}), 400
    globals()["CIVITATIS_COOKIE"] = cookie
    return jsonify({"ok": True, "cookie_len": len(cookie)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
