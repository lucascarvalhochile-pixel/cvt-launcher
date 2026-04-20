"""
CVT Launcher â Civitatis â LCX Automatic Sales Launcher
Microservice to parse Civitatis new booking emails and create sales in LCX.
"""

import os
import re
import json
import imaplib
import email as email_lib
from email.header import decode_header
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta
import traceback
import unicodedata
import threading
import time
import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# CONFIG
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
LCX_BASE = "https://app.lucascarvalhoturismo.com.br"
LCX_EMAIL = os.environ.get("LCX_EMAIL", "b2b@lucascarvalhoturismo.com.br")
LCX_PASSWORD = os.environ.get("LCX_PASSWORD", "")

GMAIL_EMAIL = os.environ.get("GMAIL_EMAIL", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
GMAIL_IMAP_HOST = "imap.gmail.com"

GSHEET_ID = os.environ.get("GSHEET_ID", "1dgMKZ31puupdU5VbzjfAPg8O_gdZfO7DDMaoMP5PAqQ")
GSHEET_CREDS_JSON = os.environ.get("GSHEET_CREDS_JSON", "")

# Server action IDs (reverse-engineered from LCX)
ACTION_CREATE_SALE = "40cd3e87175fca4124e52bf099976fe85aeb2ea432"
ACTION_GET_TOURS = "40edcb7e2887376ec41091a090643acc3971c39c09"
ACTION_UPDATE_SALE_STATUS = "40e26ca0853df6a6ee31813ceafd6657e8525a7285"
ACTION_UPDATE_SALE_ITEM_STATUS = "60e04b75876ff9dc35df21a885e286e199691081f4"

# Auto-scan config
AUTO_SCAN_INTERVAL = int(os.environ.get("AUTO_SCAN_INTERVAL", "300"))  # 5 min
GO_LIVE_DATE = os.environ.get("GO_LIVE_DATE", "2026-04-18")  # Dedup now via LCX search (no Google Sheets needed)
auto_scan_status = {"last_run": None, "last_result": None, "running": False}

# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# CITY â COUNTRY MAPPING
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
CITY_COUNTRY = {
    "santiago": ("Chile", "Santiago"),
    "santiago de chile": ("Chile", "Santiago"),
    "san pedro de atacama": ("Chile", "Atacama"),
    "atacama": ("Chile", "Atacama"),
    "valparaÃ­so": ("Chile", "Santiago"),
    "viÃ±a del mar": ("Chile", "Santiago"),
    "cartagena": ("ColÃ´mbia", "Cartagena"),
    "cartagena de indias": ("ColÃ´mbia", "Cartagena"),
    "san andrÃ©s": ("ColÃ´mbia", "San Andres"),
    "san andres": ("ColÃ´mbia", "San Andres"),
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


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# GOOGLE SHEETS â READ MAPPING TABLE
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# HARDCODED MAPPING (fallback when Google Sheets is unavailable)
# Source: planilha mapeamento-civitatis-lcx da Karina
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
HARDCODED_MAPPING = {
    # Santiago (19+)
    "estaÃ§Ã£o de esqui portillo e laguna del inca": {"codigo_lcx": "CHISAN067", "nome_lcx": "Portillo e Laguna del Inca"},
    "excursiÃ³n a las termas de colina y el embalse el yeso": {"codigo_lcx": "CHISAN040", "nome_lcx": "CajÃ³n Del Maipo, Embalse El Yeso e Termas de Colina"},
    "excursiÃ³n al parque safari de rancagua": {"codigo_lcx": "CHISAN071", "nome_lcx": "SafÃ¡ri Rancagua"},
    "excursiÃ³n al valle nevado al atardecer": {"codigo_lcx": "CHISAN059", "nome_lcx": "Cordilheira Sunset - VerÃ£o"},
    "excursiÃ³n al viÃ±edo alyan al atardecer": {"codigo_lcx": "CHISAN107", "nome_lcx": "VinÃ­cola Alyan"},
    "excursÃ£o Ã  vinÃ­cola alyan ao entardecer": {"codigo_lcx": "CHISAN107", "nome_lcx": "VinÃ­cola Alyan"},
    "excursÃ£o a isla negra, algarrobo e viÃ±a undurraga": {"codigo_lcx": "CHISAN061", "nome_lcx": "Isla Negra, Algarrobo e Undurraga"},
    "excursÃ£o a valparaÃ­so e viÃ±a del mar": {"codigo_lcx": "CHISAN106", "nome_lcx": "ViÃ±a del Mar e Valparaiso"},
    "excursÃ£o ao cajÃ³n del maipo de moto de neve": {"codigo_lcx": "CHISAN062", "nome_lcx": "Moto Neve em CajÃ³n Del Maipo"},
    "excursÃ£o ao parque de farellones": {"codigo_lcx": "CHISAN034", "nome_lcx": "Andes Full Day - Farellones"},
    "excursÃ£o ao valle nevado": {"codigo_lcx": "CHISAN1682", "nome_lcx": "Andes Full Day - Valle Nevado"},
    "excursÃ£o Ã  vinÃ­cola undurraga": {"codigo_lcx": "CHISAN116", "nome_lcx": "VinÃ­cola Undurraga - Tarde"},
    "excursÃ£o Ã  estaÃ§Ã£o de esqui el colorado": {"codigo_lcx": "CHISAN6706", "nome_lcx": "Andes Full Day - El Colorado"},
    "excursÃ£o Ã s termas valle de colina": {"codigo_lcx": "CHISAN039", "nome_lcx": "CajÃ³n Del Maipo e Termas de Colina"},
    "tour de neve por farellones e valle nevado": {"codigo_lcx": "CHISAN035", "nome_lcx": "Andes PanorÃ¢mico"},
    "tour do vinho casillero del diablo na vinÃ­cola concha y toro": {"codigo_lcx": "CHISAN111", "nome_lcx": "VinÃ­cola Concha y Toro Noturno"},
    "visita guiada pelo centro histÃ³rico de santiago": {"codigo_lcx": "CHISAN055", "nome_lcx": "City Tour Santiago"},
    "visita Ã  vinÃ­cola haras de pirque": {"codigo_lcx": "CHISAN113", "nome_lcx": "VinÃ­cola Haras de Pirque Sunset"},
    # Concha y Toro â multiple tiers (matched by cÃ³digo interno)
    "experiÃªncia centro do vinho concha y toro": {"codigo_lcx": "CHISAN109", "nome_lcx": "Centro del Vinho Concha y Toro - ManhÃ£"},
    "experiÃªncia marquÃ©s de casa concha": {"codigo_lcx": "CHISAN110", "nome_lcx": "VinÃ­cola Concha y Toro Tour do MarquÃ©s - ManhÃ£"},
    # Amor y Pastas
    "experiencia gastronÃ³mica amor y pastas": {"codigo_lcx": "CHISAN033", "nome_lcx": "Amor e Pasta - Tradicional"},
    # Atacama (15)
    "excursÃ£o ao vale do arco-Ã­ris": {"codigo_lcx": "CHIATA020", "nome_lcx": "Vale do Arco-Ãris"},
    "excursÃ£o ao valle de la luna": {"codigo_lcx": "CHIATA021", "nome_lcx": "Valle de la Luna e Pedra do Coyote"},
    "excursÃ£o aos gÃªiseres de el tatio": {"codigo_lcx": "CHIATA007", "nome_lcx": "Geyser del Tatio"},
    "excursÃ£o de 4 dias ao salar de uyuni": {"codigo_lcx": "CHIUYU128", "nome_lcx": "Uyuni Compartilhado (4D3N)"},
    "excursÃ£o Ã  cordilheira do sal": {"codigo_lcx": "CHIATA023", "nome_lcx": "Vallecito"},
    "excursÃ£o Ã s lagunas escondidas de baltinache": {"codigo_lcx": "CHIATA010", "nome_lcx": "Lagunas Escondidas de Baltinache - ManhÃ£"},
    "excursÃ£o Ã s termas de puritama": {"codigo_lcx": "CHIATA014", "nome_lcx": "Termas de Puritama - ManhÃ£"},
    "observaÃ§Ã£o de estrelas no deserto de atacama": {"codigo_lcx": "CHIATA016", "nome_lcx": "Tour AstronÃ´mico"},
    "passeio de balÃ£o por san pedro de atacama": {"codigo_lcx": "CHIATA017", "nome_lcx": "Tour de BalÃ£o"},
    "rota dos salares": {"codigo_lcx": "CHIATA012", "nome_lcx": "Ruta de los Salares"},
    "sandboarding en el valle de la muerte": {"codigo_lcx": "CHIATA013", "nome_lcx": "Sandboard"},
    "tour en bicicleta por la garganta del diablo": {"codigo_lcx": "CHIATA018", "nome_lcx": "Tour de Bike - ManhÃ£"},
    "trekking por el volcÃ¡n cerro toco": {"codigo_lcx": "CHIATA024", "nome_lcx": "VulcÃ£o Cerro Toco"},
    "trilha por purilibre": {"codigo_lcx": "CHIATA019", "nome_lcx": "Trekking de Purilibre - ManhÃ£"},
    # Cartagena (9)
    "excursÃ£o ao isla lizamar beach club": {"codigo_lcx": "COLCAR025", "nome_lcx": "Lizamar Beach Club"},
    "excursÃ£o ao mangata ocean club": {"codigo_lcx": "COLCAR034", "nome_lcx": "Mangata Beach Club"},
    "excursÃ£o ao palmarito beach": {"codigo_lcx": "COLCAR040", "nome_lcx": "Palmarito Beach â Tierra Bomba"},
    "excursÃ£o ao vulcÃ£o el totumo": {"codigo_lcx": "COLCAR055", "nome_lcx": "VolcÃ¡n del Totumo"},
    "excursÃ£o Ã  ilha mÃºcura": {"codigo_lcx": "COLCAR000", "nome_lcx": "3 lslas + San Bernardo"},
    "excursÃ£o Ã s ilhas de cartagena + plÃ¢ncton luminescente": {"codigo_lcx": "COLCAR002", "nome_lcx": "5 Islas Vip + Plancton"},
    "festa noturna de barco por cartagena": {"codigo_lcx": "COLCAR036", "nome_lcx": "Noche Blanca"},
    "tour de barco pirata pela baÃ­a de cartagena": {"codigo_lcx": "COLCAR003", "nome_lcx": "Barco Pirata"},
    "tour de chiva rumbera por cartagena das Ã­ndias": {"codigo_lcx": "COLCAR012", "nome_lcx": "City Tour no Ãnibus Chiva - ManhÃ£"},
    # San AndrÃ©s (8)
    "excursÃ£o a johnny cay + aquÃ¡rio natural": {"codigo_lcx": "COLSAO080", "nome_lcx": "Passeio do Barco Johnny Cay e AquÃ¡rio Natural"},
    "festa no bar flutuante ibiza": {"codigo_lcx": "COLSAO064", "nome_lcx": "Bar Ibiza Sai"},
    "parasailing em san andrÃ©s": {"codigo_lcx": "COLSAO077", "nome_lcx": "Parasail - ManhÃ£"},
    "passeio de barco semisubmarino por san andrÃ©s": {"codigo_lcx": "COLSAO081", "nome_lcx": "Semisubmarino - ManhÃ£"},
    "seawalker em san andrÃ©s": {"codigo_lcx": "COLSAO063", "nome_lcx": "Aquanautas - ManhÃ£"},
    "snorkel em san andrÃ©s": {"codigo_lcx": "COLSAO072", "nome_lcx": "Mergulho com Snorkel - ManhÃ£"},
    "tour de caiaque transparente pelos manguezais de san andrÃ©s": {"codigo_lcx": "COLSAO068", "nome_lcx": "ECOFIWI Caiaque Transparente - ManhÃ£"},
    # Lima (1)
    "excursÃ£o a ica e huacachina + ilhas ballestas": {"codigo_lcx": "PERLIM024", "nome_lcx": "Islas Ballestas y Desierto Huacachina"},
    # Cusco (3)
    "excursÃ£o ao vale sagrado dos incas + maras, moray e ollantaytambo": {"codigo_lcx": "PERCUS020", "nome_lcx": "Valle Sagrado + Moray e Maras"},
    "excursÃ£o Ã  lagoa humantay": {"codigo_lcx": "PERCUS005", "nome_lcx": "Laguna Humantay"},
    "visita guiada por cusco e suas 4 ruÃ­nas": {"codigo_lcx": "PERCUS002", "nome_lcx": "City Tour em Cusco - ManhÃ£"},
}


_mapping_cache = {"data": None, "ts": None}

def _fix_mojibake(s):
    """Fix double-encoded UTF-8 (mojibake) in hardcoded map keys."""
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return s


def load_mapping():
    """Load CivitatisâLCX mapping. Tries Google Sheets first, falls back to hardcoded."""
    now = datetime.now()
    if _mapping_cache["data"] and _mapping_cache["ts"] and (now - _mapping_cache["ts"]).seconds < 300:
        return _mapping_cache["data"]

    # Fix mojibake in hardcoded keys (double-encoded UTF-8 from GitHub editor)
    mapping = {}
    for k, v in HARDCODED_MAPPING.items():
        clean_key = _fix_mojibake(k)
        mapping[clean_key] = v

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
                    if nome_cvt and codigo_lcx and not codigo_lcx.startswith("â¸"):
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


def find_lcx_tour(atividade, codigo_interno):
    """Find LCX tour code from Civitatis activity name or internal code."""
    mapping = load_mapping()
    if not mapping:
        return None, None

    key = unicodedata.normalize("NFC", atividade.strip().lower())
    # Remove language/tier suffix: " - Tour em portuguÃªs", " - Tour com retirada + ..."
    key_clean = re.sub(r'\s*-\s*tour\s+.*$', '', key, flags=re.IGNORECASE).strip()

    # 1. Exact match on clean activity name
    if key_clean in mapping:
        m = mapping[key_clean]
        return m["codigo_lcx"], m["nome_lcx"]

    # 2. Match via cÃ³digo interno (e.g. "Valle Nevado Ski (Full) Day")
    if codigo_interno:
        cod_lower = codigo_interno.strip().lower()
        for k, v in mapping.items():
            if cod_lower in k or k in cod_lower:
                return v["codigo_lcx"], v["nome_lcx"]

    # 3. Partial match on activity name
    for k, v in mapping.items():
        if key_clean in k or k in key_clean:
            return v["codigo_lcx"], v["nome_lcx"]

    # 4. Word overlap match (at least 3 significant words in common)
    key_words = set(w for w in key_clean.split() if len(w) > 3)
    best_match = None
    best_score = 0
    for k, v in mapping.items():
        k_words = set(w for w in k.split() if len(w) > 3)
        overlap = len(key_words & k_words)
        if overlap > best_score and overlap >= 3:
            best_score = overlap
            best_match = v
    if best_match:
        return best_match["codigo_lcx"], best_match["nome_lcx"]

    return None, None


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# EMAIL PARSER â CIVITATIS NEW BOOKING
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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

    # Clean the text first â Civitatis HTML produces massive whitespace
    text = _clean_text(text)
    data["raw_text"] = text[:3000]

    # Check it's a "Nova reserva" (not modification or cancellation)
    if "Nova reserva" not in text:
        if "reserva foi cancelada" in text.lower() or "cancelamento" in text.lower():
            data["type"] = "CANCELAMENTO"
            return data
        if "modificaÃ§Ã£o" in text.lower() or "modificada" in text.lower():
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
    data["codigo_interno"] = extract("CÃ³digo interno")
    data["data_tour"] = extract("Data")
    data["hora"] = extract("Hora")
    data["ponto_retirada"] = extract("Ponto de retirada")

    # Booking number from "NÃºmero da reserva:" if not already set
    if not booking_number:
        nr = extract("NÃºmero da reserva")
        if nr:
            data["booking_number"] = nr.strip()

    # Nome completo (different from passenger Nome)
    nome_completo = extract("Nome completo")
    if nome_completo:
        data["nome_completo"] = nome_completo

    # Parse pessoas (people breakdown)
    # Formats: "2 adultos x R$290", "2 Por pessoa x US$126", "1 adulto + 1 crianÃ§a"
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

    # Prices â tolerate newlines between label and R$
    preco_venda = re.search(r'Preço de venda\s*\n?\s*R\$\s*\n?\s*([\d.,]+)', text)
    preco_liquido = re.search(r'Preço líquido\s*\n?\s*R\$\s*\n?\s*([\d.,]+)', text)
    # Also try "Preço total" as fallback (some email formats)
    preco_total = re.search(r'Preço total\s*\n?\s*R\$\s*\n?\s*([\d.,]+)', text)

    def parse_brl(match):
        if not match:
            return ""
        val = match.group(1).strip()
        # Handle BR format: "1.234,56" â "1234.56", "1.260" â "1260", "502,50" â "502.50"
        if "," in val:
            # Has comma = decimal separator. Dots are thousands.
            val = val.replace(".", "").replace(",", ".")
        elif "." in val:
            # Only dots, no comma. If format is "X.XXX" (thousands), remove dot.
            # "1.260" = 1260 (thousands), "502.50" = 502.50 (decimal)
            parts = val.split(".")
            if len(parts) == 2 and len(parts[1]) == 3:
                # "1.260" â thousands separator
                val = val.replace(".", "")
            # else keep as-is (already decimal format like "502.50")
        return val

    data["preco_venda"] = parse_brl(preco_venda) or parse_brl(preco_total)
    data["preco_liquido"] = parse_brl(preco_liquido)

    # Customer data (from "Dados do cliente" section)
    nome = re.search(r'Nome:\s*\n?\s*(.+)', text)
    sobrenomes = re.search(r'Sobrenomes?:\s*\n?\s*(.+)', text)

    data["nome"] = nome.group(1).strip() if nome else ""
    data["sobrenomes"] = sobrenomes.group(1).strip() if sobrenomes else ""

    # ComentÃ¡rios
    comentario = re.search(r'Coment[áa]rios?:\s*\n?\s*(.+?)(?:\n\n|$)', text, re.DOTALL | re.IGNORECASE)
    data["comentario"] = comentario.group(1).strip() if comentario else ""

    # Parse passengers â "Dados passageiro N:" blocks
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
        p["dietaryRestriction"] = pext(r"Restri[Ã§c][Ãµo]es?\s+alimentar\w*")

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
        "janeiro": 1, "fevereiro": 2, "marÃ§o": 3, "abril": 4,
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


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# LCX INTEGRATION
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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
        """Update sale status (PENDING â CONFIRMED)."""
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

    def booking_exists(self, booking_number):
        """Check if a CVT booking already exists in LCX by searching for *cvt* #BOOKING.
        FAIL-CLOSED: if we can't check, return True (assume exists) to prevent duplicates."""
        if not self.logged_in:
            if not self.login():
                print(f"[LCX-DEDUP] Cannot check booking #{booking_number} â login failed, BLOCKING launch")
                return True  # FAIL-CLOSED: don't create if we can't verify
        try:
            r = self.session.get(
                f"{LCX_BASE}/dashboard/vendas?search={booking_number}",
                headers={"Accept": "text/html"},
                timeout=15,
            )
            if r.status_code == 200:
                # Check if any sale link appears AND the booking number is in a *cvt* tag
                has_sale = bool(re.search(r'href="/dashboard/vendas/cm[a-z0-9]+"', r.text))
                has_booking = f"*cvt* #{booking_number}" in r.text or f"*cvt*#{booking_number}" in r.text
                exists = has_sale and has_booking
                if exists:
                    print(f"[LCX-DEDUP] Booking #{booking_number} ALREADY EXISTS in LCX â skipping")
                else:
                    print(f"[LCX-DEDUP] Booking #{booking_number} NOT found in LCX â will create")
                return exists
            print(f"[LCX-DEDUP] HTTP {r.status_code} checking #{booking_number} â BLOCKING launch")
            return True  # FAIL-CLOSED
        except Exception as e:
            print(f"[LCX-DEDUP] Error checking booking #{booking_number}: {e} â BLOCKING launch")
            return True  # FAIL-CLOSED: don't create if we can't verify


lcx_client = LCXClient()


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
    """Build LCX createSale payload from parsed Civitatis email data."""
    data = parsed_email
    country, city = resolve_country_city(data.get("cidade", ""))

    # Find LCX tour mapping
    codigo_lcx, nome_lcx = find_lcx_tour(
        data.get("atividade", ""),
        data.get("codigo_interno", "")
    )

    # Customer name: booking OWNER (Dados do cliente) + *cvt* + booking number
    # Priority: 1) Dados do cliente (nome + sobrenomes), 2) Passenger 1 fallback
    owner_name = f"{data.get('nome', '')} {data.get('sobrenomes', '')}".strip()
    if owner_name:
        customer_name = owner_name
    elif data.get("passageiros") and data["passageiros"][0].get("name"):
        customer_name = data["passageiros"][0]["name"].strip()
    else:
        customer_name = "Cliente Civitatis"
    booking_num = data.get("booking_number", "")
    customer_name += f" *cvt* #{booking_num}" if booking_num else " *cvt*"

    tour_name = nome_lcx or data.get("atividade", "Tour Civitatis")
    tour_date = data.get("data_iso", "")
    num_people = data.get("num_total", 1) or 1

    # Use preÃ§o LÃQUIDO (net price), not preÃ§o de venda
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
        f"CÃ³digo interno: {data.get('codigo_interno', '')}",
        f"PreÃ§o venda: R$ {data.get('preco_venda', '0')}",
        f"PreÃ§o lÃ­quido: R$ {data.get('preco_liquido', '0')}",
        f"HorÃ¡rio: {data.get('hora', '')}",
    ]
    if data.get("comentario"):
        notes_parts.append(f"ComentÃ¡rio cliente: {data['comentario']}")
    notes = " | ".join([p for p in notes_parts if p])

    # Build items array â 1 item per priceTier, price = total for that tier
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

    # Build payment â DINHEIRO (CASH) + PAGO (paid)
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
            if p.get("dietaryRestriction") and p["dietaryRestriction"].lower() not in ("nao", "nÃ£o", "no", "none", ""):
                diet_labels.append(p["dietaryRestriction"])
            participants.append({
                "name": p.get("name", "Participante"),
                "email": "",
                "cpfPassport": p.get("cpfPassport", ""),
                "whatsapp": p.get("whatsapp", ""),
                "dietaryRestrictionLabel": diet_labels,
            })
    # Fallback: use customer data if no detailed passengers
    if not participants:
        participants.append({
            "name": customer_name.replace(" *cvt*", ""),
            "email": data.get("email_cliente", ""),
            "cpfPassport": data.get("documento", ""),
            "whatsapp": data.get("telefone", ""),
        })

    # Resolve tourId from LCX via getTours API if we have a code
    tour_id = None
    if codigo_lcx:
        tour_id = _resolve_tour_id(codigo_lcx)

    # If we got a tourId, add it to each item
    if tour_id:
        for item in items:
            item["tourId"] = tour_id

    sale = {
        "customer": {
            "name": customer_name,
            "email": data.get("email_cliente", ""),
            "cpfPassport": data.get("documento", ""),
            "whatsapp": data.get("telefone", ""),
        },
        "tripCountry": country,
        "tripCity": city,
        "meetingPoint": meeting_point,
        "tripStartDate": tour_date,
        "tripEndDate": tour_date,
        "numberOfPeople": num_people,
        "status": "CONFIRMED",
        "items": items,
        "payments": payments,
        "participants": participants,
        "notes": notes,
    }

    return sale, codigo_lcx


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# GMAIL IMAP â FETCH NEW BOOKING EMAILS
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# PERSISTENT DEDUPLICATION VIA GOOGLE SHEETS
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Uses a "Launch Log" worksheet in the same spreadsheet to persist
# launched booking numbers. Survives Railway deploys (no more duplicates).

_launched_bookings_cache = {"data": set(), "ts": None}

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
        ws.update("A1:F1", [["Booking #", "Timestamp", "CÃ³digo LCX", "Status", "Sale ID", "Atividade"]])
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
            print("[SHEETS] No client â using in-memory cache only")
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
            print(f"[SHEETS] No client â cached only: #{booking_number} â {status}")
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
        print(f"[SHEETS] Recorded launch: #{booking_number} â {status}")
    except Exception as e:
        print(f"[SHEETS] Error recording launch (cached in memory): {e}")


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# ROUTES (minimal â no panel, only health check + status)
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.route("/")
def index():
    """Health check â confirms the service is running."""
    return jsonify({
        "service": "CVT Launcher",
        "status": "running",
        "auto_scan": auto_scan_status,
        "go_live": GO_LIVE_DATE,
        "launched_count": len(_launched_bookings_cache["data"]),
    })

# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# AUTO-SCAN BACKGROUND WORKER
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââ

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
            new_bookings = [e for e in emails if e.get("type") == "NOVA_RESERVA"]
            launched = 0
            skipped = 0
            errors = 0

            # Load already-launched bookings from in-memory cache
            launched_bookings = load_launched_bookings()

            # CRITICAL: ensure LCX login BEFORE processing any bookings
            if not lcx_client.logged_in:
                if not lcx_client.login():
                    print("[AUTO-SCAN] LCX login failed â aborting this scan to prevent duplicates")
                    auto_scan_status["last_result"] = f"found={len(new_bookings)} ABORTED: LCX login failed"
                    auto_scan_status["running"] = False
                    time.sleep(AUTO_SCAN_INTERVAL)
                    continue

            for em in new_bookings:
                booking_num = em.get("booking_number", "")

                # DEDUP: check if booking already exists in LCX (searches vendas page)
                if booking_num in launched_bookings:
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

                result = lcx_client.create_sale(sale_payload)

                if result.get("success") and result.get("sale_id") and result["sale_id"] not in ("unknown", "pending"):
                    lcx_client.update_sale_status(result["sale_id"], "CONFIRMED")

                status = "OK" if result.get("success") else "ERRO"
                sale_id = result.get("sale_id", "")

                # Record in Google Sheets (survives deploys!)
                record_launch(booking_num, codigo_lcx, status, sale_id, em.get("atividade", ""))

                if result.get("success"):
                    launched += 1
                else:
                    errors += 1

            summary = f"found={len(new_bookings)} launched={launched} skipped={skipped} errors={errors}"
            auto_scan_status["last_result"] = summary
            auto_scan_status["running"] = False
            print(f"[AUTO-SCAN] {summary}")

        except Exception as e:
            auto_scan_status["running"] = False
            auto_scan_status["last_result"] = f"ERROR: {e}"
            print(f"[AUTO-SCAN ERROR] {e}")
            traceback.print_exc()

        time.sleep(AUTO_SCAN_INTERVAL)


@app.route("/api/auto-scan-status")
def api_auto_scan_status():
    """Check auto-scan status."""
    return jsonify(auto_scan_status)


# Start background thread on app boot
_scan_thread = threading.Thread(target=auto_scan_worker, daemon=True)
_scan_thread.start()
print(f"[AUTO-SCAN] Thread started. Go-live: {GO_LIVE_DATE}, interval: {AUTO_SCAN_INTERVAL}s")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
