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
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import traceback
import threading
import time
import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify, render_template_string
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
GSHEET_CREDS_JSON = os.environ.get("GSHEET_CREDS_JSON", "")

# Server action IDs (reverse-engineered from LCX)
ACTION_CREATE_SALE = "40cd3e87175fca4124e52bf099976fe85aeb2ea432"
ACTION_GET_TOURS = "40edcb7e2887376ec41091a090643acc3971c39c09"
ACTION_UPDATE_SALE_STATUS = "40e26ca0853df6a6ee31813ceafd6657e8525a7285"
ACTION_UPDATE_SALE_ITEM_STATUS = "60e04b75876ff9dc35df21a885e286e199691081f4"

# In-memory log of launches
launch_log = []

# Auto-scan config
BATCH_HOURS = [int(h) for h in os.environ.get("BATCH_HOURS", "7,13,20").split(",")]  # hours to run batches
GO_LIVE_DATE = os.environ.get("GO_LIVE_DATE", "2026-04-18")  # only process emails from this date onward
auto_scan_status = {"last_run": None, "last_result": None, "running": False, "next_batch": None}

# Daily summary config
SUMMARY_EMAIL_TO = os.environ.get("SUMMARY_EMAIL_TO", "lucascarvalhochile@gmail.com")
SUMMARY_HOUR = int(os.environ.get("SUMMARY_HOUR", "7"))  # send at 7am

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
    "excursión al valle nevado al atardecer": {"codigo_lcx": "CHISAN059", "nome_lcx": "Cordilheira Sunset - Verão"},
    "excursión al viñedo alyan al atardecer": {"codigo_lcx": "CHISAN107", "nome_lcx": "Vinícola Alyan"},
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
    "excursão aos gêiseres de el tatio": {"codigo_lcx": "CHIATA007", "nome_lcx": "Geyser del Tatio"},
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
    "excursão a johnny cay + aquário natural": {"codigo_lcx": "COLSAO080", "nome_lcx": "Passeio do Barco Johnny Cay e Aquário Natural"},
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
    "visita guiada por cusco e suas 4 ruínas": {"codigo_lcx": "PERCUS002", "nome_lcx": "City Tour em Cusco - Manhã"},
}


_mapping_cache = {"data": None, "ts": None}

def load_mapping():
    """Load Civitatis→LCX mapping. Tries Google Sheets first, falls back to hardcoded."""
    now = datetime.now()
    if _mapping_cache["data"] and _mapping_cache["ts"] and (now - _mapping_cache["ts"]).seconds < 300:
        return _mapping_cache["data"]

    mapping = dict(HARDCODED_MAPPING)  # Start with hardcoded as base

    # Try to overlay with live Google Sheets data
    try:
        if GSHEET_CREDS_JSON:
            creds_dict = json.loads(GSHEET_CREDS_JSON)
            creds = Credentials.from_service_account_info(creds_dict, scopes=[
                "https://www.googleapis.com/auth/spreadsheets.readonly"
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
                        mapping[nome_cvt.lower()] = {
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

    key = atividade.strip().lower()
    # Remove language/tier suffix: " - Tour em português", " - Tour com retirada + ..."
    key_clean = re.sub(r'\s*-\s*tour\s+.*$', '', key, flags=re.IGNORECASE).strip()

    # 1. Exact match on clean activity name
    if key_clean in mapping:
        m = mapping[key_clean]
        return m["codigo_lcx"], m["nome_lcx"]

    # 2. Match via código interno (e.g. "Valle Nevado Ski (Full) Day")
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
    nome = re.search(r'Nome:\s*\n?\s*(.+)', text)
    sobrenomes = re.search(r'Sobrenomes?:\s*\n?\s*(.+)', text)

    data["nome"] = nome.group(1).strip() if nome else ""
    data["sobrenomes"] = sobrenomes.group(1).strip() if sobrenomes else ""

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

    # Customer name: full name of first passenger + *cvt* + booking number
    if data.get("passageiros") and data["passageiros"][0].get("name"):
        customer_name = data["passageiros"][0]["name"].strip()
    else:
        customer_name = f"{data.get('nome', '')} {data.get('sobrenomes', '')}".strip()
    if not customer_name:
        customer_name = "Cliente Civitatis"
    booking_num = data.get("booking_number", "")
    customer_name += f" *cvt* #{booking_num}" if booking_num else " *cvt*"

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
                    results.append(parsed)

        mail.close()
        mail.logout()
        return results
    except Exception as e:
        print(f"[IMAP ERROR] {e}")
        traceback.print_exc()
        return []


# ═══════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template_string(PANEL_HTML, launches=launch_log)


@app.route("/api/scan", methods=["POST"])
def api_scan():
    """Scan Gmail for new Civitatis booking emails."""
    hours = request.json.get("hours", 24) if request.is_json else 24
    emails = fetch_new_booking_emails(max_results=20, since_hours=hours)

    new_bookings = [e for e in emails if e.get("type") == "NOVA_RESERVA"]
    skipped = [e for e in emails if e.get("type") != "NOVA_RESERVA"]

    return jsonify({
        "total_found": len(emails),
        "new_bookings": len(new_bookings),
        "skipped": len(skipped),
        "bookings": new_bookings,
    })


@app.route("/api/launch", methods=["POST"])
def api_launch():
    """Launch a single sale to LCX from parsed email data."""
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400

    sale_payload, codigo_lcx = build_lcx_sale(data)

    # Check if we have a valid mapping
    if not codigo_lcx:
        entry = {
            "timestamp": datetime.now().isoformat(),
            "booking_number": data.get("booking_number", ""),
            "atividade": data.get("atividade", ""),
            "cidade": data.get("cidade", ""),
            "status": "SEM_CODIGO",
            "error": "Tour não mapeado na planilha",
            "sale_payload": sale_payload,
        }
        launch_log.insert(0, entry)
        return jsonify({"success": False, "error": "Tour sem código LCX mapeado", "entry": entry})

    # Create sale in LCX
    result = lcx_client.create_sale(sale_payload)

    # If sale created, update status to CONFIRMED
    if result.get("success") and result.get("sale_id") and result["sale_id"] not in ("unknown", "pending"):
        status_result = lcx_client.update_sale_status(result["sale_id"], "CONFIRMED")
        if not status_result.get("success"):
            print(f"[WARN] Sale created but status update failed: {status_result.get('error')}")

    entry = {
        "timestamp": datetime.now().isoformat(),
        "booking_number": data.get("booking_number", ""),
        "atividade": data.get("atividade", ""),
        "cidade": data.get("cidade", ""),
        "cliente": f"{data.get('nome', '')} {data.get('sobrenomes', '')}".strip(),
        "num_pessoas": data.get("num_total", 0),
        "preco_venda": data.get("preco_venda", ""),
        "codigo_lcx": codigo_lcx,
        "status": "OK" if result.get("success") else "ERRO",
        "sale_id": result.get("sale_id", ""),
        "error": result.get("error", ""),
    }
    launch_log.insert(0, entry)

    return jsonify({"success": result.get("success"), "entry": entry, "result": result})


@app.route("/api/launch-all", methods=["POST"])
def api_launch_all():
    """Scan and launch all new bookings."""
    hours = request.json.get("hours", 24) if request.is_json else 24
    emails = fetch_new_booking_emails(max_results=50, since_hours=hours)

    results = []
    for em in emails:
        if em.get("type") != "NOVA_RESERVA":
            continue

        # Check if already launched
        already = any(
            l.get("booking_number") == em.get("booking_number") and l.get("status") == "OK"
            for l in launch_log
        )
        if already:
            results.append({"booking": em.get("booking_number"), "status": "JA_LANCADO"})
            continue

        sale_payload, codigo_lcx = build_lcx_sale(em)

        if not codigo_lcx:
            entry = {
                "timestamp": datetime.now().isoformat(),
                "booking_number": em.get("booking_number", ""),
                "atividade": em.get("atividade", ""),
                "cidade": em.get("cidade", ""),
                "status": "SEM_CODIGO",
                "error": "Tour não mapeado",
            }
            launch_log.insert(0, entry)
            results.append({"booking": em.get("booking_number"), "status": "SEM_CODIGO"})
            continue

        result = lcx_client.create_sale(sale_payload)

        # Update status to CONFIRMED after creation
        if result.get("success") and result.get("sale_id") and result["sale_id"] not in ("unknown", "pending"):
            lcx_client.update_sale_status(result["sale_id"], "CONFIRMED")

        entry = {
            "timestamp": datetime.now().isoformat(),
            "booking_number": em.get("booking_number", ""),
            "atividade": em.get("atividade", ""),
            "cidade": em.get("cidade", ""),
            "cliente": f"{em.get('nome', '')} {em.get('sobrenomes', '')}".strip(),
            "num_pessoas": em.get("num_total", 0),
            "preco_venda": em.get("preco_venda", ""),
            "codigo_lcx": codigo_lcx,
            "status": "OK" if result.get("success") else "ERRO",
            "sale_id": result.get("sale_id", ""),
            "error": result.get("error", ""),
        }
        launch_log.insert(0, entry)
        results.append({"booking": em.get("booking_number"), "status": entry["status"]})

    return jsonify({"total": len(results), "results": results})


@app.route("/api/log")
def api_log():
    return jsonify(launch_log)


@app.route("/api/test-parse", methods=["POST"])
def api_test_parse():
    """Test email parsing without launching. For development/testing."""
    hours = request.json.get("hours", 48) if request.is_json else 48
    emails = fetch_new_booking_emails(max_results=5, since_hours=hours)

    enriched = []
    for em in emails:
        if em.get("type") == "NOVA_RESERVA":
            sale_payload, codigo_lcx = build_lcx_sale(em)
            em["_lcx_sale_preview"] = sale_payload
            em["_codigo_lcx"] = codigo_lcx
        enriched.append(em)

    return jsonify(enriched)


@app.route("/api/test-lcx-login")
def api_test_lcx_login():
    """Test LCX login."""
    ok = lcx_client.login()
    return jsonify({"logged_in": ok})


# ═══════════════════════════════════════════════════════
# PANEL HTML
# ═══════════════════════════════════════════════════════
PANEL_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CVT Launcher — Civitatis → LCX</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family: 'Segoe UI', system-ui, sans-serif; background:#0f172a; color:#e2e8f0; min-height:100vh; }
.header { background:linear-gradient(135deg,#1e293b,#334155); padding:20px 30px; display:flex; justify-content:space-between; align-items:center; border-bottom:2px solid #f43f5e; }
.header h1 { font-size:22px; color:#f8fafc; }
.header h1 span { color:#f43f5e; }
.header .badge { background:#f43f5e; color:white; padding:4px 12px; border-radius:20px; font-size:13px; }
.controls { padding:20px 30px; display:flex; gap:12px; flex-wrap:wrap; }
.btn { padding:10px 20px; border:none; border-radius:8px; cursor:pointer; font-size:14px; font-weight:600; transition:all .2s; }
.btn-primary { background:#3b82f6; color:white; }
.btn-primary:hover { background:#2563eb; }
.btn-success { background:#10b981; color:white; }
.btn-success:hover { background:#059669; }
.btn-warning { background:#f59e0b; color:#1e293b; }
.btn-warning:hover { background:#d97706; }
.btn-danger { background:#ef4444; color:white; }
.status-bar { padding:10px 30px; font-size:13px; color:#94a3b8; }
.table-wrap { padding:0 30px 30px; overflow-x:auto; }
table { width:100%; border-collapse:collapse; }
th { background:#1e293b; color:#94a3b8; font-size:12px; text-transform:uppercase; padding:12px 15px; text-align:left; position:sticky; top:0; }
td { padding:10px 15px; border-bottom:1px solid #1e293b; font-size:13px; }
tr:hover { background:#1e293b40; }
.badge-ok { background:#10b981; color:white; padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600; }
.badge-erro { background:#ef4444; color:white; padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600; }
.badge-sem { background:#f59e0b; color:#1e293b; padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600; }
.empty { text-align:center; padding:60px; color:#64748b; }
.toast { position:fixed; top:20px; right:20px; background:#1e293b; border:1px solid #334155; border-radius:10px; padding:15px 20px; display:none; z-index:999; max-width:400px; box-shadow:0 4px 20px rgba(0,0,0,.4); }
#scanResults { padding:0 30px; }
.scan-card { background:#1e293b; border-radius:10px; padding:15px; margin:8px 0; display:flex; justify-content:space-between; align-items:center; }
.scan-card .info { flex:1; }
.scan-card .info h4 { color:#f8fafc; font-size:14px; }
.scan-card .info p { color:#94a3b8; font-size:12px; margin-top:4px; }
</style>
</head>
<body>
<div class="header">
    <h1>🚀 <span>CVT</span> Launcher <small style="color:#94a3b8;font-size:13px">Civitatis → LCX</small></h1>
    <div class="badge" id="counter">{{ launches|length }} lançamentos</div>
</div>

<div class="controls">
    <button class="btn btn-primary" onclick="scanEmails()">📧 Escanear Emails (24h)</button>
    <button class="btn btn-success" onclick="launchAll()">🚀 Lançar Tudo</button>
    <button class="btn btn-warning" onclick="testParse()">🔍 Test Parse</button>
    <button class="btn btn-primary" onclick="testLogin()">🔑 Test Login LCX</button>
</div>

<div class="status-bar" id="statusBar">Pronto. Clique em "Escanear Emails" para buscar novas reservas.</div>

<div id="scanResults"></div>

<div class="table-wrap">
<table>
<thead>
<tr>
    <th>Data/Hora</th>
    <th>Reserva</th>
    <th>Atividade</th>
    <th>Cidade</th>
    <th>Cliente</th>
    <th>Pessoas</th>
    <th>Valor</th>
    <th>Código LCX</th>
    <th>Status</th>
    <th>Sale ID</th>
</tr>
</thead>
<tbody id="logBody">
{% for l in launches %}
<tr>
    <td>{{ l.timestamp[:16] }}</td>
    <td>{{ l.booking_number }}</td>
    <td>{{ l.atividade[:40] }}</td>
    <td>{{ l.cidade }}</td>
    <td>{{ l.get('cliente','') }}</td>
    <td>{{ l.get('num_pessoas','') }}</td>
    <td>R$ {{ l.get('preco_venda','') }}</td>
    <td>{{ l.get('codigo_lcx','—') }}</td>
    <td><span class="badge-{{ 'ok' if l.status=='OK' else 'erro' if l.status=='ERRO' else 'sem' }}">{{ l.status }}</span></td>
    <td>{{ l.get('sale_id','')[:12] }}</td>
</tr>
{% endfor %}
{% if not launches %}
<tr><td colspan="10" class="empty">Nenhum lançamento ainda. Escaneie os emails para começar.</td></tr>
{% endif %}
</tbody>
</table>
</div>

<div class="toast" id="toast"></div>

<script>
function showStatus(msg) {
    document.getElementById('statusBar').textContent = msg;
}

function showToast(msg, duration=3000) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.style.display = 'block';
    setTimeout(() => t.style.display = 'none', duration);
}

async function scanEmails() {
    showStatus('Escaneando emails da Civitatis...');
    try {
        const r = await fetch('/api/scan', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({hours:24})});
        const data = await r.json();
        showStatus(`Encontrados: ${data.total_found} emails | ${data.new_bookings} novas reservas | ${data.skipped} ignorados`);

        const container = document.getElementById('scanResults');
        container.innerHTML = '';
        if (data.bookings) {
            data.bookings.forEach(b => {
                container.innerHTML += `
                <div class="scan-card">
                    <div class="info">
                        <h4>#${b.booking_number} — ${b.atividade || 'N/A'}</h4>
                        <p>${b.cidade || ''} | ${b.data_tour || ''} | ${b.nome || ''} ${b.sobrenomes || ''} | R$ ${b.preco_venda || '0'}</p>
                    </div>
                    <button class="btn btn-success" onclick="launchOne(${JSON.stringify(b).replace(/"/g, '&quot;')})">Lançar</button>
                </div>`;
            });
        }
    } catch(e) { showStatus('Erro: ' + e.message); }
}

async function launchOne(data) {
    showStatus('Lançando venda no LCX...');
    try {
        const r = await fetch('/api/launch', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(data)});
        const res = await r.json();
        showStatus(res.success ? `✅ Venda lançada! ID: ${res.entry?.sale_id}` : `❌ Erro: ${res.error}`);
        showToast(res.success ? '✅ Lançado com sucesso!' : '❌ ' + res.error);
        location.reload();
    } catch(e) { showStatus('Erro: ' + e.message); }
}

async function launchAll() {
    if (!confirm('Lançar TODAS as novas reservas das últimas 24h?')) return;
    showStatus('Lançando todas as reservas...');
    try {
        const r = await fetch('/api/launch-all', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({hours:24})});
        const data = await r.json();
        showStatus(`Processadas: ${data.total} reservas`);
        showToast(`${data.total} reservas processadas`);
        location.reload();
    } catch(e) { showStatus('Erro: ' + e.message); }
}

async function testParse() {
    showStatus('Testando parse dos emails...');
    try {
        const r = await fetch('/api/test-parse', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({hours:48})});
        const data = await r.json();
        showStatus(`Parse test: ${data.length} emails processados`);
        console.log('Parse results:', data);
        showToast(`${data.length} emails parseados — veja o console (F12)`);
    } catch(e) { showStatus('Erro: ' + e.message); }
}

async function testLogin() {
    showStatus('Testando login no LCX...');
    try {
        const r = await fetch('/api/test-lcx-login');
        const data = await r.json();
        showStatus(data.logged_in ? '✅ Login LCX OK!' : '❌ Login LCX falhou');
        showToast(data.logged_in ? '✅ Login OK' : '❌ Login falhou');
    } catch(e) { showStatus('Erro: ' + e.message); }
}
</script>
</body>
</html>
"""

# ═══════════════════════════════════════════════════════
# AUTO-SCAN BACKGROUND WORKER
# ═══════════════════════════════════════════════════════

def _get_next_batch_time(now):
    """Calculate next batch time from BATCH_HOURS."""
    today_batches = [now.replace(hour=h, minute=0, second=0, microsecond=0) for h in BATCH_HOURS]
    # Find next future batch today
    for bt in today_batches:
        if bt > now:
            return bt
    # All today's batches passed — first batch tomorrow
    tomorrow = now + timedelta(days=1)
    return tomorrow.replace(hour=BATCH_HOURS[0], minute=0, second=0, microsecond=0)


def run_batch():
    """Execute one batch: scan emails and launch all new sales."""
    go_live = datetime.strptime(GO_LIVE_DATE, "%Y-%m-%d")

    auto_scan_status["running"] = True
    auto_scan_status["last_run"] = datetime.now().isoformat()

    # Lookback: hours since go-live, max 48h
    hours_since_live = min((datetime.now() - go_live).total_seconds() / 3600, 48)
    hours_since_live = max(hours_since_live, 1)

    print(f"[BATCH] Scanning emails from last {hours_since_live:.1f}h...")
    emails = fetch_new_booking_emails(max_results=50, since_hours=int(hours_since_live) + 1)

    new_bookings = [e for e in emails if e.get("type") == "NOVA_RESERVA"]
    launched = 0
    skipped = 0
    errors = 0

    for em in new_bookings:
        # Skip if already launched
        already = any(
            l.get("booking_number") == em.get("booking_number") and l.get("status") == "OK"
            for l in launch_log
        )
        if already:
            skipped += 1
            continue

        sale_payload, codigo_lcx = build_lcx_sale(em)

        if not codigo_lcx:
            entry = {
                "timestamp": datetime.now().isoformat(),
                "booking_number": em.get("booking_number", ""),
                "atividade": em.get("atividade", ""),
                "cidade": em.get("cidade", ""),
                "status": "SEM_CODIGO",
                "error": "Tour não mapeado",
            }
            launch_log.insert(0, entry)
            errors += 1
            continue

        result = lcx_client.create_sale(sale_payload)

        # Update status to CONFIRMED
        if result.get("success") and result.get("sale_id") and result["sale_id"] not in ("unknown", "pending"):
            lcx_client.update_sale_status(result["sale_id"], "CONFIRMED")

        entry = {
            "timestamp": datetime.now().isoformat(),
            "booking_number": em.get("booking_number", ""),
            "atividade": em.get("atividade", ""),
            "cidade": em.get("cidade", ""),
            "cliente": f"{em.get('nome', '')} {em.get('sobrenomes', '')}".strip(),
            "num_pessoas": em.get("num_total", 0),
            "preco_venda": em.get("preco_venda", ""),
            "codigo_lcx": codigo_lcx,
            "status": "OK" if result.get("success") else "ERRO",
            "sale_id": result.get("sale_id", ""),
            "error": result.get("error", ""),
            "source": "batch",
        }
        launch_log.insert(0, entry)

        if result.get("success"):
            launched += 1
        else:
            errors += 1

    summary = f"found={len(new_bookings)} launched={launched} skipped={skipped} errors={errors}"
    auto_scan_status["last_result"] = summary
    auto_scan_status["running"] = False
    return summary


def auto_scan_worker():
    """Background thread: run batches at scheduled hours (7h, 13h, 20h)."""
    go_live = datetime.strptime(GO_LIVE_DATE, "%Y-%m-%d")

    # Wait until go-live date
    while datetime.now() < go_live:
        wait_secs = (go_live - datetime.now()).total_seconds()
        print(f"[BATCH] Waiting for go-live date {GO_LIVE_DATE}. {wait_secs/3600:.1f}h remaining.")
        time.sleep(min(wait_secs + 60, 3600))

    print(f"[BATCH] GO LIVE! Batch hours: {BATCH_HOURS}")

    while True:
        now = datetime.now()
        next_batch = _get_next_batch_time(now)
        auto_scan_status["next_batch"] = next_batch.strftime("%Y-%m-%d %H:%M")
        wait_secs = (next_batch - now).total_seconds()

        print(f"[BATCH] Next batch at {next_batch.strftime('%H:%M')} ({wait_secs/60:.0f} min from now)")
        time.sleep(max(wait_secs, 60))  # sleep until next batch

        try:
            print(f"[BATCH] === Running batch {datetime.now().strftime('%H:%M')} ===")
            summary = run_batch()
            print(f"[BATCH] Done: {summary}")
        except Exception as e:
            auto_scan_status["running"] = False
            auto_scan_status["last_result"] = f"ERROR: {e}"
            print(f"[BATCH ERROR] {e}")
            traceback.print_exc()
            time.sleep(300)  # wait 5 min before retrying on error


@app.route("/api/auto-scan-status")
def api_auto_scan_status():
    """Check auto-scan status."""
    return jsonify(auto_scan_status)


# ═══════════════════════════════════════════════════════
# DAILY SUMMARY EMAIL
# ═══════════════════════════════════════════════════════

def build_daily_summary(target_date):
    """Build HTML summary of launches for a specific date."""
    date_str = target_date.strftime("%Y-%m-%d")
    day_launches = [l for l in launch_log if l.get("timestamp", "").startswith(date_str)]

    ok = [l for l in day_launches if l.get("status") == "OK"]
    errors = [l for l in day_launches if l.get("status") == "ERRO"]
    sem_codigo = [l for l in day_launches if l.get("status") == "SEM_CODIGO"]

    total_valor = 0
    for l in ok:
        try:
            v = str(l.get("preco_venda", "0")).replace(".", "").replace(",", ".")
            total_valor += float(v)
        except:
            pass

    total_pessoas = sum(l.get("num_pessoas", 0) for l in ok)

    # Build HTML
    html = f"""
    <div style="font-family:'Segoe UI',sans-serif;max-width:700px;margin:0 auto;background:#0f172a;color:#e2e8f0;border-radius:12px;overflow:hidden;">
        <div style="background:linear-gradient(135deg,#1e293b,#334155);padding:20px 30px;border-bottom:2px solid #f43f5e;">
            <h1 style="margin:0;font-size:20px;color:#f8fafc;">🚀 CVT Launcher — Resumo do dia</h1>
            <p style="margin:5px 0 0;color:#94a3b8;font-size:14px;">{target_date.strftime('%d/%m/%Y')} ({target_date.strftime('%A')})</p>
        </div>

        <div style="padding:20px 30px;">
            <div style="display:flex;gap:15px;margin-bottom:20px;">
                <div style="background:#1e293b;border-radius:10px;padding:15px 20px;flex:1;text-align:center;">
                    <div style="font-size:28px;font-weight:bold;color:#10b981;">{len(ok)}</div>
                    <div style="font-size:12px;color:#94a3b8;">Lançadas</div>
                </div>
                <div style="background:#1e293b;border-radius:10px;padding:15px 20px;flex:1;text-align:center;">
                    <div style="font-size:28px;font-weight:bold;color:#3b82f6;">{total_pessoas}</div>
                    <div style="font-size:12px;color:#94a3b8;">Pessoas</div>
                </div>
                <div style="background:#1e293b;border-radius:10px;padding:15px 20px;flex:1;text-align:center;">
                    <div style="font-size:28px;font-weight:bold;color:#f59e0b;">R$ {total_valor:,.2f}</div>
                    <div style="font-size:12px;color:#94a3b8;">Valor líquido</div>
                </div>
            </div>
    """

    if errors:
        html += f'<p style="color:#ef4444;font-size:13px;">⚠️ {len(errors)} venda(s) com erro</p>'
    if sem_codigo:
        html += f'<p style="color:#f59e0b;font-size:13px;">⚠️ {len(sem_codigo)} tour(s) sem código LCX mapeado</p>'

    if ok:
        html += """
            <table style="width:100%;border-collapse:collapse;margin-top:15px;">
                <tr style="background:#1e293b;">
                    <th style="padding:10px;text-align:left;font-size:11px;color:#94a3b8;text-transform:uppercase;">Reserva</th>
                    <th style="padding:10px;text-align:left;font-size:11px;color:#94a3b8;text-transform:uppercase;">Atividade</th>
                    <th style="padding:10px;text-align:left;font-size:11px;color:#94a3b8;text-transform:uppercase;">Cliente</th>
                    <th style="padding:10px;text-align:left;font-size:11px;color:#94a3b8;text-transform:uppercase;">Pessoas</th>
                    <th style="padding:10px;text-align:left;font-size:11px;color:#94a3b8;text-transform:uppercase;">Código</th>
                </tr>
        """
        for l in ok:
            html += f"""
                <tr style="border-bottom:1px solid #1e293b;">
                    <td style="padding:8px 10px;font-size:13px;">#{l.get('booking_number','')}</td>
                    <td style="padding:8px 10px;font-size:13px;">{l.get('atividade','')[:40]}</td>
                    <td style="padding:8px 10px;font-size:13px;">{l.get('cliente','')[:25]}</td>
                    <td style="padding:8px 10px;font-size:13px;">{l.get('num_pessoas',0)}</td>
                    <td style="padding:8px 10px;font-size:13px;">{l.get('codigo_lcx','')}</td>
                </tr>
            """
        html += "</table>"

    if not day_launches:
        html += '<p style="text-align:center;color:#64748b;padding:30px;">Nenhuma reserva processada neste dia.</p>'

    html += """
        </div>
        <div style="padding:15px 30px;background:#1e293b;text-align:center;font-size:11px;color:#64748b;">
            CVT Launcher — Automação Civitatis → LCX | LC Turismo
        </div>
    </div>
    """
    return html, len(ok), len(errors), len(sem_codigo)


def send_daily_summary(target_date):
    """Send daily summary email via SMTP."""
    if not GMAIL_EMAIL or not GMAIL_APP_PASSWORD or not SUMMARY_EMAIL_TO:
        print("[SUMMARY] Missing email credentials, skipping.")
        return

    html, ok_count, err_count, sem_count = build_daily_summary(target_date)
    date_label = target_date.strftime('%d/%m/%Y')

    subject = f"🚀 CVT Launcher — {ok_count} venda(s) lançada(s) em {date_label}"
    if ok_count == 0:
        subject = f"CVT Launcher — Nenhuma venda em {date_label}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"CVT Launcher <{GMAIL_EMAIL}>"
    msg["To"] = SUMMARY_EMAIL_TO
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_EMAIL, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)
        print(f"[SUMMARY] Email sent to {SUMMARY_EMAIL_TO}: {subject}")
    except Exception as e:
        print(f"[SUMMARY ERROR] Failed to send: {e}")
        traceback.print_exc()


def daily_summary_worker():
    """Background thread: send daily summary at SUMMARY_HOUR."""
    go_live = datetime.strptime(GO_LIVE_DATE, "%Y-%m-%d")
    last_sent_date = None

    while True:
        now = datetime.now()

        # Only start after go-live + 1 day (need at least 1 day of data)
        if now < go_live + timedelta(days=1):
            time.sleep(3600)
            continue

        # Send at SUMMARY_HOUR if not already sent today
        if now.hour >= SUMMARY_HOUR and last_sent_date != now.date():
            yesterday = (now - timedelta(days=1)).date()
            target = datetime(yesterday.year, yesterday.month, yesterday.day)
            print(f"[SUMMARY] Sending summary for {yesterday}...")
            try:
                send_daily_summary(target)
                last_sent_date = now.date()
            except Exception as e:
                print(f"[SUMMARY ERROR] {e}")
                traceback.print_exc()

        time.sleep(600)  # check every 10 min


@app.route("/api/send-summary")
def api_send_summary():
    """Manually trigger a summary email for yesterday."""
    yesterday = datetime.now() - timedelta(days=1)
    target = datetime(yesterday.year, yesterday.month, yesterday.day)
    send_daily_summary(target)
    return jsonify({"sent": True, "date": target.strftime("%Y-%m-%d"), "to": SUMMARY_EMAIL_TO})


# Start background threads on app boot
_scan_thread = threading.Thread(target=auto_scan_worker, daemon=True)
_scan_thread.start()
print(f"[AUTO-SCAN] Thread started. Go-live: {GO_LIVE_DATE}, interval: {AUTO_SCAN_INTERVAL}s")

_summary_thread = threading.Thread(target=daily_summary_worker, daemon=True)
_summary_thread.start()
print(f"[SUMMARY] Thread started. Sends at {SUMMARY_HOUR}:00 to {SUMMARY_EMAIL_TO}")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
