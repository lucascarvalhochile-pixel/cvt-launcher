"""
Bókun → LCX Integration Module for CVT Launcher

Pollers Bókun bookings via API REST (HMAC-signed) and creates sales in LCX
using the same dedup/login logic as Civitatis flow.

Configuration via env vars:
- BOKUN_ACCESS_KEY
- BOKUN_SECRET_KEY
- BOKUN_VENDOR_ID (141976)

Usage in server.py:
    from bokun_integration import BokunClient, BOKUN_TO_LCX, build_lcx_sale_from_bokun
    
    bokun = BokunClient(os.environ["BOKUN_ACCESS_KEY"], os.environ["BOKUN_SECRET_KEY"])
    for booking in bokun.list_recent_bookings(since_minutes=10):
        if lcx_client.booking_exists(f"BKN{booking['confirmationCode']}"):
            continue
        sale = build_lcx_sale_from_bokun(booking, lcx_client)
        lcx_client.create_sale(sale)
"""
import hmac
import hashlib
import base64
import json
import requests
from datetime import datetime, timezone, timedelta

BOKUN_API_BASE = "https://api.bokun.io"

# Mapping Bokun activity_id -> LCX tour code
# Updated 2026-06-22 after Phase 1+2+3 (40 tours)
BOKUN_TO_LCX = {
    # Phase 1 - Originally imported
    "1237764": "CHISAN035",   # Andes Panoramico
    "1237777": "CHISAN106",   # Vina del Mar + Valparaiso
    "1237753": "PERCUS002",   # City Tour Cusco
    "1237754": "PERCUS001",   # Machu Picchu Classic (no Karina sheet code)
    "1237756": "PERCUS003",   # Maras Moray Salineras (no Karina sheet code)
    "1237757": "PERPUN001",   # Lago Titicaca (no Karina sheet code)
    "1237758": "PERCUS005",   # Vinicunca Montanha 7 Cores
    "1237759": "PERCUS020",   # Vale Sagrado dos Incas
    "1237762": "PERLIM024",   # Ballestas + Huacachina
    "1237763": "PERLIM025",   # City Tour Lima (no Karina sheet code)
    # Phase 1 - New
    "1237814": "CHISAN067",   # Portillo + Laguna del Inca
    "1237815": "CHISAN107",   # Vinicola Alyan Family
    "1237817": "CHISAN034",   # Parque Farellones
    "1237818": "CHISAN061",   # Isla Negra + Algarrobo + Undurraga
    "1237819": "CHISAN071",   # Safari Rancagua
    "1237820": "CHISAN1682",  # Andes Full Day Valle Nevado
    "1237824": "COLSAO079",   # Johnny Cay
    "1237825": "CHISAN109",   # Concha y Toro Centro del Vino
    "1237827": "CHISAN116",   # Vinicola Undurraga
    "1237828": "CHIATA016",   # Tour Astronomico Atacama
    # Phase 2
    "1237832": "CHISAN110",   # Concha y Toro Marques
    "1237833": "CHISAN111",   # Casillero del Diablo Noite
    "1237835": "PERCUS010",   # Quadriciclo Montanha 7 Cores (no Karina code, custom)
    "1237836": "CHISAN112",   # Sunset Infinitum Valle Nevado (no Karina code, custom)
    "1237837": "CHISAN040",   # Cajon del Maipo + Yeso + Termas
    "1237839": "CHIATA010",   # Lagunas Baltinache
    "1237840": "CHIATA014",   # Termas de Puritama
    "1237841": "ARGMEN001",   # Bodega Chandon Mendoza (no Karina code, custom)
    "1237842": "MEXCAN001",   # Isla Mujeres Catamara (no Karina code, custom)
    "1237843": "DOMSDQ001",   # Isla Saona Premium (no Karina code, custom)
    # Phase 3
    "1237844": "ARGBUE001",   # Buenos Aires City Tour (no Karina code, custom)
    "1237845": "ARGBUE002",   # Tigre + Delta Classico (no Karina code, custom)
    "1237846": "ARGBUE003",   # Delta del Tigre Premium (no Karina code, custom)
    "1237847": "ARGUSH001",   # Ushuaia City Tour (no Karina code, custom)
    "1237848": "ARGUSH002",   # Canal Beagle (no Karina code, custom)
    "1237849": "MEXCAN002",   # Chichen Itza
    "1237850": "MEXCAN003",   # Isla Contoy
    "1237852": "DOMSDQ002",   # Isla Saona VIP
    "1237853": "COLCAR025",   # Rosario Islands Cartagena
    "1237854": "ARGBAR001",   # Cerro Tronador Bariloche
}


class BokunClient:
    """Thin Bokun API client with HMAC-SHA1 signing."""

    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key.encode()

    def _sign(self, method, path):
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        string_to_sign = f"{date_str}{self.access_key}{method}{path}"
        signature = base64.b64encode(
            hmac.new(self.secret_key, string_to_sign.encode(), hashlib.sha1).digest()
        ).decode()
        return {
            "X-Bokun-Date": date_str,
            "X-Bokun-AccessKey": self.access_key,
            "X-Bokun-Signature": signature,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request(self, method, path, body=None):
        headers = self._sign(method, path)
        url = f"{BOKUN_API_BASE}{path}"
        if method == "POST":
            data = json.dumps(body) if body else "{}"
            r = requests.post(url, headers=headers, data=data, timeout=20)
        else:
            r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json()

    def list_recent_bookings(self, since_minutes=10):
        """List bookings created in last N minutes. Returns list of booking dicts."""
        body = {
            "creationDate": {
                "from": int((datetime.now(timezone.utc) - timedelta(minutes=since_minutes)).timestamp() * 1000),
                "to": int(datetime.now(timezone.utc).timestamp() * 1000),
            },
            "size": 100,
        }
        try:
            data = self._request("POST", "/booking.json/booking-search", body)
            return data.get("items", [])
        except Exception as e:
            print(f"[BOKUN] booking-search failed: {e}")
            return []

    def get_booking(self, booking_id):
        """Fetch full booking detail by ID."""
        return self._request("GET", f"/booking.json/{booking_id}")


def build_lcx_sale_from_bokun(booking, lcx_client):
    """
    Convert a Bokun booking dict into LCX createSale payload.
    Uses the same fields/structure as Civitatis flow.
    """
    # Extract booking essentials
    confirmation_code = booking.get("confirmationCode", "")
    customer = booking.get("customer", {}) or {}
    customer_name = f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip() or "Cliente Bokun"
    customer_email = customer.get("email", "")
    customer_phone = customer.get("phoneNumber", "") or customer.get("mobile", "")

    items_lcx = []
    total_pax = 0
    first_tour_date = None
    first_country = "Chile"
    first_city = "Santiago"
    meeting_point = ""
    participants = []
    total_net_amount = 0.0

    for pb in booking.get("productBookings", []):
        activity = pb.get("activity", {}) or {}
        bokun_activity_id = str(activity.get("id", ""))
        lcx_code = BOKUN_TO_LCX.get(bokun_activity_id, "")
        if not lcx_code:
            print(f"[BOKUN] no LCX mapping for activity {bokun_activity_id} ({activity.get('title')})")
            continue

        tour_info = lcx_client.get_tour_by_code(lcx_code)
        if not tour_info:
            print(f"[BOKUN] LCX tour {lcx_code} not found")
            continue

        tour_date = pb.get("startDate") or pb.get("startDateTime", "")[:10]
        if not first_tour_date:
            first_tour_date = tour_date

        passengers = pb.get("passengers", []) or []
        n_pax = len(passengers) or pb.get("totalParticipants", 1)
        total_pax += n_pax

        net_price = (
            pb.get("vendorPayoutAmount")
            or pb.get("netPrice")
            or pb.get("totalPrice", 0)
        )
        total_net_amount += float(net_price or 0)

        if not meeting_point:
            meeting_point = pb.get("meetingPoint", "") or pb.get("pickupPlace", "")

        items_lcx.append({
            "country": tour_info.get("country", first_country),
            "city": tour_info.get("city", first_city),
            "tourName": tour_info.get("name"),
            "tourId": tour_info.get("id"),
            "priceTier": "ADULT",
            "numberOfPeople": n_pax,
            "tourDate": tour_date,
            "price": float(net_price or 0),
            "isGift": False,
        })

        for p in passengers:
            participants.append({
                "name": f"{p.get('firstName', '')} {p.get('lastName', '')}".strip() or customer_name,
                "cpfPassport": p.get("passportId", "") or p.get("nationalId", ""),
                "whatsapp": p.get("phoneNumber", "") or customer_phone,
                "dietaryRestrictionLabel": [],
            })

    if not items_lcx:
        return None

    channel = booking.get("channel", {}).get("title", "BOKUN")

    return {
        "customer": {
            "name": f"{customer_name} *bkn* #{confirmation_code}",
            "email": customer_email,
            "cpfPassport": customer.get("passportId", "") or customer.get("nationalId", ""),
            "whatsapp": customer_phone,
        },
        "tripCountry": items_lcx[0]["country"],
        "tripCity": items_lcx[0]["city"],
        "meetingPoint": meeting_point,
        "tripStartDate": first_tour_date,
        "tripEndDate": first_tour_date,
        "numberOfPeople": total_pax,
        "status": "CONFIRMED",
        "items": items_lcx,
        "payments": [{"method": "CASH", "amount": total_net_amount, "status": "paid"}],
        "participants": participants or [{
            "name": customer_name,
            "cpfPassport": customer.get("passportId", ""),
            "whatsapp": customer_phone,
            "dietaryRestrictionLabel": [],
        }],
        "notes": (
            f"Reserva Bokun #{confirmation_code} | Canal: {channel} | "
            f"Total liquido: R$ {total_net_amount:.2f} | "
            f"Pax: {total_pax}"
        ),
    }


def poll_and_launch_bokun(lcx_client, launched_set, bokun=None, since_minutes=10):
    """Main entry called from auto-scan thread."""
    if bokun is None:
        import os
        bokun = BokunClient(
            os.environ["BOKUN_ACCESS_KEY"],
            os.environ["BOKUN_SECRET_KEY"],
        )

    bookings = bokun.list_recent_bookings(since_minutes=since_minutes)
    found = len(bookings)
    launched = skipped = errors = 0

    for b in bookings:
        cc = b.get("confirmationCode", "")
        booking_key = f"BKN{cc}"
        if booking_key in launched_set:
            skipped += 1
            continue
        try:
            if lcx_client.booking_exists(booking_key):
                skipped += 1
                launched_set.add(booking_key)
                continue
        except Exception:
            skipped += 1
            continue

        try:
            sale = build_lcx_sale_from_bokun(b, lcx_client)
            if not sale:
                skipped += 1
                continue
            result = lcx_client.create_sale(sale)
            if result and result.get("ok"):
                launched += 1
                launched_set.add(booking_key)
            else:
                errors += 1
        except Exception as e:
            print(f"[BOKUN] error processing #{cc}: {e}")
            errors += 1

    return found, launched, skipped, errors
