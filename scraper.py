#!/usr/bin/env python3

import json
import os
import re
import time

import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from geopy.geocoders import ArcGIS


# ---------------------------------------------------------------------------
# CONFIGURACIONES
# ---------------------------------------------------------------------------

HEPTAGRAMA_URL = "https://heptagrama.com/agenda-cultural-lima.htm"
FIREBASE_SECRET_ENV = "FIREBASE_SERVICE_ACCOUNT"  # GitHub Secret con el JSON de la llave

# ---------------------------------------------------------------------------
# FIREBASE
# ---------------------------------------------------------------------------

def init_firestore_client():
    """Inicia Firebase usando el secreto (o localmente un archivo json si existe)."""

    key_json = os.getenv(FIREBASE_SECRET_ENV)
    if key_json:
        try:
            key_data = json.loads(key_json)
            cred = credentials.Certificate(key_data)
        except Exception as e:
            raise SystemExit(f"❌ No se pudo cargar FIREBASE_SERVICE_ACCOUNT: {e}")
    else:
        key_path = "serviceAccountKey.json"
        if not os.path.exists(key_path):
            raise SystemExit(
                "❌ No se encontró ninguna credencial de Firebase. "
                "Coloca 'serviceAccountKey.json' aquí o configura el secret FIREBASE_SERVICE_ACCOUNT."
            )
        cred = credentials.Certificate(key_path)

    firebase_admin.initialize_app(cred)
    return firestore.client()


# ---------------------------------------------------------------------------
# GEOCODING (ArcGIS)
# ---------------------------------------------------------------------------

geolocator = ArcGIS(timeout=10)


def obtener_coordenadas(direccion):
    """Obtiene lat/lon usando ArcGIS (no bloquea tanto como Nominatim)."""
    try:
        location = geolocator.geocode(direccion)
        if location:
            return location.latitude, location.longitude
    except Exception as e:
        print(f"   ⚠️ Error de mapa: {e}")
    return None


# ---------------------------------------------------------------------------
# SCRAPER
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    s = str(text or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-") or "evento"


def guess_tipo(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ["teatro", "obra", "función", "funcion"]):
        return "Teatro"
    if any(k in t for k in ["música", "concierto", "dj", "band", "orquesta"]):
        return "Música"
    if any(k in t for k in ["expo", "exposición", "muestra", "arte", "foto"]):
        return "Arte/Expo"
    if any(k in t for k in ["cine", "películ", "film"]):
        return "Cine"
    return "Otro"


def extract_hora(text: str) -> str:
    if not text:
        return ""
    # Busca patrones como 10:00, 9:30pm, 2:00pm, 21:00 etc.
    m = re.search(r"(\d{1,2}:\d{2}\s*(?:am|pm)?)", text, flags=re.I)
    return m.group(1).strip() if m else ""


def parse_agenda(html: str):
    """Extrae eventos desde la agenda de Heptagrama."""
    soup = BeautifulSoup(html, "html.parser")
    eventos = []

    for detalles in soup.find_all("details"):
        summary = detalles.find("summary")
        if not summary:
            continue
        dia = summary.get_text(strip=True)

        # Cada evento suele estar formado por 2 <p>: lugar+dirección, y descripción.
        ps = detalles.find_all("p")
        pair = []
        for p in ps:
            # Vacía o espacios, se ignoran.
            txt = p.get_text(" ", strip=True)
            if not txt:
                continue
            pair.append(txt)
            if len(pair) == 2:
                lugar_raw, desc_raw = pair
                # Extrae lugar y dirección si está en paréntesis
                lugar = lugar_raw
                direccion = ""
                m = re.search(r"^(.*)\s*\((.*)\)$", lugar_raw)
                if m:
                    lugar = m.group(1).strip()
                    direccion = m.group(2).strip()

                evento = {
                    "dia": dia,
                    "lugar": lugar,
                    "direccion": direccion,
                    "nombre": desc_raw,
                    "descripcion": desc_raw,
                    "hora": extract_hora(desc_raw),
                    "tipo": guess_tipo(desc_raw),
                }
                eventos.append(evento)
                pair = []

        # Si quedó un párrafo suelto, lo agregamos también (aunque sea incompleto)
        if pair:
            eventos.append({
                "dia": dia,
                "lugar": pair[0],
                "direccion": "",
                "nombre": "",
                "descripcion": pair[0],
                "hora": "",
                "tipo": "Otro",
            })

    return eventos


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("🔎 Descargando agenda desde Heptagrama...")
    html = requests.get(HEPTAGRAMA_URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    }, timeout=30).text

    eventos = parse_agenda(html)
    if not eventos:
        print("⚠️ No se encontraron eventos. Revisa que la página de Heptagrama sea accesible.")
        return

    db = init_firestore_client()

    print(f"✅ Encontrados {len(eventos)} eventos. Subiendo a Firestore...")
    for evento in eventos:
        # Normalizamos para que el ID sea estable y no genere duplicados.
        doc_id = slugify(f"{evento['dia']} {evento['lugar']} {evento['nombre']}")

        # Geocodificamos siempre que tengamos algo de dirección
        if evento.get("direccion"):
            direccion_full = f"{evento['direccion']}, {evento['lugar']}, Lima, Perú"
            coords = obtener_coordenadas(direccion_full)
            if coords:
                evento["lat"], evento["lon"] = coords
            else:
                print(f"   ⚠️ No pude geocodificar: {direccion_full}")

        db.collection("eventos").document(doc_id).set(evento)
        print(f"   ☁️  Guardado: {evento['dia']} - {evento['lugar']}")
        time.sleep(1)

    # Guardamos una marca de tiempo para que el frontend pueda mostrar "Última actualización".
    db.collection("meta").document("lastUpdate").set({
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "source": "Heptagrama"
    })

    print("\n✅ Actualización completa.")


if __name__ == "__main__":
    main()
