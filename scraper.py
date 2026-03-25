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
FIREBASE_SECRET_ENV = "FIREBASE_SERVICE_ACCOUNT"

# ---------------------------------------------------------------------------
# FIREBASE
# ---------------------------------------------------------------------------

def init_firestore_client():
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
# GEOCODING — con validación de bounding box Lima y fallbacks progresivos
# ---------------------------------------------------------------------------

geolocator = ArcGIS(timeout=10)

# Bounding box de Lima Metropolitana (holgada para incluir todos los distritos)
LIMA_LAT_MIN, LIMA_LAT_MAX = -12.30, -11.70
LIMA_LON_MIN, LIMA_LON_MAX = -77.20, -76.70


def coords_en_lima(lat: float, lon: float) -> bool:
    """Devuelve True solo si las coordenadas están dentro de Lima Metropolitana."""
    return LIMA_LAT_MIN <= lat <= LIMA_LAT_MAX and LIMA_LON_MIN <= lon <= LIMA_LON_MAX


def intentar_geocode(query: str):
    """Geocodifica una query y retorna (lat, lon) solo si el punto cae en Lima."""
    try:
        loc = geolocator.geocode(query)
        if loc:
            if coords_en_lima(loc.latitude, loc.longitude):
                return loc.latitude, loc.longitude
            else:
                print(f"   ⚠️  Fuera de Lima '{query}' → ({loc.latitude:.3f}, {loc.longitude:.3f}) descartado")
    except Exception as e:
        print(f"   ⚠️  Error geocode: {e}")
    return None


def simplificar_lugar(lugar: str) -> str:
    """
    Extrae la parte más reconocible de un nombre de recinto largo.

    Ejemplos:
      "Anfiteatro Nicomedes Santa Cruz del Parque de la Exposición"
        → "Parque de la Exposición"
      "Auditorio Julio Ramón Ribeyro del Centro Cultural Ricardo Palma"
        → "Centro Cultural Ricardo Palma"
    """
    m = re.search(r"\b(?:del|de la|de los|de las)\s+(.+)$", lugar, flags=re.I)
    if m:
        candidato = m.group(1).strip()
        if len(candidato.split()) >= 2:
            return candidato
    return lugar


def obtener_coordenadas(lugar: str, direccion: str):
    """
    Intenta geocodificar usando múltiples estrategias en orden de especificidad.
    Descarta cualquier resultado que no caiga dentro de Lima Metropolitana.

    Orden de intentos:
      1. dirección + lugar + "Lima Peru"   (máximo contexto)
      2. solo dirección + "Lima Peru"
      3. solo lugar + "Lima Peru"
      4. lugar simplificado + "Lima Peru"  (quita prefijos de tipo de recinto)
    """
    dir_norm   = (direccion or "").replace(" - ", ", ").strip()
    lugar_norm = (lugar or "").strip()

    intentos = []
    if dir_norm and lugar_norm:
        intentos.append(f"{dir_norm}, {lugar_norm}, Lima, Peru")
    if dir_norm:
        intentos.append(f"{dir_norm}, Lima, Peru")
    if lugar_norm:
        intentos.append(f"{lugar_norm}, Lima, Peru")
    simplif = simplificar_lugar(lugar_norm)
    if simplif and simplif != lugar_norm:
        intentos.append(f"{simplif}, Lima, Peru")

    for query in intentos:
        coords = intentar_geocode(query)
        if coords:
            print(f"   📍 OK con: '{query}'")
            return coords
        time.sleep(0.3)

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
    if any(k in t for k in ["música", "musica", "concierto", "dj", "band", "orquesta", "jazz"]):
        return "Música"
    if any(k in t for k in ["expo", "exposición", "exposicion", "muestra", "arte", "foto"]):
        return "Arte/Expo"
    if any(k in t for k in ["cine", "películ", "film"]):
        return "Cine"
    return "Otro"


def extract_hora(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"(\d{1,2}:\d{2}\s*(?:am|pm)?)", text, flags=re.I)
    return m.group(1).strip() if m else ""


def parse_agenda(html: str):
    """
    Extrae eventos desde la agenda de Heptagrama.

    Estructura HTML:
      <details>
        <summary>Lunes 23</summary>
        <p>Nombre del Lugar (dirección - distrito)</p>
        <p>Descripción completa del evento...</p>
        ...
      </details>
    """
    soup = BeautifulSoup(html, "html.parser")
    eventos = []

    for detalles in soup.find_all("details"):
        summary = detalles.find("summary")
        if not summary:
            continue
        dia = summary.get_text(strip=True)

        ps = detalles.find_all("p")
        pair = []
        for p in ps:
            txt = p.get_text(" ", strip=True)
            if not txt:
                continue
            pair.append(txt)
            if len(pair) == 2:
                lugar_raw, desc_raw = pair
                lugar = lugar_raw
                direccion = ""
                m = re.search(r"^(.*?)\s*\(([^)]+)\)\s*$", lugar_raw)
                if m:
                    lugar = m.group(1).strip()
                    direccion = m.group(2).strip()

                evento = {
                    "dia": dia,
                    "lugar": lugar,
                    "direccion": direccion,
                    "descripcion": desc_raw,
                    "nombre": desc_raw,
                    "hora": extract_hora(desc_raw),
                    "tipo": guess_tipo(desc_raw),
                }
                eventos.append(evento)
                pair = []

        if pair:
            eventos.append({
                "dia": dia,
                "lugar": pair[0],
                "direccion": "",
                "descripcion": "",
                "nombre": pair[0],
                "hora": "",
                "tipo": "Otro",
            })

    return eventos


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("🔎 Descargando agenda desde Heptagrama...")
    res = requests.get(HEPTAGRAMA_URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120 Safari/537.36",
    }, timeout=30)
    res.encoding = 'utf-8'
    html = res.text

    eventos = parse_agenda(html)
    if not eventos:
        print("⚠️ No se encontraron eventos.")
        return

    db = init_firestore_client()

    print("🧹 Limpiando eventos anteriores...")
    batch = db.batch()
    docs = db.collection("eventos").stream()
    count_deleted = 0
    for d in docs:
        batch.delete(d.reference)
        count_deleted += 1
        if count_deleted % 500 == 0:
            batch.commit()
            batch = db.batch()
    if count_deleted % 500 != 0:
        batch.commit()
    print(f"   ✅ {count_deleted} eventos eliminados.")

    print(f"✅ Encontrados {len(eventos)} eventos. Subiendo a Firestore...")
    for evento in eventos:
        doc_id = slugify(
            f"{evento.get('dia','')} {evento.get('lugar','')} "
            f"{evento.get('hora','')} {evento.get('nombre','')}"
        )

        if evento.get("direccion") or evento.get("lugar"):
            coords = obtener_coordenadas(evento.get("lugar", ""), evento.get("direccion", ""))
            if coords:
                evento["lat"], evento["lon"] = coords
            else:
                print(f"   ❌ Sin coordenadas válidas: {evento.get('lugar')} / {evento.get('direccion')}")

        db.collection("eventos").document(doc_id).set(evento)
        print(f"   ☁️  Guardado: {evento['dia']} - {evento['lugar']}")
        time.sleep(1)

    db.collection("meta").document("lastUpdate").set({
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "source": "Heptagrama"
    })
    print("\n✅ Actualización completa.")


if __name__ == "__main__":
    main()