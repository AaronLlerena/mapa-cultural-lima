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


def normalize_address_for_geocode(direccion: str, lugar: str) -> str:
    """Construye una dirección más amigable para geocoders."""
    if not direccion:
        return f"{lugar}, Lima, Peru"
    direccion_lim = direccion.replace(" - ", ", ")
    return f"{direccion_lim}, {lugar}, Lima, Peru"


def obtener_coordenadas(direccion):
    """Obtiene lat/lon usando ArcGIS."""
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
    # Busca "a las 8:00pm", "8:00pm", "21:00", etc.
    m = re.search(r"(\d{1,2}:\d{2}\s*(?:am|pm)?)", text, flags=re.I)
    return m.group(1).strip() if m else ""


def parse_agenda(html: str):
    """Extrae eventos desde la agenda de Heptagrama.

    Estructura HTML de Heptagrama:
      <details>
        <summary>Lunes 23</summary>
        <p>Nombre del Lugar (dirección - distrito)</p>   ← párrafo 1: lugar
        <p>Descripción completa del evento...</p>         ← párrafo 2: descripción
        <p>Otro lugar (...)</p>
        <p>Otra descripción...</p>
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

                # Separa lugar y dirección: "Teatro de Lucía (calle Bellavista 512 - Miraflores)"
                lugar = lugar_raw
                direccion = ""
                m = re.search(r"^(.*?)\s*\(([^)]+)\)\s*$", lugar_raw)
                if m:
                    lugar = m.group(1).strip()
                    direccion = m.group(2).strip()

                # ── CAMBIO CLAVE ──────────────────────────────────────────
                # 'descripcion' = texto completo del evento (para mostrar en la tarjeta)
                # 'nombre'      = texto completo también (se usa como fallback y para el doc_id)
                # ─────────────────────────────────────────────────────────
                evento = {
                    "dia": dia,
                    "lugar": lugar,
                    "direccion": direccion,
                    "descripcion": desc_raw,          # ← NUEVO: texto completo para el frontend
                    "nombre": desc_raw,               # ← se mantiene para compatibilidad / doc_id
                    "hora": extract_hora(desc_raw),
                    "tipo": guess_tipo(desc_raw),
                }
                eventos.append(evento)
                pair = []

        # Párrafo suelto (sin par de descripción)
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    }, timeout=30)

    res.encoding = 'utf-8'
    html = res.text

    eventos = parse_agenda(html)
    if not eventos:
        print("⚠️ No se encontraron eventos. Revisa que la página de Heptagrama sea accesible.")
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
        doc_id = slugify(f"{evento.get('dia','')} {evento.get('lugar','')} {evento.get('hora','')} {evento.get('nombre','')}")

        if evento.get("direccion") or evento.get("lugar"):
            direccion_full = normalize_address_for_geocode(evento.get("direccion", ""), evento.get("lugar", ""))
            coords = obtener_coordenadas(direccion_full)
            if not coords:
                coords = obtener_coordenadas(f"{evento.get('lugar','')}, Lima, Peru")
            if not coords and evento.get("direccion"):
                coords = obtener_coordenadas(f"{evento.get('direccion')}, Lima, Peru")

            if coords:
                evento["lat"], evento["lon"] = coords
            else:
                print(f"   ⚠️ No pude geocodificar: {direccion_full}")

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