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
    """Construye una dirección más amigable para geocoders.

    Heptagrama a veces usa el formato: "Calle X - Distrito" dentro de la dirección.
    Esto ayuda a convertirlo en una forma más estándar: "Calle X, Distrito, Lima, Peru".
    """
    if not direccion:
        return f"{lugar}, Lima, Peru"

    # Reemplaza guión usado como separador de distrito (" - ") por coma.
    # Ej: "Paseo Sáenz Peña s/n - Barranco" → "Paseo Sáenz Peña s/n, Barranco".
    direccion_lim = direccion.replace(" - ", ", ")

    # Si la dirección tiene "s/n" y no tiene coma, lo dejamos.
    return f"{direccion_lim}, {lugar}, Lima, Peru"


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


def summarize_text(text: str, max_len: int = 120) -> str:
    """Devuelve un título corto (resumen) y deja el resto en la descripción.

    Estrategia:
    1) Si hay comillas angulares («»), toma el contenido dentro de ellas.
    2) Si hay un separador ":" temprano, toma lo anterior.
    3) Si hay un punto (final de frase), toma la primera frase.
    4) Si no, corta en `max_len` de forma limpia.
    """
    if not text:
        return ""

    s = text.strip()

    # 1) Contenido dentro de «»
    m = re.search(r"«([^»]{10,200})»", s)
    if m:
        title = m.group(1).strip()
        if len(title) <= max_len:
            return title

    # 2) Antes de ':' (ej: "Evento: descripción...")
    if ":" in s:
        candidate = s.split(":", 1)[0].strip()
        if 15 < len(candidate) <= max_len:
            return candidate

    # 3) Primera frase (termina con '.' '!' o '?')
    m = re.search(r"^(.+?[\.\!\?])(\s|$)", s)
    if m:
        title = m.group(1).strip()
        if len(title) <= max_len:
            return title

    # 4) Fallback: truncar respetando palabras
    if len(s) <= max_len:
        return s
    cut = s[:max_len]
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut + "..."


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
                    # Almacenamos el texto completo del evento como nombre/título.
                    "nombre": desc_raw,
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

    # Forzar UTF-8 para evitar mojibake como "Ã³" o "Â«".
    res.encoding = 'utf-8'
    html = res.text

    eventos = parse_agenda(html)
    if not eventos:
        print("⚠️ No se encontraron eventos. Revisa que la página de Heptagrama sea accesible.")
        return

    db = init_firestore_client()

    # Limpiamos la colección "eventos" para no acumular datos viejos.
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
        # Normalizamos para que el ID sea estable y no genere duplicados.
        # Incluimos la hora para evitar que varios eventos con el mismo nombre en el mismo lugar se sobreescriban.
        doc_id = slugify(f"{evento.get('dia','')} {evento.get('lugar','')} {evento.get('hora','')} {evento.get('nombre','')}")

        # Geocodificamos siempre que tengamos algo de dirección
        if evento.get("direccion") or evento.get("lugar"):
            direccion_full = normalize_address_for_geocode(evento.get("direccion", ""), evento.get("lugar", ""))
            coords = obtener_coordenadas(direccion_full)
            if not coords:
                # Prueba alternativas: primero sólo lugar, luego sólo dirección
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

    # Guardamos una marca de tiempo para que el frontend pueda mostrar "Última actualización".
    db.collection("meta").document("lastUpdate").set({
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "source": "Heptagrama"
    })

    print("\n✅ Actualización completa.")


if __name__ == "__main__":
    main()
