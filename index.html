#!/usr/bin/env python3
"""
Scraper — Agenda Cultural de Lima (Heptagrama)

Estructura HTML confirmada:
  <article class="s">
    <details>
      <summary class="h2">Lunes 23</summary>

      <p> Nombre Recinto <br> (dirección - distrito) </p>   ← p_lugar
      <p> Descripción del evento... a las 8:00pm. </p>      ← p_desc
      <hr>
      <p> Otro Recinto <br> (dirección) </p>
      <p> Otra descripción... </p>
      <hr>
      ...
    </details>
    ...
    <details>                     ← EXPOSICIONES → ignorar todo
      <summary>Exposiciones</summary>
      ...
    </details>
  </article>

Reglas:
- Dentro de cada <details>, los hijos son: <p>, <p>, <hr>, <p>, <p>, <hr>, ...
- Siempre en pares (p_lugar, p_desc) separados por <hr>.
- La hora SIEMPRE está en la descripción. Tomamos solo la PRIMERA hora.
- El geocoding usa SOLO lugar + dirección del recinto (nunca descripción).
"""

import json, os, re, time
import requests
from bs4 import BeautifulSoup, NavigableString, Tag
import firebase_admin
from firebase_admin import credentials, firestore
from geopy.geocoders import ArcGIS

# ── CONFIG ────────────────────────────────────────────────────────────────────
HEPTAGRAMA_URL  = "https://heptagrama.com/agenda-cultural-lima.htm"
FIREBASE_SECRET = "FIREBASE_SERVICE_ACCOUNT"
LIMA_LAT = (-12.30, -11.70)
LIMA_LON = (-77.20, -76.70)

# ── FIREBASE ──────────────────────────────────────────────────────────────────
def init_db():
    key_json = os.getenv(FIREBASE_SECRET)
    if key_json:
        cred = credentials.Certificate(json.loads(key_json))
    else:
        cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()

# ── GEOCODING ─────────────────────────────────────────────────────────────────
geo = ArcGIS(timeout=10)

def en_lima(lat, lon):
    return LIMA_LAT[0] <= lat <= LIMA_LAT[1] and LIMA_LON[0] <= lon <= LIMA_LON[1]

def _geocode_raw(query):
    """Geocodifica y retorna (lat,lon) solo si cae dentro de Lima."""
    try:
        r = geo.geocode(query)
        if r:
            if en_lima(r.latitude, r.longitude):
                return r.latitude, r.longitude
            print(f"   ⚠ Fuera de Lima '{query}' → ({r.latitude:.3f},{r.longitude:.3f})")
    except Exception as e:
        print(f"   ⚠ Error geocode: {e}")
    return None

def simplificar_lugar(lugar):
    """
    Extrae el nombre más reconocible de un recinto largo.
    'Anfiteatro X del Parque de la Exposición' → 'Parque de la Exposición'
    """
    m = re.search(r"\b(?:del|de la|de los|de las)\s+(.+)$", lugar, re.I)
    if m:
        c = m.group(1).strip()
        if len(c.split()) >= 2:
            return c
    return lugar

def geocodificar(lugar, direccion):
    """
    Geocodifica usando SOLO el nombre del recinto y su dirección.
    Prueba múltiples variantes en orden hasta encontrar un punto en Lima.
    """
    dir_n = (direccion or "").replace(" - ", ", ").strip()
    lug_n = (lugar or "").strip()

    intentos = []
    if dir_n and lug_n:
        intentos.append(f"{dir_n}, {lug_n}, Lima, Peru")
    if dir_n:
        intentos.append(f"{dir_n}, Lima, Peru")
    if lug_n:
        intentos.append(f"{lug_n}, Lima, Peru")
    s = simplificar_lugar(lug_n)
    if s != lug_n:
        intentos.append(f"{s}, Lima, Peru")

    for q in intentos:
        coords = _geocode_raw(q)
        if coords:
            print(f"   📍 OK: '{q}'")
            return coords
        time.sleep(0.3)

    print(f"   ❌ Sin coords: {lug_n} / {dir_n}")
    return None

# ── HELPERS ───────────────────────────────────────────────────────────────────
def slugify(t):
    s = re.sub(r"[^a-z0-9]+", "-", str(t).strip().lower())
    return s.strip("-") or "evento"

def guess_tipo(t):
    t = (t or "").lower()
    if any(k in t for k in ["teatro","obra","función","funcion","dramaturgia","escena","monólogo","unipersonal"]):
        return "Teatro"
    if any(k in t for k in ["música","musica","concierto","dj","band","orquesta","jazz","rock","crioll","blues","salsa","cumbia"]):
        return "Música"
    if any(k in t for k in ["expo","exposición","exposicion","muestra","arte","foto","pintura","galería","inaugurac"]):
        return "Arte/Expo"
    if any(k in t for k in ["cine","películ","film","documental","cortometraje","kino"]):
        return "Cine"
    return "Otro"

# Regex de hora: captura "8:00pm", "8:00 pm", "8:00PM", "a las 8:00pm", "de 2:00pm"
# Acepta con o sin espacio entre número y am/pm
_RE_HORA = re.compile(
    r"(?:a\s+las?\s+|de\s+)?(\d{1,2}:\d{2}\s*[aApP][mM])",
    re.I
)

def primera_hora(texto):
    """Retorna SOLO la primera hora encontrada en el texto."""
    m = _RE_HORA.search(texto or "")
    if m:
        # Normaliza: quita espacios internos ("8:00 pm" → "8:00pm")
        return re.sub(r"\s+", "", m.group(1)).lower()
    return ""

def limpiar(t):
    return re.sub(r"\s+", " ", (t or "")).strip()

# ── EXTRAER TEXTO DE UN <p> CON <br> INTERNOS ────────────────────────────────
def p_lineas(p: Tag) -> list[str]:
    """
    Convierte <p>Texto1<br>(Texto2)</p> en ['Texto1', '(Texto2)'].
    Ignora líneas vacías.
    """
    lineas, buf = [], []
    for node in p.children:
        if isinstance(node, NavigableString):
            buf.append(str(node))
        elif isinstance(node, Tag) and node.name == "br":
            lineas.append(limpiar("".join(buf)))
            buf = []
        elif isinstance(node, Tag):
            buf.append(node.get_text(" "))
    if buf:
        lineas.append(limpiar("".join(buf)))
    return [l for l in lineas if l]

# ── PARSER PRINCIPAL ──────────────────────────────────────────────────────────
def parse_agenda(html: str) -> list[dict]:
    """
    Recorre <article class="s"> → cada <details> es un día.
    Dentro de cada <details>, los hijos directos son:
      <summary>  → nombre del día
      <p>        → lugar (con <br> y dirección)
      <p>        → descripción
      <hr>       → separador
      <p>        → siguiente lugar
      <p>        → siguiente descripción
      <hr>
      ...
    Lógica: recoge todos los <p> hijos directos en orden,
    los empareja de 2 en 2 (par, impar) como (lugar, descripción).
    Los <hr> se ignoran — solo sirven visualmente.
    """
    soup = BeautifulSoup(html, "html.parser")
    eventos = []

    article = (soup.find("article", class_="s")
               or soup.find("article")
               or soup.find("main")
               or soup.body)

    if not article:
        print("❌ No se encontró el contenedor <article>.")
        return []

    RE_DIA = re.compile(
        r"^(Lunes|Martes|Mi[eé]rcoles|Jueves|Viernes|S[aá]bado|Domingo)\s*\d*$",
        re.I
    )

    # Itera sobre los <details> hijos directos del article
    for details in article.find_all("details", recursive=False):
        summary = details.find("summary")
        if not summary:
            continue

        dia_texto = limpiar(summary.get_text())

        # Ignorar EXPOSICIONES y cualquier sección que no sea día de semana
        if re.search(r"EXPOSICI", dia_texto, re.I):
            print(f"   🛑 Ignorando sección: '{dia_texto}'")
            continue
        if not RE_DIA.match(dia_texto):
            print(f"   ⏭ Saltando sección desconocida: '{dia_texto}'")
            continue

        print(f"\n📅 {dia_texto}")

        # Recoge TODOS los <p> hijos directos del <details> (ignora <hr> y otros)
        parrafos = [h for h in details.children
                    if isinstance(h, Tag) and h.name == "p"]

        # Los empareja de 2 en 2: (p_lugar, p_desc)
        i = 0
        while i + 1 < len(parrafos):
            p_lugar = parrafos[i]
            p_desc  = parrafos[i + 1]
            i += 2

            # ── Extrae lugar y dirección del primer <p> ───────────────────
            lineas = p_lineas(p_lugar)
            if not lineas:
                continue

            lugar_raw = lineas[0]

            # Dirección: puede estar en la misma línea "Nombre (dir)" o en línea 2
            m = re.search(r"^(.*?)\s*\(([^)]+)\)\s*$", lugar_raw)
            if m:
                lugar     = m.group(1).strip()
                direccion = m.group(2).strip()
            elif len(lineas) > 1 and lineas[1].startswith("("):
                lugar     = lugar_raw
                direccion = lineas[1].strip("() ")
            else:
                lugar     = lugar_raw
                direccion = ""

            # ── Extrae descripción del segundo <p> ────────────────────────
            descripcion = limpiar(p_desc.get_text(" "))
            if not descripcion:
                continue

            # ── Hora: SIEMPRE la primera que aparezca en la descripción ───
            hora = primera_hora(descripcion)

            evento = {
                "dia":         dia_texto,
                "lugar":       lugar,
                "direccion":   direccion,
                "descripcion": descripcion,
                "nombre":      descripcion,   # compatibilidad
                "hora":        hora,
                "tipo":        guess_tipo(descripcion),
            }
            eventos.append(evento)
            print(f"   ✔ [{hora or '??:??'}] {lugar}")

    return eventos

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("🔎 Descargando agenda de Heptagrama...")
    res = requests.get(HEPTAGRAMA_URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    }, timeout=30)
    res.encoding = "utf-8"

    eventos = parse_agenda(res.text)

    if not eventos:
        print("\n❌ No se encontraron eventos.")
        return

    from collections import Counter
    print(f"\n✅ Total: {len(eventos)} eventos")
    for dia, n in sorted(Counter(e["dia"] for e in eventos).items()):
        print(f"   {dia}: {n} evento(s)")

    # ── Firestore ─────────────────────────────────────────────────────────
    db = init_db()

    print("\n🧹 Limpiando colección 'eventos'...")
    batch = db.batch()
    cnt = 0
    for doc in db.collection("eventos").stream():
        batch.delete(doc.reference)
        cnt += 1
        if cnt % 500 == 0:
            batch.commit(); batch = db.batch()
    if cnt % 500 != 0:
        batch.commit()
    print(f"   ✅ {cnt} documentos eliminados.")

    print("\n☁️  Subiendo eventos...")
    for ev in eventos:
        doc_id = slugify(
            f"{ev['dia']} {ev['lugar']} {ev['hora']} {ev['nombre'][:60]}"
        )

        # Geocodifica usando SOLO lugar + dirección del recinto
        coords = geocodificar(ev["lugar"], ev["direccion"])
        if coords:
            ev["lat"], ev["lon"] = coords

        db.collection("eventos").document(doc_id).set(ev)
        print(f"   ☁ {ev['dia']} [{ev['hora'] or '??'}] {ev['lugar']}")
        time.sleep(1)

    db.collection("meta").document("lastUpdate").set({
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "source":    "Heptagrama"
    })
    print("\n✅ Actualización completa.")

if __name__ == "__main__":
    main()