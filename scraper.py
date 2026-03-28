#!/usr/bin/env python3
"""
Scraper de la Agenda Cultural de Lima (Heptagrama)
====================================================
Estructura real de la página (markdown/HTML):

  Lunes 23          ← encabezado de día (h2 o texto tras <hr>)

  Nombre del Lugar              ← línea 1
  (dirección - distrito)        ← línea 2 (en paréntesis)
                                ← línea en blanco
  Descripción completa...       ← cuerpo, puede tener varias horas

  ---                           ← separador entre eventos

  Otro Lugar
  ...

  EXPOSICIONES      ← sección final a ignorar
"""

import json, os, re, time
import requests
from bs4 import BeautifulSoup, NavigableString, Tag
import firebase_admin
from firebase_admin import credentials, firestore
from geopy.geocoders import ArcGIS

# ── CONFIG ───────────────────────────────────────────────────────────────────
HEPTAGRAMA_URL    = "https://heptagrama.com/agenda-cultural-lima.htm"
FIREBASE_SECRET   = "FIREBASE_SERVICE_ACCOUNT"

# Bounding box Lima Metropolitana
LIMA_LAT = (-12.30, -11.70)
LIMA_LON = (-77.20, -76.70)

# ── FIREBASE ─────────────────────────────────────────────────────────────────
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

def _geocode(q):
    try:
        r = geo.geocode(q)
        if r and en_lima(r.latitude, r.longitude):
            return r.latitude, r.longitude
        if r:
            print(f"   ⚠ Fuera de Lima: '{q}' → ({r.latitude:.3f},{r.longitude:.3f})")
    except Exception as e:
        print(f"   ⚠ Geocode error: {e}")
    return None

def simplificar(lugar):
    """'Anfiteatro X del Parque de la Exposición' → 'Parque de la Exposición'"""
    m = re.search(r"\b(?:del|de la|de los|de las)\s+(.+)$", lugar, re.I)
    if m:
        c = m.group(1).strip()
        if len(c.split()) >= 2:
            return c
    return lugar

def geocodificar(lugar, direccion):
    dir_n = (direccion or "").replace(" - ", ", ").strip()
    lug_n = (lugar or "").strip()
    intentos = []
    if dir_n and lug_n: intentos.append(f"{dir_n}, {lug_n}, Lima, Peru")
    if dir_n:           intentos.append(f"{dir_n}, Lima, Peru")
    if lug_n:           intentos.append(f"{lug_n}, Lima, Peru")
    s = simplificar(lug_n)
    if s != lug_n:      intentos.append(f"{s}, Lima, Peru")
    for q in intentos:
        c = _geocode(q)
        if c:
            print(f"   📍 '{q}'")
            return c
        time.sleep(0.3)
    return None

# ── HELPERS ──────────────────────────────────────────────────────────────────
def slugify(t):
    s = re.sub(r"[^a-z0-9]+", "-", str(t).strip().lower())
    return s.strip("-") or "evento"

def guess_tipo(t):
    t = (t or "").lower()
    if any(k in t for k in ["teatro","obra","función","funcion","dramaturgia","escena"]): return "Teatro"
    if any(k in t for k in ["música","musica","concierto","dj","band","orquesta","jazz","rock","crioll","blues"]): return "Música"
    if any(k in t for k in ["expo","exposición","exposicion","muestra","arte","foto","pintura","galería"]): return "Arte/Expo"
    if any(k in t for k in ["cine","películ","film","documental","cortometraje"]): return "Cine"
    return "Otro"

# Captura horas: "9:00pm", "8:30am", "a las 8:00pm", "de 2:00pm"
_RE_HORA = re.compile(r"(?:(?:a las|de)\s+)?(\d{1,2}:\d{2}\s*(?:am|pm))", re.I)

def primera_hora(texto):
    """Extrae la primera hora mencionada en el texto."""
    m = _RE_HORA.search(texto or "")
    return m.group(1).strip() if m else ""

# ── PARSER PRINCIPAL ─────────────────────────────────────────────────────────
def limpiar(t):
    return re.sub(r"\s+", " ", (t or "")).strip()

def parse_agenda(html: str) -> list[dict]:
    """
    Parsea la página de Heptagrama.

    La página renderiza así (en HTML real):
      <h2>Lunes 23</h2>
      <p>
        Nombre del Lugar<br>
        (dirección - distrito)<br>
        <br>
        Descripción del evento...<br>
      </p>
      <hr>
      <p>...</p>
      ...
      <h2>EXPOSICIONES</h2>   ← parar aquí
    """
    soup = BeautifulSoup(html, "html.parser")
    eventos = []
    dia_actual = None

    # Recorre todos los nodos de primer nivel dentro del body
    # Heptagrama usa <h2> para días y <p> para bloques lugar+desc separados por <hr>
    contenedor = soup.find("article") or soup.find("main") or soup.body

    for elem in contenedor.descendants if contenedor else []:
        # Solo procesamos Tags directos, no texto suelto
        if not isinstance(elem, Tag):
            continue

        tag = elem.name

        # ── Encabezado de día ────────────────────────────────────────────────
        if tag in ("h2", "h3"):
            texto = limpiar(elem.get_text())
            # Parar en EXPOSICIONES
            if "exposici" in texto.lower():
                break
            # Es un día si empieza con nombre de día de la semana o tiene número
            if re.match(r"(Lunes|Martes|Mi[eé]rcoles|Jueves|Viernes|S[aá]bado|Domingo)", texto, re.I):
                dia_actual = texto
            continue

        # ── Bloque de evento (párrafo) ───────────────────────────────────────
        if tag == "p" and dia_actual:
            _procesar_parrafo(elem, dia_actual, eventos)

    return eventos


def _procesar_parrafo(p: Tag, dia: str, eventos: list):
    """
    Dentro de un <p>, Heptagrama pone:
      Nombre del Lugar<br>
      (dirección)<br>
      <br>           ← línea en blanco separa lugar de descripción
      Descripción...

    Extrae lugar, dirección y descripción, y crea uno o más eventos
    (cuando hay múltiples horas en la descripción).
    """
    # Reconstruye las líneas respetando <br>
    lineas = []
    buf = []
    for node in p.children:
        if isinstance(node, NavigableString):
            buf.append(str(node))
        elif isinstance(node, Tag):
            if node.name == "br":
                lineas.append(limpiar("".join(buf)))
                buf = []
            else:
                buf.append(node.get_text(" "))
    if buf:
        lineas.append(limpiar("".join(buf)))

    # Quita líneas vacías del principio y fin
    lineas = [l for l in lineas]  # conserva vacías internamente

    if not lineas:
        return

    # Primera línea no vacía → nombre del lugar
    idx = 0
    while idx < len(lineas) and not lineas[idx]:
        idx += 1
    if idx >= len(lineas):
        return

    lugar_raw = lineas[idx]
    idx += 1

    # Segunda línea no vacía → puede ser la dirección (si está entre paréntesis)
    direccion = ""
    if idx < len(lineas):
        siguiente = lineas[idx]
        if siguiente.startswith("(") and siguiente.endswith(")"):
            direccion = siguiente[1:-1].strip()
            idx += 1
        elif re.match(r"\(", siguiente):
            # Dirección sin cerrar paréntesis en la misma línea (raro pero ocurre)
            direccion = siguiente.strip("()")
            idx += 1

    # El resto (saltando la línea en blanco separadora) es la descripción
    while idx < len(lineas) and not lineas[idx]:
        idx += 1

    desc_lineas = lineas[idx:]
    descripcion = limpiar(" ".join(l for l in desc_lineas if l))

    if not descripcion:
        return

    # Limpia el nombre del lugar: quita paréntesis si quedaron
    lugar = re.sub(r"\s*\(.*?\)\s*$", "", lugar_raw).strip()

    # ── Detecta múltiples horas en la descripción ────────────────────────────
    # Patrón: líneas que empiezan con hora (ej: "9:00pm. Texto del evento")
    multi = re.findall(
        r"(\d{1,2}:\d{2}\s*(?:am|pm)\.?\s+[^\d].*?)(?=\d{1,2}:\d{2}\s*(?:am|pm)|$)",
        descripcion, re.I | re.S
    )

    if len(multi) > 1:
        # Evento con múltiples funciones: creamos uno por cada bloque
        # La hora de "inicio" del lugar es la del primer sub-evento
        hora_lugar = primera_hora(multi[0])
        for bloque in multi:
            bloque = limpiar(bloque)
            hora = primera_hora(bloque)
            eventos.append({
                "dia": dia,
                "lugar": lugar,
                "direccion": direccion,
                "descripcion": bloque,
                "nombre": bloque,
                "hora": hora or hora_lugar,
                "tipo": guess_tipo(bloque),
            })
    else:
        # Evento simple
        hora = primera_hora(descripcion)
        eventos.append({
            "dia": dia,
            "lugar": lugar,
            "direccion": direccion,
            "descripcion": descripcion,
            "nombre": descripcion,
            "hora": hora,
            "tipo": guess_tipo(descripcion),
        })


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("🔎 Descargando agenda...")
    res = requests.get(HEPTAGRAMA_URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    }, timeout=30)
    res.encoding = "utf-8"

    eventos = parse_agenda(res.text)
    if not eventos:
        print("⚠ No se encontraron eventos.")
        return
    print(f"✅ {len(eventos)} eventos encontrados.")

    # Muestra un resumen por día para diagnóstico
    from collections import Counter
    por_dia = Counter(e["dia"] for e in eventos)
    for d, n in sorted(por_dia.items()):
        print(f"   {d}: {n} evento(s)")

    db = init_db()

    print("🧹 Limpiando Firestore...")
    batch = db.batch()
    cnt = 0
    for d in db.collection("eventos").stream():
        batch.delete(d.reference)
        cnt += 1
        if cnt % 500 == 0:
            batch.commit(); batch = db.batch()
    if cnt % 500 != 0:
        batch.commit()
    print(f"   ✅ {cnt} eliminados.")

    print("☁️  Subiendo eventos...")
    for ev in eventos:
        doc_id = slugify(f"{ev['dia']} {ev['lugar']} {ev['hora']} {ev['nombre'][:60]}")

        if ev.get("lugar") or ev.get("direccion"):
            coords = geocodificar(ev["lugar"], ev["direccion"])
            if coords:
                ev["lat"], ev["lon"] = coords
            else:
                print(f"   ❌ Sin coords: {ev['lugar']} / {ev['direccion']}")

        db.collection("eventos").document(doc_id).set(ev)
        print(f"   ☁ {ev['dia']} - {ev['lugar']} [{ev['hora'] or '??'}]")
        time.sleep(1)

    db.collection("meta").document("lastUpdate").set({
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "source": "Heptagrama"
    })
    print("\n✅ Listo.")

if __name__ == "__main__":
    main()