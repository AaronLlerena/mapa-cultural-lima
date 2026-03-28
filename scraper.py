#!/usr/bin/env python3
"""
Scraper de la Agenda Cultural de Lima (Heptagrama)
Versión con diagnóstico automático de estructura HTML.
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

# ── HELPERS ───────────────────────────────────────────────────────────────────
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

_RE_HORA = re.compile(r"(?:(?:a las|de)\s+)?(\d{1,2}:\d{2}\s*(?:am|pm))", re.I)

def primera_hora(texto):
    m = _RE_HORA.search(texto or "")
    return m.group(1).strip() if m else ""

def limpiar(t):
    return re.sub(r"\s+", " ", (t or "")).strip()

# ── DIAGNÓSTICO ───────────────────────────────────────────────────────────────
def diagnosticar(soup):
    """
    Imprime un resumen de la estructura HTML para entender cómo está hecha la página.
    Útil si el parser falla.
    """
    print("\n═══ DIAGNÓSTICO DE ESTRUCTURA HTML ═══")
    print("Tags de primer nivel dentro de <body> o contenedor principal:")
    body = soup.find("body") or soup
    # Muestra los primeros 40 hijos directos con contenido
    count = 0
    for ch in body.children:
        if not isinstance(ch, Tag): continue
        txt = limpiar(ch.get_text())[:70]
        clases = ch.get("class", [])
        print(f"  <{ch.name} class={clases}> → '{txt}'")
        count += 1
        if count >= 40: break

    print("\n¿Hay <details>?", bool(soup.find("details")))
    print("¿Hay <h2>?",      bool(soup.find("h2")))
    print("¿Hay <h3>?",      bool(soup.find("h3")))
    print("¿Hay <hr>?",      bool(soup.find("hr")))
    print("¿Hay <section>?", bool(soup.find("section")))

    # Busca el texto "Lunes" para ver en qué tag aparece
    for tag in soup.find_all(True):
        txt = tag.get_text()
        if re.search(r"\bLunes\b", txt) and len(txt) < 30:
            print(f"\nTag con 'Lunes': <{tag.name} class={tag.get('class',[])}> → '{limpiar(txt)}'")
            break
    print("═══════════════════════════════════════\n")

# ── PARSERS ───────────────────────────────────────────────────────────────────

def extraer_bloques_de_texto(texto_completo: str) -> list[dict]:
    """
    Parser de texto plano como fallback.
    Asume el formato que vemos en el markdown de la página:

      Lunes 23
      ─────────────
      Nombre del Lugar
      (dirección - distrito)

      Descripción del evento...

      ---

      Otro lugar
      ...

      EXPOSICIONES   ← stop
    """
    eventos = []
    dia_actual = None
    RE_DIA = re.compile(r"^(Lunes|Martes|Mi[eé]rcoles|Jueves|Viernes|S[aá]bado|Domingo)\s+\d+", re.I)

    # Separa por bloques (--- o línea en blanco doble)
    bloques = re.split(r"\n---+\n|\n{3,}", texto_completo)

    for bloque in bloques:
        bloque = bloque.strip()
        if not bloque:
            continue

        # ¿Es encabezado de día?
        if RE_DIA.match(bloque):
            dia_actual = limpiar(bloque.split("\n")[0])
            continue

        # ¿Es la sección EXPOSICIONES? → parar
        if re.match(r"EXPOSICI", bloque, re.I):
            break

        if not dia_actual:
            continue

        lineas = [limpiar(l) for l in bloque.split("\n")]
        lineas = [l for l in lineas if l]  # quitar vacías
        if len(lineas) < 2:
            continue

        # Línea 1: lugar
        lugar_raw = lineas[0]
        lugar = re.sub(r"\s*\(.*?\)\s*$", "", lugar_raw).strip()

        # Línea 2: dirección (si está entre paréntesis)
        direccion = ""
        resto_idx = 1
        if lineas[1].startswith("(") and lineas[1].endswith(")"):
            direccion = lineas[1][1:-1].strip()
            resto_idx = 2

        descripcion = limpiar(" ".join(lineas[resto_idx:]))
        if not descripcion:
            continue

        # Detecta múltiples horas dentro
        _agregar_eventos(eventos, dia_actual, lugar, direccion, descripcion)

    return eventos


def extraer_de_details(soup) -> list[dict]:
    """
    Parser para cuando la página usa <details>/<summary>.
    """
    eventos = []
    RE_DIA = re.compile(r"(Lunes|Martes|Mi[eé]rcoles|Jueves|Viernes|S[aá]bado|Domingo)", re.I)

    for det in soup.find_all("details"):
        summary = det.find("summary")
        if not summary:
            continue
        dia_texto = limpiar(summary.get_text())
        if not RE_DIA.search(dia_texto):
            continue

        # Para en EXPOSICIONES
        if re.search(r"EXPOSICI", dia_texto, re.I):
            break

        # Extrae texto del bloque separando por <hr> o por pares de <p>
        contenido = det.get_text("\n")
        # Divide en sub-bloques por líneas vacías dobles o por <hr>
        sub = re.split(r"\n{2,}", contenido)
        i = 0
        while i < len(sub):
            bloque = sub[i].strip()
            if not bloque or bloque == dia_texto:
                i += 1
                continue
            lineas = [limpiar(l) for l in bloque.split("\n") if limpiar(l)]
            if not lineas:
                i += 1
                continue

            lugar_raw = lineas[0]
            lugar = re.sub(r"\s*\(.*?\)\s*$", "", lugar_raw).strip()
            direccion = ""
            desc_start = 1
            if len(lineas) > 1 and lineas[1].startswith("(") and lineas[1].endswith(")"):
                direccion = lineas[1][1:-1].strip()
                desc_start = 2

            # La descripción puede estar en el mismo bloque o en el siguiente
            desc_lineas = lineas[desc_start:]
            if not desc_lineas and i + 1 < len(sub):
                i += 1
                desc_lineas = [limpiar(l) for l in sub[i].split("\n") if limpiar(l)]

            descripcion = limpiar(" ".join(desc_lineas))
            if descripcion and lugar:
                _agregar_eventos(eventos, dia_texto, lugar, direccion, descripcion)
            i += 1

    return eventos


def extraer_de_parrafos(soup) -> list[dict]:
    """
    Parser para cuando la página usa <p> con <br> internos, separados por <hr>.
    Funciona para la mayoría de los casos que vemos en Heptagrama.
    """
    eventos = []
    dia_actual = None
    RE_DIA = re.compile(r"^(Lunes|Martes|Mi[eé]rcoles|Jueves|Viernes|S[aá]bado|Domingo)\s*\d*$", re.I)

    # Recorre todos los nodos: h2/h3 marcan días, p marcan eventos, hr separa
    body = soup.find("article") or soup.find("main") or soup.find("body") or soup

    for elem in body.find_all(["h1","h2","h3","h4","p","hr","details","summary"], recursive=True):

        # ── Encabezado de día ────────────────────────────────────────────
        if elem.name in ("h1","h2","h3","h4"):
            txt = limpiar(elem.get_text())
            if re.search(r"EXPOSICI", txt, re.I):
                break
            if RE_DIA.match(txt):
                dia_actual = txt
            continue

        if not dia_actual:
            continue

        # ── Párrafo de evento ────────────────────────────────────────────
        if elem.name == "p":
            # Reconstruye líneas por <br>
            lineas = []
            buf = []
            for node in elem.children:
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

            lineas = [l for l in lineas if l]
            if not lineas:
                continue

            lugar_raw = lineas[0]
            lugar = re.sub(r"\s*\(.*?\)\s*$", "", lugar_raw).strip()
            direccion = ""
            desc_start = 1
            if len(lineas) > 1 and lineas[1].startswith("("):
                direccion = lineas[1].strip("()")
                desc_start = 2

            descripcion = limpiar(" ".join(lineas[desc_start:]))
            if descripcion and lugar:
                _agregar_eventos(eventos, dia_actual, lugar, direccion, descripcion)

    return eventos


def _agregar_eventos(eventos, dia, lugar, direccion, descripcion):
    """
    Añade uno o varios eventos a la lista.
    Si la descripción contiene múltiples horas (ej: Jazz Zone),
    crea un evento por cada bloque de hora.
    """
    # Intenta detectar múltiples bloques "HH:MMam/pm. Texto"
    multi = re.findall(
        r"(\d{1,2}:\d{2}\s*(?:am|pm)\.?\s+.+?)(?=\d{1,2}:\d{2}\s*(?:am|pm)|$)",
        descripcion, re.I | re.S
    )
    bloques = [limpiar(b) for b in multi if limpiar(b)] if len(multi) > 1 else [descripcion]

    hora_fallback = primera_hora(descripcion)

    for bloque in bloques:
        hora = primera_hora(bloque) or hora_fallback
        eventos.append({
            "dia": dia,
            "lugar": lugar,
            "direccion": direccion,
            "descripcion": bloque,
            "nombre": bloque,
            "hora": hora,
            "tipo": guess_tipo(bloque),
        })


# ── PARSER PRINCIPAL — prueba los tres métodos ────────────────────────────────
def parse_agenda(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")

    # Diagnóstico siempre visible
    diagnosticar(soup)

    # 1) ¿Tiene <details>? → parser de details
    if soup.find("details"):
        print("🔍 Usando parser: <details>/<summary>")
        eventos = extraer_de_details(soup)
        if eventos:
            return eventos
        print("   ⚠ Parser details no encontró eventos, probando parrafos...")

    # 2) ¿Tiene <h2> o <h3> con nombres de días?
    RE_DIA = re.compile(r"(Lunes|Martes|Mi[eé]rcoles|Jueves|Viernes|S[aá]bado|Domingo)", re.I)
    headings = [h for h in soup.find_all(["h2","h3"]) if RE_DIA.search(h.get_text())]
    if headings:
        print("🔍 Usando parser: <h2>/<h3> + <p>")
        eventos = extraer_de_parrafos(soup)
        if eventos:
            return eventos
        print("   ⚠ Parser parrafos no encontró eventos, probando texto plano...")

    # 3) Fallback: texto plano de toda la página
    print("🔍 Usando parser: texto plano (fallback)")
    texto = soup.get_text("\n")
    return extraer_de_bloques_de_texto(texto)


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("🔎 Descargando agenda...")
    res = requests.get(HEPTAGRAMA_URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    }, timeout=30)
    res.encoding = "utf-8"

    eventos = parse_agenda(res.text)

    if not eventos:
        print("\n❌ Ningún parser encontró eventos.")
        print("   Guarda el HTML con: curl -o pagina.html https://heptagrama.com/agenda-cultural-lima.htm")
        print("   y compártelo para analizar la estructura exacta.")
        return

    print(f"\n✅ {len(eventos)} eventos encontrados.")
    from collections import Counter
    for d, n in sorted(Counter(e["dia"] for e in eventos).items()):
        print(f"   {d}: {n} evento(s)")

    db = init_db()

    print("\n🧹 Limpiando Firestore...")
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

    print("\n☁️  Subiendo eventos...")
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