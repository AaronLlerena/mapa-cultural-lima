#!/usr/bin/env python3
"""
Scraper — Agenda Cultural de Lima (Heptagrama)
===============================================
Estructura confirmada del HTML:

  <article class="s">
    <details>
      <summary class="h2">Lunes 23</summary>
      <p>                          ← bloque de lugar
        "Nombre del Lugar"
        <br>
        "(dirección - distrito)"
      </p>
      <p>                          ← descripción del evento
        "Texto completo..."
      </p>
      <hr>
      <p>Otro lugar<br>(direc.)</p>
      <p>Otra descripción...</p>
      <hr>
      ...
    </details>
    <details>
      <summary class="h2">Martes 24</summary>
      ...
    </details>
    ...
    <details>
      <summary class="h2">EXPOSICIONES</summary>  ← ignorar desde aquí
      ...
    </details>
  </article>

Dentro de un <p> de descripción puede haber múltiples eventos con horas:
  "8:00pm. Evento A...\n9:30pm. Evento B..."
En ese caso se crean eventos separados.
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

# ── HELPERS ───────────────────────────────────────────────────────────────────
def slugify(t):
    s = re.sub(r"[^a-z0-9]+", "-", str(t).strip().lower())
    return s.strip("-") or "evento"

def guess_tipo(t):
    t = (t or "").lower()
    if any(k in t for k in ["teatro","obra","función","funcion","dramaturgia","escena","monólogo"]): return "Teatro"
    if any(k in t for k in ["música","musica","concierto","dj","band","orquesta","jazz","rock","crioll","blues","salsa"]): return "Música"
    if any(k in t for k in ["expo","exposición","exposicion","muestra","arte","foto","pintura","galería","inaugurac"]): return "Arte/Expo"
    if any(k in t for k in ["cine","películ","film","documental","cortometraje","kino"]): return "Cine"
    return "Otro"

# Captura horas: "9:00pm", "8:30am", "a las 8pm", "de 2:00pm"
_RE_HORA = re.compile(r"(?:(?:a la[s]?|de)\s+)?(\d{1,2}:\d{2}\s*(?:am|pm))", re.I)

def primera_hora(texto):
    m = _RE_HORA.search(texto or "")
    return m.group(1).strip() if m else ""

def limpiar(t):
    return re.sub(r"\s+", " ", (t or "")).strip()

# ── EXTRAER TEXTO DE UN <p> RESPETANDO <br> ───────────────────────────────────
def p_a_lineas(p: Tag) -> list[str]:
    """
    Convierte un <p> con <br> internos en una lista de líneas limpias.
    Ignora líneas vacías.
    """
    lineas = []
    buf = []
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

# ── CREAR EVENTOS A PARTIR DE LUGAR + DESCRIPCIÓN ────────────────────────────
def crear_eventos(dia, lugar, direccion, descripcion) -> list[dict]:
    """
    A partir de un bloque de descripción, crea uno o varios eventos.

    Si la descripción tiene múltiples horas (ej: Jazz Zone con 2 funciones),
    divide en sub-eventos por cada bloque "HH:MMam/pm. texto".

    La hora del lugar (primer evento del bloque) se usa como hora_inicio
    del recinto en caso de evento multi-función.
    """
    desc = limpiar(descripcion)
    if not desc:
        return []

    # Detecta múltiples bloques iniciados con hora
    # Ejemplo: "8:00pm. Stand Up...\n10:00pm. Música y Danza..."
    multi = re.findall(
        r"(\d{1,2}:\d{2}\s*(?:am|pm)\.?\s+.+?)(?=\d{1,2}:\d{2}\s*(?:am|pm)\.|$)",
        desc, re.I | re.S
    )
    bloques = [limpiar(b) for b in multi if limpiar(b)] if len(multi) > 1 else [desc]

    hora_fallback = primera_hora(desc)  # hora del primer evento del lugar

    resultado = []
    for bloque in bloques:
        hora = primera_hora(bloque) or hora_fallback
        resultado.append({
            "dia":        dia,
            "lugar":      lugar,
            "direccion":  direccion,
            "descripcion": bloque,
            "nombre":     bloque,          # mantener para compatibilidad / doc_id
            "hora":       hora,
            "tipo":       guess_tipo(bloque),
        })
    return resultado

# ── PARSER PRINCIPAL ──────────────────────────────────────────────────────────
def parse_agenda(html: str) -> list[dict]:
    """
    Recorre <article class="s"> → <details> → pares de <p> separados por <hr>.

    Estructura dentro de cada <details>:
      <summary class="h2">Lunes 23</summary>
      <p> lugar <br> dirección </p>       ← p_lugar
      <p> descripción del evento </p>     ← p_desc
      <hr>
      <p> lugar <br> dirección </p>
      <p> descripción </p>
      <hr>
      ...
    """
    soup = BeautifulSoup(html, "html.parser")
    eventos = []

    # Contenedor principal
    article = soup.find("article", class_="s") or soup.find("article") or soup.find("main") or soup.body
    if not article:
        print("❌ No se encontró el contenedor principal <article>.")
        return []

    RE_DIA = re.compile(r"(Lunes|Martes|Mi[eé]rcoles|Jueves|Viernes|S[aá]bado|Domingo)", re.I)

    for details in article.find_all("details", recursive=False):
        summary = details.find("summary")
        if not summary:
            continue

        dia_texto = limpiar(summary.get_text())

        # Parar en EXPOSICIONES
        if re.search(r"EXPOSICI", dia_texto, re.I):
            print(f"   🛑 Sección EXPOSICIONES encontrada → deteniendo scrape.")
            break

        # Solo procesar si es un día de la semana
        if not RE_DIA.search(dia_texto):
            continue

        print(f"\n📅 Procesando: {dia_texto}")

        # Recoge los hijos directos del <details> (excepto el <summary>)
        # Los agrupa en pares: (p_lugar, p_desc) separados por <hr>
        hijos = [h for h in details.children
                 if isinstance(h, Tag) and h.name != "summary"]

        i = 0
        while i < len(hijos):
            hijo = hijos[i]

            # Saltar <hr>
            if hijo.name == "hr":
                i += 1
                continue

            # Esperamos un <p> de lugar
            if hijo.name != "p":
                i += 1
                continue

            p_lugar = hijo
            lineas_lugar = p_a_lineas(p_lugar)

            if not lineas_lugar:
                i += 1
                continue

            # Extrae lugar y dirección
            lugar_raw  = lineas_lugar[0]
            lugar      = re.sub(r"\s*\(.*?\)\s*$", "", lugar_raw).strip()
            direccion  = ""

            # La dirección puede estar en la misma línea entre paréntesis,
            # o en la segunda línea entre paréntesis
            m = re.search(r"\(([^)]+)\)", lugar_raw)
            if m:
                lugar     = lugar_raw[:m.start()].strip()
                direccion = m.group(1).strip()
            elif len(lineas_lugar) > 1 and lineas_lugar[1].startswith("("):
                direccion = lineas_lugar[1].strip("() ")

            # Busca el siguiente <p> de descripción (puede haber un <p> vacío entre medio)
            p_desc = None
            j = i + 1
            while j < len(hijos):
                if hijos[j].name == "hr":
                    break
                if hijos[j].name == "p":
                    txt = limpiar(hijos[j].get_text())
                    if txt:
                        p_desc = hijos[j]
                        j += 1
                        break
                j += 1

            if p_desc:
                descripcion = limpiar(p_desc.get_text(" "))
                nuevos = crear_eventos(dia_texto, lugar, direccion, descripcion)
                for ev in nuevos:
                    print(f"   ✔ [{ev['hora'] or '??:??'}] {lugar}")
                eventos.extend(nuevos)
                i = j  # avanza al siguiente bloque
            else:
                i += 1

    return eventos

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("🔎 Descargando agenda desde Heptagrama...")
    res = requests.get(HEPTAGRAMA_URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    }, timeout=30)
    res.encoding = "utf-8"

    eventos = parse_agenda(res.text)

    if not eventos:
        print("\n❌ No se encontraron eventos.")
        print("   Tip: guarda el HTML con:")
        print("   curl -A 'Mozilla/5.0' -o pagina.html https://heptagrama.com/agenda-cultural-lima.htm")
        print("   y compártelo para analizar la estructura exacta.")
        return

    from collections import Counter
    print(f"\n✅ {len(eventos)} eventos encontrados:")
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
    print(f"   ✅ {cnt} documentos eliminados.")

    print("\n☁️  Subiendo a Firestore...")
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
    print("\n✅ Actualización completa.")


if __name__ == "__main__":
    main()