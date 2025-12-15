import firebase_admin
from firebase_admin import credentials, firestore
from geopy.geocoders import ArcGIS
from geopy.exc import GeocoderTimedOut
import time

# --- 1. CONFIGURACIÓN DE FIREBASE ---
# ⚠️ IMPORTANTE: Necesitas tu archivo de claves 'serviceAccountKey.json'
# Si lo corres en local, pon la ruta al archivo. 
# Si usas Colab, sube el archivo a la carpeta de archivos.
try:
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("🔥 Conexión a Firebase exitosa.")
except Exception as e:
    print(f"❌ Error conectando a Firebase: {e}")
    print("Asegúrate de tener el archivo 'serviceAccountKey.json' en la misma carpeta.")
    exit()

# --- 2. CONFIGURACIÓN DEL MAPA (ARCGIS) ---
geolocator = ArcGIS(timeout=10)

def obtener_coordenadas(direccion):
    """Obtiene lat/lon usando ArcGIS (No bloquea como Nominatim)"""
    try:
        location = geolocator.geocode(direccion)
        if location:
            return location.latitude, location.longitude
    except Exception as e:
        print(f"   ⚠️ Error de mapa: {e}")
    return None

# --- 3. TU LÓGICA DE SCRAPING O DATOS ---
# (Aquí puedes poner tu código de BeautifulSoup si scropeas una web real)
# Por ahora, usamos una lista manual de prueba para verificar que funcione:

eventos_a_subir = [
    {"nombre": "Jazz Zone", "lugar": "Miraflores", "direccion": "Avenida La Paz 646", "hora": "20:00", "tipo": "Música", "dia": "Lunes"},
    {"nombre": "El Gato Tulipán", "lugar": "Barranco", "direccion": "Bajada de Baños 350", "hora": "19:00", "tipo": "Arte/Expo", "dia": "Lunes"},
    {"nombre": "C.C. Ricardo Palma", "lugar": "Miraflores", "direccion": "Avenida José Larco 770", "hora": "18:00", "tipo": "Teatro", "dia": "Martes"}
]

print("\n🚀 Iniciando carga de datos...")

for evento in eventos_a_subir:
    direccion_full = f"{evento['direccion']}, {evento['lugar']}, Lima, Peru"
    print(f"📍 Procesando: {evento['nombre']}...")
    
    # 1. Buscamos coordenadas
    coords = obtener_coordenadas(direccion_full)
    
    if coords:
        evento['lat'] = coords[0]
        evento['lon'] = coords[1]
        print(f"   ✅ GPS encontrado: {coords}")
        
        # 2. Subimos a Firebase
        # Usamos .set() con el nombre para evitar duplicados, o .add() para auto-ID
        db.collection('eventos').document(evento['nombre']).set(evento)
        print("   ☁️  Guardado en Firebase")
    else:
        print("   ❌ No se encontró ubicación, saltando...")
    
    time.sleep(1) # Respetamos al servidor del mapa

print("\n🏁 Proceso terminado.")
