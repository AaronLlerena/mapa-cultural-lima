# mapa-cultural-lima

Este proyecto es una **web interactiva de eventos culturales en Lima** (mapa + filtros + ruta entre eventos) que lee los datos desde **Firebase Firestore**.

## 🌐 Página publicada
El sitio ya está desplegado en GitHub Pages en:

https://aaronllerena.github.io/mapa-cultural-lima/

## 🚀 Cómo ejecutar localmente
1. Poblar Firestore (opcional, si quieres datos reales):
   - Guarda tu `serviceAccountKey.json` en la carpeta del proyecto.
   - Ejecuta `python scraper.py`.
2. Levanta un servidor local:
   ```bash
   python -m http.server 8000
   ```
3. Abre en el navegador:
   `http://localhost:8000`

## 🔄 Actualización automática desde Heptagrama (GitHub Actions)
Este repo incluye un workflow que corre el scraper automáticamente cada cierto tiempo y actualiza Firestore.

### 1) Configura el secreto
En tu repo de GitHub (Settings → Secrets → Actions), crea un secreto llamado:
- `FIREBASE_SERVICE_ACCOUNT` (con el contenido JSON de tu llave de servicio de Firebase)

### 2) Verifica que el workflow esté activo
El archivo que se ejecuta es:
- `.github/workflows/update-firestore.yml`

### 3) Forzar una ejecución manual
Desde GitHub, ve a la pestaña **Actions**, selecciona el workflow "Update Firestore from Heptagrama" y haz click en **Run workflow**.
