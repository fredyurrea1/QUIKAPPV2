# app.py
# ─────────────────────────────────────────────────────────────
# FastAPI + Uvicorn
# • GET  /           → sirve templates/index.html
# • POST /procesar   → recibe PDF, genera consolidado.xlsx y lo devuelve
# • Cada subida vive en runs/<uuid>/, que se borra 30 min después
# ─────────────────────────────────────────────────────────────

from fastapi import (
    FastAPI,
    UploadFile,
    File,
    BackgroundTasks,
    HTTPException,
)
from fastapi.responses import HTMLResponse, FileResponse
from pathlib import Path
import uuid
import shutil
import asyncio

# Nuestra lógica de negocio
from procesar_pdf import process_pdf

# ──────────────── Rutas base ────────────────
BASE_DIR      = Path(__file__).parent.resolve()
TEMPLATES_DIR = BASE_DIR / "templates"           # index.html está aquí
RUNS_DIR      = BASE_DIR / "runs"                # trabajos temporales
RUNS_DIR.mkdir(exist_ok=True)

# Cargamos la plantilla HTML una sola vez
INDEX_HTML = (TEMPLATES_DIR / "index.html").read_text(encoding="utf-8")

# ──────────────── App FastAPI ────────────────
app = FastAPI(
    title="Procesador de Informes PDF → Excel",
    version="1.0.0",
    docs_url="/docs",          # Swagger UI (opcional)
    redoc_url=None,
)

# ────────── Tarea de limpieza diferida ──────────
async def _cleanup_later(path: Path, delay: int = 60 * 30) -> None:
    """Borra `path` pasado `delay` segundos."""
    await asyncio.sleep(delay)
    shutil.rmtree(path, ignore_errors=True)

# ──────────────── Endpoints ────────────────
@app.get("/", response_class=HTMLResponse)
async def index():
    """Muestra el formulario de subida."""
    return INDEX_HTML


@app.post("/procesar")
async def procesar_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="Archivo PDF"),
):
    # 1) Directorio aislado
    run_id  = uuid.uuid4().hex
    workdir = RUNS_DIR / run_id
    workdir.mkdir(parents=True, exist_ok=True)

    # 2) Guardar PDF
    pdf_path = workdir / "upload.pdf"
    with pdf_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # 3) Procesar
    try:
        consolidado = process_pdf(pdf_path, workdir)
    except Exception as err:
        shutil.rmtree(workdir, ignore_errors=True)
        # ⚠️ Devolvemos error 500 para que el front no baje un .xlsx inválido
        raise HTTPException(status_code=500, detail=str(err))

    # 4) Programar limpieza en background
    background_tasks.add_task(_cleanup_later, workdir)

    # 5) Responder archivo válido
    return FileResponse(
        consolidado,
        media_type=(
            "application/"
            "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        filename="consolidado.xlsx",
    )

# ──────────────── Ejecución directa ────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,         # auto-recarga en desarrollo
    )
# ───────────── Guardar feedback ─────────────
FEEDBACK_FILE = BASE_DIR / "feedback.csv"

@app.post("/feedback")
async def save_feedback(payload: dict):
    """Guarda la calificación (1–5) con timestamp."""
    rating = int(payload.get("rating", 0))
    if rating not in {1, 2, 3, 4, 5}:
        raise HTTPException(status_code=400, detail="Valor fuera de rango")
    line = f"{rating},{asyncio.get_event_loop().time()}\n"
    with FEEDBACK_FILE.open("a", encoding="utf-8") as f:
        f.write(line)
    return {"ok": True}
