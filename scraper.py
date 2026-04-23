import threading
import time
import requests as req
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from db import engine, Mesa, Resultado, ScraperState, ELECTION_NAMES

ONPE_BASE = "https://resultadoelectoral.onpe.gob.pe"
ONPE_API  = f"{ONPE_BASE}/presentacion-backend"

_stop_event = threading.Event()
_scraper_thread = None
_session_lock = threading.Lock()
_http = req.Session()
_session_ready = False


def _ensure_session():
    global _session_ready
    with _session_lock:
        if not _session_ready:
            _http.get(ONPE_BASE, headers={"User-Agent": _UA}, timeout=10)
            _session_ready = True


_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
_HEADERS = {
    "User-Agent": _UA,
    "Accept": "application/json, text/plain, */*",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": f"{ONPE_BASE}/",
}


def fetch_mesa(codigo: str):
    """Returns list of election dicts for this mesa, or None if not found."""
    global _session_ready
    try:
        _ensure_session()
        resp = _http.get(
            f"{ONPE_API}/actas/buscar/mesa",
            params={"codigoMesa": codigo},
            headers=_HEADERS,
            timeout=12,
        )
        text = resp.text.strip()
        if not text or not text.startswith("{"):
            return None
        data = resp.json()
        return data.get("data") or None
    except Exception:
        with _session_lock:
            _session_ready = False
        return None


def _save_mesa(db: Session, elections: list):
    """Upsert mesa + resultados rows from API response."""
    first = elections[0]
    codigo = first["codigoMesa"]

    mesa = db.query(Mesa).filter_by(codigo_mesa=codigo).first()
    if not mesa:
        mesa = Mesa(codigo_mesa=codigo)
        db.add(mesa)

    mesa.nombre_local        = first.get("nombreLocalVotacion")
    mesa.centro_poblado      = first.get("centroPoblado")
    mesa.id_ubigeo           = first.get("idUbigeo")
    mesa.total_electores     = first.get("totalElectoresHabiles")
    mesa.total_votos_emitidos= first.get("totalVotosEmitidos")
    mesa.total_votos_validos = first.get("totalVotosValidos")
    mesa.pct_participacion   = first.get("porcentajeParticipacionCiudadana")
    mesa.estado_acta         = first.get("descripcionEstadoActa")
    mesa.codigo_estado       = first.get("codigoEstadoActa")
    mesa.scraped_at          = datetime.utcnow()

    # Delete old resultados for this mesa (clean upsert)
    db.query(Resultado).filter_by(codigo_mesa=codigo).delete()

    for election in elections:
        eid   = election.get("idEleccion")
        ename = ELECTION_NAMES.get(eid, f"Elección {eid}")
        for p in election.get("detalle", []):
            db.add(Resultado(
                codigo_mesa    = codigo,
                id_eleccion    = eid,
                nombre_eleccion= ename,
                codigo_partido = p.get("adCodigo"),
                nombre_partido = p.get("adDescripcion"),
                votos          = p.get("adVotos") or 0,
                pct_validos    = p.get("adPorcentajeVotosValidos") or 0,
                pct_emitidos   = p.get("adPorcentajeVotosEmitidos") or 0,
                es_partido     = bool(p.get("adGrafico") == 1),
            ))
    db.commit()


def _update_state(db: Session, **kwargs):
    state = db.get(ScraperState, 1)
    for k, v in kwargs.items():
        setattr(state, k, v)
    state.updated_at = datetime.utcnow()
    db.commit()


def _run_scraper(start: int, end: int, workers: int):
    global _session_ready
    _session_ready = False
    _ensure_session()

    with Session(engine) as db:
        _update_state(db, status="running", range_start=start, range_end=end,
                      started_at=datetime.utcnow(), total_scanned=0, total_found=0)

    batch_size = workers * 4
    scanned = 0
    found = 0

    try:
        codes = range(start, end + 1)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            it = iter(codes)
            while not _stop_event.is_set():
                batch = []
                for _ in range(batch_size):
                    c = next(it, None)
                    if c is None:
                        break
                    batch.append(str(c).zfill(6))
                if not batch:
                    break

                futures = {pool.submit(fetch_mesa, c): c for c in batch}
                for future in as_completed(futures):
                    if _stop_event.is_set():
                        break
                    futures[future]
                    scanned += 1
                    result = future.result()
                    if result:
                        found += 1
                        try:
                            with Session(engine) as db:
                                _save_mesa(db, result)
                        except Exception:
                            pass

                if scanned % 500 == 0:
                    with Session(engine) as db:
                        _update_state(db,
                            current_code=int(batch[-1]),
                            total_scanned=scanned,
                            total_found=found,
                        )

        status = "stopped" if _stop_event.is_set() else "done"
    except Exception:
        status = "stopped"
    finally:
        with Session(engine) as db:
            _update_state(db, status=status, total_scanned=scanned, total_found=found)


def start(start=1, end=89999, workers=20):
    global _scraper_thread
    _stop_event.clear()
    _scraper_thread = threading.Thread(
        target=_run_scraper, args=(start, end, workers), daemon=True
    )
    _scraper_thread.start()
    return True


def stop():
    _stop_event.set()
    return True


def is_running():
    return _scraper_thread is not None and _scraper_thread.is_alive()
