import os
from flask import Flask, render_template, jsonify, request
import requests as req

from db import (
    init_db, get_session, ELECTION_NAMES,
    stats_overview, stats_parties,
    stats_participation_buckets, stats_acta_status, stats_elecciones,
)
import scraper

app = Flask(__name__)

ONPE_BASE = "https://resultadoelectoral.onpe.gob.pe"
ONPE_API  = f"{ONPE_BASE}/presentacion-backend"

_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": f"{ONPE_BASE}/",
}
_http = req.Session()
_session_ready = False


def _ensure_session():
    global _session_ready
    if not _session_ready:
        _http.get(ONPE_BASE, headers={"User-Agent": _BROWSER_HEADERS["User-Agent"]}, timeout=10)
        _session_ready = True


def onpe_get(path, params=None):
    _ensure_session()
    resp = _http.get(f"{ONPE_API}/{path}", params=params, headers=_BROWSER_HEADERS, timeout=15)
    text = resp.text.strip()
    if not text or not text.startswith("{"):
        raise ValueError("Respuesta vacía del servidor ONPE")
    return resp.json()


# ── Pages ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/stats")
def stats_page():
    return render_template("stats.html")


# ── Mesa lookup ──────────────────────────────────────────────────────────────

@app.route("/api/mesa")
def get_mesa():
    codigo = request.args.get("codigoMesa", "").strip().zfill(6)
    if not codigo or not codigo.isdigit():
        return jsonify({"error": "Código de mesa inválido"}), 400
    try:
        data = onpe_get("actas/buscar/mesa", {"codigoMesa": codigo})
        if not data.get("success") or not data.get("data"):
            return jsonify({"error": "Mesa no encontrada o sin resultados"}), 404
        for mesa in data["data"]:
            eid = mesa.get("idEleccion")
            mesa["nombreEleccion"] = ELECTION_NAMES.get(eid, f"Elección {eid}")
        return jsonify(data)
    except Exception as e:
        global _session_ready
        _session_ready = False
        return jsonify({"error": f"Error al consultar ONPE: {e}"}), 500


# ── Scraper control ──────────────────────────────────────────────────────────

@app.route("/api/scraper/start", methods=["POST"])
def scraper_start():
    if scraper.is_running():
        return jsonify({"error": "El scraper ya está en ejecución"}), 409
    body = request.get_json(silent=True) or {}
    start = int(body.get("start", 1))
    end   = int(body.get("end",   89999))
    workers = int(body.get("workers", 20))
    scraper.start(start=start, end=end, workers=workers)
    return jsonify({"ok": True, "start": start, "end": end, "workers": workers})


@app.route("/api/scraper/stop", methods=["POST"])
def scraper_stop():
    scraper.stop()
    return jsonify({"ok": True})


@app.route("/api/scraper/status")
def scraper_status():
    ov = stats_overview()
    return jsonify({**ov["scraper"], "running": scraper.is_running()})


# ── Statistics ───────────────────────────────────────────────────────────────

@app.route("/api/stats/overview")
def api_overview():
    return jsonify(stats_overview())


@app.route("/api/stats/parties")
def api_parties():
    eid   = int(request.args.get("eleccion", 10))
    limit = int(request.args.get("limit", 30))
    return jsonify(stats_parties(eid, limit))


@app.route("/api/stats/participation")
def api_participation():
    return jsonify(stats_participation_buckets())


@app.route("/api/stats/acta_status")
def api_acta_status():
    return jsonify(stats_acta_status())


@app.route("/api/stats/elecciones")
def api_elecciones():
    return jsonify(stats_elecciones())


# ── Bootstrap ────────────────────────────────────────────────────────────────

init_db()

if __name__ == "__main__":
    app.run(debug=True, port=5000)
