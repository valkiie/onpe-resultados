from flask import Flask, render_template, jsonify, request
import requests as req

app = Flask(__name__)

ONPE_BASE = "https://resultadoelectoral.onpe.gob.pe"
ONPE_API  = f"{ONPE_BASE}/presentacion-backend"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": f"{ONPE_BASE}/",
}

_session       = req.Session()
_session_ready = False


def ensure_session():
    global _session_ready
    if not _session_ready:
        _session.get(ONPE_BASE, headers={"User-Agent": BROWSER_HEADERS["User-Agent"]}, timeout=10)
        _session_ready = True


def onpe_get(path, params=None):
    ensure_session()
    resp = _session.get(f"{ONPE_API}/{path}", params=params, headers=BROWSER_HEADERS, timeout=15)
    resp.raise_for_status()
    text = resp.text.strip()
    if not text or not text.startswith("{"):
        raise ValueError("Respuesta vacía del servidor ONPE")
    return resp.json()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/mesa")
def get_mesa():
    codigo = request.args.get("codigoMesa", "").strip().zfill(6)
    if not codigo or not codigo.isdigit():
        return jsonify({"error": "Código de mesa inválido"}), 400

    try:
        data = onpe_get("actas/buscar/mesa", {"codigoMesa": codigo})
        if not data.get("success") or not data.get("data"):
            return jsonify({"error": "Mesa no encontrada o sin resultados"}), 404

        nombres = _get_election_names()
        for mesa in data.get("data", []):
            eid = mesa.get("idEleccion")
            mesa["nombreEleccion"] = nombres.get(eid, f"Elección {eid}")

        return jsonify(data)
    except Exception as e:
        global _session_ready
        _session_ready = False
        return jsonify({"error": f"Error al consultar ONPE: {str(e)}"}), 500


ELECTION_NAMES = {
    10: "Presidencial",
    12: "Parlamento Andino",
    13: "Senadores DEU",
    14: "Senadores DEM",
    15: "Senadores 33",
    20: "Diputados",
}


def _get_election_names() -> dict:
    return ELECTION_NAMES


if __name__ == "__main__":
    app.run(debug=True, port=5000)
