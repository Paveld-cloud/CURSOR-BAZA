from flask import Flask, jsonify, request, send_from_directory
import pandas as pd
import os
import logging

# ---------------------------
# CONFIG
# ---------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

EXCEL_PATH = os.getenv("EXCEL_PATH", "data.xlsx")
SHEET_NAME = os.getenv("SHEET_NAME", "Лист1")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webapp")

app = Flask(__name__, static_folder=STATIC_DIR)


# ---------------------------
# DATA LOAD
# ---------------------------
def load_data():
    logger.info("Loading Excel data...")
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    df.columns = [c.strip() for c in df.columns]
    return df


try:
    DF = load_data()
except Exception as e:
    logger.exception("Failed to load Excel")
    DF = pd.DataFrame()


# ---------------------------
# HELPERS
# ---------------------------
def normalize(val):
    return str(val).strip().upper()


def extract_image(row):
    """
    ЖЁСТКОЕ ПРАВИЛО:
    Фото возвращается ТОЛЬКО если КОД содержится в имени файла ссылки image
    """
    code = normalize(row.get("Код", ""))
    image_raw = str(row.get("image", "")).strip()

    if not code or not image_raw:
        return ""

    if code in image_raw.upper():
        return image_raw

    return ""


# ---------------------------
# API: SEARCH
# ---------------------------
@app.route("/api/search")
def api_search():
    query = normalize(request.args.get("q", ""))

    if not query:
        return jsonify([])

    results = []

    for _, row in DF.iterrows():
        code = normalize(row.get("Код", ""))
        name = str(row.get("НАИМЕНОВАНИЕ", "")).strip()

        if query not in code and query not in name.upper():
            continue

        item = {
            "code": code,
            "name": name,
            "type": str(row.get("Тип", "")),
            "part": str(row.get("Парт Номер", "")),
            "oem_part": str(row.get("OEM Парт Номер", "")),
            "qty": row.get("КОЛИЧЕСТВО", ""),
            "price": row.get("Цена", ""),
            "currency": row.get("Валюта", ""),
            "oem": str(row.get("OEM", "")),
            "image": extract_image(row)
        }

        results.append(item)

    return jsonify(results)


# ---------------------------
# WEB APP PAGES
# ---------------------------
@app.route("/app")
def web_app():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/item")
def item_page():
    return send_from_directory(STATIC_DIR, "item.html")


# ---------------------------
# STATIC
# ---------------------------
@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory(STATIC_DIR, path)


# ---------------------------
# HEALTH
# ---------------------------
@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)


