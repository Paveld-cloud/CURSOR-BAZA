from flask import Flask, jsonify, request, send_from_directory
import pandas as pd
import os
import logging


logger = logging.getLogger("webapp")


def build_web_app():
    app = Flask(__name__)

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    STATIC_DIR = os.path.join(BASE_DIR, "web", "static")

    EXCEL_PATH = os.getenv("EXCEL_PATH", "data.xlsx")
    SHEET_NAME = os.getenv("SHEET_NAME", "Лист1")

    # ---------- LOAD DATA ----------
    def load_data():
        df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
        df.columns = [c.strip() for c in df.columns]
        return df

    try:
        df = load_data()
        logger.info("Excel loaded")
    except Exception as e:
        logger.exception("Excel load failed")
        df = pd.DataFrame()

    # ---------- HELPERS ----------
    def norm(v):
        return str(v).strip().upper()

    def extract_image(row):
        code = norm(row.get("Код", ""))
        image = str(row.get("image", "")).strip()
        if code and image and code in image.upper():
            return image
        return ""

    # ---------- API ----------
    @app.route("/api/search")
    def api_search():
        q = norm(request.args.get("q", ""))
        if not q:
            return jsonify([])

        result = []

        for _, row in df.iterrows():
            code = norm(row.get("Код", ""))
            name = str(row.get("НАИМЕНОВАНИЕ", "")).strip()

            if q not in code and q not in name.upper():
                continue

            result.append({
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
            })

        return jsonify(result)

    # ---------- WEB ----------
    @app.route("/app")
    def web_app():
        return send_from_directory(STATIC_DIR, "index.html")

    @app.route("/item")
    def item_page():
        return send_from_directory(STATIC_DIR, "item.html")

    @app.route("/static/<path:path>")
    def static_files(path):
        return send_from_directory(STATIC_DIR, path)

    return app
