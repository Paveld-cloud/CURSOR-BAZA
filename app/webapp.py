import logging
from pathlib import Path
from aiohttp import web

import app.data as data

logger = logging.getLogger("bot.webapp")

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"
STATIC_DIR = WEB_DIR / "static"


# ---------------- Pages ----------------
async def page_index(request: web.Request):
    return web.FileResponse(WEB_DIR / "index.html")


async def page_item(request: web.Request):
    return web.FileResponse(WEB_DIR / "item.html")


async def api_health(request: web.Request):
    return web.json_response({"ok": True, "service": "BAZA MG Mini App"})


# ---------------- Helpers ----------------
def _norm_code(s: str) -> str:
    return str(s or "").strip().lower()


async def _resolve_image_for_code(code: str, row: dict | None = None) -> str:
    """
    Возвращает ГОТОВЫЙ URL картинки.
    Логика как в боте:
    1) если в строке есть image/image_url — пробуем резолв
    2) если нет — ищем по коду через find_image_by_code_async
    3) затем resolve_image_url_async
    """
    code = _norm_code(code)
    if not code:
        return ""

    raw = ""
    if row:
        raw = str(row.get("image_url") or row.get("image") or "").strip()

    # если в строке пусто — ищем по индексу картинок
    if not raw:
        try:
            raw = await data.find_image_by_code_async(code)
        except Exception as e:
            logger.warning(f"[image] find_image_by_code_async failed for {code}: {e}")
            raw = ""

    if not raw:
        return ""

    # резолв (диск/drive/телеграм file_id/прямая ссылка — как у тебя в data.py)
    try:
        url = await data.resolve_image_url_async(raw)
        return url or ""
    except Exception as e:
        logger.warning(f"[image] resolve_image_url_async failed for {code}: {e}")
        return ""


async def _row_public(row: dict) -> dict:
    """
    Что отдаём фронту. Добавляем image_url уже готовый.
    """
    code = str(row.get("код", "")).strip()
    image_url = await _resolve_image_for_code(code, row=row)

    return {
        "код": code,
        "наименование": str(row.get("наименование", "")).strip(),
        "изготовитель": str(row.get("изготовитель", "")).strip(),
        "парт номер": str(row.get("парт номер", "")).strip(),
        "oem парт номер": str(row.get("oem парт номер", "")).strip(),
        "тип": str(row.get("тип", "")).strip(),
        "количество": str(row.get("количество", "")).strip(),
        "цена": str(row.get("цена", "")).strip(),
        "валюта": str(row.get("валюта", "")).strip(),
        "oem": str(row.get("oem", "")).strip(),

        # важно: отдаём и raw, и итоговый url
        "image": str(row.get("image", "")).strip(),
        "image_url": image_url,
    }


def _ensure_loaded():
    if data.df is None:
        data.ensure_fresh_data(force=True)
    return data.df is not None


def _search_rows(query: str):
    """
    Поиск через существующую логику data.py (индексы/нормализация).
    """
    q = (query or "").strip()
    if not q:
        return []

    if not _ensure_loaded():
        return []

    df_ = data.df

    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data._norm_code(q)

    matched = set()

    # 1) индексный поиск
    try:
        if norm_code:
            matched = set(data.match_row_by_index([norm_code]))
        else:
            matched = set(data.match_row_by_index(tokens))
    except Exception:
        matched = set()

    # 2) фолбэк по “склеенному”
    if not matched and q_squash:
        try:
            import re
            import pandas as pd

            mask_any = pd.Series(False, index=df_.index)
            for col in ["тип", "наименование", "код", "oem", "изготовитель", "парт номер", "oem парт номер"]:
                series = data._safe_col(df_, col)
                if series is None:
                    continue
                series_sq = series.str.replace(r"[\W_]+", "", regex=True)
                mask_any |= series_sq.str.contains(re.escape(q_squash), na=False)

            matched = set(df_.index[mask_any])
        except Exception:
            matched = set()

    if not matched:
        return []

    try:
        return [r.to_dict() for _, r in df_.loc[list(matched)].iterrows()]
    except Exception:
        return []


# ---------------- API ----------------
async def api_search(request: web.Request):
    q = request.query.get("q", "").strip()
    user_id = request.query.get("user_id", "0")

    try:
        rows = _search_rows(q)
        # ВАЖНО: обогащаем строки image_url по коду
        items = []
        for r in rows:
            items.append(await _row_public(r))

        return web.json_response({
            "ok": True,
            "q": q,
            "user_id": str(user_id),
            "count": len(items),
            "items": items
        })
    except Exception as e:
        logger.exception("api_search failed")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_item(request: web.Request):
    """
    Детальная карточка по коду (для страницы /item).
    """
    code = _norm_code(request.query.get("code", ""))
    if not code:
        return web.json_response({"ok": False, "error": "code is required"}, status=400)

    if not _ensure_loaded():
        return web.json_response({"ok": False, "error": "data not loaded"}, status=500)

    try:
        hit = data.df[data.df["код"].astype(str).str.lower() == code] if "код" in data.df.columns else None
        if hit is None or hit.empty:
            return web.json_response({"ok": False, "error": "not found"}, status=404)

        row = hit.iloc[0].to_dict()
        item = await _row_public(row)

        # Добавим текст описания (как форматируешь в боте)
        try:
            item["text"] = data.format_row(row)
        except Exception:
            item["text"] = ""

        return web.json_response({"ok": True, "item": item})
    except Exception as e:
        logger.exception("api_item failed")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_issue(request: web.Request):
    """
    Списание из Mini App -> лист История.
    payload: { user_id, name, code, qty, comment }
    """
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "Bad JSON"}, status=400)

    user_id = int(payload.get("user_id") or 0)
    name = str(payload.get("name") or "").strip()
    code = _norm_code(payload.get("code"))
    qty = str(payload.get("qty") or "").strip()
    comment = str(payload.get("comment") or "").strip()

    if not code:
        return web.json_response({"ok": False, "error": "code is required"}, status=400)
    if not qty:
        return web.json_response({"ok": False, "error": "qty is required"}, status=400)

    if not _ensure_loaded():
        return web.json_response({"ok": False, "error": "data not loaded"}, status=500)

    # найдём деталь по коду
    part = None
    try:
        if "код" in data.df.columns:
            hit = data.df[data.df["код"].astype(str).str.lower() == code]
            if not hit.empty:
                part = hit.iloc[0].to_dict()
    except Exception:
        part = None

    if not part:
        return web.json_response({"ok": False, "error": "part not found by code"}, status=404)

    # пишем в История
    try:
        from app.config import SPREADSHEET_URL
        import gspread

        client = data.get_gs_client()
        sh = client.open_by_url(SPREADSHEET_URL)

        try:
            ws = sh.worksheet("История")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title="История", rows=1000, cols=12)
            ws.append_row(["Дата", "ID", "Имя", "Тип", "Наименование", "Код", "Количество", "Коментарий"])

        ts = data.now_local_str()

        ws.append_row(
            [
                ts,
                str(user_id),
                name or str(user_id),
                str(part.get("тип", "")),
                str(part.get("наименование", "")),
                str(part.get("код", "")),
                str(qty),
                comment,
            ],
            value_input_option="USER_ENTERED",
        )

        return web.json_response({"ok": True})
    except Exception as e:
        logger.exception("api_issue failed")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


# ---------------- Build app ----------------
def build_web_app() -> web.Application:
    app = web.Application()

    # Pages
    app.router.add_get("/app", page_index)
    app.router.add_get("/app/", page_index)
    app.router.add_get("/app/item", page_item)

    # Алиасы (удобно для браузера)
    app.router.add_get("/", page_index)
    app.router.add_get("/item", page_item)

    # Health
    app.router.add_get("/app/api/health", api_health)
    app.router.add_get("/api/health", api_health)

    # API (двойные пути)
    app.router.add_get("/app/api/search", api_search)
    app.router.add_get("/api/search", api_search)

    app.router.add_get("/app/api/item", api_item)
    app.router.add_get("/api/item", api_item)

    app.router.add_post("/app/api/issue", api_issue)
    app.router.add_post("/api/issue", api_issue)

    # Static (двойные пути)
    app.router.add_static("/static/", str(STATIC_DIR), show_index=False)
    app.router.add_static("/app/static/", str(STATIC_DIR), show_index=False)

    logger.info("Mini App mounted at /app (static: /static/* and /app/static/*)")
    return app



