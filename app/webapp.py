import logging
from pathlib import Path
from aiohttp import web
import re
from typing import List, Set

import pandas as pd

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
def _norm_code_strict(x: str) -> str:
    """
    Нормализация кода:
    UZ000346 -> uz000346
    PI-8808 -> pi8808
    O->0
    """
    s = str(x or "").strip().lower()
    s = s.replace("o", "0")
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def _normalize_text(x: str) -> str:
    # для токенизации поиска
    return re.sub(r"[^\w\s]", " ", str(x or "").lower(), flags=re.U).strip()


def _squash(x: str) -> str:
    # “склеенный” фолбэк (без пробелов и символов)
    return re.sub(r"[\W_]+", "", str(x or "").lower(), flags=re.U)


def _ensure_loaded() -> bool:
    try:
        if getattr(data, "df", None) is None:
            data.ensure_fresh_data(force=True)
        return getattr(data, "df", None) is not None
    except Exception as e:
        logger.exception(f"data load failed: {e}")
        return False


def _safe_series(df_: pd.DataFrame, col: str) -> pd.Series:
    if col not in df_.columns:
        return pd.Series(["" for _ in range(len(df_))], index=df_.index, dtype="string")
    s = df_[col].astype(str)
    return s.fillna("").astype("string")


async def _resolve_image_for_code(code: str, row: dict | None = None) -> str:
    """
    Строго под твой SAP-лист:
    1) СНАЧАЛА берём ссылку из строки: row['image'] (колонка K).
    2) Если пусто — fallback: data.find_image_by_code_async(code) (индекс код->image).
    3) Приводим к прямой ссылке через data.resolve_image_url_async (ibb/drive и т.п.).
    """
    code_key = _norm_code_strict(code)
    if not code_key:
        return ""

    raw = ""
    if row:
        raw = str(row.get("image") or row.get("image_url") or "").strip()

    if not raw:
        try:
            raw = await data.find_image_by_code_async(code_key)
        except Exception as e:
            logger.warning(f"[image] find_image_by_code_async failed for {code_key}: {e}")
            raw = ""

    if not raw:
        return ""

    try:
        return (await data.resolve_image_url_async(raw)) or ""
    except Exception as e:
        logger.warning(f"[image] resolve_image_url_async failed for {code_key}: {e}")
        return ""


async def _row_public(row: dict) -> dict:
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

        # и сырьё, и итоговый url
        "image": str(row.get("image", "")).strip(),
        "image_url": image_url,
    }


def _search_rows(query: str) -> List[dict]:
    """
    Поиск:
    1) data.match_row_by_index (быстро)
    2) fallback по “склеенному” по ВСЕМ колонкам (кроме image)
    """
    q = (query or "").strip()
    if not q:
        return []

    if not _ensure_loaded():
        return []

    df_ = data.df

    tokens = _normalize_text(q).split()
    q_squash = _squash(q)
    norm_code = _norm_code_strict(q)

    matched: Set[int] = set()

    # 1) индексный поиск
    try:
        if norm_code:
            matched = set(data.match_row_by_index([norm_code]))
        else:
            matched = set(data.match_row_by_index(tokens))
    except Exception:
        matched = set()

    # 2) fallback по “склеенному”
    if not matched and q_squash:
        try:
            mask_any = pd.Series(False, index=df_.index)
            cols = [c for c in df_.columns if str(c).strip().lower() != "image"]
            for col in cols:
                series = _safe_series(df_, col)
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
        items = [await _row_public(r) for r in rows]
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
    code_in = request.query.get("code", "")
    code_key = _norm_code_strict(code_in)
    if not code_key:
        return web.json_response({"ok": False, "error": "code is required"}, status=400)

    if not _ensure_loaded():
        return web.json_response({"ok": False, "error": "data not loaded"}, status=500)

    try:
        if "код" not in data.df.columns:
            return web.json_response({"ok": False, "error": "column 'код' missing"}, status=500)

        hit = data.df[data.df["код"].astype(str).map(_norm_code_strict) == code_key]
        if hit.empty:
            return web.json_response({"ok": False, "error": "not found"}, status=404)

        row = hit.iloc[0].to_dict()
        item = await _row_public(row)

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
    code_key = _norm_code_strict(payload.get("code"))
    qty = str(payload.get("qty") or "").strip()
    comment = str(payload.get("comment") or "").strip()

    if not code_key:
        return web.json_response({"ok": False, "error": "code is required"}, status=400)
    if not qty:
        return web.json_response({"ok": False, "error": "qty is required"}, status=400)

    if not _ensure_loaded():
        return web.json_response({"ok": False, "error": "data not loaded"}, status=500)

    # найдём деталь по коду
    part = None
    try:
        if "код" in data.df.columns:
            hit = data.df[data.df["код"].astype(str).map(_norm_code_strict) == code_key]
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

    # Aliases
    app.router.add_get("/", page_index)
    app.router.add_get("/item", page_item)

    # Health
    app.router.add_get("/app/api/health", api_health)
    app.router.add_get("/api/health", api_health)

    # API
    app.router.add_get("/app/api/search", api_search)
    app.router.add_get("/api/search", api_search)

    app.router.add_get("/app/api/item", api_item)
    app.router.add_get("/api/item", api_item)

    app.router.add_post("/app/api/issue", api_issue)
    app.router.add_post("/api/issue", api_issue)

    # Static
    app.router.add_static("/static/", str(STATIC_DIR), show_index=False)
    app.router.add_static("/app/static/", str(STATIC_DIR), show_index=False)

    logger.info("Mini App mounted at /app (static: /static/* and /app/static/*)")
    return app
