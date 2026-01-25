import asyncio
import logging
import re
from pathlib import Path

from aiohttp import web
import pandas as pd

import app.data as data

logger = logging.getLogger("webapp")

# Твоя структура:
# app/
#   web/
#     index.html
#     item.html
#     static/
#       app.js, item.js, style.css
WEB_DIR = Path(__file__).resolve().parent / "web"
STATIC_DIR = WEB_DIR / "static"


# ----------------------------
# helpers
# ----------------------------
def _basename_no_query(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
    u = re.sub(r"[?#].*$", "", u)
    return u.rsplit("/", 1)[-1]


def _code_in_filename_strict(code: str, image_url: str) -> bool:
    """
    ЖЁСТКОЕ ПРАВИЛО:
    Фото валидно только если КОД присутствует в ИМЕНИ ФАЙЛА ссылки image.
    Пример: .../UZ000664.jpg -> OK
            .../abc.jpg      -> NO
    """
    code_u = str(code or "").strip().upper()
    if not code_u:
        return False
    fname = _basename_no_query(image_url).upper()
    return code_u in fname


def _s(x) -> str:
    return str(x or "").strip()


def _row_to_item(row: dict) -> dict:
    code = _s(row.get("код", "")).upper()
    img_raw = _s(row.get("image", ""))

    # строго по коду в имени файла
    image = img_raw if (img_raw and _code_in_filename_strict(code, img_raw)) else ""

    return {
        "code": code,
        "name": _s(row.get("наименование", "")),
        "type": _s(row.get("тип", "")),
        "part": _s(row.get("парт номер", "")),
        "oem_part": _s(row.get("oem парт номер", "")),
        "qty": _s(row.get("количество", "")),
        "price": _s(row.get("цена", "")),
        "currency": _s(row.get("валюта", "")),
        "oem": _s(row.get("oem", "")),
        "image": image,
    }


async def _ensure_df():
    if getattr(data, "df", None) is None:
        await asyncio.to_thread(data.ensure_fresh_data, True)


# ----------------------------
# API
# ----------------------------
async def api_search(request: web.Request):
    q = (request.query.get("q") or "").strip()
    if not q:
        return web.json_response([])

    await _ensure_df()
    df_ = data.df
    if df_ is None or df_.empty:
        return web.json_response([])

    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data._norm_code(q)

    matched = set()

    # 1) быстрый индекс
    try:
        keys = [norm_code] if norm_code else tokens
        matched = set(data.match_row_by_index(keys))
    except Exception:
        matched = set()

    # 2) фолбэк AND по токенам
    if not matched:
        mask_any = pd.Series(False, index=df_.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель", "парт номер", "oem парт номер"]:
            series = data._safe_col(df_, col)
            if series is None:
                continue
            field_mask = pd.Series(True, index=df_.index)
            for t in tokens:
                if t:
                    field_mask &= series.str.contains(re.escape(t), na=False)
            mask_any |= field_mask
        matched = set(df_.index[mask_any])

    # 3) фолбэк по склеенной строке
    if not matched and q_squash:
        mask_any = pd.Series(False, index=df_.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель", "парт номер", "oem парт номер"]:
            series = data._safe_col(df_, col)
            if series is None:
                continue
            series_sq = series.str.replace(r"[\W_]+", "", regex=True)
            mask_any |= series_sq.str.contains(re.escape(q_squash), na=False)
        matched = set(df_.index[mask_any])

    if not matched:
        return web.json_response([])

    results_df = df_.loc[list(matched)].copy()

    # сортировка по релевантности (как в боте)
    scores = []
    for _, r in results_df.iterrows():
        scores.append(data._relevance_score(r.to_dict(), tokens + ([norm_code] if norm_code else []), q_squash))
    results_df["__score"] = scores

    if "код" in results_df.columns:
        results_df = results_df.sort_values(by=["__score", "код"], ascending=[False, True])
    else:
        results_df = results_df.sort_values(by=["__score"], ascending=False)

    results_df = results_df.drop(columns="__score", errors="ignore")

    out = [_row_to_item(r.to_dict()) for _, r in results_df.iterrows()]
    return web.json_response(out)


async def api_item(request: web.Request):
    code = (request.query.get("code") or "").strip().upper()
    if not code:
        return web.json_response({"error": "no_code"}, status=400)

    await _ensure_df()
    df_ = data.df
    if df_ is None or df_.empty or "код" not in df_.columns:
        return web.json_response({"error": "no_data"}, status=500)

    hit = df_[df_["код"].astype(str).str.upper() == code]
    if hit.empty:
        return web.json_response({"error": "not_found"}, status=404)

    return web.json_response(_row_to_item(hit.iloc[0].to_dict()))


# ----------------------------
# Pages / static (с отключением кэша)
# ----------------------------
def _no_cache_headers(resp: web.StreamResponse):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


async def page_app(request: web.Request):
    p = WEB_DIR / "index.html"
    if not p.exists():
        logger.error("Missing index.html at %s", p)
        return web.Response(status=404, text="index.html not found")
    resp = web.FileResponse(p)
    return _no_cache_headers(resp)


async def page_item(request: web.Request):
    p = WEB_DIR / "item.html"
    if not p.exists():
        logger.error("Missing item.html at %s", p)
        return web.Response(status=404, text="item.html not found")
    resp = web.FileResponse(p)
    return _no_cache_headers(resp)


async def static_file(request: web.Request):
    rel = request.match_info.get("path", "")
    p = (STATIC_DIR / rel).resolve()

    if not str(p).startswith(str(STATIC_DIR.resolve())):
        return web.Response(status=403)

    if not p.exists() or not p.is_file():
        return web.Response(status=404)

    resp = web.FileResponse(p)
    return _no_cache_headers(resp)


# ----------------------------
# factory
# ----------------------------
def build_web_app() -> web.Application:
    app = web.Application()

    app.router.add_get("/app", page_app)
    app.router.add_get("/item", page_item)

    app.router.add_get("/static/{path:.*}", static_file)

    app.router.add_get("/api/search", api_search)
    app.router.add_get("/api/item", api_item)

    return app

