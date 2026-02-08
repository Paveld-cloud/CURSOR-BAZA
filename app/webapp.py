import logging
import json
import aiohttp
from aiohttp import web
from urllib.parse import parse_qs

from app import data

logger = logging.getLogger("bot.webapp")


# ======================================================
#  UTILS
# ======================================================

def ok(data_dict=None):
    return web.json_response({"ok": True, **(data_dict or {})})

def err(msg):
    return web.json_response({"ok": False, "error": msg})


# ======================================================
#  HTML / STATIC
# ======================================================

async def index(request):
    raise web.HTTPFound("/app")


async def static_app(request):
    return web.FileResponse("./app/static/app.html")


async def static_item(request):
    return web.FileResponse("./app/static/item.html")


# ======================================================
#  API SEARCH
# ======================================================

def _search_rows(q: str):
    """Логика поиска — идентична боту."""
    q = q.strip()
    if not q:
        return []

    # нормализация для поиска
    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data.norm_code(q)

    # загружаем свежую БД
    data.ensure_fresh_data()

    df = data.df
    if df is None or df.empty:
        return []

    hits = set()

    # 1) точное совпадение кода (приоритет)
    if norm_code:
        for i, row in df.iterrows():
            if data.norm_code(row.get("код", "")) == norm_code:
                hits.add(i)

    # 2) парсинг токенов через индекс
    if tokens:
        idx_hits = data.match_row_by_index(tokens)
        hits |= idx_hits

    if not hits:
        return []

    # сортировка по relevancy
    scored = []
    for i in hits:
        row = df.iloc[i].to_dict()
        score = data.relevance_score(row, tokens, q_squash)
        scored.append((score, row))

    scored.sort(key=lambda x: x[0], reverse=True)

    # готовим список
    out = []
    for _, row in scored:
        # ищем корректную картинку
        code = row.get("код", "")
        img = row.get("image", "")
        # асинхронное разрешение в later stage
        out.append({
            **row,
            "image_url": img,
        })

    return out


async def api_search(request):
    """GET /app/api/search?q=..."""
    try:
        q = request.rel_url.query.get("q", "").strip()
        user_id = request.rel_url.query.get("uid", "0")

        rows = _search_rows(q)

        return web.json_response({
            "ok": True,
            "q": q,
            "user_id": user_id,
            "count": len(rows),
            "items": rows,
        })

    except Exception as e:
        logger.exception("api_search failed")
        return web.json_response({"ok": False, "error": "search-failed"})


# ======================================================
#  API ITEM (детальная карточка)
# ======================================================

async def api_item(request):
    """GET /app/api/item?code=..."""
    try:
        code = request.rel_url.query.get("code", "").strip()
        data.ensure_fresh_data()

        df = data.df
        if df is None or df.empty:
            return err("empty-db")

        norm = data.norm_code(code)

        for _, row in df.iterrows():
            if data.norm_code(row.get("код", "")) == norm:
                row = row.to_dict()
                return ok({"item": row})

        return err("not-found")

    except Exception as e:
        logger.exception("api_item failed")
        return err("item-failed")


# ======================================================
#  REGISTER ROUTES
# ======================================================

def setup(app):
    app.router.add_get("/", index)

    # HTML страницы Mini-App
    app.router.add_get("/app", static_app)
    app.router.add_get("/item", static_item)

    # API
    app.router.add_get("/app/api/search", api_search)
    app.router.add_get("/app/api/item", api_item)

    # статика
    app.router.add_static("/static/", path="./app/static", name="static")
