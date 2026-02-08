import logging
from pathlib import Path
from aiohttp import web

import app.data as data

logger = logging.getLogger("bot.webapp")

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"
STATIC_DIR = WEB_DIR / "static"


# ---------------------------------------------------------
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ---------------------------------------------------------

def _search_rows(q: str):
    """
    Поиск — идентичен боту.
    """
    q = (q or "").strip()
    if not q:
        return []

    # нормализация как в боте
    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data.norm_code(q)

    data.ensure_fresh_data()
    df = data.df
    if df is None or df.empty:
        return []

    matched = set()

    # 1) точное совпадение кода
    if norm_code:
        for i, row in df.iterrows():
            if data.norm_code(row.get("код", "")) == norm_code:
                matched.add(i)

    # 2) индексный матч по токенам
    if tokens:
        matched |= data.match_row_by_index(tokens)

    if not matched:
        return []

    # сортировка по релевантности — как в боте
    scored = []
    for i in matched:
        row = df.iloc[i].to_dict()
        score = data.relevance_score(row, tokens, q_squash)
        scored.append((score, row))

    scored.sort(key=lambda x: x[0], reverse=True)

    # добавляем image_url (пока raw — Mini-App сам прогрузит)
    out = []
    for _, row in scored:
        item = dict(row)
        item["image_url"] = item.get("image", "")
        out.append(item)

    return out


# ---------------------------------------------------------
#  PAGES
# ---------------------------------------------------------

async def page_index(request):
    return web.FileResponse(WEB_DIR / "index.html")

async def page_item(request):
    return web.FileResponse(WEB_DIR / "item.html")

async def page_ui_demo(request):
    return web.FileResponse(WEB_DIR / "ui-demo.html")


# ---------------------------------------------------------
#  API
# ---------------------------------------------------------

async def api_search(request: web.Request):
    try:
        q = request.query.get("q", "")
        user_id = request.query.get("user_id", "0")

        items = _search_rows(q)

        return web.json_response({
            "ok": True,
            "q": q,
            "user_id": user_id,
            "count": len(items),
            "items": items
        })
    except Exception as e:
        logger.exception("api_search failed")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_item(request: web.Request):
    try:
        code = request.query.get("code", "").strip()
        if not code:
            return web.json_response({"ok": False, "error": "code required"}, status=400)

        data.ensure_fresh_data()
        df = data.df

        norm = data.norm_code(code)
        hit = df[df["код"].astype(str).str.lower().apply(data.norm_code) == norm]

        if hit.empty:
            return web.json_response({"ok": False, "error": "not found"}, status=404)

        row = hit.iloc[0].to_dict()
        row["image_url"] = row.get("image", "")

        return web.json_response({"ok": True, "item": row})
    except Exception as e:
        logger.exception("api_item failed")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


# ---------------------------------------------------------
#  BUILD WEB APP  ← НУЖНО MAIN.PY !!!
# ---------------------------------------------------------

def build_web_app() -> web.Application:
    app = web.Application()

    # Pages
    app.router.add_get("/app", page_index)
    app.router.add_get("/app/", page_index)
    app.router.add_get("/app/item", page_item)
    app.router.add_get("/app/ui-demo", page_ui_demo)

    # Aliases
    app.router.add_get("/", page_index)
    app.router.add_get("/item", page_item)
    app.router.add_get("/ui-demo", page_ui_demo)

    # API
    app.router.add_get("/app/api/search", api_search)
    app.router.add_get("/app/api/item", api_item)

    # Static
    app.router.add_static("/static/", str(STATIC_DIR), show_index=False)
    app.router.add_static("/app/static/", str(STATIC_DIR), show_index=False)

    logger.info("Mini App mounted with build_web_app()")
    return app

