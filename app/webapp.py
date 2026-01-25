import logging
from pathlib import Path
from aiohttp import web

import app.data as data

logger = logging.getLogger("bot.webapp")

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"


# ---------- HTML pages ----------

async def page_index(request: web.Request):
    return web.FileResponse(WEB_DIR / "index.html")


async def page_item(request: web.Request):
    return web.FileResponse(WEB_DIR / "item.html")


# ---------- API ----------

async def api_health(request: web.Request):
    return web.json_response({
        "ok": True,
        "service": "BAZA MG Mini App",
    })

async def api_search(request: web.Request):
    q = (request.query.get("q") or "").strip()
    user_id = request.query.get("user_id", "0")

    if not q:
        return web.json_response({"ok": False, "error": "Пустой запрос"}, status=400)

    # гарантируем загрузку данных
    if data.df is None:
        data.ensure_fresh_data(force=True)

    df_ = data.df
    if df_ is None:
        return web.json_response({"ok": False, "error": "Данные не загружены"}, status=500)

    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data._norm_code(q)

    # 1) строгий поиск по коду
    if norm_code:
        matched = data.match_row_by_index([norm_code])
    else:
        matched = data.match_row_by_index(tokens)

    # 2) fallback AND/OR
    if not matched:
        import pandas as pd
        mask_any = pd.Series(False, index=df_.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель"]:
            series = data._safe_col(df_, col)
            if series is None:
                continue
            field_mask = pd.Series(True, index=df_.index)
            for t in tokens:
                if t:
                    field_mask &= series.str.contains(t, na=False)
            mask_any |= field_mask
        matched = set(df_.index[mask_any])

    # 3) фразовый fallback
    if not matched and q_squash:
        import pandas as pd
        mask_any = pd.Series(False, index=df_.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель"]:
            series = data._safe_col(df_, col)
            if series is None:
                continue
            sq = series.str.replace(r"[\W_]+", "", regex=True)
            mask_any |= sq.str.contains(q_squash, na=False)
        matched = set(df_.index[mask_any])

    if not matched:
        return web.json_response({
            "ok": True,
            "items": [],
            "count": 0
        })

    results = df_.loc[list(matched)].copy()

    items = []
    for _, row in results.iterrows():
        d = row.to_dict()
        items.append(d)

    return web.json_response({
        "ok": True,
        "q": q,
        "user_id": user_id,
        "count": len(items),
        "items": items
    })

async def api_item(request: web.Request):
    code = (request.query.get("code") or "").strip().lower()

    if not code:
        return web.json_response({"ok": False, "error": "Код не передан"}, status=400)

    df = data.df
    if df is None:
        data.ensure_fresh_data(force=True)
        df = data.df

    if df is None or "код" not in df.columns:
        return web.json_response({"ok": False, "error": "Данные недоступны"}, status=500)

    hit = df[df["код"].astype(str).str.lower() == code]
    if hit.empty:
        return web.json_response({"ok": False, "error": "Деталь не найдена"}, status=404)

    row = hit.iloc[0].to_dict()

    image_url = None
    raw = data.find_image_by_code(code)
    if raw:
        image_url = data.resolve_image_url(raw)

    return web.json_response({
        "ok": True,
        "row": row,
        "card_html": data.format_row(row),
        "image_url": image_url,
    })


async def api_issue(request: web.Request):
    payload = await request.json()

    code = payload.get("code", "").strip().lower()
    qty = payload.get("qty")
    comment = payload.get("comment", "")
    user_id = payload.get("user_id", 0)
    name = payload.get("name", "")

    if not code or not qty:
        return web.json_response({"ok": False, "error": "Некорректные данные"}, status=400)

    df = data.df
    hit = df[df["код"].astype(str).str.lower() == code]
    if hit.empty:
        return web.json_response({"ok": False, "error": "Деталь не найдена"}, status=404)

    part = hit.iloc[0].to_dict()

    await data.save_issue_async(
        user_id=user_id,
        name=name,
        part=part,
        quantity=qty,
        comment=comment,
    )

    return web.json_response({"ok": True})


# ---------- APP ----------

def build_web_app() -> web.Application:
    app = web.Application()

    # Pages
    app.router.add_get("/app", page_index)
    app.router.add_get("/app/", page_index)
    app.router.add_get("/item", page_item)

    # API
    app.router.add_get("/api/health", api_health)
    app.router.add_get("/api/search", api_search)
    app.router.add_get("/api/item", api_item)
    app.router.add_post("/api/issue", api_issue)

    # Static
    app.router.add_static(
        "/static/",
        path=str(WEB_DIR / "static"),
        show_index=False,
    )

    logger.info("Mini App mounted at /app")
    return app
