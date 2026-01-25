import logging
import re
from pathlib import Path
from aiohttp import web

import pandas as pd

import app.data as data  # используем ту же базу/поиск, что и бот

logger = logging.getLogger("bot.webapp")

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"


async def page_index(request: web.Request):
    return web.FileResponse(WEB_DIR / "index.html")


async def page_item(request: web.Request):
    return web.FileResponse(WEB_DIR / "item.html")


async def api_health(request: web.Request):
    return web.json_response({"ok": True, "service": "BAZA MG Mini App", "path": "/app"})


def _run_search(q: str, limit: int = 25) -> list[dict]:
    q = (q or "").strip()
    if not q:
        return []

    # гарантируем актуальные данные
    data.ensure_fresh_data()

    if data.df is None or data.df.empty:
        return []

    df_ = data.df

    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data._norm_code(q)

    # 1) Индекс (строго по нормализованному коду / токенам)
    if norm_code:
        matched_indices = data.match_row_by_index([norm_code])
    else:
        matched_indices = data.match_row_by_index(tokens)

    # 2) Фолбэк: AND внутри поля, OR по полям
    if not matched_indices:
        mask_any = pd.Series(False, index=df_.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель"]:
            series = data._safe_col(df_, col)
            if series is None:
                continue
            field_mask = pd.Series(True, index=df_.index)
            for t in tokens:
                if t:
                    field_mask &= series.str.contains(re.escape(t), na=False)
            mask_any |= field_mask
        matched_indices = set(df_.index[mask_any])

    # 3) Фразовый поиск по склеенным полям
    if not matched_indices and q_squash:
        mask_any = pd.Series(False, index=df_.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель"]:
            series = data._safe_col(df_, col)
            if series is None:
                continue
            series_sq = series.str.replace(r"[\W_]+", "", regex=True)
            mask_any |= series_sq.str.contains(re.escape(q_squash), na=False)
        matched_indices = set(df_.index[mask_any])

    if not matched_indices:
        return []

    results_df = df_.loc[list(matched_indices)].copy()

    # релевантность (как в боте)
    scores = []
    for _, r in results_df.iterrows():
        scores.append(
            data._relevance_score(
                r.to_dict(),
                tokens + ([norm_code] if norm_code else []),
                q_squash,
            )
        )
    results_df["__score"] = scores

    if "код" in results_df.columns:
        results_df = results_df.sort_values(
            by=["__score", "код"],
            ascending=[False, True],
            key=lambda s: s if s.name != "код" else s.astype(str).str.len(),
        )
    else:
        results_df = results_df.sort_values(by=["__score"], ascending=False)

    results_df = results_df.drop(columns="__score").head(limit)

    # отдаём JSON: row + готовый html-текст карточки (как в боте)
    out = []
    for _, r in results_df.iterrows():
        row = r.to_dict()
        out.append(
            {
                "row": row,
                "card_html": data.format_row(row),  # компактная верстка
            }
        )
    return out


async def api_search(request: web.Request):
    q = (request.query.get("q") or "").strip()
    # user_id приходит из фронта, сейчас просто принимаем (на будущее для логов/доступа)
    user_id = request.query.get("user_id", "0")

    try:
        items = _run_search(q, limit=25)
        return web.json_response(
            {
                "ok": True,
                "q": q,
                "user_id": user_id,
                "count": len(items),
                "items": items,
            },
            dumps=lambda x: __import__("json").dumps(x, ensure_ascii=False),
        )
    except Exception as e:
        logger.exception("api_search error")
        return web.json_response(
            {"ok": False, "error": str(e)},
            status=500,
            dumps=lambda x: __import__("json").dumps(x, ensure_ascii=False),
        )


def build_web_app() -> web.Application:
    app = web.Application()

    # Pages
    app.router.add_get("/app", page_index)
    app.router.add_get("/app/", page_index)
    app.router.add_get("/app/item", page_item)

    # API (health)
    app.router.add_get("/app/api/health", api_health)

    # API (search) — ВАЖНО: фронт сейчас бьёт в /api/search
    app.router.add_get("/api/search", api_search)         # ✅ то, что просит app.js
    app.router.add_get("/app/api/search", api_search)     # ✅ на будущее/совместимость

    # Static (оставляем оба пути)
    static_dir = WEB_DIR / "static"
    app.router.add_static("/static/", str(static_dir), show_index=False)
    app.router.add_static("/app/static/", str(static_dir), show_index=False)

    logger.info("Mini App mounted at /app (static: /static/* and /app/static/*)")
    return app

