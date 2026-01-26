import asyncio
import html
import logging
import re
from pathlib import Path

from aiohttp import web
import pandas as pd

import app.data as data

logger = logging.getLogger("webapp")

WEB_DIR = Path(__file__).resolve().parent / "web"
STATIC_DIR = WEB_DIR / "static"


def _no_cache_headers(resp: web.StreamResponse) -> web.StreamResponse:
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


async def _ensure_df() -> None:
    if getattr(data, "df", None) is None:
        await asyncio.to_thread(data.ensure_fresh_data, True)


def _s(x) -> str:
    return str(x or "").strip()


def _row_to_card_item(row: dict, image_url: str) -> dict:
    return {
        "code": _s(row.get("код")).upper(),
        "name": _s(row.get("наименование")),
        "type": _s(row.get("тип")),
        "part": _s(row.get("парт номер")),
        "oem_part": _s(row.get("oem парт номер")),
        "qty": _s(row.get("количество")),
        "price": _s(row.get("цена")),
        "currency": _s(row.get("валюта")),
        "oem": _s(row.get("oem")),
        "image": image_url or "",
    }


async def _image_for_code(code: str) -> str:
    """
    ГЛАВНОЕ:
    Картинку ищем ПО КОДУ в имени файла (по всему столбцу image), а не по "своей строке".
    Это уже реализовано в app/data.py.
    """
    if not code:
        return ""

    url_raw = await data.find_image_by_code_async(code)
    if not url_raw:
        return ""

    return await data.resolve_image_url_async(url_raw)


def _card_html(row: dict) -> str:
    name = html.escape(_s(row.get("наименование")) or "Без наименования")
    typ = html.escape(_s(row.get("тип")) or "—")
    part = html.escape(_s(row.get("парт номер")) or "—")
    oem_part = html.escape(_s(row.get("oem парт номер")) or "—")
    qty = html.escape(_s(row.get("количество")) or "—")
    price = html.escape(_s(row.get("цена")) or "—")
    cur = html.escape(_s(row.get("валюта")) or "")
    maker = html.escape(_s(row.get("изготовитель")) or "—")
    oem = html.escape(_s(row.get("oem")) or "—")

    return (
        f"<div><b>{name}</b></div>"
        f"<div style='margin-top:8px; line-height:1.6'>"
        f"<div><b>Тип:</b> {typ}</div>"
        f"<div><b>Part №:</b> {part}</div>"
        f"<div><b>OEM Part №:</b> {oem_part}</div>"
        f"<div><b>OEM:</b> {oem}</div>"
        f"<div><b>Количество:</b> {qty}</div>"
        f"<div><b>Цена:</b> {price} {cur}</div>"
        f"<div><b>Изготовитель:</b> {maker}</div>"
        f"</div>"
    )


# ----------------------------
# Pages / static
# ----------------------------
async def page_app(request: web.Request):
    p = WEB_DIR / "index.html"
    if not p.exists():
        logger.error("Missing index.html at %s", p)
        return web.Response(status=404, text="index.html not found")
    return _no_cache_headers(web.FileResponse(p))


async def page_item(request: web.Request):
    p = WEB_DIR / "item.html"
    if not p.exists():
        logger.error("Missing item.html at %s", p)
        return web.Response(status=404, text="item.html not found")
    return _no_cache_headers(web.FileResponse(p))


async def static_file(request: web.Request):
    rel = request.match_info.get("path", "")
    p = (STATIC_DIR / rel).resolve()

    if not str(p).startswith(str(STATIC_DIR.resolve())):
        return web.Response(status=403)
    if not p.exists() or not p.is_file():
        return web.Response(status=404)

    return _no_cache_headers(web.FileResponse(p))


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
        scores.append(
            data._relevance_score(
                r.to_dict(),
                tokens + ([norm_code] if norm_code else []),
                q_squash
            )
        )
    results_df["__score"] = scores

    if "код" in results_df.columns:
        results_df = results_df.sort_values(by=["__score", "код"], ascending=[False, True])
    else:
        results_df = results_df.sort_values(by=["__score"], ascending=False)

    results_df = results_df.drop(columns="__score", errors="ignore")

    # чтобы не тормозить на картинках
    results_df = results_df.head(25)

    rows = [r.to_dict() for _, r in results_df.iterrows()]
    codes = [str(r.get("код", "")).strip() for r in rows]

    images = await asyncio.gather(*[_image_for_code(c) for c in codes])
    out = [_row_to_card_item(row, img) for row, img in zip(rows, images)]

    return web.json_response(out)


async def api_item(request: web.Request):
    code = (request.query.get("code") or "").strip().upper()
    if not code:
        return web.json_response({"ok": False, "error": "no_code"}, status=400)

    await _ensure_df()
    df_ = data.df
    if df_ is None or df_.empty or "код" not in df_.columns:
        return web.json_response({"ok": False, "error": "no_data"}, status=500)

    hit = df_[df_["код"].astype(str).str.upper() == code]
    if hit.empty:
        return web.json_response({"ok": False, "error": "not_found"}, status=404)

    row = hit.iloc[0].to_dict()
    image_url = await _image_for_code(code)

    return web.json_response(
        {
            "ok": True,
            "row": row,
            "card_html": _card_html(row),
            "image_url": image_url,
        }
    )


def build_web_app() -> web.Application:
    app = web.Application()

    app.router.add_get("/app", page_app)
    app.router.add_get("/item", page_item)
    app.router.add_get("/static/{path:.*}", static_file)

    app.router.add_get("/api/search", api_search)
    app.router.add_get("/api/item", api_item)

    return app

