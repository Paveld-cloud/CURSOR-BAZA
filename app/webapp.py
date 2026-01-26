import asyncio
import html
import logging
import re
from pathlib import Path

from aiohttp import web
import pandas as pd

import app.data as data

logger = logging.getLogger("webapp")

# Структура:
# app/
#   web/
#     index.html
#     item.html
#     static/
#       app.js, item.js, style.css
WEB_DIR = Path(__file__).resolve().parent / "web"
STATIC_DIR = WEB_DIR / "static"


# ----------------------------
# cache headers (иначе Telegram держит старый JS/CSS)
# ----------------------------
def _no_cache_headers(resp: web.StreamResponse) -> web.StreamResponse:
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


async def _ensure_df() -> None:
    # data.ensure_fresh_data() синхронный, уводим в thread
    if getattr(data, "df", None) is None:
        await asyncio.to_thread(data.ensure_fresh_data, True)


def _s(x) -> str:
    return str(x or "").strip()


def _to_row_dict(row: dict) -> dict:
    """Возвращаем row в исходных русских ключах (как в боте/Google Sheet)."""
    return {
        "код": _s(row.get("код")),
        "наименование": _s(row.get("наименование")),
        "изготовитель": _s(row.get("изготовитель")),
        "парт номер": _s(row.get("парт номер")),
        "oem парт номер": _s(row.get("oem парт номер")),
        "тип": _s(row.get("тип")),
        "количество": _s(row.get("количество")),
        "цена": _s(row.get("цена")),
        "валюта": _s(row.get("валюта")),
        "oem": _s(row.get("oem")),
        "image": _s(row.get("image")),
    }


async def _image_for_code(code: str) -> str:
    """
    Главное правило:
    картинку ищем ПО КОДУ в имени файла (через индекс data.py),
    а не по "image" из текущей строки (там может быть съезд).
    """
    if not code:
        return ""

    # 1) ищем URL в индексе по коду
    url_raw = await data.find_image_by_code_async(code)
    if not url_raw:
        return ""

    # 2) приводим к прямой ссылке (drive / ibb.co -> i.ibb.co)
    return await data.resolve_image_url_async(url_raw)


def _row_to_card_item(row: dict, image_url: str) -> dict:
    """Формат для списка карточек на главной (/app)."""
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


def _card_html(row: dict) -> str:
    """Небольшой HTML для item.html (безопасный)."""
    name = html.escape(_s(row.get("наименование")) or "Без наименования")
    typ = html.escape(_s(row.get("тип")) or "—")
    part = html.escape(_s(row.get("парт номер")) or "—")
    oem_part = html.escape(_s(row.get("oem парт номер")) or "—")
    qty = html.escape(_s(row.get("количество")) or "—")
    price = html.escape(_s(row.get("цена")) or "—")
    cur = html.escape(_s(row.get("валюта")) or "")
    maker = html.escape(_s(row.get("изготовитель")) or "—")

    return (
        f"<div><b>{name}</b></div>"
        f"<div style='margin-top:8px; line-height:1.55'>"
        f"<div><b>Тип:</b> {typ}</div>"
        f"<div><b>Part №:</b> {part}</div>"
        f"<div><b>OEM Part №:</b> {oem_part}</div>"
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

    # 2) фолбэк AND по токенам (мягко)
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

    # ограничим, чтобы не тормозить (картинки резолвятся)
    results_df = results_df.head(25)

    rows = [r.to_dict() for _, r in results_df.iterrows()]
    codes = [str(r.get("код", "")).strip() for r in rows]

    # картинки ищем строго по коду (даже если в строке image не совпадает)
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

    row_raw = hit.iloc[0].to_dict()
    row = _to_row_dict(row_raw)
    image_url = await _image_for_code(code)

    return web.json_response(
        {
            "ok": True,
            "row": row,
            "card_html": _card_html(row),
            "image_url": image_url,
        }
    )


async def api_issue(request: web.Request):
    """
    Списание из mini-app (кнопка 📦 Взять деталь в item.html).

    Формат payload (см. web/static/item.js):
      { user_id, name, code, qty, comment }
    """
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad_json"}, status=400)

    user_id = int(payload.get("user_id") or 0)
    name = _s(payload.get("name"))
    code = _s(payload.get("code")).upper()
    qty = payload.get("qty")
    comment = _s(payload.get("comment"))

    if not user_id or not code:
        return web.json_response({"ok": False, "error": "missing_user_or_code"}, status=400)

    try:
        qty_f = float(str(qty).replace(",", "."))
        if qty_f <= 0:
            raise ValueError
    except Exception:
        return web.json_response({"ok": False, "error": "bad_qty"}, status=400)

    await _ensure_df()
    df_ = data.df
    if df_ is None or df_.empty or "код" not in df_.columns:
        return web.json_response({"ok": False, "error": "no_data"}, status=500)

    hit = df_[df_["код"].astype(str).str.upper() == code]
    if hit.empty:
        return web.json_response({"ok": False, "error": "not_found"}, status=404)

    part = hit.iloc[0].to_dict()

    # Запись в Google Sheet "История" (логика совместима с handlers.save_issue_to_sheet)
    try:
        from app.config import SPREADSHEET_URL
        import gspread

        client = data.get_gs_client()
        sh = client.open_by_url(SPREADSHEET_URL)
        try:
            ws = sh.worksheet("История")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title="История", rows=1000, cols=12)
            ws.append_row(
                [
                    "Дата",
                    "ID",
                    "Имя",
                    "Тип",
                    "Наименование",
                    "Код",
                    "Количество",
                    "Коментарий",
                ]
            )

        headers_raw = ws.row_values(1)
        headers = [h.strip() for h in headers_raw]
        norm = [h.lower() for h in headers]

        ts = data.now_local_str()
        values_by_key = {
            "дата": ts,
            "timestamp": ts,
            "id": user_id,
            "user_id": user_id,
            "имя": name or str(user_id),
            "name": name or str(user_id),
            "тип": str(part.get("тип", "")),
            "type": str(part.get("тип", "")),
            "наименование": str(part.get("наименование", "")),
            "name_item": str(part.get("наименование", "")),
            "код": str(part.get("код", "")),
            "code": str(part.get("код", "")),
            "количество": str(qty_f),
            "qty": str(qty_f),
            "коментарий": comment or "",
            "комментарий": comment or "",
            "comment": comment or "",
        }

        row_out = [values_by_key.get(hn, "") for hn in norm]
        ws.append_row(row_out, value_input_option="USER_ENTERED")

        logger.info("[webapp] issue saved: user=%s code=%s qty=%s", user_id, code, qty_f)
        return web.json_response({"ok": True})
    except Exception as e:
        logger.exception("[webapp] issue save failed")
        return web.json_response({"ok": False, "error": f"sheet_error: {e}"}, status=500)


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
    app.router.add_post("/api/issue", api_issue)

    return app

