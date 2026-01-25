# app/webapp.py
import asyncio
import logging
import math
import re
from pathlib import Path

from aiohttp import web

import pandas as pd

import app.data as data
from app.config import MAX_QTY, SPREADSHEET_URL

logger = logging.getLogger("bot.webapp")

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"
STATIC_DIR = WEB_DIR / "static"


# ---------- Pages ----------
async def page_index(request: web.Request):
    return web.FileResponse(WEB_DIR / "index.html")


async def page_item(request: web.Request):
    return web.FileResponse(WEB_DIR / "item.html")


# ---------- Helpers ----------
def _ensure_loaded():
    # Гарантируем, что df есть
    if data.df is None:
        data.ensure_fresh_data(force=True)
    return data.df


def _to_float_qty(raw: str) -> float:
    s = (raw or "").strip().replace(",", ".")
    qty = float(s)
    if not math.isfinite(qty) or qty <= 0 or qty > MAX_QTY:
        raise ValueError("bad qty")
    return float(f"{qty:.3f}")


def _safe_str(x) -> str:
    return "" if x is None else str(x)


def _find_part_by_code(df: pd.DataFrame, code: str):
    if df is None or df.empty:
        return None
    if "код" not in df.columns:
        return None
    code_l = (code or "").strip().lower()
    hit = df[df["код"].astype(str).str.strip().str.lower() == code_l]
    if hit.empty:
        return None
    return hit.iloc[0].to_dict()


def _build_search(df: pd.DataFrame, q: str):
    # Поведение как в handlers.py (без диалога), чтобы совпадало с ботом
    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data._norm_code(q)

    # 1) строгий поиск по нормализованному коду
    if norm_code:
        matched_indices = data.match_row_by_index([norm_code])
    else:
        matched_indices = data.match_row_by_index(tokens)

    # 2) Фолбэк AND внутри поля, OR по полям
    if not matched_indices:
        mask_any = pd.Series(False, index=df.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель"]:
            series = data._safe_col(df, col)
            if series is None:
                continue
            field_mask = pd.Series(True, index=df.index)
            for t in tokens:
                if t:
                    field_mask &= series.str.contains(re.escape(t), na=False)
            mask_any |= field_mask
        matched_indices = set(df.index[mask_any])

    # 3) Фразовый поиск по склеенным полям
    if not matched_indices and q_squash:
        mask_any = pd.Series(False, index=df.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель"]:
            series = data._safe_col(df, col)
            if series is None:
                continue
            series_sq = series.str.replace(r"[\W_]+", "", regex=True)
            mask_any |= series_sq.str.contains(re.escape(q_squash), na=False)
        matched_indices = set(df.index[mask_any])

    if not matched_indices:
        return df.iloc[0:0].copy()

    results_df = df.loc[list(matched_indices)].copy()

    # сортировка по релевантности (как в боте)
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

    return results_df.drop(columns="__score")


# ---------- API ----------
async def api_health(request: web.Request):
    return web.json_response({"ok": True, "service": "BAZA MG Mini App", "path": "/app"})


async def api_search(request: web.Request):
    q = (request.query.get("q") or "").strip()
    user_id = (request.query.get("user_id") or "0").strip()

    if not q:
        return web.json_response({"ok": False, "error": "Пустой запрос"}, status=400)

    df = await asyncio.to_thread(_ensure_loaded)
    if df is None:
        return web.json_response({"ok": False, "error": "Данные не загружены"}, status=500)

    results = await asyncio.to_thread(_build_search, df, q)

    items = []
    for _, row in results.iterrows():
        d = row.to_dict()

        # ВАЖНО: у тебя колонка называется 'image'
        img = d.get("image", "")
        d["image_url"] = _safe_str(img).strip()

        items.append(d)

    return web.json_response(
        {
            "ok": True,
            "q": q,
            "user_id": user_id,
            "count": len(items),
            "items": items,
        }
    )


async def api_item(request: web.Request):
    code = (request.query.get("code") or "").strip().lower()
    if not code:
        return web.json_response({"ok": False, "error": "code обязателен"}, status=400)

    df = await asyncio.to_thread(_ensure_loaded)
    if df is None:
        return web.json_response({"ok": False, "error": "Данные не загружены"}, status=500)

    part = await asyncio.to_thread(_find_part_by_code, df, code)
    if not part:
        return web.json_response({"ok": False, "error": "Не найдено"}, status=404)

    img = _safe_str(part.get("image", "")).strip()
    return web.json_response(
        {
            "ok": True,
            "code": code,
            "item": part,
            "image_url": img,
            # формат как в боте (для страницы Описание)
            "text": data.format_row(part),
        }
    )


def _save_issue_to_sheet_sync(user_id: int, name: str, part: dict, quantity: float, comment: str):
    import gspread

    client = data.get_gs_client()
    sh = client.open_by_url(SPREADSHEET_URL)

    try:
        ws = sh.worksheet("История")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="История", rows=1000, cols=12)
        ws.append_row(["Дата", "ID", "Имя", "Тип", "Наименование", "Код", "Количество", "Коментарий"])

    headers_raw = ws.row_values(1)
    headers = [h.strip() for h in headers_raw]
    norm = [h.lower() for h in headers]

    ts = data.now_local_str()

    values_by_key = {
        "дата": ts,
        "timestamp": ts,
        "id": user_id,
        "user_id": user_id,
        "имя": name,
        "name": name,
        "тип": str(part.get("тип", "")),
        "type": str(part.get("тип", "")),
        "наименование": str(part.get("наименование", "")),
        "name_item": str(part.get("наименование", "")),
        "код": str(part.get("код", "")),
        "code": str(part.get("код", "")),
        "количество": str(quantity),
        "qty": str(quantity),
        "коментарий": comment or "",
        "комментарий": comment or "",
        "comment": comment or "",
    }

    row = [values_by_key.get(hn, "") for hn in norm]
    ws.append_row(row, value_input_option="USER_ENTERED")


async def api_issue(request: web.Request):
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "Неверный JSON"}, status=400)

    user_id = int(payload.get("user_id") or 0)
    name = (payload.get("name") or "").strip() or str(user_id)
    code = (payload.get("code") or "").strip().lower()
    qty_raw = payload.get("qty")
    comment = (payload.get("comment") or "").strip()

    if not code:
        return web.json_response({"ok": False, "error": "code обязателен"}, status=400)
    if qty_raw is None:
        return web.json_response({"ok": False, "error": "qty обязателен"}, status=400)

    try:
        qty = _to_float_qty(str(qty_raw))
    except Exception:
        return web.json_response({"ok": False, "error": f"qty должен быть > 0 и ≤ {MAX_QTY}"}, status=400)

    df = await asyncio.to_thread(_ensure_loaded)
    if df is None:
        return web.json_response({"ok": False, "error": "Данные не загружены"}, status=500)

    part = await asyncio.to_thread(_find_part_by_code, df, code)
    if not part:
        return web.json_response({"ok": False, "error": "Деталь не найдена"}, status=404)

    try:
        await asyncio.to_thread(_save_issue_to_sheet_sync, user_id, name, part, qty, comment)
    except Exception as e:
        logger.exception("Issue save failed")
        return web.json_response({"ok": False, "error": f"Ошибка записи в История: {e}"}, status=500)

    return web.json_response(
        {
            "ok": True,
            "code": code,
            "qty": qty,
        }
    )


# ---------- App factory ----------
def build_web_app() -> web.Application:
    """
    Mini App для Telegram.
    ВАЖНО: мы отдаём статику по двум путям:
      - /static/*   (чтобы index.html не менять)
      - /app/static/* (если захочешь ссылаться так)
    """
    app = web.Application()

    # Pages
    app.router.add_get("/app", page_index)
    app.router.add_get("/app/", page_index)
    app.router.add_get("/app/item", page_item)

    # API (как у тебя в JS: /api/search, /api/issue, /api/item)
    app.router.add_get("/api/health", api_health)
    app.router.add_get("/api/search", api_search)
    app.router.add_get("/api/item", api_item)
    app.router.add_post("/api/issue", api_issue)

    # Static
    # 1) чтобы работали ссылки из HTML: /static/style.css, /static/app.js, /static/item.js
    if STATIC_DIR.exists():
        app.router.add_static("/static/", str(STATIC_DIR), show_index=False)

    # 2) альтернативно (если потом захочешь): /app/static/*
    if STATIC_DIR.exists():
        app.router.add_static("/app/static/", str(STATIC_DIR), show_index=False)

    logger.info("Mini App mounted at /app (static: /static/* and /app/static/*)")
    return app


    logger.info("Mini App mounted at /app")
    return app
