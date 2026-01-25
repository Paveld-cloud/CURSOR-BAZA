import asyncio
import logging
import os
import re
from pathlib import Path
from aiohttp import web
import pandas as pd

import app.data as data

logger = logging.getLogger("webapp")


# ----------------------------
# helpers
# ----------------------------
def _basename_no_query(url: str) -> str:
    """Берём имя файла из URL без query/fragment."""
    u = str(url or "").strip()
    if not u:
        return ""
    u = re.sub(r"[?#].*$", "", u)
    return u.rsplit("/", 1)[-1]

def _code_in_filename_strict(code: str, image_url: str) -> bool:
    """
    ЖЁСТКОЕ правило:
    Фото считается валидным только если код присутствует в ИМЕНИ ФАЙЛА ссылки image.
    Пример: .../UZ000664.jpg  -> OK
            .../something.jpg -> NO
    """
    code_u = str(code or "").strip().upper()
    if not code_u:
        return False
    name = _basename_no_query(image_url).upper()
    return code_u in name

def _safe_str(x) -> str:
    return str(x or "").strip()

def _row_to_item(row: dict) -> dict:
    """
    Приведение строки df к объекту для WebApp.
    Колонки в data.df у тебя в lowercase (код, наименование, ...).
    """
    code = _safe_str(row.get("код", "")).upper()
    img_raw = _safe_str(row.get("image", ""))

    # Строгое правило по имени файла
    image = img_raw if (img_raw and _code_in_filename_strict(code, img_raw)) else ""

    return {
        "code": code,
        "name": _safe_str(row.get("наименование", "")),
        "type": _safe_str(row.get("тип", "")),
        "part": _safe_str(row.get("парт номер", "")),
        "oem_part": _safe_str(row.get("oem парт номер", "")),
        "qty": _safe_str(row.get("количество", "")),
        "price": _safe_str(row.get("цена", "")),
        "currency": _safe_str(row.get("валюта", "")),
        "oem": _safe_str(row.get("oem", "")),
        "image": image,
    }


async def _ensure_df():
    # df уже загружается у тебя в main.py, но на всякий случай
    if data.df is None:
        await asyncio.to_thread(data.ensure_fresh_data, True)


# ----------------------------
# API handlers
# ----------------------------
async def api_search(request: web.Request):
    q = (request.query.get("q") or "").strip()
    if not q:
        return web.json_response([])

    await _ensure_df()
    df_ = data.df
    if df_ is None or df_.empty:
        return web.json_response([])

    # ---- Поисковая стратегия (как в боте, но для WebApp) ----
    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data._norm_code(q)

    # 1) индекс
    if norm_code:
        matched = data.match_row_by_index([norm_code])
    else:
        matched = data.match_row_by_index(tokens)

    # 2) фолбэк AND по токенам внутри поля, OR по полям
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

    # 3) фолбэк по склеенной фразе
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

    # Сбор результатов
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

    out = []
    for _, r in results_df.iterrows():
        out.append(_row_to_item(r.to_dict()))

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

    row = hit.iloc[0].to_dict()
    return web.json_response(_row_to_item(row))


# ----------------------------
# Static / pages
# ----------------------------
def _static_dir() -> Path:
    # твоя структура: app/web/static/...
    return Path(__file__).resolve().parent / "web" / "static"

async def page_app(request: web.Request):
    return web.FileResponse(_static_dir() / "index.html")

async def page_item(request: web.Request):
    return web.FileResponse(_static_dir() / "item.html")

async def static_file(request: web.Request):
    rel = request.match_info.get("path", "")
    p = (_static_dir() / rel).resolve()
    # защита от выхода из директории
    if not str(p).startswith(str(_static_dir().resolve())):
        return web.Response(status=403)
    if not p.exists() or not p.is_file():
        return web.Response(status=404)
    return web.FileResponse(p)


# ----------------------------
# factory
# ----------------------------
def build_web_app() -> web.Application:
    app = web.Application()

    # pages
    app.router.add_get("/app", page_app)
    app.router.add_get("/item", page_item)

    # static
    app.router.add_get("/static/{path:.*}", static_file)

    # api
    app.router.add_get("/api/search", api_search)
    app.router.add_get("/api/item", api_item)

    return app

