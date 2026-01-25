import os
import json
import math
import re
import asyncio
import logging
from pathlib import Path
from aiohttp import web

import pandas as pd
import app.data as data
from app.config import PAGE_SIZE, MAX_QTY, TZ_NAME

logger = logging.getLogger("bot.webapp")

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"

def _is_allowed(uid: int) -> bool:
    # если лист пользователей пустой/нет — пускаем всех как в data.load_users_from_sheet()
    if not data.SHEET_ALLOWED and not data.SHEET_ADMINS and not data.SHEET_BLOCKED:
        return True
    if uid in data.SHEET_BLOCKED:
        return False
    return uid in data.SHEET_ALLOWED or uid in data.SHEET_ADMINS

def _json(data_obj, status=200):
    return web.json_response(data_obj, status=status, dumps=lambda x: json.dumps(x, ensure_ascii=False))

def _get_user_id(request: web.Request) -> int:
    # MVP: фронт передаёт user_id (из tg.initDataUnsafe.user.id)
    try:
        return int(request.query.get("user_id", "0"))
    except Exception:
        return 0

async def page_index(request: web.Request):
    return web.FileResponse(WEB_DIR / "index.html")

async def page_item(request: web.Request):
    return web.FileResponse(WEB_DIR / "item.html")

async def api_search(request: web.Request):
    uid = _get_user_id(request)
    if uid and not _is_allowed(uid):
        return _json({"ok": False, "error": "access_denied"}, status=403)

    q = (request.query.get("q", "") or "").strip()
    if not q:
        return _json({"ok": True, "items": []})

    # гарантируем свежие данные
    if data.df is None:
        await asyncio.to_thread(data.ensure_fresh_data, True)
    if data.df is None:
        return _json({"ok": False, "error": "data_not_loaded"}, status=500)

    df_ = data.df

    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data._norm_code(q)

    # 1) индекс
    if norm_code:
        matched_indices = data.match_row_by_index([norm_code])
    else:
        matched_indices = data.match_row_by_index(tokens)

    # 2) AND в поле, OR по полям
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

    # 3) фраза в склеенных полях
    if not matched_indices and q_squash:
        mask_any = pd.Series(False, index=df_.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель"]:
            series = data._safe_col(df_, col)
            if series is None:
                continue
            series_sq = series.str.replace(r'[\W_]+', '', regex=True)
            mask_any |= series_sq.str.contains(re.escape(q_squash), na=False)
        matched_indices = set(df_.index[mask_any])

    if not matched_indices:
        return _json({"ok": True, "items": []})

    results_df = df_.loc[list(matched_indices)].copy()

    # score + сортировка как в боте
    scores = []
    for _, r in results_df.iterrows():
        scores.append(data._relevance_score(r.to_dict(), tokens + ([norm_code] if norm_code else []), q_squash))
    results_df["__score"] = scores

    if "код" in results_df.columns:
        results_df = results_df.sort_values(
            by=["__score", "код"],
            ascending=[False, True],
            key=lambda s: s if s.name != "код" else s.astype(str).str.len()
        )
    else:
        results_df = results_df.sort_values(by=["__score"], ascending=False)

    results_df = results_df.drop(columns="__score")

    items = []
    limit = int(request.query.get("limit", "30") or "30")
    limit = max(1, min(limit, 100))

    for _, row in results_df.head(limit).iterrows():
        d = row.to_dict()
        items.append({
            "код": str(d.get("код", "")),
            "наименование": str(d.get("наименование", "")),
            "тип": str(d.get("тип", "")),
            "парт номер": str(d.get("парт номер", "")),
            "oem парт номер": str(d.get("oem парт номер", "")),
            "количество": str(d.get("количество", "")),
            "цена": str(d.get("цена", "")),
            "валюта": str(d.get("валюта", "")),
            "изготовитель": str(d.get("изготовитель", "")),
            "oem": str(d.get("oem", "")),
            "image": str(d.get("image", "")),
        })

    return _json({"ok": True, "items": items})

async def api_item(request: web.Request):
    uid = _get_user_id(request)
    if uid and not _is_allowed(uid):
        return _json({"ok": False, "error": "access_denied"}, status=403)

    code = (request.query.get("code", "") or "").strip().lower()
    if not code:
        return _json({"ok": False, "error": "code_required"}, status=400)

    if data.df is None:
        await asyncio.to_thread(data.ensure_fresh_data, True)
    if data.df is None:
        return _json({"ok": False, "error": "data_not_loaded"}, status=500)

    hit = data.df[data.df["код"].astype(str).str.lower() == code]
    if hit.empty:
        return _json({"ok": False, "error": "not_found"}, status=404)

    row = hit.iloc[0].to_dict()

    # картинку берём так же как в боте: по коду/индексу + нормализация ссылок
    img = ""
    try:
        img_raw = await data.find_image_by_code_async(code)
        img = await data.resolve_image_url_async(img_raw)
    except Exception:
        img = ""

    return _json({
        "ok": True,
        "item": row,
        "card_text": data.format_row(row),
        "image_url": img,
    })

async def api_issue(request: web.Request):
    payload = await request.json()
    uid = int(payload.get("user_id", 0) or 0)
    if uid and not _is_allowed(uid):
        return _json({"ok": False, "error": "access_denied"}, status=403)

    code = str(payload.get("code", "") or "").strip().lower()
    qty_raw = str(payload.get("qty", "") or "").strip().replace(",", ".")
    comment = str(payload.get("comment", "") or "").strip()

    try:
        qty = float(qty_raw)
        if not math.isfinite(qty) or qty <= 0 or qty > float(MAX_QTY):
            raise ValueError
        qty = float(f"{qty:.3f}")
    except Exception:
        return _json({"ok": False, "error": f"qty_invalid_max_{MAX_QTY}"}, status=400)

    if data.df is None:
        await asyncio.to_thread(data.ensure_fresh_data, True)
    if data.df is None:
        return _json({"ok": False, "error": "data_not_loaded"}, status=500)

    hit = data.df[data.df["код"].astype(str).str.lower() == code]
    if hit.empty:
        return _json({"ok": False, "error": "not_found"}, status=404)
    part = hit.iloc[0].to_dict()

    # пишем в "История" тем же методом, что у бота (логика из handlers.save_issue_to_sheet)
    # упрощённо: используем gspread клиент из data.get_gs_client()
    try:
        import gspread
        client = data.get_gs_client()
        sh = client.open_by_url(os.getenv("SPREADSHEET_URL", ""))
        try:
            ws = sh.worksheet("История")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title="История", rows=1000, cols=12)
            ws.append_row(["Дата", "ID", "Имя", "Тип", "Наименование", "Код", "Количество", "Коментарий"])

        headers_raw = ws.row_values(1)
        headers = [h.strip() for h in headers_raw]
        norm = [h.lower() for h in headers]

        display_name = str(payload.get("name", "") or f"uid:{uid}")
        ts = data.now_local_str(TZ_NAME)

        values_by_key = {
            "дата": ts, "timestamp": ts,
            "id": uid, "user_id": uid,
            "имя": display_name, "name": display_name,
            "тип": str(part.get("тип", "")), "type": str(part.get("тип", "")),
            "наименование": str(part.get("наименование", "")), "name_item": str(part.get("наименование", "")),
            "код": str(part.get("код", "")), "code": str(part.get("код", "")),
            "количество": str(qty), "qty": str(qty),
            "коментарий": comment or "", "комментарий": comment or "", "comment": comment or "",
        }
        row_out = [values_by_key.get(hn, "") for hn in norm]
        ws.append_row(row_out, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.exception(f"issue write failed: {e}")
        return _json({"ok": False, "error": "history_write_failed"}, status=500)

    return _json({"ok": True})

def build_web_app() -> web.Application:
    app = web.Application()

    # Pages
    app.router.add_get("/", page_index)
    app.router.add_get("/item", page_item)

    # API
    app.router.add_get("/api/search", api_search)
    app.router.add_get("/api/item", api_item)
    app.router.add_post("/api/issue", api_issue)

    # Static (css/js)
    app.router.add_static("/static/", str(WEB_DIR / "static"), show_index=False)

    return app
