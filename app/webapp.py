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
def _pick_image_raw(row: dict) -> str:
    """
    Максимально "живучий" поиск картинки:
    1) точные ключи (image/img/photo/фото/картинка)
    2) любой ключ, где подстрока похожа на image/img/photo/фото/картин
    """
    if not isinstance(row, dict):
        return ""

    # 1) точные
    exact = ("image", "img", "photo", "фото", "картинка", "picture")
    for k in exact:
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()

    # 2) по подстроке (учитываем пробелы, регистр)
    for k, v in row.items():
        kk = str(k).strip().lower()
        if any(s in kk for s in ("image", "img", "photo", "фото", "картин", "picture")):
            if v is not None and str(v).strip():
                return str(v).strip()

    return ""


async def _resolve_image_url(row: dict) -> str:
    raw = _pick_image_raw(row)
    if not raw:
        return ""

    # если уже URL
    if raw.startswith("http://") or raw.startswith("https://"):
        try:
            # если у тебя бывают "не прямые" ссылки - резолвим
            resolved = await data.resolve_image_url_async(raw)
            return resolved or raw
        except Exception:
            return raw

    # если хранишь ключ/имя - пробуем резолв
    try:
        resolved = await data.resolve_image_url_async(raw)
        return resolved or ""
    except Exception:
        return ""


def _ensure_df_ready():
    if data.df is None:
        data.ensure_fresh_data(force=True)
    if data.df is None:
        raise RuntimeError("DF is not loaded")


def _search_df(q: str) -> pd.DataFrame:
    _ensure_df_ready()
    df_ = data.df

    q = (q or "").strip()
    if not q:
        return df_.iloc[0:0]

    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data._norm_code(q)

    # 1) индексный
    if norm_code:
        matched = data.match_row_by_index([norm_code])
    else:
        matched = data.match_row_by_index(tokens)

    # 2) fallback AND/OR
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

    # 3) squash
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
        return df_.iloc[0:0]

    results_df = df_.loc[list(matched)].copy()

    # scoring
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
    try:
        q = (request.query.get("q") or "").strip()
        user_id = str(request.query.get("user_id") or "0")

        df_res = await asyncio.to_thread(_search_df, q)

        limit = int(request.query.get("limit") or 30)
        limit = max(1, min(limit, 100))

        items = []
        if not df_res.empty:
            for _, r in df_res.head(limit).iterrows():
                row = r.to_dict()

                image_url = ""
                try:
                    image_url = await _resolve_image_url(row)
                except Exception:
                    image_url = ""

                # для отладки: если нет картинки, логируем какие ключи есть
                if not image_url:
                    code = str(row.get("код", "")).strip()
                    keys = list(row.keys())
                    logger.info(f"[webapp] no image_url for code={code}. keys={keys}")

                row["image_url"] = image_url
                items.append(row)

        return web.json_response(
            {"ok": True, "q": q, "user_id": user_id, "count": len(items), "items": items}
        )

    except Exception as e:
        logger.exception("api_search failed")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_item(request: web.Request):
    try:
        code = (request.query.get("code") or "").strip().lower()
        if not code:
            return web.json_response({"ok": False, "error": "code is required"}, status=400)

        _ensure_df_ready()
        df_ = data.df

        found = None
        if "код" in df_.columns:
            hit = df_[df_["код"].astype(str).str.lower() == code]
            if not hit.empty:
                found = hit.iloc[0].to_dict()

        if not found:
            return web.json_response({"ok": False, "error": "not found"}, status=404)

        found["image_url"] = await _resolve_image_url(found)
        found["card_html"] = data.format_row(found)

        return web.json_response({"ok": True, "item": found})

    except Exception as e:
        logger.exception("api_item failed")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_issue(request: web.Request):
    try:
        payload = await request.json()

        user_id = int(payload.get("user_id") or 0)
        name = str(payload.get("name") or "").strip()
        code = str(payload.get("code") or "").strip().lower()
        comment = str(payload.get("comment") or "").strip()
        qty_raw = str(payload.get("qty") or "").strip().replace(",", ".")

        if not code:
            return web.json_response({"ok": False, "error": "code is required"}, status=400)

        try:
            qty = float(qty_raw)
            if not math.isfinite(qty) or qty <= 0 or qty > MAX_QTY:
                raise ValueError
            qty = float(f"{qty:.3f}")
        except Exception:
            return web.json_response({"ok": False, "error": f"qty must be >0 and <= {MAX_QTY}"}, status=400)

        _ensure_df_ready()
        df_ = data.df

        part = None
        if "код" in df_.columns:
            hit = df_[df_["код"].astype(str).str.lower() == code]
            if not hit.empty:
                part = hit.iloc[0].to_dict()

        if not part:
            return web.json_response({"ok": False, "error": "part not found"}, status=404)

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
        display_name = name or str(user_id)

        values_by_key = {
            "дата": ts,
            "timestamp": ts,
            "id": user_id,
            "user_id": user_id,
            "имя": display_name,
            "name": display_name,
            "тип": str(part.get("тип", "")),
            "type": str(part.get("тип", "")),
            "наименование": str(part.get("наименование", "")),
            "код": str(part.get("код", "")),
            "количество": str(qty),
            "qty": str(qty),
            "коментарий": comment or "",
            "комментарий": comment or "",
            "comment": comment or "",
        }

        row_out = [values_by_key.get(hn, "") for hn in norm]
        ws.append_row(row_out, value_input_option="USER_ENTERED")

        return web.json_response({"ok": True})

    except Exception as e:
        logger.exception("api_issue failed")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


# ---------- App builder ----------
def build_web_app() -> web.Application:
    app = web.Application()

    # Pages
    app.router.add_get("/app", page_index)
    app.router.add_get("/app/", page_index)
    app.router.add_get("/app/item", page_item)

    # API
    app.router.add_get("/app/api/health", api_health)
    app.router.add_get("/api/search", api_search)
    app.router.add_get("/api/item", api_item)
    app.router.add_post("/api/issue", api_issue)

    # Static (оба пути, чтобы не ловить 404)
    app.router.add_static("/static/", str(STATIC_DIR), show_index=False)
    app.router.add_static("/app/static/", str(STATIC_DIR), show_index=False)

    logger.info("Mini App mounted at /app (static: /static/* and /app/static/*)")
    return app

