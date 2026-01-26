from pathlib import Path

data_py = r'''# app/data.py
import os
import io
import re
import time
import json
import logging
from typing import Dict, Set, List, Optional, Tuple

import pandas as pd
import aiohttp
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlparse
import os.path

logger = logging.getLogger("bot.data")

# ---------------- Config ----------------
# Берём значения из app/config.py, но с фолбэками на env.
# ВАЖНО: не переименовывай переменные — на них завязаны handlers/webapp.
try:
    from app.config import (
        SPREADSHEET_URL,
        SAP_SHEET_NAME,
        USERS_SHEET_NAME,
        DATA_TTL,
        SEARCH_COLUMNS,
        TIMEZONE,
    )
except Exception:
    SPREADSHEET_URL = os.getenv("SPREADSHEET_URL", "")
    SAP_SHEET_NAME = os.getenv("SAP_SHEET_NAME", "SAP")
    USERS_SHEET_NAME = os.getenv("USERS_SHEET_NAME", "Пользователи")
    DATA_TTL = int(os.getenv("DATA_TTL", "600"))
    SEARCH_COLUMNS = os.getenv("SEARCH_COLUMNS", "").strip().split(",") if os.getenv("SEARCH_COLUMNS") else [
        "тип",
        "наименование",
        "код",
        "oem",
        "изготовитель",
        "парт номер",
        "oem парт номер",
    ]
    TIMEZONE = os.getenv("TIMEZONE", "Asia/Tashkent")

GOOGLE_APPLICATION_CREDENTIALS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------------- Runtime state ----------------
df: Optional[pd.DataFrame] = None
_last_load_ts: float = 0.0

_search_index: Dict[str, Set[int]] = {}

# Индекс "код->image" из той же строки (ускорение/для диагностики).
# Но ИСТИНА по фото — _image_file_index (по всему столбцу image, по имени файла).
_image_index: Dict[str, str] = {}

# Главный индекс для фото:
# KEY = basename(url) без расширения (например UZ000664), VALUE = url из СТОЛБЦА image (любой строки)
_image_file_index: Dict[str, str] = {}

SHEET_ALLOWED: Set[int] = set()
SHEET_ADMINS: Set[int] = set()
SHEET_BLOCKED: Set[int] = set()

# совместимость с handlers.py
ASK_QUANTITY, ASK_COMMENT, ASK_CONFIRM = range(3)

# ---------------- Helpers: text / code ----------------
def _norm_code(x: str) -> str:
    """
    Нормализация кодов/номеров:
    - lower
    - O -> 0
    - оставить только [a-z0-9]
    """
    s = str(x or "").strip().lower()
    s = s.replace("o", "0")
    s = re.sub(r"[^a-z0-9]", "", s)
    return s

def normalize(text: str) -> str:
    """для webapp: нормализация текста в токены"""
    return re.sub(r"[^\w\s]+", " ", str(text or "").lower(), flags=re.U).strip()

def squash(text: str) -> str:
    """для webapp: склейка без пробелов/символов"""
    return re.sub(r"[\W_]+", "", str(text or "").lower(), flags=re.U)

def _safe_col(df_: pd.DataFrame, col: str) -> Optional[pd.Series]:
    if df_ is None or col not in df_.columns:
        return None
    return df_[col].astype(str).fillna("").str.strip().str.lower()

def now_local_str() -> str:
    tz = ZoneInfo(TIMEZONE if TIMEZONE else "Asia/Tashkent")
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def val(row: dict, key: str, default: str = "") -> str:
    return str(row.get(key, default) or default)

# ---------------- Formatting (card) ----------------
def format_row(row: dict) -> str:
    """
    Компактная карточка (HTML) под Telegram/mini app.
    """
    code        = val(row, "код").upper()
    name        = val(row, "наименование")
    type_       = val(row, "тип")
    part_no     = val(row, "парт номер")
    oem_part    = val(row, "oem парт номер")
    qty         = val(row, "количество") or "—"
    price       = val(row, "цена")
    currency    = val(row, "валюта")
    manuf       = val(row, "изготовитель")
    oem         = val(row, "oem")

    lines: List[str] = []
    if code:
        lines.append(f"🔢 <b>Код:</b> {code}")
    if name:
        lines.append(f"📦 <b>Наименование:</b> {name}")
    if type_:
        lines.append(f"📎 <b>Тип:</b> {type_}")
    if part_no:
        lines.append(f"🧩 <b>Парт №:</b> {part_no}")
    if oem_part:
        lines.append(f"⚙️ <b>OEM №:</b> {oem_part}")
    lines.append(f"📦 <b>Кол-во:</b> {qty}")
    if price or currency:
        lines.append(f"💰 <b>Цена:</b> {price} {currency}".rstrip())
    if manuf:
        lines.append(f"🏭 <b>Изготовитель:</b> {manuf}")
    if oem:
        lines.append(f"⚙️ <b>OEM:</b> {oem}")

    return "\n".join(lines)

# ---------------- Google Sheets client ----------------
def get_gs_client():
    if not GOOGLE_APPLICATION_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON не задан")

    # поддержка: строка JSON или путь к файлу
    try:
        info = json.loads(GOOGLE_APPLICATION_CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    except json.JSONDecodeError:
        creds = Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS_JSON, scopes=SCOPES)

    return gspread.authorize(creds)

# ---------------- Load SAP dataframe ----------------
def _load_sap_dataframe() -> pd.DataFrame:
    """
    Берём данные так, как они в Google Sheets (get_all_values) — без сюрпризов по типам.
    Заголовки -> lower.
    """
    if not SPREADSHEET_URL:
        raise RuntimeError("SPREADSHEET_URL не задан")

    client = get_gs_client()
    sh = client.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SAP_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = [str(c).strip().lower() for c in values[0]]
    rows = values[1:]
    new_df = pd.DataFrame(rows, columns=headers)

    # Нормализуем критичные колонки (как строки)
    for col in ("код", "парт номер", "oem парт номер", "oem"):
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(str).fillna("").str.strip()

    if "image" in new_df.columns:
        new_df["image"] = new_df["image"].astype(str).fillna("").str.strip()

    return new_df

# ---------------- Index builders ----------------
_ALLOWED_EXTS = {".png", ".jpg", ".jpeg", ".webp"}

def build_search_index(df_: pd.DataFrame) -> Dict[str, Set[int]]:
    """
    Индекс для быстрого поиска.
    Токены из SEARCH_COLUMNS.
    """
    idx: Dict[str, Set[int]] = {}
    if df_ is None or df_.empty:
        return idx

    cols = [c for c in SEARCH_COLUMNS if c in df_.columns]

    for i, row in df_.iterrows():
        for c in cols:
            raw = str(row.get(c, "")).strip().lower()
            if not raw:
                continue

            # для кодов/номеров добавляем нормализованное значение целиком
            if c in ("код", "парт номер", "oem парт номер"):
                key = _norm_code(raw)
                if key:
                    idx.setdefault(key, set()).add(i)

            # токены a-z0-9
            for t in re.findall(r"[a-z0-9]+", raw):
                t = _norm_code(t)
                if t:
                    idx.setdefault(t, set()).add(i)

    return idx

def build_image_index(df_: pd.DataFrame) -> Dict[str, str]:
    """
    Ускорение: norm(код) -> URL из КОЛОНКИ image (из той же строки).
    ВАЖНО: этот индекс НЕ гарантирует совпадение имени файла с кодом (данные могут быть ошибочны).
    """
    out: Dict[str, str] = {}
    if df_ is None or df_.empty:
        return out
    if "код" not in df_.columns or "image" not in df_.columns:
        return out

    for _, row in df_.iterrows():
        code_raw = str(row.get("код", "")).strip()
        url = str(row.get("image", "")).strip()
        if not code_raw or not url:
            continue
        k = _norm_code(code_raw)
        if k:
            out.setdefault(k, url)
    return out

def build_image_file_index(df_: pd.DataFrame) -> Dict[str, str]:
    """
    ГЛАВНЫЙ индекс фото (как ты требуешь):
    ищем по ВСЕМ строкам столбца image совпадение имени файла.

    Пример:
      URL: https://i.ibb.co/HLzjRrsQ/UZ000662.png
      key -> "UZ000662"
    """
    out: Dict[str, str] = {}
    if df_ is None or df_.empty or "image" not in df_.columns:
        return out

    for raw_url in df_["image"].astype(str).fillna("").tolist():
        u = str(raw_url).strip()
        if not u:
            continue
        try:
            path = urlparse(u).path
            fname = os.path.basename(path)
            name, ext = os.path.splitext(fname)
            if not name or ext.lower() not in _ALLOWED_EXTS:
                continue
            key = name.strip().upper()
            out.setdefault(key, u)  # берём первое непустое
        except Exception:
            continue

    return out

# ---------------- Reload/TTL ----------------
def ensure_fresh_data(force: bool = False):
    global df, _last_load_ts, _search_index, _image_index, _image_file_index

    need = force or df is None or (time.time() - _last_load_ts > DATA_TTL)
    if not need:
        return

    new_df = _load_sap_dataframe()
    df = new_df

    _search_index = build_search_index(df)
    _image_index = build_image_index(df)
    _image_file_index = build_image_file_index(df)

    _last_load_ts = time.time()

    logger.info(
        f"✅ SAP reload: {len(df)} rows, index={len(_search_index)} keys, "
        f"images_by_row={len(_image_index)} keys, images_by_filename={len(_image_file_index)} keys"
    )

# ---------------- Strict image matching (by FULL COLUMN) ----------------
def find_image_url_by_code_strict(code: str) -> str:
    """
    ТВОЁ правило:
    1) берём код (например UZ000664)
    2) ищем по ВСЕМ строкам столбца image URL, где basename без расширения == код
    3) если нет точного совпадения — возвращаем пусто (НЕ подставляем чужие фото)
    """
    ensure_fresh_data()
    if df is None or df.empty:
        return ""

    code_raw = str(code or "").strip()
    if not code_raw:
        return ""

    url = _image_file_index.get(code_raw.upper(), "")
    return url or ""

async def find_image_by_code_async(code: str) -> str:
    """
    async wrapper для handlers/webapp.
    """
    return find_image_url_by_code_strict(code)

# ---------------- URL resolve (ibb/drive) ----------------
def normalize_drive_url(url: str) -> str:
    """
    drive.google.com/file/d/<id> -> direct
    drive.google.com/open?id=<id> -> direct
    """
    u = str(url or "").strip()
    m = re.search(r"drive\.google\.com/(?:file/d/([-\w]{20,})|open\?id=([-\w]{20,}))", u)
    if not m:
        return u
    file_id = m.group(1) or m.group(2)
    return f"https://drive.google.com/uc?export=download&id={file_id}"

async def resolve_ibb_direct_async(url: str) -> str:
    """
    если дана короткая ibb.co/<id> — пытаемся получить og:image (прямую i.ibb.co/..)
    если уже i.ibb.co — возвращаем как есть
    """
    u = str(url or "").strip()
    if not u:
        return ""
    try:
        if re.search(r"^https?://i\.ibb\.co/", u, re.I):
            return u
        if not re.search(r"^https?://ibb\.co/", u, re.I):
            return u

        async with aiohttp.ClientSession() as session:
            async with session.get(u, timeout=10) as resp:
                if resp.status != 200:
                    return u
                html = await resp.text()

        m = re.search(
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            html,
            re.I
        )
        return m.group(1) if m else u
    except Exception as e:
        logger.warning(f"resolve_ibb_direct_async error: {e}")
        return u

async def resolve_image_url_async(url_raw: str) -> str:
    """
    приводим к прямой ссылке
    """
    if not url_raw:
        return ""
    u = normalize_drive_url(url_raw)
    u = await resolve_ibb_direct_async(u)
    return u or ""

# ---------------- Search core ----------------
def match_row_by_index(tokens: List[str]) -> Set[int]:
    """
    Быстрый поиск:
    - нормализуем токены
    - сначала AND пересечение
    - если пусто — OR объединение
    """
    ensure_fresh_data()
    if not tokens:
        return set()

    keys = [_norm_code(t) for t in tokens if t]
    keys = [k for k in keys if k]
    if not keys:
        return set()

    sets: List[Set[int]] = []
    for k in keys:
        s = _search_index.get(k, set())
        if not s:
            sets = []
            break
        sets.append(s)

    if sets:
        acc = sets[0].copy()
        for s in sets[1:]:
            acc &= s
        return acc

    found: Set[int] = set()
    for k in keys:
        found |= _search_index.get(k, set())
    return found

# ---------------- Users sheet (allowed/admin/blocked) ----------------
def _parse_int(x) -> Optional[int]:
    try:
        v = int(str(x).strip())
        return v if v > 0 else None
    except Exception:
        return None

def _dedupe_headers(headers: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for i, h in enumerate(headers):
        base = re.sub(r"[^\w]+", "_", str(h or "").strip().lower()).strip("_")
        if not base:
            base = f"col{i+1}"
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out

def load_users_from_sheet() -> Tuple[Set[int], Set[int], Set[int]]:
    allowed: Set[int] = set()
    admins: Set[int] = set()
    blocked: Set[int] = set()

    try:
        client = get_gs_client()
        sh = client.open_by_url(SPREADSHEET_URL)
        ws = sh.worksheet(USERS_SHEET_NAME)
    except Exception:
        # нет листа — не блокируем никого
        return allowed, admins, blocked

    vals = ws.get_all_values()
    if not vals:
        return allowed, admins, blocked

    headers = _dedupe_headers(vals[0])
    rows = vals[1:]

    def truthy(v) -> bool:
        s = str(v).strip().lower()
        return s in ("1", "true", "да", "yes", "y")

    for r in rows:
        rec = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}
        uid = _parse_int(rec.get("user_id") or rec.get("id") or rec.get("uid"))
        if not uid:
            continue

        role = str(rec.get("role", "")).strip().lower()
        if role in ("admin", "админ"):
            admins.add(uid); allowed.add(uid); continue
        if role in ("blocked", "ban", "заблокирован"):
            blocked.add(uid); continue

        if truthy(rec.get("blocked", "")):
            blocked.add(uid); continue
        if truthy(rec.get("admin", "")):
            admins.add(uid); allowed.add(uid); continue
        if "allowed" in rec and truthy(rec.get("allowed", "")):
            allowed.add(uid); continue

        # по умолчанию разрешаем
        allowed.add(uid)

    return allowed, admins, blocked

# ---------------- Initial load ----------------
def initial_load():
    ensure_fresh_data(force=True)
    try:
        a, ad, b = load_users_from_sheet()
        SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(a)
        SHEET_ADMINS.clear(); SHEET_ADMINS.update(ad)
        SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(b)
        logger.info(f"✅ USERS reload: allowed={len(SHEET_ALLOWED)} admins={len(SHEET_ADMINS)} blocked={len(SHEET_BLOCKED)}")
    except Exception as e:
        logger.warning(f"USERS load failed: {e}")

# async helper
import asyncio
async def asyncio_to_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

async def initial_load_async():
    await asyncio_to_thread(ensure_fresh_data, True)
    try:
        a, ad, b = await asyncio_to_thread(load_users_from_sheet)
        SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(a)
        SHEET_ADMINS.clear(); SHEET_ADMINS.update(ad)
        SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(b)
        logger.info(f"✅ USERS reload: allowed={len(SHEET_ALLOWED)} admins={len(SHEET_ADMINS)} blocked={len(SHEET_BLOCKED)}")
    except Exception as e:
        logger.warning(f"USERS load failed: {e}")
'''
out_path = Path("/mnt/data/data.py")
out_path.write_text(data_py, encoding="utf-8")
str(out_path), out_path.stat().st_size
