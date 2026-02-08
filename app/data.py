import os
import io
import re
import time
import json
import logging
from typing import Dict, Set, Tuple, List, Optional

import pandas as pd
import aiohttp
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger("bot.data")

# ---------- Конфиг ----------
try:
    from app.config import (
        SPREADSHEET_URL,
        SAP_SHEET_NAME,
        USERS_SHEET_NAME,
        DATA_TTL,
        SEARCH_COLUMNS,
    )
except Exception:
    SPREADSHEET_URL = os.getenv("SPREADSHEET_URL", "")
    SAP_SHEET_NAME = os.getenv("SAP_SHEET_NAME", "SAP")
    USERS_SHEET_NAME = os.getenv("USERS_SHEET_NAME", "Пользователи")
    DATA_TTL = int(os.getenv("DATA_TTL", "600"))
    SEARCH_COLUMNS = [
        "тип",
        "наименование",
        "код",
        "oem",
        "изготовитель",
        "парт номер",
        "oem парт номер",
    ]

GOOGLE_APPLICATION_CREDENTIALS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------- Глобальное состояние ----------
df: Optional[pd.DataFrame] = None
_last_load_ts: float = 0.0
_search_index: Dict[str, Set[int]] = {}
_image_index: Dict[str, str] = {}

user_state: Dict[int, dict] = {}
issue_state: Dict[int, dict] = {}

SHEET_ALLOWED: Set[int] = set()
SHEET_ADMINS: Set[int] = set()
SHEET_BLOCKED: Set[int] = set()

ASK_QUANTITY, ASK_COMMENT, ASK_CONFIRM = range(3)

# ---------- Утилиты ----------
def norm_code(val: str) -> str:
    """
    Унификация кодов:
    - нижний регистр
    - O → 0
    - убрать всё кроме a-z0-9
    """
    s = str(val or "").strip().lower()
    s = s.replace("o", "0")
    return re.sub(r"[^a-z0-9]", "", s)

def normalize(text: str) -> str:
    """Нормализация для поисковых токенов"""
    return re.sub(r"[^\w\s]", "", str(text or "").lower()).strip()

def squash(text: str) -> str:
    """Уплотнённая строка для поиска"""
    return re.sub(r"[^a-z0-9]", "", str(text or "").lower())

def now_local_str(tz_name: str = "Asia/Tashkent") -> str:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def val(d: dict, key: str, default: str = "") -> str:
    return str(d.get(key, default) or default)

def _url_name_tokens(url: str) -> List[str]:
    try:
        path = re.sub(r"[?#].*$", "", str(url or ""))
        name = path.rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()
        return re.findall(r"[a-z0-9]+", name)
    except Exception:
        return []

def _safe_col(df_: pd.DataFrame, col: str) -> Optional[pd.Series]:
    if col not in df_.columns:
        return None
    return df_[col].astype(str).fillna("").str.strip().str.lower()

# ---------- Формат карточки ----------
def format_row(row: dict) -> str:
    code = val(row, "код").upper()
    name = val(row, "наименование")
    type_ = val(row, "тип")
    part_no = val(row, "парт номер")
    oem_part = val(row, "oem парт номер")
    qty = val(row, "количество") or "—"
    price = val(row, "цена")
    currency = val(row, "валюта")
    manuf = val(row, "изготовитель")
    oem = val(row, "oem")

    lines: List[str] = []

    if code: lines.append(f"🔢 <b>Код:</b> {code}")
    if name: lines.append(f"📦 <b>Наименование:</b> {name}")
    if type_: lines.append(f"📎 <b>Тип:</b> {type_}")
    if part_no: lines.append(f"🧩 <b>Парт №:</b> {part_no}")
    if oem_part: lines.append(f"⚙️ <b>OEM №:</b> {oem_part}")
    lines.append(f"📦 <b>Кол-во:</b> {qty}")
    if price or currency: lines.append(f"💰 <b>Цена:</b> {price} {currency}".rstrip())
    if manuf: lines.append(f"🏭 <b>Изготовитель:</b> {manuf}")
    if oem: lines.append(f"🏷 OEM: {oem}")

    return "\n".join(lines)

# ---------- Google Sheets ----------
def get_gs_client():
    if not GOOGLE_APPLICATION_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON не задан")
    try:
        info = json.loads(GOOGLE_APPLICATION_CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    except json.JSONDecodeError:
        creds = Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS_JSON, scopes=SCOPES)
    return gspread.authorize(creds)

def _load_sap_dataframe() -> pd.DataFrame:
    client = get_gs_client()
    sh = client.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SAP_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = [c.strip().lower() for c in values[0]]
    rows = values[1:]
    new_df = pd.DataFrame(rows, columns=headers)

    for col in ("код", "oem", "парт номер", "oem парт номер"):
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(str).str.lower().str.strip()

    if "image" in new_df.columns:
        new_df["image"] = new_df["image"].astype(str).str.strip()

    return new_df

# ---------- Индексы ----------
def build_search_index(df_: pd.DataFrame) -> Dict[str, Set[int]]:
    idx: Dict[str, Set[int]] = {}

    for i, row in df_.iterrows():
        for c in SEARCH_COLUMNS:
            if c not in df_.columns:
                continue
            raw = str(row.get(c, "")).lower()
            tokens = re.findall(r"[a-z0-9]+", raw)

            for t in tokens:
                t_norm = norm_code(t)
                if t_norm:
                    idx.setdefault(t_norm, set()).add(i)

            if c in ("код", "парт номер", "oem парт номер"):
                cc = norm_code(raw)
                if cc:
                    idx.setdefault(cc, set()).add(i)

    return idx

def build_image_index(df_: pd.DataFrame) -> Dict[str, str]:
    """
    Жёсткая логика: из колонки image извлекаем код и сопоставляем.
    """
    index: Dict[str, str] = {}

    if "image" not in df_.columns:
        return index

    for _, row in df_.iterrows():
        url = str(row.get("image", "")).strip()
        if not url:
            continue

        tokens = _url_name_tokens(url)
        for t in tokens:
            k = norm_code(t)
            if len(k) >= 4:
                index[k] = url

    return index

def ensure_fresh_data(force: bool = False):
    global df, _search_index, _image_index, _last_load_ts

    need = force or df is None or (time.time() - _last_load_ts > DATA_TTL)
    if not need:
        return

    logger.info("📥 Обновление SAP-данных из Google Sheets...")

    df = _load_sap_dataframe()
    _search_index = build_search_index(df)
    _image_index = build_image_index(df)

    logger.info(f"✅ Загружено {len(df)} строк, индексов: search={len(_search_index)}, images={len(_image_index)}")

    _last_load_ts = time.time()

# ---------- Поиск ----------
def match_row_by_index(tokens: List[str]) -> Set[int]:
    ensure_fresh_data()

    out: Set[int] = set()

    for t in tokens:
        tt = norm_code(t)
        if not tt:
            continue
        found = _search_index.get(tt)
        if found:
            out |= found

    return out

# ---------- Изображение ----------
async def find_image_by_code_async(code: str) -> str:
    ensure_fresh_data()

    key = norm_code(code)
    if not key:
        return ""

    hit = _image_index.get(key)
    if hit:
        return hit

    try:
        for url in df["image"]:
            u = str(url or "")
            tokens = _url_name_tokens(u)
            if key in [norm_code(t) for t in tokens]:
                return u
    except Exception:
        pass

    logger.info(f"[image] нет изображения для кода {key}")
    return ""

# ---------- Пользователи ----------
def _parse_int(x):
    try:
        v = int(str(x).strip())
        return v if v > 0 else None
    except:
        return None

def load_users_from_sheet():
    try:
        client = get_gs_client()
        sh = client.open_by_url(SPREADSHEET_URL)
        ws = sh.worksheet(USERS_SHEET_NAME)
    except Exception:
        logger.info("No users sheet — allow all")
        return set(), set(), set()

    vals = ws.get_all_values()
    if not vals:
        return set(), set(), set()

    headers = vals[0]
    rows = vals[1:]
    dfu = pd.DataFrame(rows, columns=[h.lower().strip() for h in headers])

    allowed = set()
    admins = set()
    blocked = set()

    for _, r in dfu.iterrows():
        uid = _parse_int(r.get("user_id") or r.get("id"))
        if not uid:
            continue

        role = str(r.get("role", "")).strip().lower()

        if role == "admin":
            admins.add(uid)
            allowed.add(uid)
        elif role in ("blocked", "ban"):
            blocked.add(uid)
        else:
            allowed.add(uid)

    return allowed, admins, blocked

# ---------- Async helper ----------
import asyncio
async def asyncio_to_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

# ---------- Initial load ----------
def initial_load():
    ensure_fresh_data(force=True)

    allowed, admins, blocked = load_users_from_sheet()

    SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
    SHEET_ADMINS.clear(); SHEET_ADMINS.update(admins)
    SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)

async def initial_load_async():
    await asyncio_to_thread(ensure_fresh_data, True)

    allowed, admins, blocked = await asyncio_to_thread(load_users_from_sheet)

    SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
    SHEET_ADMINS.clear(); SHEET_ADMINS.update(admins)
    SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)

