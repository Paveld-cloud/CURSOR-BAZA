# app/data.py
import os
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

logger = logging.getLogger("bot.data")

# ---------------- Config ----------------
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
    SEARCH_COLUMNS = [
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

# ИСТИНА по фото: key=имя файла без расширения (UZ000662), value=url
_image_file_index: Dict[str, str] = {}

SHEET_ALLOWED: Set[int] = set()
SHEET_ADMINS: Set[int] = set()
SHEET_BLOCKED: Set[int] = set()

# совместимость с handlers.py
ASK_QUANTITY, ASK_COMMENT, ASK_CONFIRM = range(3)

_ALLOWED_EXTS = {".png", ".jpg", ".jpeg", ".webp"}

# ---------------- Helpers ----------------
def _norm_code(x: str) -> str:
    s = str(x or "").strip().lower()
    s = s.replace("o", "0")
    return re.sub(r"[^a-z0-9]", "", s)

def normalize(text: str) -> str:
    return re.sub(r"[^\w\s]+", " ", str(text or "").lower(), flags=re.U).strip()

def squash(text: str) -> str:
    return re.sub(r"[\W_]+", "", str(text or "").lower(), flags=re.U)

def now_local_str() -> str:
    tz = ZoneInfo(TIMEZONE)
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

# ---------------- Google Sheets ----------------
def get_gs_client():
    if not GOOGLE_APPLICATION_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON not set")

    try:
        info = json.loads(GOOGLE_APPLICATION_CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    except json.JSONDecodeError:
        creds = Credentials.from_service_account_file(
            GOOGLE_APPLICATION_CREDENTIALS_JSON, scopes=SCOPES
        )
    return gspread.authorize(creds)

def _load_sap_dataframe() -> pd.DataFrame:
    client = get_gs_client()
    sh = client.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SAP_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = [h.strip().lower() for h in values[0]]
    rows = values[1:]
    df_ = pd.DataFrame(rows, columns=headers)

    for col in df_.columns:
        df_[col] = df_[col].astype(str).fillna("").str.strip()

    return df_

# ---------------- Index builders ----------------
def build_search_index(df_: pd.DataFrame) -> Dict[str, Set[int]]:
    idx: Dict[str, Set[int]] = {}
    if df_ is None or df_.empty:
        return idx

    cols = [c for c in SEARCH_COLUMNS if c in df_.columns]

    for i, row in df_.iterrows():
        for c in cols:
            raw = str(row.get(c, "")).strip().lower()
            if not raw:
                continue

            if c in ("код", "парт номер", "oem парт номер"):
                k = _norm_code(raw)
                if k:
                    idx.setdefault(k, set()).add(i)

            for t in re.findall(r"[a-z0-9]+", raw):
                t = _norm_code(t)
                if t:
                    idx.setdefault(t, set()).add(i)

    return idx

def build_image_file_index(df_: pd.DataFrame) -> Dict[str, str]:
    """
    ГЛАВНАЯ логика фото при рандомных ссылках в строках:
    ищем по ВСЕМ значениям столбца image:
    basename(url) без расширения -> url
    """
    out: Dict[str, str] = {}
    if df_ is None or df_.empty or "image" not in df_.columns:
        return out

    for url in df_["image"].astype(str).fillna("").tolist():
        u = str(url).strip()
        if not u:
            continue
        try:
            path = urlparse(u).path
            fname = os.path.basename(path)
            name, ext = os.path.splitext(fname)
            if not name or ext.lower() not in _ALLOWED_EXTS:
                continue
            key = name.strip().upper()
            out.setdefault(key, u)
        except Exception:
            continue

    return out

# ---------------- Reload ----------------
def ensure_fresh_data(force: bool = False):
    global df, _last_load_ts, _search_index, _image_file_index

    if not force and df is not None and (time.time() - _last_load_ts) < DATA_TTL:
        return

    df = _load_sap_dataframe()
    _search_index = build_search_index(df)
    _image_file_index = build_image_file_index(df)

    _last_load_ts = time.time()

    logger.info(
        f"✅ SAP reload: rows={len(df)}, search_keys={len(_search_index)}, images_by_filename={len(_image_file_index)}"
    )

# ---------------- Image API ----------------
def find_image_url_by_code_strict(code: str) -> str:
    """
    ТОЛЬКО так:
    1) берём код
    2) ищем точное совпадение по имени файла в столбце image (по всем строкам)
    3) если нет — возвращаем '' (значит фото нет)
    """
    ensure_fresh_data()
    code_raw = str(code or "").strip()
    if not code_raw:
        return ""

    url = _image_file_index.get(code_raw.upper(), "")
    if not url:
        logger.info(f"[image] no exact match for code={code_raw}")
    return url

async def find_image_by_code_async(code: str) -> str:
    return find_image_url_by_code_strict(code)

# ---------------- URL resolve ----------------
def normalize_drive_url(url: str) -> str:
    m = re.search(
        r"drive\.google\.com/(?:file/d/([-\w]{20,})|open\?id=([-\w]{20,}))", str(url or "")
    )
    if not m:
        return str(url or "")
    file_id = m.group(1) or m.group(2)
    return f"https://drive.google.com/uc?export=download&id={file_id}"

async def resolve_ibb_direct_async(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
    if re.search(r"^https?://i\.ibb\.co/", u, re.I):
        return u
    if not re.search(r"^https?://ibb\.co/", u, re.I):
        return u

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(u, timeout=10) as r:
                html = await r.text()

        m = re.search(
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            html,
            re.I,
        )
        return m.group(1) if m else u
    except Exception:
        return u

async def resolve_image_url_async(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
    u = normalize_drive_url(u)
    u = await resolve_ibb_direct_async(u)
    return u or ""

# ---------------- Search ----------------
def match_row_by_index(tokens: List[str]) -> Set[int]:
    ensure_fresh_data()
    if not tokens:
        return set()

    keys = [_norm_code(t) for t in tokens if t]
    keys = [k for k in keys if k]
    if not keys:
        return set()

    sets = []
    for k in keys:
        if k not in _search_index:
            sets = []
            break
        sets.append(_search_index[k])

    if sets:
        res = sets[0].copy()
        for s in sets[1:]:
            res &= s
        return res

    out: Set[int] = set()
    for k in keys:
        out |= _search_index.get(k, set())
    return out

# ---------------- Users ----------------
def _parse_int(x) -> Optional[int]:
    try:
        v = int(str(x).strip())
        return v if v > 0 else None
    except Exception:
        return None

def load_users_from_sheet() -> Tuple[Set[int], Set[int], Set[int]]:
    allowed, admins, blocked = set(), set(), set()

    try:
        client = get_gs_client()
        sh = client.open_by_url(SPREADSHEET_URL)
        ws = sh.worksheet(USERS_SHEET_NAME)
    except Exception:
        return allowed, admins, blocked

    rows = ws.get_all_values()
    if not rows:
        return allowed, admins, blocked

    headers = [h.strip().lower() for h in rows[0]]
    for r in rows[1:]:
        rec = {headers[i]: r[i] if i < len(r) else "" for i in range(len(headers))}
        uid = _parse_int(rec.get("user_id") or rec.get("id"))
        if not uid:
            continue

        role = str(rec.get("role", "")).strip().lower()
        if role == "admin":
            admins.add(uid); allowed.add(uid)
        elif role == "blocked":
            blocked.add(uid)
        else:
            allowed.add(uid)

    return allowed, admins, blocked

# ---------------- Init ----------------
def initial_load():
    ensure_fresh_data(force=True)
    try:
        a, ad, b = load_users_from_sheet()
        SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(a)
        SHEET_ADMINS.clear(); SHEET_ADMINS.update(ad)
        SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(b)
        logger.info(f"✅ USERS reload: allowed={len(SHEET_ALLOWED)} admins={len(SHEET_ADMINS)} blocked={len(SHEET_BLOCKED)}")
    except Exception as e:
        logger.warning(f"Users load failed: {e}")

# async helpers
import asyncio

async def asyncio_to_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

async def initial_load_async():
    await asyncio_to_thread(initial_load)

