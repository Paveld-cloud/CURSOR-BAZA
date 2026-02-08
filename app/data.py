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

ASK_QUANTITY, ASK_COMMENT, ASK_CONFIRM = range(3)

# ---------- Утилиты ----------
def normalize(text: str) -> str:
    """Как в боте — нормализация текста для токенов поиска."""
    return re.sub(r"[^\w\s]", "", str(text or "").lower()).strip()

def squash(text: str) -> str:
    """Упрощение строки для нечёткого поиска."""
    return re.sub(r"[\W_]+", "", str(text or "").lower())

def norm_code(val: str) -> str:
    """Полная нормализация кода — как в боте."""
    s = str(val or "").strip().lower()
    s = s.replace("o", "0")
    return re.sub(r"[^a-z0-9]", "", s)

# совместимость
_norm_code = norm_code

def _url_name_tokens(url: str) -> List[str]:
    try:
        name = str(url).split("/")[-1].split(".")[0].lower()
        return re.findall(r"[a-z0-9]+", name)
    except:
        return []

def _safe_col(df_: pd.DataFrame, col: str) -> Optional[pd.Series]:
    if col not in df_.columns:
        return None
    return df_[col].astype(str).fillna("").str.strip().str.lower()

def val(d: dict, key: str, default: str = "") -> str:
    return str(d.get(key, default) or default)

def now_local_str(tz_name: str = "Asia/Tashkent") -> str:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
# ---------- Google Sheets ----------
def get_gs_client():
    if not GOOGLE_APPLICATION_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON не задан")

    try:
        info = json.loads(GOOGLE_APPLICATION_CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    except json.JSONDecodeError:
        creds = Credentials.from_service_account_file(
            GOOGLE_APPLICATION_CREDENTIALS_JSON,
            scopes=SCOPES
        )
    return gspread.authorize(creds)


def _load_sap_dataframe() -> pd.DataFrame:
    """Загрузка SAP-листа 1:1 как в боте — без преобразования типа данных."""
    client = get_gs_client()
    sh = client.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SAP_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = [c.strip().lower() for c in values[0]]
    rows = values[1:]

    df_ = pd.DataFrame(rows, columns=headers)

    # Нормализация кодов, OEM и парт-номеров — как в боте
    for col in ("код", "oem", "парт номер", "oem парт номер"):
        if col in df_.columns:
            df_[col] = df_[col].astype(str).str.strip().str.lower()

    if "image" in df_.columns:
        df_["image"] = df_["image"].astype(str).str.strip()

    return df_


# ---------- Индексы ----------
def build_search_index(df_: pd.DataFrame) -> Dict[str, Set[int]]:
    """1:1 как в Telegram боте — токены + нормализация кодов."""
    idx: Dict[str, Set[int]] = {}
    cols = [c for c in SEARCH_COLUMNS if c in df_.columns]

    for i, row in df_.iterrows():
        for c in cols:
            val_ = str(row.get(c, "")).lower()

            # нормализованный код / парт номер / OEM парт номер
            if c in ("код", "парт номер", "oem парт номер"):
                nc = norm_code(val_)
                if nc:
                    idx.setdefault(nc, set()).add(i)

            # токены a-z0-9
            for t in re.findall(r"[a-z0-9]+", val_):
                t = t.strip().lower()
                if t:
                    idx.setdefault(t, set()).add(i)

    return idx


def build_image_index(df_: pd.DataFrame) -> Dict[str, str]:
    """Индекс картинок — как в боте. Сканируются токены имени файла."""
    index: Dict[str, str] = {}
    if "image" not in df_.columns:
        return index

    skip = {"png", "jpg", "jpeg", "gif", "webp", "svg"}

    for _, row in df_.iterrows():
        url = str(row.get("image", "")).strip()
        if not url:
            continue

        tokens = _url_name_tokens(url)
        for t in tokens:
            if t in skip or len(t) < 3:
                continue
            index.setdefault(norm_code(t), url)

        # ключ на склеённом имени
        index.setdefault("".join(tokens), url)

    return index


def ensure_fresh_data(force: bool = False):
    """Обновление данных и индексов (по логике Telegram-бота)."""
    global df, _search_index, _image_index, _last_load_ts

    need = (
        force
        or df is None
        or (time.time() - _last_load_ts > DATA_TTL)
    )
    if not need:
        return

    logger.info("📥 Обновление SAP-данных из Google Sheets...")

    df = _load_sap_dataframe()
    _search_index = build_search_index(df)
    _image_index = build_image_index(df)
    _last_load_ts = time.time()

    logger.info(f"✅ Загружено {len(df)} строк, индексов: search={len(_search_index)}, images={len(_image_index)}")


# ---------- Поиск ----------
def match_row_by_index(tokens: List[str]) -> Set[int]:
    """Точный алгоритм Telegram-бота — AND → OR fallback."""
    ensure_fresh_data()

    if not tokens:
        return set()

    tokens_norm = [norm_code(t) for t in tokens if t]
    if not tokens_norm:
        return set()

    sets: List[Set[int]] = []

    # AND
    for t in tokens_norm:
        s = _search_index.get(t, set())
        if not s:
            sets = []
            break
        sets.append(s)

    if sets:
        acc = sets[0].copy()
        for s in sets[1:]:
            acc &= s
        return acc

    # OR fallback
    found: Set[int] = set()
    for t in tokens_norm:
        found |= _search_index.get(t, set())

    return found


def relevance_score(row: dict, tokens: List[str], q_squash: str) -> float:
    """Телеграм-бот ранжирование 1:1."""
    tkns = [t.lower() for t in tokens if t]
    if not tkns:
        return 0.0

    code = row.get("код", "").lower()
    name = row.get("наименование", "").lower()
    type_ = row.get("тип", "").lower()
    oem = row.get("oem", "").lower()
    manuf = row.get("изготовитель", "").lower()

    weights = {
        "код": 5.0,
        "наименование": 3.0,
        "тип": 2.0,
        "oem": 2.0,
        "изготовитель": 2.0,
    }
    fields = {
        "код": code,
        "наименование": name,
        "тип": type_,
        "oem": oem,
        "изготовитель": manuf,
    }

    score = 0.0

    # токены
    for f, text in fields.items():
        for t in tkns:
            if t in text:
                score += weights[f]

    # squash
    if q_squash:
        joined = squash(code + name + type_ + oem + manuf)
        if q_squash in joined:
            score += 10.0

    # сильный буст за совпадение кода
    if code == " ".join(tkns):
        score += 100.0

    for t in tkns:
        if code.startswith(t):
            score += 20.0

    return score


# ---------- Картинки ----------
async def resolve_ibb_direct_async(url: str) -> str:
    try:
        if url.startswith("https://i.ibb.co/"):
            return url
        if not url.startswith("https://ibb.co/"):
            return url

        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return url
                html = await resp.text()

        m = re.search(r'property="og:image" content="([^"]+)"', html)
        return m.group(1) if m else url

    except Exception:
        return url


async def resolve_image_url_async(url_raw: str) -> str:
    if not url_raw:
        return ""

    # google drive
    m = re.search(r"drive\.google\.com/(?:file/d/([-\w]{10,})|open\?id=([-\w]{10,}))", url_raw)
    if m:
        fid = m.group(1) or m.group(2)
        return f"https://drive.google.com/uc?export=download&id={fid}"

    # ibb.co
    return await resolve_ibb_direct_async(url_raw)


async def find_image_by_code_async(code: str) -> str:
    ensure_fresh_data()

    if not code:
        return ""

    key = norm_code(code)
    hit = _image_index.get(key)
    if hit:
        return hit

    # fallback — полный перебор
    try:
        if df is not None and "image" in df.columns:
            for url in df["image"]:
                url = str(url or "").strip()
                if not url:
                    continue

                tokens = _url_name_tokens(url)
                joined = "".join(tokens)

                if key in tokens or key in joined:
                    return url

    except Exception:
        pass

    return ""
# ---------- Экспорт ----------
def df_to_xlsx(df_: pd.DataFrame, filename: str = "export.xlsx") -> io.BytesIO:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df_.to_excel(writer, index=False)
    buf.seek(0)
    return buf


# ---------- Пользователи ----------
def _parse_int(x) -> Optional[int]:
    try:
        n = int(str(x).strip())
        return n if n > 0 else None
    except Exception:
        return None


def _normalize_header_name(h: str, idx: int) -> str:
    name = (h or "").strip().lower()
    name = re.sub(r"[^\w]+", "_", name).strip("_")
    if not name:
        name = f"col{idx}"
    return name


def _dedupe_headers(headers: List[str]) -> List[str]:
    seen = {}
    out = []
    for i, h in enumerate(headers):
        base = _normalize_header_name(h, i)
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out


def load_users_from_sheet() -> Tuple[Set[int], Set[int], Set[int]]:
    """
    Логика 1:1 как в Telegram-боте.
    Если нет листа пользователей — пропускаем и разрешаем всем.
    """
    allowed = set()
    admins = set()
    blocked = set()

    try:
        client = get_gs_client()
        sh = client.open_by_url(SPREADSHEET_URL)
        ws = sh.worksheet(USERS_SHEET_NAME)
    except Exception:
        logger.info("No users sheet — allow all")
        return allowed, admins, blocked

    vals = ws.get_all_values()
    if not vals:
        return allowed, admins, blocked

    headers = _dedupe_headers(vals[0])
    rows = vals[1:]

    records = []
    for r in rows:
        rec = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}
        records.append(rec)

    dfu = pd.DataFrame(records)
    dfu.columns = [c.strip().lower() for c in dfu.columns]

    has_role = "role" in dfu.columns
    has_allowed = "allowed" in dfu.columns
    has_admin = "admin" in dfu.columns
    has_blocked = "blocked" in dfu.columns

    def truthy(v) -> bool:
        return str(v).strip().lower() in ("1", "true", "да", "yes", "y")

    for _, r in dfu.iterrows():
        uid = _parse_int(r.get("user_id") or r.get("uid") or r.get("id"))
        if not uid:
            continue

        if has_role:
            role = str(r.get("role", "")).strip().lower()
            if role in ("admin", "админ"):
                admins.add(uid)
                allowed.add(uid)
            elif role in ("blocked", "ban", "заблокирован"):
                blocked.add(uid)
            else:
                allowed.add(uid)
            continue

        if has_blocked and truthy(r.get("blocked")):
            blocked.add(uid)
            continue

        if has_admin and truthy(r.get("admin")):
            admins.add(uid)
            allowed.add(uid)
            continue

        if has_allowed and truthy(r.get("allowed")):
            allowed.add(uid)
            continue

        allowed.add(uid)

    return allowed, admins, blocked


# ---------- Async helper ----------
import asyncio

async def asyncio_to_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        lambda: func(*args, **kwargs)
    )


# ---------- INITIAL LOAD (бот & mini app) ----------
def initial_load():
    """
    Синхронная загрузка — используется в Telegram-боте или тестах.
    """
    try:
        ensure_fresh_data(force=True)
    except Exception as e:
        logger.exception(f"initial_load failed: {e}")
        raise

    try:
        allowed, admins, blocked = load_users_from_sheet()
        SHEET_ALLOWED.clear();   SHEET_ALLOWED.update(allowed)
        SHEET_ADMINS.clear();    SHEET_ADMINS.update(admins)
        SHEET_BLOCKED.clear();   SHEET_BLOCKED.update(blocked)
    except Exception as e:
        logger.warning(f"initial_load: cannot load users: {e}")


async def initial_load_async():
    """
    Асинхронный вариант — используется в mini app (aiohttp + FastAPI + Flask async).
    """
    try:
        await asyncio_to_thread(ensure_fresh_data, True)
    except Exception as e:
        logger.exception(f"initial_load_async: data load failed: {e}")
        raise

    try:
        allowed, admins, blocked = await asyncio_to_thread(load_users_from_sheet)
        SHEET_ALLOWED.clear();   SHEET_ALLOWED.update(allowed)
        SHEET_ADMINS.clear();    SHEET_ADMINS.update(admins)
        SHEET_BLOCKED.clear();   SHEET_BLOCKED.update(blocked)
    except Exception as e:
        logger.warning(f"initial_load_async: cannot load users: {e}")
