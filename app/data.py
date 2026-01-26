# app/data.py
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
        GOOGLE_APPLICATION_CREDENTIALS_JSON as _GA_CRED_FROM_CONFIG,
        TZ_NAME,
        DATA_TTL,
        USERS_TTL,
        IMAGE_STRICT,
        SAP_SHEET_NAME,
        USERS_SHEET_NAME,
        SEARCH_FIELDS,
    )
    SEARCH_COLUMNS = SEARCH_FIELDS
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"] = str(_GA_CRED_FROM_CONFIG or "")
except Exception:
    SPREADSHEET_URL = os.getenv("SPREADSHEET_URL", "")
    TZ_NAME = os.getenv("TIMEZONE", "Asia/Tashkent")
    DATA_TTL = int(os.getenv("DATA_TTL", "600"))
    USERS_TTL = int(os.getenv("USERS_TTL", "600"))
    IMAGE_STRICT = str(os.getenv("IMAGE_STRICT", "1")).strip().lower() in {"1", "true", "yes", "y", "да", "ok", "ок"}
    SAP_SHEET_NAME = os.getenv("SAP_SHEET_NAME", "SAP")
    USERS_SHEET_NAME = os.getenv("USERS_SHEET_NAME", "Пользователи")
    SEARCH_COLUMNS = [
        "тип",
        "наименование",
        "код",
        "oem",
        "изготовитель",
        "парт номер",
        "oem парт номер",
    ]

# приводим к пробельному виду (на случай парт_номер)
SEARCH_COLUMNS = [str(c).strip().lower().replace("_", " ") for c in (SEARCH_COLUMNS or [])]

GOOGLE_APPLICATION_CREDENTIALS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------- Глобальное состояние ----------
df: Optional[pd.DataFrame] = None
_last_load_ts: float = 0.0

_search_index: Dict[str, Set[int]] = {}
_image_index: Dict[str, str] = {}

# precomputed blobs for substring fallback (row_index -> text)
_row_blob: Dict[int, str] = {}

user_state: Dict[int, dict] = {}
issue_state: Dict[int, dict] = {}

SHEET_ALLOWED: Set[int] = set()
SHEET_ADMINS: Set[int] = set()
SHEET_BLOCKED: Set[int] = set()

_last_users_load_ts: float = 0.0

ASK_QUANTITY, ASK_COMMENT, ASK_CONFIRM = range(3)


# ---------- Утилиты ----------
def _norm_code(x: str) -> str:
    s = str(x or "").strip().lower()
    s = s.replace("o", "0")  # O -> 0
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def _norm_str(x: str) -> str:
    return str(x or "").strip().lower()


def now_local_str(tz_name: str = None) -> str:
    tz = ZoneInfo(tz_name or TZ_NAME or "Asia/Tashkent")
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def val(d: dict, key: str, default: str = "") -> str:
    return str(d.get(key, default) or default)


def _url_name_tokens(url: str) -> List[str]:
    try:
        path = re.sub(r"[?#].*$", "", str(url or ""))
        name = path.rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()
        return re.findall(r"[0-9a-zа-яё]+", name, flags=re.I)
    except Exception:
        return []


def _canon_header(h: str, idx: int) -> str:
    s = str(h or "").strip().lower().replace("_", " ")
    s = re.sub(r"\s+", " ", s, flags=re.U).strip()
    s = re.sub(r"[^\w\sа-яё]", "", s, flags=re.U).strip()
    if not s:
        s = f"col{idx+1}"
    return s


def _dedupe_headers(headers: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for i, h in enumerate(headers):
        base = _canon_header(h, i)
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base} {seen[base]}")
    return out


def _clean_query(q: str) -> str:
    """
    Нормализация запроса для substring fallback:
    - lower
    - пробелы схлопнуть
    - убрать мусор, но оставить буквы/цифры/пробел
    """
    s = str(q or "").lower()
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s, flags=re.U).strip()
    s = re.sub(r"[^\w\sа-яё0-9a-z]", " ", s, flags=re.I | re.U)
    s = re.sub(r"\s+", " ", s, flags=re.U).strip()
    return s


# ---------- Формат карточки ----------
def format_row(row: dict) -> str:
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
    if not SPREADSHEET_URL:
        raise RuntimeError("SPREADSHEET_URL не задан")

    client = get_gs_client()
    sh = client.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SAP_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = _dedupe_headers(values[0])
    rows = values[1:]

    new_df = pd.DataFrame(rows, columns=headers)

    # чистка значений
    for c in new_df.columns:
        new_df[c] = new_df[c].astype(str).fillna("").map(lambda x: str(x).strip())

    # нормализуем code-like поля
    for col in ("код", "oem", "парт номер", "oem парт номер"):
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(str).map(lambda x: str(x).strip().lower())

    if "image" in new_df.columns:
        new_df["image"] = new_df["image"].astype(str).map(lambda x: str(x).strip())

    return new_df


# ---------- Индексы ----------
def build_search_index(df_: pd.DataFrame) -> Dict[str, Set[int]]:
    idx: Dict[str, Set[int]] = {}
    cols = [c for c in SEARCH_COLUMNS if c in df_.columns]
    token_re = re.compile(r"[0-9a-zа-яё]+", flags=re.I)

    for i, row in df_.iterrows():
        for c in cols:
            raw = str(row.get(c, "") or "").strip().lower()
            if not raw:
                continue

            # code-like
            if c in ("код", "парт номер", "oem парт номер", "oem"):
                norm = _norm_code(raw)
                if norm:
                    idx.setdefault(norm, set()).add(i)

            # words/tokens (RU+EN+digits)
            for t in token_re.findall(raw):
                key = _norm_str(t)
                if key and len(key) >= 2:
                    idx.setdefault(key, set()).add(i)

    return idx


def build_row_blob(df_: pd.DataFrame) -> Dict[int, str]:
    """
    Для поиска 'как раньше' делаем текстовый blob по строке:
    - объединяем ключевые колонки
    - lower
    - с пробелами (без спецсимволов это не критично)
    """
    cols = [c for c in SEARCH_COLUMNS if c in df_.columns]
    blobs: Dict[int, str] = {}
    for i, row in df_.iterrows():
        parts = []
        for c in cols:
            v = str(row.get(c, "") or "").strip().lower()
            if v:
                parts.append(v)
        blobs[i] = " ".join(parts)
    return blobs


def build_image_index(df_: pd.DataFrame) -> Dict[str, str]:
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
            index.setdefault(_norm_code(t), url)
        join = "".join(tokens)
        if join:
            index.setdefault(_norm_code(join), url)
    return index


def ensure_fresh_data(force: bool = False):
    global df, _search_index, _image_index, _row_blob, _last_load_ts
    need = force or df is None or (time.time() - _last_load_ts > DATA_TTL)
    if not need:
        return

    new_df = _load_sap_dataframe()
    df = new_df
    _search_index = build_search_index(df)
    _row_blob = build_row_blob(df)
    _image_index = build_image_index(df)
    _last_load_ts = time.time()
    logger.info(f"✅ SAP reload: {len(df)} rows, index={len(_search_index)} keys, blobs={len(_row_blob)}, images={len(_image_index)} keys")


# ---------- Картинки ----------
async def find_image_by_code_async(code: str) -> str:
    ensure_fresh_data()
    if not code:
        return ""

    key = _norm_code(code)
    if not key:
        return ""

    hit = _image_index.get(key)
    if hit:
        return hit

    if IMAGE_STRICT:
        logger.info(f"[image][strict] нет совпадения по имени для кода: {key}")
        return ""

    try:
        if df is not None and "код" in df.columns and "image" in df.columns:
            for _, r in df.iterrows():
                if _norm_code(r.get("код", "")) == key:
                    u = str(r.get("image", "")).strip()
                    return u or ""
    except Exception as e:
        logger.warning(f"find_image_by_code_async soft fallback error: {e}")

    return ""


def normalize_drive_url(url: str) -> str:
    m = re.search(r"drive\.google\.com/(?:file/d/([-\w]{20,})|open\?id=([-\w]{20,}))", str(url or ""))
    if m:
        file_id = m.group(1) or m.group(2)
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return str(url or "")


async def resolve_ibb_direct_async(url: str) -> str:
    try:
        if re.search(r"^https?://i\.ibb\.co/", url, re.I):
            return url
        if not re.search(r"^https?://ibb\.co/", url, re.I):
            return url

        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return url
                html = await resp.text()

        m = re.search(
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            html,
            re.I
        )
        return m.group(1) if m else url
    except Exception as e:
        logger.warning(f"resolve_ibb_direct_async error: {e}")
        return url


async def resolve_image_url_async(url_raw: str) -> str:
    if not url_raw:
        return ""
    url = normalize_drive_url(url_raw)
    url = await resolve_ibb_direct_async(url)
    return url


# ---------- Поиск ----------
def _token_keys(raw_token: str) -> List[str]:
    t = str(raw_token or "").strip().lower()
    if not t:
        return []
    keys = []
    s = _norm_str(t)
    if s:
        keys.append(s)
    c = _norm_code(t)
    if c and c != s:
        keys.append(c)
    return keys


def match_row_by_index(tokens: List[str]) -> Set[int]:
    """
    1) Пытаемся через индекс (быстро)
    2) Если индекс пустой — fallback "как раньше": поиск по подстроке в blob по всей строке
    """
    ensure_fresh_data()
    if not tokens:
        return set()

    # ---- 1) индекс (AND, потом OR)
    per_token_sets: List[Set[int]] = []
    for t in tokens:
        keys = _token_keys(t)
        if not keys:
            continue
        u: Set[int] = set()
        for k in keys:
            u |= _search_index.get(k, set())
        per_token_sets.append(u)

    if per_token_sets:
        acc = per_token_sets[0].copy()
        for s in per_token_sets[1:]:
            acc &= s
        if acc:
            return acc

        found: Set[int] = set()
        for s in per_token_sets:
            found |= s
        if found:
            return found

    # ---- 2) fallback "как раньше" (substring)
    # собираем строку запроса целиком
    q = _clean_query(" ".join([str(t) for t in tokens]))
    if not q:
        return set()

    # если запрос короткий (1-2 символа) — не долбим таблицу
    if len(q) < 2:
        return set()

    hits: Set[int] = set()
    for i, blob in _row_blob.items():
        if q in blob:
            hits.add(i)

    return hits


# ---------- Экспорт ----------
def _df_to_xlsx(df_: pd.DataFrame, filename: str = "export.xlsx") -> io.BytesIO:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df_.to_excel(writer, index=False)
    buf.seek(0)
    return buf


# ---------- Пользователи ----------
def _parse_int(x) -> Optional[int]:
    try:
        v = int(str(x).strip())
        return v if v > 0 else None
    except Exception:
        return None


def load_users_from_sheet() -> Tuple[Set[int], Set[int], Set[int]]:
    allowed: Set[int] = set()
    admins: Set[int] = set()
    blocked: Set[int] = set()

    try:
        client = get_gs_client()
        sh = client.open_by_url(SPREADSHEET_URL)
        ws = sh.worksheet(USERS_SHEET_NAME)
    except Exception:
        logger.info("Лист пользователей отсутствует — пускаем всех по умолчанию")
        return allowed, admins, blocked

    all_vals = ws.get_all_values()
    if not all_vals:
        return allowed, admins, blocked

    headers = _dedupe_headers(all_vals[0])
    rows = all_vals[1:]

    recs: List[dict] = []
    for r in rows:
        recs.append({headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))})

    dfu = pd.DataFrame(recs)
    dfu.columns = [str(c).strip().lower().replace("_", " ") for c in dfu.columns]

    has_role = "role" in dfu.columns
    has_allowed = "allowed" in dfu.columns
    has_admin = "admin" in dfu.columns
    has_blocked = "blocked" in dfu.columns

    def truthy(v) -> bool:
        s = str(v).strip().lower()
        return s in ("1", "true", "да", "y", "yes")

    for _, r in dfu.iterrows():
        uid = _parse_int(r.get("user id") or r.get("user_id") or r.get("uid") or r.get("id"))
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


def ensure_fresh_users(force: bool = False):
    global _last_users_load_ts
    need = force or (time.time() - _last_users_load_ts > USERS_TTL) or (not SHEET_ALLOWED and not SHEET_ADMINS and not SHEET_BLOCKED)
    if not need:
        return

    try:
        allowed, admins, blocked = load_users_from_sheet()
        SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
        SHEET_ADMINS.clear(); SHEET_ADMINS.update(admins)
        SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)
        _last_users_load_ts = time.time()
        logger.info(f"✅ USERS reload: allowed={len(SHEET_ALLOWED)} admins={len(SHEET_ADMINS)} blocked={len(SHEET_BLOCKED)}")
    except Exception as e:
        logger.warning(f"ensure_fresh_users error: {e}")


# ---------- Async helper ----------
import asyncio
async def asyncio_to_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


# ---------- Backward-compat ----------
def initial_load():
    ensure_fresh_data(force=True)
    ensure_fresh_users(force=True)


async def initial_load_async():
    await asyncio_to_thread(ensure_fresh_data, True)
    await asyncio_to_thread(ensure_fresh_users, True)
