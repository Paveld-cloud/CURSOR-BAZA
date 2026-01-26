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

SEARCH_COLUMNS = [str(c).strip().lower().replace("_", " ") for c in (SEARCH_COLUMNS or [])]

GOOGLE_APPLICATION_CREDENTIALS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------- Глобальное состояние ----------
df: Optional[pd.DataFrame] = None
_last_load_ts: float = 0.0

_search_index: Dict[str, Set[int]] = {}
_row_blob: Dict[int, str] = {}

# ВАЖНО: теперь это код -> url
_image_by_code: Dict[str, str] = {}
# доп. индекс по имени файла (оставляем как резерв)
_image_by_filename_token: Dict[str, str] = {}

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
    s = s.replace("o", "0")
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


def squash(text: str) -> str:
    return re.sub(r"[\W_]+", "", str(text or "").lower(), flags=re.U)


def normalize(text: str) -> str:
    # требуется webapp.py
    return re.sub(r"[^\w\s]", " ", str(text or "").lower(), flags=re.U).strip()


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
    s = str(q or "").lower()
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s, flags=re.U).strip()
    s = re.sub(r"[^\w\sа-яё0-9a-z]", " ", s, flags=re.I | re.U)
    s = re.sub(r"\s+", " ", s, flags=re.U).strip()
    return s


def _get_search_cols(df_: pd.DataFrame) -> List[str]:
    all_cols = [str(c).strip().lower() for c in df_.columns]
    all_cols = [c for c in all_cols if c and c not in {"image"}]

    preferred = [c for c in SEARCH_COLUMNS if c in all_cols]

    extra = []
    patt = re.compile(r"(part|pn|p\/n|oem|номер|part\s*no|material|код|code)", re.I)
    for c in all_cols:
        if c in preferred:
            continue
        if patt.search(c):
            extra.append(c)

    rest = [c for c in all_cols if c not in preferred and c not in extra]
    return preferred + extra + rest


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

    for c in new_df.columns:
        new_df[c] = new_df[c].astype(str).fillna("").map(lambda x: str(x).strip())

    # названия колонок -> lower
    new_df.columns = [str(c).strip().lower() for c in new_df.columns]

    # нормализуем ключевые поля (если есть)
    for col in ("код", "oem", "парт номер", "oem парт номер"):
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(str).map(lambda x: str(x).strip().lower())

    if "image" in new_df.columns:
        new_df["image"] = new_df["image"].astype(str).map(lambda x: str(x).strip())

    return new_df


# ---------- Индексы ----------
def build_search_index(df_: pd.DataFrame) -> Dict[str, Set[int]]:
    idx: Dict[str, Set[int]] = {}
    cols = _get_search_cols(df_)
    token_re = re.compile(r"[0-9a-zа-яё]+", flags=re.I)

    for i, row in df_.iterrows():
        for c in cols:
            raw = str(row.get(c, "") or "").strip().lower()
            if not raw:
                continue

            norm = _norm_code(raw)
            if norm and len(norm) >= 3:
                idx.setdefault(norm, set()).add(i)

            for t in token_re.findall(raw):
                key = _norm_str(t)
                if key and len(key) >= 2:
                    idx.setdefault(key, set()).add(i)

    return idx


def build_row_blob(df_: pd.DataFrame) -> Dict[int, str]:
    cols = _get_search_cols(df_)
    blobs: Dict[int, str] = {}
    for i, row in df_.iterrows():
        parts = []
        for c in cols:
            v = str(row.get(c, "") or "").strip().lower()
            if v:
                parts.append(v)
        blobs[i] = " ".join(parts)
    return blobs


def build_image_indexes(df_: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    1) PRIMARY: code -> image (строго по строке)
    2) SECONDARY: tokens-from-filename -> image (резерв)
    """
    by_code: Dict[str, str] = {}
    by_token: Dict[str, str] = {}

    if "image" not in df_.columns:
        return by_code, by_token

    skip = {"png", "jpg", "jpeg", "gif", "webp", "svg"}

    for _, row in df_.iterrows():
        url = str(row.get("image", "")).strip()
        if not url:
            continue

        # --- PRIMARY: код -> image
        if "код" in df_.columns:
            code_raw = str(row.get("код", "")).strip()
            code_key = _norm_code(code_raw)
            if code_key:
                # первый непустой побеждает (стабильно)
                by_code.setdefault(code_key, url)

        # --- SECONDARY: по имени файла (резерв)
        tokens = _url_name_tokens(url)
        for t in tokens:
            if t in skip or len(t) < 3:
                continue
            by_token.setdefault(_norm_code(t), url)
        join = "".join(tokens)
        if join:
            by_token.setdefault(_norm_code(join), url)

    return by_code, by_token


def ensure_fresh_data(force: bool = False):
    global df, _search_index, _row_blob, _image_by_code, _image_by_filename_token, _last_load_ts
    need = force or df is None or (time.time() - _last_load_ts > DATA_TTL)
    if not need:
        return

    new_df = _load_sap_dataframe()
    df = new_df
    _search_index = build_search_index(df)
    _row_blob = build_row_blob(df)
    _image_by_code, _image_by_filename_token = build_image_indexes(df)
    _last_load_ts = time.time()

    logger.info(
        f"✅ SAP reload: {len(df)} rows, index={len(_search_index)} keys, blobs={len(_row_blob)}, "
        f"img_by_code={len(_image_by_code)}, img_by_name={len(_image_by_filename_token)}"
    )


# ---------- Картинки ----------
async def find_image_by_code_async(code: str) -> str:
    """
    ТВОЯ ЛОГИКА:
    - всегда сначала ищем по коду: код -> image (из столбца image в той же строке)
    - если пусто:
        IMAGE_STRICT=1  -> ничего не возвращаем
        IMAGE_STRICT=0  -> можно попробовать резерв по имени файла
    """
    ensure_fresh_data()
    if not code:
        return ""

    key = _norm_code(code)
    if not key:
        return ""

    # PRIMARY: по коду (из строки)
    url = _image_by_code.get(key)
    if url:
        return url

    if IMAGE_STRICT:
        return ""

    # SECONDARY: по имени файла/токенам (резерв)
    url = _image_by_filename_token.get(key, "")
    return url or ""


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
    ensure_fresh_data()
    if not tokens:
        return set()

    # 1) индекс (AND, потом OR)
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

    # 2) fallback "как раньше" — подстрока по всей строке
    q = _clean_query(" ".join([str(t) for t in tokens]))
    if not q or len(q) < 2:
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


