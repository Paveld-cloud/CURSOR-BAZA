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

user_state: Dict[int, dict] = {}
issue_state: Dict[int, dict] = {}

SHEET_ALLOWED: Set[int] = set()
SHEET_ADMINS: Set[int] = set()
SHEET_BLOCKED: Set[int] = set()

ASK_QUANTITY, ASK_COMMENT, ASK_CONFIRM = range(3)


# ---------- Утилиты ----------
def _norm_code(x: str) -> str:
    s = str(x or "").strip().lower()
    s = s.replace("o", "0")
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def _norm_str(x: str) -> str:
    return str(x or "").strip().lower()


def now_local_str(tz_name: str = "Asia/Tashkent") -> str:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def val(d: dict, key: str, default: str = "") -> str:
    return str(d.get(key, default) or default)


def squash(text: str) -> str:
    return re.sub(r"[\W_]+", "", str(text or "").lower())


def normalize(text: str) -> str:
    return re.sub(r"[^\w\s]", "", str(text or "").lower()).strip()


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
    """
    Загружаем данные полностью, как в Google Sheets.
    Никаких преобразований значений, кроме нормализации кодов.
    """
    client = get_gs_client()
    sh = client.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SAP_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = [c.strip().lower() for c in values[0]]
    rows = values[1:]

    new_df = pd.DataFrame(rows, columns=headers)

    # нормализуем только ключевые поля
    for col in ("код", "oem", "парт номер", "oem парт номер"):
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(str).str.strip().str.lower()

    if "image" in new_df.columns:
        new_df["image"] = new_df["image"].astype(str).str.strip()

    return new_df


# ---------- Индекс поиска ----------
def build_search_index(df_: pd.DataFrame) -> Dict[str, Set[int]]:
    idx: Dict[str, Set[int]] = {}
    cols = [c for c in SEARCH_COLUMNS if c in df_.columns]

    for i, row in df_.iterrows():
        for c in cols:
            v = str(row.get(c, "")).lower()

            # Коды → нормализация
            if c in ("код", "парт номер", "oem парт номер"):
                norm = _norm_code(v)
                if norm:
                    idx.setdefault(norm, set()).add(i)

            # Токены
            for t in re.findall(r"[a-z0-9]+", v):
                t = _norm_str(t)
                if t:
                    idx.setdefault(t, set()).add(i)

    return idx


# ---------- Обновление данных ----------
def ensure_fresh_data(force: bool = False):
    global df, _search_index, _last_load_ts
    need = force or df is None or (time.time() - _last_load_ts > DATA_TTL)
    if not need:
        return

    new_df = _load_sap_dataframe()
    df = new_df
    _search_index = build_search_index(df)
    _last_load_ts = time.time()
    logger.info(f"✅ Перезагружено {len(df)} строк и построены индексы")


# ============================================================
#  ⭐⭐⭐   НОВАЯ СТРОГАЯ СИСТЕМА ПОИСКА ФОТО   ⭐⭐⭐
# ============================================================

def find_exact_image_in_column(code: str) -> str:
    """
    Ищем фото строго по коду через ВСЕ значения столбца image.
    - НЕ используем индекс
    - НЕ используем частичное совпадение токенов
    - НЕ подставляем фото из строки
    """
    ensure_fresh_data()

    if not code or df is None or "image" not in df.columns:
        return ""

    key = _norm_code(code)

    for url in df["image"]:
        if not url:
            continue
        u = str(url).strip().lower()
        if key in u:    # строго по вхождению кода
            return url

    return ""


async def find_image_by_code_async(code: str) -> str:
    """
    Новая версия:
    - ищет фото строго по коду
    - НЕ делает fallback
    - НЕ берёт фото из строки
    """
    if not code:
        return ""

    try:
        url = find_exact_image_in_column(code)
        if url:
            return url
    except Exception as e:
        logger.warning(f"[image strict] error: {e}")

    logger.info(f"[image strict] фото не найдено: {code}")
    return ""
# ---------- Поиск ----------
def match_row_by_index(tokens: List[str]) -> Set[int]:
    ensure_fresh_data()
    if not tokens:
        return set()

    tokens_norm = [_norm_code(t) for t in tokens if t]
    if not tokens_norm:
        return set()

    sets: List[Set[int]] = []
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


def _relevance_score(row: dict, tokens: List[str], q_squash: str) -> float:
    tkns = [_norm_str(t) for t in tokens if t]
    if not tkns:
        return 0.0

    code  = _norm_str(row.get("код", ""))
    name  = _norm_str(row.get("наименование", ""))
    ttype = _norm_str(row.get("тип", ""))
    oem   = _norm_str(row.get("oem", ""))
    manuf = _norm_str(row.get("изготовитель", ""))

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
        "тип": ttype,
        "oem": oem,
        "изготовитель": manuf,
    }

    score = 0.0

    for f, text in fields.items():
        for t in tkns:
            if t and (t in text):
                score += weights[f]

    if q_squash:
        joined = squash(code + name + ttype + oem + manuf)
        if q_squash in joined:
            score += 10.0

    q_full = " ".join(tkns)
    q_full_no_ws = squash(q_full)

    if code:
        if code == q_full:
            score += 100.0
        if code.startswith(q_full) or code.startswith(q_full_no_ws):
            score += 20.0
        for t in tkns:
            if code.startswith(t):
                score += 5.0

    return score


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


def _normalize_header_name(h: str, idx: int) -> str:
    name = (h or "").strip().lower()
    name = re.sub(r"[^\w]+", "_", name).strip("_")
    if not name:
        name = f"col{idx+1}"
    return name


def _dedupe_headers(headers: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
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

    headers_raw = all_vals[0]
    headers = _dedupe_headers(headers_raw)
    rows = all_vals[1:]

    recs = []
    for r in rows:
        recs.append({headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))})

    dfu = pd.DataFrame(recs)
    dfu.columns = [c.strip().lower() for c in dfu.columns]

    has_role = "role" in dfu.columns
    has_allowed = "allowed" in dfu.columns
    has_admin = "admin" in dfu.columns
    has_blocked = "blocked" in dfu.columns

    def truthy(v):
        s = str(v).strip().lower()
        return s in ("1", "true", "да", "y", "yes")

    for _, r in dfu.iterrows():
        uid = _parse_int(r.get("user_id") or r.get("uid") or r.get("id"))
        if not uid:
            continue

        if has_role:
            role = str(r.get("role", "")).strip().lower()
            if role in ("admin", "админ"):
                admins.add(uid); allowed.add(uid)
            elif role in ("blocked", "ban", "заблокирован"):
                blocked.add(uid)
            else:
                allowed.add(uid)
            continue

        if has_blocked and truthy(r.get("blocked")):
            blocked.add(uid); continue
        if has_admin and truthy(r.get("admin")):
            admins.add(uid); allowed.add(uid); continue
        if has_allowed and truthy(r.get("allowed")):
            allowed.add(uid); continue

        allowed.add(uid)

    return allowed, admins, blocked


# ---------- Async helper ----------
import asyncio
async def asyncio_to_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


# ---------- Initial Load ----------
def initial_load():
    try:
        ensure_fresh_data(force=True)
    except Exception as e:
        logger.exception(f"initial_load: ensure_fresh_data error: {e}")
        raise

    try:
        allowed, admins, blocked = load_users_from_sheet()
        SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
        SHEET_ADMINS.clear(); SHEET_ADMINS.update(admins)
        SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)
    except Exception as e:
        logger.warning(f"initial_load: не удалось загрузить пользователей: {e}")


async def initial_load_async():
    try:
        await asyncio_to_thread(ensure_fresh_data, True)
    except Exception as e:
        logger.exception(f"initial_load_async error: {e}")
        raise

    try:
        allowed, admins, blocked = await asyncio_to_thread(load_users_from_sheet)
        SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
        SHEET_ADMINS.clear(); SHEET_ADMINS.update(admins)
        SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)
    except Exception as e:
        logger.warning(f"initial_load_async: не удалось загрузить пользователей: {e}")
