# data.py — Вариант B (СТРОГИЙ поиск фото по коду)
# Полностью соответствует поведению Telegram-бота.

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

SHEET_ALLOWED: Set[int] = set()
SHEET_ADMINS: Set[int] = set()
SHEET_BLOCKED: Set[int] = set()

user_state: Dict[int, dict] = {}
issue_state: Dict[int, dict] = {}

ASK_QUANTITY, ASK_COMMENT, ASK_CONFIRM = range(3)

# ---------- Нормализация ----------
def norm_code(val: str) -> str:
    """Код приводится к виду, как в боте — строгий режим."""
    s = str(val or "").strip().lower()
    s = s.replace("o", "0")
    return re.sub(r"[^a-z0-9]", "", s)

# совместимость с webapp.py
_norm_code = norm_code


def normalize(txt: str) -> str:
    """Mini-App использует это для токенизации запроса."""
    return re.sub(r"[^\w\s-]", " ", str(txt).lower()).strip()


def now_local_str(tz_name: str = "Asia/Tashkent") -> str:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def val(d: dict, key: str, default: str = "") -> str:
    return str(d.get(key, default) or default)

# ---------- Google Sheets ----------
def get_gs_client():
    if not GOOGLE_APPLICATION_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON не задан")

    try:
        info = json.loads(GOOGLE_APPLICATION_CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    except json.JSONDecodeError:
        creds = Credentials.from_service_account_file(
            GOOGLE_APPLICATION_CREDENTIALS_JSON, scopes=SCOPES
        )
    return gspread.authorize(creds)


def _load_sap_dataframe() -> pd.DataFrame:
    """Загружаем таблицу ровно в том виде, как в Google Sheets."""
    client = get_gs_client()
    sh = client.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SAP_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = [c.strip().lower() for c in values[0]]
    rows = values[1:]
    df_new = pd.DataFrame(rows, columns=headers)

    # нормализация кодовых полей
    for col in ("код", "парт номер", "oem парт номер", "oem"):
        if col in df_new.columns:
            df_new[col] = df_new[col].astype(str).str.strip().str.lower()

    if "image" in df_new.columns:
        df_new["image"] = df_new["image"].astype(str).str.strip()

    return df_new
# ---------- Индексы ----------
def build_search_index(df_: pd.DataFrame) -> Dict[str, Set[int]]:
    """Создаем индекс по ВСЕМ SEARCH_COLUMNS (как в боте)."""
    idx: Dict[str, Set[int]] = {}
    cols = [c for c in SEARCH_COLUMNS if c in df_.columns]

    for i, row in df_.iterrows():
        for col in cols:
            raw_val = str(row.get(col, "")).lower()

            # строгий режим для кодовых колонок
            if col in ("код", "парт номер", "oem парт номер"):
                nk = norm_code(raw_val)
                if nk:
                    idx.setdefault(nk, set()).add(i)

            # токены a-z0-9
            for token in re.findall(r"[a-z0-9]+", raw_val):
                idx.setdefault(token, set()).add(i)

    return idx


def build_image_index(df_: pd.DataFrame) -> Dict[str, str]:
    """
    Строгое сопоставление фото-кодов:
    Мы не пытаемся угадывать название файла.
    Мы ищем КОД в ссылке напрямую.
    """
    index: Dict[str, str] = {}
    if "image" not in df_.columns:
        return index

    for _, row in df_.iterrows():
        code_raw = str(row.get("код", "")).strip().lower()
        url = str(row.get("image", "")).strip()

        if not code_raw or not url:
            continue

        key = norm_code(code_raw)
        if not key:
            continue

        # Строгий режим: ссылка подходит только если содержит нормализованный код
        url_low = url.lower().replace("-", "").replace("_", "")
        if key in url_low:
            index[key] = url

    return index


# ---------- Обновление данных ----------
def ensure_fresh_data(force: bool = False):
    global df, _search_index, _image_index, _last_load_ts

    need_reload = force or df is None or (time.time() - _last_load_ts > DATA_TTL)
    if not need_reload:
        return

    logger.info("📥 Обновление SAP-данных из Google Sheets...")
    df = _load_sap_dataframe()

    _search_index = build_search_index(df)
    _image_index = build_image_index(df)

    _last_load_ts = time.time()
    logger.info(
        f"✅ Загружено {len(df)} строк, индексов: search={len(_search_index)}, images={len(_image_index)}"
    )


# ---------- Работа с картинками ----------
async def resolve_ibb_direct_async(url: str) -> str:
    """Конвертация ibb.co → прямой i.ibb.co"""
    try:
        if url.startswith("https://i.ibb.co/"):
            return url

        if "ibb.co" not in url:
            return url

        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return url
                html = await resp.text()

        m = re.search(r'property="og:image" content="([^"]+)"', html)
        return m.group(1) if m else url

    except Exception as e:
        logger.warning(f"resolve_ibb_direct_async error: {e}")
        return url


def normalize_drive_url(url: str) -> str:
    """Google Drive → direct download"""
    m = re.search(r"/file/d/([-\w]+)/", str(url))
    if m:
        file_id = m.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url


async def resolve_image_url_async(url: str) -> str:
    if not url:
        return ""

    url = normalize_drive_url(url)
    url = await resolve_ibb_direct_async(url)
    return url


async def find_image_by_code_async(code: str) -> str:
    """Строгое сопоставление фото ПО КОДУ как в боте."""
    ensure_fresh_data()

    key = norm_code(code)
    if not key:
        return ""

    # 1) Быстрое попадание в индекс
    hit = _image_index.get(key)
    if hit:
        return hit

    # 2) Полный перебор — но только строгий
    try:
        if df is not None and "image" in df.columns:
            for url in df["image"]:
                u = str(url or "").lower().replace("-", "").replace("_", "")
                if key in u:
                    return url
    except Exception as e:
        logger.warning(f"find_image_by_code_async fallback error: {e}")

    return ""
# ---------- Поиск ----------
def match_row_by_index(tokens: List[str]) -> Set[int]:
    """
    Точная копия логики бота:
    - нормализуем токены
    - ищем по строгому совпадению нормализованных кодов
    - если AND не найден → fallback OR
    """
    ensure_fresh_data()

    if not tokens:
        return set()

    norm_tokens = [norm_code(t) for t in tokens if t.strip()]
    if not norm_tokens:
        return set()

    and_sets: List[Set[int]] = []
    for t in norm_tokens:
        s = _search_index.get(t, set())
        if not s:
            and_sets = []
            break
        and_sets.append(s)

    if and_sets:
        acc = and_sets[0].copy()
        for s in and_sets[1:]:
            acc &= s
        return acc

    # OR fallback
    out: Set[int] = set()
    for t in norm_tokens:
        out |= _search_index.get(t, set())
    return out


def _score_row(row: dict, tokens: List[str], q_join: str) -> float:
    """
    Как в боте: балльная оценка результата.
    """
    score = 0.0
    tkn = [t.lower() for t in tokens]

    code = str(row.get("код", "")).lower()
    name = str(row.get("наименование", "")).lower()
    type_ = str(row.get("тип", "")).lower()
    oem = str(row.get("oem", "")).lower()
    manuf = str(row.get("изготовитель", "")).lower()

    fields = {
        "код": code,
        "наименование": name,
        "тип": type_,
        "oem": oem,
        "изготовитель": manuf,
    }

    weights = {
        "код": 5,
        "наименование": 3,
        "тип": 2,
        "oem": 2,
        "изготовитель": 2,
    }

    for f, txt in fields.items():
        for t in tkn:
            if t in txt:
                score += weights[f]

    combo = (code + name + type_ + oem + manuf).replace(" ", "")
    if q_join in combo:
        score += 10

    if code.startswith(tokens[0].lower()):
        score += 20

    return score


def search_rows(q: str) -> List[dict]:
    """
    Основной поиск (как у Telegram-бота).
    """
    ensure_fresh_data()

    if not q or not q.strip():
        return []

    norm_q = normalize(q)
    tokens = norm_q.split()
    if not tokens:
        return []

    idxs = match_row_by_index(tokens)
    if not idxs:
        return []

    q_join = "".join(tokens)

    rows = []
    for i in idxs:
        row = df.iloc[i].to_dict()
        row["_score"] = _score_row(row, tokens, q_join)
        rows.append(row)

    rows.sort(key=lambda x: x["_score"], reverse=True)
    return rows[:100]


# ---------- Экспорт ----------
def df_to_xlsx(df_: pd.DataFrame) -> io.BytesIO:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df_.to_excel(writer, index=False)
    buf.seek(0)
    return buf


# ---------- Пользователи ----------
def _parse_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None


def _normalize_header_name(h: str, idx: int) -> str:
    name = (h or "").strip().lower()
    name = re.sub(r"[^\w]+", "_", name).strip("_")
    return name or f"col{idx+1}"


def _dedupe_headers(hdrs: List[str]) -> List[str]:
    out = []
    seen = {}
    for i, h in enumerate(hdrs):
        base = _normalize_header_name(h, i)
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out


def load_users_from_sheet() -> Tuple[Set[int], Set[int], Set[int]]:
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

    hdr = _dedupe_headers(vals[0])
    rows = vals[1:]
    recs = [{hdr[i]: (r[i] if i < len(r) else "") for i in range(len(hdr))} for r in rows]

    dfu = pd.DataFrame(recs)
    dfu.columns = [c.lower() for c in dfu.columns]

    def truth(v):
        s = str(v).strip().lower()
        return s in ("1", "true", "yes", "да")

    for _, r in dfu.iterrows():
        uid = _parse_int(r.get("user_id") or r.get("uid") or r.get("id"))
        if not uid:
            continue

        if "blocked" in r and truth(r["blocked"]):
            blocked.add(uid)
            continue
        if "admin" in r and truth(r["admin"]):
            admins.add(uid)
            allowed.add(uid)
            continue
        if "allowed" in r and truth(r["allowed"]):
            allowed.add(uid)
            continue

        allowed.add(uid)

    return allowed, admins, blocked


# ---------- Initial Load ----------
def initial_load():
    ensure_fresh_data(force=True)
    allowed, admins, blocked = load_users_from_sheet()
    SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
    SHEET_ADMINS.clear();  SHEET_ADMINS.update(admins)
    SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)


# ---------- Async Initial Load ----------
import asyncio

async def asyncio_to_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


async def initial_load_async():
    await asyncio_to_thread(ensure_fresh_data, True)
    allowed, admins, blocked = await asyncio_to_thread(load_users_from_sheet)
    SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
    SHEET_ADMINS.clear();  SHEET_ADMINS.update(admins)
    SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)

