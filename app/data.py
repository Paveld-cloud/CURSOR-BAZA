# ============================================================
#   data.py — Финальная продакшен-версия (полностью совместима)
#   Поиск: strict code → part → OEM → SEARCH_COLUMNS → fallback
#   Изображения: поиск по всему столбцу image
#   Telegram Bot + Mini App совместимость
# ============================================================

import os
import re
import io
import json
import time
import asyncio
import logging
from typing import Dict, Set, List, Optional, Tuple

import pandas as pd
import aiohttp
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger("bot.data")

# ---------------- CONFIG ----------------
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

GOOGLE_APPLICATION_CREDENTIALS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------------- GLOBAL ----------------
df: Optional[pd.DataFrame] = None
_last_load_ts: float = 0

_search_index: Dict[str, Set[int]] = {}
_image_index: Dict[str, List[str]] = {}

SHEET_ALLOWED: Set[int] = set()
SHEET_ADMINS: Set[int] = set()
SHEET_BLOCKED: Set[int] = set()

user_state: Dict[int, dict] = {}
issue_state: Dict[int, dict] = {}

ASK_QUANTITY, ASK_COMMENT, ASK_CONFIRM = range(3)

# ============================================================
#                    NORMALIZATION
# ============================================================

def norm_code(val: str) -> str:
    """
    Приводит любой код к унифицированному виду:
    - нижний регистр
    - O → 0
    - убираем всё кроме a-z0-9
    """
    s = str(val or "").strip().lower()
    s = s.replace("o", "0")
    return re.sub(r"[^a-z0-9]", "", s)


def norm_text(val: str) -> str:
    """Упрощённая нормализация."""
    return str(val or "").strip().lower()


def squash(val: str) -> str:
    """Удаляет спецсимволы полностью."""
    return re.sub(r"[\W_]+", "", str(val or "").lower())


def tokenize(val: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", str(val or "").lower())


# --- ВАЖНО: для Mini App (совместимость с webapp.py) ---
def normalize(text: str) -> str:
    """
    Используется Mini-App.
    Убирает спецсимволы, приводит к нижнему регистру, оставляет буквы/цифры/пробелы.
    """
    text = str(text or "").lower()
    text = re.sub(r"[^\w\s]", "", text)
    return text.strip()


# ============================================================
#                 GOOGLE SHEETS LOADER
# ============================================================

def gs_client():
    info = json.loads(GOOGLE_APPLICATION_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def load_sap_df() -> pd.DataFrame:
    """
    Загружаем SAP-лист ТАК, как он отображён в Google Sheets.
    Полная совместимость с ценами и текстами.
    """
    sh = gs_client().open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SAP_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = [h.strip().lower() for h in values[0]]
    df_new = pd.DataFrame(values[1:], columns=headers)

    # Нормализация ключевых полей
    for col in ("код", "парт номер", "oem парт номер", "oem"):
        if col in df_new.columns:
            df_new[col] = df_new[col].astype(str).str.strip().str.lower()

    if "image" in df_new.columns:
        df_new["image"] = df_new["image"].astype(str).str.strip()

    return df_new
# ============================================================
#                 INDEX BUILDERS (SEARCH + IMAGE)
# ============================================================

def build_search_index(df_: pd.DataFrame) -> Dict[str, Set[int]]:
    """
    Строит глобальный поисковый индекс.
    Поддерживает:
    - код
    - парт номер
    - OEM парт номер
    - тип, наименование, oem, изготовитель
    Поля определяются через SEARCH_COLUMNS.
    """
    index: Dict[str, Set[int]] = {}
    cols = [c for c in SEARCH_COLUMNS if c in df_.columns]

    for i, row in df_.iterrows():
        for col in cols:
            raw = str(row.get(col, "")).lower()

            # Для кодов нормализуем отдельно
            if col in ("код", "парт номер", "oem парт номер"):
                key = norm_code(raw)
                if key:
                    index.setdefault(key, set()).add(i)

            # Токены общего текста
            for tok in tokenize(raw):
                index.setdefault(tok, set()).add(i)

    return index


# ------------------------------------------------------------
#                 IMAGE INDEX (strict match)
# ------------------------------------------------------------

def _image_tokens(url: str) -> List[str]:
    """
    Извлекает токены из имени файла.
    Например:
    https://.../UZ000664.jpg → ["uz000664"]
    """
    try:
        name = url.strip().lower().split("/")[-1]
        name = name.split("?")[0]
        name = name.split(".")[0]
        return tokenize(name)
    except Exception:
        return []


def build_image_index(df_: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Индекс изображений:
    key = norm_code(токен)
    value = список URL, где найдено совпадение.
    """
    index: Dict[str, List[str]] = {}
    if "image" not in df_.columns:
        return index

    for _, row in df_.iterrows():
        url = str(row.get("image", "")).strip()
        if not url:
            continue

        tokens = _image_tokens(url)
        for tok in tokens:
            key = norm_code(tok)
            if len(key) >= 3:   # отбрасываем мусор
                index.setdefault(key, []).append(url)

        # Также ключ = склеенное имя
        join_key = "".join(tokens)
        if len(join_key) >= 3:
            index.setdefault(join_key, []).append(url)

    return index


# ============================================================
#          LOAD + REFRESH (ensure_fresh_data)
# ============================================================

def ensure_fresh_data(force: bool = False):
    """
    Обновляет DataFrame и индексы, если TTL истёк.
    """
    global df, _search_index, _image_index, _last_load_ts

    need_reload = force or df is None or (time.time() - _last_load_ts > DATA_TTL)
    if not need_reload:
        return

    logger.info("📥 Обновление SAP-данных из Google Sheets...")
    df_new = load_sap_df()

    df = df_new
    _search_index = build_search_index(df)
    _image_index = build_image_index(df)

    _last_load_ts = time.time()
    logger.info(f"✅ Загружено {len(df)} строк, индексов: search={len(_search_index)}, images={len(_image_index)}")
# ============================================================
#                       SEARCH ENGINE
# ============================================================

def search_exact(df_: pd.DataFrame, q: str) -> List[int]:
    """
    1) Точное совпадение по КОДУ (norm_code)
    2) Если нет — точное совпадение по ПАРТ НОМЕРУ
    3) Если нет — точное совпадение по OEM ПАРТ НОМЕРУ
    Возвращает список индексов строк.
    """
    key = norm_code(q)
    if not key:
        return []

    hits = []

    # --- 1) код ---
    if "код" in df_.columns:
        for i, row in df_.iterrows():
            if norm_code(row.get("код", "")) == key:
                hits.append(i)

    if hits:
        return hits

    # --- 2) парт номер ---
    if "парт номер" in df_.columns:
        for i, row in df_.iterrows():
            if norm_code(row.get("парт номер", "")) == key:
                hits.append(i)

    if hits:
        return hits

    # --- 3) OEM парт номер ---
    if "oem парт номер" in df_.columns:
        for i, row in df_.iterrows():
            if norm_code(row.get("oem парт номер", "")) == key:
                hits.append(i)

    return hits


# ------------------------------------------------------------
#              INDEX MATCH (AND → OR)
# ------------------------------------------------------------

def search_index(tokens: List[str]) -> Set[int]:
    """
    Поиск по токенам через глобальный search_index.
    Логика:
    - если все токены есть в индексе: И (AND)
    - если хотя бы один отсутствует: переходим на ИЛИ (OR)
    """
    ensure_fresh_data()

    if not tokens:
        return set()

    normalized = [norm_code(t) for t in tokens if t]
    normalized = [t for t in normalized if t]

    if not normalized:
        return set()

    # AND phase
    sets = []
    for t in normalized:
        s = _search_index.get(t)
        if not s:
            sets = []
            break
        sets.append(s)

    if sets:
        result = sets[0].copy()
        for s in sets[1:]:
            result &= s
        return result

    # OR phase
    result = set()
    for t in normalized:
        result |= _search_index.get(t, set())

    return result


# ------------------------------------------------------------
#          FALLBACK SEARCH (слабый поиск по всем текстам)
# ------------------------------------------------------------

def search_fallback(df_: pd.DataFrame, q: str) -> List[int]:
    """
    Очень слабый поиск:
    - непрерывное вхождение q_squash в код, name, тип, oem
    Используется только если index match ничего не дал.
    """
    qsq = squash(q)
    if not qsq:
        return []

    out = []

    for i, row in df_.iterrows():
        code = squash(row.get("код", ""))
        name = squash(row.get("наименование", ""))
        typ = squash(row.get("тип", ""))
        oem = squash(row.get("oem", ""))

        combined = code + name + typ + oem
        if qsq in combined:
            out.append(i)

    return out


# ------------------------------------------------------------
#                        SCORING
# ------------------------------------------------------------

def relevance(row: dict, tokens: List[str], qsq: str) -> float:
    """
    Весовая модель:
    - обнаружение токена в коде → 5
    - в name → 3
    - в типе → 2
    - в OEM → 2
    - +10 за непрерывное вхождение squash
    - +100 за полное совпадение кода
    """
    tkns = [t.lower() for t in tokens if t.strip()]
    if not tkns:
        return 0.0

    code = str(row.get("код", "")).lower()
    name = str(row.get("наименование", "")).lower()
    typ  = str(row.get("тип", "")).lower()
    oem  = str(row.get("oem", "")).lower()

    score = 0

    for t in tkns:
        if t in code: score += 5
        if t in name: score += 3
        if t in typ:  score += 2
        if t in oem:  score += 2

    # squash match
    joined = squash(code + name + typ + oem)
    if qsq and qsq in joined:
        score += 10

    # exact code match
    if norm_code(code) == norm_code(" ".join(tkns)):
        score += 100

    return score


# ------------------------------------------------------------
#               ОБЪЕДИНЁННЫЙ ПОИСК (главная функция)
# ------------------------------------------------------------

def search_rows(q: str) -> List[int]:
    """
    Главная функция поиска как в боте:
    1) exact match
    2) index match (AND → OR)
    3) fallback squash
    4) сортировка по score
    """
    ensure_fresh_data()

    if not q.strip():
        return []

    tokens = q.split()
    qsq = squash(q)

    # 1) Точный поиск
    exact = search_exact(df, q)
    if exact:
        return exact

    # 2) Поиск по индексу
    idx_hits = list(search_index(tokens))
    if idx_hits:
        # сортировка по score
        return sorted(idx_hits, key=lambda i: -relevance(df.loc[i], tokens, qsq))

    # 3) fallback поиск
    fb = search_fallback(df, q)
    return fb
# ============================================================
#                       IMAGE RESOLUTION
# ============================================================

async def find_image_by_code_async(code: str) -> str:
    """
    Ищет ссылку на фото строго по КОДУ:
    1) По image_index (построено по filename tokens)
    2) Полный перебор image-столбца, ищем код в названии файла
    """
    ensure_fresh_data()

    if not code:
        return ""

    key = norm_code(code)
    if not key:
        return ""

    # 1) индекс
    url = _image_index.get(key)
    if url:
        return url

    # 2) fallback
    if df is not None and "image" in df.columns:
        for raw_url in df["image"]:
            u = str(raw_url or "").strip()
            if not u:
                continue

            tokens = url_name_tokens(u)
            joined = "".join(tokens)

            if key in tokens or key in joined:
                return u

    logger.info(f"[image] not found for code={key}")
    return ""


# -------------------------------------------------------------------
# Google Drive → direct URL
# -------------------------------------------------------------------

def normalize_drive_url(url: str) -> str:
    m = re.search(
        r"drive\.google\.com/(?:file/d/([-\w]{20,})|open\?id=([-\w]{20,}))",
        str(url or "")
    )
    if not m:
        return url

    file_id = m.group(1) or m.group(2)
    return f"https://drive.google.com/uc?export=download&id={file_id}"


# -------------------------------------------------------------------
# iBB.co → direct image resolver
# -------------------------------------------------------------------

async def resolve_ibb_direct_async(url: str) -> str:
    """
    Если ссылка ibb.co — достаём прямой og:image URL.
    """
    try:
        if re.search(r"^https?://i\.ibb\.co/", url, re.I):
            return url

        if not re.search(r"^https?://ibb\.co/", url, re.I):
            return url

        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as r:
                if r.status != 200:
                    return url
                html = await r.text()

        m = re.search(
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)',
            html, re.I
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


# ============================================================
#                        EXPORT
# ============================================================

def df_to_xlsx(df_: pd.DataFrame) -> io.BytesIO:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df_.to_excel(writer, index=False)
    buf.seek(0)
    return buf


# ============================================================
#                  USERS / PERMISSIONS
# ============================================================

def parse_int(x) -> Optional[int]:
    try:
        v = int(str(x).strip())
        return v if v > 0 else None
    except Exception:
        return None


def normalize_header(h: str, idx: int) -> str:
    h = (h or "").strip().lower()
    h = re.sub(r"[^\w]+", "_", h).strip("_")
    return h or f"col{idx+1}"


def dedupe_headers(headers: List[str]) -> List[str]:
    out = []
    seen = {}
    for i, h in enumerate(headers):
        base = normalize_header(h, i)
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out


def load_users_from_sheet() -> Tuple[Set[int], Set[int], Set[int]]:
    """
    Возвращает три множества:
    1) allowed
    2) admins
    3) blocked
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

    headers = dedupe_headers(vals[0])
    rows = vals[1:]

    recs = []
    for r in rows:
        recs.append({
            headers[i]: (r[i] if i < len(r) else "")
            for i in range(len(headers))
        })

    d = pd.DataFrame(recs)
    d.columns = [c.strip().lower() for c in d.columns]

    def truth(v):
        s = str(v).strip().lower()
        return s in ("1", "yes", "да", "true", "y")

    for _, r in d.iterrows():
        uid = parse_int(r.get("user_id") or r.get("uid") or r.get("id"))
        if not uid:
            continue

        # Ролевой режим
        if "role" in d.columns:
            role = str(r.get("role", "")).strip().lower()
            if role in ("admin", "админ"):
                admins.add(uid)
                allowed.add(uid)
            elif role in ("blocked", "ban", "заблокирован"):
                blocked.add(uid)
            else:
                allowed.add(uid)
            continue

        # Колонки allowed/admin/blocked
        if "blocked" in d.columns and truth(r.get("blocked")):
            blocked.add(uid)
            continue

        if "admin" in d.columns and truth(r.get("admin")):
            admins.add(uid)
            allowed.add(uid)
            continue

        if "allowed" in d.columns and truth(r.get("allowed")):
            allowed.add(uid)
            continue

        allowed.add(uid)

    return allowed, admins, blocked


# ============================================================
#                     INITIAL LOAD
# ============================================================

def initial_load():
    """Синхронная загрузка (для старта бота)."""
    global SHEET_ALLOWED, SHEET_ADMINS, SHEET_BLOCKED

    try:
        ensure_fresh_data(force=True)
    except Exception as e:
        logger.exception("initial_load: failed to load sheet")
        raise

    try:
        allowed, admins, blocked = load_users_from_sheet()
        SHEET_ALLOWED = set(allowed)
        SHEET_ADMINS = set(admins)
        SHEET_BLOCKED = set(blocked)
    except Exception as e:
        logger.warning(f"initial_load: user sheet failed: {e}")


import asyncio

async def asyncio_to_thread(func, *args, **kwargs):
    """Асинхронный вызов синхронной функции."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, lambda: func(*args, **kwargs)
    )


async def initial_load_async():
    """Асинхронная загрузка (для web-app)."""
    global SHEET_ALLOWED, SHEET_ADMINS, SHEET_BLOCKED

    try:
        await asyncio_to_thread(ensure_fresh_data, True)
    except Exception as e:
        logger.exception("initial_load_async: failed to load sheet")
        raise

    try:
        allowed, admins, blocked = await asyncio_to_thread(
            load_users_from_sheet
        )
        SHEET_ALLOWED = set(allowed)
        SHEET_ADMINS = set(admins)
        SHEET_BLOCKED = set(blocked)

    except Exception as e:
        logger.warning(f"initial_load_async: user sheet failed: {e}")

