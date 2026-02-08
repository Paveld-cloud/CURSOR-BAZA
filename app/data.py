# ============================================================
#  data.py — Variant B (полностью переписанная чистая версия)
#  Полная совместимость с Telegram-ботом и Mini-App
#  Поиск: строгий код → парт → OEM → расширенный индекс → fallback
#  Изображения: поиск по всем ссылкам image
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
from app.config import (
    SPREADSHEET_URL,
    SAP_SHEET_NAME,
    USERS_SHEET_NAME,
    DATA_TTL,
    SEARCH_COLUMNS,
)

GOOGLE_APPLICATION_CREDENTIALS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------------- GLOBAL STATE ----------------
df: Optional[pd.DataFrame] = None
_last_load_ts: float = 0

# Индексы
_search_index: Dict[str, Set[int]] = {}
_image_links: List[str] = []

# Пользователи
SHEET_ALLOWED: Set[int] = set()
SHEET_ADMINS: Set[int] = set()
SHEET_BLOCKED: Set[int] = set()

# Состояние списаний
user_state: Dict[int, dict] = {}
issue_state: Dict[int, dict] = {}

ASK_QUANTITY, ASK_COMMENT, ASK_CONFIRM = range(3)

# ============================================================
#                    Н О Р М А Л И З А Ц И Я
# ============================================================

def norm_code(val: str) -> str:
    """
    Нормализация кодов и парт-номеров:
    - нижний регистр
    - O → 0
    - только a-z0-9
    """
    s = str(val or "").strip().lower()
    s = s.replace("o", "0")
    return re.sub(r"[^a-z0-9]", "", s)


def norm_text(val: str) -> str:
    """Общая нормализация текста для индекса."""
    return str(val or "").strip().lower()


def squash(val: str) -> str:
    """Убираем все не-буквы/цифры для fallback-поиска."""
    return re.sub(r"[\W_]+", "", str(val or "").lower())


def tokenize(val: str) -> List[str]:
    """Токены a-z0-9."""
    return re.findall(r"[a-z0-9]+", str(val or "").lower())

# ============================================================
#               G O O G L E   S H E E T S
# ============================================================

def gs_client():
    info = json.loads(GOOGLE_APPLICATION_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def load_sap_df() -> pd.DataFrame:
    """
    Загружаем SAP-лист ТАК, как он отображён в Google Sheets.
    Это гарантирует точное совпадение цены.
    """
    sh = gs_client().open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SAP_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = [h.strip().lower() for h in values[0]]
    df_new = pd.DataFrame(values[1:], columns=headers)

    # Нормализация технических полей
    for col in ("код", "парт номер", "oem парт номер", "oem"):
        if col in df_new.columns:
            df_new[col] = df_new[col].astype(str).str.strip().str.lower()

    if "image" in df_new.columns:
        df_new["image"] = df_new["image"].astype(str).str.strip()

    return df_new
# ============================================================
#                        И Н Д Е К С Ы
# ============================================================

def build_search_index(df_: pd.DataFrame):
    """
    Индексируется ВСЁ, что перечислено в SEARCH_COLUMNS.
    Каждое значение разбивается на токены (a-z0-9).
    И создаётся карта token → set(row_index)
    """
    global _search_index
    _search_index = {}

    for i, row in df_.iterrows():
        for col in SEARCH_COLUMNS:
            if col not in df_.columns:
                continue
            value = str(row.get(col, "")).lower()

            # Токены по полю
            for t in tokenize(value):
                key = norm_code(t)
                if key:
                    _search_index.setdefault(key, set()).add(i)


# ============================================================
#                И Н Д Е К С   К А Р Т И Н О К
# ============================================================

def build_image_index(df_: pd.DataFrame):
    """
    Mini-App должен находить фото строго по коду детали,
    даже если код НЕ совпадает со строкой таблицы.
    Поэтому мы НЕ привязываемся к строкам:

    ➤ Собираем список всех ссылок image (даже если они ошибочно стоят в другой строке)
    ➤ При поиске: сравниваем norm_code(code) с токенами имени файла
    """
    global _image_links
    _image_links = []

    if "image" not in df_.columns:
        return

    for _, row in df_.iterrows():
        img = str(row.get("image", "")).strip()
        if img:
            _image_links.append(img)


async def resolve_image_url_async(url: str) -> str:
    """
    Приводим URL к прямому виду:
    - ibb.co → i.ibb.co
    - Google Drive → прямой доступ
    """
    if not url:
        return ""

    # ---- ibb ----
    if "ibb.co" in url and not "i.ibb.co" in url:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=8) as r:
                    html = await r.text()
            import re
            m = re.search(
                r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
                html, re.I
            )
            if m:
                return m.group(1)
        except Exception:
            pass

    # ---- Drive ----
    if "drive.google.com" in url:
        m = re.search(
            r"file/d/([-\w]{15,})|open\?id=([-\w]{15,})",
            url
        )
        if m:
            file_id = m.group(1) or m.group(2)
            return f"https://drive.google.com/uc?export=download&id={file_id}"

    return url


async def find_image_by_code_async(code: str) -> str:
    """
    Ищем КОД в имени файла КАЖДОЙ ссылки image.
    Это ключевое отличие Mini-App: работа НЕ по строкам, а по всему столбцу.
    """
    key = norm_code(code)
    if not key:
        return ""

    for url in _image_links:
        name = "".join(tokenize(url))
        if key in name:
            return await resolve_image_url_async(url)

    return ""


# ============================================================
#                   З А Г Р У З К А   Д А Н Н Ы Х
# ============================================================

def ensure_fresh_data(force=False):
    """
    Полная загрузка Google Sheets + индексы.
    """
    global df, _last_load_ts
    need = force or df is None or (time.time() - _last_load_ts > DATA_TTL)

    if not need:
        return

    df_new = load_sap_df()

    build_search_index(df_new)
    build_image_index(df_new)

    df = df_new
    _last_load_ts = time.time()

    logger.info(f"✔ Загружено {len(df)} строк, индексы обновлены")


# ============================================================
#               П Р О В Е Р К А   «ЭТО КОД ЛИ?»
# ============================================================

def is_exact_code(q: str) -> bool:
    """
    Для строгого поиска — если запрос «похоже на код».
    Нормализуем и проверяем длину.
    """
    nq = norm_code(q)
    return len(nq) >= 4  # оптимально для UZCSS00xxx / болтов / подшипников


# ============================================================
#       С Т Р О Г И Й   П О И С К  (код → парт → OEM)
# ============================================================

def strict_code_lookup(q: str) -> List[int]:
    """
    1) точное совпадение по 'код'
    2) если нет → точное по 'парт номер'
    3) если нет → точное по 'oem парт номер'
    """
    ensure_fresh_data()
    nq = norm_code(q)
    if not nq:
        return []

    hits = []

    # ----- 1) поиск по код -----
    if "код" in df.columns:
        mask = df["код"].apply(lambda x: norm_code(x) == nq)
        hits = df.index[mask].tolist()
        if hits:
            return hits

    # ----- 2) поиск по парт номер -----
    if "парт номер" in df.columns:
        mask = df["парт номер"].apply(lambda x: norm_code(x) == nq)
        hits = df.index[mask].tolist()
        if hits:
            return hits

    # ----- 3) поиск по OEM парт номер -----
    if "oem парт номер" in df.columns:
        mask = df["oem парт номер"].apply(lambda x: norm_code(x) == nq)
        hits = df.index[mask].tolist()
        if hits:
            return hits

    return []


# ============================================================
#       И Н Д Е К С Н Ы Й   П О И С К  (по SEARCH_COLUMNS)
# ============================================================

def index_lookup(q: str) -> Set[int]:
    ensure_fresh_data()
    keys = [norm_code(t) for t in tokenize(q)]
    hits: Set[int] = set()

    for k in keys:
        hits |= _search_index.get(k, set())

    return hits


# ============================================================
#          F A L L B A C K  —  contains / squash match
# ============================================================

def fallback_lookup(q: str) -> Set[int]:
    ensure_fresh_data()

    qs = squash(q)
    if not qs:
        return set()

    mask = pd.Series(False, index=df.index)
    for col in SEARCH_COLUMNS:
        if col not in df.columns:
            continue
        col_squash = df[col].astype(str).str.lower().str.replace(
            r"[\W_]+", "", regex=True
        )
        mask |= col_squash.str.contains(qs, na=False)

    return set(df.index[mask])


# ============================================================
#                R E L E V A N C E    S C O R E
# ============================================================

def relevance_score(row: dict, query: str) -> float:
    """
    Релевантность — комбинация:
    • вхождения токенов
    • начало строки
    • совпадение squash
    • точного соответствия кода
    """
    score = 0.0
    q = norm_text(query)
    qs = squash(query)
    tkns = tokenize(query)

    code = norm_text(row.get("код", ""))
    name = norm_text(row.get("наименование", ""))
    t = norm_text(row.get("тип", ""))

    full = code + name + t

    # Совпадение squash
    if qs and qs in squash(full):
        score += 10

    # Совпадение токенов
    for tk in tkns:
        if tk in code:
            score += 5
        if tk in name:
            score += 3
        if tk in t:
            score += 2

    # Начало кода
    if code.startswith(norm_code(query)):
        score += 20

    # Точный код
    if code == norm_code(query):
        score += 100

    return score
# ============================================================
#          Ф И Н А Л Ь Н Ы Й   П О И С К  (как в боте)
# ============================================================

def search_rows(query: str) -> List[dict]:
    """
    Объединённый поиск:
    1) строгий поиск по коду / парт / OEM
    2) индексный поиск по SEARCH_COLUMNS
    3) fallback (contains/squash)
    4) сортировка по релевантности
    """
    ensure_fresh_data()

    q = (query or "").strip()
    if not q:
        return []

    # ---- 1) строгий поиск — как в Telegram-боте ----
    strict_hits = strict_code_lookup(q)
    if strict_hits:
        return [df.loc[i].to_dict() for i in strict_hits]

    # ---- 2) индексный поиск ----
    idx_hits = index_lookup(q)

    # ---- 3) fallback ----
    fallback_hits = fallback_lookup(q)

    # объединяем
    all_hits = list(idx_hits | fallback_hits)
    if not all_hits:
        return []

    # ---- сортировка по relevance ----
    rows = [(i, df.loc[i].to_dict()) for i in all_hits]

    rows_sorted = sorted(
        rows,
        key=lambda x: relevance_score(x[1], q),
        reverse=True
    )

    return [r for _, r in rows_sorted]


# ============================================================
#             П У Б Л И Ч Н Ы Е   Ф У Н К Ц И И
# ============================================================

async def get_public_item(row: dict) -> dict:
    """
    Собирает результат для Mini-App:
    • строка
    • картинка (find_image_by_code)
    • text (format_row)
    """
    code = str(row.get("код", "")).strip()
    image_url = ""

    if code:
        try:
            image_url = await find_image_by_code_async(code)
        except Exception:
            image_url = ""

    return {
        **row,
        "image_url": image_url,
        "text": format_row(row)
    }


async def public_search(query: str) -> List[dict]:
    """
    API, используемое webapp API.
    """
    rows = search_rows(query)
    out = []
    for r in rows:
        out.append(await get_public_item(r))
    return out


async def public_get_by_code(code: str) -> Optional[dict]:
    """
    Детальная карточка по коду.
    """
    ensure_fresh_data()
    if "код" not in df.columns:
        return None

    norm = norm_code(code)
    if not norm:
        return None

    hits = df[df["код"].apply(lambda x: norm_code(x) == norm)]
    if hits.empty:
        return None

    row = hits.iloc[0].to_dict()
    return await get_public_item(row)


# ============================================================
#                  E X P O R T (xlsx)
# ============================================================

def export_df_to_xlsx() -> io.BytesIO:
    return _df_to_xlsx(df)


# ============================================================
#          П О Л Ь З О В А Т Е Л И   (allowed/admin/block)
# ============================================================

def load_users_from_sheet():
    """
    Полная логика работает так же, как в Telegram-боте.
    """
    global SHEET_ALLOWED, SHEET_ADMINS, SHEET_BLOCKED

    try:
        client = get_gs_client()
        sh = client.open_by_url(SPREADSHEET_URL)
        ws = sh.worksheet(USERS_SHEET_NAME)
    except Exception:
        logger.warning("Лист USERS не найден → доступ открыт всем по умолчанию")
        return set(), set(), set()

    data = ws.get_all_values()
    if not data:
        return set(), set(), set()

    headers = data[0]
    rows = data[1:]

    dfu = pd.DataFrame(rows, columns=[h.strip().lower() for h in headers])

    allowed = set()
    admins = set()
    blocked = set()

    def truth(v):
        return str(v).strip().lower() in ("1", "yes", "да", "true")

    for _, r in dfu.iterrows():
        uid = r.get("id") or r.get("user_id") or ""
        try:
            uid = int(str(uid).strip())
        except:
            continue

        if truth(r.get("blocked", "")):
            blocked.add(uid)
            continue
        if truth(r.get("admin", "")):
            admins.add(uid)
            allowed.add(uid)
            continue
        if truth(r.get("allowed", "")):
            allowed.add(uid)
            continue

        # по умолчанию
        allowed.add(uid)

    return allowed, admins, blocked


# ============================================================
#                 INITIAL LOAD  (sync/async)
# ============================================================

def initial_load():
    """
    Запускается в main.py при старте сервера.
    """
    ensure_fresh_data(force=True)

    try:
        allowed, admins, blocked = load_users_from_sheet()
        SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
        SHEET_ADMINS.clear(); SHEET_ADMINS.update(admins)
        SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)
    except Exception as e:
        logger.warning(f"initial_load: ошибка загрузки пользователей: {e}")


async def initial_load_async():
    import asyncio
    await asyncio.to_thread(ensure_fresh_data, True)
    allowed, admins, blocked = await asyncio.to_thread(load_users_from_sheet)

    SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
    SHEET_ADMINS.clear(); SHEET_ADMINS.update(admins)
    SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)
# ============================================================
#          Ф И Н А Л Ь Н Ы Й   П О И С К  (как в боте)
# ============================================================

def search_rows(query: str) -> List[dict]:
    """
    Объединённый поиск:
    1) строгий поиск по коду / парт / OEM
    2) индексный поиск по SEARCH_COLUMNS
    3) fallback (contains/squash)
    4) сортировка по релевантности
    """
    ensure_fresh_data()

    q = (query or "").strip()
    if not q:
        return []

    # ---- 1) строгий поиск — как в Telegram-боте ----
    strict_hits = strict_code_lookup(q)
    if strict_hits:
        return [df.loc[i].to_dict() for i in strict_hits]

    # ---- 2) индексный поиск ----
    idx_hits = index_lookup(q)

    # ---- 3) fallback ----
    fallback_hits = fallback_lookup(q)

    # объединяем
    all_hits = list(idx_hits | fallback_hits)
    if not all_hits:
        return []

    # ---- сортировка по relevance ----
    rows = [(i, df.loc[i].to_dict()) for i in all_hits]

    rows_sorted = sorted(
        rows,
        key=lambda x: relevance_score(x[1], q),
        reverse=True
    )

    return [r for _, r in rows_sorted]


# ============================================================
#             П У Б Л И Ч Н Ы Е   Ф У Н К Ц И И
# ============================================================

async def get_public_item(row: dict) -> dict:
    """
    Собирает результат для Mini-App:
    • строка
    • картинка (find_image_by_code)
    • text (format_row)
    """
    code = str(row.get("код", "")).strip()
    image_url = ""

    if code:
        try:
            image_url = await find_image_by_code_async(code)
        except Exception:
            image_url = ""

    return {
        **row,
        "image_url": image_url,
        "text": format_row(row)
    }


async def public_search(query: str) -> List[dict]:
    """
    API, используемое webapp API.
    """
    rows = search_rows(query)
    out = []
    for r in rows:
        out.append(await get_public_item(r))
    return out


async def public_get_by_code(code: str) -> Optional[dict]:
    """
    Детальная карточка по коду.
    """
    ensure_fresh_data()
    if "код" not in df.columns:
        return None

    norm = norm_code(code)
    if not norm:
        return None

    hits = df[df["код"].apply(lambda x: norm_code(x) == norm)]
    if hits.empty:
        return None

    row = hits.iloc[0].to_dict()
    return await get_public_item(row)


# ============================================================
#                  E X P O R T (xlsx)
# ============================================================

def export_df_to_xlsx() -> io.BytesIO:
    return _df_to_xlsx(df)


# ============================================================
#          П О Л Ь З О В А Т Е Л И   (allowed/admin/block)
# ============================================================

def load_users_from_sheet():
    """
    Полная логика работает так же, как в Telegram-боте.
    """
    global SHEET_ALLOWED, SHEET_ADMINS, SHEET_BLOCKED

    try:
        client = get_gs_client()
        sh = client.open_by_url(SPREADSHEET_URL)
        ws = sh.worksheet(USERS_SHEET_NAME)
    except Exception:
        logger.warning("Лист USERS не найден → доступ открыт всем по умолчанию")
        return set(), set(), set()

    data = ws.get_all_values()
    if not data:
        return set(), set(), set()

    headers = data[0]
    rows = data[1:]

    dfu = pd.DataFrame(rows, columns=[h.strip().lower() for h in headers])

    allowed = set()
    admins = set()
    blocked = set()

    def truth(v):
        return str(v).strip().lower() in ("1", "yes", "да", "true")

    for _, r in dfu.iterrows():
        uid = r.get("id") or r.get("user_id") or ""
        try:
            uid = int(str(uid).strip())
        except:
            continue

        if truth(r.get("blocked", "")):
            blocked.add(uid)
            continue
        if truth(r.get("admin", "")):
            admins.add(uid)
            allowed.add(uid)
            continue
        if truth(r.get("allowed", "")):
            allowed.add(uid)
            continue

        # по умолчанию
        allowed.add(uid)

    return allowed, admins, blocked


# ============================================================
#                 INITIAL LOAD  (sync/async)
# ============================================================

def initial_load():
    """
    Запускается в main.py при старте сервера.
    """
    ensure_fresh_data(force=True)

    try:
        allowed, admins, blocked = load_users_from_sheet()
        SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
        SHEET_ADMINS.clear(); SHEET_ADMINS.update(admins)
        SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)
    except Exception as e:
        logger.warning(f"initial_load: ошибка загрузки пользователей: {e}")


async def initial_load_async():
    import asyncio
    await asyncio.to_thread(ensure_fresh_data, True)
    allowed, admins, blocked = await asyncio.to_thread(load_users_from_sheet)

    SHEET_ALLOWED.clear(); SHEET_ALLOWED.update(allowed)
    SHEET_ADMINS.clear(); SHEET_ADMINS.update(admins)
    SHEET_BLOCKED.clear(); SHEET_BLOCKED.update(blocked)

