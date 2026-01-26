import os

# =========================
# Основные настройки бота
# =========================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "")

PORT = int(os.getenv("PORT", "8080"))

# =========================
# Google Sheets
# =========================

# URL таблицы (обязательно)
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL", "").strip()

# Названия листов
SHEET_NAME = os.getenv("SHEET_NAME", "SAP")
USERS_SHEET = os.getenv("USERS_SHEET", "Пользователи")
HISTORY_SHEET = os.getenv("HISTORY_SHEET", "История")

# =========================
# Поиск и данные
# =========================

# Колонки, участвующие в поиске
SEARCH_FIELDS = [
    "code",
    "name",
    "type",
    "oem",
    "part_number",
    "manufacturer",
    "description",
]

# Размер страницы результатов
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "6"))

# Максимальное количество списания
MAX_QTY = int(os.getenv("MAX_QTY", "999"))

# Время жизни кеша данных (сек)
DATA_TTL = int(os.getenv("DATA_TTL", "600"))

# =========================
# Доступы и роли
# =========================

# Админы можно задать через env: 123,456
ADMINS = {
    int(x)
    for x in os.getenv("ADMINS", "").split(",")
    if x.strip().isdigit()
}

# =========================
# Медиа и UI
# =========================

WELCOME_ANIMATION_URL = os.getenv("WELCOME_ANIMATION_URL", "")
WELCOME_PHOTO_URL = os.getenv("WELCOME_PHOTO_URL", "")
WELCOME_MEDIA_ID = os.getenv("WELCOME_MEDIA_ID", "")

SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@support")

# =========================
# ВАЖНО: алиасы для data.py
# =========================
# data.py ожидает именно эти имена
# ничего не ломаем, просто синхронизируем

SAP_SHEET_NAME = os.getenv("SAP_SHEET_NAME", SHEET_NAME)
USERS_SHEET_NAME = os.getenv("USERS_SHEET_NAME", USERS_SHEET)

SEARCH_COLUMNS = SEARCH_FIELDS
