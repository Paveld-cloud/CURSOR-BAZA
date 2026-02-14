# app/handlers.py
import io
import math
import re
import asyncio
import logging
from html import escape

import pandas as pd
import aiohttp  # для байтового фолбэка изображений
from telegram import (
    Update,
    InputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
    MenuButtonWebApp,
)
from telegram.ext import (
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
    ApplicationHandlerStop,
)

# Конфиг
from app.config import (
    PAGE_SIZE,
    MAX_QTY,
    WELCOME_ANIMATION_URL,
    WELCOME_PHOTO_URL,
    SUPPORT_CONTACT,
    WELCOME_MEDIA_ID,
    ADMINS,
    WEBHOOK_URL,
)

# ВАЖНО: работаем через модуль, чтобы всегда видеть актуальные данные
import app.data as data

logger = logging.getLogger("bot.handlers")

# ---------- Кнопки ----------
def cancel_markup():
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ Отменить", callback_data="cancel_action")]]
    )


def confirm_markup():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Да, списать", callback_data="confirm_yes"),
                InlineKeyboardButton("❌ Нет", callback_data="confirm_no"),
            ],
            [InlineKeyboardButton("❌ Отменить", callback_data="cancel_action")],
        ]
    )


def more_markup():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Ещё", callback_data="more")]])


def main_menu_markup():
    """Улучшенное главное меню"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔍 Поиск", callback_data="menu_search"),
            InlineKeyboardButton("📂 Категории", callback_data="menu_categories")
        ],
        [
            InlineKeyboardButton("📜 История", callback_data="menu_history"),
            InlineKeyboardButton("📊 Экспорт", callback_data="menu_export")
        ],
        [
            InlineKeyboardButton("📦 Как списать", callback_data="menu_issue_help"),
        ],
        [
            InlineKeyboardButton("ℹ️ Помощь", callback_data="menu_help"),
            InlineKeyboardButton("📞 Поддержка", callback_data="menu_contact")
        ]
    ])


# ---------- Mini App ----------
def _mini_app_url() -> str:
    base = (WEBHOOK_URL or "").strip().rstrip("/")
    if not base:
        return ""
    if not base.startswith("http://") and not base.startswith("https://"):
        base = "https://" + base
    return base + "/app"


def mini_app_markup():
    url = _mini_app_url()
    if not url:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("⚠️ Mini App URL не задан", callback_data="noop")]]
        )
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📦 Открыть каталог (Mini App)", web_app=WebAppInfo(url=url))]]
    )


# ---------- Безопасная отправка HTML ----------
async def _safe_send_html_message(bot, chat_id: int, text: str, **kwargs):
    """
    Универсальная отправка сообщения с HTML.
    При ошибке парсинга HTML — пытаемся отправить текст без тегов.
    """
    try:
        return await bot.send_message(
            chat_id=chat_id, text=text, parse_mode="HTML", **kwargs
        )
    except Exception as e:
        logger.warning(f"HTML message parse failed, fallback to plain: {e}")
        no_tags = re.sub(r"</?(b|i|code)>", "", text)
        kwargs.pop("parse_mode", None)
        return await bot.send_message(chat_id=chat_id, text=no_tags, **kwargs)


# --------------------- Пользователи: допуски -----------------
async def ensure_users_async(force: bool = False):
    allowed, admins, blocked = await asyncio.to_thread(data.load_users_from_sheet)
    data.SHEET_ALLOWED.clear()
    data.SHEET_ALLOWED.update(allowed)
    data.SHEET_ADMINS.clear()
    data.SHEET_ADMINS.update(admins)
    data.SHEET_BLOCKED.clear()
    data.SHEET_BLOCKED.update(blocked)


def ensure_users(force: bool = False):
    asyncio.create_task(ensure_users_async(force=True))


def is_admin(uid: int) -> bool:
    ensure_users()
    return uid in data.SHEET_ADMINS or uid in ADMINS


def is_allowed(uid: int) -> bool:
    ensure_users()
    if uid in data.SHEET_BLOCKED:
        return False
    if data.SHEET_ALLOWED:
        return (
            uid in data.SHEET_ALLOWED
            or uid in data.SHEET_ADMINS
            or uid in ADMINS
        )
    return True


# --------------------- Гварды -----------------
async def guard_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user and not is_allowed(user.id):
        try:
            await update.effective_message.reply_text("Доступ запрещён.")
        except Exception:
            pass
        raise ApplicationHandlerStop


async def guard_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user and not is_allowed(user.id):
        try:
            await update.callback_query.answer("Доступ запрещён.", show_alert=True)
        except Exception:
            pass
        raise ApplicationHandlerStop


# --------------------- Приветствие -----------------
async def send_welcome_sequence(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    first = escape((user.first_name or "").strip() or "коллега")

    card_html = (
        f"👋 <b>Привет, {first}!</b>\n\n"
        f"Добро пожаловать в <b>систему поиска запчастей</b> 🛠️\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"✨ <b>Что я умею:</b>\n\n"
        f"🔍 <b>Умный поиск</b>\n"
        f"   • По названию, коду или парт номеру\n"
        f"   • Быстрый доступ через категории\n"
        f"   • Сохранение истории поиска\n\n"
        f"📦 <b>Управление деталями</b>\n"
        f"   • Просмотр карточек с фото\n"
        f"   • Списание с подтверждением\n"
        f"   • Экспорт результатов в Excel\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"🧪 <b>Пример запроса:</b>\n"
        f"<code>PI8808DRG500</code>\n\n"
        f"🚀 <i>Выберите действие из меню ниже!</i>"
    )

    try:
        if WELCOME_MEDIA_ID:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=WELCOME_MEDIA_ID,
                caption=card_html,
                parse_mode="HTML",
                reply_markup=main_menu_markup(),
            )
            return
        if WELCOME_PHOTO_URL:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=WELCOME_PHOTO_URL,
                caption=card_html,
                parse_mode="HTML",
                reply_markup=main_menu_markup(),
            )
            return
        if WELCOME_ANIMATION_URL:
            await context.bot.send_animation(
                chat_id=chat_id,
                animation=WELCOME_ANIMATION_URL,
                caption=card_html,
                parse_mode="HTML",
                reply_markup=main_menu_markup(),
            )
            return
    except Exception as e:
        logger.warning(f"Welcome message with media failed: {e}")

    await _safe_send_html_message(
        context.bot, chat_id, card_html, reply_markup=main_menu_markup()
    )


# --------------------- /getfileid -----------------
async def getfileid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting_fileid"] = True
    await update.message.reply_text(
        "📸 Пришлите фото/анимацию/видео/документ одним сообщением — верну его file_id."
    )


async def media_fileid_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_fileid"):
        return

    msg = update.message
    file_id = None
    kind = None

    if msg.photo:
        file_id = msg.photo[-1].file_id
        kind = "фото"
    elif msg.video:
        file_id = msg.video.file_id
        kind = "видео"
    elif msg.animation:
        file_id = msg.animation.file_id
        kind = "анимация"
    elif msg.document:
        file_id = msg.document.file_id
        kind = "документ"

    if file_id:
        context.user_data["awaiting_fileid"] = False
        await msg.reply_text(
            f"✅ Получил {kind}. Вот ваш file_id:\n\n<code>{file_id}</code>\n\n"
            f"👉 Скопируйте значение в переменную окружения WELCOME_MEDIA_ID и перезапустите бота.",
            parse_mode="HTML",
        )
    else:
        await msg.reply_text(
            "Не удалось определить file_id. Пришлите фото/анимацию/видео/документ этим же сообщением."
        )


# --------------------- Фото карточки -----------------
async def _send_photo_with_fallback(bot, chat_id: int, url: str, caption: str, reply_markup):
    """
    Отправка фото с подписью + HTML. При ошибке с URL — фолбэк через байты.
    """
    try:
        return await bot.send_photo(
            chat_id=chat_id,
            photo=url,
            caption=caption,
            reply_markup=reply_markup,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning(f"send_photo(url) failed: {e}")

    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                content = await resp.read()

        bio = io.BytesIO(content)
        bio.name = "image.jpg"
        return await bot.send_photo(
            chat_id=chat_id,
            photo=InputFile(bio),
            caption=caption,
            reply_markup=reply_markup,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning(f"byte fallback failed: {e}")
        return None


async def send_row_with_image(update: Update, row: dict, text: str):
    """
    Отправка карточки через update: если есть фото — фото + caption (HTML),
    если нет — текстовое сообщение через _safe_send_html_message.
    """
    code = str(row.get("код", "")).strip().lower()
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📦 Взять деталь", callback_data=f"issue:{code}")]]
    )

    bot = update.get_bot()
    chat_id = update.effective_chat.id

    url_raw = await data.find_image_by_code_async(code)
    if not url_raw:
        logger.info(f"[image] нет записи в индексе для кода: {code}")
        return await _safe_send_html_message(
            bot,
            chat_id,
            "📄 (без фото)\n" + text,
            reply_markup=kb,
        )

    url = await data.resolve_image_url_async(url_raw)
    if not url:
        logger.info(f"[image] резолв не прошёл для кода {code}: {url_raw}")
        return await _safe_send_html_message(
            bot,
            chat_id,
            "📄 (без фото)\n" + text,
            reply_markup=kb,
        )

    sent = await _send_photo_with_fallback(bot, chat_id, url, text, kb)
    if sent:
        return

    logger.warning(f"[image] не удалось отправить фото для кода {code} (url={url})")
    await _safe_send_html_message(
        bot,
        chat_id,
        "📄 (без фото)\n" + text,
        reply_markup=kb,
    )


async def send_row_with_image_bot(bot, chat_id: int, row: dict, text: str):
    """
    То же, что send_row_with_image, но используется, когда у нас уже есть bot и chat_id.
    """
    code = str(row.get("код", "")).strip().lower()
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📦 Взять деталь", callback_data=f"issue:{code}")]]
    )

    url_raw = await data.find_image_by_code_async(code)
    if not url_raw:
        logger.info(f"[image] нет записи в индексе для кода: {code}")
        return await _safe_send_html_message(
            bot,
            chat_id,
            "📄 (без фото)\n" + text,
            reply_markup=kb,
        )

    url = await data.resolve_image_url_async(url_raw)
    if not url:
        logger.info(f"[image] резолв не прошёл для кода {code}: {url_raw}")
        return await _safe_send_html_message(
            bot,
            chat_id,
            "📄 (без фото)\n" + text,
            reply_markup=kb,
        )

    sent = await _send_photo_with_fallback(bot, chat_id, url, text, kb)
    if sent:
        return

    logger.warning(f"[image] не удалось отправить фото (bot) для кода {code} (url={url})")
    await _safe_send_html_message(
        bot,
        chat_id,
        "📄 (без фото)\n" + text,
        reply_markup=kb,
    )


# --------------------- Меню (callbacks) -----------------
async def menu_search_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    msg = (
        "🔍 Введите запрос: <i>название</i>/<i>модель</i>/<i>код</i>.\n"
        "Пример: <code>PI 8808 DRG 500</code>"
    )
    await _safe_send_html_message(context.bot, q.message.chat_id, msg)


async def menu_issue_help_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    msg = (
        "<b>Как списать деталь</b>:\n"
        "1) Выполните поиск по названию/коду.\n"
        "2) В карточке нажмите «📦 Взять деталь».\n"
        "3) Укажите количество и комментарий.\n"
        "4) Подтвердите списание кнопкой «Да»."
    )
    await _safe_send_html_message(context.bot, q.message.chat_id, msg)


async def menu_contact_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.message.reply_text(f"{SUPPORT_CONTACT}")


async def noop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()


# --------------------- Команды -----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    data.issue_state.pop(uid, None)
    data.user_state.pop(uid, None)

    await send_welcome_sequence(update, context)

    if update.message:
        await asyncio.sleep(0.2)
        cmds_html = (
            "<b>Команды</b>:\n"
            "• /help — помощь\n"
            "• /more — показать ещё\n"
            "• /export — выгрузка результатов (XLSX/CSV)\n"
            "• /cancel — отменить списание\n"
            "• /reload — перезагрузка данных и пользователей (только админ)\n"
            "• /broadcast — рассылка сообщения всем пользователям (только админ)\n"
            "• /app — открыть каталог (Mini App)\n"
        )
        await _safe_send_html_message(
            context.bot, update.effective_chat.id, cmds_html
        )

        # Mini App кнопка
        await _safe_send_html_message(
            context.bot,
            update.effective_chat.id,
            "📦 <b>Каталог деталей (Mini App)</b> — открыть в Telegram:",
            reply_markup=mini_app_markup(),
        )

        # (опционально) постоянная кнопка меню Telegram
        try:
            url = _mini_app_url()
            if url:
                await context.bot.set_chat_menu_button(
                    chat_id=update.effective_chat.id,
                    menu_button=MenuButtonWebApp(
                        text="📦 Каталог",
                        web_app=WebAppInfo(url=url),
                    ),
                )
        except Exception as e:
            logger.warning(f"MenuButtonWebApp failed: {e}")


async def app_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = _mini_app_url()
    if not url:
        return await update.message.reply_text("WEBHOOK_URL не задан — не могу открыть Mini App.")
    await update.message.reply_text("📦 Открыть каталог деталей:", reply_markup=mini_app_markup())


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "<b>Как пользоваться</b>:\n"
        "1) Выполните поиск по названию/модели/коду.\n"
        "2) В карточке нажмите «📦 Взять деталь» — бот спросит количество и комментарий.\n"
        "3) Подтвердите списание (Да/Нет).\n"
        "<i>У вас всё получится!</i>"
    )
    await _safe_send_html_message(context.bot, update.effective_chat.id, msg)


async def reload_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return await update.message.reply_text("Доступ запрещён.")
    data.ensure_fresh_data(force=True)
    ensure_users(force=True)
    await update.message.reply_text(
        "✅ Данные и пользователи перезагружены (в фоне)."
    )


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if data.issue_state.pop(uid, None):
        await update.message.reply_text("❌ Операция списания отменена.")
    else:
        await update.message.reply_text("Нет активной операции.")


async def export_cmd(update: Update, Context):
    uid = update.effective_user.id
    st = data.user_state.get(uid, {})
    results = st.get("results")
    if results is None or results.empty:
        return await update.message.reply_text("Сначала выполните поиск.")

    import datetime

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        buf = await asyncio.to_thread(
            data._df_to_xlsx, results, f"export_{timestamp}.xlsx"
        )
        await update.message.reply_document(
            InputFile(buf, filename=f"export_{timestamp}.xlsx")
        )
    except Exception as e:
        logger.warning(f"Не удалось XLSX (fallback CSV): {e}")
        csv = results.to_csv(index=False, encoding="utf-8-sig")
        await update.message.reply_document(
            InputFile(
                io.BytesIO(csv.encode("utf-8-sig")),
                filename=f"export_{timestamp}.csv",
            )
        )


async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /broadcast Текст сообщения
    Доступно только админам. Рассылает текст всем пользователям
    из SHEET_ALLOWED и SHEET_ADMINS (кроме SHEET_BLOCKED).
    """
    user = update.effective_user
    uid = user.id

    if not is_admin(uid):
        return await update.message.reply_text("Доступ запрещён.")

    if not context.args:
        return await update.message.reply_text(
            "Использование:\n"
            "/broadcast Текст сообщения\n\n"
            "Например:\n"
            "/broadcast В боте обновлён поиск и отображение цен."
        )

    text = " ".join(context.args).strip()
    if not text:
        return await update.message.reply_text("Текст рассылки пустой.")

    # Получатели: разрешённые + админы + ADMINS, минус заблокированные
    recipients = (data.SHEET_ALLOWED | data.SHEET_ADMINS | set(ADMINS)) - data.SHEET_BLOCKED

    if not recipients:
        return await update.message.reply_text(
            "Список получателей пуст. Проверь лист пользователей."
        )

    await update.message.reply_text(
        f"Начинаю рассылку по {len(recipients)} пользователям..."
    )

    ok = 0
    fail = 0

    for chat_id in recipients:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
            ok += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            fail += 1
            logger.warning(f"Broadcast to {chat_id} failed: {e}")

    await update.message.reply_text(
        f"Рассылка завершена.\nУспешно: {ok}\nОшибок: {fail}"
    )


# --------------------- Поиск -----------------
async def send_page(update: Update, uid: int):
    st = data.user_state.get(uid, {})
    results = st.get("results")
    page = st.get("page", 0)

    total = len(results)
    if total == 0:
        return await update.message.reply_text("Результатов больше нет.")
    pages = max(1, math.ceil(total / PAGE_SIZE))
    if page >= pages:
        st["page"] = pages - 1
        return await update.message.reply_text("Больше результатов нет.")

    start = page * PAGE_SIZE
    end = min(start + PAGE_SIZE, total)

    await update.message.reply_text(
        f"Стр. {page+1}/{pages}. Показываю {start + 1}–{end} из {total}."
    )
    for _, row in results.iloc[start:end].iterrows():
        await send_row_with_image(
            update, row.to_dict(), data.format_row(row.to_dict())
        )
    if end < total:
        await update.message.reply_text("Показать ещё?", reply_markup=more_markup())


async def send_page_via_bot(bot, chat_id: int, uid: int):
    st = data.user_state.get(uid, {})
    results = st.get("results")
    page = st.get("page", 0)

    total = len(results)
    if total == 0:
        return await bot.send_message(chat_id=chat_id, text="Результатов больше нет.")
    pages = max(1, math.ceil(total / PAGE_SIZE))
    if page >= pages:
        st["page"] = pages - 1
        return await bot.send_message(chat_id=chat_id, text="Больше результатов нет.")

    start = page * PAGE_SIZE
    end = min(start + PAGE_SIZE, total)

    await bot.send_message(
        chat_id=chat_id,
        text=f"Стр. {page+1}/{pages}. Показываю {start + 1}–{end} из {total}.",
    )
    for _, row in results.iloc[start:end].iterrows():
        await send_row_with_image_bot(
            bot, chat_id, row.to_dict(), data.format_row(row.to_dict())
        )
    if end < total:
        await bot.send_message(
            chat_id=chat_id, text="Показать ещё?", reply_markup=more_markup()
        )


async def search_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    if context.chat_data.pop("suppress_next_search", False):
        return

    uid = update.effective_user.id
    st_issue = data.issue_state.get(uid)
    if st_issue:
        if "quantity" not in st_issue:
            return await update.message.reply_text(
                "Вы вводите количество. Введите число или нажмите «Отменить».",
                reply_markup=cancel_markup(),
            )
        if st_issue.get("await_comment"):
            return await update.message.reply_text(
                "Вы вводите комментарий. Напишите текст или «-», либо нажмите «Отменить».",
                reply_markup=cancel_markup(),
            )

    q = update.message.text.strip()
    if not q:
        return await update.message.reply_text("Введите запрос.")

    # Сохраняем запрос в историю
    st = data.user_state.setdefault(uid, {})
    history = st.setdefault("search_history", [])
    if q not in history[-5:]:  # Избегаем дубликатов в последних 5
        history.append(q)
        if len(history) > 50:  # Ограничиваем размер истории
            history = history[-50:]
        st["search_history"] = history

    # Базовые токены и "склеенная" фраза
    tokens = data.normalize(q).split()
    q_squash = data.squash(q)
    norm_code = data._norm_code(q)  # "LR 7000" -> "lr7000"

    # Гарантируем наличие данных
    if data.df is None:
        await asyncio.to_thread(data.ensure_fresh_data, True)
        if data.df is None:
            return await update.message.reply_text("Ошибка загрузки данных.")
    df_ = data.df

    # 1) Строгий поиск по нормализованному коду
    if norm_code:
        matched_indices = data.match_row_by_index([norm_code])
    else:
        matched_indices = data.match_row_by_index(tokens)

    # 2) Фолбэк: AND внутри поля, OR по полям
    if not matched_indices:
        mask_any = pd.Series(False, index=df_.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель"]:
            series = data._safe_col(df_, col)
            if series is None:
                continue
            field_mask = pd.Series(True, index=df_.index)
            for t in tokens:
                if t:
                    field_mask &= series.str.contains(re.escape(t), na=False)
            mask_any |= field_mask
        matched_indices = set(df_.index[mask_any])

    # 3) Фразовый поиск по склеенным полям
    if not matched_indices and q_squash:
        mask_any = pd.Series(False, index=df_.index)
        for col in ["тип", "наименование", "код", "oem", "изготовитель"]:
            series = data._safe_col(df_, col)
            if series is None:
                continue
            series_sq = series.str.replace(r"[\W_]+", "", regex=True)
            mask_any |= series_sq.str.contains(re.escape(q_squash), na=False)
        matched_indices = set(df_.index[mask_any])

    if not matched_indices:
        return await update.message.reply_text(
            f"По запросу «{q}» ничего не найдено."
        )

    results_df = df_.loc[list(matched_indices)].copy()

    # Сортировка по релевантности
    scores = []
    for _, r in results_df.iterrows():
        scores.append(
            data._relevance_score(
                r.to_dict(),
                tokens + ([norm_code] if norm_code else []),
                q_squash,
            )
        )
    results_df["__score"] = scores

    if "код" in results_df.columns:
        results_df = results_df.sort_values(
            by=["__score", "код"],
            ascending=[False, True],
            key=lambda s: s
            if s.name != "код"
            else s.astype(str).str.len(),
        )
    else:
        results_df = results_df.sort_values(by=["__score"], ascending=False)
    results_df = results_df.drop(columns="__score")

    st["query"] = q
    st["results"] = results_df
    st["page"] = 0

    await send_page(update, uid)


async def more_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    st = data.user_state.get(uid, {})
    results = st.get("results")
    if results is None or results.empty:
        return await update.message.reply_text("Сначала выполните поиск.")
    st["page"] = st.get("page", 0) + 1
    await send_page(update, uid)


# ------------------ Списание -----------------
async def on_issue_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    code = q.data.split(":", 1)[1].strip().lower()

    found = None
    if data.df is not None and "код" in data.df.columns:
        hit = data.df[data.df["код"].astype(str).str.lower() == code]
        if not hit.empty:
            found = hit.iloc[0].to_dict()

    if not found:
        return await q.edit_message_text(
            "Не удалось найти деталь по коду. Выполните поиск заново."
        )

    data.issue_state[uid] = {"part": found}
    await q.message.reply_text(
        "Сколько списать? Укажите число (например: 1 или 2.5).",
        reply_markup=cancel_markup(),
    )
    return data.ASK_QUANTITY


async def handle_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.chat_data["suppress_next_search"] = True
    uid = update.effective_user.id
    text = (update.message.text or "").strip().replace(",", ".")
    try:
        qty = float(text)
        if not math.isfinite(qty) or qty <= 0 or qty > MAX_QTY:
            raise ValueError
        qty = float(f"{qty:.3f}")
    except Exception:
        return await update.message.reply_text(
            f"Введите число > 0 и ≤ {MAX_QTY}. Пример: 1 или 2.5",
            reply_markup=cancel_markup(),
        )

    st = data.issue_state.get(uid)
    if not st or "part" not in st:
        return await update.message.reply_text(
            "Списание неактивно — начните заново из карточки."
        )

    st["quantity"] = qty
    st["await_comment"] = True
    await update.message.reply_text(
        "Добавьте комментарий (например: Линия сборки CSS OP-1100).",
        reply_markup=cancel_markup(),
    )
    return data.ASK_COMMENT


async def handle_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.chat_data["suppress_next_search"] = True
    uid = update.effective_user.id
    comment = (update.message.text or "").strip()
    st = data.issue_state.get(uid)
    if not st:
        return await update.message.reply_text(
            "Списание неактивно. Начните заново из карточки."
        )

    part = st.get("part")
    qty = st.get("quantity")
    if part is None or qty is None:
        data.issue_state.pop(uid, None)
        return await update.message.reply_text(
            "Что-то пошло не так. Попробуйте ещё раз."
        )

    st["comment"] = "" if comment == "-" else comment
    st["await_comment"] = False
    text = (
        "Вы уверены, что хотите списать деталь?\n\n"
        f"🔢 Код: {data.val(part, 'код')}\n"
        f"📦 Наименование: {data.val(part, 'наименование')}\n"
        f"📦 Кол-во: {qty}\n"
        f"💬 Комментарий: {st['comment'] or '—'}"
    )
    await update.message.reply_text(text, reply_markup=confirm_markup())
    return data.ASK_CONFIRM


async def save_issue_to_sheet(bot, user, part: dict, quantity, comment: str):
    from app.config import SPREADSHEET_URL
    import gspread

    client = data.get_gs_client()
    sh = client.open_by_url(SPREADSHEET_URL)
    try:
        ws = sh.worksheet("История")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="История", rows=1000, cols=12)
        ws.append_row(
            [
                "Дата",
                "ID",
                "Имя",
                "Тип",
                "Наименование",
                "Код",
                "Количество",
                "Коментарий",
            ]
        )

    headers_raw = ws.row_values(1)
    headers = [h.strip() for h in headers_raw]
    norm = [h.lower() for h in headers]

    full_name = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()
    display_name = full_name or (
        f"@{user.username}" if user.username else str(user.id)
    )
    ts = data.now_local_str()

    values_by_key = {
        "дата": ts,
        "timestamp": ts,
        "id": user.id,
        "user_id": user.id,
        "имя": display_name,
        "name": display_name,
        "тип": str(part.get("тип", "")),
        "type": str(part.get("тип", "")),
        "наименование": str(part.get("наименование", "")),
        "name_item": str(part.get("наименование", "")),
        "код": str(part.get("код", "")),
        "code": str(part.get("код", "")),
        "数量": str(quantity),
        "количество": str(quantity),
        "qty": str(quantity),
        "коментарий": comment or "",
        "комментарий": comment or "",
        "comment": comment or "",
    }
    row = [values_by_key.get(hn, "") for hn in norm]
    ws.append_row(row, value_input_option="USER_ENTERED")
    logger.info("💾 Списание записано в 'История'")


async def handle_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id

    if q.data == "confirm_yes":
        st = data.issue_state.get(uid)
        if not st or "part" not in st or "quantity" not in st:
            data.issue_state.pop(uid, None)
            return await q.message.reply_text(
                "Данных для списания нет. Начните заново."
            )
        part = st["part"]
        qty = st["quantity"]
        comment = st.get("comment", "")

        await save_issue_to_sheet(context.bot, q.from_user, part, qty, comment)
        data.issue_state.pop(uid, None)

        await q.message.reply_text(
            f"✅ Списано: {qty}\n"
            f"🔢 Код: {data.val(part, 'код')}\n"
            f"📦 Наименование: {data.val(part, 'наименование')}\n"
            f"💬 Комментарий: {comment or '—'}"
        )
        return ConversationHandler.END

    if q.data == "confirm_no":
        data.issue_state.pop(uid, None)
        await q.message.reply_text("❌ Списание отменено.")
        return ConversationHandler.END


async def cancel_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    if uid in data.issue_state:
        data.issue_state.pop(uid, None)
        await q.message.reply_text("❌ Операция списания отменена.")
    return ConversationHandler.END


async def handle_cancel_in_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cancel_cmd(update, context)
    return ConversationHandler.END


async def on_more_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    st = data.user_state.get(uid, {})
    results = st.get("results")
    if results is None or results.empty:
        return await q.message.reply_text("Сначала выполните поиск.")
    st["page"] = st.get("page", 0) + 1
    chat_id = q.message.chat.id
    await send_page_via_bot(context.bot, chat_id, uid)


# --------------------- НОВЫЕ ОБРАБОТЧИКИ ДЛЯ УЛУЧШЕННОГО ИНТЕРФЕЙСА -----------------

async def menu_categories_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки Категории - показывает типы деталей"""
    q = update.callback_query
    await q.answer()
    
    # Получаем уникальные типы из базы
    if data.df is None or data.df.empty:
        return await q.message.edit_text(
            "❌ База данных пуста",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]
            ])
        )
    
    # Получаем топ-15 типов
    if 'тип' not in data.df.columns:
        return await q.message.edit_text(
            "❌ Колонка 'тип' не найдена в базе",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]
            ])
        )
    
    types = data.df['тип'].dropna().unique()
    types = sorted([str(t).strip() for t in types if str(t).strip() and str(t).strip().lower() not in ['nan', 'none', '']])[:15]
    
    if not types:
        return await q.message.edit_text(
            "❌ Типы деталей не найдены",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]
            ])
        )
    
    # Формируем кнопки по 2 в ряд
    buttons = []
    for i in range(0, len(types), 2):
        row = []
        for j in range(i, min(i + 2, len(types))):
            item_type = types[j]
            # Ограничиваем длину текста на кнопке
            label = item_type if len(item_type) <= 20 else item_type[:17] + "..."
            row.append(InlineKeyboardButton(
                f"🔧 {label}",
                callback_data=f"cat_type:{item_type[:50]}"
            ))
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")])
    
    await q.message.edit_text(
        "📂 <b>Выберите тип детали:</b>\n\n"
        "Показаны наиболее популярные категории",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def cat_type_search_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск по выбранному типу"""
    q = update.callback_query
    await q.answer("🔍 Ищу...")
    
    # Извлекаем тип из callback_data
    try:
        item_type = q.data.split(":", 1)[1]
    except IndexError:
        return await q.answer("❌ Ошибка", show_alert=True)
    
    uid = q.from_user.id
    
    # Выполняем поиск по типу
    if data.df is None or data.df.empty:
        return await q.message.edit_text("❌ База данных пуста")
    
    # Фильтруем по типу
    mask = data.df['тип'].astype(str).str.contains(re.escape(item_type), case=False, na=False)
    results = data.df[mask].copy()
    
    if results.empty:
        return await q.message.edit_text(
            f"❌ По категории <b>{escape(item_type)}</b> ничего не найдено",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Категории", callback_data="menu_categories")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="back_main")]
            ])
        )
    
    # Сохраняем результаты
    st = data.user_state.setdefault(uid, {})
    st["results"] = results
    st["query"] = f"Тип: {item_type}"
    st["page"] = 0
    
    # Удаляем сообщение с меню
    try:
        await q.message.delete()
    except Exception:
        pass
    
    # Отправляем результаты через bot
    await send_page_via_bot(context.bot, q.message.chat.id, uid)


async def menu_favorites_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки Избранное"""
    q = update.callback_query
    await q.answer()
    
    await q.message.edit_text(
        "⭐ <b>Избранное</b>\n\n"
        "Функция в разработке...\n\n"
        "Скоро здесь будут отображаться детали, которые вы добавите в избранное.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]
        ])
    )


async def menu_history_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки История поиска"""
    q = update.callback_query
    await q.answer()
    
    uid = q.from_user.id
    st = data.user_state.get(uid, {})
    history = st.get("search_history", [])
    
    if not history:
        return await q.message.edit_text(
            "📜 <b>История поиска</b>\n\n"
            "История пока пуста. Выполните поиск, и он появится здесь!",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]
            ])
        )
    
    # Показываем последние 10 запросов
    recent = history[-10:]
    history_text = "📜 <b>Последние запросы:</b>\n\n"
    
    buttons = []
    for i, query in enumerate(reversed(recent), 1):
        history_text += f"{i}. <code>{escape(query)}</code>\n"
        if i <= 5:  # Добавляем кнопки только для первых 5
            buttons.append([InlineKeyboardButton(
                f"{i}. {query[:25]}..." if len(query) > 25 else f"{i}. {query}",
                callback_data=f"hist_search:{i-1}"
            )])
    
    buttons.append([InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")])
    
    await q.message.edit_text(
        history_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def menu_export_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки Экспорт"""
    q = update.callback_query
    await q.answer()
    
    uid = q.from_user.id
    st = data.user_state.get(uid, {})
    results = st.get("results")
    
    if results is None or results.empty:
        return await q.message.edit_text(
            "📊 <b>Экспорт в Excel</b>\n\n"
            "Сначала выполните поиск, чтобы экспортировать результаты!",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]
            ])
        )
    
    await q.answer("📊 Готовлю файл...", show_alert=False)
    
    # Используем существующую функцию export
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        buf = await asyncio.to_thread(
            data._df_to_xlsx, results, f"export_{timestamp}.xlsx"
        )
        await q.message.reply_document(
            InputFile(buf, filename=f"export_{timestamp}.xlsx"),
            caption=f"📊 <b>Экспорт результатов</b>\n\n"
                    f"🔍 Запрос: <code>{escape(st.get('query', 'поиск'))}</code>\n"
                    f"📦 Записей: <b>{len(results)}</b>",
            parse_mode="HTML"
        )
        await q.message.delete()
    except Exception as e:
        logger.error(f"Export failed: {e}")
        await q.message.edit_text(
            f"❌ Ошибка при экспорте: {str(e)}\n\n"
            "Попробуйте снова или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]
            ])
        )


async def menu_help_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки Помощь"""
    q = update.callback_query
    await q.answer()
    
    help_text = (
        "📖 <b>Справка по использованию</b>\n\n"
        "🔍 <b>Поиск:</b>\n"
        "   • Введите название, код или парт номер\n"
        "   • Используйте категории для быстрого доступа\n"
        "   • Просматривайте историю поиска\n\n"
        "📦 <b>Списание:</b>\n"
        "   1. Найдите деталь через поиск\n"
        "   2. Нажмите «📦 Взять деталь»\n"
        "   3. Укажите количество\n"
        "   4. Добавьте комментарий\n"
        "   5. Подтвердите операцию\n\n"
        "⌨️ <b>Команды:</b>\n"
        "   /start - Главное меню\n"
        "   /help - Эта справка\n"
        "   /export - Экспорт результатов\n"
        "   /more - Показать ещё результатов\n"
        "   /cancel - Отменить текущее действие\n\n"
        "❓ <b>Нужна помощь?</b>\n"
        f"   Напишите в поддержку: {SUPPORT_CONTACT}"
    )
    
    await q.message.edit_text(
        help_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]
        ])
    )


async def back_main_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат в главное меню"""
    q = update.callback_query
    await q.answer()
    
    user = q.from_user
    first = escape((user.first_name or "").strip() or "коллега")
    
    await q.message.edit_text(
        f"🏠 <b>Привет, {first}!</b>\n\n"
        "Выберите действие из меню ниже:",
        parse_mode="HTML",
        reply_markup=main_menu_markup()
    )


# --------------------- Регистрация хендлеров -----------------
def register_handlers(app):
    # Гварды
    app.add_handler(MessageHandler(filters.ALL, guard_msg), group=-1)
    app.add_handler(CallbackQueryHandler(guard_cb, pattern=".*"), group=-1)

    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("app", app_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("more", more_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("reload", reload_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))

    # Новый хендлер для получения file_id
    app.add_handler(CommandHandler("getfileid", getfileid_cmd))
    app.add_handler(
        MessageHandler(
            (filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.ALL),
            media_fileid_handler,
        ),
        group=0,
    )

    # Меню приветствия
    app.add_handler(CallbackQueryHandler(menu_search_cb, pattern=r"^menu_search$"))
    app.add_handler(CallbackQueryHandler(menu_categories_cb, pattern=r"^menu_categories$"))
    app.add_handler(CallbackQueryHandler(menu_favorites_cb, pattern=r"^menu_favorites$"))
    app.add_handler(CallbackQueryHandler(menu_history_cb, pattern=r"^menu_history$"))
    app.add_handler(CallbackQueryHandler(menu_export_cb, pattern=r"^menu_export$"))
    app.add_handler(CallbackQueryHandler(menu_help_cb, pattern=r"^menu_help$"))
    app.add_handler(
        CallbackQueryHandler(menu_issue_help_cb, pattern=r"^menu_issue_help$")
    )
    app.add_handler(CallbackQueryHandler(menu_contact_cb, pattern=r"^menu_contact$"))
    app.add_handler(CallbackQueryHandler(back_main_cb, pattern=r"^back_main$"))
    app.add_handler(CallbackQueryHandler(noop_cb, pattern=r"^noop$"))
    
    # Категории
    app.add_handler(CallbackQueryHandler(cat_type_search_cb, pattern=r"^cat_type:"))

    # Пагинация и отмена
    app.add_handler(CallbackQueryHandler(on_more_click, pattern=r"^more$"))
    app.add_handler(CallbackQueryHandler(cancel_action, pattern=r"^cancel_action$"))

    # Диалог списания
    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(on_issue_click, pattern=r"^issue:")],
        states={
            data.ASK_QUANTITY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_quantity),
                CallbackQueryHandler(cancel_action, pattern=r"^cancel_action$"),
            ],
            data.ASK_COMMENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_comment),
                CallbackQueryHandler(cancel_action, pattern=r"^cancel_action$"),
            ],
            data.ASK_CONFIRM: [
                CallbackQueryHandler(handle_confirm, pattern=r"^confirm_(yes|no)$"),
                CallbackQueryHandler(cancel_action, pattern=r"^cancel_action$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", handle_cancel_in_dialog)],
        allow_reentry=True,
        per_chat=True,
        per_user=True,
        per_message=False,
    )
    app.add_handler(conv)

    # Поиск
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, search_text), group=1
    )
