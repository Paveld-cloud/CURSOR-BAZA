# app/handlers.py - УЛУЧШЕННАЯ ВЕРСИЯ
import io
import math
import re
import asyncio
import logging
from html import escape
from typing import Optional, List

import pandas as pd
import aiohttp
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

import app.data as data

logger = logging.getLogger("bot.handlers")

# ==================== УЛУЧШЕННЫЕ КЛАВИАТУРЫ ====================

def main_menu_markup():
    """Главное меню с красивыми кнопками"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔍 Поиск", callback_data="menu_search"),
            InlineKeyboardButton("📂 Категории", callback_data="menu_categories")
        ],
        [
            InlineKeyboardButton("⭐ Избранное", callback_data="menu_favorites"),
            InlineKeyboardButton("📜 История", callback_data="menu_history")
        ],
        [
            InlineKeyboardButton("📦 Списать деталь", callback_data="menu_issue_help"),
        ],
        [
            InlineKeyboardButton("📊 Экспорт в Excel", callback_data="menu_export"),
            InlineKeyboardButton("ℹ️ Помощь", callback_data="menu_help")
        ],
        [InlineKeyboardButton("📞 Поддержка", callback_data="menu_contact")]
    ])


def categories_markup():
    """Меню категорий для быстрого поиска"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔧 По типу детали", callback_data="cat_type")],
        [InlineKeyboardButton("🏭 По производителю", callback_data="cat_manufacturer")],
        [InlineKeyboardButton("🔢 По коду OEM", callback_data="cat_oem")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]
    ])


def search_mode_markup():
    """Выбор режима поиска"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔤 По названию", callback_data="search_name"),
            InlineKeyboardButton("🔢 По коду", callback_data="search_code")
        ],
        [
            InlineKeyboardButton("🏷️ По парт номеру", callback_data="search_part"),
            InlineKeyboardButton("🔍 Умный поиск", callback_data="search_smart")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_main")]
    ])


def pagination_markup(current_page: int, total_pages: int, prefix: str = "page"):
    """Улучшенная пагинация с навигацией"""
    buttons = []
    
    # Первая строка: навигация
    nav_row = []
    if current_page > 0:
        nav_row.append(InlineKeyboardButton("⏮️ Первая", callback_data=f"{prefix}:0"))
        nav_row.append(InlineKeyboardButton("◀️ Назад", callback_data=f"{prefix}:{current_page-1}"))
    
    nav_row.append(InlineKeyboardButton(
        f"📄 {current_page + 1}/{total_pages}", 
        callback_data="noop"
    ))
    
    if current_page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("▶️ Вперёд", callback_data=f"{prefix}:{current_page+1}"))
        nav_row.append(InlineKeyboardButton("⏭️ Последняя", callback_data=f"{prefix}:{total_pages-1}"))
    
    if nav_row:
        buttons.append(nav_row)
    
    # Вторая строка: быстрый переход
    if total_pages > 3:
        jump_row = []
        # Показываем до 5 кнопок быстрого перехода
        pages_to_show = []
        if current_page > 1:
            pages_to_show.append(0)  # Первая
        if current_page > 2:
            pages_to_show.append(current_page - 1)
        if 0 < current_page < total_pages - 1:
            pages_to_show.append(current_page)
        if current_page < total_pages - 2:
            pages_to_show.append(current_page + 1)
        if current_page < total_pages - 2:
            pages_to_show.append(total_pages - 1)  # Последняя
        
        # Убираем дубликаты и сортируем
        pages_to_show = sorted(set(pages_to_show))[:5]
        
        for page_num in pages_to_show:
            label = f"• {page_num + 1} •" if page_num == current_page else str(page_num + 1)
            jump_row.append(InlineKeyboardButton(label, callback_data=f"{prefix}:{page_num}"))
        
        if jump_row and len(jump_row) > 1:
            buttons.append(jump_row)
    
    # Третья строка: действия
    action_row = [
        InlineKeyboardButton("🔄 Обновить", callback_data=f"refresh:{current_page}"),
        InlineKeyboardButton("🔙 Меню", callback_data="back_main")
    ]
    buttons.append(action_row)
    
    return InlineKeyboardMarkup(buttons)


def item_card_markup(item_id: int, has_image: bool = False):
    """Карточка товара с расширенными действиями"""
    buttons = [
        [
            InlineKeyboardButton("📦 Списать", callback_data=f"issue:{item_id}"),
            InlineKeyboardButton("📋 Детали", callback_data=f"details:{item_id}")
        ],
        [
            InlineKeyboardButton("⭐ В избранное", callback_data=f"fav_add:{item_id}"),
            InlineKeyboardButton("📤 Поделиться", callback_data=f"share:{item_id}")
        ]
    ]
    
    if has_image:
        buttons.append([
            InlineKeyboardButton("🖼️ Показать фото", callback_data=f"show_img:{item_id}")
        ])
    
    buttons.append([InlineKeyboardButton("🔙 К результатам", callback_data="back_results")])
    
    return InlineKeyboardMarkup(buttons)


def filter_markup(active_filters: dict = None):
    """Фильтры для уточнения поиска"""
    active = active_filters or {}
    buttons = []
    
    # Тип детали
    type_label = f"✅ Тип: {active.get('type', 'Все')}" if 'type' in active else "🔧 Тип детали"
    buttons.append([InlineKeyboardButton(type_label, callback_data="filter_type")])
    
    # Производитель
    mfr_label = f"✅ Производитель: {active.get('manufacturer', 'Все')}" if 'manufacturer' in active else "🏭 Производитель"
    buttons.append([InlineKeyboardButton(mfr_label, callback_data="filter_mfr")])
    
    # Наличие фото
    photo_label = "✅ Только с фото" if active.get('has_photo') else "📷 Только с фото"
    buttons.append([InlineKeyboardButton(photo_label, callback_data="filter_photo")])
    
    # Действия
    action_row = []
    if active:
        action_row.append(InlineKeyboardButton("🗑️ Сбросить", callback_data="filter_clear"))
    action_row.append(InlineKeyboardButton("✅ Применить", callback_data="filter_apply"))
    buttons.append(action_row)
    
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="back_results")])
    
    return InlineKeyboardMarkup(buttons)


def confirm_markup():
    """Подтверждение действия"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Да, подтвердить", callback_data="confirm_yes"),
            InlineKeyboardButton("❌ Нет, отменить", callback_data="confirm_no")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="cancel_action")]
    ])


def cancel_markup():
    """Кнопка отмены"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Отменить", callback_data="cancel_action")]
    ])


def back_markup(callback_data: str = "back_main"):
    """Простая кнопка назад"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data=callback_data)]
    ])


# ==================== MINI APP ====================

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
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⚠️ Mini App URL не задан", callback_data="noop")]
        ])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Открыть каталог (Mini App)", web_app=WebAppInfo(url=url))],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]
    ])


# ==================== УТИЛИТЫ ====================

async def _safe_send_html_message(bot, chat_id: int, text: str, **kwargs):
    """Универсальная отправка сообщения с HTML"""
    try:
        return await bot.send_message(
            chat_id=chat_id, text=text, parse_mode="HTML", **kwargs
        )
    except Exception as e:
        logger.warning(f"HTML message parse failed, fallback to plain: {e}")
        no_tags = re.sub(r"</?(b|i|code|pre)>", "", text)
        kwargs.pop("parse_mode", None)
        return await bot.send_message(chat_id=chat_id, text=no_tags, **kwargs)


def format_item_card(item: dict, show_full: bool = False) -> str:
    """Форматирование карточки детали"""
    code = data.val(item, "код", "—")
    name = data.val(item, "наименование", "—")
    item_type = data.val(item, "тип", "—")
    oem = data.val(item, "oem", "—")
    part_num = data.val(item, "парт номер", "—")
    manufacturer = data.val(item, "изготовитель", "—")
    
    # Краткая версия для списка
    if not show_full:
        return (
            f"🔧 <b>{escape(name)}</b>\n"
            f"🔢 Код: <code>{escape(code)}</code>\n"
            f"📦 Тип: {escape(item_type)}\n"
        )
    
    # Полная версия для детального просмотра
    card = (
        f"━━━━━━━━━━━━━━━\n"
        f"🔧 <b>{escape(name)}</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"🔢 <b>Код:</b> <code>{escape(code)}</code>\n"
        f"📦 <b>Тип:</b> {escape(item_type)}\n"
    )
    
    if oem != "—":
        card += f"🏷️ <b>OEM:</b> {escape(oem)}\n"
    if part_num != "—":
        card += f"🔖 <b>Парт номер:</b> <code>{escape(part_num)}</code>\n"
    if manufacturer != "—":
        card += f"🏭 <b>Производитель:</b> {escape(manufacturer)}\n"
    
    # Дополнительные поля
    for key in ["модель", "описание", "примечание"]:
        val = data.val(item, key)
        if val and val != "—":
            card += f"💬 <b>{key.capitalize()}:</b> {escape(val)}\n"
    
    return card


# ==================== ПОЛЬЗОВАТЕЛИ И ДОСТУП ====================

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


# ==================== ГВАРДЫ ====================

async def guard_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user and not is_allowed(user.id):
        try:
            await update.effective_message.reply_text("🚫 Доступ запрещён.")
        except Exception:
            pass
        raise ApplicationHandlerStop


async def guard_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user and not is_allowed(user.id):
        try:
            await update.callback_query.answer("🚫 Доступ запрещён.", show_alert=True)
        except Exception:
            pass
        raise ApplicationHandlerStop


# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start с приветствием"""
    await send_welcome_sequence(update, context)


async def send_welcome_sequence(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправка приветственного сообщения"""
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
        f"   • С фильтрами и категориями\n"
        f"   • Сохранение истории поиска\n\n"
        f"📦 <b>Управление деталями</b>\n"
        f"   • Просмотр карточек с фото\n"
        f"   • Списание с подтверждением\n"
        f"   • Экспорт результатов в Excel\n\n"
        f"⭐ <b>Дополнительно</b>\n"
        f"   • Избранные детали\n"
        f"   • Быстрый доступ к категориям\n"
        f"   • Поддержка 24/7\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💡 <b>Пример поиска:</b>\n"
        f"<code>PI8808DRG500</code>\n\n"
        f"🚀 <i>Выберите действие из меню ниже!</i>"
    )

    try:
        # Пробуем отправить с медиа
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

    # Fallback: текстовое сообщение
    await _safe_send_html_message(
        context.bot, chat_id, card_html, reply_markup=main_menu_markup()
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    help_text = (
        "📖 <b>Справка по использованию</b>\n\n"
        "🔍 <b>Поиск:</b>\n"
        "   • Просто введите название, код или парт номер\n"
        "   • Используйте фильтры для уточнения\n"
        "   • Сохраняйте результаты в избранное\n\n"
        "📦 <b>Списание:</b>\n"
        "   1. Найдите деталь\n"
        "   2. Нажмите «Списать»\n"
        "   3. Укажите количество\n"
        "   4. Добавьте комментарий\n"
        "   5. Подтвердите операцию\n\n"
        "⌨️ <b>Команды:</b>\n"
        "   /start - Главное меню\n"
        "   /help - Эта справка\n"
        "   /export - Экспорт результатов\n"
        "   /cancel - Отменить текущее действие\n\n"
        "❓ <b>Нужна помощь?</b>\n"
        f"   Напишите в поддержку: {SUPPORT_CONTACT}"
    )
    
    await _safe_send_html_message(
        context.bot,
        update.effective_chat.id,
        help_text,
        reply_markup=back_markup()
    )


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /cancel"""
    uid = update.effective_user.id
    
    # Очищаем состояния
    if uid in data.user_state:
        data.user_state.pop(uid)
    if uid in data.issue_state:
        data.issue_state.pop(uid)
    
    await update.message.reply_text(
        "❌ Все активные операции отменены.",
        reply_markup=main_menu_markup()
    )


# ==================== ОБРАБОТЧИКИ МЕНЮ ====================

async def menu_search_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки "Поиск" """
    q = update.callback_query
    await q.answer()
    
    await q.message.edit_text(
        "🔍 <b>Выберите режим поиска:</b>\n\n"
        "🔤 <b>По названию</b> - поиск по наименованию детали\n"
        "🔢 <b>По коду</b> - точный поиск по коду детали\n"
        "🏷️ <b>По парт номеру</b> - поиск по парт номеру OEM\n"
        "🔍 <b>Умный поиск</b> - поиск по всем полям одновременно\n\n"
        "💡 Или просто введите запрос в чат!",
        parse_mode="HTML",
        reply_markup=search_mode_markup()
    )


async def menu_categories_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки "Категории" """
    q = update.callback_query
    await q.answer()
    
    await q.message.edit_text(
        "📂 <b>Выберите категорию для просмотра:</b>\n\n"
        "Категории помогут быстро найти нужную группу деталей",
        parse_mode="HTML",
        reply_markup=categories_markup()
    )


async def menu_favorites_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки "Избранное" """
    q = update.callback_query
    await q.answer()
    
    uid = q.from_user.id
    # TODO: Реализовать загрузку избранного из БД
    # Пока заглушка
    
    await q.message.edit_text(
        "⭐ <b>Избранное</b>\n\n"
        "Здесь будут отображаться детали, которые вы добавите в избранное.\n\n"
        "💡 Чтобы добавить деталь в избранное, нажмите ⭐ на карточке детали.",
        parse_mode="HTML",
        reply_markup=back_markup()
    )


async def menu_history_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки "История" """
    q = update.callback_query
    await q.answer()
    
    uid = q.from_user.id
    st = data.user_state.get(uid, {})
    history = st.get("search_history", [])
    
    if not history:
        await q.message.edit_text(
            "📜 <b>История поиска</b>\n\n"
            "История пока пуста. Выполните поиск, и он появится здесь!",
            parse_mode="HTML",
            reply_markup=back_markup()
        )
        return
    
    # Показываем последние 10 запросов
    recent = history[-10:]
    history_text = "📜 <b>Последние запросы:</b>\n\n"
    for i, query in enumerate(reversed(recent), 1):
        history_text += f"{i}. <code>{escape(query)}</code>\n"
    
    await q.message.edit_text(
        history_text,
        parse_mode="HTML",
        reply_markup=back_markup()
    )


async def menu_issue_help_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки "Как списать деталь" """
    q = update.callback_query
    await q.answer()
    
    help_text = (
        "📦 <b>Как списать деталь</b>\n\n"
        "1️⃣ Найдите нужную деталь через поиск\n"
        "2️⃣ Откройте карточку детали\n"
        "3️⃣ Нажмите кнопку «📦 Списать»\n"
        "4️⃣ Укажите количество для списания\n"
        "5️⃣ Добавьте комментарий (необязательно)\n"
        "6️⃣ Подтвердите операцию\n\n"
        "✅ Готово! Списание записано в историю.\n\n"
        "💡 <b>Совет:</b> Добавляйте комментарий для лучшей отчётности\n"
        "Пример: <i>«Линия сборки CSS OP-1100»</i>"
    )
    
    await q.message.edit_text(
        help_text,
        parse_mode="HTML",
        reply_markup=back_markup()
    )


async def menu_contact_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки "Поддержка" """
    q = update.callback_query
    await q.answer()
    
    contact_text = (
        "📞 <b>Поддержка</b>\n\n"
        f"По всем вопросам обращайтесь:\n"
        f"{SUPPORT_CONTACT}\n\n"
        "Мы ответим в ближайшее время! ⚡"
    )
    
    await q.message.edit_text(
        contact_text,
        parse_mode="HTML",
        reply_markup=back_markup()
    )


async def menu_export_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки "Экспорт в Excel" """
    q = update.callback_query
    await q.answer()
    
    uid = q.from_user.id
    st = data.user_state.get(uid, {})
    results = st.get("results")
    
    if results is None or results.empty:
        await q.message.edit_text(
            "📊 <b>Экспорт в Excel</b>\n\n"
            "Сначала выполните поиск, чтобы экспортировать результаты!",
            parse_mode="HTML",
            reply_markup=back_markup()
        )
        return
    
    await q.answer("📊 Готовлю файл...", show_alert=False)
    
    # Экспорт результатов
    await export_results(q.message.chat.id, uid, context.bot)
    
    await q.message.reply_text(
        "✅ Файл готов! Проверьте сообщения выше.",
        reply_markup=back_markup()
    )


async def menu_help_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки "Помощь" """
    await help_cmd(update, context)


async def noop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик для неактивных кнопок"""
    await update.callback_query.answer()


async def back_main_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат в главное меню"""
    q = update.callback_query
    await q.answer()
    
    await q.message.edit_text(
        "🏠 <b>Главное меню</b>\n\n"
        "Выберите действие:",
        parse_mode="HTML",
        reply_markup=main_menu_markup()
    )


# ==================== ПРОДОЛЖЕНИЕ СЛЕДУЕТ ====================
# Это первая часть улучшенного handlers.py
# Следующие части включают: поиск, карточки, списание, пагинацию
# app/handlers_search.py - ПОИСК И ПАГИНАЦИЯ

import logging
from html import escape
from typing import Optional
import pandas as pd
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
import app.data as data
from app.config import PAGE_SIZE

logger = logging.getLogger("bot.handlers.search")

# Импорт из основного файла
from improved_handlers import (
    _safe_send_html_message,
    format_item_card,
    pagination_markup,
    item_card_markup,
    filter_markup,
    back_markup,
    main_menu_markup
)


# ==================== ПОИСК ====================

async def search_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Основной обработчик текстового поиска"""
    # Проверяем флаг подавления поиска
    if context.chat_data.get("suppress_next_search"):
        context.chat_data["suppress_next_search"] = False
        return
    
    query = (update.message.text or "").strip()
    if not query or len(query) < 2:
        await update.message.reply_text(
            "🔍 Введите минимум 2 символа для поиска.",
            reply_markup=main_menu_markup()
        )
        return
    
    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # Сохраняем запрос в историю
    st = data.user_state.setdefault(uid, {})
    history = st.setdefault("search_history", [])
    if query not in history[-5:]:  # Избегаем дубликатов в последних 5 запросах
        history.append(query)
        if len(history) > 50:  # Ограничиваем размер истории
            history = history[-50:]
        st["search_history"] = history
    
    # Показываем индикатор поиска
    search_msg = await update.message.reply_text("🔍 Ищу...")
    
    # Выполняем поиск
    results = await asyncio_search(query)
    
    if results is None or results.empty:
        await search_msg.edit_text(
            f"❌ По запросу <code>{escape(query)}</code> ничего не найдено.\n\n"
            f"💡 <b>Попробуйте:</b>\n"
            f"   • Проверить правильность написания\n"
            f"   • Использовать другие ключевые слова\n"
            f"   • Искать по коду или парт номеру",
            parse_mode="HTML",
            reply_markup=main_menu_markup()
        )
        return
    
    # Сохраняем результаты
    st["results"] = results
    st["query"] = query
    st["page"] = 0
    st["filters"] = {}
    
    # Отправляем первую страницу
    await search_msg.delete()
    await send_search_results_page(context.bot, chat_id, uid, 0)


async def asyncio_search(query: str) -> Optional[pd.DataFrame]:
    """Асинхронная обёртка для поиска"""
    import asyncio
    return await asyncio.to_thread(data.search_parts, query)


async def send_search_results_page(bot, chat_id: int, uid: int, page: int = 0):
    """Отправка страницы результатов поиска"""
    st = data.user_state.get(uid, {})
    results = st.get("results")
    query = st.get("query", "")
    
    if results is None or results.empty:
        await _safe_send_html_message(
            bot, chat_id,
            "❌ Результаты поиска пусты.",
            reply_markup=main_menu_markup()
        )
        return
    
    total = len(results)
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    page = max(0, min(page, total_pages - 1))
    
    st["page"] = page
    
    # Получаем элементы для текущей страницы
    start = page * PAGE_SIZE
    end = min(start + PAGE_SIZE, total)
    page_items = results.iloc[start:end]
    
    # Формируем сообщение с результатами
    header = (
        f"🔍 <b>Результаты поиска:</b> <code>{escape(query)}</code>\n"
        f"📊 Найдено: <b>{total}</b> | "
        f"Страница: <b>{page + 1}/{total_pages}</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
    )
    
    # Добавляем карточки товаров
    items_text = ""
    for idx, row in page_items.iterrows():
        item_num = start + len(page_items[:page_items.index.get_loc(idx)]) + 1
        item_dict = row.to_dict()
        
        code = data.val(item_dict, "код", "—")
        name = data.val(item_dict, "наименование", "—")
        item_type = data.val(item_dict, "тип", "—")
        
        items_text += (
            f"<b>{item_num}.</b> {escape(name)}\n"
            f"   🔢 <code>{escape(code)}</code> | 📦 {escape(item_type)}\n"
            f"   /view_{idx}\n\n"
        )
    
    footer = (
        f"━━━━━━━━━━━━━━━\n"
        f"💡 Нажмите /view_ID для детального просмотра\n"
        f"📊 Или используйте кнопки навигации ниже"
    )
    
    message_text = header + items_text + footer
    
    # Создаём клавиатуру с навигацией
    keyboard = []
    
    # Быстрые действия для текущей страницы
    quick_actions = []
    if total <= 10:  # Если результатов мало, добавляем кнопки для каждого
        for idx in page_items.index[:5]:  # Максимум 5 кнопок
            item_num = results.index.get_loc(idx) + 1
            quick_actions.append(
                InlineKeyboardButton(f"#{item_num}", callback_data=f"view:{idx}")
            )
    
    if quick_actions:
        keyboard.append(quick_actions)
    
    # Добавляем пагинацию
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⏮️", callback_data=f"page:0"))
        nav_row.append(InlineKeyboardButton("◀️", callback_data=f"page:{page-1}"))
    
    nav_row.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="noop"))
    
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("▶️", callback_data=f"page:{page+1}"))
        nav_row.append(InlineKeyboardButton("⏭️", callback_data=f"page:{total_pages-1}"))
    
    keyboard.append(nav_row)
    
    # Дополнительные действия
    keyboard.append([
        InlineKeyboardButton("🔍 Фильтры", callback_data="show_filters"),
        InlineKeyboardButton("📊 Экспорт", callback_data="menu_export")
    ])
    
    keyboard.append([
        InlineKeyboardButton("🔄 Новый поиск", callback_data="menu_search"),
        InlineKeyboardButton("🏠 Меню", callback_data="back_main")
    ])
    
    markup = InlineKeyboardMarkup(keyboard)
    
    await _safe_send_html_message(
        bot, chat_id, message_text, reply_markup=markup
    )


# ==================== ОБРАБОТЧИКИ ПАГИНАЦИИ ====================

async def on_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик переключения страниц"""
    q = update.callback_query
    await q.answer()
    
    # Извлекаем номер страницы из callback_data
    try:
        page = int(q.data.split(":")[1])
    except (IndexError, ValueError):
        page = 0
    
    uid = q.from_user.id
    chat_id = q.message.chat.id
    
    # Удаляем старое сообщение и отправляем новое
    await q.message.delete()
    await send_search_results_page(context.bot, chat_id, uid, page)


async def on_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик просмотра детали"""
    q = update.callback_query
    await q.answer()
    
    # Извлекаем ID детали
    try:
        item_id = int(q.data.split(":")[1])
    except (IndexError, ValueError):
        await q.answer("❌ Ошибка: неверный ID", show_alert=True)
        return
    
    uid = q.from_user.id
    st = data.user_state.get(uid, {})
    results = st.get("results")
    
    if results is None or item_id not in results.index:
        await q.answer("❌ Деталь не найдена", show_alert=True)
        return
    
    # Получаем данные детали
    item = results.loc[item_id].to_dict()
    
    # Отправляем карточку
    await send_item_card(q.message.chat.id, item, item_id, context.bot)


async def send_item_card(chat_id: int, item: dict, item_id: int, bot):
    """Отправка детальной карточки товара"""
    # Проверяем наличие изображения
    image_url = data.val(item, "фото")
    has_image = bool(image_url and image_url != "—")
    
    # Формируем полную карточку
    card_text = format_item_card(item, show_full=True)
    
    # Создаём клавиатуру
    markup = item_card_markup(item_id, has_image)
    
    # Отправляем с фото, если есть
    if has_image:
        try:
            await bot.send_photo(
                chat_id=chat_id,
                photo=image_url,
                caption=card_text,
                parse_mode="HTML",
                reply_markup=markup
            )
            return
        except Exception as e:
            logger.warning(f"Failed to send photo: {e}")
    
    # Fallback: текстовое сообщение
    await _safe_send_html_message(
        bot, chat_id, card_text, reply_markup=markup
    )


# ==================== ФИЛЬТРЫ ====================

async def show_filters_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать меню фильтров"""
    q = update.callback_query
    await q.answer()
    
    uid = q.from_user.id
    st = data.user_state.get(uid, {})
    active_filters = st.get("filters", {})
    
    filter_text = (
        "🔍 <b>Фильтры поиска</b>\n\n"
        "Уточните результаты с помощью фильтров:\n\n"
    )
    
    if active_filters:
        filter_text += "<b>Активные фильтры:</b>\n"
        for key, value in active_filters.items():
            filter_text += f"   ✅ {key}: {value}\n"
        filter_text += "\n"
    else:
        filter_text += "<i>Фильтры не применены</i>\n\n"
    
    await q.message.edit_text(
        filter_text,
        parse_mode="HTML",
        reply_markup=filter_markup(active_filters)
    )


async def apply_filters_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Применить фильтры"""
    q = update.callback_query
    await q.answer("🔍 Применяю фильтры...")
    
    uid = q.from_user.id
    st = data.user_state.get(uid, {})
    
    # TODO: Реализовать применение фильтров к результатам
    # Пока просто возвращаемся к результатам
    
    await q.message.delete()
    await send_search_results_page(context.bot, q.message.chat.id, uid, 0)


# ==================== ЭКСПОРТ ====================

async def export_results(chat_id: int, uid: int, bot):
    """Экспорт результатов в Excel"""
    import io
    from datetime import datetime
    
    st = data.user_state.get(uid, {})
    results = st.get("results")
    query = st.get("query", "поиск")
    
    if results is None or results.empty:
        await bot.send_message(
            chat_id=chat_id,
            text="❌ Нет результатов для экспорта."
        )
        return
    
    try:
        # Подготовка данных
        export_df = results.copy()
        
        # Создаём Excel файл в памяти
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            export_df.to_excel(writer, sheet_name='Результаты', index=False)
            
            # Получаем workbook и worksheet для форматирования
            workbook = writer.book
            worksheet = writer.sheets['Результаты']
            
            # Форматирование заголовков
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1
            })
            
            # Применяем форматирование
            for col_num, value in enumerate(export_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(col_num, col_num, 15)
        
        buffer.seek(0)
        
        # Формируем имя файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"search_results_{timestamp}.xlsx"
        
        # Отправляем файл
        await bot.send_document(
            chat_id=chat_id,
            document=buffer,
            filename=filename,
            caption=f"📊 Экспорт результатов\n\n"
                    f"🔍 Запрос: <code>{escape(query)}</code>\n"
                    f"📦 Записей: <b>{len(results)}</b>",
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Export failed: {e}")
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ Ошибка при экспорте: {str(e)}"
        )


async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /export"""
    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    
    st = data.user_state.get(uid, {})
    results = st.get("results")
    
    if results is None or results.empty:
        await update.message.reply_text(
            "❌ Сначала выполните поиск!",
            reply_markup=main_menu_markup()
        )
        return
    
    await update.message.reply_text("📊 Готовлю экспорт...")
    await export_results(chat_id, uid, context.bot)
# app/handlers_final.py - КАТЕГОРИИ, ИЗБРАННОЕ И РЕГИСТРАЦИЯ

import logging
import asyncio
from html import escape
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)
import app.data as data
from app.config import MAX_QTY

logger = logging.getLogger("bot.handlers.final")

# Импорты из других частей
from improved_handlers import (
    _safe_send_html_message,
    format_item_card,
    cancel_markup,
    confirm_markup,
    back_markup,
    categories_markup,
    main_menu_markup,
    # Функции доступа
    guard_msg,
    guard_cb,
    # Команды
    start,
    help_cmd,
    cancel_cmd,
    # Обработчики меню
    menu_search_cb,
    menu_categories_cb,
    menu_favorites_cb,
    menu_history_cb,
    menu_issue_help_cb,
    menu_contact_cb,
    menu_export_cb,
    menu_help_cb,
    noop_cb,
    back_main_cb,
)

from improved_handlers_search import (
    search_text,
    on_page_callback,
    on_view_callback,
    show_filters_cb,
    apply_filters_cb,
    export_cmd,
    send_search_results_page,
)

# ==================== КАТЕГОРИИ ====================

async def cat_type_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр категорий по типу"""
    q = update.callback_query
    await q.answer()
    
    # Получаем уникальные типы из базы
    if data.df is None or data.df.empty:
        await q.message.edit_text(
            "❌ База данных пуста",
            reply_markup=back_markup("menu_categories")
        )
        return
    
    types = data.df['тип'].dropna().unique()
    types = sorted([str(t).strip() for t in types if str(t).strip()])[:20]  # Топ 20
    
    if not types:
        await q.message.edit_text(
            "❌ Типы деталей не найдены",
            reply_markup=back_markup("menu_categories")
        )
        return
    
    # Формируем кнопки
    buttons = []
    for item_type in types:
        buttons.append([InlineKeyboardButton(
            f"🔧 {item_type}",
            callback_data=f"search_type:{item_type[:50]}"
        )])
    
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="menu_categories")])
    
    await q.message.edit_text(
        "🔧 <b>Выберите тип детали:</b>\n\n"
        "Показаны наиболее популярные категории",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def cat_manufacturer_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр категорий по производителю"""
    q = update.callback_query
    await q.answer()
    
    if data.df is None or data.df.empty:
        await q.message.edit_text(
            "❌ База данных пуста",
            reply_markup=back_markup("menu_categories")
        )
        return
    
    manufacturers = data.df['изготовитель'].dropna().unique()
    manufacturers = sorted([str(m).strip() for m in manufacturers if str(m).strip()])[:20]
    
    if not manufacturers:
        await q.message.edit_text(
            "❌ Производители не найдены",
            reply_markup=back_markup("menu_categories")
        )
        return
    
    buttons = []
    for mfr in manufacturers:
        buttons.append([InlineKeyboardButton(
            f"🏭 {mfr}",
            callback_data=f"search_mfr:{mfr[:50]}"
        )])
    
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="menu_categories")])
    
    await q.message.edit_text(
        "🏭 <b>Выберите производителя:</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def search_by_category_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск по выбранной категории"""
    q = update.callback_query
    await q.answer("🔍 Ищу...")
    
    # Парсим callback_data: search_type:Значение или search_mfr:Значение
    parts = q.data.split(":", 1)
    if len(parts) != 2:
        await q.answer("❌ Ошибка", show_alert=True)
        return
    
    category_type, value = parts
    uid = q.from_user.id
    
    # Выполняем поиск
    if category_type == "search_type":
        column = "тип"
        label = "Тип"
    elif category_type == "search_mfr":
        column = "изготовитель"
        label = "Производитель"
    else:
        await q.answer("❌ Неизвестная категория", show_alert=True)
        return
    
    # Фильтруем результаты
    if data.df is None or data.df.empty:
        await q.message.edit_text("❌ База данных пуста")
        return
    
    results = data.df[data.df[column].astype(str).str.contains(value, case=False, na=False)]
    
    if results.empty:
        await q.message.edit_text(
            f"❌ По категории <b>{label}: {escape(value)}</b> ничего не найдено",
            parse_mode="HTML",
            reply_markup=back_markup("menu_categories")
        )
        return
    
    # Сохраняем результаты
    st = data.user_state.setdefault(uid, {})
    st["results"] = results
    st["query"] = f"{label}: {value}"
    st["page"] = 0
    
    # Показываем результаты
    await q.message.delete()
    await send_search_results_page(context.bot, q.message.chat.id, uid, 0)


# ==================== ИЗБРАННОЕ ====================

async def add_to_favorites_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавление в избранное"""
    q = update.callback_query
    
    # Извлекаем ID детали
    try:
        item_id = int(q.data.split(":")[1])
    except (IndexError, ValueError):
        await q.answer("❌ Ошибка", show_alert=True)
        return
    
    uid = q.from_user.id
    
    # Получаем или создаём список избранного
    st = data.user_state.setdefault(uid, {})
    favorites = st.setdefault("favorites", [])
    
    if item_id in favorites:
        await q.answer("⭐ Уже в избранном!", show_alert=True)
        return
    
    favorites.append(item_id)
    await q.answer("✅ Добавлено в избранное!", show_alert=False)


async def share_item_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поделиться деталью"""
    q = update.callback_query
    
    try:
        item_id = int(q.data.split(":")[1])
    except (IndexError, ValueError):
        await q.answer("❌ Ошибка", show_alert=True)
        return
    
    uid = q.from_user.id
    st = data.user_state.get(uid, {})
    results = st.get("results")
    
    if results is None or item_id not in results.index:
        await q.answer("❌ Деталь не найдена", show_alert=True)
        return
    
    item = results.loc[item_id].to_dict()
    
    # Формируем текст для шаринга
    share_text = (
        f"📦 Деталь из базы\n\n"
        f"🔢 Код: {data.val(item, 'код')}\n"
        f"📝 Наименование: {data.val(item, 'наименование')}\n"
        f"🔧 Тип: {data.val(item, 'тип')}\n"
    )
    
    part_num = data.val(item, 'парт номер')
    if part_num != "—":
        share_text += f"🏷️ Парт номер: {part_num}\n"
    
    await q.message.reply_text(share_text)
    await q.answer("✅ Информация отправлена", show_alert=False)


# ==================== СПИСАНИЕ (из оригинального кода) ====================

async def on_issue_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало процесса списания"""
    q = update.callback_query
    await q.answer()
    
    try:
        item_id = int(q.data.split(":")[1])
    except (IndexError, ValueError):
        await q.answer("❌ Ошибка", show_alert=True)
        return ConversationHandler.END
    
    uid = q.from_user.id
    st = data.user_state.get(uid, {})
    results = st.get("results")
    
    if results is None or item_id not in results.index:
        await q.answer("❌ Деталь не найдена", show_alert=True)
        return ConversationHandler.END
    
    part = results.loc[item_id].to_dict()
    
    # Инициализируем состояние списания
    data.issue_state[uid] = {
        "part": part,
        "await_quantity": True
    }
    
    await q.message.reply_text(
        f"📦 <b>Списание детали</b>\n\n"
        f"🔢 Код: <code>{data.val(part, 'код')}</code>\n"
        f"📝 Наименование: {data.val(part, 'наименование')}\n\n"
        f"Введите количество для списания (от 0 до {MAX_QTY}):",
        parse_mode="HTML",
        reply_markup=cancel_markup()
    )
    
    return data.ASK_QUANTITY


async def handle_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода количества"""
    context.chat_data["suppress_next_search"] = True
    uid = update.effective_user.id
    text = (update.message.text or "").strip().replace(",", ".")
    
    try:
        qty = float(text)
        if not math.isfinite(qty) or qty <= 0 or qty > MAX_QTY:
            raise ValueError
        qty = float(f"{qty:.3f}")
    except Exception:
        await update.message.reply_text(
            f"❌ Введите число от 0 до {MAX_QTY}\n"
            f"Пример: 1 или 2.5",
            reply_markup=cancel_markup()
        )
        return data.ASK_QUANTITY
    
    st = data.issue_state.get(uid)
    if not st or "part" not in st:
        await update.message.reply_text(
            "❌ Списание неактивно. Начните заново.",
            reply_markup=main_menu_markup()
        )
        return ConversationHandler.END
    
    st["quantity"] = qty
    st["await_comment"] = True
    
    await update.message.reply_text(
        "💬 <b>Добавьте комментарий</b>\n\n"
        "Например: <i>Линия сборки CSS OP-1100</i>\n"
        "Или отправьте <code>-</code> чтобы пропустить",
        parse_mode="HTML",
        reply_markup=cancel_markup()
    )
    
    return data.ASK_COMMENT


async def handle_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка комментария"""
    context.chat_data["suppress_next_search"] = True
    uid = update.effective_user.id
    comment = (update.message.text or "").strip()
    
    st = data.issue_state.get(uid)
    if not st or "part" not in st or "quantity" not in st:
        await update.message.reply_text(
            "❌ Что-то пошло не так. Начните заново.",
            reply_markup=main_menu_markup()
        )
        return ConversationHandler.END
    
    part = st["part"]
    qty = st["quantity"]
    st["comment"] = "" if comment == "-" else comment
    
    # Подтверждение
    confirm_text = (
        "✅ <b>Подтвердите списание</b>\n\n"
        f"🔢 Код: <code>{data.val(part, 'код')}</code>\n"
        f"📝 Наименование: {data.val(part, 'наименование')}\n"
        f"📦 Количество: <b>{qty}</b>\n"
        f"💬 Комментарий: {escape(st['comment']) if st['comment'] else '—'}"
    )
    
    await update.message.reply_text(
        confirm_text,
        parse_mode="HTML",
        reply_markup=confirm_markup()
    )
    
    return data.ASK_CONFIRM


async def handle_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение списания"""
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    
    if q.data == "confirm_yes":
        st = data.issue_state.get(uid)
        if not st or "part" not in st or "quantity" not in st:
            data.issue_state.pop(uid, None)
            await q.message.reply_text(
                "❌ Данных для списания нет.",
                reply_markup=main_menu_markup()
            )
            return ConversationHandler.END
        
        part = st["part"]
        qty = st["quantity"]
        comment = st.get("comment", "")
        
        # Сохраняем в Google Sheets
        await save_issue_to_sheet(context.bot, q.from_user, part, qty, comment)
        data.issue_state.pop(uid, None)
        
        await q.message.reply_text(
            f"✅ <b>Списание выполнено!</b>\n\n"
            f"🔢 Код: <code>{data.val(part, 'код')}</code>\n"
            f"📝 Наименование: {data.val(part, 'наименование')}\n"
            f"📦 Количество: <b>{qty}</b>\n"
            f"💬 Комментарий: {escape(comment) if comment else '—'}",
            parse_mode="HTML",
            reply_markup=main_menu_markup()
        )
        return ConversationHandler.END
    
    elif q.data == "confirm_no":
        data.issue_state.pop(uid, None)
        await q.message.reply_text(
            "❌ Списание отменено.",
            reply_markup=main_menu_markup()
        )
        return ConversationHandler.END


async def save_issue_to_sheet(bot, user, part: dict, quantity, comment: str):
    """Сохранение списания в Google Sheets"""
    from app.config import SPREADSHEET_URL
    import gspread
    
    client = data.get_gs_client()
    sh = client.open_by_url(SPREADSHEET_URL)
    
    try:
        ws = sh.worksheet("История")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="История", rows=1000, cols=12)
        ws.append_row([
            "Дата", "ID", "Имя", "Тип",
            "Наименование", "Код", "Количество", "Комментарий"
        ])
    
    headers_raw = ws.row_values(1)
    headers = [h.strip() for h in headers_raw]
    norm = [h.lower() for h in headers]
    
    full_name = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()
    display_name = full_name or (f"@{user.username}" if user.username else str(user.id))
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
        "количество": str(quantity),
        "qty": str(quantity),
        "комментарий": comment or "",
        "comment": comment or "",
    }
    
    row = [values_by_key.get(hn, "") for hn in norm]
    ws.append_row(row, value_input_option="USER_ENTERED")
    logger.info(f"💾 Списание записано: {display_name}, {quantity}x {part.get('код')}")


async def cancel_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена текущего действия"""
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    
    if uid in data.issue_state:
        data.issue_state.pop(uid, None)
    
    await q.message.reply_text(
        "❌ Действие отменено.",
        reply_markup=main_menu_markup()
    )
    return ConversationHandler.END


# ==================== КОМАНДЫ ДЛЯ АДМИНОВ ====================

async def reload_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /reload - перезагрузка базы данных"""
    uid = update.effective_user.id
    
    # Проверка прав (из оригинального кода)
    from improved_handlers import is_admin
    if not is_admin(uid):
        await update.message.reply_text("❌ Нет доступа")
        return
    
    await update.message.reply_text("🔄 Перезагружаю базу данных...")
    
    try:
        await asyncio.to_thread(data.force_reload)
        await update.message.reply_text(
            "✅ База данных обновлена!",
            reply_markup=main_menu_markup()
        )
    except Exception as e:
        logger.error(f"Reload failed: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /broadcast для рассылки (только для админов)"""
    uid = update.effective_user.id
    
    from improved_handlers import is_admin
    if not is_admin(uid):
        await update.message.reply_text("❌ Нет доступа")
        return
    
    # Простая реализация
    text = " ".join(context.args)
    if not text:
        await update.message.reply_text(
            "📢 Использование:\n/broadcast Текст сообщения"
        )
        return
    
    await update.message.reply_text(
        f"📢 Рассылка:\n{text}\n\n"
        "(Функция в разработке)"
    )


# ==================== РЕГИСТРАЦИЯ ВСЕХ ХЕНДЛЕРОВ ====================

def register_handlers(app):
    """Регистрация всех обработчиков"""
    import math
    
    # Гварды (приоритет -1)
    app.add_handler(MessageHandler(filters.ALL, guard_msg), group=-1)
    app.add_handler(CallbackQueryHandler(guard_cb, pattern=".*"), group=-1)
    
    # Основные команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("reload", reload_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    
    # Обработчики главного меню
    app.add_handler(CallbackQueryHandler(menu_search_cb, pattern=r"^menu_search$"))
    app.add_handler(CallbackQueryHandler(menu_categories_cb, pattern=r"^menu_categories$"))
    app.add_handler(CallbackQueryHandler(menu_favorites_cb, pattern=r"^menu_favorites$"))
    app.add_handler(CallbackQueryHandler(menu_history_cb, pattern=r"^menu_history$"))
    app.add_handler(CallbackQueryHandler(menu_issue_help_cb, pattern=r"^menu_issue_help$"))
    app.add_handler(CallbackQueryHandler(menu_contact_cb, pattern=r"^menu_contact$"))
    app.add_handler(CallbackQueryHandler(menu_export_cb, pattern=r"^menu_export$"))
    app.add_handler(CallbackQueryHandler(menu_help_cb, pattern=r"^menu_help$"))
    app.add_handler(CallbackQueryHandler(back_main_cb, pattern=r"^back_main$"))
    app.add_handler(CallbackQueryHandler(noop_cb, pattern=r"^noop$"))
    
    # Категории
    app.add_handler(CallbackQueryHandler(cat_type_cb, pattern=r"^cat_type$"))
    app.add_handler(CallbackQueryHandler(cat_manufacturer_cb, pattern=r"^cat_manufacturer$"))
    app.add_handler(CallbackQueryHandler(search_by_category_cb, pattern=r"^search_(type|mfr):"))
    
    # Пагинация и просмотр
    app.add_handler(CallbackQueryHandler(on_page_callback, pattern=r"^page:\d+$"))
    app.add_handler(CallbackQueryHandler(on_view_callback, pattern=r"^view:\d+$"))
    
    # Фильтры
    app.add_handler(CallbackQueryHandler(show_filters_cb, pattern=r"^show_filters$"))
    app.add_handler(CallbackQueryHandler(apply_filters_cb, pattern=r"^filter_apply$"))
    
    # Избранное и шаринг
    app.add_handler(CallbackQueryHandler(add_to_favorites_cb, pattern=r"^fav_add:\d+$"))
    app.add_handler(CallbackQueryHandler(share_item_cb, pattern=r"^share:\d+$"))
    
    # Диалог списания
    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(on_issue_click, pattern=r"^issue:\d+$")],
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
        fallbacks=[
            CommandHandler("cancel", cancel_cmd),
            CallbackQueryHandler(cancel_action, pattern=r"^cancel_action$")
        ],
        allow_reentry=True,
        per_chat=True,
        per_user=True,
        per_message=False,
    )
    app.add_handler(conv)
    
    # Текстовый поиск (последний, группа 1)
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, search_text),
        group=1
    )
    
    logger.info("✅ Все обработчики зарегистрированы")


# Экспорт для использования в main.py
__all__ = ['register_handlers']
