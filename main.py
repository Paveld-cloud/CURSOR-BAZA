import asyncio
import logging
import signal
from aiohttp import web
from telegram import Update
from telegram.ext import ApplicationBuilder

# Импорт конфигураций и обработчиков из вашего проекта 
from app.config import (
    TELEGRAM_TOKEN, 
    WEBHOOK_URL, 
    WEBHOOK_PATH, 
    PORT, 
    WEBHOOK_SECRET_TOKEN, 
    TZ_NAME,
)
from app.data import initial_load
from app.handlers import register_handlers
from app.webapp import build_web_app

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("bot-core")

def _normalize_full_url(url: str, path: str) -> str:
    """Создает полный URL для Telegram Webhook """
    base = (url or "").strip().rstrip("/")
    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"
    
    p = (path or "").strip()
    if not p.startswith("/"):
        p = f"/{p}"
    return f"{base}{p}"

def _normalize_local_path(path: str) -> str:
    """Создает относительный путь для внутреннего роутера сервера """
    p = (path or "").strip()
    if not p.startswith("/"):
        p = f"/{p}"
    return p

async def main_async():
    logger.info(f"🚀 Запуск системы (Часовой пояс: {TZ_NAME}) ")

    # 1) Загрузка базы данных (Google Sheets) перед стартом 
    try:
        initial_load()
        logger.info("✅ База данных успешно загружена")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при загрузке данных: {e}")
        # В продакшене можно решить, останавливать ли приложение или продолжать
        # return 

    # 2) Инициализация Telegram Application 
    tg_app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    register_handlers(tg_app)

    # 3) Подготовка Web-приложения (Mini App) 
    web_app = build_web_app()
    
    # ПУТИ: Разделяем полный URL для Telegram и локальный путь для aiohttp
    full_webhook_url = _normalize_full_url(WEBHOOK_URL, WEBHOOK_PATH)
    local_webhook_path = _normalize_local_path(WEBHOOK_PATH)

    async def telegram_webhook_handler(request: web.Request) -> web.Response:
        """Обработчик обновлений от Telegram """
        # Проверка секретного токена для защиты эндпоинта
        if WEBHOOK_SECRET_TOKEN:
            header_token = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
            if header_token != WEBHOOK_SECRET_TOKEN:
                logger.warning("🚫 Попытка несанкционированного доступа к Webhook")
                return web.Response(status=403, text="Forbidden")

        try:
            data = await request.json()
            update = Update.de_json(data, tg_app.bot)
            # Обработка обновления в фоновой задаче
            asyncio.create_task(tg_app.process_update(update))
            return web.Response(text="OK")
        except Exception as e:
            logger.error(f"⚠️ Ошибка при обработке webhook: {e}")
            return web.Response(status=400)

    # Регистрация маршрутов в aiohttp 
    web_app.router.add_post(local_webhook_path, telegram_webhook_handler)
    web_app.router.add_get("/health", lambda r: web.Response(text="Healthy"))

    # 4) Старт Telegram Bot 
    await tg_app.initialize()
    await tg_app.start()

    # Установка вебхука в Telegram
    await tg_app.bot.set_webhook(
        url=full_webhook_url,
        secret_token=WEBHOOK_SECRET_TOKEN or None,
        drop_pending_updates=True,
        allowed_updates=None
    )

    # 5) Запуск сервера aiohttp 
    runner = web.AppRunner(web_app)
    await runner.setup()
    
    # Railway передает PORT автоматически 
    server_port = int(PORT) if PORT else 8080
    site = web.TCPSite(runner, host="0.0.0.0", port=server_port)
    
    logger.info(f"🌐 Сервер Mini App запущен на порту {server_port}")
    logger.info(f"🔗 Webhook установлен на: {full_webhook_url}")
    
    await site.start()

    # Настройка завершения работы (Graceful Shutdown) 
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    
    def _handle_exit():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_exit)
        except NotImplementedError:
            pass # Для совместимости с Windows (локальная разработка)

    await stop_event.wait()

    # 6) Остановка сервисов 
    logger.info("🛑 Остановка приложения...")
    await runner.cleanup()
    await tg_app.stop()
    await tg_app.shutdown()
    logger.info("✅ Приложение успешно остановлено")

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
