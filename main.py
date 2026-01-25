import asyncio
import logging
import signal
from aiohttp import web
from telegram import Update
from telegram.ext import ApplicationBuilder

from app.config import (
    TELEGRAM_TOKEN, WEBHOOK_URL, WEBHOOK_PATH, 
    PORT, WEBHOOK_SECRET_TOKEN, TZ_NAME,
)
from app.data import initial_load
from app.handlers import register_handlers
from app.webapp import build_web_app

# Настройка логирования с более читаемым форматом
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("bot-core")

def _normalize_url(url: str, path: str) -> str:
    """Безопасная склейка URL и пути """
    base = (url or "").strip().rstrip("/")
    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"
    p = (path or "").strip()
    if not p.startswith("/"):
        p = f"/{p}"
    return f"{base}{p}"

async def main_async():
    logger.info(f"🚀 Запуск системы (TZ: {TZ_NAME})")

    # 1) Предварительная загрузка данных из Google Sheets 
    try:
        initial_load()
        logger.info("✅ База данных успешно загружена")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки базы: {e}")
        return

    # 2) Настройка Telegram Bot
    tg_app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    register_handlers(tg_app)

    # 3) Инициализация Web-приложения (Mini App) 
    web_app = build_web_app()
    full_webhook_url = _normalize_url(WEBHOOK_URL, WEBHOOK_PATH)

    async def telegram_webhook_handler(request: web.Request) -> web.Response:
        """Обработчик входящих обновлений от Telegram """
        if WEBHOOK_SECRET_TOKEN:
            if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET_TOKEN:
                return web.Response(status=403, text="Unauthorized")

        try:
            data = await request.json()
            update = Update.de_json(data, tg_app.bot)
            # Отправляем в фон, чтобы Telegram не ждал ответа 
            asyncio.create_task(tg_app.process_update(update))
            return web.Response(text="OK")
        except Exception as e:
            logger.error(f"⚠️ Ошибка в webhook: {e}")
            return web.Response(status=400)

    # Добавляем эндпоинт для бота и Health-check для Docker
    web_app.router.add_post(_normalize_url("", WEBHOOK_PATH), telegram_webhook_handler)
    web_app.router.add_get("/health", lambda r: web.Response(text="Healthy"))

    # 4) Жизненный цикл бота и сервера 
    await tg_app.initialize()
    await tg_app.start()
    
    await tg_app.bot.set_webhook(
        url=full_webhook_url,
        secret_token=WEBHOOK_SECRET_TOKEN or None,
        drop_pending_updates=True
    )

    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=int(PORT))
    
    logger.info(f"🌐 Сервер запущен на порту {PORT}")
    logger.info(f"🔗 Webhook установлен: {full_webhook_url}")
    
    await site.start()

    # Graceful shutdown (правильная остановка)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()

    logger.info("🛑 Завершение работы...")
    await runner.cleanup()
    await tg_app.stop()
    await tg_app.shutdown()

if __name__ == "__main__":
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
