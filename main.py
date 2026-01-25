import asyncio
import logging
import signal
from aiohttp import web

from telegram import Update
from telegram.ext import ApplicationBuilder

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
from app.webapp import build_web_app  # твой Mini App (/app)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bot")


def _normalize_webhook_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    return url.rstrip("/")


def _normalize_path(path: str) -> str:
    path = (path or "").strip()
    if not path.startswith("/"):
        path = "/" + path
    return path


async def main_async():
    logger.info(f"⌚ Используем часовой пояс: {TZ_NAME}")

    if not WEBHOOK_SECRET_TOKEN:
        logger.warning("WEBHOOK_SECRET_TOKEN не задан — рекомендуется включить для продакшена.")

    # 1) Грузим базу (Google Sheets) ДО старта сервера
    initial_load()

    # 2) Telegram Application
    tg_app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    register_handlers(tg_app)

    # 3) Aiohttp app: Mini App + webhook endpoint
    web_app = build_web_app()

    webhook_url_base = _normalize_webhook_url(WEBHOOK_URL)
    webhook_path = _normalize_path(WEBHOOK_PATH)
    full_webhook = f"{webhook_url_base}{webhook_path}"

    logger.info(f"🚀 Стартуем aiohttp сервер на 0.0.0.0:{PORT}")
    logger.info(f"🌐 Устанавливаем webhook: {full_webhook}")

    async def telegram_webhook_handler(request: web.Request) -> web.Response:
        # Secret token check (если задан)
        if WEBHOOK_SECRET_TOKEN:
            got = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
            if got != WEBHOOK_SECRET_TOKEN:
                return web.Response(status=403, text="forbidden")

        data = await request.json()
        update = Update.de_json(data, tg_app.bot)

        # Быстро отдать 200 Telegram, обработку — в фоне
        asyncio.create_task(tg_app.process_update(update))
        return web.Response(text="ok")

    # webhook endpoint
    web_app.router.add_post(webhook_path, telegram_webhook_handler)

    # 4) Старт PTB
    await tg_app.initialize()
    await tg_app.start()

    # 5) Установить webhook в Telegram
    await tg_app.bot.set_webhook(
        url=full_webhook,
        secret_token=WEBHOOK_SECRET_TOKEN or None,
        drop_pending_updates=True,
        allowed_updates=None,
    )

    # 6) Старт aiohttp сервера
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=int(PORT))
    await site.start()

    stop_event = asyncio.Event()

    def _stop(*_):
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    await stop_event.wait()

    # shutdown
    logger.info("🛑 Остановка...")
    await runner.cleanup()
    await tg_app.stop()
    await tg_app.shutdown()


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
