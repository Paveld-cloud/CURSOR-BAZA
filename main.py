import logging
from telegram.ext import ApplicationBuilder
from app.config import TELEGRAM_TOKEN, WEBHOOK_URL, WEBHOOK_PATH, PORT, WEBHOOK_SECRET_TOKEN, TZ_NAME
from app.data import initial_load
from app.handlers import register_handlers
from app.webapp import build_web_app  # <-- добавили

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bot")

def main():
    logger.info(f"⌚ Используем часовой пояс: {TZ_NAME}")
    if not WEBHOOK_SECRET_TOKEN:
        logger.warning("WEBHOOK_SECRET_TOKEN не задан — рекомендуется включить для продакшена.")

    # Начальная синхронная загрузка
    initial_load()

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    register_handlers(app)

    full_webhook = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
    logger.info(f"🚀 Стартуем webhook-сервер на 0.0.0.0:{PORT}")
    logger.info(f"🌐 Устанавливаем webhook: {full_webhook}")

    # ✅ Mini App + API на том же aiohttp-сервере
    web_app = build_web_app()

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        secret_token=WEBHOOK_SECRET_TOKEN or None,
        webhook_url=full_webhook,
        url_path=WEBHOOK_PATH.lstrip("/"),
        drop_pending_updates=True,
        allowed_updates=None,
        web_app=web_app,  # <-- важно
    )

if __name__ == "__main__":
    main()
