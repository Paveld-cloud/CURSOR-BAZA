import logging
from pathlib import Path
from aiohttp import web

logger = logging.getLogger("bot.webapp")

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"


async def page_index(request: web.Request):
    return web.FileResponse(WEB_DIR / "index.html")


async def page_item(request: web.Request):
    return web.FileResponse(WEB_DIR / "item.html")


async def api_health(request: web.Request):
    return web.json_response({
        "ok": True,
        "service": "BAZA MG Mini App",
        "path": "/app"
    })


def build_web_app() -> web.Application:
    """
    Mini App для Telegram.
    Подключается в main.py через run_webhook(..., web_app=...)
    """
    app = web.Application()

    # Pages
    app.router.add_get("/app", page_index)
    app.router.add_get("/app/", page_index)
    app.router.add_get("/app/item", page_item)

    # API
    app.router.add_get("/app/api/health", api_health)

    # Static
    app.router.add_static(
        "/app/static/",
        str(WEB_DIR / "static"),
        show_index=False
    )

    logger.info("Mini App mounted at /app")
    return app

