import logging
from pathlib import Path
from aiohttp import web

logger = logging.getLogger("bot.webapp")

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"


async def page_index(request: web.Request):
    # /app
    return web.FileResponse(WEB_DIR / "index.html")


async def page_item(request: web.Request):
    # /app/item
    return web.FileResponse(WEB_DIR / "item.html")


async def api_health(request: web.Request):
    # /app/api/health
    return web.json_response(
        {"ok": True, "service": "BAZA MG Mini App", "path": "/app"}
    )


def build_web_app() -> web.Application:
    """
    Mini App для Telegram.
    Поднимается на aiohttp и монтируется на /app.

    Важно: index.html сейчас запрашивает /static/style.css и /static/app.js,
    поэтому статика смонтирована на /static/ (и дополнительно на /app/static/).
    """
    app = web.Application()

    # Pages
    app.router.add_get("/app", page_index)
    app.router.add_get("/app/", page_index)
    app.router.add_get("/app/item", page_item)

    # API
    app.router.add_get("/app/api/health", api_health)

    static_dir = WEB_DIR / "static"

    # ✅ Основная статика (как у тебя в HTML: /static/...)
    app.router.add_static(
        "/static/",
        str(static_dir),
        show_index=False,
    )

    # ✅ Алиас (если позже переведёшь HTML на /app/static/...)
    app.router.add_static(
        "/app/static/",
        str(static_dir),
        show_index=False,
    )

    logger.info("Mini App mounted at /app (static: /static/* and /app/static/*)")
    return app
