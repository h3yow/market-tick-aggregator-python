"""兼容入口：委托给 app 包的 FastAPI 应用。

保留 ``python main.py`` 可直接启动；``GET /`` 行为不变。
"""

from app.main import app, create_app

__all__ = ["app", "create_app"]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
