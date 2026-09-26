"""冒烟测试：应用工厂与根路由兼容性。"""

from fastapi.testclient import TestClient

from app.main import create_app


def test_app_factory_and_root(tmp_path) -> None:
    app = create_app(tmp_path / "smoke.db")
    with TestClient(app) as client:
        assert client.get("/").json() == {"Hello": "World"}
        assert client.get("/health").json() == {"status": "ok"}
