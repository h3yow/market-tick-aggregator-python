"""pytest 公共夹具：每个测试使用独立的本地临时 SQLite 文件，无任何外部依赖。"""

from __future__ import annotations

import logging
import os
import tempfile

import pytest

from app.deps import reset_service
from app.service import TickService
from app.storage import TickStore


def _log_case(request, **fields) -> None:
    """打印用例输入数据与判定依据（满足可审计要求）。"""
    logging.getLogger("tests").info(
        "[%s] %s %s",
        request.node.name,
        request.node.fspath.basename,
        " ".join(f"{k}={v!r}" for k, v in fields.items()),
    )


@pytest.fixture
def db_path(tmp_path) -> str:
    return str(tmp_path / "test_ticks.db")


@pytest.fixture
def store(db_path) -> TickStore:
    s = TickStore(db_path)
    yield s
    s.close()


@pytest.fixture
def service(store) -> TickService:
    return TickService(store)


@pytest.fixture
def db_path_factory(tmp_path):
    def _make(name: str = "ticks.db") -> str:
        return str(tmp_path / name)

    return _make


@pytest.fixture(autouse=True)
def _test_logging(request, caplog):
    """保证每个用例的输入与判定日志可见。"""
    caplog.set_level(logging.INFO, logger="market_tick.service")
    caplog.set_level(logging.INFO, logger="tests")
    request.node._log_case = lambda **kw: _log_case(request, **kw)  # type: ignore[attr-defined]
    yield


@pytest.fixture
def client_factory(db_path_factory):
    """生成绑定独立数据库的 TestClient。"""
    from fastapi.testclient import TestClient

    import main

    clients = []

    def _make(name: str = "api.db"):
        path = db_path_factory(name)
        reset_service(path)
        c = TestClient(main.app)
        clients.append(c)
        return c, path

    yield _make
    for c in clients:
        c.close()


@pytest.fixture
def client(client_factory):
    c, _ = client_factory()
    return c
