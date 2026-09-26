"""测试公共夹具：每个测试使用独立临时数据库，互不干扰。"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.service import MarketService  # noqa: E402
from app.store import TickStore  # noqa: E402

# 单测日志：打印输入数据和判定依据
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
log = logging.getLogger("tests")


@pytest.fixture
def service(tmp_path: Path):
    store = TickStore(tmp_path / "test_market.db")
    store.init_db()
    svc = MarketService(store)
    yield svc
    store.close()


@pytest.fixture
def store(tmp_path: Path):
    s = TickStore(tmp_path / "test_market.db")
    s.init_db()
    yield s
    s.close()
