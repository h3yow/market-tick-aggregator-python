"""依赖装配：进程内单例（默认本地 SQLite 文件）。

``TICK_DB_PATH`` 环境变量可覆盖数据库位置；测试可用 :memory: 或临时文件。
"""

from __future__ import annotations

import threading

from .config import settings
from .service import TickService
from .storage import TickStore

_store: TickStore | None = None
_service: TickService | None = None
_init_lock = threading.Lock()


def get_service() -> TickService:
    """返回进程级单例服务。"""
    global _store, _service
    if _service is None:
        with _init_lock:
            if _service is None:
                _store = TickStore(settings.db_path, settings.busy_timeout_ms)
                _service = TickService(_store)
    return _service


def reset_service(
    db_path: str | None = None, busy_timeout_ms: int | None = None
) -> TickService:
    """测试辅助：关闭旧实例并以指定数据库重新初始化。"""
    global _store, _service
    with _init_lock:
        if _store is not None:
            _store.close()
        _store = TickStore(
            db_path if db_path is not None else settings.db_path,
            busy_timeout_ms if busy_timeout_ms is not None else settings.busy_timeout_ms,
        )
        _service = TickService(_store)
    return _service
