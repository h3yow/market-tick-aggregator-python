"""运行期配置：仅依赖本地环境，默认使用本地 SQLite 文件。"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    db_path: str = os.environ.get("TICK_DB_PATH", "data/ticks.db")
    busy_timeout_ms: int = 10_000


settings = Settings()
