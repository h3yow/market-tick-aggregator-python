"""异常与错误分类（骨架）。"""

from __future__ import annotations


class AppError(Exception):
    """服务层错误基类。"""


class ValidationBatchError(AppError):
    """整批不可处理的请求级错误（HTTP 400）。"""


class StorageError(AppError):
    """持久化层错误（HTTP 500），可重试。"""


class ConflictError(AppError):
    """并发冲突（HTTP 409），调用方应重试。"""
