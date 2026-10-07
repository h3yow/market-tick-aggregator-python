"""错误分类：领域异常与 FastAPI 适配。"""

from __future__ import annotations


class DomainError(Exception):
    """所有可预期业务错误的基类。"""

    error_code: str = "domain_error"
    http_status: int = 400

    def __init__(self, message: str, rejected_records: list | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.rejected_records = rejected_records or []


class InvalidBatchError(DomainError):
    """批次整体无效（空批次、超长、批次 ID 非法、含无效记录）。"""

    error_code = "invalid_batch"
    http_status = 422

    def __init__(
        self,
        message: str,
        rejected_records: list | None = None,
        code: str = "invalid_batch",
    ) -> None:
        super().__init__(message, rejected_records)
        self.error_code = code


class BatchConflictError(DomainError):
    """相同 batch_id 重放但负载与首次不一致。"""

    error_code = "batch_id_conflict"
    http_status = 409


class CorrectionConflictError(DomainError):
    """相同 correction_id 的元数据/负载/分片内容与首次提交不一致。"""

    error_code = "correction_conflict"
    http_status = 409


class NotFoundError(DomainError):
    error_code = "not_found"
    http_status = 404
