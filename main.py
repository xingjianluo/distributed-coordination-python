"""HTTP 入口：把统一协调服务（LeaseService）暴露为 REST 接口。

库直调与 HTTP 调用共用同一套协调逻辑，因此配额口径、拒绝原因与幂等
结论在两种调用方式下完全一致。

错误分类映射：所有 LeaseRejected 都以 409 返回，body 中 error 字段
给出可区分的拒绝原因码（见 coordination.errors.RejectionReason）。
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from coordination import LeaseRejected, LeaseService


class AcquireRequest(BaseModel):
    resource: str
    client: str
    ttl: float = Field(gt=0)
    idempotency_key: str | None = None


class BatchAcquireRequest(BaseModel):
    resources: list[str] = Field(min_length=1)
    client: str
    ttl: float = Field(gt=0)
    idempotency_key: str | None = None


class ReleaseRequest(BaseModel):
    resource: str
    client: str
    fencing_token: int


class RenewRequest(BaseModel):
    resource: str
    client: str
    fencing_token: int
    ttl: float = Field(gt=0)


class GuardedRequest(BaseModel):
    resource: str
    fencing_token: int


def create_app(service: LeaseService | None = None) -> FastAPI:
    """构造应用；默认使用全局单例服务，测试可注入隔离实例。"""
    app = FastAPI(title="distributed-coordination")
    svc = service or get_service()

    @app.exception_handler(LeaseRejected)
    async def lease_rejected_handler(_: Request, exc: LeaseRejected) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"error": exc.reason.value, "detail": exc.detail},
        )

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.post("/leases/acquire", status_code=201)
    def acquire(req: AcquireRequest):
        lease = svc.acquire(
            req.resource,
            req.client,
            req.ttl,
            idempotency_key=req.idempotency_key,
        )
        return lease.__dict__

    @app.post("/leases/acquire-batch", status_code=201)
    def acquire_batch(req: BatchAcquireRequest):
        leases = svc.acquire_batch(
            req.resources,
            req.client,
            req.ttl,
            idempotency_key=req.idempotency_key,
        )
        return {"leases": [lease.__dict__ for lease in leases]}

    @app.post("/leases/release")
    def release(req: ReleaseRequest):
        svc.release(req.resource, req.client, req.fencing_token)
        return {"released": req.resource}

    @app.post("/leases/renew")
    def renew(req: RenewRequest):
        lease = svc.renew(req.resource, req.client, req.fencing_token, req.ttl)
        return lease.__dict__

    @app.post("/leases/guarded")
    def guarded(req: GuardedRequest):
        svc.guarded_access(req.resource, req.fencing_token)
        return {"ok": True}

    @app.get("/leases/{resource}")
    def holder(resource: str):
        lease = svc.holder_of(resource)
        return {"lease": lease.__dict__ if lease else None}

    return app


_service: LeaseService | None = None


def get_service() -> LeaseService:
    """进程级单例：库入口与 HTTP 入口共享同一个协调服务。"""
    global _service
    if _service is None:
        _service = LeaseService(max_concurrent=8)
    return _service


app = create_app()
