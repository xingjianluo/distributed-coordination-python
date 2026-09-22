"""FastAPI 入口：把租约状态机暴露为 HTTP 接口。

路由：
- POST /resources/{resource}/acquire  body: client_id, ttl, request_id?
- POST /resources/{resource}/renew    body: client_id, version, ttl?
- POST /resources/{resource}/release  body: client_id, version
- POST /resources/{resource}/access   body: client_id, version
- GET  /quota/{client_id}             查看配额
- PUT  /quota/{client_id}             body: quota，设置配额
- GET  /snapshot                      状态快照（调试用）

所有 :class:`CoordinationError` 统一转换为携带稳定 ``error`` 码的 JSON。
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import CoordinationError
from .models import (
    AccessRequest,
    AcquireRequest,
    ReleaseRequest,
    RenewRequest,
)
from .store import LeaseStore


class AcquireBody(BaseModel):
    client_id: str = Field(min_length=1)
    ttl: float = Field(gt=0)
    request_id: str | None = None


class RenewBody(BaseModel):
    client_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    ttl: float | None = Field(default=None, gt=0)


class VersionBody(BaseModel):
    client_id: str = Field(min_length=1)
    version: int = Field(ge=1)


class QuotaBody(BaseModel):
    quota: int = Field(ge=1)


def create_app(store: LeaseStore | None = None) -> FastAPI:
    store = store or LeaseStore()
    app = FastAPI(title="distributed-coordination")

    @app.exception_handler(CoordinationError)
    async def _on_coordination_error(_: Request, exc: CoordinationError):
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.post("/resources/{resource}/acquire")
    def acquire(resource: str, body: AcquireBody):
        token = store.acquire(AcquireRequest(
            resource=resource, client_id=body.client_id,
            ttl=body.ttl, request_id=body.request_id))
        return {"resource": token.resource, "version": token.version,
                "fence_token": token.encode()}

    @app.post("/resources/{resource}/renew")
    def renew(resource: str, body: RenewBody):
        token = store.renew(RenewRequest(
            resource=resource, client_id=body.client_id,
            version=body.version, ttl=body.ttl))
        return {"resource": token.resource, "version": token.version,
                "renewed": True}

    @app.post("/resources/{resource}/release")
    def release(resource: str, body: VersionBody):
        token = store.release(ReleaseRequest(
            resource=resource, client_id=body.client_id,
            version=body.version))
        return {"resource": token.resource, "version": token.version,
                "released": True}

    @app.post("/resources/{resource}/access")
    def access(resource: str, body: VersionBody):
        token = store.access(AccessRequest(
            resource=resource, client_id=body.client_id,
            version=body.version))
        return {"resource": token.resource, "version": token.version,
                "allowed": True}

    @app.get("/quota/{client_id}")
    def get_quota(client_id: str):
        return {"client_id": client_id, "quota": store.get_quota(client_id)}

    @app.put("/quota/{client_id}")
    def set_quota(client_id: str, body: QuotaBody):
        store.set_quota(client_id, body.quota)
        return {"client_id": client_id, "quota": body.quota}

    @app.get("/snapshot")
    def snapshot():
        return store.snapshot()

    # 便于测试访问内部 store
    app.state.store = store
    return app
