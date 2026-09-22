"""HTTP 入口：把进程内租约协调器暴露为 REST 接口。

错误分类映射：所有 LeaseRejected 都以 409 返回，body 中 reason 字段
给出可区分的拒绝原因码（见 coordination.errors.RejectionReason）。
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from coordination import LeaseRegistry, LeaseRejected
from coordination.quota import ClientQuota

app = FastAPI(title="distributed-coordination")
registry = LeaseRegistry()
quota = ClientQuota(max_concurrent=8)


class AcquireRequest(BaseModel):
    resource: str
    client: str
    ttl: float = Field(gt=0)


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
    with quota.acquire(req.client):
        lease = registry.acquire(req.resource, req.client, req.ttl)
    return lease.__dict__


@app.post("/leases/release")
def release(req: ReleaseRequest):
    with quota.acquire(req.client):
        registry.release(req.resource, req.client, req.fencing_token)
    return {"released": req.resource}


@app.post("/leases/renew")
def renew(req: RenewRequest):
    with quota.acquire(req.client):
        lease = registry.renew(req.resource, req.client, req.fencing_token, req.ttl)
    return lease.__dict__


@app.post("/leases/guarded")
def guarded(req: GuardedRequest):
    registry.guarded_access(req.resource, req.fencing_token)
    return {"ok": True}


@app.get("/leases/{resource}")
def holder(resource: str):
    lease = registry.holder_of(resource)
    return {"lease": lease.__dict__ if lease else None}
