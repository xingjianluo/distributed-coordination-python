# 本地分布式协调能力

用进程内状态模拟多个客户端竞争同一组资源，提供基于租约的互斥占用、
持有权版本（fencing token）、按客户端维度的统一并发配额、超时重试幂等
与批量原子占用。所有协调结论在各入口（Python 库 API 与 HTTP API）下
一致且可解释；两种入口共享同一个 `LeaseService`，不存在两套口径。

## 租约与凭证语义

- **互斥占用**：同一资源同一时刻最多一个持有者。只有租约到期（TTL 耗尽）
  或持有者主动释放后，其他客户端才能获取。
- **fencing token**：每次成功获取或续租都签发一个严格递增的整数凭证，
  按资源维度单调递增，释放或过期后**永不回退、永不复活**。
- **续租**：仅持有者在租约未过期时可续租；续租签发新 token，旧 token
  立即失效。过期后的续租请求被拒绝（`lease_expired`），旧凭证不得复活。
- **受保护操作**：`guarded_access` 只放行携带当前有效凭证的请求；
  落后凭证（如续租前的旧 token）一律拒绝（`stale_fencing_token`）。

## 统一并发配额（库直调与 HTTP 同一口径）

- **计量口径**：配额按客户端隔离，统计的是该客户端**当前有效持有的资源数**，
  而不是瞬时在途请求数。因此无论占用请求从库直调还是 HTTP 进来，都走同一个
  `LeaseService`、同一把锁、同一套结论。
- **判定顺序**：先判资源是否被其他客户端持有（`held_by_other`），再判客户端
  配额是否够（`quota_exceeded`），两种拒绝原因始终可区分。
- **占用成功才计入**；同持有者对自己仍持有资源的重复获取按续租处理，
  不重复占配额。
- **回收**：主动 `release` 立即归还许可；过期许可在下一次操作前被统一
  sweep 回收；占用失败不拿许可、不留状态。回收后额度立即可再用。
- **不排队**：到上限立即拒绝，短时间并发打进来也绝不会超过上限。
- HTTP 与库共享同一默认服务实例（`main.get_service()`，默认上限 8）；
  `main.create_app(service)` 可注入独立实例（测试隔离用）。

## 超时重试幂等（idempotency key）

占用（单个 / 批量）支持客户端在请求中携带自己生成的唯一标识
`idempotency_key`：

- 同一 `(client, idempotency_key)` 的整笔操作**只生效一次**：重试不签发
  新 fencing token、不重复占用资源、不多占配额，重放返回与首次**逐字段
  一致**的结果（即使首次结果是失败，也重放同一个可区分拒绝原因）。
- 即使客户端配额已满，携带原标识的重放仍返回首次结果，而不会被
  `quota_exceeded` 误拒。
- 标识缺失（未传/空串）视为普通操作；换一个新标识视为一次新操作。
- 单个占用与批量占用的标识命名空间相互隔离，相同字面键不会串用。

## 批量原子占用（all-or-nothing）

`acquire_batch(resources, client, ttl)` 一次申请一组资源：

- **全部成功才提交**：组内所有资源都拿到才返回全部租约。
- **任一失败整体失败**：某个资源被其他客户端持有（`held_by_other`）或
  新增所需许可超过剩余配额（`quota_exceeded`），整笔立即失败、不排队干等。
- **失败回滚**：冲突与配额都在任何写入前判定；极端情况下写入中途失败，
  已拿到的许可与已写入的新增租约会全部回滚，绝不留下"半组"持有，
  也不多占配额。
- 组内重复资源自动去重；组中包含该客户端**自己仍持有**的资源时按续租处理，
  只为真正新增的资源计配额。
- 并发下两个客户端批量争抢同一组资源：恰好一方全拿、另一方因冲突整笔失败，
  不会出现双持有。

## 错误分类

所有拒绝抛出 `LeaseRejected`，`reason` 字段为可区分的原因码
（HTTP 入口统一返回 409，body 中 `error` 字段为同一原因码）：

| 原因码 | 含义 |
| --- | --- |
| `held_by_other` | 资源被其他客户端持有且未过期（抢占被拒；批量中任一资源冲突） |
| `not_holder` | 伪造持有者身份 / 非持有者执行持有者操作 |
| `stale_fencing_token` | 携带落后或不存在的凭证访问受保护操作 |
| `lease_expired` | 租约已过期，续租或受保护访问无效 |
| `not_held` | 资源当前无人持有（如重复释放） |
| `quota_exceeded` | 客户端并发配额耗尽（单个或批量新增许可超限） |

## 代码结构

- `coordination/clock.py` — 时钟抽象（`SystemClock` / 测试用 `ManualClock`）
- `coordination/errors.py` — `RejectionReason` 与 `LeaseRejected`
- `coordination/registry.py` — `LeaseRegistry`：租约互斥 + fencing token
- `coordination/quota.py` — `ClientQuota`：按客户端的并发许可原语
- `coordination/service.py` — `LeaseService`：统一门面（统一配额 + 幂等 +
  批量原子占用），库入口与 HTTP 入口共用
- `main.py` — FastAPI 入口（`POST /leases/acquire`、
  `POST /leases/acquire-batch`、`/leases/release|renew|guarded`，
  `GET /leases/{resource}`；既有 `GET /` 行为不变）
- `tests/` — 竞争、过期、凭证回退、统一配额、幂等与批量原子性场景测试

### HTTP 请求示例

```bash
# 单个占用（可带 idempotency_key）
curl -s -XPOST localhost:8000/leases/acquire \
  -H 'content-type: application/json' \
  -d '{"resource":"r1","client":"c1","ttl":30,"idempotency_key":"uuid-1"}'

# 批量占用：全部成功返回 {"leases":[...]}，任一失败返回 409 + 原因码
curl -s -XPOST localhost:8000/leases/acquire-batch \
  -H 'content-type: application/json' \
  -d '{"resources":["r1","r2"],"client":"c1","ttl":30,"idempotency_key":"uuid-2"}'
```

## 本地验证

```bash
uv sync
uv run python -m pytest tests/ -q        # 运行全部测试（38 个用例）
uv run python -m pytest tests/ -q -s     # 查看每个用例打印的关键输入与判定依据
uv run fastapi dev main.py               # 本地启动 HTTP 服务
```

重点可关注的测试：

- `tests/test_service_unified.py` — 跨调用方式配额一致、拒绝原因可区分、
  释放/过期回收、按客户端隔离、并发 burst 不超限。
- `tests/test_idempotency.py` — 重放结果一致、不换版本、不多占配额，
  缺键/换键按新操作，失败重放同原因且无残留。
- `tests/test_batch.py` — 批量全成功、冲突/配额不足整体失败回滚、
  并发批量无双持有且立即返回、HTTP 批量端点行为。
