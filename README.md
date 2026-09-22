# 本地分布式协调能力

用进程内状态模拟多个客户端竞争同一组资源，提供基于租约的互斥占用、
持有权版本（fencing token）与按客户端维度的并发配额。所有协调结论
在各入口（Python API 与 HTTP API）下一致且可解释。

## 租约与凭证语义

- **互斥占用**：同一资源同一时刻最多一个持有者。只有租约到期（TTL 耗尽）
  或持有者主动释放后，其他客户端才能获取。
- **fencing token**：每次成功获取或续租都签发一个严格递增的整数凭证，
  按资源维度单调递增，释放或过期后**永不回退、永不复活**。
- **续租**：仅持有者在租约未过期时可续租；续租签发新 token，旧 token
  立即失效。过期后的续租请求被拒绝（`lease_expired`），旧凭证不得复活。
- **受保护操作**：`guarded_access` 只放行携带当前有效凭证的请求；
  落后凭证（如续租前的旧 token）一律拒绝（`stale_fencing_token`）。
- **并发配额**：同一客户端的在途请求数有上限，超限立即拒绝
  （`quota_exceeded`），不排队；许可经上下文管理器归还，异常路径
  也会释放，失败重试后不留状态残留。

## 错误分类

所有拒绝抛出 `LeaseRejected`，`reason` 字段为可区分的原因码
（HTTP 入口统一返回 409，body 中 `error` 字段为同一原因码）：

| 原因码 | 含义 |
| --- | --- |
| `held_by_other` | 资源被其他客户端持有且未过期（抢占被拒） |
| `not_holder` | 伪造持有者身份 / 非持有者执行持有者操作 |
| `stale_fencing_token` | 携带落后或不存在的凭证访问受保护操作 |
| `lease_expired` | 租约已过期，续租或受保护访问无效 |
| `not_held` | 资源当前无人持有（如重复释放） |
| `quota_exceeded` | 客户端并发配额耗尽 |

## 代码结构

- `coordination/clock.py` — 时钟抽象（`SystemClock` / 测试用 `ManualClock`）
- `coordination/errors.py` — `RejectionReason` 与 `LeaseRejected`
- `coordination/registry.py` — `LeaseRegistry`：租约互斥 + fencing token
- `coordination/quota.py` — `ClientQuota`：按客户端的并发配额
- `main.py` — FastAPI 入口（`POST /leases/acquire|release|renew|guarded`，
  `GET /leases/{resource}`；既有 `GET /` 行为不变）
- `tests/` — 竞争、过期与凭证回退场景测试

## 本地验证

```bash
uv sync
uv run python -m pytest tests/ -q        # 运行全部测试
uv run python -m pytest tests/ -q -s     # 查看每个用例打印的关键输入与判定依据
uv run fastapi dev main.py               # 本地启动 HTTP 服务
```
