# 本地分布式协调能力（租约 / 持有凭证 / 并发配额）

用**进程内状态 + 一把锁**模拟多个客户端竞争同一组资源，提供基于租约的互斥占用、
单调递增的持有凭证（fence token）、按客户端的并发配额与失败幂等重试。
无需任何外部依赖即可在本地运行与验证。

## 快速开始

```bash
uv sync                       # 安装依赖
.venv/bin/python -m pytest tests/ -v -s   # 运行全部单测（-s 可看到判定日志）
uv run fastapi dev main.py    # 启动 HTTP 服务，默认 http://127.0.0.1:8000
```

根路径行为保持不变：`GET /` 仍返回 `{"Hello": "World"}`。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/resources/{resource}/acquire` | 获取租约。body: `client_id`, `ttl`(秒,>0), `request_id?` |
| POST | `/resources/{resource}/renew`   | 续租。body: `client_id`, `version`, `ttl?` |
| POST | `/resources/{resource}/release` | 主动释放。body: `client_id`, `version` |
| POST | `/resources/{resource}/access`  | 访问受保护操作（校验持有者 + 凭证）。body: `client_id`, `version` |
| GET  | `/quota/{client_id}`            | 查看该客户端并发配额（默认 1） |
| PUT  | `/quota/{client_id}`            | 设置配额。body: `quota`(>=1) |
| GET  | `/snapshot`                     | 当前活跃租约/版本/配额快照（调试用） |

成功获取返回持有凭证：

```json
{"resource": "r1", "version": 3, "fence_token": "r1#3"}
```

### 使用示例

```bash
curl -s -XPOST localhost:8000/resources/r1/acquire \
  -H 'content-type: application/json' \
  -d '{"client_id":"alice","ttl":10,"request_id":"req-001"}'
# -> {"resource":"r1","version":1,"fence_token":"r1#1"}

curl -s -XPOST localhost:8000/resources/r1/access \
  -H 'content-type: application/json' \
  -d '{"client_id":"alice","version":1}'
```

## 租约（Lease）语义

- 每个资源同一时刻**至多有一个活跃持有者**；租约未到期时，其他客户端的
  获取请求一律以 `resource_held` 拒绝（不支持抢占）。
- 租约有 TTL：`expires_at = 获取/续租时刻 + ttl`。到期后惰性失效，
  其他客户端即可获取，过期租约不再占用持有者的并发配额。
- 持有者可主动 `release`；释放后他人可立即获取。
- 时间源可注入：生产用 `SystemClock`（单调时钟），测试用 `FakeClock`
  手动推进时间，确定性地验证过期行为。

## 持有凭证（Fence Token）语义

- 凭证为 `(resource, version)`，**版本号在单个资源维度严格递增、不可回退**，
  历史最大版本会一直保留。
- **只有成功 acquire 才产生新版本**；renew 成功只延长到期时间，版本号不变。
- 租约过期后，对旧凭证 renew 会得到 `lease_expired`——旧凭证不可能通过
  续租"复活"；他人获取后版本前进，旧持有者再拿旧凭证操作得到 `stale_fence`。
- `access`（受保护操作）同时校验三件事，按固定顺序给出可区分结论：
  1. 资源有活跃租约，否则 `lease_expired` / `resource_not_found`；
  2. 凭证版本能被识别（`version <= 历史最大版本`），否则 `invalid_fence`；
  3. 凭证版本等于当前版本，否则 `stale_fence`（落后凭证）；
  4. 客户端确为当前持有者，否则 `forged_holder`（伪造身份）。

## 并发配额与失败重试

- 配额按 `client_id` 统计**活跃租约数**（默认 1，可用 `/quota` 调整）；
  超出配额的获取以 `concurrency_quota_exceeded` 拒绝。
- **幂等重试**：acquire 携带 `request_id` 时，同一客户端相同 `request_id`
  的超时重试/抖动重发返回第一次的凭证，不会产生新版本或重复占用。
- 未带 `request_id` 时，若客户端自己仍活跃持有该资源，重复 acquire
  也返回同一凭证，杜绝"双持有"。
- 所有状态变更在同一把 `RLock` 下完成；8 线程并发争抢测试验证恰有 1 个
  成功者、其余 7 个收到 `resource_held`。

## 错误分类

拒绝原因与 HTTP 状态码一一对应，响应体形如
`{"error": "<code>", "message": "...", "resource": ..., "client_id": ...}`：

| `error` 码 | HTTP | 触发场景 |
| --- | --- | --- |
| `resource_held` | 409 | 租约未过期时被他人抢占 |
| `forged_holder` | 403 | 非当前持有者伪造身份执行 release / 用当前版本访问 |
| `lease_expired` | 410 | 租约已过期，或对过期凭证续租（旧凭证不得复活） |
| `stale_fence` | 409 | 携带落后于当前版本的凭证访问/续租/释放 |
| `invalid_fence` | 422 | 凭证版本无法识别（大于历史最大版本） |
| `already_released` | 409 | 对不存在活跃租约的资源重复释放 |
| `concurrency_quota_exceeded` | 429 | 超出该客户端的并发持有配额 |
| `resource_not_found` | 404 | 访问从未发放过凭证的资源 |

请求体本身不合法（缺字段、ttl <= 0 等）由 FastAPI 返回 `422`。

## 本地验证

```bash
# 全部测试（核心状态机 + HTTP 端到端），-s 显示关键输入与判定依据日志
.venv/bin/python -m pytest tests/ -v -s
```

测试覆盖：

- 正常获取 / 续租 / 访问 / 释放，版本严格递增；
- 未过期抢占被拒、伪造持有者身份被拒、重复释放被拒；
- 过期续租旧凭证被拒且不复活，过期后可被他人接管；
- 落后凭证 / 无法识别凭证访问受保护操作被拒；
- 8 线程并发争抢同一资源恰有一个成功者；
- 按客户端并发配额、`request_id` 幂等重试、抖动重复获取不产生双持有。

核心逻辑位于 `coordination/store.py`，所有判定分支都通过
`coordination.store` logger 打印**关键输入与判定依据**（INFO 级别）。

## 代码结构

```
coordination/
  clock.py    # SystemClock / FakeClock
  models.py   # Lease / FenceToken / 请求模型
  errors.py   # 可区分的错误分类
  store.py    # LeaseStore：进程内租约状态机
  api.py      # FastAPI 路由与错误码映射
main.py       # 应用入口（保留 GET / 既有行为）
tests/        # 核心状态机测试 + HTTP 端到端测试
```
