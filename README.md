# 本地分布式协调能力

用进程内状态模拟多个客户端竞争同一组资源，提供基于租约的互斥占用、
持有权版本（fencing token）、按客户端维度的并发配额、超时重试幂等与
批量原子占用。所有协调结论在各入口（Python API 与 HTTP API）下一致
且可解释。

## 统一门面 Coordinator

`coordination.coordinator.Coordinator` 是唯一的业务入口，内部组合
`LeaseRegistry`（租约/凭证）与 `ClientQuota`（配额原语）。**库方式调用
与 HTTP 入口共享同一个 `Coordinator` 实例**（`main.coordinator`），因此
无论请求从哪个调用方式进来，配额、幂等与批量结论天然一致。直接绕过
门面调用 `LeaseRegistry` 则不享受这些统一口径。

## 并发配额（统一口径）

- **计量对象是“并发持有数”**：一次成功的占用（单个或批量中的一个资源）
  占一个许可；同一客户端对**自己已持有**资源的重复申请（续租式占位）
  不新增许可；批量申请中已自持的资源也不新增许可。
- **按客户端隔离**：某个客户端打满不影响其它客户端。
- **统一生效**：库调用与 HTTP 请求经过同一门面、同一计数器，结论一致：
  没到上限就正常占用，到了上限立即以 `quota_exceeded` 拒绝（不排队），
  与 `held_by_other` 等其它拒绝原因码可明确区分。
- **许可回收无残留**：释放（`release`）、租约过期（门面在下一次操作时
  惰性回收）、占用失败/整单回滚都会归还许可；突发并发下在途数绝不超过
  上限，结束后归零。

## 超时重试幂等

占用（单个与批量）可携带客户端生成的唯一标识 `idem_key`：

- 同一 `(client, idem_key)` 的整笔操作**只生效一次**：重放不换出新的
  fencing token、不重复占用资源、不多占配额，返回与首次完全一致的结果
  （批量则返回同一组凭证，顺序也一致）。
- **标识的生效范围按操作类型各自独立**：单个占用（`acquire`）与批量申请
  （`acquire_batch`）维护各自的幂等账。同一客户端在两种操作上使用**同一个
  `idem_key`** 互不影响——它们是两笔不同的操作，各自占用各自的资源、各自
  只重放自己第一次的结论，跨类型不会互相顶替（不会把单个的凭证当成批量
  结果、反之亦然），也不会走到内部错误路径（HTTP 不会返回 5xx）。
- **空标识按未提供处理**：`idem_key` 为空串或只含空白字符（`"   "`、
  `"\t"` 等）一律等同没有携带标识，每次请求都作为一次全新操作执行：
  不同资源各自被真实占用、持有数正常增加；只有带非空白标识的重放才命中
  幂等记录。
- **首次结论无论成败都会落账并原样重放**：若第一次就被拒绝（如
  `held_by_other`、`quota_exceeded`），该拒绝原因同样落账。之后拿同一个
  标识重放（即使冲突已经解除、配额已经空闲）也永远返回与第一次一致的
  拒绝结论，不会被当成新操作重新执行、不会换发新的 fencing token；
  首次失败本身不占任何资源与许可，重放也不留残留状态、不会出现双持有。
  冲突解除后想真正占用，请换一个新的 `idem_key`（或不带标识）。
- 重放即使发生在配额已满时也仍返回首次结果，不再次申请许可。
- **缺失 `idem_key`**：按一次新的占用处理（同客户端重复占用同一资源会
  换发新凭证，但仍只占一个许可）。
- **换新的 `idem_key`**：视为一次新操作。
- 并发重复提交（同一键双发）在锁内串行判定，只有第一次真正生效，其余
  线程拿到首次结论（成功凭证或拒绝原因），无双持有。

## 批量占用（全部成功或整体失败）

`acquire_batch(resources, client, ttl, idem_key)` 一次申请一组资源：

- **全部拿到才算成功**：写入前先统一判定。任一资源被其他客户端持有，
  整单以 `held_by_other` 拒绝；若新资源数会导致客户端超限，整单以
  `quota_exceeded` 拒绝。两种失败都发生在任何写入之前，不会只占一半。
- **异常路径回滚**：底层若在写入过程中异常，已拿到的资源会被释放、
  对应许可被回收，不留部分占用、无双持有。
- **已自持资源**：无键申请按新操作处理（与单个占用一致，换发新凭证），
  但**沿用原许可、不多占配额**，只为新资源申请许可；幂等重放则原样返回
  首次凭证。输入资源去重保序。
- 并发下原子判定，冲突立即返回，不排队、不无限等待。
- 批量同样支持 `idem_key`，重放返回首次的整组凭证。

## 租约与凭证语义

- **互斥占用**：同一资源同一时刻最多一个持有者。只有租约到期（TTL 耗尽）
  或持有者主动释放后，其他客户端才能获取。
- **fencing token**：每次成功获取或续租都签发一个严格递增的整数凭证，
  按资源维度单调递增，释放或过期后**永不回退、永不复活**。
- **续租**：仅持有者在租约未过期时可续租；续租签发新 token，旧 token
  立即失效。过期后的续租请求被拒绝（`lease_expired`），旧凭证不得复活。
- **受保护操作**：`guarded_access` 只放行携带当前有效凭证的请求；
  落后凭证一律拒绝（`stale_fencing_token`）。

## 错误分类

所有拒绝抛出 `LeaseRejected`，`reason` 字段为可区分的原因码
（HTTP 入口统一返回 409，body 中 `error` 字段为同一原因码）：

| 原因码 | 含义 |
| --- | --- |
| `held_by_other` | 资源被其他客户端持有且未过期（抢占被拒/批量整单失败） |
| `not_holder` | 伪造持有者身份 / 非持有者执行持有者操作 |
| `stale_fencing_token` | 携带落后或不存在的凭证访问受保护操作 |
| `lease_expired` | 租约已过期，续租或受保护访问无效 |
| `not_held` | 资源当前无人持有（如重复释放） |
| `quota_exceeded` | 客户端并发持有配额耗尽（单个/批量一致） |

## HTTP 接口

- `POST /leases/acquire` — body：`resource, client, ttl, idem_key?`
- `POST /leases/acquire-batch` — body：`resources[], client, ttl, idem_key?`，
  返回 `{"leases": [...]}`；冲突/超限时整单 409
- `POST /leases/release` / `POST /leases/renew` / `POST /leases/guarded`
- `GET /leases/{resource}` — 查询当前有效持有者
- 既有 `GET /` 行为不变

## 代码结构

- `coordination/clock.py` — 时钟抽象（`SystemClock` / 测试用 `ManualClock`）
- `coordination/errors.py` — `RejectionReason` 与 `LeaseRejected`
- `coordination/registry.py` — `LeaseRegistry`：租约互斥 + fencing token
- `coordination/quota.py` — `ClientQuota`：通用的按客户端并发许可原语
- `coordination/coordinator.py` — `Coordinator`：统一门面（配额口径、
  幂等落账、批量原子占用、许可回收）
- `main.py` — FastAPI 入口，导出库/HTTP 共享单例 `coordinator`
- `tests/` — 租约/凭证、配额、统一配额、幂等、批量与 HTTP 场景测试

## 本地验证

```bash
uv sync
uv run python -m pytest tests/ -q        # 运行全部测试（含本轮新增）
uv run python -m pytest tests/ -q -s     # 查看每个用例打印的关键输入与判定依据
uv run python -m pytest tests/test_idempotency_fixes.py -q  # 本轮幂等修复专项
uv run fastapi dev main.py               # 本地启动 HTTP 服务
```

幂等修复专项（`tests/test_idempotency_fixes.py`）稳定覆盖三类场景：

1. **单个占用与批量申请交叉使用同一标识**：两种顺序（单个→批量、
   批量→单个）各为独立操作、各自占资源、各自只重放自己的首次结论；
   一侧首次被拒也不会被另一侧顶替；HTTP 交叉使用返回 201 而非 500。
2. **空/纯空白标识下的两次不同占用**：`""`、`"   "`、`"\t"` 等都按
   未携带标识处理，两笔占用各自真实生效（两个资源都占上、持有数增加），
   库调用与 HTTP 两侧一致。
3. **首次被拒后同标识重试**：单个与批量的 `held_by_other`、批量的
   `quota_exceeded` 首次结论都会落账；无论冲突是否解除，重放都返回与
   第一次一致的拒绝原因，不换发凭证、不留许可、无部分占用/双持有。

手工验证批量与幂等：

```bash
curl -s -X POST localhost:8000/leases/acquire-batch -H 'Content-Type: application/json' \
  -d '{"resources":["r1","r2"],"client":"c1","ttl":30,"idem_key":"bulk-1"}'
curl -s -X POST localhost:8000/leases/acquire-batch -H 'Content-Type: application/json' \
  -d '{"resources":["r1","r3"],"client":"c2","ttl":30}'   # 整单 409 held_by_other
```
