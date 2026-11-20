# 计费结算两阶段提交（2PC）运维台

计费写入依赖两个 PostgreSQL 库：**扣款库（debit-db）** 与 **入账库（credit-db）**。
协调进程用两阶段提交保证两边一致，并在 `xact_log` 中留下协调日志。当协调进程在
两个预设故障点退出后，值班员在运维网页上按**结算号**查看两库残留的预备事务
（`pg_prepared_xacts`）和协调日志，选择一笔中断的结算恢复。

## 恢复决策规则

| 日志/状态证据 | 动作 |
| --- | --- |
| 任一库 `xact_log` 有 `decision_commit` | 对两库补齐 `COMMIT PREPARED`（已结束的一侧幂等跳过） |
| 任一库有 `decision_abort` | 对两库补齐 `ROLLBACK PREPARED` |
| 无任何决定，且两库都还持有预备事务 | 视为尚无决定 → 两库 `ROLLBACK PREPARED`（撤销，不扣款/入账） |
| 无任何决定，且只有一库还持有预备事务（无法判断另一侧当年是提交还是回滚） | **保留预备事务不动**，页面返回 `held` 并提示人工核实 |
| 两库均无预备事务 | 不再做任何写操作；按流水是否存在判定已完成/无此结算 |

只有两库收尾（均不再持有该预备事务）后，页面才显示「已提交 / 已撤销」。

## 防重复扣款、入账与并发保护

1. `ledger_entries(settlement_no)` 上有**唯一索引**，同号重复写入会被数据库直接拒绝。
2. 恢复入口先在扣款库取**咨询锁**（`pg_try_advisory_lock`，键含结算号）：同号并发/重复
   恢复只有一个请求能拿到锁，其余立即返回 `duplicate`，不做任何写操作。
3. `COMMIT/ROLLBACK PREPARED` 对"事务已不存在"做幂等处理（`already_gone`），崩溃后重放安全。
4. 发起结算前检查两库是否已存在同号预备事务或流水，存在则拒绝（HTTP 409），要求走恢复。

## 两个可注入的中断点

由环境变量 `FAILPOINT` 控制（默认 `none`）：

- `after_both_prepared`：两库都已 `PREPARE TRANSACTION`、协调日志只有 `prepared`、
  **尚未写入任何决定**时，协调进程 `os._exit(99)` 硬退出。重启后恢复 → 撤销。
- `after_one_commit`：扣款库已 `COMMIT PREPARED`、两库已写 `decision_commit`、
  入账库仍持有预备事务时硬退出。重启后恢复 → 补齐入账库提交。

## 方式一：Docker Compose 演练（推荐交付路径）

```bash
# 演练中断点一（两库已预备、尚未决定）
FAILPOINT=after_both_prepared docker compose up --build
# 浏览器打开 http://localhost:8080
#   1) 在②区填结算号（如 S1001）和金额，点“发起两阶段结算”，ops-web 进程会退出
#   2) docker compose up -d 重新拉起（FAILPOINT 可改回 none）
#   3) 在③区输入同一结算号，先“查看日志”再“恢复此结算” → 状态为 aborted
#
# 中断点二把 FAILPOINT 换成 after_one_commit，恢复后状态为 committed
FAILPOINT=none docker compose up -d
```

- 扣款库映射宿主端口 55432，入账库 55433，网页 8080。
- 两个库都显式设置 `max_prepared_transactions=200`（PostgreSQL 默认关闭预备事务）。
- 数据存放在命名卷 `debit-data` / `credit-data`，容器重启后预备事务与日志都保留。
- 清理：`docker compose down -v`。

## 方式二：无 Docker 的本地等价演练

本仓库还提供基于 `pgserver` wheel（内含完整 PostgreSQL 16 二进制）的双实例脚本，
用于没有容器运行时的环境，行为与 Compose 双库等价。

```bash
python -m venv --without-pip .venv && .venv/bin/python get-pip.py   # 仅首次引导
.venv/bin/python -m pip install -r requirements.txt pgserver httpx
# 启动两个本地 PG 实例（.local-pg/，端口 55432/55433）
.venv/bin/python -I scripts/local_pg.py start
.venv/bin/python -I scripts/local_pg.py status

# 带故障点启动网页
set -a && . .local-pg/state.env && set +a
FAILPOINT=after_both_prepared DSN_DEBIT="$DSN_DEBIT" DSN_CREDIT="$DSN_CREDIT" \
  .venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8080
# 进程退出后用 FAILPOINT=none 重新执行同一条 uvicorn 命令即可“重启继续”

# 一键自测（含两处中断的崩溃→重启→恢复、并发、保留、正常路径）
.venv/bin/python -I scripts/self_test.py
```

## 目录结构

```
app/
  config.py       环境配置、结算号/GID 校验
  db.py           双库同构 schema、预备事务与协调日志操作、咨询锁
  coordinator.py  2PC 发起、故障注入、按日志恢复（提交/撤销/保留）
  main.py         FastAPI 路由与运维网页
  templates/index.html
scripts/
  local_pg.py     无 Docker 时的双 PG 实例管理
  self_test.py    端到端自测（35 项断言）
docker-compose.yml / Dockerfile / requirements.txt
```

## API 摘要

- `GET  /` 运维网页
- `GET  /api/state` 两库残留预备事务
- `GET  /api/settlement/{no}` 该结算号在两库的协调日志与流水情况
- `POST /api/settlement/start`（表单 `settlement_no`, `amount`）发起两阶段结算
- `POST /api/settlement/recover`（表单 `settlement_no`）恢复
- `GET  /healthz`
