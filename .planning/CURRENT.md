# 当前项目状态

> 唯一当前状态入口。最后核验：2026-08-26（Plan 05.6-207 local
> closure；production revision 026 pending authorization）。
> `JOURNAL.md` 是追加式历史；其中旧 `[NEXT]` 均不代表当前任务。

稳定的使用流程、健康语义和命令安全分级见
[M1 市场感知平台使用手册](../docs/M1-市场感知平台使用手册.md)；本文继续只维护动态状态。

## 一句话结论

M1 self-healing 的本地实现已经推进到 Plan 05.6-207 closure：最终 Task-5
SHA 为 `e3c1fc83`，本地代码包含 revision 026、scoped runtime-controller /
qualification-worker 数据库能力角色、daemon 启动身份检查、login-role
operator tooling、release/config identity、两份私有 Fly app 模板，以及
scoped-DSN deterministic fault matrix v2。Tasks 1-5 的独立 review 均为 clean。

生产边界仍然严格保持在授权前状态：production DB 是 `postgres`，只 applied
`022`/`023`/`024`/`025`；revision `026` **NOT APPLIED**。原四个 production
apps 运行中；新的 runtime-controller 与 qualification-worker apps 不存在。没有
scoped production login changes、没有新 secrets、没有 recovery enablement、
没有 fault mutation，observe-only window 仍 **NOT RUN**。

下一步不是直接 migration 或 deploy，而是准备一份全新的 exact authorization
package，明确绑定 final Task-5 SHA `e3c1fc83`、production DB `postgres`、
revision 026、两个 scoped login roles、两个新 private apps、observe-only mode、
empty recovery allowlist、rollback procedure 和 05.6 evidence directory。

## 已验证可用的内容

| 范围 | 当前结论 | 现在能否使用 |
|---|---|---|
| M1 L1/L2/L3 existing production apps | 原四个 apps 运行；2026-08-25 post-migration worker health pass | 继续作为只读生产事实来源 |
| Production database | `postgres`，revisions 022/023/024/025 applied；026 NOT APPLIED | 不允许假定 revision 026 权限已存在 |
| Qualification incident ingress | 2026-08-25 audit rows = 1643 | 只作为生产审计事实，不替代新 observe-only window |
| Plan 05.6-207 local runtime-role implementation | final Task-5 SHA `e3c1fc83`；reviews clean；local matrix v2 pass | 可用于准备授权包 |
| New runtime-controller / qualification-worker apps | templates exist locally; production apps absent | 不可当作已部署 |
| Observe-only production window | NOT RUN | 不可声明生产 enablement 通过 |
| M2 paper execution/accounting | 既有本地模拟、账本和恢复测试可用 | 仍非真实下单系统 |
| M3/M4 | 未开始 | 不可用 |
| M5 | 有计划但依赖 M1 closure | 尚不可用 |

## Plan 05.6-207 本地证据

- Additive revision 026 defines `m1_runtime_controller_capability` and
  `m1_qualification_worker_capability` as non-login, non-inheriting,
  non-elevated capability roles.
- Runtime controller startup proves exact database, login/capability membership,
  positive table/function grants, missing cross-capability grants, and denied
  sequence access before any controller work.
- Qualification worker startup proves matching qualification permissions and
  release/config identity before service construction.
- Operator login-role commands are default-off and require `enable=1`; they do
  not print DSNs, passwords, auth headers, provider response bodies, or SQL
  password literals.
- Local deterministic PG16/testcontainers matrix v2 covered 12 cases with
  77 qualification facts, 12 observe decisions, 0 recovery actions, 0 database
  leaks, 0 role leaks, repeated digest/cmp pass, and digest
  `f1f4abe704d859409d01ba1e060839abf47b039a5b5cd4898aed76110c6b860c`.

## 不要误用

- Local PG16/testcontainers matrix evidence is local-only; it is not production
  evidence and does not prove production revision 026.
- A clean review does not authorize production migration, Fly deploy/app
  creation, secret installation, login provisioning, recovery enablement, fault
  injection, restart, or downgrade.
- `make runtime-reconcile-serve enable=1` still defaults to observe-only unless
  execute mode, exact allowlist, provider authority, budgets, leases and
  independent health checks are all separately authorized.
- `make qualification-worker-serve enable=1` cannot be treated as a production
  epoch claim until revision 026 and the exact app/login/release/config package
  are authorized and deployed, then the certificate independently verifies the
  full window.
- `make planning-status` now audits Plan 05.6-207 through its explicit
  `plan-source` summary anchor; summary-only historical files without such an
  external-plan registration are not silently promoted into completed plans.

## 当前下一步

Run:

```bash
/gsd-resume-work --ws m1-perception
make planning-status
```

Then prepare, but do not execute, a fresh exact authorization package for:
final Task-5 SHA `e3c1fc83`; production database `postgres`; revision 026; the
two scoped login roles; the two new private Fly apps; observe-only mode; empty
recovery allowlist; rollback; and evidence directory
`.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/`.
