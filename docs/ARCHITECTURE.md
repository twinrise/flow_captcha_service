# Flow Captcha Service — 架构设计文档

> 版本：1.0 | 更新日期：2026-04-03

---

## 目录

1. [项目概述](#1-项目概述)
2. [系统架构总览](#2-系统架构总览)
3. [部署模式](#3-部署模式)
4. [模块结构](#4-模块结构)
5. [核心流程](#5-核心流程)
6. [数据模型](#6-数据模型)
7. [API 设计](#7-api-设计)
8. [浏览器自动化引擎](#8-浏览器自动化引擎)
9. [集群调度](#9-集群调度)
10. [认证与权限体系](#10-认证与权限体系)
11. [配置体系](#11-配置体系)
12. [容器化与部署](#12-容器化与部署)
13. [性能优化策略](#13-性能优化策略)
14. [测试体系](#14-测试体系)

---

## 1. 项目概述

Flow Captcha Service (FCS) 是一套 **自托管的验证码解决服务**，支持 reCAPTCHA v2/v3、Cloudflare Turnstile 等主流验证码类型。系统基于 Python 异步框架构建，提供：

- 双浏览器引擎（Playwright / nodriver）
- 水平扩展的 Master-Subnode 集群架构
- 用户门户 + 管理后台（配额管理、API Key 分发）
- YesCaptcha 协议兼容，可作为直接替代

**技术栈：** Python 3.11 / FastAPI / aiosqlite / Playwright / nodriver / Redis (可选)

---

## 2. 系统架构总览

```
┌─────────────────────────────────────────────────────────────────────┐
│                          客户端 / 调用方                             │
│                  Authorization: Bearer <api_key>                    │
└──────────────────┬──────────────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│          HTTP Bridge 层               │
│  (公网 HTTP Server → 内部 Uvicorn)    │
│  src/http_bridge.py                  │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│         FastAPI 应用层                │
│  src/main.py (lifespan 管理)         │
│                                      │
│  ┌────────┐ ┌────────┐ ┌──────────┐ │
│  │Service │ │ Admin  │ │ Portal   │ │
│  │  API   │ │  API   │ │   API    │ │
│  └───┬────┘ └───┬────┘ └────┬─────┘ │
│      │          │            │       │
│  ┌───┴──────────┴────────────┴─────┐ │
│  │        CaptchaRuntime           │ │
│  │     src/services/captcha_       │ │
│  │         runtime.py              │ │
│  └──────────┬──────────────────────┘ │
│             │                        │
│  ┌──────────┴──────────────────────┐ │
│  │     浏览器自动化引擎              │ │
│  │  ┌─────────────┬──────────────┐ │ │
│  │  │ Playwright  │   nodriver   │ │ │
│  │  │ (browser)   │  (personal)  │ │ │
│  │  └─────────────┴──────────────┘ │ │
│  └─────────────────────────────────┘ │
│                                      │
│  ┌─────────────────────────────────┐ │
│  │  数据层                          │ │
│  │  SQLite (aiosqlite) + Redis     │ │
│  └─────────────────────────────────┘ │
└──────────────────────────────────────┘
```

### 分层职责

| 层级 | 职责 | 关键文件 |
|------|------|----------|
| HTTP Bridge | 请求接入、Header 清洗、健康探活 | `src/http_bridge.py` |
| API 路由层 | 请求分发、认证鉴权、参数校验 | `src/api/service.py`, `admin.py`, `portal.py`, `cluster.py`, `yescaptcha.py` |
| 运行时层 | 会话管理、引擎调度、配额扣减 | `src/services/captcha_runtime.py`, `session_registry.py` |
| 引擎层 | 浏览器操控、Token 获取、指纹伪装 | `src/services/browser_captcha.py`, `browser_captcha_personal.py` |
| 数据层 | 持久化存储、日志、集群状态 | `src/core/database.py`, `log_store.py` |

---

## 3. 部署模式

系统支持三种部署角色，通过 `cluster.role` 配置项切换：

### 3.1 Standalone（单机模式）

```
┌─────────────────────────┐
│      Standalone Node     │
│  API + Browser + DB      │
│  角色: standalone        │
└─────────────────────────┘
```

- 所有组件运行在同一进程
- 本地 SQLite 存储
- 适用于开发、小规模部署

### 3.2 Master + Subnode（集群模式）

```
                    ┌──────────────┐
             ┌──────│    Master    │──────┐
             │      │  (调度/API)  │      │
             │      │  无浏览器     │      │
             │      └──────────────┘      │
             │              │             │
        heartbeat      heartbeat     heartbeat
        + dispatch     + dispatch    + dispatch
             │              │             │
             ▼              ▼             ▼
      ┌────────────┐ ┌────────────┐ ┌────────────┐
      │  Subnode A  │ │  Subnode B  │ │  Subnode C  │
      │  Browser ×2 │ │  Browser ×4 │ │  Browser ×2 │
      └────────────┘ └────────────┘ └────────────┘
```

**Master 节点：**
- 接收客户端请求，本身不运行浏览器
- 根据权重和可用容量调度请求到 Subnode
- 维护集群状态（注册、心跳、错误日志）
- 可选 Redis 作为分布式日志后端

**Subnode 节点：**
- 运行浏览器引擎，执行实际的验证码解决
- 周期性向 Master 发送心跳（默认 15s）
- 上报当前活跃会话数、可用槽位、待命 Token 桶

### 3.3 角色对比

| 能力 | Standalone | Master | Subnode |
|------|-----------|--------|---------|
| 接受客户端请求 | ✅ | ✅ | ✅ (本地) |
| 运行浏览器 | ✅ | ❌ | ✅ |
| 集群调度 | ❌ | ✅ | ❌ |
| 需要 Redis | ❌ | 可选 | ❌ |
| 需要 Playwright/nodriver | ✅ | ❌ | ✅ |

---

## 4. 模块结构

```
src/
├── __init__.py
├── main.py                          # FastAPI app 创建 + lifespan 生命周期
├── http_bridge.py                   # 双层 HTTP Server（公网桥 → 内部 Uvicorn）
│
├── core/                            # 基础设施层
│   ├── config.py                    # 配置加载（TOML + 环境变量，154 个配置项）
│   ├── models.py                    # Pydantic 请求/响应模型
│   ├── database.py                  # SQLite 异步数据库（15 张表）
│   ├── auth.py                      # 认证模块（Admin / Portal / Cluster）
│   ├── logger.py                    # 日志工具
│   ├── log_store.py                 # Redis 日志存储层
│   └── diagnostics.py              # 诊断工具
│
├── api/                             # HTTP API 路由层
│   ├── service.py                   # 核心 /api/v1/* 端点（solve/prefill/finish/error）
│   ├── admin.py                     # 管理后台 /api/admin/* 端点
│   ├── portal.py                    # 用户门户 /portal/* 端点
│   ├── cluster.py                   # 集群通信 /api/cluster/* 端点
│   └── yescaptcha.py               # YesCaptcha 兼容 /createTask, /getTaskResult
│
└── services/                        # 业务服务层
    ├── captcha_runtime.py           # 验证码运行时（引擎调度、会话编排）
    ├── browser_captcha.py           # Playwright 引擎（浏览器池、Token 池、指纹）
    ├── browser_captcha_personal.py  # nodriver 引擎（常驻标签页、反检测）
    ├── cluster_manager.py           # 集群管理（Master 调度 / Subnode 心跳）
    ├── session_registry.py          # 会话注册表（生命周期状态机）
    └── yescaptcha_manager.py        # YesCaptcha 任务队列
```

---

## 5. 核心流程

### 5.1 验证码解决主流程

```
Client                    API Layer              CaptchaRuntime            Browser Engine
  │                          │                        │                        │
  │  POST /api/v1/solve      │                        │                        │
  │─────────────────────────>│                        │                        │
  │                          │  verify API Key        │                        │
  │                          │  check quota           │                        │
  │                          │                        │                        │
  │                          │  runtime.solve()       │                        │
  │                          │───────────────────────>│                        │
  │                          │                        │                        │
  │                          │                        │  1. 检查待命 Token 池   │
  │                          │                        │─────────────────────────>
  │                          │                        │  [命中] 直接返回        │
  │                          │                        │<─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
  │                          │                        │                        │
  │                          │                        │  [未命中] 获取浏览器槽位 │
  │                          │                        │─────────────────────────>
  │                          │                        │  加载页面 + 执行验证码   │
  │                          │                        │  提取 Token             │
  │                          │                        │<─────────────────────────
  │                          │                        │                        │
  │                          │  创建 Session          │                        │
  │                          │  登记到 Registry       │                        │
  │                          │<───────────────────────│                        │
  │                          │                        │                        │
  │  SolveResponse           │                        │                        │
  │  {session_id, token}     │                        │                        │
  │<─────────────────────────│                        │                        │
  │                          │                        │                        │
  │  POST /sessions/{id}/finish                       │                        │
  │─────────────────────────>│                        │                        │
  │                          │  标记完成 + 扣减配额    │                        │
  │                          │  写入日志              │                        │
  │  200 OK                  │                        │                        │
  │<─────────────────────────│                        │                        │
```

### 5.2 会话状态机

```
         solve()
           │
           ▼
      ┌──────────┐
      │ pending   │
      └─────┬─────┘
            │
     ┌──────┼──────┐
     │      │      │
     ▼      │      ▼
┌────────┐  │  ┌────────┐
│finished│  │  │ error  │
└────────┘  │  └────────┘
            │
            ▼ (TTL 超时)
      ┌──────────┐
      │ expired  │
      └──────────┘
            │
            ▼ (60s 后)
        [从 Registry 移除]
```

### 5.3 集群调度流程（Master 视角）

```
Client Request
      │
      ▼
  Master 接收
      │
      ├── 查询所有在线 Subnode 心跳
      │
      ├── 选择最优节点：
      │   1. 优先选择有匹配待命 Token 的节点（项目亲和）
      │   2. 按 (available_slots × weight) 加权选择
      │   3. 预留槽位，防止超分配
      │
      ├── 转发请求到 Subnode /api/v1/solve
      │   超时: master_dispatch_timeout_seconds (默认 45s)
      │
      └── 返回 Subnode 响应给客户端
```

---

## 6. 数据模型

### 6.1 SQLite 表结构概览

```
┌─────────────────────┐     ┌──────────────────────┐
│   service_admin      │     │   service_api_keys    │
│─────────────────────│     │──────────────────────│
│ id, username,        │     │ id, key_prefix,       │
│ password_hash        │     │ key_hash, label,      │
│                      │     │ quota_total,          │
│                      │     │ quota_remaining,      │
│                      │     │ enabled               │
└─────────────────────┘     └──────────┬───────────┘
                                       │ 1:N (API Key → Jobs)
                                       ▼
┌─────────────────────┐     ┌──────────────────────┐
│   portal_users       │     │   captcha_jobs        │
│─────────────────────│     │──────────────────────│
│ id, username,        │     │ id, session_id,       │
│ password_hash,       │     │ project_id, action,   │
│ enabled,             │     │ status, duration_ms,  │
│ quota_remaining      │     │ node_name, browser_id,│
│                      │     │ api_key_id, timestamp  │
└──────────┬──────────┘     └──────────────────────┘
           │
           │ 1:N
           ▼
┌─────────────────────┐     ┌──────────────────────┐
│ portal_user_api_keys │     │ session_quota_events  │
│─────────────────────│     │──────────────────────│
│ id, user_id,         │     │ session_id,           │
│ key_prefix,          │     │ api_key_id,           │
│ key_hash, label      │     │ event_type,           │
│                      │     │ amount, timestamp     │
└─────────────────────┘     └──────────────────────┘

┌─────────────────────┐     ┌──────────────────────┐
│   portal_cdks        │     │ portal_user_           │
│─────────────────────│     │   transactions         │
│ id, code, batch,     │     │──────────────────────│
│ quota_amount,        │     │ id, user_id, type,    │
│ redeemed_by          │     │ amount, balance_after  │
└─────────────────────┘     └──────────────────────┘

┌─────────────────────┐     ┌──────────────────────┐
│   cluster_nodes      │     │ cluster_node_          │
│─────────────────────│     │   heartbeats           │
│ id, node_name,       │     │──────────────────────│
│ node_url, weight,    │     │ node_id,              │
│ max_concurrency,     │     │ active_sessions,      │
│ registered_at        │     │ available_slots,      │
│                      │     │ standby_buckets,      │
│                      │     │ timestamp             │
└──────────┬──────────┘     └──────────────────────┘
           │ 1:N
           ▼
┌──────────────────────┐
│ cluster_node_errors   │
│──────────────────────│
│ node_id, error_type,  │
│ error_message,        │
│ timestamp             │
└──────────────────────┘
```

### 6.2 可选 Redis 存储

当 `log.storage_backend = "redis"` 时，日志类数据写入 Redis List：

| Redis Key | 内容 | 最大条目 |
|-----------|------|---------|
| `fcs:captcha_jobs` | 验证码任务日志 | 20,000 (可配) |
| `fcs:cluster_node_heartbeats` | 心跳记录 | 20,000 |
| `fcs:cluster_node_errors` | 节点错误日志 | 20,000 |

---

## 7. API 设计

### 7.1 端点总览

| 模块 | 前缀 | 认证方式 | 说明 |
|------|------|---------|------|
| Service API | `/api/v1/` | Bearer API Key | 核心验证码解决接口 |
| YesCaptcha 兼容 | `/` | clientKey 参数 | 兼容 YesCaptcha 协议 |
| Admin API | `/api/admin/` | Admin Cookie Token | 管理后台 |
| Portal API | `/portal/` | Portal Cookie Token / Bearer API Key | 用户门户 |
| Cluster API | `/api/cluster/` | X-Cluster-Key Header | 集群内部通信 |
| Static | `/admin`, `/portal`, `/subnode` | 无 | 前端页面 |

### 7.2 核心 Service API 详情

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/v1/health` | GET | 健康检查（无认证） |
| `/api/v1/solve` | POST | 解决验证码，返回 session_id + token |
| `/api/v1/prefill` | POST | 预生成 Token（暖池） |
| `/api/v1/sessions/{id}/finish` | POST | 标记会话成功 |
| `/api/v1/sessions/{id}/error` | POST | 报告会话失败 |
| `/api/v1/custom-score` | POST | reCAPTCHA v3 评分测试 |
| `/api/v1/custom-token` | POST | 自定义验证码解决 |

### 7.3 请求/响应示例

**解决验证码：**

```json
// POST /api/v1/solve
// Headers: Authorization: Bearer fcs_xxxx
{
    "project_id": "my-project",
    "action": "IMAGE_GENERATION",
    "token_id": "optional-idempotency-key"
}

// Response 200
{
    "success": true,
    "session_id": "550e8400-e29b-41d4-a716-446655440000",
    "token": "03AGdBq24...",
    "node_name": "subnode-01",
    "expires_in_seconds": 1200,
    "fingerprint": {
        "user_agent": "Mozilla/5.0 ...",
        "viewport": {"width": 1920, "height": 1080}
    }
}
```

### 7.4 YesCaptcha 兼容协议

系统实现了 YesCaptcha 的 `createTask` / `getTaskResult` / `getBalance` 接口，可作为 YesCaptcha 的自托管替代，客户端无需修改代码。

---

## 8. 浏览器自动化引擎

### 8.1 双引擎架构

通过 `captcha.captcha_method` 配置切换，由 `CaptchaRuntime` 统一编排：

| 特性 | Playwright (browser) | nodriver (personal) |
|------|---------------------|---------------------|
| 底层库 | Playwright + Chromium | nodriver (undetected-chromedriver 后继) |
| 反检测 | 指纹池伪装 (100+ UA) | 原生反检测 |
| 并发模型 | 浏览器池 × 多上下文 | 常驻标签页池 |
| 适用场景 | 高吞吐、无状态 | 需要登录态 / 高反检测要求 |
| Docker 支持 | Xvfb headed / headless | 需要 headed 环境 |

### 8.2 Playwright 引擎核心机制

**浏览器池管理：**
- 维护 `browser_count` 个并发浏览器实例
- 每个实例可持有多个 Context / Tab
- 空闲回收（`browser_idle_ttl_seconds`，默认 600s）
- 项目亲和路由：相同 project_id 优先分配到同一浏览器

**Token 待命池（Standby Pool）：**
- 按 `(project_id, action)` 分桶预生成 Token
- 池深度可配（默认 2），命中时亚 100ms 响应
- 后台异步补充，不阻塞请求

**指纹池：**
- 通过 `fake_useragent` 维护 100+ User-Agent
- 每个浏览器实例随机分配指纹
- 防止验证码服务商通过指纹关联检测

**代理支持：**
- HTTP/HTTPS/SOCKS5 代理
- SOCKS5 + 认证 → 自动降级为 HTTP 代理（Chromium 限制）

### 8.3 nodriver 引擎核心机制

**常驻标签页池：**
- 每个项目最多 `personal_max_resident_tabs`（默认 5）个常驻标签页
- 同时支持 `personal_project_pool_size`（默认 4）个项目
- 标签页在 TTL 内保持打开，复用登录态

**故障恢复：**
- 单标签页连续失败 ≥ `browser_personal_recreate_threshold` → 重建标签页
- 累计失败 ≥ `browser_personal_restart_threshold` → 重启整个浏览器

### 8.4 支持的验证码类型

| 类型 | 端点 | 说明 |
|------|------|------|
| Flow 原生 Token | `/api/v1/solve` | 使用内置 website key |
| reCAPTCHA v2 | `/api/v1/custom-token` | 任意网站 URL + Key |
| reCAPTCHA v3 | `/api/v1/custom-token` | 任意网站 URL + Key |
| reCAPTCHA v3 Score | `/api/v1/custom-score` | 评分测试 |
| Cloudflare Turnstile | `/api/v1/custom-token` | Turnstile 类型 |

---

## 9. 集群调度

### 9.1 ClusterManager 职责

**Master 角色：**
- 接受 Subnode 注册 (`/api/cluster/register`)
- 收集 Subnode 心跳 (`/api/cluster/heartbeat`)
- 根据策略选择最优节点转发请求
- 检测失联节点（`master_node_stale_seconds`，默认 120s）

**Subnode 角色：**
- 启动时向 Master 注册
- 周期性发送心跳（默认 15s）
- 上报：活跃会话数、可用槽位、待命 Token 桶签名

### 9.2 调度策略

节点选择优先级：

1. **Token 亲和**：如果某 Subnode 的待命池中已有匹配的 `(project_id, action)` Token，优先选择该节点（命中待命池 → 毫秒级响应）
2. **加权容量**：按 `available_slots × node_weight` 计算得分，选择得分最高的节点
3. **槽位预留**：Master 在转发前预留槽位，防止并发请求导致超分配

### 9.3 心跳数据结构

```json
{
    "active_sessions": 3,
    "available_slots": 5,
    "standby_buckets": [
        {"project_id": "proj-a", "action": "IMAGE_GENERATION", "count": 2},
        {"project_id": "proj-b", "action": "VIDEO_GENERATION", "count": 1}
    ]
}
```

---

## 10. 认证与权限体系

### 10.1 认证类型

| 类型 | 格式 | 存储 | 适用端点 |
|------|------|------|---------|
| Service API Key | `Bearer fcs_xxxx` Header | DB (hash) | `/api/v1/*` |
| Portal User API Key | `Bearer fcs_xxxx` Header | DB (hash) | `/api/v1/*`, `/portal/user/*` |
| Admin Token | Cookie `admin_token` | 内存 | `/api/admin/*` |
| Portal User Token | Cookie `portal_token` | 内存 | `/portal/*` |
| Cluster Key | `X-Cluster-Key` Header | 配置文件 | `/api/cluster/*` |

### 10.2 配额体系

```
Service API Key ──→ quota_total / quota_remaining
                    每次 solve + finish 扣减 1

Portal User ──→ quota_remaining
               ├── 管理员分配
               ├── CDK 兑换码充值
               └── 每日签到奖励
```

- 配额在 `finish` 时扣减（`error` 不扣减）
- 配额为 -1 表示无限制
- `session_quota_events` 表记录每次扣减/退还

---

## 11. 配置体系

### 11.1 配置优先级

```
环境变量 (FCS_*)  >  TOML 配置文件  >  代码默认值
```

### 11.2 配置加载

- 配置文件路径：`data/setting.toml`（可通过 `FCS_CONFIG_FILE` 环境变量覆盖）
- 支持 154 个 `FCS_*` 前缀的环境变量
- 运行时可通过 Admin API 动态修改部分配置（如 `captcha_method`、`browser_count`）

### 11.3 关键配置分组

| 分组 | 说明 | 关键配置项 |
|------|------|-----------|
| `[server]` | 服务监听 | `host`, `port` |
| `[storage]` | 数据库路径 | `db_path` |
| `[admin]` | 管理员凭据 | `username`, `password` |
| `[captcha]` | 引擎参数 | `captcha_method`, `browser_count`, `session_ttl_seconds` |
| `[log]` | 日志配置 | `level`, `storage_backend`, `redis_url` |
| `[cluster]` | 集群配置 | `role`, `master_base_url`, `master_cluster_key` |

---

## 12. 容器化与部署

### 12.1 Docker 镜像

| 镜像 | Dockerfile | 用途 | 包含浏览器 |
|------|-----------|------|-----------|
| headed | `Dockerfile.headed` | Standalone / Subnode | ✅ (Chromium + Xvfb) |
| master | `Dockerfile.master` | Master 调度节点 | ❌ |

### 12.2 Compose 编排文件

| 文件 | 用途 |
|------|------|
| `docker-compose.headed.yml` | 单机部署（端口 8060） |
| `docker-compose.cluster.master.yml` | Master + Redis |
| `docker-compose.cluster.subnode.yml` | Subnode 节点 |
| `docker-compose.cluster.stack.yml` | 完整集群演示栈 |

### 12.3 headed 容器启动流程

```
entrypoint.headed.sh
    │
    ├── 启动 Xvfb 虚拟显示 (:99)
    ├── 启动 fluxbox 窗口管理器
    ├── 检测 Chromium 可执行路径
    └── python main.py
```

---

## 13. 性能优化策略

### 13.1 Token 预生成（Prefill）

- 客户端可提前调用 `/api/v1/prefill` 预热 Token 池
- 服务端维护待命 Token 池，下次 `solve` 直接命中
- 池深度可配（默认 2），桶按 `(project_id, action)` 分组

### 13.2 项目亲和路由

- 相同 `project_id` 优先路由到同一浏览器实例
- 复用已加载的页面上下文，减少页面加载开销
- LRU 策略管理亲和映射，防止内存膨胀

### 13.3 自定义页面缓存

- 对静态 URL 模式缓存 Playwright Page 对象
- 最大缓存页数可配（默认 3）
- TTL 过期后自动回收（默认 240s）

### 13.4 异步全链路

- 基于 `asyncio` 的全异步架构
- aiosqlite 异步数据库访问
- `asyncio.Lock` 保护共享状态（会话注册表、浏览器槽位）
- 后台协程处理：会话清理、心跳上报、日志清理、Token 补充

### 13.5 HTTP Bridge

- 自定义双层 HTTP 架构避免 Docker 网络问题
- 公网 HTTP Bridge 接收请求 → 转发到内部 Uvicorn
- 自动清洗 hop-by-hop Header，注入 X-Forwarded-* 头

---

## 14. 测试体系

### 14.1 测试框架

- 基于 Python `unittest` + `asyncio.IsolatedAsyncioTestCase`
- 使用临时数据库和 Mock 隔离外部依赖
- httpx `AsyncClient` 驱动 API 集成测试

### 14.2 测试覆盖范围

| 测试模块 | 覆盖领域 |
|---------|---------|
| `test_service_local_captcha_modes.py` | API 端点行为、并发解决、配额管理 |
| `test_browser_token_pool.py` | Token 预生成池、桶亲和、淘汰策略 |
| `test_browser_captcha_personal_concurrency.py` | Personal 模式并发、标签页故障恢复 |
| `test_captcha_runtime_*.py` | 运行时生命周期、并发解决 |
| `test_admin_captcha_config.py` | 管理后台配置 API |
| `test_cluster_manager.py` | 集群调度、节点选择、槽位预留 |
| `test_log_storage.py` / `test_log_cleanup.py` | 日志持久化、自动清理 |
| `test_yescaptcha_*.py` | YesCaptcha 协议兼容 |
| `test_http_bridge.py` | HTTP 桥接、Header 清洗 |
| `test_browser_process_cleanup.py` | 浏览器进程清理、资源泄漏 |

### 14.3 运行测试

```bash
python -m pytest tests/ -v
```

---

## 附录：技术决策摘要

| 决策 | 选型 | 理由 |
|------|------|------|
| Web 框架 | FastAPI | 原生异步、Pydantic 校验、OpenAPI 文档 |
| 数据库 | SQLite (aiosqlite) | 零运维、单文件部署、异步访问 |
| 浏览器自动化 | Playwright + nodriver | Playwright 高吞吐，nodriver 强反检测 |
| HTTP Client | curl-cffi | TLS 指纹伪装，避免被目标站检测 |
| 配置格式 | TOML | 可读性好，Python 3.11 原生支持 |
| 可选缓存/日志 | Redis | 集群场景下的分布式日志聚合 |
| 容器基础镜像 | python:3.11-slim | 体积小，满足所有依赖 |
