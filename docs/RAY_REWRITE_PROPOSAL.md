# Flow Captcha Service — Ray / Ray Serve 重写架构设计提案

> 状态：**待审核（Draft）** | 版本：1.2 | 更新日期：2026-04-09
>
> 本文档评估将 Flow Captcha Service 从当前的"自研 HTTP + SQLite + 心跳"集群协调方案迁移到 **Ray / Ray Serve** 框架的可行性、架构设计和迁移成本。

---

## 目录

1. [背景与动机](#1-背景与动机)
2. [核心抽象映射](#2-核心抽象映射)
3. [整体架构](#3-整体架构)
4. [Deployment 拆分设计](#4-deployment-拆分设计)
5. [功能点逐项映射检查](#5-功能点逐项映射检查)
6. [无法完美映射的部分](#6-无法完美映射的部分)
7. [迁移工作量评估](#7-迁移工作量评估)
8. [收益与成本分析](#8-收益与成本分析)
9. [最终评估与建议](#9-最终评估与建议)
10. [完整技术栈](#10-完整技术栈)
11. [k3s 起步方案（推荐）](#11-k3s-起步方案推荐)

---

## 1. 背景与动机

### 1.1 当前方案痛点

当前 Flow Captcha Service 使用的是自研的 HTTP + SQLite + 心跳轮询方案，主要痛点：

| 痛点 | 严重程度 |
|------|---------|
| Master 单点故障，无 HA | 高 |
| 内存状态（bucket affinity / reservations）重启丢失 | 中 |
| 同步 HTTP 在 executor 里跑，不是原生 async | 中 |
| 自研集群代码（约 2000 行），维护成本高 | 中 |
| 无 Master 主动探活，Subnode 假死时检测延迟到 120s | 低 |
| 固定 350ms 重试，无指数退避 | 低 |

### 1.2 为什么考虑 Ray

Ray 的核心抽象（**Actor 模型 + Serve Deployment**）几乎与本项目的业务实体一一对应：

- 浏览器实例 ≈ 长生命周期、有状态、独占资源 → **Ray Actor**
- 浏览器池 ≈ 多个对等的 Actor 副本 → **Serve Deployment Replicas**
- Project Affinity ≈ 把同一项目的请求路由到加载过该项目的副本 → **Ray Serve Model Multiplexing**

理论上，Ray 能用框架原生能力替代当前所有自研的集群协调代码。

---

## 2. 核心抽象映射

| 现有概念 | Ray 对应 | 说明 |
|---------|---------|------|
| 浏览器实例 | **Ray Actor**（stateful） | 长生命周期、有状态、独占资源 |
| Subnode 节点 | **Ray Worker Node** | Ray 集群的物理/虚拟节点 |
| Master 调度 | **Ray Serve Router + Replica Scheduler** | 内置加权路由、负载均衡 |
| CaptchaRuntime | **Ray Serve Deployment** | HTTP 入口 + 业务编排 |
| 浏览器池 | **Deployment 的 num_replicas** | 副本数 = 池大小 |
| Project Affinity | **`@serve.multiplexed`** | 按 model_id 路由到加载过该 model 的副本 |
| 心跳/健康检查 | **Ray GCS + Serve `check_health`** | 框架原生支持，无需自研 |
| Token 待命池 | **Detached Ray Actor** | 跨副本共享的有状态服务 |
| 会话注册表 | **Detached Ray Actor** 或 **Redis** | 跨 Deployment 访问 |
| Master HA | **Ray Head Node HA**（KubeRay + 外部 Redis） | Ray 2.x 原生支持 |
| Cluster Key 认证 | **Serve FastAPI Ingress 中间件** | 标准 FastAPI 中间件 |
| SQLite | **PostgreSQL / Redis** | Ray 不管数据层，必须迁出 SQLite |

---

## 3. 整体架构

```
                    ┌────────────────────────────────────┐
                    │      Ray Head Node (HA Pair)        │
                    │  - GCS (Global Control Store)       │
                    │  - Ray Dashboard                    │
                    │  - Serve Controller                 │
                    │  - 外部 Redis 持久化 GCS state       │
                    └─────────────────┬──────────────────┘
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        │                             │                             │
   ┌────▼────────────┐         ┌─────▼──────────┐          ┌──────▼─────────┐
   │  Worker Node 1  │         │  Worker Node 2 │          │  Worker Node 3 │
   │ resources:      │         │ resources:     │          │ resources:     │
   │  {browser: 4}   │         │  {browser: 4}  │          │  {browser: 2}  │
   │                 │         │                │          │                │
   │ ┌─────────────┐ │         │┌─────────────┐ │          │┌─────────────┐ │
   │ │HTTP Proxy   │ │         ││HTTP Proxy   │ │          ││HTTP Proxy   │ │
   │ │(Serve 内置) │ │         ││             │ │          ││             │ │
   │ └──────┬──────┘ │         │└──────┬──────┘ │          │└──────┬──────┘ │
   │        │        │         │       │        │          │       │        │
   │ ┌──────▼──────┐ │         │┌──────▼──────┐ │          │┌──────▼──────┐ │
   │ │APIGateway   │ │         ││APIGateway   │ │          ││APIGateway   │ │
   │ │Deployment   │ │ ◄─────► ││Deployment   │ │ ◄──────► ││Deployment   │ │
   │ │(FastAPI)    │ │         ││(FastAPI)    │ │          ││(FastAPI)    │ │
   │ └──────┬──────┘ │         │└──────┬──────┘ │          │└──────┬──────┘ │
   │        │        │         │       │        │          │       │        │
   │ ┌──────▼──────┐ │         │┌──────▼──────┐ │          │┌──────▼──────┐ │
   │ │BrowserPool  │ │         ││BrowserPool  │ │          ││BrowserPool  │ │
   │ │Deployment   │ │         ││Deployment   │ │          ││Deployment   │ │
   │ │(Replicas=4) │ │         ││(Replicas=4) │ │          ││(Replicas=2) │ │
   │ │ + Multiplex │ │         ││ + Multiplex │ │          ││ + Multiplex │ │
   │ └─────────────┘ │         │└─────────────┘ │          │└─────────────┘ │
   └─────────────────┘         └────────────────┘          └────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
       ┌──────▼─────┐         ┌──────▼─────┐          ┌─────▼──────┐
       │PostgreSQL  │         │ Redis      │          │TokenPool   │
       │(用户/配额/  │         │(会话缓存/   │          │ Actor      │
       │ Job 历史)  │         │ 限流)      │          │(detached)  │
       └────────────┘         └────────────┘          └────────────┘
```

### 3.1 关键架构变化

相比当前的 Master/Subnode 双角色架构：

- **取消 Master/Subnode 角色区分**：所有 worker 节点都是对等的，HTTP Proxy 在每个节点都跑，请求进入任何节点都能被路由到合适的 Replica
- **删除自研的 cluster_manager**：调度、健康检查、注册全部交给 Ray GCS 和 Serve Controller
- **删除 SQLite**：必须迁移到 PostgreSQL（SQLite 不支持多写者）
- **删除 HTTP Bridge**：Ray Serve 自带 HTTPProxy，不再需要双层 HTTP 架构

---

## 4. Deployment 拆分设计

整体拆分为 **4 个 Serve Deployment + 2 个 Detached Actor**：

### 4.1 `APIGateway` Deployment（无状态，纯路由）

```python
@serve.deployment(num_replicas="auto", max_ongoing_requests=100)
@serve.ingress(fastapi_app)
class APIGateway:
    def __init__(self, browser_pool, token_pool, session_registry):
        self.browser_pool = browser_pool      # handle to BrowserPool
        self.token_pool = token_pool          # handle to TokenPoolActor
        self.session_registry = session_registry
        # 复用现有的 FastAPI 路由：admin/portal/yescaptcha/service
```

**职责**：
- 处理认证、配额校验、API Key 管理
- Admin / Portal UI 静态文件托管
- YesCaptcha 协议兼容路由
- 无状态，可任意水平扩展
- **复用现有 FastAPI 路由代码（约 90% 可复用）**

### 4.2 `BrowserPool` Deployment（核心业务，stateful）

```python
@serve.deployment(
    ray_actor_options={"num_cpus": 1, "resources": {"browser": 1}},
    max_ongoing_requests=1,            # 一个 browser actor 一次只处理一个请求
    autoscaling_config={
        "min_replicas": 2,
        "max_replicas": 20,
        "target_ongoing_requests": 1,
    },
    health_check_period_s=10,
    health_check_timeout_s=30,
)
class BrowserPoolReplica:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.warm_pages: dict[str, Page] = {}  # project_id → warm page

    async def __init_browser__(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(...)

    @serve.multiplexed(max_num_models_per_replica=5)
    async def get_page_for_project(self, project_id: str) -> Page:
        # 关键: Ray Serve 会自动把同一 project_id 路由到同一 replica
        if project_id not in self.warm_pages:
            self.warm_pages[project_id] = await self.browser.new_page()
            await self.warm_pages[project_id].goto(f"https://.../{project_id}")
        return self.warm_pages[project_id]

    async def solve(self, request: SolveRequest) -> TokenResult:
        page = await self.get_page_for_project(request.project_id)
        return await self._extract_token(page, request)

    async def check_health(self):
        # Ray Serve 会周期调用，失败自动重启 actor
        if not self.browser or not self.browser.is_connected():
            raise RuntimeError("browser disconnected")
```

**关键设计点**：
- `@serve.multiplexed` 装饰器是 Ray Serve 的杀手级特性 — **天然实现 project affinity**
- `max_num_models_per_replica=5` 直接对应现有的 `personal_max_resident_tabs`
- `max_ongoing_requests=1` 对应"每个 browser 一次一个请求"
- `check_health` 自动健康检查，失败自动重启
- `autoscaling_config` 自动扩缩容，对应现有的浏览器池伸缩

### 4.3 `TokenPoolActor`（detached，跨 Replica 共享的待命池）

```python
@ray.remote(num_cpus=0.1, lifetime="detached", name="token_pool")
class TokenPoolActor:
    def __init__(self):
        self.buckets: dict[BucketKey, deque[Token]] = defaultdict(deque)

    async def get(self, bucket_key) -> Optional[Token]: ...
    async def put(self, bucket_key, token): ...
    async def stats(self) -> dict: ...
```

**作用**：
- 替代现有的 `_standby_buckets`
- 因为是 detached actor，重启 Deployment 不影响它
- 跨副本共享，APIGateway 命中即返回
- TTL 与桶分组逻辑保留现有实现

### 4.4 `SessionRegistryActor`（detached，会话生命周期）

```python
@ray.remote(num_cpus=0.1, lifetime="detached", name="session_registry")
class SessionRegistryActor:
    def __init__(self):
        self.sessions: dict[str, SessionState] = {}

    async def register(self, session_id, ...): ...
    async def finish(self, session_id): ...
    async def expire_loop(self): ...  # 后台清理
```

**作用**：
- 替代当前的 `session_registry.py`
- 也可以用 Redis 替代，更可靠

---

## 5. 功能点逐项映射检查

下面对当前项目的**每个功能点**做逐一检查，确认 Ray 方案的覆盖度：

| 功能点 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| **服务发现** | Ray GCS 自动 | ✅ 100% | 节点加入即被 GCS 发现 |
| **健康检查** | Serve `check_health` | ✅ 100% | 失败自动重启 |
| **负载均衡** | Serve 内置 | ✅ 100% | 默认 power-of-2-choices |
| **加权调度** | `ray_actor_options.resources` 自定义资源 | 🟡 80% | 没有现成的"权重"概念，要用资源数模拟 |
| **Project Affinity** | `@serve.multiplexed` | ✅ 100% | **比现在的实现更优雅**，框架原生支持 |
| **槽位预留** | `max_ongoing_requests=1` + Serve queue | ✅ 100% | 不需要自己写预留逻辑 |
| **重试** | Serve 内置 retry + `tenacity` | ✅ 100% | 配置即用 |
| **浏览器池伸缩** | `autoscaling_config` | ✅ 100% | 比当前的固定池更灵活 |
| **Token 待命池** | `TokenPoolActor` | ✅ 100% | 改成 detached actor |
| **prefill 预热** | 调用 `BrowserPool.solve.remote()` | ✅ 100% | 接口不变 |
| **Master 单点** | Ray Head HA + 外部 Redis | ✅ 100% | KubeRay 支持双 head |
| **节点失活检测** | Ray 自动 | ✅ 100% | 比 120s 心跳更及时 |
| **认证 (API Key)** | FastAPI 中间件 | ✅ 100% | 完全复用现有代码 |
| **YesCaptcha 协议** | FastAPI 路由 | ✅ 100% | 完全复用 |
| **Admin/Portal UI** | FastAPI + StaticFiles | ✅ 100% | 完全复用 |
| **配额管理** | PostgreSQL + 现有逻辑 | ✅ 100% | 数据层迁移，逻辑不变 |
| **CDK 兑换** | PostgreSQL + 现有逻辑 | ✅ 100% | 同上 |
| **TLS / HTTPS** | Serve HTTP options | ✅ 100% | 配置即用 |
| **自定义页面缓存** | Replica 内部 dict + multiplexing | ✅ 100% | 天然映射 |
| **代理支持** | Replica 内部配置 | ✅ 100% | 不变 |
| **指纹池** | Replica 内部状态 | ✅ 100% | 不变 |
| **日志聚合** | Ray 内置 log streaming + Loki | ✅ 100% | 不再需要 Redis 日志后端 |
| **Headed 模式 (Xvfb)** | Worker 节点容器内启动 Xvfb | 🟡 90% | Ray 不管 GUI，需要自定义 entrypoint |
| **nodriver 模式** | 同上，不同的 Replica 类 | ✅ 100% | 可以做成两个不同的 Deployment |
| **HTTP Bridge (双层架构)** | 不需要 | ✅ N/A | Ray Serve 自带 HTTP Proxy |
| **集群通信认证** | 不需要 | ✅ N/A | Ray 内部通信由 Ray 自己管 |

---

## 6. 无法完美映射的部分

### 6.1 SQLite → PostgreSQL 迁移（**最大的工作量**）

- SQLite 无法用于多写者环境
- 必须迁移到 PostgreSQL（或 MySQL）
- 涉及 **15 张表**的 schema 改写，所有 SQL 语句的方言适配
- `aiosqlite` → `asyncpg` 全部重写
- **这是迁移工作量最大的部分，比 Ray 改造本身还多**
- **注意**：这不是 Ray 的问题，但是迁移到 Ray 的前置条件

### 6.2 加权调度 (`node_weight`)

- Ray Serve 没有"节点权重"的直接概念
- **变通方案**：用 Ray 的自定义资源数量来模拟
  ```bash
  # 高性能节点
  ray start --resources='{"browser_slot": 8}'
  # 低性能节点
  ray start --resources='{"browser_slot": 2}'
  ```
- 调度时按资源声明自动均衡，等效于权重

### 6.3 YesCaptcha 异步任务模式

- YesCaptcha 协议是 **异步任务 + 轮询** 模型（`createTask` + `getTaskResult`）
- Ray Serve 本质是请求-响应模型
- **需要额外实现**：用 `SessionRegistryActor` 模拟任务队列，或者用 Ray 的 ObjectRef 持久化结果
- 不难做但需要写一些胶水代码

### 6.4 配置热加载

- 当前 Admin API 可以直接改内存配置（如 `captcha_method`），零停机
- Ray Serve 支持 `serve.run()` 重新部署，但**会有几秒钟的服务不可用**（rolling update）
- 这种"零停机配置切换"的特性会丢失
- 可以接受，因为这种修改本来就不应该频繁发生

### 6.5 Standalone 单机模式

- 现在的 standalone 模式可以零依赖跑起来（一个 Python 进程 + SQLite）
- Ray 模式即使在单机也需要先 `ray start --head` 启动 GCS
- 部署复杂度上升
- **变通方案**：用 `serve.run(local_testing_mode=True)` 或 `ray.init()` 内嵌模式

### 6.6 Docker 部署的 Xvfb / fluxbox

- Ray 本身不知道 GUI 这回事
- 需要在 Worker 容器的 entrypoint 里手动启动 Xvfb + fluxbox
- 然后 Ray Serve 的 actor 才能正常调用 Playwright headed 模式
- 不是阻塞问题，只是额外的部署步骤

---

## 7. 迁移工作量评估

| 模块 | 工作量 | 风险 |
|------|--------|------|
| `cluster_manager.py` | **完全删除**（约 1200 行） | 低 |
| `session_registry.py` | 重写为 Ray Actor | 中 |
| `browser_captcha.py` | 改造为 Serve Deployment | 中 |
| `browser_captcha_personal.py` | 同上 | 中 |
| `captcha_runtime.py` | 简化为 APIGateway 中的胶水代码 | 中 |
| `core/database.py` | **PostgreSQL 全量迁移** | **高** |
| `core/auth.py` | 几乎不变 | 低 |
| `api/*.py` | 几乎不变（只改依赖注入方式） | 低 |
| `http_bridge.py` | **删除** | 低 |
| Docker 部署 | **完全重写**（KubeRay / docker-compose ray） | 高 |
| 测试 | **大量重写**（mock Ray 比较麻烦） | 高 |

**总体评估**：
- 业务逻辑代码大约 **60% 可复用**
- 数据层和部署需要完全重写
- 实际改造工作量预计是**重构而不是重写**，但接近重写的复杂度

---

## 8. 收益与成本分析

### 8.1 真实收益

1. ✅ **删除约 2000 行自研集群代码**（cluster_manager + http_bridge + 部分 runtime）
2. ✅ **Project Affinity 用 `@serve.multiplexed` 一行装饰器解决**，比现在的 bucket affinity 实现更优雅可靠
3. ✅ **自动伸缩、自动健康检查、自动重启**全部框架原生
4. ✅ **Ray Dashboard 提供可视化监控**，不用自己写 admin 监控页
5. ✅ **真正的 HA**（Ray Head 双活 + 外部 Redis state）
6. ✅ **未来扩展到几十个节点完全无压力**

### 8.2 真实成本

1. ❌ **必须迁出 SQLite**，丧失"单文件部署"的便利性
2. ❌ **部署复杂度大幅上升**：用户原来 `docker-compose up` 就能跑，现在需要理解 Ray 集群、KubeRay
3. ❌ **学习曲线陡峭**，团队需要懂 Ray 的 Actor 模型、Serve、autoscaling 调参
4. ❌ **调试更难**：actor 之间的异步调用栈追踪、Ray Object Store 的内存管理
5. ❌ **额外资源开销**：Ray Head + GCS + Dashboard 即使空载也吃几百 MB 内存
6. ❌ **失去 Python 单进程的简单性**：日志、配置、热更新模型全部要重新设计
7. ❌ **依赖膨胀**：Ray 是个大依赖（200MB+），且对 Python 版本敏感

---

## 9. 最终评估与建议

### 9.1 技术可行性

技术上 **100% 可行**，每个功能点都有对应方案，且大部分能对应得很优雅：

- **能完美对应的**：浏览器池管理、Project Affinity、健康检查、负载均衡、HA、伸缩、认证、API 路由
- **能对应但需要变通的**：加权调度、YesCaptcha 异步协议、配置热加载
- **完全不在 Ray 范畴的**：SQLite → PostgreSQL 迁移、Xvfb GUI 环境、Standalone 零依赖部署

### 9.2 适用场景判断

**Ray 真正发光的场景**：
- 节点数 > 20
- 需要弹性自动伸缩（Spot 实例池）
- 多种异构 worker 类型（GPU + CPU 混合调度）
- 已经有其他 Ray 工作负载（比如 ML 训练）

**当前项目的实际定位**：
- 自托管、轻量、单 Master 够用
- 节点规模通常 < 10
- 团队规模小，运维资源有限

### 9.3 最终建议

> **当前规模和定位下，迁移到完整 K8s + Ray 是"杀鸡用牛刀"，但 k3s + Ray 是一个很好的折中方案。**

**修订后的建议路径**：

1. **短期（推荐立即执行）**：保留当前架构，做针对性强化
   - 用 `httpx.AsyncClient` 替换 `_sync_json_http_request`
   - 引入 `tenacity` 做退避重试
   - 把 bucket affinity 持久化到 SQLite
   - Master 端加主动探活 loop

2. **中期路径 A（节点 > 5，想解决 SPOF）**：**k3s + KubeRay + Ray**（**新推荐**）
   - 单节点 k3s 起步，4-8GB 服务器即可
   - 拿到完整的 Ray Serve 能力（Project Affinity / 自动伸缩 / HA）
   - 比完整 K8s 简单 10 倍，比裸机 Ray 集群运维成本低
   - 详见第 11 章
   - **未来无缝升级到全量 K8s，无迁移成本**

3. **中期路径 B（不想引入 K8s）**：NATS 升级
   - 解决 SPOF 和服务发现
   - 改造成本最小
   - 但失去 Ray 带来的伸缩/调度能力

4. **长期（如果做成 SaaS 或 20+ 节点大规模部署）**：完整 K8s + Ray
   - 从 k3s 平滑迁移（manifest 完全兼容）
   - 云托管 K8s（EKS / GKE / ACK）
   - 完整的 HA + 多 AZ + 弹性扩缩

### 9.4 决策矩阵

| 场景 | 推荐方案 |
|------|---------|
| 团队自用 + 几个节点 | **保留现有架构 + 短期强化** |
| 5-10 个节点，想解决 SPOF + 不想用 K8s | **NATS 升级** |
| 5-10 个节点，能接受轻量 K8s | **k3s + KubeRay + Ray**（推荐） |
| 10-20 个节点，需要 HA 和弹性 | **k3s HA 模式 + Ray** |
| 20+ 节点，需要云原生 | **完整 K8s（EKS/GKE）+ Ray** |
| 已经在 K8s 上 | **直接用 KubeRay + Ray Serve** |

---

## 10. 完整技术栈

### 10.1 技术栈总览

```
┌──────────────────────────────────────────────────────┐
│  Layer 7: 部署 / 运维                                 │
│  KubeRay + Kubernetes + Helm                          │
├──────────────────────────────────────────────────────┤
│  Layer 6: 可观测性                                    │
│  Ray Dashboard + Prometheus + Grafana + Loki          │
├──────────────────────────────────────────────────────┤
│  Layer 5: 集群编排 / 调度                             │
│  Ray Core + Ray Serve  ← 替代自研 cluster_manager     │
├──────────────────────────────────────────────────────┤
│  Layer 4: Web / API                                   │
│  FastAPI + Pydantic v2 + Uvicorn (Serve 内置)         │
├──────────────────────────────────────────────────────┤
│  Layer 3: 业务逻辑                                    │
│  Playwright + nodriver (保留不变)                      │
├──────────────────────────────────────────────────────┤
│  Layer 2: 数据层                                      │
│  PostgreSQL + Redis + asyncpg + redis-py              │
├──────────────────────────────────────────────────────┤
│  Layer 1: 运行时                                      │
│  Python 3.11+ / asyncio                               │
└──────────────────────────────────────────────────────┘
```

### 10.2 核心组件详细列表

#### 10.2.1 计算 / 编排层（核心新增）

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **Ray Core** | 2.9+ | 分布式 actor runtime, GCS 服务发现 | 🆕 新增 |
| **Ray Serve** | 2.9+ | HTTP 服务框架, Deployment 管理, 路由, 自动扩缩 | 🆕 新增 |
| **Ray Dashboard** | 内置 | 集群监控 UI | 🆕 新增 |

**Ray 内部使用的依赖**（自动安装）：
- gRPC（节点间通信）
- Redis（GCS 持久化，HA 模式必需）
- Plasma Object Store（共享内存对象存储）
- Protobuf

#### 10.2.2 Web / API 层（保留 + 调整）

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **FastAPI** | 0.110+ | API 路由（通过 `@serve.ingress` 集成） | ✅ 保留 |
| **Pydantic v2** | 2.x | 请求/响应模型校验 | ✅ 保留（如已是 v2） |
| **Uvicorn** | 0.27+ | ASGI 服务器（Serve 内部使用） | ✅ 保留 |
| **Starlette** | 0.36+ | FastAPI 底层 | ✅ 保留 |

#### 10.2.3 浏览器自动化层（完全保留）

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **Playwright** | 1.40+ | Chromium 自动化 | ✅ 保留 |
| **nodriver** | 0.48+ | 反检测浏览器（personal 模式） | ✅ 保留 |
| **Xvfb + fluxbox** | - | Linux headed 模式虚拟显示 | ✅ 保留 |
| **fake_useragent** | - | UA 指纹池 | ✅ 保留 |

#### 10.2.4 数据层（全部替换）

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **PostgreSQL** | 15+ | 主数据库（用户、配额、Job 历史） | 🔄 替换 SQLite |
| **asyncpg** | 0.29+ | 异步 PostgreSQL 驱动 | 🔄 替换 aiosqlite |
| **SQLAlchemy 2.x** (可选) | 2.x | ORM（如果不想手写 SQL） | 🆕 可选新增 |
| **Alembic** | 1.13+ | 数据库 schema 迁移工具 | 🆕 新增 |
| **Redis** | 7+ | 会话缓存、限流、Ray GCS HA 后端 | 🆕 新增（必需） |
| **redis-py** | 5.0+ | 异步 Redis 客户端 | 🆕 新增 |

**关键变化**：
- SQLite 完全移除
- Redis 从"可选日志后端"变成"必需的基础设施"
- 需要引入 Alembic 做 schema 版本管理

#### 10.2.5 HTTP 客户端 / 工具库（升级）

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **httpx** | 0.27+ | 异步 HTTP 客户端 | 🔄 替换 `_sync_json_http_request` |
| **curl-cffi** | 0.6+ | TLS 指纹伪装客户端 | ✅ 保留 |
| **tenacity** | 8.2+ | 重试库（指数退避） | 🆕 新增 |

#### 10.2.6 可观测性（新增整套）

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **Ray Dashboard** | 内置 | actor / replica / node 状态 | 🆕 新增 |
| **Prometheus** | 2.45+ | 指标采集 | 🆕 新增 |
| **Grafana** | 10+ | 可视化看板 | 🆕 新增 |
| **Loki** | 2.9+ (可选) | 日志聚合 | 🆕 可选 |
| **OpenTelemetry** | 1.20+ (可选) | 分布式追踪 | 🆕 可选 |
| **ray[default] metrics** | - | Ray 内置 Prometheus exporter | 🆕 新增 |

#### 10.2.7 容器化与编排（完全重写）

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **Docker** | 24+ | 容器运行时 | ✅ 保留 |
| **Kubernetes** | 1.28+ | 容器编排（推荐） | 🆕 新增 |
| **KubeRay Operator** | 1.1+ | Ray 集群的 K8s CRD 控制器 | 🆕 新增 |
| **Helm** | 3.13+ | K8s 应用包管理 | 🆕 新增 |
| **Ingress Controller** | - | Traefik / Nginx Ingress | 🆕 新增 |
| **cert-manager** | 1.13+ | HTTPS 证书自动管理 | 🆕 可选 |

**部署模式选项**：
1. **Kubernetes + KubeRay**（推荐，生产级）
2. **docker-compose + Ray standalone**（开发/小规模）
3. **裸机 Ray 集群**（不推荐）

#### 10.2.8 认证 / 安全（保留）

| 组件 | 作用 | 状态 |
|------|------|------|
| **bcrypt / passlib** | 密码哈希 | ✅ 保留 |
| **PyJWT**（可选） | JWT token | ✅ 保留 |
| **Bearer Token / API Key** | 认证机制 | ✅ 保留 |
| **TLS / mTLS** | 节点间通信加密 | 🆕 升级 |

#### 10.2.9 配置管理（调整）

| 组件 | 作用 | 状态 |
|------|------|------|
| **TOML 配置文件** | 静态配置 | ✅ 保留 |
| **环境变量** | `FCS_*` 前缀 | ✅ 保留 |
| **Ray Serve Config YAML** | Deployment 配置 | 🆕 新增 |
| **K8s ConfigMap / Secret** | 部署配置 | 🆕 新增 |

#### 10.2.10 测试 / CI（部分重写）

| 组件 | 作用 | 状态 |
|------|------|------|
| **pytest + pytest-asyncio** | 单元/集成测试 | ✅ 保留 |
| **ray.serve.test_utils** | Ray Serve 测试工具 | 🆕 新增 |
| **testcontainers-python** | PostgreSQL/Redis 容器测试 | 🆕 新增 |
| **httpx AsyncClient** | API 集成测试 | ✅ 保留 |

### 10.3 最小可运行依赖清单

```toml
# pyproject.toml / requirements.txt

# === 核心 Ray 依赖 ===
ray[serve]==2.9.3              # Ray Core + Ray Serve + Dashboard
ray[default]==2.9.3            # 包含 metrics exporter

# === Web 框架（保留）===
fastapi>=0.110.0
pydantic>=2.6.0
uvicorn[standard]>=0.27.0

# === 数据层（替换 SQLite）===
asyncpg>=0.29.0                # PostgreSQL 异步驱动
redis>=5.0.0                   # Redis 客户端
alembic>=1.13.0                # 数据库迁移
sqlalchemy>=2.0.25             # 可选 ORM

# === 浏览器自动化（保留）===
playwright>=1.40.0
nodriver==0.48.1
fake-useragent

# === HTTP 工具 ===
httpx>=0.27.0
curl-cffi>=0.6.0
tenacity>=8.2.0

# === 安全 ===
bcrypt>=4.1.0
passlib>=1.7.4

# === 可观测性 ===
prometheus-client>=0.19.0      # 自定义业务指标
opentelemetry-api>=1.20.0      # 可选追踪
opentelemetry-sdk>=1.20.0      # 可选追踪

# === 配置 ===
tomli>=2.0.1                   # TOML 解析（Python 3.11 已内置）

# === 测试 ===
pytest>=8.0.0
pytest-asyncio>=0.23.0
testcontainers[postgresql,redis]>=4.0.0
```

### 10.4 部署架构的基础设施清单

#### 10.4.1 生产环境（K8s）

```
Kubernetes 集群 (1.28+)
  ├── KubeRay Operator
  │     └── RayCluster CRD
  │           ├── Head Node (1-2 个,HA 模式)
  │           └── Worker Nodes (按需扩缩)
  ├── PostgreSQL (StatefulSet 或外部托管)
  │     └── 推荐:云厂商托管 (AWS RDS / GCP CloudSQL / Aliyun RDS)
  ├── Redis (StatefulSet 或外部托管)
  │     ├── 用途 1: Ray GCS HA 持久化
  │     ├── 用途 2: 业务会话缓存
  │     └── 用途 3: 限流计数器
  ├── Prometheus + Grafana (监控栈)
  ├── Loki (可选,日志聚合)
  ├── Ingress Controller (Traefik / Nginx)
  └── cert-manager (HTTPS 证书)
```

#### 10.4.2 开发环境（docker-compose）

```
docker-compose.yml
  ├── ray-head           # ray start --head
  ├── ray-worker-1       # ray start --address=ray-head:6379
  ├── ray-worker-2
  ├── postgres           # PostgreSQL 15
  ├── redis              # Redis 7
  └── nginx              # 本地 ingress（可选）
```

### 10.5 技术栈对比：现状 vs Ray 方案

| 层 | 现状 | Ray 方案 | 变化 |
|----|------|---------|------|
| **运行时** | Python 3.11 + asyncio | Python 3.11 + asyncio + Ray | + Ray runtime |
| **数据库** | SQLite (aiosqlite) | PostgreSQL (asyncpg) | 🔄 完全替换 |
| **缓存** | 无 / 可选 Redis | **必需** Redis | 🆕 升级为必需 |
| **Web 框架** | FastAPI | FastAPI + Serve 集成 | ✅ 保留 |
| **HTTP 客户端** | 自研 + httpx | httpx + tenacity | 🔄 标准化 |
| **集群协调** | 自研 cluster_manager (~1200 行) | Ray GCS + Serve | 🔄 删除自研代码 |
| **服务发现** | HTTP 注册接口 | Ray GCS 自动 | 🔄 框架接管 |
| **健康检查** | 心跳 (15s) + SQL 查询 | Serve `check_health` | 🔄 框架接管 |
| **负载均衡** | 自研加权轮询 | Serve 内置 + multiplexed | 🔄 框架接管 |
| **HA** | 无 | Ray Head HA + KubeRay | 🆕 新增 |
| **自动伸缩** | 无 / 手动 | Serve `autoscaling_config` | 🆕 新增 |
| **容器化** | Docker (Dockerfile.headed/master) | Docker + KubeRay | 🔄 完全重写 |
| **编排** | docker-compose | Kubernetes + Helm | 🔄 升级 |
| **监控** | 自研 admin 页面 | Ray Dashboard + Prometheus + Grafana | 🆕 标准化 |
| **日志** | 文件 / Redis List | Ray log streaming + Loki | 🔄 升级 |
| **配置** | TOML + 环境变量 | TOML + Serve YAML + K8s ConfigMap | 🔄 多层 |
| **测试** | pytest + 临时 SQLite | pytest + testcontainers | 🔄 升级 |

### 10.6 技术栈复杂度对比

#### 现状（Standalone 模式最小启动）

```
1. Python 3.11
2. pip install -r requirements.txt
3. python -m playwright install chromium
4. python -m src.main
```

**外部依赖数**：0（SQLite 是文件）

#### Ray 方案最小启动（开发）

```
1. Python 3.11
2. PostgreSQL (本地或 Docker)
3. Redis (本地或 Docker)
4. pip install -r requirements.txt
5. python -m playwright install chromium
6. ray start --head
7. alembic upgrade head  # 数据库 schema 初始化
8. serve run config.yaml  # 启动所有 Deployment
```

**外部依赖数**：3（PostgreSQL + Redis + Ray Cluster）

#### Ray 方案生产部署（K8s）

```
1. Kubernetes 集群
2. helm install kuberay-operator ...
3. kubectl apply -f raycluster.yaml
4. kubectl apply -f postgres.yaml (或外部托管)
5. kubectl apply -f redis.yaml
6. kubectl apply -f rayservice.yaml
7. kubectl apply -f ingress.yaml
8. helm install prometheus ...
9. helm install grafana ...
```

**外部依赖数**：6+（K8s + KubeRay + PostgreSQL + Redis + Prometheus + Grafana + Ingress）

### 10.7 技术栈成本/复杂度评估

| 维度 | 现状 | **k3s + Ray（推荐）** | 完整 K8s + Ray |
|------|------|---------------------|----------------|
| **依赖组件数** | 1 (SQLite) | **4 (k3s + KubeRay + PG + Redis)** | 6+ (含 Ingress/Prometheus/Grafana) |
| **最小机器规模** | 1 台 2GB | **1 台 4-8GB** | 3 台 8GB+ |
| **运维门槛** | 低（懂 Python 即可） | **中（K8s 基础 + KubeRay）** | 高（K8s 全栈 + Ray + DB 运维） |
| **学习成本** | 1-2 天 | **1 周** | 2-4 周 |
| **部署复杂度** | `docker-compose up` | **30 分钟，几条命令** | Helm Chart + 多 CRD，半天起步 |
| **资源占用（最小）** | ~500MB RAM | **~3-4GB RAM** | ~6-8GB RAM |
| **故障排查难度** | 简单（单进程日志） | **中（kubectl + Ray Dashboard）** | 复杂（跨 actor + 多组件追踪） |
| **HA 能力** | 无 | **可后续启用（embedded etcd）** | 原生 HA |
| **未来扩展性** | 受限 | **可平滑升级到全量 K8s** | 已是终态 |
| **文档/资料** | 自研，需要自己维护 | k3s + Ray 官方生态完善 | Ray 官方生态完善 |
| **社区支持** | 无 | 活跃（Rancher + Anyscale） | 活跃（Anyscale 商业支持） |

### 10.8 关键决策点

如果要采用 Ray 方案，**有几个无法回避的硬性要求**：

1. ✅ **必须迁移到 PostgreSQL**（SQLite 无法用于多写者环境）
2. ✅ **必须引入 Redis**（Ray GCS HA 持久化需要）
3. ✅ **必须用容器编排**（推荐 **k3s** 起步，未来可迁移到完整 K8s）
4. 🟡 **建议搭建监控栈**（Prometheus + Grafana，k3s 上可用 `kube-prometheus-stack`）
5. ✅ **团队需要至少 1 人懂 Ray**（autoscaling 调参、actor 调试）
6. 🟡 **团队需要懂基本 K8s 操作**（kubectl、Helm、ConfigMap），k3s 大幅降低这一门槛

> ⚠️ **第 1、2、3、5 条是硬性要求，不满足不建议迁移。第 4、6 条可在初期简化。**

> 💡 **如果觉得完整 K8s 太重，强烈推荐看第 11 章的 k3s 起步方案** —— 它能让 K8s 的运维成本降到一个小团队可以承受的范围。

---

## 11. k3s 起步方案（推荐）

> **本章是对第 10 章的重要补充**：完整 K8s 对小团队来说运维成本过高，**k3s 能以极低的复杂度提供完整的 K8s + KubeRay 能力**，是这个项目早期采用 Ray 方案的最佳起点。

### 11.1 为什么选 k3s

[k3s](https://k3s.io/) 是 Rancher（现 SUSE）出品的轻量级 Kubernetes 发行版，**通过 CNCF 官方一致性认证**，与标准 K8s API 100% 兼容。

| 特性 | k3s | 对 FCS 的价值 |
|------|-----|--------------|
| **单二进制安装** | `curl -sfL https://get.k3s.io \| sh -` 一行命令 | 5 分钟完成集群安装 |
| **资源占用低** | k3s 本身 ~700MB RAM | 一台 4GB 小 VM 都能跑 |
| **100% K8s API 兼容** | 通过 CNCF 一致性认证 | **KubeRay / Helm / kubectl 全部原样可用** |
| **内置 Traefik Ingress** | 自带 LB 和 Ingress | 不用单独装 ingress controller |
| **内置 local-path storage** | 自动配置 StorageClass | 不用配 PV/PVC |
| **内置 Klipper LB** | 裸机也能用 LoadBalancer Service | 不需要 MetalLB |
| **单节点模式可用** | 可以从 1 节点起步 | 开发/测试/小规模生产都行 |
| **HA 模式可后续启用** | 支持 embedded etcd 或外部 DB | 未来要 HA 时无需重建 |
| **Air-gapped 支持** | 可离线部署 | 内网环境友好 |

**关键事实**：**KubeRay 在 k3s 上和在 EKS/GKE 上的运行完全一致**，因为它就是一个标准的 K8s Operator + CRD。Ray 官方文档里也有 k3s 的部署示例。

### 11.2 k3s 方案的技术栈调整

相比第 10 章的"完整 K8s"方案，用 k3s 后**这些组件可以省掉或简化**：

| 原方案组件 | k3s 替代方案 | 节省 |
|-----------|------------|------|
| Nginx Ingress Controller | **k3s 内置 Traefik** | 不用装 |
| MetalLB（裸机 LB） | **k3s 内置 Klipper LB** | 不用装 |
| 外部 etcd 集群 | **k3s 内置 SQLite**（单节点）/ **embedded etcd**（HA） | 不用维护 |
| StorageClass / CSI | **k3s 内置 local-path-provisioner** | 不用装 |
| Helm 仓库管理 | k3s 直接支持 HelmChart CRD | 简化 |
| K8s 安装/升级 | `k3s server` / `k3s agent` 命令 | 简化 |

### 11.3 最小可运行架构（单节点 k3s）

```
┌────────────────────────────────────────────────────┐
│  单台服务器 (4-8 GB RAM, 2-4 vCPU)                  │
│                                                    │
│  ┌──────────────────────────────────────────────┐  │
│  │              k3s server (~700MB)              │  │
│  │  ┌────────────┐  ┌──────────────┐            │  │
│  │  │  Traefik   │  │ local-path SC│            │  │
│  │  │  (内置)    │  │   (内置)     │            │  │
│  │  └────────────┘  └──────────────┘            │  │
│  │                                              │  │
│  │  ┌────────────────────────────────────────┐  │  │
│  │  │       KubeRay Operator (~100MB)        │  │  │
│  │  └────────────────────────────────────────┘  │  │
│  │                                              │  │
│  │  ┌────────────────────────────────────────┐  │  │
│  │  │           RayCluster CR                │  │  │
│  │  │  ┌───────────┐    ┌────────────────┐  │  │  │
│  │  │  │ Head Pod  │    │  Worker Pods   │  │  │  │
│  │  │  │ (~1.5GB)  │    │ (Browser×2-4)  │  │  │  │
│  │  │  └───────────┘    └────────────────┘  │  │  │
│  │  └────────────────────────────────────────┘  │  │
│  │                                              │  │
│  │  ┌──────────────┐     ┌─────────────────┐    │  │
│  │  │ PostgreSQL    │     │     Redis       │    │  │
│  │  │  Pod (~200MB) │     │  Pod (~50MB)    │    │  │
│  │  └──────────────┘     └─────────────────┘    │  │
│  └──────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────┘

总计资源占用 ≈ 3-4 GB RAM (不含浏览器实例)
浏览器实例每个 ≈ 300-500 MB
4 个浏览器 ≈ 1.5-2 GB
总计运行时 ≈ 5-6 GB RAM,一台机器够用
```

### 11.4 具体部署步骤

```bash
# === 1. 装 k3s（30 秒搞定）===
curl -sfL https://get.k3s.io | sh -
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config

# === 2. 装 Helm ===
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# === 3. 装 KubeRay Operator ===
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm install kuberay-operator kuberay/kuberay-operator

# === 4. 部署 PostgreSQL（用 Bitnami chart 最快）===
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install postgres bitnami/postgresql \
  --set auth.postgresPassword=changeme \
  --set primary.persistence.size=10Gi

# === 5. 部署 Redis ===
helm install redis bitnami/redis \
  --set auth.password=changeme \
  --set master.persistence.size=5Gi

# === 6. 部署 RayService（包含 Flow Captcha 应用）===
kubectl apply -f rayservice-fcs.yaml

# === 7. 通过 Traefik 暴露（k3s 自带）===
kubectl apply -f ingress-fcs.yaml
```

**整个部署 30 分钟内可完成**，比裸机 Ray 集群还快。

### 11.5 渐进式扩展路径

k3s 的最大优势是**可以从单节点平滑成长到生产 HA 集群**：

```
阶段 1: 单节点 k3s (推荐起步)
  ├── 1 台 4-8GB 服务器
  ├── 所有组件单点
  └── 适合: 开发、测试、小规模生产 (< 100 并发)
        │
        │ 节点数增长
        ▼
阶段 2: 3 节点 k3s 集群
  ├── 1 server + 2 agent
  ├── Worker pods 分布到多台机器
  ├── PostgreSQL/Redis 仍单点（或用云托管）
  └── 适合: 中等规模生产
        │
        │ 需要 HA
        ▼
阶段 3: HA k3s 集群
  ├── 3 server (HA control plane) + N agents
  ├── 启用 embedded etcd 或外部 DB
  ├── PostgreSQL/Redis 用云托管或独立 HA
  └── 适合: 高可用生产环境
        │
        │ 节点 > 20 / 上云
        ▼
阶段 4: 迁移到云托管 K8s (可选)
  ├── EKS / GKE / ACK
  ├── 因为 k3s 100% 兼容 K8s API
  ├── 所有 manifest 文件直接复用
  └── 只需要重新部署,不需要改代码
```

> **关键优势：k3s → 全量 K8s 没有迁移成本**，因为 API 完全一致。所有 Helm chart、CRD、镜像、配置都可以无缝迁移。

### 11.6 注意事项

#### ⚠️ 1. 单节点默认 SQLite 后端不是 HA

- k3s 默认用 SQLite 存 K8s 元数据（**注意**：这是 k3s 的元数据，不是你的业务数据）
- 单节点没问题，要 HA 需要切换到 embedded etcd：`k3s server --cluster-init`
- **影响**：首次安装时就要决定，后期切换需要重建集群
- **建议**：开发/测试用默认 SQLite，生产部署直接用 embedded etcd

#### ⚠️ 2. Traefik 配置语法和 Nginx Ingress 不同

- k3s 默认用 Traefik 而不是 nginx-ingress
- 大部分 Helm chart 兼容，但部分特殊 annotation 需要调整
- 也可以禁用内置 Traefik：`k3s server --disable=traefik`，然后装 nginx-ingress

#### ⚠️ 3. 资源监控容易被忽略

- k3s 不强制装监控，容易出现"跑满了才发现"
- **强烈建议装 k3s + Prometheus 的轻量组合**（`kube-prometheus-stack` Helm chart）
- 加上 Ray Dashboard，可观测性就够用了

#### ⚠️ 4. PostgreSQL 持久化用 local-path 的限制

- k3s 内置的 local-path-provisioner 把数据存在节点本地
- 节点挂了数据就丢了
- **生产环境建议**：PostgreSQL 用云托管（RDS），或者 Pod 挂载宿主机持久化目录 + 定期备份

#### ⚠️ 5. 浏览器需要 Xvfb，Pod 配置要正确

- Worker Pod 的容器镜像里需要包含 Xvfb + fluxbox
- 启动命令需要先起 Xvfb 再启 Ray Worker
- 这部分和现在的 `Dockerfile.headed` 类似，迁移成本不大

### 11.7 修订后的整体演进路径

```
现状（自研 cluster_manager + SQLite）
        │
        │ 短期（1-2 周）
        ▼
方案 F: 针对性强化
(httpx + tenacity + 主动探活 + bucket affinity 持久化)
        │
        │ 节点数 > 5 或想引入 HA
        ▼
┌─────────────────────────┐
│  k3s + KubeRay + Ray    │  ← **新的推荐起步目标**
│  + PostgreSQL + Redis   │
│  单节点起步              │
└─────────────────────────┘
        │
        │ 节点数 > 20 或要做 SaaS
        ▼
完整 K8s（EKS/GKE）+ Ray
（无迁移成本，只需重新部署）
```

**这条路径的核心优势**：**没有任何"推倒重来"的环节** —— k3s 上跑通的所有 manifest、Helm chart、镜像都可以无缝迁移到全量 K8s。

### 11.8 k3s 方案小结

| 评估维度 | 结论 |
|---------|------|
| **是否能完全替代完整 K8s** | ✅ 早期阶段完全可以 |
| **是否能跑 KubeRay/Ray Serve** | ✅ 100% 兼容 |
| **运维门槛** | 🟡 中等（远低于完整 K8s） |
| **未来升级路径** | ✅ 平滑迁移到完整 K8s |
| **生产可用性** | ✅ 单节点适合小规模生产，HA 模式适合中等规模 |
| **推荐场景** | 节点数 5-20 的早期/中期阶段 |

> **最终建议**：如果决定走 Ray 方案，**强烈推荐从 k3s 单节点起步**，等业务规模真的需要再迁移到完整 K8s。这样既能拿到 Ray 的全部好处，又不会被 K8s 的运维复杂度劝退。

---

## 附录 A：Ray Serve 关键 API 速查

| 特性 | API | 说明 |
|------|-----|------|
| Deployment 定义 | `@serve.deployment(...)` | 标记一个类为可部署的服务 |
| FastAPI 集成 | `@serve.ingress(fastapi_app)` | 把 FastAPI 路由挂到 Deployment |
| 多模型路由 | `@serve.multiplexed` | 实现 model_id 亲和路由（用于 project affinity） |
| 自动伸缩 | `autoscaling_config={...}` | 配置 min/max replicas 和目标负载 |
| 健康检查 | `check_health(self)` 方法 | 周期性调用，失败触发重启 |
| 资源声明 | `ray_actor_options={"resources": {...}}` | 声明 actor 需要的自定义资源 |
| Detached Actor | `@ray.remote(lifetime="detached", name=...)` | 跨 Deployment 共享的有状态服务 |
| 部署 | `serve.run(deployment)` | 启动/更新 Deployment |

## 附录 B：参考资料

- Ray Serve 官方文档：https://docs.ray.io/en/latest/serve/index.html
- Model Multiplexing：https://docs.ray.io/en/latest/serve/model-multiplexing.html
- KubeRay Operator：https://github.com/ray-project/kuberay
- Ray HA Head Node：https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/kuberay-gcs-ft.html

---

**审核人**：_______________
**审核日期**：_______________
**审核结果**：☐ 通过　☐ 需修改　☐ 拒绝
