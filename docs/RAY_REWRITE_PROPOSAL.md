# Flow Captcha Service — Ray / Ray Serve 重写架构设计提案

> 状态：**待审核（Draft）** | 版本：2.0 | 更新日期：2026-04-09
>
> 本文档评估将 Flow Captcha Service 从当前的"自研 HTTP + SQLite + 心跳"集群协调方案迁移到 **Ray / Ray Serve** 框架的可行性、架构设计和迁移成本。
>
> **v2.0 重大修订说明**：v1.x 版本对项目的代理系统和 67 个配置项处理过于简化，错误地宣称 `@serve.multiplexed` 一行装饰器即可解决 project affinity。经过完整的代码审计后发现：bucket 的 affinity key 是 **三维**（`project_id × action × proxy_signature`），且支持 **per-token 代理覆盖**。本次修订全面重写第 4-7 章，给出真实可行的方案。

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
12. [新代码库目录结构规划](#12-新代码库目录结构规划)

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

> **v2.0 修订**：v1.x 提案过度简化，本章重新设计。整体拆分为 **5 个 Serve Deployment + 4 个 Detached Actor + 1 个 Background Loop**，比 v1.x 多出 ConfigResolver、ProxyPoolActor、WarmupActor，以及自定义的三维路由层。

### 4.0 设计的根本约束

任何 Ray 重写方案都必须解决以下三个**非平凡**问题，否则无法替代当前实现：

1. **Bucket key 是三维的**：`(project_id, action, proxy_signature)`。同一 project_id 但不同 proxy 必须被视为不同的 warm 状态，因为浏览器上下文绑定了代理。
2. **代理可被 per-token 覆盖**：`Token.captcha_proxy_url` 数据库字段允许每个 token 自带代理，必须在每次请求时动态解析，不能在 Replica 启动时绑定。
3. **7 个配置项有自动派生关系**：`0 = auto` 语义需要在所有 Deployment 启动前统一解析，否则各组件读到的值会不一致。

### 4.1 `ConfigResolver`（启动时一次性服务，不是 Deployment）

处理 7 个 `0 = auto` 的配置项，在所有 Deployment 实例化之前调用一次。

```python
# fcs/core/config_resolver.py
@dataclass(frozen=True)
class ResolvedConfig:
    browser_count: int
    project_affinity_max_keys: int      # auto: max(32, browser_count * 16)
    standby_bucket_max_count: int        # auto: max(32, browser_count * 12)
    standby_bucket_idle_ttl: float       # auto: max(token_ttl * 2, 180)
    request_finish_image_wait: int       # auto: 复用 flow_timeout
    request_finish_non_image_wait: int   # auto: 复用 upsample_timeout
    cluster_node_max_concurrency: int    # auto: 复用 browser_count
    execute_timeout: float               # 0 = 禁用
    # ... 其余 60 个直接传递的配置
    timeouts: TimeoutBundle              # 10 个超时维度的封装
    proxy_pool: list[str]                # 全局代理池(已 normalize)

def resolve_config(raw: RawConfig) -> ResolvedConfig:
    """把 7 个 auto 配置项展开为具体值，校验代理 URL，构建超时 bundle"""
    ...
```

**为什么必须有这一层**：
- Ray Serve 的 YAML 配置不支持配置项之间的派生关系
- 如果分散到各 Deployment 各自计算，会出现"BrowserPool 用 8，TokenPool 用 6"这类不一致
- 必须在 `entrypoint.py` 里集中解析后注入到所有 Deployment

### 4.2 `ProxyPoolActor`（detached，全局代理池管理）

```python
@ray.remote(num_cpus=0.1, lifetime="detached", name="proxy_pool")
class ProxyPoolActor:
    def __init__(self, global_pool: list[str]):
        self.global_pool = global_pool          # 已 normalize 的全局代理列表
        self.global_cursor = 0                  # 全局轮询游标
        self.token_cursors: dict[str, int] = {} # per-token 池游标(token 自带代理池时)

    async def resolve(
        self,
        token_proxy_url: Optional[str],         # token 上的代理(可能是单个或池)
        scope_key: str,                          # 用于游标隔离的 key(token_id)
    ) -> Optional[ProxyConfig]:
        """
        优先级:
          1. token 自带代理池 → 在 token_cursors[scope_key] 上轮询
          2. token 自带单个代理 → 直接返回
          3. 全局代理池 → 在 global_cursor 上轮询
          4. 都没有 → 返回 None(直连)
        """
        ...

    async def to_chromium_args(self, proxy: ProxyConfig) -> dict:
        """处理 SOCKS5+auth → HTTP fallback 的 Chromium 限制"""
        ...

    async def proxy_signature(self, proxy: Optional[ProxyConfig]) -> str:
        """返回 proxy 的 SHA1 短哈希,用于 bucket key 计算; None 返回 'direct'"""
        ...
```

**为什么必须独立成 Actor**：
- 池游标必须全局一致，否则多个 APIGateway Replica 会各自轮询导致代理分配不均
- per-token 游标也必须共享，否则同一 token 在不同 Replica 上会拿到不同代理
- 配置热更新（代理池变更）需要单点处理

### 4.3 `APIGateway` Deployment（请求编排核心）

```python
@serve.deployment(num_replicas="auto", max_ongoing_requests=100)
@serve.ingress(fastapi_app)
class APIGateway:
    def __init__(
        self,
        config: ResolvedConfig,
        browser_pool: DeploymentHandle,
        browser_pool_personal: DeploymentHandle,  # nodriver 版本(可选)
        token_pool: ActorHandle,
        proxy_pool: ActorHandle,
        session_registry: ActorHandle,
        api_key_repo: APIKeyRepository,
        quota_repo: QuotaRepository,
    ):
        self.cfg = config
        self.browser_pool = browser_pool
        self.browser_pool_personal = browser_pool_personal
        self.token_pool = token_pool
        self.proxy_pool = proxy_pool
        self.session_registry = session_registry
        # ... 其它依赖

    async def solve(self, request: SolveRequest) -> SolveResponse:
        # 1. 鉴权 + 配额校验
        api_key = await self.api_key_repo.find_by_hash(request.api_key_hash)
        await self.quota_repo.check_and_reserve(api_key.id)

        # 2. 加载 token 上下文(含 captcha_proxy_url 字段)
        token_ctx = await self._build_token_context(request)

        # 3. 解析 effective proxy(关键!)
        effective_proxy = await self.proxy_pool.resolve.remote(
            token_proxy_url=token_ctx.captcha_proxy_url,
            scope_key=token_ctx.token_id,
        )
        proxy_sig = await self.proxy_pool.proxy_signature.remote(effective_proxy)

        # 4. 构建三维 bucket key
        bucket_key = f"{request.project_id}|{request.action}|{proxy_sig}"

        # 5. 先查 standby pool
        cached = await self.token_pool.get.remote(bucket_key)
        if cached:
            return self._wrap_response(cached)

        # 6. 派发到 BrowserPool,multiplexed_model_id 用 bucket_key
        engine = self._select_engine(token_ctx)  # browser or personal
        handle = engine.options(multiplexed_model_id=bucket_key)
        result = await handle.solve.remote(SolveContext(
            request=request,
            effective_proxy=effective_proxy,
            bucket_key=bucket_key,
            timeouts=self.cfg.timeouts,
            fingerprint_seed=token_ctx.fingerprint_seed,
        ))

        # 7. 注册 session
        await self.session_registry.register.remote(
            session_id=result.session_id,
            api_key_id=api_key.id,
            bucket_key=bucket_key,
        )

        return self._wrap_response(result)
```

**关键设计点**：
- **不是无状态的简单网关**，而是请求编排器（替代现有的 `captcha_runtime.py`）
- **每次请求都要解析 proxy → 计算 bucket_key → 路由**，这是不可省略的核心流程
- 通过 `handle.options(multiplexed_model_id=bucket_key)` 触发 Ray Serve 的 multiplex 路由
- FastAPI 路由（`admin/portal/yescaptcha`）通过 `@serve.ingress` 集成

### 4.4 `BrowserPool` Deployment（Playwright 引擎）

```python
@serve.deployment(
    ray_actor_options={"num_cpus": 1, "resources": {"browser_slot": 1}},
    max_ongoing_requests=1,
    autoscaling_config={
        "min_replicas": 2,
        "max_replicas": 20,
        "target_ongoing_requests": 1,
    },
    health_check_period_s=10,
    health_check_timeout_s=30,
)
class BrowserPoolReplica:
    def __init__(self, config: ResolvedConfig):
        self.cfg = config
        self.playwright = None
        self.browser = None
        # warm_contexts: model_id → BrowserContext (不是 Page!)
        # 因为代理是绑定在 BrowserContext 上的
        self.warm_contexts: dict[str, BrowserContext] = {}
        self.fingerprint_pool = FingerprintPool(
            extra_count=config.browser_fingerprint_pool_extra_count,
        )

    async def __init_browser__(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=not self.cfg.browser_launch_background,
            args=CHROMIUM_LAUNCH_ARGS,  # 11 个硬编码 flags
        )

    @serve.multiplexed(max_num_models_per_replica=5)  # = personal_max_resident_tabs
    async def get_warm_context(self, model_id: str) -> BrowserContext:
        """
        model_id 是合成的 "{project}|{action}|{proxy_hash}" 三维 key。
        每个 model 对应一个 BrowserContext + 已加载的预热页面。
        """
        # model_id 不在缓存里 → 创建新 context(此处会触发代理绑定)
        # 注意:proxy 绑定在 BrowserContext 上,而不是 Browser 上
        # 这是 Playwright 的关键能力,nodriver 没有这个灵活性
        proxy_config = self._proxy_from_model_id(model_id)  # 反查 proxy
        fingerprint = self.fingerprint_pool.get(seed=model_id)

        ctx = await self.browser.new_context(
            proxy=proxy_config,                    # ← 代理在 context 级别
            user_agent=fingerprint.user_agent,
            viewport=fingerprint.viewport,
            locale=fingerprint.locale,
            timezone_id=fingerprint.timezone,
            device_scale_factor=fingerprint.device_scale,
        )
        # 预热页面
        page = await ctx.new_page()
        await page.goto(self._warmup_url_for(model_id))
        return ctx

    async def solve(self, ctx: SolveContext) -> TokenResult:
        warm_ctx = await self.get_warm_context(ctx.bucket_key)
        page = warm_ctx.pages[0]

        # 应用 10 个超时维度
        page.set_default_timeout(ctx.timeouts.execute * 1000)

        # 重试逻辑(replica 内重试 + 超时控制)
        for attempt in range(self.cfg.browser_retry_max_attempts):
            try:
                token = await self._extract_token(
                    page=page,
                    request=ctx.request,
                    score_dom_wait=ctx.timeouts.score_dom_wait,
                    reload_wait=ctx.timeouts.reload_wait,
                    clr_wait=ctx.timeouts.clr_wait,
                    settle=ctx.timeouts.recaptcha_settle,
                    image_wait=ctx.timeouts.request_finish_image_wait,
                    non_image_wait=ctx.timeouts.request_finish_non_image_wait,
                )
                return token
            except RetryableError:
                await asyncio.sleep(self.cfg.browser_retry_backoff_seconds * (2 ** attempt))

        raise SolveFailed("max retries exceeded")

    async def check_health(self):
        if not self.browser or not self.browser.is_connected():
            raise RuntimeError("browser disconnected")
        # 检查 context 是否还活着
        for model_id, ctx in list(self.warm_contexts.items()):
            try:
                if not ctx.pages:
                    self.warm_contexts.pop(model_id)
            except Exception:
                self.warm_contexts.pop(model_id)
```

**关键设计点（与 v1.x 的区别）**：
- `warm_contexts` 的 key 是合成的三维 `model_id`，**不再是单纯的 `project_id`**
- **代理绑定在 `BrowserContext` 上**，不是 `Browser` 上 — 这是 Playwright 的关键能力
- 每个 model = 一个独立的 BrowserContext + 一个预热好的 Page
- 10 个超时维度通过 `SolveContext` 显式传递，不依赖全局变量
- 重试在 Replica 内部完成，避免跨 Replica 失去 warm 状态

### 4.5 `BrowserPoolPersonal` Deployment（nodriver 引擎）

```python
@serve.deployment(...)
class BrowserPoolPersonalReplica:
    def __init__(self, config: ResolvedConfig):
        self.cfg = config
        self.browser = None
        self.warm_tabs: dict[str, Tab] = {}
        self.tab_failures: dict[str, int] = defaultdict(int)
        self.browser_failures = 0

    @serve.multiplexed(max_num_models_per_replica=5)
    async def get_warm_tab(self, model_id: str) -> Tab:
        """
        nodriver 的关键限制:代理只能在 browser 启动时绑定,无法 per-tab 切换。
        因此每个 Personal Replica 只对应一个代理。
        如果 model_id 隐含的 proxy 与当前 Replica 的 proxy 不匹配 → raise
        让 Ray Serve 选择另一个 Replica。
        """
        if not self._proxy_matches(model_id):
            raise WrongProxyForReplica()
        ...

    async def solve(self, ctx: SolveContext) -> TokenResult:
        try:
            result = await self._do_solve(ctx)
            self.tab_failures[ctx.bucket_key] = 0
            return result
        except Exception as e:
            self.tab_failures[ctx.bucket_key] += 1

            # 实现现有的 recreate/restart 阈值逻辑
            if self.tab_failures[ctx.bucket_key] >= self.cfg.browser_personal_recreate_threshold:
                tab = self.warm_tabs.pop(ctx.bucket_key, None)
                if tab:
                    await tab.close()

            self.browser_failures += 1
            if self.browser_failures >= self.cfg.browser_personal_restart_threshold:
                # 主动 raise → Ray Serve 会重启这个 Replica
                raise BrowserNeedsRestart() from e

            raise
```

**关键设计点**：
- nodriver 的代理绑定在 Browser 级别（与 Playwright 不同），所以 **同一个 Personal Replica 只能对应一个代理**
- 如果路由到了"代理不匹配"的 Replica，主动 raise，让 Ray Serve 重新选 Replica
- `recreate_threshold` 和 `restart_threshold` 的语义在 Replica 内部实现，超过阈值时主动崩溃 → Ray 重启

### 4.6 `TokenPoolActor`（detached，三维 bucket）

```python
@ray.remote(num_cpus=0.1, lifetime="detached", name="token_pool")
class TokenPoolActor:
    def __init__(self, config: ResolvedConfig):
        self.cfg = config
        # bucket_key = "{project}|{action}|{proxy_hash}"
        self.buckets: dict[str, deque[StandbyToken]] = defaultdict(deque)
        self.bucket_last_used: dict[str, float] = {}
        # LRU 淘汰:每个 project 最多 standby_bucket_max_count 个 bucket
        self.project_buckets: dict[str, list[str]] = defaultdict(list)

    async def get(self, bucket_key: str) -> Optional[StandbyToken]:
        bucket = self.buckets.get(bucket_key)
        if not bucket:
            return None
        # TTL 检查
        while bucket:
            token = bucket.popleft()
            if time.time() - token.created_at < self.cfg.browser_standby_token_ttl_seconds:
                self.bucket_last_used[bucket_key] = time.time()
                return token
        return None

    async def put(self, bucket_key: str, token: StandbyToken) -> None:
        if len(self.buckets[bucket_key]) >= self.cfg.browser_standby_token_pool_depth:
            return  # 池满
        self.buckets[bucket_key].append(token)
        self._enforce_lru(bucket_key)

    async def expire_idle_buckets(self):
        """后台定期调用,清理超过 standby_bucket_idle_ttl 的空 bucket"""
        ...
```

**关键变化**：
- bucket key 改为合成字符串
- 实现现有的 5 个 standby pool 配置项：`enabled / depth / token_ttl / bucket_max_count / bucket_idle_ttl / refill_idle`

### 4.7 `WarmupActor`（detached，自动预热）

```python
@ray.remote(num_cpus=0.1, lifetime="detached", name="warmup")
class WarmupActor:
    def __init__(
        self,
        config: ResolvedConfig,
        browser_pool: DeploymentHandle,
        token_pool: ActorHandle,
        proxy_pool: ActorHandle,
    ):
        self.cfg = config
        self.browser_pool = browser_pool
        self.token_pool = token_pool
        self.proxy_pool = proxy_pool

    async def start(self):
        """启动后台预热循环"""
        if self.cfg.browser_auto_warm_project_id:
            asyncio.create_task(self._warm_native_loop())
        if self.cfg.browser_auto_warm_website_url:
            asyncio.create_task(self._warm_custom_loop())

    async def _warm_native_loop(self):
        """处理 6 个 auto_warm_* 配置项"""
        while True:
            # 用 browser_auto_warm_project_id + browser_auto_warmup_action
            # 调用 browser_pool 生成 token,放到 token_pool
            ...
            await asyncio.sleep(self.cfg.browser_standby_refill_idle_seconds)
```

**作用**：
- 替代现有的 `_browser_auto_warm_*` 配置驱动的后台预热逻辑
- 必须独立成 Actor，因为 APIGateway 是无状态的，没法承载长生命周期的预热循环

### 4.8 `SessionRegistryActor`（detached，会话生命周期）

```python
@ray.remote(num_cpus=0.1, lifetime="detached", name="session_registry")
class SessionRegistryActor:
    def __init__(self, config: ResolvedConfig):
        self.cfg = config
        self.sessions: dict[str, SessionState] = {}

    async def register(self, session_id, api_key_id, bucket_key, ...): ...
    async def finish(self, session_id): ...
    async def expire_loop(self):
        """根据 session_ttl_seconds 定期清理过期 session"""
```

**也可以替换为 Redis**（推荐生产环境），因为 detached actor 的状态在 actor 崩溃时会丢失。

### 4.9 完整请求流程（含代理上下文传递）

```
Client HTTP Request (含 Bearer fcs_xxx)
        │
        ▼
┌─────────────────────────────────────────────────┐
│  APIGateway Deployment                           │
│                                                  │
│  1. Auth: api_key_repo.find_by_hash()           │
│  2. Quota: quota_repo.check_and_reserve()       │
│  3. Token ctx: 加载 captcha_proxy_url 字段       │
│  4. Proxy: proxy_pool.resolve(token_proxy, key) │
│  5. Sig: proxy_pool.proxy_signature(proxy)      │
│  6. bucket_key = project|action|sig             │
│  7. cache: token_pool.get(bucket_key)           │
│       │                                          │
│       ├─ HIT  → 返回 token,完成                  │
│       │                                          │
│       └─ MISS → 8. 派发                          │
│                                                  │
│  8. handle = browser_pool.options(              │
│       multiplexed_model_id=bucket_key)          │
│  9. handle.solve.remote(SolveContext(...))      │
└─────────────────────────────────────────────────┘
        │
        │  Ray Serve multiplexed router
        │  (按 bucket_key 路由到拥有 warm context 的 replica)
        ▼
┌─────────────────────────────────────────────────┐
│  BrowserPool Replica N                           │
│                                                  │
│  10. get_warm_context(bucket_key)               │
│        │                                         │
│        ├─ 已有 → 复用                            │
│        │                                         │
│        └─ 没有 → browser.new_context(           │
│                    proxy=effective_proxy, ...)  │
│  11. extract_token(page, request, timeouts)    │
│        │                                         │
│        └─ 重试逻辑(retry_max_attempts)          │
│  12. 返回 TokenResult                            │
└─────────────────────────────────────────────────┘
        │
        ▼
APIGateway 注册 session → 返回响应
```

### 4.10 与 v1.x 设计的关键差异

| 项 | v1.x（错误） | v2.0（修订） |
|----|-------------|-------------|
| 路由 key | 单维 `project_id` | 三维 `{project}\|{action}\|{proxy_hash}` |
| 代理处理 | "Replica 内部配置" 一句话带过 | 独立的 ProxyPoolActor + per-context 绑定 |
| Per-token 代理 | 完全没考虑 | 通过 SolveContext 显式传递 |
| Warm 状态对象 | `warm_pages: dict[project_id, Page]` | `warm_contexts: dict[bucket_key, BrowserContext]` |
| 配置传递 | 隐式（各 Replica 自己读 TOML） | 显式（ConfigResolver 集中解析后注入） |
| 自动预热 | 没提 | 独立的 WarmupActor |
| Personal 模式重启阈值 | 没实现 | Replica 内置失败计数器 + 主动 raise |
| 超时维度 | 笼统的"timeout" | 10 个独立维度通过 SolveContext 传递 |
| Deployment / Actor 总数 | 4 + 2 = 6 | 5 + 4 = 9（+ 1 ConfigResolver 启动时服务） |

---

## 5. 功能点逐项映射检查

> **v2.0 修订**：v1.x 表格只有 26 项，遗漏了 40+ 个浏览器/代理相关配置。本章按类别完整覆盖 67 个 FCS_ 配置项 + 关键功能。

### 5.1 浏览器引擎与生命周期（11 项）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `browser_count` | Replica `min_replicas` 起始值 | ✅ 100% | 由 autoscaling 接管 |
| `browser_launch_background` | 启动时 `launch(headless=...)` | ✅ 100% | 通过 ResolvedConfig 注入 |
| `browser_idle_ttl_seconds` | Serve `downscale_delay_s` | 🟡 80% | 语义略有差异 |
| `browser_idle_reaper_interval_seconds` | Serve 内部周期 | 🟡 N/A | Ray 自动管理 |
| `browser_personal_recreate_threshold` | Replica 内 `tab_failures` 计数器 | ✅ 100% | 见 4.5 |
| `browser_personal_restart_threshold` | Replica 主动 raise 触发 Ray 重启 | ✅ 100% | 见 4.5 |
| `browser_count` 自动派生 | ConfigResolver 处理 | ✅ 100% | 见 4.1 |
| 11 个硬编码 launch args | Replica 启动时传入 | ✅ 100% | 不变 |
| 浏览器实例对等 | Ray Replica 天然对等 | ✅ 100% | 框架原生 |
| 进程清理 | Ray Serve 自动 | ✅ 100% | replica 销毁时 cleanup |
| 浏览器崩溃恢复 | Ray Serve check_health → 重启 | ✅ 100% | 框架原生 |

### 5.2 超时配置（10 项，全部需要正确传递）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `browser_execute_timeout_seconds` | SolveContext.timeouts.execute | ✅ 100% | Replica 内 page.set_default_timeout |
| `browser_reload_wait_timeout_seconds` | SolveContext.timeouts.reload_wait | ✅ 100% | extract_token 内使用 |
| `browser_clr_wait_timeout_seconds` | SolveContext.timeouts.clr_wait | ✅ 100% | 同上 |
| `browser_score_dom_wait_seconds` | SolveContext.timeouts.score_dom_wait | ✅ 100% | 同上 |
| `browser_recaptcha_settle_seconds` | SolveContext.timeouts.recaptcha_settle | ✅ 100% | 同上 |
| `browser_score_test_warmup_seconds` | SolveContext.timeouts.score_test_warmup | ✅ 100% | 同上 |
| `browser_score_test_settle_seconds` | SolveContext.timeouts.score_test_settle | ✅ 100% | 同上 |
| `browser_request_finish_image_wait_seconds` | ConfigResolver 自动派生 | ✅ 100% | 0=auto 复用 flow_timeout |
| `browser_request_finish_non_image_wait_seconds` | ConfigResolver 自动派生 | ✅ 100% | 同上 |
| `flow_timeout` / `upsample_timeout` / `session_ttl_seconds` | ConfigResolver 全局 | ✅ 100% | 同上 |

### 5.3 重试与故障恢复（4 项）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `browser_retry_max_attempts` | Replica 内重试循环 | ✅ 100% | 不依赖 Ray Serve 重试 |
| `browser_retry_backoff_seconds` | 指数退避 | ✅ 100% | 同上 |
| `browser_personal_recreate_threshold` | tab_failures 计数器 | ✅ 100% | 见 4.5 |
| `browser_personal_restart_threshold` | 主动 raise → Ray 重启 replica | ✅ 100% | 见 4.5 |

### 5.4 Standby Token Pool（6 项）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `browser_standby_token_pool_enabled` | TokenPoolActor 配置 | ✅ 100% | |
| `browser_standby_token_pool_depth` | TokenPoolActor.put 拒绝条件 | ✅ 100% | |
| `browser_standby_token_ttl_seconds` | TokenPoolActor.get TTL 检查 | ✅ 100% | |
| `browser_standby_bucket_max_count` | LRU 淘汰策略 | ✅ 100% | ConfigResolver 处理 0=auto |
| `browser_standby_bucket_idle_ttl_seconds` | expire_idle_buckets 后台任务 | ✅ 100% | ConfigResolver 处理 0=auto |
| `browser_standby_refill_idle_seconds` | WarmupActor 后台预热间隔 | ✅ 100% | |

### 5.5 Project Affinity（实际是三维 affinity）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `browser_project_affinity_max_keys` | `@serve.multiplexed(max_num_models_per_replica=...)` | 🟡 80% | 语义略有差异 |
| `browser_project_affinity_ttl_seconds` | model 自动 LRU 淘汰 | 🟡 70% | Ray 没有 TTL，只有 LRU |
| **三维 bucket key** | 合成 model_id `{p}\|{a}\|{ph}` | 🟡 75% | **不能用 Ray 原生 multiplex 直接做**，需要在 APIGateway 拼接 |
| **per-token 代理覆盖** | SolveContext + ProxyPoolActor.resolve | ✅ 100% | 见 4.3 |

### 5.6 自动预热（6 项，v1.x 完全没考虑）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `browser_flow_website_key` | WarmupActor 配置 | ✅ 100% | |
| `browser_auto_warm_project_id` | WarmupActor 启动条件 | ✅ 100% | |
| `browser_auto_warmup_action` | WarmupActor 配置 | ✅ 100% | |
| `browser_auto_warm_website_url` | WarmupActor 配置 | ✅ 100% | |
| `browser_auto_warm_website_key` | 同上 | ✅ 100% | |
| `browser_auto_warm_action` | 同上 | ✅ 100% | |

### 5.7 自定义页面缓存（2 项）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `browser_custom_page_cache_max_pages` | Replica 内 dict + LRU | ✅ 100% | |
| `browser_custom_page_idle_ttl_seconds` | Replica 后台清理协程 | ✅ 100% | |

### 5.8 指纹池（1 项 + 大量硬编码）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `browser_fingerprint_pool_extra_count` | FingerprintPool 类 | ✅ 100% | |
| ~20 个基础 UA pool | 硬编码常量 | ✅ 100% | 不变 |
| 5 个桌面/移动分辨率映射 | 硬编码常量 | ✅ 100% | 不变 |
| 6-8 个 locale/timezone 区域 | 硬编码常量 | ✅ 100% | 不变 |
| UA hash → profile 确定性映射 | 工具函数 | ✅ 100% | 不变 |

### 5.9 代理系统（**v1.x 严重遗漏的部分**）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `browser_proxy_enabled` | ProxyPoolActor 启用判断 | ✅ 100% | |
| `browser_proxy_url`（单个或池） | ProxyPoolActor 全局池 | ✅ 100% | |
| 代理池轮询（cursor） | ProxyPoolActor.global_cursor | ✅ 100% | 必须独立 actor 才能保证一致性 |
| Per-token 代理覆盖（DB 字段） | APIGateway 显式传递 | ✅ 100% | 见 4.3 |
| Per-token 代理池游标隔离 | ProxyPoolActor.token_cursors | ✅ 100% | |
| SOCKS5+auth → HTTP fallback | ProxyPoolActor.to_chromium_args | ✅ 100% | |
| 代理签名计算（SHA1） | ProxyPoolActor.proxy_signature | ✅ 100% | 用于 bucket key |
| 代理池 normalize/validate | ConfigResolver 启动时处理 | ✅ 100% | |
| 代理切换的 BrowserContext 重建 | Playwright 原生支持 | ✅ 100% | nodriver 不支持 |
| 共享浏览器代理跟踪 | Replica 内 dict 状态 | 🟡 80% | 概念变化，不再有"shared browser" |

### 5.10 Personal 模式（nodriver，3 项）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `personal_project_pool_size` | BrowserPoolPersonal autoscaling | 🟡 70% | 语义不完全一致 |
| `personal_max_resident_tabs` | `@serve.multiplexed(max_num_models_per_replica=5)` | ✅ 100% | |
| `personal_idle_tab_ttl_seconds` | LRU + 后台清理 | ✅ 100% | |

### 5.11 服务器与基础设施（5 项）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `server.host` / `server.port` | Serve HTTP options | ✅ 100% | |
| `storage.db_path` | **删除** | 🔴 N/A | 改用 `database.url` (PostgreSQL) |
| `admin.username` / `admin.password` | 不变 | ✅ 100% | |
| `node_name` | Ray namespace 或 deployment name | 🟡 80% | 用途变化 |

### 5.12 集群配置（8 项，**全部由 Ray 接管**）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `cluster.role` | **删除** | 🔴 N/A | Ray 节点对等 |
| `cluster.master_base_url` | **删除** | 🔴 N/A | Ray GCS 自动 |
| `cluster.master_cluster_key` | **删除** | 🔴 N/A | Ray 内部认证 |
| `cluster.node_public_base_url` | **删除** | 🔴 N/A | Ray GCS 自动 |
| `cluster.node_api_key` | **删除** | 🔴 N/A | Ray 内部认证 |
| `cluster.heartbeat_interval_seconds` | **删除** | 🔴 N/A | Ray 内部心跳 |
| `cluster.node_weight` | Ray 自定义资源 `browser_slot` | 🟡 80% | 用资源数模拟 |
| `cluster.node_max_concurrency` | Replica `max_ongoing_requests` | ✅ 100% | |

### 5.13 日志（6 项）

| 配置项 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| `log.level` | 不变 | ✅ 100% | |
| `log.storage_backend` | 改为 Loki / Ray log streaming | 🔄 替换 | 不再用 SQLite/Redis 后端 |
| `log.redis_url` / `log.redis_key_prefix` / `log.redis_max_entries` | **删除** | 🔴 N/A | 用 Loki 替代 |
| `log.startup_clear_on_boot` | **删除** | 🔴 N/A | Loki 自管理 |
| `log.auto_clear_interval_minutes` | **删除** | 🔴 N/A | 同上 |

### 5.14 其他系统能力

| 功能点 | Ray 方案 | 完整度 | 备注 |
|--------|---------|--------|------|
| 服务发现 | Ray GCS | ✅ 100% | |
| 健康检查 | Serve check_health | ✅ 100% | |
| 负载均衡 | Serve 内置 | ✅ 100% | |
| 槽位预留 | max_ongoing_requests | ✅ 100% | |
| 浏览器池伸缩 | autoscaling_config | ✅ 100% | |
| Master HA | Ray Head HA + Redis | ✅ 100% | |
| 节点失活检测 | Ray 自动 | ✅ 100% | |
| 认证（API Key） | FastAPI 中间件 | ✅ 100% | |
| YesCaptcha 协议 | FastAPI 路由 + SessionRegistry 模拟任务队列 | 🟡 85% | 见 6.10 |
| Admin/Portal UI | FastAPI + StaticFiles | ✅ 100% | |
| 配额管理 | PostgreSQL | ✅ 100% | |
| CDK 兑换 | PostgreSQL | ✅ 100% | |
| TLS / HTTPS | Serve HTTP options | ✅ 100% | |
| Headed 模式（Xvfb） | Worker 容器 entrypoint | 🟡 90% | |
| HTTP Bridge | **删除** | ✅ N/A | Serve 自带 HTTPProxy |
| 集群通信认证 | **删除** | ✅ N/A | Ray 内部 |

### 5.15 修订后的完整度统计

| 类别 | 总数 | ✅ 100% | 🟡 80%+ | 🔴 删除/N/A |
|------|-----|--------|---------|-----------|
| 浏览器引擎与生命周期 | 11 | 9 | 2 | 0 |
| 超时配置 | 10 | 10 | 0 | 0 |
| 重试与故障恢复 | 4 | 4 | 0 | 0 |
| Standby Token Pool | 6 | 6 | 0 | 0 |
| Project Affinity | 4 | 1 | 3 | 0 |
| 自动预热 | 6 | 6 | 0 | 0 |
| 自定义页面缓存 | 2 | 2 | 0 | 0 |
| 指纹池 | 5 | 5 | 0 | 0 |
| 代理系统 | 10 | 9 | 1 | 0 |
| Personal 模式 | 3 | 2 | 1 | 0 |
| 服务器与基础设施 | 4 | 2 | 1 | 1 |
| 集群配置 | 8 | 1 | 1 | 6 |
| 日志 | 6 | 1 | 0 | 5 |
| 其他系统能力 | 16 | 13 | 2 | 1 |
| **合计** | **95** | **71 (75%)** | **11 (12%)** | **13 (13%)** |

> **关键观察**：实际可以"框架原生支持 100%"的项目只有 75%，需要变通的项目占 12%，集群相关的 13% 直接被 Ray 取代删除。这与 v1.x 的"几乎都是 ✅ 100%" 的描述差距很大。

---

## 6. 无法完美映射的部分

> **v2.0 修订**：v1.x 只列了 6 个，本章扩展到 14 个，新增 8 个 v1.x 完全没考虑的项。

### 6.1 SQLite → PostgreSQL 迁移（**最大的工作量**）

- 与 v1.x 描述一致：15 张表 schema 改写、所有 SQL 方言适配、`aiosqlite → asyncpg`
- **不是 Ray 的问题，但是迁移到 Ray 的前置条件**

### 6.2 三维 Bucket Affinity 路由（**v1.x 严重错误的设计**）

**问题**：v1.x 提案声称 `@serve.multiplexed(model_id=project_id)` 一行装饰器可解决 project affinity。**实际上是错的**，因为 bucket key 是三维 `(project, action, proxy)`。

**变通方案**：
- 在 APIGateway 里手动拼接 `model_id = f"{project}|{action}|{proxy_hash}"`
- 通过 `handle.options(multiplexed_model_id=model_id)` 触发 Ray Serve 的 multiplex 路由
- Ray Serve 会确保同一 model_id 路由到同一 Replica（如果 Replica 已经加载过该 model）
- 缺点：每多一个代理，warm context 数量翻倍；高代理多样性场景下 cache miss 率会上升

**未解决的问题**：
- Ray Serve 的 multiplex LRU 策略不可配置，无法完全等价于现有的 `browser_project_affinity_ttl_seconds`
- 跨 Replica 的 model 重叠问题（同一 model_id 可能在多个 Replica 上都加载，浪费内存）

### 6.3 Per-Token 代理覆盖（**v1.x 完全没考虑**）

**问题**：`Token` 表的 `captcha_proxy_url` 字段允许每个 token 自带代理，必须在每次请求时动态解析。

**变通方案**：
- ProxyPoolActor 统一管理（见 4.2）
- APIGateway 在每次 solve 时调用 `proxy_pool.resolve()` 解析 effective proxy
- effective proxy 通过 SolveContext 透传到 BrowserPool Replica
- BrowserPool Replica 根据 effective proxy 创建对应的 BrowserContext

**遗留问题**：
- nodriver 模式下代理只能在 Browser 启动时绑定，**无法 per-tab 切换**。这意味着 Personal Replica 必须按代理分组，每组对应一个 Replica。
- 这与 Playwright 的灵活性差异巨大，需要在文档里明确说明 Personal 模式的代理使用场景受限。

### 6.4 配置自动派生（7 个 `0 = auto` 项）

**问题**：Ray Serve 的 YAML 配置不支持配置项之间的派生关系。

**变通方案**：
- 引入 `ConfigResolver`（见 4.1）
- 在 `entrypoint.py` 里集中解析后注入到所有 Deployment
- 所有 Deployment 接收 `ResolvedConfig` 而不是直接读 TOML

**代价**：
- 多了一层间接性
- 配置热更新更复杂（需要重新部署所有 Deployment）

### 6.5 10 层超时维度的传递

**问题**：现有项目有 10 个独立的超时配置项（execute / reload_wait / clr_wait / score_dom_wait / recaptcha_settle / ...）。Ray Serve 本身只有"请求总超时"。

**变通方案**：
- 通过 SolveContext 显式传递所有超时维度
- Replica 内部根据每个维度调用对应的 Playwright API（`page.set_default_timeout`、`wait_for_timeout`、`wait_for_load_state`）

**不是阻塞问题**，但需要确保每一层超时都被正确应用，不能简化为单一超时。

### 6.6 自动预热（6 个 auto_warm 配置）

**问题**：现有项目支持启动时自动预热 Token 池，APIGateway 是无状态的没法承载长生命周期任务。

**变通方案**：
- 独立的 WarmupActor（见 4.7）
- 启动时根据配置创建后台任务循环

**需要注意**：
- WarmupActor 必须在 BrowserPool 之后启动（依赖 deployment handle）
- 重启时需要重新建立 handle 引用

### 6.7 Personal 模式重建/重启阈值

**问题**：现有的 `browser_personal_recreate_threshold` 和 `browser_personal_restart_threshold` 是细粒度的故障恢复策略。Ray 只有"健康检查失败 → 重启 Replica"的粗粒度模型。

**变通方案**：
- Replica 内置 `tab_failures` 和 `browser_failures` 计数器
- 超过 `recreate_threshold` → 删除 tab
- 超过 `restart_threshold` → 主动 raise → Ray 重启 Replica

**遗留问题**：
- 主动 raise 会导致正在处理的其它请求失败
- 需要在 APIGateway 加一层重试

### 6.8 加权调度 (`node_weight`)

- Ray Serve 没有"节点权重"的直接概念
- **变通方案**：用 Ray 的自定义资源 `browser_slot` 数量来模拟（与 v1.x 相同）

### 6.9 共享浏览器代理跟踪

**问题**：现有项目有"shared browser instance"概念，多个请求复用同一个浏览器，代理切换需要协调。

**变通方案**：
- 在 Ray 模型下，"shared browser" 的概念被 BrowserPool Replica 取代
- 每个 Replica 就是一个 shared browser
- 代理状态绑定在 BrowserContext（Playwright）而不是 Browser

**遗留问题**：
- nodriver 模式下做不到 per-context 代理，只能整个 Replica 锁定一个代理

### 6.10 YesCaptcha 异步任务模式

- 与 v1.x 描述一致
- 用 SessionRegistryActor 模拟任务队列
- 需要写胶水代码

### 6.11 配置热加载

- 与 v1.x 描述一致
- Ray Serve `serve.run()` 重新部署有几秒钟服务不可用
- 失去现有的"零停机配置切换"

### 6.12 Standalone 单机模式

- 与 v1.x 描述一致
- `ray.init()` 内嵌模式可以缓解，但无法完全等同于"一个 Python 进程 + SQLite"

### 6.13 Docker 部署的 Xvfb / fluxbox

- 与 v1.x 描述一致
- 需要在 Worker 容器 entrypoint 里手动启动 Xvfb + fluxbox

### 6.14 日志后端切换

**问题**：现有项目支持 SQLite 或 Redis 作为日志后端，可热切换。

**变通方案**：
- 全部统一到 Loki + Ray log streaming
- 失去"开发用 SQLite，生产用 Redis"的灵活性
- 需要 Loki 作为新的强依赖

---

## 7. 迁移工作量评估

> **v2.0 修订**：v1.x 估计"60% 业务逻辑可复用"，本次审计后修正为 **40-50%**。新增的 ConfigResolver / ProxyPoolActor / WarmupActor / 自定义路由层增加了实质工作量。

### 7.1 模块级工作量

| 模块 | v1.x 评估 | v2.0 修订评估 | 风险 |
|------|----------|--------------|------|
| `cluster_manager.py` | 删除 | **删除（约 1200 行）** | 低 |
| `http_bridge.py` | 删除 | **删除** | 低 |
| `session_registry.py` | 重写为 Ray Actor | **重写为 Detached Actor** | 中 |
| `core/database.py` | PG 迁移 | **PG 全量迁移 + Repository 模式** | **高** |
| `core/auth.py` | 几乎不变 | 几乎不变 | 低 |
| `core/config.py` | 几乎不变 | **新增 ConfigResolver 层** | 中 |
| `core/models.py` | 几乎不变 | **拆分 domain models 和 API DTOs** | 中 |
| `api/*.py` | 几乎不变 | 几乎不变（依赖注入方式调整） | 低 |
| `services/captcha_runtime.py` | 简化 | **完全重写为 APIGateway 编排逻辑** | **高** |
| `services/browser_captcha.py` | 改造为 Deployment | **拆分为 `browser/playwright_engine.py` + `ray_app/deployments/browser_pool.py`** | **高** |
| `services/browser_captcha_personal.py` | 同上 | 同上 + Replica 内置故障计数器 | **高** |
| `services/yescaptcha_manager.py` | 简化 | 改造为 Service 层 | 中 |
| **新增**：`ProxyPoolActor` | 没考虑 | **新写约 300 行** | 中 |
| **新增**：`ConfigResolver` | 没考虑 | **新写约 200 行** | 中 |
| **新增**：`WarmupActor` | 没考虑 | **新写约 200 行** | 中 |
| **新增**：自定义三维路由层 | 没考虑 | **新写约 150 行** | **高** |
| **新增**：SolveContext + TimeoutBundle | 没考虑 | **新写约 100 行** | 低 |
| **新增**：Repository 层（多个） | 没考虑 | **新写约 800 行** | 中 |
| Docker 部署 | 完全重写 | **完全重写（Helm Chart + KubeRay）** | **高** |
| Alembic 迁移 | 没考虑 | **新写 15 张表的 schema 迁移** | 中 |
| 测试 | 大量重写 | **大量重写（含 Ray testing utilities）** | **高** |

### 7.2 工作量统计

| 类别 | v1.x 估计 | v2.0 修订估计 |
|------|----------|--------------|
| **可直接复用的代码** | ~60% | **40-50%** |
| **必须重写** | ~30% | **40-50%** |
| **新增代码** | ~10% | **15-20%**（ConfigResolver / ProxyPool / Warmup / 三维路由 / Repository） |

### 7.3 关键风险点（按严重程度排序）

| 风险 | 严重程度 | 缓解措施 |
|------|---------|---------|
| **三维 affinity 在 Ray Serve multiplex 上的语义匹配** | 🔴 高 | 充分原型验证，准备自定义 router fallback |
| **nodriver 代理无法 per-context 切换** | 🔴 高 | Personal 模式需要重新设计代理使用文档 |
| **PostgreSQL 全量迁移** | 🔴 高 | 用 Alembic + 灰度迁移 |
| **WarmupActor 与 BrowserPool 的启动顺序依赖** | 🟡 中 | 显式的依赖注入 + 重试 |
| **配置热更新失去** | 🟡 中 | 文档说明，建议改用 rolling update |
| **测试体系需要 mock Ray** | 🟡 中 | 用 ray.serve.test_utils + testcontainers |
| **Personal 模式重启会丢失正在处理的请求** | 🟡 中 | APIGateway 重试 |
| **Playwright BrowserContext 切换的内存开销** | 🟢 低 | LRU 控制 |

### 7.4 时间估算（**高度依赖团队规模**）

> ⚠️ 这是工作量数量级估算，不是承诺。具体时间因团队 Ray 经验、代码质量要求、测试覆盖度而异。

| 阶段 | 主要任务 | 估算 |
|------|---------|------|
| 阶段 1：基础设施 | PostgreSQL 迁移、Alembic schema、Repository 层 | 数据层重写的大头 |
| 阶段 2：核心抽象 | ConfigResolver、SolveContext、ProxyPoolActor | 全新组件 |
| 阶段 3：浏览器层 | 拆分 browser/ 业务逻辑出 Ray 依赖 | 重要拆分 |
| 阶段 4：Ray Serve 层 | Deployments + Detached Actors + 三维路由 | Ray 学习 + 调试的大头 |
| 阶段 5：API 层 | 复用现有 FastAPI 路由 + 依赖注入调整 | 工作量较小 |
| 阶段 6：部署 | Dockerfile + Helm Chart + KubeRay | 运维相关 |
| 阶段 7：测试 | 单元 + 集成 + e2e + Ray-specific | 测试覆盖率决定时长 |
| 阶段 8：灰度 | 双跑 + 数据校对 + 切流 | 风险控制 |

**保守估算**：3-5 人团队 **3-6 个月**完成全量迁移。

### 7.5 与 v1.x 的对比

| 项 | v1.x 评估 | v2.0 修订 |
|----|----------|----------|
| 业务代码可复用率 | 60% | **40-50%** |
| 新增代码量 | "少量胶水" | **约 1750 行新组件** |
| 关键风险数量 | 3 个 | **8 个** |
| 时间估算 | 不明确 | 3-6 个月 |
| 复杂度评级 | 中等 | **高** |

---

## 8. 收益与成本分析

### 8.1 真实收益

1. ✅ **删除约 2000 行自研集群代码**（cluster_manager + http_bridge + 部分 runtime）
2. ✅ **路由层框架化**：虽然不是"一行装饰器"那么简单，但 `@serve.multiplexed` + 三维 model_id 组合相比现有的 `_dispatch_bucket_affinity` 实现更可控、更可观测
3. ✅ **自动伸缩、自动健康检查、自动重启**全部框架原生
4. ✅ **Ray Dashboard 提供可视化监控**，不用自己写 admin 监控页
5. ✅ **真正的 HA**（Ray Head 双活 + 外部 Redis state）
6. ✅ **配置层显式化**：通过 ConfigResolver + SolveContext 把隐式的全局配置变成显式参数传递，便于测试和调试
7. ✅ **未来扩展到几十个节点完全无压力**

### 8.2 真实成本

1. ❌ **必须迁出 SQLite**，丧失"单文件部署"的便利性
2. ❌ **部署复杂度大幅上升**：用户原来 `docker-compose up` 就能跑，现在需要理解 Ray 集群、KubeRay
3. ❌ **学习曲线陡峭**，团队需要懂 Ray 的 Actor 模型、Serve、autoscaling 调参、multiplex 路由语义
4. ❌ **调试更难**：actor 之间的异步调用栈追踪、Ray Object Store 的内存管理
5. ❌ **额外资源开销**：Ray Head + GCS + Dashboard 即使空载也吃几百 MB 内存
6. ❌ **失去 Python 单进程的简单性**：日志、配置、热更新模型全部要重新设计
7. ❌ **依赖膨胀**：Ray 是个大依赖（200MB+），且对 Python 版本敏感
8. ❌ **新增组件成本**：ConfigResolver / ProxyPoolActor / WarmupActor / 三维路由 / Repository 层共约 1750 行新代码需要从零编写和维护
9. ❌ **nodriver 模式代理使用受限**：Personal Replica 无法 per-context 切换代理，必须按代理分组部署

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

## 12. 新代码库目录结构规划

> 本章描述如果**从头重写**项目时推荐的目录结构。遵循现代 Python 工程实践 + 领域驱动设计 + Ray Serve 项目惯例。

### 12.1 核心设计原则

| 原则 | 说明 |
|------|------|
| **`src/` layout** | 现代 Python 最佳实践，强制通过包安装方式导入，避免 sys.path 污染 |
| **按领域 + 按层混合分组** | 不纯粹按层（如 `models/`、`controllers/`），而是先按领域（captcha/identity/billing），领域内再按层 |
| **领域层独立于框架** | `domain/` 不依赖 FastAPI、Ray、SQLAlchemy，便于单元测试和未来重构 |
| **Ray 代码隔离到独立模块** | `ray_app/` 集中管理 Deployment 和 Actor，不污染业务代码 |
| **基础设施抽象在 `infra/`** | DB、Redis、Ray runtime 的连接和配置统一管理 |
| **测试镜像源码结构** | `tests/` 目录结构与 `src/fcs/` 一一对应，分 unit/integration/e2e |
| **部署与代码分离** | Dockerfile、Helm Chart、k3s manifests 全部放 `deploy/` |
| **配置外部化** | `config/` 存放 TOML，环境变量覆盖 |

### 12.2 完整目录树

```
flow_captcha_service/
│
├── pyproject.toml                  # 项目元数据 + 依赖（取代 requirements.txt）
├── README.md
├── LICENSE
├── .python-version                 # pyenv 锁定 Python 版本
├── .gitignore
├── .dockerignore
├── .pre-commit-config.yaml         # 代码检查 hook
│
├── src/                            # ============ 所有源代码 ============
│   └── fcs/                        # 主包(Flow Captcha Service)
│       ├── __init__.py
│       │
│       ├── core/                   # 横切关注点 / 通用基础
│       │   ├── __init__.py
│       │   ├── config.py           # 配置加载（TOML + ENV）
│       │   ├── logging.py          # 结构化日志配置
│       │   ├── errors.py           # 业务异常类层次
│       │   ├── types.py            # 共享类型别名
│       │   └── constants.py        # 全局常量
│       │
│       ├── infra/                  # ========== 基础设施层 ==========
│       │   ├── __init__.py
│       │   ├── db/
│       │   │   ├── engine.py       # asyncpg 连接池
│       │   │   ├── session.py      # 事务/会话管理
│       │   │   ├── models.py       # SQLAlchemy ORM 模型（可选）
│       │   │   ├── repositories/   # Repository 模式
│       │   │   │   ├── api_keys.py
│       │   │   │   ├── users.py
│       │   │   │   ├── jobs.py
│       │   │   │   ├── quota.py
│       │   │   │   └── cdk.py
│       │   │   └── migrations/     # Alembic
│       │   │       ├── env.py
│       │   │       ├── alembic.ini
│       │   │       └── versions/
│       │   ├── redis/
│       │   │   ├── client.py       # 异步 Redis 客户端
│       │   │   ├── rate_limit.py   # 限流实现
│       │   │   └── cache.py        # 缓存抽象
│       │   └── ray/                # Ray runtime 相关
│       │       ├── handles.py      # Detached actor handles 获取
│       │       ├── config.py       # ray.init / serve.start 配置
│       │       └── lifecycle.py    # Ray 生命周期管理
│       │
│       ├── domain/                 # ========== 领域层（纯业务） ==========
│       │   ├── __init__.py         # 不依赖任何框架，纯 Pydantic/dataclass
│       │   ├── captcha/
│       │   │   ├── models.py       # CaptchaRequest, TokenResult
│       │   │   ├── enums.py        # CaptchaType, CaptchaMethod
│       │   │   └── protocols.py    # CaptchaSolver 接口定义
│       │   ├── identity/
│       │   │   ├── api_key.py      # APIKey 实体
│       │   │   ├── user.py         # User 实体
│       │   │   └── permissions.py
│       │   ├── billing/
│       │   │   ├── quota.py        # Quota 值对象
│       │   │   ├── transaction.py
│       │   │   └── cdk.py          # CDK 兑换码
│       │   └── session/
│       │       ├── state.py        # SessionState 状态机
│       │       └── lifecycle.py
│       │
│       ├── browser/                # ========== 浏览器自动化 ==========
│       │   ├── __init__.py         # 业务逻辑独立于 Ray
│       │   ├── base.py             # BrowserEngine 抽象基类
│       │   ├── playwright_engine.py  # Playwright 实现
│       │   ├── nodriver_engine.py    # nodriver 实现
│       │   ├── fingerprint.py      # UA 指纹池
│       │   ├── proxy.py            # 代理配置
│       │   ├── stealth.py          # 反检测 patches
│       │   └── solvers/            # 各类验证码解决器
│       │       ├── __init__.py
│       │       ├── recaptcha_v2.py
│       │       ├── recaptcha_v3.py
│       │       ├── turnstile.py
│       │       └── flow_native.py
│       │
│       ├── ray_app/                # ========== Ray Serve 应用 ==========
│       │   ├── __init__.py
│       │   ├── entrypoint.py       # serve.run() 主入口
│       │   ├── deployments/
│       │   │   ├── api_gateway.py        # APIGateway Deployment
│       │   │   ├── browser_pool.py       # BrowserPool (Playwright)
│       │   │   └── browser_pool_personal.py  # BrowserPool (nodriver)
│       │   ├── actors/             # Detached Actor 集
│       │   │   ├── token_pool.py         # TokenPoolActor
│       │   │   ├── session_registry.py   # SessionRegistryActor
│       │   │   └── quota_tracker.py      # QuotaTrackerActor
│       │   └── routing/
│       │       ├── multiplex.py    # @serve.multiplexed 封装
│       │       └── affinity.py     # Project affinity 辅助
│       │
│       ├── api/                    # ========== HTTP API 层 ==========
│       │   ├── __init__.py
│       │   ├── app.py              # FastAPI app factory
│       │   ├── deps.py             # 依赖注入（actor handles 等）
│       │   ├── schemas/            # 请求/响应 Pydantic 模型
│       │   │   ├── captcha.py
│       │   │   ├── admin.py
│       │   │   ├── portal.py
│       │   │   └── common.py
│       │   ├── middleware/
│       │   │   ├── auth.py
│       │   │   ├── logging.py
│       │   │   ├── error_handler.py
│       │   │   └── cors.py
│       │   ├── v1/                 # 主 API（版本化）
│       │   │   ├── __init__.py
│       │   │   ├── service.py      # /api/v1/solve, /prefill, /finish
│       │   │   ├── admin.py        # /api/admin/*
│       │   │   ├── portal.py       # /portal/*
│       │   │   └── health.py
│       │   └── compat/             # 第三方协议兼容
│       │       └── yescaptcha.py   # YesCaptcha 协议
│       │
│       ├── services/               # ========== 应用服务层 ==========
│       │   ├── __init__.py         # 用例编排，不直接依赖 HTTP
│       │   ├── captcha_service.py  # 解决验证码用例
│       │   ├── quota_service.py    # 配额扣减/退还
│       │   ├── api_key_service.py
│       │   ├── user_service.py
│       │   └── cdk_service.py
│       │
│       ├── auth/                   # ========== 认证模块 ==========
│       │   ├── __init__.py
│       │   ├── api_key_auth.py     # Bearer fcs_xxx
│       │   ├── admin_auth.py       # Admin Cookie
│       │   ├── portal_auth.py      # Portal Cookie
│       │   ├── password.py         # bcrypt 工具
│       │   └── tokens.py           # token 生成/校验
│       │
│       └── cli/                    # ========== 命令行工具 ==========
│           ├── __init__.py
│           ├── main.py             # 主 CLI 入口（typer/click）
│           ├── admin.py            # 创建管理员/重置密码
│           ├── migrate.py          # alembic 包装
│           └── seed.py             # 初始化测试数据
│
├── tests/                          # ============ 测试代码 ============
│   ├── conftest.py                 # 全局 fixtures
│   ├── fixtures/                   # 测试数据
│   │   └── captcha_pages/
│   ├── unit/                       # 单元测试（无外部依赖）
│   │   ├── domain/
│   │   ├── browser/
│   │   ├── services/
│   │   └── core/
│   ├── integration/                # 集成测试（真实 PG/Redis/Ray）
│   │   ├── api/
│   │   ├── infra/
│   │   ├── ray_app/
│   │   └── browser/
│   └── e2e/                        # 端到端测试
│       ├── test_full_solve_flow.py
│       └── test_yescaptcha_compat.py
│
├── frontend/                       # ============ 前端 UI ============
│   ├── admin/                      # 管理后台静态文件
│   │   ├── index.html
│   │   ├── assets/
│   │   └── ...
│   ├── portal/                     # 用户门户静态文件
│   │   ├── index.html
│   │   └── ...
│   └── shared/                     # 共享资源
│       └── styles/
│
├── deploy/                         # ============ 部署相关 ============
│   ├── docker/
│   │   ├── Dockerfile.head         # Ray head node 镜像
│   │   ├── Dockerfile.worker       # Ray worker 镜像（含浏览器）
│   │   ├── docker-compose.dev.yml  # 本地开发栈
│   │   └── entrypoints/
│   │       ├── head.sh
│   │       └── worker.sh           # Xvfb + Ray worker 启动
│   ├── helm/                       # Helm Chart
│   │   └── flow-captcha/
│   │       ├── Chart.yaml
│   │       ├── values.yaml
│   │       ├── values-prod.yaml
│   │       ├── values-dev.yaml
│   │       └── templates/
│   │           ├── rayservice.yaml
│   │           ├── ingress.yaml
│   │           ├── configmap.yaml
│   │           └── secrets.yaml
│   ├── k3s/                        # k3s 特定脚本和清单
│   │   ├── install.sh              # 一键安装脚本
│   │   ├── bootstrap.md            # 部署文档
│   │   └── manifests/
│   │       ├── kuberay-operator.yaml
│   │       ├── postgres.yaml
│   │       ├── redis.yaml
│   │       └── monitoring.yaml
│   └── ray/                        # Ray Serve 配置
│       ├── serve_config.yaml       # 生产
│       └── serve_config.dev.yaml   # 开发
│
├── config/                         # ============ 应用配置 ============
│   ├── default.toml                # 默认配置（含所有键的默认值）
│   ├── development.toml            # 开发环境覆盖
│   ├── production.toml             # 生产环境覆盖
│   └── schema.json                 # 配置 schema 校验
│
├── scripts/                        # ============ 辅助脚本 ============
│   ├── dev/                        # 开发用
│   │   ├── start_local.sh          # 本地一键启动
│   │   ├── reset_db.sh
│   │   └── load_fixtures.py
│   ├── ops/                        # 运维用
│   │   ├── backup_db.sh
│   │   ├── migrate.sh
│   │   └── rotate_keys.py
│   └── ci/                         # CI 用
│       ├── lint.sh
│       └── build_image.sh
│
├── docs/                           # ============ 文档 ============
│   ├── ARCHITECTURE.md
│   ├── DEPLOY_GUIDE.md
│   ├── DEVELOPMENT.md              # 本地开发指南
│   ├── API.md                      # API 文档
│   ├── RAY_REWRITE_PROPOSAL.md     # 本提案
│   ├── adr/                        # 架构决策记录
│   │   ├── 0001-use-ray-serve.md
│   │   ├── 0002-postgresql.md
│   │   └── 0003-k3s-bootstrap.md
│   └── images/
│
└── .github/                        # ============ CI/CD ============
    └── workflows/
        ├── test.yml
        ├── build.yml
        └── deploy.yml
```

### 12.3 关键模块的设计理由

#### 12.3.1 为什么用 `src/` layout

```python
# 旧的 flat layout
flow_captcha_service/
├── src/
│   ├── api/
│   ├── core/
│   └── ...

# 新的 src/fcs/ layout
flow_captcha_service/
├── src/
│   └── fcs/
│       ├── api/
│       ├── core/
│       └── ...
```

**好处**：
- 强制以 `from fcs.api import ...` 方式导入，避免相对路径混乱
- 必须通过 `pip install -e .` 安装才能 import，防止意外用到未安装的包
- pytest 不会污染 sys.path
- 这是 PyPA 官方推荐的现代布局

#### 12.3.2 `domain/` 层为什么独立

```python
# domain/captcha/models.py
from pydantic import BaseModel
# ❌ 不要 import fastapi, ray, sqlalchemy

class CaptchaRequest(BaseModel):
    project_id: str
    action: str

class TokenResult(BaseModel):
    token: str
    expires_at: int
```

**好处**：
- 单元测试时无需启动任何外部服务
- 未来如果要换框架（比如 FastAPI → Litestar），领域层完全不动
- 业务规则集中，看 `domain/` 就能理解项目做什么

#### 12.3.3 `ray_app/` 为什么单独抽出

```python
# ray_app/deployments/browser_pool.py
from ray import serve
from fcs.browser.playwright_engine import PlaywrightEngine
from fcs.domain.captcha.models import CaptchaRequest, TokenResult

@serve.deployment(...)
class BrowserPoolReplica:
    def __init__(self):
        self.engine = PlaywrightEngine()  # ← 复用 browser/ 模块

    @serve.multiplexed(max_num_models_per_replica=5)
    async def get_warm_page(self, project_id: str):
        return await self.engine.warm_page_for(project_id)

    async def solve(self, req: CaptchaRequest) -> TokenResult:
        return await self.engine.solve(req)
```

**好处**：
- Ray 的 Deployment 装饰器和 Actor 装饰器都集中在一处，便于查找
- `browser/` 模块本身不依赖 Ray，可以脱离 Ray 单独运行（用于测试）
- 未来如果要替换 Ray（比如换成 NATS），只需要重写 `ray_app/`，业务代码不动

#### 12.3.4 `api/` 与 `services/` 的分离

```python
# api/v1/service.py
from fcs.services.captcha_service import CaptchaService
from fcs.api.deps import get_captcha_service

@router.post("/solve")
async def solve(
    request: SolveRequest,
    service: CaptchaService = Depends(get_captcha_service),
):
    # API 层只做参数校验和响应封装
    result = await service.solve(request.to_domain())
    return SolveResponse.from_domain(result)


# services/captcha_service.py
class CaptchaService:
    def __init__(self, browser_pool, token_pool, quota_repo):
        self.browser_pool = browser_pool
        self.token_pool = token_pool
        self.quota_repo = quota_repo

    async def solve(self, req: CaptchaRequest) -> TokenResult:
        # 用例编排:配额检查 → token 池命中 → 浏览器解决
        await self.quota_repo.check(req.api_key)
        if cached := await self.token_pool.get(req.bucket):
            return cached
        return await self.browser_pool.solve.remote(req)
```

**好处**：
- API 层只关心 HTTP 协议（参数解析、状态码、响应格式）
- Service 层只关心业务用例编排
- 同一个 Service 可以被 HTTP API、CLI、消息队列等多个入口复用

#### 12.3.5 `infra/db/repositories/` 仓储模式

```python
# infra/db/repositories/api_keys.py
class APIKeyRepository:
    def __init__(self, session):
        self.session = session

    async def find_by_hash(self, key_hash: str) -> Optional[APIKey]:
        ...

    async def save(self, api_key: APIKey) -> None:
        ...
```

**好处**：
- 数据库访问全部走 Repository，方便测试时 mock
- 切换 ORM（asyncpg ↔ SQLAlchemy）只改 Repository 实现
- 业务代码不直接写 SQL

#### 12.3.6 `frontend/` 与 `src/` 分离

把前端静态文件放到顶层 `frontend/` 而不是 `src/fcs/static/`：
- 前端的构建产物可能很大，不应该和 Python 包绑在一起
- 部署时可以独立打包前端到 CDN 或 Nginx
- 未来前端如果用 React/Vue 改造，有独立的构建流程

### 12.4 新旧文件迁移对照表

| 现有文件 | 新位置 | 备注 |
|---------|-------|------|
| `src/main.py` | `src/fcs/ray_app/entrypoint.py` | Ray Serve 入口 |
| `src/http_bridge.py` | **删除** | Ray Serve 自带 HTTP Proxy |
| `src/core/config.py` | `src/fcs/core/config.py` | 几乎不变 |
| `src/core/database.py` | `src/fcs/infra/db/engine.py` + `repositories/*` | 拆分为多个 Repository |
| `src/core/auth.py` | `src/fcs/auth/*.py` | 按认证类型拆分 |
| `src/core/models.py` | `src/fcs/domain/*/models.py` + `src/fcs/api/schemas/*` | 领域模型和 API DTO 分离 |
| `src/core/log_store.py` | `src/fcs/infra/redis/log_store.py` | 移到基础设施层 |
| `src/api/service.py` | `src/fcs/api/v1/service.py` | API 版本化 |
| `src/api/admin.py` | `src/fcs/api/v1/admin.py` | 同上 |
| `src/api/portal.py` | `src/fcs/api/v1/portal.py` | 同上 |
| `src/api/cluster.py` | **删除** | Ray GCS 接管 |
| `src/api/yescaptcha.py` | `src/fcs/api/compat/yescaptcha.py` | 移到兼容层 |
| `src/services/captcha_runtime.py` | `src/fcs/services/captcha_service.py` | 简化 |
| `src/services/browser_captcha.py` | `src/fcs/browser/playwright_engine.py` + `src/fcs/ray_app/deployments/browser_pool.py` | 业务和 Ray 分离 |
| `src/services/browser_captcha_personal.py` | `src/fcs/browser/nodriver_engine.py` + `src/fcs/ray_app/deployments/browser_pool_personal.py` | 同上 |
| `src/services/cluster_manager.py` | **完全删除**（约 1200 行） | Ray GCS + Serve 接管 |
| `src/services/session_registry.py` | `src/fcs/ray_app/actors/session_registry.py` | 改造为 Ray Actor |
| `src/services/yescaptcha_manager.py` | `src/fcs/services/yescaptcha_service.py` | 简化 |
| `Dockerfile.headed` | `deploy/docker/Dockerfile.worker` | Worker 镜像 |
| `Dockerfile.master` | `deploy/docker/Dockerfile.head` | Head 镜像 |
| `docker-compose.*.yml` | `deploy/docker/docker-compose.dev.yml` | 仅保留开发用 |
| `config/setting_*.toml` | `config/{development,production}.toml` | 标准化命名 |
| `static/admin/*` | `frontend/admin/*` | 前端独立目录 |
| `static/portal/*` | `frontend/portal/*` | 同上 |
| `tests/*` | `tests/{unit,integration,e2e}/*` | 分层测试 |

### 12.5 关键文件示例

#### 12.5.1 `pyproject.toml`

```toml
[project]
name = "flow-captcha-service"
version = "2.0.0"
description = "Self-hosted CAPTCHA solving service powered by Ray Serve"
requires-python = ">=3.11"
dependencies = [
    "ray[serve]==2.9.3",
    "ray[default]==2.9.3",
    "fastapi>=0.110.0",
    "pydantic>=2.6.0",
    "asyncpg>=0.29.0",
    "redis>=5.0.0",
    "alembic>=1.13.0",
    "playwright>=1.40.0",
    "nodriver==0.48.1",
    "httpx>=0.27.0",
    "tenacity>=8.2.0",
    "bcrypt>=4.1.0",
    "typer>=0.9.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "testcontainers[postgresql,redis]>=4.0.0",
    "ruff>=0.3.0",
    "mypy>=1.8.0",
]

[project.scripts]
fcs = "fcs.cli.main:app"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/fcs"]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
```

#### 12.5.2 `src/fcs/ray_app/entrypoint.py`

```python
"""Ray Serve 应用入口"""
from ray import serve
from fcs.core.config import load_config
from fcs.ray_app.actors.token_pool import create_token_pool
from fcs.ray_app.actors.session_registry import create_session_registry
from fcs.ray_app.deployments.api_gateway import APIGateway
from fcs.ray_app.deployments.browser_pool import BrowserPool

def build_app():
    config = load_config()

    # 1. 启动 detached actors（跨 deployment 共享）
    token_pool = create_token_pool()
    session_registry = create_session_registry()

    # 2. 构建 BrowserPool deployment
    browser_pool = BrowserPool.bind()

    # 3. 构建 APIGateway deployment(注入依赖)
    return APIGateway.bind(
        browser_pool=browser_pool,
        token_pool=token_pool,
        session_registry=session_registry,
    )

app = build_app()
```

#### 12.5.3 `config/default.toml`

```toml
[server]
host = "0.0.0.0"
port = 8060

[database]
url = "postgresql+asyncpg://fcs:changeme@localhost:5432/fcs"
pool_size = 20
max_overflow = 10

[redis]
url = "redis://localhost:6379/0"

[ray]
address = "auto"
namespace = "fcs"

[captcha]
default_method = "playwright"  # playwright | nodriver
session_ttl_seconds = 1200
standby_pool_depth = 2

[browser_pool]
min_replicas = 2
max_replicas = 20
target_ongoing_requests = 1
max_warm_pages_per_replica = 5
```

### 12.6 目录结构的演进性

这套结构在不同阶段的演进路径：

#### 起步期（1-2 人开发）
- `domain/`、`infra/`、`services/` 可以保持简单，每个领域只有 1-2 个文件
- 不一定立刻引入 Repository 模式，可以先用直接 SQL

#### 成长期（3-5 人）
- 按领域拆分子模块
- 引入 Repository 模式，便于测试
- 加入 ADR（架构决策记录）文档

#### 成熟期（5+ 人或多团队）
- 可以把 `domain/` + `services/` + 部分 `infra/` 拆成独立的 Python 包
- 用 monorepo 工具（如 uv workspace）管理多个子包
- 前端可能完全独立成另一个 repo

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
