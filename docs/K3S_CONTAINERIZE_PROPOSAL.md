# Flow Captcha Service — k3s + 容器化现状架构改造提案

> 状态：**待审核（Draft）** | 版本：1.0 | 更新日期：2026-04-09
>
> 本文档提出一套**保留现有路由逻辑、用 k3s 替换基础设施层**的渐进式架构改造方案。它是 [`RAY_REWRITE_PROPOSAL.md`](./RAY_REWRITE_PROPOSAL.md) 的对照方案——后者试图同时替换路由层和基础设施层，但因 Ray Serve multiplex 与本项目"高基数三维 affinity"业务模型不匹配而带来真实退化。本提案规避了这个根本性问题，**只替换该替换的部分**。

---

## 目录

1. [背景与动机](#1-背景与动机)
2. [核心思路与设计原则](#2-核心思路与设计原则)
3. [整体架构](#3-整体架构)
4. [改造方案详述](#4-改造方案详述)
5. [与 Ray 方案的对比](#5-与-ray-方案的对比)
6. [工作量评估](#6-工作量评估)
7. [收益与成本分析](#7-收益与成本分析)
8. [分阶段实施路径](#8-分阶段实施路径)
9. [风险与缓解](#9-风险与缓解)
10. [完整技术栈](#10-完整技术栈)
11. [推荐的目标代码结构](#11-推荐的目标代码结构)

---

## 1. 背景与动机

### 1.1 当前架构的痛点

Flow Captcha Service 当前使用自研的 HTTP + SQLite + 心跳轮询方案做集群协调，存在以下痛点：

| 痛点 | 严重程度 | 当前现状 |
|------|---------|---------|
| **Master 单点故障，无 HA** | 🔴 高 | Master 挂了整个集群停摆 |
| **内存状态重启丢失** | 🟡 中 | bucket affinity / reservations 重启失效 |
| **同步 HTTP 在 executor 跑** | 🟡 中 | 不是原生 async，吞吐受限 |
| **自研集群代码维护成本** | 🟡 中 | cluster_manager 约 1200 行 |
| **无主动探活** | 🟢 低 | Subnode 假死时检测延迟到 120s |
| **固定 350ms 重试** | 🟢 低 | 没有指数退避 |

### 1.2 为什么放弃 Ray 方案

经过对 [`RAY_REWRITE_PROPOSAL.md`](./RAY_REWRITE_PROPOSAL.md) v2.0 的完整审查，发现 Ray Serve 的 multiplexed routing 与本项目的业务模型存在**根本性不匹配**：

- **本项目的 bucket affinity key 是三维**：`(project_id, action, proxy_signature)`
- **Ray Serve multiplex 是为低基数高频访问设计的**：典型用例是 50 个 ML 模型
- **本项目可能产生几百到几千个 model_id**：项目数 × action 数 × 代理数的乘法爆炸
- **结果**：Ray Serve 的 LRU 淘汰机制会导致严重 thrashing，需要 5-10 倍的 Replica 数量来缓解

详细分析见 `RAY_REWRITE_PROPOSAL.md` 第 5、6 章。

### 1.3 关键洞察

> **本项目的 3D affinity + per-token 代理覆盖 是非常精巧、贴合业务的设计。它不应该被替换，应该被保留。需要替换的是周边的基础设施层（HA、服务发现、健康检查、节点重启）。**

| 应该保留的（项目最有价值的资产） | 应该替换的（重复造轮子的部分） |
|---------------------------|---------------------------|
| 三维 bucket affinity 路由 | 自研 HTTP 注册接口 |
| Per-token 代理覆盖 | 自研心跳轮询 |
| Standby Token Pool | 自研失活检测 |
| 项目亲和路由（项目 + action + 代理） | 自研重启逻辑 |
| 槽位预留机制 | 自研日志聚合 |
| 7 个 0=auto 配置自动派生 | Master 单点架构 |

### 1.4 解决方案

**用 k3s 接管基础设施层，让现有的 cluster_manager 路由逻辑继续运行。**

k3s 提供的能力恰好覆盖了这些痛点：

| 现有痛点 | k3s 提供的解决方案 |
|---------|------------------|
| Master 单点 | **K8s Lease 选主**（Active-Standby） |
| 服务发现自研 | **K8s Service + Endpoints**（自动） |
| 心跳轮询 | **livenessProbe / readinessProbe**（原生） |
| 失活节点剔除 | K8s 自动 |
| 节点重启 | K8s Pod 自动重启 |
| 错误日志 | K8s events + 标准日志 |
| 滚动升级 | K8s rolling update |
| 资源监控 | K8s Dashboard + Prometheus |

---

## 2. 核心思路与设计原则

| 原则 | 说明 |
|------|------|
| **关注点分离** | 基础设施层（k3s 接管） vs 业务路由层（保留现有 cluster_manager） |
| **增量演进** | 小步迁移，每一步可独立验证、可回滚 |
| **K8s 原生** | 用 K8s 标准能力（Lease / Service / Probes）替代自研机制 |
| **路由逻辑零改动** | 三维 affinity / per-token 代理 / Standby Pool 完整保留 |
| **数据层迁移作为前置任务** | SQLite → PostgreSQL 是 HA 的基础，但与 k3s 改造解耦 |
| **代码净减少** | 删除约 600 行自研基础设施逻辑，新增约 400 行 K8s 集成 |
| **未来兼容** | k3s 100% K8s API 兼容，未来可无缝迁移到 EKS/GKE |

---

## 3. 整体架构

### 3.1 拓扑图

```
┌────────────────────────────────────────────────────────────┐
│              k3s 集群 (单节点 4-8GB 起步)                    │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │   Master Deployment (replicas=2, Active-Standby)      │  │
│  │   ┌──────────────┐   ┌──────────────┐                │  │
│  │   │  Master 1    │   │  Master 2    │                │  │
│  │   │  ACTIVE      │   │  STANDBY     │                │  │
│  │   │  (持有 Lease)│   │  (等待 Lease) │                │  │
│  │   │              │   │              │                │  │
│  │   │ - cluster_   │   │ - cluster_   │                │  │
│  │   │   manager    │   │   manager    │                │  │
│  │   │ - 3D 路由     │   │   (待激活)   │                │  │
│  │   │ - Token Pool │   │              │                │  │
│  │   │ - APIs       │   │              │                │  │
│  │   └──────┬───────┘   └──────────────┘                │  │
│  │          │                                            │  │
│  │   K8s Lease Object (coordination.k8s.io/v1)          │  │
│  └──────────┼───────────────────────────────────────────┘  │
│             │                                              │
│             │ 通过 K8s API 发现 Subnodes                    │
│             ▼                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │   Subnode StatefulSet (replicas=N)                    │  │
│  │   ┌────────────┐  ┌────────────┐  ┌────────────┐    │  │
│  │   │ Subnode-0  │  │ Subnode-1  │  │ Subnode-2  │    │  │
│  │   │            │  │            │  │            │    │  │
│  │   │ Browsers×2 │  │ Browsers×4 │  │ Browsers×4 │    │  │
│  │   │ + Xvfb     │  │ + Xvfb     │  │ + Xvfb     │    │  │
│  │   │ + fluxbox  │  │ + fluxbox  │  │ + fluxbox  │    │  │
│  │   │            │  │            │  │            │    │  │
│  │   │ readiness  │  │ readiness  │  │ readiness  │    │  │
│  │   │ Probe ✓    │  │ Probe ✓    │  │ Probe ✓    │    │  │
│  │   └────────────┘  └────────────┘  └────────────┘    │  │
│  │   Headless Service: subnode.fcs.svc.cluster.local    │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                            │
│  ┌────────────────────┐  ┌────────────────────┐            │
│  │  PostgreSQL        │  │  Redis             │            │
│  │  StatefulSet       │  │  StatefulSet       │            │
│  │  (or 外部托管)      │  │  (可选)            │            │
│  └────────────────────┘  └────────────────────┘            │
└────────────────────────────────────────────────────────────┘
              │
              │ Traefik Ingress (k3s 内置)
              ▼
       客户端 HTTP 请求
```

### 3.2 关键架构变化

相比当前的"自研 HTTP 注册 + SQLite + 心跳"架构：

| 组件 | 当前 | k3s 方案 |
|------|------|---------|
| Master 数量 | 1 个进程 | **2 个 Pod（Active-Standby）** |
| 服务发现 | HTTP `POST /register` | **K8s Service + Endpoints** |
| 心跳 | 15s HTTP heartbeat | **K8s readinessProbe** |
| 失活检测 | 120s 心跳超时 | **K8s 自动**（probe 失败即剔除） |
| Master 故障转移 | 无 | **Lease 过期 → Standby 接管（5-10s）** |
| 节点重启 | 无 | **K8s Pod 自动重启** |
| 数据存储 | SQLite 本地文件 | **PostgreSQL（共享）** |
| 状态持久化 | 内存（重启丢失） | **PostgreSQL + 内存缓存** |
| 部署 | docker-compose | **Helm Chart** |
| 监控 | 自研 admin 页面 | **K8s Dashboard + Prometheus + Grafana** |
| 路由逻辑 | cluster_manager 1200 行 | **保留**，仅 200 行精简（去掉网络层） |

---

## 4. 改造方案详述

### 4.1 服务发现：HTTP 注册 → K8s Service + Endpoints

#### 当前实现

```python
# Subnode 启动时调用
POST /api/cluster/register
Headers: X-Cluster-Key: ...
Body: { node_name, base_url, weight, max_concurrency, ... }
```

#### k3s 方案

利用 K8s 的 **Headless Service**：每个 Subnode Pod 自动出现在 Service 的 Endpoints 列表里。

```yaml
# Helm template
apiVersion: v1
kind: Service
metadata:
  name: fcs-subnode-headless
spec:
  clusterIP: None  # Headless
  selector:
    app: fcs-subnode
  ports:
  - port: 8060
    name: http
```

```python
# Master 内的服务发现 (替代 HTTP 注册接口)
# fcs/cluster/discovery.py
from kubernetes import client, config

class K8sDiscovery:
    def __init__(self):
        config.load_incluster_config()
        self.v1 = client.CoreV1Api()

    async def list_subnodes(self) -> list[NodeInfo]:
        endpoints = self.v1.read_namespaced_endpoints(
            name="fcs-subnode-headless",
            namespace="fcs",
        )
        nodes = []
        for subset in endpoints.subsets or []:
            for addr in subset.addresses or []:
                nodes.append(NodeInfo(
                    name=addr.target_ref.name,  # Pod name
                    base_url=f"http://{addr.ip}:8060",
                    weight=int(addr.target_ref.labels.get("weight", "100")),
                    healthy=True,  # K8s 已经过滤了不健康的
                ))
        return nodes

    async def watch_subnodes(self):
        """监听 endpoint 变化,实时更新节点列表"""
        w = watch.Watch()
        for event in w.stream(self.v1.list_namespaced_endpoints, namespace="fcs"):
            yield event
```

**收益**：
- ✅ 删除 `register_node`、`upsert_cluster_node` 等约 200 行代码
- ✅ K8s 保证服务发现的实时性（Pod 上线/下线立即生效）
- ✅ 自动跨节点

### 4.2 健康检查：心跳轮询 → K8s Probes

#### 当前实现

```python
# Subnode 每 15s 上报
POST /api/cluster/heartbeat
Body: { active_sessions, available_slots, standby_buckets, healthy }

# Master 端查询时过滤
WHERE last_heartbeat_at >= datetime('now', '-120 seconds')
```

#### k3s 方案

```yaml
# Subnode StatefulSet
spec:
  template:
    spec:
      containers:
      - name: fcs-subnode
        readinessProbe:
          httpGet:
            path: /api/cluster/local/health
            port: 8060
          periodSeconds: 5
          failureThreshold: 3   # 15s 内 3 次失败 → 标记 unhealthy
        livenessProbe:
          httpGet:
            path: /api/cluster/local/liveness
            port: 8060
          periodSeconds: 30
          failureThreshold: 3   # 90s 不响应 → 重启 Pod
```

```python
# Subnode 暴露的健康端点(替代心跳)
@router.get("/api/cluster/local/health")
async def local_health():
    """K8s readinessProbe 调用"""
    runtime = get_captcha_runtime()
    return {
        "healthy": runtime.is_healthy(),
        "active_sessions": runtime.active_count(),
        "available_slots": runtime.available_slots(),
        "standby_buckets": runtime.standby_signatures(),
    }

@router.get("/api/cluster/local/liveness")
async def local_liveness():
    """K8s livenessProbe 调用,仅检查进程存活"""
    return {"alive": True}
```

**关键变化**：
- 不再向 Master "推送" 心跳
- Master 通过 K8s API 拉取 endpoint + 主动调用 `local/health` 获取实时状态
- 失活检测从 "120s 超时" 变成 "15s probe 失败"

**收益**：
- ✅ 删除心跳上报循环（约 150 行）
- ✅ 失活检测速度从 120s → 15s（提升 8 倍）
- ✅ Pod 假死自动重启（livenessProbe）

### 4.3 Master HA：无 → K8s Lease 选主

#### 当前现状

Master 是单点，挂了整个集群停摆。

#### k3s 方案

使用 Kubernetes 的 **Lease 对象**实现 Active-Standby 选主：

```python
# fcs/cluster/leader_election.py
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import asyncio
import time
import os
import socket

class K8sLeaderElection:
    def __init__(self, lease_name="fcs-master-lease", namespace="fcs"):
        config.load_incluster_config()
        self.coordination = client.CoordinationV1Api()
        self.lease_name = lease_name
        self.namespace = namespace
        self.identity = f"{socket.gethostname()}-{os.getpid()}"
        self.lease_duration = 15  # 秒
        self.renew_interval = 5
        self.is_leader = False

    async def run(self, on_become_leader, on_lose_leadership):
        """主循环:不停尝试获取或续约 lease"""
        while True:
            try:
                acquired = await self._try_acquire_or_renew()
                if acquired and not self.is_leader:
                    self.is_leader = True
                    await on_become_leader()
                elif not acquired and self.is_leader:
                    self.is_leader = False
                    await on_lose_leadership()
            except Exception as e:
                logger.error(f"leader election error: {e}")
                if self.is_leader:
                    self.is_leader = False
                    await on_lose_leadership()
            await asyncio.sleep(self.renew_interval)

    async def _try_acquire_or_renew(self) -> bool:
        try:
            lease = self.coordination.read_namespaced_lease(
                name=self.lease_name, namespace=self.namespace,
            )
            holder = lease.spec.holder_identity
            renew_time = lease.spec.renew_time

            # 我是当前持有者 → 续约
            if holder == self.identity:
                lease.spec.renew_time = datetime.now(timezone.utc)
                self.coordination.replace_namespaced_lease(...)
                return True

            # 别人持有但已过期 → 抢
            if renew_time and (datetime.now(timezone.utc) - renew_time).total_seconds() > self.lease_duration:
                lease.spec.holder_identity = self.identity
                lease.spec.renew_time = datetime.now(timezone.utc)
                self.coordination.replace_namespaced_lease(...)
                return True

            return False  # 别人持有且仍有效
        except ApiException as e:
            if e.status == 404:
                # Lease 不存在 → 创建并成为 leader
                self._create_lease()
                return True
            raise


# Master 启动逻辑
async def main():
    election = K8sLeaderElection()
    runtime = CaptchaRuntime(...)

    async def on_become_leader():
        logger.info("became master leader")
        await runtime.start_master_loops()  # 启动调度循环、Token 池补充等

    async def on_lose_leadership():
        logger.info("lost master leadership")
        await runtime.stop_master_loops()

    await election.run(on_become_leader, on_lose_leadership)
```

```yaml
# RBAC: Master 需要 Lease 权限
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: fcs-master
  namespace: fcs
rules:
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "list", "create", "update", "patch", "delete"]
- apiGroups: [""]
  resources: ["endpoints", "pods"]
  verbs: ["get", "list", "watch"]
```

**关键设计点**：
- Lease duration 15s, renew interval 5s
- Active Master 每 5s 续约
- Active 挂掉后,15s 内 lease 过期, Standby 抢到 lease 立即接管
- **故障转移时间 ≤ 15s**

**收益**：
- ✅ Master HA(15s 内自动故障转移)
- ✅ 不引入额外的外部协调服务(用 k3s 自带的 etcd)
- ✅ 标准 K8s 模式,kubectl 可直接观察

### 4.4 节点失活检测:自动化

K8s 的 readinessProbe 失败 → endpoint 自动剔除 → Master 通过 watch 立即感知。

无需写代码,这是 K8s 原生行为。

### 4.5 数据层:SQLite → PostgreSQL

这是**必须做的前置任务**,因为多 Master 必须共享状态。

#### 改造范围

| 表 | 用途 | 改造 |
|----|------|------|
| `service_admin` | 管理员 | schema 适配 |
| `service_api_keys` | API Key | schema 适配 |
| `portal_users` | 用户 | schema 适配 |
| `portal_user_api_keys` | 用户 Key | schema 适配 |
| `captcha_jobs` | Job 历史 | schema 适配 + 索引优化 |
| `session_quota_events` | 配额事件 | schema 适配 |
| `portal_cdks` | CDK 兑换码 | schema 适配 |
| `portal_user_transactions` | 交易记录 | schema 适配 |
| `cluster_nodes` | **删除**(K8s 接管) | 删除 |
| `cluster_node_heartbeats` | **删除** | 删除 |
| `cluster_node_errors` | 错误日志 | 改为写 K8s events 或保留 |
| 其它 4 张表 | ... | schema 适配 |

#### 实现路径

```python
# fcs/infra/db/engine.py
import asyncpg

class Database:
    async def connect(self, dsn: str):
        self.pool = await asyncpg.create_pool(
            dsn,
            min_size=5,
            max_size=20,
            command_timeout=60,
        )

# fcs/infra/db/repositories/api_keys.py
class APIKeyRepository:
    def __init__(self, db: Database):
        self.db = db

    async def find_by_hash(self, key_hash: str) -> Optional[APIKey]:
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM service_api_keys WHERE key_hash = $1",
                key_hash,
            )
            return APIKey.from_row(row) if row else None
```

**工作量**：约 1-2 周(15 张表的 schema 改写 + 所有 SQL 方言适配 + Repository 模式包装)。

**与 Ray 方案相同**：这是 HA 架构的前置成本,与具体框架无关。

### 4.6 路由层：保留(0 改动)

**这是本方案的核心收益**：

| 模块 | 改动 |
|------|------|
| 三维 bucket affinity 路由 | **0 改动** |
| Per-token 代理覆盖 | **0 改动** |
| Standby Token Pool | **0 改动** |
| 槽位预留机制 | **0 改动** |
| 7 个 0=auto 配置自动派生 | **0 改动** |
| 浏览器引擎管理(Playwright + nodriver) | **0 改动** |
| 67 个配置项 | **0 改动** |
| 10 层超时配置 | **0 改动** |
| 重试与故障恢复 | **0 改动** |

`cluster_manager.py` 中只删除以下部分(共约 600 行):

| 删除模块 | 行数 |
|---------|-----|
| `register_node` / `upsert_cluster_node` | ~200 |
| `_send_subnode_heartbeat` 循环 | ~150 |
| `heartbeat_cluster_node` 处理 | ~100 |
| `get_available_cluster_nodes` SQL 查询 | ~50 |
| 节点错误日志写入 | ~50 |
| 网络层胶水代码(`_sync_json_http_request` 等) | ~50 |

**保留约 600 行**(三维路由、槽位预留、bucket affinity、Token 池调度)。

### 4.7 Dockerfile 改造

现有的 `Dockerfile.headed` 已经接近可用,只需小幅调整：

```dockerfile
# deploy/docker/Dockerfile.subnode
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_BROWSERS_PATH=0 \
    DISPLAY=:99 \
    XVFB_WHD=1920x1080x24

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl procps xvfb x11-utils fluxbox \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml requirements.txt /app/
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m playwright install --with-deps chromium

COPY src/ /app/src/
COPY config/ /app/config/

# K8s 健康检查端点(替代心跳)
EXPOSE 8060

COPY deploy/docker/entrypoints/subnode.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
```

```bash
#!/bin/bash
# deploy/docker/entrypoints/subnode.sh
set -e

# 启动 Xvfb
Xvfb $DISPLAY -screen 0 $XVFB_WHD -ac +extension RANDR +render -nolisten tcp &
fluxbox >/tmp/fluxbox.log 2>&1 &

# 等待 X server 就绪
sleep 1

# 启动 Subnode 进程
exec python -m fcs.cli.main subnode-server
```

```dockerfile
# deploy/docker/Dockerfile.master
FROM python:3.11-slim
# Master 不需要浏览器,镜像更小
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml requirements.txt /app/
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/
COPY config/ /app/config/

EXPOSE 8060
CMD ["python", "-m", "fcs.cli.main", "master-server"]
```

### 4.8 Helm Chart

```
deploy/helm/flow-captcha/
├── Chart.yaml
├── values.yaml
├── values-dev.yaml
├── values-prod.yaml
└── templates/
    ├── master-deployment.yaml      # Master Deployment (replicas=2)
    ├── master-rbac.yaml            # Lease 权限
    ├── master-service.yaml         # Master ClusterIP Service
    ├── subnode-statefulset.yaml    # Subnode StatefulSet
    ├── subnode-headless-svc.yaml   # Subnode Headless Service
    ├── postgres-statefulset.yaml   # PostgreSQL (可选,可外部托管)
    ├── redis-statefulset.yaml      # Redis (可选)
    ├── ingress.yaml                # Traefik Ingress
    ├── configmap.yaml              # 应用配置
    ├── secrets.yaml                # 敏感配置
    └── _helpers.tpl
```

关键 manifest 示例：

```yaml
# templates/master-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fcs-master
spec:
  replicas: 2  # Active-Standby
  selector:
    matchLabels:
      app: fcs-master
  template:
    metadata:
      labels:
        app: fcs-master
    spec:
      serviceAccountName: fcs-master
      containers:
      - name: master
        image: fcs/master:{{ .Values.image.tag }}
        env:
        - name: FCS_CLUSTER_ROLE
          value: "master"
        - name: FCS_DB_DSN
          valueFrom:
            secretKeyRef:
              name: fcs-secrets
              key: db-dsn
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        readinessProbe:
          httpGet:
            path: /api/cluster/local/health
            port: 8060
        livenessProbe:
          httpGet:
            path: /api/cluster/local/liveness
            port: 8060
```

```yaml
# templates/subnode-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: fcs-subnode
spec:
  serviceName: fcs-subnode-headless
  replicas: {{ .Values.subnode.replicas }}
  selector:
    matchLabels:
      app: fcs-subnode
  template:
    metadata:
      labels:
        app: fcs-subnode
        weight: "{{ .Values.subnode.weight }}"
    spec:
      containers:
      - name: subnode
        image: fcs/subnode:{{ .Values.image.tag }}
        env:
        - name: FCS_CLUSTER_ROLE
          value: "subnode"
        - name: FCS_BROWSER_COUNT
          value: "{{ .Values.subnode.browserCount }}"
        readinessProbe:
          httpGet:
            path: /api/cluster/local/health
            port: 8060
          periodSeconds: 5
          failureThreshold: 3
        livenessProbe:
          httpGet:
            path: /api/cluster/local/liveness
            port: 8060
          periodSeconds: 30
          failureThreshold: 3
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2"
```

---

## 5. 与 Ray 方案的对比

| 维度 | Ray 方案 | **k3s + 容器化方案** |
|------|---------|-------------------|
| **核心思路** | 同时替换路由层和基础设施层 | **只替换基础设施层** |
| **3D affinity 路由** | ❌ Ray multiplex 高基数下退化 | ✅ **完全保留** |
| **per-token 代理覆盖** | 🟡 需要 ProxyPoolActor + SolveContext 重写 | ✅ **完全保留** |
| **配置自动派生** | 🟡 需要 ConfigResolver 层 | ✅ **完全保留** |
| **7 个 0=auto 配置** | 🟡 需要后处理层 | ✅ **完全保留** |
| **Standby Token Pool** | 🟡 需要 TokenPoolActor 重写 | ✅ **完全保留** |
| **WarmupActor** | 🟡 需要新写约 200 行 | ✅ **现有逻辑不变** |
| **Personal 模式重启** | 🔴 in-flight 请求丢失 | ✅ **现有粒度恢复保留** |
| **学习成本** | 🔴 Ray + Serve + multiplex + actor | 🟡 K8s 基础(更通用) |
| **业务代码可复用率** | 40-50% | **80-90%** |
| **新增代码量** | ~1750 行 | **~400 行** |
| **删除代码量** | ~2000 行 | **~600 行** |
| **净代码变化** | +约 250 行 | **-约 200 行** |
| **时间估算** | 3-6 个月 | **4-6 周** |
| **回滚难度** | 🔴 大改造,难回滚 | 🟢 **可分阶段回滚** |
| **未来升级到完整 K8s** | ✅ 无缝 | ✅ **无缝** |
| **依赖膨胀** | Ray + KubeRay + PG + Redis + Loki | k3s + PG + Redis |
| **资源占用** | ~3 GB(Ray Head 占大头) | ~1.5 GB |

**关键差异**：

> Ray 方案的失败点在于试图用 `@serve.multiplexed` 接管路由,但这恰好是项目最复杂的部分。k3s 方案让 K8s 接管基础设施(它擅长的部分),把路由留给现有代码(它已经写得很好的部分)。

---

## 6. 工作量评估

### 6.1 模块级估算

| 模块 | 改动 | 工作量 | 风险 |
|------|------|--------|------|
| `cluster_manager.py` 路由部分 | **保留** | 0 | 低 |
| `cluster_manager.py` 网络层 | 删除 ~600 行 | 1 天 | 低 |
| 服务发现 (`fcs/cluster/discovery.py`) | 新写 K8s API 客户端 | 2-3 天 | 中 |
| Master 选主 (`fcs/cluster/leader_election.py`) | 新写 Lease 选主 | 2-3 天 | 中 |
| 健康端点 (`fcs/api/v1/health.py`) | 新写 local/health + local/liveness | 半天 | 低 |
| `cluster_manager.py` 集成选主 | 启动时等待成为 leader 才激活 | 1-2 天 | 中 |
| `core/database.py` | **PG 迁移** | **1-2 周** | **高** |
| `core/auth.py` | 几乎不变 | 0 | 低 |
| `api/*.py` | 路径不变,依赖注入微调 | 1-2 天 | 低 |
| `services/browser_captcha.py` | 不变 | 0 | 低 |
| `services/browser_captcha_personal.py` | 不变 | 0 | 低 |
| `services/captcha_runtime.py` | 不变 | 0 | 低 |
| Dockerfile.master | 新写(轻量) | 半天 | 低 |
| Dockerfile.subnode | 现有 Dockerfile.headed 改造 | 1 天 | 低 |
| Helm Chart | 新写 | 3-5 天 | 中 |
| RBAC YAML | 新写 | 半天 | 低 |
| 测试 | K8s 集成测试 | 1 周 | 中 |
| 文档 | 部署文档 + 运维手册 | 3-5 天 | 低 |
| 灰度切换 | 双跑 + 流量切换 | 1 周 | 中 |

**总估算**:**4-6 周一个人**(主要是 PostgreSQL 迁移)。

### 6.2 工作量统计

| 类别 | 数值 |
|------|------|
| **保留的代码** | ~80-90%(路由、Token 池、浏览器、配置、API、认证) |
| **删除的代码** | ~600 行(集群网络层) |
| **新增的代码** | ~400 行(K8s discovery + leader election + health endpoints) |
| **净代码变化** | **-约 200 行** |
| **PostgreSQL 迁移** | ~1500 行 SQL 改写 + Repository 包装 |
| **Helm Chart** | ~500 行 YAML |

---

## 7. 收益与成本分析

### 7.1 真实收益

1. ✅ **Master HA**(15s 故障转移),不再担心单点
2. ✅ **保留所有路由优化**:三维 affinity / per-token 代理 / Standby Pool 完整不动
3. ✅ **失活检测从 120s 缩短到 15s**(K8s probe 频率提升 8 倍)
4. ✅ **节点自动重启**(K8s 原生)
5. ✅ **滚动升级**(K8s rolling update,零停机)
6. ✅ **服务发现自动化**(删除 200 行 HTTP 注册逻辑)
7. ✅ **代码净减少 200 行**(删除 600 + 新增 400)
8. ✅ **未来无缝迁移到完整 K8s**(EKS / GKE / ACK)
9. ✅ **可观测性提升**(K8s Dashboard + Prometheus + Grafana)
10. ✅ **比 Ray 方案省 3-4 倍工作量**(4-6 周 vs 3-6 月)

### 7.2 真实成本

1. ❌ **必须迁出 SQLite**(与 Ray 方案相同的前置成本)
2. ❌ **必须引入 Kubernetes**(虽然是轻量的 k3s)
3. ❌ **团队需要懂基本 K8s**(kubectl, Helm, Pod, Service, Probe)
4. ❌ **资源占用上升**:从 ~500MB(单进程)到 ~3GB(完整集群)
5. ❌ **部署复杂度上升**:从 `python -m src.main` 到 `helm install`
6. ❌ **需要新写约 400 行 K8s 集成代码**
7. ❌ **测试复杂度上升**(需要 testcontainers + k3s 集成测试)
8. ❌ **PostgreSQL 迁移工作量大**(约占总工作量的 30%)

### 7.3 收益/成本对比

| 维度 | 现状 | k3s 方案 |
|------|------|---------|
| **Master 故障影响** | 全集群停摆 | 15s 自动恢复 |
| **失活检测速度** | 120s | 15s |
| **节点崩溃恢复** | 需要人工介入 | 自动重启 |
| **代码维护成本** | 1200 行 cluster_manager | 600 行核心 + 400 行 K8s 集成 |
| **学习曲线** | 0(已掌握) | 1 周 K8s 基础 |
| **运维复杂度** | 低(单进程) | 中(容器编排) |

---

## 8. 分阶段实施路径

> **核心原则**:每个阶段独立可验证,可回滚,降低整体风险。

### 阶段 1:容器化(第 1 周)

**目标**:把现有架构原样打包成 Docker 镜像。

- 写 `Dockerfile.master` 和 `Dockerfile.subnode`
- 写 `docker-compose.yml`(用于本地开发)
- 验证容器内能正常启动 Master 和 Subnode
- **不动任何代码逻辑**

**验收**:`docker-compose up` 可启动完整集群,行为与现在一致。

### 阶段 2:PostgreSQL 迁移(第 2-3 周)

**目标**:把 SQLite 替换为 PostgreSQL。

- 写 Alembic schema migration
- 把 `core/database.py` 改造为 asyncpg + Repository 模式
- 改写所有 SQL 方言
- 跑全量测试
- **仍然在 docker-compose 上跑**,不引入 K8s

**验收**:所有现有测试通过,数据库换成 PG。

### 阶段 3:K8s 部署(单 Master,第 4 周前半)

**目标**:把容器化好的镜像部署到 k3s 上。

- 安装 k3s
- 写 Helm Chart(Master replicas=1, Subnode StatefulSet)
- `helm install` 部署
- **此时 Master 仍是单点**,但已经在 k3s 上运行
- Subnode 通过 K8s Service 暴露,但 Master 还是用现有的 HTTP 注册接口

**验收**:`helm install` 成功,API 可用,行为与 docker-compose 一致。

### 阶段 4:服务发现迁移(第 4 周后半)

**目标**:用 K8s API 替代 HTTP 注册接口。

- 新写 `fcs/cluster/discovery.py`(K8s API client)
- Master 改为通过 K8s API 发现 Subnodes
- **删除 `register_node` 接口**(但保留兼容,Subnode 不再调用)
- Subnode 不再发送 register 请求

**验收**:Master 通过 K8s 看到所有 Subnode,路由正常。

### 阶段 5:健康检查迁移(第 5 周前半)

**目标**:用 K8s probes 替代心跳轮询。

- 新写 `local/health` 和 `local/liveness` 端点
- 在 StatefulSet manifest 里配置 probes
- **删除心跳上报循环**(`_send_subnode_heartbeat`)
- **删除 Master 端的失活检测 SQL 查询**
- Master 改为通过 K8s endpoint watch 实时获取节点健康状态

**验收**:杀死一个 Subnode Pod,K8s 自动重启,Master 在 15s 内感知。

### 阶段 6:Master HA(第 5 周后半)

**目标**:启用 Active-Standby 选主。

- 新写 `fcs/cluster/leader_election.py`(K8s Lease)
- 新写 RBAC YAML(Lease 权限)
- Master Deployment replicas=2
- 启动逻辑:等待成为 leader 才激活路由循环

**验收**:杀死 Active Master,15s 内 Standby 接管,API 不中断。

### 阶段 7:灰度切换(第 6 周)

**目标**:生产环境渐进式切流。

- 新旧集群并行跑
- 用 Ingress 做流量分割(10% → 50% → 100%)
- 监控错误率、延迟、token 命中率
- 观察 24-48 小时
- 完全切流后,关闭旧集群

**验收**:生产流量稳定,无回归。

### 阶段 8:旧代码清理(第 6 周末)

**目标**:删除所有不再使用的代码。

- 删除 `cluster_manager.py` 里的网络层(register/heartbeat 处理)
- 删除 `api/cluster.py`(已被 K8s 接管)
- 删除 `cluster_nodes` / `cluster_node_heartbeats` 表
- 删除 `http_bridge.py`(可选)
- 更新文档

**验收**:`grep -r "heartbeat" src/` 应该返回空。

---

## 9. 风险与缓解

### 9.1 技术风险

| 风险 | 严重程度 | 缓解措施 |
|------|---------|---------|
| **PostgreSQL 迁移引入 SQL 方言 bug** | 🔴 高 | 完整测试覆盖 + 灰度切换 |
| **K8s Lease 选主出现脑裂** | 🟡 中 | Lease duration 设置合理(15s),应用层加 sanity check |
| **Master 重启导致内存状态丢失**(bucket affinity) | 🟡 中 | 持久化 affinity 到 PostgreSQL |
| **K8s API 速率限制** | 🟢 低 | 用 watch 模式而不是 poll |
| **readinessProbe 抖动** | 🟡 中 | 调高 failureThreshold 和 periodSeconds |
| **Helm Chart 模板出错** | 🟢 低 | helm lint + 干跑测试 |
| **PostgreSQL 单点** | 🟡 中 | 用云托管 RDS 或部署 PG HA |

### 9.2 运维风险

| 风险 | 缓解 |
|------|------|
| 团队不熟悉 K8s | 1 周培训 + 内部文档 |
| 监控告警没跟上 | 部署 Prometheus + Grafana 模板 |
| 故障排查变难 | 准备 kubectl 速查手册 |
| 资源不够 | 提前压测,预留 30% buffer |

### 9.3 业务风险

| 风险 | 缓解 |
|------|------|
| 灰度期间数据不一致 | 双写 + 校对脚本 |
| 切流后性能下降 | 准备快速回滚预案 |
| K8s 升级影响业务 | 先在 staging 验证,生产分时段升级 |

---

## 10. 完整技术栈

### 10.1 技术栈总览

```
┌──────────────────────────────────────────────────────┐
│  Layer 7: 部署 / 运维                                 │
│  k3s + Helm                                           │
├──────────────────────────────────────────────────────┤
│  Layer 6: 可观测性                                    │
│  K8s Dashboard + Prometheus + Grafana (可选 Loki)     │
├──────────────────────────────────────────────────────┤
│  Layer 5: 集群协调                                    │
│  K8s Service + Endpoints + Lease + Probes             │
│  (替代自研 cluster_manager 网络层)                     │
├──────────────────────────────────────────────────────┤
│  Layer 4: Web / API                                   │
│  FastAPI + Pydantic v2 + Uvicorn                      │
├──────────────────────────────────────────────────────┤
│  Layer 3: 业务逻辑                                    │
│  Playwright + nodriver (完全保留)                      │
│  3D affinity 路由 (完全保留)                           │
│  Token Pool (完全保留)                                 │
├──────────────────────────────────────────────────────┤
│  Layer 2: 数据层                                      │
│  PostgreSQL + Redis + asyncpg + redis-py              │
├──────────────────────────────────────────────────────┤
│  Layer 1: 运行时                                      │
│  Python 3.11+ / asyncio                               │
└──────────────────────────────────────────────────────┘
```

### 10.2 核心组件详细列表

#### 10.2.1 容器编排层(核心新增)

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **k3s** | 1.28+ | 轻量级 Kubernetes 发行版,单二进制安装 | 🆕 新增 |
| **Helm** | 3.13+ | K8s 应用包管理 | 🆕 新增 |
| **Traefik** | 内置 | k3s 自带 Ingress Controller | 🆕 内置 |
| **Klipper LB** | 内置 | k3s 自带的裸机 LoadBalancer | 🆕 内置 |
| **local-path-provisioner** | 内置 | k3s 自带的 StorageClass | 🆕 内置 |
| **CoreDNS** | 内置 | k3s 自带的服务发现 DNS | 🆕 内置 |
| **embedded etcd** | 内置 | k3s HA 模式下用于 K8s 元数据存储 | 🆕 内置(HA 模式) |

**为什么选 k3s 而不是完整 K8s**:
- 单二进制 ~50MB,资源占用 ~700MB RAM(完整 K8s 需要 ~2GB)
- 100% K8s API 兼容,kubectl/Helm 全部可用
- 内置 Ingress / LB / Storage / DNS,**不需要单独部署**
- 单节点起步,可平滑扩展到 HA 集群和完整 K8s

#### 10.2.2 集群协调层(K8s 原生能力)

| K8s 资源 | 作用 | 替代了什么 |
|---------|------|----------|
| **Service + Endpoints** | Subnode 服务发现 | 替代自研 HTTP `/register` 接口 |
| **Headless Service** | 直接拿到所有 Subnode Pod IP | 替代节点列表 SQL 查询 |
| **readinessProbe** | 5s 周期健康检查 | 替代 15s 心跳上报循环 |
| **livenessProbe** | 30s 周期存活检查 | 替代 120s 心跳超时检测 |
| **Coordination Lease** | Master 选主 | 解决 Master 单点(SPOF) |
| **StatefulSet** | Subnode 有序部署 | 提供稳定的网络标识 |
| **Deployment** | Master 滚动升级 + 副本管理 | 替代手动重启 |
| **ConfigMap / Secret** | 配置和密钥分发 | 替代环境变量手动注入 |
| **RBAC (Role + RoleBinding)** | Master 访问 Lease 的权限控制 | 新能力 |
| **Pod 自动重启** | 崩溃恢复 | 替代手动介入 |
| **滚动升级 (RollingUpdate)** | 零停机升级 | 替代停机维护 |

#### 10.2.3 Web / API 层(完全保留)

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **FastAPI** | 0.110+ | API 路由 | ✅ 保留 |
| **Pydantic v2** | 2.x | 请求/响应模型校验 | ✅ 保留 |
| **Uvicorn** | 0.27+ | ASGI 服务器 | ✅ 保留 |
| **Starlette** | 0.36+ | FastAPI 底层 | ✅ 保留 |
| **现有 67 个 API 路由** | - | admin / portal / service / yescaptcha | ✅ 完全不变 |

#### 10.2.4 业务逻辑层(完全保留 — 这是本方案的核心价值)

| 组件 | 状态 | 备注 |
|------|------|------|
| **三维 bucket affinity 路由** | ✅ 完全保留 | 项目最有价值的资产 |
| **Per-token 代理覆盖** | ✅ 完全保留 | DB 字段 + 5 个解析函数不变 |
| **Standby Token Pool** | ✅ 完全保留 | 5 个配置项 + LRU 淘汰 |
| **槽位预留机制** | ✅ 完全保留 | 防超分配 |
| **WarmupActor 自动预热** | ✅ 完全保留 | 6 个 auto_warm 配置 |
| **7 个 0=auto 配置自动派生** | ✅ 完全保留 | 现有 ConfigResolver 逻辑 |
| **10 层超时配置** | ✅ 完全保留 | execute / reload / clr / score 等 |
| **重试与故障恢复** | ✅ 完全保留 | recreate_threshold + restart_threshold |

#### 10.2.5 浏览器自动化层(完全保留)

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **Playwright** | 1.40+ | Chromium 自动化 | ✅ 保留 |
| **nodriver** | 0.48+ | 反检测浏览器(personal 模式) | ✅ 保留 |
| **Xvfb + fluxbox** | - | Linux headed 模式虚拟显示 | ✅ 保留 |
| **fake_useragent** | - | UA 指纹池 | ✅ 保留 |
| **curl-cffi** | 0.6+ | TLS 指纹伪装 | ✅ 保留 |
| **代理池(全局 + per-token)** | - | 5 个解析/规范化函数 | ✅ 保留 |

#### 10.2.6 数据层(替换 SQLite)

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **PostgreSQL** | 15+ | 主数据库(用户/配额/Job 历史) | 🔄 替换 SQLite |
| **asyncpg** | 0.29+ | 异步 PostgreSQL 驱动 | 🔄 替换 aiosqlite |
| **SQLAlchemy 2.x** (可选) | 2.x | ORM(如不想手写 SQL) | 🆕 可选 |
| **Alembic** | 1.13+ | 数据库 schema 迁移工具 | 🆕 新增 |
| **Redis** | 7+ | 会话缓存 + 限流 + bucket affinity 持久化 | 🆕 升级为必需 |
| **redis-py** | 5.0+ | 异步 Redis 客户端 | 🆕 新增 |

**关键变化**:
- SQLite 完全移除(多 Master 必须共享存储)
- Redis 从"可选日志后端"升级为"必需的运行时缓存"
- 引入 Alembic 做 schema 版本管理

**与 Ray 方案相同**: 这是 HA 架构的前置成本,与具体框架无关。

#### 10.2.7 K8s 集成层(本方案唯一的新代码)

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **kubernetes (Python)** | 29.0+ | K8s API Python 客户端 | 🆕 新增 |
| **K8sLeaderElection** | 自研约 200 行 | 基于 Lease 的 Master 选主 | 🆕 新增 |
| **K8sDiscovery** | 自研约 100 行 | 基于 Endpoints 的 Subnode 发现 | 🆕 新增 |
| **健康端点** | 自研约 50 行 | `local/health` + `local/liveness` | 🆕 新增 |
| **RBAC YAML** | 约 50 行 | Lease + Endpoints + Pods 读取权限 | 🆕 新增 |

**新增代码总量**: **约 400 行**(对比 Ray 方案的 1750 行)

#### 10.2.8 HTTP 客户端 / 工具库

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **httpx** | 0.27+ | 异步 HTTP 客户端(Master 调用 Subnode) | 🔄 替换 `_sync_json_http_request` |
| **tenacity** | 8.2+ | 重试库(指数退避) | 🆕 新增 |

#### 10.2.9 可观测性

| 组件 | 版本 | 作用 | 状态 |
|------|------|------|------|
| **K8s Dashboard** | k3s 内置 | 集群可视化 | 🆕 新增 |
| **kubectl logs** | k3s 内置 | 日志查看(替代自研 admin 页面) | 🆕 新增 |
| **K8s Events** | k3s 内置 | Pod 状态变更事件 | 🆕 新增 |
| **Prometheus** | 2.45+ | 指标采集 | 🆕 推荐 |
| **Grafana** | 10+ | 可视化看板 | 🆕 推荐 |
| **Loki** | 2.9+ | 日志聚合 | 🆕 可选 |
| **OpenTelemetry** | 1.20+ | 分布式追踪 | 🆕 可选 |

#### 10.2.10 认证 / 安全

| 组件 | 作用 | 状态 |
|------|------|------|
| **bcrypt / passlib** | 密码哈希 | ✅ 保留 |
| **PyJWT**(可选) | JWT token | ✅ 保留 |
| **Bearer Token / API Key** | 业务认证机制 | ✅ 保留 |
| **K8s ServiceAccount** | Master Pod 访问 K8s API 的身份 | 🆕 新增 |
| **TLS / Ingress 证书** | Traefik + cert-manager | 🆕 升级 |

#### 10.2.11 配置管理

| 组件 | 作用 | 状态 |
|------|------|------|
| **TOML 配置文件** | 静态配置(67 个键) | ✅ 保留 |
| **环境变量** | `FCS_*` 前缀覆盖 | ✅ 保留 |
| **K8s ConfigMap** | 把 TOML 注入容器 | 🆕 新增 |
| **K8s Secret** | 数据库密码、API Key 等 | 🆕 新增 |

#### 10.2.12 测试 / CI

| 组件 | 作用 | 状态 |
|------|------|------|
| **pytest + pytest-asyncio** | 单元/集成测试 | ✅ 保留 |
| **testcontainers-python** | PostgreSQL/Redis 容器测试 | 🆕 新增 |
| **kind / k3d** | 本地 K8s 集成测试 | 🆕 可选 |
| **httpx AsyncClient** | API 集成测试 | ✅ 保留 |

### 10.3 最小可运行依赖清单

```toml
# pyproject.toml

[project]
name = "flow-captcha-service"
version = "2.0.0"
description = "Self-hosted CAPTCHA solving service running on k3s"
requires-python = ">=3.11"
dependencies = [
    # === Web 框架(保留)===
    "fastapi>=0.110.0",
    "pydantic>=2.6.0",
    "uvicorn[standard]>=0.27.0",

    # === 数据层(替换 SQLite)===
    "asyncpg>=0.29.0",            # PostgreSQL 异步驱动
    "redis>=5.0.0",               # Redis 客户端
    "alembic>=1.13.0",            # 数据库迁移
    "sqlalchemy>=2.0.25",         # 可选 ORM

    # === 浏览器自动化(完全保留)===
    "playwright>=1.40.0",
    "nodriver==0.48.1",
    "fake-useragent",

    # === K8s 集成(本方案唯一新增的核心依赖)===
    "kubernetes>=29.0.0",         # K8s Python API 客户端

    # === HTTP 工具 ===
    "httpx>=0.27.0",
    "curl-cffi>=0.6.0",
    "tenacity>=8.2.0",

    # === 安全(保留)===
    "bcrypt>=4.1.0",
    "passlib>=1.7.4",

    # === CLI ===
    "typer>=0.9.0",

    # === 配置(保留)===
    "tomli>=2.0.1",               # TOML 解析(Python 3.11 已内置)
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "testcontainers[postgresql,redis]>=4.0.0",
    "ruff>=0.3.0",
    "mypy>=1.8.0",
]

observability = [
    "prometheus-client>=0.19.0",
    "opentelemetry-api>=1.20.0",
    "opentelemetry-sdk>=1.20.0",
]

[project.scripts]
fcs = "fcs.cli.main:app"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/fcs"]
```

**与现有 requirements.txt 对比**:
- 新增 1 个核心依赖: `kubernetes`
- 替换 1 个: `aiosqlite` → `asyncpg`
- 升级 1 个为必需: `redis`
- 其余 90% 完全不变

### 10.4 部署架构基础设施清单

#### 10.4.1 生产环境(k3s 单节点起步)

```
单台服务器 (4-8 GB RAM, 2-4 vCPU, 50GB 磁盘)
  │
  ├── k3s server (~700MB RAM)
  │     ├── Traefik Ingress (内置)
  │     ├── local-path-provisioner (内置)
  │     ├── CoreDNS (内置)
  │     └── embedded etcd (HA 模式时启用)
  │
  ├── Application Layer (Helm Chart)
  │     ├── Master Deployment (replicas=2, ~500MB each)
  │     │     ├── Master 1 (ACTIVE,持有 Lease)
  │     │     └── Master 2 (STANDBY)
  │     ├── Subnode StatefulSet (replicas=N, ~1.5GB each + browsers)
  │     │     ├── Subnode-0 (Browsers×2-4)
  │     │     ├── Subnode-1 (Browsers×2-4)
  │     │     └── Subnode-N
  │     └── Headless Service (subnode 服务发现)
  │
  ├── Data Layer
  │     ├── PostgreSQL StatefulSet (~300MB)
  │     │     └── Persistent Volume (10-20GB)
  │     └── Redis StatefulSet (~50MB)
  │           └── Persistent Volume (1-5GB)
  │
  └── Observability (可选)
        ├── Prometheus (~500MB)
        ├── Grafana (~150MB)
        └── Loki (~300MB)

总计资源占用 ≈ 4-6 GB RAM (含浏览器)
```

#### 10.4.2 生产环境(k3s HA 集群,3 节点)

```
3 台服务器 (每台 8 GB RAM, 4 vCPU)
  │
  ├── k3s Server Node 1 (control plane + worker)
  │     ├── etcd member 1
  │     ├── Master Pod 1 (Active)
  │     └── Subnode Pod 0
  │
  ├── k3s Server Node 2 (control plane + worker)
  │     ├── etcd member 2
  │     ├── Master Pod 2 (Standby)
  │     └── Subnode Pod 1
  │
  ├── k3s Server Node 3 (control plane + worker)
  │     ├── etcd member 3
  │     └── Subnode Pod 2
  │
  └── 外部托管(推荐):
        ├── PostgreSQL: 云厂商 RDS / 自建 HA
        ├── Redis: 云厂商 / 自建主从
        └── 监控栈: 独立部署或 SaaS
```

#### 10.4.3 开发环境(docker-compose)

```yaml
# deploy/docker/docker-compose.dev.yml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: fcs
      POSTGRES_USER: fcs
      POSTGRES_PASSWORD: changeme
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  master:
    build:
      context: ../..
      dockerfile: deploy/docker/Dockerfile.master
    environment:
      FCS_DB_DSN: postgresql://fcs:changeme@postgres:5432/fcs
      FCS_REDIS_URL: redis://redis:6379/0
      FCS_CLUSTER_ROLE: master
      FCS_DEV_MODE: "true"  # 开发模式跳过 K8s,用文件锁选主
    ports:
      - "8060:8060"
    depends_on: [postgres, redis]

  subnode:
    build:
      context: ../..
      dockerfile: deploy/docker/Dockerfile.subnode
    environment:
      FCS_DB_DSN: postgresql://fcs:changeme@postgres:5432/fcs
      FCS_CLUSTER_ROLE: subnode
      FCS_DEV_MODE: "true"
    deploy:
      replicas: 2
    depends_on: [postgres, redis, master]
```

**开发模式说明**: 设置 `FCS_DEV_MODE=true` 跳过 K8s 集成,改用文件锁/SQLite Lease 模拟选主,便于本地开发调试。

### 10.5 技术栈对比: 现状 vs k3s 方案 vs Ray 方案

| 层 | 现状 | **k3s 方案** | Ray 方案 |
|----|------|-------------|---------|
| **运行时** | Python 3.11 + asyncio | Python 3.11 + asyncio | Python 3.11 + asyncio + Ray |
| **数据库** | SQLite (aiosqlite) | **PostgreSQL (asyncpg)** | PostgreSQL (asyncpg) |
| **缓存** | 无 / 可选 Redis | **Redis (必需)** | Redis (必需) |
| **Web 框架** | FastAPI | **FastAPI(完全保留)** | FastAPI + Serve 集成 |
| **HTTP 客户端** | 自研 + httpx | **httpx + tenacity** | httpx + tenacity |
| **集群协调** | 自研 cluster_manager (~1200 行) | **K8s Service + Lease + Probes** | Ray GCS + Serve |
| **路由层** | 自研三维 affinity 路由 | **完全保留(0 改动)** | ❌ Ray multiplex 不匹配 |
| **服务发现** | HTTP 注册接口 | **K8s Service 自动** | Ray GCS 自动 |
| **健康检查** | 心跳 (15s) + SQL 查询 | **K8s Probes (15s 检测)** | Serve `check_health` |
| **失活检测速度** | 120s | **15s** | 即时 |
| **负载均衡** | 自研加权轮询 | **保留(配合 K8s 服务发现)** | Serve 内置 + multiplex |
| **HA** | 无 | **K8s Lease 选主(15s 故障转移)** | Ray Head HA + KubeRay |
| **自动伸缩** | 无 / 手动 | **K8s HPA(可选)** | Serve `autoscaling_config` |
| **容器化** | Docker (Dockerfile.headed/master) | **Docker(轻度改造)** | Docker(完全重写) |
| **编排** | docker-compose | **k3s + Helm** | Kubernetes + KubeRay |
| **监控** | 自研 admin 页面 | **K8s Dashboard + Prometheus** | Ray Dashboard + Prometheus |
| **日志** | 文件 / Redis List | **kubectl logs + Loki(可选)** | Ray log streaming + Loki |
| **配置** | TOML + 环境变量 | **TOML + ConfigMap + Secret** | TOML + Serve YAML + ConfigMap |
| **测试** | pytest + 临时 SQLite | **pytest + testcontainers** | pytest + testcontainers + ray.test_utils |

### 10.6 技术栈复杂度对比

#### 现状(Standalone 模式最小启动)

```
1. Python 3.11
2. pip install -r requirements.txt
3. python -m playwright install chromium
4. python -m src.main
```

**外部依赖数**: 0 (SQLite 是文件)
**启动时间**: ~1 秒
**资源占用**: ~500MB RAM

#### k3s 方案(开发环境)

```
1. Python 3.11
2. PostgreSQL (本地或 Docker)
3. Redis (本地或 Docker)
4. pip install -e .
5. python -m playwright install chromium
6. alembic upgrade head        # schema 初始化
7. docker-compose -f deploy/docker/docker-compose.dev.yml up
```

**外部依赖数**: 2 (PostgreSQL + Redis,K8s 在开发模式跳过)
**启动时间**: ~10 秒
**资源占用**: ~1.5 GB RAM

#### k3s 方案(生产部署,单节点)

```
1. curl -sfL https://get.k3s.io | sh -          # 装 k3s
2. helm install postgres bitnami/postgresql ...  # 装 PostgreSQL
3. helm install redis bitnami/redis ...          # 装 Redis
4. helm install fcs ./deploy/helm/flow-captcha   # 装应用
5. kubectl apply -f ingress.yaml                 # 暴露服务
```

**外部依赖数**: 3 (k3s + PostgreSQL + Redis)
**启动时间**: ~30 秒(整个集群)
**资源占用**: ~4-6 GB RAM

#### Ray 方案(生产部署,对比)

```
1. Kubernetes 集群
2. helm install kuberay-operator ...
3. kubectl apply -f raycluster.yaml
4. kubectl apply -f postgres.yaml
5. kubectl apply -f redis.yaml
6. kubectl apply -f rayservice.yaml
7. kubectl apply -f ingress.yaml
8. helm install prometheus ...
9. helm install grafana ...
```

**外部依赖数**: 6+ (K8s + KubeRay + Ray + PG + Redis + Prometheus + Grafana)
**启动时间**: ~2-3 分钟
**资源占用**: ~6-8 GB RAM

### 10.7 技术栈成本/复杂度评估

| 维度 | 现状 | **k3s 方案(推荐)** | Ray 方案 |
|------|------|------------------|---------|
| **依赖组件数** | 1 (SQLite) | **3 (k3s + PG + Redis)** | 6+ |
| **最小机器规模** | 1 台 2GB | **1 台 4-8GB** | 3 台 8GB+ |
| **运维门槛** | 低(懂 Python 即可) | **中(K8s 基础)** | 高(K8s + Ray + DB) |
| **学习成本** | 1-2 天 | **1 周(K8s 基础)** | 2-4 周(Ray + multiplex + K8s) |
| **代码改动** | - | **~600 行删除 + ~400 行新增 = -200 行** | ~2000 行删除 + ~1750 行新增 = -250 行 |
| **业务代码可复用率** | - | **80-90%** | 40-50% |
| **部署复杂度** | `python -m src.main` | **`helm install`(几条命令)** | Helm + 多 CRD + KubeRay operator |
| **资源占用(生产最小)** | ~500MB RAM | **~4-6GB RAM** | ~6-8GB RAM |
| **故障排查难度** | 简单(单进程日志) | **中(kubectl + 标准 K8s 工具)** | 复杂(跨 actor 异步追踪) |
| **HA 能力** | 无 | **15s 故障转移** | 即时 |
| **未来扩展性** | 受限 | **可平滑升级到完整 K8s** | 已是终态 |
| **3D affinity 路由** | ✅ 现有实现 | **✅ 完全保留** | ❌ Ray multiplex 退化 |
| **Per-token 代理** | ✅ 现有实现 | **✅ 完全保留** | 🟡 需要重写 |
| **改造时间** | - | **4-6 周** | 3-6 个月 |
| **回滚难度** | - | **低(分阶段可回滚)** | 高 |
| **社区支持** | 无 | **活跃(Rancher + CNCF)** | 活跃(Anyscale) |

### 10.8 关键决策点

如果要采用 k3s 方案,**有几个无法回避的硬性要求**:

1. ✅ **必须迁移到 PostgreSQL**(SQLite 无法用于多 Master 共享状态)
2. ✅ **必须引入 Redis**(用于会话缓存、bucket affinity 持久化)
3. ✅ **必须用 k3s 或 K8s**(本方案的核心)
4. 🟡 **建议搭建监控栈**(Prometheus + Grafana,k3s 上可用 `kube-prometheus-stack`)
5. 🟡 **团队需要懂基本 K8s 操作**(kubectl, Helm, Pod, Service, Probe, ConfigMap)

> ⚠️ **第 1、2、3 条是硬性要求,不满足无法启动该方案。第 4、5 条可在初期简化。**

> 💡 **与 Ray 方案的区别**: k3s 方案不要求团队懂 Ray 的复杂概念(Actor、Serve、multiplex、autoscaling 调参),只需要 K8s 的基础知识,而 K8s 知识在云原生时代是更通用的技能投资。

### 10.9 技术栈选型总结

| 层级 | 选型 | 理由 |
|------|------|------|
| **容器编排** | **k3s** | 单二进制、轻量、100% K8s 兼容、可平滑升级 |
| **Master HA** | **K8s Lease** | 标准模式、不引入额外协调服务 |
| **服务发现** | **K8s Service + Endpoints** | 框架原生、自动维护 |
| **健康检查** | **K8s Probes** | 框架原生、5s 周期 |
| **数据库** | **PostgreSQL** | 多写者支持、生态成熟 |
| **缓存** | **Redis** | 持久化 affinity、限流 |
| **路由层** | **保留现有 cluster_manager** | 业务最优,不重新发明轮子 |
| **浏览器** | **Playwright + nodriver** | 完全保留 |
| **Web 框架** | **FastAPI** | 完全保留 |
| **配置** | **TOML + ConfigMap** | 现有 TOML 不变,K8s 注入容器 |
| **监控** | **K8s Dashboard + Prometheus** | 标准云原生监控栈 |
| **HTTP Client** | **httpx + tenacity** | 替换自研同步实现 |

---

## 11. 推荐的目标代码结构

> **本章规划"如果在改造过程中顺便清理代码库,推荐的目标结构"**。可以一次到位,也可以渐进式迁移。结构遵循现代 Python 工程实践 + 领域驱动设计。

### 11.1 设计原则

| 原则 | 说明 |
|------|------|
| **`src/` layout** | 现代 Python 最佳实践,强制通过包安装方式导入 |
| **按领域 + 按层混合分组** | 先按领域(captcha/identity/billing),领域内再按层 |
| **领域层独立于框架** | `domain/` 不依赖 FastAPI、K8s、SQLAlchemy |
| **基础设施抽象** | DB / Redis / K8s 的连接和配置统一在 `infra/` |
| **集群代码隔离** | `cluster/` 集中管理路由、选主、服务发现 |
| **测试镜像源码结构** | `tests/` 与 `src/fcs/` 一一对应 |
| **部署与代码分离** | Dockerfile、Helm Chart 全部放 `deploy/` |
| **配置外部化** | `config/` TOML + 环境变量 |
| **现有 cluster_manager 渐进式拆分** | 不一次性重写,按 11.6 节描述的步骤迁移 |

### 11.2 完整目录树

```
flow_captcha_service/
│
├── pyproject.toml                  # 项目元数据 + 依赖
├── README.md
├── LICENSE
├── .python-version                 # pyenv 锁定
├── .gitignore
├── .dockerignore
├── .pre-commit-config.yaml
│
├── src/                            # ============ 所有源代码 ============
│   └── fcs/                        # 主包
│       ├── __init__.py
│       │
│       ├── core/                   # 横切关注点
│       │   ├── __init__.py
│       │   ├── config.py           # 配置加载(TOML + ENV)
│       │   ├── logging.py          # 结构化日志
│       │   ├── errors.py           # 业务异常类层次
│       │   ├── types.py            # 共享类型
│       │   └── constants.py
│       │
│       ├── infra/                  # ========== 基础设施层 ==========
│       │   ├── __init__.py
│       │   ├── db/
│       │   │   ├── engine.py       # asyncpg 连接池
│       │   │   ├── session.py      # 事务管理
│       │   │   ├── repositories/   # Repository 模式
│       │   │   │   ├── api_keys.py
│       │   │   │   ├── users.py
│       │   │   │   ├── jobs.py
│       │   │   │   ├── quota.py
│       │   │   │   ├── cdk.py
│       │   │   │   └── transactions.py
│       │   │   └── migrations/     # Alembic
│       │   │       ├── env.py
│       │   │       ├── alembic.ini
│       │   │       └── versions/
│       │   ├── redis/
│       │   │   ├── client.py
│       │   │   ├── rate_limit.py
│       │   │   └── cache.py
│       │   └── k8s/                # ← K8s 集成(新增,替代 ray/)
│       │       ├── __init__.py
│       │       ├── client.py       # K8s API client wrapper
│       │       ├── leader_election.py  # Lease 选主
│       │       ├── discovery.py    # Endpoint watch & list
│       │       └── rbac.py         # 权限检查辅助
│       │
│       ├── domain/                 # ========== 领域层(纯业务) ==========
│       │   ├── __init__.py         # 不依赖任何框架
│       │   ├── captcha/
│       │   │   ├── models.py       # CaptchaRequest, TokenResult
│       │   │   ├── enums.py        # CaptchaType, CaptchaMethod
│       │   │   └── protocols.py    # 接口定义
│       │   ├── identity/
│       │   │   ├── api_key.py
│       │   │   ├── user.py
│       │   │   └── permissions.py
│       │   ├── billing/
│       │   │   ├── quota.py
│       │   │   ├── transaction.py
│       │   │   └── cdk.py
│       │   └── session/
│       │       ├── state.py
│       │       └── lifecycle.py
│       │
│       ├── browser/                # ========== 浏览器自动化 ==========
│       │   ├── __init__.py         # 完全保留现有逻辑
│       │   ├── base.py             # BrowserEngine 抽象基类
│       │   ├── playwright_engine.py    # Playwright 实现
│       │   ├── nodriver_engine.py      # nodriver 实现
│       │   ├── fingerprint.py      # UA 指纹池
│       │   ├── proxy.py            # 代理配置(完全保留 5 个解析函数)
│       │   ├── stealth.py          # 反检测 patches
│       │   └── solvers/            # 各类验证码解决器
│       │       ├── __init__.py
│       │       ├── recaptcha_v2.py
│       │       ├── recaptcha_v3.py
│       │       ├── turnstile.py
│       │       └── flow_native.py
│       │
│       ├── cluster/                # ← ============ 集群协调 ============
│       │   ├── __init__.py         # (替代原 cluster_manager.py)
│       │   ├── master.py           # Master 角色:启动选主 + 路由循环
│       │   ├── subnode.py          # Subnode 角色:启动健康端点
│       │   ├── routing/            # 路由层(完全保留 3D affinity)
│       │   │   ├── __init__.py
│       │   │   ├── bucket_key.py   # 三维 bucket key 计算
│       │   │   ├── affinity.py     # Project + action + proxy 亲和路由
│       │   │   ├── proxy_resolver.py   # 代理解析(全局池 + per-token)
│       │   │   ├── candidate_select.py # 加权候选节点选择
│       │   │   ├── slot_reserve.py     # 槽位预留机制
│       │   │   └── dispatcher.py   # 实际派发
│       │   ├── token_pool/         # Standby Token Pool(保留)
│       │   │   ├── __init__.py
│       │   │   ├── pool.py         # 5 个 standby 配置项
│       │   │   ├── bucket.py       # bucket 管理 + LRU
│       │   │   └── warmup.py       # 自动预热(6 个 auto_warm 配置)
│       │   ├── session_registry.py # 会话生命周期
│       │   └── config_resolver.py  # 7 个 0=auto 配置自动派生
│       │
│       ├── api/                    # ========== HTTP API 层 ==========
│       │   ├── __init__.py
│       │   ├── app.py              # FastAPI app factory
│       │   ├── deps.py             # 依赖注入
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
│       │   ├── v1/                 # 主 API
│       │   │   ├── __init__.py
│       │   │   ├── service.py      # /api/v1/solve, /prefill, /finish
│       │   │   ├── admin.py        # /api/admin/*
│       │   │   ├── portal.py       # /portal/*
│       │   │   └── health.py       # /api/cluster/local/health 等
│       │   └── compat/             # 第三方协议兼容
│       │       └── yescaptcha.py   # YesCaptcha 协议
│       │
│       ├── services/               # ========== 应用服务层 ==========
│       │   ├── __init__.py         # 用例编排
│       │   ├── captcha_service.py  # 解决验证码用例
│       │   ├── quota_service.py    # 配额扣减/退还
│       │   ├── api_key_service.py
│       │   ├── user_service.py
│       │   └── cdk_service.py
│       │
│       ├── auth/                   # ========== 认证模块 ==========
│       │   ├── __init__.py
│       │   ├── api_key_auth.py
│       │   ├── admin_auth.py
│       │   ├── portal_auth.py
│       │   ├── password.py
│       │   └── tokens.py
│       │
│       └── cli/                    # ========== 命令行工具 ==========
│           ├── __init__.py
│           ├── main.py             # 主 CLI 入口
│           ├── master_server.py    # 启动 Master
│           ├── subnode_server.py   # 启动 Subnode
│           ├── admin.py            # 创建管理员/重置密码
│           ├── migrate.py          # alembic 包装
│           └── seed.py
│
├── tests/                          # ============ 测试代码 ============
│   ├── conftest.py
│   ├── fixtures/
│   │   └── captcha_pages/
│   ├── unit/                       # 单元测试(无外部依赖)
│   │   ├── domain/
│   │   ├── browser/
│   │   ├── cluster/
│   │   │   ├── routing/            # 三维 affinity 测试
│   │   │   └── token_pool/
│   │   ├── services/
│   │   └── core/
│   ├── integration/                # 集成测试(真实 PG/Redis)
│   │   ├── api/
│   │   ├── infra/
│   │   ├── cluster/
│   │   └── browser/
│   └── e2e/                        # 端到端测试
│       ├── test_full_solve_flow.py
│       └── test_yescaptcha_compat.py
│
├── frontend/                       # ============ 前端 UI ============
│   ├── admin/                      # 管理后台
│   ├── portal/                     # 用户门户
│   └── shared/
│
├── deploy/                         # ============ 部署相关 ============
│   ├── docker/
│   │   ├── Dockerfile.master       # Master 镜像(轻量,无浏览器)
│   │   ├── Dockerfile.subnode      # Subnode 镜像(含 Xvfb + 浏览器)
│   │   ├── docker-compose.dev.yml  # 本地开发栈
│   │   └── entrypoints/
│   │       ├── master.sh
│   │       └── subnode.sh          # Xvfb + fluxbox + Subnode
│   ├── helm/                       # Helm Chart
│   │   └── flow-captcha/
│   │       ├── Chart.yaml
│   │       ├── values.yaml
│   │       ├── values-dev.yaml
│   │       ├── values-prod.yaml
│   │       └── templates/
│   │           ├── master-deployment.yaml
│   │           ├── master-service.yaml
│   │           ├── master-rbac.yaml
│   │           ├── subnode-statefulset.yaml
│   │           ├── subnode-headless-svc.yaml
│   │           ├── postgres-statefulset.yaml
│   │           ├── redis-statefulset.yaml
│   │           ├── ingress.yaml
│   │           ├── configmap.yaml
│   │           ├── secrets.yaml
│   │           └── _helpers.tpl
│   └── k3s/                        # k3s 特定脚本
│       ├── install.sh              # 一键安装
│       ├── bootstrap.md            # 部署文档
│       └── manifests/              # 不通过 Helm 的额外清单
│
├── config/                         # ============ 应用配置 ============
│   ├── default.toml                # 默认配置(67 个键)
│   ├── development.toml
│   ├── production.toml
│   └── schema.json
│
├── scripts/                        # ============ 辅助脚本 ============
│   ├── dev/
│   │   ├── start_local.sh          # 本地一键启动
│   │   ├── reset_db.sh
│   │   └── load_fixtures.py
│   ├── ops/
│   │   ├── backup_db.sh
│   │   ├── migrate.sh
│   │   └── rotate_keys.py
│   └── ci/
│       ├── lint.sh
│       └── build_image.sh
│
├── docs/                           # ============ 文档 ============
│   ├── ARCHITECTURE.md
│   ├── DEPLOY_GUIDE.md
│   ├── DEVELOPMENT.md
│   ├── API.md
│   ├── K3S_CONTAINERIZE_PROPOSAL.md  # 本文档
│   ├── RAY_REWRITE_PROPOSAL.md       # 对照方案
│   ├── adr/                        # 架构决策记录
│   │   ├── 0001-use-k3s.md
│   │   ├── 0002-postgresql.md
│   │   └── 0003-keep-cluster-manager-routing.md
│   └── images/
│
└── .github/                        # ============ CI/CD ============
    └── workflows/
        ├── test.yml
        ├── build.yml
        └── deploy.yml
```

### 11.3 关键模块的设计理由

#### 11.3.1 为什么用 `src/` layout

```python
# 强制通过包名导入
from fcs.cluster.routing.affinity import compute_bucket_key
```

**好处**:
- 必须通过 `pip install -e .` 安装才能 import,防止意外用到未安装的包
- pytest 不会污染 sys.path
- PyPA 官方推荐的现代布局

#### 11.3.2 `cluster/` 模块为什么单独抽出

```python
# fcs/cluster/master.py
from fcs.infra.k8s.leader_election import K8sLeaderElection
from fcs.cluster.routing.dispatcher import Dispatcher
from fcs.cluster.token_pool.pool import TokenPool

class MasterService:
    def __init__(self):
        self.election = K8sLeaderElection()
        self.dispatcher = Dispatcher(...)
        self.token_pool = TokenPool(...)

    async def run(self):
        await self.election.run(
            on_become_leader=self._activate,
            on_lose_leadership=self._deactivate,
        )

    async def _activate(self):
        await self.token_pool.start_warmup()
        await self.dispatcher.start()
```

**好处**:
- 集群协调相关代码集中在一处,便于查找
- `routing/` 子模块完全保留现有的 3D affinity 实现
- `infra/k8s/` 处理 K8s 特定细节,与业务解耦
- 未来如果要换 Kubernetes(比如改用 NATS),只需要替换 `infra/k8s/` 和 `cluster/master.py` 中的少量代码

#### 11.3.3 `domain/` 层为什么独立

```python
# domain/captcha/models.py
from pydantic import BaseModel
# ❌ 不要 import fastapi, kubernetes, asyncpg

class CaptchaRequest(BaseModel):
    project_id: str
    action: str
    proxy_url: Optional[str] = None  # per-token 覆盖
```

**好处**:
- 单元测试时无需启动任何外部服务
- 业务规则集中,看 `domain/` 就能理解项目做什么
- 未来如果要换 Web 框架,领域层完全不动

#### 11.3.4 `api/` 与 `services/` 的分离

```python
# api/v1/service.py
@router.post("/solve")
async def solve(
    request: SolveRequest,
    captcha_service: CaptchaService = Depends(get_captcha_service),
):
    # API 层只做参数校验和响应封装
    result = await captcha_service.solve(request.to_domain())
    return SolveResponse.from_domain(result)


# services/captcha_service.py
class CaptchaService:
    def __init__(self, dispatcher, token_pool, quota_repo):
        self.dispatcher = dispatcher    # cluster.routing.dispatcher
        self.token_pool = token_pool    # cluster.token_pool
        self.quota_repo = quota_repo    # infra.db.repositories

    async def solve(self, req: CaptchaRequest) -> TokenResult:
        # 用例编排:配额检查 → token 池命中 → 派发
        await self.quota_repo.check(req.api_key)
        if cached := self.token_pool.get(req.bucket_key):
            return cached
        return await self.dispatcher.dispatch(req)
```

**好处**:
- API 层只关心 HTTP 协议
- Service 层只关心业务用例编排
- 同一个 Service 可以被多个入口复用(HTTP / CLI / 内部调用)

#### 11.3.5 `infra/db/repositories/` 仓储模式

```python
# infra/db/repositories/api_keys.py
class APIKeyRepository:
    def __init__(self, db: Database):
        self.db = db

    async def find_by_hash(self, key_hash: str) -> Optional[APIKey]:
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(...)
            return APIKey.from_row(row) if row else None
```

**好处**:
- 数据库访问全部走 Repository,方便 mock 测试
- 切换 ORM(asyncpg ↔ SQLAlchemy)只改 Repository 实现
- 业务代码不直接写 SQL

#### 11.3.6 `infra/k8s/` 的边界

```python
# infra/k8s/leader_election.py - 纯 K8s 操作
class K8sLeaderElection:
    """只关心如何与 K8s API 交互,不关心业务逻辑"""
    async def run(self, on_become_leader, on_lose_leadership): ...

# cluster/master.py - 业务逻辑
class MasterService:
    """使用 K8sLeaderElection,但定义"成为 leader 后做什么"""
    async def _activate(self): ...
```

**好处**:
- K8s 集成代码完全隔离,便于测试(可以 mock K8s API)
- 业务逻辑不感知 K8s 细节
- 未来换协调机制(比如换成 etcd 直接用)只改 `infra/k8s/`

### 11.4 新旧文件迁移对照表

| 现有文件 | 新位置 | 备注 |
|---------|-------|------|
| `src/main.py` | `src/fcs/cli/main.py` | CLI 入口 |
| `src/http_bridge.py` | **保留或删除** | 容器化后可删除(K8s Ingress 接管) |
| `src/core/config.py` | `src/fcs/core/config.py` | 几乎不变 |
| `src/core/database.py` | `src/fcs/infra/db/engine.py` + `repositories/*` | 拆分为多个 Repository |
| `src/core/auth.py` | `src/fcs/auth/*.py` | 按认证类型拆分 |
| `src/core/models.py` | `src/fcs/domain/*/models.py` + `src/fcs/api/schemas/*` | 领域模型和 API DTO 分离 |
| `src/core/log_store.py` | `src/fcs/infra/redis/log_store.py` | 移到基础设施层 |
| `src/api/service.py` | `src/fcs/api/v1/service.py` | API 版本化 |
| `src/api/admin.py` | `src/fcs/api/v1/admin.py` | 同上 |
| `src/api/portal.py` | `src/fcs/api/v1/portal.py` | 同上 |
| `src/api/cluster.py` | **大部分删除** | K8s 接管,只保留 health endpoints |
| `src/api/yescaptcha.py` | `src/fcs/api/compat/yescaptcha.py` | 移到兼容层 |
| `src/services/captcha_runtime.py` | `src/fcs/services/captcha_service.py` | 简化为用例编排 |
| `src/services/browser_captcha.py` | `src/fcs/browser/playwright_engine.py` + `src/fcs/cluster/routing/*` | 浏览器与路由分离 |
| `src/services/browser_captcha_personal.py` | `src/fcs/browser/nodriver_engine.py` | 同上 |
| `src/services/cluster_manager.py` | **拆分到** `src/fcs/cluster/` 的多个子模块 | 路由保留,网络层删除 |
| `src/services/session_registry.py` | `src/fcs/cluster/session_registry.py` | 路径调整 |
| `src/services/yescaptcha_manager.py` | `src/fcs/services/yescaptcha_service.py` | 简化 |
| `Dockerfile.headed` | `deploy/docker/Dockerfile.subnode` | Subnode 镜像 |
| `Dockerfile.master` | `deploy/docker/Dockerfile.master` | Master 镜像(轻量) |
| `docker-compose.*.yml` | `deploy/docker/docker-compose.dev.yml` | 仅保留开发用 |
| `config/setting_*.toml` | `config/{development,production}.toml` | 标准化命名 |
| `static/admin/*` | `frontend/admin/*` | 前端独立目录 |
| `static/portal/*` | `frontend/portal/*` | 同上 |
| `tests/*` | `tests/{unit,integration,e2e}/*` | 分层测试 |

### 11.5 关键文件示例

#### 11.5.1 `pyproject.toml`

```toml
[project]
name = "flow-captcha-service"
version = "2.0.0"
description = "Self-hosted CAPTCHA solving service running on k3s"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.110.0",
    "pydantic>=2.6.0",
    "uvicorn[standard]>=0.27.0",
    "asyncpg>=0.29.0",
    "redis>=5.0.0",
    "alembic>=1.13.0",
    "playwright>=1.40.0",
    "nodriver==0.48.1",
    "kubernetes>=29.0.0",
    "httpx>=0.27.0",
    "curl-cffi>=0.6.0",
    "tenacity>=8.2.0",
    "bcrypt>=4.1.0",
    "typer>=0.9.0",
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

#### 11.5.2 `src/fcs/cli/master_server.py`

```python
"""Master 服务启动入口"""
import asyncio
import typer
from fcs.core.config import load_config
from fcs.infra.db.engine import Database
from fcs.infra.k8s.leader_election import K8sLeaderElection
from fcs.cluster.master import MasterService
from fcs.api.app import create_app
import uvicorn

app = typer.Typer()

@app.command()
def run():
    asyncio.run(_main())

async def _main():
    config = load_config()
    db = Database()
    await db.connect(config.database.url)

    master = MasterService(config=config, db=db)

    # 同时启动 HTTP 服务和选主循环
    api = create_app(master_service=master)
    server = uvicorn.Server(uvicorn.Config(api, host="0.0.0.0", port=8060))

    await asyncio.gather(
        master.run_election_loop(),  # 选主 + 激活/反激活
        server.serve(),               # HTTP API
    )

if __name__ == "__main__":
    app()
```

#### 11.5.3 `config/default.toml`

```toml
[server]
host = "0.0.0.0"
port = 8060

[database]
url = "postgresql+asyncpg://fcs:changeme@postgres:5432/fcs"
pool_size = 20

[redis]
url = "redis://redis:6379/0"

[k8s]
namespace = "fcs"
master_lease_name = "fcs-master-lease"
subnode_service_name = "fcs-subnode-headless"

[captcha]
captcha_method = "browser"
session_ttl_seconds = 1200
browser_count = 4

# (其余 60+ 配置项保持现有结构,完全兼容)
[browser]
proxy_enabled = false
proxy_url = ""
standby_token_pool_depth = 2
project_affinity_max_keys = 0  # 0 = auto
# ... 等
```

### 11.6 渐进式迁移路径

不必一次性按上面的目录结构重写,可以按"先做改造,再清理结构"的顺序:

```
当前结构(扁平 src/)
        │
        │ 阶段 1-3: 容器化 + PG 迁移 + k3s 部署
        │ (代码结构暂时不动)
        ▼
当前结构 + Docker + Helm
        │
        │ 阶段 4-6: K8s 服务发现 + Probes + Lease 选主
        │ (新增 fcs/infra/k8s/ 目录)
        ▼
当前结构 + K8s 集成
        │
        │ 阶段 7: 灰度切流 + 旧代码删除
        ▼
最终目标: src/fcs/ layout(可分多个 PR 完成)
```

**关键**:目录结构重构是**可选的优化**,不是 k3s 改造的前置条件。即使保持现有的扁平 `src/` 结构,也能完成 k3s 改造。本章描述的目标结构是"如果将来要做代码清理,推荐这样做"。

---

## 附录 A:K8s API 速查

| 操作 | Python 代码 |
|------|------------|
| 加载集群内配置 | `config.load_incluster_config()` |
| 列出 Pod | `client.CoreV1Api().list_namespaced_pod(namespace)` |
| 列出 Endpoint | `client.CoreV1Api().read_namespaced_endpoints(name, namespace)` |
| Watch 资源变化 | `watch.Watch().stream(api_func, ...)` |
| 读取 Lease | `client.CoordinationV1Api().read_namespaced_lease(name, namespace)` |
| 更新 Lease | `client.CoordinationV1Api().replace_namespaced_lease(name, namespace, body)` |
| 创建 Lease | `client.CoordinationV1Api().create_namespaced_lease(namespace, body)` |

## 附录 B:参考资料

- k3s 官方文档:https://k3s.io/
- Kubernetes Python Client:https://github.com/kubernetes-client/python
- K8s Lease(Leader Election):https://kubernetes.io/docs/concepts/architecture/leases/
- Helm Chart 开发指南:https://helm.sh/docs/chart_best_practices/
- 现代 Python 项目结构(`src/` layout):https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/

---

**审核人**:_______________
**审核日期**:_______________
**审核结果**:☐ 通过　☐ 需修改　☐ 拒绝
