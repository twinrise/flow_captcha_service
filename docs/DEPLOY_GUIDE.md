# Flow Captcha Service — 部署指南

> 版本：1.0 | 更新日期：2026-04-03

---

## 目录

1. [环境要求](#1-环境要求)
2. [快速开始](#2-快速开始)
3. [部署模式详解](#3-部署模式详解)
4. [配置说明](#4-配置说明)
5. [部署脚本用法](#5-部署脚本用法)
6. [手工部署](#6-手工部署)
7. [Staging 环境部署](#7-staging-环境部署)
8. [部署后验证](#8-部署后验证)
9. [常用运维操作](#9-常用运维操作)
10. [故障排查](#10-故障排查)

---

## 1. 环境要求

| 依赖 | 最低版本 | 说明 |
|------|---------|------|
| Docker | 20.10+ | 容器运行时 |
| Docker Compose | v2.0+ | 使用 `docker compose`（非 `docker-compose`） |
| 系统内存 | 2GB+ | 每个浏览器实例约占 300-500MB |
| 磁盘空间 | 2GB+ | Docker 镜像 ~1.5GB (headed) |

> WSL2 用户：确保已在 Docker Desktop 设置中启用 WSL2 集成。

---

## 2. 快速开始

最快的方式是使用部署脚本单机启动：

```bash
# 单机模式，一行搞定
./scripts/deploy.sh standalone
```

启动后访问：

| 地址 | 说明 |
|------|------|
| http://localhost:8060 | 服务根地址 |
| http://localhost:8060/admin | 管理后台（默认账号 admin/admin） |
| http://localhost:8060/portal | 用户门户 |
| http://localhost:8060/api/v1/health | 健康检查 |

---

## 3. 部署模式详解

### 3.1 Standalone — 单机模式

最简单的部署方式，所有组件运行在一个容器中。

```
┌──────────────────────────┐
│   flow-captcha-service    │
│   API + Browser + DB      │
│   端口: 8060              │
└──────────────────────────┘
```

**适用场景：** 开发调试、小规模使用、个人部署

**对应文件：** `docker-compose.headed.yml`

```bash
./scripts/deploy.sh standalone
```

### 3.2 Master — 集群调度节点

不运行浏览器，仅负责接收请求并调度到 Subnode。需搭配 Redis 和至少一个 Subnode。

```
┌────────────────────────────────┐
│   flow-captcha-master + Redis   │
│   API + 调度 (无浏览器)          │
│   端口: 8060                    │
└────────────────────────────────┘
```

**适用场景：** 生产集群的调度入口

**对应文件：** `docker-compose.cluster.master.yml`

```bash
./scripts/deploy.sh master
```

### 3.3 Subnode — 集群工作节点

运行浏览器引擎，执行实际的验证码解决，向 Master 汇报心跳。

```
┌──────────────────────────┐
│   flow-captcha-subnode    │
│   Browser × N             │
│   端口: 8061              │
└──────────────────────────┘
```

**适用场景：** 集群中的工作节点，可部署多个

**对应文件：** `docker-compose.cluster.subnode.yml`

```bash
./scripts/deploy.sh subnode
```

> 部署前需配置 `data/subnode/setting.toml` 中的 Master 连接信息，或通过环境变量覆盖。

### 3.4 Stack — 完整集群演示栈

一键启动 Master + Subnode + Redis 完整集群，适合快速体验集群模式。

```
┌───────────────┐     ┌────────────────┐     ┌──────────────┐
│     Redis      │────▶│     Master      │◀────│   Subnode     │
│  :6379 (内部)  │     │  :8060 (对外)   │     │  :8061 (对外) │
└───────────────┘     └────────────────┘     └──────────────┘
```

**对应文件：** `docker-compose.cluster.stack.yml`

```bash
./scripts/deploy.sh stack
```

### 3.5 模式对比

| | Standalone | Master | Subnode | Stack |
|--|-----------|--------|---------|-------|
| 浏览器 | ✅ | ❌ | ✅ | ✅ (Subnode) |
| Redis | ❌ | ✅ | ❌ | ✅ |
| 对外端口 | 8060 | 8060 | 8061 | 8060 + 8061 |
| 适用场景 | 开发/小规模 | 生产入口 | 生产工作节点 | 演示/测试集群 |

---

## 4. 配置说明

### 4.1 配置文件位置

| 文件 | 用途 |
|------|------|
| `config/setting_example.toml` | 默认模板（standalone / master / subnode） |
| `config/setting_staging_master.toml` | Staging Master 配置 |
| `config/setting_staging_subnode.toml` | Staging Subnode 配置 |
| `data/setting.toml` | 运行时实际配置（由脚本自动复制） |

### 4.2 配置优先级

```
环境变量 (FCS_*)  >  data/setting.toml  >  代码默认值
```

Docker Compose 文件中的 `environment` 会覆盖 TOML 文件中的同名配置。

### 4.3 关键配置项

**必须关注的配置：**

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `admin.password` | 管理员密码，**生产环境务必修改** | `admin` |
| `captcha.browser_count` | 浏览器并发数，决定吞吐量 | `1` |
| `captcha.captcha_method` | 引擎类型 `browser` / `personal` | `browser` |
| `cluster.role` | 部署角色 | `standalone` |
| `cluster.master_cluster_key` | 集群通信密钥，Master 与 Subnode 必须一致 | 空 |

**Subnode 必填配置：**

| 配置项 | 说明 |
|--------|------|
| `cluster.master_base_url` | Master 地址，如 `http://master-host:8060` |
| `cluster.master_cluster_key` | 与 Master 一致的集群密钥 |
| `cluster.node_public_base_url` | 本节点对 Master 可达的地址 |
| `cluster.node_api_key` | 节点内部认证 Key |

### 4.4 环境变量覆盖

所有配置项都可通过 `FCS_` 前缀的环境变量覆盖，在 docker-compose 中直接设置即可：

```yaml
environment:
  - FCS_CAPTCHA_BROWSER_COUNT=4
  - FCS_CLUSTER_MASTER_CLUSTER_KEY=my-secret-key
  - FCS_LOG_LEVEL=DEBUG
```

---

## 5. 部署脚本用法

脚本路径：`scripts/deploy.sh`

### 5.1 部署命令

```bash
# 单机模式
./scripts/deploy.sh standalone

# 集群模式
./scripts/deploy.sh master
./scripts/deploy.sh subnode
./scripts/deploy.sh stack

# Staging 环境
./scripts/deploy.sh staging-master
./scripts/deploy.sh staging-subnode
```

### 5.2 管理命令

```bash
# 停止容器（需指定模式，默认 standalone）
./scripts/deploy.sh down standalone
./scripts/deploy.sh down master
./scripts/deploy.sh down stack

# 查看日志（实时跟踪）
./scripts/deploy.sh logs standalone
./scripts/deploy.sh logs master

# 重启容器
./scripts/deploy.sh restart subnode

# 查看所有 flow-captcha 容器状态
./scripts/deploy.sh status
```

### 5.3 脚本做了什么

1. 创建 `data/` 数据目录
2. 复制配置模板到 `data/setting.toml`（已存在则跳过，不会覆盖已有配置）
3. 执行 `docker compose -f <compose文件> up -d --build`
4. 显示容器运行状态

---

## 6. 手工部署

不使用脚本时的手工操作步骤。

### 6.1 Standalone

```bash
mkdir -p data
cp config/setting_example.toml data/setting.toml
# 按需编辑 data/setting.toml
docker compose -f docker-compose.headed.yml up -d --build
```

### 6.2 集群 Master

```bash
mkdir -p data/master data/redis
cp config/setting_example.toml data/master/setting.toml
# 编辑 data/master/setting.toml，确认 cluster.role = "master"
docker compose -f docker-compose.cluster.master.yml up -d --build
```

### 6.3 集群 Subnode

```bash
mkdir -p data/subnode
cp config/setting_example.toml data/subnode/setting.toml
# 编辑 data/subnode/setting.toml:
#   cluster.role = "subnode"
#   cluster.master_base_url = "http://<master-ip>:8060"
#   cluster.master_cluster_key = "<与 Master 一致>"
#   cluster.node_public_base_url = "http://<本机IP>:8061"
#   cluster.node_api_key = "<自定义密钥>"
docker compose -f docker-compose.cluster.subnode.yml up -d --build
```

### 6.4 完整集群演示栈

```bash
mkdir -p data/master data/subnode data/redis
cp config/setting_example.toml data/master/setting.toml
cp config/setting_example.toml data/subnode/setting.toml
docker compose -f docker-compose.cluster.stack.yml up -d --build
```

---

## 7. Staging 环境部署

Staging 配置已预置在 `config/` 目录中，与生产环境的主要区别：

| 差异点 | Staging | 生产 |
|--------|---------|------|
| 日志级别 | DEBUG | INFO |
| Redis Key 前缀 | `fcs_staging` | `fcs` |
| 浏览器数量 (Subnode) | 3 | 按需 |
| 日志自动清理 | 每 60 分钟 | 按需 |
| 节点命名 | `staging-master` / `staging-subnode-1` | 自定义 |

### 7.1 部署 Staging Master

```bash
./scripts/deploy.sh staging-master
```

部署前编辑 `config/setting_staging_master.toml`：
- `log.redis_url` — 改为 Staging Redis 实际地址
- `cluster.master_cluster_key` — 替换为真实密钥

### 7.2 部署 Staging Subnode

```bash
./scripts/deploy.sh staging-subnode
```

部署前编辑 `config/setting_staging_subnode.toml`：
- `cluster.master_base_url` — 改为 Staging Master 实际地址
- `cluster.master_cluster_key` — 与 Master 一致
- `cluster.node_public_base_url` — 改为本机对 Master 可达的地址
- `captcha.node_name` — 多个 Subnode 时改为不同名称

### 7.3 多 Subnode 部署

每台 Subnode 机器上：

```bash
# 复制并修改配置
cp config/setting_staging_subnode.toml config/setting_staging_subnode_2.toml
# 编辑: node_name = "staging-subnode-2", node_public_base_url = "http://<本机IP>:8061"

# 指定配置启动
FCS_CONFIG_FILE=config/setting_staging_subnode_2.toml ./scripts/deploy.sh staging-subnode
```

或直接通过环境变量覆盖：

```bash
export FCS_NODE_NAME=staging-subnode-2
export FCS_CLUSTER_NODE_PUBLIC_BASE_URL=http://10.0.1.12:8061
./scripts/deploy.sh staging-subnode
```

---

## 8. 部署后验证

### 8.1 健康检查

```bash
curl http://localhost:8060/api/v1/health
# 期望: {"status":"ok"} 或类似 JSON
```

### 8.2 容器状态

```bash
./scripts/deploy.sh status

# 或直接
docker ps --filter "name=flow-captcha"
```

期望所有容器状态为 `Up` 且健康检查为 `(healthy)`。

### 8.3 登录管理后台

1. 访问 http://localhost:8060/admin
2. 使用默认账号 `admin` / `admin` 登录
3. 创建 API Key 用于调用服务

### 8.4 测试验证码解决

```bash
# 替换 <your-api-key> 为管理后台创建的 API Key
curl -X POST http://localhost:8060/api/v1/solve \
  -H "Authorization: Bearer <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{"project_id": "test", "action": "IMAGE_GENERATION"}'
```

### 8.5 集群验证（Master 模式）

```bash
# 检查已注册的 Subnode
curl http://localhost:8060/api/v1/health
# 响应中应包含集群节点信息

# 在管理后台 → 集群页面查看节点状态
```

---

## 9. 常用运维操作

### 9.1 查看日志

```bash
# 实时日志
./scripts/deploy.sh logs standalone

# 指定容器
docker logs -f flow-captcha-service --tail=200
docker logs -f flow-captcha-master --tail=200
docker logs -f flow-captcha-subnode --tail=200
```

### 9.2 重启服务

```bash
./scripts/deploy.sh restart standalone
```

### 9.3 更新部署

```bash
# 拉取最新代码后重新构建
git pull
./scripts/deploy.sh standalone  # 脚本包含 --build 参数，会重新构建镜像
```

### 9.4 修改配置

```bash
# 1. 编辑配置文件
vim data/setting.toml

# 2. 重启生效
./scripts/deploy.sh restart standalone
```

> 部分配置可通过管理后台热更新（如 `captcha_method`、`browser_count`），无需重启。

### 9.5 备份数据

```bash
# 数据目录包含 SQLite 数据库和配置
cp -r data/ data_backup_$(date +%Y%m%d)
```

### 9.6 完全清理

```bash
# 停止并移除容器
./scripts/deploy.sh down standalone

# 如需清理数据（谨慎操作）
rm -rf data/
```

---

## 10. 故障排查

### 10.1 容器启动失败

```bash
# 查看容器日志
docker logs flow-captcha-service

# 常见原因:
# - 端口 8060 被占用 → 修改 docker-compose 端口映射或释放端口
# - 配置文件格式错误 → 检查 data/setting.toml 的 TOML 语法
```

### 10.2 健康检查不通过

容器状态显示 `(unhealthy)`：

```bash
# 检查 Xvfb 虚拟显示是否启动 (headed 模式)
docker exec flow-captcha-service xdpyinfo -display :99

# 检查服务是否监听
docker exec flow-captcha-service curl -s http://127.0.0.1:8060/api/v1/health
```

### 10.3 Subnode 无法连接 Master

```bash
# 在 Subnode 容器内测试连通性
docker exec flow-captcha-subnode curl -s http://<master-ip>:8060/api/v1/health

# 检查项:
# - master_base_url 是否正确
# - master_cluster_key 是否与 Master 一致
# - 网络/防火墙是否放通 8060 端口
```

### 10.4 浏览器启动失败

```bash
# 查看 Chromium 是否安装
docker exec flow-captcha-service python -m playwright install --dry-run

# 查看 Xvfb 进程
docker exec flow-captcha-service ps aux | grep -E "Xvfb|chromium"

# 内存不足的表现: 浏览器频繁崩溃
# → 减少 browser_count 或增加容器内存限制
```

### 10.5 日志级别调整

临时调高日志级别排查问题：

```bash
# 通过环境变量覆盖，无需改配置文件
docker compose -f docker-compose.headed.yml down
FCS_LOG_LEVEL=DEBUG docker compose -f docker-compose.headed.yml up -d
```
