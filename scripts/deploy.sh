#!/usr/bin/env bash
# ============================================================
# Flow Captcha Service 部署脚本
#
# 用法:
#   ./scripts/deploy.sh standalone
#   ./scripts/deploy.sh master
#   ./scripts/deploy.sh subnode [选项]
#   ./scripts/deploy.sh stack
#   ./scripts/deploy.sh staging-master
#   ./scripts/deploy.sh staging-subnode [选项]
#   ./scripts/deploy.sh down [模��]
#   ./scripts/deploy.sh logs [模式]
#   ./scripts/deploy.sh restart [模式]
#   ./scripts/deploy.sh status
#
# Subnode 选项:
#   --master-url <url>       Master 地址
#   --cluster-key <key>      集群通信密钥
#   --node-url <url>         本节点对 Master 可达地址
#   --node-api-key <key>     节点内部认证 Key
#   --node-name <name>       节点名称
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# ── 颜色 ──

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── 用法 ──

usage() {
    cat <<'EOF'
用���: ./scripts/deploy.sh <命令> [选项]

部署命令:
  standalone                单机模式 (headed 浏览器)
  master                    ��群 Master 节点
  subnode [选项]             集�� Subnode 节点
  stack                     完整集群演示栈 (Master + Subnode + Redis)
  staging-master            Staging Master 节点
  staging-subnode [选项]     Staging Subnode 节点

Subnode / staging-subnode 选项:
  --master-url <url>        Master 地址 (FCS_CLUSTER_MASTER_BASE_URL)
  --cluster-key <key>       集群通信密钥 (FCS_CLUSTER_MASTER_CLUSTER_KEY)
  --node-url <url>          本节点对 Master 可达地址 (FCS_CLUSTER_NODE_PUBLIC_BASE_URL)
  --node-api-key <key>      节点内部认证 Key (FCS_CLUSTER_NODE_API_KEY)
  --node-name <name>        节点名称 (FCS_NODE_NAME)

管理命令:
  down [模��]                停止并移除容器 (默认 standalone)
  logs [模式]                查看容���日志 (默认 standalone)
  restart [模式]             重启容器 (默认 standalone)
  status                    查看所有 flow-captcha 容器状态

示例:
  ./scripts/deploy.sh standalone
  ./scripts/deploy.sh stack
  ./scripts/deploy.sh subnode \
    --master-url http://10.0.1.10:8060 \
    --cluster-key my-secret \
    --node-url http://10.0.1.11:8061
  ./scripts/deploy.sh down stack
EOF
}

# ── 模式解析 ──

COMPOSE_FILE=""
CONFIG_SRC=""
DATA_DIR=""
ENV_FILE=""
EXTRA_DATA_DIRS=()

# subnode 参数
ARG_MASTER_URL=""
ARG_CLUSTER_KEY=""
ARG_NODE_URL=""
ARG_NODE_API_KEY=""
ARG_NODE_NAME=""

resolve_mode() {
    local mode="$1"
    case "$mode" in
        standalone)
            COMPOSE_FILE="docker-compose.headed.yml"
            CONFIG_SRC="config/setting_example.toml"
            DATA_DIR="data"
            ;;
        master)
            COMPOSE_FILE="docker-compose.cluster.master.yml"
            CONFIG_SRC="config/setting_example.toml"
            DATA_DIR="data/master"
            ;;
        subnode)
            COMPOSE_FILE="docker-compose.cluster.subnode.yml"
            CONFIG_SRC="config/setting_example.toml"
            DATA_DIR="data/subnode"
            ENV_FILE="deploy/subnode.env.local"
            ;;
        stack)
            COMPOSE_FILE="docker-compose.cluster.stack.yml"
            CONFIG_SRC="config/setting_example.toml"
            DATA_DIR="data/master"
            EXTRA_DATA_DIRS=("data/subnode" "data/redis")
            ENV_FILE="deploy/stack.env.local"
            ;;
        staging-master)
            COMPOSE_FILE="docker-compose.cluster.master.yml"
            CONFIG_SRC="config/setting_staging_master.toml"
            DATA_DIR="data/master"
            ;;
        staging-subnode)
            COMPOSE_FILE="docker-compose.cluster.subnode.yml"
            CONFIG_SRC="config/setting_staging_subnode.toml"
            DATA_DIR="data/subnode"
            ENV_FILE="deploy/subnode.env.local"
            ;;
        *)
            log_error "未知模式: $mode"
            usage
            exit 1
            ;;
    esac
}

parse_subnode_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --master-url)
                ARG_MASTER_URL="$2"; shift 2 ;;
            --cluster-key)
                ARG_CLUSTER_KEY="$2"; shift 2 ;;
            --node-url)
                ARG_NODE_URL="$2"; shift 2 ;;
            --node-api-key)
                ARG_NODE_API_KEY="$2"; shift 2 ;;
            --node-name)
                ARG_NODE_NAME="$2"; shift 2 ;;
            *)
                log_error "未知选项: $1"
                usage
                exit 1 ;;
        esac
    done
}

# ── 配置与 env 文件 ──

ensure_data_dirs() {
    mkdir -p "$DATA_DIR"
    for dir in "${EXTRA_DATA_DIRS[@]}"; do
        mkdir -p "$dir"
    done
    mkdir -p deploy
}

ensure_config() {
    local target="$DATA_DIR/setting.toml"
    if [[ -f "$target" ]]; then
        log_info "配置已存在: $target (跳过)"
    else
        if [[ ! -f "$CONFIG_SRC" ]]; then
            log_error "配置源不存在: $CONFIG_SRC"
            exit 1
        fi
        cp "$CONFIG_SRC" "$target"
        log_info "已复制配置: $CONFIG_SRC → $target"
    fi
}

# 为 subnode 生成 env 文件（合并命��行参数）
generate_subnode_env() {
    local env_path="$1"
    local example_path="deploy/subnode.env.example"

    if [[ -f "$env_path" ]] && [[ -z "$ARG_MASTER_URL" ]] && [[ -z "$ARG_CLUSTER_KEY" ]] \
       && [[ -z "$ARG_NODE_URL" ]] && [[ -z "$ARG_NODE_API_KEY" ]] && [[ -z "$ARG_NODE_NAME" ]]; then
        log_info "Env 文件已存在: $env_path (跳过)"
        return
    fi

    # 如果有命令行参数或 env 文件不存在，生成/更新
    local master_url="${ARG_MASTER_URL:-http://host.docker.internal:8060}"
    local cluster_key="${ARG_CLUSTER_KEY:-replace-with-master-cluster-key}"
    local node_url="${ARG_NODE_URL:-http://host.docker.internal:8061}"
    local node_api_key="${ARG_NODE_API_KEY:-replace-with-node-internal-key}"
    local node_name="${ARG_NODE_NAME:-subnode-1}"

    cat > "$env_path" <<EOF
# 自动生成 — $(date '+%Y-%m-%d %H:%M:%S')
FCS_NODE_NAME=${node_name}
FCS_CLUSTER_MASTER_BASE_URL=${master_url}
FCS_CLUSTER_MASTER_CLUSTER_KEY=${cluster_key}
FCS_CLUSTER_NODE_PUBLIC_BASE_URL=${node_url}
FCS_CLUSTER_NODE_API_KEY=${node_api_key}
FCS_CLUSTER_NODE_WEIGHT=100
FCS_CLUSTER_NODE_MAX_CONCURRENCY=0
FCS_CLUSTER_HEARTBEAT_INTERVAL_SECONDS=15
EOF
    log_info "已生成 env: $env_path"

    # 检查是否仍有占位值
    if [[ "$cluster_key" == "replace-with-master-cluster-key" ]]; then
        log_warn "cluster_key 仍为占位值，请编辑 $env_path 后重新部署"
    fi
}

# 为 stack 生成 env 文件
generate_stack_env() {
    local env_path="$1"

    if [[ -f "$env_path" ]]; then
        log_info "Env 文件已存在: $env_path (跳过)"
        return
    fi

    if [[ -f "deploy/stack.env.example" ]]; then
        cp "deploy/stack.env.example" "$env_path"
    else
        cat > "$env_path" <<EOF
# 自动生成 — $(date '+%Y-%m-%d %H:%M:%S')
FCS_MASTER_NODE_NAME=master-1
FCS_LOG_LEVEL=INFO
FCS_LOG_STORAGE_BACKEND=redis
FCS_LOG_REDIS_URL=redis://flow-captcha-redis:6379/0
FCS_LOG_REDIS_KEY_PREFIX=fcs
FCS_LOG_REDIS_MAX_ENTRIES=20000
FCS_LOG_STARTUP_CLEAR_ON_BOOT=true
FCS_SUBNODE_NODE_NAME=subnode-1
FCS_CLUSTER_MASTER_BASE_URL=http://flow-captcha-master:8060
FCS_CLUSTER_MASTER_CLUSTER_KEY=replace-with-master-cluster-key
FCS_CLUSTER_NODE_PUBLIC_BASE_URL=http://flow-captcha-subnode:8060
FCS_CLUSTER_NODE_API_KEY=replace-with-node-internal-key
FCS_CLUSTER_NODE_WEIGHT=100
FCS_CLUSTER_NODE_MAX_CONCURRENCY=0
FCS_CLUSTER_HEARTBEAT_INTERVAL_SECONDS=15
EOF
    fi
    log_info "已���成 env: $env_path"

    if grep -q "replace-with-master-cluster-key" "$env_path" 2>/dev/null; then
        log_warn "cluster_key 仍为占位值，请编辑 $env_path 后重新部署"
    fi
}

# ── 健康检查等待 ──

wait_healthy() {
    local compose_file="$1"
    local max_wait=120   # 最多等 120 秒
    local interval=3
    local elapsed=0

    log_info "等待健康检查通过 (最多 ${max_wait}s)..."

    while [[ $elapsed -lt $max_wait ]]; do
        # 获取所有服务的健康状态
        local output
        output=$(docker compose -f "$compose_file" ps --format json 2>/dev/null || true)

        if [[ -z "$output" ]]; then
            sleep "$interval"
            elapsed=$((elapsed + interval))
            continue
        fi

        # 检查是否有 starting 状态的容器
        local has_starting=false
        local has_unhealthy=false

        # docker compose ps --format json 每行一个 JSON 对象
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            local health
            health=$(echo "$line" | grep -oP '"Health"\s*:\s*"\K[^"]+' 2>/dev/null || true)
            local state
            state=$(echo "$line" | grep -oP '"State"\s*:\s*"\K[^"]+' 2>/dev/null || true)

            if [[ "$state" == "exited" ]] || [[ "$state" == "dead" ]]; then
                has_unhealthy=true
            elif [[ "$health" == "starting" ]] || [[ -z "$health" && "$state" == "running" ]]; then
                has_starting=true
            elif [[ "$health" == "unhealthy" ]]; then
                has_unhealthy=true
            fi
        done <<< "$output"

        if [[ "$has_unhealthy" == true ]]; then
            log_error "存在异常容器"
            docker compose -f "$compose_file" ps
            return 1
        fi

        if [[ "$has_starting" == false ]]; then
            # 所有容器都 healthy
            log_info "所有容器健康检查通过"
            return 0
        fi

        printf "  等待中... %ds\r" "$elapsed"
        sleep "$interval"
        elapsed=$((elapsed + interval))
    done

    log_warn "等待超时 (${max_wait}s)，部分容器可能仍未就绪"
    docker compose -f "$compose_file" ps
    return 1
}

# ── 打印访问地址 ──

print_endpoints() {
    local mode="$1"
    echo ""
    echo -e "${CYAN}════════════════════════════════════════${NC}"

    case "$mode" in
        standalone)
            echo -e "  用户门户:   ${GREEN}http://localhost:8060/${NC}"
            echo -e "  管理后台:   ${GREEN}http://localhost:8060/admin${NC}"
            echo -e "  健康检查:   ${GREEN}http://localhost:8060/api/v1/health${NC}"
            ;;
        master|staging-master)
            echo -e "  用户门户:   ${GREEN}http://localhost:8060/${NC}"
            echo -e "  管理后台:   ${GREEN}http://localhost:8060/admin${NC}"
            echo -e "  健康检查:   ${GREEN}http://localhost:8060/api/v1/health${NC}"
            ;;
        subnode|staging-subnode)
            echo -e "  状态页:     ${GREEN}http://localhost:8061/${NC}"
            echo -e "  管理后台:   ${GREEN}http://localhost:8061/admin${NC}"
            echo -e "  健康检查:   ${GREEN}http://localhost:8061/api/v1/health${NC}"
            ;;
        stack)
            echo -e "  Master 门户:    ${GREEN}http://localhost:8060/${NC}"
            echo -e "  Master ��台:    ${GREEN}http://localhost:8060/admin${NC}"
            echo -e "  Master 健康:    ${GREEN}http://localhost:8060/api/v1/health${NC}"
            echo -e "  Subnode 状态:   ${GREEN}http://localhost:8061/${NC}"
            echo -e "  Subnode 健康:   ${GREEN}http://localhost:8061/api/v1/health${NC}"
            ;;
    esac

    echo -e "${CYAN}══════���═════════════════════════════════${NC}"
    echo ""
}

# ── 核心部署 ──

do_deploy() {
    local mode="$1"
    shift
    resolve_mode "$mode"

    # 解析 subnode 额外参数
    if [[ "$mode" == "subnode" || "$mode" == "staging-subnode" ]]; then
        parse_subnode_args "$@"
    fi

    log_info "���署模式: ${CYAN}$mode${NC}"
    log_info "Compose:  $COMPOSE_FILE"
    log_info "配置源:   $CONFIG_SRC"

    # 1. 创建目录
    ensure_data_dirs

    # 2. 复制配置文件
    ensure_config

    # stack 模式下 subnode 也需要配置
    if [[ "$mode" == "stack" ]]; then
        if [[ ! -f "data/subnode/setting.toml" ]]; then
            cp "$CONFIG_SRC" "data/subnode/setting.toml"
            log_info "已复制配��: $CONFIG_SRC → data/subnode/setting.toml"
        fi
    fi

    # 3. 生成 env 文件
    if [[ "$mode" == "subnode" || "$mode" == "staging-subnode" ]]; then
        generate_subnode_env "$ENV_FILE"
    elif [[ "$mode" == "stack" ]]; then
        generate_stack_env "$ENV_FILE"
    fi

    # 4. 构建并启动
    log_info "开始构建并启动..."
    local compose_cmd=(docker compose -f "$COMPOSE_FILE")
    if [[ -n "$ENV_FILE" ]] && [[ -f "$ENV_FILE" ]]; then
        compose_cmd=(docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE")
        log_info "Env 文件: $ENV_FILE"
    fi
    "${compose_cmd[@]}" up -d --build

    # 5. 等待健康检查
    if wait_healthy "$COMPOSE_FILE"; then
        log_info "部署完成!"
    else
        log_warn "部署已启动，但健康检查未完全通过，请检查日志"
    fi

    docker compose -f "$COMPOSE_FILE" ps

    # 6. 打印访问地址
    print_endpoints "$mode"
}

do_down() {
    local mode="${1:-standalone}"
    resolve_mode "$mode"
    log_info "停止模���: ${CYAN}$mode${NC}"
    local compose_cmd=(docker compose -f "$COMPOSE_FILE")
    if [[ -n "$ENV_FILE" ]] && [[ -f "$ENV_FILE" ]]; then
        compose_cmd=(docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE")
    fi
    "${compose_cmd[@]}" down
    log_info "已停止"
}

do_logs() {
    local mode="${1:-standalone}"
    resolve_mode "$mode"
    docker compose -f "$COMPOSE_FILE" logs -f --tail=100
}

do_restart() {
    local mode="${1:-standalone}"
    resolve_mode "$mode"
    log_info "重启模式: ${CYAN}$mode${NC}"
    local compose_cmd=(docker compose -f "$COMPOSE_FILE")
    if [[ -n "$ENV_FILE" ]] && [[ -f "$ENV_FILE" ]]; then
        compose_cmd=(docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE")
    fi
    "${compose_cmd[@]}" restart
    "${compose_cmd[@]}" ps
}

do_status() {
    docker ps --filter "name=flow-captcha" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
}

# ── 入口 ──

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

CMD="$1"
shift

case "$CMD" in
    standalone|master|subnode|stack|staging-master|staging-subnode)
        do_deploy "$CMD" "$@"
        ;;
    down)
        do_down "${1:-standalone}"
        ;;
    logs)
        do_logs "${1:-standalone}"
        ;;
    restart)
        do_restart "${1:-standalone}"
        ;;
    status)
        do_status
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        log_error "未知命令: $CMD"
        usage
        exit 1
        ;;
esac
