#!/bin/bash

#########################################################
# methods.sh - Airflow Cluster (Scheduler/Worker) + Spark Standalone only
#########################################################

METHODS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_JSON="$METHODS_DIR/env.json"

log_info() { echo "[$(date '+%F %T')] [INFO] $1" >> "$ACCESS_LOG"; }
log_error() { echo "[$(date '+%F %T')] [ERROR] $1" >> "$ERROR_LOG"; }
get_current_host() { hostname -I | awk '{print $1}'; }
get_service_value() {
    local service="$1" key="$2" host
    host=$(get_current_host)
    jq -r --arg host "$host" --arg service "$service" --arg key "$key" '
      .servers[] | select(.hostname==$host) | .services[] | select(.name==$service) | .[$key] // empty
    ' "$ENV_JSON"
}
require_value() { local n="$1" v="$2"; [[ -n "$v" && "$v" != "null" && "$v" != CHANGE_ME* ]] || { log_error "Missing required config: $n (still set to a CHANGE_ME_ placeholder in env.json?)"; return 1; }; }
require_matching_secret() { local n="$1" v="$2" label="$3"; [[ -n "$v" && "$v" != "null" && "$v" != COPY_FROM_* && "$v" != CHANGE_ME* ]] || { log_error "$n is required and must be IDENTICAL across every node in the cluster ($label)."; return 1; }; }
get_sudo() { if [[ $EUID -eq 0 ]]; then SUDO=""; elif command -v sudo >/dev/null 2>&1; then SUDO="sudo"; else return 1; fi; }
get_pkg_mgr() { if command -v dnf >/dev/null 2>&1; then PKG_MGR=dnf; elif command -v yum >/dev/null 2>&1; then PKG_MGR=yum; elif command -v apt-get >/dev/null 2>&1; then PKG_MGR=apt; else return 1; fi; }
wait_for_tcp_port() { local host="$1" port="$2" tries="${3:-30}"; while [[ $tries -gt 0 ]]; do (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null && { exec 3<&- 3>&-; return 0; }; sleep 2; tries=$((tries - 1)); done; return 1; }
url_encode() { local s="$1" out="" c i; for ((i = 0; i < ${#s}; i++)); do c="${s:$i:1}"; case "$c" in [a-zA-Z0-9.~_-]) out+="$c" ;; *) printf -v hex '%02X' "'$c"; out+="%$hex" ;; esac; done; echo "$out"; }
# Run a command as $1 (the rest of the args are the command + its args).
# AIRFLOW_HOME/PYTHON_BINARIES get chown'd to SERVICE_USER before the
# one-off `airflow db migrate`/`db check` calls, but those calls used to
# run as whoever SSH'd in to execute this script -- which only "worked"
# when SERVICE_USER happened to match that SSH user. Route through this
# so the CLI runs as the same user that will own (and later run, via
# systemd) the resulting files.
_airflow_run_as() {
    local user="$1"; shift
    if [[ "$(id -un)" == "$user" ]]; then
        "$@"
    elif [[ $EUID -eq 0 ]]; then
        su -s /bin/bash -c "$(printf '%q ' "$@")" "$user"
    else
        $SUDO -u "$user" "$@"
    fi
}

#########################################################
# AIRFLOW - shared internals (prefixed _airflow_ to avoid
# colliding with any other service's functions in this file)
#########################################################
_airflow_build_deps() {
    case "$PKG_MGR" in
        apt) echo "build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev libbz2-dev liblzma-dev default-libmysqlclient-dev libpq-dev pkg-config" ;;
        dnf|yum) echo "gcc gcc-c++ make zlib-devel ncurses-devel gdbm-devel nss-devel openssl-devel readline-devel libffi-devel sqlite-devel bzip2-devel xz-devel mysql-devel postgresql-devel pkgconfig" ;;
    esac
}
_airflow_install_deps() {
    local deps; deps=$(_airflow_build_deps)
    if [[ "$PKG_MGR" == apt ]]; then
        $SUDO apt-get update -y >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
        # shellcheck disable=SC2086
        $SUDO apt-get install -y jq wget tar $deps >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
    else
        # shellcheck disable=SC2086
        $SUDO "$PKG_MGR" install -y jq wget tar $deps >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
    fi
}
_airflow_ensure_user() { id "$1" >/dev/null 2>&1 || $SUDO useradd --system --home-dir "$2" --shell /usr/sbin/nologin "$1"; }
_airflow_build_python() {
    local version="$1" source_dir="$2" prefix="$3" bin_path="${3}/bin/python${1%.*}"
    [[ -x "$bin_path" ]] && { log_info "Python ${version} already installed at ${bin_path} -- skipping build."; return 0; }
    mkdir -p "$source_dir"; cd "$source_dir" || return 1
    local tarball="Python-${version}.tgz" url="https://www.python.org/ftp/python/${version}/Python-${version}.tgz"
    [[ -f "$tarball" ]] || wget -q -O "$tarball" "$url" >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || { log_error "Python source download failed: $url"; return 1; }
    tar -xzf "$tarball" >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
    cd "Python-${version}" || return 1
    ./configure --enable-optimizations --prefix="$prefix" >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
    make -j"$(nproc)" altinstall >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
    [[ -x "$bin_path" ]] || { log_error "Python build finished but $bin_path was not produced"; return 1; }
    log_info "Python ${version} installed to ${prefix}"
}
_airflow_pip_install() {
    local pybin="$1" version="$2" user_extras="$3" merged extra
    merged=$(echo "celery,postgres,redis,${user_extras}" | tr ',' '\n' | sed '/^$/d' | awk '!seen[$0]++' | paste -sd, -)
    "$pybin" -m pip install --upgrade pip >>"$ACCESS_LOG" 2>>"$ERROR_LOG"
    "$pybin" -m pip install "apache-airflow==${version}" >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
    IFS=',' read -ra EXTRA_LIST <<< "$merged"
    for extra in "${EXTRA_LIST[@]}"; do
        extra=$(echo "$extra" | xargs); [[ -z "$extra" ]] && continue
        "$pybin" -m pip install "apache-airflow[${extra}]==${version}" >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
    done
    log_info "Apache Airflow ${version} installed (extras: ${merged})"
}
_airflow_conn_strings() {
    SQL_ALCHEMY_CONN="postgresql+psycopg2://$(url_encode "$DB_USER"):$(url_encode "$DB_PASSWORD")@${DB_HOST}:${DB_PORT}/${DB_NAME}"
    RESULT_BACKEND="db+postgresql://$(url_encode "$DB_USER"):$(url_encode "$DB_PASSWORD")@${DB_HOST}:${DB_PORT}/${DB_NAME}"
    if [[ -n "$REDIS_PASSWORD" ]]; then
        BROKER_URL="redis://:$(url_encode "$REDIS_PASSWORD")@${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}"
    else
        BROKER_URL="redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}"
    fi
}
_airflow_create_service_unit() {
    local name="$1" description="$2" svc_user="$3" airflow_home="$4" python_bin_dir="$5" exec_cmd="$6" extra_env="$7" unit_file="/etc/systemd/system/${1}.service"
    {
        echo "[Unit]"; echo "Description=${description}"; echo "Requires=network-online.target"; echo "After=network-online.target"; echo
        echo "[Service]"; echo "Type=simple"; echo "User=${svc_user}"; echo "Group=${svc_user}"
        echo "Environment=\"AIRFLOW_HOME=${airflow_home}\""
        echo "Environment=\"PATH=${python_bin_dir}:/usr/bin:/bin\""
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            echo "Environment=\"${line%%=*}=${line#*=}\""
        done <<< "$extra_env"
        echo "ExecStart=${exec_cmd}"; echo "Restart=always"; echo "RestartSec=5s"; echo
        echo "[Install]"; echo "WantedBy=multi-user.target"
    } | $SUDO tee "$unit_file" >/dev/null
    $SUDO systemctl daemon-reload
    $SUDO systemctl enable "${name}.service" >>"$ACCESS_LOG" 2>>"$ERROR_LOG"
}

#########################################################
# AIRFLOW SCHEDULER (control node: apiserver + scheduler +
# dag-processor + triggerer, external Postgres + Redis)
#########################################################
airflow_scheduler_init_vars() {
    local S="Airflow-Scheduler"
    DISK_PATH=$(get_service_value "$S" DiskPath); require_value DiskPath "$DISK_PATH" || return 1
    BASE_DIR="${DISK_PATH%/}"; SOURCE="$BASE_DIR/softwares"
    PYTHON_BINARIES="$BASE_DIR/apps/python"; PYTHON_BIN_DIR="$PYTHON_BINARIES/bin"
    AIRFLOW_HOME="$BASE_DIR/apps/airflow"
    ACCESS_LOG="$BASE_DIR/airflow_scheduler.access.log"; ERROR_LOG="$BASE_DIR/airflow_scheduler.error.log"
    mkdir -p "$SOURCE" "$PYTHON_BINARIES" "$AIRFLOW_HOME"; touch "$ACCESS_LOG" "$ERROR_LOG"

    SERVICE_USER=$(get_service_value "$S" SERVICE_USER); SERVICE_USER="${SERVICE_USER:-airflow}"
    PYTHON_VERSION=$(get_service_value "$S" PYTHON_VERSION); PYTHON_VERSION="${PYTHON_VERSION:-3.10.10}"
    AIRFLOW_VERSION=$(get_service_value "$S" AIRFLOW_VERSION); AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.1.2}"
    AIRFLOW_EXTRAS=$(get_service_value "$S" AIRFLOW_EXTRAS)
    API_PORT=$(get_service_value "$S" API_PORT); API_PORT="${API_PORT:-8080}"
    JWT_ISSUER=$(get_service_value "$S" JWT_ISSUER); JWT_ISSUER="${JWT_ISSUER:-airflow-api}"
    DAGS_FOLDER=$(get_service_value "$S" DAGS_FOLDER); require_value DAGS_FOLDER "$DAGS_FOLDER" || return 1

    DB_HOST=$(get_service_value "$S" DB_HOST); require_value DB_HOST "$DB_HOST" || return 1
    DB_PORT=$(get_service_value "$S" DB_PORT); DB_PORT="${DB_PORT:-5432}"
    DB_NAME=$(get_service_value "$S" DB_NAME); DB_NAME="${DB_NAME:-airflow}"
    DB_USER=$(get_service_value "$S" DB_USER); DB_USER="${DB_USER:-airflow}"
    DB_PASSWORD=$(get_service_value "$S" DB_PASSWORD); require_value DB_PASSWORD "$DB_PASSWORD" || return 1

    REDIS_HOST=$(get_service_value "$S" REDIS_HOST); require_value REDIS_HOST "$REDIS_HOST" || return 1
    REDIS_PORT=$(get_service_value "$S" REDIS_PORT); REDIS_PORT="${REDIS_PORT:-6379}"
    REDIS_DB=$(get_service_value "$S" REDIS_DB); REDIS_DB="${REDIS_DB:-0}"
    REDIS_PASSWORD=$(get_service_value "$S" REDIS_PASSWORD)

    FERNET_KEY=$(get_service_value "$S" FERNET_KEY)
    require_matching_secret FERNET_KEY "$FERNET_KEY" "must be pre-generated by the user and set IDENTICALLY on the scheduler and every Airflow-Worker block in env.json -- it is no longer auto-generated" || return 1
    WEBSERVER_SECRET_KEY=$(get_service_value "$S" WEBSERVER_SECRET_KEY)
    require_matching_secret WEBSERVER_SECRET_KEY "$WEBSERVER_SECRET_KEY" "must be pre-generated by the user and set IDENTICALLY on the scheduler and every Airflow-Worker block in env.json -- it is no longer auto-generated" || return 1

    ADMIN_USERNAME=$(get_service_value "$S" ADMIN_USERNAME); ADMIN_USERNAME="${ADMIN_USERNAME:-admin}"
    ADMIN_PASSWORD=$(get_service_value "$S" ADMIN_PASSWORD); require_value ADMIN_PASSWORD "$ADMIN_PASSWORD" || return 1
    INSTALL_FLOWER=$(get_service_value "$S" INSTALL_FLOWER); INSTALL_FLOWER="${INSTALL_FLOWER:-0}"
    FLOWER_PORT=$(get_service_value "$S" FLOWER_PORT); FLOWER_PORT="${FLOWER_PORT:-5555}"

    _airflow_conn_strings
    COMMON_ENV="AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__DAGS_FOLDER=${DAGS_FOLDER}
AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${SQL_ALCHEMY_CONN}
AIRFLOW__CELERY__BROKER_URL=${BROKER_URL}
AIRFLOW__CELERY__RESULT_BACKEND=${RESULT_BACKEND}
AIRFLOW__WEBSERVER__SECRET_KEY=${WEBSERVER_SECRET_KEY}
AIRFLOW__API_AUTH__JWT_ISSUER=${JWT_ISSUER}"
}
airflow_scheduler_install() {
    get_sudo || { echo "sudo/root required" >&2; return 1; }
    get_pkg_mgr || { echo "Unsupported package manager" >&2; return 1; }
    airflow_scheduler_init_vars || return 1

    wait_for_tcp_port "$DB_HOST" "$DB_PORT" 5 || { log_error "Postgres metadata DB not reachable at ${DB_HOST}:${DB_PORT}"; return 1; }
    wait_for_tcp_port "$REDIS_HOST" "$REDIS_PORT" 5 || { log_error "Redis broker not reachable at ${REDIS_HOST}:${REDIS_PORT}"; return 1; }

    _airflow_install_deps || return 1
    _airflow_ensure_user "$SERVICE_USER" "$BASE_DIR" || return 1
    mkdir -p "$DAGS_FOLDER" 2>/dev/null || { $SUDO mkdir -p "$DAGS_FOLDER"; $SUDO chown "$SERVICE_USER:$SERVICE_USER" "$DAGS_FOLDER" 2>/dev/null; }

    _airflow_build_python "$PYTHON_VERSION" "$SOURCE" "$PYTHON_BINARIES" || return 1
    $SUDO ln -sfn "${PYTHON_BINARIES}/bin/python${PYTHON_VERSION%.*}" /usr/bin/python

    local PYTHON_EXE="${PYTHON_BIN_DIR}/python${PYTHON_VERSION%.*}" AIRFLOW_EXE="${PYTHON_BIN_DIR}/airflow"
    if [[ ! -x "$AIRFLOW_EXE" ]]; then _airflow_pip_install "$PYTHON_EXE" "$AIRFLOW_VERSION" "$AIRFLOW_EXTRAS" || return 1; fi

    $SUDO tee /etc/profile.d/airflow.sh >/dev/null <<EOF
export AIRFLOW_HOME=${AIRFLOW_HOME}
export PATH=${PYTHON_BIN_DIR}:\$PATH
EOF
    [[ "$SERVICE_USER" != root ]] && $SUDO chown -R "$SERVICE_USER:$SERVICE_USER" "$BASE_DIR"

    _airflow_run_as "$SERVICE_USER" env \
        AIRFLOW_HOME="$AIRFLOW_HOME" AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$SQL_ALCHEMY_CONN" AIRFLOW__CORE__FERNET_KEY="$FERNET_KEY" \
        _AIRFLOW_WWW_USER_USERNAME="$ADMIN_USERNAME" _AIRFLOW_WWW_USER_PASSWORD="$ADMIN_PASSWORD" \
        "$AIRFLOW_EXE" db migrate >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || { log_error "airflow db migrate failed"; return 1; }
    [[ "$SERVICE_USER" != root ]] && $SUDO chown -R "$SERVICE_USER:$SERVICE_USER" "$AIRFLOW_HOME"

    _airflow_create_service_unit "airflow-apiserver" "Airflow API Server" "$SERVICE_USER" "$AIRFLOW_HOME" "$PYTHON_BIN_DIR" "${AIRFLOW_EXE} api-server --port ${API_PORT}" "$COMMON_ENV" || return 1
    _airflow_create_service_unit "airflow-dagprocessor" "Airflow DAG Processor" "$SERVICE_USER" "$AIRFLOW_HOME" "$PYTHON_BIN_DIR" "${AIRFLOW_EXE} dag-processor" "$COMMON_ENV" || return 1
    _airflow_create_service_unit "airflow-scheduler" "Airflow Scheduler" "$SERVICE_USER" "$AIRFLOW_HOME" "$PYTHON_BIN_DIR" "${AIRFLOW_EXE} scheduler" "$COMMON_ENV" || return 1
    _airflow_create_service_unit "airflow-triggerer" "Airflow Triggerer" "$SERVICE_USER" "$AIRFLOW_HOME" "$PYTHON_BIN_DIR" "${AIRFLOW_EXE} triggerer" "$COMMON_ENV" || return 1

    local SERVICES="airflow-apiserver airflow-scheduler airflow-dagprocessor airflow-triggerer" svc
    if [[ "$INSTALL_FLOWER" == 1 ]]; then
        _airflow_create_service_unit "airflow-flower" "Airflow Flower" "$SERVICE_USER" "$AIRFLOW_HOME" "$PYTHON_BIN_DIR" "${AIRFLOW_EXE} celery flower --port ${FLOWER_PORT}" "$COMMON_ENV" || return 1
        SERVICES="$SERVICES airflow-flower"
    fi
    for svc in $SERVICES; do $SUDO systemctl restart "${svc}.service" || { log_error "Failed to start ${svc}.service"; return 1; }; done

    wait_for_tcp_port "127.0.0.1" "$API_PORT" 30 || { log_error "Airflow API server did not open port ${API_PORT}"; return 1; }
    log_info "Airflow control node installed: apiserver/scheduler/dag-processor/triggerer on port ${API_PORT}, DAGS_FOLDER=${DAGS_FOLDER}"
}

#########################################################
# AIRFLOW WORKER (celery worker, same external DB/Redis/
# FERNET_KEY/DAGS_FOLDER as the scheduler)
#########################################################
airflow_worker_init_vars() {
    local S="Airflow-Worker"
    DISK_PATH=$(get_service_value "$S" DiskPath); require_value DiskPath "$DISK_PATH" || return 1
    BASE_DIR="${DISK_PATH%/}"; SOURCE="$BASE_DIR/softwares"
    PYTHON_BINARIES="$BASE_DIR/apps/python"; PYTHON_BIN_DIR="$PYTHON_BINARIES/bin"
    AIRFLOW_HOME="$BASE_DIR/apps/airflow"
    ACCESS_LOG="$BASE_DIR/airflow_worker.access.log"; ERROR_LOG="$BASE_DIR/airflow_worker.error.log"
    mkdir -p "$SOURCE" "$PYTHON_BINARIES" "$AIRFLOW_HOME"; touch "$ACCESS_LOG" "$ERROR_LOG"

    SERVICE_USER=$(get_service_value "$S" SERVICE_USER); SERVICE_USER="${SERVICE_USER:-airflow}"
    PYTHON_VERSION=$(get_service_value "$S" PYTHON_VERSION); PYTHON_VERSION="${PYTHON_VERSION:-3.10.10}"
    AIRFLOW_VERSION=$(get_service_value "$S" AIRFLOW_VERSION); AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.1.2}"
    AIRFLOW_EXTRAS=$(get_service_value "$S" AIRFLOW_EXTRAS)
    DAGS_FOLDER=$(get_service_value "$S" DAGS_FOLDER); require_value DAGS_FOLDER "$DAGS_FOLDER" || return 1

    DB_HOST=$(get_service_value "$S" DB_HOST); require_value DB_HOST "$DB_HOST" || return 1
    DB_PORT=$(get_service_value "$S" DB_PORT); DB_PORT="${DB_PORT:-5432}"
    DB_NAME=$(get_service_value "$S" DB_NAME); DB_NAME="${DB_NAME:-airflow}"
    DB_USER=$(get_service_value "$S" DB_USER); DB_USER="${DB_USER:-airflow}"
    DB_PASSWORD=$(get_service_value "$S" DB_PASSWORD); require_value DB_PASSWORD "$DB_PASSWORD" || return 1

    REDIS_HOST=$(get_service_value "$S" REDIS_HOST); require_value REDIS_HOST "$REDIS_HOST" || return 1
    REDIS_PORT=$(get_service_value "$S" REDIS_PORT); REDIS_PORT="${REDIS_PORT:-6379}"
    REDIS_DB=$(get_service_value "$S" REDIS_DB); REDIS_DB="${REDIS_DB:-0}"
    REDIS_PASSWORD=$(get_service_value "$S" REDIS_PASSWORD)

    FERNET_KEY=$(get_service_value "$S" FERNET_KEY)
    require_matching_secret FERNET_KEY "$FERNET_KEY" "must be identical to the Airflow-Scheduler node's value" || return 1
    WEBSERVER_SECRET_KEY=$(get_service_value "$S" WEBSERVER_SECRET_KEY)
    require_matching_secret WEBSERVER_SECRET_KEY "$WEBSERVER_SECRET_KEY" "must be identical to the Airflow-Scheduler node's value" || return 1

    WORKER_QUEUES=$(get_service_value "$S" WORKER_QUEUES); WORKER_QUEUES="${WORKER_QUEUES:-default}"
    WORKER_CONCURRENCY=$(get_service_value "$S" WORKER_CONCURRENCY); WORKER_CONCURRENCY="${WORKER_CONCURRENCY:-16}"

    _airflow_conn_strings
    COMMON_ENV="AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__DAGS_FOLDER=${DAGS_FOLDER}
AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${SQL_ALCHEMY_CONN}
AIRFLOW__CELERY__BROKER_URL=${BROKER_URL}
AIRFLOW__CELERY__RESULT_BACKEND=${RESULT_BACKEND}
AIRFLOW__WEBSERVER__SECRET_KEY=${WEBSERVER_SECRET_KEY}"
    WORKER_SVC_NAME="airflow-worker-$(hostname -s | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9-')"
}
airflow_worker_install() {
    get_sudo || { echo "sudo/root required" >&2; return 1; }
    get_pkg_mgr || { echo "Unsupported package manager" >&2; return 1; }
    airflow_worker_init_vars || return 1

    wait_for_tcp_port "$DB_HOST" "$DB_PORT" 5 || { log_error "Postgres metadata DB not reachable at ${DB_HOST}:${DB_PORT}"; return 1; }
    wait_for_tcp_port "$REDIS_HOST" "$REDIS_PORT" 5 || { log_error "Redis broker not reachable at ${REDIS_HOST}:${REDIS_PORT}"; return 1; }
    [[ -d "$DAGS_FOLDER" ]] || log_error "DAGS_FOLDER '${DAGS_FOLDER}' does not exist on this node yet -- mount it before the worker starts or it will find no DAG code."

    _airflow_install_deps || return 1
    _airflow_ensure_user "$SERVICE_USER" "$BASE_DIR" || return 1

    _airflow_build_python "$PYTHON_VERSION" "$SOURCE" "$PYTHON_BINARIES" || return 1
    $SUDO ln -sfn "${PYTHON_BINARIES}/bin/python${PYTHON_VERSION%.*}" /usr/bin/python

    local PYTHON_EXE="${PYTHON_BIN_DIR}/python${PYTHON_VERSION%.*}" AIRFLOW_EXE="${PYTHON_BIN_DIR}/airflow"
    if [[ ! -x "$AIRFLOW_EXE" ]]; then _airflow_pip_install "$PYTHON_EXE" "$AIRFLOW_VERSION" "$AIRFLOW_EXTRAS" || return 1; fi

    $SUDO tee /etc/profile.d/airflow.sh >/dev/null <<EOF
export AIRFLOW_HOME=${AIRFLOW_HOME}
export PATH=${PYTHON_BIN_DIR}:\$PATH
EOF
    [[ "$SERVICE_USER" != root ]] && $SUDO chown -R "$SERVICE_USER:$SERVICE_USER" "$PYTHON_BINARIES" "$AIRFLOW_HOME"

    _airflow_run_as "$SERVICE_USER" env \
        AIRFLOW_HOME="$AIRFLOW_HOME" AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$SQL_ALCHEMY_CONN" AIRFLOW__CORE__FERNET_KEY="$FERNET_KEY" \
        "$AIRFLOW_EXE" db check >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || { log_error "airflow db check failed -- unreachable DB or FERNET_KEY mismatch with the scheduler"; return 1; }

    _airflow_create_service_unit "$WORKER_SVC_NAME" "Airflow Celery Worker ($(hostname -s))" "$SERVICE_USER" "$AIRFLOW_HOME" "$PYTHON_BIN_DIR" \
        "${AIRFLOW_EXE} celery worker --queues ${WORKER_QUEUES} --concurrency ${WORKER_CONCURRENCY}" "$COMMON_ENV" || return 1

    $SUDO systemctl restart "${WORKER_SVC_NAME}.service" || { log_error "Failed to start ${WORKER_SVC_NAME}.service"; return 1; }
    sleep 3
    $SUDO systemctl is-active --quiet "${WORKER_SVC_NAME}.service" || { log_error "${WORKER_SVC_NAME}.service failed to stay up -- check journalctl -u ${WORKER_SVC_NAME}"; return 1; }
    log_info "Airflow worker installed and running: ${WORKER_SVC_NAME}, queues=${WORKER_QUEUES}, concurrency=${WORKER_CONCURRENCY}"
}

#########################################################
# SPARK STANDALONE (master + worker on the same node)
#########################################################
spark_standalone_init_vars() {
    local S="Spark-Standalone"
    DISK_PATH=$(get_service_value "$S" DiskPath); require_value DiskPath "$DISK_PATH" || return 1
    BASE_DIR="${DISK_PATH%/}"; SOURCE="$BASE_DIR/softwares"; SPARK_HOME="$BASE_DIR/apps/spark"
    ACCESS_LOG="$BASE_DIR/spark_standalone.access.log"; ERROR_LOG="$BASE_DIR/spark_standalone.error.log"
    mkdir -p "$SOURCE"; touch "$ACCESS_LOG" "$ERROR_LOG"

    SPARK_VERSION=$(get_service_value "$S" SPARK_VERSION); SPARK_VERSION="${SPARK_VERSION:-4.0.3}"
    HADOOP_VARIANT=$(get_service_value "$S" HADOOP_VARIANT); HADOOP_VARIANT="${HADOOP_VARIANT:-hadoop3}"
    SERVICE_USER=$(get_service_value "$S" SERVICE_USER); SERVICE_USER="${SERVICE_USER:-spark}"
    MASTER_IP=$(get_service_value "$S" MASTER_IP); MASTER_IP="${MASTER_IP:-$(get_current_host)}"
    MASTER_PORT=$(get_service_value "$S" MASTER_PORT); MASTER_PORT="${MASTER_PORT:-7077}"
    MASTER_UI_PORT=$(get_service_value "$S" MASTER_UI_PORT); MASTER_UI_PORT="${MASTER_UI_PORT:-8080}"
    WORKER_UI_PORT=$(get_service_value "$S" WORKER_UI_PORT); WORKER_UI_PORT="${WORKER_UI_PORT:-8081}"
    WORKER_CORES=$(get_service_value "$S" WORKER_CORES); WORKER_CORES="${WORKER_CORES:-0}"
    WORKER_MEMORY=$(get_service_value "$S" WORKER_MEMORY); WORKER_MEMORY="${WORKER_MEMORY:-0}"

    SPARK_PKG="spark-${SPARK_VERSION}-bin-${HADOOP_VARIANT}"; SPARK_TARBALL="${SPARK_PKG}.tgz"
    SPARK_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TARBALL}"
    SPARK_URL_ARCHIVE="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TARBALL}"
}
spark_standalone_install() {
    get_sudo || { echo "sudo/root required" >&2; return 1; }
    get_pkg_mgr || { echo "Unsupported package manager" >&2; return 1; }
    spark_standalone_init_vars || return 1

    if [[ "$PKG_MGR" == apt ]]; then
        $SUDO apt-get update -y >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
        $SUDO apt-get install -y jq wget tar openjdk-17-jre-headless >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
    else
        $SUDO "$PKG_MGR" install -y jq wget tar java-17-openjdk-headless >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
    fi
    _airflow_ensure_user "$SERVICE_USER" "$BASE_DIR" || return 1

    if [[ ! -x "$SPARK_HOME/bin/spark-class" ]]; then
        cd "$SOURCE" || return 1
        wget -q -O "$SPARK_TARBALL" "$SPARK_URL" >>"$ACCESS_LOG" 2>>"$ERROR_LOG" \
            || wget -q -O "$SPARK_TARBALL" "$SPARK_URL_ARCHIVE" >>"$ACCESS_LOG" 2>>"$ERROR_LOG" \
            || { log_error "Spark tarball download failed for ${SPARK_VERSION}/${HADOOP_VARIANT}"; return 1; }
        tar -xzf "$SPARK_TARBALL" >>"$ACCESS_LOG" 2>>"$ERROR_LOG" || return 1
        $SUDO rm -rf "$SPARK_HOME"; $SUDO mkdir -p "$(dirname "$SPARK_HOME")"; $SUDO cp -a "$SOURCE/$SPARK_PKG" "$SPARK_HOME" || return 1
    fi

    local JAVA_HOME_DETECTED; JAVA_HOME_DETECTED="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
    local WORKER_EXTRA_ARGS=""
    [[ "$WORKER_CORES" != 0 ]] && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS -c ${WORKER_CORES}"
    [[ "$WORKER_MEMORY" != 0 ]] && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS -m ${WORKER_MEMORY}"

    $SUDO tee "$SPARK_HOME/conf/spark-env.sh" >/dev/null <<EOF
export JAVA_HOME=${JAVA_HOME_DETECTED}
export SPARK_MASTER_HOST=${MASTER_IP}
export SPARK_MASTER_PORT=${MASTER_PORT}
export SPARK_MASTER_WEBUI_PORT=${MASTER_UI_PORT}
export SPARK_WORKER_WEBUI_PORT=${WORKER_UI_PORT}
EOF
    $SUDO chmod +x "$SPARK_HOME/conf/spark-env.sh"
    [[ "$SERVICE_USER" != root ]] && $SUDO chown -R "$SERVICE_USER:$SERVICE_USER" "$SPARK_HOME"

    $SUDO tee /etc/systemd/system/spark-master.service >/dev/null <<EOF
[Unit]
Description=Apache Spark Master (standalone)
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
Environment="JAVA_HOME=${JAVA_HOME_DETECTED}"
Environment="SPARK_HOME=${SPARK_HOME}"
ExecStart=${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master --host ${MASTER_IP} --port ${MASTER_PORT} --webui-port ${MASTER_UI_PORT}
Restart=always
RestartSec=5s
[Install]
WantedBy=multi-user.target
EOF

    $SUDO tee /etc/systemd/system/spark-worker.service >/dev/null <<EOF
[Unit]
Description=Apache Spark Worker (standalone)
After=network-online.target spark-master.service
Wants=network-online.target
Requires=spark-master.service
[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
Environment="JAVA_HOME=${JAVA_HOME_DETECTED}"
Environment="SPARK_HOME=${SPARK_HOME}"
ExecStart=${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker spark://${MASTER_IP}:${MASTER_PORT} --webui-port ${WORKER_UI_PORT}${WORKER_EXTRA_ARGS}
Restart=always
RestartSec=5s
[Install]
WantedBy=multi-user.target
EOF

    $SUDO systemctl daemon-reload
    $SUDO systemctl enable spark-master.service spark-worker.service >>"$ACCESS_LOG" 2>>"$ERROR_LOG"
    $SUDO systemctl restart spark-master.service || { log_error "Failed to start spark-master.service"; return 1; }
    wait_for_tcp_port "$MASTER_IP" "$MASTER_PORT" 20 || { log_error "Spark master did not open port ${MASTER_PORT} on ${MASTER_IP}"; return 1; }
    $SUDO systemctl restart spark-worker.service || { log_error "Failed to start spark-worker.service"; return 1; }
    sleep 3
    $SUDO systemctl is-active --quiet spark-worker.service || { log_error "spark-worker.service failed to stay up -- check journalctl -u spark-worker"; return 1; }
    log_info "Spark standalone installed: master ${MASTER_IP}:${MASTER_PORT} (UI ${MASTER_UI_PORT}), worker UI ${WORKER_UI_PORT}"
}

