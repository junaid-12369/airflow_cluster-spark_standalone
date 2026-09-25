#!/bin/bash
#################################################################
# deploy_all.sh
#
# Run this from ONE control server only (the one you copied
# env.json / methods.sh / executable.sh onto). It:
#
#   1. Reads every server hostname out of env.json
#   2. Copies env.json + methods.sh + executable.sh to each one
#      over scp, using a shared SSH key
#   3. SSHes in and runs executable.sh on that server
#
# executable.sh itself already figures out which services belong
# to whichever host it's running on (via `hostname -I` + jq), so
# the exact same env.json/methods.sh/executable.sh trio gets
# pushed everywhere unchanged -- each server just installs its
# own piece.
#
# For the Airflow cluster + Spark trio: FERNET_KEY and
# WEBSERVER_SECRET_KEY are NOT auto-generated anymore. Generate
# them yourself once and paste the identical values into the
# Airflow-Scheduler block AND every Airflow-Worker block in
# env.json (see env.json's _notes) BEFORE running this script.
# With that done, every node -- scheduler/master included -- can
# be deployed together in a single pass; there is no required
# "scheduler first" step. ONLY= below is just an optional filter
# if you ever want to target a subset of hosts.
#
# Usage:
#   ./deploy_all.sh                                       # deploy every host in env.json in one go
#   ONLY=172.31.37.239 ./deploy_all.sh                    # optional: just the scheduler
#   ONLY=172.31.0.85,172.31.8.214,172.31.13.3 ./deploy_all.sh   # optional: just workers + spark
#
# Overrides (env vars), if your setup differs from the defaults:
#   SSH_USER=ec2-user SSH_KEY=~/.ssh/prod.pem REMOTE_DIR=Methods ./deploy_all.sh
#################################################################

set -uo pipefail

SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
REMOTE_DIR="${REMOTE_DIR:-Methods}"          # relative to the remote user's $HOME
SSH_OPTS=(-i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)

LOCAL_FILES=(env.json methods.sh executable.sh)

FILENAME=$(date +"%d%m%Y%H%M%S")
DEPLOY_LOG="./deploy.${FILENAME}.log"

log() {
    echo "[$(date '+%F %T')] $1" | tee -a "$DEPLOY_LOG"
}

# ---------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------
for f in "${LOCAL_FILES[@]}"; do
    [[ -f "$f" ]] || { echo "Missing required file: $f -- run this from the directory containing env.json/methods.sh/executable.sh"; exit 1; }
done

command -v jq >/dev/null 2>&1 || { echo "jq is required on the control server (apt/yum install jq)"; exit 1; }

if [[ ! -f "$SSH_KEY" ]]; then
    echo "SSH key not found at $SSH_KEY. Set SSH_KEY=/path/to/key or place your key there."
    exit 1
fi
chmod 600 "$SSH_KEY" 2>/dev/null || true

# ---------------------------------------------------------------
# Auto-fill FERNET_KEY / WEBSERVER_SECRET_KEY, once, for the
# whole cluster.
#
# This is the one script that already touches every host before
# anything installs (it's what pushes env.json to each of them),
# so it's the right place to generate these two opaque, meaningless
# -to-a-human tokens and bake them into env.json -- identical
# across the scheduler and every worker -- before that push
# happens. DB_PASSWORD / ADMIN_PASSWORD are deliberately NOT
# auto-filled here: those are secrets you choose, not random
# tokens, so they still need a human to set them.
#
# Only fires if a placeholder/blank is still present, so re-running
# this script after a real deploy is a no-op here.
# ---------------------------------------------------------------
if grep -qE '"FERNET_KEY": *("CHANGE_ME_FERNET_KEY"|"")' env.json || grep -qE '"WEBSERVER_SECRET_KEY": *("CHANGE_ME_WEBSERVER_SECRET_KEY"|"")' env.json; then
    command -v python3 >/dev/null 2>&1 || { echo "python3 is required on the control server to auto-generate FERNET_KEY/WEBSERVER_SECRET_KEY"; exit 1; }
    log "FERNET_KEY/WEBSERVER_SECRET_KEY are blank or placeholders in env.json -- generating them once for this whole deploy"

    NEW_FERNET=$(python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
    NEW_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(16))")
    [[ -n "$NEW_FERNET" && -n "$NEW_SECRET" ]] || { log "Failed to generate FERNET_KEY/WEBSERVER_SECRET_KEY"; exit 1; }

    TMP_ENV=$(mktemp)
    jq --arg fk "$NEW_FERNET" --arg wk "$NEW_SECRET" '
        (.servers[].services[] | select(has("FERNET_KEY")) | .FERNET_KEY) = $fk
        | (.servers[].services[] | select(has("WEBSERVER_SECRET_KEY")) | .WEBSERVER_SECRET_KEY) = $wk
    ' env.json > "$TMP_ENV" && mv "$TMP_ENV" env.json \
        || { log "Failed to write generated keys into env.json"; rm -f "$TMP_ENV"; exit 1; }

    log "FERNET_KEY/WEBSERVER_SECRET_KEY generated and written into env.json (identical across scheduler + every worker)"
fi

HOSTS=$(jq -r '.servers[].hostname' env.json)
if [[ -n "${ONLY:-}" ]]; then
    HOSTS=$(echo "$HOSTS" | grep -Fxf <(echo "$ONLY" | tr ',' '\n'))
fi
if [[ -z "$HOSTS" ]]; then
    log "No servers found in env.json (or none matched ONLY=${ONLY:-})"
    exit 1
fi

# This control server's own IPs, so it can deploy to itself
# locally instead of SSHing to itself.
LOCAL_IPS=$(hostname -I)

FAILED_HOSTS=()

# Count the total up front (not inside the loop), so an interrupted
# run still reports the true "X/TOTAL" instead of only counting
# hosts the loop actually reached before being killed.
TOTAL=$(grep -c . <<< "$HOSTS")

# ---------------------------------------------------------------
# Deploy loop
#
# NOTE: the loop reads hosts from file descriptor 3, NOT stdin.
# If it read from stdin (e.g. `done <<< "$HOSTS"` with a plain
# `read -r host`), the `ssh`/`scp` calls below -- which inherit
# the script's stdin by default -- can consume the remaining
# here-string content meant for `read`. That silently truncates
# the loop after the first remote host: `read` hits EOF early,
# the loop exits, and every host after the first (including any
# "run locally" host) is skipped entirely -- with no error, since
# FAILED_HOSTS stays empty and the summary below still prints
# "TOTAL/TOTAL succeeded".
# ---------------------------------------------------------------
while IFS= read -r host <&3; do
    [[ -z "$host" ]] && continue

    if grep -qw "$host" <<< "$LOCAL_IPS"; then
        log "=== $host (control server itself) === running locally"
        if bash ./executable.sh >> "$DEPLOY_LOG" 2>&1; then
            log "=== $host: SUCCESS (local) ==="
        else
            log "=== $host: FAILED (local) -- see $DEPLOY_LOG ==="
            FAILED_HOSTS+=("$host")
        fi
        continue
    fi

    log "=== $host === ensuring remote directory exists"
    if ! ssh "${SSH_OPTS[@]}" "$SSH_USER@$host" "mkdir -p ~/$REMOTE_DIR" < /dev/null >> "$DEPLOY_LOG" 2>&1; then
        log "=== $host: FAILED (SSH connect / mkdir) -- see $DEPLOY_LOG ==="
        FAILED_HOSTS+=("$host")
        continue
    fi

    log "=== $host === copying env.json, methods.sh, executable.sh"
    if ! scp "${SSH_OPTS[@]}" "${LOCAL_FILES[@]}" "$SSH_USER@$host:~/$REMOTE_DIR/" < /dev/null >> "$DEPLOY_LOG" 2>&1; then
        log "=== $host: FAILED (scp) -- see $DEPLOY_LOG ==="
        FAILED_HOSTS+=("$host")
        continue
    fi

    log "=== $host === running executable.sh remotely"
    if ssh "${SSH_OPTS[@]}" "$SSH_USER@$host" "cd ~/$REMOTE_DIR && chmod +x executable.sh && ./executable.sh" < /dev/null >> "$DEPLOY_LOG" 2>&1; then
        log "=== $host: SUCCESS ==="
    else
        log "=== $host: FAILED (remote install) -- see $DEPLOY_LOG ==="
        FAILED_HOSTS+=("$host")
    fi

done 3<<< "$HOSTS"

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
echo
if [[ ${#FAILED_HOSTS[@]} -eq 0 ]]; then
    log "Deployment complete: $TOTAL/$TOTAL hosts succeeded."
    exit 0
else
    log "Deployment finished with failures on: ${FAILED_HOSTS[*]} ($(( TOTAL - ${#FAILED_HOSTS[@]} ))/$TOTAL succeeded)"
    log "Full details in $DEPLOY_LOG"
    exit 1
fi

