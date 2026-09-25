#!/bin/bash

# 1. Source the methods file so all functions become available in memory
source ./methods.sh

# 2. Identify the current server's hostname
CURRENT_HOST=$(hostname -I | awk '{print $1}')

# 3. Read the install commands from env.json for this specific host using 'jq'
# This pulls every 'install_cmd' associated with the current server's hostname
# (one command per service, e.g. airflow_scheduler_install AND kafka_install if a
# host runs both).
INSTALL_CMDS=$(jq -r --arg host "$CURRENT_HOST" '.servers[] | select(.hostname == $host) | .services[].install_cmd' "$ENV_JSON")

if [[ -z "$INSTALL_CMDS" ]]; then
    echo "No install commands found for host $CURRENT_HOST in env.json"
    exit 1
fi

# 4. Loop through and execute each command, one per line.
#
# NOTE: this used to be `for cmd in "$INSTALL_CMDS"; do`, which treats the
# entire (quoted) multi-line jq output as a SINGLE loop item -- it only
# "worked" because every host in env.json happened to have exactly one
# service. Reading line-by-line here is what actually lets multiple
# services (and their potentially multi-word install commands) run
# correctly per host.
while IFS= read -r cmd; do
    [[ -z "$cmd" ]] && continue
    echo "Executing: $cmd"
    # Using eval safely parses the command string (e.g., "install_db_psql --role master")
    eval "$cmd"

    if [ $? -eq 0 ]; then
        echo "Successfully executed: $cmd"
    else
        echo "Error executing: $cmd"
        exit 1
    fi
done <<< "$INSTALL_CMDS"

