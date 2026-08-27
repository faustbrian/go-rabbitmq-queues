#!/usr/bin/env bash

set -euo pipefail

if [[ "${CI:-}" != true || "${GITHUB_ACTIONS:-}" != true ]]; then
    printf '%s\n' 'ci-live-cluster.sh may run only in GitHub Actions CI' >&2
    exit 1
fi

fault_scenario="${1:-}"
case "${fault_scenario}" in
    classic-node-loss)
        fault_queue_type='classic'
        ;;
    quorum-leader-loss | quorum-network-partition | cluster-restart | reconnect-storm)
        fault_queue_type='quorum'
        ;;
    *)
        printf '%s\n' \
            'ci-live-cluster.sh requires classic-node-loss, quorum-leader-loss, quorum-network-partition, cluster-restart, or reconnect-storm' >&2
        exit 1
        ;;
esac

project_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
task_root="$(mktemp -d "${RUNNER_TEMP}/go-rabbitmq-queues-cluster.XXXXXX")"
network_name="go-rabbitmq-queues-cluster-${fault_scenario}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
image='rabbitmq:4.3.5-management-alpine@sha256:7224161872a48060e980a611f4778ad18168f00cfa974cab30604dbd855511dc'
bootstrap_user='ci-bootstrap'
client_user='ci-client'
vhost='/go-rabbitmq-queues-ci'
encoded_vhost='%2Fgo-rabbitmq-queues-ci'
bootstrap_password="$(openssl rand -hex 24)"
client_password="$(openssl rand -hex 24)"
erlang_cookie="$(openssl rand -hex 24)"
node_names=(rabbit1 rabbit2 rabbit3)
container_names=()
reconnect_storm_cycles=3
reconnect_storm_outage_seconds=5
reconnect_storm_resource_pairs=4
declare -A amqp_ports
declare -A management_ports
test_pid=''

cleanup() {
    result=$?
    trap - EXIT HUP INT TERM
    if [[ -n "${test_pid}" ]] && kill -0 "${test_pid}" >/dev/null 2>&1; then
        kill "${test_pid}" >/dev/null 2>&1 || true
        wait "${test_pid}" >/dev/null 2>&1 || true
    fi
    if ((result != 0)); then
        for container_name in "${container_names[@]}"; do
            if docker inspect "${container_name}" >/dev/null 2>&1; then
                printf 'BROKER_LOG node=%s\n' "${container_name}" >&2
                docker logs "${container_name}" 2>&1 |
                    sed \
                        -e "s/${bootstrap_password}/[REDACTED]/g" \
                        -e "s/${client_password}/[REDACTED]/g" \
                        -e "s/${erlang_cookie}/[REDACTED]/g" >&2 || true
            fi
        done
    fi
    for container_name in "${container_names[@]}"; do
        docker rm --force "${container_name}" >/dev/null 2>&1 || true
    done
    docker network rm "${network_name}" >/dev/null 2>&1 || true
    if [[ -d "${task_root}" ]]; then
        chmod -R u+w "${task_root}" 2>/dev/null || true
        find "${task_root}" -depth -delete
    fi
    exit "${result}"
}
trap cleanup EXIT HUP INT TERM

mkdir -p \
    "${task_root}/tls" \
    "${task_root}/gates" \
    "${task_root}/go-build" \
    "${task_root}/go-modules" \
    "${task_root}/go-tmp"

openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
    -subj '/CN=go-rabbitmq-queues-ci-ca' \
    -keyout "${task_root}/tls/ca-key.pem" \
    -out "${task_root}/tls/ca.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -sha256 -nodes \
    -subj '/CN=localhost' \
    -keyout "${task_root}/tls/server-key.pem" \
    -out "${task_root}/tls/server.csr" >/dev/null 2>&1
printf '%s\n' \
    'subjectAltName=DNS:localhost,IP:127.0.0.1' \
    'extendedKeyUsage=serverAuth' \
    >"${task_root}/tls/server.ext"
openssl x509 -req -sha256 -days 1 \
    -in "${task_root}/tls/server.csr" \
    -CA "${task_root}/tls/ca.pem" \
    -CAkey "${task_root}/tls/ca-key.pem" \
    -CAcreateserial \
    -extfile "${task_root}/tls/server.ext" \
    -out "${task_root}/tls/server.pem" >/dev/null 2>&1
chmod 0644 "${task_root}/tls/"*.pem

cat >"${task_root}/rabbitmq.conf" <<'EOF'
listeners.tcp = none
listeners.ssl.default = 5671
management.tcp.ip = 0.0.0.0
management.tcp.port = 15672
loopback_users.guest = false
cluster_partition_handling = pause_minority
ssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem
ssl_options.certfile = /etc/rabbitmq/tls/server.pem
ssl_options.keyfile = /etc/rabbitmq/tls/server-key.pem
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
ssl_options.versions.1 = tlsv1.3
ssl_options.versions.2 = tlsv1.2
EOF

docker network create \
    --label 'com.faustbrian.task=go-rabbitmq-queues-cluster-ci' \
    "${network_name}" >/dev/null

for index in "${!node_names[@]}"; do
    node_name="${node_names[${index}]}"
    amqp_ports["${node_name}"]="$((35671 + index))"
    management_ports["${node_name}"]="$((45671 + index))"
    container_name="go-rabbitmq-queues-${node_name}-${fault_scenario}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
    container_names+=("${container_name}")
    mkdir "${task_root}/${node_name}-data"
    chmod 0700 "${task_root}/${node_name}-data"
    docker run --detach \
        --name "${container_name}" \
        --hostname "${node_name}" \
        --network "${network_name}" \
        --network-alias "${node_name}" \
        --user "$(id -u):$(id -g)" \
        --label 'com.faustbrian.task=go-rabbitmq-queues-cluster-ci' \
        --env "RABBITMQ_NODENAME=rabbit@${node_name}" \
        --env "RABBITMQ_DEFAULT_USER=${bootstrap_user}" \
        --env "RABBITMQ_DEFAULT_PASS=${bootstrap_password}" \
        --env "RABBITMQ_DEFAULT_VHOST=${vhost}" \
        --env "RABBITMQ_ERLANG_COOKIE=${erlang_cookie}" \
        --mount "type=bind,source=${task_root}/rabbitmq.conf,target=/etc/rabbitmq/rabbitmq.conf,readonly" \
        --mount "type=bind,source=${task_root}/tls,target=/etc/rabbitmq/tls,readonly" \
        --mount "type=bind,source=${task_root}/${node_name}-data,target=/var/lib/rabbitmq" \
        --publish "127.0.0.1:${amqp_ports[${node_name}]}:5671" \
        --publish "127.0.0.1:${management_ports[${node_name}]}:15672" \
        "${image}" >/dev/null
done

for index in "${!node_names[@]}"; do
    node_name="${node_names[${index}]}"
    container_name="${container_names[${index}]}"
    for _ in $(seq 1 120); do
        if docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null
    test "$(docker exec "${container_name}" rabbitmqctl version)" = '4.3.5'
    [[ "${amqp_ports[${node_name}]}" =~ ^[0-9]+$ ]]
    [[ "${management_ports[${node_name}]}" =~ ^[0-9]+$ ]]
done

for index in 1 2; do
    container_name="${container_names[${index}]}"
    docker exec "${container_name}" rabbitmqctl stop_app >/dev/null
    docker exec "${container_name}" rabbitmqctl reset >/dev/null
    docker exec "${container_name}" rabbitmqctl join_cluster rabbit@rabbit1 >/dev/null
    docker exec "${container_name}" rabbitmqctl start_app >/dev/null
done

management_url() {
    local node_name="$1"
    printf 'http://127.0.0.1:%s/api' "${management_ports[${node_name}]}"
}

get_json() {
    local node_name="$1"
    local endpoint="$2"
    curl --fail --silent --show-error \
        --user "${bootstrap_user}:${bootstrap_password}" \
        "$(management_url "${node_name}")/${endpoint}"
}

put_json() {
    local node_name="$1"
    local endpoint="$2"
    local body="$3"
    curl --fail --silent --show-error \
        --user "${bootstrap_user}:${bootstrap_password}" \
        --header 'content-type: application/json' \
        --request PUT \
        --data "${body}" \
        "$(management_url "${node_name}")/${endpoint}" >/dev/null
}

post_json() {
    local node_name="$1"
    local endpoint="$2"
    local body="$3"
    curl --fail --silent --show-error \
        --user "${bootstrap_user}:${bootstrap_password}" \
        --header 'content-type: application/json' \
        --request POST \
        --data "${body}" \
        "$(management_url "${node_name}")/${endpoint}" >/dev/null
}

for node_name in "${node_names[@]}"; do
    for _ in $(seq 1 60); do
        if get_json "${node_name}" overview >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    get_json "${node_name}" overview >/dev/null
done

for _ in $(seq 1 60); do
    if get_json rabbit1 nodes | jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null; then
        break
    fi
    sleep 1
done
get_json rabbit1 nodes | jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null

put_json rabbit1 "users/${client_user}" \
    "$(jq -cn --arg password "${client_password}" '{password: $password, tags: ""}')"
put_json rabbit1 "permissions/${encoded_vhost}/${client_user}" \
    '{"configure":"^$","write":"^go-rabbitmq-queues\\.events$","read":"^go-rabbitmq-queues\\.(classic|quorum)$"}'
put_json rabbit1 "exchanges/${encoded_vhost}/go-rabbitmq-queues.events" \
    '{"type":"direct","auto_delete":false,"durable":true,"internal":false,"arguments":{}}'
put_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.classic" \
    '{"auto_delete":false,"durable":true,"arguments":{"x-queue-type":"classic"}}'
put_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum" \
    '{"auto_delete":false,"durable":true,"arguments":{"x-queue-type":"quorum"}}'
post_json rabbit1 "bindings/${encoded_vhost}/e/go-rabbitmq-queues.events/q/go-rabbitmq-queues.classic" \
    '{"routing_key":"classic","arguments":{}}'
post_json rabbit1 "bindings/${encoded_vhost}/e/go-rabbitmq-queues.events/q/go-rabbitmq-queues.quorum" \
    '{"routing_key":"quorum","arguments":{}}'

for _ in $(seq 1 60); do
    if queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum")" &&
        jq -e '.members | length == 3' <<<"${queue_json}" >/dev/null; then
        break
    fi
    sleep 1
done
queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum")"
jq -e '.members | length == 3' <<<"${queue_json}" >/dev/null

for node_name in "${node_names[@]}"; do
    openssl s_client \
        -connect "127.0.0.1:${amqp_ports[${node_name}]}" \
        -servername localhost \
        -CAfile "${task_root}/tls/ca.pem" \
        -tls1_3 </dev/null >/dev/null 2>&1
done

endpoints_json="$({
    for node_name in "${node_names[@]}"; do
        printf '%s\n' "${amqp_ports[${node_name}]}"
    done
} | jq -R 'tonumber | {host: "127.0.0.1", port: .}' | jq -s '.')"
fault_start_gate="${task_root}/gates/fault-started"
fault_complete_gate="${task_root}/gates/fault-complete"
fault_cycle_gate_files=()
fault_cycle_gate_files_json='[]'
if [[ "${fault_scenario}" == reconnect-storm ]]; then
    fault_start_gate=''
    fault_complete_gate=''
    for cycle in $(seq 1 "${reconnect_storm_cycles}"); do
        fault_cycle_gate_files+=("${task_root}/gates/reconnect-cycle-${cycle}")
    done
    fault_cycle_gate_files_json="$(printf '%s\n' "${fault_cycle_gate_files[@]}" | jq -R . | jq -s .)"
fi
jq -n \
    --argjson endpoints "${endpoints_json}" \
    --arg vhost "${vhost}" \
    --arg username "${client_user}" \
    --arg password "${client_password}" \
    --arg root_ca_file "${task_root}/tls/ca.pem" \
    --arg fault_start_gate_file "${fault_start_gate}" \
    --arg fault_complete_gate_file "${fault_complete_gate}" \
    --arg fault_queue_type "${fault_queue_type}" \
    --arg fault_scenario "${fault_scenario}" \
    --argjson fault_cycle_gate_files "${fault_cycle_gate_files_json}" \
    --argjson fault_resource_pairs "${reconnect_storm_resource_pairs}" \
    '{
        endpoints: $endpoints,
        virtual_host: $vhost,
        username: $username,
        password: $password,
        tls: {
            server_name: "localhost",
            root_ca_file: $root_ca_file,
            client_certificate_file: "",
            client_private_key_file: ""
        },
        exchange: "go-rabbitmq-queues.events",
        classic: {name: "go-rabbitmq-queues.classic", routing_key: "classic"},
        quorum: {name: "go-rabbitmq-queues.quorum", routing_key: "quorum"},
        unroutable_routing_key: "intentionally-unbound",
        fault_start_gate_file: $fault_start_gate_file,
        fault_complete_gate_file: $fault_complete_gate_file,
        fault_queue_type: $fault_queue_type,
        fault_scenario: $fault_scenario,
        fault_cycle_gate_files: $fault_cycle_gate_files,
        fault_resource_pairs: (if $fault_scenario == "reconnect-storm" then $fault_resource_pairs else 0 end),
        fault_window_messages: 64
    }' >"${task_root}/live-cluster.json"
chmod 0600 "${task_root}/live-cluster.json"

test_log="${task_root}/cluster-test.log"
(
    cd "${project_root}"
    GOTOOLCHAIN=local \
        GOWORK=off \
        GOCACHE="${task_root}/go-build" \
        GOMODCACHE="${task_root}/go-modules" \
        GOTMPDIR="${task_root}/go-tmp" \
        RABBITMQ_QUEUE_CLUSTER_CONFIG="${task_root}/live-cluster.json" \
        go test -v -count=1 -tags=livebroker -run '^TestLiveBrokerThreeNodeInterruption$' .
) >"${test_log}" 2>&1 &
test_pid=$!

ready=false
for _ in $(seq 1 120); do
    if grep -q 'FAULT_WINDOW_READY' "${test_log}"; then
        ready=true
        break
    fi
    if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
if [[ "${ready}" != true ]]; then
    if wait "${test_pid}"; then
        test_status=0
    else
        test_status=$?
    fi
    test_pid=''
    cat "${test_log}"
    if ((test_status == 0)); then
        exit 1
    fi
    exit "${test_status}"
fi

if [[ "${fault_scenario}" == reconnect-storm ]]; then
    for index in "${!fault_cycle_gate_files[@]}"; do
        cycle="$((index + 1))"
        cycle_waiting=false
        for _ in $(seq 1 30); do
            if grep -q "RECONNECT_CYCLE_WAITING cycle=${cycle}" "${test_log}"; then
                cycle_waiting=true
                break
            fi
            if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        if [[ "${cycle_waiting}" != true ]]; then
            cat "${test_log}"
            exit 1
        fi

        docker stop --time 10 "${container_names[@]}" >/dev/null
        cycle_started_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
        : >"${fault_cycle_gate_files[${index}]}"
        printf 'FAULT_TIMELINE scenario=%s queue_type=%s cycle=%d event=cluster-stopped at=%s\n' \
            "${fault_scenario}" "${fault_queue_type}" "${cycle}" "${cycle_started_at}"

        cycle_observed=false
        for _ in $(seq 1 30); do
            if grep -q "RECONNECT_CYCLE_STARTED cycle=${cycle}" "${test_log}"; then
                cycle_observed=true
                break
            fi
            if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        if [[ "${cycle_observed}" != true ]]; then
            cat "${test_log}"
            exit 1
        fi

        sleep "${reconnect_storm_outage_seconds}"
        for container_name in "${container_names[@]}"; do
            docker start "${container_name}" >/dev/null
        done
        for container_name in "${container_names[@]}"; do
            for _ in $(seq 1 120); do
                if docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
                    break
                fi
                sleep 1
            done
            docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null
        done
        for _ in $(seq 1 120); do
            if get_json rabbit1 nodes |
                jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null; then
                break
            fi
            sleep 1
        done
        get_json rabbit1 nodes |
            jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null
        for _ in $(seq 1 120); do
            if queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum" 2>/dev/null)" &&
                jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
                    <<<"${queue_json}" >/dev/null; then
                break
            fi
            sleep 1
        done
        queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum")"
        jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
            <<<"${queue_json}" >/dev/null
        recovered_leader="$(jq -er '.leader' <<<"${queue_json}")"
        cycle_recovered_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
        printf 'FAULT_TIMELINE scenario=%s queue_type=%s cycle=%d event=cluster-restarted hold_seconds=%d leader=%s at=%s\n' \
            "${fault_scenario}" "${fault_queue_type}" "${cycle}" "${reconnect_storm_outage_seconds}" \
            "${recovered_leader}" "${cycle_recovered_at}"

        recovery_observed=false
        for _ in $(seq 1 120); do
            if grep -q "RECOVERY_CYCLE_READY cycle=${cycle}" "${test_log}"; then
                recovery_observed=true
                break
            fi
            if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        if [[ "${recovery_observed}" != true ]]; then
            cat "${test_log}"
            exit 1
        fi
        for _ in $(seq 1 60); do
            if queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum" 2>/dev/null)" &&
                jq -e --argjson consumers "${reconnect_storm_resource_pairs}" \
                    '.state == "running" and .consumers == $consumers' <<<"${queue_json}" >/dev/null; then
                break
            fi
            sleep 1
        done
        queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum")"
        jq -e --argjson consumers "${reconnect_storm_resource_pairs}" \
            '.state == "running" and .consumers == $consumers' <<<"${queue_json}" >/dev/null
    done

    if wait "${test_pid}"; then
        test_status=0
    else
        test_status=$?
    fi
    test_pid=''
    cat "${test_log}"
    test "${test_status}" -eq 0
    exit
fi

fault_member=''
fault_node=''
fault_index=-1
fault_container=''
observer_node=''
if [[ "${fault_scenario}" != cluster-restart ]]; then
    queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.${fault_queue_type}")"
    if [[ "${fault_queue_type}" == classic ]]; then
        fault_member="$(jq -er '.node' <<<"${queue_json}")"
    else
        fault_member="$(jq -er '.leader' <<<"${queue_json}")"
    fi
    fault_node="${fault_member#rabbit@}"
    for index in "${!node_names[@]}"; do
        if [[ "${node_names[${index}]}" == "${fault_node}" ]]; then
            fault_index="${index}"
        else
            observer_node="${node_names[${index}]}"
        fi
    done
    if ((fault_index < 0)) || [[ -z "${observer_node}" ]]; then
        printf 'unable to map fault member %s to a cluster container\n' "${fault_member}" >&2
        exit 1
    fi
    fault_container="${container_names[${fault_index}]}"
fi

case "${fault_scenario}" in
    classic-node-loss | quorum-leader-loss)
        docker stop --time 10 "${fault_container}" >/dev/null
        fault_event="node-stopped node=${fault_member}"
        ;;
    quorum-network-partition)
        docker network disconnect "${network_name}" "${fault_container}"
        fault_event="node-partitioned node=${fault_member}"
        ;;
    cluster-restart)
        for container_name in "${container_names[@]}"; do
            docker stop --time 10 "${container_name}" >/dev/null
        done
        fault_event='cluster-stopped'
        ;;
esac
fault_started_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
: >"${fault_start_gate}"
printf 'FAULT_TIMELINE scenario=%s queue_type=%s event=%s at=%s\n' \
    "${fault_scenario}" "${fault_queue_type}" "${fault_event}" "${fault_started_at}"

for _ in $(seq 1 30); do
    if grep -q 'FAULT_WINDOW_STARTED' "${test_log}"; then
        break
    fi
    sleep 1
done
grep -q 'FAULT_WINDOW_STARTED' "${test_log}"

case "${fault_scenario}" in
    classic-node-loss)
        sleep 3
        docker start "${fault_container}" >/dev/null
        for _ in $(seq 1 120); do
            if docker exec "${fault_container}" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        docker exec "${fault_container}" rabbitmq-diagnostics -q ping >/dev/null
        for _ in $(seq 1 120); do
            if queue_json="$(get_json "${observer_node}" "queues/${encoded_vhost}/go-rabbitmq-queues.classic" 2>/dev/null)" &&
                jq -e --arg node "${fault_member}" '.state == "running" and .node == $node' <<<"${queue_json}" >/dev/null; then
                break
            fi
            sleep 1
        done
        queue_json="$(get_json "${observer_node}" "queues/${encoded_vhost}/go-rabbitmq-queues.classic")"
        jq -e --arg node "${fault_member}" '.state == "running" and .node == $node' <<<"${queue_json}" >/dev/null
        recovery_event="node-restarted node=${fault_member}"
        ;;
    quorum-leader-loss | quorum-network-partition)
        new_leader=''
        for _ in $(seq 1 120); do
            if queue_json="$(get_json "${observer_node}" "queues/${encoded_vhost}/go-rabbitmq-queues.quorum" 2>/dev/null)"; then
                new_leader="$(jq -r '.leader // ""' <<<"${queue_json}")"
                if [[ -n "${new_leader}" && "${new_leader}" != "${fault_member}" ]] &&
                    jq -e '.state == "running" and (.members | length) >= 2' <<<"${queue_json}" >/dev/null; then
                    break
                fi
            fi
            sleep 1
        done
        [[ -n "${new_leader}" && "${new_leader}" != "${fault_member}" ]]
        jq -e '.state == "running" and (.members | length) >= 2' <<<"${queue_json}" >/dev/null
        if [[ "${fault_scenario}" == quorum-network-partition ]]; then
            docker network connect --alias "${fault_node}" "${network_name}" "${fault_container}"
            for _ in $(seq 1 120); do
                if get_json "${observer_node}" nodes |
                    jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null &&
                    queue_json="$(get_json "${observer_node}" "queues/${encoded_vhost}/go-rabbitmq-queues.quorum" 2>/dev/null)" &&
                    jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
                        <<<"${queue_json}" >/dev/null; then
                    break
                fi
                sleep 1
            done
            get_json "${observer_node}" nodes |
                jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null
            queue_json="$(get_json "${observer_node}" "queues/${encoded_vhost}/go-rabbitmq-queues.quorum")"
            jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
                <<<"${queue_json}" >/dev/null
            healed_leader="$(jq -er '.leader' <<<"${queue_json}")"
            recovery_event="partition-healed leader=${healed_leader}"
        else
            recovery_event="leader-elected leader=${new_leader}"
        fi
        ;;
    cluster-restart)
        prolonged_outage_seconds=20
        sleep "${prolonged_outage_seconds}"
        for container_name in "${container_names[@]}"; do
            docker start "${container_name}" >/dev/null
        done
        for container_name in "${container_names[@]}"; do
            for _ in $(seq 1 120); do
                if docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
                    break
                fi
                sleep 1
            done
            docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null
        done
        for _ in $(seq 1 120); do
            if get_json rabbit1 nodes |
                jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null; then
                break
            fi
            sleep 1
        done
        get_json rabbit1 nodes |
            jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null
        for _ in $(seq 1 120); do
            if queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum" 2>/dev/null)" &&
                jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' <<<"${queue_json}" >/dev/null; then
                break
            fi
            sleep 1
        done
        queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum")"
        jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' <<<"${queue_json}" >/dev/null
        new_leader="$(jq -er '.leader' <<<"${queue_json}")"
        recovery_event="cluster-restarted hold_seconds=${prolonged_outage_seconds} leader=${new_leader}"
        ;;
esac

fault_completed_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
: >"${fault_complete_gate}"
printf 'FAULT_TIMELINE scenario=%s queue_type=%s event=%s at=%s\n' \
    "${fault_scenario}" "${fault_queue_type}" "${recovery_event}" "${fault_completed_at}"

if wait "${test_pid}"; then
    test_status=0
else
    test_status=$?
fi
test_pid=''
cat "${test_log}"
test "${test_status}" -eq 0
