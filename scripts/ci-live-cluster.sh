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
    quorum-leader-loss | quorum-network-partition | cluster-restart | reconnect-storm | rolling-upgrade | application-rolling-deployment | prolonged-outage | quorum-performance-leader-loss)
        fault_queue_type='quorum'
        ;;
    *)
        printf '%s\n' \
            'ci-live-cluster.sh requires classic-node-loss, quorum-leader-loss, quorum-network-partition, cluster-restart, reconnect-storm, rolling-upgrade, application-rolling-deployment, prolonged-outage, or quorum-performance-leader-loss' >&2
        exit 1
        ;;
esac
daily_messages="${DAILY_MESSAGES:-0}"
node_resource_args=()
if [[ "${fault_scenario}" == quorum-performance-leader-loss ]] &&
    [[ ! "${daily_messages}" =~ ^(1000000|10000000|100000000)$ ]]; then
    printf '%s\n' 'quorum-performance-leader-loss requires a supported daily message target' >&2
    exit 1
fi
if [[ "${fault_scenario}" == quorum-performance-leader-loss ]]; then
    node_resource_args=(--cpus 1 --memory 2g --memory-swap 2g)
fi

project_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
task_root="$(mktemp -d "${RUNNER_TEMP}/go-rabbitmq-queues-cluster.XXXXXX")"
network_name="go-rabbitmq-queues-cluster-${fault_scenario}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
image='rabbitmq:4.3.5-management-alpine@sha256:7224161872a48060e980a611f4778ad18168f00cfa974cab30604dbd855511dc'
image_version='4.3.5'
rolling_upgrade_source_image='rabbitmq:4.3.4-management-alpine@sha256:39f934e10a7b95179171a70f15f02636201a153a2c689e961fc0f445bac275f2'
rolling_upgrade_source_version='4.3.4'
initial_image="${image}"
initial_image_version="${image_version}"
if [[ "${fault_scenario}" == rolling-upgrade ]]; then
    initial_image="${rolling_upgrade_source_image}"
    initial_image_version="${rolling_upgrade_source_version}"
fi
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
rolling_upgrade_cycles=3
prolonged_outage_seconds=90
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
done

run_node_container() {
    local index="$1"
    local node_image="$2"
    local node_name="${node_names[${index}]}"
    local container_name="${container_names[${index}]}"
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
        "${node_resource_args[@]}" \
        --publish "127.0.0.1:${amqp_ports[${node_name}]}:5671" \
        --publish "127.0.0.1:${management_ports[${node_name}]}:15672" \
        "${node_image}" >/dev/null
}

for index in "${!node_names[@]}"; do
    run_node_container "${index}" "${initial_image}"
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
    test "$(docker exec "${container_name}" rabbitmqctl version)" = "${initial_image_version}"
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
if [[ "${fault_scenario}" == rolling-upgrade ]]; then
    docker exec "${container_names[0]}" rabbitmqctl enable_feature_flag all >/dev/null
fi

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
read_permission='^go-rabbitmq-queues\.(classic|quorum)$'
if [[ "${fault_scenario}" == quorum-performance-leader-loss ]]; then
    read_permission='^go-rabbitmq-queues\.performance\.quorum\.[1-4]$'
fi
put_json rabbit1 "permissions/${encoded_vhost}/${client_user}" \
    "$(jq -cn --arg read "${read_permission}" \
        '{configure: "^$", write: "^go-rabbitmq-queues\\.events$", read: $read}')"
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
if [[ "${fault_scenario}" == quorum-performance-leader-loss ]]; then
    for index in $(seq 1 4); do
        queue_name="go-rabbitmq-queues.performance.quorum.${index}"
        put_json rabbit1 "queues/${encoded_vhost}/${queue_name}" \
            '{"auto_delete":false,"durable":true,"arguments":{"x-queue-type":"quorum","x-quorum-initial-group-size":3}}'
        post_json rabbit1 "bindings/${encoded_vhost}/e/go-rabbitmq-queues.events/q/${queue_name}" \
            "$(jq -cn --arg routing_key "performance.quorum.${index}" \
                '{routing_key: $routing_key, arguments: {}}')"
    done
fi

for _ in $(seq 1 60); do
    if queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum")" &&
        jq -e '.members | length == 3' <<<"${queue_json}" >/dev/null; then
        break
    fi
    sleep 1
done
queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.quorum")"
jq -e '.members | length == 3' <<<"${queue_json}" >/dev/null
if [[ "${fault_scenario}" == quorum-performance-leader-loss ]]; then
    for index in $(seq 1 4); do
        queue_name="go-rabbitmq-queues.performance.quorum.${index}"
        for _ in $(seq 1 60); do
            if queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/${queue_name}")" &&
                jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
                    <<<"${queue_json}" >/dev/null; then
                break
            fi
            sleep 1
        done
        queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/${queue_name}")"
        jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
            <<<"${queue_json}" >/dev/null
    done
fi

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
fault_cycle_complete_gate_files=()
fault_cycle_gate_files_json='[]'
fault_cycle_complete_gate_files_json='[]'
if [[ "${fault_scenario}" == reconnect-storm || "${fault_scenario}" == rolling-upgrade ]]; then
    fault_start_gate=''
    fault_complete_gate=''
    cycle_count="${reconnect_storm_cycles}"
    cycle_prefix='reconnect'
    if [[ "${fault_scenario}" == rolling-upgrade ]]; then
        cycle_count="${rolling_upgrade_cycles}"
        cycle_prefix='upgrade'
    fi
    for cycle in $(seq 1 "${cycle_count}"); do
        fault_cycle_gate_files+=("${task_root}/gates/${cycle_prefix}-cycle-${cycle}")
        fault_cycle_complete_gate_files+=("${task_root}/gates/${cycle_prefix}-cycle-${cycle}-verified")
    done
    fault_cycle_gate_files_json="$(printf '%s\n' "${fault_cycle_gate_files[@]}" | jq -R . | jq -s .)"
    fault_cycle_complete_gate_files_json="$(
        printf '%s\n' "${fault_cycle_complete_gate_files[@]}" | jq -R . | jq -s .
    )"
elif [[ "${fault_scenario}" == application-rolling-deployment ]]; then
    fault_cycle_gate_files+=("${task_root}/gates/new-consumer-verified")
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
    --argjson daily_messages "${daily_messages}" \
    --argjson fault_cycle_gate_files "${fault_cycle_gate_files_json}" \
    --argjson fault_cycle_complete_gate_files "${fault_cycle_complete_gate_files_json}" \
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
        fault_cycle_complete_gate_files: $fault_cycle_complete_gate_files,
        fault_resource_pairs: (if $fault_scenario == "reconnect-storm" then $fault_resource_pairs else 0 end),
        fault_window_messages: 64,
        performance: (if $fault_scenario == "quorum-performance-leader-loss" then {
            queue_type: "quorum",
            queues: [range(1; 5) | {
                name: "go-rabbitmq-queues.performance.quorum.\(.)",
                routing_key: "performance.quorum.\(.)"
            }],
            daily_messages: $daily_messages,
            warmup_seconds: 5,
            sample_seconds: 30,
            samples: 3,
            burst_multiplier: 4,
            burst_seconds: 5,
            publisher_concurrency: 64,
            consumer_concurrency: 16,
            payload_bytes: [256, 1024, 4096],
            header_bytes: [0, 64, 512],
            handler_delay_ms: 0
        } else {} end)
    }' >"${task_root}/live-cluster.json"
chmod 0600 "${task_root}/live-cluster.json"

test_log="${task_root}/cluster-test.log"
test_name='TestLiveBrokerThreeNodeInterruption'
ready_marker='FAULT_WINDOW_READY'
ready_wait_seconds=120
if [[ "${fault_scenario}" == quorum-performance-leader-loss ]]; then
    test_name='TestLiveBrokerThreeNodePerformanceLeaderLoss'
    ready_marker='PERFORMANCE_FAULT_READY'
    ready_wait_seconds=180
elif [[ "${fault_scenario}" == application-rolling-deployment ]]; then
    test_name='TestLiveBrokerApplicationRollingDeployment'
    ready_marker='APPLICATION_ROLLOUT_OLD_ADMITTED'
fi
(
    cd "${project_root}"
    GOTOOLCHAIN=local \
        GOWORK=off \
        GOCACHE="${task_root}/go-build" \
        GOMODCACHE="${task_root}/go-modules" \
        GOTMPDIR="${task_root}/go-tmp" \
        RABBITMQ_QUEUE_CLUSTER_CONFIG="${task_root}/live-cluster.json" \
        go test -v -count=1 -tags=livebroker -run "^${test_name}$" .
) >"${test_log}" 2>&1 &
test_pid=$!

ready=false
for _ in $(seq 1 "${ready_wait_seconds}"); do
    if grep -q "${ready_marker}" "${test_log}"; then
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

if [[ "${fault_scenario}" == application-rolling-deployment ]]; then
    rollout_queue_endpoint="queues/${encoded_vhost}/go-rabbitmq-queues.quorum"
    old_admitted=false
    for _ in $(seq 1 60); do
        if queue_json="$(get_json rabbit1 "${rollout_queue_endpoint}")" &&
            jq -e '.consumers == 1 and .messages_unacknowledged == 1' <<<"${queue_json}" >/dev/null; then
            old_admitted=true
            break
        fi
        sleep 1
    done
    if [[ "${old_admitted}" != true ]]; then
        printf '%s\n' 'old application consumer did not retain one admitted delivery' >&2
        cat "${test_log}"
        exit 1
    fi
    printf '%s\n' 'APPLICATION_ROLLOUT_BROKER phase=old-admitted consumers=1 unacknowledged=1'
    : >"${fault_start_gate}"

    old_drained=false
    for _ in $(seq 1 60); do
        if grep -q 'APPLICATION_ROLLOUT_OLD_DRAINED' "${test_log}" &&
            queue_json="$(get_json rabbit1 "${rollout_queue_endpoint}")" &&
            jq -e '.consumers == 0 and .messages_unacknowledged == 0' <<<"${queue_json}" >/dev/null; then
            old_drained=true
            break
        fi
        if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    if [[ "${old_drained}" != true ]]; then
        printf '%s\n' 'old application consumer did not drain before handoff' >&2
        cat "${test_log}"
        exit 1
    fi
    printf '%s\n' 'APPLICATION_ROLLOUT_BROKER phase=old-drained consumers=0 unacknowledged=0'
    : >"${fault_complete_gate}"

    new_ready=false
    for _ in $(seq 1 60); do
        if grep -q 'APPLICATION_ROLLOUT_NEW_READY' "${test_log}" &&
            queue_json="$(get_json rabbit1 "${rollout_queue_endpoint}")" &&
            jq -e '.consumers == 1 and .messages_unacknowledged == 0' <<<"${queue_json}" >/dev/null; then
            new_ready=true
            break
        fi
        if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    if [[ "${new_ready}" != true ]]; then
        printf '%s\n' 'new application consumer did not become sole queue owner' >&2
        cat "${test_log}"
        exit 1
    fi
    printf '%s\n' 'APPLICATION_ROLLOUT_BROKER phase=new-ready consumers=1 unacknowledged=0'
    : >"${fault_cycle_gate_files[0]}"

    if wait "${test_pid}"; then
        test_status=0
    else
        test_status=$?
    fi
    test_pid=''
    cat "${test_log}"
    test "${test_status}" -eq 0
    for _ in $(seq 1 60); do
        if queue_json="$(get_json rabbit1 "${rollout_queue_endpoint}")" &&
            jq -e '.consumers == 0 and .messages == 0 and .messages_unacknowledged == 0' \
                <<<"${queue_json}" >/dev/null; then
            break
        fi
        sleep 1
    done
    queue_json="$(get_json rabbit1 "${rollout_queue_endpoint}")"
    jq -e '.consumers == 0 and .messages == 0 and .messages_unacknowledged == 0' \
        <<<"${queue_json}" >/dev/null
    printf '%s\n' 'APPLICATION_ROLLOUT_BROKER phase=complete consumers=0 messages=0 unacknowledged=0'
    exit
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
        observed_consumers="$(jq -er '.consumers' <<<"${queue_json}")"
        if ! jq -e --argjson consumers "${reconnect_storm_resource_pairs}" \
            '.state == "running" and .consumers == $consumers' <<<"${queue_json}" >/dev/null; then
            printf 'CONSUMER_OUTCOMES scenario=%s cycle=%d expected=%d observed=%s state=%s\n' \
                "${fault_scenario}" "${cycle}" "${reconnect_storm_resource_pairs}" \
                "${observed_consumers}" "$(jq -er '.state' <<<"${queue_json}")" >&2
            cat "${test_log}"
            exit 1
        fi
        printf 'CONSUMER_OUTCOMES scenario=%s cycle=%d expected=%d observed=%s\n' \
            "${fault_scenario}" "${cycle}" "${reconnect_storm_resource_pairs}" "${observed_consumers}"
        : >"${fault_cycle_complete_gate_files[${index}]}"
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

if [[ "${fault_scenario}" == rolling-upgrade ]]; then
    cycle=0
    for node_index in 2 1 0; do
        cycle="$((cycle + 1))"
        gate_index="$((cycle - 1))"
        node_name="${node_names[${node_index}]}"
        container_name="${container_names[${node_index}]}"

        cycle_waiting=false
        for _ in $(seq 1 30); do
            if grep -q "UPGRADE_CYCLE_WAITING cycle=${cycle}" "${test_log}"; then
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

        test "$(docker exec "${container_name}" rabbitmqctl version)" = "${rolling_upgrade_source_version}"
        docker exec "${container_name}" rabbitmq-upgrade await_online_quorum_plus_one >/dev/null
        docker stop --time 10 "${container_name}" >/dev/null
        cycle_started_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
        : >"${fault_cycle_gate_files[${gate_index}]}"
        printf 'FAULT_TIMELINE scenario=%s queue_type=%s cycle=%d event=node-stopped node=rabbit@%s from_version=%s at=%s\n' \
            "${fault_scenario}" "${fault_queue_type}" "${cycle}" "${node_name}" \
            "${rolling_upgrade_source_version}" "${cycle_started_at}"

        fault_observed=false
        for _ in $(seq 1 30); do
            if grep -q "UPGRADE_CYCLE_FAULT_OBSERVED cycle=${cycle}" "${test_log}"; then
                fault_observed=true
                break
            fi
            if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        if [[ "${fault_observed}" != true ]]; then
            cat "${test_log}"
            exit 1
        fi

        docker rm "${container_name}" >/dev/null
        run_node_container "${node_index}" "${image}"
        for _ in $(seq 1 120); do
            if docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null
        test "$(docker exec "${container_name}" rabbitmqctl version)" = "${image_version}"
        docker exec "${container_name}" rabbitmq-diagnostics -q check_local_alarms >/dev/null
        for _ in $(seq 1 120); do
            if get_json "${node_name}" nodes |
                jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null; then
                break
            fi
            sleep 1
        done
        get_json "${node_name}" nodes |
            jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null
        for _ in $(seq 1 120); do
            if queue_json="$(get_json "${node_name}" "queues/${encoded_vhost}/go-rabbitmq-queues.quorum" 2>/dev/null)" &&
                jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
                    <<<"${queue_json}" >/dev/null; then
                break
            fi
            sleep 1
        done
        queue_json="$(get_json "${node_name}" "queues/${encoded_vhost}/go-rabbitmq-queues.quorum")"
        jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
            <<<"${queue_json}" >/dev/null

        messages_ready=false
        for _ in $(seq 1 120); do
            if grep -q "UPGRADE_CYCLE_MESSAGES_READY cycle=${cycle}" "${test_log}"; then
                messages_ready=true
                break
            fi
            if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        if [[ "${messages_ready}" != true ]]; then
            cat "${test_log}"
            exit 1
        fi
        client_ready=false
        for _ in $(seq 1 120); do
            if grep -q "UPGRADE_CYCLE_CLIENT_READY cycle=${cycle}" "${test_log}"; then
                client_ready=true
                break
            fi
            if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        if [[ "${client_ready}" != true ]]; then
            cat "${test_log}"
            exit 1
        fi
        for _ in $(seq 1 60); do
            if queue_json="$(get_json "${node_name}" "queues/${encoded_vhost}/go-rabbitmq-queues.quorum" 2>/dev/null)" &&
                jq -e '.state == "running" and .consumers == 1' <<<"${queue_json}" >/dev/null; then
                break
            fi
            sleep 1
        done
        queue_json="$(get_json "${node_name}" "queues/${encoded_vhost}/go-rabbitmq-queues.quorum")"
        jq -e '.state == "running" and .consumers == 1' <<<"${queue_json}" >/dev/null
        printf 'CONSUMER_OUTCOMES scenario=%s cycle=%d expected=1 observed=%s\n' \
            "${fault_scenario}" "${cycle}" "$(jq -er '.consumers' <<<"${queue_json}")"
        cycle_completed_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
        recovered_leader="$(jq -er '.leader' <<<"${queue_json}")"
        printf 'FAULT_TIMELINE scenario=%s queue_type=%s cycle=%d event=node-upgraded node=rabbit@%s to_version=%s leader=%s at=%s\n' \
            "${fault_scenario}" "${fault_queue_type}" "${cycle}" "${node_name}" \
            "${image_version}" "${recovered_leader}" "${cycle_completed_at}"
        : >"${fault_cycle_complete_gate_files[${gate_index}]}"

        cycle_verified=false
        for _ in $(seq 1 60); do
            if grep -q "UPGRADE_CYCLE_VERIFIED cycle=${cycle}" "${test_log}"; then
                cycle_verified=true
                break
            fi
            if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        if [[ "${cycle_verified}" != true ]]; then
            cat "${test_log}"
            exit 1
        fi
    done

    for container_name in "${container_names[@]}"; do
        test "$(docker exec "${container_name}" rabbitmqctl version)" = "${image_version}"
    done
    docker exec "${container_names[0]}" rabbitmqctl enable_feature_flag all >/dev/null
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

if [[ "${fault_scenario}" == prolonged-outage ]]; then
    docker stop --time 10 "${container_names[@]}" >/dev/null
    outage_started_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    : >"${fault_start_gate}"
    printf 'FAULT_TIMELINE scenario=%s queue_type=%s event=cluster-stopped at=%s\n' \
        "${fault_scenario}" "${fault_queue_type}" "${outage_started_at}"

    outage_observed=false
    for _ in $(seq 1 30); do
        if grep -q 'PROLONGED_OUTAGE_STARTED' "${test_log}"; then
            outage_observed=true
            break
        fi
        if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    if [[ "${outage_observed}" != true ]]; then
        cat "${test_log}"
        exit 1
    fi

    sleep "${prolonged_outage_seconds}"
    outage_hold_completed_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    : >"${fault_complete_gate}"
    printf 'FAULT_TIMELINE scenario=%s queue_type=%s event=outage-hold-complete hold_seconds=%d at=%s\n' \
        "${fault_scenario}" "${fault_queue_type}" "${prolonged_outage_seconds}" \
        "${outage_hold_completed_at}"

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
    recovered_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    printf 'FAULT_TIMELINE scenario=%s queue_type=%s event=cluster-restarted leader=%s at=%s\n' \
        "${fault_scenario}" "${fault_queue_type}" "$(jq -er '.leader' <<<"${queue_json}")" "${recovered_at}"

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

if [[ "${fault_scenario}" == quorum-performance-leader-loss ]]; then
    backlog_ready=false
    for _ in $(seq 1 30); do
        backlog_total=0
        backlog_depths=()
        backlog_ready=true
        for index in $(seq 1 4); do
            queue_name="go-rabbitmq-queues.performance.quorum.${index}"
            queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/${queue_name}")"
            queue_depth="$(jq -er '.messages' <<<"${queue_json}")"
            backlog_depths+=("${queue_depth}")
            backlog_total="$((backlog_total + queue_depth))"
            if ((queue_depth <= 0)); then
                backlog_ready=false
            fi
        done
        if [[ "${backlog_ready}" == true ]]; then
            break
        fi
        sleep 1
    done
    if [[ "${backlog_ready}" != true ]]; then
        printf '%s\n' 'all four performance queues did not contain a backlog before leader loss' >&2
        cat "${test_log}"
        exit 1
    fi
    printf 'BACKLOG_SNAPSHOT scenario=%s phase=before-fault total=%d queues=%s\n' \
        "${fault_scenario}" "${backlog_total}" "$(IFS=,; printf '%s' "${backlog_depths[*]}")"
    storage_driver="$(docker info --format '{{.Driver}}')"
    cpu_limit="$(docker inspect --format '{{.HostConfig.NanoCpus}}' "${container_names[0]}")"
    memory_limit="$(docker inspect --format '{{.HostConfig.Memory}}' "${container_names[0]}")"
    printf 'PERFORMANCE_ENVIRONMENT scenario=%s rabbitmq=%s image=%s daily_messages=%d queues=4 replicas=3 tls=true cpu_limit_nanocpus=%s memory_limit_bytes=%s storage_driver=%s\n' \
        "${fault_scenario}" "${image_version}" "${image}" "${daily_messages}" \
        "${cpu_limit}" "${memory_limit}" "${storage_driver}"

    queue_json="$(get_json rabbit1 "queues/${encoded_vhost}/go-rabbitmq-queues.performance.quorum.1")"
    fault_member="$(jq -er '.leader' <<<"${queue_json}")"
    fault_node="${fault_member#rabbit@}"
    fault_index=-1
    observer_node=''
    for index in "${!node_names[@]}"; do
        if [[ "${node_names[${index}]}" == "${fault_node}" ]]; then
            fault_index="${index}"
        else
            observer_node="${node_names[${index}]}"
        fi
    done
    if ((fault_index < 0)) || [[ -z "${observer_node}" ]]; then
        printf 'unable to map performance leader %s to a cluster container\n' "${fault_member}" >&2
        exit 1
    fi
    fault_container="${container_names[${fault_index}]}"
    docker stop --time 10 "${fault_container}" >/dev/null
    fault_started_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    fault_started_ms="$(date -u +%s%3N)"
    : >"${fault_start_gate}"
    printf 'FAULT_TIMELINE scenario=%s queue_type=%s event=node-stopped node=%s at=%s\n' \
        "${fault_scenario}" "${fault_queue_type}" "${fault_member}" "${fault_started_at}"

    recovered=false
    for _ in $(seq 1 120); do
        recovered=true
        if ! get_json "${observer_node}" nodes |
            jq -e '[.[] | select(.running == true)] | length == 2' >/dev/null; then
            recovered=false
        fi
        for index in $(seq 1 4); do
            queue_name="go-rabbitmq-queues.performance.quorum.${index}"
            if ! queue_json="$(get_json "${observer_node}" "queues/${encoded_vhost}/${queue_name}" 2>/dev/null)" ||
                ! jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
                    <<<"${queue_json}" >/dev/null; then
                recovered=false
                break
            fi
            if [[ "${index}" == 1 ]] && [[ "$(jq -er '.leader' <<<"${queue_json}")" == "${fault_member}" ]]; then
                recovered=false
                break
            fi
        done
        if [[ "${recovered}" == true ]]; then
            break
        fi
        sleep 1
    done
    if [[ "${recovered}" != true ]]; then
        printf '%s\n' 'four-queue quorum topology did not recover after leader loss' >&2
        cat "${test_log}"
        exit 1
    fi
    recovered_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    recovered_ms="$(date -u +%s%3N)"
    recovery_duration_ms="$((recovered_ms - fault_started_ms))"
    : >"${fault_complete_gate}"
    printf 'FAULT_TIMELINE scenario=%s queue_type=%s event=leaders-recovered stopped=%s recovery_ms=%d at=%s\n' \
        "${fault_scenario}" "${fault_queue_type}" "${fault_member}" \
        "${recovery_duration_ms}" "${recovered_at}"

    backlog_drained=false
    for _ in $(seq 1 180); do
        if grep -q 'PERFORMANCE_BACKLOG_DRAINED' "${test_log}"; then
            backlog_drained=true
            break
        fi
        if ! kill -0 "${test_pid}" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    if [[ "${backlog_drained}" != true ]]; then
        cat "${test_log}"
        exit 1
    fi
    management_backlog_drained=false
    for _ in $(seq 1 180); do
        backlog_depths=()
        management_backlog_drained=true
        for index in $(seq 1 4); do
            queue_name="go-rabbitmq-queues.performance.quorum.${index}"
            if ! queue_json="$(get_json "${observer_node}" "queues/${encoded_vhost}/${queue_name}" 2>/dev/null)" ||
                ! queue_depth="$(jq -er '.messages' <<<"${queue_json}")" ||
                ! jq -e '.state == "running"' <<<"${queue_json}" >/dev/null; then
                management_backlog_drained=false
                break
            fi
            backlog_depths+=("${queue_depth}")
            if ((queue_depth != 0)); then
                management_backlog_drained=false
            fi
        done
        if [[ "${management_backlog_drained}" == true ]]; then
            break
        fi
        sleep 1
    done
    if [[ "${management_backlog_drained}" != true ]]; then
        printf '%s\n' 'management backlog depth did not converge to zero after application drain' >&2
        cat "${test_log}"
        exit 1
    fi
    printf 'BACKLOG_SNAPSHOT scenario=%s phase=after-drain total=0 queues=%s\n' \
        "${fault_scenario}" "$(IFS=,; printf '%s' "${backlog_depths[*]}")"

    docker start "${fault_container}" >/dev/null
    for _ in $(seq 1 120); do
        if docker exec "${fault_container}" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    docker exec "${fault_container}" rabbitmq-diagnostics -q ping >/dev/null
    for _ in $(seq 1 120); do
        if get_json "${observer_node}" nodes |
            jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null; then
            break
        fi
        sleep 1
    done
    get_json "${observer_node}" nodes |
        jq -e '[.[] | select(.running == true)] | length == 3' >/dev/null
    for index in $(seq 1 4); do
        queue_name="go-rabbitmq-queues.performance.quorum.${index}"
        for _ in $(seq 1 120); do
            if queue_json="$(get_json "${observer_node}" "queues/${encoded_vhost}/${queue_name}" 2>/dev/null)" &&
                jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
                    <<<"${queue_json}" >/dev/null; then
                break
            fi
            sleep 1
        done
        queue_json="$(get_json "${observer_node}" "queues/${encoded_vhost}/${queue_name}")"
        jq -e '.state == "running" and (.members | length) == 3 and (.leader | length) > 0' \
            <<<"${queue_json}" >/dev/null
    done
    printf 'FAULT_TIMELINE scenario=%s queue_type=%s event=node-restored node=%s members=3\n' \
        "${fault_scenario}" "${fault_queue_type}" "${fault_member}"

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
        cluster_restart_outage_seconds=20
        sleep "${cluster_restart_outage_seconds}"
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
        recovery_event="cluster-restarted hold_seconds=${cluster_restart_outage_seconds} leader=${new_leader}"
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
