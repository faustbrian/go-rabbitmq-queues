#!/usr/bin/env bash

set -euo pipefail

if [[ "${CI:-}" != true || "${GITHUB_ACTIONS:-}" != true ]]; then
    printf '%s\n' 'ci-live-broker.sh may run only in GitHub Actions CI' >&2
    exit 1
fi

suite="${1:-single}"
if [[ "${suite}" != single && "${suite}" != php && "${suite}" != performance ]]; then
    printf '%s\n' 'ci-live-broker.sh suite must be single, php, or performance' >&2
    exit 1
fi
queue_type="${QUEUE_TYPE:-}"
daily_messages="${DAILY_MESSAGES:-0}"
if [[ "${suite}" == performance ]] &&
    { [[ "${queue_type}" != classic && "${queue_type}" != quorum ]] ||
        [[ ! "${daily_messages}" =~ ^(1000000|10000000|100000000)$ ]]; }; then
    printf '%s\n' 'performance suite requires a supported queue type and daily message target' >&2
    exit 1
fi

project_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
task_root="$(mktemp -d "${RUNNER_TEMP}/go-rabbitmq-queues-live.XXXXXX")"
container_name="go-rabbitmq-queues-live-${suite}-${queue_type:-none}-${daily_messages}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
bootstrap_password="$(openssl rand -hex 24)"
client_password="$(openssl rand -hex 24)"
erlang_cookie="$(openssl rand -hex 24)"
cleanup() {
    result=$?
    trap - EXIT HUP INT TERM
    if (( result != 0 )) && docker inspect "${container_name}" >/dev/null 2>&1; then
        docker logs "${container_name}" 2>&1 |
            sed \
                -e "s/${bootstrap_password}/[REDACTED]/g" \
                -e "s/${client_password}/[REDACTED]/g" \
                -e "s/${erlang_cookie}/[REDACTED]/g" >&2 || true
    fi
    docker rm --force "${container_name}" >/dev/null 2>&1 || true
    if [[ -d "${task_root}" ]]; then
        chmod -R u+w "${task_root}"
        find "${task_root}" -depth -delete
    fi
    exit "${result}"
}
trap cleanup EXIT HUP INT TERM

mkdir -p \
    "${task_root}/tls" \
    "${task_root}/rabbitmq-data" \
    "${task_root}/go-build" \
    "${task_root}/go-modules" \
    "${task_root}/go-tmp" \
    "${task_root}/composer-home" \
    "${task_root}/php-vendor"
chmod 0700 "${task_root}/rabbitmq-data"

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
ssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem
ssl_options.certfile = /etc/rabbitmq/tls/server.pem
ssl_options.keyfile = /etc/rabbitmq/tls/server-key.pem
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
ssl_options.versions.1 = tlsv1.3
ssl_options.versions.2 = tlsv1.2
EOF

bootstrap_user='ci-bootstrap'
client_user='ci-client'
vhost='/go-rabbitmq-queues-ci'
encoded_vhost='%2Fgo-rabbitmq-queues-ci'
image='rabbitmq:4.3.5-management-alpine@sha256:7224161872a48060e980a611f4778ad18168f00cfa974cab30604dbd855511dc'

docker run --detach \
    --name "${container_name}" \
    --user "$(id -u):$(id -g)" \
    --label 'com.faustbrian.task=go-rabbitmq-queues-live-ci' \
    --env "RABBITMQ_DEFAULT_USER=${bootstrap_user}" \
    --env "RABBITMQ_DEFAULT_PASS=${bootstrap_password}" \
    --env "RABBITMQ_DEFAULT_VHOST=${vhost}" \
    --env "RABBITMQ_ERLANG_COOKIE=${erlang_cookie}" \
    --mount "type=bind,source=${task_root}/rabbitmq.conf,target=/etc/rabbitmq/rabbitmq.conf,readonly" \
    --mount "type=bind,source=${task_root}/tls,target=/etc/rabbitmq/tls,readonly" \
    --mount "type=bind,source=${task_root}/rabbitmq-data,target=/var/lib/rabbitmq" \
    --publish 127.0.0.1::5671 \
    --publish 127.0.0.1::15672 \
    "${image}" >/dev/null

for _ in $(seq 1 120); do
    if docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null
test "$(docker exec "${container_name}" rabbitmqctl version)" = '4.3.5'
amqp_port="$(docker port "${container_name}" 5671/tcp | awk -F: 'NR == 1 { print $NF }')"
management_port="$(docker port "${container_name}" 15672/tcp | awk -F: 'NR == 1 { print $NF }')"
[[ "${amqp_port}" =~ ^[0-9]+$ ]]
[[ "${management_port}" =~ ^[0-9]+$ ]]

management_url="http://127.0.0.1:${management_port}/api"
for _ in $(seq 1 60); do
    if curl --fail --silent --show-error \
        --user "${bootstrap_user}:${bootstrap_password}" \
        "${management_url}/overview" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
curl --fail --silent --show-error \
    --user "${bootstrap_user}:${bootstrap_password}" \
    "${management_url}/overview" >/dev/null

put_json() {
    local endpoint="$1"
    local body="$2"
    curl --fail --silent --show-error \
        --user "${bootstrap_user}:${bootstrap_password}" \
        --header 'content-type: application/json' \
        --request PUT \
        --data "${body}" \
        "${management_url}/${endpoint}" >/dev/null
}

post_json() {
    local endpoint="$1"
    local body="$2"
    curl --fail --silent --show-error \
        --user "${bootstrap_user}:${bootstrap_password}" \
        --header 'content-type: application/json' \
        --request POST \
        --data "${body}" \
        "${management_url}/${endpoint}" >/dev/null
}

read_permission='^go-rabbitmq-queues\.(classic|quorum)$'
if [[ "${suite}" == php ]]; then
    read_permission='^go-rabbitmq-queues\.(classic|quorum|go-to-php|php-to-go)$'
elif [[ "${suite}" == performance ]]; then
    read_permission="^go-rabbitmq-queues\\.performance\\.${queue_type}\\.[1-4]$"
fi
put_json "users/${client_user}" "$(jq -cn --arg password "${client_password}" '{password: $password, tags: ""}')"
put_json "permissions/${encoded_vhost}/${client_user}" \
    "$(jq -cn --arg read "${read_permission}" \
        '{configure: "^$", write: "^go-rabbitmq-queues\\.events$", read: $read}')"
put_json "exchanges/${encoded_vhost}/go-rabbitmq-queues.events" \
    '{"type":"direct","auto_delete":false,"durable":true,"internal":false,"arguments":{}}'
put_json "queues/${encoded_vhost}/go-rabbitmq-queues.classic" \
    '{"auto_delete":false,"durable":true,"arguments":{"x-queue-type":"classic"}}'
put_json "queues/${encoded_vhost}/go-rabbitmq-queues.quorum" \
    '{"auto_delete":false,"durable":true,"arguments":{"x-queue-type":"quorum"}}'
post_json "bindings/${encoded_vhost}/e/go-rabbitmq-queues.events/q/go-rabbitmq-queues.classic" \
    '{"routing_key":"classic","arguments":{}}'
post_json "bindings/${encoded_vhost}/e/go-rabbitmq-queues.events/q/go-rabbitmq-queues.quorum" \
    '{"routing_key":"quorum","arguments":{}}'
if [[ "${suite}" == php ]]; then
    put_json "queues/${encoded_vhost}/go-rabbitmq-queues.go-to-php" \
        '{"auto_delete":false,"durable":true,"arguments":{"x-queue-type":"classic"}}'
    put_json "queues/${encoded_vhost}/go-rabbitmq-queues.php-to-go" \
        '{"auto_delete":false,"durable":true,"arguments":{"x-queue-type":"classic"}}'
    post_json "bindings/${encoded_vhost}/e/go-rabbitmq-queues.events/q/go-rabbitmq-queues.go-to-php" \
        '{"routing_key":"interop.go-to-php","arguments":{}}'
    post_json "bindings/${encoded_vhost}/e/go-rabbitmq-queues.events/q/go-rabbitmq-queues.php-to-go" \
        '{"routing_key":"interop.php-to-go","arguments":{}}'
elif [[ "${suite}" == performance ]]; then
    for index in $(seq 1 4); do
        queue_name="go-rabbitmq-queues.performance.${queue_type}.${index}"
        put_json "queues/${encoded_vhost}/${queue_name}" \
            "$(jq -cn --arg type "${queue_type}" \
                '{auto_delete: false, durable: true, arguments: {"x-queue-type": $type}}')"
        post_json "bindings/${encoded_vhost}/e/go-rabbitmq-queues.events/q/${queue_name}" \
            "$(jq -cn --arg routing_key "performance.${queue_type}.${index}" \
                '{routing_key: $routing_key, arguments: {}}')"
    done
fi

openssl s_client \
    -connect "127.0.0.1:${amqp_port}" \
    -servername localhost \
    -CAfile "${task_root}/tls/ca.pem" \
    -tls1_2 </dev/null >/dev/null 2>&1
openssl s_client \
    -connect "127.0.0.1:${amqp_port}" \
    -servername localhost \
    -CAfile "${task_root}/tls/ca.pem" \
    -tls1_3 </dev/null >/dev/null 2>&1

jq -n \
    --argjson port "${amqp_port}" \
    --arg vhost "${vhost}" \
    --arg username "${client_user}" \
    --arg password "${client_password}" \
    --arg root_ca_file "${task_root}/tls/ca.pem" \
    --arg php_binary "$(command -v php || true)" \
    --arg performance_queue_type "${queue_type}" \
    --argjson daily_messages "${daily_messages}" \
    '{
        endpoints: [{host: "127.0.0.1", port: $port}],
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
        php_interoperability: {
            binary: $php_binary,
            queue_type: "classic",
            go_to_php_queue: "go-rabbitmq-queues.go-to-php",
            go_to_php_routing_key: "interop.go-to-php",
            php_to_go_queue: "go-rabbitmq-queues.php-to-go",
            php_to_go_routing_key: "interop.php-to-go",
            unroutable_routing_key: "interop.intentionally-unbound"
        },
        performance: {
            queue_type: $performance_queue_type,
            queues: [range(1; 5) | {
                name: "go-rabbitmq-queues.performance.\($performance_queue_type).\(.)",
                routing_key: "performance.\($performance_queue_type).\(.)"
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
        }
    }' >"${task_root}/live-broker.json"
chmod 0600 "${task_root}/live-broker.json"

(
    cd "${project_root}"
    if [[ "${suite}" == php ]]; then
        test "$(php -r 'echo PHP_VERSION;')" = '8.5.9'
        composer --version --no-ansi | grep -Eq '^Composer version 2\.10\.1 '
        COMPOSER_HOME="${task_root}/composer-home" \
            COMPOSER_VENDOR_DIR="${task_root}/php-vendor" \
            composer install \
                --working-dir=testdata/interoperability/php \
                --no-dev \
                --no-interaction \
                --no-ansi \
                --no-progress \
                --classmap-authoritative
        PHP_AMQPLIB_AUTOLOAD="${task_root}/php-vendor/autoload.php" \
            GOTOOLCHAIN=local \
            GOWORK=off \
            GOCACHE="${task_root}/go-build" \
            GOMODCACHE="${task_root}/go-modules" \
            GOTMPDIR="${task_root}/go-tmp" \
            RABBITMQ_QUEUE_LIVE_CONFIG="${task_root}/live-broker.json" \
            go test -v -count=1 -tags=livebroker -run '^TestLiveBrokerPHPInteroperability$' .
    elif [[ "${suite}" == performance ]]; then
        GOTOOLCHAIN=local \
            GOWORK=off \
            GOCACHE="${task_root}/go-build" \
            GOMODCACHE="${task_root}/go-modules" \
            GOTMPDIR="${task_root}/go-tmp" \
            RABBITMQ_QUEUE_PERFORMANCE_CONFIG="${task_root}/live-broker.json" \
            go test -v -count=1 -tags=livebroker -run '^TestLiveBrokerPerformanceProfiles$' .
    else
        GOTOOLCHAIN=local \
            GOWORK=off \
            GOCACHE="${task_root}/go-build" \
            GOMODCACHE="${task_root}/go-modules" \
            GOTMPDIR="${task_root}/go-tmp" \
            RABBITMQ_QUEUE_LIVE_CONFIG="${task_root}/live-broker.json" \
            go test -v -count=1 -tags=livebroker -run '^TestLiveBrokerSingleNode$' .
    fi
)
