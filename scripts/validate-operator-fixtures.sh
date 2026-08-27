#!/bin/sh

set -eu

project_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
task_root=$(mktemp -d /tmp/go-rabbitmq-queues-operator.XXXXXX)
cleanup() {
    if [ -d "$task_root" ]; then
        chmod -R u+w "$task_root"
        find "$task_root" -depth -delete
    fi
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir "$task_root/cluster-operator" "$task_root/topology-operator" \
    "$task_root/kubeconform" "$task_root/schemas" "$task_root/go-build" \
    "$task_root/go-modules" "$task_root/bin"

git -c advice.detachedHead=false clone --quiet --depth 1 --branch v2.22.5 \
    https://github.com/rabbitmq/cluster-operator.git "$task_root/cluster-operator"
git -c advice.detachedHead=false clone --quiet --depth 1 --branch v1.20.2 \
    https://github.com/rabbitmq/messaging-topology-operator.git "$task_root/topology-operator"
git -c advice.detachedHead=false clone --quiet --depth 1 --branch v0.8.0 \
    https://github.com/yannh/kubeconform.git "$task_root/kubeconform"

test "$(git -C "$task_root/cluster-operator" rev-parse HEAD)" = \
    17dd297f71de40a722baf69167b8af511072175e
test "$(git -C "$task_root/topology-operator" rev-parse HEAD)" = \
    58cdfa3610a8bbac51a0fc8a7fd90f2fa448b960
test "$(git -C "$task_root/kubeconform" rev-parse HEAD)" = \
    02374e583d700721f57300fae78e11acd27ee539

python3 -m venv "$task_root/python"
python_bin="$task_root/python/bin/python"
"$python_bin" -m pip --isolated install --index-url https://pypi.org/simple \
    --disable-pip-version-check --no-cache-dir --no-input --quiet \
    PyYAML==6.0.3
(
    cd "$task_root/kubeconform"
    GOTOOLCHAIN=local GOWORK=off GOCACHE="$task_root/go-build" \
        GOMODCACHE="$task_root/go-modules" GOBIN="$task_root/bin" \
        go install ./cmd/kubeconform
)

cluster_crds="$task_root/cluster-operator/config/crd/bases"
topology_crds="$task_root/topology-operator/config/crd/bases"
converter="$task_root/kubeconform/scripts/openapi2jsonschema.py"
(
    cd "$task_root/schemas"
    for crd in \
        "$cluster_crds/rabbitmq.com_rabbitmqclusters.yaml" \
        "$topology_crds/rabbitmq.com_bindings.yaml" \
        "$topology_crds/rabbitmq.com_exchanges.yaml" \
        "$topology_crds/rabbitmq.com_permissions.yaml" \
        "$topology_crds/rabbitmq.com_policies.yaml" \
        "$topology_crds/rabbitmq.com_queues.yaml" \
        "$topology_crds/rabbitmq.com_users.yaml" \
        "$topology_crds/rabbitmq.com_vhosts.yaml"
    do
        DENY_ROOT_ADDITIONAL_PROPERTIES=1 FILENAME_FORMAT='{kind}_{version}' \
            "$python_bin" "$converter" "$crd"
    done
)

"$task_root/bin/kubeconform" -strict -summary -verbose \
    -schema-location "$task_root/schemas/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json" \
    "$project_root/testdata/operator"
