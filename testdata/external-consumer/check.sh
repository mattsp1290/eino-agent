#!/usr/bin/env bash

set -euo pipefail

readonly root_module="github.com/mattsp1290/eino-agent"
readonly nested_module="${root_module}/wasmext/gen"
readonly nested_version="v0.1.0"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly script_dir
repository_root="$(cd -- "${script_dir}/../.." && pwd -P)"
readonly repository_root

temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/eino-agent-consumer.XXXXXX")"
temporary_root="$(cd -- "${temporary_root}" && pwd -P)"
readonly temporary_root
readonly consumer_dir="${temporary_root}/consumer"
readonly module_cache="${temporary_root}/module-cache"

cleanup() {
	chmod -R u+w -- "${temporary_root}" 2>/dev/null || true
	rm -rf -- "${temporary_root}"
}
trap cleanup EXIT

published_version="${EINO_AGENT_CONSUMER_VERSION:-}"
if [[ -n "${published_version}" ]]; then
	mode="published"
	required_version="${published_version}"
else
	mode="local"
	required_version="v0.0.0"
fi

require_exact_go_env() {
	local name="$1"
	local expected="$2"
	local actual
	actual="$(GOWORK=off go env "${name}")"
	if [[ "${actual}" != "${expected}" ]]; then
		printf 'external-consumer: %s must equal %s\n' "${name}" "${expected}" >&2
		return 1
	fi
	printf '%s=%s\n' "${name}" "${actual}"
}

require_empty_go_env() {
	local name="$1"
	local actual
	actual="$(GOWORK=off go env "${name}")"
	if [[ -n "${actual}" ]]; then
		printf '%s_empty=false\n' "${name}" >&2
		return 1
	fi
	printf '%s_empty=true\n' "${name}"
}

if [[ "${mode}" == "published" ]]; then
	require_exact_go_env GOPROXY "https://proxy.golang.org,direct"
	require_exact_go_env GOSUMDB "sum.golang.org"
	require_empty_go_env GOFLAGS
	require_empty_go_env GOPRIVATE
	require_empty_go_env GONOSUMDB
	require_empty_go_env GONOPROXY

	go_version="$(GOWORK=off go version)"
	if [[ "${go_version}" != *" go1.26.3 "* ]]; then
		printf 'external-consumer: go version must report go1.26.3\n' >&2
		exit 1
	fi
	printf 'GO_VERSION=%s\n' "${go_version}"
fi

mkdir -p -- "${consumer_dir}" "${module_cache}"
cp -f -- "${script_dir}/consumer.go" "${consumer_dir}/consumer.go"
cp -f -- "${script_dir}/delegated_web_search_fixture_test.go" "${consumer_dir}/delegated_web_search_fixture_test.go"

cd -- "${consumer_dir}"

go_command=(env GOWORK=off GOMODCACHE="${module_cache}" go)
if [[ "${mode}" == "published" ]]; then
	go_command=(env GOWORK=off GOMODCACHE="${module_cache}" GOPROXY=https://proxy.golang.org go)
	printf 'MODULE_DOWNLOAD_GOPROXY=https://proxy.golang.org\n'
fi
"${go_command[@]}" mod init example.com/eino-agent-external-consumer
"${go_command[@]}" mod edit -go=1.26.3
selected_required_version="${required_version}"
if [[ "${mode}" == "published" ]]; then
	selected_required_version="$("${go_command[@]}" list -m -f '{{.Version}}' "${root_module}@${required_version}")"
	if [[ -z "${selected_required_version}" ]]; then
		printf 'external-consumer: root query %s did not resolve to a version\n' "${required_version}" >&2
		exit 1
	fi
fi
printf 'ROOT_MODULE_REQUESTED=%s@%s\n' "${root_module}" "${required_version}"
printf 'ROOT_MODULE_SELECTED=%s@%s\n' "${root_module}" "${selected_required_version}"
"${go_command[@]}" mod edit -require="${root_module}@${selected_required_version}"

if [[ "${mode}" == "local" ]]; then
	"${go_command[@]}" mod edit -replace="${root_module}=${repository_root}"
fi

if [[ "$("${go_command[@]}" env GOMOD)" != "${consumer_dir}/go.mod" ]]; then
	printf 'external-consumer: go command selected an unexpected module\n' >&2
	exit 1
fi
if [[ -e "${consumer_dir}/go.work" || -d "${consumer_dir}/vendor" ]]; then
	printf 'external-consumer: workspace or vendor state is not allowed\n' >&2
	exit 1
fi

if [[ "${mode}" == "published" ]]; then
	if grep -Eq '^[[:space:]]*replace[[:space:](]' go.mod; then
		printf 'external-consumer: published mode must not contain replacements\n' >&2
		exit 1
	fi
else
	if [[ "$(grep -Ec '^replace ' go.mod)" -ne 1 ]]; then
		printf 'external-consumer: local mode requires exactly one root replacement\n' >&2
		exit 1
	fi
	grep -Fqx "replace ${root_module} => ${repository_root}" go.mod
fi

"${go_command[@]}" mod tidy
"${go_command[@]}" list -m all

nested_selection="$("${go_command[@]}" list -m -f '{{.Version}}|{{if .Replace}}{{.Replace.Path}}{{end}}' "${nested_module}")"
if [[ "${nested_selection}" != "${nested_version}|" ]]; then
	printf 'external-consumer: %s selected %s, expected %s without replacement\n' \
		"${nested_module}" "${nested_selection}" "${nested_version}|" >&2
	exit 1
fi
printf 'NESTED_MODULE=%s@%s replacement=false\n' "${nested_module}" "${nested_version}"

if [[ "${mode}" == "published" ]]; then
	root_selection="$("${go_command[@]}" list -m -f '{{.Version}}|{{if .Replace}}{{.Replace.Path}}{{end}}' "${root_module}")"
	if [[ "${root_selection}" != "${selected_required_version}|" ]]; then
		printf 'external-consumer: root selected %s, expected %s without replacement\n' \
			"${root_selection}" "${selected_required_version}|" >&2
		exit 1
	fi
	if grep -Eq '^[[:space:]]*replace[[:space:](]' go.mod; then
		printf 'external-consumer: tidy introduced a replacement in published mode\n' >&2
		exit 1
	fi
fi

"${go_command[@]}" mod verify
"${go_command[@]}" test ./...
"${go_command[@]}" build ./...

if [[ -e "${consumer_dir}/go.work" || -d "${consumer_dir}/vendor" ]]; then
	printf 'external-consumer: verification created forbidden workspace or vendor state\n' >&2
	exit 1
fi

printf 'external-consumer: %s verification passed\n' "${mode}"
