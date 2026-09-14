#!/usr/bin/env bash

set -euo pipefail

readonly root_module="github.com/mattsp1290/eino-agent"
readonly nested_module="${root_module}/wasmext/gen"
readonly nested_version="v0.1.0"
# The eino-agui bridge (adopted in W7) requires the AG-UI Go SDK fork below.
# Go replace directives are not transitive, so every consumer of eino-agent
# -- including this external-consumer check -- must carry this same root
# replacement itself; it is not satisfied by eino-agent's own go.mod replace.
readonly aguisdk_replace_path="github.com/ag-ui-protocol/ag-ui/sdks/community/go"
readonly aguisdk_replace_target="github.com/mattsp1290/ag-ui/sdks/community/go"
readonly aguisdk_replace_version="v0.0.0-20260909025854-aaa75b54d572"
# agentic_fixture_test.go imports the real native-provider constructors from
# github.com/mattsp1290/eino-providers directly. eino-agent's own go.mod does
# not (and should not: the library is provider-agnostic) require this
# module, so it is invisible to `go mod tidy` scanning eino-agent's own
# packages -- testdata/ directories are excluded from that scan the same way
# they are excluded from `go build ./...`. This consumer module DOES need
# it, since it is the one that actually combines eino-agent with a concrete
# native provider. Pin the exact commit verified in
# docs/dependency-status.md (no release tag exists upstream, so this is a
# pseudo-version pin) rather than letting a bare `go mod tidy` resolve
# whatever the module's default branch HEAD happens to be at run time.
readonly einoproviders_version="v0.0.0-20260914001852-8ff8a67b377e"
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
postgres_mode="${EINO_AGENT_CONSUMER_POSTGRES:-}"
if [[ -n "${postgres_mode}" && "${postgres_mode}" != "1" ]]; then
	printf 'external-consumer: EINO_AGENT_CONSUMER_POSTGRES must be 1 when set\n' >&2
	exit 1
fi
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
cp -f -- "${script_dir}/sqlite_pool_test.go" "${consumer_dir}/sqlite_pool_test.go"
cp -f -- "${script_dir}/session_discovery_fixture_test.go" "${consumer_dir}/session_discovery_fixture_test.go"
cp -f -- "${script_dir}/session_title_fixture_test.go" "${consumer_dir}/session_title_fixture_test.go"
cp -f -- "${script_dir}/session_watch_fixture_test.go" "${consumer_dir}/session_watch_fixture_test.go"
cp -f -- "${script_dir}/delegated_web_search_fixture_test.go" "${consumer_dir}/delegated_web_search_fixture_test.go"
cp -f -- "${script_dir}/agentic_fixture_test.go" "${consumer_dir}/agentic_fixture_test.go"
cp -f -- "${script_dir}/admission_receipt_fixture_test.go" "${consumer_dir}/admission_receipt_fixture_test.go"
if [[ "${postgres_mode}" == "1" ]]; then
	cp -f -- "${script_dir}/../../internal/testpostgres/check_output.py" "${temporary_root}/check_output.py"
	cp -f -- "${script_dir}/postgres_store_fixture_test.go" "${consumer_dir}/postgres_store_fixture_test.go"
fi

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
"${go_command[@]}" mod edit -replace="${aguisdk_replace_path}=${aguisdk_replace_target}@${aguisdk_replace_version}"
# Pin the exact verified eino-providers pseudo-version explicitly (see the
# comment at this script's top) rather than letting `go mod tidy` resolve
# whatever version its default branch happens to report right now.
"${go_command[@]}" mod edit -require="github.com/mattsp1290/eino-providers@${einoproviders_version}"

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
	if [[ "$(grep -Ec '^replace ' go.mod)" -ne 1 ]]; then
		printf 'external-consumer: published mode requires exactly the ag-ui-protocol host replacement\n' >&2
		exit 1
	fi
	grep -Fqx "replace ${aguisdk_replace_path} => ${aguisdk_replace_target} ${aguisdk_replace_version}" go.mod
else
	if [[ "$(grep -Ec '^replace ' go.mod)" -ne 2 ]]; then
		printf 'external-consumer: local mode requires exactly the root and ag-ui-protocol replacements\n' >&2
		exit 1
	fi
	grep -Fqx "replace ${root_module} => ${repository_root}" go.mod
	grep -Fqx "replace ${aguisdk_replace_path} => ${aguisdk_replace_target} ${aguisdk_replace_version}" go.mod
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
	root_provenance="$("${go_command[@]}" list -m -f '{{.Version}}|{{if .Origin}}{{.Origin.Hash}}{{end}}' "${root_module}@${selected_required_version}")"
	printf 'ROOT_MODULE_PROVENANCE=%s\n' "${root_provenance}"
	if [[ "${root_provenance}" != "${selected_required_version}|"* || -z "${root_provenance#*|}" ]]; then
		printf 'external-consumer: published root module has no full commit provenance\n' >&2
		exit 1
	fi
	if [[ "${required_version}" =~ ^[0-9a-f]{40}$ && "${root_provenance#*|}" != "${required_version}" ]]; then
		printf 'external-consumer: published commit provenance does not match requested pin\n' >&2
		exit 1
	fi
	if [[ "$(grep -Ec '^replace ' go.mod)" -ne 1 ]]; then
		printf 'external-consumer: tidy changed the expected replacement set in published mode\n' >&2
		exit 1
	fi
	grep -Fqx "replace ${aguisdk_replace_path} => ${aguisdk_replace_target} ${aguisdk_replace_version}" go.mod
fi

"${go_command[@]}" mod verify
if [[ "${postgres_mode}" == "1" ]]; then
	"${go_command[@]}" test -tags postgres_integration -timeout 10m -json ./... | python3 "${temporary_root}/check_output.py" "example.com/eino-agent-external-consumer:TestPostgresConsumer"
else
	"${go_command[@]}" test -race ./...
fi
"${go_command[@]}" build ./...

if [[ -e "${consumer_dir}/go.work" || -d "${consumer_dir}/vendor" ]]; then
	printf 'external-consumer: verification created forbidden workspace or vendor state\n' >&2
	exit 1
fi

printf 'external-consumer: %s verification passed\n' "${mode}"
