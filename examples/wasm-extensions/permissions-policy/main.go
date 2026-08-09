// Command permissions-policy is a minimal guest implementation of the
// eino-agent:extensions/permissions-policy@0.1.0 world.
package main

import (
	"go.bytecodealliance.org/cm"

	_ "github.com/mattsp1290/eino-agent/examples/wasm-extensions/internal/guestabi"
	policyapi "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/permissions-policy-api"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
	hostlog "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/host/v0.1.0/log"
)

func init() { policyapi.Exports.Decide = decide }

func decide(request policyapi.PermissionRequest) cm.Result[policyapi.StructuredErrorShape, policyapi.PermissionDecision, policyapi.StructuredError] {
	hostlog.Log(hostlog.LevelInfo, "evaluating example permission request")
	action := wittypes.PermissionActionAsk
	switch request.ArgumentsSummary {
	case "allow":
		action = wittypes.PermissionActionAllow
	case "deny":
		action = wittypes.PermissionActionDeny
	}
	return cm.OK[cm.Result[policyapi.StructuredErrorShape, policyapi.PermissionDecision, policyapi.StructuredError]](policyapi.PermissionDecision{
		Action: action,
		Reason: "example policy decision",
	})
}

func main() {}
