package tools

import (
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
)

// PermissionPolicy returns the static permission policy configured for one
// turn snapshot.
func PermissionPolicy(snapshot runtime.TurnSnapshot) permissions.Policy {
	return permissions.StaticPolicy{
		Rules: snapshot.Config.Tools.Permissions,
	}
}
