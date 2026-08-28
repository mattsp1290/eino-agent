package einotools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cloudwego/eino/components/tool"
	"github.com/mattsp1290/eino-tools/catalog"

	"github.com/mattsp1290/eino-agent/internal/workspace"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

func executeDefinition(definition catalog.Definition) agenttools.Executor {
	return func(ctx context.Context, execution agenttools.Execution) (json.RawMessage, error) {
		input := execution.Input
		instance := catalog.Instance{}
		lockKey := "registration:" + definition.ID
		if definition.Binding == catalog.BindingWorkspace {
			root, err := workspace.CanonicalRoot(execution.Context.WorkspaceRoot)
			if err != nil {
				return nil, err
			}
			if root != execution.Context.WorkspaceRoot {
				return nil, fmt.Errorf("%w: admitted workspace root is not canonical", workspace.ErrInvalidRoot)
			}
			instance.WorkspaceRoot = root
			lockKey = "workspace:" + root
		}
		invokeLeaf := func() (json.RawMessage, error) {
			leaf, err := definition.New(ctx, instance)
			if err != nil {
				return nil, err
			}
			return invoke(ctx, leaf, input)
		}
		if definition.Concurrent {
			return invokeLeaf()
		}
		var result json.RawMessage
		err := standardLocks.Do(ctx, lockKey, func() error {
			var err error
			result, err = invokeLeaf()
			return err
		})
		return result, err
	}
}

func invoke(ctx context.Context, leaf tool.InvokableTool, input json.RawMessage) (json.RawMessage, error) {
	output, err := leaf.InvokableRun(ctx, string(input))
	if err != nil {
		return nil, err
	}
	raw := json.RawMessage(output)
	if !json.Valid(raw) {
		return nil, fmt.Errorf("eino-tools returned invalid JSON")
	}
	return cloneRaw(raw), nil
}
