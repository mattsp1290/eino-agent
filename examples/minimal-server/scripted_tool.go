package main

import (
	"context"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/tools"
)

type echoInput struct {
	Text string `json:"text"`
}

func mountScriptedTool(ctx context.Context, registry *composition.Registry) (*composition.Mount, error) {
	return registry.Mount(ctx, extension.Component{
		InstanceID: "scripted-echo",
		Artifact:   extension.Artifact{Name: "scripted-echo", Version: "v1", Hash: "scripted-echo-v1", ConfigHash: "default", SourceKind: extension.SourceNative},
	}, composition.InstallerFunc(func(_ context.Context, r *composition.Registrar) error {
		return r.Tool(composition.ToolRegistration{ID: "echo", Scope: extension.GlobalScope(), Definition: tools.Definition{
			Name: "echo", Description: "Echo safe scripted input locally.",
			Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{"text": {Type: einoschema.String, Required: true}}),
			Execute:    tools.TypedExecutor[echoInput, echoInput](func(_ context.Context, e tools.TypedExecution[echoInput]) (echoInput, error) { return e.Input, nil }),
		}})
	}))
}
func currentTurnHasTool(messages []*einoschema.Message) bool {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == einoschema.User {
			return false
		}
		if messages[i].Role == einoschema.Tool {
			return true
		}
	}
	return false
}
