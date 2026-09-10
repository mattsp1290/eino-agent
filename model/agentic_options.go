package model

import (
	"encoding/json"
	"fmt"

	einoschema "github.com/cloudwego/eino/schema"
)

// RequestControls carries the agentic call-time knobs that used to live as
// scattered Request fields: tool partitions, tool choice and scalar
// generation controls.
type RequestControls struct {
	// Tools are ordinary function tools bound to the model for this request.
	// The list is always sent to the provider: a nil or empty slice
	// explicitly clears any tool set the underlying client was constructed
	// with.
	Tools []*einoschema.ToolInfo
	// DeferredTools are registered with defer_loading=true for the model's
	// built-in tool-search capability. Nil means "do not set"; a non-nil
	// (possibly empty) slice explicitly replaces the prior set.
	DeferredTools []*einoschema.ToolInfo
	// ToolSearchTool registers a tool-search tool with the model. It must not
	// also appear in Tools or DeferredTools.
	ToolSearchTool *einoschema.ToolInfo
	// ToolChoice controls how the agentic model calls tools.
	ToolChoice  *einoschema.AgenticToolChoice
	Temperature *float32
	TopP        *float32
	MaxTokens   *int
	Stop        []string
}

// clone returns a defensive deep copy of c. Tool lists and the tool choice
// are cloned exactly as Request.Clone clones them.
func (c RequestControls) clone() (RequestControls, error) {
	next := c
	var err error
	next.Tools, err = cloneToolInfos(c.Tools)
	if err != nil {
		return RequestControls{}, fmt.Errorf("clone controls tools: %w", err)
	}
	next.DeferredTools, err = cloneToolInfos(c.DeferredTools)
	if err != nil {
		return RequestControls{}, fmt.Errorf("clone controls deferred tools: %w", err)
	}
	if c.ToolSearchTool != nil {
		cloned, err := cloneToolInfos([]*einoschema.ToolInfo{c.ToolSearchTool})
		if err != nil {
			return RequestControls{}, fmt.Errorf("clone controls tool search tool: %w", err)
		}
		next.ToolSearchTool = cloned[0]
	}
	next.ToolChoice, err = cloneToolChoice(c.ToolChoice)
	if err != nil {
		return RequestControls{}, err
	}
	next.Temperature = clonePtr(c.Temperature)
	next.TopP = clonePtr(c.TopP)
	next.MaxTokens = clonePtr(c.MaxTokens)
	next.Stop = cloneSlice(c.Stop)
	return next, nil
}

// cloneToolChoice deep-clones an agentic tool choice via a JSON round trip.
func cloneToolChoice(src *einoschema.AgenticToolChoice) (*einoschema.AgenticToolChoice, error) {
	if src == nil {
		return nil, nil
	}
	raw, err := json.Marshal(src)
	if err != nil {
		return nil, fmt.Errorf("clone tool choice: %w", err)
	}
	var cloned einoschema.AgenticToolChoice
	if err := json.Unmarshal(raw, &cloned); err != nil {
		return nil, fmt.Errorf("clone tool choice: %w", err)
	}
	return &cloned, nil
}

// ValidateControls validates the tool partitions, tool choice identity and
// scalar generation controls of c before any adapter dispatches a request.
func ValidateControls(c RequestControls) error {
	names := make(map[string]struct{}, len(c.Tools)+len(c.DeferredTools))
	for _, t := range c.Tools {
		if t == nil || t.Name == "" {
			return toolPartitionError("tool requires a name")
		}
		if _, dup := names[t.Name]; dup {
			return toolPartitionError("duplicate tool name %q", t.Name)
		}
		names[t.Name] = struct{}{}
	}
	for _, t := range c.DeferredTools {
		if t == nil || t.Name == "" {
			return toolPartitionError("deferred tool requires a name")
		}
		if _, dup := names[t.Name]; dup {
			return toolPartitionError("duplicate tool name %q", t.Name)
		}
		names[t.Name] = struct{}{}
	}
	if c.ToolSearchTool != nil {
		if c.ToolSearchTool.Name == "" {
			return toolPartitionError("tool search tool requires a name")
		}
		if _, exists := names[c.ToolSearchTool.Name]; exists {
			return toolPartitionError("tool search tool name %q collides with an ordinary or deferred tool", c.ToolSearchTool.Name)
		}
	}
	if err := validateToolChoice(c.ToolChoice, names); err != nil {
		return err
	}
	if c.Temperature != nil && (*c.Temperature < 0 || *c.Temperature > 2) {
		return toolPartitionError("temperature %v out of range [0,2]", *c.Temperature)
	}
	if c.TopP != nil && (*c.TopP < 0 || *c.TopP > 1) {
		return toolPartitionError("top_p %v out of range [0,1]", *c.TopP)
	}
	if c.MaxTokens != nil && *c.MaxTokens <= 0 {
		return toolPartitionError("max_tokens must be positive")
	}
	for _, s := range c.Stop {
		if s == "" {
			return toolPartitionError("stop entries must be non-empty")
		}
	}
	return nil
}

func validateToolChoice(tc *einoschema.AgenticToolChoice, names map[string]struct{}) error {
	if tc == nil {
		return nil
	}
	if tc.Allowed != nil && tc.Forced != nil {
		return toolPartitionError("tool choice cannot set both Allowed and Forced")
	}
	switch tc.Type {
	case einoschema.ToolChoiceAllowed:
		if tc.Forced != nil {
			return toolPartitionError("tool choice type %q cannot carry Forced", tc.Type)
		}
	case einoschema.ToolChoiceForced:
		if tc.Allowed != nil {
			return toolPartitionError("tool choice type %q cannot carry Allowed", tc.Type)
		}
	case einoschema.ToolChoiceForbidden:
		if tc.Allowed != nil || tc.Forced != nil {
			return toolPartitionError("tool choice type %q cannot carry Allowed or Forced", tc.Type)
		}
	default:
		return toolPartitionError("unsupported tool choice type %q", tc.Type)
	}
	if tc.Allowed != nil {
		if err := validateAllowedToolList(tc.Allowed.Tools, names); err != nil {
			return err
		}
	}
	if tc.Forced != nil {
		if err := validateAllowedToolList(tc.Forced.Tools, names); err != nil {
			return err
		}
	}
	return nil
}

func validateAllowedToolList(tools []*einoschema.AllowedTool, names map[string]struct{}) error {
	for _, t := range tools {
		if t == nil {
			return toolPartitionError("allowed tool entry required")
		}
		set := 0
		if t.FunctionName != "" {
			set++
		}
		if t.MCPTool != nil {
			set++
		}
		if t.ServerTool != nil {
			set++
		}
		if set != 1 {
			return toolPartitionError("allowed tool entry must specify exactly one of FunctionName, MCPTool, ServerTool")
		}
		switch {
		case t.FunctionName != "":
			if _, ok := names[t.FunctionName]; !ok {
				return toolPartitionError("tool choice references unknown function %q", t.FunctionName)
			}
		case t.MCPTool != nil:
			if t.MCPTool.ServerLabel == "" || t.MCPTool.Name == "" {
				return toolPartitionError("MCP allowed tool requires a server label and a name")
			}
		case t.ServerTool != nil:
			if t.ServerTool.Name == "" {
				return toolPartitionError("server allowed tool requires a name")
			}
		}
	}
	return nil
}

func toolPartitionError(format string, args ...any) error {
	return Error{
		Code:    "tool_partition_invalid",
		Message: fmt.Sprintf(format, args...),
		Cause:   ErrProviderRejected,
	}
}
