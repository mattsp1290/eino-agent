package session

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"time"
	"unicode/utf8"

	"github.com/eino-contrib/jsonschema"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/cloudwego/eino/schema/claude"
	"github.com/cloudwego/eino/schema/gemini"
	"github.com/cloudwego/eino/schema/openai"
)

// ContentSchemaVersion is the durable schema version stamped into every
// content block and response-meta envelope persisted by this file.
const ContentSchemaVersion = 1

// BlockKind identifies one of the durable content block variants. String
// values are identical to Eino's schema.ContentBlockType values so that
// mapping to and from *schema.AgenticMessage never needs translation tables.
type BlockKind string

const (
	BlockKindReasoning               BlockKind = "reasoning"
	BlockKindUserInputText           BlockKind = "user_input_text"
	BlockKindUserInputImage          BlockKind = "user_input_image"
	BlockKindUserInputAudio          BlockKind = "user_input_audio"
	BlockKindUserInputVideo          BlockKind = "user_input_video"
	BlockKindUserInputFile           BlockKind = "user_input_file"
	BlockKindToolSearchResult        BlockKind = "tool_search_result"
	BlockKindAssistantGenText        BlockKind = "assistant_gen_text"
	BlockKindAssistantGenImage       BlockKind = "assistant_gen_image"
	BlockKindAssistantGenAudio       BlockKind = "assistant_gen_audio"
	BlockKindAssistantGenVideo       BlockKind = "assistant_gen_video"
	BlockKindFunctionToolCall        BlockKind = "function_tool_call"
	BlockKindFunctionToolResult      BlockKind = "function_tool_result"
	BlockKindServerToolCall          BlockKind = "server_tool_call"
	BlockKindServerToolResult        BlockKind = "server_tool_result"
	BlockKindMCPToolCall             BlockKind = "mcp_tool_call"
	BlockKindMCPToolResult           BlockKind = "mcp_tool_result"
	BlockKindMCPListToolsResult      BlockKind = "mcp_list_tools_result"
	BlockKindMCPToolApprovalRequest  BlockKind = "mcp_tool_approval_request"
	BlockKindMCPToolApprovalResponse BlockKind = "mcp_tool_approval_response"
)

// AllBlockKinds returns the 20 durable block kinds in declaration order.
func AllBlockKinds() []BlockKind {
	return []BlockKind{
		BlockKindReasoning, BlockKindUserInputText, BlockKindUserInputImage, BlockKindUserInputAudio,
		BlockKindUserInputVideo, BlockKindUserInputFile, BlockKindToolSearchResult, BlockKindAssistantGenText,
		BlockKindAssistantGenImage, BlockKindAssistantGenAudio, BlockKindAssistantGenVideo, BlockKindFunctionToolCall,
		BlockKindFunctionToolResult, BlockKindServerToolCall, BlockKindServerToolResult, BlockKindMCPToolCall,
		BlockKindMCPToolResult, BlockKindMCPListToolsResult, BlockKindMCPToolApprovalRequest, BlockKindMCPToolApprovalResponse,
	}
}

// partKindByBlockKind and blockKindByPartKind are the 1:1 maps between the
// public BlockKind space and the durable PartKind space. BlockKindReasoning
// reuses the pre-existing PartReasoning constant since both already share the
// "reasoning" string; every other block kind gets its own PartKind constant
// declared in types.go.
var partKindByBlockKind = map[BlockKind]PartKind{
	BlockKindReasoning:               PartReasoning,
	BlockKindUserInputText:           PartUserInputText,
	BlockKindUserInputImage:          PartUserInputImage,
	BlockKindUserInputAudio:          PartUserInputAudio,
	BlockKindUserInputVideo:          PartUserInputVideo,
	BlockKindUserInputFile:           PartUserInputFile,
	BlockKindToolSearchResult:        PartToolSearchResult,
	BlockKindAssistantGenText:        PartAssistantGenText,
	BlockKindAssistantGenImage:       PartAssistantGenImage,
	BlockKindAssistantGenAudio:       PartAssistantGenAudio,
	BlockKindAssistantGenVideo:       PartAssistantGenVideo,
	BlockKindFunctionToolCall:        PartFunctionToolCall,
	BlockKindFunctionToolResult:      PartFunctionToolResult,
	BlockKindServerToolCall:          PartServerToolCall,
	BlockKindServerToolResult:        PartServerToolResult,
	BlockKindMCPToolCall:             PartMCPToolCall,
	BlockKindMCPToolResult:           PartMCPToolResult,
	BlockKindMCPListToolsResult:      PartMCPListToolsResult,
	BlockKindMCPToolApprovalRequest:  PartMCPToolApprovalRequest,
	BlockKindMCPToolApprovalResponse: PartMCPToolApprovalResponse,
}

var blockKindByPartKind = func() map[PartKind]BlockKind {
	out := make(map[PartKind]BlockKind, len(partKindByBlockKind))
	for block, part := range partKindByBlockKind {
		out[part] = block
	}
	return out
}()

// PartKindForBlock returns the durable PartKind that stores the given block
// kind. It returns "" for an unrecognized BlockKind.
func PartKindForBlock(kind BlockKind) PartKind {
	return partKindByBlockKind[kind]
}

// BlockKindForPart returns the BlockKind stored by a given durable PartKind.
// ok is false for legacy or non-content part kinds (including provider_state
// and response_meta), which are not block-shaped.
func BlockKindForPart(kind PartKind) (BlockKind, bool) {
	block, ok := blockKindByPartKind[kind]
	return block, ok
}

// envelopeKeyForKind maps each BlockKind to the JSON key used for its variant
// payload inside the on-wire content block envelope.
var envelopeKeyForKind = map[BlockKind]string{
	BlockKindReasoning:               "reasoning",
	BlockKindUserInputText:           "text",
	BlockKindAssistantGenText:        "text",
	BlockKindUserInputImage:          "media",
	BlockKindUserInputAudio:          "media",
	BlockKindUserInputVideo:          "media",
	BlockKindUserInputFile:           "media",
	BlockKindAssistantGenImage:       "media",
	BlockKindAssistantGenAudio:       "media",
	BlockKindAssistantGenVideo:       "media",
	BlockKindFunctionToolCall:        "function_call",
	BlockKindFunctionToolResult:      "function_result",
	BlockKindToolSearchResult:        "tool_search",
	BlockKindServerToolCall:          "server_call",
	BlockKindServerToolResult:        "server_result",
	BlockKindMCPToolCall:             "mcp_call",
	BlockKindMCPToolResult:           "mcp_result",
	BlockKindMCPListToolsResult:      "mcp_list_tools",
	BlockKindMCPToolApprovalRequest:  "mcp_approval_request",
	BlockKindMCPToolApprovalResponse: "mcp_approval_response",
}

// ReasoningBlock is the public projection of model reasoning content. The
// provider signature and any encrypted reasoning material are private and
// never appear here.
type ReasoningBlock struct {
	Text    string   `json:"text,omitempty"`
	Summary []string `json:"summary"`
}

// TextAnnotation is a neutral closed projection of openai.TextAnnotation and
// claude.TextCitation. Claude's EncryptedIndex is private and never appears
// here.
type TextAnnotation struct {
	Type string `json:"type"`

	Title       string `json:"title,omitempty"`
	URL         string `json:"url,omitempty"`
	FileID      string `json:"file_id,omitempty"`
	Filename    string `json:"filename,omitempty"`
	ContainerID string `json:"container_id,omitempty"`

	StartIndex int `json:"start_index,omitempty"`
	EndIndex   int `json:"end_index,omitempty"`

	CitedText     string `json:"cited_text,omitempty"`
	DocumentTitle string `json:"document_title,omitempty"`
	DocumentIndex int    `json:"document_index,omitempty"`

	StartPage int `json:"start_page,omitempty"`
	EndPage   int `json:"end_page,omitempty"`

	StartBlock int `json:"start_block,omitempty"`
	EndBlock   int `json:"end_block,omitempty"`

	// AnnotationIndex carries the provider's own index for this citation
	// (openai file_citation.index / file_path.index). It is distinct from
	// the outer openai.TextAnnotation.Index, which Eino clears on
	// finalisation and which this contract does not persist.
	AnnotationIndex int `json:"annotation_index,omitempty"`
}

// TextBlock is the public payload for user_input_text and assistant_gen_text
// blocks.
type TextBlock struct {
	Text        string           `json:"text,omitempty"`
	Refusal     string           `json:"refusal,omitempty"`
	Annotations []TextAnnotation `json:"annotations"`
}

// MediaBlock is the public payload for the image/audio/video/file variants of
// both user input and assistant-generated content. Exactly one of URL or
// Base64Data is required. Name only applies to file blocks; Detail only
// applies to image blocks.
type MediaBlock struct {
	URL        string `json:"url,omitempty"`
	Base64Data string `json:"base64_data,omitempty"`
	MIMEType   string `json:"mime_type,omitempty"`
	Name       string `json:"name,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

// FunctionCallBlock is the public payload for function_tool_call blocks.
type FunctionCallBlock struct {
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments,omitempty"`
}

// ResultContentType identifies which field of a ResultContent is populated.
type ResultContentType string

const (
	ResultContentText  ResultContentType = "text"
	ResultContentImage ResultContentType = "image"
	ResultContentAudio ResultContentType = "audio"
	ResultContentVideo ResultContentType = "video"
	ResultContentFile  ResultContentType = "file"
)

// ResultContent is a single content item inside a function tool result.
type ResultContent struct {
	Type  ResultContentType `json:"type"`
	Text  string            `json:"text,omitempty"`
	Media *MediaBlock       `json:"media,omitempty"`
}

// FunctionResultBlock is the public payload for function_tool_result blocks.
type FunctionResultBlock struct {
	CallID  string          `json:"call_id"`
	Name    string          `json:"name"`
	Content []ResultContent `json:"content"`
}

// ToolSearchBlock is the public payload for tool_search_result blocks. Each
// entry in Tools is the exact schema.ToolInfo.MarshalJSON output, preserving
// the nil-vs-empty ParamsOneOf distinction.
type ToolSearchBlock struct {
	CallID string            `json:"call_id"`
	Name   string            `json:"name"`
	Tools  []json.RawMessage `json:"tools"`
}

// ToolInfos decodes every Tools entry back into an *schema.ToolInfo.
func (b *ToolSearchBlock) ToolInfos() ([]*einoschema.ToolInfo, error) {
	if b == nil || b.Tools == nil {
		return nil, nil
	}
	out := make([]*einoschema.ToolInfo, 0, len(b.Tools))
	for _, raw := range b.Tools {
		info := &einoschema.ToolInfo{}
		if err := json.Unmarshal(raw, info); err != nil {
			return nil, errors.Join(ErrContentInvalid, err)
		}
		out = append(out, info)
	}
	return out, nil
}

// ServerCallBlock is the public payload for server_tool_call blocks. The
// original Arguments value (Go `any`) is stored as bounded finite JSON.
type ServerCallBlock struct {
	Name      string          `json:"name"`
	CallID    string          `json:"call_id,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// ServerResultBlock is the public payload for server_tool_result blocks.
type ServerResultBlock struct {
	Name    string          `json:"name"`
	CallID  string          `json:"call_id,omitempty"`
	Content json.RawMessage `json:"content,omitempty"`
}

// MCPCallBlock is the public payload for mcp_tool_call blocks.
type MCPCallBlock struct {
	ServerLabel       string `json:"server_label,omitempty"`
	ApprovalRequestID string `json:"approval_request_id,omitempty"`
	CallID            string `json:"call_id,omitempty"`
	Name              string `json:"name"`
	Arguments         string `json:"arguments,omitempty"`
}

// MCPResultBlock is the public payload for mcp_tool_result blocks.
type MCPResultBlock struct {
	ServerLabel  string `json:"server_label,omitempty"`
	CallID       string `json:"call_id,omitempty"`
	Name         string `json:"name"`
	Content      string `json:"content,omitempty"`
	ErrorCode    *int64 `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// MCPToolDefinition is one tool definition inside an mcp_list_tools_result
// block.
type MCPToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

// MCPListToolsBlock is the public payload for mcp_list_tools_result blocks.
type MCPListToolsBlock struct {
	ServerLabel string              `json:"server_label,omitempty"`
	Tools       []MCPToolDefinition `json:"tools"`
	Error       string              `json:"error,omitempty"`
}

// MCPApprovalRequestBlock is the public payload for
// mcp_tool_approval_request blocks.
type MCPApprovalRequestBlock struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Arguments   string `json:"arguments,omitempty"`
	ServerLabel string `json:"server_label,omitempty"`
}

// MCPApprovalResponseBlock is the public payload for
// mcp_tool_approval_response blocks.
type MCPApprovalResponseBlock struct {
	ApprovalRequestID string `json:"approval_request_id"`
	Approve           bool   `json:"approve"`
	Reason            string `json:"reason,omitempty"`
}

// ContentBlock is one durable, ordered unit of rich content. Exactly one of
// the typed payload pointers is populated, selected by Kind.
type ContentBlock struct {
	ID   string
	Kind BlockKind

	Reasoning           *ReasoningBlock
	Text                *TextBlock
	Media               *MediaBlock
	FunctionCall        *FunctionCallBlock
	FunctionResult      *FunctionResultBlock
	ToolSearch          *ToolSearchBlock
	ServerCall          *ServerCallBlock
	ServerResult        *ServerResultBlock
	MCPCall             *MCPCallBlock
	MCPResult           *MCPResultBlock
	MCPListTools        *MCPListToolsBlock
	MCPApprovalRequest  *MCPApprovalRequestBlock
	MCPApprovalResponse *MCPApprovalResponseBlock
}

// Segment identifies a public sub-range of a Gemini grounding support.
type Segment struct {
	Start     int    `json:"start,omitempty"`
	End       int    `json:"end,omitempty"`
	PartIndex int    `json:"part_index,omitempty"`
	Text      string `json:"text,omitempty"`
}

// GroundingChunk is one public Gemini grounding source.
type GroundingChunk struct {
	Domain string `json:"domain,omitempty"`
	Title  string `json:"title,omitempty"`
	URI    string `json:"uri,omitempty"`
}

// GroundingSupport is one public Gemini grounding attribution.
type GroundingSupport struct {
	ConfidenceScores []float32 `json:"confidence_scores"`
	ChunkIndices     []int     `json:"chunk_indices"`
	Segment          *Segment  `json:"segment,omitempty"`
}

// Grounding is the public projection of Gemini grounding metadata. The
// search entry point's SDK blob is private and never appears here.
type Grounding struct {
	Chunks             []GroundingChunk   `json:"chunks"`
	Supports           []GroundingSupport `json:"supports"`
	RenderedEntryPoint string             `json:"rendered_entry_point,omitempty"`
	WebSearchQueries   []string           `json:"web_search_queries"`
}

// OpenAIResponseMeta is the public projection of openai.ResponseMetaExtension.
// The response ID, previous response ID, and created-at timestamp are private
// and never appear here.
type OpenAIResponseMeta struct {
	Status               string `json:"status,omitempty"`
	ErrorCode            string `json:"error_code,omitempty"`
	ErrorMessage         string `json:"error_message,omitempty"`
	IncompleteReason     string `json:"incomplete_reason,omitempty"`
	ServiceTier          string `json:"service_tier,omitempty"`
	ReasoningEffort      string `json:"reasoning_effort,omitempty"`
	ReasoningSummary     string `json:"reasoning_summary,omitempty"`
	PromptCacheRetention string `json:"prompt_cache_retention,omitempty"`
}

// ClaudeResponseMeta is the public projection of claude.ResponseMetaExtension.
// The response ID is private and never appears here.
type ClaudeResponseMeta struct {
	StopReason      string `json:"stop_reason,omitempty"`
	StopSequence    string `json:"stop_sequence,omitempty"`
	StopCategory    string `json:"stop_category,omitempty"`
	StopExplanation string `json:"stop_explanation,omitempty"`
}

// GeminiResponseMeta is the public projection of gemini.ResponseMetaExtension.
// The response ID is private and never appears here.
type GeminiResponseMeta struct {
	FinishReason string     `json:"finish_reason,omitempty"`
	Grounding    *Grounding `json:"grounding,omitempty"`
}

// ResponseMeta is the public projection of one assistant AgenticResponseMeta.
// It is persisted as one PartResponseMeta part per assistant message,
// ordered after all of that message's block parts.
type ResponseMeta struct {
	Usage  *Usage              `json:"usage,omitempty"`
	OpenAI *OpenAIResponseMeta `json:"openai,omitempty"`
	Claude *ClaudeResponseMeta `json:"claude,omitempty"`
	Gemini *GeminiResponseMeta `json:"gemini,omitempty"`
}

// Content is the durable, ordered public content of one message.
type Content struct {
	Role   Role
	Blocks []ContentBlock
	Meta   *ResponseMeta
}

// ContentLimits bounds one durable Content value.
type ContentLimits struct {
	MaxMessageBytes int
	MaxBlocks       int
	MaxBlockBytes   int
}

// DefaultContentLimits returns the default production content bounds.
func DefaultContentLimits() ContentLimits {
	return ContentLimits{
		MaxMessageBytes: 8 << 20,
		MaxBlocks:       1024,
		MaxBlockBytes:   1 << 20,
	}
}

// Validate requires every bound to be positive.
func (l ContentLimits) Validate() error {
	if l.MaxMessageBytes <= 0 || l.MaxBlocks <= 0 || l.MaxBlockBytes <= 0 {
		return ErrContentInvalid
	}
	return nil
}

var (
	// ErrContentTooLarge reports that content exceeds configured bounds.
	ErrContentTooLarge = errors.New("session content exceeds configured limits")
	// ErrContentInvalid reports structurally or semantically malformed content.
	ErrContentInvalid = errors.New("session content is invalid")
	// ErrContentUnsupported reports an unknown block kind or an unsupported
	// provider feature that cannot be represented by this durable contract.
	ErrContentUnsupported = errors.New("session content uses an unsupported block or feature")
)

const maxBlockIDBytes = 128

func setOfKinds(kinds ...BlockKind) map[BlockKind]bool {
	out := make(map[BlockKind]bool, len(kinds))
	for _, k := range kinds {
		out[k] = true
	}
	return out
}

var roleAllowedKinds = map[Role]map[BlockKind]bool{
	RoleUser: setOfKinds(
		BlockKindUserInputText, BlockKindUserInputImage, BlockKindUserInputAudio, BlockKindUserInputVideo, BlockKindUserInputFile,
		BlockKindFunctionToolResult, BlockKindToolSearchResult, BlockKindMCPToolApprovalResponse,
	),
	RoleAssistant: setOfKinds(
		BlockKindReasoning, BlockKindAssistantGenText, BlockKindAssistantGenImage, BlockKindAssistantGenAudio, BlockKindAssistantGenVideo,
		BlockKindFunctionToolCall, BlockKindServerToolCall, BlockKindServerToolResult, BlockKindMCPToolCall, BlockKindMCPToolResult,
		BlockKindMCPListToolsResult, BlockKindMCPToolApprovalRequest,
	),
	RoleSystem: setOfKinds(BlockKindUserInputText),
}

// Validate enforces role/kind compatibility, block-identity uniqueness,
// exactly-one-variant unions, per-kind field rules, and the given byte
// bounds.
func (c Content) Validate(limits ContentLimits) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	allowed, ok := roleAllowedKinds[c.Role]
	if !ok {
		return ErrContentInvalid
	}
	if c.Meta != nil && c.Role != RoleAssistant {
		return ErrContentInvalid
	}
	if len(c.Blocks) > limits.MaxBlocks {
		return ErrContentTooLarge
	}
	seenIDs := make(map[string]bool, len(c.Blocks))
	seenFunctionResultCallIDs := make(map[string]bool)
	total := 0
	for i := range c.Blocks {
		block := c.Blocks[i]
		if !allowed[block.Kind] {
			if _, known := envelopeKeyForKind[block.Kind]; !known {
				return ErrContentUnsupported
			}
			return ErrContentInvalid
		}
		if block.ID == "" || seenIDs[block.ID] || !printableASCII(block.ID, maxBlockIDBytes) {
			return ErrContentInvalid
		}
		seenIDs[block.ID] = true
		variant, err := blockVariant(block.Kind, block)
		if err != nil {
			return err
		}
		if err := validateVariant(block.Kind, variant, seenFunctionResultCallIDs, limits); err != nil {
			return err
		}
		raw, err := json.Marshal(variant)
		if err != nil {
			return ErrContentInvalid
		}
		if len(raw) > limits.MaxBlockBytes {
			return ErrContentTooLarge
		}
		total += len(raw)
		if total > limits.MaxMessageBytes {
			return ErrContentTooLarge
		}
	}
	if c.Meta != nil {
		raw, err := json.Marshal(c.Meta)
		if err != nil {
			return ErrContentInvalid
		}
		if len(raw) > limits.MaxBlockBytes {
			return ErrContentTooLarge
		}
		total += len(raw)
		if total > limits.MaxMessageBytes {
			return ErrContentTooLarge
		}
	}
	return nil
}

// blockVariant returns the single populated payload pointer for block,
// verifying that it is the one and only variant set and that it matches
// kind.
func blockVariant(kind BlockKind, block ContentBlock) (any, error) {
	set := 0
	if block.Reasoning != nil {
		set++
	}
	if block.Text != nil {
		set++
	}
	if block.Media != nil {
		set++
	}
	if block.FunctionCall != nil {
		set++
	}
	if block.FunctionResult != nil {
		set++
	}
	if block.ToolSearch != nil {
		set++
	}
	if block.ServerCall != nil {
		set++
	}
	if block.ServerResult != nil {
		set++
	}
	if block.MCPCall != nil {
		set++
	}
	if block.MCPResult != nil {
		set++
	}
	if block.MCPListTools != nil {
		set++
	}
	if block.MCPApprovalRequest != nil {
		set++
	}
	if block.MCPApprovalResponse != nil {
		set++
	}
	if set != 1 {
		return nil, ErrContentInvalid
	}
	switch kind {
	case BlockKindReasoning:
		if block.Reasoning == nil {
			return nil, ErrContentInvalid
		}
		return block.Reasoning, nil
	case BlockKindUserInputText, BlockKindAssistantGenText:
		if block.Text == nil {
			return nil, ErrContentInvalid
		}
		return block.Text, nil
	case BlockKindUserInputImage, BlockKindUserInputAudio, BlockKindUserInputVideo, BlockKindUserInputFile,
		BlockKindAssistantGenImage, BlockKindAssistantGenAudio, BlockKindAssistantGenVideo:
		if block.Media == nil {
			return nil, ErrContentInvalid
		}
		return block.Media, nil
	case BlockKindFunctionToolCall:
		if block.FunctionCall == nil {
			return nil, ErrContentInvalid
		}
		return block.FunctionCall, nil
	case BlockKindFunctionToolResult:
		if block.FunctionResult == nil {
			return nil, ErrContentInvalid
		}
		return block.FunctionResult, nil
	case BlockKindToolSearchResult:
		if block.ToolSearch == nil {
			return nil, ErrContentInvalid
		}
		return block.ToolSearch, nil
	case BlockKindServerToolCall:
		if block.ServerCall == nil {
			return nil, ErrContentInvalid
		}
		return block.ServerCall, nil
	case BlockKindServerToolResult:
		if block.ServerResult == nil {
			return nil, ErrContentInvalid
		}
		return block.ServerResult, nil
	case BlockKindMCPToolCall:
		if block.MCPCall == nil {
			return nil, ErrContentInvalid
		}
		return block.MCPCall, nil
	case BlockKindMCPToolResult:
		if block.MCPResult == nil {
			return nil, ErrContentInvalid
		}
		return block.MCPResult, nil
	case BlockKindMCPListToolsResult:
		if block.MCPListTools == nil {
			return nil, ErrContentInvalid
		}
		return block.MCPListTools, nil
	case BlockKindMCPToolApprovalRequest:
		if block.MCPApprovalRequest == nil {
			return nil, ErrContentInvalid
		}
		return block.MCPApprovalRequest, nil
	case BlockKindMCPToolApprovalResponse:
		if block.MCPApprovalResponse == nil {
			return nil, ErrContentInvalid
		}
		return block.MCPApprovalResponse, nil
	default:
		return nil, ErrContentUnsupported
	}
}

func validateVariant(kind BlockKind, variant any, seenFunctionResultCallIDs map[string]bool, limits ContentLimits) error {
	switch v := variant.(type) {
	case *ReasoningBlock:
		if !utf8.ValidString(v.Text) {
			return ErrContentInvalid
		}
		for _, s := range v.Summary {
			if !utf8.ValidString(s) {
				return ErrContentInvalid
			}
		}
	case *TextBlock:
		if len(v.Text) > limits.MaxBlockBytes {
			return ErrContentTooLarge
		}
		if !utf8.ValidString(v.Text) || !utf8.ValidString(v.Refusal) {
			return ErrContentInvalid
		}
		if kind == BlockKindUserInputText && (v.Refusal != "" || len(v.Annotations) > 0) {
			return ErrContentInvalid
		}
		for _, a := range v.Annotations {
			if err := validateAnnotation(a); err != nil {
				return err
			}
		}
	case *MediaBlock:
		if err := validateMedia(kind, v, limits); err != nil {
			return err
		}
	case *FunctionCallBlock:
		if v.CallID == "" || v.Name == "" || !utf8.ValidString(v.Name) || !utf8.ValidString(v.CallID) || !validJSONObjectString(v.Arguments) {
			return ErrContentInvalid
		}
	case *FunctionResultBlock:
		if v.CallID == "" || v.Name == "" || !utf8.ValidString(v.Name) || !utf8.ValidString(v.CallID) {
			return ErrContentInvalid
		}
		if seenFunctionResultCallIDs[v.CallID] {
			return ErrContentInvalid
		}
		seenFunctionResultCallIDs[v.CallID] = true
		for _, item := range v.Content {
			if err := validateResultContent(item, limits); err != nil {
				return err
			}
		}
	case *ToolSearchBlock:
		if v.CallID == "" || v.Name == "" || !utf8.ValidString(v.Name) || !utf8.ValidString(v.CallID) {
			return ErrContentInvalid
		}
		for _, raw := range v.Tools {
			if len(raw) > limits.MaxBlockBytes {
				return ErrContentTooLarge
			}
			if err := json.Unmarshal(raw, &einoschema.ToolInfo{}); err != nil {
				return ErrContentInvalid
			}
		}
	case *ServerCallBlock:
		if v.Name == "" || !utf8.ValidString(v.Name) || !utf8.ValidString(v.CallID) {
			return ErrContentInvalid
		}
		if len(v.Arguments) > limits.MaxBlockBytes {
			return ErrContentTooLarge
		}
		if err := boundedFiniteJSON(v.Arguments); err != nil {
			return err
		}
	case *ServerResultBlock:
		if v.Name == "" || !utf8.ValidString(v.Name) || !utf8.ValidString(v.CallID) {
			return ErrContentInvalid
		}
		if len(v.Content) > limits.MaxBlockBytes {
			return ErrContentTooLarge
		}
		if err := boundedFiniteJSON(v.Content); err != nil {
			return err
		}
	case *MCPCallBlock:
		if v.CallID == "" || v.Name == "" || !utf8.ValidString(v.Name) || !utf8.ValidString(v.ServerLabel) || !validJSONObjectString(v.Arguments) {
			return ErrContentInvalid
		}
	case *MCPResultBlock:
		if v.CallID == "" || v.Name == "" || !utf8.ValidString(v.Name) || !utf8.ValidString(v.Content) || !utf8.ValidString(v.ErrorMessage) {
			return ErrContentInvalid
		}
	case *MCPListToolsBlock:
		if !utf8.ValidString(v.ServerLabel) || !utf8.ValidString(v.Error) {
			return ErrContentInvalid
		}
		for _, tool := range v.Tools {
			if tool.Name == "" || !utf8.ValidString(tool.Name) || !utf8.ValidString(tool.Description) {
				return ErrContentInvalid
			}
			if len(tool.InputSchema) > 0 {
				if len(tool.InputSchema) > limits.MaxBlockBytes {
					return ErrContentTooLarge
				}
				if err := boundedFiniteJSON(tool.InputSchema); err != nil {
					return err
				}
			}
		}
	case *MCPApprovalRequestBlock:
		if v.ID == "" || v.Name == "" || !utf8.ValidString(v.Name) || !validJSONObjectString(v.Arguments) {
			return ErrContentInvalid
		}
	case *MCPApprovalResponseBlock:
		if v.ApprovalRequestID == "" || !utf8.ValidString(v.Reason) {
			return ErrContentInvalid
		}
	default:
		return ErrContentInvalid
	}
	return nil
}

func validateAnnotation(a TextAnnotation) error {
	if a.Type == "" || !utf8.ValidString(a.Type) || !utf8.ValidString(a.Title) || !utf8.ValidString(a.URL) ||
		!utf8.ValidString(a.FileID) || !utf8.ValidString(a.Filename) || !utf8.ValidString(a.ContainerID) ||
		!utf8.ValidString(a.CitedText) || !utf8.ValidString(a.DocumentTitle) {
		return ErrContentInvalid
	}
	if !isOpenAIAnnotationType(a.Type) && !isClaudeAnnotationType(a.Type) {
		return ErrContentUnsupported
	}
	return nil
}

func validateMediaFields(m *MediaBlock, allowName, allowDetail bool, limits ContentLimits) error {
	if m == nil {
		return ErrContentInvalid
	}
	hasURL := m.URL != ""
	hasData := m.Base64Data != ""
	if hasURL == hasData {
		return ErrContentInvalid
	}
	if hasURL && !utf8.ValidString(m.URL) {
		return ErrContentInvalid
	}
	if hasData {
		// Cheap upper bound before the base64 decode: the encoded string is
		// never shorter than the bytes it represents.
		if len(m.Base64Data) > limits.MaxBlockBytes {
			return ErrContentTooLarge
		}
		if _, err := base64.StdEncoding.Strict().DecodeString(m.Base64Data); err != nil {
			return ErrContentInvalid
		}
	}
	if m.MIMEType != "" && !validMIMEType(m.MIMEType) {
		return ErrContentInvalid
	}
	if m.Name != "" {
		if !allowName || !utf8.ValidString(m.Name) {
			return ErrContentInvalid
		}
	}
	if m.Detail != "" {
		if !allowDetail || !utf8.ValidString(m.Detail) {
			return ErrContentInvalid
		}
	}
	return nil
}

func validateMedia(kind BlockKind, m *MediaBlock, limits ContentLimits) error {
	allowName := kind == BlockKindUserInputFile
	allowDetail := kind == BlockKindUserInputImage || kind == BlockKindAssistantGenImage
	return validateMediaFields(m, allowName, allowDetail, limits)
}

func validateResultContent(item ResultContent, limits ContentLimits) error {
	switch item.Type {
	case ResultContentText:
		if item.Media != nil || !utf8.ValidString(item.Text) {
			return ErrContentInvalid
		}
	case ResultContentImage:
		if item.Text != "" {
			return ErrContentInvalid
		}
		return validateMediaFields(item.Media, false, true, limits)
	case ResultContentAudio, ResultContentVideo:
		if item.Text != "" {
			return ErrContentInvalid
		}
		return validateMediaFields(item.Media, false, false, limits)
	case ResultContentFile:
		if item.Text != "" {
			return ErrContentInvalid
		}
		return validateMediaFields(item.Media, true, false, limits)
	default:
		return ErrContentInvalid
	}
	return nil
}

func validMIMEType(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	slash := -1
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			if slash != -1 {
				return false
			}
			slash = i
		}
	}
	return slash > 0 && slash < len(s)-1
}

func validJSONObjectString(s string) bool {
	if s == "" {
		return true
	}
	if !utf8.ValidString(s) {
		return false
	}
	trimmed := bytes.TrimSpace([]byte(s))
	return len(trimmed) >= 2 && trimmed[0] == '{' && json.Valid(trimmed)
}

func printableASCII(s string, maxBytes int) bool {
	if s == "" || len(s) > maxBytes {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// boundedFiniteJSON reports whether raw is either empty or a syntactically
// valid, finite JSON value: bounded nesting depth and bounded token count.
// It rejects anything json.Marshal would have refused to produce (NaN, Inf,
// unsupported Go types) indirectly, since callers only ever pass bytes that
// already came from a successful json.Marshal or from a previously stored
// envelope.
func boundedFiniteJSON(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	if !json.Valid(raw) {
		return ErrContentUnsupported
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	depth := 0
	entries := 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return ErrContentUnsupported
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{', '[':
				depth++
				if depth > 64 {
					return ErrContentUnsupported
				}
			case '}', ']':
				depth--
			}
			continue
		}
		entries++
		if entries > 4096 {
			return ErrContentUnsupported
		}
	}
	return nil
}

// contentBlockEnvelope is the on-wire shape of one durable content block
// Part payload.
type contentBlockEnvelope struct {
	Schema  int       `json:"schema"`
	BlockID string    `json:"block_id"`
	Kind    BlockKind `json:"kind"`

	Reasoning           *ReasoningBlock           `json:"reasoning,omitempty"`
	Text                *TextBlock                `json:"text,omitempty"`
	Media               *MediaBlock               `json:"media,omitempty"`
	FunctionCall        *FunctionCallBlock        `json:"function_call,omitempty"`
	FunctionResult      *FunctionResultBlock      `json:"function_result,omitempty"`
	ToolSearch          *ToolSearchBlock          `json:"tool_search,omitempty"`
	ServerCall          *ServerCallBlock          `json:"server_call,omitempty"`
	ServerResult        *ServerResultBlock        `json:"server_result,omitempty"`
	MCPCall             *MCPCallBlock             `json:"mcp_call,omitempty"`
	MCPResult           *MCPResultBlock           `json:"mcp_result,omitempty"`
	MCPListTools        *MCPListToolsBlock        `json:"mcp_list_tools,omitempty"`
	MCPApprovalRequest  *MCPApprovalRequestBlock  `json:"mcp_approval_request,omitempty"`
	MCPApprovalResponse *MCPApprovalResponseBlock `json:"mcp_approval_response,omitempty"`
}

// responseMetaEnvelope is the on-wire shape of the one PartResponseMeta Part
// payload persisted per assistant message.
type responseMetaEnvelope struct {
	Schema int           `json:"schema"`
	Meta   *ResponseMeta `json:"meta"`
}

func encodeBlockEnvelope(block ContentBlock) (json.RawMessage, error) {
	variant, err := blockVariant(block.Kind, block)
	if err != nil {
		return nil, err
	}
	env := contentBlockEnvelope{Schema: ContentSchemaVersion, BlockID: block.ID, Kind: block.Kind}
	switch v := variant.(type) {
	case *ReasoningBlock:
		env.Reasoning = v
	case *TextBlock:
		env.Text = v
	case *MediaBlock:
		env.Media = v
	case *FunctionCallBlock:
		env.FunctionCall = v
	case *FunctionResultBlock:
		env.FunctionResult = v
	case *ToolSearchBlock:
		env.ToolSearch = v
	case *ServerCallBlock:
		env.ServerCall = v
	case *ServerResultBlock:
		env.ServerResult = v
	case *MCPCallBlock:
		env.MCPCall = v
	case *MCPResultBlock:
		env.MCPResult = v
	case *MCPListToolsBlock:
		env.MCPListTools = v
	case *MCPApprovalRequestBlock:
		env.MCPApprovalRequest = v
	case *MCPApprovalResponseBlock:
		env.MCPApprovalResponse = v
	default:
		return nil, ErrContentUnsupported
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return nil, ErrContentInvalid
	}
	return raw, nil
}

func decodeStrict[T any](raw json.RawMessage, limits ContentLimits) (T, error) {
	var value T
	if len(raw) > limits.MaxBlockBytes {
		return value, ErrContentTooLarge
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return value, ErrContentInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, ErrContentInvalid
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return value, ErrContentInvalid
	}
	// Go's encoding/json silently accepts duplicate object keys (last value
	// wins) even with DisallowUnknownFields set, so re-marshal the decoded
	// value and require it to be byte-identical to the trimmed input. This
	// is the same canonical-form gate DecodeProviderStatePayload applies
	// (session/provider_state.go); without it, a duplicate-key payload could
	// mean two different things to two readers (this package's
	// encoding/json versus Eino's sonic decoder).
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, trimmed) {
		return value, ErrContentInvalid
	}
	return value, nil
}

func decodeBlockEnvelope(raw json.RawMessage, expectedKind BlockKind, limits ContentLimits) (ContentBlock, error) {
	env, err := decodeStrict[contentBlockEnvelope](raw, limits)
	if err != nil {
		return ContentBlock{}, err
	}
	if env.Schema != ContentSchemaVersion {
		return ContentBlock{}, ErrContentInvalid
	}
	if _, known := envelopeKeyForKind[env.Kind]; !known {
		return ContentBlock{}, ErrContentUnsupported
	}
	if env.Kind != expectedKind {
		return ContentBlock{}, ErrContentInvalid
	}
	block := ContentBlock{
		ID: env.BlockID, Kind: env.Kind,
		Reasoning: env.Reasoning, Text: env.Text, Media: env.Media,
		FunctionCall: env.FunctionCall, FunctionResult: env.FunctionResult, ToolSearch: env.ToolSearch,
		ServerCall: env.ServerCall, ServerResult: env.ServerResult, MCPCall: env.MCPCall, MCPResult: env.MCPResult,
		MCPListTools: env.MCPListTools, MCPApprovalRequest: env.MCPApprovalRequest, MCPApprovalResponse: env.MCPApprovalResponse,
	}
	if _, err := blockVariant(block.Kind, block); err != nil {
		return ContentBlock{}, err
	}
	return block, nil
}

func decodeResponseMetaEnvelope(raw json.RawMessage, limits ContentLimits) (*ResponseMeta, error) {
	env, err := decodeStrict[responseMetaEnvelope](raw, limits)
	if err != nil {
		return nil, err
	}
	if env.Schema != ContentSchemaVersion {
		return nil, ErrContentInvalid
	}
	return env.Meta, nil
}

// EncodeContentParts encodes validated content into one Part per block
// (ordinal = block index, in order) followed by an optional response_meta
// Part at ordinal len(content.Blocks). Bounds are enforced by Content.Validate
// before any block is marshaled or measured.
//
// The ordinals this function assigns are contiguous, but DecodeContentParts
// does not require that: content-kind parts (block kinds and response_meta)
// must appear in strictly increasing ordinal order among themselves, with
// response_meta after the last block, but they need not be contiguous —
// other part kinds persisted in the same message (provider_state, legacy,
// compaction) may occupy ordinals interleaved between them and are ignored
// regardless of their own ordinal values. This lets callers such as
// persistAssistantTurn share one ordinal counter across content and
// provider-state parts.
func EncodeContentParts(content Content, ids func() PartID, messageID MessageID, sessionID ID, runID RunID, at time.Time, limits ContentLimits) ([]Part, error) {
	if ids == nil {
		return nil, ErrContentInvalid
	}
	if err := content.Validate(limits); err != nil {
		return nil, err
	}
	parts := make([]Part, 0, len(content.Blocks)+1)
	total := 0
	for i, block := range content.Blocks {
		raw, err := encodeBlockEnvelope(block)
		if err != nil {
			return nil, err
		}
		if len(raw) > limits.MaxBlockBytes {
			return nil, ErrContentTooLarge
		}
		total += len(raw)
		if total > limits.MaxMessageBytes {
			return nil, ErrContentTooLarge
		}
		parts = append(parts, Part{
			ID: ids(), MessageID: messageID, SessionID: sessionID, RunID: runID,
			Kind: PartKindForBlock(block.Kind), Ordinal: int64(i), Payload: raw,
			CreatedAt: at, UpdatedAt: at,
		})
	}
	if content.Meta != nil {
		raw, err := json.Marshal(responseMetaEnvelope{Schema: ContentSchemaVersion, Meta: content.Meta})
		if err != nil {
			return nil, ErrContentInvalid
		}
		if len(raw) > limits.MaxBlockBytes {
			return nil, ErrContentTooLarge
		}
		total += len(raw)
		if total > limits.MaxMessageBytes {
			return nil, ErrContentTooLarge
		}
		parts = append(parts, Part{
			ID: ids(), MessageID: messageID, SessionID: sessionID, RunID: runID,
			Kind: PartResponseMeta, Ordinal: int64(len(content.Blocks)), Payload: raw,
			CreatedAt: at, UpdatedAt: at,
		})
	}
	return parts, nil
}

// DecodeContentParts strictly decodes the block-kind and response_meta parts
// of the given slice back into Content, in ordinal order. Parts of any other
// kind (legacy kinds, compaction, provider_state) are ignored regardless of
// their ordinal; provider_state parts are specifically never decoded here.
//
// Content-kind parts (block kinds and, if present, response_meta) must occupy
// strictly increasing ordinals, with response_meta's ordinal greater than
// every block's — but that ordinal sequence need not be contiguous, since
// other part kinds may share the same message's ordinal space at
// interleaved positions.
func DecodeContentParts(role Role, parts []Part, limits ContentLimits) (Content, error) {
	if err := limits.Validate(); err != nil {
		return Content{}, err
	}
	recognizedBlocks := 0
	cumulative := 0
	for _, part := range parts {
		switch part.Kind {
		case PartProviderState:
			continue
		case PartResponseMeta:
			if len(part.Payload) > limits.MaxBlockBytes {
				return Content{}, ErrContentTooLarge
			}
			cumulative += len(part.Payload)
		default:
			if _, ok := BlockKindForPart(part.Kind); ok {
				recognizedBlocks++
				if recognizedBlocks > limits.MaxBlocks {
					return Content{}, ErrContentTooLarge
				}
				if len(part.Payload) > limits.MaxBlockBytes {
					return Content{}, ErrContentTooLarge
				}
				cumulative += len(part.Payload)
			}
		}
		if cumulative > limits.MaxMessageBytes {
			return Content{}, ErrContentTooLarge
		}
	}

	blocks := make([]ContentBlock, 0, recognizedBlocks)
	var meta *ResponseMeta
	metaSeen := false
	lastOrdinal := int64(-1)
	haveLastOrdinal := false
	for _, part := range parts {
		if part.Kind == PartProviderState {
			continue
		}
		if blockKind, ok := BlockKindForPart(part.Kind); ok {
			if metaSeen || (haveLastOrdinal && part.Ordinal <= lastOrdinal) {
				return Content{}, ErrContentInvalid
			}
			block, err := decodeBlockEnvelope(part.Payload, blockKind, limits)
			if err != nil {
				return Content{}, err
			}
			blocks = append(blocks, block)
			lastOrdinal = part.Ordinal
			haveLastOrdinal = true
			continue
		}
		if part.Kind == PartResponseMeta {
			if metaSeen || (haveLastOrdinal && part.Ordinal <= lastOrdinal) {
				return Content{}, ErrContentInvalid
			}
			decoded, err := decodeResponseMetaEnvelope(part.Payload, limits)
			if err != nil {
				return Content{}, err
			}
			meta = decoded
			metaSeen = true
			lastOrdinal = part.Ordinal
			haveLastOrdinal = true
			continue
		}
		// legacy/unrecognized kinds are ignored by design, regardless of ordinal.
	}
	content := Content{Role: role, Blocks: blocks, Meta: meta}
	if err := content.Validate(limits); err != nil {
		return Content{}, err
	}
	return content, nil
}

// PrivateBlockState is one item of provider-private material split out of an
// Eino AgenticMessage during ContentFromAgenticMessage. BlockID is empty for
// message-level private state (response identity, Gemini SDK blob).
type PrivateBlockState struct {
	BlockID string
	Data    json.RawMessage
}

type privateSignature struct {
	Signature string `json:"signature"`
}

type privateEncryptedIndex struct {
	Annotation     int    `json:"annotation"`
	EncryptedIndex string `json:"encrypted_index"`
}

type privateEncryptedIndexes struct {
	EncryptedIndexes []privateEncryptedIndex `json:"encrypted_indexes"`
}

type privateSDKBlob struct {
	SDKBlobBase64 string `json:"sdk_blob_base64"`
}

type privateResponseIDs struct {
	ResponseID         string `json:"response_id,omitempty"`
	PreviousResponseID string `json:"previous_response_id,omitempty"`
	CreatedAt          int64  `json:"created_at,omitempty"`
}

func roleFromAgentic(role einoschema.AgenticRoleType) (Role, error) {
	switch role {
	case einoschema.AgenticRoleTypeSystem:
		return RoleSystem, nil
	case einoschema.AgenticRoleTypeUser:
		return RoleUser, nil
	case einoschema.AgenticRoleTypeAssistant:
		return RoleAssistant, nil
	default:
		return "", ErrContentInvalid
	}
}

func agenticRoleFromContent(role Role) (einoschema.AgenticRoleType, error) {
	switch role {
	case RoleSystem:
		return einoschema.AgenticRoleTypeSystem, nil
	case RoleUser:
		return einoschema.AgenticRoleTypeUser, nil
	case RoleAssistant:
		return einoschema.AgenticRoleTypeAssistant, nil
	default:
		return "", ErrContentInvalid
	}
}

// ContentFromAgenticMessage converts one finalized *schema.AgenticMessage
// into its durable public Content projection, splitting out provider-private
// material as a side channel. ids is called once per content block, in
// order, to mint a durable block identity; it may be nil, in which case
// block IDs are left empty (the caller must assign them before encoding).
func ContentFromAgenticMessage(msg *einoschema.AgenticMessage, ids func() string) (Content, []PrivateBlockState, error) {
	if msg == nil {
		return Content{}, nil, ErrContentInvalid
	}
	if len(msg.Extra) != 0 {
		return Content{}, nil, ErrContentUnsupported
	}
	role, err := roleFromAgentic(msg.Role)
	if err != nil {
		return Content{}, nil, err
	}
	blocks := make([]ContentBlock, 0, len(msg.ContentBlocks))
	var private []PrivateBlockState
	for _, b := range msg.ContentBlocks {
		if b == nil {
			return Content{}, nil, ErrContentInvalid
		}
		if b.StreamingMeta != nil {
			return Content{}, nil, ErrContentUnsupported
		}
		if len(b.Extra) != 0 {
			return Content{}, nil, ErrContentUnsupported
		}
		id := ""
		if ids != nil {
			id = ids()
		}
		block, blockPrivate, err := contentBlockFromEino(id, b)
		if err != nil {
			return Content{}, nil, err
		}
		blocks = append(blocks, block)
		private = append(private, blockPrivate...)
	}
	meta, metaPrivate, err := responseMetaFromEino(msg.ResponseMeta)
	if err != nil {
		return Content{}, nil, err
	}
	private = append(private, metaPrivate...)
	return Content{Role: role, Blocks: blocks, Meta: meta}, private, nil
}

func contentBlockFromEino(id string, b *einoschema.ContentBlock) (ContentBlock, []PrivateBlockState, error) {
	switch b.Type {
	case einoschema.ContentBlockTypeReasoning:
		if b.Reasoning == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		summary, err := reasoningSummaryFromOpenAI(b.Reasoning.OpenAIExtension)
		if err != nil {
			return ContentBlock{}, nil, err
		}
		block := ContentBlock{ID: id, Kind: BlockKindReasoning, Reasoning: &ReasoningBlock{Text: b.Reasoning.Text, Summary: summary}}
		var private []PrivateBlockState
		if b.Reasoning.Signature != "" {
			raw, err := json.Marshal(privateSignature{Signature: b.Reasoning.Signature})
			if err != nil {
				return ContentBlock{}, nil, ErrContentInvalid
			}
			private = append(private, PrivateBlockState{BlockID: id, Data: raw})
		}
		return block, private, nil

	case einoschema.ContentBlockTypeUserInputText:
		if b.UserInputText == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindUserInputText, Text: &TextBlock{Text: b.UserInputText.Text}}, nil, nil

	case einoschema.ContentBlockTypeUserInputImage:
		if b.UserInputImage == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindUserInputImage, Media: &MediaBlock{
			URL: b.UserInputImage.URL, Base64Data: b.UserInputImage.Base64Data, MIMEType: b.UserInputImage.MIMEType, Detail: string(b.UserInputImage.Detail),
		}}, nil, nil

	case einoschema.ContentBlockTypeUserInputAudio:
		if b.UserInputAudio == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindUserInputAudio, Media: &MediaBlock{
			URL: b.UserInputAudio.URL, Base64Data: b.UserInputAudio.Base64Data, MIMEType: b.UserInputAudio.MIMEType,
		}}, nil, nil

	case einoschema.ContentBlockTypeUserInputVideo:
		if b.UserInputVideo == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindUserInputVideo, Media: &MediaBlock{
			URL: b.UserInputVideo.URL, Base64Data: b.UserInputVideo.Base64Data, MIMEType: b.UserInputVideo.MIMEType,
		}}, nil, nil

	case einoschema.ContentBlockTypeUserInputFile:
		if b.UserInputFile == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindUserInputFile, Media: &MediaBlock{
			URL: b.UserInputFile.URL, Base64Data: b.UserInputFile.Base64Data, MIMEType: b.UserInputFile.MIMEType, Name: b.UserInputFile.Name,
		}}, nil, nil

	case einoschema.ContentBlockTypeAssistantGenText:
		if b.AssistantGenText == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		if b.AssistantGenText.Extension != nil {
			return ContentBlock{}, nil, ErrContentUnsupported
		}
		text, private, err := assistantGenTextFromEino(id, b.AssistantGenText)
		if err != nil {
			return ContentBlock{}, nil, err
		}
		return ContentBlock{ID: id, Kind: BlockKindAssistantGenText, Text: text}, private, nil

	case einoschema.ContentBlockTypeAssistantGenImage:
		if b.AssistantGenImage == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindAssistantGenImage, Media: &MediaBlock{
			URL: b.AssistantGenImage.URL, Base64Data: b.AssistantGenImage.Base64Data, MIMEType: b.AssistantGenImage.MIMEType,
		}}, nil, nil

	case einoschema.ContentBlockTypeAssistantGenAudio:
		if b.AssistantGenAudio == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindAssistantGenAudio, Media: &MediaBlock{
			URL: b.AssistantGenAudio.URL, Base64Data: b.AssistantGenAudio.Base64Data, MIMEType: b.AssistantGenAudio.MIMEType,
		}}, nil, nil

	case einoschema.ContentBlockTypeAssistantGenVideo:
		if b.AssistantGenVideo == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindAssistantGenVideo, Media: &MediaBlock{
			URL: b.AssistantGenVideo.URL, Base64Data: b.AssistantGenVideo.Base64Data, MIMEType: b.AssistantGenVideo.MIMEType,
		}}, nil, nil

	case einoschema.ContentBlockTypeFunctionToolCall:
		if b.FunctionToolCall == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindFunctionToolCall, FunctionCall: &FunctionCallBlock{
			CallID: b.FunctionToolCall.CallID, Name: b.FunctionToolCall.Name, Arguments: b.FunctionToolCall.Arguments,
		}}, nil, nil

	case einoschema.ContentBlockTypeFunctionToolResult:
		if b.FunctionToolResult == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		content, err := functionResultContentFromEino(b.FunctionToolResult.Content)
		if err != nil {
			return ContentBlock{}, nil, err
		}
		return ContentBlock{ID: id, Kind: BlockKindFunctionToolResult, FunctionResult: &FunctionResultBlock{
			CallID: b.FunctionToolResult.CallID, Name: b.FunctionToolResult.Name, Content: content,
		}}, nil, nil

	case einoschema.ContentBlockTypeToolSearchResult:
		if b.ToolSearchFunctionToolResult == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		r := b.ToolSearchFunctionToolResult
		var tools []json.RawMessage
		if r.Result != nil {
			tools = make([]json.RawMessage, 0, len(r.Result.Tools))
			for _, ti := range r.Result.Tools {
				if ti == nil {
					return ContentBlock{}, nil, ErrContentInvalid
				}
				raw, err := json.Marshal(ti)
				if err != nil {
					return ContentBlock{}, nil, ErrContentUnsupported
				}
				tools = append(tools, raw)
			}
		}
		return ContentBlock{ID: id, Kind: BlockKindToolSearchResult, ToolSearch: &ToolSearchBlock{CallID: r.CallID, Name: r.Name, Tools: tools}}, nil, nil

	case einoschema.ContentBlockTypeServerToolCall:
		if b.ServerToolCall == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		args, err := anyToBoundedJSON(b.ServerToolCall.Arguments)
		if err != nil {
			return ContentBlock{}, nil, err
		}
		return ContentBlock{ID: id, Kind: BlockKindServerToolCall, ServerCall: &ServerCallBlock{
			Name: b.ServerToolCall.Name, CallID: b.ServerToolCall.CallID, Arguments: args,
		}}, nil, nil

	case einoschema.ContentBlockTypeServerToolResult:
		if b.ServerToolResult == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		content, err := anyToBoundedJSON(b.ServerToolResult.Content)
		if err != nil {
			return ContentBlock{}, nil, err
		}
		return ContentBlock{ID: id, Kind: BlockKindServerToolResult, ServerResult: &ServerResultBlock{
			Name: b.ServerToolResult.Name, CallID: b.ServerToolResult.CallID, Content: content,
		}}, nil, nil

	case einoschema.ContentBlockTypeMCPToolCall:
		if b.MCPToolCall == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindMCPToolCall, MCPCall: &MCPCallBlock{
			ServerLabel: b.MCPToolCall.ServerLabel, ApprovalRequestID: b.MCPToolCall.ApprovalRequestID,
			CallID: b.MCPToolCall.CallID, Name: b.MCPToolCall.Name, Arguments: b.MCPToolCall.Arguments,
		}}, nil, nil

	case einoschema.ContentBlockTypeMCPToolResult:
		if b.MCPToolResult == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		var errorCode *int64
		errorMessage := ""
		if b.MCPToolResult.Error != nil {
			if b.MCPToolResult.Error.Code != nil {
				v := *b.MCPToolResult.Error.Code
				errorCode = &v
			}
			errorMessage = b.MCPToolResult.Error.Message
		}
		return ContentBlock{ID: id, Kind: BlockKindMCPToolResult, MCPResult: &MCPResultBlock{
			ServerLabel: b.MCPToolResult.ServerLabel, CallID: b.MCPToolResult.CallID, Name: b.MCPToolResult.Name,
			Content: b.MCPToolResult.Content, ErrorCode: errorCode, ErrorMessage: errorMessage,
		}}, nil, nil

	case einoschema.ContentBlockTypeMCPListToolsResult:
		if b.MCPListToolsResult == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		tools := make([]MCPToolDefinition, 0, len(b.MCPListToolsResult.Tools))
		for _, tool := range b.MCPListToolsResult.Tools {
			if tool == nil {
				return ContentBlock{}, nil, ErrContentInvalid
			}
			var schemaRaw json.RawMessage
			if tool.InputSchema != nil {
				raw, err := json.Marshal(tool.InputSchema)
				if err != nil {
					return ContentBlock{}, nil, ErrContentUnsupported
				}
				schemaRaw = raw
			}
			tools = append(tools, MCPToolDefinition{Name: tool.Name, Description: tool.Description, InputSchema: schemaRaw})
		}
		return ContentBlock{ID: id, Kind: BlockKindMCPListToolsResult, MCPListTools: &MCPListToolsBlock{
			ServerLabel: b.MCPListToolsResult.ServerLabel, Tools: tools, Error: b.MCPListToolsResult.Error,
		}}, nil, nil

	case einoschema.ContentBlockTypeMCPToolApprovalRequest:
		if b.MCPToolApprovalRequest == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindMCPToolApprovalRequest, MCPApprovalRequest: &MCPApprovalRequestBlock{
			ID: b.MCPToolApprovalRequest.ID, Name: b.MCPToolApprovalRequest.Name, Arguments: b.MCPToolApprovalRequest.Arguments, ServerLabel: b.MCPToolApprovalRequest.ServerLabel,
		}}, nil, nil

	case einoschema.ContentBlockTypeMCPToolApprovalResponse:
		if b.MCPToolApprovalResponse == nil {
			return ContentBlock{}, nil, ErrContentInvalid
		}
		return ContentBlock{ID: id, Kind: BlockKindMCPToolApprovalResponse, MCPApprovalResponse: &MCPApprovalResponseBlock{
			ApprovalRequestID: b.MCPToolApprovalResponse.ApprovalRequestID, Approve: b.MCPToolApprovalResponse.Approve, Reason: b.MCPToolApprovalResponse.Reason,
		}}, nil, nil

	default:
		return ContentBlock{}, nil, ErrContentUnsupported
	}
}

func reasoningSummaryFromOpenAI(ext *openai.ReasoningExtension) ([]string, error) {
	if ext == nil {
		return nil, nil
	}
	out := make([]string, 0, len(ext.Content))
	for _, c := range ext.Content {
		if c == nil {
			return nil, ErrContentInvalid
		}
		out = append(out, c.Text)
	}
	return out, nil
}

func assistantGenTextFromEino(id string, t *einoschema.AssistantGenText) (*TextBlock, []PrivateBlockState, error) {
	block := &TextBlock{Text: t.Text}
	var private []PrivateBlockState
	if t.OpenAIExtension != nil {
		if t.OpenAIExtension.Refusal != nil {
			block.Refusal = t.OpenAIExtension.Refusal.Reason
		}
		for _, a := range t.OpenAIExtension.Annotations {
			if a == nil {
				continue
			}
			annotation, err := openAIAnnotationToContent(a)
			if err != nil {
				return nil, nil, err
			}
			block.Annotations = append(block.Annotations, annotation)
		}
	}
	if t.ClaudeExtension != nil {
		var encrypted []privateEncryptedIndex
		for _, c := range t.ClaudeExtension.Citations {
			if c == nil {
				continue
			}
			annotation, encIndex, err := claudeCitationToContent(c)
			if err != nil {
				return nil, nil, err
			}
			if encIndex != "" {
				encrypted = append(encrypted, privateEncryptedIndex{Annotation: len(block.Annotations), EncryptedIndex: encIndex})
			}
			block.Annotations = append(block.Annotations, annotation)
		}
		if len(encrypted) > 0 {
			raw, err := json.Marshal(privateEncryptedIndexes{EncryptedIndexes: encrypted})
			if err != nil {
				return nil, nil, ErrContentInvalid
			}
			private = append(private, PrivateBlockState{BlockID: id, Data: raw})
		}
	}
	return block, private, nil
}

func openAIAnnotationToContent(a *openai.TextAnnotation) (TextAnnotation, error) {
	out := TextAnnotation{Type: string(a.Type)}
	switch a.Type {
	case openai.TextAnnotationTypeFileCitation:
		if a.FileCitation != nil {
			out.FileID = a.FileCitation.FileID
			out.Filename = a.FileCitation.Filename
			out.AnnotationIndex = a.FileCitation.Index
		}
	case openai.TextAnnotationTypeURLCitation:
		if a.URLCitation != nil {
			out.Title = a.URLCitation.Title
			out.URL = a.URLCitation.URL
			out.StartIndex = a.URLCitation.StartIndex
			out.EndIndex = a.URLCitation.EndIndex
		}
	case openai.TextAnnotationTypeContainerFileCitation:
		if a.ContainerFileCitation != nil {
			out.ContainerID = a.ContainerFileCitation.ContainerID
			out.FileID = a.ContainerFileCitation.FileID
			out.Filename = a.ContainerFileCitation.Filename
			out.StartIndex = a.ContainerFileCitation.StartIndex
			out.EndIndex = a.ContainerFileCitation.EndIndex
		}
	case openai.TextAnnotationTypeFilePath:
		if a.FilePath != nil {
			out.FileID = a.FilePath.FileID
			out.AnnotationIndex = a.FilePath.Index
		}
	default:
		return TextAnnotation{}, ErrContentUnsupported
	}
	return out, nil
}

func claudeCitationToContent(c *claude.TextCitation) (TextAnnotation, string, error) {
	out := TextAnnotation{Type: string(c.Type)}
	encrypted := ""
	switch c.Type {
	case claude.TextCitationTypeCharLocation:
		if c.CharLocation != nil {
			out.CitedText = c.CharLocation.CitedText
			out.DocumentTitle = c.CharLocation.DocumentTitle
			out.DocumentIndex = c.CharLocation.DocumentIndex
			out.StartIndex = c.CharLocation.StartCharIndex
			out.EndIndex = c.CharLocation.EndCharIndex
		}
	case claude.TextCitationTypePageLocation:
		if c.PageLocation != nil {
			out.CitedText = c.PageLocation.CitedText
			out.DocumentTitle = c.PageLocation.DocumentTitle
			out.DocumentIndex = c.PageLocation.DocumentIndex
			out.StartPage = c.PageLocation.StartPageNumber
			out.EndPage = c.PageLocation.EndPageNumber
		}
	case claude.TextCitationTypeContentBlockLocation:
		if c.ContentBlockLocation != nil {
			out.CitedText = c.ContentBlockLocation.CitedText
			out.DocumentTitle = c.ContentBlockLocation.DocumentTitle
			out.DocumentIndex = c.ContentBlockLocation.DocumentIndex
			out.StartBlock = c.ContentBlockLocation.StartBlockIndex
			out.EndBlock = c.ContentBlockLocation.EndBlockIndex
		}
	case claude.TextCitationTypeWebSearchResultLocation:
		if c.WebSearchResultLocation != nil {
			out.CitedText = c.WebSearchResultLocation.CitedText
			out.Title = c.WebSearchResultLocation.Title
			out.URL = c.WebSearchResultLocation.URL
			encrypted = c.WebSearchResultLocation.EncryptedIndex
		}
	default:
		return TextAnnotation{}, "", ErrContentUnsupported
	}
	return out, encrypted, nil
}

func functionResultContentFromEino(items []*einoschema.FunctionToolResultContentBlock) ([]ResultContent, error) {
	if len(items) == 0 {
		return nil, nil
	}
	out := make([]ResultContent, 0, len(items))
	for _, item := range items {
		if item == nil {
			return nil, ErrContentInvalid
		}
		if len(item.Extra) != 0 {
			return nil, ErrContentUnsupported
		}
		switch item.Type {
		case einoschema.FunctionToolResultContentBlockTypeText:
			if item.Text == nil {
				return nil, ErrContentInvalid
			}
			out = append(out, ResultContent{Type: ResultContentText, Text: item.Text.Text})
		case einoschema.FunctionToolResultContentBlockTypeImage:
			if item.Image == nil {
				return nil, ErrContentInvalid
			}
			out = append(out, ResultContent{Type: ResultContentImage, Media: &MediaBlock{
				URL: item.Image.URL, Base64Data: item.Image.Base64Data, MIMEType: item.Image.MIMEType, Detail: string(item.Image.Detail),
			}})
		case einoschema.FunctionToolResultContentBlockTypeAudio:
			if item.Audio == nil {
				return nil, ErrContentInvalid
			}
			out = append(out, ResultContent{Type: ResultContentAudio, Media: &MediaBlock{
				URL: item.Audio.URL, Base64Data: item.Audio.Base64Data, MIMEType: item.Audio.MIMEType,
			}})
		case einoschema.FunctionToolResultContentBlockTypeVideo:
			if item.Video == nil {
				return nil, ErrContentInvalid
			}
			out = append(out, ResultContent{Type: ResultContentVideo, Media: &MediaBlock{
				URL: item.Video.URL, Base64Data: item.Video.Base64Data, MIMEType: item.Video.MIMEType,
			}})
		case einoschema.FunctionToolResultContentBlockTypeFile:
			if item.File == nil {
				return nil, ErrContentInvalid
			}
			out = append(out, ResultContent{Type: ResultContentFile, Media: &MediaBlock{
				URL: item.File.URL, Base64Data: item.File.Base64Data, MIMEType: item.File.MIMEType, Name: item.File.Name,
			}})
		default:
			return nil, ErrContentUnsupported
		}
	}
	return out, nil
}

func anyToBoundedJSON(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, ErrContentUnsupported
	}
	if err := boundedFiniteJSON(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func usageFromTokenUsage(tu *einoschema.TokenUsage) *Usage {
	if tu == nil {
		return nil
	}
	return &Usage{
		InputTokens:     int64(tu.PromptTokens),
		OutputTokens:    int64(tu.CompletionTokens),
		TotalTokens:     int64(tu.TotalTokens),
		ReasoningTokens: int64(tu.CompletionTokensDetails.ReasoningTokens),
		CacheReadTokens: int64(tu.PromptTokenDetails.CachedTokens),
	}
}

func tokenUsageFromUsage(u *Usage) *einoschema.TokenUsage {
	if u == nil {
		return nil
	}
	return &einoschema.TokenUsage{
		PromptTokens:            int(u.InputTokens),
		CompletionTokens:        int(u.OutputTokens),
		TotalTokens:             int(u.TotalTokens),
		PromptTokenDetails:      einoschema.PromptTokenDetails{CachedTokens: int(u.CacheReadTokens)},
		CompletionTokensDetails: einoschema.CompletionTokensDetails{ReasoningTokens: int(u.ReasoningTokens)},
	}
}

func groundingFromEino(g *gemini.GroundingMetadata) (*Grounding, []byte) {
	out := &Grounding{WebSearchQueries: append([]string(nil), g.WebSearchQueries...)}
	for _, c := range g.GroundingChunks {
		if c != nil && c.Web != nil {
			out.Chunks = append(out.Chunks, GroundingChunk{Domain: c.Web.Domain, Title: c.Web.Title, URI: c.Web.URI})
		} else {
			out.Chunks = append(out.Chunks, GroundingChunk{})
		}
	}
	for _, s := range g.GroundingSupports {
		if s == nil {
			continue
		}
		support := GroundingSupport{
			ConfidenceScores: append([]float32(nil), s.ConfidenceScores...),
			ChunkIndices:     append([]int(nil), s.GroundingChunkIndices...),
		}
		if s.Segment != nil {
			support.Segment = &Segment{Start: s.Segment.StartIndex, End: s.Segment.EndIndex, PartIndex: s.Segment.PartIndex, Text: s.Segment.Text}
		}
		out.Supports = append(out.Supports, support)
	}
	var blob []byte
	if g.SearchEntryPoint != nil {
		out.RenderedEntryPoint = g.SearchEntryPoint.RenderedContent
		blob = g.SearchEntryPoint.SDKBlob
	}
	return out, blob
}

func groundingToEino(g *Grounding) *gemini.GroundingMetadata {
	out := &gemini.GroundingMetadata{WebSearchQueries: append([]string(nil), g.WebSearchQueries...)}
	for _, c := range g.Chunks {
		if c.Domain == "" && c.Title == "" && c.URI == "" {
			out.GroundingChunks = append(out.GroundingChunks, &gemini.GroundingChunk{})
			continue
		}
		out.GroundingChunks = append(out.GroundingChunks, &gemini.GroundingChunk{Web: &gemini.GroundingChunkWeb{Domain: c.Domain, Title: c.Title, URI: c.URI}})
	}
	for _, s := range g.Supports {
		support := &gemini.GroundingSupport{
			ConfidenceScores:      append([]float32(nil), s.ConfidenceScores...),
			GroundingChunkIndices: append([]int(nil), s.ChunkIndices...),
		}
		if s.Segment != nil {
			support.Segment = &gemini.Segment{StartIndex: s.Segment.Start, EndIndex: s.Segment.End, PartIndex: s.Segment.PartIndex, Text: s.Segment.Text}
		}
		out.GroundingSupports = append(out.GroundingSupports, support)
	}
	if g.RenderedEntryPoint != "" {
		out.SearchEntryPoint = &gemini.SearchEntryPoint{RenderedContent: g.RenderedEntryPoint}
	}
	return out
}

func responseMetaFromEino(rm *einoschema.AgenticResponseMeta) (*ResponseMeta, []PrivateBlockState, error) {
	if rm == nil {
		return nil, nil, nil
	}
	if rm.Extension != nil {
		return nil, nil, ErrContentUnsupported
	}
	meta := &ResponseMeta{Usage: usageFromTokenUsage(rm.TokenUsage)}
	var private []PrivateBlockState
	responseID := ""
	previousResponseID := ""
	var createdAt int64
	if rm.OpenAIExtension != nil {
		ext := rm.OpenAIExtension
		openAIMeta := &OpenAIResponseMeta{
			Status: string(ext.Status), ServiceTier: string(ext.ServiceTier), PromptCacheRetention: string(ext.PromptCacheRetention),
		}
		if ext.Error != nil {
			openAIMeta.ErrorCode = string(ext.Error.Code)
			openAIMeta.ErrorMessage = ext.Error.Message
		}
		if ext.IncompleteDetails != nil {
			openAIMeta.IncompleteReason = ext.IncompleteDetails.Reason
		}
		if ext.Reasoning != nil {
			openAIMeta.ReasoningEffort = string(ext.Reasoning.Effort)
			openAIMeta.ReasoningSummary = string(ext.Reasoning.Summary)
		}
		meta.OpenAI = openAIMeta
		if ext.ID != "" {
			responseID = ext.ID
		}
		previousResponseID = ext.PreviousResponseID
		createdAt = ext.CreatedAt
	}
	if rm.ClaudeExtension != nil {
		ext := rm.ClaudeExtension
		claudeMeta := &ClaudeResponseMeta{StopReason: ext.StopReason, StopSequence: ext.StopSequence}
		if ext.StopDetails != nil {
			claudeMeta.StopCategory = ext.StopDetails.Category
			claudeMeta.StopExplanation = ext.StopDetails.Explanation
		}
		meta.Claude = claudeMeta
		if ext.ID != "" && responseID == "" {
			responseID = ext.ID
		}
	}
	if rm.GeminiExtension != nil {
		ext := rm.GeminiExtension
		geminiMeta := &GeminiResponseMeta{FinishReason: ext.FinishReason}
		if ext.GroundingMeta != nil {
			grounding, blob := groundingFromEino(ext.GroundingMeta)
			geminiMeta.Grounding = grounding
			if len(blob) > 0 {
				raw, err := json.Marshal(privateSDKBlob{SDKBlobBase64: base64.StdEncoding.EncodeToString(blob)})
				if err != nil {
					return nil, nil, ErrContentInvalid
				}
				private = append(private, PrivateBlockState{BlockID: "", Data: raw})
			}
		}
		meta.Gemini = geminiMeta
		if ext.ID != "" && responseID == "" {
			responseID = ext.ID
		}
	}
	if responseID != "" || previousResponseID != "" || createdAt != 0 {
		raw, err := json.Marshal(privateResponseIDs{ResponseID: responseID, PreviousResponseID: previousResponseID, CreatedAt: createdAt})
		if err != nil {
			return nil, nil, ErrContentInvalid
		}
		private = append(private, PrivateBlockState{BlockID: "", Data: raw})
	}
	return meta, private, nil
}

// ContentToAgenticMessage rebuilds the public *schema.AgenticMessage from
// durable Content. The result carries no private fields, no Extra, and no
// StreamingMeta.
func ContentToAgenticMessage(content Content) (*einoschema.AgenticMessage, error) {
	role, err := agenticRoleFromContent(content.Role)
	if err != nil {
		return nil, err
	}
	blocks := make([]*einoschema.ContentBlock, 0, len(content.Blocks))
	for _, block := range content.Blocks {
		eb, err := contentBlockToEino(block)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, eb)
	}
	meta, err := responseMetaToEino(content.Meta)
	if err != nil {
		return nil, err
	}
	return &einoschema.AgenticMessage{Role: role, ContentBlocks: blocks, ResponseMeta: meta}, nil
}

func mediaToUserInputImage(m *MediaBlock) *einoschema.UserInputImage {
	return &einoschema.UserInputImage{URL: m.URL, Base64Data: m.Base64Data, MIMEType: m.MIMEType, Detail: einoschema.ImageURLDetail(m.Detail)}
}

func mediaToUserInputAudio(m *MediaBlock) *einoschema.UserInputAudio {
	return &einoschema.UserInputAudio{URL: m.URL, Base64Data: m.Base64Data, MIMEType: m.MIMEType}
}

func mediaToUserInputVideo(m *MediaBlock) *einoschema.UserInputVideo {
	return &einoschema.UserInputVideo{URL: m.URL, Base64Data: m.Base64Data, MIMEType: m.MIMEType}
}

func mediaToUserInputFile(m *MediaBlock) *einoschema.UserInputFile {
	return &einoschema.UserInputFile{URL: m.URL, Base64Data: m.Base64Data, MIMEType: m.MIMEType, Name: m.Name}
}

func isOpenAIAnnotationType(t string) bool {
	switch openai.TextAnnotationType(t) {
	case openai.TextAnnotationTypeFileCitation, openai.TextAnnotationTypeURLCitation, openai.TextAnnotationTypeContainerFileCitation, openai.TextAnnotationTypeFilePath:
		return true
	default:
		return false
	}
}

func isClaudeAnnotationType(t string) bool {
	switch claude.TextCitationType(t) {
	case claude.TextCitationTypeCharLocation, claude.TextCitationTypePageLocation, claude.TextCitationTypeContentBlockLocation, claude.TextCitationTypeWebSearchResultLocation:
		return true
	default:
		return false
	}
}

func annotationToOpenAI(a TextAnnotation) *openai.TextAnnotation {
	out := &openai.TextAnnotation{Type: openai.TextAnnotationType(a.Type)}
	switch out.Type {
	case openai.TextAnnotationTypeFileCitation:
		out.FileCitation = &openai.TextAnnotationFileCitation{FileID: a.FileID, Filename: a.Filename, Index: a.AnnotationIndex}
	case openai.TextAnnotationTypeURLCitation:
		out.URLCitation = &openai.TextAnnotationURLCitation{Title: a.Title, URL: a.URL, StartIndex: a.StartIndex, EndIndex: a.EndIndex}
	case openai.TextAnnotationTypeContainerFileCitation:
		out.ContainerFileCitation = &openai.TextAnnotationContainerFileCitation{
			ContainerID: a.ContainerID, FileID: a.FileID, Filename: a.Filename, StartIndex: a.StartIndex, EndIndex: a.EndIndex,
		}
	case openai.TextAnnotationTypeFilePath:
		out.FilePath = &openai.TextAnnotationFilePath{FileID: a.FileID, Index: a.AnnotationIndex}
	}
	return out
}

func annotationToClaude(a TextAnnotation) *claude.TextCitation {
	out := &claude.TextCitation{Type: claude.TextCitationType(a.Type)}
	switch out.Type {
	case claude.TextCitationTypeCharLocation:
		out.CharLocation = &claude.CitationCharLocation{
			CitedText: a.CitedText, DocumentTitle: a.DocumentTitle, DocumentIndex: a.DocumentIndex,
			StartCharIndex: a.StartIndex, EndCharIndex: a.EndIndex,
		}
	case claude.TextCitationTypePageLocation:
		out.PageLocation = &claude.CitationPageLocation{
			CitedText: a.CitedText, DocumentTitle: a.DocumentTitle, DocumentIndex: a.DocumentIndex,
			StartPageNumber: a.StartPage, EndPageNumber: a.EndPage,
		}
	case claude.TextCitationTypeContentBlockLocation:
		out.ContentBlockLocation = &claude.CitationContentBlockLocation{
			CitedText: a.CitedText, DocumentTitle: a.DocumentTitle, DocumentIndex: a.DocumentIndex,
			StartBlockIndex: a.StartBlock, EndBlockIndex: a.EndBlock,
		}
	case claude.TextCitationTypeWebSearchResultLocation:
		out.WebSearchResultLocation = &claude.CitationWebSearchResultLocation{CitedText: a.CitedText, Title: a.Title, URL: a.URL}
	}
	return out
}

func assistantGenTextToEino(t *TextBlock) (*einoschema.AssistantGenText, error) {
	out := &einoschema.AssistantGenText{Text: t.Text}
	if t.Refusal != "" {
		out.OpenAIExtension = &openai.AssistantGenTextExtension{Refusal: &openai.OutputRefusal{Reason: t.Refusal}}
	}
	var openaiAnnotations []*openai.TextAnnotation
	var claudeCitations []*claude.TextCitation
	for _, a := range t.Annotations {
		switch {
		case isOpenAIAnnotationType(a.Type):
			openaiAnnotations = append(openaiAnnotations, annotationToOpenAI(a))
		case isClaudeAnnotationType(a.Type):
			claudeCitations = append(claudeCitations, annotationToClaude(a))
		default:
			return nil, ErrContentInvalid
		}
	}
	if len(openaiAnnotations) > 0 {
		if out.OpenAIExtension == nil {
			out.OpenAIExtension = &openai.AssistantGenTextExtension{}
		}
		out.OpenAIExtension.Annotations = openaiAnnotations
	}
	if len(claudeCitations) > 0 {
		out.ClaudeExtension = &claude.AssistantGenTextExtension{Citations: claudeCitations}
	}
	return out, nil
}

func functionResultContentToEino(items []ResultContent) ([]*einoschema.FunctionToolResultContentBlock, error) {
	if len(items) == 0 {
		return nil, nil
	}
	out := make([]*einoschema.FunctionToolResultContentBlock, 0, len(items))
	for _, item := range items {
		switch item.Type {
		case ResultContentText:
			out = append(out, &einoschema.FunctionToolResultContentBlock{Type: einoschema.FunctionToolResultContentBlockTypeText, Text: &einoschema.UserInputText{Text: item.Text}})
		case ResultContentImage:
			if item.Media == nil {
				return nil, ErrContentInvalid
			}
			out = append(out, &einoschema.FunctionToolResultContentBlock{Type: einoschema.FunctionToolResultContentBlockTypeImage, Image: mediaToUserInputImage(item.Media)})
		case ResultContentAudio:
			if item.Media == nil {
				return nil, ErrContentInvalid
			}
			out = append(out, &einoschema.FunctionToolResultContentBlock{Type: einoschema.FunctionToolResultContentBlockTypeAudio, Audio: mediaToUserInputAudio(item.Media)})
		case ResultContentVideo:
			if item.Media == nil {
				return nil, ErrContentInvalid
			}
			out = append(out, &einoschema.FunctionToolResultContentBlock{Type: einoschema.FunctionToolResultContentBlockTypeVideo, Video: mediaToUserInputVideo(item.Media)})
		case ResultContentFile:
			if item.Media == nil {
				return nil, ErrContentInvalid
			}
			out = append(out, &einoschema.FunctionToolResultContentBlock{Type: einoschema.FunctionToolResultContentBlockTypeFile, File: mediaToUserInputFile(item.Media)})
		default:
			return nil, ErrContentInvalid
		}
	}
	return out, nil
}

func toolSearchToEino(t *ToolSearchBlock) (*einoschema.ToolSearchFunctionToolResult, error) {
	out := &einoschema.ToolSearchFunctionToolResult{CallID: t.CallID, Name: t.Name}
	if t.Tools != nil {
		tools := make([]*einoschema.ToolInfo, 0, len(t.Tools))
		for _, raw := range t.Tools {
			info := &einoschema.ToolInfo{}
			if err := json.Unmarshal(raw, info); err != nil {
				return nil, ErrContentInvalid
			}
			tools = append(tools, info)
		}
		out.Result = &einoschema.ToolSearchResult{Tools: tools}
	}
	return out, nil
}

func boundedJSONToAny(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, ErrContentInvalid
	}
	return v, nil
}

func responseMetaToEino(meta *ResponseMeta) (*einoschema.AgenticResponseMeta, error) {
	if meta == nil {
		return nil, nil
	}
	out := &einoschema.AgenticResponseMeta{TokenUsage: tokenUsageFromUsage(meta.Usage)}
	if meta.OpenAI != nil {
		m := meta.OpenAI
		ext := &openai.ResponseMetaExtension{
			Status: openai.ResponseStatus(m.Status), ServiceTier: openai.ServiceTier(m.ServiceTier), PromptCacheRetention: openai.PromptCacheRetention(m.PromptCacheRetention),
		}
		if m.ErrorCode != "" || m.ErrorMessage != "" {
			ext.Error = &openai.ResponseError{Code: openai.ResponseErrorCode(m.ErrorCode), Message: m.ErrorMessage}
		}
		if m.IncompleteReason != "" {
			ext.IncompleteDetails = &openai.IncompleteDetails{Reason: m.IncompleteReason}
		}
		if m.ReasoningEffort != "" || m.ReasoningSummary != "" {
			ext.Reasoning = &openai.Reasoning{Effort: openai.ReasoningEffort(m.ReasoningEffort), Summary: openai.ReasoningSummary(m.ReasoningSummary)}
		}
		out.OpenAIExtension = ext
	}
	if meta.Claude != nil {
		m := meta.Claude
		ext := &claude.ResponseMetaExtension{StopReason: m.StopReason, StopSequence: m.StopSequence}
		if m.StopCategory != "" || m.StopExplanation != "" {
			ext.StopDetails = &claude.StopDetails{Category: m.StopCategory, Explanation: m.StopExplanation}
		}
		out.ClaudeExtension = ext
	}
	if meta.Gemini != nil {
		m := meta.Gemini
		ext := &gemini.ResponseMetaExtension{FinishReason: m.FinishReason}
		if m.Grounding != nil {
			ext.GroundingMeta = groundingToEino(m.Grounding)
		}
		out.GeminiExtension = ext
	}
	return out, nil
}

func contentBlockToEino(block ContentBlock) (*einoschema.ContentBlock, error) {
	switch block.Kind {
	case BlockKindReasoning:
		if block.Reasoning == nil {
			return nil, ErrContentInvalid
		}
		r := &einoschema.Reasoning{Text: block.Reasoning.Text}
		if len(block.Reasoning.Summary) > 0 {
			content := make([]*openai.ReasoningContent, 0, len(block.Reasoning.Summary))
			for _, s := range block.Reasoning.Summary {
				content = append(content, &openai.ReasoningContent{Text: s})
			}
			r.OpenAIExtension = &openai.ReasoningExtension{Content: content}
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeReasoning, Reasoning: r}, nil

	case BlockKindUserInputText:
		if block.Text == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputText, UserInputText: &einoschema.UserInputText{Text: block.Text.Text}}, nil

	case BlockKindUserInputImage:
		if block.Media == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputImage, UserInputImage: mediaToUserInputImage(block.Media)}, nil

	case BlockKindUserInputAudio:
		if block.Media == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputAudio, UserInputAudio: mediaToUserInputAudio(block.Media)}, nil

	case BlockKindUserInputVideo:
		if block.Media == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputVideo, UserInputVideo: mediaToUserInputVideo(block.Media)}, nil

	case BlockKindUserInputFile:
		if block.Media == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputFile, UserInputFile: mediaToUserInputFile(block.Media)}, nil

	case BlockKindToolSearchResult:
		if block.ToolSearch == nil {
			return nil, ErrContentInvalid
		}
		result, err := toolSearchToEino(block.ToolSearch)
		if err != nil {
			return nil, err
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeToolSearchResult, ToolSearchFunctionToolResult: result}, nil

	case BlockKindAssistantGenText:
		if block.Text == nil {
			return nil, ErrContentInvalid
		}
		t, err := assistantGenTextToEino(block.Text)
		if err != nil {
			return nil, err
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: t}, nil

	case BlockKindAssistantGenImage:
		if block.Media == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenImage, AssistantGenImage: &einoschema.AssistantGenImage{
			URL: block.Media.URL, Base64Data: block.Media.Base64Data, MIMEType: block.Media.MIMEType,
		}}, nil

	case BlockKindAssistantGenAudio:
		if block.Media == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenAudio, AssistantGenAudio: &einoschema.AssistantGenAudio{
			URL: block.Media.URL, Base64Data: block.Media.Base64Data, MIMEType: block.Media.MIMEType,
		}}, nil

	case BlockKindAssistantGenVideo:
		if block.Media == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenVideo, AssistantGenVideo: &einoschema.AssistantGenVideo{
			URL: block.Media.URL, Base64Data: block.Media.Base64Data, MIMEType: block.Media.MIMEType,
		}}, nil

	case BlockKindFunctionToolCall:
		if block.FunctionCall == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{
			CallID: block.FunctionCall.CallID, Name: block.FunctionCall.Name, Arguments: block.FunctionCall.Arguments,
		}}, nil

	case BlockKindFunctionToolResult:
		if block.FunctionResult == nil {
			return nil, ErrContentInvalid
		}
		content, err := functionResultContentToEino(block.FunctionResult.Content)
		if err != nil {
			return nil, err
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeFunctionToolResult, FunctionToolResult: &einoschema.FunctionToolResult{
			CallID: block.FunctionResult.CallID, Name: block.FunctionResult.Name, Content: content,
		}}, nil

	case BlockKindServerToolCall:
		if block.ServerCall == nil {
			return nil, ErrContentInvalid
		}
		args, err := boundedJSONToAny(block.ServerCall.Arguments)
		if err != nil {
			return nil, err
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeServerToolCall, ServerToolCall: &einoschema.ServerToolCall{
			Name: block.ServerCall.Name, CallID: block.ServerCall.CallID, Arguments: args,
		}}, nil

	case BlockKindServerToolResult:
		if block.ServerResult == nil {
			return nil, ErrContentInvalid
		}
		content, err := boundedJSONToAny(block.ServerResult.Content)
		if err != nil {
			return nil, err
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeServerToolResult, ServerToolResult: &einoschema.ServerToolResult{
			Name: block.ServerResult.Name, CallID: block.ServerResult.CallID, Content: content,
		}}, nil

	case BlockKindMCPToolCall:
		if block.MCPCall == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolCall, MCPToolCall: &einoschema.MCPToolCall{
			ServerLabel: block.MCPCall.ServerLabel, ApprovalRequestID: block.MCPCall.ApprovalRequestID, CallID: block.MCPCall.CallID,
			Name: block.MCPCall.Name, Arguments: block.MCPCall.Arguments,
		}}, nil

	case BlockKindMCPToolResult:
		if block.MCPResult == nil {
			return nil, ErrContentInvalid
		}
		var mcpErr *einoschema.MCPToolCallError
		if block.MCPResult.ErrorCode != nil || block.MCPResult.ErrorMessage != "" {
			mcpErr = &einoschema.MCPToolCallError{Message: block.MCPResult.ErrorMessage}
			if block.MCPResult.ErrorCode != nil {
				v := *block.MCPResult.ErrorCode
				mcpErr.Code = &v
			}
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolResult, MCPToolResult: &einoschema.MCPToolResult{
			ServerLabel: block.MCPResult.ServerLabel, CallID: block.MCPResult.CallID, Name: block.MCPResult.Name,
			Content: block.MCPResult.Content, Error: mcpErr,
		}}, nil

	case BlockKindMCPListToolsResult:
		if block.MCPListTools == nil {
			return nil, ErrContentInvalid
		}
		tools := make([]*einoschema.MCPListToolsItem, 0, len(block.MCPListTools.Tools))
		for _, t := range block.MCPListTools.Tools {
			item := &einoschema.MCPListToolsItem{Name: t.Name, Description: t.Description}
			if len(t.InputSchema) > 0 {
				schema := &jsonschema.Schema{}
				if err := json.Unmarshal(t.InputSchema, schema); err != nil {
					return nil, ErrContentInvalid
				}
				item.InputSchema = schema
			}
			tools = append(tools, item)
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPListToolsResult, MCPListToolsResult: &einoschema.MCPListToolsResult{
			ServerLabel: block.MCPListTools.ServerLabel, Tools: tools, Error: block.MCPListTools.Error,
		}}, nil

	case BlockKindMCPToolApprovalRequest:
		if block.MCPApprovalRequest == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{
			ID: block.MCPApprovalRequest.ID, Name: block.MCPApprovalRequest.Name, Arguments: block.MCPApprovalRequest.Arguments, ServerLabel: block.MCPApprovalRequest.ServerLabel,
		}}, nil

	case BlockKindMCPToolApprovalResponse:
		if block.MCPApprovalResponse == nil {
			return nil, ErrContentInvalid
		}
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolApprovalResponse, MCPToolApprovalResponse: &einoschema.MCPToolApprovalResponse{
			ApprovalRequestID: block.MCPApprovalResponse.ApprovalRequestID, Approve: block.MCPApprovalResponse.Approve, Reason: block.MCPApprovalResponse.Reason,
		}}, nil

	default:
		return nil, ErrContentUnsupported
	}
}

// Clone returns a deep copy of c. Mutating the clone's blocks, metadata, or
// any nested slice never affects c. An error is returned, rather than a
// partial or empty copy, if c cannot be round-tripped through JSON (for
// example a json.RawMessage field holding non-JSON bytes, or a NaN/Inf float
// in Grounding.Supports[].ConfidenceScores).
func (c Content) Clone() (Content, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return Content{}, errors.Join(ErrContentInvalid, err)
	}
	var out Content
	if err := json.Unmarshal(raw, &out); err != nil {
		return Content{}, errors.Join(ErrContentInvalid, err)
	}
	return out, nil
}
