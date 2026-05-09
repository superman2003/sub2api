package kiro

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// DefaultModelMapping maps Anthropic model aliases to Kiro internal model IDs.
// Values correspond to the IDs returned by ListAvailableModels on the free tier
// (see kiro-gateway jwadow/kiro-gateway HIDDEN_MODELS + /v1/models output).
//
// Unknown Anthropic names fall through and are forwarded as-is so that a user
// group can map them to Kiro IDs through the existing model_mapping override.
var DefaultModelMapping = map[string]string{
	// Claude 4.7 family
	"claude-opus-4-7":   "claude-opus-4.7",
	"claude-opus-4.7":   "claude-opus-4.7",
	// Claude 4.6 family
	"claude-opus-4-6":   "claude-opus-4.6",
	"claude-opus-4.6":   "claude-opus-4.6",
	"claude-sonnet-4-6": "claude-sonnet-4.6",
	"claude-sonnet-4.6": "claude-sonnet-4.6",
	// Claude 4.5 family
	"claude-sonnet-4-5":          "claude-sonnet-4.5",
	"claude-sonnet-4-5-20250929": "claude-sonnet-4.5",
	"claude-haiku-4-5":           "claude-haiku-4.5",
	"claude-haiku-4-5-20251001":  "claude-haiku-4.5",
	"claude-opus-4-5":            "claude-opus-4.5",
	"claude-opus-4-5-20251101":   "claude-opus-4.5",
	// Claude 4 family
	"claude-sonnet-4":          "claude-sonnet-4",
	"claude-sonnet-4-20250514": "claude-sonnet-4",
	// Claude 3.7
	"claude-3-7-sonnet-20250219": "claude-3.7-sonnet",
	"claude-3-7-sonnet":          "claude-3.7-sonnet",
	"claude-3.7-sonnet":          "claude-3.7-sonnet",
	// Explicit passthrough for dotted names used by Kiro directly.
	"claude-sonnet-4.5": "claude-sonnet-4.5",
	"claude-haiku-4.5":  "claude-haiku-4.5",
	"claude-opus-4.5":   "claude-opus-4.5",
}

// ResolveModel returns the Kiro-internal model id for the given Anthropic
// model name. It first consults accountMapping (per-account override), then
// falls back to DefaultModelMapping, otherwise returns the input unchanged.
func ResolveModel(modelName string, accountMapping map[string]string) string {
	if modelName == "" {
		return "claude-sonnet-4.5"
	}
	if accountMapping != nil {
		if v, ok := accountMapping[modelName]; ok && v != "" {
			return v
		}
	}
	if v, ok := DefaultModelMapping[modelName]; ok {
		return v
	}
	return modelName
}

// AnthropicRequest is a minimal shape of the Anthropic /v1/messages body we
// consume. Fields we do not care about are captured in Extra for passthrough.
type AnthropicRequest struct {
	Model     string            `json:"model"`
	System    any               `json:"system,omitempty"`
	Messages  []AnthropicMessage `json:"messages"`
	Tools     []AnthropicTool    `json:"tools,omitempty"`
	MaxTokens int                `json:"max_tokens,omitempty"`
	Stream    bool               `json:"stream,omitempty"`
	Thinking  *AnthropicThinking `json:"thinking,omitempty"`
	Temperature *float64         `json:"temperature,omitempty"`
	TopP      *float64           `json:"top_p,omitempty"`
}

// AnthropicThinking represents the {"type":"enabled","budget_tokens":n} block.
type AnthropicThinking struct {
	Type         string `json:"type,omitempty"`
	BudgetTokens int    `json:"budget_tokens,omitempty"`
}

// AnthropicMessage mirrors the Anthropic Messages API message shape.
type AnthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// AnthropicTool mirrors a single tool in the Anthropic request.
type AnthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

// BuildOptions carries per-request knobs for transformers.
type BuildOptions struct {
	// ConversationID reuses an existing Kiro conversation id when stable
	// (empty => generate a new UUID).
	ConversationID string
	// ProfileArn must match the account's profileArn captured at OAuth time.
	ProfileArn string
	// ModelMapping is a per-account override map (anthropic -> kiro model id).
	ModelMapping map[string]string
}

// BuildKiroPayload converts an Anthropic-style request into Kiro's
// conversationState payload shape expected by /generateAssistantResponse.
func BuildKiroPayload(req *AnthropicRequest, opts BuildOptions) (map[string]any, error) {
	if req == nil {
		return nil, fmt.Errorf("anthropic request is nil")
	}
	if len(req.Messages) == 0 {
		return nil, fmt.Errorf("no messages to send")
	}

	modelID := ResolveModel(req.Model, opts.ModelMapping)
	conversationID := opts.ConversationID
	if conversationID == "" {
		conversationID = uuid.NewString()
	}

	systemPrompt := extractSystemPrompt(req.System)

	// Split messages: history = all but last; current = last.
	msgs := normaliseMessages(req.Messages)
	if len(msgs) == 0 {
		return nil, fmt.Errorf("no messages after normalisation")
	}

	// Prepend system prompt to the first user message in history (if any),
	// otherwise merge into the current message later.
	var history []any
	if len(msgs) > 1 {
		historyMsgs := msgs[:len(msgs)-1]
		if systemPrompt != "" {
			for i := range historyMsgs {
				if historyMsgs[i].Role == "user" {
					historyMsgs[i].Text = systemPrompt + "\n\n" + historyMsgs[i].Text
					break
				}
			}
		}
		history = buildHistory(historyMsgs, modelID)
	}

	current := msgs[len(msgs)-1]
	currentText := current.Text
	if systemPrompt != "" && len(history) == 0 {
		currentText = systemPrompt + "\n\n" + currentText
	}

	// Ensure current message ends up as a user role.
	if current.Role == "assistant" {
		history = append(history, map[string]any{
			"assistantResponseMessage": map[string]any{
				"content": currentText,
			},
		})
		currentText = "Continue"
	}
	if strings.TrimSpace(currentText) == "" {
		currentText = "Continue"
	}

	// Build userInputMessageContext (tools + toolResults; images are inline).
	userInputContext := map[string]any{}
	if len(req.Tools) > 0 {
		kiroTools := make([]any, 0, len(req.Tools))
		for _, t := range req.Tools {
			var schema any
			if len(t.InputSchema) > 0 {
				_ = json.Unmarshal(t.InputSchema, &schema)
			}
			spec := map[string]any{
				"name":        t.Name,
				"description": t.Description,
			}
			if schema != nil {
				spec["inputSchema"] = map[string]any{"json": schema}
			}
			kiroTools = append(kiroTools, map[string]any{"toolSpecification": spec})
		}
		userInputContext["tools"] = kiroTools
	}

	// Assemble userInputMessage
	userInputMessage := map[string]any{
		"content": currentText,
		"modelId": modelID,
		"origin":  "AI_EDITOR",
	}
	if len(userInputContext) > 0 {
		userInputMessage["userInputMessageContext"] = userInputContext
	}

	conversationState := map[string]any{
		"chatTriggerType": "MANUAL",
		"conversationId":  conversationID,
		"currentMessage": map[string]any{
			"userInputMessage": userInputMessage,
		},
	}
	if len(history) > 0 {
		conversationState["history"] = history
	}

	payload := map[string]any{
		"conversationState": conversationState,
	}
	if opts.ProfileArn != "" {
		payload["profileArn"] = opts.ProfileArn
	}
	return payload, nil
}

// uniMessage is the minimal per-message shape after we normalise content.
type uniMessage struct {
	Role string
	Text string
}

// normaliseMessages extracts text-only messages and merges adjacent same-role
// ones. Tool results / images are currently out of scope for the MVP.
func normaliseMessages(in []AnthropicMessage) []uniMessage {
	out := make([]uniMessage, 0, len(in))
	for _, m := range in {
		text := extractMessageText(m.Content)
		if strings.TrimSpace(text) == "" {
			continue
		}
		role := m.Role
		if role != "user" && role != "assistant" {
			role = "user"
		}
		if len(out) > 0 && out[len(out)-1].Role == role {
			out[len(out)-1].Text += "\n" + text
			continue
		}
		out = append(out, uniMessage{Role: role, Text: text})
	}
	// Ensure first message is from user.
	if len(out) > 0 && out[0].Role != "user" {
		out = append([]uniMessage{{Role: "user", Text: ""}}, out...)
	}
	// Ensure alternating user/assistant roles by inserting synthetic assistant "..." between consecutive users.
	norm := make([]uniMessage, 0, len(out))
	for i, m := range out {
		norm = append(norm, m)
		if i < len(out)-1 && out[i+1].Role == m.Role {
			if m.Role == "user" {
				norm = append(norm, uniMessage{Role: "assistant", Text: "..."})
			} else {
				norm = append(norm, uniMessage{Role: "user", Text: "Continue"})
			}
		}
	}
	return norm
}

func buildHistory(msgs []uniMessage, modelID string) []any {
	history := make([]any, 0, len(msgs))
	for _, m := range msgs {
		switch m.Role {
		case "user":
			history = append(history, map[string]any{
				"userInputMessage": map[string]any{
					"content": m.Text,
					"modelId": modelID,
					"origin":  "AI_EDITOR",
				},
			})
		case "assistant":
			history = append(history, map[string]any{
				"assistantResponseMessage": map[string]any{
					"content": m.Text,
				},
			})
		}
	}
	return history
}

func extractSystemPrompt(system any) string {
	if system == nil {
		return ""
	}
	switch s := system.(type) {
	case string:
		return s
	case []any:
		var parts []string
		for _, item := range s {
			if m, ok := item.(map[string]any); ok && m["type"] == "text" {
				if text, ok := m["text"].(string); ok && text != "" {
					parts = append(parts, text)
				}
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		if text, ok := s["text"].(string); ok {
			return text
		}
	}
	// Some callers pass RawMessage.
	if raw, ok := system.(json.RawMessage); ok {
		var asStr string
		if err := json.Unmarshal(raw, &asStr); err == nil {
			return asStr
		}
		var asSlice []map[string]any
		if err := json.Unmarshal(raw, &asSlice); err == nil {
			var parts []string
			for _, m := range asSlice {
				if m["type"] == "text" {
					if text, ok := m["text"].(string); ok && text != "" {
						parts = append(parts, text)
					}
				}
			}
			return strings.Join(parts, "\n")
		}
	}
	return ""
}

func extractMessageText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	// Try string first.
	var asStr string
	if err := json.Unmarshal(raw, &asStr); err == nil {
		return asStr
	}
	// Fallback to content blocks array.
	var blocks []map[string]any
	if err := json.Unmarshal(raw, &blocks); err == nil {
		var parts []string
		for _, b := range blocks {
			switch b["type"] {
			case "text":
				if text, ok := b["text"].(string); ok && text != "" {
					parts = append(parts, text)
				}
			case "tool_use":
				// Minimal MVP: serialise tool_use as text so it's preserved in history.
				if name, _ := b["name"].(string); name != "" {
					input, _ := json.Marshal(b["input"])
					parts = append(parts, fmt.Sprintf("[tool_use %s %s]", name, input))
				}
			case "tool_result":
				if content, ok := b["content"].(string); ok && content != "" {
					parts = append(parts, content)
				} else if arr, ok := b["content"].([]any); ok {
					for _, it := range arr {
						if m, ok := it.(map[string]any); ok {
							if text, ok := m["text"].(string); ok {
								parts = append(parts, text)
							}
						}
					}
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return string(raw)
}
