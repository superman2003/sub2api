package kiro

import (
	"encoding/json"
	"testing"
)

// TestBuildKiroPayload_DropsAnthropicServerSideTools verifies that Anthropic
// server-side tools (web_search_*, computer_*, text_editor_*, ...) are
// filtered out before the payload is sent to Kiro CodeWhisperer. Forwarding
// them verbatim previously triggered upstream "Invalid tool parameters"
// errors whenever Claude Code CLI was used against a Kiro account.
func TestBuildKiroPayload_DropsAnthropicServerSideTools(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}}}`)
	req := &AnthropicRequest{
		Model: "claude-sonnet-4.5",
		Messages: []AnthropicMessage{
			{Role: "user", Content: json.RawMessage(`"Hello"`)},
		},
		Tools: []AnthropicTool{
			// User-defined tool: should survive.
			{Name: "Read", Description: "Read a file", InputSchema: schema},
			// Anthropic server-side tools: should be dropped.
			{Type: "web_search_20250305", Name: "web_search"},
			{Type: "computer_20250124", Name: "computer"},
			{Type: "text_editor_20250124", Name: "str_replace_editor"},
			// Explicit function type is equivalent to user-defined.
			{Type: "function", Name: "Grep", InputSchema: schema},
			// Explicit custom type, treated as user-defined.
			{Type: "custom", Name: "Bash", InputSchema: schema},
		},
	}

	payload, err := BuildKiroPayload(req, BuildOptions{ProfileArn: "arn:test"})
	if err != nil {
		t.Fatalf("BuildKiroPayload: %v", err)
	}

	cs, _ := payload["conversationState"].(map[string]any)
	current, _ := cs["currentMessage"].(map[string]any)
	uim, _ := current["userInputMessage"].(map[string]any)
	ctx, _ := uim["userInputMessageContext"].(map[string]any)
	tools, _ := ctx["tools"].([]any)

	if got, want := len(tools), 3; got != want {
		t.Fatalf("expected %d user-defined tools, got %d: %#v", want, got, tools)
	}

	gotNames := map[string]bool{}
	for _, tool := range tools {
		m, ok := tool.(map[string]any)
		if !ok {
			t.Fatalf("tool entry is not a map: %#v", tool)
		}
		spec, ok := m["toolSpecification"].(map[string]any)
		if !ok {
			t.Fatalf("tool missing toolSpecification: %#v", m)
		}
		name, _ := spec["name"].(string)
		gotNames[name] = true
	}
	for _, want := range []string{"Read", "Grep", "Bash"} {
		if !gotNames[want] {
			t.Errorf("expected user tool %q to be forwarded, got names: %#v", want, gotNames)
		}
	}
	for _, forbidden := range []string{"web_search", "computer", "str_replace_editor"} {
		if gotNames[forbidden] {
			t.Errorf("server-side tool %q should have been dropped, got names: %#v", forbidden, gotNames)
		}
	}
}

// TestBuildKiroPayload_OmitsEmptyToolsWhenAllFiltered ensures that when every
// submitted tool is an Anthropic server-side tool, the resulting payload does
// not include an empty "tools" array (which Kiro may reject).
func TestBuildKiroPayload_OmitsEmptyToolsWhenAllFiltered(t *testing.T) {
	req := &AnthropicRequest{
		Model: "claude-sonnet-4.5",
		Messages: []AnthropicMessage{
			{Role: "user", Content: json.RawMessage(`"Hi"`)},
		},
		Tools: []AnthropicTool{
			{Type: "web_search_20250305", Name: "web_search"},
		},
	}

	payload, err := BuildKiroPayload(req, BuildOptions{})
	if err != nil {
		t.Fatalf("BuildKiroPayload: %v", err)
	}
	cs, _ := payload["conversationState"].(map[string]any)
	current, _ := cs["currentMessage"].(map[string]any)
	uim, _ := current["userInputMessage"].(map[string]any)
	ctx, ok := uim["userInputMessageContext"].(map[string]any)
	if !ok {
		// No context at all is acceptable too.
		return
	}
	if _, present := ctx["tools"]; present {
		t.Errorf("expected tools to be omitted when all entries are filtered, got context: %#v", ctx)
	}
}

func TestIsUserDefinedTool(t *testing.T) {
	cases := []struct {
		name  string
		tool  AnthropicTool
		allow bool
	}{
		{"empty type", AnthropicTool{Name: "a"}, true},
		{"function", AnthropicTool{Type: "function", Name: "a"}, true},
		{"custom", AnthropicTool{Type: "custom", Name: "a"}, true},
		{"web_search", AnthropicTool{Type: "web_search_20250305", Name: "web_search"}, false},
		{"computer", AnthropicTool{Type: "computer_20250124", Name: "computer"}, false},
		{"text_editor", AnthropicTool{Type: "text_editor_20250124", Name: "editor"}, false},
		{"bash server", AnthropicTool{Type: "bash_20250124", Name: "bash"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isUserDefinedTool(tc.tool); got != tc.allow {
				t.Errorf("isUserDefinedTool(%+v) = %v, want %v", tc.tool, got, tc.allow)
			}
		})
	}
}
