package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/kiro"
)

// kiroWebSearchInterceptor implements kiro.ToolCallInterceptor. When the
// Kiro model invokes the "web_search" tool (which the request transformer
// synthesises from an Anthropic server-side web_search_* entry), this
// interceptor:
//  1. Buffers the tool_use_start / deltas / stop events (hiding them from
//     the downstream client entirely).
//  2. Once the tool input JSON is complete, calls Kiro's /mcp endpoint
//     with the account's own bearer token to perform the actual search.
//  3. Streams the search summary back as plain assistant text_content.
//
// The end result is that Claude Code sees WebSearch return a regular text
// answer, as if the model wrote it itself. No third-party API key is
// required; the search is fulfilled by Kiro infrastructure using the
// caller's own Kiro account credentials.
type kiroWebSearchInterceptor struct {
	ctx    context.Context
	client *http.Client
	token  string
	region string
}

// newKiroWebSearchInterceptor constructs an interceptor bound to a single
// Kiro request. The http.Client should already honour any account-specific
// proxy configuration.
func newKiroWebSearchInterceptor(ctx context.Context, client *http.Client, token, profileArn string) *kiroWebSearchInterceptor {
	region := kiro.RegionFromProfileArn(profileArn)
	if region == "" {
		region = kiro.DefaultMCPRegion
	}
	return &kiroWebSearchInterceptor{
		ctx:    ctx,
		client: client,
		token:  token,
		region: region,
	}
}

// OnToolStart matches the synthetic "web_search" function tool that the
// request transformer injects on behalf of Anthropic server-side
// web_search_* entries. Any other tool call passes through unchanged.
func (i *kiroWebSearchInterceptor) OnToolStart(_ context.Context, ev *kiro.StreamEvent) bool {
	return ev != nil && ev.ToolName == "web_search"
}

// OnToolStop performs the MCP call and returns events that emit a single
// text content block with the formatted search results.
func (i *kiroWebSearchInterceptor) OnToolStop(ctx context.Context, toolUseID, toolName, input string) ([]*kiro.StreamEvent, error) {
	query := extractQueryFromToolInput(input)
	if query == "" {
		slog.Warn("kiro web_search interceptor: empty query; skipping",
			"tool_use_id", toolUseID, "raw_input", input)
		return []*kiro.StreamEvent{{
			Kind: "content",
			Text: "\n<web_search>\nNo query provided.\n</web_search>\n",
		}}, nil
	}

	slog.Info("kiro web_search interceptor: executing MCP query",
		"tool_use_id", toolUseID, "query", query, "region", i.region)

	resp, err := kiro.CallMCPWebSearch(ctx, i.client, i.region, i.token, query)
	if err != nil {
		slog.Error("kiro web_search interceptor: MCP call failed", "error", err)
		// Return a graceful text event instead of propagating an error;
		// the model's turn can continue even if search failed.
		return []*kiro.StreamEvent{{
			Kind: "content",
			Text: fmt.Sprintf(
				"\n<web_search>\nWeb search failed: %s\n</web_search>\n",
				err.Error(),
			),
		}}, nil
	}

	summary := kiro.FormatSearchSummary(resp)
	return []*kiro.StreamEvent{{Kind: "content", Text: summary}}, nil
}

// extractQueryFromToolInput parses the aggregated partial_json string that
// the Kiro model emits for the web_search tool and returns the "query"
// field. Returns an empty string if the JSON is malformed or the field is
// missing.
func extractQueryFromToolInput(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return ""
	}
	if q, ok := m["query"].(string); ok {
		return strings.TrimSpace(q)
	}
	return ""
}
