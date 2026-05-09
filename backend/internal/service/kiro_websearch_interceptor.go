package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/kiro"
)

// kiroWebSearchInterceptor implements kiro.ToolCallInterceptor. When the
// Kiro model invokes the "web_search" / "WebSearch" tool, this interceptor:
//  1. Buffers the tool_use_start / deltas / stop events (hiding them from
//     the downstream client entirely).
//  2. Once the tool input JSON is complete, calls Kiro's /mcp endpoint
//     with the account's own bearer token to perform the actual search.
//  3. Launches a SECOND Kiro /generateAssistantResponse request whose
//     history contains the original conversation + the model's tool_use +
//     our synthetic tool_result carrying the search results, and streams
//     the model's follow-up natural-language answer back to the client.
//
// The end result is that Claude Code sees WebSearch behave like a proper
// Anthropic server-side tool: the search is fulfilled transparently and
// the model's second-turn summary is what the user receives. No
// third-party API keys, no manual configuration.
type kiroWebSearchInterceptor struct {
	ctx       context.Context
	client    *http.Client
	token     string
	region    string
	anthReq   *kiro.AnthropicRequest
	buildOpts kiro.BuildOptions
	endpoint  string
	headers   http.Header
	cache     *kiroMCPResultCache
	accountID int64
}

// newKiroWebSearchInterceptor constructs an interceptor bound to a single
// Kiro request. The http.Client should already honour any account-specific
// proxy configuration. anthReq is the original parsed request; a shallow
// copy is made so that appending tool-call turns does not mutate the
// caller's state. cache is optional; pass nil to disable result caching.
func newKiroWebSearchInterceptor(
	ctx context.Context,
	client *http.Client,
	token string,
	anthReq *kiro.AnthropicRequest,
	buildOpts kiro.BuildOptions,
	endpoint string,
	headers http.Header,
	cache *kiroMCPResultCache,
	accountID int64,
) *kiroWebSearchInterceptor {
	region := kiro.RegionFromProfileArn(buildOpts.ProfileArn)
	if region == "" {
		region = kiro.DefaultMCPRegion
	}
	return &kiroWebSearchInterceptor{
		ctx:       ctx,
		client:    client,
		token:     token,
		region:    region,
		anthReq:   anthReq,
		buildOpts: buildOpts,
		endpoint:  endpoint,
		headers:   headers,
		cache:     cache,
		accountID: accountID,
	}
}

// OnToolStart matches Kiro's `web_search` tool invocation. Matching is
// case-insensitive and tolerates `web_search`, `WebSearch`, etc.
func (i *kiroWebSearchInterceptor) OnToolStart(_ context.Context, ev *kiro.StreamEvent) bool {
	if ev == nil {
		return false
	}
	return isKiroWebSearchToolName(ev.ToolName)
}

// isKiroWebSearchToolName recognises the tool-name variants that should be
// fulfilled via Kiro's /mcp endpoint. Matching is case-insensitive.
//
//	"web_search"  → rewritten by the request transformer
//	"WebSearch"   → Claude Code's built-in server-side tool (function-shape)
func isKiroWebSearchToolName(name string) bool {
	switch strings.ToLower(name) {
	case "web_search", "websearch":
		return true
	}
	return false
}

// OnToolStop:
//  1. Parses the reassembled tool input to extract the search query.
//  2. Calls Kiro /mcp to run the search.
//  3. Launches a follow-up /generateAssistantResponse turn whose input
//     includes the tool_use + tool_result, and streams the model's
//     natural-language response back through the provided emit callback.
//
// If any step fails, a short explanatory text event is emitted so the
// client gets *something* instead of silence.
func (i *kiroWebSearchInterceptor) OnToolStop(
	ctx context.Context,
	toolUseID, toolName, input string,
	emit func(*kiro.StreamEvent) error,
) error {
	query := extractQueryFromToolInput(input)
	if query == "" {
		slog.Warn("kiro web_search interceptor: empty query; skipping",
			"tool_use_id", toolUseID, "raw_input", input)
		return emit(&kiro.StreamEvent{
			Kind: "content",
			Text: "\n<web_search>\nNo query provided.\n</web_search>\n",
		})
	}

	slog.Info("kiro web_search interceptor: executing MCP query",
		"tool_use_id", toolUseID, "query", query, "region", i.region)

	var mcpResp *kiro.MCPWebSearchResponse
	if cached := i.cache.Get(ctx, i.accountID, query); cached != nil {
		slog.Info("kiro web_search interceptor: cache hit",
			"tool_use_id", toolUseID, "query", query,
			"results", len(cached.Results))
		mcpResp = cached
	} else {
		resp, err := kiro.CallMCPWebSearch(ctx, i.client, i.region, i.token, query)
		if err != nil {
			slog.Error("kiro web_search interceptor: MCP call failed", "error", err)
			return emit(&kiro.StreamEvent{
				Kind: "content",
				Text: fmt.Sprintf(
					"\n<web_search>\nWeb search failed: %s\n</web_search>\n",
					err.Error(),
				),
			})
		}
		mcpResp = resp
		i.cache.Set(ctx, i.accountID, query, mcpResp)
	}

	// The MCP payload is our source of truth for the follow-up turn.
	// Serialise it as JSON text so the model can quote it naturally.
	rawResultsJSON, merr := json.Marshal(mcpResp)
	if merr != nil {
		slog.Error("kiro web_search interceptor: marshal MCP response failed", "error", merr)
		return emit(&kiro.StreamEvent{
			Kind: "content",
			Text: kiro.FormatSearchSummary(mcpResp),
		})
	}

	// Ask Kiro to summarise the results. If this follow-up turn fails for
	// any reason we fall back to shipping the raw formatted summary so
	// the user still sees something useful.
	slog.Info("kiro web_search interceptor: launching follow-up summary turn",
		"tool_use_id", toolUseID, "results", len(mcpResp.Results))
	if summarised, err := i.requestFollowUpSummary(ctx, toolUseID, toolName, query, rawResultsJSON, emit); err != nil {
		slog.Warn("kiro web_search interceptor: follow-up summary failed; falling back to raw results",
			"tool_use_id", toolUseID, "error", err)
		if summarised {
			// We already streamed some content to the client; don't
			// duplicate it with the fallback — just signal stop.
			return nil
		}
		return emit(&kiro.StreamEvent{
			Kind: "content",
			Text: kiro.FormatSearchSummary(mcpResp),
		})
	}
	return nil
}

// requestFollowUpSummary builds a new Kiro request that continues the
// assistant turn with a synthetic `tool_result`, sends it through the same
// upstream HTTP path, and streams the response back using the provided
// emit callback.
//
// The first return value indicates whether *any* tokens were already
// streamed to the client. Callers use this to decide whether to emit the
// raw-results fallback on error.
func (i *kiroWebSearchInterceptor) requestFollowUpSummary(
	ctx context.Context,
	toolUseID, toolName, query string,
	resultsJSON []byte,
	emit func(*kiro.StreamEvent) error,
) (bool, error) {
	if i.anthReq == nil {
		return false, errors.New("no original request captured")
	}

	// Clone the original request and append two synthetic messages:
	//   - assistant with a tool_use block (the one the model just emitted)
	//   - user with a tool_result block (our MCP output)
	// Then re-issue it against Kiro. The model has full context and will
	// produce a natural-language summary.
	followUp := *i.anthReq
	followUp.Messages = append([]kiro.AnthropicMessage{}, i.anthReq.Messages...)

	assistantContent := []map[string]any{
		{
			"type":  "tool_use",
			"id":    toolUseID,
			"name":  toolName,
			"input": json.RawMessage(fmt.Sprintf(`{"query":%q}`, query)),
		},
	}
	assistantJSON, err := json.Marshal(assistantContent)
	if err != nil {
		return false, fmt.Errorf("marshal assistant content: %w", err)
	}
	userContent := []map[string]any{
		{
			"type":        "tool_result",
			"tool_use_id": toolUseID,
			"content":     string(resultsJSON),
		},
	}
	userJSON, err := json.Marshal(userContent)
	if err != nil {
		return false, fmt.Errorf("marshal tool result: %w", err)
	}

	followUp.Messages = append(followUp.Messages,
		kiro.AnthropicMessage{Role: "assistant", Content: assistantJSON},
		kiro.AnthropicMessage{Role: "user", Content: userJSON},
	)

	payload, err := kiro.BuildKiroPayload(&followUp, i.buildOpts)
	if err != nil {
		return false, fmt.Errorf("build follow-up payload: %w", err)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal follow-up payload: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, i.endpoint, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("build follow-up http request: %w", err)
	}
	httpReq.Header = i.headers.Clone()

	resp, err := i.client.Do(httpReq)
	if err != nil {
		return false, fmt.Errorf("follow-up http do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return false, fmt.Errorf("follow-up status %d: %s", resp.StatusCode, string(raw))
	}

	// Stream the follow-up response. We use a nil interceptor because we
	// don't want to recurse into another web_search turn if the model for
	// some reason decides to call the tool again.
	wroteAny := false
	reader := kiro.NewEventStreamReader(resp.Body)
	for {
		if ctx.Err() != nil {
			return wroteAny, ctx.Err()
		}
		msg, rerr := reader.Next()
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return wroteAny, nil
			}
			return wroteAny, rerr
		}
		ev, perr := kiro.ParseEventStreamFrame(msg)
		if perr != nil {
			return wroteAny, perr
		}
		if ev == nil {
			continue
		}
		// Surface only content and usage. tool_use from this second turn
		// would be unexpected; if it happens we skip to avoid confusing
		// the downstream encoder.
		switch ev.Kind {
		case "content":
			wroteAny = true
			if err := emit(ev); err != nil {
				return wroteAny, err
			}
		case "usage", "metering", "context_usage":
			// Pass through for accounting so billing/context widgets
			// work even for search turns.
			if err := emit(ev); err != nil {
				return wroteAny, err
			}
		}
	}
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
