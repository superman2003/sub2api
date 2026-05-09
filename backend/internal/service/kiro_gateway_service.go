package service

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/kiro"
	"github.com/Wei-Shaw/sub2api/internal/pkg/proxyurl"

	"github.com/gin-gonic/gin"
)

// kiroUpstreamEndpoint is the Amazon CodeWhisperer endpoint used by the Kiro
// IDE to stream Claude responses. The endpoint is region-qualified; sub2api
// currently pins it to us-east-1 to match the profileArn returned by
// ExchangeToken. If we ever surface regional accounts we can read the region
// off account.Credentials.
const kiroUpstreamEndpoint = "https://codewhisperer.us-east-1.amazonaws.com/generateAssistantResponse"

// kiroForwardTimeout bounds the total round-trip for a single Kiro request.
// Claude Sonnet 4.5 streaming responses complete in well under 2 minutes on
// average; we allow 3 minutes so slow tool-calling turns do not abort early.
const kiroForwardTimeout = 3 * time.Minute

// KiroGatewayService forwards Anthropic-style /v1/messages requests to Kiro's
// CodeWhisperer endpoint and translates the AWS EventStream response back into
// Anthropic SSE.
type KiroGatewayService struct {
	tokenProvider *KiroTokenProvider
	settings      *SettingService
	// httpClient is reused across requests; Kiro happily serves multiple
	// sequential streaming calls over the same HTTP/2 connection so a single
	// pool is fine.
	httpClient *http.Client
}

// NewKiroGatewayService constructs the service and its long-lived http client.
func NewKiroGatewayService(tokenProvider *KiroTokenProvider, settings *SettingService) *KiroGatewayService {
	return &KiroGatewayService{
		tokenProvider: tokenProvider,
		settings:      settings,
		httpClient: &http.Client{
			Timeout: 0, // set per-request via context
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          128,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
			},
		},
	}
}

// Forward is the entry point called from the gateway handler. It mirrors the
// contract of AntigravityGatewayService.Forward: parse the request, select
// payload fields, perform the streaming POST, and write an Anthropic-shaped
// response to the gin context.
func (s *KiroGatewayService) Forward(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	parsed *ParsedRequest,
) (*ForwardResult, error) {
	start := time.Now()
	if account == nil {
		return nil, errors.New("kiro forward: account is nil")
	}
	if parsed == nil {
		return nil, errors.New("kiro forward: parsed request is nil")
	}

	// Resolve token (refresh as needed).
	token, err := s.tokenProvider.GetAccessToken(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("kiro token: %w", err)
	}
	profileArn := s.tokenProvider.ProfileArn(account)
	if profileArn == "" {
		return nil, errors.New("kiro forward: account missing profile_arn")
	}

	// Convert Anthropic payload to Kiro conversationState.
	anthropicReq, err := parsedToKiroAnthropic(parsed)
	if err != nil {
		return nil, fmt.Errorf("kiro forward: %w", err)
	}
	mapping := kiroModelMappingForAccount(account)
	payload, err := kiro.BuildKiroPayload(anthropicReq, kiro.BuildOptions{
		ProfileArn:     profileArn,
		ModelMapping:   mapping,
		ConversationID: conversationIDFromContext(c),
	})
	if err != nil {
		return nil, fmt.Errorf("kiro forward: build payload: %w", err)
	}

	upstreamModel := kiro.ResolveModel(parsed.Model, mapping)

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("kiro forward: marshal payload: %w", err)
	}

	forwardCtx, cancel := context.WithTimeout(ctx, kiroForwardTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(forwardCtx, http.MethodPost, kiroUpstreamEndpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("kiro forward: build request: %w", err)
	}
	httpReq.Header = kiroUpstreamHeaders(token)

	// Route through the account's outbound proxy (if configured).
	client := s.clientForAccount(account)
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("kiro forward: http do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return nil, &UpstreamFailoverError{
			StatusCode:      resp.StatusCode,
			ResponseBody:    raw,
			ResponseHeaders: resp.Header,
		}
	}

	requestID := resp.Header.Get("X-Amzn-Requestid")
	conversationID := resp.Header.Get("X-Amzn-Codewhisperer-Conversation-Id")

	// Prepare response for Anthropic SSE streaming.
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	c.Writer.WriteHeader(http.StatusOK)

	flusher, _ := c.Writer.(http.Flusher)
	flush := func() {
		if flusher != nil {
			flusher.Flush()
		}
	}

	encoder := kiro.NewAnthropicSSEEncoder(c.Writer, flush, parsed.Model)
	text, err := kiro.DriveEventStreamToAnthropic(forwardCtx, resp.Body, encoder)
	if err != nil && !errors.Is(err, context.Canceled) {
		// Stream already started; emit an error event so the client can fail gracefully.
		_ = encoder.Emit(&kiro.StreamEvent{Kind: "error", ErrorType: "upstream_error", ErrorMessage: err.Error()})
	}
	stopReason := "end_turn"
	if err != nil {
		stopReason = "error"
	}
	_ = encoder.Finish(stopReason)
	flush()

	// Compute coarse-grained usage; detailed metering comes from meteringEvent
	// frames emitted by Kiro. For billing purposes we surface input/output
	// tokens in the ForwardResult (the handler layer persists the usage log).
	duration := time.Since(start)
	_ = text // currently unused; could be logged for debug

	// Kiro 上游不返回 input_tokens（meteringEvent 只给 credit），这里用请求体
	// 字符数近似估算：Claude tokenizer 平均 ~3.5 字符/token，乘以 1.15 校正。
	// 真实计费以 credit 为准，此数字仅用于 UI 与统计展示。
	inputTokens := int(encoder.InputTokens())
	if inputTokens == 0 {
		inputTokens = estimateKiroInputTokens(parsed)
	}

	return &ForwardResult{
		RequestID:     kiroFirstNonEmpty(requestID, conversationID),
		Model:         parsed.Model,
		UpstreamModel: upstreamModel,
		Stream:        parsed.Stream,
		Duration:      duration,
		Usage: ClaudeUsage{
			InputTokens:  inputTokens,
			OutputTokens: int(encoder.OutputTokens()),
		},
		KiroMeteringCredit:  encoder.MeteringCredit(),
		KiroContextUsagePct: encoder.ContextUsagePct(),
	}, nil
}

// estimateKiroInputTokens approximates the prompt token count for a Kiro
// request by summing the character length of the system prompt and every
// message. Conversion constant: 3.5 chars/token * 1.15 Claude correction
// => ~1 token per 3 characters. Used only when Kiro upstream omits usage.
func estimateKiroInputTokens(parsed *ParsedRequest) int {
	if parsed == nil {
		return 0
	}
	var chars int
	chars += countAnyCharacters(parsed.System)
	for _, m := range parsed.Messages {
		chars += countAnyCharacters(m)
	}
	if chars == 0 {
		return 0
	}
	return chars / 3
}

// countAnyCharacters best-effort counts textual characters inside an arbitrary
// JSON-decoded value (string, map, slice, nested). Numbers / bools ignored.
func countAnyCharacters(v any) int {
	switch x := v.(type) {
	case string:
		return len(x)
	case []any:
		n := 0
		for _, it := range x {
			n += countAnyCharacters(it)
		}
		return n
	case map[string]any:
		n := 0
		for _, vv := range x {
			n += countAnyCharacters(vv)
		}
		return n
	}
	return 0
}

// clientForAccount returns a client that honours the outbound proxy
// configured on the account. Falls back to the shared client when none.
func (s *KiroGatewayService) clientForAccount(account *Account) *http.Client {
	if account == nil || account.Proxy == nil {
		return s.httpClient
	}
	_, parsed, err := proxyurl.Parse(account.Proxy.URL())
	if err != nil || parsed == nil {
		return s.httpClient
	}
	transport := s.httpClient.Transport.(*http.Transport).Clone()
	transport.Proxy = http.ProxyURL(parsed)
	return &http.Client{Transport: transport, Timeout: 0}
}

// kiroUpstreamHeaders mimics the Kiro IDE bearer auth flow.
func kiroUpstreamHeaders(token string) http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Accept", "application/json")
	h.Set("Authorization", "Bearer "+token)
	h.Set("User-Agent", "aws-sdk-js/1.0.27 KiroIDE-0.7.45-sub2api")
	h.Set("x-amz-user-agent", "aws-sdk-js/1.0.27 KiroIDE-0.7.45-sub2api")
	h.Set("x-amzn-codewhisperer-optout", "true")
	h.Set("x-amzn-kiro-agent-mode", "vibe")
	return h
}

// parsedToKiroAnthropic converts the gateway's internal parsed request into
// the kiro.AnthropicRequest shape consumed by BuildKiroPayload.
func parsedToKiroAnthropic(p *ParsedRequest) (*kiro.AnthropicRequest, error) {
	if p == nil {
		return nil, errors.New("parsed request is nil")
	}
	var body map[string]any
	if err := json.Unmarshal(p.Body, &body); err != nil {
		return nil, fmt.Errorf("unmarshal parsed body: %w", err)
	}

	req := &kiro.AnthropicRequest{
		Model:  p.Model,
		Stream: p.Stream,
	}
	if raw, ok := body["system"]; ok {
		req.System = raw
	}
	if raw, ok := body["max_tokens"].(float64); ok {
		req.MaxTokens = int(raw)
	}
	if raw, ok := body["temperature"].(float64); ok {
		v := raw
		req.Temperature = &v
	}
	if raw, ok := body["top_p"].(float64); ok {
		v := raw
		req.TopP = &v
	}
	if raw, ok := body["messages"].([]any); ok {
		msgs := make([]kiro.AnthropicMessage, 0, len(raw))
		for _, it := range raw {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			role, _ := m["role"].(string)
			contentRaw, _ := json.Marshal(m["content"])
			msgs = append(msgs, kiro.AnthropicMessage{Role: role, Content: contentRaw})
		}
		req.Messages = msgs
	}
	if raw, ok := body["tools"].([]any); ok {
		tools := make([]kiro.AnthropicTool, 0, len(raw))
		for _, it := range raw {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			t := kiro.AnthropicTool{}
			if n, ok := m["name"].(string); ok {
				t.Name = n
			}
			if d, ok := m["description"].(string); ok {
				t.Description = d
			}
			if sch, ok := m["input_schema"]; ok {
				if schemaRaw, err := json.Marshal(sch); err == nil {
					t.InputSchema = schemaRaw
				}
			}
			tools = append(tools, t)
		}
		req.Tools = tools
	}
	return req, nil
}

// kiroModelMappingForAccount resolves the effective Anthropic -> Kiro model
// mapping, giving the per-account Extra["model_mapping"] precedence over the
// built-in default.
func kiroModelMappingForAccount(account *Account) map[string]string {
	if account == nil {
		return kiro.DefaultModelMapping
	}
	if raw, ok := account.Extra["model_mapping"].(map[string]any); ok && len(raw) > 0 {
		out := make(map[string]string, len(raw))
		for k, v := range raw {
			if s, ok := v.(string); ok && s != "" {
				out[k] = s
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return kiro.DefaultModelMapping
}

// conversationIDFromContext returns a stable conversation id if the handler
// layer already derived one (session hash + group), otherwise the empty
// string (BuildKiroPayload will generate a UUID).
func conversationIDFromContext(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if v, ok := c.Get("kiro.conversation_id"); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// ensure url is imported (silence tree-shakers during partial builds).
var _ = url.URL{}

func kiroFirstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}
