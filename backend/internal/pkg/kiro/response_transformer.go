package kiro

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
)

// AssistantEventPayload represents the JSON body of an "assistantResponseEvent"
// frame emitted by /generateAssistantResponse.
type AssistantEventPayload struct {
	Content string `json:"content,omitempty"`
}

// ToolUseEventPayload is emitted when the model decides to call a tool.
// Input 字段在 Kiro 上游可能是：
//   - string（partial JSON 片段，多帧追加后拼成完整 JSON 对象）
//   - object（完整的参数对象，一帧到位）
// 参考 kiro-gateway 开源实现：两种形式都要支持。
type ToolUseEventPayload struct {
	ToolUseID string `json:"toolUseId,omitempty"`
	Name      string `json:"name,omitempty"`
	Input     any    `json:"input,omitempty"`
	Stop      bool   `json:"stop,omitempty"`
}

// toolInputAsPartialJSON 把 toolUseEvent.input 统一转成客户端期望的
// partial_json 片段形式（纯 JSON 文本，不带外层引号）。
//
//   - 空值 → 返回 ""
//   - string（已是 partial JSON，例如 `{"query": "hi`）→ 原样返回
//   - object / array / number / bool → json.Marshal 后返回
func toolInputAsPartialJSON(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// UsageEventPayload carries Kiro-side token accounting.
type UsageEventPayload struct {
	InputTokens     int64 `json:"inputTokens,omitempty"`
	OutputTokens    int64 `json:"outputTokens,omitempty"`
	CacheReadTokens int64 `json:"cacheReadInputTokens,omitempty"`
}

// MeteringEventPayload carries the credit-level billing signal that Kiro
// emits at the end of every assistant turn. Example frame:
//
//	{"unit":"credit","unitPlural":"credits","usage":0.01077093}
type MeteringEventPayload struct {
	Unit        string  `json:"unit,omitempty"`
	UnitPlural  string  `json:"unitPlural,omitempty"`
	Usage       float64 `json:"usage,omitempty"`
	ServiceTier string  `json:"serviceTier,omitempty"`
}

// ContextUsageEventPayload reports how much of the model's context window
// has been consumed so far (percentage).
type ContextUsageEventPayload struct {
	Percentage float64 `json:"contextUsagePercentage,omitempty"`
}

// StreamEvent is the canonical shape emitted by our EventStream -> internal
// event pipeline. The handler layer converts it to Anthropic SSE framing.
type StreamEvent struct {
	// Kind is one of: content, thinking, tool_use_start, tool_use_delta,
	// tool_use_stop, usage, metering, context_usage, error, done
	Kind string
	// Text for content events; Thinking payload for thinking events.
	Text string
	// Tool use fields
	ToolName   string
	ToolUseID  string
	ToolInput  string // partial JSON fragment emitted together with tool_use_start
	ToolDelta  string // partial JSON for tool_use_delta
	StopReason string
	// Usage stats for usage event
	Usage UsageEventPayload
	// Metering (credit-based billing signal)
	Metering MeteringEventPayload
	// ContextUsagePct for context_usage event
	ContextUsagePct float64
	// Error details for error event
	ErrorMessage string
	ErrorType    string
}

// ParseEventStreamFrame attempts to read one logical StreamEvent from a
// decoded EventStream message. It returns (nil, nil) if the frame carried
// nothing interesting (e.g. unknown event type we want to swallow).
func ParseEventStreamFrame(msg *EventStreamMessage) (*StreamEvent, error) {
	if msg == nil {
		return nil, nil
	}
	if msg.MessageType() == "exception" || msg.MessageType() == "error" {
		errMsg := msg.StringValue(":exception-type")
		body := string(msg.Payload)
		if decoded, err := DecodeCBOR(msg.Payload); err == nil {
			if m, ok := decoded.(map[string]any); ok {
				if s := AsString(m, "message"); s != "" {
					body = s
				}
			}
		}
		return &StreamEvent{
			Kind:         "error",
			ErrorType:    errMsg,
			ErrorMessage: body,
		}, nil
	}

	switch msg.EventType() {
	case "assistantResponseEvent":
		var p AssistantEventPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return nil, fmt.Errorf("assistantResponseEvent json: %w", err)
		}
		if p.Content == "" {
			return nil, nil
		}
		return &StreamEvent{Kind: "content", Text: p.Content}, nil

	case "toolUseEvent":
		var p ToolUseEventPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return nil, fmt.Errorf("toolUseEvent json: %w", err)
		}
		if p.Stop {
			return &StreamEvent{Kind: "tool_use_stop", ToolUseID: p.ToolUseID}, nil
		}
		inputFragment := toolInputAsPartialJSON(p.Input)
		if p.Name != "" {
			return &StreamEvent{
				Kind:      "tool_use_start",
				ToolName:  p.Name,
				ToolUseID: p.ToolUseID,
				ToolInput: inputFragment,
			}, nil
		}
		if inputFragment != "" {
			return &StreamEvent{Kind: "tool_use_delta", ToolUseID: p.ToolUseID, ToolDelta: inputFragment}, nil
		}
		return nil, nil

	case "messageMetadataEvent", "followupPromptEvent", "codeReferenceEvent", "supplementaryWebLinksEvent":
		// messageMetadataEvent 在部分 Kiro 响应里携带 input/output token 计数，
		// 尝试按 usage 负载解析；失败或没有字段时直接忽略该帧。
		if msg.EventType() == "messageMetadataEvent" && len(msg.Payload) > 0 {
			var p UsageEventPayload
			if err := json.Unmarshal(msg.Payload, &p); err == nil {
				if p.InputTokens > 0 || p.OutputTokens > 0 || p.CacheReadTokens > 0 {
					return &StreamEvent{Kind: "usage", Usage: p}, nil
				}
			}
		}
		return nil, nil

	case "usageEvent":
		var p UsageEventPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return nil, fmt.Errorf("usageEvent json: %w", err)
		}
		return &StreamEvent{Kind: "usage", Usage: p}, nil

	case "meteringEvent":
		var p MeteringEventPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return nil, fmt.Errorf("meteringEvent json: %w", err)
		}
		return &StreamEvent{Kind: "metering", Metering: p}, nil

	case "contextUsageEvent":
		var p ContextUsageEventPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return nil, fmt.Errorf("contextUsageEvent json: %w", err)
		}
		return &StreamEvent{Kind: "context_usage", ContextUsagePct: p.Percentage}, nil
	}
	return nil, nil
}

// AnthropicSSEEncoder converts a stream of StreamEvents into the Anthropic
// /v1/messages Server-Sent Events wire format:
//
//	event: message_start\ndata: {...}\n\n
//	event: content_block_start\ndata: {...}\n\n
//	event: content_block_delta\ndata: {...}\n\n
//	event: content_block_stop\ndata: {...}\n\n
//	event: message_delta\ndata: {...}\n\n
//	event: message_stop\ndata: {...}\n\n
type AnthropicSSEEncoder struct {
	w             io.Writer
	flush         func()
	model         string
	messageID     string
	textBlockOpen bool
	// thinkingBlockOpen tracks whether a thinking content_block is
	// currently active. Mutually exclusive with textBlockOpen — switching
	// between thinking and text closes the other block first.
	thinkingBlockOpen bool
	// thinkingBuf accumulates the raw text of the current thinking block
	// so closeCurrentNonToolBlock can emit a stable synthetic
	// signature_delta. Claude Code's UI silently hides thinking blocks
	// that arrive without a signature, so we derive one deterministically
	// from the thinking text itself.
	thinkingBuf       strings.Builder
	blockIndex        int
	toolBlocks        map[string]int // toolUseID -> block index
	inputTokens       int64
	outputTokens      int64
	// Kiro-specific aggregates captured during the stream.
	meteringCredit  float64
	meteringUnit    string
	contextUsagePct float64
	started         bool
}

// NewAnthropicSSEEncoder builds an encoder. The flusher is called after each
// event so clients receive chunks in real time. inputTokensHint is an
// optional pre-computed estimate of the prompt token count; when > 0 it is
// surfaced in the message_start usage block so downstream consumers (e.g. a
// second sub2api instance acting as a relay) can record it without waiting
// for the message_delta at the end of the stream.
func NewAnthropicSSEEncoder(w io.Writer, flush func(), model string) *AnthropicSSEEncoder {
	return &AnthropicSSEEncoder{
		w:          w,
		flush:      flush,
		model:      model,
		messageID:  "msg_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:24],
		toolBlocks: make(map[string]int),
	}
}

// SetInputTokensHint pre-populates the input token count that will be
// emitted in message_start. Call this before the first Emit/Start so the
// hint is visible to relay consumers from the very first SSE frame.
func (e *AnthropicSSEEncoder) SetInputTokensHint(n int64) {
	if n > 0 && e.inputTokens == 0 {
		e.inputTokens = n
	}
}

func (e *AnthropicSSEEncoder) write(event string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(e.w, "event: %s\ndata: %s\n\n", event, body); err != nil {
		return err
	}
	if e.flush != nil {
		e.flush()
	}
	return nil
}

// Start emits the message_start event.
func (e *AnthropicSSEEncoder) Start() error {
	if e.started {
		return nil
	}
	e.started = true
	msg := map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            e.messageID,
			"type":          "message",
			"role":          "assistant",
			"model":         e.model,
			"content":       []any{},
			"stop_reason":   nil,
			"stop_sequence": nil,
			"usage": map[string]any{
				"input_tokens":  e.inputTokens,
				"output_tokens": 0,
			},
		},
	}
	return e.write("message_start", msg)
}

// Emit forwards a single internal event.
func (e *AnthropicSSEEncoder) Emit(ev *StreamEvent) error {
	if err := e.Start(); err != nil {
		return err
	}
	if ev == nil {
		return nil
	}
	switch ev.Kind {
	case "content":
		if e.thinkingBlockOpen {
			if err := e.closeCurrentNonToolBlock(); err != nil {
				return err
			}
		}
		if !e.textBlockOpen {
			e.textBlockOpen = true
			if err := e.write("content_block_start", map[string]any{
				"type":  "content_block_start",
				"index": e.blockIndex,
				"content_block": map[string]any{
					"type": "text",
					"text": "",
				},
			}); err != nil {
				return err
			}
		}
		if err := e.write("content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": e.blockIndex,
			"delta": map[string]any{
				"type": "text_delta",
				"text": ev.Text,
			},
		}); err != nil {
			return err
		}
		e.outputTokens += int64(len(ev.Text) / 4) // rough
		return nil

	case "thinking":
		if e.textBlockOpen {
			if err := e.closeCurrentNonToolBlock(); err != nil {
				return err
			}
		}
		if !e.thinkingBlockOpen {
			e.thinkingBlockOpen = true
			if err := e.write("content_block_start", map[string]any{
				"type":  "content_block_start",
				"index": e.blockIndex,
				"content_block": map[string]any{
					"type":      "thinking",
					"thinking":  "",
					"signature": "",
				},
			}); err != nil {
				return err
			}
		}
		if err := e.write("content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": e.blockIndex,
			"delta": map[string]any{
				"type":     "thinking_delta",
				"thinking": ev.Text,
			},
		}); err != nil {
			return err
		}
		// Accumulate thinking text so Finish-time / block-close code can
		// produce a stable synthetic signature. Kiro has no native
		// signed-thinking output; without some signature at all the
		// Claude Code UI silently hides the thinking block.
		e.thinkingBuf.WriteString(ev.Text)
		e.outputTokens += int64(len(ev.Text) / 4)
		return nil

	case "tool_use_start":
		// Close any open text or thinking block before starting a tool block.
		if e.textBlockOpen || e.thinkingBlockOpen {
			if err := e.closeCurrentNonToolBlock(); err != nil {
				return err
			}
		}
		idx := e.blockIndex
		e.toolBlocks[ev.ToolUseID] = idx
		block := map[string]any{
			"type":  "tool_use",
			"id":    normalizeToolUseID(ev.ToolUseID),
			"name":  ev.ToolName,
			"input": map[string]any{},
		}
		if err := e.write("content_block_start", map[string]any{
			"type":          "content_block_start",
			"index":         idx,
			"content_block": block,
		}); err != nil {
			return err
		}
		if len(ev.ToolInput) > 0 {
			if err := e.write("content_block_delta", map[string]any{
				"type":  "content_block_delta",
				"index": idx,
				"delta": map[string]any{
					"type":         "input_json_delta",
					"partial_json": ev.ToolInput,
				},
			}); err != nil {
				return err
			}
		}
		return nil

	case "tool_use_delta":
		idx, ok := e.toolBlocks[ev.ToolUseID]
		if !ok {
			idx = e.blockIndex
		}
		return e.write("content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": idx,
			"delta": map[string]any{
				"type":         "input_json_delta",
				"partial_json": ev.ToolDelta,
			},
		})

	case "tool_use_stop":
		idx, ok := e.toolBlocks[ev.ToolUseID]
		if !ok {
			idx = e.blockIndex
		}
		delete(e.toolBlocks, ev.ToolUseID)
		err := e.write("content_block_stop", map[string]any{
			"type":  "content_block_stop",
			"index": idx,
		})
		e.blockIndex++
		return err

	case "usage":
		if ev.Usage.InputTokens > 0 {
			e.inputTokens = ev.Usage.InputTokens
		}
		if ev.Usage.OutputTokens > 0 {
			e.outputTokens = ev.Usage.OutputTokens
		}
		return nil

	case "metering":
		// Kiro emits a terminal `meteringEvent` carrying the definitive
		// credit usage for this turn. We don't surface it in the SSE
		// framing (clients don't know about it) but stash the numbers so
		// the service layer can log/bill them later.
		if ev.Metering.Usage > 0 {
			e.meteringCredit += ev.Metering.Usage
		}
		if ev.Metering.Unit != "" {
			e.meteringUnit = ev.Metering.Unit
		}
		return nil

	case "context_usage":
		if ev.ContextUsagePct > 0 {
			e.contextUsagePct = ev.ContextUsagePct
		}
		return nil

	case "error":
		return e.write("error", map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    firstNonEmpty(ev.ErrorType, "api_error"),
				"message": firstNonEmpty(ev.ErrorMessage, "upstream error"),
			},
		})
	}
	return nil
}

// closeCurrentNonToolBlock finalises whichever of text/thinking block is
// currently open and advances the block index. No-op when nothing is
// open. Tool blocks have their own lifecycle managed via toolBlocks and
// are untouched here.
func (e *AnthropicSSEEncoder) closeCurrentNonToolBlock() error {
	if !e.textBlockOpen && !e.thinkingBlockOpen {
		return nil
	}
	// Emit a synthetic signature before closing a thinking block. Claude
	// Code's UI hides thinking blocks that finish without a signature,
	// so we produce a stable, self-derived value. Genuine Anthropic
	// signatures are server-signed base64 blobs; our synthetic one is
	// clearly marked so downstream consumers can distinguish them.
	if e.thinkingBlockOpen {
		sig := syntheticThinkingSignature(e.thinkingBuf.String())
		if err := e.write("content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": e.blockIndex,
			"delta": map[string]any{
				"type":      "signature_delta",
				"signature": sig,
			},
		}); err != nil {
			return err
		}
		e.thinkingBuf.Reset()
	}
	if err := e.write("content_block_stop", map[string]any{
		"type":  "content_block_stop",
		"index": e.blockIndex,
	}); err != nil {
		return err
	}
	e.textBlockOpen = false
	e.thinkingBlockOpen = false
	e.blockIndex++
	return nil
}

// syntheticThinkingSignature derives a stable identifier for a thinking
// block. Anthropic's native signatures are server-signed; we can't forge
// those, but any non-empty signature is enough to keep Claude Code's UI
// from hiding the block. The prefix makes it clear the signature is
// gateway-synthesised and not from Anthropic.
func syntheticThinkingSignature(thinking string) string {
	sum := sha256.Sum256([]byte(thinking))
	return "sub2api-kiro-emulated:" + hex.EncodeToString(sum[:16])
}

// Finish closes any open blocks and emits message_delta/message_stop.
func (e *AnthropicSSEEncoder) Finish(stopReason string) error {
	if err := e.Start(); err != nil {
		return err
	}
	if e.textBlockOpen || e.thinkingBlockOpen {
		_ = e.closeCurrentNonToolBlock()
	}
	for id, idx := range e.toolBlocks {
		_ = e.write("content_block_stop", map[string]any{
			"type":  "content_block_stop",
			"index": idx,
		})
		delete(e.toolBlocks, id)
		e.blockIndex++
	}
	if stopReason == "" {
		stopReason = "end_turn"
	}
	_ = e.write("message_delta", map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason":   stopReason,
			"stop_sequence": nil,
		},
		"usage": map[string]any{
			"input_tokens":  e.inputTokens,
			"output_tokens": e.outputTokens,
		},
	})
	return e.write("message_stop", map[string]any{
		"type": "message_stop",
	})
}

// InputTokens returns the accumulated input token count (if upstream reported).
func (e *AnthropicSSEEncoder) InputTokens() int64 { return e.inputTokens }

// OutputTokens returns the accumulated output token count (estimated if
// upstream did not report).
func (e *AnthropicSSEEncoder) OutputTokens() int64 { return e.outputTokens }

// MeteringCredit returns the sum of all `usage` values seen in meteringEvent
// frames. Unit is whatever Kiro reports in MeteringUnit() (usually "credit").
func (e *AnthropicSSEEncoder) MeteringCredit() float64 { return e.meteringCredit }

// MeteringUnit returns the last unit string observed (typically "credit").
func (e *AnthropicSSEEncoder) MeteringUnit() string { return e.meteringUnit }

// ContextUsagePct returns the highest context-usage percentage reported by
// the upstream (useful for UI dashboards).
func (e *AnthropicSSEEncoder) ContextUsagePct() float64 { return e.contextUsagePct }

// MessageID returns the stable message id used in the SSE stream.
func (e *AnthropicSSEEncoder) MessageID() string { return e.messageID }

// ToolCallInterceptor lets callers intercept a complete tool_use lifecycle
// (start → deltas → stop) and either let it pass through to the client or
// replace it with an arbitrary synthetic event stream. Typical use: fulfil
// Kiro's web_search tool_use server-side via /mcp so the client never sees
// the tool call — it sees the search result as regular assistant text.
//
// The interceptor is consulted exactly once per tool_use lifecycle. When
// OnToolStart returns handled=true, the gateway buffers the whole
// lifecycle (start + any deltas + stop) until stop arrives, then calls
// OnToolStop. The interceptor uses the provided emit callback to stream
// any number of replacement events; returning nil without emitting drops
// the tool call entirely.
type ToolCallInterceptor interface {
	// OnToolStart is called when a tool_use_start event is observed. It
	// should quickly decide whether to intercept (buffer) this tool call.
	OnToolStart(ctx context.Context, start *StreamEvent) (handled bool)
	// OnToolStop is called once the matching tool_use_stop arrives, with
	// the full concatenated input string. Events are sent to the client
	// via emit; returning an error surfaces it as a stream-level error.
	OnToolStop(ctx context.Context, toolUseID, toolName, input string, emit func(*StreamEvent) error) error
}

// DriveEventStreamToAnthropic reads EventStream frames from r, converts them
// through ParseEventStreamFrame, and drives the encoder until the stream ends.
//
// Returns accumulated text (for non-stream use cases) and any error other than
// io.EOF.
func DriveEventStreamToAnthropic(ctx context.Context, r io.Reader, enc *AnthropicSSEEncoder) (string, error) {
	return DriveEventStreamToAnthropicWithInterceptor(ctx, r, enc, nil)
}

// DriveEventStreamToAnthropicWithInterceptor is the same as
// DriveEventStreamToAnthropic but lets the caller plug in a tool-call
// interceptor. Pass nil to get the default pass-through behaviour.
func DriveEventStreamToAnthropicWithInterceptor(
	ctx context.Context,
	r io.Reader,
	enc *AnthropicSSEEncoder,
	interceptor ToolCallInterceptor,
) (string, error) {
	reader := NewEventStreamReader(r)
	var textBuf bytes.Buffer

	// Per-intercepted-tool buffer: tool_use_start + its deltas are held
	// here until tool_use_stop is observed, at which point the interceptor
	// decides what to emit.
	type pendingTool struct {
		name   string
		input  strings.Builder
		active bool
	}
	pending := make(map[string]*pendingTool)

	// Kiro's wire format emits every toolUseEvent frame with the same
	// `name` field populated, even though only the first one is a real
	// "start" and subsequent ones are partial-input continuations
	// (the reference kiro-gateway Python implementation handles this the
	// same way in parsers.py). We track which toolUseIds we've already
	// started so we can demote later frames from tool_use_start to
	// tool_use_delta before downstream logic runs.
	seenStarts := make(map[string]struct{})

	// thinkSplitter converts raw assistant text — which may include
	// <thinking>...</thinking> blocks when we injected the
	// thinking_mode=enabled prompt — into a mix of content and thinking
	// events. Stateful across chunks to handle tags split at arbitrary
	// boundaries. The byte cap defensively protects against models that
	// ignore the prompt-level budget and would otherwise eat the whole
	// output token ceiling on reasoning alone.
	thinkSplitter := &ThinkingSplitter{MaxThinkingBytes: defaultThinkingByteCap}

	emit := func(ev *StreamEvent) error {
		if ev.Kind == "content" {
			textBuf.WriteString(ev.Text)
		}
		return enc.Emit(ev)
	}

	// emitParsed runs a (possibly content) event through the thinking
	// splitter and forwards whatever it produces. Non-content events pass
	// straight through.
	emitParsed := func(ev *StreamEvent) error {
		if ev.Kind != "content" {
			return emit(ev)
		}
		for _, out := range thinkSplitter.Feed(ev.Text) {
			if err := emit(out); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		if ctx.Err() != nil {
			return textBuf.String(), ctx.Err()
		}
		msg, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Flush any trailing bytes still held by the thinking
				// splitter so unterminated chunks reach the client.
				for _, out := range thinkSplitter.Flush() {
					if eerr := emit(out); eerr != nil {
						return textBuf.String(), eerr
					}
				}
				// Flush any intercepted tool_use lifecycles that did not
				// receive an explicit stop frame. Kiro's streaming format
				// does not always emit a trailing stop so we finalise
				// here to avoid silent truncation.
				if interceptor != nil {
					for id, p := range pending {
						if !p.active {
							continue
						}
						delete(pending, id)
						if ierr := interceptor.OnToolStop(ctx, id, p.name, p.input.String(), emit); ierr != nil {
							_ = emit(&StreamEvent{
								Kind:         "error",
								ErrorType:    "tool_interceptor_error",
								ErrorMessage: ierr.Error(),
							})
						}
					}
				}
				return textBuf.String(), nil
			}
			return textBuf.String(), err
		}
		ev, perr := ParseEventStreamFrame(msg)
		if perr != nil {
			return textBuf.String(), perr
		}
		if ev == nil {
			continue
		}

		// Normalise Kiro's repeated tool_use_start frames: keep the first
		// one (real start), convert the rest into tool_use_delta carrying
		// the same input fragment, so downstream code handles them as
		// continuations.
		if ev.Kind == "tool_use_start" {
			if _, already := seenStarts[ev.ToolUseID]; already {
				ev = &StreamEvent{
					Kind:      "tool_use_delta",
					ToolUseID: ev.ToolUseID,
					ToolDelta: ev.ToolInput,
				}
			} else {
				seenStarts[ev.ToolUseID] = struct{}{}
			}
		} else if ev.Kind == "tool_use_stop" {
			// Once the tool finishes we no longer need to remember it.
			delete(seenStarts, ev.ToolUseID)
		}

		// Interceptor hook: buffer tool_use events of interest until stop.
		if interceptor != nil {
			switch ev.Kind {
			case "tool_use_start":
				if interceptor.OnToolStart(ctx, ev) {
					p := &pendingTool{name: ev.ToolName, active: true}
					if ev.ToolInput != "" {
						p.input.WriteString(ev.ToolInput)
					}
					pending[ev.ToolUseID] = p
					continue
				}
			case "tool_use_delta":
				if p, ok := pending[ev.ToolUseID]; ok && p.active {
					p.input.WriteString(ev.ToolDelta)
					continue
				}
			case "tool_use_stop":
				if p, ok := pending[ev.ToolUseID]; ok && p.active {
					delete(pending, ev.ToolUseID)
					if ierr := interceptor.OnToolStop(ctx, ev.ToolUseID, p.name, p.input.String(), emit); ierr != nil {
						if eerr := emit(&StreamEvent{
							Kind:         "error",
							ErrorType:    "tool_interceptor_error",
							ErrorMessage: ierr.Error(),
						}); eerr != nil {
							return textBuf.String(), eerr
						}
					}
					continue
				}
			}
		}

		if err := emitParsed(ev); err != nil {
			return textBuf.String(), err
		}
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// normalizeToolUseID converts Kiro's tool_use ID format ("tooluse_xxx") to
// the Anthropic format ("toolu_xxx") that Claude Code expects. Without this
// prefix, Claude Code treats the tool_use block as plain text instead of
// executing the tool.
func normalizeToolUseID(id string) string {
	if strings.HasPrefix(id, "tooluse_") {
		return "toolu_" + id[len("tooluse_"):]
	}
	if strings.HasPrefix(id, "toolu_") {
		return id // already correct
	}
	return "toolu_" + id
}
