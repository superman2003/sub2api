package kiro

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEncoder_ThinkingThenText covers the happy path: a thinking block
// followed by a regular text block in the same message. Every SSE
// content_block_start must be paired with a content_block_stop, and
// indices must strictly increase by one.
func TestEncoder_ThinkingThenText(t *testing.T) {
	var out bytes.Buffer
	enc := NewAnthropicSSEEncoder(&out, nil, "claude-opus-4-7")
	require.NoError(t, enc.Emit(&StreamEvent{Kind: "thinking", Text: "deliberating"}))
	require.NoError(t, enc.Emit(&StreamEvent{Kind: "content", Text: "Answer."}))
	require.NoError(t, enc.Finish("end_turn"))

	sse := out.String()

	// Two blocks: thinking (idx 0) + text (idx 1). Both must be fully
	// framed with start and stop.
	require.Contains(t, sse, `"content_block_start"`)
	require.Contains(t, sse, `"type":"thinking"`)
	require.Contains(t, sse, `"thinking_delta"`)
	require.Contains(t, sse, `"text_delta"`)
	require.Contains(t, sse, `Answer.`)

	// Count block starts and stops; must match.
	starts := strings.Count(sse, `"type":"content_block_start"`)
	stops := strings.Count(sse, `"type":"content_block_stop"`)
	require.Equal(t, starts, stops, "content_block_start count must equal content_block_stop count\nSSE:\n%s", sse)
	require.Equal(t, 2, starts, "expected exactly 2 blocks, got:\n%s", sse)

	// message_delta / message_stop present exactly once.
	require.Equal(t, 1, strings.Count(sse, `"type":"message_delta"`))
	require.Equal(t, 1, strings.Count(sse, `"type":"message_stop"`))
}

// TestEncoder_ThinkingOnlyThenFinish — only a thinking block, no text.
// Finish must still close the thinking block cleanly.
func TestEncoder_ThinkingOnlyThenFinish(t *testing.T) {
	var out bytes.Buffer
	enc := NewAnthropicSSEEncoder(&out, nil, "claude-opus-4-7")
	require.NoError(t, enc.Emit(&StreamEvent{Kind: "thinking", Text: "just planning"}))
	require.NoError(t, enc.Finish("end_turn"))
	sse := out.String()
	require.Equal(t, strings.Count(sse, `"type":"content_block_start"`), strings.Count(sse, `"type":"content_block_stop"`))
	require.Contains(t, sse, `"thinking":"just planning"`)
}

// TestEncoder_ThinkingTextThinkingText — interleaved kinds, each switch
// must close the previous block and open a new one.
func TestEncoder_ThinkingTextThinkingText(t *testing.T) {
	var out bytes.Buffer
	enc := NewAnthropicSSEEncoder(&out, nil, "claude-opus-4-7")
	require.NoError(t, enc.Emit(&StreamEvent{Kind: "thinking", Text: "t1"}))
	require.NoError(t, enc.Emit(&StreamEvent{Kind: "content", Text: "c1"}))
	require.NoError(t, enc.Emit(&StreamEvent{Kind: "thinking", Text: "t2"}))
	require.NoError(t, enc.Emit(&StreamEvent{Kind: "content", Text: "c2"}))
	require.NoError(t, enc.Finish("end_turn"))
	sse := out.String()
	require.Equal(t, 4, strings.Count(sse, `"type":"content_block_start"`))
	require.Equal(t, 4, strings.Count(sse, `"type":"content_block_stop"`))
}
