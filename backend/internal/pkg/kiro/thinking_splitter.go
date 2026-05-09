package kiro

import "strings"

// ThinkingSplitter converts a stream of raw assistant text — in which the
// model may emit <thinking>...</thinking> blocks — into two logical
// streams: thinking fragments and regular content fragments. It is
// resilient to the thinking tags being split across chunks (a common
// occurrence with streaming token-level output).
//
// Usage:
//
//	s := &ThinkingSplitter{}
//	for chunk := range stream {
//	    for _, ev := range s.Feed(chunk) {
//	        enc.Emit(ev)
//	    }
//	}
//	for _, ev := range s.Flush() { enc.Emit(ev) } // on EOF
//
// The splitter recognises the literal ASCII tags "<thinking>" and
// "</thinking>". Nested or malformed tags are treated as plain content.
type ThinkingSplitter struct {
	// inThinking tracks whether the current cursor is inside a thinking
	// block (between <thinking> and </thinking>).
	inThinking bool
	// pending buffers trailing characters that could be the start of a
	// tag boundary (e.g. we saw "<" but haven't seen enough bytes yet to
	// know if it's "<thinking>" or something unrelated). Flushed on next
	// Feed or on explicit Flush().
	pending string
}

const (
	openThinkingTag  = "<thinking>"
	closeThinkingTag = "</thinking>"
	// maxTagPrefix is the longest useful pending buffer: 11 chars covers
	// both "</thinking>" and "<thinking>". Anything longer can be safely
	// flushed as plain content.
	maxTagPrefix = len(closeThinkingTag)
)

// Feed consumes a single chunk of assistant text and returns any emit-ready
// StreamEvents. Either kind may be repeated, interleaved, or absent.
func (s *ThinkingSplitter) Feed(chunk string) []*StreamEvent {
	if chunk == "" {
		return nil
	}
	buf := s.pending + chunk
	s.pending = ""
	return s.consume(buf, false)
}

// Flush should be called when the upstream stream ends. It releases any
// buffered pending bytes as content/thinking events depending on the
// current mode.
func (s *ThinkingSplitter) Flush() []*StreamEvent {
	if s.pending == "" {
		return nil
	}
	out := s.consume(s.pending, true)
	s.pending = ""
	return out
}

// consume is the internal loop that repeatedly scans for the next
// state-changing tag. When isFinal is true we never leave bytes in
// pending — everything gets flushed as-is.
func (s *ThinkingSplitter) consume(buf string, isFinal bool) []*StreamEvent {
	out := make([]*StreamEvent, 0, 2)
	for len(buf) > 0 {
		if s.inThinking {
			// Look for closing tag.
			idx := strings.Index(buf, closeThinkingTag)
			if idx < 0 {
				// No closing tag in buffer.
				if isFinal {
					out = appendThinking(out, buf)
					return out
				}
				// Keep a safe tail so we don't split "</thinking>" in half.
				safe, tail := splitOnTagBoundary(buf, closeThinkingTag)
				if safe != "" {
					out = appendThinking(out, safe)
				}
				s.pending = tail
				return out
			}
			if idx > 0 {
				out = appendThinking(out, buf[:idx])
			}
			buf = buf[idx+len(closeThinkingTag):]
			s.inThinking = false
			continue
		}
		// In content mode: look for opening tag.
		idx := strings.Index(buf, openThinkingTag)
		if idx < 0 {
			if isFinal {
				out = appendContent(out, buf)
				return out
			}
			safe, tail := splitOnTagBoundary(buf, openThinkingTag)
			if safe != "" {
				out = appendContent(out, safe)
			}
			s.pending = tail
			return out
		}
		if idx > 0 {
			out = appendContent(out, buf[:idx])
		}
		buf = buf[idx+len(openThinkingTag):]
		s.inThinking = true
	}
	return out
}

// splitOnTagBoundary returns (safeToEmit, mustKeepAsPending). The tail
// portion is kept small — at most len(tag)-1 bytes — and is kept only
// when the trailing bytes could plausibly be the start of tag.
func splitOnTagBoundary(buf, tag string) (string, string) {
	maxKeep := len(tag) - 1
	if maxKeep > maxTagPrefix {
		maxKeep = maxTagPrefix
	}
	if maxKeep <= 0 || len(buf) == 0 {
		return buf, ""
	}
	// Walk backwards from the end; keep the shortest suffix that could
	// be the prefix of tag. For "<think" we'd keep "<think"; for a plain
	// byte we keep nothing.
	for keep := maxKeep; keep > 0; keep-- {
		if keep > len(buf) {
			continue
		}
		suffix := buf[len(buf)-keep:]
		if strings.HasPrefix(tag, suffix) {
			return buf[:len(buf)-keep], suffix
		}
	}
	return buf, ""
}

func appendThinking(out []*StreamEvent, s string) []*StreamEvent {
	if s == "" {
		return out
	}
	// Coalesce with the previous thinking event if possible to keep SSE
	// event counts sensible.
	if n := len(out); n > 0 && out[n-1].Kind == "thinking" {
		out[n-1].Text += s
		return out
	}
	return append(out, &StreamEvent{Kind: "thinking", Text: s})
}

func appendContent(out []*StreamEvent, s string) []*StreamEvent {
	if s == "" {
		return out
	}
	if n := len(out); n > 0 && out[n-1].Kind == "content" {
		out[n-1].Text += s
		return out
	}
	return append(out, &StreamEvent{Kind: "content", Text: s})
}
