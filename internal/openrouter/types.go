package openrouter

import "encoding/json"

// ChatRequest is the request body for OpenRouter's chat-completions
// endpoint (POST {base}/v1/chat/completions). Field set mirrors the
// documented OpenRouter ChatRequest: model is a "vendor/model" slug;
// messages are OpenAI-style; models is OpenRouter's ordered model-fallback
// list. Unknown fields are preserved via Extras so the desktop can pass
// provider-specific parameters (provider preferences, session_id, …)
// without the gateway needing a release.
type ChatRequest struct {
	Model               string             `json:"model"`
	Messages            []Message          `json:"messages"`
	Stream              bool               `json:"stream,omitempty"`
	Models              []string           `json:"models,omitempty"` // ordered fallback models (after the primary)
	Temperature         *float64           `json:"temperature,omitempty"`
	TopP                *float64           `json:"top_p,omitempty"`
	MaxCompletionTokens *int64             `json:"max_completion_tokens,omitempty"`
	MaxTokens           *int64             `json:"max_tokens,omitempty"`
	Stop                json.RawMessage    `json:"stop,omitempty"` // string or []string
	Seed                *int64             `json:"seed,omitempty"`
	FrequencyPenalty    *float64           `json:"frequency_penalty,omitempty"`
	PresencePenalty     *float64           `json:"presence_penalty,omitempty"`
	LogitBias           map[string]float64 `json:"logit_bias,omitempty"`
	Logprobs            *bool              `json:"logprobs,omitempty"`
	TopLogprobs         *int               `json:"top_logprobs,omitempty"`
	ParallelToolCalls   *bool              `json:"parallel_tool_calls,omitempty"`
	Tools               []Tool             `json:"tools,omitempty"`
	ToolChoice          json.RawMessage    `json:"tool_choice,omitempty"` // "auto"|"none"|"required" or struct
	ResponseFormat      json.RawMessage    `json:"response_format,omitempty"`
	Reasoning           *Reasoning         `json:"reasoning,omitempty"`
	User                string             `json:"user,omitempty"`
	Metadata            map[string]any     `json:"metadata,omitempty"`

	// Extras preserves forward-compatible fields validated-but-forwarded.
	Extras map[string]json.RawMessage `json:"-"`
}

// MarshalJSON emits known fields plus Extras inline.
func (r *ChatRequest) MarshalJSON() ([]byte, error) {
	type alias ChatRequest // avoid recursion
	a := alias(*r)
	base, err := json.Marshal(a)
	if err != nil || len(r.Extras) == 0 {
		return base, err
	}
	// merge Extras into the object
	var m map[string]json.RawMessage
	if err := json.Unmarshal(base, &m); err != nil {
		return base, err
	}
	for k, v := range r.Extras {
		if _, exists := m[k]; !exists {
			m[k] = v
		}
	}
	return json.Marshal(m)
}

// Message is an OpenAI-style chat message. Content is RawMessage: a string
// or an array of typed content blocks, validated upstream, passed through
// without re-encoding to avoid unnecessary serialization.
type Message struct {
	Role       string            `json:"role"` // system|user|assistant|tool|developer
	Content    json.RawMessage   `json:"content,omitempty"`
	Name       string            `json:"name,omitempty"`
	ToolCallID string            `json:"tool_call_id,omitempty"`
	ToolCalls  []MessageToolCall `json:"tool_calls,omitempty"`
	Reasoning  string            `json:"reasoning,omitempty"`
	Refusal    string            `json:"refusal,omitempty"`
}

// MessageToolCall is a completed assistant tool call.
type MessageToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function FunctionCall `json:"function"`
}

// FunctionCall is the function invocation payload.
type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Tool is a tool definition (function or custom).
type Tool struct {
	Type     string       `json:"type"` // "function" | "custom"
	Function *FunctionDef `json:"function,omitempty"`
}

// FunctionDef is the tool function schema.
type FunctionDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	Strict      *bool           `json:"strict,omitempty"`
}

// Reasoning mirrors OpenRouter's reasoning control (subset we forward).
type Reasoning struct {
	Effort    string `json:"effort,omitempty"` // none|minimal|low|medium|high|xhigh
	MaxTokens *int64 `json:"max_tokens,omitempty"`
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

// ChatResponse is a non-stream completion result.
//
// A 200-OK body may still carry an error object (OpenRouter commits the
// response before the provider fails): IsError() detects that shape, and
// the client surfaces it instead of returning an empty completion.
type ChatResponse struct {
	ID       string       `json:"id"`
	Object   string       `json:"object"`
	Created  int64        `json:"created"`
	Model    string       `json:"model"`
	Provider string       `json:"provider,omitempty"`
	Choices  []RespChoice `json:"choices"`
	Usage    *Usage       `json:"usage,omitempty"`
	Error    *ErrorField  `json:"error,omitempty"`

	// Raw is the verbatim upstream JSON body (audit M10). The tunnel's
	// contract is verbatim passthrough: unknown upstream fields must
	// survive non-streaming calls exactly as they survive streaming
	// ones. Decoding into this struct serves validation and metering;
	// the client answer is Raw, never a re-marshal of the struct.
	Raw []byte `json:"-"`
}

// IsError reports whether the body carries an error payload.
func (c *ChatResponse) IsError() bool {
	return c.Error != nil && (c.Error.Message != "" || len(c.Error.Code) > 0)
}

// RespChoice is a completion choice (message for non-stream).
type RespChoice struct {
	Index        int         `json:"index"`
	FinishReason string      `json:"finish_reason"`
	Message      ChatMessage `json:"message"`
}

// ChatMessage is the completed assistant message.
type ChatMessage struct {
	Role      string            `json:"role"`
	Content   string            `json:"content,omitempty"`
	Reasoning string            `json:"reasoning,omitempty"`
	Refusal   string            `json:"refusal,omitempty"`
	ToolCalls []MessageToolCall `json:"tool_calls,omitempty"`
}

// ChatChunk is one SSE data payload of a streaming completion. OpenRouter
// standardization: usage (with cost, is_byok and cost_details) arrives in
// the final chunk, just before [DONE]; that terminal chunk also repeats the
// finish_reason on a content-free delta — it is an accounting frame, not a
// second terminal event. A 200-OK stream may also carry a top-level error
// object (mid-stream provider failures after the response is committed,
// terminal finish_reason "error"), which IsError() detects.
type ChatChunk struct {
	ID       string        `json:"id"`
	Object   string        `json:"object"`
	Created  int64         `json:"created"`
	Model    string        `json:"model"`
	Provider string        `json:"provider,omitempty"`
	Choices  []ChunkChoice `json:"choices"`
	Usage    *Usage        `json:"usage,omitempty"`

	// In-band error payload (present only on error chunks; OpenRouter
	// sends the error at the TOP LEVEL of the chunk with a numeric code).
	Error *ErrorField `json:"error,omitempty"`
}

// IsError reports whether this chunk carries an in-band upstream error.
func (c *ChatChunk) IsError() bool {
	return c.Error != nil && (c.Error.Message != "" || len(c.Error.Code) > 0)
}

// ChunkChoice is the streaming choice with a delta.
type ChunkChoice struct {
	Index        int     `json:"index"`
	FinishReason *string `json:"finish_reason"` // pointer: null in most chunks
	Delta        Delta   `json:"delta"`
}

// Delta is the streaming message delta.
type Delta struct {
	Role             string            `json:"role,omitempty"`
	Content          string            `json:"content,omitempty"`
	Reasoning        string            `json:"reasoning,omitempty"`
	ReasoningContent string            `json:"reasoning_content,omitempty"` // DeepSeek/OpenRouter ext
	Refusal          string            `json:"refusal,omitempty"`
	ReasoningDetails []ReasoningDetail `json:"reasoning_details,omitempty"`
	ToolCalls        []ToolCallDelta   `json:"tool_calls,omitempty"`
}

// ReasoningDetail is a structured reasoning block.
type ReasoningDetail struct {
	ID        string `json:"id,omitempty"`
	Index     *int   `json:"index,omitempty"`
	Type      string `json:"type,omitempty"` // reasoning.summary|reasoning.text|reasoning.encrypted
	Summary   string `json:"summary,omitempty"`
	Text      string `json:"text,omitempty"`
	Signature string `json:"signature,omitempty"`
	Data      string `json:"data,omitempty"`
}

// ToolCallDelta is a streamed tool-call fragment. Index identifies the slot;
// id/name arrive once; arguments arrive incrementally.
type ToolCallDelta struct {
	Index    *int           `json:"index,omitempty"`
	ID       string         `json:"id,omitempty"`
	Type     string         `json:"type,omitempty"`
	Function *FunctionDelta `json:"function,omitempty"`
}

// FunctionDelta is the streamed function fragment.
type FunctionDelta struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

// Usage is OpenRouter's authoritative token/cost accounting. It rides the
// final stream chunk and every non-streaming body, always.
type Usage struct {
	PromptTokens            int64                    `json:"prompt_tokens"`
	CompletionTokens        int64                    `json:"completion_tokens"`
	TotalTokens             int64                    `json:"total_tokens"`
	PromptTokensDetails     *PromptTokensDetails     `json:"prompt_tokens_details,omitempty"`
	CompletionTokensDetails *CompletionTokensDetails `json:"completion_tokens_details,omitempty"`
	Cost                    *float64                 `json:"cost,omitempty"` // USD
	IsByok                  *bool                    `json:"is_byok,omitempty"`
	CostDetails             *CostDetails             `json:"cost_details,omitempty"`
}

// PromptTokensDetails breaks down input tokens. CachedWriteTokens decodes
// both documented spellings ("cache_write_tokens" — OpenRouter's — and
// "cached_write_tokens").
type PromptTokensDetails struct {
	TextTokens        int64 `json:"text_tokens,omitempty"`
	AudioTokens       int64 `json:"audio_tokens,omitempty"`
	ImageTokens       int64 `json:"image_tokens,omitempty"`
	CachedTokens      int64 `json:"cached_tokens,omitempty"`      // OpenRouter native
	CachedReadTokens  int64 `json:"cached_read_tokens,omitempty"` // tolerated alias
	CachedWriteTokens int64 `json:"cached_write_tokens,omitempty"`
}

// UnmarshalJSON additionally tolerates the "cache_write_tokens" spelling.
func (p *PromptTokensDetails) UnmarshalJSON(data []byte) error {
	type alias PromptTokensDetails
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*p = PromptTokensDetails(a)
	if p.CachedWriteTokens == 0 {
		var probe struct {
			CacheWriteTokens int64 `json:"cache_write_tokens"`
		}
		if json.Unmarshal(data, &probe) == nil {
			p.CachedWriteTokens = probe.CacheWriteTokens
		}
	}
	return nil
}

// CompletionTokensDetails breaks down output tokens.
type CompletionTokensDetails struct {
	TextTokens               int64 `json:"text_tokens,omitempty"`
	AudioTokens              int64 `json:"audio_tokens,omitempty"`
	ReasoningTokens          int64 `json:"reasoning_tokens,omitempty"`
	AcceptedPredictionTokens int64 `json:"accepted_prediction_tokens,omitempty"`
	RejectedPredictionTokens int64 `json:"rejected_prediction_tokens,omitempty"`
}

// CostDetails is OpenRouter's cost breakdown (USD).
type CostDetails struct {
	UpstreamInferencePromptCost      float64  `json:"upstream_inference_prompt_cost"`
	UpstreamInferenceCompletionsCost float64  `json:"upstream_inference_completions_cost"`
	UpstreamInferenceCost            *float64 `json:"upstream_inference_cost,omitempty"`
	ServerToolCost                   *float64 `json:"server_tool_cost,omitempty"`
}

// ReasoningText returns the effective reasoning delta text across the
// extension fields providers use.
func (d *Delta) ReasoningText() string {
	if d.Reasoning != "" {
		return d.Reasoning
	}
	if d.ReasoningContent != "" {
		return d.ReasoningContent
	}
	for _, rd := range d.ReasoningDetails {
		if rd.Text != "" {
			return rd.Text
		}
		if rd.Summary != "" {
			return rd.Summary
		}
	}
	return ""
}
