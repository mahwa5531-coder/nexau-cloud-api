package api

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
)

// decodeJSON body-tail contract (audit L3): exactly ONE JSON document per
// request body, enforced uniformly. The live suite's H11 probe (a valid body
// followed by "]}") exposed that the first version of this fix re-read
// r.Body AFTER json.Decoder had already buffered the tail — the re-read saw
// nothing and the garbage sailed through with a 200. These cases pin the
// contract at the unit level so it can never regress silently again.

func reqWithBody(t *testing.T, body string) *http.Request {
	t.Helper()
	return httptestReqBody(body)
}

func TestDecodeJSON_OneDocumentAccepted(t *testing.T) {
	var v map[string]any
	if err := decodeJSON(reqWithBody(t, `{"model":"openai/gpt-6-luna","messages":[{"role":"user","content":"hi"}]}`), &v); err != nil {
		t.Fatalf("clean single document must decode, got %v", err)
	}
	if v["model"] != "openai/gpt-6-luna" {
		t.Fatalf("payload lost: %v", v)
	}
}

func TestDecodeJSON_TrailingWhitespaceOnlyAccepted(t *testing.T) {
	var v map[string]any
	if err := decodeJSON(reqWithBody(t, "{\"a\":1}\n\t \n"), &v); err != nil {
		t.Fatalf("whitespace-only tail is not content, got %v", err)
	}
}

func TestDecodeJSON_TrailingGarbageRejected(t *testing.T) {
	// The exact live-suite H11 shape: every '}' of a valid body followed by
	// ']}' — invalid JSON tail AFTER a complete first document.
	cases := map[string]string{
		"live-suite H11":  `{"model":"openai/gpt-6-luna","messages":[{"role":"user","content":"hi"}]}]}`,
		"junk tail":       `{"a":1} not json at all`,
		"partial second":  `{"a":1} {"b":`,
		"lone bracket":    `{"a":1} ]`,
		"truncated value": `{"a":1} {"b":tru`,
	}
	for name, body := range cases {
		var v map[string]any
		if err := decodeJSON(reqWithBody(t, body), &v); err == nil {
			t.Errorf("%s: trailing garbage must be rejected, body=%q", name, body)
		}
	}
}

func TestDecodeJSON_TrailingValidDocumentRejected(t *testing.T) {
	var v map[string]any
	if err := decodeJSON(reqWithBody(t, `{"a":1} {"b":2}`), &v); err == nil {
		t.Fatal("a second complete JSON document must be rejected")
	}
}

func TestDecodeJSON_LargeBodyTailNotSwallowedByBuffering(t *testing.T) {
	// Bodies larger than the decoder's read-ahead chunk: the tail may still
	// be in r.Body when the probe runs — both paths must reject.
	var sb strings.Builder
	sb.WriteString(`{"pad":"`)
	sb.WriteString(strings.Repeat("x", 64*1024))
	sb.WriteString(`","ok":true} trailing junk`)
	var v map[string]any
	if err := decodeJSON(httptestReqBody(sb.String()), &v); err == nil {
		t.Fatal("large body with trailing junk must be rejected")
	}
}

func httptestReqBody(body string) *http.Request {
	return &http.Request{Body: io.NopCloser(bytes.NewReader([]byte(body)))}
}
