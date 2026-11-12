package auth

import (
	"net/mail"
	"strings"
	"unicode/utf8"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/store"
)

// validEmail accepts a plain address (no display names, no source routes).
func validEmail(s string) bool {
	addr, err := mail.ParseAddress(s)
	return err == nil && addr.Address == s
}

// truncateUTF8 bounds s to max BYTES without splitting a multi-byte rune and
// strips invalid UTF-8 (audit M4). The old byte-slices could cut a CJK or
// emoji name mid-character at a column limit; PostgreSQL refuses to store the
// resulting invalid UTF-8, and the user's very first login answered 500.
func truncateUTF8(s string, max int) string {
	s = strings.ToValidUTF8(s, "")
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// storeMapError maps storage-layer failures onto the client-facing error model.
func storeMapError(err error) error {
	if err == nil {
		return nil
	}
	if de := domain.AsError(err); de != nil {
		return de
	}
	return store.MapDBError(err)
}
