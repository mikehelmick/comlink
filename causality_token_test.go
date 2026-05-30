// Copyright 2026 the comlink authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package comlink

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func mustID(b byte) []byte {
	out := make([]byte, idLen)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestCausalityTokenRoundtrip(t *testing.T) {
	conv := ConversationID(mustID(0xAA))
	sender := ReplicaID(mustID(0xBB))

	for _, slot := range []uint64{0, 1, 42, 1<<32 - 1, 1<<63 - 1, ^uint64(0)} {
		tok := newCausalityToken(conv, sender, slot)
		if len(tok) == 0 {
			t.Fatalf("slot=%d: encode returned empty token", slot)
		}
		// Token must be in the URL-safe base64 alphabet so it
		// can ride in HTTP headers / query params without
		// further escaping.
		for _, c := range tok {
			urlSafe := (c >= 'A' && c <= 'Z') ||
				(c >= 'a' && c <= 'z') ||
				(c >= '0' && c <= '9') ||
				c == '-' || c == '_'
			if !urlSafe {
				t.Fatalf("slot=%d: token has non-URL-safe byte %q", slot, c)
			}
		}
		got, err := parseCausalityToken(tok)
		if err != nil {
			t.Fatalf("slot=%d: decode: %v", slot, err)
		}
		if !bytes.Equal(got.conv[:], conv[:]) {
			t.Errorf("slot=%d: conv mismatch", slot)
		}
		if !bytes.Equal(got.sender[:], sender[:]) {
			t.Errorf("slot=%d: sender mismatch", slot)
		}
		if got.slot != slot {
			t.Errorf("slot=%d: round-trip got %d", slot, got.slot)
		}
	}
}

func TestCausalityTokenEmptyIsInvalid(t *testing.T) {
	_, err := parseCausalityToken(nil)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("nil token: want ErrInvalidToken, got %v", err)
	}
	_, err = parseCausalityToken(CausalityToken{})
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("empty token: want ErrInvalidToken, got %v", err)
	}
}

func TestCausalityTokenWrongLengthRejected(t *testing.T) {
	_, err := parseCausalityToken(CausalityToken("AAAA"))
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("short token: want ErrInvalidToken, got %v", err)
	}
}

func TestCausalityTokenBadBase64Rejected(t *testing.T) {
	_, err := parseCausalityToken(CausalityToken("not-valid!!"))
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("bad base64: want ErrInvalidToken, got %v", err)
	}
}

func TestCausalityTokenWrongVersionRejected(t *testing.T) {
	raw := make([]byte, tokenLenRaw)
	raw[0] = 99
	copy(raw[1:1+idLen], mustID(0x01))
	copy(raw[1+idLen:1+idLen+idLen], mustID(0x02))
	tampered := base64.RawURLEncoding.EncodeToString(raw)

	_, err := parseCausalityToken(CausalityToken(tampered))
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("bad version: want ErrInvalidToken, got %v", err)
	}
	if !strings.Contains(err.Error(), "version") {
		t.Errorf("bad-version error should mention 'version'; got %v", err)
	}
}

func TestCausalityTokenZeroBytesRoundtrip(t *testing.T) {
	// All-zero conv + sender + slot=0 should still round-trip
	// cleanly. The substrate is responsible for refusing to
	// MINT zero-conv tokens; the codec itself is content-
	// agnostic so long as the lengths are correct.
	conv := ConversationID(mustID(0x00))
	sender := ReplicaID(mustID(0x00))
	tok := newCausalityToken(conv, sender, 0)
	if len(tok) == 0 {
		t.Fatal("zero-byte encode returned empty token")
	}
	got, err := parseCausalityToken(tok)
	if err != nil {
		t.Fatalf("zero-byte decode: %v", err)
	}
	if got.slot != 0 {
		t.Errorf("slot = %d, want 0", got.slot)
	}
}

func TestCausalityTokenZeroLengthRejected(t *testing.T) {
	// Zero-LENGTH IDs are different from zero-value bytes —
	// they're the uninitialized/nil case and we refuse them.
	var conv ConversationID
	var sender ReplicaID
	tok := newCausalityToken(conv, sender, 0)
	if len(tok) != 0 {
		t.Errorf("nil-ID encode should return empty token; got %q", tok)
	}
}

func TestCausalityTokenStableEncoding(t *testing.T) {
	conv := ConversationID(mustID(0x11))
	sender := ReplicaID(mustID(0x22))
	const slot = 12345
	a := newCausalityToken(conv, sender, slot)
	b := newCausalityToken(conv, sender, slot)
	if !bytes.Equal(a, b) {
		t.Errorf("encoding non-deterministic: %s vs %s", a, b)
	}
}
