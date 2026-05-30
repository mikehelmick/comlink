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
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
)

// CausalityToken is an opaque handle representing a specific
// position in a substrate's causal order. It's returned by
// Substrate.Submit and consumed by Substrate.WaitForCausality
// to support read-your-writes semantics across replicas.
//
// Callers should treat the bytes as opaque: encode them for
// transport (base64, hex, etc), pass them through clients
// untouched, and let comlink decode them on the read path.
// The wire format may change between major versions; the
// version byte at the front lets us reject incompatible
// tokens cleanly.
type CausalityToken []byte

// Token wire format (41 bytes raw, ~56 chars when base64'd):
//   byte 0    : version (currently 1)
//   bytes 1-16: conversation_id (16 bytes)
//   bytes 17-32: sender ReplicaID (16 bytes)
//   bytes 33-40: sender_slot (8 bytes, big-endian uint64)
//
// Encoded form is RawURLEncoded base64 (URL-safe, no padding)
// so the token can be passed in HTTP headers, query params,
// or paths without further escaping.
const (
	tokenVersion = 1
	tokenLenRaw  = 1 + idLen + idLen + 8 // 41
)

// Errors returned by token decode + WaitForCausality.
var (
	// ErrInvalidToken — the token's bytes don't parse: wrong
	// length, unknown version, or malformed encoding.
	ErrInvalidToken = errors.New("comlink: invalid causality token")
	// ErrTokenWrongConversation — the token is well-formed
	// but addresses a different conversation than the
	// substrate it was presented to.
	ErrTokenWrongConversation = errors.New("comlink: token for wrong conversation")
	// ErrReadConsistencyTimeout — WaitForCausality's timeout
	// fired before the local replica observed the token's
	// position. Retryable; the position may arrive shortly.
	ErrReadConsistencyTimeout = errors.New("comlink: read consistency wait timed out")
)

// newCausalityToken constructs a token from the substrate-side
// triple (conv, sender, slot). Called from Submit just before
// returning.
func newCausalityToken(conv ConversationID, sender ReplicaID, slot uint64) CausalityToken {
	if len(conv) != idLen || len(sender) != idLen {
		// Shouldn't happen — substrate guarantees both are
		// 16 bytes. Return a clearly-invalid token rather
		// than panic; the decode side will reject it.
		return nil
	}
	raw := make([]byte, tokenLenRaw)
	raw[0] = tokenVersion
	copy(raw[1:1+idLen], conv[:])
	copy(raw[1+idLen:1+idLen+idLen], sender[:])
	binary.BigEndian.PutUint64(raw[1+idLen+idLen:], slot)
	enc := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(enc, raw)
	return CausalityToken(enc)
}

// parsedToken is the decoded form. Internal-only — callers
// never see the components; they pass the CausalityToken to
// WaitForCausality which decodes it itself.
type parsedToken struct {
	conv   ConversationID
	sender ReplicaID
	slot   uint64
}

// parseCausalityToken decodes a token from its wire form.
// Returns ErrInvalidToken for any structural issue.
func parseCausalityToken(t CausalityToken) (parsedToken, error) {
	if len(t) == 0 {
		return parsedToken{}, ErrInvalidToken
	}
	raw := make([]byte, base64.RawURLEncoding.DecodedLen(len(t)))
	n, err := base64.RawURLEncoding.Decode(raw, []byte(t))
	if err != nil {
		return parsedToken{}, fmt.Errorf("%w: base64 decode: %v", ErrInvalidToken, err)
	}
	raw = raw[:n]
	if len(raw) != tokenLenRaw {
		return parsedToken{}, fmt.Errorf("%w: length %d, want %d", ErrInvalidToken, len(raw), tokenLenRaw)
	}
	if raw[0] != tokenVersion {
		return parsedToken{}, fmt.Errorf("%w: version %d, want %d", ErrInvalidToken, raw[0], tokenVersion)
	}
	var p parsedToken
	p.conv = ConversationID(raw[1 : 1+idLen])
	p.sender = ReplicaID(raw[1+idLen : 1+idLen+idLen])
	p.slot = binary.BigEndian.Uint64(raw[1+idLen+idLen:])
	return p, nil
}

// String returns the token's wire form (RawURLEncoded base64).
// Useful for logging; the byte slice itself is already encoded.
func (t CausalityToken) String() string {
	return string(t)
}
