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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
)

// idLen is the canonical byte length for ClusterID, ReplicaID,
// and ConversationID. Matches the 16-byte UUID-like format used
// throughout the substrate.
const idLen = 16

// ErrInvalidID is returned when a parse or decode operation
// receives malformed input.
var ErrInvalidID = errors.New("comlink: invalid id")

// ClusterID identifies a comlink cluster (PLAN §5). Generated
// once at bootstrap (Cluster created with Bootstrap.Force=true),
// persisted to stable.Storage, and exchanged at gRPC connection
// handshake to prevent two separate clusters with overlapping
// ConversationIDs from accidentally merging.
type ClusterID []byte

// ReplicaID identifies one participant in a conversation.
// Stable across restarts of the same logical replica.
type ReplicaID []byte

// ConversationID identifies one Psync conversation. The system
// conversation's ConversationID is deterministically derived
// from the ClusterID via SystemConversationID; application
// conversations have caller-chosen IDs.
type ConversationID []byte

// ─── construction ────────────────────────────────────────────────

// NewClusterID generates a fresh ClusterID using crypto/rand.
func NewClusterID() (ClusterID, error) {
	b := make([]byte, idLen)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return ClusterID(b), nil
}

// NewReplicaID generates a fresh ReplicaID using crypto/rand.
func NewReplicaID() (ReplicaID, error) {
	b := make([]byte, idLen)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return ReplicaID(b), nil
}

// NewConversationID generates a fresh ConversationID.
func NewConversationID() (ConversationID, error) {
	b := make([]byte, idLen)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return ConversationID(b), nil
}

// SystemConversationID derives the well-known system
// conversation's ConversationID from a ClusterID. Every node in
// the cluster computes the same ID without coordination.
//
// Derivation: first idLen bytes of sha256(clusterID).
func SystemConversationID(clusterID ClusterID) ConversationID {
	h := sha256.Sum256(clusterID)
	return ConversationID(h[:idLen])
}

// ─── formatting & parsing ────────────────────────────────────────

// String returns the hex encoding of the id.
func (c ClusterID) String() string      { return hex.EncodeToString(c) }
func (r ReplicaID) String() string      { return hex.EncodeToString(r) }
func (c ConversationID) String() string { return hex.EncodeToString(c) }

// ParseClusterID parses a hex-encoded ClusterID.
func ParseClusterID(s string) (ClusterID, error) {
	b, err := parseHexID(s)
	if err != nil {
		return nil, fmt.Errorf("ClusterID: %w", err)
	}
	return ClusterID(b), nil
}

// ParseReplicaID parses a hex-encoded ReplicaID.
func ParseReplicaID(s string) (ReplicaID, error) {
	b, err := parseHexID(s)
	if err != nil {
		return nil, fmt.Errorf("ReplicaID: %w", err)
	}
	return ReplicaID(b), nil
}

// ParseConversationID parses a hex-encoded ConversationID.
func ParseConversationID(s string) (ConversationID, error) {
	b, err := parseHexID(s)
	if err != nil {
		return nil, fmt.Errorf("ConversationID: %w", err)
	}
	return ConversationID(b), nil
}

func parseHexID(s string) ([]byte, error) {
	if s == "" {
		return nil, fmt.Errorf("%w: empty", ErrInvalidID)
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidID, err)
	}
	if len(b) != idLen {
		return nil, fmt.Errorf("%w: wrong length %d (expected %d)", ErrInvalidID, len(b), idLen)
	}
	return b, nil
}

// ─── envconfig.Decoder support ───────────────────────────────────

// EnvDecode implements envconfig.Decoder so ClusterID can be
// loaded from environment variables.
func (c *ClusterID) EnvDecode(val string) error {
	parsed, err := ParseClusterID(val)
	if err != nil {
		return err
	}
	*c = parsed
	return nil
}

// EnvDecode implements envconfig.Decoder.
func (r *ReplicaID) EnvDecode(val string) error {
	parsed, err := ParseReplicaID(val)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

// EnvDecode implements envconfig.Decoder.
func (c *ConversationID) EnvDecode(val string) error {
	parsed, err := ParseConversationID(val)
	if err != nil {
		return err
	}
	*c = parsed
	return nil
}

// ─── equality & comparison ───────────────────────────────────────

// Equal reports whether a and b are the same id.
func (c ClusterID) Equal(other ClusterID) bool           { return bytes.Equal(c, other) }
func (r ReplicaID) Equal(other ReplicaID) bool           { return bytes.Equal(r, other) }
func (c ConversationID) Equal(other ConversationID) bool { return bytes.Equal(c, other) }

// ─── proto bridges (internal — used by comlink package code only) ───

// toPB converts the public ID into its internal protobuf form for
// use with the substrate's lower layers (psync, membership,
// transport, etc.). Currently only ReplicaID and ConversationID
// flow into the substrate; ClusterID stays at the public layer
// (the gRPC handshake interceptor in Phase 5(i) will need its
// pb form, at which point we re-add it).
func (r ReplicaID) toPB() *pb.ReplicaID           { return &pb.ReplicaID{Value: r} }
func (c ConversationID) toPB() *pb.ConversationID { return &pb.ConversationID{Value: c} }

// replicaIDFromPB converts an internal protobuf ReplicaID back
// to the public type.
func replicaIDFromPB(p *pb.ReplicaID) ReplicaID {
	if p == nil {
		return nil
	}
	return ReplicaID(p.GetValue())
}

