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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// DefaultSnapshotChunkBytes is the size in bytes of each
// SnapshotChunk.data emitted by the streaming RPC. ~1 MiB is
// a reasonable balance: small enough to limit memory pressure
// and stream-pause latency, large enough that per-chunk gRPC
// framing overhead is amortized.
const DefaultSnapshotChunkBytes = 1 << 20

// StreamSnapshot is the server-streaming Cluster RPC handler
// (Phase 10(c)). Pulls the snapshot from the SM registered for
// the requested ConversationID, chunks it for transmission,
// and streams the chunks. The first chunk carries the header
// (through_offset + size hint); subsequent chunks carry only
// payload. The last chunk has last=true.
//
// Errors surface to the client as gRPC status:
//   - NotFound      : no snapshot source registered for this conv.
//   - Internal      : the SM's Snapshot() call failed.
//   - Canceled      : the stream context was cancelled.
//
// Exempt from the ClusterID handshake interceptor (see
// transport/grpc.ExemptHandshakeMethods).
func (s *joinService) StreamSnapshot(req *pb.SnapshotRequest, stream pb.Cluster_StreamSnapshotServer) error {
	convID := ConversationID(req.GetConversationId().GetValue())
	if len(convID) != idLen {
		return status.Errorf(codes.InvalidArgument,
			"StreamSnapshot: invalid conversation_id length %d", len(convID))
	}
	sub := s.owner.lookupSnapshotSource(convID)
	if sub == nil {
		return status.Errorf(codes.NotFound,
			"StreamSnapshot: no snapshot source registered for conv %s", convID)
	}
	snapper, ok := sub.cfg.StateMachine.(Snapshotter)
	if !ok {
		// This shouldn't happen — registration only fires for
		// SMs that implement Snapshotter — but guard anyway.
		return status.Errorf(codes.Internal,
			"StreamSnapshot: SM for conv %s doesn't implement Snapshotter", convID)
	}

	snapBytes, throughOff, err := snapper.Snapshot()
	if err != nil {
		return status.Errorf(codes.Internal,
			"StreamSnapshot: SM.Snapshot() failed: %v", err)
	}

	s.owner.logger.Info("StreamSnapshot: serving",
		"conv", convID.String()[:8],
		"requester", ReplicaID(req.GetRequester().GetValue()).String(),
		"through_offset", throughOff,
		"size_bytes", len(snapBytes))

	return streamSnapshotChunks(stream, snapBytes, throughOff, DefaultSnapshotChunkBytes)
}

// streamSnapshotChunks pumps snapBytes through the stream in
// chunks of chunkSize bytes. Header on first chunk only; last=true
// on the final chunk. Handles the empty-snapshot case by sending
// a single header-only chunk with last=true.
func streamSnapshotChunks(stream pb.Cluster_StreamSnapshotServer, snapBytes []byte, throughOff uint64, chunkSize int) error {
	if chunkSize <= 0 {
		chunkSize = DefaultSnapshotChunkBytes
	}
	header := &pb.SnapshotHeader{
		ThroughOffset:  throughOff,
		TotalSizeBytes: uint64(len(snapBytes)),
	}
	if len(snapBytes) == 0 {
		// Empty snapshot: header-only chunk with last=true.
		return stream.Send(&pb.SnapshotChunk{
			Header:     header,
			ChunkIndex: 0,
			Last:       true,
		})
	}
	var idx uint64
	for off := 0; off < len(snapBytes); off += chunkSize {
		end := off + chunkSize
		if end > len(snapBytes) {
			end = len(snapBytes)
		}
		chunk := &pb.SnapshotChunk{
			ChunkIndex: idx,
			Data:       snapBytes[off:end],
			Last:       end == len(snapBytes),
		}
		if idx == 0 {
			chunk.Header = header
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
		idx++
	}
	return nil
}

// PullSnapshot is the joiner-side helper: dials a peer, calls
// StreamSnapshot for the given conversation, and stages the
// chunks to a temp file under DataDir/snapshots/<conv>/. Returns
// a *Snapshot whose Reader is open on the staged file. The
// caller owns closing the reader after Substrate construction
// completes.
//
// peerAddr is the gRPC address of the snapshot source (typically
// the sponsor used during Join; could be any peer that has a
// matching substrate up).
//
// On any error during the stream, the temp file is removed.
func (c *Cluster) PullSnapshot(ctx context.Context, peerAddr string, convID ConversationID) (*Snapshot, error) {
	if len(convID) != idLen {
		return nil, errors.New("comlink: PullSnapshot: invalid ConversationID")
	}

	conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("comlink: PullSnapshot dial %s: %w", peerAddr, err)
	}
	defer conn.Close()
	client := pb.NewClusterClient(conn)

	stream, err := client.StreamSnapshot(ctx, &pb.SnapshotRequest{
		ConversationId: convID.toPB(),
		Requester:      c.cfg.Self.toPB(),
	})
	if err != nil {
		return nil, fmt.Errorf("comlink: PullSnapshot stream open: %w", err)
	}

	// Stage to a temp file under DataDir/snapshots/<conv>/
	// — large snapshots stay off-heap.
	stageDir := filepath.Join(c.cfg.DataDir, "snapshots", convID.String())
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return nil, fmt.Errorf("comlink: PullSnapshot mkdir: %w", err)
	}
	stagePath := filepath.Join(stageDir, "incoming.snap")
	f, err := os.Create(stagePath)
	if err != nil {
		return nil, fmt.Errorf("comlink: PullSnapshot stage: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
			_ = os.Remove(stagePath)
		}
	}()

	var throughOff uint64
	var expectedIdx uint64
	var bytesWritten uint64
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return nil, fmt.Errorf("comlink: PullSnapshot: stream ended without last=true (got %d chunks, %d bytes)", expectedIdx, bytesWritten)
		}
		if err != nil {
			return nil, fmt.Errorf("comlink: PullSnapshot recv: %w", err)
		}
		if chunk.GetChunkIndex() != expectedIdx {
			return nil, fmt.Errorf("comlink: PullSnapshot: out-of-order chunk %d (expected %d)", chunk.GetChunkIndex(), expectedIdx)
		}
		if expectedIdx == 0 {
			h := chunk.GetHeader()
			if h == nil {
				return nil, errors.New("comlink: PullSnapshot: first chunk missing header")
			}
			throughOff = h.GetThroughOffset()
		}
		if data := chunk.GetData(); len(data) > 0 {
			n, err := f.Write(data)
			if err != nil {
				return nil, fmt.Errorf("comlink: PullSnapshot write stage: %w", err)
			}
			bytesWritten += uint64(n)
		}
		expectedIdx++
		if chunk.GetLast() {
			break
		}
	}

	// Sync and rewind so the returned Reader starts at byte 0.
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("comlink: PullSnapshot fsync: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("comlink: PullSnapshot rewind: %w", err)
	}
	closed = true // hand ownership of f off to the Snapshot.Reader

	c.logger.Info("PullSnapshot: complete",
		"conv", convID.String()[:8],
		"peer", peerAddr,
		"through_offset", throughOff,
		"chunks", expectedIdx,
		"bytes", bytesWritten,
		"stage", stagePath)

	return &Snapshot{
		Reader:        &stagedSnapshotReader{File: f, path: stagePath},
		ThroughOffset: throughOff,
	}, nil
}

// stagedSnapshotReader is the io.Reader form of a snapshot file
// staged in DataDir/snapshots/<conv>/incoming.snap. Cleans up
// the underlying file when closed.
//
// Embeds *os.File so callers get Read for free.
type stagedSnapshotReader struct {
	*os.File
	path string
}

func (s *stagedSnapshotReader) Close() error {
	err := s.File.Close()
	if removeErr := os.Remove(s.path); err == nil {
		err = removeErr
	}
	return err
}

// MarshalForTest is exported so test code can byte-compare what
// would have gone over the wire. Not part of the public API.
func snapshotChunksForTest(snapBytes []byte, throughOff uint64, chunkSize int) ([]*pb.SnapshotChunk, error) {
	var captured []*pb.SnapshotChunk
	mock := &mockSnapshotStream{
		recv: func(c *pb.SnapshotChunk) error { captured = append(captured, c); return nil },
	}
	if err := streamSnapshotChunks(mock, snapBytes, throughOff, chunkSize); err != nil {
		return nil, err
	}
	return captured, nil
}

// mockSnapshotStream is a test helper that satisfies the
// stream-server interface enough to capture Sent chunks.
type mockSnapshotStream struct {
	pb.Cluster_StreamSnapshotServer // embed for any unused methods
	recv                            func(*pb.SnapshotChunk) error
}

func (m *mockSnapshotStream) Send(c *pb.SnapshotChunk) error { return m.recv(c) }
func (m *mockSnapshotStream) Context() context.Context       { return context.Background() }

// Unused-import guards so editors don't strip our deps before
// the streaming server compiles.
var (
	_ = bytes.NewReader
)
