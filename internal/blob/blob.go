// Package blob defines the content-addressed S3 blob envelope shared by the
// server (which writes blobs at ingest and reads them back for enrichment)
// and the client (which recomputes the same bytes at eval time to map
// content hashes back to dataset turn ids).
//
// docs/02-storage.md E.8: "Recommended blob format: a small
// protobuf-serialized (or JSON) Memory envelope carrying turns/speaker/date
// so the enricher can parse structure from the blob; content-addressed keys
// (sha256) give free dedup and idempotent re-writes."
//
// The JSON encoding must stay byte-stable: encoding/json emits struct fields
// in declaration order, so identical Envelope values always marshal to
// identical bytes and therefore identical content hashes / S3 keys.
package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Envelope is the raw-memory blob stored in S3. All fields are the
// client-supplied raw values; nothing derived lives here (derived fields are
// appended to the Postgres enrichment ledger).
type Envelope struct {
	ConversationID string `json:"conversation_id"`
	SessionID      string `json:"session_id"`
	TurnID         string `json:"turn_id"`
	Speaker        string `json:"speaker,omitempty"`
	Text           string `json:"text"`
	// DateTime is the raw, unparsed session/turn date string exactly as the
	// dataset carries it; the enricher parses it with pipeline.ParseTimestamp.
	DateTime string `json:"date_time,omitempty"`
}

// Marshal renders the canonical blob bytes.
func (e Envelope) Marshal() ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("blob: marshal: %w", err)
	}
	return b, nil
}

// Parse decodes blob bytes back into an Envelope.
func Parse(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("blob: parse: %w", err)
	}
	return e, nil
}

// Hash is the sha256 of the raw blob bytes (memories.content_hash).
func Hash(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}

// Key is the content-addressed S3 object key for a blob hash.
func Key(hash []byte) string {
	return hex.EncodeToString(hash)
}
