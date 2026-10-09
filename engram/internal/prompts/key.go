package prompts

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"sort"
)

// writeField appends a length-prefixed field, so that the concatenation of fields is unambiguous ("ab"+"c" and
// "a"+"bc" are different keys).
func writeField(h interface{ Write([]byte) (int, error) }, b []byte) {
	var n [binary.MaxVarintLen64]byte
	k := binary.PutUvarint(n[:], uint64(len(b)))
	_, _ = h.Write(n[:k])
	_, _ = h.Write(b)
}

// RenderHash is the render_hash of the extraction cache key (PLAN.md N87, N110(a), section 2.2.10):
//
//	sha256(day(mentioned_at) ‖ context ‖ canonical(metadata) ‖ sorted(entity_hints) ‖ retain.mission ‖ heading path)
//
// with every field length-prefixed. It is computed from the very struct that ExtractInputs renders, so the key cannot
// cover less than the prompt shows (an entity hint's type is part of the hint, a metadata value of any JSON type is
// part of the metadata). Only the UTC day of ItemTimestamp is hashed. The document summary is NOT an input: N87 and
// N110(a) say a summary refresh re-embeds the chunk without re-extracting, so Summary is not hashed while HeadingPath
// is; the prompt's header is built from both fields (ExtractInput.Header), so the shown and the hashed path are one.
// Section 6.0 and the section 8.4 row of TestExtraction_RenderHashKey say the opposite; the register wins (CONFLICTS.md
// #32). Content is not an input either: the key's chunk_hash covers it.
func RenderHash(in ExtractInput) [32]byte {
	h := sha256.New()
	writeField(h, []byte(in.ItemTimestamp.UTC().Format("2006-01-02")))
	writeField(h, []byte(in.Context))
	meta, _ := json.Marshal(in.Metadata) // a map marshals with sorted keys
	writeField(h, meta)
	hints := append([]EntityHint(nil), in.EntityHints...)
	sort.Slice(hints, func(i, j int) bool {
		if hints[i].Name != hints[j].Name {
			return hints[i].Name < hints[j].Name
		}
		return hints[i].Type < hints[j].Type
	})
	writeField(h, []byte{byte(len(hints) >> 8), byte(len(hints))})
	for _, e := range hints {
		writeField(h, []byte(e.Name))
		writeField(h, []byte(e.Type))
	}
	writeField(h, []byte(in.Mission))
	writeField(h, []byte(in.HeadingPath))
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// KeyInput is the five inputs of the extraction cache key (PLAN.md section 2.2.10, 6.0).
type KeyInput struct {
	ChunkHash     [32]byte
	PromptVersion string // the prompt id, "extract/v1"
	Model         string
	SchemaVersion int
	RenderHash    [32]byte
}

// ExtractionKey is sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version ‖ render_hash), the name of the cache
// blob "{prefix}xcache/{hex}.json". It changes when any one input changes and with nothing else. Nothing else is
// hashed: not the namespace epoch (D11), not a timestamp, not the document summary.
func ExtractionKey(in KeyInput) [32]byte {
	h := sha256.New()
	writeField(h, in.ChunkHash[:])
	writeField(h, []byte(in.PromptVersion))
	writeField(h, []byte(in.Model))
	var sv [8]byte
	binary.BigEndian.PutUint64(sv[:], uint64(in.SchemaVersion))
	writeField(h, sv[:])
	writeField(h, in.RenderHash[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
