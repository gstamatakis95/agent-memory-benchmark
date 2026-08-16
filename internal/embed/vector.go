package embed

import (
	"encoding/binary"
	"fmt"
	"math"
)

// PackVector packs a []float32 into little-endian bytes — the BYTEA layout
// of the embedding columns (docs/01-retrieval.md section 4.5: packed 768
// float32 little-endian = 3072 bytes). Exported because retrieval unpacks
// the same layout.
func PackVector(v []float32) []byte {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(f))
	}
	return b
}

// UnpackVector is the inverse of PackVector.
func UnpackVector(b []byte) ([]float32, error) {
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("embed: packed vector length %d is not a multiple of 4", len(b))
	}
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return v, nil
}

// L2Normalize scales v in place to unit L2 norm and returns it, so cosine
// similarity reduces to a dot product (docs/01-retrieval.md section 4.1).
// Normalizing an already-normalized vector is a no-op up to float error; a
// zero vector is returned unchanged (no NaNs).
func L2Normalize(v []float32) []float32 {
	var sum float64
	for _, f := range v {
		sum += float64(f) * float64(f)
	}
	if sum == 0 {
		return v
	}
	inv := 1 / math.Sqrt(sum)
	for i := range v {
		v[i] = float32(float64(v[i]) * inv)
	}
	return v
}
