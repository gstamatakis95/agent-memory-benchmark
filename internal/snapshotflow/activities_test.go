package snapshotflow

import (
	"math"
	"testing"

	"example.com/agentmem/internal/embed"
)

func unitVector(dims int, seed float32) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = seed + float32(i)
	}
	return embed.L2Normalize(v)
}

func norm(v []float32) float64 {
	var sum float64
	for _, f := range v {
		sum += float64(f) * float64(f)
	}
	return math.Sqrt(sum)
}

func TestSnapshotVectorUnchangedAtNativeDims(t *testing.T) {
	v := unitVector(embed.Dims, 1)
	packed := embed.PackVector(v)

	got, err := snapshotVector(packed, embed.Dims)
	if err != nil {
		t.Fatalf("snapshotVector: %v", err)
	}
	if len(got) != embed.Dims {
		t.Fatalf("len(got) = %d, want %d", len(got), embed.Dims)
	}
	for i := range v {
		if got[i] != v[i] {
			t.Fatalf("got[%d] = %v, want unchanged %v", i, got[i], v[i])
		}
	}
}

func TestSnapshotVectorTruncatesAndRenormalizes(t *testing.T) {
	v := unitVector(embed.Dims, 1)
	packed := embed.PackVector(v)

	const dim = 256
	got, err := snapshotVector(packed, dim)
	if err != nil {
		t.Fatalf("snapshotVector: %v", err)
	}
	if len(got) != dim {
		t.Fatalf("len(got) = %d, want %d", len(got), dim)
	}
	if n := norm(got); math.Abs(n-1.0) > 1e-4 {
		t.Fatalf("norm(got) = %v, want ~1.0", n)
	}
	// Direction (up to the rescale) must match the leading dims of the
	// original unit vector: got[i] == v[i] * (||v[:dim]|| / ||v[:dim]||) is
	// trivially true, so check the ratio between components is preserved.
	scale := got[0] / v[0]
	for i := 1; i < dim; i++ {
		want := v[i] * scale
		if math.Abs(float64(got[i]-want)) > 1e-4 {
			t.Fatalf("got[%d] = %v, want %v (proportional to v[%d]=%v)", i, got[i], want, i, v[i])
		}
	}
}

func TestSnapshotVectorRejectsWrongPackedLength(t *testing.T) {
	// One float32 short of embed.Dims.
	short := embed.PackVector(make([]float32, embed.Dims-1))
	if _, err := snapshotVector(short, embed.Dims); err == nil {
		t.Fatal("expected an error for a wrong-length embedding")
	}

	// Not even a multiple of 4 bytes.
	if _, err := snapshotVector([]byte{1, 2, 3}, embed.Dims); err == nil {
		t.Fatal("expected an error for a malformed byte length")
	}
}

func TestSnapshotVectorRejectsWrongDimsEvenIfDivisibleBy4(t *testing.T) {
	// Well-formed as bytes (multiple of 4) but the wrong dimensionality —
	// e.g. a 512-dim response from a misbehaving embedder.
	wrong := embed.PackVector(make([]float32, 512))
	if _, err := snapshotVector(wrong, embed.Dims); err == nil {
		t.Fatal("expected an error: 512 dims != embed.Dims")
	}
}
