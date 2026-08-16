package embed

import (
	"encoding/binary"
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func assertUnitNorm(t *testing.T, v []float32) {
	t.Helper()
	var sum float64
	for _, f := range v {
		sum += float64(f) * float64(f)
	}
	if norm := math.Sqrt(sum); math.Abs(norm-1) > 1e-6 {
		t.Fatalf("|v| = %v, want 1 within 1e-6", norm)
	}
}

func TestL2Normalize(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	v := make([]float32, Dims)
	for i := range v {
		v[i] = rng.Float32()*10 - 5
	}
	assertUnitNorm(t, L2Normalize(v))

	// Idempotent (normalizing a normalized vector is a no-op up to eps).
	assertUnitNorm(t, L2Normalize(v))

	// Zero vector: unchanged, no NaNs.
	z := L2Normalize(make([]float32, 4))
	for _, f := range z {
		if f != 0 {
			t.Fatalf("zero vector changed: %v", z)
		}
	}
}

func TestPackUnpackRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	v := make([]float32, Dims)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	b := PackVector(v)
	if len(b) != 4*Dims { // 3072 bytes for 768 dims
		t.Fatalf("packed length = %d, want %d", len(b), 4*Dims)
	}
	got, err := UnpackVector(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, v) {
		t.Fatal("pack/unpack round trip mismatch")
	}
}

func TestPackVectorLittleEndian(t *testing.T) {
	b := PackVector([]float32{1.0})
	want := make([]byte, 4)
	binary.LittleEndian.PutUint32(want, math.Float32bits(1.0)) // 00 00 80 3f
	if !reflect.DeepEqual(b, want) {
		t.Fatalf("packed 1.0 = %x, want %x (little-endian)", b, want)
	}
}

func TestUnpackVectorBadLength(t *testing.T) {
	if _, err := UnpackVector(make([]byte, 5)); err == nil {
		t.Fatal("want error for length not divisible by 4")
	}
}
