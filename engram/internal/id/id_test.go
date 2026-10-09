package id_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/gstamatakis95/engram/internal/id"
)

// uuidCase drives the round-trip test of one UUID-backed id type through closures, so that a missing method on any of
// the nine types is a compile error here.
type uuidCase struct {
	name      string
	new       func() (string, []byte)
	parse     func(string) (string, []byte, error)
	fromBytes func([]byte) (string, error)
}

func cases() []uuidCase {
	return []uuidCase{
		{"NamespaceID",
			func() (string, []byte) { x := id.NewNamespaceID(); return x.String(), x.Bytes() },
			func(s string) (string, []byte, error) {
				x, err := id.ParseNamespaceID(s)
				return x.String(), x.Bytes(), err
			},
			func(b []byte) (string, error) { x, err := id.NamespaceIDFromBytes(b); return x.String(), err }},
		{"FactID",
			func() (string, []byte) { x := id.NewFactID(); return x.String(), x.Bytes() },
			func(s string) (string, []byte, error) { x, err := id.ParseFactID(s); return x.String(), x.Bytes(), err },
			func(b []byte) (string, error) { x, err := id.FactIDFromBytes(b); return x.String(), err }},
		{"ChunkID",
			func() (string, []byte) { x := id.NewChunkID(); return x.String(), x.Bytes() },
			func(s string) (string, []byte, error) {
				x, err := id.ParseChunkID(s)
				return x.String(), x.Bytes(), err
			},
			func(b []byte) (string, error) { x, err := id.ChunkIDFromBytes(b); return x.String(), err }},
		{"EntityID",
			func() (string, []byte) { x := id.NewEntityID(); return x.String(), x.Bytes() },
			func(s string) (string, []byte, error) {
				x, err := id.ParseEntityID(s)
				return x.String(), x.Bytes(), err
			},
			func(b []byte) (string, error) { x, err := id.EntityIDFromBytes(b); return x.String(), err }},
		{"ObservationID",
			func() (string, []byte) { x := id.NewObservationID(); return x.String(), x.Bytes() },
			func(s string) (string, []byte, error) {
				x, err := id.ParseObservationID(s)
				return x.String(), x.Bytes(), err
			},
			func(b []byte) (string, error) { x, err := id.ObservationIDFromBytes(b); return x.String(), err }},
		{"PageID",
			func() (string, []byte) { x := id.NewPageID(); return x.String(), x.Bytes() },
			func(s string) (string, []byte, error) { x, err := id.ParsePageID(s); return x.String(), x.Bytes(), err },
			func(b []byte) (string, error) { x, err := id.PageIDFromBytes(b); return x.String(), err }},
		{"LedgerID",
			func() (string, []byte) { x := id.NewLedgerID(); return x.String(), x.Bytes() },
			func(s string) (string, []byte, error) {
				x, err := id.ParseLedgerID(s)
				return x.String(), x.Bytes(), err
			},
			func(b []byte) (string, error) { x, err := id.LedgerIDFromBytes(b); return x.String(), err }},
		{"OperationID",
			func() (string, []byte) { x := id.NewOperationID(); return x.String(), x.Bytes() },
			func(s string) (string, []byte, error) {
				x, err := id.ParseOperationID(s)
				return x.String(), x.Bytes(), err
			},
			func(b []byte) (string, error) { x, err := id.OperationIDFromBytes(b); return x.String(), err }},
		{"MoveID",
			func() (string, []byte) { x := id.NewMoveID(); return x.String(), x.Bytes() },
			func(s string) (string, []byte, error) { x, err := id.ParseMoveID(s); return x.String(), x.Bytes(), err },
			func(b []byte) (string, error) { x, err := id.MoveIDFromBytes(b); return x.String(), err }},
	}
}

func TestUUIDIDs_RoundTrip(t *testing.T) {
	for _, c := range cases() {
		t.Run(c.name, func(t *testing.T) {
			s, b := c.new()
			if len(b) != 16 {
				t.Fatalf("Bytes() has %d bytes, want 16", len(b))
			}
			u, err := uuid.Parse(s)
			if err != nil || u.Version() != 7 {
				t.Fatalf("New() = %q: want a UUIDv7 (err %v, version %d)", s, err, u.Version())
			}
			s2, b2, err := c.parse(s)
			if err != nil || s2 != s || string(b2) != string(b) {
				t.Fatalf("Parse(%q) = %q, %x, %v; want the same id", s, s2, b2, err)
			}
			s3, err := c.fromBytes(b)
			if err != nil || s3 != s {
				t.Fatalf("FromBytes(%x) = %q, %v; want %q", b, s3, err, s)
			}
		})
	}
}

func TestUUIDIDs_RejectMalformed(t *testing.T) {
	bad := []string{"", "not-a-uuid", "0192-5c1e", "{01925c1e-0000-7000-8000-000000000000}",
		"urn:uuid:01925c1e-0000-7000-8000-000000000000", "0192Xc1e-0000-7000-8000-000000000000",
		"01925c1e000070008000000000000000"}
	for _, c := range cases() {
		for _, s := range bad {
			if _, _, err := c.parse(s); !errors.Is(err, id.ErrInvalid) {
				t.Errorf("%s: Parse(%q) error = %v, want id.ErrInvalid", c.name, s, err)
			}
		}
		for _, n := range []int{0, 15, 17} {
			if _, err := c.fromBytes(make([]byte, n)); !errors.Is(err, id.ErrInvalid) {
				t.Errorf("%s: FromBytes(%d bytes) error = %v, want id.ErrInvalid", c.name, n, err)
			}
		}
	}
}

func TestUUIDIDs_ZeroAndOrdering(t *testing.T) {
	var z id.FactID
	if !z.IsZero() || id.NewFactID().IsZero() {
		t.Fatal("IsZero must be true only for the nil UUID")
	}
	a, b := id.NewFactID(), id.NewFactID()
	if a == b {
		t.Fatal("two New ids must differ")
	}
}

func TestTenantID(t *testing.T) {
	for _, ok := range []string{"a", "acme", "acme-42", strings.Repeat("a", 64), "0"} {
		got, err := id.ParseTenantID(ok)
		if err != nil || got.String() != ok {
			t.Errorf("ParseTenantID(%q) = %q, %v", ok, got, err)
		}
	}
	for _, bad := range []string{"", "Acme", "acme_corp", "acme corp", strings.Repeat("a", 65), "é"} {
		if _, err := id.ParseTenantID(bad); !errors.Is(err, id.ErrInvalid) {
			t.Errorf("ParseTenantID(%q) error = %v, want ErrInvalid", bad, err)
		}
	}
}

func TestDocumentID(t *testing.T) {
	if _, err := id.ParseDocumentID(strings.Repeat("x", 256)); err != nil {
		t.Errorf("256 bytes must be accepted: %v", err)
	}
	for _, bad := range []string{"", strings.Repeat("x", 257), strings.Repeat("é", 129)} {
		if _, err := id.ParseDocumentID(bad); !errors.Is(err, id.ErrInvalid) {
			t.Errorf("ParseDocumentID(len %d) error = %v, want ErrInvalid", len(bad), err)
		}
	}
}

func TestShardID_String(t *testing.T) {
	if got := id.ShardID(7).String(); got != "7" {
		t.Errorf("ShardID(7).String() = %q, want 7 (the shard=\"7\" label)", got)
	}
}

func TestSubject_Encoding(t *testing.T) {
	ns := id.NewNamespaceID()
	var h [32]byte
	h[0], h[31] = 0xab, 0xcd
	tests := []struct {
		name string
		s    id.Subject
		want string
	}{
		{"document", id.DocumentSubject("doc-1"), "document:doc-1"},
		{"memory", id.MemorySubject("doc-1", h), "memory:doc-1:ab" + strings.Repeat("00", 30) + "cd"},
		{"namespace", id.NamespaceSubject(ns), "namespace:" + ns.String()},
		{"tenant", id.TenantSubject("acme"), "tenant:acme"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.s.String(); got != tc.want {
				t.Fatalf("String() = %q, want %q", got, tc.want)
			}
			back, err := id.ParseSubject(tc.want)
			if err != nil || back != tc.s {
				t.Fatalf("ParseSubject(%q) = %+v, %v; want %+v", tc.want, back, err, tc.s)
			}
		})
	}
}

func TestSubject_NoAliasing(t *testing.T) {
	// A client-chosen document_id that looks like a memory key must not alias the memory subject (N162).
	var h [32]byte
	mem := id.MemorySubject("d", h)
	doc := id.DocumentSubject(id.DocumentID(mem.Key))
	if mem.String() == doc.String() || mem == doc {
		t.Fatalf("document %q and memory %q subjects alias", doc, mem)
	}
	back, err := id.ParseSubject(doc.String())
	if err != nil || back.Class != id.SubjectDocument || back.Key != mem.Key {
		t.Fatalf("round trip of the document subject = %+v, %v", back, err)
	}
}

func TestParseSubject_Rejects(t *testing.T) {
	for _, bad := range []string{"", "document", "document:", "bogus:x", ":x", "Document:x"} {
		if _, err := id.ParseSubject(bad); !errors.Is(err, id.ErrInvalid) {
			t.Errorf("ParseSubject(%q) error = %v, want ErrInvalid", bad, err)
		}
	}
}

func TestMemoryRef(t *testing.T) {
	f, o, c := id.NewFactID(), id.NewObservationID(), id.NewChunkID()
	refs := []struct {
		r    id.MemoryRef
		kind id.MemoryKind
		str  string
	}{
		{id.FactRef(f), id.KindFact, "fact:" + f.String()},
		{id.ObservationRef(o), id.KindObservation, "observation:" + o.String()},
		{id.ChunkRef(c), id.KindChunk, "chunk:" + c.String()},
	}
	for _, x := range refs {
		if x.r.Kind != x.kind || x.r.String() != x.str {
			t.Errorf("ref = %+v (%s), want kind %v and %q", x.r, x.r, x.kind, x.str)
		}
	}
	// The same UUID under two kinds is two distinct refs, so a map keyed by MemoryRef cannot confuse them.
	same := uuid.New()
	if (id.MemoryRef{Kind: id.KindFact, ID: same}) == (id.MemoryRef{Kind: id.KindChunk, ID: same}) {
		t.Error("refs of different kinds must not compare equal")
	}
}

func TestVersionsAreDistinctTypes(t *testing.T) {
	// Compile-time documentation: these assignments only type-check through explicit conversions.
	d := id.DocVersion(3)
	o := id.ObsVersion(d)
	p := id.PageVersion(o)
	s := id.SnapshotVersion(p)
	if int64(s) != 3 {
		t.Fatal("conversion changed the value")
	}
}

// TestUUIDIDs_VersionAndNil: D1 makes every entity id a UUIDv7 and the zero value means "absent", so Parse and
// FromBytes refuse the nil UUID, other versions and other variants. The one exception is OperationID, which section
// 4.1.3 accepts as "any RFC 4122 UUID, UUIDv7 recommended" (the server's own per-document operation ids are UUIDv5).
func TestUUIDIDs_VersionAndNil(t *testing.T) {
	nilID := "00000000-0000-0000-0000-000000000000"
	v4 := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479").String()
	v5 := uuid.NewSHA1(uuid.NameSpaceURL, []byte("item/0")).String()
	ncs := "01925c1e-0000-7000-0000-000000000000" // version 7 digit, variant bits 0xxx (NCS), not RFC 4122
	for _, c := range cases() {
		anyVersion := c.name == "OperationID"
		if _, _, err := c.parse(nilID); !errors.Is(err, id.ErrInvalid) {
			t.Errorf("%s: the nil UUID must be refused, got %v", c.name, err)
		}
		if _, err := c.fromBytes(make([]byte, 16)); !errors.Is(err, id.ErrInvalid) {
			t.Errorf("%s: 16 zero bytes must be refused, got %v", c.name, err)
		}
		if _, _, err := c.parse(ncs); !errors.Is(err, id.ErrInvalid) {
			t.Errorf("%s: a non-RFC 4122 variant must be refused, got %v", c.name, err)
		}
		for _, s := range []string{v4, v5} {
			_, _, err := c.parse(s)
			if anyVersion && err != nil {
				t.Errorf("%s: %s must be accepted (section 4.1.3): %v", c.name, s, err)
			}
			if !anyVersion && !errors.Is(err, id.ErrInvalid) {
				t.Errorf("%s: %s is not a UUIDv7 and must be refused, got %v", c.name, s, err)
			}
		}
		b := uuid.MustParse(v4)
		if _, err := c.fromBytes(b[:]); (err == nil) != anyVersion {
			t.Errorf("%s: FromBytes of a v4 id = %v, want accepted only for OperationID", c.name, err)
		}
	}
}

func TestParseSubject_ValidatesKeyPerClass(t *testing.T) {
	ns := id.NewNamespaceID().String()
	h := strings.Repeat("ab", 32)
	good := []string{"document:doc-1", "document:a:b:c", "memory:doc-1:" + h, "memory:with:colons:in:doc:" + h,
		"namespace:" + ns, "tenant:acme-42"}
	for _, s := range good {
		got, err := id.ParseSubject(s)
		if err != nil || got.String() != s {
			t.Errorf("ParseSubject(%q) = %v, %v", s, got, err)
		}
	}
	bad := []string{
		"memory:doc",                             // no content hash
		"memory:doc:" + h[:62],                   // hash too short
		"memory:doc:" + strings.ToUpper(h),       // hash must be lower case
		"memory:doc:" + strings.Repeat("zz", 32), // not hex
		"memory::" + h,                           // empty document id
		"document:" + strings.Repeat("x", 257),   // document id over 256 bytes
		"namespace:not-a-uuid",                   // not a UUID
		"namespace:" + uuid.NewString(),          // a v4, namespaces are v7
		"tenant:Acme",                            // upper case
		"tenant:" + strings.Repeat("a", 65),      // over 64
		"tenant:acme_corp",                       // underscore
	}
	for _, s := range bad {
		if _, err := id.ParseSubject(s); !errors.Is(err, id.ErrInvalid) {
			t.Errorf("ParseSubject(%q) = %v, want ErrInvalid", s, err)
		}
	}
}

func TestLSN(t *testing.T) {
	for _, tc := range []struct {
		s string
		l id.LSN
	}{{"0/0", 0}, {"16/B374D848", 0x16_B374D848}, {"FFFFFFFF/FFFFFFFF", ^id.LSN(0)}} {
		got, err := id.ParseLSN(tc.s)
		if err != nil || got != tc.l || got.String() != tc.s {
			t.Errorf("ParseLSN(%q) = %v, %v; String() = %q", tc.s, got, err, got.String())
		}
	}
	for _, bad := range []string{"", "16", "G/1", "1/", "100000000/0", "1/2/3"} {
		if _, err := id.ParseLSN(bad); !errors.Is(err, id.ErrInvalid) {
			t.Errorf("ParseLSN(%q) must fail", bad)
		}
	}
}
