package codec_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	workflowv1 "github.com/gstamatakis95/engram/gen/go/engram/private/workflow/v1"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/workflows/codec"
)

const sentinel = "SENTINEL-Z1-the-quick-brown-fox"

func writeKey(t *testing.T, dir, kid string) {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, kid+".key"), k, 0o600); err != nil {
		t.Fatal(err)
	}
}

// newProvider returns a provider over a fresh key directory holding the given kids, with every namespace on shard 1.
func newProvider(t *testing.T, kids ...string) (*codec.FileKeyProvider, string) {
	t.Helper()
	dir := t.TempDir()
	for _, k := range kids {
		writeKey(t, dir, k)
	}
	return codec.NewFileKeyProvider(dir, codec.NewMemStore(), codec.StaticShard(1)), dir
}

func wfCtx(wf string) converter.SerializationContext {
	return converter.WorkflowSerializationContext{Namespace: "engram", WorkflowID: wf}
}

func nsWf(ns id.NamespaceID) string { return "ns/" + ns.String() + "/op/x" }

func forNS(dc converter.DataConverter, ns id.NamespaceID) converter.DataConverter {
	return converter.WithDataConverterSerializationContext(dc, wfCtx(nsWf(ns)))
}

func chunkWork() *workflowv1.ChunkWork {
	return &workflowv1.ChunkWork{Header: sentinel, Context: "ctx " + sentinel}
}

func TestRoundTripAndNoPlaintext(t *testing.T) {
	keys, _ := newProvider(t, "s1-k1")
	dc := forNS(codec.NewDataConverter(keys), id.NewNamespaceID())
	in := chunkWork()
	pl, err := dc.ToPayloads(in, "plain-string "+sentinel, 42)
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range pl.Payloads {
		raw, _ := proto.Marshal(p)
		if bytes.Contains(raw, []byte(sentinel)) || bytes.Contains(raw, []byte("json")) {
			t.Errorf("payload %d leaks plaintext or its original encoding: %q", i, raw)
		}
		if got := string(p.Metadata[codec.MetadataEncoding]); got != codec.EncodingEncrypted {
			t.Errorf("payload %d encoding = %q, want %q", i, got, codec.EncodingEncrypted)
		}
		if string(p.Metadata[codec.MetadataKeyID]) != "s1-k1" {
			t.Errorf("payload %d kid = %q", i, p.Metadata[codec.MetadataKeyID])
		}
	}
	var out workflowv1.ChunkWork
	var s string
	var n int
	if err := dc.FromPayloads(pl, &out, &s, &n); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, &out) || s != "plain-string "+sentinel || n != 42 {
		t.Errorf("round trip mismatch: %v %q %d", &out, s, n)
	}
}

func TestEncodeFailsClosed(t *testing.T) {
	keys, _ := newProvider(t, "s1-k1", "cell-k1")
	dc := codec.NewDataConverter(keys)
	if _, err := dc.ToPayloads(sentinel); !errors.Is(err, codec.ErrNoScope) {
		t.Errorf("encode with no serialization context: %v, want ErrNoScope", err)
	}
	for _, wf := range []string{"", "ns/not-a-uuid/op/x", "RetainBackfill/42", "move/abc/1", "tenant/acme", "shard/1",
		"shard/1/anything", "shard/x/op-sweeper", "shard/1/op-sweeper/extra", "shard//op-sweeper"} {
		_, err := converter.WithDataConverterSerializationContext(dc, wfCtx(wf)).ToPayloads(sentinel)
		if !errors.Is(err, codec.ErrNoScope) {
			t.Errorf("workflow id %q: %v, want ErrNoScope", wf, err)
		}
	}
	// the explicit cell-wide ids get the cell key
	cellWide := []string{"tenant/acme/delete", "shard/1/op-sweeper", "shard/7/outbox-trim-2026-10-09T00:00:00Z"}
	for _, wf := range cellWide {
		c := converter.WithDataConverterSerializationContext(dc, wfCtx(wf))
		pl, err := c.ToPayloads(sentinel)
		if err != nil {
			t.Errorf("cell-wide id %q: %v", wf, err)
			continue
		}
		p := pl.Payloads[0]
		if string(p.Metadata[codec.MetadataKeyID]) != "cell-k1" || p.Metadata[codec.MetadataNamespace] != nil ||
			bytes.Contains(p.Data, []byte(sentinel)) {
			t.Errorf("cell-wide id %q: payload %v", wf, p)
		}
		var s string
		if err := dc.FromPayloads(pl, &s); err != nil || s != sentinel {
			t.Errorf("cell-wide id %q decodes to %q (%v)", wf, s, err)
		}
	}
}

func TestScopeOfWorkflow(t *testing.T) {
	ns := id.NewNamespaceID()
	type want struct {
		ns id.NamespaceID
		ok bool
	}
	for wf, w := range map[string]want{
		"ns/" + ns.String() + "/op/abc":      {ns, true},
		"ns/" + ns.String() + "/consolidate": {ns, true},
		"ns/" + ns.String() + "/page/p1":     {ns, true},
		"move/" + ns.String() + "/7":         {ns, true},
		"tenant/acme/delete":                 {id.NamespaceID{}, true},
		"shard/1/op-sweeper":                 {id.NamespaceID{}, true},
		"ns/not-a-uuid/op/abc":               {id.NamespaceID{}, false},
		"ns/" + ns.String():                  {id.NamespaceID{}, false},
		"tenant/acme/delete/extra":           {id.NamespaceID{}, false},
		"":                                   {id.NamespaceID{}, false},
	} {
		if got, ok := codec.ScopeOfWorkflow(wf); got != w.ns || ok != w.ok {
			t.Errorf("ScopeOfWorkflow(%q) = %v, %v; want %v, %v", wf, got, ok, w.ns, w.ok)
		}
	}
	keys, _ := newProvider(t, "s1-k1")
	act := converter.WithDataConverterSerializationContext(codec.NewDataConverter(keys),
		converter.ActivitySerializationContext{WorkflowID: nsWf(ns), ActivityType: "CommitChunk"})
	pl, err := act.ToPayloads(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(pl.Payloads[0].Metadata[codec.MetadataNamespace]); got != ns.String() {
		t.Errorf("activity payload namespace = %q, want %q", got, ns)
	}
}

func TestTamperingAndMetadataSwapsAreRejected(t *testing.T) {
	keys, _ := newProvider(t, "s1-k1", "s1-k2")
	a, b := id.NewNamespaceID(), id.NewNamespaceID()
	dc := codec.NewDataConverter(keys)
	pl, err := forNS(dc, a).ToPayloads(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	var s string
	flip := proto.Clone(pl).(*commonpb.Payloads)
	flip.Payloads[0].Data[len(flip.Payloads[0].Data)-1] ^= 1
	if err := dc.FromPayloads(flip, &s); err == nil {
		t.Error("a flipped ciphertext byte decoded")
	}
	swap := proto.Clone(pl).(*commonpb.Payloads)
	swap.Payloads[0].Metadata[codec.MetadataNamespace] = []byte(b.String())
	if err := dc.FromPayloads(swap, &s); err == nil {
		t.Error("a payload re-labelled with another namespace decoded")
	}
	swap = proto.Clone(pl).(*commonpb.Payloads)
	swap.Payloads[0].Metadata[codec.MetadataKeyID] = []byte("s1-k1")
	if err := dc.FromPayloads(swap, &s); err == nil {
		t.Error("a payload re-labelled with another key id decoded")
	}
	short := proto.Clone(pl).(*commonpb.Payloads)
	short.Payloads[0].Data = short.Payloads[0].Data[:5]
	if err := dc.FromPayloads(short, &s); err == nil {
		t.Error("a truncated payload decoded")
	}
}

func TestStrictDecodeRejectsPlaintext(t *testing.T) {
	keys, _ := newProvider(t, "s1-k1")
	plain, err := converter.GetDefaultDataConverter().ToPayloads("not encrypted")
	if err != nil {
		t.Fatal(err)
	}
	var s string
	if err := codec.NewDataConverter(keys).FromPayloads(plain, &s); !errors.Is(err, codec.ErrPlaintext) {
		t.Errorf("strict decode of a plain payload: %v, want ErrPlaintext", err)
	}
	dev := codec.New(keys, codec.Options{AllowPlaintext: true}).Data
	if err := dev.FromPayloads(plain, &s); err != nil || s != "not encrypted" {
		t.Errorf("dev pass-through decode: %q %v", s, err)
	}
}

func TestWorkerWithAnotherShardsKeyCannotDecode(t *testing.T) {
	shardOf := shardMap{}
	a, b := id.NewNamespaceID(), id.NewNamespaceID()
	shardOf[a], shardOf[b] = 1, 2
	dir := t.TempDir()
	for _, k := range []string{"s1-k1", "s2-k1", "cell-k1"} {
		writeKey(t, dir, k)
	}
	store := codec.NewMemStore()
	full := codec.NewDataConverter(codec.NewFileKeyProvider(dir, store, shardOf))
	pa, err := forNS(full, a).ToPayloads(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := forNS(full, b).ToPayloads(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	ka, kb := string(pa.Payloads[0].Metadata[codec.MetadataKeyID]), string(pb.Payloads[0].Metadata[codec.MetadataKeyID])
	if ka != "s1-k1" || kb != "s2-k1" {
		t.Fatalf("kids %q and %q: namespaces of two shards must use their shard's key", ka, kb)
	}
	// a worker of shard 1 holds s1 keys only (same wrapped-key store: the data keys are shared state)
	dir1 := t.TempDir()
	if err := os.Rename(filepath.Join(dir, "s1-k1.key"), filepath.Join(dir1, "s1-k1.key")); err != nil {
		t.Fatal(err)
	}
	w1 := codec.NewDataConverter(codec.NewFileKeyProvider(dir1, store, shardOf))
	var s string
	if err := forNS(w1, a).FromPayloads(pa, &s); err != nil || s != sentinel {
		t.Errorf("the shard-1 worker must read shard 1: %q %v", s, err)
	}
	if err := forNS(w1, b).FromPayloads(pb, &s); !errors.Is(err, codec.ErrUnknownKey) {
		t.Errorf("the shard-1 worker read a shard-2 payload: %v, want ErrUnknownKey", err)
	}
	// and a worker holding a different key under the same kid fails authentication
	other := t.TempDir()
	writeKey(t, other, "s1-k1")
	wrong := codec.NewDataConverter(codec.NewFileKeyProvider(other, store, shardOf))
	if err := forNS(wrong, a).FromPayloads(pa, &s); err == nil {
		t.Error("decoded with a different key under the same kid")
	}
	empty := codec.NewDataConverter(codec.NewFileKeyProvider(t.TempDir(), codec.NewMemStore(), shardOf))
	if _, err := forNS(empty, a).ToPayloads(sentinel); err == nil {
		t.Error("encoded without any codec key")
	}
}

type shardMap map[id.NamespaceID]id.ShardID

func (m shardMap) ShardOf(ns id.NamespaceID) (id.ShardID, bool) { s, ok := m[ns]; return s, ok }

func TestUnknownNamespaceShardRefusesEncode(t *testing.T) {
	dir := t.TempDir()
	writeKey(t, dir, "s1-k1")
	dc := codec.NewDataConverter(codec.NewFileKeyProvider(dir, codec.NewMemStore(), shardMap{}))
	if _, err := forNS(dc, id.NewNamespaceID()).ToPayloads(sentinel); err == nil {
		t.Error("encoded for a namespace the shard resolver does not know")
	}
}

func TestShredMakesPayloadsUnreadableForever(t *testing.T) {
	keys, dir := newProvider(t, "s1-k1")
	a, b := id.NewNamespaceID(), id.NewNamespaceID()
	dc := codec.NewDataConverter(keys)
	pa, err := forNS(dc, a).ToPayloads(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := forNS(dc, b).ToPayloads(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	if err := keys.Shred(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	var s string
	if err := forNS(dc, a).FromPayloads(pa, &s); !errors.Is(err, codec.ErrKeyShredded) {
		t.Errorf("decode after Shred: %v, want ErrKeyShredded", err)
	}
	if _, err := forNS(dc, a).ToPayloads(sentinel); !errors.Is(err, codec.ErrKeyShredded) {
		t.Errorf("encode after Shred: %v, want ErrKeyShredded (no fresh key for a shredded namespace)", err)
	}
	if err := forNS(dc, b).FromPayloads(pb, &s); err != nil || s != sentinel {
		t.Errorf("another namespace must stay readable: %q %v", s, err)
	}
	again := codec.NewFileKeyProvider(dir, keys.Store(), codec.StaticShard(1))
	if err := forNS(codec.NewDataConverter(again), a).FromPayloads(pa, &s); !errors.Is(err, codec.ErrKeyShredded) {
		t.Errorf("fresh provider after Shred: %v", err)
	}
	if err := keys.Shred(context.Background(), id.NamespaceID{}); err == nil {
		t.Error("the cell key must not be shreddable per namespace")
	}
}

// TestFailuresAreSealedUnderTheNamespaceKey is review B1: error messages, stack traces and details were sealed with no
// namespace, so Shred left them readable.
func TestFailuresAreSealedUnderTheNamespaceKey(t *testing.T) {
	keys, _ := newProvider(t, "s1-k1")
	a, b := id.NewNamespaceID(), id.NewNamespaceID()
	cv := codec.New(keys, codec.Options{})
	fcFor := func(ns id.NamespaceID) converter.FailureConverter {
		return converter.WithFailureConverterSerializationContext(cv.Failure, wfCtx(nsWf(ns)))
	}
	secret := "tenant text " + sentinel
	mk := func() error {
		return temporal.NewApplicationError(secret, "ChunkInvalid", "detail "+sentinel)
	}
	fa, fb := fcFor(a).ErrorToFailure(mk()), fcFor(b).ErrorToFailure(mk())
	for _, f := range []interface{ String() string }{fa, fb} {
		if strings.Contains(f.String(), sentinel) {
			t.Fatalf("failure leaks the sentinel: %s", f)
		}
	}
	if got := string(fa.GetEncodedAttributes().GetMetadata()[codec.MetadataNamespace]); got != a.String() {
		t.Errorf("failure attributes sealed for namespace %q, want %q", got, a)
	}
	var appErr *temporal.ApplicationError
	// FailureToError decodes the attributes in place, so the round trip works on a clone
	err := fcFor(a).FailureToError(proto.Clone(fa).(*failurepb.Failure))
	if !errors.As(err, &appErr) || !strings.Contains(appErr.Error(), sentinel) {
		t.Fatalf("failure does not round trip: %v", err)
	}
	if err := keys.Shred(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	// after Shred nothing of the failure of namespace A is readable, whichever converter is asked
	readers := map[string]converter.FailureConverter{"namespace context": fcFor(a), "no context": cv.Failure}
	for name, fc := range readers {
		if err := fc.FailureToError(proto.Clone(fa).(*failurepb.Failure)); err != nil &&
			strings.Contains(err.Error(), sentinel) {
			t.Errorf("%s: the shredded namespace's failure still reads %q", name, err)
		}
		var ae *temporal.ApplicationError
		if err := fc.FailureToError(proto.Clone(fa).(*failurepb.Failure)); errors.As(err, &ae) {
			var d string
			if derr := ae.Details(&d); derr == nil && strings.Contains(d, sentinel) {
				t.Errorf("%s: the details of the shredded failure still read", name)
			}
		}
	}
	err = fcFor(b).FailureToError(proto.Clone(fb).(*failurepb.Failure))
	if err == nil || !strings.Contains(err.Error(), sentinel) {
		t.Errorf("another namespace's failure must stay readable: %v", err)
	}
}

func TestFailureThatCannotBeEncryptedIsWithheldNotFatal(t *testing.T) {
	dir := t.TempDir() // no keys at all
	cv := codec.New(codec.NewFileKeyProvider(dir, codec.NewMemStore(), codec.StaticShard(1)), codec.Options{})
	fc := converter.WithFailureConverterSerializationContext(cv.Failure, wfCtx(nsWf(id.NewNamespaceID())))
	f := fc.ErrorToFailure(temporal.NewApplicationError("secret "+sentinel, "T"))
	if strings.Contains(f.String(), sentinel) {
		t.Errorf("a failure that could not be sealed leaked: %s", f)
	}
	// and with no context at all the same
	if f := cv.Failure.ErrorToFailure(errors.New(sentinel)); strings.Contains(f.String(), sentinel) {
		t.Errorf("context-less failure leaked: %s", f)
	}
}

func TestRotationKeepsOldPayloadsReadable(t *testing.T) {
	keys, dir := newProvider(t, "s1-2026-10-a")
	ns := id.NewNamespaceID()
	dc := forNS(codec.NewDataConverter(keys), ns)
	old, err := dc.ToPayloads(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	writeKey(t, dir, "s1-2026-10-b")
	rotated := codec.NewFileKeyProvider(dir, keys.Store(), codec.StaticShard(1))
	rdc := forNS(codec.NewDataConverter(rotated), ns)
	fresh, err := rdc.ToPayloads(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(fresh.Payloads[0].Metadata[codec.MetadataKeyID]); got != "s1-2026-10-b" {
		t.Errorf("new payload kid = %q, want the rotated key", got)
	}
	var s string
	if err := rdc.FromPayloads(old, &s); err != nil || s != sentinel {
		t.Errorf("old payload after rotation: %q %v", s, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "s1.current"), []byte("s1-2026-10-a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pinned := codec.NewFileKeyProvider(dir, keys.Store(), codec.StaticShard(1))
	if got := pinned.CurrentKeyID(ns); got != "s1-2026-10-a" {
		t.Errorf("CurrentKeyID with a current file = %q", got)
	}
}

func TestDirStoreConcurrentFirstUseAndShredRace(t *testing.T) {
	dir := t.TempDir()
	writeKey(t, dir, "s1-k1")
	root := t.TempDir()
	store := codec.NewDirStore(root)
	ns := id.NewNamespaceID()
	var wg sync.WaitGroup
	keys := make([][]byte, 16)
	for i := range keys {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := codec.NewFileKeyProvider(dir, store, codec.StaticShard(1)) // sixteen "workers", one store
			k, err := p.DataKey(context.Background(), ns, "s1-k1")
			if err != nil {
				t.Error(err)
			}
			keys[i] = k
		}()
	}
	wg.Wait()
	for i := range keys {
		if !bytes.Equal(keys[0], keys[i]) {
			t.Fatalf("worker %d got a different data key: first-use race not resolved", i)
		}
	}
	p := codec.NewFileKeyProvider(dir, store, codec.StaticShard(1))
	if err := p.Shred(context.Background(), ns); err != nil {
		t.Fatal(err)
	}
	_, err := codec.NewFileKeyProvider(dir, store, codec.StaticShard(1)).DataKey(context.Background(), ns, "s1-k1")
	if !errors.Is(err, codec.ErrKeyShredded) {
		t.Errorf("DirStore after Shred: %v", err)
	}
	// review m4: a first use that passed the tombstone check before the Shred must not leave a key behind
	_, err = store.PutIfAbsent(context.Background(), ns, "s1-k1", []byte("late"))
	if !errors.Is(err, codec.ErrKeyShredded) {
		t.Errorf("PutIfAbsent after Shred: %v, want ErrKeyShredded", err)
	}
	if _, err := os.Stat(filepath.Join(root, ns.String(), "s1-k1.wrapped")); !os.IsNotExist(err) {
		t.Errorf("a wrapped key exists after Shred: %v", err)
	}
	mem := codec.NewMemStore()
	if err := mem.Shred(context.Background(), ns); err != nil {
		t.Fatal(err)
	}
	if _, err := mem.PutIfAbsent(context.Background(), ns, "k", []byte("x")); !errors.Is(err, codec.ErrKeyShredded) {
		t.Errorf("MemStore.PutIfAbsent after Shred: %v", err)
	}
}

func TestHexShardKeyAndBadKeyID(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "s1-k1.key"), []byte(strings.Repeat("ab", 32)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := codec.NewFileKeyProvider(dir, codec.NewMemStore(), codec.StaticShard(1))
	if _, err := p.DataKey(context.Background(), id.NewNamespaceID(), "s1-k1"); err != nil {
		t.Errorf("hex shard key: %v", err)
	}
	for _, kid := range []string{"", "../k1", "a/b", ".hidden"} {
		if _, err := p.DataKey(context.Background(), id.NewNamespaceID(), kid); !errors.Is(err, codec.ErrUnknownKey) {
			t.Errorf("kid %q: %v, want ErrUnknownKey", kid, err)
		}
	}
}

// TestWorkerModePanicsInsteadOfFailingTheWorkflow: the SDK fails a workflow whose input does not decode, for good. A
// worker with the wrong keys, or one that meets a plaintext payload, must make the workflow TASK fail instead.
func TestWorkerModePanicsInsteadOfFailingTheWorkflow(t *testing.T) {
	keys, _ := newProvider(t, "s1-k1")
	ns := id.NewNamespaceID()
	pl, err := forNS(codec.NewDataConverter(keys), ns).ToPayloads(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := converter.GetDefaultDataConverter().ToPayloads(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	keyless, _ := newProvider(t)
	worker := forNS(codec.New(keyless, codec.Options{Worker: true}).Data, ns)
	strict := forNS(codec.New(keys, codec.Options{Worker: true}).Data, ns)
	client := forNS(codec.New(keyless, codec.Options{}).Data, ns)
	var s string
	for name, f := range map[string]func() error{
		"unknown key": func() error { return worker.FromPayloads(pl, &s) },
		"plaintext":   func() error { return strict.FromPayloads(plain, &s) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: a worker-mode decoder returned instead of panicking", name)
				}
			}()
			_ = f()
		}()
	}
	if err := client.FromPayloads(pl, &s); !errors.Is(err, codec.ErrUnknownKey) {
		t.Errorf("a client-mode decoder must return the error, got %v", err)
	}
}

type failingKeys struct{ err error }

func (f failingKeys) DataKey(context.Context, id.NamespaceID, string) ([]byte, error) {
	return nil, f.err
}
func (f failingKeys) CurrentKeyID(id.NamespaceID) string          { return "s1-k1" }
func (f failingKeys) Shred(context.Context, id.NamespaceID) error { return nil }

// TestUnsealableFailureKeepsItsRetryability is review M3: a failure that cannot be sealed is replaced by a withheld
// one,
// which must stay retryable unless retrying cannot help, or the Worker-mode panic would fail activities for good.
func TestUnsealableFailureKeepsItsRetryability(t *testing.T) {
	ns := id.NewNamespaceID()
	actCtx := converter.ActivitySerializationContext{WorkflowID: nsWf(ns), ActivityType: "CommitChunk"}
	nonRetryable := func(keys codec.KeyProvider, sc converter.SerializationContext, err error) bool {
		t.Helper()
		cv := codec.New(keys, codec.Options{Worker: true})
		fc := converter.WithFailureConverterSerializationContext(cv.Failure, sc)
		f := fc.ErrorToFailure(err)
		info := f.GetApplicationFailureInfo()
		if info == nil || info.GetType() != "EngramFailureWithheld" || strings.Contains(f.String(), sentinel) {
			t.Fatalf("expected a withheld failure that quotes nothing, got %s", f)
		}
		return info.GetNonRetryable()
	}
	busy := temporal.NewApplicationError("DocumentBusy "+sentinel, "DocumentBusy")
	keyless, _ := newProvider(t)
	if nonRetryable(keyless, actCtx, busy) {
		t.Error("a keyless worker-mode provider must yield a retryable failure")
	}
	if nonRetryable(failingKeys{errors.New("key store down")}, actCtx, busy) {
		t.Error("a transient key store error while sealing a retryable error must yield a retryable failure")
	}
	if nonRetryable(failingKeys{errors.New("key store down")}, actCtx, errors.New("result error "+sentinel)) {
		t.Error("a transient error while sealing a plain error must yield a retryable failure")
	}
	if !nonRetryable(failingKeys{codec.ErrKeyShredded}, actCtx, busy) {
		t.Error("a shredded namespace must yield a non-retryable failure")
	}
	if !nonRetryable(keyless, converter.ActivitySerializationContext{WorkflowID: "bogus/id"}, busy) {
		t.Error("a workflow id with no key scope must yield a non-retryable failure")
	}
	perm := temporal.NewNonRetryableApplicationError("cannot extract "+sentinel, "ChunkInvalid", nil)
	if !nonRetryable(failingKeys{errors.New("key store down")}, actCtx, perm) {
		t.Error("an error that was non-retryable stays non-retryable")
	}
}
