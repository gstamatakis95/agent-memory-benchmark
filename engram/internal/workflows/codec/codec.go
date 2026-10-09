// Package codec is the Temporal payload codec of PLAN.md section 2.2.17 (register N59, N99): every payload that
// reaches Temporal's persistence, failures (messages, stack traces, details) included, is AES-256-GCM ciphertext under
// a per-namespace data key, which is itself wrapped by a per-shard codec key (the `kid` in the payload metadata).
// Deleting a namespace's wrapped data key is the shredding step of a namespace or tenant delete: the histories stay in
// Temporal until retention expires them (7 days) but can never be read again.
//
// The codec chooses the data key from the workflow id the SDK hands it through the serialization context
// ("ns/{ns}/op/{op}", "ns/{ns}/consolidate", "move/{ns}/{epoch}", ...). It fails closed: a workflow id that is neither
// namespaced nor on the explicit list of cell-wide ids (ScopeOfWorkflow), and an encode with no context at all, are
// errors, never a silent fallback to a key that Shred cannot destroy. Cell-wide workflows ("tenant/{t}/delete",
// "shard/{n}/..." schedules) are encrypted under the cell key. Decoding is independent of the context: the namespace
// and the key id are read from the payload metadata, and both are bound into the GCM additional data, so a payload
// cannot be replayed under another namespace's key. Decoding is strict by default: a payload that is not encrypted is
// an error, so a client built without the codec fails at its first workflow task instead of writing cleartext
// histories; the dev stack's tools can opt into the SDK's usual pass-through with Options.AllowPlaintext.
package codec

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	"github.com/gstamatakis95/engram/internal/id"
)

// Payload metadata written by the codec. Everything else about a payload, its own encoding and message type
// included, is inside the ciphertext.
const (
	// MetadataEncoding is the key Temporal itself uses for the encoding marker.
	MetadataEncoding = "encoding"
	// EncodingEncrypted is the marker value of an encrypted payload (PLAN.md section 8.3).
	EncodingEncrypted = "binary/encrypted"
	// MetadataKeyID carries the shard codec key id (`kid`, section 9.1 secrets table).
	MetadataKeyID = "encryption-key-id"
	// MetadataNamespace carries the namespace id whose data key encrypted the payload; absent for the shard-level key.
	MetadataNamespace = "engram-namespace"
)

// Errors a KeyProvider returns and the codec propagates.
var (
	// ErrKeyShredded means the namespace's data key was destroyed (the shredding step): its payloads are unreadable.
	ErrKeyShredded = errors.New("codec: namespace data key shredded")
	// ErrUnknownKey means the shard codec key `kid` is not available to this process.
	ErrUnknownKey = errors.New("codec: unknown codec key id")
	// ErrNoScope means a payload was encoded outside any serialization context, or for a workflow id that is neither
	// namespaced nor cell-wide: the codec refuses instead of picking a key Shred cannot destroy.
	ErrNoScope = errors.New("codec: no namespace for this payload")
	// ErrPlaintext means a payload that was not encrypted reached a strict decoder.
	ErrPlaintext = errors.New("codec: payload is not encrypted")
)

// KeyTimeout bounds one key lookup made on behalf of a payload, which the SDK gives no context for.
const KeyTimeout = 10 * time.Second

// KeyProvider supplies the per-namespace data keys. The zero namespace is the shard-level key. PLAN.md section 2.2.17
// declares it as workflows.KeyProvider, which aliases this type.
type KeyProvider interface {
	// DataKey returns the 32-byte data key of ns under codec key keyID; ErrKeyShredded after Shred.
	DataKey(ctx context.Context, ns id.NamespaceID, keyID string) ([]byte, error)
	// CurrentKeyID is the codec key id new payloads of ns are encrypted under.
	CurrentKeyID(ns id.NamespaceID) string
	// Shred is the shredding step of a namespace or tenant delete.
	Shred(ctx context.Context, ns id.NamespaceID) error
}

// Options of the converters.
type Options struct {
	// AllowPlaintext lets Decode pass unencrypted payloads through, as the SDK's codec contract usually asks. It is a
	// dev-stack switch for tools that talk to workflows started without the codec; Engram workers leave it off.
	AllowPlaintext bool
	// Worker marks the converters of a Temporal worker. The SDK turns a decode error of a workflow input into the
	// permanent failure of the workflow, so a worker that holds the wrong keys, or meets a payload that was never
	// encrypted, would kill workflows that a correctly configured worker could run. With Worker set, those two errors
	// (ErrUnknownKey, ErrPlaintext) panic instead; the SDK recovers a panic in workflow code as a failed workflow
	// TASK, which the server retries on another worker, and in an activity as a retried activity attempt. Clients
	// (starters, engramctl) leave it off and get an error. Safe only inside the SDK's task handlers and under the
	// SDK's default panic policy (worker.BlockWorkflow, which the Engram worker must keep); a process that decodes
	// outside a worker task - a client calling Get on a run, a tool reading histories - must not set it, or the panic
	// reaches its caller, and with worker.FailWorkflow the workflow would fail for good.
	Worker bool
}

// Converters are the two halves that must be installed together: the failure converter carries the error messages and
// stack traces of failed activities and workflows into the history, so one built without the codec would leave
// tenant text in the clear.
type Converters struct {
	Data    converter.DataConverter
	Failure converter.FailureConverter
}

// New returns the encrypting data converter and the matching failure converter.
func New(keys KeyProvider, o Options) Converters {
	dc := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), &payloadCodec{keys: keys, opts: o})
	return Converters{Data: dc, Failure: newFailureConverter(dc)}
}

// Apply installs both converters in client (and worker) options.
func (c Converters) Apply(o *client.Options) {
	o.DataConverter = c.Data
	o.FailureConverter = c.Failure
}

// NewDataConverter returns only the data converter of New(keys, Options{}); prefer New, which also returns the failure
// converter (PLAN.md section 2.2.17 declares DataConverter(keys) alone).
func NewDataConverter(keys KeyProvider) converter.DataConverter { return New(keys, Options{}).Data }

// failureConverter is the default failure converter with common attributes encoded, rebuilt over the namespace-aware
// data converter whenever the SDK hands it a serialization context. SDK 1.45's DefaultFailureConverter does not
// implement that interface itself, so without this type failures would be sealed with no namespace and survive Shred.
type failureConverter struct {
	base converter.DataConverter
	cur  *temporal.DefaultFailureConverter
}

func newFailureConverter(dc converter.DataConverter) *failureConverter {
	return &failureConverter{base: dc, cur: temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{
		DataConverter: dc, EncodeCommonAttributes: true,
	})}
}

// WithSerializationContext implements converter.FailureConverterWithSerializationContext.
func (f *failureConverter) WithSerializationContext(sc converter.SerializationContext) converter.FailureConverter {
	return newFailureConverter(converter.WithDataConverterSerializationContext(f.base, sc))
}

// ErrorToFailure implements converter.FailureConverter. The SDK panics when the codec cannot encode a failure (no key,
// no namespace, a key store that is down); a worker must not die for that, so the failure is replaced by one that
// quotes nothing. The replacement stays RETRYABLE, because most causes are transient or are a worker that is wrongly
// configured, and failing the activity for good would defeat Options.Worker. It is non-retryable only when retrying
// cannot help - the namespace was shredded or the workflow id has no key scope - or when the original error already was
// a non-retryable application error.
func (f *failureConverter) ErrorToFailure(err error) (out *failurepb.Failure) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		rerr, _ := r.(error)
		var orig *temporal.ApplicationError
		final := errors.Is(rerr, ErrKeyShredded) || errors.Is(rerr, ErrNoScope) ||
			(errors.As(err, &orig) && orig.NonRetryable())
		out = &failurepb.Failure{
			Message: "failure withheld: it could not be encrypted", Source: "GoSDK",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Type: "EngramFailureWithheld", NonRetryable: final},
			},
		}
	}()
	return f.cur.ErrorToFailure(err)
}

// FailureToError implements converter.FailureConverter.
func (f *failureConverter) FailureToError(fail *failurepb.Failure) error {
	return f.cur.FailureToError(fail)
}

type payloadCodec struct {
	keys   KeyProvider
	opts   Options
	scoped bool           // the SDK gave a serialization context
	ns     id.NamespaceID // zero: a cell-wide workflow (the cell key)
	err    error          // the context named a workflow this codec refuses to encrypt for
}

// WithSerializationContext implements converter.PayloadCodecWithSerializationContext: the workflow id selects the
// namespace whose data key encrypts what is encoded next.
func (c *payloadCodec) WithSerializationContext(sc converter.SerializationContext) converter.PayloadCodec {
	var wf string
	switch v := sc.(type) {
	case converter.WorkflowSerializationContext:
		wf = v.WorkflowID
	case converter.ActivitySerializationContext:
		wf = v.WorkflowID
	}
	ns, ok := ScopeOfWorkflow(wf)
	n := &payloadCodec{keys: c.keys, opts: c.opts, scoped: true, ns: ns}
	if !ok {
		n.err = fmt.Errorf("%w: workflow id %q is neither namespaced nor cell-wide", ErrNoScope, wf)
	}
	return n
}

// ScopeOfWorkflow extracts the namespace of an Engram workflow id: "ns/{ns}/..." and "move/{ns}/{epoch}". The ids of
// cell-wide control workflows - "tenant/{tenant}/delete" and the named schedules "shard/{n}/{name}[-time]" - have no
// namespace and yield the zero NamespaceID (the cell key) with ok. Any other id, a malformed namespace included, is not
// ok.
func ScopeOfWorkflow(workflowID string) (ns id.NamespaceID, ok bool) {
	parts := strings.Split(workflowID, "/")
	switch {
	case len(parts) >= 3 && (parts[0] == "ns" || parts[0] == "move"):
		ns, err := id.ParseNamespaceID(parts[1])
		return ns, err == nil
	case len(parts) == 3 && parts[0] == "tenant" && parts[1] != "" && parts[2] == "delete":
		return id.NamespaceID{}, true
	case len(parts) == 3 && parts[0] == "shard" && isNumber(parts[1]) && isCellSchedule(parts[2]):
		return id.NamespaceID{}, true
	}
	return id.NamespaceID{}, false
}

// cellSchedules are the per-shard schedules of the plan (PLAN.md sections 2.2.17 and 5.9): their workflow ids are
// "shard/{n}/{name}" with the "-<start time>" suffix Temporal appends to a scheduled run.
var cellSchedules = []string{"op-sweeper", "expunge-sweep", "purge-sweep", "consolidate-sweep", "page-cron",
	"outbox-relay", "outbox-trim"}

func isCellSchedule(name string) bool {
	for _, s := range cellSchedules {
		if name == s || strings.HasPrefix(name, s+"-") {
			return true
		}
	}
	return false
}

func isNumber(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func additionalData(ns id.NamespaceID, keyID string) []byte {
	return []byte("engram/payload/v1\x00" + nsString(ns) + "\x00" + keyID)
}

func nsString(ns id.NamespaceID) string {
	if ns.IsZero() {
		return ""
	}
	return ns.String()
}

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("codec: data key is %d bytes, want 32", len(key))
	}
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

// Encode implements converter.PayloadCodec: the whole payload (metadata included) is marshalled and sealed.
func (c *payloadCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	if len(payloads) == 0 {
		return payloads, nil
	}
	if !c.scoped {
		return nil, fmt.Errorf("%w: encode outside a serialization context", ErrNoScope)
	}
	if c.err != nil {
		return nil, c.err
	}
	ctx, cancel := context.WithTimeout(context.Background(), KeyTimeout)
	defer cancel()
	keyID := c.keys.CurrentKeyID(c.ns)
	key, err := c.keys.DataKey(ctx, c.ns, keyID)
	if err != nil {
		return nil, fmt.Errorf("codec: data key for encode: %w", err)
	}
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	ad := additionalData(c.ns, keyID)
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		plain, err := proto.Marshal(p)
		if err != nil {
			return nil, err
		}
		nonce := make([]byte, aead.NonceSize(), aead.NonceSize()+len(plain)+aead.Overhead())
		if _, err := rand.Read(nonce); err != nil {
			return nil, err
		}
		md := map[string][]byte{MetadataEncoding: []byte(EncodingEncrypted), MetadataKeyID: []byte(keyID)}
		if !c.ns.IsZero() {
			md[MetadataNamespace] = []byte(c.ns.String())
		}
		out[i] = &commonpb.Payload{Metadata: md, Data: aead.Seal(nonce, nonce, plain, ad)}
	}
	return out, nil
}

// Decode implements converter.PayloadCodec. Only payloads carrying the encrypted marker are touched.
func (c *payloadCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		if string(p.GetMetadata()[MetadataEncoding]) != EncodingEncrypted {
			if !c.opts.AllowPlaintext {
				err := fmt.Errorf("%w (encoding %q)", ErrPlaintext, p.GetMetadata()[MetadataEncoding])
				if c.opts.Worker {
					panic(err)
				}
				return nil, err
			}
			out[i] = p
			continue
		}
		dec, err := c.decodeOne(p)
		if err != nil {
			if c.opts.Worker && errors.Is(err, ErrUnknownKey) {
				panic(err)
			}
			return nil, err
		}
		out[i] = dec
	}
	return out, nil
}

func (c *payloadCodec) decodeOne(p *commonpb.Payload) (*commonpb.Payload, error) {
	keyID := string(p.Metadata[MetadataKeyID])
	if keyID == "" {
		return nil, errors.New("codec: encrypted payload without a key id")
	}
	var ns id.NamespaceID
	if s := p.Metadata[MetadataNamespace]; len(s) > 0 {
		var err error
		if ns, err = id.ParseNamespaceID(string(s)); err != nil {
			return nil, fmt.Errorf("codec: payload namespace: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), KeyTimeout)
	defer cancel()
	key, err := c.keys.DataKey(ctx, ns, keyID)
	if err != nil {
		return nil, fmt.Errorf("codec: data key for decode: %w", err)
	}
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(p.Data) < aead.NonceSize()+aead.Overhead() {
		return nil, errors.New("codec: encrypted payload is too short")
	}
	nonce, sealed := p.Data[:aead.NonceSize()], p.Data[aead.NonceSize():]
	plain, err := aead.Open(nil, nonce, sealed, additionalData(ns, keyID))
	if err != nil {
		return nil, fmt.Errorf("codec: payload does not authenticate: %w", err)
	}
	res := &commonpb.Payload{}
	if err := proto.Unmarshal(plain, res); err != nil {
		return nil, err
	}
	return res, nil
}
