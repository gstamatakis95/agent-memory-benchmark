package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"

	"github.com/gstamatakis95/engram/internal/errs"
)

const (
	publicErrorsProto   = "memory/v1/errors.proto"
	internalErrorsProto = "engram/internal/errors/v1/errors.proto"
)

// grpcCodes maps the SCREAMING_CASE code names the proto comments use to the codes of google.golang.org/grpc/codes.
var grpcCodes = func() map[string]codes.Code {
	m := map[string]codes.Code{}
	for c := codes.OK; c <= codes.Unauthenticated; c++ {
		m[screaming(c.String())] = c
	}
	return m
}()

// screaming turns "InvalidArgument" into "INVALID_ARGUMENT".
func screaming(s string) string {
	var b strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToUpper(b.String())
}

var arrowRe = regexp.MustCompile(`^(\w+) → (.*)$`)

// detail is a typed error detail of the proto with the gRPC code(s) its comment names.
type detail struct {
	Name     string
	Codes    []string // SCREAMING_CASE
	Comment  string
	Fields   []string
	File     string
	Internal bool
}

func readDetails(protos *Protos, file string, internal bool) ([]detail, error) {
	f, ok := protos.Files[file]
	if !ok {
		return nil, fmt.Errorf("errors: %s not found", file)
	}
	var out []detail
	ms := f.Messages()
	for i := 0; i < ms.Len(); i++ {
		m := ms.Get(i)
		c := protos.Comment(file, m)
		am := arrowRe.FindStringSubmatch(c)
		if am == nil || am[1] != string(m.Name()) {
			continue
		}
		dt := detail{Name: string(m.Name()), Comment: c, File: file, Internal: internal}
		for _, w := range regexp.MustCompile(`\b[A-Z]+(?:_[A-Z]+)*\b`).FindAllString(firstSentence(am[2]), -1) {
			if _, isCode := grpcCodes[w]; isCode && !contains(dt.Codes, w) {
				dt.Codes = append(dt.Codes, w)
			}
		}
		for j := 0; j < m.Fields().Len(); j++ {
			fd := m.Fields().Get(j)
			dt.Fields = append(dt.Fields, string(fd.Name())+" "+typeName(fd))
		}
		out = append(out, dt)
	}
	return out, nil
}

// kindDetail names the proto detail of an errs.Kind ("" when the kind carries no Engram detail).
func kindDetail(k errs.Kind) string {
	switch k {
	case errs.KindValidation:
		return "ValidationError"
	case errs.KindNotFound, errs.KindQuotaExceeded, errs.KindWrongShardOrEpoch, errs.KindNamespaceFrozen,
		errs.KindNamespaceNotReady, errs.KindOperationConflict, errs.KindPreconditionFailed:
		return k.String()
	case errs.KindFenceBusy, errs.KindDocumentBusy, errs.KindInputBlobMissing:
		return k.String()
	}
	return ""
}

// planErrorCodes reads the typed-detail table of section 4.1.6 of the plan: detail name to the codes of its rows.
func planErrorCodes(plan *Plan) map[string][]string {
	out := map[string][]string{}
	nameRe := regexp.MustCompile("^`(\\w+)")
	codeRe := regexp.MustCompile("^`([A-Z_]+)`")
	for _, l := range strings.Split(plan.section("4.1.6"), "\n") {
		if !strings.HasPrefix(l, "| `") {
			continue
		}
		cells := strings.SplitN(l, "|", 4)
		if len(cells) < 4 {
			continue
		}
		n := nameRe.FindStringSubmatch(strings.TrimSpace(cells[1]))
		c := codeRe.FindStringSubmatch(strings.TrimSpace(cells[2]))
		if n != nil && c != nil && !contains(out[n[1]], c[1]) {
			out[n[1]] = append(out[n[1]], c[1])
		}
	}
	return out
}

// genErrors renders the error-detail table (N139, A-10: one gRPC code per detail type, generated from the proto
// comments and the errs package so that the proto, the Go mapping and the plan cannot disagree) and fails when they do.
func genErrors(protos *Protos, plan *Plan) (string, []string, error) {
	pub, err := readDetails(protos, publicErrorsProto, false)
	if err != nil {
		return "", nil, err
	}
	internal, err := readDetails(protos, internalErrorsProto, true)
	if err != nil {
		return "", nil, err
	}
	var problems []string

	// proto comment vs the Go mapping of internal/errs
	kinds := map[string]errs.Kind{}
	for k := errs.Kind(1); !strings.HasPrefix(k.String(), "Kind("); k++ {
		if n := kindDetail(k); n != "" {
			kinds[n] = k
		}
	}
	for _, dt := range append(append([]detail(nil), pub...), internal...) {
		k, ok := kinds[dt.Name]
		if !ok || len(dt.Codes) == 0 { // an internal detail may name no gRPC code (FenceBusy, InputBlobMissing)
			continue
		}
		got := screaming(k.Code().String())
		if !contains(dt.Codes, got) {
			addf(&problems, "errors: %s: the proto comment says %s, internal/errs maps %s to %s",
				dt.Name, strings.Join(dt.Codes, " or "), k, got)
		}
	}
	// proto comment vs the table of section 4.1.6
	plans := planErrorCodes(plan)
	byName := map[string]detail{}
	for _, dt := range pub {
		byName[dt.Name] = dt
	}
	for _, n := range sortedKeys(plans) {
		dt, ok := byName[n]
		if !ok {
			addf(&problems, "errors: section 4.1.6 lists %s, which %s does not define", n, publicErrorsProto)
			continue
		}
		a, b := append([]string(nil), dt.Codes...), append([]string(nil), plans[n]...)
		sort.Strings(a)
		sort.Strings(b)
		if strings.Join(a, ",") != strings.Join(b, ",") {
			addf(&problems, "errors: %s: the proto comment says %s, section 4.1.6 says %s",
				n, strings.Join(a, " or "), strings.Join(b, " or "))
		}
	}

	d := NewDoc("Error details", "`proto/"+publicErrorsProto+"`, `proto/"+internalErrorsProto+"`, `internal/errs`",
		"PLAN.md 4.1.6; register N128, N139")
	d.Para("Every error is a `google.rpc.Status`; the typed details below ride in `Status.details` with type URL " +
		"`type.googleapis.com/memory.v1.<Name>`. There is one gRPC code per detail type (and per `OperationConflict` " +
		"reason): the code named in the first sentence of each proto comment is checked against `internal/errs` and " +
		"against the table of section 4.1.6, and this generator fails when they disagree.")
	d.H2("Public details")
	var rows [][]string
	for _, dt := range pub {
		rows = append(rows, []string{"`" + dt.Name + "`", "`" + strings.Join(dt.Codes, "` or `") + "`"})
	}
	d.Table([]string{"Typed detail", "gRPC code"}, rows)
	for _, dt := range pub {
		d.Bullet("`" + dt.Name + "`: " + raisedWhen(dt) + " Fields: " + strings.Join(dt.Fields, ", ") + ".")
	}
	d.EndList()
	d.H2("Internal details (never leave the process)")
	for _, dt := range internal {
		lead := firstSentence(arrowRe.FindStringSubmatch(dt.Comment)[2])
		d.Bullet("`" + dt.Name + "`: " + lead + " " + raisedWhen(dt))
	}
	d.EndList()
	d.H2("Kinds of internal/errs")
	rows = nil
	for k := errs.Kind(1); !strings.HasPrefix(k.String(), "Kind("); k++ {
		det := kindDetail(k)
		if det == "" {
			det = "-"
		} else {
			det = "`" + det + "`"
		}
		scope := "public"
		if k.IsInternal() {
			scope = "internal"
		}
		rows = append(rows, []string{"`" + k.String() + "`", "`" + screaming(k.Code().String()) + "`", det, scope})
	}
	d.Table([]string{"Kind", "gRPC code", "Detail", "Scope"}, rows)
	d.Para("`OperationConflict` is `ALREADY_EXISTS` instead of `ABORTED` when its reason is `IDEMPOTENCY_KEY_REUSED`.")
	return d.String(), problems, nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// raisedWhen is the sentence of the proto comment that follows the "Name → CODE." sentence.
func raisedWhen(dt detail) string {
	rest := strings.TrimSpace(strings.TrimPrefix(arrowRe.FindStringSubmatch(dt.Comment)[2],
		firstSentence(arrowRe.FindStringSubmatch(dt.Comment)[2])))
	return firstSentence(rest)
}
