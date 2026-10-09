package api

import (
	"context"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc"

	"github.com/gstamatakis95/engram/internal/errs"
)

// DeadlineLimits are the minimum and the maximum deadline of one method (PLAN.md section 4.1.2, N11).
type DeadlineLimits struct {
	Min, Max time.Duration
}

// DefaultDeadlineLimits is the table of section 4.1.2 for the served methods (minimum .. maximum):
//
//	Recall            200 ms .. 10 s     below 200 ms even the semantic arm cannot finish
//	Retain            500 ms .. 30 s     ledger and operation row insert only
//	Reflect           5 s .. 330 s       300 s wall budget (D12) plus 30 s to stream the answer
//	WaitOperation     1 s .. 65 s        the server wait clamps to deadline - 500 ms and 60 s
//	StreamSnapshot    5 s .. 600 s       resumable by offset
//	DeleteDocument, DeleteNamespace, DeleteTenant, Restore
//	                  1 s .. 40 s        an exclusive lock in one 35 s attempt (N82, N139)
//	everything else   100 ms .. 30 s
func DefaultDeadlineLimits(method string) DeadlineLimits {
	switch method[strings.LastIndexByte(method, '/')+1:] {
	case "Recall":
		return DeadlineLimits{200 * time.Millisecond, 10 * time.Second}
	case "Retain":
		return DeadlineLimits{500 * time.Millisecond, 30 * time.Second}
	case "Reflect":
		return DeadlineLimits{5 * time.Second, 330 * time.Second}
	case "WaitOperation":
		return DeadlineLimits{time.Second, 65 * time.Second}
	case "StreamSnapshot":
		return DeadlineLimits{5 * time.Second, 600 * time.Second}
	case "DeleteDocument", "DeleteNamespace", "DeleteTenant", "Restore":
		return DeadlineLimits{time.Second, 40 * time.Second}
	}
	return DeadlineLimits{100 * time.Millisecond, 30 * time.Second}
}

// DeadlineGuard enforces N11 and PLAN.md section 4.1.2 for every served method: a call without a deadline is
// INVALID_ARGUMENT (ValidationError on the pseudo-field "grpc-timeout", reason MISSING_DEADLINE) before any work is
// done, and a deadline over the method's cap is INVALID_ARGUMENT (reason DEADLINE_TOO_LONG). The cap is enforced, not
// clamped: the register (N11) and section 4.1.2 say a silent clamp "produces DEADLINE_EXCEEDED errors that nobody can
// explain from the client side", and Options.MaxDeadline says "over the cap is INVALID_ARGUMENT, never clamped". (The
// prose of section 1.3 step 1 and of the section 8.2 internal/api row says "clamps"; the register wins, CONFLICTS.md
// #21.) A deadline below the
// method's minimum is INVALID_ARGUMENT (reason OUT_OF_RANGE): "reject rather than always time out".
//
// It reads the deadline from the context, where grpc-go puts `grpc-timeout` and connect-go puts `Connect-Timeout-Ms`,
// so one implementation serves gRPC unary, gRPC stream and Connect. It sits before authz.Interceptor in the chain
// (step 1 of section 1.3). Methods outside memory.v1 and memory.admin.v1 (health, reflection) are not guarded.
type DeadlineGuard struct {
	Options Options
	now     func() time.Time // nil: time.Now (tests inject a clock)
}

// TransitAllowance is the time a call may have lost on the wire before the guard sees it: the server reads the client's
// deadline minus the transit time, so a client that asked for exactly the minimum must not be refused for it. The plan
// gives no figure (CONFLICTS.md #21); 20 ms is the allowance of this implementation (a same-cell hop through Envoy is a
// few milliseconds), applied only to the minimum: a call is refused when remaining + TransitAllowance < Min.
const TransitAllowance = 20 * time.Millisecond

// NewDeadlineGuard builds the guard; o.MaxDeadline overrides the cap of the methods it names (section 4.1.2 defaults
// for the rest).
func NewDeadlineGuard(o Options) DeadlineGuard { return DeadlineGuard{Options: o} }

func guarded(method string) bool {
	return strings.HasPrefix(method, "/memory.v1.") || strings.HasPrefix(method, "/memory.admin.v1.")
}

// Limits returns the limits that apply to method.
func (g DeadlineGuard) Limits(method string) DeadlineLimits {
	l := DefaultDeadlineLimits(method)
	if m, ok := g.Options.MaxDeadline[method]; ok {
		l.Max = m
	}
	return l
}

// CheckRemaining validates a deadline that has remaining time left. It is the pure core of Check.
func (g DeadlineGuard) CheckRemaining(method string, remaining time.Duration) error {
	l := g.Limits(method)
	switch {
	case remaining <= 0:
		return errs.DeadlineExceeded("the deadline has already elapsed", nil)
	case remaining > l.Max:
		return errs.ValidationReason(errs.DeadlineField, errs.ReasonDeadlineTooLong,
			fmt.Sprintf("the deadline exceeds the %s cap of %s", l.Max, shortName(method)))
	case remaining+TransitAllowance < l.Min:
		return errs.ValidationReason(errs.DeadlineField, errs.ReasonOutOfRange,
			fmt.Sprintf("the deadline is below the %s minimum of %s", l.Min, shortName(method)))
	}
	return nil
}

// Check validates the deadline of ctx for method.
func (g DeadlineGuard) Check(ctx context.Context, method string) error {
	if !guarded(method) {
		return nil
	}
	dl, ok := ctx.Deadline()
	if !ok {
		return errs.ValidationReason(errs.DeadlineField, errs.ReasonMissingDeadline,
			"every call must carry a deadline (grpc-timeout or Connect-Timeout-Ms)")
	}
	now := time.Now
	if g.now != nil {
		now = g.now
	}
	return g.CheckRemaining(method, dl.Sub(now()))
}

func shortName(method string) string { return method[strings.LastIndexByte(method, '/')+1:] }

// Unary returns the gRPC unary interceptor.
func (g DeadlineGuard) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if err := g.Check(ctx, info.FullMethod); err != nil {
			return nil, errs.ToStatus(err).Err()
		}
		return h(ctx, req)
	}
}

// Stream returns the gRPC stream interceptor; the deadline is checked when the stream opens.
func (g DeadlineGuard) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if err := g.Check(ss.Context(), info.FullMethod); err != nil {
			return errs.ToStatus(err).Err()
		}
		return h(srv, ss)
	}
}

// Connect returns the Connect interceptor (Connect-Timeout-Ms maps to the context deadline inside connect-go).
func (g DeadlineGuard) Connect() connect.Interceptor { return deadlineConnect{g} }

type deadlineConnect struct{ g DeadlineGuard }

func (d deadlineConnect) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if err := d.g.Check(ctx, req.Spec().Procedure); err != nil {
			return nil, errs.ToConnect(err)
		}
		return next(ctx, req)
	}
}

func (d deadlineConnect) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (d deadlineConnect) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if err := d.g.Check(ctx, conn.Spec().Procedure); err != nil {
			return errs.ToConnect(err)
		}
		return next(ctx, conn)
	}
}
