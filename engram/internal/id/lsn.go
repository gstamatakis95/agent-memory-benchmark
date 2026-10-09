package id

import (
	"fmt"
	"strconv"
	"strings"
)

// LSN is a Postgres write-ahead-log position (pg_lsn). It lives in the id leaf because both catalog (the move ledger's
// floor, N179) and store (Replication.Replayed) name it, and the section 2.1 edge list lets neither import the other
// (CONFLICTS.md #6: one shared type, no new package or edge).
type LSN uint64

// String renders the pg_lsn text form "HI/LO" in upper-case hexadecimal.
func (l LSN) String() string { return fmt.Sprintf("%X/%X", uint32(l>>32), uint32(l)) }

// ParseLSN parses the pg_lsn text form "HI/LO".
func ParseLSN(s string) (LSN, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("%w: lsn %q is not HI/LO", ErrInvalid, s)
	}
	h, err1 := strconv.ParseUint(hi, 16, 32)
	l, err2 := strconv.ParseUint(lo, 16, 32)
	if err1 != nil || err2 != nil {
		return 0, fmt.Errorf("%w: lsn %q is not hexadecimal", ErrInvalid, s)
	}
	return LSN(h<<32 | l), nil
}
