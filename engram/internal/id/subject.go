package id

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// SubjectClass is the class of a curation or delete subject (N162).
type SubjectClass uint8

// The subject classes. The zero value is invalid on purpose. The key of SubjectDocument is the document_id; of
// SubjectMemory `document_id ':' hex(content_hash)` (a fact and its twins are ONE subject); of SubjectNamespace the
// namespace id; of SubjectTenant the tenant id.
const (
	SubjectDocument SubjectClass = iota + 1
	SubjectMemory
	SubjectNamespace
	SubjectTenant
)

var subjectClassNames = [...]string{SubjectDocument: "document", SubjectMemory: "memory", SubjectNamespace: "namespace",
	SubjectTenant: "tenant"}

// String returns the class name used in the `class:key` encoding.
func (c SubjectClass) String() string {
	if c == 0 || int(c) >= len(subjectClassNames) {
		return fmt.Sprintf("subject_class(%d)", uint8(c))
	}
	return subjectClassNames[c]
}

// Subject is the typed curation and delete subject (N162). It is encoded `class:key` in subject locks, deletion_log and
// intents, so a client-chosen document_id cannot alias the subject of a fact.
type Subject struct {
	Class SubjectClass
	Key   string
}

// DocumentSubject is the subject of a document delete.
func DocumentSubject(d DocumentID) Subject { return Subject{Class: SubjectDocument, Key: string(d)} }

// MemorySubject is the subject of a fact: (document_id, content_hash). The hash is rendered as lower-case hex.
func MemorySubject(d DocumentID, contentHash [32]byte) Subject {
	return Subject{Class: SubjectMemory, Key: string(d) + ":" + hex.EncodeToString(contentHash[:])}
}

// NamespaceSubject is the subject of a namespace delete.
func NamespaceSubject(ns NamespaceID) Subject {
	return Subject{Class: SubjectNamespace, Key: ns.String()}
}

// TenantSubject is the subject of a tenant delete.
func TenantSubject(t TenantID) Subject { return Subject{Class: SubjectTenant, Key: string(t)} }

// String encodes the subject as `class:key`.
func (s Subject) String() string { return s.Class.String() + ":" + s.Key }

// ParseSubject decodes `class:key` and validates the key for its class, so a malformed subject read back from an intent
// or a deletion_log row is refused: document is a DocumentID (1 to 256 bytes); memory is `document_id ':' content_hash`
// with a 64-digit lower-case hex hash (the document_id may itself contain ':', so the LAST colon splits); namespace is
// a UUIDv7; tenant matches [a-z0-9-]{1,64}. Only the first colon separates the class.
func ParseSubject(s string) (Subject, error) {
	cls, key, ok := strings.Cut(s, ":")
	if !ok || key == "" {
		return Subject{}, fmt.Errorf("%w: subject %q is not class:key", ErrInvalid, s)
	}
	for c := SubjectDocument; int(c) < len(subjectClassNames); c++ {
		if subjectClassNames[c] != cls {
			continue
		}
		if err := validateSubjectKey(c, key); err != nil {
			return Subject{}, fmt.Errorf("%w: subject %q: %w", ErrInvalid, s, err)
		}
		return Subject{Class: c, Key: key}, nil
	}
	return Subject{}, fmt.Errorf("%w: unknown subject class %q", ErrInvalid, cls)
}

func validateSubjectKey(c SubjectClass, key string) error {
	switch c {
	case SubjectDocument:
		_, err := ParseDocumentID(key)
		return err
	case SubjectMemory:
		i := strings.LastIndex(key, ":")
		if i < 0 {
			return fmt.Errorf("memory key %q has no content hash", key)
		}
		if _, err := ParseDocumentID(key[:i]); err != nil {
			return err
		}
		h := key[i+1:]
		if raw, err := hex.DecodeString(h); err != nil || len(raw) != 32 || h != strings.ToLower(h) {
			return fmt.Errorf("content hash %q is not 64 lower-case hex digits", h)
		}
		return nil
	case SubjectNamespace:
		_, err := ParseNamespaceID(key)
		return err
	case SubjectTenant:
		_, err := ParseTenantID(key)
		return err
	}
	return fmt.Errorf("class %v", c)
}
