package main

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Protos holds the compiled proto workspace with source comments. It is compiled from the .proto files with
// protocompile (the compiler buf uses), so the comments the documents quote are the ones `buf lint` saw.
type Protos struct {
	Files map[string]linker.File // by path relative to proto/, e.g. "memory/v1/errors.proto"
	order []string
}

// LoadProtos compiles every .proto file under dir.
func LoadProtos(dir string) (*Protos, error) {
	var names []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".proto") {
			rel, _ := filepath.Rel(dir, path)
			names = append(names, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("protos: no .proto files under %s", dir)
	}
	c := protocompile.Compiler{
		Resolver:       protocompile.WithStandardImports(&protocompile.SourceResolver{ImportPaths: []string{dir}}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	files, err := c.Compile(context.Background(), names...)
	if err != nil {
		return nil, fmt.Errorf("protos: %w", err)
	}
	p := &Protos{Files: map[string]linker.File{}, order: names}
	for i, f := range files {
		p.Files[names[i]] = f
	}
	return p, nil
}

// Comment returns the leading comment of a descriptor, whitespace-normalised to one paragraph.
func (p *Protos) Comment(file string, d protoreflect.Descriptor) string {
	f, ok := p.Files[file]
	if !ok {
		return ""
	}
	loc := f.SourceLocations().ByDescriptor(d)
	return strings.Join(strings.Fields(loc.LeadingComments), " ")
}

// EnumValues lists the value names of an enum, in declaration order.
func EnumValues(e protoreflect.EnumDescriptor) []string {
	var out []string
	vs := e.Values()
	for i := 0; i < vs.Len(); i++ {
		out = append(out, string(vs.Get(i).Name()))
	}
	return out
}

// FindEnum returns the enum with the given full name ("memory.v1.OperationState") and the file it is declared in.
func (p *Protos) FindEnum(full string) (protoreflect.EnumDescriptor, string, bool) {
	for _, name := range p.order {
		f := p.Files[name]
		es := f.Enums()
		for i := 0; i < es.Len(); i++ {
			if string(es.Get(i).FullName()) == full {
				return es.Get(i), name, true
			}
		}
		if e := findNestedEnum(f.Messages(), full); e != nil {
			return e, name, true
		}
	}
	return nil, "", false
}

func findNestedEnum(ms protoreflect.MessageDescriptors, full string) protoreflect.EnumDescriptor {
	for i := 0; i < ms.Len(); i++ {
		m := ms.Get(i)
		es := m.Enums()
		for j := 0; j < es.Len(); j++ {
			if string(es.Get(j).FullName()) == full {
				return es.Get(j)
			}
		}
		if e := findNestedEnum(m.Messages(), full); e != nil {
			return e
		}
	}
	return nil
}
