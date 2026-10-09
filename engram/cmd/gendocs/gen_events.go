package main

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

const eventsProto = "engram/internal/events/v1/events.proto"

// typeName renders a field type the way a reader of the proto expects it.
func typeName(fd protoreflect.FieldDescriptor) string {
	var t string
	switch {
	case fd.IsMap():
		t = fmt.Sprintf("map<%s, %s>", typeName(fd.MapKey()), typeName(fd.MapValue()))
		return t
	case fd.Message() != nil:
		t = string(fd.Message().Name())
	case fd.Enum() != nil:
		t = string(fd.Enum().Name())
	default:
		t = fd.Kind().String()
	}
	if fd.IsList() {
		return "repeated " + t
	}
	return t
}

// genEvents renders the events table from events.proto: the envelope, one row per payload variant of the oneof, and
// every payload message with its fields. The proto is the source of truth (register N80, N115, N128: thin, bounded,
// paged events); this file is what the plan's section 5.6 and the consumers read.
func genEvents(protos *Protos) (string, error) {
	f, ok := protos.Files[eventsProto]
	if !ok {
		return "", fmt.Errorf("events: %s not found", eventsProto)
	}
	var event protoreflect.MessageDescriptor
	ms := f.Messages()
	for i := 0; i < ms.Len(); i++ {
		if ms.Get(i).Name() == "Event" {
			event = ms.Get(i)
		}
	}
	if event == nil {
		return "", fmt.Errorf("events: message Event not found in %s", eventsProto)
	}
	d := NewDoc("Outbox events", "`proto/"+eventsProto+"`", "register N80, N115, N124, N128")
	d.Para("Every row of the per-shard `outbox` is one `Event` (decision D6). Events are thin: ids, hashes and " +
		"small scalars, never text or vectors. The envelope below is followed by the payload variants of its " +
		"`oneof payload` and by every payload message. Fields suffixed `_bytes` are 16-byte ids (N80); a message " +
		"with `page` and `page_count` is paged and a consumer treats a group as complete only when every page " +
		"was seen.")

	d.H2("Envelope")
	var rows [][]string
	fs := event.Fields()
	for i := 0; i < fs.Len(); i++ {
		fd := fs.Get(i)
		if fd.ContainingOneof() != nil {
			continue
		}
		rows = append(rows, []string{fmt.Sprint(fd.Number()), "`" + string(fd.Name()) + "`", typeName(fd)})
	}
	d.Table([]string{"No.", "Field", "Type"}, rows)

	d.H2("Payload variants")
	rows = nil
	var payloads []protoreflect.MessageDescriptor
	for i := 0; i < fs.Len(); i++ {
		fd := fs.Get(i)
		if fd.ContainingOneof() == nil || fd.Message() == nil {
			continue
		}
		m := fd.Message()
		payloads = append(payloads, m)
		paged, elided := "", ""
		if m.Fields().ByName("page") != nil && m.Fields().ByName("page_count") != nil {
			paged = "yes"
		}
		if m.Fields().ByName("ids_elided") != nil {
			elided = "yes"
		}
		rows = append(rows, []string{fmt.Sprint(fd.Number()), "`" + string(fd.Name()) + "`",
			"`" + string(m.Name()) + "`", fmt.Sprint(m.Fields().Len()), paged, elided})
	}
	d.Table([]string{"No.", "Variant", "Message", "Fields", "Paged", "Elides ids"}, rows)
	d.Para("The oneof has %d variants. Unknown variants are skipped by consumers and counted in "+
		"`events_unknown_payload_total`.", len(payloads))

	d.H2("Messages")
	for _, m := range payloads {
		d.H3(string(m.Name()))
		if c := protos.Comment(eventsProto, m); c != "" {
			d.Para("%s", c)
		}
		var frows [][]string
		for i := 0; i < m.Fields().Len(); i++ {
			fd := m.Fields().Get(i)
			frows = append(frows, []string{fmt.Sprint(fd.Number()), "`" + string(fd.Name()) + "`", typeName(fd)})
		}
		d.Table([]string{"No.", "Field", "Type"}, frows)
		any := false
		for i := 0; i < m.Fields().Len(); i++ {
			fd := m.Fields().Get(i)
			if c := protos.Comment(eventsProto, fd); c != "" {
				d.Bullet("`" + string(fd.Name()) + "`: " + c)
				any = true
			}
		}
		if any {
			d.EndList()
		}
	}
	var enums []string
	es := f.Enums()
	for i := 0; i < es.Len(); i++ {
		e := es.Get(i)
		enums = append(enums, "`"+string(e.Name())+"`: "+strings.Join(EnumValues(e), ", "))
	}
	if len(enums) > 0 {
		d.H2("Enums")
		for _, e := range enums {
			d.Bullet(e)
		}
		d.EndList()
	}
	return d.String(), nil
}
