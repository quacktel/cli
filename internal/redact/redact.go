package redact

import (
	"path"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const placeholder = "[REDACTED]"

var defaultGlobs = []string{
	"*authorization*", "*cookie*", "*token", "*token.*", "*_token_*", "*bearer*", "*secret*",
	"*password*", "*api_key*", "*apikey*", "*credential*",
}

type Redactor struct {
	globs []string
}

func New(globs []string) *Redactor {
	return &Redactor{globs: globs}
}

func Default() *Redactor {
	return New(defaultGlobs)
}

// Logs masks matching attribute values in place and returns how many it masked.
func (r *Redactor) Logs(ld plog.Logs) int {
	n := 0
	for _, rl := range ld.ResourceLogs().All() {
		n += r.attrs(rl.Resource().Attributes())
		for _, sl := range rl.ScopeLogs().All() {
			n += r.attrs(sl.Scope().Attributes())
			for _, lr := range sl.LogRecords().All() {
				n += r.attrs(lr.Attributes())
			}
		}
	}
	return n
}

func (r *Redactor) match(key string) bool {
	// Hyphens fold to underscores so header-style keys like X-API-KEY match *api_key*.
	key = strings.ReplaceAll(strings.ToLower(key), "-", "_")
	for _, g := range r.globs {
		if ok, _ := path.Match(g, key); ok {
			return true
		}
	}
	return false
}

func (r *Redactor) attrs(m pcommon.Map) int {
	n := 0
	for k, v := range m.All() {
		if r.match(k) {
			v.SetStr(placeholder)
			n++
			continue
		}
		n += r.value(v)
	}
	return n
}

func (r *Redactor) value(v pcommon.Value) int {
	switch v.Type() {
	case pcommon.ValueTypeMap:
		return r.attrs(v.Map())
	case pcommon.ValueTypeSlice:
		n := 0
		for _, e := range v.Slice().All() {
			n += r.value(e)
		}
		return n
	default:
		return 0
	}
}
