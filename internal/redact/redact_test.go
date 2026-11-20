package redact_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/quacktel/cli/internal/redact"
)

func TestLogs(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("Authorization", "Bearer x")
	rl.Resource().Attributes().PutStr("service.name", "svc")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().Attributes().PutStr("my_Token", "t")
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("password=hunter2")
	a := lr.Attributes()
	a.PutStr("X-API-KEY", "k")
	a.PutStr("http.method", "GET")
	a.PutEmptyMap("outer").PutStr("db.Password", "p")
	list := a.PutEmptySlice("list")
	list.AppendEmpty().SetEmptyMap().PutStr("client_secret", "s")
	list.AppendEmpty().SetStr("plain")

	require.Equal(t, 5, redact.Default().Logs(ld))

	ra := rl.Resource().Attributes()
	v, _ := ra.Get("Authorization")
	require.Equal(t, "[REDACTED]", v.Str())
	v, _ = ra.Get("service.name")
	require.Equal(t, "svc", v.Str())
	v, _ = sl.Scope().Attributes().Get("my_Token")
	require.Equal(t, "[REDACTED]", v.Str())
	v, _ = a.Get("X-API-KEY")
	require.Equal(t, "[REDACTED]", v.Str())
	v, _ = a.Get("http.method")
	require.Equal(t, "GET", v.Str())
	v, _ = a.Get("outer")
	inner, _ := v.Map().Get("db.Password")
	require.Equal(t, "[REDACTED]", inner.Str())
	v, _ = a.Get("list")
	el, _ := v.Slice().At(0).Map().Get("client_secret")
	require.Equal(t, "[REDACTED]", el.Str())
	require.Equal(t, "plain", v.Slice().At(1).Str())
	require.Equal(t, "password=hunter2", lr.Body().Str())
}
