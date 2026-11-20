package redact_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/quacktel/cli/internal/redact"
)

func TestKeyMatching(t *testing.T) {
	redacted := []string{
		"token", "access_token", "refresh_token", "id_token", "session_token", "X-Auth-Token", "x-csrf-token",
		"http.request.header.x-auth-token", "csrftoken", "github_token", "auth_token_value", "Authorization",
		"X-API-KEY", "api_key", "password", "client_secret", "cookie", "aws_credentials", "bearer_auth",
	}
	kept := []string{
		"input_tokens", "output_tokens", "cache_read_tokens", "cache_creation_tokens", "cache_creation_5m_tokens",
		"total_tokens", "pre_tokens", "post_tokens", "token_count", "max_tokens", "cost_usd", "session.id",
		"prompt_length", "tokenizer",
	}
	for _, k := range redacted {
		t.Run("redacted/"+k, func(t *testing.T) {
			ld := plog.NewLogs()
			lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			lr.Attributes().PutStr(k, "v")
			require.Equal(t, 1, redact.Default().Logs(ld))
			v, _ := lr.Attributes().Get(k)
			require.Equal(t, "[REDACTED]", v.Str())
		})
	}
	for _, k := range kept {
		t.Run("kept/"+k, func(t *testing.T) {
			ld := plog.NewLogs()
			lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			lr.Attributes().PutStr(k, "v")
			require.Zero(t, redact.Default().Logs(ld))
			v, _ := lr.Attributes().Get(k)
			require.Equal(t, "v", v.Str())
		})
	}
}

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

func TestTraces(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("auth_token", "r")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().Attributes().PutStr("auth_token", "s")
	sp := ss.Spans().AppendEmpty()
	sp.SetName("password=hunter2")
	sp.Status().SetMessage("token=abc")
	a := sp.Attributes()
	a.PutStr("auth_token", "a")
	a.PutInt("gen_ai.usage.input_tokens", 42)
	a.PutInt("http.response.status_code", 200)
	a.PutEmptyMap("outer").PutStr("auth_token", "n")
	ev := sp.Events().AppendEmpty()
	ev.Attributes().PutStr("auth_token", "e")
	ln := sp.Links().AppendEmpty()
	ln.Attributes().PutStr("auth_token", "l")

	require.Equal(t, 6, redact.Default().Traces(td))

	for _, m := range []pcommon.Map{rs.Resource().Attributes(), ss.Scope().Attributes(), a, ev.Attributes(), ln.Attributes()} {
		v, _ := m.Get("auth_token")
		require.Equal(t, "[REDACTED]", v.Str())
	}
	v, _ := a.Get("outer")
	inner, _ := v.Map().Get("auth_token")
	require.Equal(t, "[REDACTED]", inner.Str())
	v, _ = a.Get("gen_ai.usage.input_tokens")
	require.EqualValues(t, 42, v.Int())
	v, _ = a.Get("http.response.status_code")
	require.EqualValues(t, 200, v.Int())
	require.Equal(t, "token=abc", sp.Status().Message())
	require.Equal(t, "password=hunter2", sp.Name())
}
