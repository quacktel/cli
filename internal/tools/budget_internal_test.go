package tools

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func synthRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"s": strings.Repeat("x", 100)}
	}

	return rows
}

func TestFitBudgetEmptiesLaterSetsOnceCut(t *testing.T) {
	first, second := synthRows(10), synthRows(2)
	_, rowsDropped := fitBudget(500, false, &first, &second)
	require.True(t, rowsDropped)
	require.NotEmpty(t, first)
	require.Less(t, len(first), 10)
	require.NotNil(t, second)
	require.Empty(t, second)
}

func TestFitBudgetCutsSecondSetWhenFirstFits(t *testing.T) {
	first, second := synthRows(2), synthRows(10)
	_, rowsDropped := fitBudget(500, false, &first, &second)
	require.True(t, rowsDropped)
	require.Len(t, first, 2)
	require.NotEmpty(t, second)
	require.Less(t, len(second), 10)
}
