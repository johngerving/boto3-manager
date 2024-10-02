package strutil

import (
	"testing"
)

func TestWildCardToRegexp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		wanted  string
	}{
		{
			name:    "empty string",
			pattern: "",
			wanted:  "",
		},
		{
			name:    "main*",
			pattern: "main*",
			wanted:  "main.*",
		},
		{
			name:    "*.txt",
			pattern: "*.txt",
			wanted:  ".*\\.txt",
		},
		{
			name:    "?_main*.txt",
			pattern: "?_main*.txt",
			wanted:  "._main.*\\.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WildCardToRegexp(tt.pattern); got != tt.wanted {
				t.Errorf("WildCardToRegexp(\"%v\") = %v, want %v", tt.pattern, got, tt.wanted)
			}
		})
	}

}
