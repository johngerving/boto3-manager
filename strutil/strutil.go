package strutil

import (
	"regexp"
	"strings"
)

// WildCardToRegexp converts a wildcard expression to an equivalent regular expression
func WildCardToRegexp(pattern string) string {
	patternRegex := regexp.QuoteMeta(pattern)
	patternRegex = strings.Replace(patternRegex, "\\?", ".", -1)
	patternRegex = strings.Replace(patternRegex, "\\*", ".*", -1)
	return patternRegex
}
