package strutil

import (
	"io/fs"
	"regexp"
)

var replaces = regexp.MustCompile(`(\.)|(\*\*\/)|(\*)|([^\/\*\?]+)|(\/)|(\?)`)

func WildCardToRegexp(pattern string) string {
	pat := replaces.ReplaceAllStringFunc(pattern, func(s string) string {
		switch s {
		case "/":
			return "\\/"
		case ".":
			return "\\."
		case "**/":
			return ".*"
		case "?":
			return "[^\\/]"
		case "*":
			return "[^\\/]*"
		default:
			return s
		}
	})
	return "^" + pat + "$"
}

// Glob returns a list of files matching the pattern.
// The pattern can include **/ to match any number of directories.
func Glob(inputFS fs.FS, pattern string) ([]string, error) {
	files := []string{}

	regexpPat := regexp.MustCompile(WildCardToRegexp(pattern))

	err := fs.WalkDir(inputFS, ".", func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() || err != nil {
			return nil
		}
		if regexpPat.MatchString(path) {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}
