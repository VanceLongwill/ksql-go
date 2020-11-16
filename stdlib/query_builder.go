package stdlib

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

var (
	// ErrMissingSemicolon is returned when the sql statement is missing a trailing semi-colon
	ErrMissingSemicolon = errors.New("statement is missing a trailing semi-colon")
)

// @TODO: this will need revisiting to add more validation logic/be replaced with a less naive implementation
func buildStatement(q string, args []driver.NamedValue) (string, error) {
	if !strings.HasSuffix(q, ";") {
		return "", ErrMissingSemicolon
	}

	var replacements []string

	for _, arg := range args {
		// value must be json serializable
		b, err := json.Marshal(arg.Value)
		// strings get wrapped in double quotes
		s := strings.Trim(string(b), `"`)
		if err != nil {
			return "", err
		}
		if arg.Name != "" {
			replacements = append(replacements, ":"+arg.Name, s)
		} else {
			replacements = append(replacements, "$"+strconv.Itoa(arg.Ordinal), s)
		}
	}

	return strings.NewReplacer(replacements...).Replace(q), nil
}
