package stdlib

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

var (
	ErrMissingSemicolon = errors.New("statement is missing a terminating semi-colon")
)

func buildStatement(q string, args []driver.NamedValue) (string, error) {
	if !strings.HasSuffix(q, ";") {
		return "", ErrMissingSemicolon
	}

	var replacements []string

	for _, arg := range args {
		b, err := json.Marshal(arg.Value)
		if err != nil {
			return "", err
		}
		if arg.Name != "" {
			replacements = append(replacements, ":"+arg.Name, string(b))
		} else {
			replacements = append(replacements, "$"+strconv.Itoa(arg.Ordinal), string(b))
		}
	}

	return strings.NewReplacer(replacements...).Replace(q), nil
}
