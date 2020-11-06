package ksql

import (
	"strings"
)

func parseSchemaKeys(str string) []string {
	var keys []string
	b := strings.Builder{}
	write := false
	for _, r := range str {
		if r == '`' {
			write = !write
			if !write {
				keys = append(keys, b.String())
				b.Reset()
			}
		} else if write {
			b.WriteRune(r)
		}

	}
	return keys
}
