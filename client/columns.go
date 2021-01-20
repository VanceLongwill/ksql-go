package client

const unset int = -1

type columns struct {
	count int
	names []string
}

func (c columns) Validate(dest []interface{}) error {
	if c.count == unset {
		return nil
	}
	if len(dest) != c.count {
		return ErrColumnNumberMismatch
	}
	return nil
}

func (c columns) Columns() []string {
	if len(c.names) > 0 &&
		(c.count == unset || len(c.names) == c.count) {
		return c.names
	}
	// if there's no column names provided, just return empty strings
	cols := make([]string, c.count)
	return cols
}
