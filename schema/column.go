package schema

import "go/types"

// Column describes a field in the data
type Column struct {
	Name string
	Kind types.BasicKind
}