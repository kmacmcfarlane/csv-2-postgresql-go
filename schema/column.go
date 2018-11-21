package schema

import "go/types"

type Column struct {
	Name string
	Kind types.BasicKind
}