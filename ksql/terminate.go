package ksql

import (
	"fmt"
)

type TerminateRequest struct {
	Name string
}

func (req *TerminateRequest) query() string {
	return fmt.Sprintf("TERMINATE %s;", req.Name)
}
