package ksql

import "fmt"

type DropTableRequest struct {
	Name string
}

type DropStreamRequest struct {
	Name string
}

type dropRequest struct {
	t    KSQLType
	name string
}

func (r *DropTableRequest) query() string {
	return (&dropRequest{t: TableType, name: r.Name}).query()
}

func (r *DropStreamRequest) query() string {
	return (&dropRequest{t: StreamType, name: r.Name}).query()
}

func (req *dropRequest) query() string {
	return fmt.Sprintf("DROP %s %s;", req.t, req.name)
}
