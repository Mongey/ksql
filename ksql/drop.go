package ksql

import "fmt"

type DropTableRequest struct {
	name string
}

type DropStreamRequest struct {
	name string
}

type dropRequest struct {
	t    KSQLType
	name string
}

func (r *DropTableRequest) query() string {
	return (&dropRequest{t: TableType, name: r.name}).query()
}

func (r *DropStreamRequest) query() string {
	return (&dropRequest{t: StreamType, name: r.name}).query()
}

func (req *dropRequest) query() string {
	return fmt.Sprintf("DROP %s %s;", req.t, req.name)
}
