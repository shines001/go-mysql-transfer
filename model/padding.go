package model

import "github.com/siddontang/go-mysql/schema"

type Padding struct {
	WrapName string

	ColumnName     string
	ColumnIndex    int
	ColumnType     int
	ColumnMetadata *schema.TableColumn
}

/*
type TableColumn struct {
	Name       string
	Type       int
	Collation  string
	RawType    string
	IsAuto     bool
	IsUnsigned bool
	IsVirtual  bool
	EnumValues []string
	SetValues  []string
	FixedSize  uint
	MaxSize    uint
}

type Table struct {
	Schema string
	Name   string

	Columns   []TableColumn
	Indexes   []*Index
	PKColumns []int

	UnsignedColumns []int
}
*/
