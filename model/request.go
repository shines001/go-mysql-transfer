package model

import (
	"sync"

	"github.com/siddontang/go-mysql/mysql"
)

/*
 *sync Pool 是用作存放可重用对象的对象池，可提升性能，存放在里面的对象只在两次GC之间有效
 * Put() 向池子增加一个对象
 * Get() 从池子获取一个对象
 */
var RowRequestPool = sync.Pool{
	New: func() interface{} {
		return new(RowRequest)
	},
}

type RowRequest struct {
	RuleKey   string
	Action    string
	Timestamp uint32
	Old       []interface{}
	Row       []interface{}
	Schema    string
	Query     string
}

type PosRequest struct {
	Name  string
	Pos   uint32
	Force bool
}

type GtidRequest struct {
	Set   mysql.GTIDSet
	Force bool
}

func BuildRowRequest() *RowRequest {
	return RowRequestPool.Get().(*RowRequest)
}

func ReleaseRowRequest(t *RowRequest) {
	RowRequestPool.Put(t)
}
