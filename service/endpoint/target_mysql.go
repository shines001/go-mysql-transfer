/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package endpoint

import (
	//"context"
	//"log"
	//"strings"
	"fmt"
	"sort"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/model"
	"go-mysql-transfer/util/logs"
	"go-mysql-transfer/util/stringutil"
)

type MysqlEndpoint struct {
	client *client.Conn
	lock   sync.Mutex
}

func newMysqlEndpoint() *MysqlEndpoint {

	r := &MysqlEndpoint{}

	r.client = nil

	return r
}

func (s *MysqlEndpoint) Connect() error {
	cfg := global.Cfg()

	var err error
	s.client, err = client.Connect(cfg.MysqlAddr, cfg.MysqlUser, cfg.MysqlPass, "")
	if err != nil {
		logs.Errorf("MysqlEndpoint connetc mysql error: %v, addr: %s,user: %s,pass: %s", err, cfg.MysqlAddr, cfg.MysqlUser, cfg.MysqlPass)
		return err
	}

	return nil
}

func (s *MysqlEndpoint) Ping() error {

	err := s.client.Ping()
	return err
}

func (s *MysqlEndpoint) Close() {
	err := s.client.Close()
	if err != nil {
		logs.Errorf("MysqlEndpoint Close error :%v", err)
	}
}

func (s *MysqlEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	for _, row := range rows {

		//ddl的支持
		if row.Action == "DDL" {
			err := s.client.UseDB(row.Schema)
			if err != nil {
				return errors.Errorf("Consume  Use error: %q", err)
			}
			_, er := s.client.Execute(row.Query)
			if er != nil {
				return errors.Errorf("Consume  ddl  error: %q", er)
			}
			continue
		}

		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		table := rule.MysqlDatabase + "." + rule.MysqlTable
		var sql_text string
		kvm := rowMap(row, rule, false)

		//为了确保绑定变量和数据一一对应
		var fieldList []string      //field列表
		var valueList []interface{} //值列表
		for k, v := range kvm {
			//新的值列表,insert 和 delete只需要kvm,update语句需要旧的值,即old_kvm
			fieldList = append(fieldList, k)
			valueList = append(valueList, v)
		}

		switch row.Action {
		case canal.UpdateAction:
			old_kvm := oldRowMap(row, rule, false)

			for _, k := range fieldList {
				valueList = append(valueList, old_kvm[k])
			}
			sql_text = stringutil.BuildUpdate(table, fieldList, "mysql")

		case canal.InsertAction:
			sql_text = stringutil.BuildInsert(table, fieldList, "mysql")
		case canal.DeleteAction:
			sql_text = stringutil.BuildDelete(table, fieldList, "mysql")
		default:
			logs.Errorf("Consume get error action: %v", row)
			continue
		}

		stmt, err := s.client.Prepare(sql_text)
		if err != nil {
			return errors.Errorf("mysql prepare error: %q", err)
		}

		defer stmt.Close()

		_, errExt := stmt.Execute(valueList[0:]...)
		if errExt != nil {
			return errors.Errorf("mysql execute error: %q", errExt)
		}

		//logs.Infof("Excute %s OK!  SQL is :%s, value: %v ", row.Action, sql_text, valueList)
		//fmt.Printf("Excute %s OK!  SQL is :%s ,value: %v\n ", row.Action, sql_text, valueList)

	}

	logs.Infof("MysqlEndpoint Consume处理完成 %d 条数据", len(rows))
	return nil
}

func (s *MysqlEndpoint) Stock(rows []*model.RowRequest) int64 {

	//fmt.Printf("rows length is : %d\n", len(rows))
	if len(rows) == 0 {
		return 0
	}

	//建立连接
	cfg := global.Cfg()
	myConn, err := client.Connect(cfg.MysqlAddr, cfg.MysqlUser, cfg.MysqlPass, "")
	if err != nil {
		logs.Errorf("MysqlEndpoint connetc mysql error: %v, addr: %s,user: %s,pass: %s", err, cfg.MysqlAddr, cfg.MysqlUser, cfg.MysqlPass)
		return 0
	}
	defer myConn.Close()

	row := rows[0]
	rule, _ := global.RuleIns(row.RuleKey)
	table := rule.MysqlDatabase + "." + rule.MysqlTable
	kvm := rowMap(row, rule, false)

	//对字段名进行排序
	var fieldList []string
	for k := range kvm {
		fieldList = append(fieldList, k)
	}
	sort.Strings(fieldList)

	sql_text := stringutil.BuildBatchInsert(table, fieldList, len(rows), "mysql")
	//fmt.Printf("sql :%s \n", sql_text)
	stmt, err1 := myConn.Prepare(sql_text)
	if err != nil {
		logs.Errorf("mysql prepare error : %v, sql is :%s  ", err1, sql_text)
		return 0
	}
	defer stmt.Close()

	var valueList []interface{} //值列表
	for _, row := range rows {
		tmpkvm := rowMap(row, rule, false)

		for _, kk := range fieldList {
			valueList = append(valueList, tmpkvm[kk])
		}
	}

	_, errExt := stmt.Execute(valueList[0:]...)
	if errExt != nil {
		logs.Errorf("mysql execute error : %v, value is :%v  ", errExt, valueList)
		return 0
	}

	return int64(len(rows))

}
