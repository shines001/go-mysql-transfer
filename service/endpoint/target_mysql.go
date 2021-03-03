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
	"sync"

	//"github.com/juju/errors"
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
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		table := rule.MysqlDatabase + "." + rule.MysqlTable
		var sql_text string
		kvm := rowMap(row, rule, false)
		switch row.Action {
		case canal.UpdateAction:
			old_kvm := oldRowMap(row, rule, false)
			sql_text = stringutil.BuildUpdate(table, kvm, old_kvm)
		case canal.InsertAction:
			sql_text = stringutil.BuildInsert(table, kvm)
		case canal.DeleteAction:
			sql_text = stringutil.BuildDelete(table, kvm)
		default:
			logs.Errorf("Consume get error action: %v", row)
			continue
		}

		_, err := s.client.Execute(sql_text)
		if err != nil {
			logs.Errorf("mysql execute error : %v, sql is :%s  ", err, sql_text)
			continue
		}
		logs.Infof("Excute %s OK!  SQL is :%s ", row.Action, sql_text)
		fmt.Printf("Excute %s OK!  SQL is :%s \n ", row.Action, sql_text)

	}

	logs.Infof("MysqlEndpoint Consume处理完成 %d 条数据", len(rows))
	return nil
}

func (s *MysqlEndpoint) Stock(rows []*model.RowRequest) int64 {
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		table := rule.MysqlDatabase + "." + rule.MysqlTable
		var sql_text string
		kvm := rowMap(row, rule, false)
		switch row.Action {
		case canal.UpdateAction:
			old_kvm := oldRowMap(row, rule, false)
			sql_text = stringutil.BuildUpdate(table, kvm, old_kvm)
		case canal.InsertAction:
			sql_text = stringutil.BuildInsert(table, kvm)
		case canal.DeleteAction:
			sql_text = stringutil.BuildDelete(table, kvm)
		default:
			logs.Errorf("Consume get error action: %v", row)
			continue
		}

		_, err := s.client.Execute(sql_text)
		if err != nil {
			logs.Errorf("mysql execute error : %v, sql is :%s  ", err, sql_text)
			continue
		}
		logs.Infof("Excute %s OK!  SQL is :%s ", row.Action, sql_text)
		fmt.Printf("Excute %s OK!  SQL is :%s \n ", row.Action, sql_text)
	}

	return int64(len(rows))
}
