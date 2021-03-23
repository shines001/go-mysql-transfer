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
package service

import (
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/atomic"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/service/endpoint"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logs"
)

const _transferLoopInterval = 1

type TransferService struct {
	canal        *canal.Canal
	canalCfg     *canal.Config
	canalHandler *handler
	canalEnable  atomic.Bool //协程安全bool， store():设置值  load():取值
	lockOfCanal  sync.Mutex
	firstsStart  atomic.Bool

	//wg.Add(n) 计数器设置为n   wg.Done() 计数器减1    wg.Wait()等待计数器为0
	wg             sync.WaitGroup
	endpoint       endpoint.Endpoint
	endpointEnable atomic.Bool
	positionDao    storage.PositionStorage
	loopStopSignal chan struct{} //循环终止 channel
}

func (s *TransferService) initialize() error {
	s.canalCfg = canal.NewDefaultConfig()
	s.canalCfg.Addr = global.Cfg().Addr
	s.canalCfg.User = global.Cfg().User
	s.canalCfg.Password = global.Cfg().Password
	s.canalCfg.Charset = global.Cfg().Charset
	s.canalCfg.Flavor = global.Cfg().Flavor
	s.canalCfg.ServerID = global.Cfg().SlaveID
	s.canalCfg.Dump.ExecutionPath = global.Cfg().DumpExec
	s.canalCfg.Dump.DiscardErr = false
	s.canalCfg.Dump.SkipMasterData = global.Cfg().SkipMasterData

	if err := s.createCanal(); err != nil {
		return errors.Trace(err)
	}

	if err := s.completeRules(); err != nil {
		return errors.Trace(err)
	}

	s.addDumpDatabaseOrTable()

	positionDao := storage.NewPositionStorage()
	if err := positionDao.Initialize(); err != nil {
		return errors.Trace(err)
	}
	s.positionDao = positionDao

	// 启用了Gtid模式
	if global.Cfg().Gtid == true {
		if err := s.checkInitGtid(); err != nil {
			return errors.Trace(err)
		}
	}

	// endpoint
	endpoint := endpoint.NewEndpoint(s.canal)
	if err := endpoint.Connect(); err != nil {
		return errors.Trace(err)
	}
	// 异步，必须要ping下才能确定连接成功
	if global.Cfg().IsMongodb() {
		err := endpoint.Ping()
		if err != nil {
			return err
		}
	}
	s.endpoint = endpoint
	s.endpointEnable.Store(true)
	metrics.SetDestState(metrics.DestStateOK)

	s.firstsStart.Store(true)
	s.startLoop()

	return nil
}

func (s *TransferService) run() error {
	pos, gtid, err := s.positionDao.Get()
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func(p mysql.Position, gtid mysql.MysqlGTIDSet) {
		s.canalEnable.Store(true)
		log.Println(fmt.Sprintf("transfer run from position(%s %d) gtid(%s)", p.Name, p.Pos, gtid.String()))

		if global.Cfg().Gtid == true {
			err := s.canal.StartFromGTID(&gtid)
			if err != nil {
				log.Println(fmt.Sprintf("start transfer : %v", err))
				logs.Errorf("canal : %v", errors.ErrorStack(err))
				if s.canalHandler != nil {
					s.canalHandler.stopListener()
				}
				s.canalEnable.Store(false)
			}
		} else {
			err := s.canal.RunFrom(p)
			if err != nil {
				log.Println(fmt.Sprintf("start transfer : %v", err))
				logs.Errorf("canal : %v", errors.ErrorStack(err))
				if s.canalHandler != nil {
					s.canalHandler.stopListener()
				}
				s.canalEnable.Store(false)
			}
		}

		logs.Info("Canal is Closed")
		s.canalEnable.Store(false)
		s.canal = nil
		s.wg.Done()
	}(pos, gtid)

	// canal未提供回调，停留一秒，确保RunFrom启动成功
	time.Sleep(time.Second)
	return nil
}

/*
 *首次执行： 创建并设置Handler,启动handler的监听,然后正式开始同步binlog
 *非首次执行：重新启动
 */
func (s *TransferService) StartUp() {
	s.lockOfCanal.Lock()
	defer s.lockOfCanal.Unlock()

	if s.firstsStart.Load() {
		s.canalHandler = newHandler()
		s.canal.SetEventHandler(s.canalHandler) //设置handler
		s.canalHandler.startListener()
		s.firstsStart.Store(false)
		s.run()
	} else {
		s.restart()
	}
}

func (s *TransferService) restart() {
	if s.canal != nil {
		s.canal.Close()
		s.wg.Wait()
	}

	s.createCanal()
	s.addDumpDatabaseOrTable()
	s.canalHandler = newHandler()
	s.canal.SetEventHandler(s.canalHandler)
	s.canalHandler.startListener()
	s.run()
}

func (s *TransferService) stopDump() {
	s.lockOfCanal.Lock()
	defer s.lockOfCanal.Unlock()

	if s.canal == nil {
		return
	}

	if !s.canalEnable.Load() {
		return
	}

	if s.canalHandler != nil {
		s.canalHandler.stopListener()
		s.canalHandler = nil
	}

	s.canal.Close()
	s.wg.Wait()

	log.Println("dumper stopped")
}

func (s *TransferService) Close() {
	s.stopDump()
	s.loopStopSignal <- struct{}{}
}

func (s *TransferService) Position() (mysql.Position, mysql.MysqlGTIDSet, error) {
	return s.positionDao.Get()
}

/*
 * 创建canal
 */
func (s *TransferService) createCanal() error {
	for _, rc := range global.Cfg().RuleConfigs {
		s.canalCfg.IncludeTableRegex = append(s.canalCfg.IncludeTableRegex, rc.Schema+"\\."+rc.Table)
	}
	var err error
	s.canal, err = canal.NewCanal(s.canalCfg)
	return errors.Trace(err)
}

/*
 * 初始化_ruleInsMap
 * 读取mysql表信息
 */
func (s *TransferService) completeRules() error {
	wildcards := make(map[string]bool)
	for _, rc := range global.Cfg().RuleConfigs {
		if rc.Table == "*" {
			return errors.Errorf("wildcard * is not allowed for table name")
		}

		if regexp.QuoteMeta(rc.Table) != rc.Table { //通配符
			//QuoteMeta(string)在预定义的字符前添加 反斜杠， 预定义字符包括. \ + * ? [] ^ $ ()等
			//来到这里说明表名里包含通配符
			if _, ok := wildcards[global.RuleKey(rc.Schema, rc.Schema)]; ok {
				return errors.Errorf("duplicate wildcard table defined for %s.%s", rc.Schema, rc.Table)
			}

			tableName := rc.Table
			if rc.Table == "*" {
				tableName = "." + rc.Table
			}
			sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
					table_name RLIKE "%s" AND table_schema = "%s";`, tableName, rc.Schema)
			res, err := s.canal.Execute(sql)
			if err != nil {
				return errors.Trace(err)
			}
			for i := 0; i < res.Resultset.RowNumber(); i++ {
				tableName, _ := res.GetString(i, 0)
				newRule, err := global.RuleDeepClone(rc)
				if err != nil {
					return errors.Trace(err)
				}
				newRule.Table = tableName
				ruleKey := global.RuleKey(rc.Schema, tableName)
				global.AddRuleIns(ruleKey, newRule)
			}
		} else {
			newRule, err := global.RuleDeepClone(rc)
			if err != nil {
				return errors.Trace(err)
			}
			ruleKey := global.RuleKey(rc.Schema, rc.Table)
			global.AddRuleIns(ruleKey, newRule)
		}
	}

	for _, rule := range global.RuleInsList() {
		tableMata, err := s.canal.GetTable(rule.Schema, rule.Table)
		if err != nil {
			return errors.Trace(err)
		}
		if len(tableMata.PKColumns) == 0 {
			if !global.Cfg().SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}
		}
		if len(tableMata.PKColumns) > 1 {
			rule.IsCompositeKey = true // 组合主键
		}
		rule.TableInfo = tableMata
		rule.TableColumnSize = len(tableMata.Columns)

		if err := rule.Initialize(); err != nil {
			return errors.Trace(err)
		}

		if rule.LuaEnable() {
			if err := rule.CompileLuaScript(global.Cfg().DataDir); err != nil {
				return err
			}
		}
	}

	return nil
}

/*
 * 初始化检查Gtid，如果当前Gtid为空，则使用数据库中的Gtid
 */
func (s *TransferService) checkInitGtid() error {
	pos, gtid, err := s.positionDao.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if gtid.Sets == nil {
		res, err := s.canal.Execute("show global variables like 'gtid_purged'")
		defer res.Close()
		if err != nil {
			return errors.Trace(err)
		}
		gtidStr, _ := res.GetString(0, 1)
		purge_gtid, err := mysql.ParseMysqlGTIDSet(gtidStr)
		if err != nil {
			return errors.Trace(err)
		}

		sets := purge_gtid.(*mysql.MysqlGTIDSet)
		return s.positionDao.Save(pos, *sets)
	}

	return nil
}

/*
 * 将源表添加到dump计划中
 */
func (s *TransferService) addDumpDatabaseOrTable() {
	var schema string
	schemas := make(map[string]int)
	tables := make([]string, 0, global.RuleInsTotal())
	for _, rule := range global.RuleInsList() {
		schema = rule.Schema //modified by lxy  原来是rule.Table, 应该是Schema
		schemas[rule.Schema] = 1
		tables = append(tables, rule.Table)
	}
	if len(schemas) == 1 {
		s.canal.AddDumpTables(schema, tables...)
	} else {
		keys := make([]string, 0, len(schemas))
		for key := range schemas {
			keys = append(keys, key)
		}
		s.canal.AddDumpDatabases(keys...)
	}
}

/*
 *重新读取配置
 */
func (s *TransferService) updateRule(schema, table string) error {
	rule, ok := global.RuleIns(global.RuleKey(schema, table))
	if ok {
		tableInfo, err := s.canal.GetTable(schema, table)
		if err != nil {
			return errors.Trace(err)
		}

		if len(tableInfo.PKColumns) == 0 {
			if !global.Cfg().SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}
		}

		if len(tableInfo.PKColumns) > 1 {
			rule.IsCompositeKey = true
		}

		rule.TableInfo = tableInfo
		rule.TableColumnSize = len(tableInfo.Columns)

		err = rule.AfterUpdateTableInfo()
		if err != nil {
			return err
		}
	}

	return nil
}

/*
 * 设置定时器, 每秒检查一次，如果endpointEnable = false  则执行ping, ok之后 重启canal
 */
func (s *TransferService) startLoop() {
	go func() {
		ticker := time.NewTicker(_transferLoopInterval * time.Second) //_transferLoopInterval =1
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !s.endpointEnable.Load() {
					err := s.endpoint.Ping()
					if err != nil {
						log.Println("destination not available,see the log file for details")
						logs.Error(err.Error())
					} else {
						s.endpointEnable.Store(true)
						if global.Cfg().IsRabbitmq() {
							s.endpoint.Connect()
						}
						s.StartUp()
						metrics.SetDestState(metrics.DestStateOK)
					}
				}
			case <-s.loopStopSignal:
				return
			}
		}
	}()
}
