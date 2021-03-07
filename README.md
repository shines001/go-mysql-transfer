[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# 简介

go-mysql-transfer是一款MySQL数据库实时增量同步工具。

能够监听MySQL二进制日志(Binlog)的变动，将变更内容形成指定格式的消息，实时发送到接收端。从而在数据库和接收端之间形成一个高性能、低延迟的增量数据同步更新管道。

**官方项目信息**

* [官方源github地址](https://github.com/wj596/go-mysql-transfer)


* [官方产品手册](https://www.kancloud.cn/wj596/go-mysql-transfer/2064425)

 本项目致力于丰富功能，提升使用体验，并新增对关系型数据库的支持，包括mysql, postgresql, oracle等...


**源码编译**

1、依赖Golang 1.14 及以上版本

2、设置' GO111MODULE=on '

3、拉取源码 ' git clone https://github.com/shines001/go-mysql-transfer/go-mysql-transfer.git '

4、进入目录，执行 ' go build '编译

**查看binlog命令**

mysqlbinlog  --no-defaults   -v --base64-output=decode-rows /var/lib/mysql/mysql-bin.000001


# 工作列表

| 工作项 |  描述   |  状态 |
| :------ | :------ | :------ |
| 宕机处理 |  如果目标库宕了，go-mysql-transfer没宕，源库上后续插的数据，全部丢了。目标库重启后，go-mysql-transfer还是夯住状态，无法同步，必须要重启go-mysql-transfer |   未开始 |
| truncate支持 |truncate操作没有同步  | 进行中 |
| gtid支持  |不支持gtid，在源库做了主备切换后，go-mysql-transfer切换到新主库时，可能会有数据丢失|未开始| 
| 写目标库用存储过程|写目标端的SQL，未考虑绑定变量，执行效率有问题。|已完成|
| 表结构变化| 源数据库表结构变化，工具无法自动修改目标库的表结构，会造成同步中断|此情况主要在目标库是关系型数据库中出现，该需求暂时搁置，无太大必要，源表结构变化需手动修改目标库结构|
| mysql支持| 目标库支持mysql|已完成|
| postgres支持 | 目标库支持postgres | 未开始|
| oracle支持 | 目标库支持oracle| 未开始|




