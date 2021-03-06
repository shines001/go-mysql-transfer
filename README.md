[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# 简介

go-mysql-transfer是一款MySQL数据库实时增量同步工具。

能够监听MySQL二进制日志(Binlog)的变动，将变更内容形成指定格式的消息，实时发送到接收端。从而在数据库和接收端之间形成一个高性能、低延迟的增量数据同步更新管道。

**官方项目信息**

* [官方源github地址](https://github.com/wj596/go-mysql-transfer)


* [官方产品手册](https://www.kancloud.cn/wj596/go-mysql-transfer/2064425)

**项目目标**

1、新增目标数据库源，包括mysql, postgresql, oracle等....
2、提升产品体验




# 源码编译

1、依赖Golang 1.14 及以上版本

2、设置' GO111MODULE=on '

3、拉取源码 ' git clone https://github.com/shines001/go-mysql-transfer/go-mysql-transfer.git '

4、进入目录，执行 ' go build '编译


**查看binlog命令**
mysqlbinlog  --no-defaults   -v --base64-output=decode-rows /var/lib/mysql/mysql-bin.000001
