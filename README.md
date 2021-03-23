[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# 简介

**概念**

go-mysql-transfer是一款MySQL数据库实时增量同步工具。

能够监听MySQL二进制日志(Binlog)的变动，将变更内容形成指定格式的消息，实时发送到接收端。从而在数据库和接收端之间形成一个高性能、低延迟的增量数据同步更新管道。

**原理**

1、将自己伪装为MySQL的Slave监听binlog，获取binlog的变更数据

2、根据规则或者lua脚本解析数据，生成指定格式的消息

3、将生成的消息批量发送给接收端


**特性**

1、简单，不依赖其它组件，一键部署

2、集成多种接收端，如：Mysql、Redis、MongoDB、Elasticsearch、RocketMQ、Kafka、RabbitMQ、HTTP API等，无需编写客户端，开箱即用

3、内置丰富的数据解析、消息生成规则、模板语法

4、支持Lua脚本扩展，可处理复杂逻辑

5、集成Prometheus客户端，支持监控告警

6、集成Web Admin监控页面

7、支持高可用集群部署

8、数据同步失败重试

9、支持全量数据初始化



**源码编译**

1、依赖Golang 1.14 及以上版本

2、设置' GO111MODULE=on '

3、拉取源码 ' git clone https://github.com/shines001/go-mysql-transfer.git '

4、进入目录，执行 ' go build '编译




# 工作列表

| 工作项 |  描述   |  状态 |
| :------ | :------ | :------ |
| mysql支持| 目标库支持mysql|done|
| 写目标库用存储过程|写目标端的SQL，未考虑绑定变量，执行效率有问题。|done|
| truncate支持 |truncate操作没有同步  |doing|
| 宕机处理 |  如果目标库宕了，go-mysql-transfer没宕，会不断增加pos，导致数据未同步 |todo|
| gtid支持  |不支持gtid，在源库做了主备切换后，go-mysql-transfer切换到新主库时，可能会有数据丢失|todo| 
| 表结构变化| 源数据库表结构变化，工具无法自动修改目标库的表结构，会造成同步中断|todo|
| postgres支持 | 目标库支持postgres | todo|
| oracle支持 | 目标库支持oracle| todo|


**感谢**

* [go-mysql](https://github.com/siddontang/go-mysql)

* [源github地址](https://github.com/wj596/go-mysql-transfer)

* [手册](https://www.kancloud.cn/wj596/go-mysql-transfer/2064425)

 
 
 **binlog相关命令**

查看binlog  ：   mysqlbinlog  --no-defaults   -v --base64-output=decode-rows /var/lib/mysql/mysql-bin.000001

查看当前服务器使用的biglog文件及大小  show binary logs;

查看最新一个binlog日志文件名称和Position    show master status;

查看 binlog 内容  show binlog events;

是否启用binlog日志  show variables like 'log_bin';

查看详细的日志配置信息  show global variables like '%log%';

mysql数据存储目录  show variables like '%dir%';

查看binlog的目录  show global variables like "%log_bin%";


事件查询命令

 IN 'log_name' ：指定要查询的binlog文件名(不指定就是第一个binlog文件)
 
 FROM pos ：指定从哪个pos起始点开始查起(不指定就是从整个文件首个pos点开始算)
 
 LIMIT [offset,] ：偏移量(不指定就是0)
 
 row_count ：查询总条数(不指定就是所有行)
 
show binlog events [IN 'log_name'] [FROM pos] [LIMIT [offset,] row_count];

查看具体一个binlog文件的内容 （in 后面为binlog的文件名）    show binlog events in 'master.000003';

设置binlog文件保存事件，过期删除，单位天   set global expire_log_days=3; 

删除当前的binlog文件   reset master; 

删除slave的中继日志reset slave;

删除指定日期前的日志索引中binlog日志文件  purge master logs before '2019-03-09 14:00:00';

删除指定日志文件   purge master logs to 'master.000003';


* [mysql binlog介绍](https://blog.csdn.net/wwwdc1012/article/details/88373440)
   
