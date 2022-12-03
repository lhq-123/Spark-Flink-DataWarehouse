[TOC]

# 数据处理(ODS)

通常在开始搭建数仓时，会提前生成一些数据作为历史数据，一般是业务数据库存在历史数据，而用户行为日志无历史数据。假定数仓上线的日期为2022-11-12，为模拟真实场景，需准备以下数据

### 用户行为日志

用户行为日志，一般是没有历史数据的，故日志只需要准备2022-11-12一天的数据。具体操作如下：

- 启动 Kafka。

- 启动一个命令行 Kafka 消费者，消费 topic_log 主题的数据。

- 修改两个日志服务器（Flink02、Flink03）中的

/opt/module/applog/application.yml配置文件，将mock.date参数改为2022-11-12。

- 执行日志生成脚本lg.sh。

- 观察命令行 Kafka 消费者是否消费到数据

### 业务数据

实时计算不考虑历史的事实数据，但要考虑历史维度数据。因此要对维度相关的业务表做一次全量同步。

- 维度数据首日全量同步
  - 修改Maxwell配置文件中的mock_date参数 mock_date=2022-11-21
  
  - 启动业务数据采集通道，包括Maxwell、Kafka
  
  - 编写业务数据首日全量脚本mysql_to_kafka_init.sh

  - 维度相关表如下：
  
    ```shell
    activity_info
    activity_rule
    activity_sku
    base_category1
    base_category2
    base_category3
    base_province
    base_region
    base_trademark
    coupon_info
    coupon_range
    financial_sku_cost
    sku_info
    spu_info
    user_info
    总共15张
    ```
  
  - 执行脚本mysql_to_kafka_init.sh
  
  - 启动 Kafka 消费者，观察数据是否写入 Kafka的topic_db主题
  
- 业务数据生成

业务数据生成之前要将 application. properties 文件中的 mock.date 参数修改为业务时间，首日应设置为 2022-11-12。mock.clear 和 mock.clear.user 均为 0，表示不重置业务数据和用户数据。如下。

```properties
 15 #业务日期
 16 mock.date=2022-11-12
 17 #是否重置，首日须置为1，之后置为0
 18 mock.clear=1
 19 #是否重置用户，首日须置为1，之后置为0
 20 mock.clear.user=1
```

同时要保证 Maxwell 配置文件 config.properties 中的 mock.date 参数和 application. properties 中 mock.date 参数的值保持一致。如下。

```properties
 12 #业务数据日期与变更日期应该一致
 13 mock_date=2022-11-12
```

采集到 Kafka 的 topic_log 和 topic_db 主题的数据即为实时数仓的 ODS 层，这一层的作用是对数据做原样展示和备份。

运行脚本即可