package com.alex.project.app.dwd;


import com.alex.project.app.base.BaseTask;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdToolCouponGet -> Kafka(ZK)
public class DwdToolCouponGet extends BaseTask{
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = getEnv(DwdToolCouponGet.class.getSimpleName());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置状态的TTL  生产环境设置为最大乱序程度
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table `topic_db`( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string " +
                ")" + BaseTask.getKafkaDDL("topic_db", "dwd_tool_coupon_get"));

        // TODO 4. 读取优惠券领用数据，封装为表
        Table resultTable = tableEnv.sqlQuery("select " +
                "data['id'], " +
                "data['coupon_id'], " +
                "data['user_id'], " +
                "date_format(data['get_time'],'yyyy-MM-dd') date_id, " +
                "data['get_time'], " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 5. 建立 Kafka-Connector dwd_tool_coupon_get 表
        tableEnv.executeSql("create table dwd_tool_coupon_get ( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "date_id string, " +
                "get_time string, " +
                "ts string " +
                ")" + BaseTask.getKafkaSinkDDL("dwd_tool_coupon_get"));

        // TODO 6. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_get select * from result_table");
    }
}
