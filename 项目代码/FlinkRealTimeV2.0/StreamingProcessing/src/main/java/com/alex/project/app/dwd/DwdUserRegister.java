package com.alex.project.app.dwd;

import com.alex.project.app.base.BaseTask;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdUserRegister -> Kafka(ZK)
public class DwdUserRegister extends BaseTask{
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = getEnv(DwdUserRegister.class.getSimpleName());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`ts` string " +
                ")" + BaseTask.getKafkaDDL("topic_db", "dwd_trade_order_detail_211126"));

        // TODO 4. 读取用户表数据
        Table userInfo = tableEnv.sqlQuery("select " +
                "data['id'] user_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'user_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("user_info", userInfo);

        // TODO 5. 创建 Kafka-Connector dwd_user_register 表
        tableEnv.executeSql("create table `dwd_user_register`( " +
                "`user_id` string, " +
                "`date_id` string, " +
                "`create_time` string, " +
                "`ts` string " +
                ")" + BaseTask.getKafkaSinkDDL("dwd_user_register"));

        // TODO 6. 将输入写入 Kafka-Connector 表
        tableEnv.executeSql("insert into dwd_user_register " +
                "select  " +
                "user_id, " +
                "date_format(create_time, 'yyyy-MM-dd') date_id, " +
                "create_time, " +
                "ts " +
                "from user_info");
    }
}
