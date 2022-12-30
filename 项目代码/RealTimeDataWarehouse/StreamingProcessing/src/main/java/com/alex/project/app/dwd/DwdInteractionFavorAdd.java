package com.alex.project.app.dwd;

import com.alex.project.app.base.BaseTask;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdInteractionFavorAdd -> Kafka(ZK)
public class DwdInteractionFavorAdd extends BaseTask{
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = getEnv(DwdInteractionFavorAdd.class.getSimpleName());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置状态的TTL  生产环境设置为最大乱序程度
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`ts` string " +
                ")" + BaseTask.getKafkaDDL("topic_db", "dwd_interaction_favor_add_211126"));

        // TODO 4. 读取收藏表数据
        Table favorInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "date_format(data['create_time'],'yyyy-MM-dd') date_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'favor_info' " +
                "and (`type` = 'insert' or (`type` = 'update' and data['is_cancel'] = '0'))" +
                "");
        tableEnv.createTemporaryView("favor_info", favorInfo);

        // TODO 5. 创建 Kafka-Connector dwd_interaction_favor_add 表
        tableEnv.executeSql("create table dwd_interaction_favor_add ( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "date_id string, " +
                "create_time string, " +
                "ts string " +
                ")" + BaseTask.getKafkaSinkDDL("dwd_interaction_favor_add"));

        // TODO 6. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_favor_add select * from favor_info");
    }
}
