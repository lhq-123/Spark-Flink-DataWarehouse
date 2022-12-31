package com.alex.project.app.dwd;

import com.alex.project.app.base.BaseTask;
import com.alex.project.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Alex_liu
 * @create 2022-11-29
 * @Description
 *
 * 数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
 * 程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdInteractionComment -> Kafka(ZK)
 */

public class DwdInteractionComment extends BaseTask {
    public static void main(String[] args) throws Exception {

        // TODO 1）获取执行环境
        StreamExecutionEnvironment env = getEnv(DwdInteractionComment.class.getSimpleName());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置状态的TTL  设置为最大乱序程度
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // TODO 2）从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + BaseTask.getKafkaDDL("topic_db", "dwd_interaction_comment"));

        // TODO 3）读取评论表数据
        Table commentInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "data['order_id'] order_id, " +
                "data['create_time'] create_time, " +
                "data['appraise'] appraise, " +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'comment_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // TODO 4）建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 5）关联两张表
        Table resultTable = tableEnv.sqlQuery("select " +
                "ci.id, " +
                "ci.user_id, " +
                "ci.sku_id, " +
                "ci.order_id, " +
                "date_format(ci.create_time,'yyyy-MM-dd') date_id, " +
                "ci.create_time, " +
                "ci.appraise, " +
                "dic.dic_name, " +
                "ts " +
                "from comment_info ci " +
                "join " +
                "base_dic for system_time as of ci.proc_time as dic " +
                "on ci.appraise = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 6）建立 Kafka-Connector dwd_interaction_comment 表
        tableEnv.executeSql("create table dwd_interaction_comment( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "order_id string, " +
                "date_id string, " +
                "create_time string, " +
                "appraise_code string, " +
                "appraise_name string, " +
                "ts string " +
                ")" + BaseTask.getKafkaSinkDDL("dwd_interaction_comment"));

        // TODO 7）将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_comment select * from result_table");
    }
}
