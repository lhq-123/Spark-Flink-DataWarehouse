package com.alex.project.app.dwd;

import com.alex.project.app.base.BaseTask;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdToolCouponPay -> Kafka(ZK)
public class DwdToolCouponPay extends BaseTask{
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = getEnv(DwdToolCouponPay.class.getSimpleName());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置状态的TTL  生产环境设置为最大乱序程度
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table `topic_db` ( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`old` string, " +
                "`ts` string " +
                ")" + BaseTask.getKafkaDDL("topic_db", "dwd_tool_coupon_pay_211126"));

        // TODO 4. 读取优惠券领用表数据，筛选优惠券使用（支付）数据
        Table couponUsePay = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['coupon_id'] coupon_id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "date_format(data['used_time'],'yyyy-MM-dd') date_id, " +
                "data['used_time'] used_time, " +
                "`old`, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'update' " +
                "and data['used_time'] is not null");
        tableEnv.createTemporaryView("coupon_use_pay", couponUsePay);

        // TODO 5. 建立 Kafka-Connector dwd_tool_coupon_order 表
        tableEnv.executeSql("create table dwd_tool_coupon_pay( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "order_id string, " +
                "date_id string, " +
                "payment_time string, " +
                "ts string " +
                ")" + BaseTask.getKafkaSinkDDL("dwd_tool_coupon_pay"));

        // TODO 6. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_pay select " +
                "id, " +
                "coupon_id, " +
                "user_id, " +
                "order_id, " +
                "date_id, " +
                "used_time payment_time, " +
                "ts from coupon_use_pay");
    }
}
