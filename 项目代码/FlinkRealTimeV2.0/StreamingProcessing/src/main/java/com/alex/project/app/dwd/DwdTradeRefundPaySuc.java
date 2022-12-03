package com.alex.project.app.dwd;

import com.alex.project.app.base.BaseTask;
import com.alex.project.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdTradeRefundPaySuc -> Kafka(ZK)
public class DwdTradeRefundPaySuc {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 为表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", "5 s");

        // TODO 2. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + BaseTask.getKafkaDDL("topic_db", "refund_pay_suc_211126"));

        // TODO 4. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 5. 读取退款表数据，并筛选退款成功数据
        Table refundPayment = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['order_id'] order_id, " +
                "data['sku_id'] sku_id, " +
                "data['payment_type'] payment_type, " +
                "data['callback_time'] callback_time, " +
                "data['total_amount'] total_amount, " +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'refund_payment' "
//                +
//                "and `type` = 'update' " +
//                "and data['refund_status'] = '0702' " +
//                "and `old`['refund_status'] is not null"
        );
        tableEnv.createTemporaryView("refund_payment", refundPayment);

        // TODO 6. 读取订单表数据并过滤退款成功订单数据
        Table orderInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from topic_db " +
                "where `table` = 'order_info' " +
                "and `type` = 'update' "
                +
                "and data['order_status']='1006' " +
                "and `old`['order_status'] is not null"
        );
        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 7. 读取退单表数据并过滤退款成功数据
        Table orderRefundInfo = tableEnv.sqlQuery("select " +
                        "data['order_id'] order_id, " +
                        "data['sku_id'] sku_id, " +
                        "data['refund_num'] refund_num, " +
                        "`old` " +
                        "from topic_db " +
                        "where `table` = 'order_refund_info' "
//                        +
//                        "and `type` = 'update' " +
//                        "and data['refund_status']='0705' " +
//                        "and `old`['refund_status'] is not null"
                // order_refund_info 表的 refund_status 字段值均为 null
        );
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // TODO 8. 关联四张表获得退款成功表
        Table resultTable = tableEnv.sqlQuery("select " +
                "rp.id, " +
                "oi.user_id, " +
                "rp.order_id, " +
                "rp.sku_id, " +
                "oi.province_id, " +
                "rp.payment_type, " +
                "dic.dic_name payment_type_name, " +
                "date_format(rp.callback_time,'yyyy-MM-dd') date_id, " +
                "rp.callback_time, " +
                "ri.refund_num, " +
                "rp.total_amount, " +
                "rp.ts, " +
                "current_row_timestamp() row_op_ts " +
                "from refund_payment rp  " +
                "join  " +
                "order_info oi " +
                "on rp.order_id = oi.id " +
                "join " +
                "order_refund_info ri " +
                "on rp.order_id = ri.order_id " +
                "and rp.sku_id = ri.sku_id " +
                "join  " +
                "base_dic for system_time as of rp.proc_time as dic " +
                "on rp.payment_type = dic.dic_code ");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 9. 创建 Kafka-Connector dwd_trade_refund_pay_suc 表
        tableEnv.executeSql("create table dwd_trade_refund_pay_suc( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "date_id string, " +
                "callback_time string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + BaseTask.getKafkaSinkDDL("dwd_trade_refund_pay_suc"));

        // TODO 10. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_refund_pay_suc select * from result_table");
    }
}
