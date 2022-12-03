package com.alex.project.app.dws;

import com.alex.project.app.base.BaseTask;
import com.alex.project.bean.KeywordStats;
import com.alex.project.common.SplitFunction;
import com.alex.project.utils.ClickHouseUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

//数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：     Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwsTrafficSourceKeywordPageViewWindow > ClickHouse(ZK)
public class DwsTrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //TODO 2.使用DDL方式读取Kafka page_log 主题的数据创建表并且提取时间戳生成Watermark
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window_211126";
        tableEnv.executeSql("" +
                "create table page_log( " +
                "    `page` map<string,string>, " +
                "    `ts` bigint, " +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                " ) " + BaseTask.getKafkaDDL(topic, groupId));

        //TODO 3.过滤出搜索数据
        Table filterTable = tableEnv.sqlQuery("" +
                " select " +
                "    page['item'] item, " +
                "    rt " +
                " from page_log " +
                " where page['last_page_id'] = 'search' " +
                " and page['item_type'] = 'keyword' " +
                " and page['item'] is not null");
        tableEnv.createTemporaryView("filter_table", filterTable);

        //TODO 4.注册UDTF & 切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT " +
                "    word, " +
                "    rt " +
                "FROM filter_table,  " +
                "LATERAL TABLE(SplitFunction(item))");
        tableEnv.createTemporaryView("split_table", splitTable);
        tableEnv.toAppendStream(splitTable, Row.class).print("Split>>>>>>");

        //TODO 5.分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) keyword_count, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from split_table " +
                "group by word,TUMBLE(rt, INTERVAL '10' SECOND)");

        //TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);
        keywordBeanDataStream.print(">>>>>>>>>>>>");

        //TODO 7.将数据写出到ClickHouse
        keywordBeanDataStream.addSink(ClickHouseUtil.getSink("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("DwsTrafficSourceKeywordPageViewWindow");

    }

}
