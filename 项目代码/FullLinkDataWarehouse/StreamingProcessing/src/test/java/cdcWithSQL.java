
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
/**
 * @author Alex_liu
 * @create 2022-11-29 16:52
 * @Description
 */
public class cdcWithSQL {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.DDL方式建表
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS mysql_cdc");
//        // 创建mysql cdc 数据表
        tableEnv.executeSql("DROP TABLE IF EXISTS mysql_cdc.mysql_binlog");
        tableEnv.executeSql("CREATE TABLE mysql_cdc.mysql_binlog(\n" +
                "    id BIGINT,\n" +
                "    name STRING\n" +
                "  ) WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = 'Flink01',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'gmall_v2',\n" +
                "    'table-name' = 'test_binlog'\n" +
                ")");
        //3.查询数据
        Table table = tableEnv.sqlQuery("select * from mysql_binlog");

        //4.将动态表转换为流
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        //5.启动任务
        env.execute("FlinkCDCWithSQL");

    }
}
