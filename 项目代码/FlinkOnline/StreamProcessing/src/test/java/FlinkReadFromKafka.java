/**
 * @author Alex_liu
 * @create 2022-11-28 21:30
 * @Description
 */
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;

/**
 * @author liu
 * @Create 2022-11-19
 * @Description 从最早的位置消费 kafka 集群中的数据并打印到控制台中
 *  创建StreamExecutionEnvironment环境
 *  设置env参数
 *  设置kafka消费者参数属性，创建FlinkKafkaConsumer
 *  env添加数据源，打印数据源到控制台
 *  执行flink任务
 */
public class FlinkReadFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"Flink01:9092,Flink02:9092,Flink03:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"ods_consumer");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "ods_base_log",
                new SimpleStringSchema(),
                props
        );
        //设置consumer参数
        //从头消费
        consumer.setStartFromEarliest();
        DataStreamSource<String> dataStreamSource = env.addSource(consumer);
        dataStreamSource.print();
        env.execute();
    }
}