package com.alex.project.app.base;

import com.alex.project.utils.ConfigLoader;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author liu
 * @Create 2022-11-20
 * @Description
 *
 *  定义所有task作业的父类，在父类中实现公共的代码
 *  加载配置文件内容到ParameterTool对象中
 *  1）flink流处理环境的初始化
 *  2）flink接入kafka数据源消费数据
 */
public abstract class BaseTask {
    //定义parameterTool工具类
    public static ParameterTool parameterTool;
    public static String appName;

    //定义静态代码块，加载配置文件数据到ParameterTool对象中
    static {
        try {
            parameterTool = ParameterTool.fromPropertiesFile(BaseTask.class.getClassLoader().getResourceAsStream("conf.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //TODO 1）初始化flink流式处理的开发环境
    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //TODO 2）flink任务的初始化方法
    public static StreamExecutionEnvironment getEnv(String className) {
        System.setProperty("HADOOP_USER_NAME", "root");
        //TODO 3）按照事件时间处理数据（terminalTimeStamp）进行窗口的划分和水印的添加
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //为了后续进行测试方便，将并行度设置为1，在生产环境一定不要设置代码级别的并行度，可以设置client级别的并行度
        env.setParallelism(1);
        //TODO 4）开启checkpoint
        //  TODO 4.1）设置每隔30s周期性开启checkpoint
        env.enableCheckpointing(30 * 1000);
        //  TODO 4.2）设置检查点的model、exactly-once、保证数据一次性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //  TODO 4.3）设置两次checkpoint的时间间隔，避免两次间隔太近导致频繁的checkpoint，而出现业务处理能力下降
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(20 * 1000);
        //  TODO 4.4）设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(20 * 1000);
        //  TODO 4.5）设置checkpoint的最大尝试次数，同一个时间有几个checkpoint在运行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //  TODO 4.6）设置checkpoint取消的时候，是否保留checkpoint，checkpoint默认会在job取消的时候删除checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //  TODO 4.7）设置执行job过程中，保存检查点错误时，job不失败
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        //  TODO 4.8）设置检查点的存储位置，使用rocketDBStateBackend，存储本地+hdfs分布式文件系统，可以进行增量检查点
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(ConfigLoader.get("hdfsUri") + "/flink/checkpoint/" + className));
        //TODO 5）设置任务的重启策略（固定延迟重启策略、失败率重启策略、无重启策略）
        //  TODO 5.1）如果开启了checkpoint，默认不停的重启，没有开启checkpoint，无重启策略
        env.setRestartStrategy(RestartStrategies.fallBackRestart());
        appName = className;
        //返回env对象
        return env;
    }


    /**
     *  创建FlinkKafkaConsumer消费kafka主题里的据源
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {

        //给配置信息对象添加配置项
            //TODO 6）创建flink消费kafka数据的对象，指定kafka的参数信息
            Properties props = new Properties();
            //  TODO 6.1）设置kafka的集群地址
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigLoader.get("bootstrap.servers"));
            //  TODO 6.2）设置消费者组id
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId + appName);
            //  TODO 6.3）设置kafka的分区感知（动态感知）
            props.setProperty(FlinkKafkaConsumer.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,"30000");
            //  TODO 6.4）设置key和value的反序列化（可选）
            //  TODO 6.5）设置是否自动递交offset
            //FlinkKafkaConsumer.setStartFromTimestamp(1624896000000L)/setStartFromEarliest/setStartFromLatest
            //enable.auto.commit=FALSE，则消费者重启之后会消费到相同的消息\
            //auto.offset.reset=earliest情况下，新的消费者（消费者二）将会从头开始消费Topic下的消息，即从offset=0的位置开始消费
            //props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
            //props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //获取 KafkaSource
        FlinkKafkaConsumer<String> FlinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        FlinkKafkaConsumer.setStartFromEarliest();
        return FlinkKafkaConsumer;

    }


    /**
     * 创建kafka生产者实例，往kafka主题里写数据
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {

            Properties props = new Properties();
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigLoader.get("bootstrap.servers"));
            props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"5");
            props.setProperty(ProducerConfig.ACKS_CONFIG,"0");

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                topic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(
                                topic,
                                element.getBytes()
                        );
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.NONE
        );
        return producer;
    }

    /**
     * 自定义序列化规则往kafka写数据
     * @param kafkaSerializationSchema
     * @return
     * @param <T>
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigLoader.get("bootstrap.servers"));

        return new FlinkKafkaProducer<T>(ConfigLoader.get("kafka_dwd_db"),
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * 将数据流以文件形式写入HDFS
     * @param prefix
     * @param suffix
     * @param path
     * @param bucketAssignerFormat
     * @return
     */
    public static StreamingFileSink<String> HdfsSink(
            String prefix,
            String suffix,
            String path,
            String bucketAssignerFormat
    ){
        OutputFileConfig config = OutputFileConfig.builder().withPartPrefix(prefix).withPartSuffix(suffix).build();
        StreamingFileSink DataSink = StreamingFileSink.forRowFormat(
                        new Path(path),
                        new SimpleStringEncoder<>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>(bucketAssignerFormat))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        //设置滚动时间间隔，5秒钟产生一个文件
                        .withRolloverInterval(TimeUnit.SECONDS.toMillis(5))
                        //设置不活动的时间间隔，未写入数据处于不活动状态时滚动文件
                        .withInactivityInterval(TimeUnit.SECONDS.toMillis(2))
                        //文件大小，默认是128M滚动一次
                        .withMaxPartSize(128 * 1024 * 1024).build())
                .withOutputFileConfig(config).build();
        return DataSink;
    }

    /**
     *     拼接Kafka相关属性到DDL
     * @param topic
     * @param groupId
     * @return
     *   scan.startup.mode:有五种消费kafka的方式
     *                       1.earliest-offset:该模式为每次执行时从头开始消费 topic
     *                       2.latest-offset:从最新的offset 开始消费，也就是说在任务启动之前的消息是不会被消费到的,消费时会丢失数据
     *                       3.timestamp:指定每个分区(partition)的时间戳开始消费,设置时间戳之前的数据不会被消费
     *                         通过 scan.startup.timestamp-millis 配置进行设置时间戳
     *                       4.specific-offsets:指定从每个分区（partition）的某个偏移量（offset）开始消费
     *                         设置方式: 'specific-offsets' = 'partition:0,offset:15' 表示 分区 0 从 偏移量(offset)15 开始消费消息
     *                       5.group-offsets(默认的消费方式):从 zookeeper 或者 kafka 中某个指定的消费组已提交的偏移量开始消费
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return  " with ('connector' = 'kafka', \n" +
                " 'topic' = '" + topic + "', \n" +
                " 'properties.bootstrap.servers' = '" + ConfigLoader.get("bootstrap.servers") + "',  \n" +
                " 'properties.group.id' = '" + groupId + "',  \n" +
                " 'format' = 'json', \n" +
                " 'scan.startup.mode' = 'earliest-offset')";
    }

    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getKafkaSinkDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + ConfigLoader.get("bootstrap.servers") + "', " +
                "  'format' = 'json' " +
                ")";
    }

    /**
     * UpsertKafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 UpsertKafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + ConfigLoader.get("bootstrap.servers") + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }


    /**
     * ods_base_db主题的  Kafka-Source DDL 语句
     *
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getTopicDb(String groupId) {
        return "CREATE TABLE ods_base_db ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `data` MAP<STRING,STRING>, " +
                "  `old` MAP<STRING,STRING>, " +
                "  `pt` AS PROCTIME() " +
                ") " + getKafkaDDL("ods_base_db", groupId);
    }
}
