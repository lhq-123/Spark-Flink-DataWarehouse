package com.alex.project.app.dim;

import com.alex.project.app.base.BaseTask;
import com.alex.project.common.TableProcessFunction;
import com.alex.project.bean.TableProcess;
import com.alex.project.sink.ToHbaseSink;
import com.alex.project.utils.ConfigLoader;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;


/**
 * @author Alex_liu
 * @create 2022-11-29 12:02
 * @Description  读取kafka的业务数据跟配置信息进行处理过滤，将业务数据分开来存放(Kafka+Hbase)
 */
public class DimAppDB extends BaseTask {
    private static Logger logger = LoggerFactory.getLogger("DimAppDB");
    public static void main(String[] args) throws Exception {
        //TODO 1）获取执行环境
        StreamExecutionEnvironment env = getEnv(DimAppDB.class.getSimpleName());
        //TODO 2）读取Kafka主流及配置流
        //主流
        DataStreamSource<String> kafkaDS = env.addSource(BaseTask.getKafkaSource(ConfigLoader.get("kafka_ods_db"), ConfigLoader.get("group_ods_db")));
        //配置流
        DataStreamSource<String> configDS = env.readTextFile(ConfigLoader.get("hdfsUri") + "/FlinkCarDataSource/table_process.txt");
        //TODO 3）将主流的每行数据转换为JSON对象并过滤(delete)
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //取出数据的操作类型
                        String type = value.getString("type");
                        return !"delete".equals(type);
                    }
                });
        //jsonObjDS.print("过滤后的主流数据:");
        //configDS.print("配置流数据:");
        //TODO 4）读取配置信息封装成流->广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = configDS.broadcast(mapStateDescriptor);
        //TODO 5）连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);
        //TODO 6）处理主流跟广播流数据(根据广播流数据进行处理)
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {};
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag,mapStateDescriptor));
        //TODO 7）提取Kafka流数据和HBase流数据(分流)
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);
        //TODO 8）将Kafka数据写入Kafka主题,将HBase数据写入Phoenix表
        kafka.print("Kafka>>>>>>>>");
        hbase.print("HBase>>>>>>>>");
        //TODO 9）根据不同数据实现不同的序列化方法
        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = BaseTask.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                logger.info("开始序列化kafka数据");
            }
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sinkTable"),
                        element.getString("data").getBytes());
            }
        });
        hbase.addSink(new ToHbaseSink());
        kafka.addSink(kafkaSinkBySchema);
        //TODO 10）启动任务
        env.execute("DIM_APP");

    }
}
