package com.alex.project.app.dwm;

import com.alex.project.app.base.BaseTask;
import com.alex.project.utils.ConfigLoader;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.util.OutputTag;
import java.time.Duration;
import java.util.List;
import java.util.Map;


/**
 * @author Alex_liu
 * @create 2022-12-01 15:42
 * @Description 计算跳出率
 *  跳出就是用户成功访问了网站的一个页面后就退出，不在继续访问网站的其它页面。而
 *  跳出率就是用跳出次数除以访问次数
 *  用户跳出事件，本质上就是一个条件事件加一个超时事件的组合
 */
public class JR extends BaseTask {
    public static void main(String[] args) throws Exception {
        //TODO 1）获取执行环境
        StreamExecutionEnvironment env = getEnv(JR.class.getSimpleName());
        //TODO 2）读取kafka的dwd_page_log的数据
        FlinkKafkaConsumer<String> kafkaSource = BaseTask.getKafkaSource(ConfigLoader.get("kafka.dwd_topic1"), ConfigLoader.get("group.dwm_id2"));
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //TODO 3）将数据转换为JSON对象并提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        })
                );
        //TODO 4）定义序列模式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));
        //TODO 5）使用循环模式，定义序列模式
        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
            ///默认的使用宽松近邻，指定使用严格近邻(next)
        }).times(2).consecutive().within(Time.seconds(10));
        //TODO 6）将模式序列化作用到流上(根据表达式筛选流)
        PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"))
                , pattern);
        //TODO 7）提取匹配上的和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long ts) throws Exception {
                        return map.get("start").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                });
        //TODO 8）UNION两种事件
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);
        //TODO 9）将数据写入kafka作为DWM层
        unionDS.print();
        unionDS.map(JSON::toString).addSink(BaseTask.getKafkaProducer("kafka.dwm_topic2"));
        //TODO 10）启动任务
        env.execute("JR");
    }
}
