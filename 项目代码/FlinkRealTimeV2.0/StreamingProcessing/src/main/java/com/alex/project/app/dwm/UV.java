package com.alex.project.app.dwm;

import com.alex.project.app.base.BaseTask;
import com.alex.project.utils.ConfigLoader;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * @author Alex_liu
 * @create 2022-12-01 15:07
 * @Description  根据dwd层区分的页面日志数据中计算访客UV
 */
public class UV extends BaseTask {
    public static void main(String[] args) throws Exception {
        //TODO 1）获取执行环境
        StreamExecutionEnvironment env = getEnv(UV.class.getSimpleName());
        //TODO 2）读取kafka的dwd_page_log的数据
        FlinkKafkaConsumer<String> kafkaSource = BaseTask.getKafkaSource(ConfigLoader.get("kafka.dwd_topic1"), ConfigLoader.get("group.dwm_id1"));
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //TODO 3）将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //TODO 4）过滤数据(分组)->写入状态->只保留每个mid每天第一次登陆的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dataState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("data_state", String.class);
                //设置状态的超时时间及更新时间的方式
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dataState = getRuntimeContext().getState(valueStateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出上一条页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                //判断上一条页面是否为空
                if (lastPageId == null || lastPageId.length() <= 0) {
                    //取出状态数据
                    String lastDate = dataState.value();
                    //取出今天的日期
                    String currentDate = simpleDateFormat.format(value.getLong("ts"));
                    //判断两个日期是否相同
                    if (!currentDate.equals(lastDate)) {
                        dataState.update(currentDate);
                        return true;
                    }
                }
                return false;
            }
        });
        //打印测试
        uvDS.print();
        uvDS.map(JSON::toString).addSink(BaseTask.getKafkaProducer(ConfigLoader.get("kafka.dwm_topic1")));
        //TODO 5）启动任务
        env.execute("UV");
    }
}
