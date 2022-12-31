package com.alex.project.app.dim;

import com.alex.project.app.base.BaseTask;
import com.alex.project.utils.ConfigLoader;
import com.alex.project.utils.DateTimeFormatUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.text.SimpleDateFormat;


/**
 * @author Alex_liu
 * @create 2022-11-28 19:54
 * @Description  ods层已经将数据消费进kafka里，dwd层将kafka的数据读取出来，分为三类：页面日志、启动日志和曝光日志，将日志做拆分处理。写回kafka不同主题里
 *               页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光侧输出流
 *        主要任务：0.将数据转换成JSONObject对象
 *                1.识别新老用户(不涉及业务操作，只是单纯的做个状态确认)
 *                2.利用侧输出流实现数据拆分
 *                3.将不同流的数据推送下游的 Kafka 的不同 Topic 中
 */
public class DimAppLog extends BaseTask{
    public static void main(String[] args) throws Exception {
        // TODO 1）获取执行环境
        StreamExecutionEnvironment env = getEnv(DimAppLog.class.getSimpleName());

        // TODO 2）将kafka读出来的数据封装为DataStream
        FlinkKafkaConsumer<String> kafkaSource = BaseTask.getKafkaSource(ConfigLoader.get("kafka_ods_log"), ConfigLoader.get("group_ods_log"));
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //kafkaDS.print("ods日志数据>>>>>");

        //TODO 3.过滤掉非JSON数据&保留新增、变化以及初始化数据并将数据转换为JSON格式
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> filterJsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //发生异常,将数据写入侧输出流
                    ctx.output(outputTag, value);
                }
            }
        });
        DataStream<String> dirtyDS = filterJsonObjDS.getSideOutput(outputTag);
        //dirtyDS.print("脏数据>>>>>>");
        filterJsonObjDS.print("正常数据>>>>>>");
        // TODO 4）将过滤过后的数据发一份到HDFS上，用于SparkSQL分析
        //jsonObjDS.map(JSONObject::toString).addSink(BaseTask.HdfsSink("applog",".txt",ConfigLoader.get("hdfsUri")+"/origin_data/gmall/ods","yyyyMMdd"));
        // TODO 4）识别新老访客，按照 Mid 分组
        KeyedStream<JSONObject, String> keyedStream = filterJsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
        // TODO 5）使用状态做新老用户体验，筛选出新用户
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //声明状态用于表示当前 Mid 是否已经访问过
            private ValueState<String> firstVisitDateState;
            private SimpleDateFormat simpleDateFormat;
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameter) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid", String.class));
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //获取is_new标记 & ts 并将时间戳转换为年月日
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                String curDate = DateTimeFormatUtil.toDate(ts);
                //获取状态中的日期
                String lastDate = lastVisitState.value();
                //判断is_new标记是否为"1"
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else if (!lastDate.equals(curDate)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastDate == null) {
                    lastVisitState.update(DateTimeFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }
                return value;
            }
        });
        //jsonObjWithNewFlagDS.print("新用户>>>>>>");
        // TODO 6）使用侧输出流进行分流处理，即分流，使用ProcessFunction，将页面日志放到主流  启动、曝光、动作、错误放到侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {};
        OutputTag<String> displayTag = new OutputTag<String>("display") {};
        OutputTag<String> actionTag = new OutputTag<String>("action") {};
        OutputTag<String> errorTag = new OutputTag<String>("error") {};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //尝试获取错误信息
                String err = value.getString("err");
                if (err != null) {
                    //将数据写到error侧输出流
                    ctx.output(errorTag, value.toJSONString());
                }
                //移除错误信息
                value.remove("err");
                //尝试获取启动信息
                String start = value.getString("start");
                if (start != null) {
                    //将数据写到start侧输出流
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //获取公共信息&页面id&时间戳
                    String common = value.getString("common");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //遍历曝光数据&写到display侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                    //尝试获取动作数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        //遍历曝光数据&写到display侧输出流
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }
                    //移除曝光和动作数据&写到页面日志主流
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }
        });
        // TODO 7）提取各个侧输出流数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        //打印数据
        startDS.print("启动日志>>>>>>");
        displayDS.print("曝光日志>>>>>>");
        actionDS.print("动作日志>>>>>>");
        errorDS.print("错误日志>>>>>>");
        // TODO 7.1）将各个侧输出流写入Kafka对应的主题
        pageDS.addSink(BaseTask.getKafkaProducer(ConfigLoader.get("kafka_dwd_page")));
        startDS.addSink(BaseTask.getKafkaProducer(ConfigLoader.get("kafka_dwd_start")));
        displayDS.addSink(BaseTask.getKafkaProducer(ConfigLoader.get("kafka_dwd_display")));
        actionDS.addSink(BaseTask.getKafkaProducer(ConfigLoader.get("kafka_dwd_action")));
        errorDS.addSink(BaseTask.getKafkaProducer(ConfigLoader.get("kafka_dwd_error")));
        // TODO 8）执行任务
        env.execute("DWD_LOG");
    }
}
