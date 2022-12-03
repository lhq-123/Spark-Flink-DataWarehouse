package com.alex.project.app.dws;

import com.alex.project.app.base.BaseTask;
import com.alex.project.bean.UserLoginBean;
import com.alex.project.utils.ClickHouseUtil;
import com.alex.project.utils.DateTimeFormatUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：     Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwsUserUserLoginWindow -> ClickHouse(ZK)
public class DwsUserUserLoginWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //TODO 2.读取Kafka 页面日志主题创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_user_login_window_211126";
        DataStreamSource<String> kafkaDS = env.addSource(BaseTask.getKafkaSource(topic, groupId));

        //TODO 3.转换数据为JSON对象并过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                //转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);
                //获取UID以及上一跳页面
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                //当UID不等于空并且上一跳页面为null或者为"login"才是登录数据
                if (uid != null && (lastPageId == null || lastPageId.equals("login"))) {
                    out.collect(jsonObject);
                }
            }
        });

        //TODO 4.提取事件时间生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //TODO 5.按照uid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(json -> json.getJSONObject("common").getString("uid"));

        //TODO 6.使用状态编程获取独立用户以及七日回流用户
        SingleOutputStreamOperator<UserLoginBean> userLoginDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

            private ValueState<String> lastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastLoginState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-login", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {

                //获取状态日期以及当前数据日期
                String lastLoginDt = lastLoginState.value();
                Long ts = value.getLong("ts");
                String curDt = DateTimeFormatUtil.toDate(ts);

                //定义当日独立用户数&七日回流用户数
                long uv = 0L;
                long backUv = 0L;

                if (lastLoginDt == null) {
                    uv = 1L;
                    lastLoginState.update(curDt);
                } else if (!lastLoginDt.equals(curDt)) {

                    uv = 1L;
                    lastLoginState.update(curDt);

                    if ((DateTimeFormatUtil.toTs(curDt) - DateTimeFormatUtil.toTs(lastLoginDt)) / (24 * 60 * 60 * 1000L) >= 8) {
                        backUv = 1L;
                    }
                }

                if (uv != 0L) {
                    out.collect(new UserLoginBean("", "",
                            backUv, uv, ts));
                }
            }
        });

        //TODO 7.开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultDS = userLoginDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean next = values.iterator().next();

                        next.setEdt(DateTimeFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateTimeFormatUtil.toYmdHms(window.getStart()));
                        next.setTs(System.currentTimeMillis());

                        out.collect(next);
                    }
                });

        //TODO 8.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>>>");
        resultDS.addSink(ClickHouseUtil.getSink("insert into dws_user_user_login_window values(?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("DwsUserUserLoginWindow");

    }

}
