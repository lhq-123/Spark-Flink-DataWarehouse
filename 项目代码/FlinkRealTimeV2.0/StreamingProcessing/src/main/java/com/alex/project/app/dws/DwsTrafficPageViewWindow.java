package com.alex.project.app.dws;

import com.alex.project.app.base.BaseTask;
import com.alex.project.bean.TrafficHomeDetailPageViewBean;
import com.alex.project.utils.ClickHouseUtil;
import com.alex.project.utils.DateTimeFormatUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：     Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwsTrafficPageViewWindow -> ClickHouse(ZK)
public class DwsTrafficPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //TODO 2.读取 Kafka 页面日志主题数据创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window_211126";
        DataStreamSource<String> kafkaDS = env.addSource(BaseTask.getKafkaSource(topic, groupId));

        //TODO 3.将每行数据转换为JSON对象并过滤(首页与商品详情页)
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                //转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);
                //获取当前页面id
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                //过滤出首页与商品详情页的数据
                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
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

        //TODO 5.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 6.使用状态编程过滤出首页与商品详情页的独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

            private ValueState<String> homeLastState;
            private ValueState<String> detailLastState;

            @Override
            public void open(Configuration parameters) throws Exception {

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                ValueStateDescriptor<String> homeStateDes = new ValueStateDescriptor<>("home-state", String.class);
                ValueStateDescriptor<String> detailStateDes = new ValueStateDescriptor<>("detail-state", String.class);

                //设置TTL
                homeStateDes.enableTimeToLive(ttlConfig);
                detailStateDes.enableTimeToLive(ttlConfig);

                homeLastState = getRuntimeContext().getState(homeStateDes);
                detailLastState = getRuntimeContext().getState(detailStateDes);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                //获取状态数据以及当前数据中的日期
                Long ts = value.getLong("ts");
                String curDt = DateTimeFormatUtil.toDate(ts);
                String homeLastDt = homeLastState.value();
                String detailLastDt = detailLastState.value();

                //定义访问首页或者详情页的数据
                long homeCt = 0L;
                long detailCt = 0L;

                //如果状态为空或者状态时间与当前时间不同,则为需要的数据
                if ("home".equals(value.getJSONObject("page").getString("page_id"))) {
                    if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                        homeCt = 1L;
                        homeLastState.update(curDt);
                    }
                } else {
                    if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                        detailCt = 1L;
                        detailLastState.update(curDt);
                    }
                }

                //满足任何一个数据不等于0,则可以写出
                if (homeCt == 1L || detailCt == 1L) {
                    out.collect(new TrafficHomeDetailPageViewBean("", "",
                            homeCt,
                            detailCt,
                            ts));
                }
            }
        });

        //TODO 7.开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = trafficHomeDetailDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))).reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                return value1;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                //获取数据
                TrafficHomeDetailPageViewBean pageViewBean = values.iterator().next();
                //补充字段
                pageViewBean.setTs(System.currentTimeMillis());
                pageViewBean.setStt(DateTimeFormatUtil.toYmdHms(window.getStart()));
                pageViewBean.setEdt(DateTimeFormatUtil.toYmdHms(window.getEnd()));
                //输出数据
                out.collect(pageViewBean);
            }
        });

        //TODO 8.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>>>>");
        resultDS.addSink(ClickHouseUtil.getSink("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("DwsTrafficPageViewWindow");

    }

}
