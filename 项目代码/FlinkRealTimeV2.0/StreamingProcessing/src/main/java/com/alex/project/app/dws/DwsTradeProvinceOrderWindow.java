package com.alex.project.app.dws;

import com.alex.project.app.base.BaseTask;
import com.alex.project.bean.TradeProvinceOrderWindow;
import com.alex.project.common.DimAsyncFunction;
import com.alex.project.utils.ClickHouseUtil;
import com.alex.project.utils.DateTimeFormatUtil;
import com.alex.project.utils.TimestampLtz3CompareUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK) -> DwsTradeProvinceOrderWindow(Phoenix-(HBase-HDFS、ZK)、Redis) -> ClickHouse(ZK)

public class DwsTradeProvinceOrderWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.读取Kafka DWD层下单主题数据创建流
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_window";
        DataStreamSource<String> kafkaDS = env.addSource(BaseTask.getKafkaSource(topic, groupId));

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("Value>>>>>>>>" + value);
                }
            }
        });

        //TODO 4.按照 order_detail_id 分组、去重(取最后一条数据)
        KeyedStream<JSONObject, String> keyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("id"));
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByDetailIdDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                //取出状态中的数据
                JSONObject lastValue = valueState.value();

                //判断状态数据是否为null
                if (lastValue == null) {
                    valueState.update(value);
                    long processingTime = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerProcessingTimeTimer(processingTime + 5000L);
                } else {

                    //取出状态数据以及当前数据中的时间字段
                    String lastTs = lastValue.getString("row_op_ts");
                    String curTs = value.getString("row_op_ts");

                    if (TimestampLtz3CompareUtil.compare(lastTs, curTs) != 1) {
                        valueState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                //输出数据并清空状态
                out.collect(valueState.value());
                valueState.clear();
            }
        });

        //TODO 5.将每行数据转换为JavaBean
        SingleOutputStreamOperator<TradeProvinceOrderWindow> provinceOrderDS = filterDS.map(line -> {

            HashSet<String> orderIdSet = new HashSet<>();
            orderIdSet.add(line.getString("order_id"));

            return new TradeProvinceOrderWindow("", "",
                    line.getString("province_id"),
                    "",
                    0L,
                    orderIdSet,
                    line.getDouble("split_total_amount"),
                    DateTimeFormatUtil.toTs(line.getString("create_time"), true));
        });

        //TODO 6.提取时间戳生成Watermark
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceWithWmDS = provinceOrderDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
            @Override
            public long extractTimestamp(TradeProvinceOrderWindow element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 7.分组开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceDS = tradeProvinceWithWmDS.keyBy(TradeProvinceOrderWindow::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindow> input, Collector<TradeProvinceOrderWindow> out) throws Exception {

                        TradeProvinceOrderWindow provinceOrderWindow = input.iterator().next();

                        provinceOrderWindow.setTs(System.currentTimeMillis());
                        provinceOrderWindow.setOrderCount((long) provinceOrderWindow.getOrderIdSet().size());
                        provinceOrderWindow.setStt(DateTimeFormatUtil.toYmdHms(window.getStart()));
                        provinceOrderWindow.setEdt(DateTimeFormatUtil.toYmdHms(window.getEnd()));

                        out.collect(provinceOrderWindow);
                    }
                });
        reduceDS.print("reduceDS>>>>>>>>>>>>");

        //TODO 8.关联省份维表补充省份名称字段
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceWithProvinceDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<TradeProvinceOrderWindow>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(TradeProvinceOrderWindow input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public void join(TradeProvinceOrderWindow input, JSONObject dimInfo) {
                        input.setProvinceName(dimInfo.getString("NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //TODO 9.将数据写出到ClickHouse
        reduceWithProvinceDS.print("reduceWithProvinceDS>>>>>>>>>>>");
        reduceWithProvinceDS.addSink(ClickHouseUtil.getSink("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));

        //TODO 10.启动任务
        env.execute("DwsTradeProvinceOrderWindow");

    }

}
