package com.alex.project.app.dws;

import com.alex.project.app.base.BaseTask;
import com.alex.project.bean.TradePaymentWindowBean;
import com.alex.project.utils.ClickHouseUtil;
import com.alex.project.utils.DateTimeFormatUtil;
import com.alex.project.utils.TimestampLtz3CompareUtil;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK) -> DwdTradePayDetailSuc -> Kafka(ZK) -> DwsTradePaymentSucWindow -> ClickHouse(ZK)
public class DwsTradePaymentSucWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数

        //1.1 开启CheckPoint
        //env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        //1.2 设置状态后端
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/211126/ck");
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        //1.3 设置状态的TTL  生产环境设置为最大乱序程度
        //tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        //TODO 2.读取DWD层成功支付主题数据创建流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window_211126";
        DataStreamSource<String> kafkaDS = env.addSource(BaseTask.getKafkaSource(topic, groupId));

        //TODO 3.将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println(">>>>>>>" + value);
                }
            }
        });

        //TODO 4.按照订单明细id分组
        KeyedStream<JSONObject, String> jsonObjKeyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));

        //TODO 5.使用状态编程保留最新的数据输出
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjKeyedByDetailIdDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                //获取状态中的数据
                JSONObject state = valueState.value();

                //判断状态是否为null
                if (state == null) {
                    valueState.update(value);
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
                } else {
                    String stateRt = state.getString("row_op_ts");
                    String curRt = value.getString("row_op_ts");

                    int compare = TimestampLtz3CompareUtil.compare(stateRt, curRt);

                    if (compare != 1) {
                        valueState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                //输出并清空状态数据
                JSONObject value = valueState.value();
                out.collect(value);

                valueState.clear();
            }
        });

        //TODO 6.提取事件时间生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                String callbackTime = element.getString("callback_time");
                return DateTimeFormatUtil.toTs(callbackTime, true);
            }
        }));

        //TODO 7.按照user_id分组
        KeyedStream<JSONObject, String> keyedByUidDS = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));

        //TODO 8.提取独立支付成功用户数
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentDS = keyedByUidDS.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {

            private ValueState<String> lastDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-dt", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradePaymentWindowBean> out) throws Exception {

                //取出状态中以及当前数据的日期
                String lastDt = lastDtState.value();
                String curDt = value.getString("callback_time").split(" ")[0];

                //定义当日支付人数以及新增付费人数
                long paymentSucUniqueUserCount = 0L;
                long paymentSucNewUserCount = 0L;

                //判断状态日期是否为null
                if (lastDt == null) {
                    paymentSucUniqueUserCount = 1L;
                    paymentSucNewUserCount = 1L;
                    lastDtState.update(curDt);
                } else if (!lastDt.equals(curDt)) {
                    paymentSucUniqueUserCount = 1L;
                    lastDtState.update(curDt);
                }

                //返回数据
                if (paymentSucUniqueUserCount == 1L) {
                    out.collect(new TradePaymentWindowBean("",
                            "",
                            paymentSucUniqueUserCount,
                            paymentSucNewUserCount,
                            null));
                }
            }
        });

        //TODO 9.开窗、聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDS = tradePaymentDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        return value1;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {

                        TradePaymentWindowBean next = values.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateTimeFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateTimeFormatUtil.toYmdHms(window.getStart()));

                        out.collect(next);
                    }
                });

        //TODO 10.将数据写出到ClickHouse
        resultDS.print(">>>>>>>");
        resultDS.addSink(ClickHouseUtil.getSink("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));

        //TODO 11.启动任务
        env.execute("DwsTradePaymentSucWindow");

    }

}
