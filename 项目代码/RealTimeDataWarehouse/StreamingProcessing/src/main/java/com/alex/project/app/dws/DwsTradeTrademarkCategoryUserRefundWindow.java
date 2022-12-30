package com.alex.project.app.dws;

import com.alex.project.app.base.BaseTask;
import com.alex.project.bean.TradeTrademarkCategoryUserRefundBean;
import com.alex.project.common.DimAsyncFunction;
import com.alex.project.utils.ClickHouseUtil;
import com.alex.project.utils.DateTimeFormatUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdTradeOrderRefund -> Kafka(ZK) -> DwsTradeTrademarkCategoryUserRefundWindow(Phoenix(HBase-HDFS、ZK)、Redis) -> ClickHouse(ZK)
public class DwsTradeTrademarkCategoryUserRefundWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数


        //TODO 2.读取 Kafka DWD层 退单主题数据
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window";
        DataStreamSource<String> kafkaDS = env.addSource(BaseTask.getKafkaSource(topic, groupId));

        //TODO 3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeTmCategoryUserDS = kafkaDS.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getString("order_id"));

            return TradeTrademarkCategoryUserRefundBean.builder()
                    .skuId(jsonObject.getString("sku_id"))
                    .userId(jsonObject.getString("user_id"))
                    .orderIdSet(orderIds)
                    .ts(DateTimeFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();
        });

        //TODO 4.关联sku_info维表补充 tm_id以及category3_id
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeWithSkuDS = AsyncDataStream.unorderedWait(
                tradeTmCategoryUserDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setTrademarkId(dimInfo.getString("TM_ID"));
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }
                }, 100, TimeUnit.SECONDS);

        //TODO 5.分组开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceDS = tradeWithSkuDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                return element.getTs();
            }
        })).keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(TradeTrademarkCategoryUserRefundBean value) throws Exception {
                return new Tuple3<>(value.getUserId(),
                        value.getTrademarkId(),
                        value.getCategory3Id()
                );
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple3<String, String, String> stringStringStringTuple3, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {

                        TradeTrademarkCategoryUserRefundBean refundBean = input.iterator().next();

                        refundBean.setTs(System.currentTimeMillis());
                        refundBean.setEdt(DateTimeFormatUtil.toYmdHms(window.getEnd()));
                        refundBean.setStt(DateTimeFormatUtil.toYmdHms(window.getStart()));
                        refundBean.setRefundCount((long) refundBean.getOrderIdSet().size());

                        out.collect(refundBean);
                    }
                });

        //TODO 6.关联维表补充其他字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWithTmDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWith3DS = AsyncDataStream.unorderedWait(
                reduceWithTmDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory3Name(dimInfo.getString("NAME"));
                        input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }
                }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWith2DS = AsyncDataStream.unorderedWait(
                reduceWith3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("NAME"));
                        input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWith1DS = AsyncDataStream.unorderedWait(
                reduceWith2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //TODO 7.将数据写出到ClickHouse
        reduceWith1DS.print(">>>>>>>>>>>>");
        reduceWith1DS.addSink(ClickHouseUtil.getSink("insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");

    }

}
