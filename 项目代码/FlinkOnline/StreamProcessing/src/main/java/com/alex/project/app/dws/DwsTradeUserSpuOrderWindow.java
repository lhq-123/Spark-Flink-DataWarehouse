package com.alex.project.app.dws;

import com.alex.project.app.base.BaseTask;
import com.alex.project.bean.TradeUserSpuOrderBean;
import com.alex.project.common.DimAsyncFunction;
import com.alex.project.utils.ClickHouseUtil;
import com.alex.project.utils.DateTimeFormatUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK) -> DwsTradeUserSpuOrderWindow(Phoenix-(HBase-HDFS、ZK)、Redis) -> ClickHouse(ZK)
public class DwsTradeUserSpuOrderWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数


        //TODO 2.读取Kafka DWD层下单主题数据创建流
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_user_spu_order_window_211126";
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

        //TODO 4.按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("id"));

        //TODO 5.针对 order_detail_id 进行去重(保留第一条数据即可)
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByDetailIdDS.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("is-exists", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);

                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //获取状态数据
                String state = valueState.value();

                //判断状态是否为null
                if (state == null) {
                    valueState.update("1");
                    return true;
                } else {
                    return false;
                }
            }
        });

        //TODO 6.将数据转换为JavaBean对象
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuDS = filterDS.map(jsonObject -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getString("order_id"));

            return TradeUserSpuOrderBean.builder()
                    .skuId(jsonObject.getString("sku_id"))
                    .userId(jsonObject.getString("user_id"))
                    .orderAmount(jsonObject.getDouble("split_total_amount"))
                    .orderIdSet(orderIds)
                    .ts(DateTimeFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();
        });
        tradeUserSpuDS.print("tradeUserSpuDS>>>>>>>>>>>>>>");

        //TODO 7.关联sku_info维表 补充 spu_id,tm_id,category3_id
//        tradeUserSpuDS.map(new RichMapFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean>() {
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                //创建Phoenix连接池
//            }
//            @Override
//            public TradeUserSpuOrderBean map(TradeUserSpuOrderBean value) throws Exception {
//                //查询维表,将查到的信息补充至JavaBean中
//                return null;
//            }
//        });
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWithSkuDS = AsyncDataStream.unorderedWait(
                tradeUserSpuDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setSpuId(dimInfo.getString("SPU_ID"));
                        input.setTrademarkId(dimInfo.getString("TM_ID"));
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }
                },
                100, TimeUnit.SECONDS);

        tradeUserSpuWithSkuDS.print("tradeUserSpuWithSkuDS>>>>>>>>>>>");

        //TODO 8.提取事件时间生成Watermark
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWithWmDS = tradeUserSpuWithSkuDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeUserSpuOrderBean>() {
            @Override
            public long extractTimestamp(TradeUserSpuOrderBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 9.分组、开窗、聚合
        KeyedStream<TradeUserSpuOrderBean, Tuple4<String, String, String, String>> keyedStream = tradeUserSpuWithWmDS.keyBy(new KeySelector<TradeUserSpuOrderBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TradeUserSpuOrderBean value) throws Exception {
                return new Tuple4<>(value.getUserId(),
                        value.getSpuId(),
                        value.getTrademarkId(),
                        value.getCategory3Id());
            }
        });
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeUserSpuOrderBean>() {
                    @Override
                    public TradeUserSpuOrderBean reduce(TradeUserSpuOrderBean value1, TradeUserSpuOrderBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<TradeUserSpuOrderBean> input, Collector<TradeUserSpuOrderBean> out) throws Exception {

                        TradeUserSpuOrderBean userSpuOrderBean = input.iterator().next();

                        userSpuOrderBean.setTs(System.currentTimeMillis());
                        userSpuOrderBean.setOrderCount((long) userSpuOrderBean.getOrderIdSet().size());
                        userSpuOrderBean.setStt(DateTimeFormatUtil.toYmdHms(window.getStart()));
                        userSpuOrderBean.setEdt(DateTimeFormatUtil.toYmdHms(window.getEnd()));

                        out.collect(userSpuOrderBean);
                    }
                });

        //TODO 10.关联spu,tm,category维表补充相应的信息
        //10.1 关联SPU表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithSpuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getSpuId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setSpuName(dimInfo.getString("SPU_NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //10.2 关联Tm表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithTmDS = AsyncDataStream.unorderedWait(reduceWithSpuDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //10.3 关联Category3
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCategory3DS = AsyncDataStream.unorderedWait(reduceWithTmDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setCategory3Name(dimInfo.getString("NAME"));
                        input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }
                }, 100, TimeUnit.SECONDS);

        //10.4 关联Category2
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCategory2DS = AsyncDataStream.unorderedWait(reduceWithCategory3DS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("NAME"));
                        input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                }, 100, TimeUnit.SECONDS);

        //10.5 关联Category1
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCategory1DS = AsyncDataStream.unorderedWait(reduceWithCategory2DS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //TODO 11.将数据写出到ClickHouse
        reduceWithCategory1DS.print(">>>>>>>>>>>>>>>>>");
        reduceWithCategory1DS.addSink(ClickHouseUtil.getSink("insert into dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 12.启动
        env.execute("DwsTradeUserSpuOrderWindow");

    }

}