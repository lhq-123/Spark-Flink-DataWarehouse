package com.alex.project.app.dwm;

import com.alex.project.app.base.BaseTask;
import com.alex.project.bean.OrderWide;
import com.alex.project.bean.PaymentInfo;
import com.alex.project.bean.PaymentWide;
import com.alex.project.utils.ConfigLoader;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author Alex_liu
 * @create 2022-12-01 18:22
 * @Description 支付表没有到订单明细，支付金额没有细分到商品上，没有办法统计商品级的支付状况
 *              所以我们要将支付表的信息与订单宽表关联上
 * 解决方案有两个:
 * 一个是把订单宽表输出到 HBase 上，在支付宽表计算时查询 HBase，这相当于把订单宽表作为一种维度进行管理。
 * 一个是用流的方式接收订单宽表，然后用双流 join 方式进行合并。因为订单与支付产生有一定的时差。所以必须用 intervalJoin 来管理流的状态时间，保证当支付到达时订单宽表还保存在状态中。
 */
//数据流：web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix(dwd-dim) -> FlinkApp(redis) -> Kafka(dwm) -> FlinkApp -> Kafka(dwm)
//程  序：         MockDb               -> Mysql -> FlinkCDC -> Kafka(ZK) -> BaseDbApp -> Kafka/Phoenix(zk/hdfs/hbase) -> OrderWideApp(Redis) -> Kafka -> PaymentWideApp -> Kafka
public class PayWide extends BaseTask{
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = getEnv(PayWide.class.getSimpleName());

        //TODO 2.读取Kafka主题的数据创建流 并转换为JavaBean对象 提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(BaseTask.getKafkaSource(ConfigLoader.get("kafka_dwm_orderWide"), ConfigLoader.get("group_dwm_pt")))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(BaseTask.getKafkaSource(ConfigLoader.get("kafka_dwd_payment_info"), ConfigLoader.get("group_dwm_pt")))
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));

        //TODO 3.双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 4.将数据写入Kafka
        paymentWideDS.print(">>>>>>>>>");
        paymentWideDS
                .map(JSONObject::toJSONString)
                .addSink(BaseTask.getKafkaProducer(ConfigLoader.get("kafka_dwm_paymentWide")));

        //TODO 5.启动任务
        env.execute("PaymentWideApp");

    }

}