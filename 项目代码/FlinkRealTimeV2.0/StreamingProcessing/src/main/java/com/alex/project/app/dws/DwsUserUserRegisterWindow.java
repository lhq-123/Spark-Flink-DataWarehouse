package com.alex.project.app.dws;

import com.alex.project.app.base.BaseTask;
import com.alex.project.bean.UserRegisterBean;
import com.alex.project.utils.ClickHouseUtil;
import com.alex.project.utils.DateTimeFormatUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdUserRegister -> Kafka(ZK) -> DwsUserUserRegisterWindow -> ClickHouse(ZK)
public class DwsUserUserRegisterWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //TODO 2.读取Kafka DWD层用户注册主题数据创建流
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window";
        DataStreamSource<String> kafkaDS = env.addSource(BaseTask.getKafkaSource(topic, groupId));

        //TODO 3.将每行数据转换为JavaBean对象
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDS = kafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            //yyyy-MM-dd HH:mm:ss
            String createTime = jsonObject.getString("create_time");

            return new UserRegisterBean("",
                    "",
                    1L,
                    DateTimeFormatUtil.toTs(createTime, true));
        });

        //TODO 4.提取时间戳生成Watermark
        SingleOutputStreamOperator<UserRegisterBean> userRegisterWithWmDS = userRegisterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 5.开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDS = userRegisterWithWmDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                        UserRegisterBean userRegisterBean = values.iterator().next();

                        userRegisterBean.setTs(System.currentTimeMillis());
                        userRegisterBean.setStt(DateTimeFormatUtil.toYmdHms(window.getStart()));
                        userRegisterBean.setEdt(DateTimeFormatUtil.toYmdHms(window.getEnd()));

                        out.collect(userRegisterBean);
                    }
                });

        //TODO 6.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>>>");
        resultDS.addSink(ClickHouseUtil.getSink("insert into dws_user_user_register_window values(?,?,?,?)"));

        //TODO 7.启动任务
        env.execute("DwsUserUserRegisterWindow");

    }

}
