package com.alex.project.common;

import com.alex.project.bean.TableProcess;
import com.alex.project.bean.PhoenixConfig;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag,MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    /**
     * 获取Phoenix连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(PhoenixConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER);
    }

    /**
     * 处理广播流
     * 1.获取配置流并解析数据
     * 2.校验表是否存在，不存在则需在Phoenix中创建
     * 3.将处理好的配置流写入状态，广播出去跟主流连接
     * @param jsonStr
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {

        //配置流数据格式：{"database":"gmall","table":"table_process","type":"insert","ts":1669777172,"xid":9036,"commit":true,"data":{"source_table":"coupon_use","operate_type":"insert","sink_type":"kafka","sink_table":"dwd_coupon_use","sink_columns":"id,coupon_id,user_id,order_id,coupon_status,get_type,get_time,using_time,used_time,expire_time","sink_pk":"id","sink_extend":" SALT_BUCKETS = 3"}}
        //提取出来的："data":{"source_table":"coupon_use","operate_type":"insert","sink_type":"kafka","sink_table":"dwd_coupon_use","sink_columns":"expire_time,....","sink_pk":"id","sink_extend":" SALT_BUCKETS = 3"}}

        //将配置流中的数据转换为JSON对象
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        //取出数据中的表名及操作类型封装成key
        String data = jsonObject.getString("data");
        //打印data的value值
        System.out.println(data);
        //取出value数据封装为TableProcess对象
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);
        //校验并建表
        System.out.println("Sink类型:"+tableProcess.getSinkType()+"--要Sink的表:"+tableProcess.getSinkTable());
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }
        //获取状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + ":" + tableProcess.getOperateType();
        //3.写入状态,广播出去
        broadcastState.put(key, tableProcess);
    }

    /**
     * Phoenix 建表
     *
     * @param sinkTable 表名 test
     * @param sinkColumns 表名字段 id,name,sex
     * @param sinkPk 表主键 id
     * @param sinkExtend 表扩展字段 ""
     * create table if not exists mydb.test(id varchar primary key,name varchar,sex varchar) ...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend)
    {
        //给主键以及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        //封装建表 SQL
        StringBuilder createSql = new StringBuilder("create table if not exists ")
                .append(PhoenixConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");
        //遍历添加字段信息
        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            //取出字段
            String field = fields[i];
            //判断当前字段是否为主键
            if (sinkPk.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            } else {
                createSql.append(field).append(" varchar ");
            }
            //如果当前字段不是最后一个字段,则追加","
            if (i < fields.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(sinkExtend);
        //打印
        System.out.println(createSql);
        //执行建表 SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建 Phoenix 表" + sinkTable + "失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 核心处理方法：
     * 1.获取广播的配置数据
     * 2.过滤字段
     * 3.根据配置数据的信息为每条数据打上标签，决定其sink到Kafka还是Hbase
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx,
                               Collector<JSONObject> out) throws Exception {
        //获取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState =
                ctx.getBroadcastState(mapStateDescriptor);
        //获取表名和操作类型
        String table = value.getString("table");
        String type = value.getString("type");
        String key = table + ":" + type;
        //取出对应的配置信息数据
        TableProcess tableProcess = broadcastState.get(key);
        //分流
        if (tableProcess != null) {
            //根据配置信息中提供的字段做数据过滤(sink_column)，判断要sink到kafka还是hbase
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());
            //向数据中追加 sink_table(输出表)信息
            value.put("sinkTable", tableProcess.getSinkTable());
            //判断当前数据应该写往 HBASE还是Kafka
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //Kafka 数据,将数据输出到主流
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //HBase 数据,将数据输出到侧输出流
                ctx.output(objectOutputTag, value);
            }
        } else {
            //业务数据中的表不一定全部都需要，有些表在配置表中不存在，但是它的业务数据又发生了变化，并且在初始化
            //的时候又能读到，在正常情况下是会出现的
            System.out.println("表" + table + "在table_process中不存在");
        }
    }

    /**
     * 过滤字段
     *
     * @param data        {"id":13,"tm_name":"aaa","logo_url":"/bbb/bbb"}
     * @param sinkColumns "id,tm_name"
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

        //切分sinkColumns
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

//        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }
}