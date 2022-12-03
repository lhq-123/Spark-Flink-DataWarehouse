package com.alex.project.common;

import com.alex.project.utils.DimUtil;
import com.alex.project.utils.DruidDSUtil;
import com.alex.project.utils.PhoenixUtil;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    //value:{"database":"gmall-211126-flink","table":"base_trademark","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,"tm_name":"atguigu"},"old":{"logo_url":"/aaa/aaa"},"sinkTable":"dim_xxx"}
    //value:{"database":"gmall-211126-flink","table":"order_info","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,...},"old":{"xxx":"/aaa/aaa"},"sinkTable":"dim_xxx"}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        //获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();

        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");

        //获取数据类型
        String type = value.getString("type");
        //如果为更新数据,则需要删除Redis中的数据
        if ("update".equals(type)) {
            DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), data.getString("id"));
        }

        //写出数据
        PhoenixUtil.upsertValues(connection, sinkTable, data);

        //归还连接
        connection.close();
    }

}
