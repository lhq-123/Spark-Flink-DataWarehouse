package com.alex.project.sink;

import com.alex.project.bean.PhoenixConfig;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author Alex_liu
 * @create 2022-12-01 11:35
 * @Description  将分流之后的维度数据也就是要sink到hbase的数据写入到hbase
 */
public class ToHbaseSink extends RichSinkFunction<JSONObject> {
    private Logger logger = LoggerFactory.getLogger("ToHbaseSink");
    private Connection conn = null;

    /**
     * 重写open方法：加载资源配置
     *    创建hbase配置对象，设置hbase配置信息，添加客户端相关配置
     *    跟据hbase的连接地址，获得hbase连接
     *    根据hbase连接和表名，获得hbase客户端Table对象
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取Phoenix连接
        Class.forName(PhoenixConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER);
        conn.setAutoCommit(true);
    }

    /**
     * 重新invoke方法，将数据写入hbase
     * @param value
     * @param context
     * @throws Exception
     *   value:{"sinkTable":"dim_base_trademark","database":"gmall","before":{"tm_name":"aaa","id":12},
     *   "after":{"tm_name":"bbb","id":12},"type":"update","tableName":"base_trademark"}
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        //获取创建更新数据SQL语句需要的信息
        try {
            String sinkTable = value.getString("sinkTable");
            JSONObject data = value.getJSONObject("data");
            //创建更新数据的SQL
            String upsertSql = createUpsertSql(sinkTable, data);
            //打印
            System.out.println(upsertSql);
            //编译SQL
            preparedStatement= conn.prepareStatement(upsertSql);
            //判断如果当前数据为更新操作，先删除Redis中的数据
//        if ("update".equals(value.getString("type"))){
//            DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
//        }
            //执行SQL
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("更新Phoenix表插入数据失败");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    /**
     * 创建插入数据的SQL语句
     *  upsert into tbl_name(xxx,xxx,xxx)  values(xxx,xxx,xxx)
     * @param tbl_name
     * @param data
     * @return
     */
    private String createUpsertSql(String tbl_name, JSONObject data) {

        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        return "upsert into " + PhoenixConfig.HBASE_SCHEMA + "." + tbl_name + "(" +
                StringUtils.join(keySet, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
