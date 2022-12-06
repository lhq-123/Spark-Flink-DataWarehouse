package com.alex.project.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;

/**
 * @author Alex_liu
 * @create 2022-12-06 16:06
 * @Description  将数据流直接插入hive表里
 *    ds.addSink(new ToHiveSink("db_name","tbl_name"))
 */
public class ToHiveSink extends RichSinkFunction<JSONObject> {
    private static Logger logger = LoggerFactory.getLogger(ToHiveSink.class.getSimpleName());

    private String dataBaseName;
    private String tableName;

    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://Flink01:10000";

    private static Connection conn = null;
    private static Statement stmt = null;

    public ToHiveSink() {
    }

    /**
     * @param dataBaseName
     * @param tableName
     */
    public ToHiveSink(String dataBaseName, String tableName) {
        this.dataBaseName = dataBaseName;
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            /** 第一步：把JDBC驱动通过反射的方式加载进来 */
            Class.forName(driver);
            /** 第二步：通过JDBC建立和Hive的连接器，默认端口是10000 */
            conn = DriverManager.getConnection(url, "root", "123456");
            /** 第三步：创建Statement句柄，基于该句柄进行SQL的各种操作 */
            stmt = conn.createStatement();
            /** 第四步：库不存在创建库，表不存在创建表 */
            schemaAndTableExists(dataBaseName, tableName, stmt);
        } catch (Exception e) {
            logger.error("connect hive fail,detail info:", e.getMessage());
        }
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into " + tableName + " partition(dt='");
//        sb.append(DateUtil.getTodayDate());
        sb.append("2022-11-22");
        sb.append("') values ('");
        sb.append(value.getString("data"));
        sb.append("')");
        logger.info("insert data sql:"+sb.toString());
        logger.info("insert error data sql:",sb.toString());
        PreparedStatement ps = null;
        try {
            stmt.executeUpdate(sb.toString());
        } catch (Exception e) {
            logger.error("Insert Data Error! Error Info:{}",e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        closeConnection();
    }

    /**
     * 判断数据库、表是否存在，不存在就创建
     *
     * @param databaseName
     * @param tableName
     * @param stmt
     */
    private static void schemaAndTableExists(String databaseName, String tableName, Statement stmt) throws SQLException, SQLException {
        // 如果数据库不存在，创建库
        String createDataBaseSql = "create database if not exists " + databaseName;
        Boolean result = stmt.execute(createDataBaseSql);
        logger.warn("Database " + databaseName + " created successfully or already exists.");

        // 如果表不存在，创建表
        String createTableSql = "create table IF NOT EXISTS "+ tableName + " (json string) partitioned by(dt String)";
        // 选择库
        stmt.execute("use "+ databaseName);
        stmt.execute(createTableSql);
        logger.warn("Table " + tableName + " created successfully or already exists.");
    }

    public static void closeConnection() throws SQLException {
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}

