package com.alex.project.bean;

/**
 * @author Alex_liu
 * @create 2022-11-29 20:47
 * @Description
 */
public class PhoenixConfig {
    //Phoenix 库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME_V2";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:Flink01,Flink02,Flink03:2181";

}
