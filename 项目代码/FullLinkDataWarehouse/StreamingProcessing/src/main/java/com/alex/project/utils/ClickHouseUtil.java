package com.alex.project.utils;

import com.alex.project.bean.ClickHouseConfig;
import com.alex.project.bean.PhoenixConfig;
import com.alex.project.bean.TransientSink;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;


//obj.getField()   =>  field.get(obj)
//obj.method(args) =>  method.invoke(obj,args)
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql) {

        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try {
                            //使用反射的方式获取t对象中的数据
                            Class<?> tClz = t.getClass();

                            //获取所有的属性信息
                            Field[] fields = tClz.getClass().getDeclaredFields();

                            //遍历字段
                            int offset = 0;
                            for (int i = 0; i < fields.length; i++) {
                                //获取字段
                                Field field = fields[i];
                                //设置私有属性可访问
                                field.setAccessible(true);

                                //获取字段上注解
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation != null) {
                                    //存在该注解
                                    offset++;
                                    continue;
                                }

                                //获取值
                                Object value = field.get(t);

                                //给预编译SQL对象赋值
                                preparedStatement.setObject(i + 1 - offset, value);
                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        //.withDriverName(ClickHouseConfig.CLICKHOUSE_DRIVER)
                        .withDriverName(ConfigLoader.get("clickhouse.driver"))
                        //.withUrl(ClickHouseConfig.CLICKHOUSE_URL)
                        .withUrl(ConfigLoader.get("clickhouse.url"))
                        .build());

    }

}
