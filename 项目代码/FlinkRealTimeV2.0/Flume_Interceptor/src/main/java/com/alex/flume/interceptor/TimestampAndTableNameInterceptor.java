package com.alex.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class TimestampAndTableNameInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /*
    * 1 将body当中的table 赋值到header当中的tableName
    * 2 将body当中的ts    赋值到header当中的timestamp
    * */
    @Override
    public Event intercept(Event event) {
        //1 获取header和body
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        Map<String, String> headers = event.getHeaders();

        //2 解析body当中的table 和 ts
        JSONObject jsonObject = JSONObject.parseObject(log);
        String table = jsonObject.getString("table");
        String ts = jsonObject.getString("ts");

        //3 将body当中的table 赋值到header当中的tableName 将body当中的ts    赋值到header当中的timestamp
        headers.put("tableName", table);
        headers.put("timestamp", ts + "000");
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builddr implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimestampAndTableNameInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
