package com.alex.flume.utils;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;

public class JSONUtil {

    public static boolean isJSONValidate(String log) {
        try {
            JSONObject.parseObject(log);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static void main(String[] args) {
        List<String> events = new ArrayList<>();
        events.add("a"); //index = 0
        events.add("b"); //index = 1
        events.add("c"); //index = 2
        events.add("d"); //index = 3

        System.out.println(events);
        events.remove(1);
        System.out.println(events);
        events.remove(3);
        System.out.println(events);
    }
}
