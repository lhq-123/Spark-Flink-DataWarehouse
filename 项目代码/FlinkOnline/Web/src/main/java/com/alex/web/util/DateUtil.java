package com.alex.web.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtil {
    public static Integer now() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        String date = dtf.format(LocalDateTime.now());
        return Integer.parseInt(date);
    }
}
