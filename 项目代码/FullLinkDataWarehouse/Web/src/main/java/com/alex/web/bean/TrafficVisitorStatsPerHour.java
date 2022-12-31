package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficVisitorStatsPerHour {
    // 小时
    Integer hr;
    // 独立访客数
    Long uvCt;
    // 页面浏览数
    Long pvCt;
    // 新访客数
    Long newUvCt;
}
