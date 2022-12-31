package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficPvPerSession {
    // 渠道
    String ch;
    // 各会话页面浏览数
    Double pvPerSession;
}
