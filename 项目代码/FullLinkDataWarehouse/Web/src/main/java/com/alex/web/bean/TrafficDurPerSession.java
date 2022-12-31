package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
public class TrafficDurPerSession {
    // 渠道
    String ch;
    // 各会话页面访问时长
    Double durPerSession;
}
