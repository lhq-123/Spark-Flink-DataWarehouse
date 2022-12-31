package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
public class TrafficUjRate {
    // 渠道
    String ch;
    // 跳出率
    Double ujRate;
}
