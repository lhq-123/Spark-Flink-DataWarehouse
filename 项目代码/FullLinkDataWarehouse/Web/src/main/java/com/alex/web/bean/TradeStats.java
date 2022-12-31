package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TradeStats {
    // 指标类型
    String type;
    // 度量值
    Integer orderCt;
}
