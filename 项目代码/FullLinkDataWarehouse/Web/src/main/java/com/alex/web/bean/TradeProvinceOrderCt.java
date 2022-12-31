package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TradeProvinceOrderCt {
    // 省份名称
    String provinceName;
    // 订单数
    Integer orderCt;
}
