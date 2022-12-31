package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrademarkOrderAmountPieGraph {
    // 品牌名称
    String trademarkName;
    // 销售额
    Double orderAmount;
}
