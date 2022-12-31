package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrademarkCommodityStats {
    // 品牌名称
    String trademarkName;
    // 订单数
    Integer orderCt;
    // 订单人数
    Integer uuCt;
    // 订单金额
    Double orderAmount;
    // 退单数
    Integer refundCt;
    // 退单人数
    Integer refundUuCt;
}
