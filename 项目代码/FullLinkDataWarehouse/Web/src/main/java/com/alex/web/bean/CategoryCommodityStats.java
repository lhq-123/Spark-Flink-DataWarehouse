package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CategoryCommodityStats {
    // 一级品类名称
    String category1_name;
    // 二级品类名称
    String category2_name;
    // 三级品类名称
    String category3_name;
    // 订单数
    Integer orderCt;
    // 下单用户数
    Integer uuCt;
    // 订单金额
    Double orderAmount;
    // 退单数
    Integer refundCt;
    // 退单金额
    Double refundUcCt;
}
