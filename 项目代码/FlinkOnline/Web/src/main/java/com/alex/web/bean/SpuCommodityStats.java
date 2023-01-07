package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SpuCommodityStats {
    // SPU 名称
    String spuName;
    // 下单数
    Integer orderCt;
    // 下单用户数
    Integer uuCt;
    // 下单金额
    Double orderAmount;
}
