package com.alex.mock.log.enums;


import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum  DisplayType {

    promotion("商品推广"),
    recommend("算法推荐商品"),
    query("查询结果商品"),
    activity("促销活动");


    private String desc;


}
