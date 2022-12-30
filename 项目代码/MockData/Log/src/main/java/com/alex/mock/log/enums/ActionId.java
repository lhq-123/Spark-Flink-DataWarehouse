package com.alex.mock.log.enums;


public enum ActionId {


    favor_add("添加收藏"),
    favor_canel("取消收藏"),
    cart_add("添加购物车"),
    cart_remove("删除购物车"),
    cart_add_num("增加购物车商品数量"),
    cart_minus_num("减少购物车商品数量"),
    trade_add_address("增加收货地址"),
    get_coupon("领取优惠券");


    private  String desc;


    ActionId(String desc ){
       this.desc=desc;
    }


}
