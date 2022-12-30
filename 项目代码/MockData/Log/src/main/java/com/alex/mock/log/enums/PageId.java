package com.alex.mock.log.enums;


public enum PageId {

    home("首页"),
    category("分类页"),
    discovery("发现页"),
    top_n("热门排行"),
    favor("收藏页"),
    search("搜索页"),
    good_list("商品列表页"),
    good_detail("商品详情"),
    good_spec("商品规格"),
    comment("评价"),
    comment_done("评价完成"),
    comment_list("评价列表"),
    cart("购物车"),
    trade("下单结算"),
    payment("支付页面"),
    payment_done("支付完成"),
    orders_all("全部订单"),
    orders_unpaid("订单待支付"),
    orders_undelivered("订单待发货"),
    orders_unreceipted("订单待收货"),
    orders_wait_comment("订单待评价"),
    mine("我的"),
    activity("活动"),
    login("登录"),
    register("注册");


    private  String desc;


    PageId(String desc ){
       this.desc=desc;
    }


}
