package com.alex.mock.bean;

/**
 * @author Alex_liu
 * @create 2023-01-11 19:37
 * @Description  商品点击日志
 */
public class AppDisplay {
    private String action;//动作：曝光商品=1，点击商品=2，
    private String goodsid;//商品 ID（服务端下发的 ID）
    private String place;//顺序（第几条商品，第一条为 0，第二条为 1，如此类推）
    private String extend1;//曝光类型：1 - 首次曝光 2-重复曝光（没有使用）
    private String category;//分类 ID（服务端定义的分类 ID）
    public String getAction() {
        return action;
    }
    public void setAction(String action) {
        this.action = action;
    }
    public String getGoodsid() {
        return goodsid;
    }
    public void setGoodsid(String goodsid) {
        this.goodsid = goodsid;
    }
    public String getPlace() {
        return place;
    }
    public void setPlace(String place) {
        this.place = place;
    }
    public String getExtend1() {
        return extend1;
    }
    public void setExtend1(String extend1) {
        this.extend1 = extend1;
    }
    public String getCategory() {
        return category;
    }
    public void setCategory(String category) {
        this.category = category;
    }
}
