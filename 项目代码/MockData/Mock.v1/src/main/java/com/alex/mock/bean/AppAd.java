package com.alex.mock.bean;

/**
 * @author Alex_liu
 * @create 2023-01-11 19:40
 * @Description 广告
 */
public class AppAd {
    private String entry;//入口：商品列表页=1 应用首页=2 商品详情页=3
    private String action;//动作： 广告展示=1 广告点击=2
    private String contentType;//Type: 1 商品 2 营销活动
    private String displayMills;//展示时长 毫秒数
    private String itemId; //商品 id
    private String activityId; //营销活动 id
    public String getEntry() {
        return entry;
    }
    public void setEntry(String entry) {
        this.entry = entry;
    }
    public String getAction() {
        return action;
    }
    public void setAction(String action) {
        this.action = action;
    }
    public String getActivityId() {
        return activityId;
    }
    public void setActivityId(String activityId) {
        this.activityId = activityId;
    }
    public String getContentType() {
        return contentType;
    }
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
    public String getDisplayMills() {
        return displayMills;
    }
    public void setDisplayMills(String displayMills) {
        this.displayMills = displayMills;
    }
    public String getItemId() {
        return itemId;
    }
    public void setItemId(String itemId) {
        this.itemId = itemId;
    }
}
