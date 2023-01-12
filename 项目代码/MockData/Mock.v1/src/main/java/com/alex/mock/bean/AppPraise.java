package com.alex.mock.bean;

/**
 * @author Alex_liu
 * @create 2023-01-11 19:42
 * @Description 用户点赞
 */
public class AppPraise {
    private int id; //主键 id
    private int userid;//用户 id
    private int target_id;//点赞的对象 id
    private int type;//点赞类型 1 问答点赞 2 问答评论点赞 3 文章点赞数 4 评论点赞
    private String add_time;//添加时间
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public int getUserid() {
        return userid;
    }
    public void setUserid(int userid) {
        this.userid = userid;
    }
    public int getTarget_id() {
        return target_id;
    }
    public void setTarget_id(int target_id) {
        this.target_id = target_id;
    }
    public int getType() {
        return type;
    }
    public void setType(int type) {
        this.type = type;
    }
    public String getAdd_time() {
        return add_time;
    }
    public void setAdd_time(String add_time) {
        this.add_time = add_time;
    }
}
