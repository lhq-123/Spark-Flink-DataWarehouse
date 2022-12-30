package com.alex.mock.db.bean;

import java.util.Date;
import java.io.Serializable;

/**
 * <p>
 * 商品评论表
 * </p>
 *
 * @author  alex
 * @since 2020-02-24
 */
public class CommentInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 编号
     */
    private Long id;

    /**
     * 用户名称
     */
    private Long userId;

    /**
     * skuid
     */
    private Long skuId;

    /**
     * 商品id
     */
    private Long spuId;

    /**
     * 订单编号
     */
    private Long orderId;

    /**
     * 评价 1 好评 2 中评 3 差评
     */
    private String appraise;

    /**
     * 评价内容
     */
    private String commentTxt;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 修改时间
     */
    private Date operateTime;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getSkuId() {
        return skuId;
    }

    public void setSkuId(Long skuId) {
        this.skuId = skuId;
    }

    public Long getSpuId() {
        return spuId;
    }

    public void setSpuId(Long spuId) {
        this.spuId = spuId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getAppraise() {
        return appraise;
    }

    public void setAppraise(String appraise) {
        this.appraise = appraise;
    }

    public String getCommentTxt() {
        return commentTxt;
    }

    public void setCommentTxt(String commentTxt) {
        this.commentTxt = commentTxt;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getOperateTime() {
        return operateTime;
    }

    public void setOperateTime(Date operateTime) {
        this.operateTime = operateTime;
    }

    @Override
    public String toString() {
        return "CommentInfo{" +
        "id=" + id +
        ", userId=" + userId +
        ", skuId=" + skuId +
        ", spuId=" + spuId +
        ", orderId=" + orderId +
        ", appraise=" + appraise +
        ", commentTxt=" + commentTxt +
        ", createTime=" + createTime +
        ", operateTime=" + operateTime +
        "}";
    }
}
