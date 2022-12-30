package com.alex.mock.db.bean;

import lombok.Data;

import java.util.Date;
import java.io.Serializable;

/**
 * <p>
 * 商品收藏表
 * </p>
 *
 * @author  alex
 * @since 2020-02-24
 */
@Data
public class FavorInfo implements Serializable {

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
     * 是否已取消 0 正常 1 已取消
     */
    private String isCancel;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 修改时间
     */
    private Date cancelTime;


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



    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getCancelTime() {
        return cancelTime;
    }

    public void setCancelTime(Date cancelTime) {
        this.cancelTime = cancelTime;
    }

    @Override
    public String toString() {
        return "FavorInfo{" +
        "id=" + id +
        ", userId=" + userId +
        ", skuId=" + skuId +
        ", spuId=" + spuId +
        ", isCancel=" + isCancel +
        ", createTime=" + createTime +
        ", cancelTime=" + cancelTime +
        "}";
    }
}
