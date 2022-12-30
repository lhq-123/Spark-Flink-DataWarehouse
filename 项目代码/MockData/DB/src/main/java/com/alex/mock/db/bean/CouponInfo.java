package com.alex.mock.db.bean;

import java.math.BigDecimal;
import com.baomidou.mybatisplus.annotation.IdType;
import java.util.Date;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.io.Serializable;

/**
 * <p>
 * 优惠券表
 * </p>
 *
 * @author  alex
 * @since 2020-02-26
 */
@Data
public class CouponInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 购物券编号
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 购物券名称
     */
    private String couponName;

    /**
     * 购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券
     */
    private String couponType;

    /**
     * 满额数
     */
    private BigDecimal conditionAmount;

    /**
     * 满件数
     */
    private Long conditionNum;

    /**
     * 活动编号
     */
    private Long activityId;

    /**
     * 减金额
     */
    private BigDecimal benefitAmount;

    /**
     * 折扣
     */
    private Long benefitDiscount;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 范围类型 1、商品 2、品类 3、品牌
     */
    private String rangeType;

    /**
     * 商品id
     */
    private Long spuId;

    /**
     * 品牌id
     */
    private Long tmId;

    /**
     * 品类id
     */
    private Long category3Id;

    /**
     * 最多领用次数
     */
    private Integer limitNum;

    /**
     * 修改时间
     */
    private Date operateTime;

    private Date  expireTime;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCouponName() {
        return couponName;
    }

    public void setCouponName(String couponName) {
        this.couponName = couponName;
    }

    public String getCouponType() {
        return couponType;
    }

    public void setCouponType(String couponType) {
        this.couponType = couponType;
    }

    public BigDecimal getConditionAmount() {
        return conditionAmount;
    }

    public void setConditionAmount(BigDecimal conditionAmount) {
        this.conditionAmount = conditionAmount;
    }

    public Long getConditionNum() {
        return conditionNum;
    }

    public void setConditionNum(Long conditionNum) {
        this.conditionNum = conditionNum;
    }

    public Long getActivityId() {
        return activityId;
    }

    public void setActivityId(Long activityId) {
        this.activityId = activityId;
    }

    public BigDecimal getBenefitAmount() {
        return benefitAmount;
    }

    public void setBenefitAmount(BigDecimal benefitAmount) {
        this.benefitAmount = benefitAmount;
    }

    public Long getBenefitDiscount() {
        return benefitDiscount;
    }

    public void setBenefitDiscount(Long benefitDiscount) {
        this.benefitDiscount = benefitDiscount;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getRangeType() {
        return rangeType;
    }

    public void setRangeType(String rangeType) {
        this.rangeType = rangeType;
    }

    public Long getSpuId() {
        return spuId;
    }

    public void setSpuId(Long spuId) {
        this.spuId = spuId;
    }

    public Long getTmId() {
        return tmId;
    }

    public void setTmId(Long tmId) {
        this.tmId = tmId;
    }

    public Long getCategory3Id() {
        return category3Id;
    }

    public void setCategory3Id(Long category3Id) {
        this.category3Id = category3Id;
    }

    public Integer getLimitNum() {
        return limitNum;
    }

    public void setLimitNum(Integer limitNum) {
        this.limitNum = limitNum;
    }

    public Date getOperateTime() {
        return operateTime;
    }

    public void setOperateTime(Date operateTime) {
        this.operateTime = operateTime;
    }

    @Override
    public String toString() {
        return "CouponInfo{" +
        "id=" + id +
        ", couponName=" + couponName +
        ", couponType=" + couponType +
        ", conditionAmount=" + conditionAmount +
        ", conditionNum=" + conditionNum +
        ", activityId=" + activityId +
        ", benefitAmount=" + benefitAmount +
        ", benefitDiscount=" + benefitDiscount +
        ", createTime=" + createTime +
        ", rangeType=" + rangeType +
        ", spuId=" + spuId +
        ", tmId=" + tmId +
        ", category3Id=" + category3Id +
        ", limitNum=" + limitNum +
        ", operateTime=" + operateTime +
        "}";
    }
}
