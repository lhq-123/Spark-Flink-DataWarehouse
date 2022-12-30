package com.alex.mock.db.bean;

import java.math.BigDecimal;
import java.io.Serializable;

/**
 * <p>
 * 优惠规则
 * </p>
 *
 * @author  alex
 * @since 2020-02-25
 */
public class ActivityRule implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 编号
     */
    private Integer id;

    /**
     * 类型
     */
    private Integer activityId;

    /**
     * 满减金额
     */
    private BigDecimal conditionAmount;

    /**
     * 满减件数
     */
    private Long conditionNum;

    /**
     * 优惠金额
     */
    private BigDecimal benefitAmount;

    /**
     * 优惠折扣
     */
    private Long benefitDiscount;

    /**
     * 优惠级别
     */
    private Long benefitLevel;


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getActivityId() {
        return activityId;
    }

    public void setActivityId(Integer activityId) {
        this.activityId = activityId;
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

    public Long getBenefitLevel() {
        return benefitLevel;
    }

    public void setBenefitLevel(Long benefitLevel) {
        this.benefitLevel = benefitLevel;
    }

    @Override
    public String toString() {
        return "ActivityRule{" +
        "id=" + id +
        ", activityId=" + activityId +
        ", conditionAmount=" + conditionAmount +
        ", conditionNum=" + conditionNum +
        ", benefitAmount=" + benefitAmount +
        ", benefitDiscount=" + benefitDiscount +
        ", benefitLevel=" + benefitLevel +
        "}";
    }
}
