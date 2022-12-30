package com.alex.mock.db.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import java.util.Date;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;

/**
 * <p>
 * 活动与订单关联表
 * </p>
 *
 * @author  alex
 * @since 2020-02-25
 */
@Data
@AllArgsConstructor
public class ActivityOrder implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 编号
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 活动id 
     */
    private Long activityId;

    /**
     * 订单编号
     */
    private Long orderId;


    @TableField(exist = false)
    private OrderInfo orderInfo;
    /**
     * 发生日期
     */
    private Date createTime;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getActivityId() {
        return activityId;
    }

    public void setActivityId(Long activityId) {
        this.activityId = activityId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "ActivityOrder{" +
        "id=" + id +
        ", activityId=" + activityId +
        ", orderId=" + orderId +
        ", createTime=" + createTime +
        "}";
    }
}
