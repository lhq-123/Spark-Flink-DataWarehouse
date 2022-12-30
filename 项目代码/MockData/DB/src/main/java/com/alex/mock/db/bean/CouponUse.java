package com.alex.mock.db.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import java.util.Date;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;

/**
 * <p>
 * 优惠券领用表
 * </p>
 *
 * @author  alex
 * @since 2020-02-26
 */
@Data
public class CouponUse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 编号
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 购物券ID
     */
    private Long couponId;

    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 订单ID
     */
    private Long orderId;

    /**
     * 购物券状态
     */
    private String couponStatus;

    /**
     * 领券时间
     */
    private Date getTime;

    /**
     * 使用时间
     */
    private Date usingTime;

    /**
     * 支付时间
     */
    private Date usedTime;

    /**
     * 过期时间
     */
    private Date expireTime;

    @TableField(exist = false)
    private CouponInfo couponInfo;

    @TableField(exist = false)
    private OrderInfo orderInfo;

}
