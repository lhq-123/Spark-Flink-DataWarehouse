package com.alex.mock.db.bean;

import java.math.BigDecimal;
import com.baomidou.mybatisplus.annotation.IdType;
import java.util.Date;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.io.Serializable;

/**
 * <p>
 * 退单表
 * </p>
 *
 * @author  alex
 * @since 2020-02-25
 */
@Data
public class OrderRefundInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 编号
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    private Long userId;

    /**
     * 订单编号
     */
    private Long orderId;

    /**
     * skuid
     */
    private Long skuId;

    /**
     * 退款类型
     */
    private String refundType;

    /**
     * 退款件数
     */
    private Long refundNum;


    /**
     * 退款金额
     */
    private BigDecimal refundAmount;

    /**
     * 原因类型
     */
    private String refundReasonType;

    /**
     * 原因内容
     */
    private String refundReasonTxt;

    /**
     * 创建时间
     */
    private Date createTime;



}
