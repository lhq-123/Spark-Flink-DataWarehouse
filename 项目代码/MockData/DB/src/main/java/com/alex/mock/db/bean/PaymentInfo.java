package com.alex.mock.db.bean;

import java.math.BigDecimal;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * <p>
 * 支付流水表
 * </p>
 *
 * @author    alex
 * @since 2020-02-24
 */
@Data
public class PaymentInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 编号
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 对外业务编号
     */
    private String outTradeNo;

    /**
     * 订单编号
     */
    private Long orderId;

    /**
     * 用户编号
     */
    private Long userId;

    /**
     * 支付宝交易流水编号
     */
    private String alipayTradeNo;

    /**
     * 支付金额
     */
    private BigDecimal totalAmount;

    /**
     * 交易内容
     */
    private String subject;

    /**
     * 支付方式
     */
    private String paymentType;

    /**
     * 支付时间
     */
    private Date paymentTime;





    @Override
    public String toString() {
        return "PaymentInfo{" +
        "id=" + id +
        ", outTradeNo=" + outTradeNo +
        ", orderId=" + orderId +
        ", userId=" + userId +
        ", alipayTradeNo=" + alipayTradeNo +
        ", totalAmount=" + totalAmount +
        ", subject=" + subject +
        ", paymentType=" + paymentType +
        ", paymentTime=" + paymentTime +
        "}";
    }
}
