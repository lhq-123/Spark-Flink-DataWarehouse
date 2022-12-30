package com.alex.mock.db.bean;

import java.math.BigDecimal;
import com.baomidou.mybatisplus.annotation.IdType;
import java.util.Date;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.io.Serializable;

/**
 * <p>
 * 订单明细表
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
@Data
public class OrderDetail implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 编号
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 订单编号
     */
    private Long orderId;

    /**
     * sku_id
     */
    private Long skuId;

    /**
     * sku名称（冗余)
     */
    private String skuName;

    /**
     * 图片名称（冗余)
     */
    private String imgUrl;

    /**
     * 购买价格(下单时sku价格）
     */
    private BigDecimal orderPrice;

    /**
     * 购买个数
     */
    private Long skuNum;

    /**
     * 创建时间
     */
    private Date createTime;


    @TableField(exist = false)
    private SkuInfo skuInfo;

    private String sourceType;
    private Long sourceId;

}
