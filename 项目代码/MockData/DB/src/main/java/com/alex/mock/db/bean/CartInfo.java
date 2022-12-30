package com.alex.mock.db.bean;

import java.math.BigDecimal;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * <p>
 * 购物车表 用户登录系统时更新冗余
 * </p>
 *
 * @author    alex
 * @since 2020-02-24
 */
@Data
public class CartInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 编号
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 用户id
     */
    private Long userId;

    /**
     * skuid
     */
    private Long skuId;

    /**
     * 放入购物车时价格
     */
    private BigDecimal cartPrice;

    /**
     * 数量
     */
    private Long skuNum;

    /**
     * 图片文件
     */
    private String imgUrl;

    /**
     * sku名称 (冗余)
     */
    private String skuName;

    private Date createTime;

    private Date operateTime;

    private Integer isOrdered;

    private Date orderTime;

    private String sourceType;
    private Long sourceId;




}
