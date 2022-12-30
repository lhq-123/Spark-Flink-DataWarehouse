package com.alex.mock.db.bean;

import java.math.BigDecimal;
import com.baomidou.mybatisplus.annotation.IdType;
import java.util.Date;
import com.baomidou.mybatisplus.annotation.TableId;
import java.io.Serializable;

/**
 * <p>
 * 库存单元表
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
public class SkuInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * skuid(itemID)
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * spuid
     */
    private Long spuId;

    /**
     * 价格
     */
    private BigDecimal price;

    /**
     * sku名称
     */
    private String skuName;

    /**
     * 商品规格描述
     */
    private String skuDesc;

    /**
     * 重量
     */
    private BigDecimal weight;

    /**
     * 品牌(冗余)
     */
    private Long tmId;

    /**
     * 三级分类id（冗余)
     */
    private Long category3Id;

    /**
     * 默认显示图片(冗余)
     */
    private String skuDefaultImg;

    /**
     * 创建时间
     */
    private Date createTime;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getSpuId() {
        return spuId;
    }

    public void setSpuId(Long spuId) {
        this.spuId = spuId;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public String getSkuName() {
        return skuName;
    }

    public void setSkuName(String skuName) {
        this.skuName = skuName;
    }

    public String getSkuDesc() {
        return skuDesc;
    }

    public void setSkuDesc(String skuDesc) {
        this.skuDesc = skuDesc;
    }

    public BigDecimal getWeight() {
        return weight;
    }

    public void setWeight(BigDecimal weight) {
        this.weight = weight;
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

    public String getSkuDefaultImg() {
        return skuDefaultImg;
    }

    public void setSkuDefaultImg(String skuDefaultImg) {
        this.skuDefaultImg = skuDefaultImg;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "SkuInfo{" +
        "id=" + id +
        ", spuId=" + spuId +
        ", price=" + price +
        ", skuName=" + skuName +
        ", skuDesc=" + skuDesc +
        ", weight=" + weight +
        ", tmId=" + tmId +
        ", category3Id=" + category3Id +
        ", skuDefaultImg=" + skuDefaultImg +
        ", createTime=" + createTime +
        "}";
    }
}
