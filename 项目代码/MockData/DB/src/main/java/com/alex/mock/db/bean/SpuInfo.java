package com.alex.mock.db.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import java.io.Serializable;

/**
 * <p>
 * 商品表
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
public class SpuInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 商品id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 商品名称
     */
    private String spuName;

    /**
     * 商品描述(后台简述）
     */
    private String description;

    /**
     * 三级分类id
     */
    private Long category3Id;

    /**
     * 品牌id
     */
    private Long tmId;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSpuName() {
        return spuName;
    }

    public void setSpuName(String spuName) {
        this.spuName = spuName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getCategory3Id() {
        return category3Id;
    }

    public void setCategory3Id(Long category3Id) {
        this.category3Id = category3Id;
    }

    public Long getTmId() {
        return tmId;
    }

    public void setTmId(Long tmId) {
        this.tmId = tmId;
    }

    @Override
    public String toString() {
        return "SpuInfo{" +
        "id=" + id +
        ", spuName=" + spuName +
        ", description=" + description +
        ", category3Id=" + category3Id +
        ", tmId=" + tmId +
        "}";
    }
}
