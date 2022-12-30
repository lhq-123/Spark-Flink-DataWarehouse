package com.alex.mock.db.bean;

import java.io.Serializable;

/**
 * <p>
 * 
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
public class BaseProvince implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * id
     */
    private Long id;

    /**
     * 省名称
     */
    private String name;

    /**
     * 大区id
     */
    private String regionId;

    /**
     * 行政区位码
     */
    private String areaCode;

    /**
     * 国际编码
     */
    private String isoCode;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public String getAreaCode() {
        return areaCode;
    }

    public void setAreaCode(String areaCode) {
        this.areaCode = areaCode;
    }

    public String getIsoCode() {
        return isoCode;
    }

    public void setIsoCode(String isoCode) {
        this.isoCode = isoCode;
    }

    @Override
    public String toString() {
        return "BaseProvince{" +
        "id=" + id +
        ", name=" + name +
        ", regionId=" + regionId +
        ", areaCode=" + areaCode +
        ", isoCode=" + isoCode +
        "}";
    }
}
