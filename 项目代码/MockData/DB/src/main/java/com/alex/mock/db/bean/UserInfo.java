package com.alex.mock.db.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import java.util.Date;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.io.Serializable;

/**
 * <p>
 * 用户表
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
@Data
public class UserInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 编号
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 用户名称
     */
    private String loginName;

    /**
     * 用户昵称
     */
    private String nickName;

    /**
     * 用户密码
     */
    private String passwd;

    /**
     * 用户姓名
     */
    private String name;

    /**
     * 手机号
     */
    private String phoneNum;

    /**
     * 邮箱
     */
    private String email;

    /**
     * 头像
     */
    private String headImg;

    /**
     * 用户级别
     */
    private String userLevel;

    /**
     * 用户生日
     */
    private Date birthday;

    /**
     * 性别 M男,F女
     */
    private String gender;

    /**
     * 创建时间
     */
    private Date createTime;
    /**
     * 修改时间
     */
    private Date operateTime;


}
