package com.alex.mock.db.service;

import com.  alex.mock.db.bean.UserInfo;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 用户表 服务类
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
public interface UserInfoService extends IService<UserInfo> {

    public void  genUserInfos(Boolean ifClear);

}
