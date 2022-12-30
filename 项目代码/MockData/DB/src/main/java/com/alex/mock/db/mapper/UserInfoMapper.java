package com.alex.mock.db.mapper;

import com.  alex.mock.db.bean.UserInfo;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Update;

/**
 * <p>
 * 用户表 Mapper 接口
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
public interface UserInfoMapper extends BaseMapper<UserInfo> {

    @Update("truncate table user_info")
    public  void truncateUserInfo();

}
