package com.alex.mock.db.service;

import com.  alex.mock.db.bean.FavorInfo;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 商品收藏表 服务类
 * </p>
 *
 * @author  alex
 * @since 2020-02-24
 */
public interface FavorInfoService extends IService<FavorInfo> {

    public void  genFavors(Boolean ifClear);

}
