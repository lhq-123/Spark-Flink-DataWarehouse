package com.alex.mock.db.service.impl;

import com.  alex.mock.db.bean.OrderDetail;
import com.  alex.mock.db.mapper.OrderDetailMapper;
import com.  alex.mock.db.service.OrderDetailService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 * 订单明细表 服务实现类
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
@Service
public class OrderDetailServiceImpl extends ServiceImpl<OrderDetailMapper, OrderDetail> implements OrderDetailService {

}
