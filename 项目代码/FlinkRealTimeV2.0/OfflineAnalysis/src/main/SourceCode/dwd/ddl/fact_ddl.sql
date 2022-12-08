CREATE DATABASE IF NOT EXISTS dwd location 'hdfs://Flink01:8020/spark/gmall/dwd';
USE dwd;
    
--交易域加购事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `id`               STRING COMMENT '编号',
    `user_id`          STRING COMMENT '用户id',
    `sku_id`           STRING COMMENT '商品id',
    `date_id`          STRING COMMENT '时间id',
    `create_time`      STRING COMMENT '加购时间',
    `source_id`        STRING COMMENT '来源类型ID',
    `source_type_code` STRING COMMENT '来源类型编码',
    `source_type_name` STRING COMMENT '来源类型名称',
    `sku_num`          BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购物车事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    source_id,
    source_type,
    dic.dic_name,
    sku_num,
    date_format(create_time, 'yyyy-MM-dd')
from
    (
        select
            data.id,
            data.user_id,
            data.sku_id,
            data.create_time,
            data.source_id,
            data.source_type,
            data.sku_num
        from ods.ods_cart_info_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-04'
          and parent_code='24'
    )dic
    on ci.source_type=dic.dic_code;
    --每日装载
insert overwrite table dwd_trade_cart_add_inc partition(dt='2022-12-05')
select
    id,
    user_id,
    sku_id,
    date_id,
    create_time,
    source_id,
    source_type_code,
    source_type_name,
    sku_num
from
    (
        select
            data.id,
            data.user_id,
            data.sku_id,
            date_format(from_utc_timestamp(ts*1000,'GMT+8'),'yyyy-MM-dd') date_id,
            date_format(from_utc_timestamp(ts*1000,'GMT+8'),'yyyy-MM-dd HH:mm:ss') create_time,
            data.source_id,
            data.source_type source_type_code,
            if(type='insert',data.sku_num,data.sku_num-old['sku_num']) sku_num
        from ods.ods_cart_info_inc
        where dt='2022-12-05'
          and (type='insert'
            or(type='update' and old['sku_num'] is not null and data.sku_num>cast(old['sku_num'] as int)))
    )cart
        left join
    (
        select
            dic_code,
            dic_name source_type_name
        from ods.ods_base_dic_full
        where dt='2022-12-05'
          and parent_code='24'
    )dic
    on cart.source_type_code=dic.dic_code;

--交易域下单事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `date_id`               STRING COMMENT '下单日期id',
    `create_time`           STRING COMMENT '下单时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单明细事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    source_id,
    source_type,
    dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    date_format(create_time,'yyyy-MM-dd')
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.create_time,
            data.source_id,
            data.source_type,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods.ods_order_detail_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) od
        left join
    (
        select
            data.id,
            data.user_id,
            data.province_id
        from ods.ods_order_info_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods.ods_order_detail_activity_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods.ods_order_detail_coupon_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-04'
          and parent_code='24'
    )dic
    on od.source_type=dic.dic_code;
    --每日装载
insert overwrite table dwd_trade_order_detail_inc partition (dt='2022-12-05')
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_id,
    create_time,
    source_id,
    source_type,
    dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            date_format(data.create_time, 'yyyy-MM-dd') date_id,
            data.create_time,
            data.source_id,
            data.source_type,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods.ods_order_detail_inc
        where dt = '2022-12-05'
          and type = 'insert'
    ) od
        left join
    (
        select
            data.id,
            data.user_id,
            data.province_id
        from ods.ods_order_info_inc
        where dt = '2022-12-05'
          and type = 'insert'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods.ods_order_detail_activity_inc
        where dt = '2022-12-05'
          and type = 'insert'
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods.ods_order_detail_coupon_inc
        where dt = '2022-12-05'
          and type = 'insert'
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-05'
          and parent_code='24'
    )dic
    on od.source_type=dic.dic_code;

--交易域取消订单事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_trade_cancel_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_cancel_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `date_id`               STRING COMMENT '取消订单日期id',
    `cancel_time`           STRING COMMENT '取消订单时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域取消订单明细事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_trade_cancel_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cancel_detail_inc partition (dt)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(canel_time,'yyyy-MM-dd') date_id,
    canel_time,
    source_id,
    source_type,
    dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    date_format(canel_time,'yyyy-MM-dd')
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.source_id,
            data.source_type,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods.ods_order_detail_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) od
        join
    (
        select
            data.id,
            data.user_id,
            data.province_id,
            data.operate_time canel_time
        from ods.ods_order_info_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
          and data.order_status='1003'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods.ods_order_detail_activity_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods.ods_order_detail_coupon_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-04'
          and parent_code='24'
    )dic
    on od.source_type=dic.dic_code;
    --每日装载
insert overwrite table dwd_trade_cancel_detail_inc partition (dt='2022-12-05')
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(canel_time,'yyyy-MM-dd') date_id,
    canel_time,
    source_id,
    source_type,
    dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.source_id,
            data.source_type,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods.ods_order_detail_inc
        where (dt='2022-12-05' or dt=date_add('2022-12-05',-1))
          and (type = 'insert' or type= 'bootstrap-insert')
    ) od
        join
    (
        select
            data.id,
            data.user_id,
            data.province_id,
            data.operate_time canel_time
        from ods.ods_order_info_inc
        where dt = '2022-12-05'
          and type = 'update'
          and data.order_status='1003'
          and array_contains(map_keys(old),'order_status')
    ) oi
    on order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods.ods_order_detail_activity_inc
        where (dt='2022-12-05' or dt=date_add('2022-12-05',-1))
          and (type = 'insert' or type= 'bootstrap-insert')
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods.ods_order_detail_coupon_inc
        where (dt='2022-12-05' or dt=date_add('2022-12-05',-1))
          and (type = 'insert' or type= 'bootstrap-insert')
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-05'
          and parent_code='24'
    )dic
    on od.source_type=dic.dic_code;

--交易域支付成功事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`               STRING COMMENT '支付日期id',
    `callback_time`         STRING COMMENT '支付成功时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域成功支付事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    source_id,
    source_type,
    src_dic.dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    date_format(callback_time,'yyyy-MM-dd')
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.source_id,
            data.source_type,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods.ods_order_detail_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) od
        join
    (
        select
            data.user_id,
            data.order_id,
            data.payment_type,
            data.callback_time
        from ods.ods_payment_info_inc
        where dt='2022-12-04'
          and type='bootstrap-insert'
          and data.payment_status='1602'
    ) pi
    on od.order_id=pi.order_id
        left join
    (
        select
            data.id,
            data.province_id
        from ods.ods_order_info_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods.ods_order_detail_activity_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods.ods_order_detail_coupon_inc
        where dt = '2022-12-04'
          and type = 'bootstrap-insert'
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-04'
          and parent_code='11'
    ) pay_dic
    on pi.payment_type=pay_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-04'
          and parent_code='24'
    )src_dic
    on od.source_type=src_dic.dic_code;
    --每日装载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt='2022-12-05')
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    source_id,
    source_type,
    src_dic.dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.source_id,
            data.source_type,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods.ods_order_detail_inc
        where (dt = '2022-12-05' or dt = date_add('2022-12-05',-1))
          and (type = 'insert' or type = 'bootstrap-insert')
    ) od
        join
    (
        select
            data.user_id,
            data.order_id,
            data.payment_type,
            data.callback_time
        from ods.ods_payment_info_inc
        where dt='2022-12-05'
          and type='update'
          and array_contains(map_keys(old),'payment_status')
          and data.payment_status='1602'
    ) pi
    on od.order_id=pi.order_id
        left join
    (
        select
            data.id,
            data.province_id
        from ods.ods_order_info_inc
        where (dt = '2022-12-05' or dt = date_add('2022-12-05',-1))
          and (type = 'insert' or type = 'bootstrap-insert')
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods.ods_order_detail_activity_inc
        where (dt = '2022-12-05' or dt = date_add('2022-12-05',-1))
          and (type = 'insert' or type = 'bootstrap-insert')
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods.ods_order_detail_coupon_inc
        where (dt = '2022-12-05' or dt = date_add('2022-12-05',-1))
          and (type = 'insert' or type = 'bootstrap-insert')
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-05'
          and parent_code='11'
    ) pay_dic
    on pi.payment_type=pay_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-05'
          and parent_code='24'
    )src_dic
    on od.source_type=src_dic.dic_code;

--交易域退单事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_trade_order_refund_inc;
CREATE EXTERNAL TABLE dwd_trade_order_refund_inc
(
    `id`                      STRING COMMENT '编号',
    `user_id`                 STRING COMMENT '用户ID',
    `order_id`                STRING COMMENT '订单ID',
    `sku_id`                  STRING COMMENT '商品ID',
    `province_id`             STRING COMMENT '地区ID',
    `date_id`                 STRING COMMENT '日期ID',
    `create_time`             STRING COMMENT '退单时间',
    `refund_type_code`        STRING COMMENT '退单类型编码',
    `refund_type_name`        STRING COMMENT '退单类型名称',
    `refund_reason_type_code` STRING COMMENT '退单原因类型编码',
    `refund_reason_type_name` STRING COMMENT '退单原因类型名称',
    `refund_reason_txt`       STRING COMMENT '退单原因描述',
    `refund_num`              BIGINT COMMENT '退单件数',
    `refund_amount`           DECIMAL(16, 2) COMMENT '退单金额'
) COMMENT '交易域退单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_trade_order_refund_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
--数据装载
    --首日装载
insert overwrite table dwd_trade_order_refund_inc partition(dt)
select
    ri.id,
    user_id,
    order_id,
    sku_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    refund_type,
    type_dic.dic_name,
    refund_reason_type,
    reason_dic.dic_name,
    refund_reason_txt,
    refund_num,
    refund_amount,
    date_format(create_time,'yyyy-MM-dd')
from
    (
        select
            data.id,
            data.user_id,
            data.order_id,
            data.sku_id,
            data.refund_type,
            data.refund_num,
            data.refund_amount,
            data.refund_reason_type,
            data.refund_reason_txt,
            data.create_time
        from ods.ods_order_refund_info_inc
        where dt='2022-12-04'
          and type='bootstrap-insert'
    )ri
        left join
    (
        select
            data.id,
            data.province_id
        from ods.ods_order_info_inc
        where dt='2022-12-04'
          and type='bootstrap-insert'
    )oi
    on ri.order_id=oi.id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-04'
          and parent_code = '15'
    )type_dic
    on ri.refund_type=type_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-04'
          and parent_code = '13'
    )reason_dic
    on ri.refund_reason_type=reason_dic.dic_code;
    --每日装载
insert overwrite table dwd_trade_order_refund_inc partition(dt='2022-12-05')
select
    ri.id,
    user_id,
    order_id,
    sku_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    refund_type,
    type_dic.dic_name,
    refund_reason_type,
    reason_dic.dic_name,
    refund_reason_txt,
    refund_num,
    refund_amount
from
    (
        select
            data.id,
            data.user_id,
            data.order_id,
            data.sku_id,
            data.refund_type,
            data.refund_num,
            data.refund_amount,
            data.refund_reason_type,
            data.refund_reason_txt,
            data.create_time
        from ods.ods_order_refund_info_inc
        where dt='2022-12-05'
          and type='insert'
    )ri
        left join
    (
        select
            data.id,
            data.province_id
        from ods.ods_order_info_inc
        where dt='2022-12-05'
          and type='update'
          and data.order_status='1005'
          and array_contains(map_keys(old),'order_status')
    )oi
    on ri.order_id=oi.id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-05'
          and parent_code = '15'
    )type_dic
    on ri.refund_type=type_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-05'
          and parent_code = '13'
    )reason_dic
    on ri.refund_reason_type=reason_dic.dic_code;

--交易域退款成功事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_trade_refund_pay_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_refund_pay_suc_inc
(
    `id`                STRING COMMENT '编号',
    `user_id`           STRING COMMENT '用户ID',
    `order_id`          STRING COMMENT '订单编号',
    `sku_id`            STRING COMMENT 'SKU编号',
    `province_id`       STRING COMMENT '地区ID',
    `payment_type_code` STRING COMMENT '支付类型编码',
    `payment_type_name` STRING COMMENT '支付类型名称',
    `date_id`           STRING COMMENT '日期ID',
    `callback_time`     STRING COMMENT '支付成功时间',
    `refund_num`        DECIMAL(16, 2) COMMENT '退款件数',
    `refund_amount`     DECIMAL(16, 2) COMMENT '退款金额'
) COMMENT '交易域提交退款成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_trade_refund_pay_suc_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
--数据装载
--首日装载
insert overwrite table dwd_trade_refund_pay_suc_inc partition(dt)
select
    rp.id,
    user_id,
    rp.order_id,
    rp.sku_id,
    province_id,
    payment_type,
    dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    refund_num,
    total_amount,
    date_format(callback_time,'yyyy-MM-dd')
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.payment_type,
            data.callback_time,
            data.total_amount
        from ods.ods_refund_payment_inc
        where dt='2022-12-04'
          and type = 'bootstrap-insert'
          and data.refund_status='1602'
    )rp
        left join
    (
        select
            data.id,
            data.user_id,
            data.province_id
        from ods.ods_order_info_inc
        where dt='2022-12-04'
          and type='bootstrap-insert'
    )oi
    on rp.order_id=oi.id
        left join
    (
        select
            data.order_id,
            data.sku_id,
            data.refund_num
        from ods.ods_order_refund_info_inc
        where dt='2022-12-04'
          and type='bootstrap-insert'
    )ri
    on rp.order_id=ri.order_id
        and rp.sku_id=ri.sku_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-04'
          and parent_code='11'
    )dic
    on rp.payment_type=dic.dic_code;
    --每日装载
insert overwrite table dwd_trade_refund_pay_suc_inc partition(dt='2022-12-05')
select
    rp.id,
    user_id,
    rp.order_id,
    rp.sku_id,
    province_id,
    payment_type,
    dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    refund_num,
    total_amount
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.payment_type,
            data.callback_time,
            data.total_amount
        from ods.ods_refund_payment_inc
        where dt='2022-12-05'
          and type = 'update'
          and array_contains(map_keys(old),'refund_status')
          and data.refund_status='1602'
    )rp
        left join
    (
        select
            data.id,
            data.user_id,
            data.province_id
        from ods.ods_order_info_inc
        where dt='2022-12-05'
          and type='update'
          and data.order_status='1006'
          and array_contains(map_keys(old),'order_status')
    )oi
    on rp.order_id=oi.id
        left join
    (
        select
            data.order_id,
            data.sku_id,
            data.refund_num
        from ods.ods_order_refund_info_inc
        where dt='2022-12-05'
          and type='update'
          and data.refund_status='0705'
          and array_contains(map_keys(old),'refund_status')
    )ri
    on rp.order_id=ri.order_id
        and rp.sku_id=ri.sku_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-05'
          and parent_code='11'
    )dic
    on rp.payment_type=dic.dic_code;

--交易域购物车周期快照事实表
    --建表语句
DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`       STRING COMMENT '编号',
    `user_id`  STRING COMMENT '用户id',
    `sku_id`   STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num`  BIGINT COMMENT '加购物车件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dwd_trade_cart_full partition(dt='2022-12-04')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods.ods_cart_info_full
where dt='2022-12-04'
  and is_ordered='0';

--工具域优惠券领取事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_tool_coupon_get_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_get_inc
(
    `id`        STRING COMMENT '编号',
    `coupon_id` STRING COMMENT '优惠券ID',
    `user_id`   STRING COMMENT 'userid',
    `date_id`   STRING COMMENT '日期ID',
    `get_time`  STRING COMMENT '领取时间'
) COMMENT '优惠券领取事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_tool_coupon_get_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
--数据装载
    --首日装载
insert overwrite table dwd_tool_coupon_get_inc partition(dt)
select
    data.id,
    data.coupon_id,
    data.user_id,
    date_format(data.get_time,'yyyy-MM-dd') date_id,
    data.get_time,
    date_format(data.get_time,'yyyy-MM-dd')
from ods.ods_coupon_use_inc
where dt='2022-12-04'
  and type='bootstrap-insert';
    --每日装载
insert overwrite table dwd_tool_coupon_get_inc partition (dt='2022-12-05')
select
    data.id,
    data.coupon_id,
    data.user_id,
    date_format(data.get_time,'yyyy-MM-dd') date_id,
    data.get_time
from ods.ods_coupon_use_inc
where dt='2022-12-05'
  and type='insert';

--工具域优惠券使用(下单)事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_tool_coupon_order_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_order_inc
(
    `id`         STRING COMMENT '编号',
    `coupon_id`  STRING COMMENT '优惠券ID',
    `user_id`    STRING COMMENT 'user_id',
    `order_id`   STRING COMMENT 'order_id',
    `date_id`    STRING COMMENT '日期ID',
    `order_time` STRING COMMENT '使用下单时间'
) COMMENT '优惠券使用下单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_tool_coupon_order_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
--数据装载
    --首日装载
insert overwrite table dwd_tool_coupon_order_inc partition(dt)
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.using_time,'yyyy-MM-dd') date_id,
    data.using_time,
    date_format(data.using_time,'yyyy-MM-dd')
from ods.ods_coupon_use_inc
where dt='2022-12-04'
  and type='bootstrap-insert'
  and data.using_time is not null;
    --每日装载
insert overwrite table dwd_tool_coupon_order_inc partition(dt='2022-12-05')
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.using_time,'yyyy-MM-dd') date_id,
    data.using_time
from ods.ods_coupon_use_inc
where dt='2022-12-05'
  and type='update'
  and array_contains(map_keys(old),'using_time');

--工具域优惠券使用(支付)事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_tool_coupon_pay_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_pay_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT 'user_id',
    `order_id`     STRING COMMENT 'order_id',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用下单时间'
) COMMENT '优惠券使用支付事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_tool_coupon_pay_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
--数据装载
    --首日装载
insert overwrite table dwd_tool_coupon_pay_inc partition(dt)
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.used_time,'yyyy-MM-dd') date_id,
    data.used_time,
    date_format(data.used_time,'yyyy-MM-dd')
from ods.ods_coupon_use_inc
where dt='2022-12-04'
  and type='bootstrap-insert'
  and data.used_time is not null;
    --每日装载
insert overwrite table dwd_tool_coupon_pay_inc partition(dt='2022-12-05')
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.used_time,'yyyy-MM-dd') date_id,
    data.used_time
from ods.ods_coupon_use_inc
where dt='2022-12-05'
  and type='update'
  and array_contains(map_keys(old),'used_time');

--互动域收藏商品事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户id',
    `sku_id`      STRING COMMENT 'sku_id',
    `date_id`     STRING COMMENT '日期id',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '收藏事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_interaction_favor_add_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
--数据装载
    --首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_favor_add_inc partition(dt)
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time,
    date_format(data.create_time,'yyyy-MM-dd')
from ods.ods_favor_info_inc
where dt='2022-12-04'
  and type = 'bootstrap-insert';
    --每日装载
insert overwrite table dwd_interaction_favor_add_inc partition(dt='2022-12-05')
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time
from ods.ods_favor_info_inc
where dt='2022-12-05'
  and type = 'insert';

--互动域评价事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_interaction_comment_inc;
CREATE EXTERNAL TABLE dwd_interaction_comment_inc
(
    `id`            STRING COMMENT '编号',
    `user_id`       STRING COMMENT '用户ID',
    `sku_id`        STRING COMMENT 'sku_id',
    `order_id`      STRING COMMENT '订单ID',
    `date_id`       STRING COMMENT '日期ID',
    `create_time`   STRING COMMENT '评价时间',
    `appraise_code` STRING COMMENT '评价编码',
    `appraise_name` STRING COMMENT '评价名称'
) COMMENT '评价事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_interaction_comment_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
--数据装载
    --首日装载
insert overwrite table dwd_interaction_comment_inc partition(dt)
select
    id,
    user_id,
    sku_id,
    order_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    appraise,
    dic_name,
    date_format(create_time,'yyyy-MM-dd')
from
    (
        select
            data.id,
            data.user_id,
            data.sku_id,
            data.order_id,
            data.create_time,
            data.appraise
        from ods.ods_comment_info_inc
        where dt='2022-12-04'
          and type='bootstrap-insert'
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-04'
          and parent_code='12'
    )dic
    on ci.appraise=dic.dic_code;
    --每日装载
insert overwrite table dwd_interaction_comment_inc partition(dt='2022-12-05')
select
    id,
    user_id,
    sku_id,
    order_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    appraise,
    dic_name
from
    (
        select
            data.id,
            data.user_id,
            data.sku_id,
            data.order_id,
            data.create_time,
            data.appraise
        from ods.ods_comment_info_inc
        where dt='2022-12-05'
          and type='insert'
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where dt='2022-12-05'
          and parent_code='12'
    )dic
    on ci.appraise=dic.dic_code;

--流量域页面浏览事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份id',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备id',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员id',
    `version_code`   STRING COMMENT 'app版本号',
    `page_item`      STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页类型',
    `page_id`        STRING COMMENT '页面ID ',
    `source_type`    STRING COMMENT '来源类型',
    `date_id`        STRING COMMENT '日期id',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话id',
    `during_time`    BIGINT COMMENT '持续时间毫秒'
) COMMENT '页面日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_page_view_inc partition (dt='2022-12-04')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
    concat(mid_id,'-',last_value(session_start_point,true) over (partition by mid_id order by ts)) session_id,
    during_time
from
    (
        select
            common.ar area_code,
            common.ba brand,
            common.ch channel,
            common.is_new is_new,
            common.md model,
            common.mid mid_id,
            common.os operate_system,
            common.uid user_id,
            common.vc version_code,
            page.during_time,
            page.item page_item,
            page.item_type page_item_type,
            page.last_page_id,
            page.page_id,
            page.source_type,
            ts,
            if(page.last_page_id is null,ts,null) session_start_point
        from ods.ods_log_inc
        where dt='2022-12-04'
          and page is not null
    )log
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
        where dt='2022-12-04'
    )bp
    on log.area_code=bp.area_code;

--流量域启动事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_traffic_start_inc;
CREATE EXTERNAL TABLE dwd_traffic_start_inc
(
    `province_id`     STRING COMMENT '省份id',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `operate_system`  STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `entry`           STRING COMMENT 'icon手机图标 notice 通知',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `date_id`         STRING COMMENT '日期id',
    `start_time`      STRING COMMENT '启动时间',
    `loading_time_ms` BIGINT COMMENT '启动加载时间',
    `open_ad_ms`      BIGINT COMMENT '广告总共播放时间',
    `open_ad_skip_ms` BIGINT COMMENT '用户跳过广告时点'
) COMMENT '启动日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_traffic_start_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_start_inc partition(dt='2022-12-04')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    entry,
    open_ad_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') action_time,
    loading_time,
    open_ad_ms,
    open_ad_skip_ms
from
    (
        select
            common.ar area_code,
            common.ba brand,
            common.ch channel,
            common.is_new,
            common.md model,
            common.mid mid_id,
            common.os operate_system,
            common.uid user_id,
            common.vc version_code,
            `start`.entry,
            `start`.loading_time,
            `start`.open_ad_id,
            `start`.open_ad_ms,
            `start`.open_ad_skip_ms,
            ts
        from ods.ods_log_inc
        where dt='2022-12-04'
          and `start` is not null
    )log
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
        where dt='2022-12-04'
    )bp
    on log.area_code=bp.area_code;

--流量域动作事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_traffic_action_inc;
CREATE EXTERNAL TABLE dwd_traffic_action_inc
(
    `province_id`      STRING COMMENT '省份id',
    `brand`            STRING COMMENT '手机品牌',
    `channel`          STRING COMMENT '渠道',
    `is_new`           STRING COMMENT '是否首次启动',
    `model`            STRING COMMENT '手机型号',
    `mid_id`           STRING COMMENT '设备id',
    `operate_system`   STRING COMMENT '操作系统',
    `user_id`          STRING COMMENT '会员id',
    `version_code`     STRING COMMENT 'app版本号',
    `during_time`      BIGINT COMMENT '持续时间毫秒',
    `page_item`        STRING COMMENT '目标id ',
    `page_item_type`   STRING COMMENT '目标类型',
    `last_page_id`     STRING COMMENT '上页类型',
    `page_id`          STRING COMMENT '页面id ',
    `source_type`      STRING COMMENT '来源类型',
    `action_id`        STRING COMMENT '动作id',
    `action_item`      STRING COMMENT '目标id ',
    `action_item_type` STRING COMMENT '目标类型',
    `date_id`          STRING COMMENT '日期id',
    `action_time`      STRING COMMENT '动作发生时间'
) COMMENT '动作日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_traffic_action_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_action_inc partition(dt='2022-12-04')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    during_time,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    action_id,
    action_item,
    action_item_type,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') action_time
from
    (
        select
            common.ar area_code,
            common.ba brand,
            common.ch channel,
            common.is_new,
            common.md model,
            common.mid mid_id,
            common.os operate_system,
            common.uid user_id,
            common.vc version_code,
            page.during_time,
            page.item page_item,
            page.item_type page_item_type,
            page.last_page_id,
            page.page_id,
            page.source_type,
            action.action_id,
            action.item action_item,
            action.item_type action_item_type,
            action.ts
        from ods.ods_log_inc lateral view explode(actions) tmp as action
        where dt='2022-12-04'
          and actions is not null
    )log
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
        where dt='2022-12-04'
    )bp
    on log.area_code=bp.area_code;

--流量域曝光事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_traffic_display_inc;
CREATE EXTERNAL TABLE dwd_traffic_display_inc
(
    `province_id`       STRING COMMENT '省份id',
    `brand`             STRING COMMENT '手机品牌',
    `channel`           STRING COMMENT '渠道',
    `is_new`            STRING COMMENT '是否首次启动',
    `model`             STRING COMMENT '手机型号',
    `mid_id`            STRING COMMENT '设备id',
    `operate_system`    STRING COMMENT '操作系统',
    `user_id`           STRING COMMENT '会员id',
    `version_code`      STRING COMMENT 'app版本号',
    `during_time`       BIGINT COMMENT 'app版本号',
    `page_item`         STRING COMMENT '目标id ',
    `page_item_type`    STRING COMMENT '目标类型',
    `last_page_id`      STRING COMMENT '上页类型',
    `page_id`           STRING COMMENT '页面ID ',
    `source_type`       STRING COMMENT '来源类型',
    `date_id`           STRING COMMENT '日期id',
    `display_time`      STRING COMMENT '曝光时间',
    `display_type`      STRING COMMENT '曝光类型',
    `display_item`      STRING COMMENT '曝光对象id ',
    `display_item_type` STRING COMMENT 'app版本号',
    `display_order`     BIGINT COMMENT '曝光顺序',
    `display_pos_id`    BIGINT COMMENT '曝光位置'
) COMMENT '曝光日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_traffic_display_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_display_inc partition(dt='2022-12-04')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    during_time,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') display_time,
    display_type,
    display_item,
    display_item_type,
    display_order,
    display_pos_id
from
    (
        select
            common.ar area_code,
            common.ba brand,
            common.ch channel,
            common.is_new,
            common.md model,
            common.mid mid_id,
            common.os operate_system,
            common.uid user_id,
            common.vc version_code,
            page.during_time,
            page.item page_item,
            page.item_type page_item_type,
            page.last_page_id,
            page.page_id,
            page.source_type,
            display.display_type,
            display.item display_item,
            display.item_type display_item_type,
            display.`order` display_order,
            display.pos_id display_pos_id,
            ts
        from ods.ods_log_inc lateral view explode(displays) tmp as display
        where dt='2022-12-04'
          and displays is not null
    )log
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
        where dt='2022-12-04'
    )bp
    on log.area_code=bp.area_code;

--流量域错误事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_traffic_error_inc;
CREATE EXTERNAL TABLE dwd_traffic_error_inc
(
    `province_id`     STRING COMMENT '地区编码',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `operate_system`  STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `page_item`       STRING COMMENT '目标id ',
    `page_item_type`  STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页类型',
    `page_id`         STRING COMMENT '页面ID ',
    `source_type`     STRING COMMENT '来源类型',
    `entry`           STRING COMMENT 'icon手机图标  notice 通知',
    `loading_time`    STRING COMMENT '启动加载时间',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `open_ad_ms`      STRING COMMENT '广告总共播放时间',
    `open_ad_skip_ms` STRING COMMENT '用户跳过广告时点',
    `actions`         ARRAY<STRUCT<action_id:STRING,item:STRING,item_type:STRING,ts:BIGINT>> COMMENT '动作信息',
    `displays`        ARRAY<STRUCT<display_type :STRING,item :STRING,item_type :STRING,`order` :STRING,pos_id
                                   :STRING>> COMMENT '曝光信息',
    `date_id`         STRING COMMENT '日期id',
    `error_time`      STRING COMMENT '错误时间',
    `error_code`      STRING COMMENT '错误码',
    `error_msg`       STRING COMMENT '错误信息'
) COMMENT '错误日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_traffic_error_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
set hive.cbo.enable=false;
set hive.execution.engine=mr;
insert overwrite table dwd_traffic_error_inc partition(dt='2022-12-04')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    entry,
    loading_time,
    open_ad_id,
    open_ad_ms,
    open_ad_skip_ms,
    actions,
    displays,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') error_time,
    error_code,
    error_msg
from
    (
        select
            common.ar area_code,
            common.ba brand,
            common.ch channel,
            common.is_new,
            common.md model,
            common.mid mid_id,
            common.os operate_system,
            common.uid user_id,
            common.vc version_code,
            page.during_time,
            page.item page_item,
            page.item_type page_item_type,
            page.last_page_id,
            page.page_id,
            page.source_type,
            `start`.entry,
            `start`.loading_time,
            `start`.open_ad_id,
            `start`.open_ad_ms,
            `start`.open_ad_skip_ms,
            actions,
            displays,
            err.error_code,
            err.msg error_msg,
            ts
        from ods.ods_log_inc
        where dt='2022-12-04'
          and err is not null
    )log
        join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
        where dt='2022-12-04'
    )bp
    on log.area_code=bp.area_code;

--用户域用户注册事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_user_register_inc;
CREATE EXTERNAL TABLE dwd_user_register_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `create_time`    STRING COMMENT '注册时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份id',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备id',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_user_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
--数据装载
    --首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_user_register_inc partition(dt)
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system,
    date_format(create_time,'yyyy-MM-dd')
from
    (
        select
            data.id user_id,
            data.create_time
        from ods.ods_user_info_inc
        where dt='2022-12-04'
          and type='bootstrap-insert'
    )ui
        left join
    (
        select
            common.ar area_code,
            common.ba brand,
            common.ch channel,
            common.md model,
            common.mid mid_id,
            common.os operate_system,
            common.uid user_id,
            common.vc version_code
        from ods.ods_log_inc
        where dt='2022-12-04'
          and page.page_id='register'
          and common.uid is not null
    )log
    on ui.user_id=log.user_id
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
        where dt='2022-12-04'
    )bp
    on log.area_code=bp.area_code;
    --每日装载
insert overwrite table dwd_user_register_inc partition(dt='2022-12-05')
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
    (
        select
            data.id user_id,
            data.create_time
        from ods.ods_user_info_inc
        where dt='2022-12-05'
          and type='insert'
    )ui
        left join
    (
        select
            common.ar area_code,
            common.ba brand,
            common.ch channel,
            common.md model,
            common.mid mid_id,
            common.os operate_system,
            common.uid user_id,
            common.vc version_code
        from ods.ods_log_inc
        where dt='2022-12-05'
          and page.page_id='register'
          and common.uid is not null
    )log
    on ui.user_id=log.user_id
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
        where dt='2022-12-05'
    )bp
    on log.area_code=bp.area_code;

--用户域用户登录事务事实表
    --建表语句
DROP TABLE IF EXISTS dwd_user_login_inc;
CREATE EXTERNAL TABLE dwd_user_login_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份id',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备id',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dwd/dwd_user_login_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
--数据装载
insert overwrite table dwd_user_login_inc partition(dt='2022-12-04')
select
    user_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') login_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
    (
        select
            user_id,
            channel,
            area_code,
            version_code,
            mid_id,
            brand,
            model,
            operate_system,
            ts
        from
            (
                select
                    user_id,
                    channel,
                    area_code,
                    version_code,
                    mid_id,
                    brand,
                    model,
                    operate_system,
                    ts,
                    row_number() over (partition by session_id order by ts) rn
                from
                    (
                        select
                            user_id,
                            channel,
                            area_code,
                            version_code,
                            mid_id,
                            brand,
                            model,
                            operate_system,
                            ts,
                            concat(mid_id,'-',last_value(session_start_point,true) over(partition by mid_id order by ts)) session_id
                        from
                            (
                                select
                                    common.uid user_id,
                                    common.ch channel,
                                    common.ar area_code,
                                    common.vc version_code,
                                    common.mid mid_id,
                                    common.ba brand,
                                    common.md model,
                                    common.os operate_system,
                                    ts,
                                    if(page.last_page_id is null,ts,null) session_start_point
                                from ods.ods_log_inc
                                where dt='2022-12-04'
                                  and page is not null
                            )t1
                    )t2
                where user_id is not null
            )t3
        where rn=1
    )t4
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
        where dt='2022-12-04'
    )bp
    on t4.area_code=bp.area_code;
