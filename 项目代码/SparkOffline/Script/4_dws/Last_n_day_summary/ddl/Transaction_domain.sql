--最近n日交易域汇总
CREATE DATABASE IF NOT EXISTS dws location 'hdfs://Flink01:8020/spark/gmall/dws';
USE dws;
--交易域用户商品粒度订单最近n日汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_user_sku_order_nd;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_nd
(
    `user_id`                    STRING COMMENT '用户id',
    `sku_id`                     STRING COMMENT 'sku_id',
    `sku_name`                   STRING COMMENT 'sku名称',
    `category1_id`               STRING COMMENT '一级分类id',
    `category1_name`             STRING COMMENT '一级分类名称',
    `category2_id`               STRING COMMENT '一级分类id',
    `category2_name`             STRING COMMENT '一级分类名称',
    `category3_id`               STRING COMMENT '一级分类id',
    `category3_name`             STRING COMMENT '一级分类名称',
    `tm_id`                      STRING COMMENT '品牌id',
    `tm_name`                    STRING COMMENT '品牌名称',
    `order_count_7d`             STRING COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_sku_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dws_trade_user_sku_order_nd partition(dt='2022-12-04')
select
    user_id,
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(if(dt>=date_add('2022-12-04',-6),order_count_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_num_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
where dt>=date_add('2022-12-04',-29)
group by  user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;

--交易域用户商品粒度退单最近n日汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_user_sku_order_refund_nd;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_refund_nd
(
    `user_id`                     STRING COMMENT '用户id',
    `sku_id`                      STRING COMMENT 'sku_id',
    `sku_name`                    STRING COMMENT 'sku名称',
    `category1_id`                STRING COMMENT '一级分类id',
    `category1_name`              STRING COMMENT '一级分类名称',
    `category2_id`                STRING COMMENT '一级分类id',
    `category2_name`              STRING COMMENT '一级分类名称',
    `category3_id`                STRING COMMENT '一级分类id',
    `category3_name`              STRING COMMENT '一级分类名称',
    `tm_id`                       STRING COMMENT '品牌id',
    `tm_name`                     STRING COMMENT '品牌名称',
    `order_refund_count_7d`       BIGINT COMMENT '最近7日退单次数',
    `order_refund_num_7d`         BIGINT COMMENT '最近7日退单件数',
    `order_refund_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日退单金额',
    `order_refund_count_30d`      BIGINT COMMENT '最近30日退单次数',
    `order_refund_num_30d`        BIGINT COMMENT '最近30日退单件数',
    `order_refund_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日退单金额'
) COMMENT '交易域用户商品粒度退单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_sku_order_refund_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dws_trade_user_sku_order_refund_nd partition(dt='2022-12-04')
select
    user_id,
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(if(dt>=date_add('2022-12-04',-6),order_refund_count_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_refund_num_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_refund_amount_1d,0)),
    sum(order_refund_count_1d),
    sum(order_refund_num_1d),
    sum(order_refund_amount_1d)
from dws_trade_user_sku_order_refund_1d
where dt>=date_add('2022-12-04',-29)
  and dt<='2022-12-04'
group by user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;

--交易域用户粒度订单最近n日汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_user_order_nd;
CREATE EXTERNAL TABLE dws_trade_user_order_nd
(
    `user_id`                    STRING COMMENT '用户id',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单商品件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单商品件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域用户粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dws_trade_user_order_nd partition(dt='2022-12-04')
select
    user_id,
    sum(if(dt>=date_add('2022-12-04',-6),order_count_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_num_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_user_order_1d
where dt>=date_add('2022-12-04',-29)
  and dt<='2022-12-04'
group by user_id;

--交易域用户粒度加购最近n日汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_user_cart_add_nd;
CREATE EXTERNAL TABLE dws_trade_user_cart_add_nd
(
    `user_id`            STRING COMMENT '用户id',
    `cart_add_count_7d`  BIGINT COMMENT '最近7日加购次数',
    `cart_add_num_7d`    BIGINT COMMENT '最近7日加购商品件数',
    `cart_add_count_30d` BIGINT COMMENT '最近30日加购次数',
    `cart_add_num_30d`   BIGINT COMMENT '最近30日加购商品件数'
) COMMENT '交易域用户粒度加购最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_cart_add_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dws_trade_user_cart_add_nd partition(dt='2022-12-04')
select
    user_id,
    sum(if(dt>=date_add('2022-12-04',-6),cart_add_count_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),cart_add_num_1d,0)),
    sum(cart_add_count_1d),
    sum(cart_add_num_1d)
from dws_trade_user_cart_add_1d
where dt>=date_add('2022-12-04',-29)
  and dt<='2022-12-04'
group by user_id;

--交易域用户粒度支付最近n日汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_user_payment_nd;
CREATE EXTERNAL TABLE dws_trade_user_payment_nd
(
    `user_id`            STRING COMMENT '用户id',
    `payment_count_7d`   BIGINT COMMENT '最近7日支付次数',
    `payment_num_7d`     BIGINT COMMENT '最近7日支付商品件数',
    `payment_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日支付金额',
    `payment_count_30d`  BIGINT COMMENT '最近30日支付次数',
    `payment_num_30d`    BIGINT COMMENT '最近30日支付商品件数',
    `payment_amount_30d` DECIMAL(16, 2) COMMENT '最近30日支付金额'
) COMMENT '交易域用户粒度支付最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_payment_nd'
TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dws_trade_user_payment_nd partition (dt = '2022-12-04')
select user_id,
       sum(if(dt >= date_add('2022-12-04', -6), payment_count_1d, 0)),
       sum(if(dt >= date_add('2022-12-04', -6), payment_num_1d, 0)),
       sum(if(dt >= date_add('2022-12-04', -6), payment_amount_1d, 0)),
       sum(payment_count_1d),
       sum(payment_num_1d),
       sum(payment_amount_1d)
from dws_trade_user_payment_1d
where dt >= date_add('2022-12-04', -29)
  and dt <= '2022-12-04'
group by user_id;

--交易域省份粒度订单最近n日汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_province_order_nd;
CREATE EXTERNAL TABLE dws_trade_province_order_nd
(
    `province_id`                STRING COMMENT '用户id',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版ISO-3166-2编码',
    `iso_3166_2`                 STRING COMMENT '新版版ISO-3166-2编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_province_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dws_trade_province_order_nd partition(dt='2022-12-04')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(dt>=date_add('2022-12-04',-6),order_count_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_province_order_1d
where dt>=date_add('2022-12-04',-29)
  and dt<='2022-12-04'
group by province_id,province_name,area_code,iso_code,iso_3166_2;

--交易域优惠券粒度订单最近n日汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_coupon_order_nd;
CREATE EXTERNAL TABLE dws_trade_coupon_order_nd
(
    `coupon_id`                STRING COMMENT '优惠券id',
    `coupon_name`              STRING COMMENT '优惠券名称',
    `coupon_type_code`         STRING COMMENT '优惠券类型id',
    `coupon_type_name`         STRING COMMENT '优惠券类型名称',
    `coupon_rule`              STRING COMMENT '优惠券规则',
    `start_date`               STRING COMMENT '发布日期',
    `original_amount_30d`      DECIMAL(16, 2) COMMENT '使用下单原始金额',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT '使用下单优惠金额'
) COMMENT '交易域优惠券粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_coupon_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dws_trade_coupon_order_nd partition(dt='2022-12-04')
select
    id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    start_date,
    sum(split_original_amount),
    sum(split_coupon_amount)
from
    (
        select
            id,
            coupon_name,
            coupon_type_code,
            coupon_type_name,
            benefit_rule,
            date_format(start_time,'yyyy-MM-dd') start_date
        from dim_coupon_full
        where dt='2022-12-04'
          and date_format(start_time,'yyyy-MM-dd')>=date_add('2022-12-04',-29)
    )cou
        left join
    (
        select
            coupon_id,
            order_id,
            split_original_amount,
            split_coupon_amount
        from dwd.dwd_trade_order_detail_inc
        where dt>=date_add('2022-12-04',-29)
          and dt<='2022-12-04'
          and coupon_id is not null
    )od
    on cou.id=od.coupon_id
group by id,coupon_name,coupon_type_code,coupon_type_name,benefit_rule,start_date;

--交易域活动粒度订单最近n日汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_activity_order_nd;
CREATE EXTERNAL TABLE dws_trade_activity_order_nd
(
    `activity_id`                STRING COMMENT '活动id',
    `activity_name`              STRING COMMENT '活动名称',
    `activity_type_code`         STRING COMMENT '活动类型编码',
    `activity_type_name`         STRING COMMENT '活动类型名称',
    `start_date`                 STRING COMMENT '发布日期',
    `original_amount_30d`        DECIMAL(16, 2) COMMENT '参与活动订单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '参与活动订单优惠金额'
) COMMENT '交易域活动粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_activity_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dws_trade_activity_order_nd partition(dt='2022-12-04')
select
    act.activity_id,
    activity_name,
    activity_type_code,
    activity_type_name,
    date_format(start_time,'yyyy-MM-dd'),
    sum(split_original_amount),
    sum(split_activity_amount)
from
    (
        select
            activity_id,
            activity_name,
            activity_type_code,
            activity_type_name,
            start_time
        from dim_activity_full
        where dt='2022-12-04'
          and date_format(start_time,'yyyy-MM-dd')>=date_add('2022-12-04',-29)
        group by activity_id, activity_name, activity_type_code, activity_type_name,start_time
    )act
        left join
    (
        select
            activity_id,
            order_id,
            split_original_amount,
            split_activity_amount
        from dwd.dwd_trade_order_detail_inc
        where dt>=date_add('2022-12-04',-29)
          and dt<='2022-12-04'
          and activity_id is not null
    )od
    on act.activity_id=od.activity_id
group by act.activity_id,activity_name,activity_type_code,activity_type_name,start_time;

--交易域用户粒度退单最近n日汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_user_order_refund_nd;
CREATE EXTERNAL TABLE dws_trade_user_order_refund_nd
(
    `user_id`                 STRING COMMENT '用户id',
    `order_refund_count_7d`   BIGINT COMMENT '最近7日退单次数',
    `order_refund_num_7d`     BIGINT COMMENT '最近7日退单商品件数',
    `order_refund_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日退单金额',
    `order_refund_count_30d`  BIGINT COMMENT '最近30日退单次数',
    `order_refund_num_30d`    BIGINT COMMENT '最近30日退单商品件数',
    `order_refund_amount_30d` DECIMAL(16, 2) COMMENT '最近30日退单金额'
) COMMENT '交易域用户粒度退单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_order_refund_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dws_trade_user_order_refund_nd partition(dt='2022-12-04')
select
    user_id,
    sum(if(dt>=date_add('2022-12-04',-6),order_refund_count_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_refund_num_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),order_refund_amount_1d,0)),
    sum(order_refund_count_1d),
    sum(order_refund_num_1d),
    sum(order_refund_amount_1d)
from dws_trade_user_order_refund_1d
where dt>=date_add('2022-12-04',-29)
  and dt<='2022-12-04'
group by user_id;