--最近1日交易域汇总
CREATE DATABASE IF NOT EXISTS dws location 'hdfs://Flink01:8020/spark/gmall/dws';
USE dws;
--交易域用户商品粒度订单最近1日汇总表
    --建表语句
DROP TABLE IF EXISTS dws_trade_user_sku_order_1d;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_1d
(
    `user_id`                   STRING COMMENT '用户id',
    `sku_id`                    STRING COMMENT 'sku_id',
    `sku_name`                  STRING COMMENT 'sku名称',
    `category1_id`              STRING COMMENT '一级分类id',
    `category1_name`            STRING COMMENT '一级分类名称',
    `category2_id`              STRING COMMENT '一级分类id',
    `category2_name`            STRING COMMENT '一级分类名称',
    `category3_id`              STRING COMMENT '一级分类id',
    `category3_name`            STRING COMMENT '一级分类名称',
    `tm_id`                     STRING COMMENT '品牌id',
    `tm_name`                   STRING COMMENT '品牌名称',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_sku_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_sku_order_1d partition(dt)
select
    user_id,
    id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    order_count_1d,
    order_num_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    dt
from
    (
        select
            dt,
            user_id,
            sku_id,
            count(*) order_count_1d,
            sum(sku_num) order_num_1d,
            sum(split_original_amount) order_original_amount_1d,
            sum(nvl(split_activity_amount,0.0)) activity_reduce_amount_1d,
            sum(nvl(split_coupon_amount,0.0)) coupon_reduce_amount_1d,
            sum(split_total_amount) order_total_amount_1d
        from dwd.dwd_trade_order_detail_inc
        group by dt,user_id,sku_id
    )od
        left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            tm_id,
            tm_name
        from dim.dim_sku_full
        where dt='2022-12-04'
    )sku
    on od.sku_id=sku.id;
    --每日装载
insert overwrite table dws_trade_user_sku_order_1d partition(dt='2022-12-05')
select
    user_id,
    id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    order_count,
    order_num,
    order_original_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    order_total_amount
from
    (
        select
            user_id,
            sku_id,
            count(*) order_count,
            sum(sku_num) order_num,
            sum(split_original_amount) order_original_amount,
            sum(nvl(split_activity_amount,0)) activity_reduce_amount,
            sum(nvl(split_coupon_amount,0)) coupon_reduce_amount,
            sum(split_total_amount) order_total_amount
        from dwd.dwd_trade_order_detail_inc
        where dt='2022-12-05'
        group by user_id,sku_id
    )od
        left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            tm_id,
            tm_name
        from dim.dim_sku_full
        where dt='2022-12-05'
    )sku
    on od.sku_id=sku.id;

--交易域用户商品粒度退单最近1日汇总表
    --建表语句
DROP TABLE IF EXISTS dws_trade_user_sku_order_refund_1d;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_refund_1d
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
    `order_refund_count_1d`      BIGINT COMMENT '最近1日退单次数',
    `order_refund_num_1d`        BIGINT COMMENT '最近1日退单件数',
    `order_refund_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日退单金额'
) COMMENT '交易域用户商品粒度退单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_sku_order_refund_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_sku_order_refund_1d partition(dt)
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
    order_refund_count,
    order_refund_num,
    order_refund_amount,
    dt
from
    (
        select
            dt,
            user_id,
            sku_id,
            count(*) order_refund_count,
            sum(refund_num) order_refund_num,
            sum(refund_amount) order_refund_amount
        from dwd.dwd_trade_order_refund_inc
        group by dt,user_id,sku_id
    )od
        left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            tm_id,
            tm_name
        from dim.dim_sku_full
        where dt='2022-12-04'
    )sku
    on od.sku_id=sku.id;
    --每日装载
insert overwrite table dws_trade_user_sku_order_refund_1d partition(dt='2022-12-05')
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
    order_refund_count,
    order_refund_num,
    order_refund_amount
from
    (
        select
            user_id,
            sku_id,
            count(*) order_refund_count,
            sum(refund_num) order_refund_num,
            sum(refund_amount) order_refund_amount
        from dwd.dwd_trade_order_refund_inc
        where dt='2022-12-05'
        group by user_id,sku_id
    )od
        left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            tm_id,
            tm_name
        from dim.dim_sku_full
        where dt='2022-12-05'
    )sku
    on od.sku_id=sku.id;

--交易域用户粒度订单最近1日汇总表
    --建表语句
DROP TABLE IF EXISTS dws_trade_user_order_1d;
CREATE EXTERNAL TABLE dws_trade_user_order_1d
(
    `user_id`                   STRING COMMENT '用户id',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户粒度订单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
insert overwrite table dws_trade_user_order_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount),
    dt
from dwd.dwd_trade_order_detail_inc
group by user_id,dt;
    --每日装载
insert overwrite table dws_trade_user_order_1d partition(dt='2022-12-05')
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount)
from dwd.dwd_trade_order_detail_inc
where dt='2022-12-05'
group by user_id;

--交易域用户粒度加购最近1日汇总表
    --建表语句
DROP TABLE IF EXISTS dws_trade_user_cart_add_1d;
CREATE EXTERNAL TABLE dws_trade_user_cart_add_1d
(
    `user_id`           STRING COMMENT '用户id',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
    `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数'
) COMMENT '交易域用户粒度加购最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_cart_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
insert overwrite table dws_trade_user_cart_add_1d partition(dt)
select
    user_id,
    count(*),
    sum(sku_num),
    dt
from dwd.dwd_trade_cart_add_inc
group by user_id,dt;
    --每日装载
insert overwrite table dws_trade_user_cart_add_1d partition(dt='2022-12-05')
select
    user_id,
    count(*),
    sum(sku_num)
from dwd.dwd_trade_cart_add_inc
where dt='2022-12-05'
group by user_id;

--交易域用户粒度支付最近1日汇总表
    --建表语句
DROP TABLE IF EXISTS dws_trade_user_payment_1d;
CREATE EXTERNAL TABLE dws_trade_user_payment_1d
(
    `user_id`           STRING COMMENT '用户id',
    `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
    `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
    `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
) COMMENT '交易域用户粒度支付最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_payment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
insert overwrite table dws_trade_user_payment_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount),
    dt
from dwd.dwd_trade_pay_detail_suc_inc
group by user_id,dt;
    --每日装载
insert overwrite table dws_trade_user_payment_1d partition(dt='2022-12-05')
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount)
from dwd.dwd_trade_pay_detail_suc_inc
where dt='2022-12-05'
group by user_id;

--交易域省份粒度订单最近1日汇总表
    --建表语句
DROP TABLE IF EXISTS dws_trade_province_order_1d;
CREATE EXTERNAL TABLE dws_trade_province_order_1d
(
    `province_id`               STRING COMMENT '用户id',
    `province_name`             STRING COMMENT '省份名称',
    `area_code`                 STRING COMMENT '地区编码',
    `iso_code`                  STRING COMMENT '旧版ISO-3166-2编码',
    `iso_3166_2`                STRING COMMENT '新版版ISO-3166-2编码',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域省份粒度订单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_province_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_province_order_1d partition(dt)
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    dt
from
    (
        select
            province_id,
            count(distinct(order_id)) order_count_1d,
            sum(split_original_amount) order_original_amount_1d,
            sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
            sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
            sum(split_total_amount) order_total_amount_1d,
            dt
        from dwd.dwd_trade_order_detail_inc
        group by province_id,dt
    )o
        left join
    (
        select
            id,
            province_name,
            area_code,
            iso_code,
            iso_3166_2
        from dim_province_full
        where dt='2022-12-04'
    )p
    on o.province_id=p.id;
    --每日装载
insert overwrite table dws_trade_province_order_1d partition(dt='2022-12-05')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d
from
    (
        select
            province_id,
            count(distinct(order_id)) order_count_1d,
            sum(split_original_amount) order_original_amount_1d,
            sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
            sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
            sum(split_total_amount) order_total_amount_1d
        from dwd.dwd_trade_order_detail_inc
        where dt='2022-12-05'
        group by province_id
    )o
        left join
    (
        select
            id,
            province_name,
            area_code,
            iso_code,
            iso_3166_2
        from dim_province_full
        where dt='2022-12-05'
    )p
    on o.province_id=p.id;

--交易域用户粒度退单最近1日汇总表
    --建表语句
DROP TABLE IF EXISTS dws_trade_user_order_refund_1d;
CREATE EXTERNAL TABLE dws_trade_user_order_refund_1d
(
    `user_id`                STRING COMMENT '用户id',
    `order_refund_count_1d`  BIGINT COMMENT '最近1日退单次数',
    `order_refund_num_1d`    BIGINT COMMENT '最近1日退单商品件数',
    `order_refund_amount_1d` DECIMAL(16, 2) COMMENT '最近1日退单金额'
) COMMENT '交易域用户粒度退单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_trade_user_order_refund_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
    --首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_order_refund_1d partition(dt)
select
    user_id,
    count(*) order_refund_count,
    sum(refund_num) order_refund_num,
    sum(refund_amount) order_refund_amount,
    dt
from dwd.dwd_trade_order_refund_inc
group by user_id,dt;
    --每日装载
insert overwrite table dws_trade_user_order_refund_1d partition(dt='2022-12-05')
select
    user_id,
    count(*),
    sum(refund_num),
    sum(refund_amount)
from dwd.dwd_trade_order_refund_inc
where dt='2022-12-05'
group by user_id;