--优惠券主题
--最近30天发布的优惠券的补贴率
CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
USE ads;
--建表语句
DROP TABLE IF EXISTS ads_coupon_stats;
CREATE EXTERNAL TABLE ads_coupon_stats
(
    `dt`          STRING COMMENT '统计日期',
    `coupon_id`   STRING COMMENT '优惠券ID',
    `coupon_name` STRING COMMENT '优惠券名称',
    `start_date`  STRING COMMENT '发布日期',
    `rule_name`   STRING COMMENT '优惠规则，例如满100元减10元',
    `reduce_rate` DECIMAL(16, 2) COMMENT '补贴率'
) COMMENT '优惠券统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_coupon_stats/';
--数据装载
insert overwrite table ads_coupon_stats
select * from ads_coupon_stats
union
select
    '2022-12-04' dt,
    coupon_id,
    coupon_name,
    start_date,
    coupon_rule,
    cast(coupon_reduce_amount_30d/original_amount_30d as decimal(16,2))
from dws.dws_trade_coupon_order_nd
where dt='2022-12-04';