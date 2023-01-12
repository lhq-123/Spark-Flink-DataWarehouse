--活动主题
--最近30天发布的活动的补贴率
CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
USE ads;
--建表语句
DROP TABLE IF EXISTS ads_activity_stats;
CREATE EXTERNAL TABLE ads_activity_stats
(
    `dt`            STRING COMMENT '统计日期',
    `activity_id`   STRING COMMENT '活动ID',
    `activity_name` STRING COMMENT '活动名称',
    `start_date`    STRING COMMENT '活动开始日期',
    `reduce_rate`   DECIMAL(16, 2) COMMENT '补贴率'
) COMMENT '活动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_activity_stats/';
--数据装载
insert overwrite table ads_activity_stats
select * from ads_activity_stats
union
select
    '2022-12-04' dt,
    activity_id,
    activity_name,
    start_date,
    cast(activity_reduce_amount_30d/original_amount_30d as decimal(16,2))
from dws.dws_trade_activity_order_nd
where dt='2022-12-04';