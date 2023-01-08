--新增交易用户统计
--统计周期	    指标
--最近1、7、30日	新增下单人数
--最近1、7、30日	新增支付人数

CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
USE ads;
--建表语句
DROP TABLE IF EXISTS ads_new_buyer_stats;
CREATE EXTERNAL TABLE ads_new_buyer_stats
(
    `dt`                     STRING COMMENT '统计日期',
    `recent_days`            BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count`   BIGINT COMMENT '新增下单人数',
    `new_payment_user_count` BIGINT COMMENT '新增支付人数'
) COMMENT '新增交易用户统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_new_buyer_stats/';
--数据装载
insert overwrite table ads_new_buyer_stats
select * from ads_new_buyer_stats
union
select
    '2022-12-04',
    odr.recent_days,
    new_order_user_count,
    new_payment_user_count
from
    (
        select
            recent_days,
            sum(if(order_date_first>=date_add('2022-12-04',-recent_days+1),1,0)) new_order_user_count
        from dws.dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
        where dt='2022-12-04'
        group by recent_days
    )odr
        join
    (
        select
            recent_days,
            sum(if(payment_date_first>=date_add('2022-12-04',-recent_days+1),1,0)) new_payment_user_count
        from dws.dws_trade_user_payment_td lateral view explode(array(1,7,30)) tmp as recent_days
        where dt='2022-12-04'
        group by recent_days
    )pay
    on odr.recent_days=pay.recent_days;