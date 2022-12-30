--交易综合统计
--统计周期	    指标
--最近1/7/30日	订单总额	->订单最终金额
--最近1/7/30日	订单数	  
--最近1/7/30日	订单人数  
--最近1/7/30日	退单数	  
--最近1/7/30日	退单人数

CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
USE ads;
--建表语句
DROP TABLE IF EXISTS ads_trade_stats;
CREATE EXTERNAL TABLE ads_trade_stats
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1日,7:最近7天,30:最近30天',
    `order_total_amount`      DECIMAL(16, 2) COMMENT '订单总额,GMV',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '下单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
) COMMENT '交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_trade_stats/';
--数据装载
insert overwrite table ads_trade_stats
select * from ads_trade_stats
union
select
    '2022-12-04',
    odr.recent_days,
    order_total_amount,
    order_count,
    order_user_count,
    order_refund_count,
    order_refund_user_count
from
    (
        select
            1 recent_days,
            sum(order_total_amount_1d) order_total_amount,
            sum(order_count_1d) order_count,
            count(*) order_user_count
        from dws_trade_user_order_1d
        where dt='2022-12-04'
        union all
        select
            recent_days,
            sum(order_total_amount),
            sum(order_count),
            sum(if(order_count>0,1,0))
        from
            (
                select
                    recent_days,
                    case recent_days
                        when 7 then order_total_amount_7d
                        when 30 then order_total_amount_30d
                        end order_total_amount,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                        end order_count
                from dws_trade_user_order_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2022-12-04'
            )t1
        group by recent_days
    )odr
        join
    (
        select
            1 recent_days,
            sum(order_refund_count_1d) order_refund_count,
            count(*) order_refund_user_count
        from dws_trade_user_order_refund_1d
        where dt='2022-12-04'
        union all
        select
            recent_days,
            sum(order_refund_count),
            sum(if(order_refund_count>0,1,0))
        from
            (
                select
                    recent_days,
                    case recent_days
                        when 7 then order_refund_count_7d
                        when 30 then order_refund_count_30d
                        end order_refund_count
                from dws_trade_user_order_refund_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2022-12-04'
            )t1
        group by recent_days
    )refund
    on odr.recent_days=refund.recent_days;