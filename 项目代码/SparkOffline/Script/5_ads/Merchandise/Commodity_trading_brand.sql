--各品牌商品交易统计
--统计周期	统计粒度	指标	说明
--最近1/7/30日	品牌	订单数	略
--最近1/7/30日	品牌	订单人数	略
--最近1/7/30日	品牌	退单数	略
--最近1/7/30日	品牌	退单人数	略

CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
USE ads;
--建表语句
DROP TABLE IF EXISTS ads_trade_stats_by_tm;
CREATE EXTERNAL TABLE ads_trade_stats_by_tm
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`                   STRING COMMENT '品牌ID',
    `tm_name`                 STRING COMMENT '品牌名称',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '订单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
) COMMENT '各品牌商品交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_trade_stats_by_tm/';
--数据装载
insert overwrite table ads_trade_stats_by_tm
select * from ads_trade_stats_by_tm
union
select
    '2022-12-04' dt,
    nvl(odr.recent_days,refund.recent_days),
    nvl(odr.tm_id,refund.tm_id),
    nvl(odr.tm_name,refund.tm_name),
    nvl(order_count,0),
    nvl(order_user_count,0),
    nvl(order_refund_count,0),
    nvl(order_refund_user_count,0)
from
    (
        select
            1 recent_days,
            tm_id,
            tm_name,
            sum(order_count_1d) order_count,
            count(distinct(user_id)) order_user_count
        from dws.dws_trade_user_sku_order_1d
        where dt='2022-12-04'
        group by tm_id,tm_name
        union all
        select
            recent_days,
            tm_id,
            tm_name,
            sum(order_count),
            count(distinct(if(order_count>0,user_id,null)))
        from
            (
                select
                    recent_days,
                    user_id,
                    tm_id,
                    tm_name,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                        end order_count
                from dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2022-12-04'
            )t1
        group by recent_days,tm_id,tm_name
    )odr
        full outer join
    (
        select
            1 recent_days,
            tm_id,
            tm_name,
            sum(order_refund_count_1d) order_refund_count,
            count(distinct(user_id)) order_refund_user_count
        from dws.dws_trade_user_sku_order_refund_1d
        where dt='2022-12-04'
        group by tm_id,tm_name
        union all
        select
            recent_days,
            tm_id,
            tm_name,
            sum(order_refund_count),
            count(if(order_refund_count>0,user_id,null))
        from
            (
                select
                    recent_days,
                    user_id,
                    tm_id,
                    tm_name,
                    case recent_days
                        when 7 then order_refund_count_7d
                        when 30 then order_refund_count_30d
                        end order_refund_count
                from dws.dws_trade_user_sku_order_refund_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2022-12-04'
            )t1
        group by recent_days,tm_id,tm_name
    )refund
    on odr.recent_days=refund.recent_days
        and odr.tm_id=refund.tm_id
        and odr.tm_name=refund.tm_name;