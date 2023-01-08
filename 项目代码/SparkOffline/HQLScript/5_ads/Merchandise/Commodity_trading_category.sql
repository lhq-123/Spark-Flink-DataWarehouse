--各品类商品交易统计
--统计周期	统计粒度	指标	说明
--最近1/7/30日	品类	订单数	略
--最近1/7/30日	品类	订单人数	略
--最近1/7/30日	品类	退单数	略
--最近1/7/30日	品类	退单人数	略

CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
USE ads;
--建表语句
DROP TABLE IF EXISTS ads_trade_stats_by_cate;
CREATE EXTERNAL TABLE ads_trade_stats_by_cate
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`            STRING COMMENT '一级分类id',
    `category1_name`          STRING COMMENT '一级分类名称',
    `category2_id`            STRING COMMENT '二级分类id',
    `category2_name`          STRING COMMENT '二级分类名称',
    `category3_id`            STRING COMMENT '三级分类id',
    `category3_name`          STRING COMMENT '三级分类名称',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '订单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
) COMMENT '各分类商品交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_trade_stats_by_cate/';
--数据装载
insert overwrite table ads_trade_stats_by_cate
select * from ads_trade_stats_by_cate
union
select
    '2022-12-04' dt,
    nvl(odr.recent_days,refund.recent_days),
    nvl(odr.category1_id,refund.category1_id),
    nvl(odr.category1_name,refund.category1_name),
    nvl(odr.category2_id,refund.category2_id),
    nvl(odr.category2_name,refund.category2_name),
    nvl(odr.category3_id,refund.category3_id),
    nvl(odr.category3_name,refund.category3_name),
    nvl(order_count,0),
    nvl(order_user_count,0),
    nvl(order_refund_count,0),
    nvl(order_refund_user_count,0)
from
    (
        select
            1 recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_count_1d) order_count,
            count(distinct(user_id)) order_user_count
        from dws.dws_trade_user_sku_order_1d
        where dt='2022-12-04'
        group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
        union all
        select
            recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_count),
            count(distinct(if(order_count>0,user_id,null)))
        from
            (
                select
                    recent_days,
                    user_id,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                        end order_count
                from dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2022-12-04'
            )t1
        group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    )odr
        full outer join
    (
        select
            1 recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_refund_count_1d) order_refund_count,
            count(distinct(user_id)) order_refund_user_count
        from dws.dws_trade_user_sku_order_refund_1d
        where dt='2022-12-04'
        group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
        union all
        select
            recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_refund_count),
            count(distinct(if(order_refund_count>0,user_id,null)))
        from
            (
                select
                    recent_days,
                    user_id,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    case recent_days
                        when 7 then order_refund_count_7d
                        when 30 then order_refund_count_30d
                        end order_refund_count
                from dws.dws_trade_user_sku_order_refund_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2022-12-04'
            )t1
        group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    )refund
    on odr.recent_days=refund.recent_days
        and odr.category1_id=refund.category1_id
        and odr.category1_name=refund.category1_name
        and odr.category2_id=refund.category2_id
        and odr.category2_name=refund.category2_name
        and odr.category3_id=refund.category3_id
        and odr.category3_name=refund.category3_name;