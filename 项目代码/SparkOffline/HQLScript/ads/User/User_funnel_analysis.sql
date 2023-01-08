--用户行为漏斗分析(漏斗分析是一个数据分析模型,它能够科学反映一个业务过程从起点到终点各阶段用户转化情况)
--统计周期	    指标
--最近1/7/30日	首页浏览人数
--最近1/7/30日	商品详情页浏览人数
--最近1/7/30日	加购人数
--最近1/7/30日	下单人数
--最近1/7/30日	支付人数->支付成功人数

CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
USE ads;
--建表语句
DROP TABLE IF EXISTS ads_user_action;
CREATE EXTERNAL TABLE ads_user_action
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加入购物车人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
) COMMENT '漏斗分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_user_action/';
--数据装载
insert overwrite table ads_user_action
select * from ads_user_action
union
select
    '2022-12-04' dt,
    page.recent_days,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
    (
        select
            1 recent_days,
            sum(if(page_id='home',1,0)) home_count,
            sum(if(page_id='good_detail',1,0)) good_detail_count
        from dws.dws_traffic_page_visitor_page_view_1d
        where dt='2022-12-04'
          and page_id in ('home','good_detail')
        union all
        select
            recent_days,
            sum(if(page_id='home' and view_count>0,1,0)),
            sum(if(page_id='good_detail' and view_count>0,1,0))
        from
            (
                select
                    recent_days,
                    page_id,
                    case recent_days
                        when 7 then view_count_7d
                        when 30 then view_count_30d
                        end view_count
                from dws.dws_traffic_page_visitor_page_view_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2022-12-04'
                  and page_id in ('home','good_detail')
            )t1
        group by recent_days
    )page
        join
    (
        select
            1 recent_days,
            count(*) cart_count
        from dws.dws_trade_user_cart_add_1d
        where dt='2022-12-04'
        union all
        select
            recent_days,
            sum(if(cart_count>0,1,0))
        from
            (
                select
                    recent_days,
                    case recent_days
                        when 7 then cart_add_count_7d
                        when 30 then cart_add_count_30d
                        end cart_count
                from dws.dws_trade_user_cart_add_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2022-12-04'
            )t1
        group by recent_days
    )cart
    on page.recent_days=cart.recent_days
        join
    (
        select
            1 recent_days,
            count(*) order_count
        from dws.dws_trade_user_order_1d
        where dt='2022-12-04'
        union all
        select
            recent_days,
            sum(if(order_count>0,1,0))
        from
            (
                select
                    recent_days,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                        end order_count
                from dws.dws_trade_user_order_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2022-12-04'
            )t1
        group by recent_days
    )ord
    on page.recent_days=ord.recent_days
        join
    (
        select
            1 recent_days,
            count(*) payment_count
        from dws.dws_trade_user_payment_1d
        where dt='2022-12-04'
        union all
        select
            recent_days,
            sum(if(order_count>0,1,0))
        from
            (
                select
                    recent_days,
                    case recent_days
                        when 7 then payment_count_7d
                        when 30 then payment_count_30d
                        end order_count
                from dws.dws_trade_user_payment_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2022-12-04'
            )t1
        group by recent_days
    )pay
    on page.recent_days=pay.recent_days;
