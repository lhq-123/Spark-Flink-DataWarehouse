#!/bin/bash
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi

dws_trade_province_order_1d="
insert overwrite table dws.dws_trade_province_order_1d partition(dt)
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
    from dim.dim_province_full
    where dt='$do_date'
)p
on o.province_id=p.id;
"
dws_trade_user_cart_add_1d="
insert overwrite table dws.dws_trade_user_cart_add_1d partition(dt)
select
    user_id,
    count(*),
    sum(sku_num),
    dt
from dwd.dwd_trade_cart_add_inc
group by user_id,dt;
"
dws_trade_user_order_1d="
insert overwrite table dws.dws_trade_user_order_1d partition(dt)
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
"
dws_trade_user_order_refund_1d="
insert overwrite table dws.dws_trade_user_order_refund_1d partition(dt)
select
    user_id,
    count(*) order_refund_count,
    sum(refund_num) order_refund_num,
    sum(refund_amount) order_refund_amount,
    dt
from dwd.dwd_trade_order_refund_inc
group by user_id,dt;
"
dws_trade_user_payment_1d="
insert overwrite table dws.dws_trade_user_payment_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount),
    dt
from dwd.dwd_trade_pay_detail_suc_inc
group by user_id,dt;
"
dws_trade_user_sku_order_1d="
insert overwrite table dws.dws_trade_user_sku_order_1d partition(dt)
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
    where dt='$do_date'
)sku
on od.sku_id=sku.id;
"
dws_trade_user_sku_order_refund_1d="
insert overwrite table dws.dws_trade_user_sku_order_refund_1d partition(dt)
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
    where dt='$do_date'
)sku
on od.sku_id=sku.id;
"
dws_traffic_page_visitor_page_view_1d="
insert overwrite table dws.dws_traffic_page_visitor_page_view_1d partition(dt='$do_date')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from dwd.dwd_traffic_page_view_inc
where dt='$do_date'
group by mid_id,brand,model,operate_system,page_id;
"
dws_traffic_session_page_view_1d="
insert overwrite table dws.dws_traffic_session_page_view_1d partition(dt='$do_date')
select
    session_id,
    mid_id,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from dwd.dwd_traffic_page_view_inc
where dt='$do_date'
group by session_id,mid_id,brand,model,operate_system,version_code,channel;
"

case $1 in
    "dws_trade_province_order_1d" )
        hive -e "$dws_trade_province_order_1d"
    ;;
    "dws_trade_user_cart_add_1d" )
        hive -e "$dws_trade_user_cart_add_1d"
    ;;
    "dws_trade_user_order_1d" )
        hive -e "$dws_trade_user_order_1d"
    ;;
    "dws_trade_user_order_refund_1d" )
        hive -e "$dws_trade_user_order_refund_1d"
    ;;
    "dws_trade_user_payment_1d" )
        hive -e "$dws_trade_user_payment_1d"
    ;;
    "dws_trade_user_sku_order_1d" )
        hive -e "$dws_trade_user_sku_order_1d"
    ;;
    "dws_trade_user_sku_order_refund_1d" )
        hive -e "$dws_trade_user_sku_order_refund_1d"
    ;;
    "dws_traffic_page_visitor_page_view_1d" )
        hive -e "$dws_traffic_page_visitor_page_view_1d"
    ;;
    "dws_traffic_session_page_view_1d" )
        hive -e "$dws_traffic_session_page_view_1d"
    ;;
    "all" )
        hive -e "$dws_trade_province_order_1d$dws_trade_user_cart_add_1d$dws_trade_user_order_1d$dws_trade_user_order_refund_1d$dws_trade_user_payment_1d$dws_trade_user_sku_order_1d$dws_trade_user_sku_order_refund_1d$dws_traffic_page_visitor_page_view_1d$dws_traffic_session_page_view_1d"
    ;;
esac