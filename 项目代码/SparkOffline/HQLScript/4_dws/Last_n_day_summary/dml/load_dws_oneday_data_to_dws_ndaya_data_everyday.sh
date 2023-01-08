#!/bin/bash
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dws_trade_activity_order_nd="
insert overwrite table dws.dws_trade_activity_order_nd partition(dt='$do_date')
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
    from dim.dim_activity_full
    where dt='$do_date'
    and date_format(start_time,'yyyy-MM-dd')>=date_add('$do_date',-29)
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
    where dt>=date_add('$do_date',-29)
    and dt<='$do_date'
    and activity_id is not null
)od
on act.activity_id=od.activity_id
group by act.activity_id,activity_name,activity_type_code,activity_type_name,start_time;
"
dws_trade_coupon_order_nd="
insert overwrite table dws.dws_trade_coupon_order_nd partition(dt='$do_date')
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
    from dim.dim_coupon_full
    where dt='$do_date'
    and date_format(start_time,'yyyy-MM-dd')>=date_add('$do_date',-29)
)cou
left join
(
    select
        coupon_id,
        order_id,
        split_original_amount,
        split_coupon_amount
    from dwd.dwd_trade_order_detail_inc
    where dt>=date_add('$do_date',-29)
    and dt<='$do_date'
    and coupon_id is not null
)od
on cou.id=od.coupon_id
group by id,coupon_name,coupon_type_code,coupon_type_name,benefit_rule,start_date;
"
dws_trade_province_order_nd="
insert overwrite table dws.dws_trade_province_order_nd partition(dt='$do_date')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws.dws_trade_province_order_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by province_id,province_name,area_code,iso_code,iso_3166_2;
"
dws_trade_user_cart_add_nd="
insert overwrite table dws.dws_trade_user_cart_add_nd partition(dt='$do_date')
select
    user_id,
    sum(if(dt>=date_add('$do_date',-6),cart_add_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),cart_add_num_1d,0)),
    sum(cart_add_count_1d),
    sum(cart_add_num_1d)
from dws.dws_trade_user_cart_add_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by user_id;
"
dws_trade_user_order_nd="
insert overwrite table dws.dws_trade_user_order_nd partition(dt='$do_date')
select
    user_id,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_num_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws.dws_trade_user_order_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by user_id;
"
dws_trade_user_order_refund_nd="
insert overwrite table dws.dws_trade_user_order_refund_nd partition(dt='$do_date')
select
    user_id,
    sum(if(dt>=date_add('$do_date',-6),order_refund_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_refund_num_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_refund_amount_1d,0)),
    sum(order_refund_count_1d),
    sum(order_refund_num_1d),
    sum(order_refund_amount_1d)
from dws.dws_trade_user_order_refund_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by user_id;
"
dws_trade_user_payment_nd="
insert overwrite table dws.dws_trade_user_payment_nd partition (dt = '$do_date')
select user_id,
       sum(if(dt >= date_add('$do_date', -6), payment_count_1d, 0)),
       sum(if(dt >= date_add('$do_date', -6), payment_num_1d, 0)),
       sum(if(dt >= date_add('$do_date', -6), payment_amount_1d, 0)),
       sum(payment_count_1d),
       sum(payment_num_1d),
       sum(payment_amount_1d)
from dws.dws_trade_user_payment_1d
where dt >= date_add('$do_date', -29)
  and dt <= '$do_date'
group by user_id;
"
dws_trade_user_sku_order_nd="
insert overwrite table dws.dws_trade_user_sku_order_nd partition(dt='$do_date')
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
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_num_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws.dws_trade_user_sku_order_1d
where dt>=date_add('$do_date',-30)
group by  user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
"
dws_trade_user_sku_order_refund_nd="
insert overwrite table dws.dws_trade_user_sku_order_refund_nd partition(dt='$do_date')
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
    sum(if(dt>=date_add('$do_date',-6),order_refund_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_refund_num_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_refund_amount_1d,0)),
    sum(order_refund_count_1d),
    sum(order_refund_num_1d),
    sum(order_refund_amount_1d)
from dws.dws_trade_user_sku_order_refund_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
"
dws_traffic_page_visitor_page_view_nd="
insert overwrite table dws.dws_traffic_page_visitor_page_view_nd partition(dt='$do_date')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(if(dt>=date_add('$do_date',-6),during_time_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),view_count_1d,0)),
    sum(during_time_1d),
    sum(view_count_1d)
from dws.dws_traffic_page_visitor_page_view_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by mid_id,brand,model,operate_system,page_id;
"

case $1 in
    "dws_trade_activity_order_nd" )
        hive -e "$dws_trade_activity_order_nd"
    ;;
    "dws_trade_coupon_order_nd" )
        hive -e "$dws_trade_coupon_order_nd"
    ;;
    "dws_trade_province_order_nd" )
        hive -e "$dws_trade_province_order_nd"
    ;;
    "dws_trade_user_cart_add_nd" )
        hive -e "$dws_trade_user_cart_add_nd"
    ;;
    "dws_trade_user_order_nd" )
        hive -e "$dws_trade_user_order_nd"
    ;;
    "dws_trade_user_order_refund_nd" )
        hive -e "$dws_trade_user_order_refund_nd"
    ;;
    "dws_trade_user_payment_nd" )
        hive -e "$dws_trade_user_payment_nd"
    ;;
    "dws_trade_user_sku_order_nd" )
        hive -e "$dws_trade_user_sku_order_nd"
    ;;
    "dws_trade_user_sku_order_refund_nd" )
        hive -e "$dws_trade_user_sku_order_refund_nd"
    ;;
    "dws_traffic_page_visitor_page_view_nd" )
        hive -e "$dws_traffic_page_visitor_page_view_nd"
    ;;
    "all" )
        hive -e "$dws_trade_activity_order_nd$dws_trade_coupon_order_nd$dws_trade_province_order_nd$dws_trade_user_cart_add_nd$dws_trade_user_order_nd$dws_trade_user_order_refund_nd$dws_trade_user_payment_nd$dws_trade_user_sku_order_nd$dws_trade_user_sku_order_refund_nd$dws_traffic_page_visitor_page_view_nd"
    ;;
esac