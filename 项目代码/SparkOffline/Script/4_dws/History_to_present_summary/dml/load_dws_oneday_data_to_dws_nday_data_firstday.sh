#!/bin/bash
if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi

dws_trade_user_order_td="
insert overwrite table dws.dws_trade_user_order_td partition(dt='$do_date')
select
    user_id,
    min(dt) login_date_first,
    max(dt) login_date_last,
    sum(order_count_1d) order_count,
    sum(order_num_1d) order_num,
    sum(order_original_amount_1d) original_amount,
    sum(activity_reduce_amount_1d) activity_reduce_amount,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount,
    sum(order_total_amount_1d) total_amount
from dws.dws_trade_user_order_1d
group by user_id;
"

dws_trade_user_payment_td="
insert overwrite table dws.dws_trade_user_payment_td partition(dt='$do_date')
select
    user_id,
    min(dt) payment_date_first,
    max(dt) payment_date_last,
    sum(payment_count_1d) payment_count,
    sum(payment_num_1d) payment_num,
    sum(payment_amount_1d) payment_amount
from dws.dws_trade_user_payment_1d
group by user_id;
"

dws_user_user_login_td="
insert overwrite table dws.dws_user_user_login_td partition(dt='$do_date')
select
    u.id,
    nvl(login_date_last,date_format(create_time,'yyyy-MM-dd')),
    nvl(login_count_td,1)
from
(
    select
        id,
        create_time
    from dim.dim_user_zip
    where dt='9999-12-31'
)u
left join
(
    select
        user_id,
        max(dt) login_date_last,
        count(*) login_count_td
    from dwd.dwd_user_login_inc
    group by user_id
)l
on u.id=l.user_id;
"

case $1 in
    "dws_trade_user_order_td" )
        hive -e "$dws_trade_user_order_td"
    ;;
    "dws_trade_user_payment_td" )
        hive -e "$dws_trade_user_payment_td"
    ;;
    "dws_user_user_login_td" )
        hive -e "$dws_user_user_login_td"
    ;;
    "all" )
        hive -e "$dws_trade_user_order_td$dws_trade_user_payment_td$dws_user_user_login_td"
    ;;
esac