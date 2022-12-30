#!/bin/bash
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

ads_activity_stats="
insert overwrite table ads.ads_activity_stats
select * from ads.ads_activity_stats
union
select
    '$do_date' dt,
    activity_id,
    activity_name,
    start_date,
    cast(activity_reduce_amount_30d/original_amount_30d as decimal(16,2))
from dws.dws_trade_activity_order_nd
where dt='$do_date';
"
ads_coupon_stats="
insert overwrite table ads.ads_coupon_stats
select * from ads.ads_coupon_stats
union
select
    '$do_date' dt,
    coupon_id,
    coupon_name,
    start_date,
    coupon_rule,
    cast(coupon_reduce_amount_30d/original_amount_30d as decimal(16,2))
from dws.dws_trade_coupon_order_nd
where dt='$do_date';

"
ads_new_buyer_stats="
insert overwrite table ads.ads_new_buyer_stats
select * from ads.ads_new_buyer_stats
union
select
    '$do_date',
    odr.recent_days,
    new_order_user_count,
    new_payment_user_count
from
(
    select
        recent_days,
        sum(if(order_date_first>=date_add('$do_date',-recent_days+1),1,0)) new_order_user_count
    from dws.dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
    where dt='$do_date'
    group by recent_days
)odr
join
(
    select
        recent_days,
        sum(if(payment_date_first>=date_add('$do_date',-recent_days+1),1,0)) new_payment_user_count
    from dws.dws_trade_user_payment_td lateral view explode(array(1,7,30)) tmp as recent_days
    where dt='$do_date'
    group by recent_days
)pay
on odr.recent_days=pay.recent_days;
"
ads_order_by_province="
insert overwrite table ads.ads_order_by_province
select * from ads.ads_order_by_province
union
select
    '$do_date' dt,
    1 recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_total_amount_1d
from dws.dws_trade_province_order_1d
where dt='$do_date'
union
select
    '$do_date' dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(order_count),
    sum(order_total_amount)
from
(
    select
        recent_days,
        province_id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2,
        case recent_days
            when 7 then order_count_7d
            when 30 then order_count_30d
        end order_count,
        case recent_days
            when 7 then order_total_amount_7d
            when 30 then order_total_amount_30d
        end order_total_amount
    from dws.dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
    where dt='$do_date'
)t1
group by recent_days,province_id,province_name,area_code,iso_code,iso_3166_2;
"

ads_page_path="
insert overwrite table ads.ads_page_path
select * from ads.ads_page_path
union
select
    '$do_date' dt,
    recent_days,
    source,
    nvl(target,'null'),
    count(*) path_count
from
(
    select
        recent_days,
        concat('step-',rn,':',page_id) source,
        concat('step-',rn+1,':',next_page_id) target
    from
    (
        select
            recent_days,
            page_id,
            lead(page_id,1,null) over(partition by session_id,recent_days) next_page_id,
            row_number() over (partition by session_id,recent_days order by view_time) rn
        from dwd.dwd_traffic_page_view_inc lateral view explode(array(1,7,30)) tmp as recent_days
        where dt>=date_add('$do_date',-recent_days+1)
    )t1
)t2
group by recent_days,source,target;
"
ads_repeat_purchase_by_tm="
insert overwrite table ads.ads_repeat_purchase_by_tm
select * from ads.ads_repeat_purchase_by_tm
union
select
    '$do_date' dt,
    recent_days,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
(
    select
        '$do_date' dt,
        recent_days,
        user_id,
        tm_id,
        tm_name,
        sum(order_count) order_count
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
        where dt='$do_date'
    )t1
    group by recent_days,user_id,tm_id,tm_name
)t2
group by recent_days,tm_id,tm_name;
"
ads_sku_cart_num_top3_by_cate="
insert overwrite table ads.ads_sku_cart_num_top3_by_cate
select * from ads.ads_sku_cart_num_top3_by_cate
union
select
    '$do_date' dt,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
(
    select
        sku_id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        cart_num,
        rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
        select
            sku_id,
            sum(sku_num) cart_num
        from dwd.dwd_trade_cart_full
        where dt='$do_date'
        group by sku_id
    )cart
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
            category3_name
        from dim.dim_sku_full
        where dt='$do_date'
    )sku
    on cart.sku_id=sku.id
)t1
where rk<=3;
"
ads_trade_stats="
insert overwrite table ads.ads_trade_stats
select * from ads.ads_trade_stats
union
select
    '$do_date',
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
    from dws.dws_trade_user_order_1d
    where dt='$do_date'
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
        from dws.dws_trade_user_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by recent_days
)odr
join
(
    select
        1 recent_days,
        sum(order_refund_count_1d) order_refund_count,
        count(*) order_refund_user_count
    from dws.dws_trade_user_order_refund_1d
    where dt='$do_date'
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
        from dws.dws_trade_user_order_refund_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by recent_days
)refund
on odr.recent_days=refund.recent_days;
"
ads_trade_stats_by_cate="
insert overwrite table ads.ads_trade_stats_by_cate
select * from ads.ads_trade_stats_by_cate
union
select
    '$do_date' dt,
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
    where dt='$do_date'
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
        where dt='$do_date'
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
    where dt='$do_date'
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
        where dt='$do_date'
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
"
ads_trade_stats_by_tm="
insert overwrite table ads.ads_trade_stats_by_tm
select * from ads.ads_trade_stats_by_tm
union
select
    '$do_date' dt,
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
    where dt='$do_date'
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
        where dt='$do_date'
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
    where dt='$do_date'
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
        where dt='$do_date'
    )t1
    group by recent_days,tm_id,tm_name
)refund
on odr.recent_days=refund.recent_days
and odr.tm_id=refund.tm_id
and odr.tm_name=refund.tm_name;
"
ads_traffic_stats_by_channel="
insert overwrite table ads.ads_traffic_stats_by_channel
select * from ads.ads_traffic_stats_by_channel
union
select
    '$do_date' dt,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from dws.dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt>=date_add('$do_date',-recent_days+1)
group by recent_days,channel;
"
ads_user_action="
insert overwrite table ads.ads_user_action
select * from ads.ads_user_action
union
select
    '$do_date' dt,
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
    where dt='$do_date'
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
        where dt='$do_date'
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
    where dt='$do_date'
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
        where dt='$do_date'
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
    where dt='$do_date'
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
        where dt='$do_date'
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
    where dt='$do_date'
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
        where dt='$do_date'
    )t1
    group by recent_days
)pay
on page.recent_days=pay.recent_days;
"
ads_user_change="
insert overwrite table ads.ads_user_change
select * from ads.ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '$do_date' dt,
        count(*) user_churn_count
    from dws.dws_user_user_login_td
    where dt='$do_date'
    and login_date_last=date_add('$do_date',-7)
)churn
join
(
    select
        '$do_date' dt,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from dws.dws_user_user_login_td
        where dt='$do_date'
    )t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from dws.dws_user_user_login_td
        where dt=date_add('$do_date',-1)
    )t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
)back
on churn.dt=back.dt;
"
ads_user_retention="
insert overwrite table ads.ads_user_retention
select * from ads.ads_user_retention
union
select
    '$do_date' dt,
    login_date_first create_date,
    datediff('$do_date',login_date_first) retention_day,
    sum(if(login_date_last='$do_date',1,0)) retention_count,
    count(*) new_user_count,
    cast(sum(if(login_date_last='$do_date',1,0))/count(*)*100 as decimal(16,2)) retention_rate
from
(
    select
        user_id,
        date_id login_date_first
    from dwd.dwd_user_register_inc
    where dt>=date_add('$do_date',-7)
    and dt<'$do_date'
)t1
join
(
    select
        user_id,
        login_date_last
    from dws.dws_user_user_login_td
    where dt='$do_date'
)t2
on t1.user_id=t2.user_id
group by login_date_first;
"
ads_user_stats="
insert overwrite table ads.ads_user_stats
select * from ads.ads_user_stats
union
select
    '$do_date' dt,
    t1.recent_days,
    new_user_count,
    active_user_count
from
(
    select
        recent_days,
        sum(if(login_date_last>=date_add('$do_date',-recent_days+1),1,0)) new_user_count
    from dws.dws_user_user_login_td lateral view explode(array(1,7,30)) tmp as recent_days
    where dt='$do_date'
    group by recent_days
)t1
join
(
    select
        recent_days,
        sum(if(date_id>=date_add('$do_date',-recent_days+1),1,0)) active_user_count
    from dwd.dwd_user_register_inc lateral view explode(array(1,7,30)) tmp as recent_days
    group by recent_days
)t2
on t1.recent_days=t2.recent_days;
"

case $1 in
    "ads_activity_stats" )
        hive -e "$ads_activity_stats"
    ;;
    "ads_coupon_stats" )
        hive -e "$ads_coupon_stats"
    ;;
    "ads_new_buyer_stats" )
        hive -e "$ads_new_buyer_stats"
    ;;
    "ads_order_by_province" )
        hive -e "$ads_order_by_province"
    ;;
    "ads_page_path" )
        hive -e "$ads_page_path"
    ;;
    "ads_repeat_purchase_by_tm" )
        hive -e "$ads_repeat_purchase_by_tm"
    ;;
    "ads_sku_cart_num_top3_by_cate" )
        hive -e "$ads_sku_cart_num_top3_by_cate"
    ;;
    "ads_trade_stats" )
        hive -e "$ads_trade_stats"
    ;;
    "ads_trade_stats_by_cate" )
        hive -e "$ads_trade_stats_by_cate"
    ;;
    "ads_trade_stats_by_tm" )
        hive -e "$ads_trade_stats_by_tm"
    ;;
    "ads_traffic_stats_by_channel" )
        hive -e "$ads_traffic_stats_by_channel"
    ;;
    "ads_user_action" )
        hive -e "$ads_user_action"
    ;;
    "ads_user_change" )
        hive -e "$ads_user_change"
    ;;
    "ads_user_retention" )
        hive -e "$ads_user_retention"
    ;;
    "ads_user_stats" )
        hive -e "$ads_user_stats"
    ;;
    "all" )
        hive -e "$ads_activity_stats$ads_coupon_stats$ads_new_buyer_stats$ads_order_by_province$ads_page_path$ads_repeat_purchase_by_tm$ads_sku_cart_num_top3_by_cate$ads_trade_stats$ads_trade_stats_by_cate$ads_trade_stats_by_tm$ads_traffic_stats_by_channel$ads_user_action$ads_user_change$ads_user_retention$ads_user_stats"
    ;;
esac