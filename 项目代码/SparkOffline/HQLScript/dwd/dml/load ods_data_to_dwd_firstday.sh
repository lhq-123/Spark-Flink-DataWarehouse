#!/bin/bash
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi

dwd_interaction_comment_inc="
insert overwrite table dwd.dwd_interaction_comment_inc partition(dt)
select
    id,
    user_id,
    sku_id,
    order_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    appraise,
    dic_name,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        data.id,
        data.user_id,
        data.sku_id,
        data.order_id,
        data.create_time,
        data.appraise
    from ods.ods_comment_info_inc
    where dt='$do_date'
    and type='bootstrap-insert'
)ci
left join
(
    select
        dic_code,
        dic_name
    from ods.ods_base_dic_full
    where dt='$do_date'
    and parent_code='12'
)dic
on ci.appraise=dic.dic_code;
"
dwd_interaction_favor_add_inc="
insert overwrite table dwd.dwd_interaction_favor_add_inc partition(dt)
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time,
    date_format(data.create_time,'yyyy-MM-dd')
from ods.ods_favor_info_inc
where dt='$do_date'
and type = 'bootstrap-insert';
"

dwd_tool_coupon_get_inc="
insert overwrite table dwd.dwd_tool_coupon_get_inc partition(dt)
select
    data.id,
    data.coupon_id,
    data.user_id,
    date_format(data.get_time,'yyyy-MM-dd') date_id,
    data.get_time,
    date_format(data.get_time,'yyyy-MM-dd')
from ods.ods_coupon_use_inc
where dt='$do_date'
and type='bootstrap-insert';
"
dwd_tool_coupon_order_inc="
insert overwrite table dwd.dwd_tool_coupon_order_inc partition(dt)
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.using_time,'yyyy-MM-dd') date_id,
    data.using_time,
    date_format(data.using_time,'yyyy-MM-dd')
from ods.ods_coupon_use_inc
where dt='$do_date'
and type='bootstrap-insert'
and data.using_time is not null;
"
dwd_tool_coupon_pay_inc="
insert overwrite table dwd.dwd_tool_coupon_pay_inc partition(dt)
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.used_time,'yyyy-MM-dd') date_id,
    data.used_time,
    date_format(data.used_time,'yyyy-MM-dd')
from ods.ods_coupon_use_inc
where dt='$do_date'
and type='bootstrap-insert'
and data.used_time is not null;
"
dwd_trade_cancel_detail_inc="
insert overwrite table dwd.dwd_trade_cancel_detail_inc partition (dt)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(canel_time,'yyyy-MM-dd') date_id,
    canel_time,
    source_id,
    source_type,
    dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    date_format(canel_time,'yyyy-MM-dd')
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.source_id,
        data.source_type,
        data.sku_num,
        data.sku_num * data.order_price split_original_amount,
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods.ods_order_detail_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) od
join
(
    select
        data.id,
        data.user_id,
        data.province_id,
        data.operate_time canel_time
    from ods.ods_order_info_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
    and data.order_status='1003'
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods.ods_order_detail_activity_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods.ods_order_detail_coupon_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) cou
on od.id = cou.order_detail_id
left join
(
    select
        dic_code,
        dic_name
    from ods.ods_base_dic_full
    where dt='$do_date'
    and parent_code='24'
)dic
on od.source_type=dic.dic_code;
"
dwd_trade_cart_add_inc="
insert overwrite table dwd.dwd_trade_cart_add_inc partition (dt)
select
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    source_id,
    source_type,
    dic.dic_name,
    sku_num,
    date_format(create_time, 'yyyy-MM-dd')
from
(
    select
        data.id,
        data.user_id,
        data.sku_id,
        data.create_time,
        data.source_id,
        data.source_type,
        data.sku_num
    from ods.ods_cart_info_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
)ci
left join
(
    select
        dic_code,
        dic_name
    from ods.ods_base_dic_full
    where dt='$do_date'
    and parent_code='24'
)dic
on ci.source_type=dic.dic_code;
"
dwd_trade_cart_full="
insert overwrite table dwd.dwd_trade_cart_full partition(dt='$do_date')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods.ods_cart_info_full
where dt='$do_date'
and is_ordered='0';
"
dwd_trade_order_detail_inc="
insert overwrite table dwd.dwd_trade_order_detail_inc partition (dt)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    source_id,
    source_type,
    dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.create_time,
        data.source_id,
        data.source_type,
        data.sku_num,
        data.sku_num * data.order_price split_original_amount,
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods.ods_order_detail_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) od
left join
(
    select
        data.id,
        data.user_id,
        data.province_id
    from ods.ods_order_info_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods.ods_order_detail_activity_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods.ods_order_detail_coupon_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) cou
on od.id = cou.order_detail_id
left join
(
    select
        dic_code,
        dic_name
    from ods.ods_base_dic_full
    where dt='$do_date'
    and parent_code='24'
)dic
on od.source_type=dic.dic_code;
"
dwd_trade_order_refund_inc="
insert overwrite table dwd.dwd_trade_order_refund_inc partition(dt)
select
    ri.id,
    user_id,
    order_id,
    sku_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    refund_type,
    type_dic.dic_name,
    refund_reason_type,
    reason_dic.dic_name,
    refund_reason_txt,
    refund_num,
    refund_amount,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        data.id,
        data.user_id,
        data.order_id,
        data.sku_id,
        data.refund_type,
        data.refund_num,
        data.refund_amount,
        data.refund_reason_type,
        data.refund_reason_txt,
        data.create_time
    from ods.ods_order_refund_info_inc
    where dt='$do_date'
    and type='bootstrap-insert'
)ri
left join
(
    select
        data.id,
        data.province_id
    from ods.ods_order_info_inc
    where dt='$do_date'
    and type='bootstrap-insert'
)oi
on ri.order_id=oi.id
left join
(
    select
        dic_code,
        dic_name
    from ods.ods_base_dic_full
    where dt='$do_date'
    and parent_code = '15'
)type_dic
on ri.refund_type=type_dic.dic_code
left join
(
    select
        dic_code,
        dic_name
    from ods.ods_base_dic_full
    where dt='$do_date'
    and parent_code = '13'
)reason_dic
on ri.refund_reason_type=reason_dic.dic_code;
"

dwd_trade_pay_detail_suc_inc="
insert overwrite table dwd.dwd_trade_pay_detail_suc_inc partition (dt)
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    source_id,
    source_type,
    src_dic.dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    date_format(callback_time,'yyyy-MM-dd')
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.source_id,
        data.source_type,
        data.sku_num,
        data.sku_num * data.order_price split_original_amount,
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods.ods_order_detail_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) od
join
(
    select
        data.user_id,
        data.order_id,
        data.payment_type,
        data.callback_time
    from ods.ods_payment_info_inc
    where dt='$do_date'
    and type='bootstrap-insert'
    and data.payment_status='1602'
) pi
on od.order_id=pi.order_id
left join
(
    select
        data.id,
        data.province_id
    from ods.ods_order_info_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods.ods_order_detail_activity_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods.ods_order_detail_coupon_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) cou
on od.id = cou.order_detail_id
left join
(
    select
        dic_code,
        dic_name
    from ods.ods_base_dic_full
    where dt='$do_date'
    and parent_code='11'
) pay_dic
on pi.payment_type=pay_dic.dic_code
left join
(
    select
        dic_code,
        dic_name
    from ods.ods_base_dic_full
    where dt='$do_date'
    and parent_code='24'
)src_dic
on od.source_type=src_dic.dic_code;
"
dwd_trade_refund_pay_suc_inc="
insert overwrite table dwd.dwd_trade_refund_pay_suc_inc partition(dt)
select
    rp.id,
    user_id,
    rp.order_id,
    rp.sku_id,
    province_id,
    payment_type,
    dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    refund_num,
    total_amount,
    date_format(callback_time,'yyyy-MM-dd')
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.payment_type,
        data.callback_time,
        data.total_amount
    from ods.ods_refund_payment_inc
    where dt='$do_date'
    and type = 'bootstrap-insert'
    and data.refund_status='1602'
)rp
left join
(
    select
        data.id,
        data.user_id,
        data.province_id
    from ods.ods_order_info_inc
    where dt='$do_date'
    and type='bootstrap-insert'
)oi
on rp.order_id=oi.id
left join
(
    select
        data.order_id,
        data.sku_id,
        data.refund_num
    from ods.ods_order_refund_info_inc
    where dt='$do_date'
    and type='bootstrap-insert'
)ri
on rp.order_id=ri.order_id
and rp.sku_id=ri.sku_id
left join
(
    select
        dic_code,
        dic_name
    from ods.ods_base_dic_full
    where dt='$do_date'
    and parent_code='11'
)dic
on rp.payment_type=dic.dic_code;
"
dwd_traffic_action_inc="
set hive.cbo.enable=false;
insert overwrite table dwd.dwd_traffic_action_inc partition(dt='$do_date')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    during_time,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    action_id,
    action_item,
    action_item_type,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') action_time
from
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
        page.during_time,
        page.item page_item,
        page.item_type page_item_type,
        page.last_page_id,
        page.page_id,
        page.source_type,
        action.action_id,
        action.item action_item,
        action.item_type action_item_type,
        action.ts
    from ods.ods_log_inc lateral view explode(actions) tmp as action
    where dt='$do_date'
    and actions is not null
)log
left join
(
    select
        id province_id,
        area_code
    from ods.ods_base_province_full
    where dt='$do_date'
)bp
on log.area_code=bp.area_code;
"
dwd_traffic_display_inc="
set hive.cbo.enable=false;
insert overwrite table dwd.dwd_traffic_display_inc partition(dt='$do_date')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    during_time,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') display_time,
    display_type,
    display_item,
    display_item_type,
    display_order,
    display_pos_id
from
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
        page.during_time,
        page.item page_item,
        page.item_type page_item_type,
        page.last_page_id,
        page.page_id,
        page.source_type,
        display.display_type,
        display.item display_item,
        display.item_type display_item_type,
        display.\`order\` display_order,
        display.pos_id display_pos_id,
        ts
    from ods.ods_log_inc lateral view explode(displays) tmp as display
    where dt='$do_date'
    and displays is not null
)log
left join
(
    select
        id province_id,
        area_code
    from ods.ods_base_province_full
    where dt='$do_date'
)bp
on log.area_code=bp.area_code;
"
dwd_traffic_error_inc="
set hive.cbo.enable=false;
set hive.execution.engine=mr;
insert overwrite table dwd.dwd_traffic_error_inc partition(dt='$do_date')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    entry,
    loading_time,
    open_ad_id,
    open_ad_ms,
    open_ad_skip_ms,
    actions,
    displays,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') error_time,
    error_code,
    error_msg
from
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
        page.during_time,
        page.item page_item,
        page.item_type page_item_type,
        page.last_page_id,
        page.page_id,
        page.source_type,
        \`start\`.entry,
        \`start\`.loading_time,
        \`start\`.open_ad_id,
        \`start\`.open_ad_ms,
        \`start\`.open_ad_skip_ms,
        actions,
        displays,
        err.error_code,
        err.msg error_msg,
        ts
    from ods.ods_log_inc
    where dt='$do_date'
    and err is not null
)log
left join
(
    select
        id province_id,
        area_code
    from ods.ods_base_province_full
    where dt='$do_date'
)bp
on log.area_code=bp.area_code;
set hive.execution.engine=spark;
"
dwd_traffic_page_view_inc="
set hive.cbo.enable=false;
insert overwrite table dwd.dwd_traffic_page_view_inc partition (dt='$do_date')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
    concat(mid_id,'-',last_value(session_start_point,true) over (partition by mid_id order by ts)) session_id,
    during_time
from
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.is_new is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
        page.during_time,
        page.item page_item,
        page.item_type page_item_type,
        page.last_page_id,
        page.page_id,
        page.source_type,
        ts,
        if(page.last_page_id is null,ts,null) session_start_point
    from ods.ods_log_inc
    where dt='$do_date'
    and page is not null
)log
left join
(
    select
        id province_id,
        area_code
    from ods.ods_base_province_full
    where dt='$do_date'
)bp
on log.area_code=bp.area_code;
"
dwd_traffic_start_inc="
set hive.cbo.enable=false;
insert overwrite table dwd.dwd_traffic_start_inc partition(dt='$do_date')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    entry,
    open_ad_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') action_time,
    loading_time,
    open_ad_ms,
    open_ad_skip_ms
from
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
        \`start\`.entry,
        \`start\`.loading_time,
        \`start\`.open_ad_id,
        \`start\`.open_ad_ms,
        \`start\`.open_ad_skip_ms,
        ts
    from ods.ods_log_inc
    where dt='$do_date'
    and \`start\` is not null
)log
left join
(
    select
        id province_id,
        area_code
    from ods.ods_base_province_full
    where dt='$do_date'
)bp
on log.area_code=bp.area_code;
"
dwd_user_login_inc="
insert overwrite table dwd.dwd_user_login_inc partition(dt='$do_date')
select
    user_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') login_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
(
    select
        user_id,
        channel,
        area_code,
        version_code,
        mid_id,
        brand,
        model,
        operate_system,
        ts
    from
    (
        select
            user_id,
            channel,
            area_code,
            version_code,
            mid_id,
            brand,
            model,
            operate_system,
            ts,
            row_number() over (partition by session_id order by ts) rn
        from
        (
            select
                user_id,
                channel,
                area_code,
                version_code,
                mid_id,
                brand,
                model,
                operate_system,
                ts,
                concat(mid_id,'-',last_value(session_start_point,true) over(partition by mid_id order by ts)) session_id
            from
            (
                select
                    common.uid user_id,
                    common.ch channel,
                    common.ar area_code,
                    common.vc version_code,
                    common.mid mid_id,
                    common.ba brand,
                    common.md model,
                    common.os operate_system,
                    ts,
                    if(page.last_page_id is null,ts,null) session_start_point
                from ods.ods_log_inc
                where dt='$do_date'
                and page is not null
            )t1
        )t2
        where user_id is not null
    )t3
    where rn=1
)t4
left join
(
    select
        id province_id,
        area_code
    from ods.ods_base_province_full
    where dt='$do_date'
)bp
on t4.area_code=bp.area_code;
"
dwd_user_register_inc="
insert overwrite table dwd.dwd_user_register_inc partition(dt)
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        data.id user_id,
        data.create_time
    from ods.ods_user_info_inc
    where dt='$do_date'
    and type='bootstrap-insert'
)ui
left join
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code
    from ods.ods_log_inc
    where dt='$do_date'
    and page.page_id='register'
    and common.uid is not null
)log
on ui.user_id=log.user_id
left join
(
    select
        id province_id,
        area_code
    from ods.ods_base_province_full
    where dt='$do_date'
)bp
on log.area_code=bp.area_code;
"

case $1 in
    "dwd_interaction_comment_inc" )
        hive -e "$dwd_interaction_comment_inc"
    ;;
    "dwd_interaction_favor_add_inc" )
        hive -e "$dwd_interaction_favor_add_inc"
    ;;
    "dwd_tool_coupon_get_inc" )
        hive -e "$dwd_tool_coupon_get_inc"
    ;;
    "dwd_tool_coupon_order_inc" )
        hive -e "$dwd_tool_coupon_order_inc"
    ;;
    "dwd_tool_coupon_pay_inc" )
        hive -e "$dwd_tool_coupon_pay_inc"
    ;;
    "dwd_trade_cancel_detail_inc" )
        hive -e "$dwd_trade_cancel_detail_inc"
    ;;
    "dwd_trade_cart_add_inc" )
        hive -e "$dwd_trade_cart_add_inc"
    ;;
    "dwd_trade_cart_full" )
        hive -e "$dwd_trade_cart_full"
    ;;
    "dwd_trade_order_detail_inc" )
        hive -e "$dwd_trade_order_detail_inc"
    ;;
    "dwd_trade_order_refund_inc" )
        hive -e "$dwd_trade_order_refund_inc"
    ;;
    "dwd_trade_pay_detail_suc_inc" )
        hive -e "$dwd_trade_pay_detail_suc_inc"
    ;;
    "dwd_trade_refund_pay_suc_inc" )
        hive -e "$dwd_trade_refund_pay_suc_inc"
    ;;
    "dwd_traffic_action_inc" )
        hive -e "$dwd_traffic_action_inc"
    ;;
    "dwd_traffic_display_inc" )
        hive -e "$dwd_traffic_display_inc"
    ;;
    "dwd_traffic_error_inc" )
        hive -e "$dwd_traffic_error_inc"
    ;;
    "dwd_traffic_page_view_inc" )
        hive -e "$dwd_traffic_page_view_inc"
    ;;
    "dwd_traffic_start_inc" )
        hive -e "$dwd_traffic_start_inc"
    ;;
    "dwd_user_login_inc" )
        hive -e "$dwd_user_login_inc"
    ;;
    "dwd_user_register_inc" )
        hive -e "$dwd_user_register_inc"
    ;;
    "all" )
        hive -e "$dwd_interaction_comment_inc$dwd_interaction_favor_add_inc$dwd_tool_coupon_get_inc$dwd_tool_coupon_order_inc$dwd_tool_coupon_pay_inc$dwd_trade_cancel_detail_inc$dwd_trade_cart_add_inc$dwd_trade_cart_full$dwd_trade_order_detail_inc$dwd_trade_order_refund_inc$dwd_trade_pay_detail_suc_inc$dwd_trade_refund_pay_suc_inc$dwd_traffic_action_inc$dwd_traffic_display_inc$dwd_traffic_error_inc$dwd_traffic_page_view_inc$dwd_traffic_start_inc$dwd_user_login_inc$dwd_user_register_inc"
esac