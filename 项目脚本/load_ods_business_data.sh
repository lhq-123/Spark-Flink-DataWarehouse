#!/bin/bash

#传入的第一个参数是表名或者是all,第二个参数是时间,没有就是当前时间的前一天
if [ -n "$2" ] ;then
   do_date=$2
else
   do_date=`date -d '-1 day' +%F`
fi

load_data(){
    sql=""
    for i in $*; do
        #判断路径是否存在
        hadoop fs -test -e hdfs://Flink01:8020/origin_data/gmall/db/${i:4}/$do_date
        #路径存在方可装载数据
        if [[ $? = 0 ]]; then
            sql=$sql"load data inpath 'hdfs://Flink01:8020/origin_data/gmall/db/${i:4}/$do_date' OVERWRITE into table ods.$i partition(dt='$do_date');"
        fi
    done
    hive -e "$sql"
}

case $1 in
    "ods_activity_info_full")
        load_data "ods_activity_info_full"
    ;;
    "ods_activity_rule_full")
        load_data "ods_activity_rule_full"
    ;;
    "ods_base_category1_full")
        load_data "ods_base_category1_full"
    ;;
    "ods_base_category2_full")
        load_data "ods_base_category2_full"
    ;;
    "ods_base_category3_full")
        load_data "ods_base_category3_full"
    ;;
    "ods_base_dic_full")
        load_data "ods_base_dic_full"
    ;;
    "ods_base_province_full")
        load_data "ods_base_province_full"
    ;;
    "ods_base_region_full")
        load_data "ods_base_region_full"
    ;;
    "ods_base_trademark_full")
        load_data "ods_base_trademark_full"
    ;;
    "ods_cart_info_full")
        load_data "ods_cart_info_full"
    ;;
    "ods_coupon_info_full")
        load_data "ods_coupon_info_full"
    ;;
    "ods_sku_attr_value_full")
        load_data "ods_sku_attr_value_full"
    ;;
    "ods_sku_info_full")
        load_data "ods_sku_info_full"
    ;;
    "ods_sku_sale_attr_value_full")
        load_data "ods_sku_sale_attr_value_full"
    ;;
    "ods_spu_info_full")
        load_data "ods_spu_info_full"
    ;;

    "ods_cart_info_inc")
        load_data "ods_cart_info_inc"
    ;;
    "ods_comment_info_inc")
        load_data "ods_comment_info_inc"
    ;;
    "ods_coupon_use_inc")
        load_data "ods_coupon_use_inc"
    ;;
    "ods_favor_info_inc")
        load_data "ods_favor_info_inc"
    ;;
    "ods_order_detail_inc")
        load_data "ods_order_detail_inc"
    ;;
    "ods_order_detail_activity_inc")
        load_data "ods_order_detail_activity_inc"
    ;;
    "ods_order_detail_coupon_inc")
        load_data "ods_order_detail_coupon_inc"
    ;;
    "ods_order_info_inc")
        load_data "ods_order_info_inc"
    ;;
    "ods_order_refund_info_inc")
        load_data "ods_order_refund_info_inc"
    ;;
    "ods_order_status_log_inc")
        load_data "ods_order_status_log_inc"
    ;;
    "ods_payment_info_inc")
        load_data "ods_payment_info_inc"
    ;;
    "ods_refund_payment_inc")
        load_data "ods_refund_payment_inc"
    ;;
    "ods_user_info_inc")
        load_data "ods_user_info_inc"
    ;;
    "all")
        load_data "ods_activity_info_full" "ods_activity_rule_full" "ods_base_category1_full" "ods_base_category2_full" "ods_base_category3_full" "ods_base_dic_full" "ods_base_province_full" "ods_base_region_full" "ods_base_trademark_full" "ods_cart_info_full" "ods_coupon_info_full" "ods_sku_attr_value_full" "ods_sku_info_full" "ods_sku_sale_attr_value_full" "ods_spu_info_full" "ods_cart_info_inc" "ods_comment_info_inc" "ods_coupon_use_inc" "ods_favor_info_inc" "ods_order_detail_inc" "ods_order_detail_activity_inc" "ods_order_detail_coupon_inc" "ods_order_info_inc" "ods_order_refund_info_inc" "ods_order_status_log_inc" "ods_payment_info_inc" "ods_refund_payment_inc" "ods_user_info_inc"
    ;;
esac
