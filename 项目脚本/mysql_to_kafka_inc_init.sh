#!/bin/bash

# 该脚本的作用是初始化所有的增量表，只需执行一次

MAXWELL_HOME=/opt/module/maxwell

import_data() {
    $MAXWELL_HOME/bin/maxwell-bootstrap --database gmall --table $1 --config $MAXWELL_HOME/config.properties
}

case $1 in
"cart_info")
  import_data cart_info
  ;;
"comment_info")
  import_data comment_info
  ;;
"coupon_use")
  import_data coupon_use
  ;;
"favor_info")
  import_data favor_info
  ;;
"order_detail")
  import_data order_detail
  ;;
"order_detail_activity")
  import_data order_detail_activity
  ;;
"order_detail_coupon")
  import_data order_detail_coupon
  ;;
"order_info")
  import_data order_info
  ;;
"order_refund_info")
  import_data order_refund_info
  ;;
"order_status_log")
  import_data order_status_log
  ;;
"payment_info")
  import_data payment_info
  ;;
"refund_payment")
  import_data refund_payment
  ;;
"user_info")
  import_data user_info
  ;;
"all")
  import_data cart_info
  import_data comment_info
  import_data coupon_use
  import_data favor_info
  import_data order_detail
  import_data order_detail_activity
  import_data order_detail_coupon
  import_data order_info
  import_data order_refund_info
  import_data order_status_log
  import_data payment_info
  import_data refund_payment
  import_data user_info
  ;;
esac
