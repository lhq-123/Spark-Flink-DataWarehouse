#! /bin/bash
DATAX_HOME=/opt/module/datax
#DataX导出路径不允许存在空文件，该函数作用为清理空文件
handle_export_path(){
  target_file=$1
  for i in `hadoop fs -ls -R $target_file | awk '{print $8}'`; do
    hadoop fs -test -z $i
    if [[ $? -eq 0 ]]; then
      echo "$i文件大小为0，正在删除"
      hadoop fs -rm -r -f $i
    fi
  done

}

#数据导出
export_data() {
  datax_config=$1
  export_dir=$2
  hadoop fs -test -e $export_dir
  if [[ $? -eq 0 ]]
  then
    handle_export_path $export_dir
    file_count=$(hadoop fs -ls $export_dir | wc -l)
    if [ $file_count -gt 0 ]
    then
      set -e;
      $DATAX_HOME/bin/datax.py -p"-Dexportdir=$export_dir" $datax_config
      set +e;
    else 
      echo "$export_dir 目录为空，跳过~"
    fi
  else
    echo "路径 $export_dir 不存在，跳过~"
  fi
}

case $1 in
  "ads_new_buyer_stats")
    export_data /opt/module/datax/job/export/gmall_report.ads_new_buyer_stats.json hdfs://Flink01:8020/spark/gmall/ads/ads_new_buyer_stats
  ;;                                                              
  "ads_order_by_province")
    export_data /opt/module/datax/job/export/gmall_report.ads_order_by_province.json hdfs://Flink01:8020/spark/gmall/ads/ads_order_by_province
  ;;
  "ads_page_path")
    export_data /opt/module/datax/job/export/gmall_report.ads_page_path.json hdfs://Flink01:8020/spark/gmall/ads/ads_page_path
  ;;
  "ads_repeat_purchase_by_tm")
    export_data /opt/module/datax/job/export/gmall_report.ads_repeat_purchase_by_tm.json hdfs://Flink01:8020/spark/gmall/ads/ads_repeat_purchase_by_tm
  ;;
  "ads_trade_stats")
    export_data /opt/module/datax/job/export/gmall_report.ads_trade_stats.json hdfs://Flink01:8020/spark/gmall/ads/ads_trade_stats
  ;;
  "ads_trade_stats_by_cate")
    export_data /opt/module/datax/job/export/gmall_report.ads_trade_stats_by_cate.json hdfs://Flink01:8020/spark/gmall/ads/ads_trade_stats_by_cate
  ;;
  "ads_trade_stats_by_tm")
    export_data /opt/module/datax/job/export/gmall_report.ads_trade_stats_by_tm.json hdfs://Flink01:8020/spark/gmall/ads/ads_trade_stats_by_tm
  ;;
  "ads_traffic_stats_by_channel")
    export_data /opt/module/datax/job/export/gmall_report.ads_traffic_stats_by_channel.json hdfs://Flink01:8020/spark/gmall/ads/ads_traffic_stats_by_channel
  ;;
  "ads_user_action")
    export_data /opt/module/datax/job/export/gmall_report.ads_user_action.json hdfs://Flink01:8020/spark/gmall/ads/ads_user_action
  ;;
  "ads_user_change")
    export_data /opt/module/datax/job/export/gmall_report.ads_user_change.json hdfs://Flink01:8020/spark/gmall/ads/ads_user_change
  ;;
  "ads_user_retention")
    export_data /opt/module/datax/job/export/gmall_report.ads_user_retention.json hdfs://Flink01:8020/spark/gmall/ads/ads_user_retention
  ;;
  "ads_user_stats")
    export_data /opt/module/datax/job/export/gmall_report.ads_user_stats.json hdfs://Flink01:8020/spark/gmall/ads/ads_user_stats
  ;;
  "ads_activity_stats")
    export_data /opt/module/datax/job/export/gmall_report.ads_activity_stats.json hdfs://Flink01:8020/spark/gmall/ads/ads_activity_stats
  ;;
  "ads_coupon_stats")
    export_data /opt/module/datax/job/export/gmall_report.ads_coupon_stats.json hdfs://Flink01:8020/spark/gmall/ads/ads_coupon_stats
  ;;
  "ads_sku_cart_num_top3_by_cate")
    export_data /opt/module/datax/job/export/gmall_report.ads_sku_cart_num_top3_by_cate.json hdfs://Flink01:8020/spark/gmall/ads/ads_sku_cart_num_top3_by_cate
  ;;

"all")
  export_data /opt/module/datax/job/export/gmall_report.ads_new_buyer_stats.json hdfs://Flink01:8020/spark/gmall/ads/ads_new_buyer_stats
  export_data /opt/module/datax/job/export/gmall_report.ads_order_by_province.json hdfs://Flink01:8020/spark/gmall/ads/ads_order_by_province
  export_data /opt/module/datax/job/export/gmall_report.ads_page_path.json hdfs://Flink01:8020/spark/gmall/ads/ads_page_path
  export_data /opt/module/datax/job/export/gmall_report.ads_repeat_purchase_by_tm.json hdfs://Flink01:8020/spark/gmall/ads/ads_repeat_purchase_by_tm
  export_data /opt/module/datax/job/export/gmall_report.ads_trade_stats.json hdfs://Flink01:8020/spark/gmall/ads/ads_trade_stats
  export_data /opt/module/datax/job/export/gmall_report.ads_trade_stats_by_cate.json hdfs://Flink01:8020/spark/gmall/ads/ads_trade_stats_by_cate
  export_data /opt/module/datax/job/export/gmall_report.ads_trade_stats_by_tm.json hdfs://Flink01:8020/spark/gmall/ads/ads_trade_stats_by_tm
  export_data /opt/module/datax/job/export/gmall_report.ads_traffic_stats_by_channel.json hdfs://Flink01:8020/spark/gmall/ads/ads_traffic_stats_by_channel
  export_data /opt/module/datax/job/export/gmall_report.ads_user_action.json hdfs://Flink01:8020/spark/gmall/ads/ads_user_action
  export_data /opt/module/datax/job/export/gmall_report.ads_user_change.json hdfs://Flink01:8020/spark/gmall/ads/ads_user_change
  export_data /opt/module/datax/job/export/gmall_report.ads_user_retention.json hdfs://Flink01:8020/spark/gmall/ads/ads_user_retention
  export_data /opt/module/datax/job/export/gmall_report.ads_user_stats.json hdfs://Flink01:8020/spark/gmall/ads/ads_user_stats
  export_data /opt/module/datax/job/export/gmall_report.ads_activity_stats.json hdfs://Flink01:8020/spark/gmall/ads/ads_activity_stats
  export_data /opt/module/datax/job/export/gmall_report.ads_coupon_stats.json hdfs://Flink01:8020/spark/gmall/ads/ads_coupon_stats
  export_data /opt/module/datax/job/export/gmall_report.ads_sku_cart_num_top3_by_cate.json hdfs://Flink01:8020/spark/gmall/ads/ads_sku_cart_num_top3_by_cate
  ;;
esac