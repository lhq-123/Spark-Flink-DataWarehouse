#!/bin/bash
case $1 in
"start"){
        echo "Flink03 启动 sparksql"
        ssh Flink03 "/opt/module/spark/sbin/start-thriftserver.sh --name sparksql-job --master yarn --deploy-mode client --driver-memory 1g --hiveconf hive.server2.thrift.http.port=10000 --num-executors 1 --executor-memory 1g --conf spark.sql.shuffle.partitions=10"
};;
"stop"){
        echo "Flink03 关闭 sparksql"
        ssh Flink03 "/opt/module/spark/sbin/stop-thriftserver.sh"
};;
esac

