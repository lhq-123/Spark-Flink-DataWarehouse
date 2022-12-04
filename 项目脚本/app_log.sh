#!/bin/bash
 for i in Flink01 Flink02; do
 echo "========== $i 开始生成用户日志数据 =========="
 ssh $i "cd /opt/module/data/; java -jar app_log.jar >/dev/null 2>&1 &"
 done



