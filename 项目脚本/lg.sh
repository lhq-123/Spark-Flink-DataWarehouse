#!/bin/bash
 echo "========== 生成用户日志数据=========="
 cd /opt/module/applog/; java -jar gmall2020-mock-log-2021-10-10.jar >/dev/null 2>&1 &
