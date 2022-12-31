FullLinkDataWarehouse 全链路数仓源码,包括离线和实时

MockData 数据模拟器源码

代码结构

FullLinkDataWarehouse:

```properties
├─Flume_Interceptor      #自定义flume拦截器(java)
├─OfflineAnalysis        #离线处理代码(sql+shell)
├─StreamingProcessing    #实时处理逻辑(java)
└─Web
```

MockData:

```properties
├─Common                 #模拟数据时一些通用的实体类(java)
├─DB                     #模拟业务数据(java)
├─Log                    #模拟用户日志数据(java)
```

