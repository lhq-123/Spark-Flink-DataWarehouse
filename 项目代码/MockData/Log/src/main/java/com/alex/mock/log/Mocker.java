package com.alex.mock.log;

import com.alex.mock.log.bean.*;
import com.alex.mock.log.config.AppConfig;
import com.alex.mock.log.enums.PageId;
import com.alex.mock.log.util.HttpUtil;
import com.alex.mock.log.util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alex.mock.db.util.ConfigUtil;
import com.alex.mock.db.util.RandomNum;
import com.alex.mock.db.util.RandomOptionGroup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.EnumUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class Mocker   {
    private Long ts;



    public  List<AppMain> doAppMock(){

        List<AppMain> logList=new ArrayList<>();
       ts=  AppConfig.date.getTime();

       // 启动
        AppMain.AppMainBuilder appMainBuilder = AppMain.builder();
        AppCommon appCommon =   AppCommon.build() ;
        appMainBuilder.common(appCommon);

        appMainBuilder.checkError();
       AppStart appStart = new AppStart.Builder().build();
        appMainBuilder.start(appStart);
        appMainBuilder.ts(ts);

       logList.add(appMainBuilder.build());

       // 读取配置
       String jsonFile = ConfigUtil.loadJsonFile("path.json");
       List<Map> pathList = JSON.parseArray(jsonFile, Map.class);

       RandomOptionGroup.Builder<List> builder =   RandomOptionGroup.builder();

       //抽取一个访问路径
       for (Map map : pathList) {
           List path = (List) map.get("path");
           Integer rate = (Integer) map.get("rate");
           builder.add(path,rate);
       }
       List chosenPath = builder.build().getRandomOpt().getValue();
        ts+=appStart.getLoading_time() ;
       //逐个输入日志
        // 每条日志  1  主行为  2 曝光  3 错误
       PageId lastPageId=null;
       for (Object o : chosenPath) {
           AppMain.AppMainBuilder pageBuilder = AppMain.builder().common(appCommon);

           String path = (String) o;

           int pageDuringTime = RandomNum.getRandInt(1000, AppConfig.page_during_max_ms);
           //添加页面
           PageId pageId = EnumUtils.getEnum(PageId.class, path);
           AppPage page =   AppPage.build (pageId,lastPageId,pageDuringTime) ;
           pageBuilder.page(page);
           //置入上一个页面
           lastPageId=page.getPage_id();

           //页面中的动作
           List<AppAction> appActionList =   AppAction.buildList (page,ts,pageDuringTime) ;
           if(appActionList.size()>0){
               pageBuilder.actions(appActionList);
           }

           List<AppDisplay> displayList = AppDisplay.buildList(page);
           if(displayList.size()>0){
               pageBuilder.displays(displayList);
           }
           pageBuilder.ts(ts);
           pageBuilder.checkError();
           logList.add(pageBuilder.build());
           ts+= pageDuringTime ;
       }

       //  随机发送通知日志
    //   System.out.println(logList);

        return logList;
   }

    public static void main(String[] args) {
       // System.out.println(RandomStringUtils.random(16,true,true));
        new Mocker( ).doAppMock();
    }


    public void run() {
        List<AppMain> appMainList = doAppMock();

        for (AppMain appMain : appMainList) {
            if(AppConfig.mock_type.equals("log")){
                log.info(appMain.toString());
            }else if(AppConfig.mock_type.equals("http")){
                HttpUtil.post(appMain.toString());
            }else if(AppConfig.mock_type.equals("kafka"))
                KafkaUtil.send(AppConfig.kafka_topic, appMain.toString());
            try {
                Thread.sleep(AppConfig.log_sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
