package com.alex.mock.log.bean;

import com.alibaba.fastjson.JSON;
import com.  alex.mock.log.config.AppConfig;
import com.  alex.mock.db.util.RandomOptionGroup;
import lombok.Builder;
import lombok.Data;


import java.util.List;

@Data
@Builder
public class AppMain {


    private Long ts;   // (String) 客户端日志产生时的时间

    private AppCommon common;

    private AppPage page;

    private AppError err;

    private AppNotice notice;

    private AppStart start;

    private List<AppDisplay> displays;
    private List<AppAction>  actions;

    @Override
    public String toString(){
         return  JSON.toJSONString(this);
    }


   public  static class AppMainBuilder {



       public void checkError(){
           Integer errorRate = AppConfig.error_rate;
           Boolean ifError = RandomOptionGroup.builder().add(true, errorRate).add(false, 100 - errorRate).build().getRandBoolValue();
           if(ifError){
               AppError appError = AppError.build();
               this.err=appError;
           }

       }


    }


}
