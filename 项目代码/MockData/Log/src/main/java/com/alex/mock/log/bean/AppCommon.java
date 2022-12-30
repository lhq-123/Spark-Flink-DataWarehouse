package com.alex.mock.log.bean;

import com.  alex.mock.log.config.AppConfig;
import com.  alex.mock.db.util.RanOpt;
import com.  alex.mock.db.util.RandomNum;
import com.  alex.mock.db.util.RandomOptionGroup;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder(builderClassName = "Builder")
public class AppCommon {

    private String mid; // (String) 设备唯一标识
    private String uid; // (String) 用户uid
    private String vc;  // (String) versionCode，程序版本号
    private String ch;  // (String) 渠道号，应用从哪个渠道来的。
    private String os;  // (String) 系统版本
    private String ar;  // (String) 区域
    private String md;  // (String) 手机型号
    private String ba;  // (String) 手机品牌


    public  static AppCommon build(   ){
        String mid; // (String) 设备唯一标识
        String uid; // (String) 用户uid
        String vc;  // (String) versionCode，程序版本号
        String ch;  // (String) 渠道号，应用从哪个渠道来的。
        String os;  // (String) 系统版本
        String ar;  // (String) 区域
        String md;  // (String) 手机型号
        String ba;  // (String) 手机品牌


        mid="mid_"+RandomNum.getRandInt(1, AppConfig.max_mid)+"";;

        ar = new RandomOptionGroup<String>(new RanOpt<String>("110000", 30),
                new RanOpt<String>("310000", 20),
                new RanOpt<String>("230000", 10),
                new RanOpt<String>("370000", 10),
                new RanOpt<String>("420000", 5),
                new RanOpt<String>("440000", 20),
                new RanOpt<String>("500000", 5),
                new RanOpt<String>("530000", 5)
        ).getRandStringValue();

        md = new RandomOptionGroup<String>(new RanOpt<String>("Xiaomi 9", 30),
                new RanOpt<String>("Xiaomi 10 Pro ", 30),
                new RanOpt<String>("Xiaomi Mix2 ", 30),
                new RanOpt<String>("iPhone X", 20),
                new RanOpt<String>("iPhone 8", 20),
                new RanOpt<String>("iPhone 11", 5),
                new RanOpt<String>("iPhone 12", 6),
                new RanOpt<String>("iPhone 13 Pro", 7),
                new RanOpt<String>("iPhone 12 Pro", 5),
                new RanOpt<String>("iPhone Xs", 20),
                new RanOpt<String>("iPhone Xs Max", 20),
                new RanOpt<String>("Huawei P30", 10),
                new RanOpt<String>("Huawei P40", 5),
                new RanOpt<String>("Huawei Mate 30", 10),
                new RanOpt<String>("Redmi k30", 10),
                new RanOpt<String>("Honor 20s", 5),
                new RanOpt<String>("vivo iqoo3", 20),
                new RanOpt<String>("Sumsung Galaxy S20", 3)).getRandStringValue();

        ba = md.split(" ")[0];

        if(ba.equals("iPhone")){
            ch="Appstore";
            os ="iOS "+ new RandomOptionGroup<String>(new RanOpt<String>("13.3.1", 30),
                    new RanOpt<String>("13.2.9", 10),
                    new RanOpt<String>("13.2.3", 10),
                    new RanOpt<String>("12.4.1", 5)
            ).getRandStringValue();

        }else{
            ch = new RandomOptionGroup<String>(new RanOpt<String>("xiaomi", 30),
                    new RanOpt<String>("wandoujia", 10),
                    new RanOpt<String>("web", 10),
                    new RanOpt<String>("huawei", 5),
                    new RanOpt<String>("oppo", 20),
                    new RanOpt<String>("vivo", 5),
                    new RanOpt<String>("360", 5)
            ).getRandStringValue();
            os="Android "+ new RandomOptionGroup<String>(new RanOpt<String>("11.0", 70),
                    new RanOpt<String>("10.0", 20),
                    new RanOpt<String>("9.0", 5),
                    new RanOpt<String>("8.1", 5)
            ).getRandStringValue();
        }

        vc="v"+ new RandomOptionGroup<String>(new RanOpt<String>("2.1.134", 70),
                new RanOpt<String>("2.1.132", 20),
                new RanOpt<String>("2.1.111", 5),
                new RanOpt<String>("2.0.1", 5)
        ).getRandStringValue();

        uid= RandomNum.getRandInt(1,AppConfig.max_uid)+"";

        AppCommon appBase = new AppCommon(mid, uid, vc, ch, os, ar, md, ba  );
        return appBase;
    }




}
