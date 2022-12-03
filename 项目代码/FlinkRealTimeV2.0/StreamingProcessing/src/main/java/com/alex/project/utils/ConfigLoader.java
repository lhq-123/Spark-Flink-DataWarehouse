package com.alex.project.utils;

/**
 * @author liu
 * @Create 2022-11-19
 * @Description 读取resource目录下的配置文件conf.properties形成键值对
 *
 *  ClassLoader(类加载器)，加载conf.properties
 *  通过Properties的load方法加载InputStream
 *  编写方法实现获得string的key值
 *  编写方法实现获得int的key值
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ConfigLoader {

    private final static Logger logger = LoggerFactory.getLogger(ConfigLoader.class);
    //TODO 1）使用classLoader（类加载器），加载conf.properties
    private final static InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream("conf.properties");

    //定义properties
    private  final static Properties props = new Properties();

    //TODO 2）使用Properties的load方法加载inputStream
    static {
        try {
            //加载inputStream -> conf.properties
            props.load(inputStream);
        } catch (IOException e) {
            logger.error("导入配置文件错误："+ e.getMessage());
        }
    }

    //TODO 3）编写方法获取配置项的key对应的值
    public static String get(String key) {
        return  props.getProperty(key);
    }

    //TODO 4）编写方法获取int的key值
    public static int getInt(String key){
        //将获取到的value值转换成int类型返回
        return  Integer.parseInt(props.getProperty(key));
    }
}
