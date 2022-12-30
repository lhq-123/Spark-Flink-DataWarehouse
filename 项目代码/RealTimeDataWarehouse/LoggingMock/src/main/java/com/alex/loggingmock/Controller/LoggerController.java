package com.alex.loggingmock.Controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Alex_liu
 * @create 2022-11-28 14:32
 * @Description
 */
//@RestController = @Controller+@ResponseBody
@RestController //表示返回普通对象而不是页面
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
        //落盘
        log.info(jsonStr);
        //写入 Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }
}