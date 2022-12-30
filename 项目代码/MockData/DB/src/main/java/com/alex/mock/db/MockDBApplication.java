package com.alex.mock.db;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
@MapperScan("com.alex.mock.db.mapper")
public class MockDBApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MockDBApplication.class, args);

        MockTask mockTask = context.getBean(MockTask.class);

        mockTask.mainTask();
    }
}
