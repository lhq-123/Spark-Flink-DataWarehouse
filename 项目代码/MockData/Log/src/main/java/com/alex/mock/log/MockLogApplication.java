package com.alex.mock.log;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class MockLogApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MockLogApplication.class, args);
        MockTask mockTask = context.getBean(MockTask.class);

        mockTask.mainTask();
    }
}
