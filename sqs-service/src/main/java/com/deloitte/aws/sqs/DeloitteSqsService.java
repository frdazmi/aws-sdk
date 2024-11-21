package com.deloitte.aws.sqs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DeloitteSqsService {

    public static void main(String[] args) {
        SpringApplication.run(DeloitteSqsService.class, args);
    }

}
