package com.deloitte.aws.sqs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.deloitte.aws", "com.deloitte.sdk"})
@EnableScheduling
public class DeloitteSqsService {

    public static void main(String[] args) {
        SpringApplication.run(DeloitteSqsService.class, args);
    }

}
