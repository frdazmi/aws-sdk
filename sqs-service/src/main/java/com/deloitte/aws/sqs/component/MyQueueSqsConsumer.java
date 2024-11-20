package com.deloitte.aws.sqs.component;

import com.deloitte.aws.sqs.service.MyQueueProducerService;
import com.deloitte.sdk.sqs.consumer.AbstractSqsConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.time.Instant;
import java.util.concurrent.Executor;

@Component
public class MyQueueSqsConsumer extends AbstractSqsConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MyQueueSqsConsumer.class);

    @Value("${my-queue-sqs.url}")
    private String queueUrl;
    @Value("${my-queue-sqs.consumer.maxNumberOfMessages}")
    private Integer maxNumberOfMessages;
    @Value("${my-queue-sqs.consumer.waitTimeSeconds}")
    private Integer waitTimeSeconds;

    private static String MESSAGE_PREFIX = "Message sent at ";

    @Autowired
    private MyQueueProducerService myQueueProducerService;

    @Autowired
    public MyQueueSqsConsumer(SqsClient sqsClient,
                              @Qualifier("sqsWorkerPool") Executor workerPool) {
        super(sqsClient, workerPool);
    }

    @Override
    protected Config getConfig() {
        return AbstractSqsConsumer.Config.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxNumberOfMessages)
                .waitTimeSeconds(waitTimeSeconds)
                .build();
    }

    @Scheduled(fixedRateString = "${my-queue-sqs.consumer.pollingInterval}")
    public void pollQueueMessages() {
        super.pollQueueMessages();
    }

    @Override
    protected void handleMessage(Message message) {
        logger.info("Received message at {} : {}", Instant.now(), message.body());
        logger.info("Sending message to another queue");
        myQueueProducerService.sendMessage(MESSAGE_PREFIX + Instant.now());
    }
}
