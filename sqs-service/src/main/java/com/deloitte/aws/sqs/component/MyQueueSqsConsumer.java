package com.deloitte.aws.sqs.component;

import com.deloitte.sdk.sqs.consumer.AbstractSqsConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.concurrent.Executor;

@Component
public class MyQueueSqsConsumer extends AbstractSqsConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MyQueueSqsConsumer.class);

    @Value("${my-queue-sqs-consumer.url}")
    private String queueUrl;
    @Value("${my-queue-sqs-consumer.maxNumberOfMessages}")
    private Integer maxNumberOfMessages;
    @Value("${my-queue-sqs-consumer.waitTimeSeconds}")
    private Integer waitTimeSeconds;

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

    @Override
    protected void handleMessage(Message message) {
        logger.info("Received message: {}", message.body());
    }
}
