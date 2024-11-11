package com.deloitte.aws.sqs.service;

import com.deloitte.sdk.sqs.exceptions.SqsProducerException;
import com.deloitte.sdk.sqs.producer.SqsProducer;
import com.deloitte.sdk.sqs.serializer.JsonMessageSerializer;
import com.deloitte.sdk.sqs.serializer.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;

@Service
public class MyQueueProducerService {

    private static final Logger logger = LoggerFactory.getLogger(MyQueueProducerService.class);
    private final SqsProducer sqsProducer;

    @Value("${my-queue-sqs.url}")
    private String queueUrl;

    public MyQueueProducerService(SqsClient sqsClient) {
        MessageSerializer serializer = new JsonMessageSerializer();
        this.sqsProducer = new SqsProducer(sqsClient, serializer);
    }

    public void sendMessage(Object message) {
        try {
            sqsProducer.sendMessage(queueUrl, message);
            logger.info("Message sent successfully");
        } catch (SqsProducerException e) {
            logger.error("Failed to send message", e);
            // Handle exception (e.g., logging, retry logic)
        }
    }
}
