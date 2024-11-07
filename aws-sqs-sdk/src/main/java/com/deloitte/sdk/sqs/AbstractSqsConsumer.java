package com.deloitte.sdk.sqs;


import com.deloitte.sdk.sqs.exceptions.SkipTaskException;
import com.deloitte.sdk.sqs.handler.SqsMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.concurrent.Executor;

public abstract class AbstractSqsConsumer {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSqsConsumer.class);
    private final SqsClient sqsClient;
    private final SqsMessageHandler handler;
    private final Executor workerPool;

    public AbstractSqsConsumer(SqsClient sqsClient, SqsMessageHandler handler, Executor workerPool) {
        this.sqsClient = sqsClient;
        this.handler = handler;
        this.workerPool = workerPool;
    }

    public void pollQueueMessages() {
        Config config = getConfig();

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(config.getQueueUrl())
                .maxNumberOfMessages(config.getMaxNumberOfMessages())
                .waitTimeSeconds(config.getWaitTimeSeconds())
                .build();

        sqsClient.receiveMessage(receiveMessageRequest)
                .messages()
                .forEach(message -> workerPool.execute(() -> {
                    try {
                        handler.handleMessage(message);
                        deleteMessage(config.getQueueUrl(), message);
                    } catch (SkipTaskException e) {
                        logger.info("Skipping message: {}", message.body());
                    } catch (Exception e) {
                        logger.error("Error processing message: {}", message.body(), e);
                    }
                }));
    }

    protected abstract Config getConfig();

    private void deleteMessage(String queueUrl, Message message) {
        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build());
    }


    public static class Config {
        private final String queueUrl;
        private int maxNumberOfMessages = 10;
        private int waitTimeSeconds = 5;

        public Config(String queueUrl) {
            this.queueUrl = queueUrl;
        }

        public Config(String queueUrl, int maxNumberOfMessages, int waitTimeSeconds) {
            this.queueUrl = queueUrl;
            this.maxNumberOfMessages = maxNumberOfMessages;
            this.waitTimeSeconds = waitTimeSeconds;
        }

        public String getQueueUrl() {
            return queueUrl;
        }

        public int getMaxNumberOfMessages() {
            return maxNumberOfMessages;
        }

        public int getWaitTimeSeconds() {
            return waitTimeSeconds;
        }
    }

}
