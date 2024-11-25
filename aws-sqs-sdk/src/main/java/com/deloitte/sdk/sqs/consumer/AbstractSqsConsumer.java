package com.deloitte.sdk.sqs.consumer;


import com.deloitte.sdk.sqs.exceptions.SkipTaskException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.lang.reflect.ParameterizedType;
import java.util.concurrent.Executor;


/**
 * Abstract class for consuming messages from an Amazon SQS queue.
 * Subclasses must implement the {@link #getConfig()} and {@link #handleMessage(T)} methods.
 * subclasses need to call the {@link #pollQueueMessages()} method to start polling the SQS queue.
 * You can use the @Scheduled annotation to schedule this method to run at fixed intervals.
 */
public abstract class AbstractSqsConsumer<T> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSqsConsumer.class);
    private final SqsClient sqsClient;
    private final Executor workerPool;

    public AbstractSqsConsumer(SqsClient sqsClient, Executor workerPool) {
        this.sqsClient = sqsClient;
        this.workerPool = workerPool;
    }

    protected void pollQueueMessages() {
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
                        handleMessage(convertIntoObject(message));
                        deleteMessage(config.getQueueUrl(), message);
                    } catch (SkipTaskException e) {
                        logger.info("Skipping message: {}", message.body());
                        deleteMessage(config.getQueueUrl(), message);
                    } catch (Exception e) {
                        logger.error("Error processing message: {}", message.body(), e);
                    }
                }));
    }

    protected abstract Config getConfig();

    protected abstract void handleMessage(T message) throws Exception;

    private void deleteMessage(String queueUrl, Message message) {
        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build());
    }

    private T convertIntoObject(Message message) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(message.body(), getGenericType());
        } catch (Exception e) {
            logger.error("Error converting message to object: {}", message.body(), e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Class<T> getGenericType() {
        return (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    @Builder
    @Getter
    public static class Config {
        private final String queueUrl;
        @Builder.Default
        private int maxNumberOfMessages = 10;
        @Builder.Default
        private int waitTimeSeconds = 5;

        public Config(String queueUrl) {
            this.queueUrl = queueUrl;
        }

        public Config(String queueUrl, int maxNumberOfMessages, int waitTimeSeconds) {
            this.queueUrl = queueUrl;
            this.maxNumberOfMessages = maxNumberOfMessages;
            this.waitTimeSeconds = waitTimeSeconds;
        }
    }

}
