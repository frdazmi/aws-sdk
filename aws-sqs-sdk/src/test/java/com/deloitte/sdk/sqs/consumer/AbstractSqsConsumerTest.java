package com.deloitte.sdk.sqs.consumer;

import com.deloitte.sdk.sqs.exceptions.SkipTaskException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractSqsConsumerTest {
    private SqsClient sqsClient;
    private AbstractSqsConsumer sqsConsumer;

    @BeforeEach
    void setUp() {
        sqsClient = mock(SqsClient.class);
        sqsConsumer = new TestSqsConsumer(sqsClient, Executors.newSingleThreadExecutor());
    }

    @Test
    void testPollQueueMessages() {
        Message message = Message.builder().body("test message").receiptHandle("testHandle").build();
        ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(message).build();

        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

        sqsConsumer.pollQueueMessages();

        verify(sqsClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
        verify(sqsClient, times(1)).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void testPollQueueSkipMessages() {
        Message message = Message.builder().body("skip").receiptHandle("testHandle").build();
        ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(message).build();

        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

        sqsConsumer.pollQueueMessages();

        verify(sqsClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    void testPollQueueExceptionMessages() {
        Message message = Message.builder().body("exception").receiptHandle("testHandle").build();
        ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(message).build();

        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

        sqsConsumer.pollQueueMessages();

        verify(sqsClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
        verify(sqsClient, times(0)).deleteMessage(any(DeleteMessageRequest.class));
    }

    private static class TestSqsConsumer extends AbstractSqsConsumer {

        public TestSqsConsumer(SqsClient sqsClient, Executor workerPool) {
            super(sqsClient, workerPool);
        }

        @Override
        protected Config getConfig() {
            return Config.builder()
                    .queueUrl("testQueueUrl")
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(1)
                    .build();
        }

        @Override
        protected void handleMessage(Message message) throws Exception {
            if (message.body().equalsIgnoreCase("exception")) {
                throw new Exception("Test exception");
            } else if (message.body().equalsIgnoreCase("skip")) {
                throw new SkipTaskException("Test skip");
            }
        }
    }
}
