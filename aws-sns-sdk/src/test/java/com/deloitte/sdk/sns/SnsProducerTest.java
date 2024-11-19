package com.deloitte.sdk.sns;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class SnsProducerTest {

    @Mock
    private SnsClient snsClient;

    @InjectMocks
    private SnsProducer snsProducer;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testPublishMessage() {
        String topicArn = "arn:aws:sns:us-east-1:123456789012:MyTopic";
        String message = "Test message";
        String messageId = "12345";

        PublishResponse publishResponse = PublishResponse.builder()
                .messageId(messageId)
                .build();

        when(snsClient.publish(any(PublishRequest.class))).thenReturn(publishResponse);

        String result = snsProducer.publishMessage(topicArn, message);

        assertEquals(messageId, result);
    }

    @Test
    public void testPublishMessageThrowsException() {
        String topicArn = "arn:aws:sns:us-east-1:123456789012:MyTopic";
        String message = "Test message";

        when(snsClient.publish(any(PublishRequest.class))).thenThrow(SnsException.class);
        Assertions.assertThrows(SnsException.class, () -> {
            snsProducer.publishMessage(topicArn, message);
        });
    }
}
