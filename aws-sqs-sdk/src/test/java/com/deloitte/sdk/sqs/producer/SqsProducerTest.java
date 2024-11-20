package com.deloitte.sdk.sqs.producer;

import com.deloitte.sdk.sqs.exceptions.SqsProducerException;
import com.deloitte.sdk.sqs.serializer.MessageSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SqsProducerTest {
    @Mock
    private SqsClient sqsClient;

    @Mock
    private MessageSerializer serializer;

    @InjectMocks
    private SqsProducer sqsProducer;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testSendMessage_Success() throws Exception {
        String queueUrl = "test-queue-url";
        Object message = new Object();
        String serializedMessage = "serialized-message";

        when(serializer.serialize(message)).thenReturn(serializedMessage);

        sqsProducer.sendMessage(queueUrl, message);

        ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient).sendMessage(requestCaptor.capture());

        SendMessageRequest capturedRequest = requestCaptor.getValue();
        assertEquals(queueUrl, capturedRequest.queueUrl());
        assertEquals(serializedMessage, capturedRequest.messageBody());
    }

    @Test
    public void testSendMessage_SerializationFailure() throws Exception {
        String queueUrl = "test-queue-url";
        Object message = new Object();

        when(serializer.serialize(message)).thenThrow(new RuntimeException("Serialization failed"));

        assertThrows(SqsProducerException.class, () -> sqsProducer.sendMessage(queueUrl, message));
    }

    @Test
    public void testSendMessage_SqsClientFailure() throws Exception {
        String queueUrl = "test-queue-url";
        Object message = new Object();
        String serializedMessage = "serialized-message";

        when(serializer.serialize(message)).thenReturn(serializedMessage);
        doThrow(new RuntimeException("SQS client failure")).when(sqsClient).sendMessage(any(SendMessageRequest.class));

        assertThrows(SqsProducerException.class, () -> sqsProducer.sendMessage(queueUrl, message));
    }
}
