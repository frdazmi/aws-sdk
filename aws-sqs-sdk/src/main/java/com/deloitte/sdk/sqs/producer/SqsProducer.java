package com.deloitte.sdk.sqs.producer;

import com.deloitte.sdk.sqs.exceptions.SqsProducerException;
import com.deloitte.sdk.sqs.serializer.MessageSerializer;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsProducer {
    private final SqsClient sqsClient;
    private final MessageSerializer serializer;

    /**
     * Constructs an SqsProducer with the given SQS client and message serializer.
     *
     * @param sqsClient  The SqsClient instance to use for sending messages.
     * @param serializer The MessageSerializer to serialize message bodies.
     */
    public SqsProducer(SqsClient sqsClient, MessageSerializer serializer) {
        this.sqsClient = sqsClient;
        this.serializer = serializer;
    }

    /**
     * Sends a message to the specified SQS queue.
     *
     * @param queueUrl The URL of the SQS queue.
     * @param message  The message object to send.
     * @throws SqsProducerException If an error occurs while sending the message.
     */
    public void sendMessage(String queueUrl, Object message) throws SqsProducerException {
        try {
            String messageBody = serializer.serialize(message);
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .build();

            sqsClient.sendMessage(sendMessageRequest);
        } catch (Exception e) {
            throw new SqsProducerException("Failed to send message to SQS queue", e);
        }
    }
}
