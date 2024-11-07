package com.deloitte.sdk.sqs.handler;

import software.amazon.awssdk.services.sqs.model.Message;

public interface SqsMessageHandler {
    void handleMessage(Message message);
}
