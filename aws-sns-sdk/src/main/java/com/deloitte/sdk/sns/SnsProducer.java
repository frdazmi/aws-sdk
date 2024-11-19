package com.deloitte.sdk.sns;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

public class SnsProducer {

    private static final Logger logger = LoggerFactory.getLogger(SnsProducer.class);
    private final SnsClient snsClient;

    public SnsProducer(SnsClient snsClient) {
        this.snsClient = snsClient;
    }

    public String publishMessage(String topicArn, String message) throws SnsException {
        PublishRequest request = PublishRequest.builder()
                .topicArn(topicArn)
                .message(message)
                .build();

        PublishResponse response = snsClient.publish(request);
        return response.messageId();
    }
}
