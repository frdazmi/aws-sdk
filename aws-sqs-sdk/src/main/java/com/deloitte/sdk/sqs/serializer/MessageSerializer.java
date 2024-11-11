package com.deloitte.sdk.sqs.serializer;

public interface MessageSerializer {

    /**
     * Serializes the given object into a string for sending to SQS.
     *
     * @param message The message object to serialize.
     * @return The serialized message as a string.
     * @throws Exception If serialization fails.
     */
    String serialize(Object message) throws Exception;
}
