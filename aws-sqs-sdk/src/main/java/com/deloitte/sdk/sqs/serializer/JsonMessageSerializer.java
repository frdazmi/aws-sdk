package com.deloitte.sdk.sqs.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonMessageSerializer implements MessageSerializer {

    private final ObjectMapper objectMapper;

    public JsonMessageSerializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String serialize(Object message) throws Exception {
        return objectMapper.writeValueAsString(message);
    }
}
