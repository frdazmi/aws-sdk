package com.deloitte.sdk.sqs.exceptions;

public class SkipTaskException extends RuntimeException {
    public SkipTaskException(String message) {
        super(message);
    }
}
