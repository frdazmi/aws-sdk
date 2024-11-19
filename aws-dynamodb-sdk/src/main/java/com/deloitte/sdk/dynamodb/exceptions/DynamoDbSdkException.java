package com.deloitte.sdk.dynamodb.exceptions;

public class DynamoDbSdkException extends Exception {

        public DynamoDbSdkException(String message, Throwable cause) {
            super(message, cause);
        }

        public DynamoDbSdkException(String message) {
            super(message);
        }
}
