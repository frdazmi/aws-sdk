package com.deloitte.sdk.s3.exceptions;

public class S3SdkException extends Exception {

    public S3SdkException(String message, Throwable cause) {
        super(message, cause);
    }

    public S3SdkException(String message) {
        super(message);
    }
}
