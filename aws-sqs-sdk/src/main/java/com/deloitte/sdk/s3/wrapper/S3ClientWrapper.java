package com.deloitte.sdk.s3.wrapper;

import com.deloitte.sdk.s3.exceptions.S3SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class S3ClientWrapper {

    private final S3Client s3Client;

    public S3ClientWrapper(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public void createBucket(String bucketName) throws S3SdkException {
        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        } catch (S3Exception e) {
            throw new S3SdkException("Failed to create bucket: " + bucketName, e);
        }
    }

    public List<String> listBuckets() throws S3SdkException {
        try {
            ListBucketsResponse response = s3Client.listBuckets();
            return response.buckets().stream().map(Bucket::name).collect(Collectors.toList());
        } catch (S3Exception e) {
            throw new S3SdkException("Failed to list buckets", e);
        }
    }

    public void deleteBucket(String bucketName) throws S3SdkException {
        try {
            s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
        } catch (S3Exception e) {
            throw new S3SdkException("Failed to delete bucket: " + bucketName, e);
        }
    }

    // Object Operations

    public void uploadObject(String bucketName, String key, Path filePath) throws S3SdkException {
        try {
            s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(), filePath);
        } catch (S3Exception e) {
            throw new S3SdkException("Failed to upload object to bucket: " + bucketName, e);
        }
    }

    public InputStream downloadObject(String bucketName, String key) throws S3SdkException {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
            return s3Client.getObject(getObjectRequest);
        } catch (S3Exception e) {
            throw new S3SdkException("Failed to download object from bucket: " + bucketName, e);
        }
    }

    public void deleteObject(String bucketName, String key) throws S3SdkException {
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(key).build());
        } catch (S3Exception e) {
            throw new S3SdkException("Failed to delete object from bucket: " + bucketName, e);
        }
    }

    public List<String> listObjects(String bucketName) throws S3SdkException {
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName).build();
            ListObjectsV2Response response = s3Client.listObjectsV2(request);
            return response.contents().stream().map(S3Object::key).collect(Collectors.toList());
        } catch (S3Exception e) {
            throw new S3SdkException("Failed to list objects in bucket: " + bucketName, e);
        }
    }
}
