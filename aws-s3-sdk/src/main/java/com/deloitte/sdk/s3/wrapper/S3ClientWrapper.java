package com.deloitte.sdk.s3.wrapper;

import com.deloitte.sdk.s3.exceptions.S3SdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class S3ClientWrapper {

    private static final Logger logger = LoggerFactory.getLogger(S3ClientWrapper.class);

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;

    public S3ClientWrapper(S3Client s3Client, S3Presigner s3Presigner) {
        this.s3Client = s3Client;
        this.s3Presigner = s3Presigner;
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

    public void copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws S3SdkException {
        try {
            CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                    .sourceBucket(sourceBucketName)
                    .sourceKey(sourceKey)
                    .destinationBucket(destinationBucketName)
                    .destinationKey(destinationKey)
                    .build();

            s3Client.copyObject(copyRequest);
            logger.info("Object copied successfully");
        } catch (S3Exception e) {
            throw new S3SdkException("Failed to copy object", e);
        }
    }

    // Presigned URL Methods

    public URL generatePresignedUrlForDownload(String bucketName, String key, Duration expiration) throws S3SdkException {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(expiration)
                    .getObjectRequest(getObjectRequest)
                    .build();

            PresignedGetObjectRequest presignedGetObjectRequest = s3Presigner.presignGetObject(presignRequest);

            return presignedGetObjectRequest.url();
        } catch (Exception e) {
            throw new S3SdkException("Failed to generate presigned URL for download", e);
        }
    }

    public URL generatePresignedUrlForUpload(String bucketName, String key, Duration expiration) throws S3SdkException {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(expiration)
                    .putObjectRequest(putObjectRequest)
                    .build();

            PresignedPutObjectRequest presignedPutObjectRequest = s3Presigner.presignPutObject(presignRequest);

            return presignedPutObjectRequest.url();
        } catch (Exception e) {
            throw new S3SdkException("Failed to generate presigned URL for upload", e);
        }
    }
}
