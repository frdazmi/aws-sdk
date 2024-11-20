package com.deloitte.sdk.s3;

import com.deloitte.sdk.s3.exceptions.S3SdkException;
import com.deloitte.sdk.s3.wrapper.S3ClientWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3WrapperTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private S3Presigner s3Presigner;

    @InjectMocks
    private S3ClientWrapper s3ClientWrapper;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCreateBucket() throws S3SdkException {
        doReturn(CreateBucketResponse.builder().build()).when(s3Client).createBucket(any(CreateBucketRequest.class));
        s3ClientWrapper.createBucket("bucketName");
        verify(s3Client, times(1)).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    public void testCreateBucketThrowsException() {
        doThrow(S3Exception.class).when(s3Client).createBucket(any(CreateBucketRequest.class));
        assertThrows(S3SdkException.class, () -> s3ClientWrapper.createBucket("bucketName"));
    }

    @Test
    public void testListBuckets() throws S3SdkException {
        ListBucketsResponse response = ListBucketsResponse.builder().buckets(Bucket.builder().name("bucketName").build()).build();
        when(s3Client.listBuckets()).thenReturn(response);
        List<String> buckets = s3ClientWrapper.listBuckets();
        assertEquals(1, buckets.size());
        assertEquals("bucketName", buckets.getFirst());
    }

    @Test
    public void testListBucketsThrowsException() {
        when(s3Client.listBuckets()).thenThrow(S3Exception.class);
        assertThrows(S3SdkException.class, () -> s3ClientWrapper.listBuckets());
    }

    @Test
    public void testDeleteBucket() throws S3SdkException {
        doReturn(DeleteBucketResponse.builder().build()).when(s3Client).deleteBucket(any(DeleteBucketRequest.class));
        s3ClientWrapper.deleteBucket("bucketName");
        verify(s3Client, times(1)).deleteBucket(any(DeleteBucketRequest.class));
    }

    @Test
    public void testDeleteBucketThrowsException() {
        doThrow(S3Exception.class).when(s3Client).deleteBucket(any(DeleteBucketRequest.class));
        assertThrows(S3SdkException.class, () -> s3ClientWrapper.deleteBucket("bucketName"));
    }

    @Test
    public void testUploadObject() throws S3SdkException {
        doReturn(PutObjectResponse.builder().build()).when(s3Client).putObject(any(PutObjectRequest.class), any(Path.class));
        s3ClientWrapper.uploadObject("bucketName", "key", Path.of("filePath"));
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(Path.class));
    }

    @Test
    public void testUploadObjectThrowsException() {
        doThrow(S3Exception.class).when(s3Client).putObject(any(PutObjectRequest.class), any(Path.class));
        assertThrows(S3SdkException.class, () -> s3ClientWrapper.uploadObject("bucketName", "key", Path.of("filePath")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDownloadObject() throws S3SdkException {
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn((ResponseInputStream<GetObjectResponse>) mock(ResponseInputStream.class));
        s3ClientWrapper.downloadObject("bucketName", "key");
        verify(s3Client, times(1)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testDownloadObjectThrowsException() {
        when(s3Client.getObject(any(GetObjectRequest.class))).thenThrow(S3Exception.class);
        assertThrows(S3SdkException.class, () -> s3ClientWrapper.downloadObject("bucketName", "key"));
    }

    @Test
    public void testDeleteObject() throws S3SdkException {
        doReturn(DeleteObjectResponse.builder().build()).when(s3Client).deleteObject(any(DeleteObjectRequest.class));
        s3ClientWrapper.deleteObject("bucketName", "key");
        verify(s3Client, times(1)).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    public void testDeleteObjectThrowsException() {
        doThrow(S3Exception.class).when(s3Client).deleteObject(any(DeleteObjectRequest.class));
        assertThrows(S3SdkException.class, () -> s3ClientWrapper.deleteObject("bucketName", "key"));
    }

    @Test
    public void testListObjects() throws S3SdkException {
        ListObjectsV2Response response = ListObjectsV2Response.builder().contents(S3Object.builder().key("key").build()).build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);
        List<String> objects = s3ClientWrapper.listObjects("bucketName");
        assertEquals(1, objects.size());
        assertEquals("key", objects.getFirst());
    }

    @Test
    public void testListObjectsThrowsException() {
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(S3Exception.class);
        assertThrows(S3SdkException.class, () -> s3ClientWrapper.listObjects("bucketName"));
    }

    @Test
    public void testCopyObject() throws S3SdkException {
        doReturn(CopyObjectResponse.builder().build()).when(s3Client).copyObject(any(CopyObjectRequest.class));
        s3ClientWrapper.copyObject("sourceBucket", "sourceKey", "destBucket", "destKey");
        verify(s3Client, times(1)).copyObject(any(CopyObjectRequest.class));
    }

    @Test
    public void testCopyObjectThrowsException() {
        doThrow(S3Exception.class).when(s3Client).copyObject(any(CopyObjectRequest.class));
        assertThrows(S3SdkException.class, () -> s3ClientWrapper.copyObject("sourceBucket", "sourceKey", "destBucket", "destKey"));
    }

    @Test
    public void testGeneratePresignedUrlForDownload() throws S3SdkException, MalformedURLException {
        URL mockUrl = new URL("http://example.com");
        PresignedGetObjectRequest presignedGetObjectRequest = mock(PresignedGetObjectRequest.class);
        when(presignedGetObjectRequest.url()).thenReturn(mockUrl);
        when(s3Presigner.presignGetObject(any(GetObjectPresignRequest.class))).thenReturn(presignedGetObjectRequest);

        URL url = s3ClientWrapper.generatePresignedUrlForDownload("bucketName", "key", Duration.ofMinutes(10));
        assertEquals(mockUrl, url);
    }

    @Test
    public void testGeneratePresignedUrlForDownloadThrowsException() {
        when(s3Presigner.presignGetObject(any(GetObjectPresignRequest.class))).thenThrow(RuntimeException.class);
        assertThrows(S3SdkException.class, () -> s3ClientWrapper.generatePresignedUrlForDownload("bucketName", "key", Duration.ofMinutes(10)));
    }

    @Test
    public void testGeneratePresignedUrlForUpload() throws S3SdkException, MalformedURLException {
        URL mockUrl = new URL("http://example.com");
        PresignedPutObjectRequest presignedPutObjectRequest = mock(PresignedPutObjectRequest.class);
        when(presignedPutObjectRequest.url()).thenReturn(mockUrl);
        when(s3Presigner.presignPutObject(any(PutObjectPresignRequest.class))).thenReturn(presignedPutObjectRequest);

        URL url = s3ClientWrapper.generatePresignedUrlForUpload("bucketName", "key", Duration.ofMinutes(10));
        assertEquals(mockUrl, url);
    }

    @Test
    public void testGeneratePresignedUrlForUploadThrowsException() {
        when(s3Presigner.presignPutObject(any(PutObjectPresignRequest.class))).thenThrow(RuntimeException.class);
        assertThrows(S3SdkException.class, () -> s3ClientWrapper.generatePresignedUrlForUpload("bucketName", "key", Duration.ofMinutes(10)));
    }
}
