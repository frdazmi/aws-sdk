package com.deloitte.sdk.dynamodb.wrapper;

import com.deloitte.sdk.dynamodb.exceptions.DynamoDbSdkException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamoDbWrapperTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    @InjectMocks
    private DynamoDbWrapper dynamoDbWrapper;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void createTable_createsTableSuccessfully() throws DynamoDbSdkException {
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName("testTable")
                .attributeDefinitions(Collections.emptyList())
                .keySchema(Collections.emptyList())
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build())
                .build();

        dynamoDbWrapper.createTable("testTable", Collections.emptyList(), Collections.emptyList(), ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build());

        verify(dynamoDbClient).createTable(request);
    }

    @Test
    void deleteTable_deletesTableSuccessfully() throws DynamoDbSdkException {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName("testTable")
                .build();

        dynamoDbWrapper.deleteTable("testTable");

        verify(dynamoDbClient).deleteTable(request);
    }

    @Test
    void describeTable_returnsTableDescription() throws DynamoDbSdkException {
        DescribeTableResponse response = DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName("testTable").build())
                .build();
        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(response);

        TableDescription tableDescription = dynamoDbWrapper.describeTable("testTable");

        assertEquals("testTable", tableDescription.tableName());
    }

    @Test
    void listTables_returnsTableNames() throws DynamoDbSdkException {
        ListTablesResponse response = ListTablesResponse.builder()
                .tableNames("testTable")
                .build();
        when(dynamoDbClient.listTables()).thenReturn(response);

        assertEquals(Collections.singletonList("testTable"), dynamoDbWrapper.listTables());
    }

    @Test
    void putItem_putsItemSuccessfully() throws DynamoDbSdkException {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("key", AttributeValue.builder().s("value").build());
        PutItemRequest request = PutItemRequest.builder()
                .tableName("testTable")
                .item(item)
                .build();

        dynamoDbWrapper.putItem("testTable", item);

        verify(dynamoDbClient).putItem(request);
    }

    @Test
    void getItem_returnsItem() throws DynamoDbSdkException {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("key", AttributeValue.builder().s("value").build());
        GetItemResponse response = GetItemResponse.builder()
                .item(key)
                .build();
        when(dynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(response);

        assertEquals(key, dynamoDbWrapper.getItem("testTable", key));
    }

    @Test
    void updateItem_updatesItemSuccessfully() throws DynamoDbSdkException {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("key", AttributeValue.builder().s("value").build());
        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put("attribute", AttributeValueUpdate.builder().value(AttributeValue.builder().s("newValue").build()).build());
        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName("testTable")
                .key(key)
                .attributeUpdates(updates)
                .build();

        dynamoDbWrapper.updateItem("testTable", key, updates);

        verify(dynamoDbClient).updateItem(request);
    }

    @Test
    void deleteItem_deletesItemSuccessfully() throws DynamoDbSdkException {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("key", AttributeValue.builder().s("value").build());
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName("testTable")
                .key(key)
                .build();

        dynamoDbWrapper.deleteItem("testTable", key);

        verify(dynamoDbClient).deleteItem(request);
    }

    @Test
    void query_returnsItems() throws DynamoDbSdkException {
        QueryResponse response = QueryResponse.builder()
                .items(Collections.singletonList(Collections.singletonMap("key", AttributeValue.builder().s("value").build())))
                .build();
        when(dynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        assertEquals(response.items(), dynamoDbWrapper.query("testTable", "key = :value", Collections.emptyMap(), Collections.emptyMap()));
    }

    @Test
    void scan_returnsItems() throws DynamoDbSdkException {
        ScanResponse response = ScanResponse.builder()
                .items(Collections.singletonList(Collections.singletonMap("key", AttributeValue.builder().s("value").build())))
                .build();
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(response);

        assertEquals(response.items(), dynamoDbWrapper.scan("testTable", Collections.emptyMap(), Collections.emptyMap(), "attribute = :value"));
    }

    @Test
    void batchWriteItems_writesItemsSuccessfully() throws DynamoDbSdkException {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put("testTable", Collections.singletonList(WriteRequest.builder().build()));
        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                .requestItems(requestItems)
                .build();

        dynamoDbWrapper.batchWriteItems(requestItems);

        verify(dynamoDbClient).batchWriteItem(request);
    }

    @Test
    void batchGetItems_returnsItems() throws DynamoDbSdkException {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put("testTable", KeysAndAttributes.builder().build());
        BatchGetItemResponse response = BatchGetItemResponse.builder()
                .responses(Collections.singletonMap("testTable", Collections.singletonList(Collections.singletonMap("key", AttributeValue.builder().s("value").build()))))
                .build();
        when(dynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(response);

        assertEquals(response.responses(), dynamoDbWrapper.batchGetItems(requestItems));
    }

    @Test
    void createTable_throwsException() {
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName("testTable")
                .attributeDefinitions(Collections.emptyList())
                .keySchema(Collections.emptyList())
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build())
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).createTable(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.createTable("testTable", Collections.emptyList(), Collections.emptyList(), ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build())
        );
    }

    @Test
    void deleteTable_throwsException() {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName("testTable")
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).deleteTable(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.deleteTable("testTable")
        );
    }

    @Test
    void describeTable_throwsException() {
        DescribeTableRequest request = DescribeTableRequest.builder()
                .tableName("testTable")
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).describeTable(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.describeTable("testTable")
        );
    }

    @Test
    void listTables_throwsException() {
        doThrow(DynamoDbException.class).when(dynamoDbClient).listTables();

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.listTables()
        );
    }

    @Test
    void putItem_throwsException() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("key", AttributeValue.builder().s("value").build());
        PutItemRequest request = PutItemRequest.builder()
                .tableName("testTable")
                .item(item)
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).putItem(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.putItem("testTable", item)
        );
    }

    @Test
    void getItem_throwsException() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("key", AttributeValue.builder().s("value").build());
        GetItemRequest request = GetItemRequest.builder()
                .tableName("testTable")
                .key(key)
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).getItem(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.getItem("testTable", key)
        );
    }

    @Test
    void updateItem_throwsException() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("key", AttributeValue.builder().s("value").build());
        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put("attribute", AttributeValueUpdate.builder().value(AttributeValue.builder().s("newValue").build()).build());
        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName("testTable")
                .key(key)
                .attributeUpdates(updates)
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).updateItem(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.updateItem("testTable", key, updates)
        );
    }

    @Test
    void deleteItem_throwsException() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("key", AttributeValue.builder().s("value").build());
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName("testTable")
                .key(key)
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).deleteItem(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.deleteItem("testTable", key)
        );
    }

    @Test
    void query_throwsException() {
        QueryRequest request = QueryRequest.builder()
                .tableName("testTable")
                .keyConditionExpression("key = :value")
                .expressionAttributeNames(Collections.emptyMap())
                .expressionAttributeValues(Collections.emptyMap())
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).query(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.query("testTable", "key = :value", Collections.emptyMap(), Collections.emptyMap())
        );
    }

    @Test
    void scan_throwsException() {
        ScanRequest request = ScanRequest.builder()
                .tableName("testTable")
                .expressionAttributeNames(Collections.emptyMap())
                .expressionAttributeValues(Collections.emptyMap())
                .filterExpression("attribute = :value")
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).scan(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.scan("testTable", Collections.emptyMap(), Collections.emptyMap(), "attribute = :value")
        );
    }

    @Test
    void batchWriteItems_throwsException() {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put("testTable", Collections.singletonList(WriteRequest.builder().build()));
        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                .requestItems(requestItems)
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).batchWriteItem(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.batchWriteItems(requestItems)
        );
    }

    @Test
    void batchGetItems_throwsException() {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put("testTable", KeysAndAttributes.builder().build());
        BatchGetItemRequest request = BatchGetItemRequest.builder()
                .requestItems(requestItems)
                .build();
        doThrow(DynamoDbException.class).when(dynamoDbClient).batchGetItem(request);

        assertThrows(DynamoDbSdkException.class, () ->
                dynamoDbWrapper.batchGetItems(requestItems)
        );
    }
}
