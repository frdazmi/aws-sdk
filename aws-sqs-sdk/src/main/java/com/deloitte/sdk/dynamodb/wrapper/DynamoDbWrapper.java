package com.deloitte.sdk.dynamodb.wrapper;

import com.deloitte.sdk.dynamodb.exceptions.DynamoDbSdkException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
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
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
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

import java.util.List;
import java.util.Map;

public class DynamoDbWrapper {

    private final DynamoDbClient dynamoDbClient;

    public DynamoDbWrapper(DynamoDbClient dynamoDbClient) {
        this.dynamoDbClient = dynamoDbClient;
    }

    // Table Operations

    public void createTable(String tableName, List<AttributeDefinition> attributeDefinitions,
                            List<KeySchemaElement> keySchema, ProvisionedThroughput provisionedThroughput) throws DynamoDbSdkException {
        try {
            CreateTableRequest request = CreateTableRequest.builder()
                    .tableName(tableName)
                    .attributeDefinitions(attributeDefinitions)
                    .keySchema(keySchema)
                    .provisionedThroughput(provisionedThroughput)
                    .build();
            dynamoDbClient.createTable(request);
            System.out.println("Table created successfully");
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to create table: " + tableName, e);
        }
    }

    public void deleteTable(String tableName) throws DynamoDbSdkException {
        try {
            DeleteTableRequest request = DeleteTableRequest.builder()
                    .tableName(tableName)
                    .build();
            dynamoDbClient.deleteTable(request);
            System.out.println("Table deleted successfully");
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to delete table: " + tableName, e);
        }
    }

    public TableDescription describeTable(String tableName) throws DynamoDbSdkException {
        try {
            DescribeTableRequest request = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();
            DescribeTableResponse response = dynamoDbClient.describeTable(request);
            return response.table();
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to describe table: " + tableName, e);
        }
    }

    public List<String> listTables() throws DynamoDbSdkException {
        try {
            ListTablesResponse response = dynamoDbClient.listTables();
            return response.tableNames();
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to list tables", e);
        }
    }

    // Item Operations

    public void putItem(String tableName, Map<String, AttributeValue> item) throws DynamoDbSdkException {
        try {
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();
            dynamoDbClient.putItem(request);
            System.out.println("Item inserted successfully");
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to put item into table: " + tableName, e);
        }
    }

    public Map<String, AttributeValue> getItem(String tableName, Map<String, AttributeValue> key) throws DynamoDbSdkException {
        try {
            GetItemRequest request = GetItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .build();
            GetItemResponse response = dynamoDbClient.getItem(request);
            return response.item();
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to get item from table: " + tableName, e);
        }
    }

    public void updateItem(String tableName, Map<String, AttributeValue> key, Map<String, AttributeValueUpdate> updates) throws DynamoDbSdkException {
        try {
            UpdateItemRequest request = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .attributeUpdates(updates)
                    .build();
            dynamoDbClient.updateItem(request);
            System.out.println("Item updated successfully");
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to update item in table: " + tableName, e);
        }
    }

    public void deleteItem(String tableName, Map<String, AttributeValue> key) throws DynamoDbSdkException {
        try {
            DeleteItemRequest request = DeleteItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .build();
            dynamoDbClient.deleteItem(request);
            System.out.println("Item deleted successfully");
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to delete item from table: " + tableName, e);
        }
    }

    // Query and Scan Operations

    public List<Map<String, AttributeValue>> query(String tableName, String keyConditionExpression, Map<String, String> expressionAttributeNames,
                                                   Map<String, AttributeValue> expressionAttributeValues) throws DynamoDbSdkException {
        try {
            QueryRequest request = QueryRequest.builder()
                    .tableName(tableName)
                    .keyConditionExpression(keyConditionExpression)
                    .expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues)
                    .build();
            QueryResponse response = dynamoDbClient.query(request);
            return response.items();
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to query table: " + tableName, e);
        }
    }

    public List<Map<String, AttributeValue>> scan(String tableName, Map<String, String> expressionAttributeNames,
                                                  Map<String, AttributeValue> expressionAttributeValues, String filterExpression) throws DynamoDbSdkException {
        try {
            ScanRequest request = ScanRequest.builder()
                    .tableName(tableName)
                    .expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues)
                    .filterExpression(filterExpression)
                    .build();
            ScanResponse response = dynamoDbClient.scan(request);
            return response.items();
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to scan table: " + tableName, e);
        }
    }

    // Batch Operations

    public void batchWriteItems(Map<String, List<WriteRequest>> requestItems) throws DynamoDbSdkException {
        try {
            BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                    .requestItems(requestItems)
                    .build();
            dynamoDbClient.batchWriteItem(request);
            System.out.println("Batch write operation completed");
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to perform batch write operation", e);
        }
    }

    public Map<String, List<Map<String, AttributeValue>>> batchGetItems(Map<String, KeysAndAttributes> requestItems) throws DynamoDbSdkException {
        try {
            BatchGetItemRequest request = BatchGetItemRequest.builder()
                    .requestItems(requestItems)
                    .build();
            BatchGetItemResponse response = dynamoDbClient.batchGetItem(request);
            return response.responses();
        } catch (DynamoDbException e) {
            throw new DynamoDbSdkException("Failed to perform batch get operation", e);
        }
    }
}
