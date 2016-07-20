package com.bizo.awsstubs.services.dynamodb2;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public class AmazonDynamoDBStubTest {

  private static final String TEST_TABLE_NAME = "TestTable";
  private static final Long UNITS = new Long(100);

  private static final String TEST_ATTRIBUTE = "Attribute1";
  private static final String TEST_ATTRIBUTE_VALUE = "AttributeValue1";
  private static final String TEST_ATTRIBUTE_2 = "Attribute2";
  private static final String TEST_ATTRIBUTE_VALUE_2 = "AttributeValue2";

  private AmazonDynamoDBStub dynamoDb = null;

  @Before
  public void setUp() {
    dynamoDb = new AmazonDynamoDBStub();
  }

//  @Test
//  public void test_batchGetItem_WithBatchGetItemRequest() throws Exception {
//    BatchGetItemResult result = dynamoDb.batchGetItem(batchGetItemRequest);
//  }

//  @Test
//  public void test_batchGetItem_WithRequestItems() throws Exception {
//    BatchGetItemResult result = dynamoDb.batchGetItem(requestItems);
//  }

  @Test
  public void test_batchGetItem_WithAllParameters() throws Exception {
    createTable();
    putItem(TEST_ATTRIBUTE, TEST_ATTRIBUTE_VALUE);
    putItem(TEST_ATTRIBUTE_2, TEST_ATTRIBUTE_VALUE_2);

    List<Map<String, AttributeValue>> keys = new ArrayList<Map<String, AttributeValue>>();
    Map<String, AttributeValue> key1 = new HashMap<String, AttributeValue>();
    key1.put(TEST_ATTRIBUTE, new AttributeValue()
      .withS(TEST_ATTRIBUTE_VALUE));
    keys.add(key1);
    Map<String, AttributeValue> key2 = new HashMap<String, AttributeValue>();
    key2.put(TEST_ATTRIBUTE_2, new AttributeValue()
      .withS(TEST_ATTRIBUTE_VALUE_2));
    keys.add(key2);

    Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
    requestItems.put(TEST_TABLE_NAME, new KeysAndAttributes()
      .withKeys(keys));
    String returnConsumedCapacity = "";

    BatchGetItemResult result = dynamoDb.batchGetItem(requestItems,returnConsumedCapacity);
    String tableName = result.getResponses().keySet().toArray(new String[1])[0];
    List<Map<String, AttributeValue>> items = result.getResponses().get(tableName);
    AttributeValue value1 = items.get(0).get(TEST_ATTRIBUTE);
    AttributeValue value2 = items.get(1).get(TEST_ATTRIBUTE_2);

    assertThat(tableName, equalTo(TEST_TABLE_NAME));
    assertThat(items.size(), equalTo(keys.size()));
    assertThat(value1.getS(), equalTo(TEST_ATTRIBUTE_VALUE));
    assertThat(value2.getS(), equalTo(TEST_ATTRIBUTE_VALUE_2));
  }

//  @Test
//  public void test_batchWriteItem_WithBatchWriteItemRequest() throws Exception {
//    BatchWriteItemResult result = dynamoDb.batchWriteItem(batchWriteItemRequest);
//  }

  @Test
  public void test_batchWriteItem_WithAllParameters() throws Exception {
    createTable();

    String TEST_ATTRIBUTE_2 = "Attribute2";
    String TEST_ATTRIBUTE_VALUE_2 = "AttributeValue2";

    Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
    List<WriteRequest> writeRequests = new ArrayList<WriteRequest>();

    Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>();
    item1.put(TEST_ATTRIBUTE, new AttributeValue()
      .withS(TEST_ATTRIBUTE_VALUE));
    WriteRequest writeRequest1 = new WriteRequest()
      .withPutRequest(new PutRequest()
        .withItem(item1));
    writeRequests.add(writeRequest1);

    Map<String, AttributeValue> item2 = new HashMap<String, AttributeValue>();
    item2.put(TEST_ATTRIBUTE_2, new AttributeValue()
      .withS(TEST_ATTRIBUTE_VALUE_2));
    WriteRequest writeRequest2 = new WriteRequest()
      .withPutRequest(new PutRequest()
        .withItem(item2));
    writeRequests.add(writeRequest2);

    requestItems.put(TEST_TABLE_NAME, writeRequests);

    BatchWriteItemResult result = dynamoDb.batchWriteItem(requestItems);
    List<ConsumedCapacity> consumedCapacities = result.getConsumedCapacity();

    assertThat(consumedCapacities.size(), equalTo(writeRequests.size()));
  }

//  @Test
//  public void test_createTable_WithCreateTableRequest() throws Exception {
//    CreateTableResult result = dynamoDb.createTable(createTableRequest);
//  }

  @Test
  public void test_createTable_WithParameters() throws Exception {
    CreateTableResult result = createTable();
    TableDescription tableDescription = result.getTableDescription();
    String createdTableName = tableDescription.getTableName();

    assertThat(createdTableName, equalTo(TEST_TABLE_NAME));
  }

  private CreateTableResult createTable() throws Exception {
    List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
    AttributeDefinition attributeDefinition = new AttributeDefinition()
      .withAttributeName(TEST_ATTRIBUTE)
      .withAttributeType(ScalarAttributeType.S);
    attributeDefinitions.add(attributeDefinition);

    List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
    KeySchemaElement keySchemaElement = new KeySchemaElement()
      .withAttributeName(TEST_ATTRIBUTE)
      .withKeyType(KeyType.HASH);

    ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
      .withReadCapacityUnits(UNITS)
      .withWriteCapacityUnits(UNITS);

    CreateTableResult result = dynamoDb.createTable(attributeDefinitions, TEST_TABLE_NAME, keySchema, provisionedThroughput);

    return result;
  }

//  @Test
//  public void test_deleteItem_WithDeleteItemRequest() throws Exception {
//    DeleteItemResult result = dynamoDb.deleteItem(deleteItemRequest);
//  }

//  @Test
//  public void test_deleteItem_WithPartialParameters() throws Exception {
//    DeleteItemResult deleteItem(tableName, key);
//  }

  @Test
  public void test_deleteItem_WithAllParameters() throws Exception {
    createTable();
    putItem(TEST_ATTRIBUTE, TEST_ATTRIBUTE_VALUE);

    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
    key.put(TEST_ATTRIBUTE, new AttributeValue()
      .withS(TEST_ATTRIBUTE_VALUE));
    String returnValues = "";

    DeleteItemResult deleteResult = dynamoDb.deleteItem(TEST_TABLE_NAME, key, returnValues);
    AttributeValue attributeValue = deleteResult.getAttributes().get(TEST_ATTRIBUTE);

    GetItemResult getResult = getItem(TEST_ATTRIBUTE, TEST_ATTRIBUTE_VALUE);

    assertThat(attributeValue.getS(), equalTo(TEST_ATTRIBUTE_VALUE));
    assertThat(getResult, nullValue());
  }

//  @Test
//  public void test_deleteTable_WithDeleteTableRequest() throws Exception {
//    DeleteTableResult result = dynamoDb.deleteTable(deleteTableRequest);
//  }

  @Test
  public void test_deleteTable() throws Exception {
    createTable();

    DeleteTableResult deleteResult = dynamoDb.deleteTable(TEST_TABLE_NAME);
    String tableName = deleteResult.getTableDescription().getTableName();

    assertThat(tableName, equalTo(TEST_TABLE_NAME));

    ListTablesResult listResult = listTables();

    assertThat(listResult.getTableNames().size(), equalTo(0));
  }

  @Test
  public void test_describeLimits() throws Exception {
    DescribeLimitsResult result = dynamoDb.describeLimits(new DescribeLimitsRequest());

    assertThat(result, notNullValue());
  }

//  @Test
//  public void test_describeTable_WithDescribeTableRequest() throws Exception {
//    DescribeTableResult result = dynamoDb.describeTable(describeTableRequest);
//  }

  @Test
  public void test_describeTable() throws Exception {
    createTable();

    DescribeTableResult result = dynamoDb.describeTable(TEST_TABLE_NAME);
    String tableName = result.getTable().getTableName();

    assertThat(tableName, equalTo(TEST_TABLE_NAME));
  }

  @Test
  public void test_getCachedResponseMetadata() throws Exception {
    ResponseMetadata result = dynamoDb.getCachedResponseMetadata(null);

    assertThat(result, notNullValue());
  }

//  @Test
//  public void test_getItem_WithGetItemRequest() throws Exception {
//    GetItemResult result = dynamoDb.getItem(getItemRequest);
//  }

//  @Test
//  public void test_getItem_WithPartialParameters() throws Exception {
//    GetItemResult getItem(tableName, key);
//  }

  @Test
  public void test_getItem_WithAllParameters() throws Exception {
    createTable();
    putItem(TEST_ATTRIBUTE, TEST_ATTRIBUTE_VALUE);

    GetItemResult result = getItem(TEST_ATTRIBUTE, TEST_ATTRIBUTE_VALUE);
    AttributeValue attributeValue = result.getItem().get(TEST_ATTRIBUTE);

    assertThat(attributeValue.getS(), equalTo(TEST_ATTRIBUTE_VALUE));
  }

  private GetItemResult getItem(String attributeName, String attributeValue) throws Exception {
    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
    key.put(attributeName, new AttributeValue()
      .withS(attributeValue));
    Boolean consistentRead = new Boolean(true);

    GetItemResult result = dynamoDb.getItem(TEST_TABLE_NAME, key, consistentRead);

    return result;
  }

  @Test
  public void test_listTables_WithNpParameters() throws Exception {
    createTable();

    ListTablesResult result = listTables();
    List<String> tableNames = result.getTableNames();

    assertThat(tableNames.size(), equalTo(1));
    assertThat(tableNames.get(0), equalTo(TEST_TABLE_NAME));
  }

  private ListTablesResult listTables() throws Exception {
    ListTablesResult result = dynamoDb.listTables();

    return result;
  }

//  @Test
//  public void test_listTables_WithListTablesRequest() throws Exception {
//    ListTablesResult result = dynamoDb.listTables(listTablesRequest);
//  }

  @Test
  public void test_listTables_WithExclusiveStartTableName() throws Exception {
    createTable();

    ListTablesResult result = dynamoDb.listTables(TEST_TABLE_NAME);
    List<String> tableNames = result.getTableNames();

    assertThat(tableNames.size(), equalTo(1));
    assertThat(tableNames.get(0), equalTo(TEST_TABLE_NAME));
  }

  @Test
  public void test_listTables_WithLimit() throws Exception {
    createTable();

    ListTablesResult result = dynamoDb.listTables(new Integer(10));
    List<String> tableNames = result.getTableNames();

    assertThat(tableNames.size(), equalTo(1));
    assertThat(tableNames.get(0), equalTo(TEST_TABLE_NAME));
  }

  @Test
  public void test_listTables_WithAllParameters() throws Exception {
    createTable();

    ListTablesResult result = dynamoDb.listTables(TEST_TABLE_NAME, new Integer(10));
    List<String> tableNames = result.getTableNames();

    assertThat(tableNames.size(), equalTo(1));
    assertThat(tableNames.get(0), equalTo(TEST_TABLE_NAME));
  }

//  @Test
//  public void test_putItem_WithPutItemRequest throws Exception {
//    PutItemResult result = dynamoDb.putItem(putItemRequest);
//  }

//  @Test
//  public void test_putItem_WithPartialParameters() throws Exception {
//    PutItemResult result = dynamoDb.putItem(tableName, item);
//  }

  @Test
  public void test_putItem_WithAllParameters() throws Exception {
    createTable();

    PutItemResult result = putItem(TEST_ATTRIBUTE, TEST_ATTRIBUTE_VALUE);
    Double units = result.getConsumedCapacity().getCapacityUnits();

    assertThat(units.doubleValue(), equalTo(1.0));
  }

  private PutItemResult putItem(String attributeName, String attributeValue) throws Exception {
    Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
    item.put(attributeName, new AttributeValue()
      .withS(attributeValue));
    String returnValues = "";

    PutItemResult result = dynamoDb.putItem(TEST_TABLE_NAME, item, returnValues);

    return result;
  }

  @Test
  public void test_query() throws Exception {
    createTable();
    putItem(TEST_ATTRIBUTE, TEST_ATTRIBUTE_VALUE);

    Condition keyCondition = new Condition()
      .withComparisonOperator(ComparisonOperator.EQ)
      .withAttributeValueList(new AttributeValue().withS(TEST_ATTRIBUTE_VALUE));

    Map<String, Condition> keyConditions = new HashMap<String, Condition>();
    keyConditions.put(TEST_ATTRIBUTE, keyCondition);

    QueryRequest queryRequest = new QueryRequest()
      .withTableName(TEST_TABLE_NAME)
      .withKeyConditions(keyConditions);

    QueryResult result = dynamoDb.query(queryRequest);
    Integer found = result.getCount();

    assertNotNull(found);
    assertEquals(found.longValue(), 1);
  }

//  @Test(expected=InternalServerErrorException.class)
//  public void test_scan_WithScanRequest() throws Exception {
//    ScanResult scan(scanRequest);
//  }

//  @Test(expected=InternalServerErrorException.class)
//  public void test_scan_WithAttributesToGet() throws Exception {
//    ScanResult scan(tableName, attributesToGet);
//  }

//  @Test(expected=InternalServerErrorException.class)
//  public void test_scan_WithScanFilter) throws Exception {
//    ScanResult scan(tableName, scanFilter);
//  }

  @Test(expected=InternalServerErrorException.class)
  public void test_scan_WithAllParameters() throws Exception {
    ScanResult result = dynamoDb.scan(TEST_TABLE_NAME, new ArrayList<String>(), new HashMap<String, Condition>());

    return;
  }

  @Test
  public void test_setEndpoint() throws Exception {
    dynamoDb.setEndpoint("endpoint");
  }

  @Test
  public void test_setRegion() throws Exception {
    dynamoDb.setRegion(Region.getRegion(Regions.AP_NORTHEAST_1));
  }

  @Test(expected=InternalServerErrorException.class)
  public void test_shutdown() throws Exception {
    dynamoDb.shutdown();

    listTables();
  }

//  @Test
//  public void test_updateItem_WithUpdateItemRequest() throws Exception {
//    UpdateItemResult result = dynamoDb.updateItem(updateItemRequest);
//  }

//  @Test
//  public void test_updateItem_WithPartialParameters() throws Exception {
//    UpdateItemResult result = dynamoDb.updateItem(tableName, key, attributeUpdates);
//  }

  @Test
  public void test_updateItem_WithAllParameters() throws Exception {
    createTable();
    putItem(TEST_ATTRIBUTE, TEST_ATTRIBUTE_VALUE);

    String UPDATE_ATTRIBUTE_VALUE = "UpdateAttributeValue1";

    Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
    key.put(TEST_ATTRIBUTE, new AttributeValue()
      .withS(TEST_ATTRIBUTE_VALUE));
    Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
    attributeUpdates.put(TEST_ATTRIBUTE, new AttributeValueUpdate()
      .withAction(AttributeAction.PUT)
      .withValue(new AttributeValue()
        .withS(UPDATE_ATTRIBUTE_VALUE)));
    String returnValues = "";

    UpdateItemResult result = dynamoDb.updateItem(TEST_TABLE_NAME, key, attributeUpdates, returnValues);
    Double units = result.getConsumedCapacity().getCapacityUnits();

    GetItemResult getItemResult = getItem(TEST_ATTRIBUTE, UPDATE_ATTRIBUTE_VALUE);
    String updatedValue = getItemResult.getItem().get(TEST_ATTRIBUTE).getS();

    assertThat(units.doubleValue(), equalTo(1.0));
    assertThat(updatedValue, equalTo(UPDATE_ATTRIBUTE_VALUE));
  }

//  @Test
//  public void test_updateTable_WithUpdateTableRequest() throws Exception {
//    UpdateTableResult result = dynamoDb.updateTable(updateTableRequest);
//  }

  @Test
  public void test_updateTable() throws Exception {
    createTable();

    DescribeTableResult describeResult = dynamoDb.describeTable(TEST_TABLE_NAME);
    Long readUnits = describeResult.getTable().getProvisionedThroughput().getReadCapacityUnits();

    assertThat(readUnits, equalTo(UNITS));

    UpdateTableResult updateResult = dynamoDb.updateTable(TEST_TABLE_NAME, new ProvisionedThroughput()
      .withReadCapacityUnits(new Long(200))
      .withWriteCapacityUnits(new Long(200)));
    String tableName = updateResult.getTableDescription().getTableName();

    assertThat(tableName, equalTo(TEST_TABLE_NAME));

    ListTablesResult listResult = listTables();

    describeResult = dynamoDb.describeTable(TEST_TABLE_NAME);
    readUnits = describeResult.getTable().getProvisionedThroughput().getReadCapacityUnits();

    assertThat(readUnits, equalTo(new Long(200)));
  }
}
