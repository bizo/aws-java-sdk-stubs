package com.bizo.awsstubs.services.dynamodb2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.Capacity;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

/**
 * @author rzanelli
 * Amazon DynamoDB stub.
 */
public class AmazonDynamoDBStub implements AmazonDynamoDB {

  private static final Long ZERO_COUNT = new Long(0);
  private static final Long MAX_CAPACITY_UNITS = new Long(100);
  private static final Double CAPACITY_UNITS = new Double(1.0);

  private final Map<String, Table> dynamoDB = new HashMap<String, Table>();
  private String lastEvaluatedTable = null;

  private String endpoint = null;
  private Region region = null;

  private boolean shutdown = false;

  public AmazonDynamoDBStub() {
  }

  @Override
  public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) {
    shutdownThrowsException();

    Collection<ConsumedCapacity> consumedCapacities = new ArrayList<ConsumedCapacity>();
    Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<String, List<Map<String, AttributeValue>>>();

    Map<String, KeysAndAttributes> requestItems = batchGetItemRequest.getRequestItems();
    for (String tableName : requestItems.keySet()) {
      Table table = dynamoDB.get(tableName);

      if (table == null) {
        throw new ResourceNotFoundException("Resource " + tableName + " not found.");
      }

      KeysAndAttributes keysAndAttributes = requestItems.get(tableName);
      List<Map<String, AttributeValue>> keys = keysAndAttributes.getKeys();
      List<String> attributesToGet = keysAndAttributes.getAttributesToGet();

      List<Map<String, AttributeValue>> matches = table.find(keys, attributesToGet);

      responses.put(tableName, matches);
    }

    BatchGetItemResult batchGetItemResult = new BatchGetItemResult()
      .withResponses(responses)
      .withConsumedCapacity(consumedCapacities);

    return batchGetItemResult;
  }

  @Override
  public BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> requestItems) {
    return batchGetItem(new BatchGetItemRequest()
      .withRequestItems(requestItems));
  }

  @Override
  public BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> requestItems,
                                        String returnConsumedCapacity) {
    return batchGetItem(new BatchGetItemRequest()
      .withRequestItems(requestItems)
      .withReturnConsumedCapacity(returnConsumedCapacity));
  }

  @Override
  public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest) {
    shutdownThrowsException();

    Collection<ConsumedCapacity> consumedCapacities = new ArrayList<ConsumedCapacity>();

    Map<String, List<WriteRequest>> requestItems = batchWriteItemRequest.getRequestItems();
    for (String tableName : requestItems.keySet()) {
      Table table = dynamoDB.get(tableName);

      if (table == null) {
        throw new ResourceNotFoundException("Resource " + tableName + " not found.");
      }

      List<WriteRequest> writeRequests = requestItems.get(tableName);
      for (WriteRequest writeRequest : writeRequests) {
        PutRequest putRequest = writeRequest.getPutRequest();
        if (putRequest != null) {
          Map<String, AttributeValue> item = putRequest.getItem();
          PutItemResult putItemResult = this.putItem(tableName, item);
          consumedCapacities.add(putItemResult.getConsumedCapacity());
        }

        DeleteRequest deleteRequest = writeRequest.getDeleteRequest();
        if (deleteRequest != null) {
          Map<String, AttributeValue> key = deleteRequest.getKey();
          DeleteItemResult deleteItemResult = this.deleteItem(tableName, key);
          consumedCapacities.add(deleteItemResult.getConsumedCapacity());
        }
      }
    }

    BatchWriteItemResult batchWriteItemResult = new BatchWriteItemResult()
      .withConsumedCapacity(consumedCapacities);

    return batchWriteItemResult;
  }

  @Override
  public BatchWriteItemResult batchWriteItem(Map<String, List<WriteRequest>> requestItems) {
    return batchWriteItem(new BatchWriteItemRequest()
      .withRequestItems(requestItems));
  }

  @Override
  public CreateTableResult createTable(CreateTableRequest createTableRequest) {
    shutdownThrowsException();

    String tableName = createTableRequest.getTableName();
    ProvisionedThroughput provisionedThroughput = createTableRequest.getProvisionedThroughput();
    List<AttributeDefinition> attributeDefinitions = createTableRequest.getAttributeDefinitions();
    List<KeySchemaElement> keySchema = createTableRequest.getKeySchema();

    Table table = dynamoDB.get(tableName);

    if (table != null) {
      throw new ResourceInUseException("Resource " + tableName + " already exists.");
    }

    ProvisionedThroughputDescription provisionedThroughputDescription = new ProvisionedThroughputDescription()
      .withReadCapacityUnits(provisionedThroughput.getReadCapacityUnits())
      .withWriteCapacityUnits(provisionedThroughput.getWriteCapacityUnits())
      .withNumberOfDecreasesToday(ZERO_COUNT)
      .withLastDecreaseDateTime(new Date())
      .withLastIncreaseDateTime(new Date());

    TableDescription tableDescription = new TableDescription()
      .withAttributeDefinitions(attributeDefinitions)
      .withTableName(tableName)
      .withKeySchema(keySchema)
      .withProvisionedThroughput(provisionedThroughputDescription)
      .withCreationDateTime(new Date())
      .withItemCount(ZERO_COUNT)
      .withTableSizeBytes(ZERO_COUNT)
      .withTableStatus(TableStatus.ACTIVE);

    table = new Table(tableDescription);

    dynamoDB.put(tableName, table);
    lastEvaluatedTable = tableName;

    CreateTableResult createTableResult = new CreateTableResult()
      .withTableDescription(tableDescription);

    return createTableResult;
  }

  @Override
  public CreateTableResult createTable(List<AttributeDefinition> attributeDefinitions,
                                      String tableName,
                                      List<KeySchemaElement> keySchema,
                                      ProvisionedThroughput provisionedThroughput) {
    return createTable(new CreateTableRequest()
      .withAttributeDefinitions(attributeDefinitions)
      .withTableName(tableName)
      .withKeySchema(keySchema)
      .withProvisionedThroughput(provisionedThroughput));
  }

  @Override
  public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
    shutdownThrowsException();

    String tableName = deleteItemRequest.getTableName();
    Map<String, AttributeValue> key = deleteItemRequest.getKey();

    Table table = dynamoDB.get(tableName);

    if (table == null) {
      throw new ResourceNotFoundException("Resource " + tableName + " not found.");
    }

    Map<String, AttributeValue> match = table.delete(key);

    DeleteItemResult deleteItemResult = new DeleteItemResult()
      .withAttributes(match)
      .withConsumedCapacity(new ConsumedCapacity()
        .withTableName(tableName)
        .withCapacityUnits(CAPACITY_UNITS)
        .withTable(new Capacity()
          .withCapacityUnits(CAPACITY_UNITS)));

    return deleteItemResult;
  }

  @Override
  public DeleteItemResult deleteItem(String tableName,
                                    Map<String, AttributeValue> key) {
    return deleteItem(new DeleteItemRequest()
      .withTableName(tableName)
      .withKey(key));
  }

  @Override
  public DeleteItemResult deleteItem(String tableName,
                                    Map<String, AttributeValue> key,
                                    String returnValues) {
    return deleteItem(new DeleteItemRequest()
      .withTableName(tableName)
      .withKey(key)
      .withReturnValues(returnValues));
  }

  @Override
  public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
    shutdownThrowsException();

    String tableName = deleteTableRequest.getTableName();

    Table table = dynamoDB.get(tableName);

    if (table == null) {
      throw new ResourceNotFoundException("Resource " + tableName + " not found.");
    }

    dynamoDB.remove(tableName);

    lastEvaluatedTable = tableName;

    DeleteTableResult deleteTableResult = new DeleteTableResult()
      .withTableDescription(table.getTableDescription());

    return deleteTableResult;
  }

  @Override
  public DeleteTableResult deleteTable(String tableName) {
    return deleteTable(new DeleteTableRequest()
      .withTableName(tableName));
  }

  @Override
  public DescribeLimitsResult describeLimits(DescribeLimitsRequest describeLimitsRequest) {
    shutdownThrowsException();

    DescribeLimitsResult describeLimitsResult = new DescribeLimitsResult()
      .withAccountMaxReadCapacityUnits(MAX_CAPACITY_UNITS)
      .withAccountMaxWriteCapacityUnits(MAX_CAPACITY_UNITS)
      .withTableMaxReadCapacityUnits(MAX_CAPACITY_UNITS)
      .withTableMaxWriteCapacityUnits(MAX_CAPACITY_UNITS);

    return describeLimitsResult;
  }

  @Override
  public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
    shutdownThrowsException();

    String tableName = describeTableRequest.getTableName();

    Table table = dynamoDB.get(tableName);

    if (table == null) {
      throw new ResourceNotFoundException("Resource " + tableName + " not found.");
    }

    lastEvaluatedTable = tableName;

    DescribeTableResult describeTableResult = new DescribeTableResult()
      .withTable(table.getTableDescription());

    return describeTableResult;
  }

  @Override
  public DescribeTableResult describeTable(String tableName) {
    return describeTable(new DescribeTableRequest()
      .withTableName(tableName));
  }

  @Override
  public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    Map<String, String> metadata = new HashMap<String, String>();
    ResponseMetadata responseMetadata = new ResponseMetadata(metadata);

    return responseMetadata;
  }

  @Override
  public GetItemResult getItem(GetItemRequest getItemRequest) {
    shutdownThrowsException();

    String tableName = getItemRequest.getTableName();
    Map<String, AttributeValue> key = getItemRequest.getKey();
    List<String> attributesToGet = getItemRequest.getAttributesToGet();

    Table table = dynamoDB.get(tableName);

    if (table == null) {
      throw new ResourceNotFoundException("Resource " + tableName + " not found.");
    }

    Map<String, AttributeValue> match = table.find(key, attributesToGet);

    GetItemResult getItemResult = null;

    if (match != null) {
      getItemResult = new GetItemResult()
        .withItem(match)
        .withConsumedCapacity(new ConsumedCapacity()
          .withTableName(tableName)
          .withCapacityUnits(CAPACITY_UNITS)
          .withTable(new Capacity()
            .withCapacityUnits(CAPACITY_UNITS)));
    }

    return getItemResult;
  }

  @Override
  public GetItemResult getItem(String tableName,
                              Map<String, AttributeValue> key) {
    return getItem(new GetItemRequest()
      .withTableName(tableName)
      .withKey(key));
  }

  @Override
  public GetItemResult getItem(String tableName,
                              Map<String, AttributeValue> key,
                              Boolean consistentRead) {
    return getItem(new GetItemRequest()
      .withTableName(tableName)
      .withKey(key)
      .withConsistentRead(consistentRead));
  }

  @Override
  public ListTablesResult listTables() {
    return listTables(new ListTablesRequest());
  }

  @Override
  public ListTablesResult listTables(ListTablesRequest listTablesRequest) {
    shutdownThrowsException();

    ListTablesResult listTablesResult = new ListTablesResult()
      .withTableNames(dynamoDB.keySet())
      .withLastEvaluatedTableName(lastEvaluatedTable);

    return listTablesResult;
  }

  @Override
  public ListTablesResult listTables(String exclusiveStartTableName) {
    return listTables(new ListTablesRequest()
      .withExclusiveStartTableName(exclusiveStartTableName));
  }

  @Override
  public ListTablesResult listTables(Integer limit) {
    return listTables(new ListTablesRequest()
      .withLimit(limit));
  }

  @Override
  public ListTablesResult listTables(String exclusiveStartTableName,
                                    Integer limit) {
    return listTables(new ListTablesRequest()
      .withExclusiveStartTableName(exclusiveStartTableName)
      .withLimit(limit));
  }

  @Override
  public PutItemResult putItem(PutItemRequest putItemRequest) {
    shutdownThrowsException();

    String tableName = putItemRequest.getTableName();
    Map<String, AttributeValue> item = putItemRequest.getItem();

    Table table = dynamoDB.get(tableName);

    if (table == null) {
      throw new ResourceNotFoundException("Resource " + tableName + " not found.");
    }

    table.addRow(item);

    PutItemResult putItemResult = new PutItemResult()
      .withAttributes(item)
      .withConsumedCapacity(new ConsumedCapacity()
        .withTableName(tableName)
        .withCapacityUnits(CAPACITY_UNITS)
        .withTable(new Capacity()
          .withCapacityUnits(CAPACITY_UNITS)));

    return putItemResult;
  }

  @Override
  public PutItemResult putItem(String tableName,
                              Map<String, AttributeValue> item) {
    return putItem(new PutItemRequest()
      .withTableName(tableName)
      .withItem(item));
  }

  @Override
  public PutItemResult putItem(String tableName,
                              Map<String, AttributeValue> item,
                              String returnValues) {
    return putItem(new PutItemRequest()
      .withTableName(tableName)
      .withItem(item)
      .withReturnValues(returnValues));
  }

  @Override
  public QueryResult query(QueryRequest queryRequest) {
    shutdownThrowsException();

    String tableName = queryRequest.getTableName();
    Map<String, Condition> keyConditions = queryRequest.getKeyConditions();
    List<String> attributesToGet = queryRequest.getAttributesToGet();

    Table table = dynamoDB.get(tableName);

    List<Map<String, AttributeValue>> match = table.query(keyConditions, attributesToGet);

    QueryResult queryResult = null;

    if (match != null) {
      queryResult = new QueryResult()
        .withItems(match)
        .withConsumedCapacity(new ConsumedCapacity()
          .withTableName(tableName)
          .withCapacityUnits(CAPACITY_UNITS)
          .withTable(new Capacity()
            .withCapacityUnits(CAPACITY_UNITS)));
    }

    return queryResult;
  }

  @Override
  public ScanResult scan(ScanRequest scanRequest) {
    shutdownThrowsException();

    ScanResult scanResult = new ScanResult()
      .withCount(new Integer(0));

    if (true) {
      throw new InternalServerErrorException("Operation not supported.");
    }

    return scanResult;
  }

  @Override
  public ScanResult scan(String tableName,
                        List<String> attributesToGet) {
    return scan(new ScanRequest()
      .withTableName(tableName)
      .withAttributesToGet(attributesToGet));
  }

  @Override
  public ScanResult scan(String tableName,
                        Map<String, Condition> scanFilter) {
    return scan(tableName, null, scanFilter);
  }

  @Override
  public ScanResult scan(String tableName,
                        List<String> attributesToGet,
                        Map<String, Condition> scanFilter) {
    return scan(new ScanRequest()
      .withTableName(tableName)
      .withScanFilter(scanFilter));
  }

  @Override
  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  @Override
  public void setRegion(Region region) {
    this.region = region;
  }

  @Override
  public void shutdown() {
    this.shutdown = true;
  }

  @Override
  public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
    shutdownThrowsException();

    String tableName = updateItemRequest.getTableName();
    Map<String, AttributeValue> key = updateItemRequest.getKey();
    Map<String, AttributeValueUpdate> attributeUpdates = updateItemRequest.getAttributeUpdates();

    Table table = dynamoDB.get(tableName);

    if (table == null) {
      throw new ResourceNotFoundException("Resource " + tableName + " not found.");
    }

    Map<String, AttributeValue> match = table.update(key, attributeUpdates);

    UpdateItemResult updateItemResult = new UpdateItemResult()
      .withAttributes(match)
      .withConsumedCapacity(new ConsumedCapacity()
        .withTableName(tableName)
        .withCapacityUnits(CAPACITY_UNITS)
        .withTable(new Capacity()
          .withCapacityUnits(CAPACITY_UNITS)));

    return updateItemResult;
  }

  @Override
  public UpdateItemResult updateItem(String tableName,
                                    Map<String, AttributeValue> key,
                                    Map<String, AttributeValueUpdate> attributeUpdates) {
    return updateItem(new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(key)
      .withAttributeUpdates(attributeUpdates));
  }

  @Override
  public UpdateItemResult updateItem(String tableName,
                                    Map<String, AttributeValue> key,
                                    Map<String, AttributeValueUpdate> attributeUpdates,
                                    String returnValues) {
    return updateItem(new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(key)
      .withAttributeUpdates(attributeUpdates)
      .withReturnValues(returnValues));
  }

  @Override
  public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest) {
    shutdownThrowsException();

    String tableName = updateTableRequest.getTableName();
    ProvisionedThroughput provisionedThroughput = updateTableRequest.getProvisionedThroughput();

    Table table = dynamoDB.get(tableName);

    if (table == null) {
      throw new ResourceNotFoundException("Resource " + tableName + " not found.");
    }

    ProvisionedThroughputDescription provisionedThroughputDescription = new ProvisionedThroughputDescription()
      .withReadCapacityUnits(provisionedThroughput.getReadCapacityUnits())
      .withWriteCapacityUnits(provisionedThroughput.getWriteCapacityUnits())
      .withNumberOfDecreasesToday(ZERO_COUNT)
      .withLastDecreaseDateTime(new Date())
      .withLastIncreaseDateTime(new Date());

    TableDescription tableDescription = table.getTableDescription();
    tableDescription.setProvisionedThroughput(provisionedThroughputDescription);

    lastEvaluatedTable = tableName;

    UpdateTableResult updateTableResult = new UpdateTableResult()
      .withTableDescription(tableDescription);

    return updateTableResult;
  }

  @Override
  public UpdateTableResult updateTable(String tableName,
                                      ProvisionedThroughput provisionedThroughput) {
    return updateTable(new UpdateTableRequest()
      .withTableName(tableName)
      .withProvisionedThroughput(provisionedThroughput));
  }

  private void shutdownThrowsException() {
    if (shutdown) {
      throw new InternalServerErrorException("The DynamoDB server is shutdown.");
    }
  }

  // Added for testing
  protected Map<String, Table> getDynamoDB() {
    return dynamoDB;
  }

  protected static class Table {

    private TableDescription description = null;
    private List<Map<String, AttributeValue>> data = null;

    private Table(TableDescription tableDescription) {
      this.setTableDescription(tableDescription);
      this.setRows(new ArrayList<Map<String, AttributeValue>>());
    }

    public TableDescription getTableDescription() {
      return description;
    }

    public void setTableDescription(TableDescription tableDescription) {
      this.description = tableDescription;
    }

    private List<Map<String, AttributeValue>> getRows() {
      return data;
    }

    private void setRows(List<Map<String, AttributeValue>> rows) {
      this.data = rows;
    }

    public void addRow(Map<String, AttributeValue> row) {
      data.add(row);
    }

    public void addRows(Collection<Map<String, AttributeValue>> rows) {
      data.addAll(rows);
    }

    public Map<String, AttributeValue> find(Map<String, AttributeValue> key,
                                            List<String> attributesToGet) {
      Map<String, AttributeValue> match = null;

      List<Map<String, AttributeValue>> rows = this.getRows();

      for (Map<String, AttributeValue> row : rows) {
        Set<Entry<String, AttributeValue>> rowColumns = new HashSet<Entry<String, AttributeValue>>(row.entrySet());
        Set<Entry<String, AttributeValue>> keyColumns = new HashSet<Entry<String, AttributeValue>>(key.entrySet());

        if (rowColumns.containsAll(keyColumns)) {
          match = new HashMap<String, AttributeValue>(row);

          if ((attributesToGet != null) && (attributesToGet.size() > 0)) {
            match.keySet().retainAll(attributesToGet);
          }

          break;
        }
      }

      return match;
    }

    public List<Map<String, AttributeValue>> find(List<Map<String, AttributeValue>> keys,
                                                  List<String> attributesToGet) {
      List<Map<String, AttributeValue>> matches = new ArrayList<Map<String, AttributeValue>>();

      List<Map<String, AttributeValue>> rows = this.getRows();

      for (Map<String, AttributeValue> row : rows) {
        Set<Entry<String, AttributeValue>> rowColumns = new HashSet<Entry<String, AttributeValue>>(row.entrySet());

        for (Map<String, AttributeValue> key : keys) {
          Set<Entry<String, AttributeValue>> keyColumns = new HashSet<Entry<String, AttributeValue>>(key.entrySet());

          if (rowColumns.containsAll(keyColumns)) {
            Map<String, AttributeValue> match = new HashMap<String, AttributeValue>(row);

            if ((attributesToGet != null) && (attributesToGet.size() > 0)) {
              match.keySet().retainAll(attributesToGet);
            }

            matches.add(match);
            break;
          }
        }
      }

      return matches;
    }

    public List<Map<String, AttributeValue>> query(Map<String, Condition> keyConditions,
      List<String> attributesToGet) {
      List<Map<String, AttributeValue>> matches = new ArrayList<Map<String, AttributeValue>>();

      List<Map<String, AttributeValue>> rows = this.getRows();

      for (Map<String, AttributeValue> row : rows) {
        boolean match = true;

        for (Entry<String, Condition> keyCondition : keyConditions.entrySet()) {
          String columnName = keyCondition.getKey();
          Condition condition = keyCondition.getValue();

          AttributeValue columnValue = row.get(columnName);

          boolean comparisonMatch = comparisonMatch(columnValue, condition);
          match &= comparisonMatch;
        }

        if (match) {
          matches.add(row);
        }
      }

      return matches;
    }

    private boolean comparisonMatch(AttributeValue columnValue, Condition condition) {
      boolean comparisonMatch = false;

      ComparisonOperator comparisonOperator = ComparisonOperator.fromValue(condition.getComparisonOperator());
      AttributeValue comparisonAttributeValue = condition.getAttributeValueList().get(0);

      switch (comparisonOperator) {
        case EQ:
          comparisonMatch = AttributeValueComparator.equal(columnValue, comparisonAttributeValue);
          break;
        case LT:
          comparisonMatch = AttributeValueComparator.lessThan(columnValue, comparisonAttributeValue);
          break;
        case GT:
          comparisonMatch = AttributeValueComparator.greaterThan(columnValue, comparisonAttributeValue);
          break;
        case LE:
          comparisonMatch = AttributeValueComparator.lessThanEqual(columnValue, comparisonAttributeValue);
          break;
        case GE:
          comparisonMatch = AttributeValueComparator.greaterThanEqual(columnValue, comparisonAttributeValue);
          break;
        default:
          throw new InternalServerErrorException("Operation not supported.");
      }

      return comparisonMatch;
    }

    public Map<String, AttributeValue> delete(Map<String, AttributeValue> key) {
      Map<String, AttributeValue> match = null;

      List<Map<String, AttributeValue>> rows = this.getRows();

      for (Map<String, AttributeValue> row : rows) {
        Set<Entry<String, AttributeValue>> rowColumns = new HashSet<Entry<String, AttributeValue>>(row.entrySet());
        Set<Entry<String, AttributeValue>> keyColumns = new HashSet<Entry<String, AttributeValue>>(key.entrySet());

        if (rowColumns.containsAll(keyColumns)) {
          match = new HashMap<String, AttributeValue>(row);
          rows.remove(match);

          break;
        }
      }

      return match;
    }

    public Map<String, AttributeValue> update(Map<String, AttributeValue> key, Map<String, AttributeValueUpdate> attributeUpdates) {
      Map<String, AttributeValue> match = null;

      List<Map<String, AttributeValue>> rows = this.getRows();

      for (Map<String, AttributeValue> row : rows) {
        Set<Entry<String, AttributeValue>> rowColumns = new HashSet<Entry<String, AttributeValue>>(row.entrySet());
        Set<Entry<String, AttributeValue>> keyColumns = new HashSet<Entry<String, AttributeValue>>(key.entrySet());

        if (rowColumns.containsAll(keyColumns)) {
          for (String updateKey : attributeUpdates.keySet()) {
            AttributeValueUpdate update = attributeUpdates.get(updateKey);
            AttributeAction action = AttributeAction.fromValue(update.getAction());
            if ((action == AttributeAction.ADD) || (action == AttributeAction.PUT)) {
              row.put(updateKey, update.getValue());
            }
            else if (action == AttributeAction.DELETE) {
              row.remove(updateKey);
            }
          }

          match = new HashMap<String, AttributeValue>(row);

          break;
        }
      }

      return match;
    }
  }
}
