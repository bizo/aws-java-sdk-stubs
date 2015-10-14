package com.bizo.awsstubs.services.kinesis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.*;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesis.model.transform.PutRecordRequestMarshaller;
import com.amazonaws.services.kinesis.model.transform.PutRecordsRequestMarshaller;

import javax.xml.bind.DatatypeConverter;

/**
 * This should be kept threadsafe so that off thread writes can be tested
 */
public class AmazonKinesisClientStub implements AmazonKinesis {

  public ConcurrentHashMap<String, Stream> streams = new ConcurrentHashMap<String, Stream>();

  public void reset() {
    streams.clear();
  }

  @Override
  public void createStream(final CreateStreamRequest createStreamRequest) throws AmazonServiceException, AmazonClientException {
    streams.putIfAbsent(createStreamRequest.getStreamName(), new Stream());
  }

  @Override
  public void createStream(final String streamName, final Integer shardCount) throws AmazonServiceException, AmazonClientException {
    createStream(new CreateStreamRequest().withStreamName(streamName).withShardCount(shardCount));
  }

  @Override
  public void deleteStream(final DeleteStreamRequest deleteStreamRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteStream(final String streamName) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeStreamResult describeStream(final DescribeStreamRequest describeStreamRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeStreamResult describeStream(final String streamName) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeStreamResult describeStream(final String streamName, final String exclusiveStartShardId) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeStreamResult describeStream(final String streamName, final Integer limit, final String exclusiveStartShardId) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResponseMetadata getCachedResponseMetadata(final AmazonWebServiceRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetRecordsResult getRecords(final GetRecordsRequest request) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetShardIteratorResult getShardIterator(final GetShardIteratorRequest getShardIteratorRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetShardIteratorResult getShardIterator(final String streamName, final String shardId, final String shardIteratorType) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetShardIteratorResult getShardIterator(
    final String streamName,
    final String shardId,
    final String shardIteratorType,
    final String startingSequenceNumber) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListStreamsResult listStreams() throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListStreamsResult listStreams(final ListStreamsRequest listStreamsRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListStreamsResult listStreams(final String exclusiveStartStreamName) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListStreamsResult listStreams(final Integer limit, final String exclusiveStartStreamName) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mergeShards(final MergeShardsRequest mergeShardsRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mergeShards(final String streamName, final String shardToMerge, final String adjacentShardToMerge) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PutRecordResult putRecord(final PutRecordRequest putRecordRequest) throws AmazonServiceException, AmazonClientException {
    if (putRecordRequest.getData().array().length > 1024 * 1024) {
      throw new InvalidArgumentException("Payload exceeds 1 MB");
    }

    final Stream stream = streams.get(putRecordRequest.getStreamName());
    final String nextSequenceNumber = String.valueOf(streams.get(putRecordRequest.getStreamName()).sequenceNumber.incrementAndGet());

    stream.records.add(new Record()
      .withData(putRecordRequest.getData())
      .withPartitionKey(putRecordRequest.getPartitionKey())
      .withSequenceNumber(nextSequenceNumber));
    return new PutRecordResult().withSequenceNumber(nextSequenceNumber).withShardId("hard-coded-only-stub-shard");
  }

  @Override
  public PutRecordResult putRecord(final String streamName, final ByteBuffer data, final String partitionKey) throws AmazonServiceException, AmazonClientException {
    return putRecord(new PutRecordRequest().withStreamName(streamName).withData(data).withPartitionKey(partitionKey));
  }

  @Override
  public PutRecordResult putRecord(
    final String streamName,
    final ByteBuffer data,
    final String partitionKey,
    final String exclusiveMinimumSequenceNumber) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  /*
    Doesn't validate that payload size is less than 5MB.
   */
  @Override
  public PutRecordsResult putRecords(final PutRecordsRequest putRecordsRequest) throws AmazonServiceException, AmazonClientException {
    if (putRecordsRequest.getRecords().size() > 500) {
      throw new InvalidArgumentException("Payload contains more than 500 records");
    }
    final Stream stream = streams.get(putRecordsRequest.getStreamName());
    int failedRecordCount = 0;
    List<PutRecordsResultEntry> putRecordsResultEntries = new ArrayList<PutRecordsResultEntry>();
    for (PutRecordsRequestEntry putRecordsRequestEntry : putRecordsRequest.getRecords()) {
      if (putRecordsRequestEntry.getData().array().length > 1024 * 1024) {
        failedRecordCount += 1;
        putRecordsResultEntries.add(new PutRecordsResultEntry()
            .withErrorCode("InternalFailure")
            .withErrorMessage("Record exceeds 1MB"));
      } else {
        final String nextSequenceNumber = String.valueOf(stream.sequenceNumber.incrementAndGet());
        putRecordsResultEntries.add(
            new PutRecordsResultEntry()
                .withSequenceNumber(nextSequenceNumber)
                .withShardId("hard-coded-only-stub-shard"));
        stream.records.add(new Record()
            .withData(putRecordsRequestEntry.getData())
            .withSequenceNumber(nextSequenceNumber)
            .withPartitionKey(putRecordsRequestEntry.getPartitionKey()));
      }
    }
    return new PutRecordsResult().withFailedRecordCount(failedRecordCount).withRecords(putRecordsResultEntries);
  }

  @Override
  public void setEndpoint(final String endpoint) throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRegion(final Region region) throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void splitShard(final SplitShardRequest splitShardRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void splitShard(final String streamName, final String shardToSplit, final String newStartingHashKey) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeTagsFromStream(final RemoveTagsFromStreamRequest removeTagsFromStreamRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListTagsForStreamResult listTagsForStream(final ListTagsForStreamRequest listTagsForStreamRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addTagsToStream(final AddTagsToStreamRequest addTagsToStreamRequest) {
    throw new UnsupportedOperationException();
  }

  public static class Stream {
    public AtomicInteger sequenceNumber = new AtomicInteger(0);
    public BlockingQueue<Record> records = new LinkedBlockingQueue<Record>();
  }

  public BlockingQueue<Record> getRecordsForStream(final String streamName) {
    return streams.get(streamName).records;
  }
}
