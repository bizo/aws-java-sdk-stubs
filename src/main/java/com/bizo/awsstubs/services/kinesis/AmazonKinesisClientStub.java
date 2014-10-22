package com.bizo.awsstubs.services.kinesis;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;

/**
 * This should be kept threadsafe so that off thread writes can be tested
 */
public class AmazonKinesisClientStub implements AmazonKinesis {

  public ConcurrentHashMap<String, Stream> streams = new ConcurrentHashMap<String, Stream>();

  public void reset() {
    streams.clear();
  }

  @Override
  public void createStream(final CreateStreamRequest createStreamRequest)
      throws AmazonServiceException,
      AmazonClientException {
    streams.putIfAbsent(createStreamRequest.getStreamName(), new Stream());
  }

  @Override
  public void createStream(final String streamName, final Integer shardCount)
      throws AmazonServiceException,
      AmazonClientException {
    createStream(new CreateStreamRequest().withStreamName(streamName).withShardCount(shardCount));
  }

  @Override
  public void deleteStream(final DeleteStreamRequest deleteStreamRequest)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteStream(final String streamName) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeStreamResult describeStream(final DescribeStreamRequest describeStreamRequest)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeStreamResult describeStream(final String streamName)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeStreamResult describeStream(final String streamName, final String exclusiveStartShardId)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeStreamResult describeStream(
      final String streamName,
      final Integer limit,
      final String exclusiveStartShardId) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResponseMetadata getCachedResponseMetadata(final AmazonWebServiceRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetRecordsResult getRecords(final GetRecordsRequest request)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetShardIteratorResult getShardIterator(final GetShardIteratorRequest getShardIteratorRequest)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetShardIteratorResult getShardIterator(
      final String streamName,
      final String shardId,
      final String shardIteratorType) throws AmazonServiceException, AmazonClientException {
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
  public ListStreamsResult listStreams(final ListStreamsRequest listStreamsRequest)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListStreamsResult listStreams(final String exclusiveStartStreamName)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListStreamsResult listStreams(final Integer limit, final String exclusiveStartStreamName)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mergeShards(final MergeShardsRequest mergeShardsRequest)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mergeShards(final String streamName, final String shardToMerge, final String adjacentShardToMerge)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PutRecordResult putRecord(final PutRecordRequest putRecordRequest)
      throws AmazonServiceException,
      AmazonClientException {
    if (putRecordRequest.getData().array().length > 50 * 1024) {
      throw new InvalidArgumentException("Payload exceeds 50 KB");
    }

    final Stream stream = streams.get(putRecordRequest.getStreamName());
    final String nextSequenceNumber =
      String.valueOf(streams.get(putRecordRequest.getStreamName()).sequenceNumber.incrementAndGet());

    stream.records.add(new Record()
      .withData(putRecordRequest.getData())
      .withPartitionKey(putRecordRequest.getPartitionKey())
      .withSequenceNumber(nextSequenceNumber));
    return new PutRecordResult().withSequenceNumber(nextSequenceNumber).withShardId("hard-coded-only-stub-shard");
  }

  @Override
  public PutRecordResult putRecord(final String streamName, final ByteBuffer data, final String partitionKey)
      throws AmazonServiceException,
      AmazonClientException {
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
  public void splitShard(final SplitShardRequest splitShardRequest)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void splitShard(final String streamName, final String shardToSplit, final String newStartingHashKey)
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeTagsFromStream(final RemoveTagsFromStreamRequest removeTagsFromStreamRequest) 
      throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListTagsForStreamResult listTagsForStream(final ListTagsForStreamRequest listTagsForStreamRequest)
      throws AmazonServiceException,
      AmazonClientException {
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
