package com.bizo.awsstubs.services.sqs;

// com.amazonaws.services.sqs.model defines its own UnsupportedOperationException, for some reason
import java.lang.UnsupportedOperationException;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;

public class AmazonSQSStub implements AmazonSQS {
  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEndpoint(final String arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRegion(final Region arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setQueueAttributes(final SetQueueAttributesRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(final ChangeMessageVisibilityBatchRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void changeMessageVisibility(final ChangeMessageVisibilityRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetQueueUrlResult getQueueUrl(final GetQueueUrlRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removePermission(final RemovePermissionRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetQueueAttributesResult getQueueAttributes(final GetQueueAttributesRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SendMessageBatchResult sendMessageBatch(final SendMessageBatchRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteQueue(final DeleteQueueRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SendMessageResult sendMessage(final SendMessageRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ReceiveMessageResult receiveMessage(final ReceiveMessageRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListQueuesResult listQueues(final ListQueuesRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListQueuesResult listQueues() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DeleteMessageBatchResult deleteMessageBatch(final DeleteMessageBatchRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateQueueResult createQueue(final CreateQueueRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addPermission(final AddPermissionRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteMessage(final DeleteMessageRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResponseMetadata getCachedResponseMetadata(final AmazonWebServiceRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListDeadLetterSourceQueuesResult listDeadLetterSourceQueues(
      final ListDeadLetterSourceQueuesRequest listDeadLetterSourceQueuesRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setQueueAttributes(final String queueUrl, final Map<String, String> attributes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(
      final String queueUrl,
      final List<ChangeMessageVisibilityBatchRequestEntry> entries) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void changeMessageVisibility(final String queueUrl, final String receiptHandle, final Integer visibilityTimeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetQueueUrlResult getQueueUrl(final String queueName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removePermission(final String queueUrl, final String label) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetQueueAttributesResult getQueueAttributes(final String queueUrl, final List<String> attributeNames) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SendMessageBatchResult sendMessageBatch(final String queueUrl, final List<SendMessageBatchRequestEntry> entries) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteQueue(final String queueUrl) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SendMessageResult sendMessage(final String queueUrl, final String messageBody) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ReceiveMessageResult receiveMessage(final String queueUrl) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListQueuesResult listQueues(final String queueNamePrefix) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DeleteMessageBatchResult deleteMessageBatch(
      final String queueUrl,
      final List<DeleteMessageBatchRequestEntry> entries) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateQueueResult createQueue(final String queueName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addPermission(
      final String queueUrl,
      final String label,
      final List<String> aWSAccountIds,
      final List<String> actions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteMessage(final String queueUrl, final String receiptHandle) {
    throw new UnsupportedOperationException();
  }

}