package com.bizo.awsstubs.services.sqs;

// com.amazonaws.services.sqs.model defines its own UnsupportedOperationException, for some reason
import java.lang.UnsupportedOperationException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;

/**
 * An AmazonSQS stub which stores its queues in memory.
 * <p>
 * This stub doesn't handle visibility timeouts (<a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html">http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html</a>),
 * i.e. if a received message hasn't been deleted before its visibility timeout expires, it doesn't become visible and
 * return to its queue automatically. To simulate a message being returned to its queue, use the {@link
 * #returnMessage(String, Message)} method.
 */
public class AmazonSQSStub implements AmazonSQS {

  private final Map<String, Queue> queuesByQueueUrl = new TreeMap<String, Queue>();
  private Region region = Region.getRegion(Regions.US_EAST_1);

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEndpoint(final String arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRegion(final Region region) {
    this.region = region;
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
  public GetQueueUrlResult getQueueUrl(final GetQueueUrlRequest request) {
    return new GetQueueUrlResult().withQueueUrl(generateQueueUrl(request.getQueueName()));
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
  public SendMessageResult sendMessage(final SendMessageRequest request) {
    final Message message =
      new Message()
        .withBody(request.getMessageBody())
        .withMessageAttributes(request.getMessageAttributes());
    final Queue queue = queuesByQueueUrl.get(request.getQueueUrl());
    queue.sendMessage(message);
    return new SendMessageResult().withMessageId(message.getMessageId());
  }

  @Override
  public ReceiveMessageResult receiveMessage(final ReceiveMessageRequest request) {
    final Queue queue = queuesByQueueUrl.get(request.getQueueUrl());

    final List<Message> messages = new ArrayList<Message>();
    final int maxNumberOfMessages =
      request.getMaxNumberOfMessages() == null ? 1 : Math.min(request.getMaxNumberOfMessages(), 10);
    Message m;
    while (messages.size() < maxNumberOfMessages && (m = queue.receiveMessage()) != null) {
      final Map<String, String> attributes = new HashMap<String, String>(m.getAttributes());
      attributes.keySet().retainAll(request.getAttributeNames());

      final Map<String, MessageAttributeValue> messageAttributes =
        new HashMap<String, MessageAttributeValue>(m.getMessageAttributes());
      messageAttributes.keySet().retainAll(request.getMessageAttributeNames());

      Message transformedMessage =
        new Message()
          .withBody(m.getBody())
          .withMessageId(m.getMessageId())
          .withReceiptHandle(m.getReceiptHandle())
          .withAttributes(attributes)
          .withMessageAttributes(messageAttributes);
      messages.add(transformedMessage);
    }

    return new ReceiveMessageResult().withMessages(messages);
  }

  @Override
  public ListQueuesResult listQueues(final ListQueuesRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListQueuesResult listQueues() {
    return new ListQueuesResult().withQueueUrls(queuesByQueueUrl.keySet());
  }

  @Override
  public DeleteMessageBatchResult deleteMessageBatch(final DeleteMessageBatchRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateQueueResult createQueue(final CreateQueueRequest request) {
    final String queueName = request.getQueueName();
    final String queueUrl = generateQueueUrl(queueName);

    if (queuesByQueueUrl.containsKey(queueUrl)) {
      throw new QueueNameExistsException(queueName);
    }

    queuesByQueueUrl.put(queueUrl, new Queue());

    return new CreateQueueResult().withQueueUrl(queueUrl);
  }

  @Override
  public void addPermission(final AddPermissionRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteMessage(final DeleteMessageRequest request) {
    final Queue queue = queuesByQueueUrl.get(request.getQueueUrl());
    queue.deleteMessage(request.getReceiptHandle());
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
    return getQueueUrl(new GetQueueUrlRequest().withQueueName(queueName));
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
    return sendMessage(new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(messageBody));
  }

  @Override
  public ReceiveMessageResult receiveMessage(final String queueUrl) {
    return receiveMessage(new ReceiveMessageRequest().withQueueUrl(queueUrl));
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
    return createQueue(new CreateQueueRequest().withQueueName(queueName));
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
    deleteMessage(new DeleteMessageRequest().withQueueUrl(queueUrl).withReceiptHandle(receiptHandle));
  }

  // Testing support

  public String generateQueueUrl(final String queueName) {
    return "https://sqs." + region.getName() + ".amazonaws.com/000000000000/" + queueName;
  }

  /**
   * Return message back to a queue, so it can be received again.
   * <p>
   * @see <a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html">http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html</a>
   */
  public void returnMessage(final String queueUrl, final Message message) {
    final Queue queue = queuesByQueueUrl.get(queueUrl);
    queue.returnMessage(message);
  }

  /**
   * Get messages belonging to a particular queue so that they can be examined.
   */
  public List<Message> getMessages(final String queueUrl) {
    final Queue queue = queuesByQueueUrl.get(queueUrl);
    return new ArrayList<Message>(queue.messageQueue);
  }

  private static class Queue {
    private int sequenceNumber = 0;

    // Sent messages go into this queue and can be received. If a received message is returned, it goes back into
    // this queue. The message with the lowest sequence number is received first.
    private final PriorityQueue<Message> messageQueue = new PriorityQueue<Message>(11, new Comparator<Message>() {
      @Override
      public int compare(Message m1, Message m2) {
        return Integer.parseInt(m1.getMessageId()) - Integer.parseInt(m2.getMessageId());
      }
    });

    // Received messages move into this list and are considered in-flight and are not visible.
    private final List<Message> inflightMessages = new LinkedList<Message>();

    public void sendMessage(final Message message) {
      message.setMessageId(String.valueOf(++sequenceNumber));
      messageQueue.offer(message);
    }

    public Message receiveMessage() {
      final Message message = messageQueue.poll();

      if (message != null) {
        message.setReceiptHandle(message.getMessageId() + "-" + System.currentTimeMillis());
        inflightMessages.add(message);
      }

      return message;
    }

    public void deleteMessage(final String receiptHandle) throws ReceiptHandleIsInvalidException {
      final Iterator<Message> it = inflightMessages.iterator();
      while (it.hasNext()) {
        final Message m = it.next();
        if (m.getReceiptHandle().equals(receiptHandle)) {
          it.remove();
          return;
        }
      }

      throw new ReceiptHandleIsInvalidException(receiptHandle);
    }

    public void returnMessage(final Message message) {
      inflightMessages.remove(message);
      messageQueue.offer(message);
    }
  }
}