package com.bizo.awsstubs.services.sqs;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class AmazonSQSStubTest {
  
  AmazonSQSStub sqs;
  
  @Before
  public void setUp() {
    sqs = new AmazonSQSStub();    
  }
  
  @Test
  public void createQueue() {
    final String queueName = "bizo";
    
    final CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(queueName);
    sqs.createQueue(createQueueRequest);
    
    final ListQueuesResult listQueuesResult = sqs.listQueues();
    assertThat(listQueuesResult.getQueueUrls(), hasItem(containsString(queueName)));
  }
  
  @Test
  public void getQueueUrl() {
    final String queueName = "bizo";
    
    final CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(queueName);
    sqs.createQueue(createQueueRequest);
    
    final GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest().withQueueName(queueName);
    final GetQueueUrlResult getQueueUrlResult = sqs.getQueueUrl(getQueueUrlRequest);
    final String queueUrl = getQueueUrlResult.getQueueUrl();
    assertThat(queueUrl, containsString(queueName));
  }
  
  @Test
  public void sendAndReceiveMessage() {
    final String queueName = "bizo";
    final String messageBody = "hi everybody";
    
    final CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(queueName);
    sqs.createQueue(createQueueRequest);
    
    final GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest().withQueueName(queueName);
    final GetQueueUrlResult getQueueUrlResult = sqs.getQueueUrl(getQueueUrlRequest);
    final String queueUrl = getQueueUrlResult.getQueueUrl();
    
    final SendMessageRequest sendMessageRequest =
      new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(messageBody);
    sqs.sendMessage(sendMessageRequest);
    
    final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(queueUrl);
    final ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
    final List<Message> messages = receiveMessageResult.getMessages();
    
    assertThat(messages.size(), equalTo(1));
    assertThat(messages.get(0).getBody(), equalTo(messageBody));
  }
  
  @Test
  public void receiveMessageReturnsXMessagesAtATime() {
    final String queueName = "bizo";
    final String baseMessageBody = "hi everybody";
    
    final CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(queueName);
    sqs.createQueue(createQueueRequest);
    
    final GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest().withQueueName(queueName);
    final GetQueueUrlResult getQueueUrlResult = sqs.getQueueUrl(getQueueUrlRequest);
    final String queueUrl = getQueueUrlResult.getQueueUrl();
    
    final List<String> messageBodies = new ArrayList<String>();
    final int numMessages = 13;
    for (int i = 0; i < numMessages; i++) {
      messageBodies.add(baseMessageBody + " " + i);
    }
    
    for (String messageBody : messageBodies) {
      final SendMessageRequest sendMessageRequest =
        new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(messageBody);
      sqs.sendMessage(sendMessageRequest);
    }
    
    final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(queueUrl);
    final ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
    
    final List<Message> firstMessages = receiveMessageResult.getMessages();
    
    assertThat(firstMessages.size(), equalTo(10));
    
    
    
  }
}
