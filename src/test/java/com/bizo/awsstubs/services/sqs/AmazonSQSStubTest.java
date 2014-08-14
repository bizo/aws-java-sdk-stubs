package com.bizo.awsstubs.services.sqs;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.ArrayList;
import java.util.Arrays;
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
  public void receiveMessageReturnsAtMostXMessagesAtATime() {
    final String queueName = "bizo";
    final String baseMessageBody = "hi everybody";
    
    final CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(queueName);
    sqs.createQueue(createQueueRequest);
    
    final GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest().withQueueName(queueName);
    final GetQueueUrlResult getQueueUrlResult = sqs.getQueueUrl(getQueueUrlRequest);
    final String queueUrl = getQueueUrlResult.getQueueUrl();
    
    final List<String> expectedMessageBodies = new ArrayList<String>();
    final int numMessages = 13;
    for (int i = 0; i < numMessages; i++) {
      expectedMessageBodies.add(baseMessageBody + " " + i);
    }
    
    for (String messageBody : expectedMessageBodies) {
      final SendMessageRequest sendMessageRequest =
        new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(messageBody);
      sqs.sendMessage(sendMessageRequest);
    }
    
    final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(queueUrl);
    
    final ReceiveMessageResult receiveMessageResult1 = sqs.receiveMessage(receiveMessageRequest);
    final List<Message> messages1 = receiveMessageResult1.getMessages();
    assertThat(messages1.size(), equalTo(10));
    
    final ReceiveMessageResult receiveMessageResult2 = sqs.receiveMessage(receiveMessageRequest);
    final List<Message> messages2 = receiveMessageResult2.getMessages();
    assertThat(messages2.size(), equalTo(3));
    
    final ReceiveMessageResult receiveMessageResult3 = sqs.receiveMessage(receiveMessageRequest);
    final List<Message> messages3 = receiveMessageResult3.getMessages();
    assertThat(messages3.size(), equalTo(0));
    
    final List<String> actualMessageBodies = new ArrayList<String>();
    for (Message m : messages1) {
      actualMessageBodies.add(m.getBody());
    }
    for (Message m : messages2) {
      actualMessageBodies.add(m.getBody());
    }
    assertThat(expectedMessageBodies, equalTo(actualMessageBodies));
  }
  
  @Test
  public void deleteMessage() {
    fail();
  }
  
  @Test
  public void returnMessage() {
    final String queueName = "bizo";

    final CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(queueName);
    sqs.createQueue(createQueueRequest);

    final GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest().withQueueName(queueName);
    final GetQueueUrlResult getQueueUrlResult = sqs.getQueueUrl(getQueueUrlRequest);
    final String queueUrl = getQueueUrlResult.getQueueUrl();

    final SendMessageRequest sendMessageRequest1 =
      new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody("hi everybody 1");
    sqs.sendMessage(sendMessageRequest1);

    final SendMessageRequest sendMessageRequest2 =
      new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody("hi everybody 2");
    sqs.sendMessage(sendMessageRequest2);

    final SendMessageRequest sendMessageRequest3 =
      new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody("hi everybody 3");
    sqs.sendMessage(sendMessageRequest3);

    final ReceiveMessageRequest receiveMessageRequest1 = new ReceiveMessageRequest().withQueueUrl(queueUrl);
    final ReceiveMessageResult receiveMessageResult1 = sqs.receiveMessage(receiveMessageRequest1);
    final List<Message> messages1 = receiveMessageResult1.getMessages();

    final SendMessageRequest sendMessageRequest4 =
      new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody("hi everybody 4");
    sqs.sendMessage(sendMessageRequest4);

    // "hi everybody 2" should be at the top of the queue after being returned.
    sqs.returnMessage(queueUrl, messages1.get(1));

    final ReceiveMessageRequest receiveMessageRequest2 = new ReceiveMessageRequest().withQueueUrl(queueUrl);
    final ReceiveMessageResult receiveMessageResult2 = sqs.receiveMessage(receiveMessageRequest2);
    final List<Message> messages2 = receiveMessageResult2.getMessages();

    final List<String> expectedMessageBodies =
      Arrays.asList(sendMessageRequest2.getMessageBody(), sendMessageRequest4.getMessageBody());

    final List<String> actualMessageBodies = new ArrayList<String>();
    for (Message m : messages2) {
      actualMessageBodies.add(m.getBody());
    }
    
    assertThat(expectedMessageBodies, equalTo(actualMessageBodies));
  }
}
