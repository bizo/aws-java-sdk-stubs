package com.bizo.awsstubs.services.kinesis;

import com.amazonaws.services.kinesis.model.*;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.junit.rules.ExpectedException;

import javax.xml.bind.DatatypeConverter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class AmazonKinesisClientStubTest {
  AmazonKinesisClientStub kinesis;

  @Before
  public void setUp() {
    kinesis = new AmazonKinesisClientStub();
  }

  @Test
  public void putRecords() throws IOException {
    String streamName = "testStream";
    final ByteBuffer deadbeef = ByteBuffer.wrap(new byte[]{0xD, 0xE, 0xA, 0xD, 0xB, 0xE, 0xE, 0xF});;
    final ByteBuffer icebooda = ByteBuffer.wrap(new byte[]{0x1, 0xC, 0xE, 0xB, 0x0, 0x0, 0xD, 0xA});
    String partitionKey = "foo";
    kinesis.createStream(streamName, 1);
    PutRecordsRequest putRecordsRequest = new PutRecordsRequest()
        .withStreamName(streamName)
        .withRecords(Arrays.asList(
            new PutRecordsRequestEntry()
                .withPartitionKey(partitionKey)
                .withData(deadbeef),
            new PutRecordsRequestEntry()
                .withPartitionKey(partitionKey)
                .withData(deadbeef)));
    PutRecordsResult putRecordsResult = kinesis.putRecords(putRecordsRequest);
    assertThat(putRecordsResult.getFailedRecordCount(), equalTo(0));
    assertThat(putRecordsResult.getRecords().size(), equalTo(2));
  }
}
