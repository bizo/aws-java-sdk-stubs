package com.bizo.awsstubs.services.kinesis;

import com.amazonaws.services.kinesis.model.*;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.junit.rules.ExpectedException;

import javax.xml.bind.DatatypeConverter;

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
  public void putRecords() {
    String streamName = "testStream";
    ByteBuffer deadbeef = ByteBuffer.wrap(DatatypeConverter.parseHexBinary("DEADBEEF"));
    ByteBuffer icebooda = ByteBuffer.wrap(DatatypeConverter.parseHexBinary("1CEB00DA"));
    String partitionKey = "foo";
    kinesis.createStream(streamName, 1);
    PutRecordsResult putRecordsResult = kinesis.putRecords(
        new PutRecordsRequest()
            .withStreamName(streamName)
            .withRecords(Arrays.asList(
                new PutRecordsRequestEntry()
                    .withPartitionKey(partitionKey)
                    .withData(deadbeef),
                new PutRecordsRequestEntry()
                    .withPartitionKey(partitionKey)
                    .withData(deadbeef))));
    assertThat(putRecordsResult.getFailedRecordCount(), equalTo(0));
    assertThat(putRecordsResult.getRecords().size(), equalTo(2));
  }
}
