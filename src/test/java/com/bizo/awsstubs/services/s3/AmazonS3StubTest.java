package com.bizo.awsstubs.services.s3;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.*;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.s3.model.*;

public class AmazonS3StubTest {

  private final AmazonS3Stub s3 = new AmazonS3Stub();

  @Test
  public void createBucketForNewBucket() {
    s3.createBucket("foo");
    assertThat(s3.listBuckets().size(), is(1));
  }

  @Test
  public void createBucketForExistingBucket() {
    s3.createBucket("foo");
    s3.createBucket("foo");
    assertThat(s3.listBuckets().size(), is(1));
  }

  @Test
  public void listObjectsFailsIfNoBucket() {
    try {
      s3.listObjects("b");
    } catch (final AmazonServiceException e) {
      assertThat(e.getStatusCode(), is(404));
      assertThat(e.getErrorCode(), is("NoSuchBucket"));
      assertThat(e.getErrorType(), is(ErrorType.Client));
    }
  }

  @Test
  public void listObjectsWithNoObjects() {
    s3.createBucket("b");
    final ObjectListing l = s3.listObjects("b");
    assertThat(l.getObjectSummaries().size(), is(0));
    assertThat(l.getBucketName(), is("b"));
    assertThat(l.getDelimiter(), is(nullValue()));
    assertThat(l.getMarker(), is(nullValue()));
    assertThat(l.getNextMarker(), is(nullValue()));
    assertThat(l.isTruncated(), is(false));
  }

  @Test
  public void listObjectsWithOneObject() {
    s3.createBucket("b");
    s3.putObject("b", "key1");
    final ObjectListing l = s3.listObjects("b");
    assertThat(l.getObjectSummaries().size(), is(1));
    final S3ObjectSummary summary = l.getObjectSummaries().get(0);
    assertThat(summary.getKey(), is("key1"));
  }

  @Test
  public void listObjectsWithPrefix() {
    s3.createBucket("b");
    s3.putObject("b", "bar");
    s3.putObject("b", "zaz");
    final ObjectListing l = s3.listObjects("b", "ba");
    assertThat(l.getObjectSummaries().size(), is(1));
    assertThat(l.getObjectSummaries().get(0).getKey(), is("bar"));
  }

  @Test
  public void listObjectsWithDelimiter() {
    s3.createBucket("b");
    s3.putObject("b", "bart");
    s3.putObject("b", "bar/");
    s3.putObject("b", "bar/a");
    final ObjectListing l = s3.listObjects(new ListObjectsRequest("b", "ba", null, "/", null));
    assertThat(l.getObjectSummaries().size(), is(1));
    assertThat(l.getObjectSummaries().get(0).getKey(), is("bart"));
    assertThat(l.getCommonPrefixes().size(), is(1));
    assertThat(l.getCommonPrefixes().get(0), is("bar/"));
  }

  @Test
  public void listObjectsWithNestedDelimiter() {
    s3.createBucket("b");
    s3.putObject("b", "bar/bart");
    s3.putObject("b", "bar/bar/");
    s3.putObject("b", "bar/bar/a");
    final ObjectListing l = s3.listObjects(new ListObjectsRequest("b", "bar/", null, "/", null));
    assertThat(l.getObjectSummaries().size(), is(1));
    assertThat(l.getObjectSummaries().get(0).getKey(), is("bar/bart"));
    assertThat(l.getCommonPrefixes().size(), is(1));
    assertThat(l.getCommonPrefixes().get(0), is("bar/bar/"));
  }

  @Test
  public void listObjectsWithLimit() {
    s3.createBucket("b");
    s3.putObject("b", "a");
    s3.putObject("b", "b");
    s3.putObject("b", "c");
    s3.putObject("b", "d");
    s3.putObject("b", "e");
    s3.setMaxKeys(2);
    final ObjectListing l1 = s3.listObjects("b");
    assertThat(l1.isTruncated(), is(true));
    assertThat(l1.getObjectSummaries().size(), is(2));
    assertThat(l1.getObjectSummaries().get(0).getKey(), is("a"));
    assertThat(l1.getObjectSummaries().get(1).getKey(), is("b"));
    assertThat(l1.getNextMarker(), is("c"));
    final ObjectListing l2 = s3.listNextBatchOfObjects(l1);
    assertThat(l2.isTruncated(), is(true));
    assertThat(l2.getObjectSummaries().size(), is(2));
    assertThat(l2.getObjectSummaries().get(0).getKey(), is("c"));
    assertThat(l2.getObjectSummaries().get(1).getKey(), is("d"));
    assertThat(l2.getNextMarker(), is("e"));
    final ObjectListing l3 = s3.listNextBatchOfObjects(l2);
    assertThat(l3.isTruncated(), is(false));
    assertThat(l3.getObjectSummaries().size(), is(1));
    assertThat(l3.getObjectSummaries().get(0).getKey(), is("e"));
    assertThat(l3.getNextMarker(), is(nullValue()));
  }

  @Test
  public void listObjectsWithRequestLimit() {
    s3.createBucket("b");
    s3.putObject("b", "a");
    s3.putObject("b", "b");
    s3.putObject("b", "c");
    s3.putObject("b", "d");
    s3.putObject("b", "e");
    final ObjectListing l1 = s3.listObjects(new ListObjectsRequest().withBucketName("b").withMaxKeys(2));
    assertThat(l1.isTruncated(), is(true));
    assertThat(l1.getObjectSummaries().size(), is(2));
    assertThat(l1.getObjectSummaries().get(0).getKey(), is("a"));
    assertThat(l1.getObjectSummaries().get(1).getKey(), is("b"));
    final ObjectListing l2 = s3.listNextBatchOfObjects(l1);
    assertThat(l2.isTruncated(), is(true));
    assertThat(l2.getObjectSummaries().size(), is(2));
    assertThat(l2.getObjectSummaries().get(0).getKey(), is("c"));
    assertThat(l2.getObjectSummaries().get(1).getKey(), is("d"));
    final ObjectListing l3 = s3.listNextBatchOfObjects(l2);
    assertThat(l3.isTruncated(), is(false));
    assertThat(l3.getObjectSummaries().size(), is(1));
    assertThat(l3.getObjectSummaries().get(0).getKey(), is("e"));
  }

  @Test
  public void putObjectWithDummyData() throws Exception {
    s3.createBucket("b");
    s3.putObject("b", "a");
    final ObjectListing l1 = s3.listObjects("b");
    assertThat(l1.getObjectSummaries().get(0).getKey(), is("a"));
    final S3Object a = s3.getObject("b", "a");
    assertThat(IOUtils.toString(a.getObjectContent()), is("a"));
    
    s3.putObject("b", "c", "z");
    final S3Object c = s3.getObject("b", "c");
    assertThat(IOUtils.toString(c.getObjectContent()), is("z"));
  }

  @Test
  public void putObjectWithMetaFillsInSize() {
    s3.createBucket("b");
    s3.putObject("b", "a", key1Stream(), key1Data());
    final ObjectListing l1 = s3.listObjects("b");
    assertThat(l1.getObjectSummaries().get(0).getSize(), is(4L));
  }

  @Test
  public void putObjectFromFile() throws Exception {
    s3.createBucket("b");
    File z = File.createTempFile("zzz", ".tmp");
    try {
      Writer writer = new BufferedWriter(new FileWriter(z));
      writer.write("z");
      writer.close();

      s3.putObject("b", "c", z);

      final S3Object c = s3.getObject("b", "c");
      assertThat(IOUtils.toString(c.getObjectContent()), is("z"));
    } finally {
      z.delete();
    }
  }

  @Test
  public void getObjectWithInvalidBucket() {
    try {
      s3.getObject("b", "a");
    } catch (final AmazonServiceException e) {
      assertThat(e.getStatusCode(), is(404));
      assertThat(e.getErrorCode(), is("NoSuchBucket"));
      assertThat(e.getErrorType(), is(ErrorType.Client));
    }
  }

  @Test
  public void getObjectWithInvalidKey() {
    s3.createBucket("b");
    try {
      s3.getObject("b", "a");
    } catch (final AmazonServiceException e) {
      assertThat(e.getStatusCode(), is(404));
      assertThat(e.getErrorCode(), is("NoSuchKey"));
      assertThat(e.getErrorType(), is(ErrorType.Client));
    }
  }

  @Test
  public void getObject() throws Exception {
    s3.createBucket("b");
    s3.putObject("b", "a", key1Stream(), key1Data());
    final S3Object a = s3.getObject("b", "a");
    assertThat(IOUtils.toString(a.getObjectContent()), is("data"));
    assertThat(a.getObjectMetadata().getContentType(), is("text/plain"));
    assertThat(a.getObjectMetadata().getContentLength(), is(4L));
  }

  private static final ByteArrayInputStream key1Stream() {
    return new ByteArrayInputStream("data".getBytes());
  }

  private static final ObjectMetadata key1Data() {
    final ObjectMetadata meta = new ObjectMetadata();
    meta.setContentType("text/plain");
    return meta;
  }
}
