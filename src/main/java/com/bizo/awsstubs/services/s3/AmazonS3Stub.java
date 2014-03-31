package com.bizo.awsstubs.services.s3;

import java.io.*;
import java.net.URL;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.amazonaws.*;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.internal.RepeatableFileInputStream;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.Md5Utils;

public class AmazonS3Stub implements AmazonS3 {

  private final Map<String, BucketInfo> buckets = new TreeMap<String, BucketInfo>();
  private int maxKeys = 1000;

  public int getMaxKeys() {
    return maxKeys;
  }

  public void setMaxKeys(final int maxKeys) {
    this.maxKeys = maxKeys;
  }

  @Override
  public void setEndpoint(final String endpoint) {
  }

  @Override
  public ObjectListing listObjects(final String bucketName) {
    return listObjects(new ListObjectsRequest().withBucketName(bucketName));
  }

  @Override
  public ObjectListing listObjects(final String bucketName, final String prefix) {
    return listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix));
  }

  @Override
  public ObjectListing listNextBatchOfObjects(final ObjectListing p) {
    return listObjects(new ListObjectsRequest(
      p.getBucketName(),
      p.getPrefix(),
      p.getNextMarker(),
      p.getDelimiter(),
      p.getMaxKeys()));
  }

  @Override
  public ObjectListing listObjects(final ListObjectsRequest req) {
    final BucketInfo b = getBucketInfo(req.getBucketName());
    final ObjectListing l = new ObjectListing();
    l.setBucketName(req.getBucketName());
    l.setPrefix(req.getPrefix());
    l.setDelimiter(req.getDelimiter());
    l.setMarker(req.getMarker());
    l.setMaxKeys(req.getMaxKeys() == null ? 0 : req.getMaxKeys().intValue());
    for (final S3ObjectInfo object : b.objects) {
      // check marker
      if (req.getMarker() != null && req.getMarker().compareTo(object.key) > 0) {
        continue;
      }
      // check size
      if (l.getObjectSummaries().size() >= maxKeys
        || (req.getMaxKeys() != null && l.getMaxKeys() > 0 && l.getObjectSummaries().size() >= req.getMaxKeys())) {
        l.setNextMarker(object.key);
        l.setTruncated(true);
        break;
      }
      // check prefix
      if (req.getPrefix() != null && !object.key.startsWith(req.getPrefix())) {
        continue;
      }
      // check delimiter
      if (req.getDelimiter() != null) {
        // e.g. if prefix=/foo/ then /foo/bar => bar
        final String subkey;
        if (req.getPrefix() == null) {
          subkey = object.key;
        } else {
          subkey = object.key.substring(req.getPrefix().length());
        }
        if (subkey.contains(req.getDelimiter())) {
          final String prefix =
            object.key.substring(0, object.key.indexOf(req.getDelimiter(), req.getPrefix().length()) + 1);
          if (!l.getCommonPrefixes().contains(prefix)) {
            l.getCommonPrefixes().add(prefix);
          }
          continue;
        }
      }
      l.getObjectSummaries().add(object.summary(b.bucket.getName()));
    }
    return l;
  }

  private BucketInfo getBucketInfo(final String name) {
    final BucketInfo info = getBucketInfoOrNull(name);
    if (info == null) {
      final AmazonServiceException e = new AmazonServiceException("The specified bucket does not exist");
      e.setStatusCode(404);
      e.setErrorType(ErrorType.Client);
      e.setServiceName("Amazon S3");
      e.setErrorCode("NoSuchBucket");
      throw e;
    }
    return info;
  }

  private BucketInfo getBucketInfoOrNull(final String name) {
    return buckets.get(name);
  }

  @Override
  public void changeObjectStorageClass(final String bucketName, final String key, final StorageClass newStorageClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VersionListing listVersions(final String bucketName, final String prefix) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VersionListing listNextBatchOfVersions(final VersionListing previousVersionListing) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VersionListing listVersions(
      final String bucketName,
      final String prefix,
      final String keyMarker,
      final String versionIdMarker,
      final String delimiter,
      final Integer maxResults) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VersionListing listVersions(final ListVersionsRequest listVersionsRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Owner getS3AccountOwner() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean doesBucketExist(final String bucketName) {
    return getBucketInfoOrNull(bucketName) != null;
  }

  @Override
  public List<Bucket> listBuckets() {
    return listBuckets(new ListBucketsRequest());
  }

  @Override
  public List<Bucket> listBuckets(final ListBucketsRequest listBucketsRequest) {
    final List<Bucket> copies = new ArrayList<Bucket>();
    for (final BucketInfo bucket : buckets.values()) {
      copies.add(bucket.copy());
    }
    return copies;
  }

  @Override
  public String getBucketLocation(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getBucketLocation(final GetBucketLocationRequest getBucketLocationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Bucket createBucket(final String bucketName) {
    return createBucket(new CreateBucketRequest(bucketName));
  }

  @Override
  public Bucket createBucket(final String bucketName, final Region region) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Bucket createBucket(final String bucketName, final String region) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Bucket createBucket(final CreateBucketRequest req) {
    BucketInfo bucket = getBucketInfoOrNull(req.getBucketName());
    if (bucket == null) {
      bucket = new BucketInfo(new Bucket(req.getBucketName()));
      buckets.put(req.getBucketName(), bucket);
    }
    return bucket.copy();
  }

  @Override
  public AccessControlList getObjectAcl(final String bucketName, final String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AccessControlList getObjectAcl(final String bucketName, final String key, final String versionId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setObjectAcl(final String bucketName, final String key, final AccessControlList acl) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setObjectAcl(final String bucketName, final String key, final CannedAccessControlList acl) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setObjectAcl(
      final String bucketName,
      final String key,
      final String versionId,
      final AccessControlList acl) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setObjectAcl(
      final String bucketName,
      final String key,
      final String versionId,
      final CannedAccessControlList acl) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AccessControlList getBucketAcl(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketAcl(final SetBucketAclRequest setBucketAclRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AccessControlList getBucketAcl(final GetBucketAclRequest getBucketAclRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketAcl(final String bucketName, final AccessControlList acl) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketAcl(final String bucketName, final CannedAccessControlList acl) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ObjectMetadata getObjectMetadata(final String bucketName, final String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ObjectMetadata getObjectMetadata(final GetObjectMetadataRequest getObjectMetadataRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public S3Object getObject(final String bucketName, final String key) {
    return getObject(new GetObjectRequest(bucketName, key));
  }

  @Override
  public ObjectMetadata getObject(final GetObjectRequest req, final File destinationFile) {
    final S3Object object = getObject(req);
    try {
      FileUtils.writeByteArrayToFile(destinationFile, IOUtils.toByteArray(object.getObjectContent()));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return object.getObjectMetadata();
  }

  @Override
  public S3Object getObject(final GetObjectRequest req) {
    final BucketInfo bucket = getBucketInfo(req.getBucketName());
    final S3ObjectInfo object = bucket.getObject(req.getKey());
    return object.object(req.getBucketName());
  }

  @Override
  public void deleteBucket(final DeleteBucketRequest deleteBucketRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBucket(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  public PutObjectResult putObject(final String bucketName, final String key) {
    return putObject(bucketName, key, key);
  }

  public PutObjectResult putObject(final String bucketName, final String key, final String content) {
    final ObjectMetadata meta = new ObjectMetadata();
    meta.setContentType("text/plain");
    meta.setContentLength(content.length());
    return putObject(new PutObjectRequest(bucketName, key, new ByteArrayInputStream(content.getBytes()), meta));
  }

  @Override
  public PutObjectResult putObject(final String bucketName, final String key, final File file) {
    return putObject(new PutObjectRequest(bucketName, key, file));
  }

  @Override
  public PutObjectResult putObject(
      final String bucketName,
      final String key,
      final InputStream input,
      final ObjectMetadata metadata) {
    return putObject(new PutObjectRequest(bucketName, key, input, metadata));
  }

  @Override
  public PutObjectResult putObject(final PutObjectRequest req) {
    ObjectMetadata metadata = req.getMetadata();
    if (metadata == null) {
      metadata = new ObjectMetadata();
    }
    InputStream input = req.getInputStream();

    // If a file is specified for upload, we need to pull some additional
    // information from it to auto-configure a few options
    if (req.getFile() != null) {
      final File file = req.getFile();

      // Always set the content length, even if it's already set
      metadata.setContentLength(file.length());

      // Only set the content type if it hasn't already been set
      if (metadata.getContentType() == null) {
        metadata.setContentType(Mimetypes.getInstance().getMimetype(file));
      }

      FileInputStream fileInputStream = null;
      try {
        fileInputStream = new FileInputStream(file);
        final byte[] md5Hash = Md5Utils.computeMD5Hash(fileInputStream);
        metadata.setContentMD5(BinaryUtils.toBase64(md5Hash));
      } catch (final Exception e) {
        throw new AmazonClientException("Unable to calculate MD5 hash: " + e.getMessage(), e);
      } finally {
        try {
          fileInputStream.close();
        } catch (final Exception e) {
        }
      }

      try {
        input = new RepeatableFileInputStream(file);
      } catch (final FileNotFoundException fnfe) {
        throw new AmazonClientException("Unable to find file to upload", fnfe);
      }
    }

    final BucketInfo bucket = getBucketInfo(req.getBucketName());
    for (final S3ObjectInfo object : bucket.objects) {
      if (object.key.equals(req.getKey())) {
        object.setData(input, metadata);
        return new PutObjectResult();
      }
    }

    // make a new one
    final S3ObjectInfo object = new S3ObjectInfo(req.getKey());
    object.setData(input, metadata);
    bucket.objects.add(object);
    return new PutObjectResult();
  }

  @Override
  public CopyObjectResult copyObject(
      final String sourceBucketName,
      final String sourceKey,
      final String destinationBucketName,
      final String destinationKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CopyObjectResult copyObject(final CopyObjectRequest copyObjectRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CopyPartResult copyPart(final CopyPartRequest copyPartRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteObject(final String bucketName, final String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteObject(final DeleteObjectRequest deleteObjectRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DeleteObjectsResult deleteObjects(final DeleteObjectsRequest deleteObjectsRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteVersion(final String bucketName, final String key, final String versionId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteVersion(final DeleteVersionRequest deleteVersionRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketLoggingConfiguration getBucketLoggingConfiguration(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketLoggingConfiguration(
      final SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketVersioningConfiguration getBucketVersioningConfiguration(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketVersioningConfiguration(
      final SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketLifecycleConfiguration(
      final String bucketName,
      final BucketLifecycleConfiguration bucketLifecycleConfiguration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBucketLifecycleConfiguration(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketNotificationConfiguration getBucketNotificationConfiguration(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketNotificationConfiguration(
      final String bucketName,
      final BucketNotificationConfiguration bucketNotificationConfiguration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(
      final GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketWebsiteConfiguration(final String bucketName, final BucketWebsiteConfiguration configuration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketWebsiteConfiguration(
      final SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBucketWebsiteConfiguration(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBucketWebsiteConfiguration(
      final DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketPolicy getBucketPolicy(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketPolicy getBucketPolicy(final GetBucketPolicyRequest getBucketPolicyRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketPolicy(final String bucketName, final String policyText) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketPolicy(final SetBucketPolicyRequest setBucketPolicyRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBucketPolicy(final String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBucketPolicy(final DeleteBucketPolicyRequest deleteBucketPolicyRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL generatePresignedUrl(final String bucketName, final String key, final Date expiration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL generatePresignedUrl(
      final String bucketName,
      final String key,
      final Date expiration,
      final HttpMethod method) {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL generatePresignedUrl(final GeneratePresignedUrlRequest generatePresignedUrlRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InitiateMultipartUploadResult initiateMultipartUpload(final InitiateMultipartUploadRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UploadPartResult uploadPart(final UploadPartRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PartListing listParts(final ListPartsRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abortMultipartUpload(final AbortMultipartUploadRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompleteMultipartUploadResult completeMultipartUpload(final CompleteMultipartUploadRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MultipartUploadListing listMultipartUploads(final ListMultipartUploadsRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public S3ResponseMetadata getCachedResponseMetadata(final AmazonWebServiceRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (final BucketInfo bucket : buckets.values()) {
      sb.append(bucket.bucket.getName()).append("\n");
      int i = 0;
      for (final S3ObjectInfo object : bucket.objects) {
        sb.append("  " + object.key).append("\n");
        if (i++ > 50) {
          break;
        }
      }
    }
    return sb.toString();
  }

  private static class BucketInfo implements Comparable<BucketInfo> {
    private final Bucket bucket;
    private final Set<S3ObjectInfo> objects = new TreeSet<AmazonS3Stub.S3ObjectInfo>();

    private BucketInfo(final Bucket bucket) {
      this.bucket = bucket;
    }

    @Override
    public int hashCode() {
      return bucket.getName().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
      return other instanceof BucketInfo && ((BucketInfo) other).bucket.getName().equals(bucket.getName());
    }

    @Override
    public int compareTo(final BucketInfo o) {
      return bucket.getName().compareTo(o.bucket.getName());
    }

    @Override
    public String toString() {
      return bucket.getName();
    }

    private Bucket copy() {
      final Bucket copy = new Bucket(bucket.getName());
      copy.setCreationDate(bucket.getCreationDate());
      copy.setOwner(bucket.getOwner());
      return copy;
    }

    private S3ObjectInfo getObjectOrNull(final String key) {
      for (final S3ObjectInfo object : objects) {
        if (object.key.equals(key)) {
          return object;
        }
      }
      return null;
    }

    private S3ObjectInfo getObject(final String key) {
      final S3ObjectInfo info = getObjectOrNull(key);
      if (info == null) {
        final AmazonServiceException e = new AmazonServiceException("The specified key does not exist");
        e.setErrorCode("NoSuchKey");
        e.setErrorType(ErrorType.Client);
        e.setServiceName("Amazon S3");
        e.setStatusCode(404);
        throw e;
      }
      return info;
    }
  }

  private static class S3ObjectInfo implements Comparable<S3ObjectInfo> {
    private final String key;
    private ObjectMetadata metadata;
    private byte[] data;

    private S3ObjectInfo(final String key) {
      this.key = key;
    }

    private S3ObjectSummary summary(final String bucketName) {
      final S3ObjectSummary s = new S3ObjectSummary();
      s.setBucketName(bucketName);
      s.setKey(key);
      s.setSize(data.length);
      s.setLastModified(metadata.getLastModified());
      return s;
    }

    private S3Object object(final String bucketName) {
      final S3Object s = new S3Object();
      s.setBucketName(bucketName);
      s.setKey(key);
      s.setObjectContent(new ByteArrayInputStream(data));
      s.setObjectMetadata(copy(metadata));
      return s;
    }

    @Override
    public int hashCode() {
      return key.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
      return other instanceof S3ObjectInfo && ((S3ObjectInfo) other).key.equals(key);
    }

    @Override
    public int compareTo(final S3ObjectInfo o) {
      return key.compareTo(o.key);
    }

    @Override
    public String toString() {
      return key;
    }

    private void setData(final InputStream s, final ObjectMetadata metadata) {
      try {
        data = IOUtils.toByteArray(s);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      // warn if the user's content length doesn't match?
      this.metadata = copy(metadata);
      this.metadata.setContentLength(data.length);
    }
  }

  private static ObjectMetadata copy(final ObjectMetadata meta) {
    final ObjectMetadata copy = new ObjectMetadata();
    if (meta == null) {
      return copy;
    }
    copy.setUserMetadata(meta.getUserMetadata());
    copy.setExpirationTime(meta.getExpirationTime());
    copy.setExpirationTimeRuleId(meta.getExpirationTimeRuleId());
    copy.setLastModified(meta.getLastModified());
    copy.setContentLength(meta.getContentLength());
    copy.setContentType(meta.getContentType());
    copy.setContentEncoding(meta.getContentEncoding());
    copy.setCacheControl(meta.getCacheControl());
    copy.setContentMD5(meta.getContentMD5());
    copy.setContentDisposition(meta.getContentDisposition());
    // copy.setETag
    // copy.setVersionId
    copy.setServerSideEncryption(meta.getServerSideEncryption());
    return copy;
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(final String arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBucketTaggingConfiguration(final String arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(final String arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketTaggingConfiguration getBucketTaggingConfiguration(final String arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void restoreObject(final RestoreObjectRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void restoreObject(final String arg0, final String arg1, final int arg2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketCrossOriginConfiguration(final String arg0, final BucketCrossOriginConfiguration arg1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketTaggingConfiguration(final String arg0, final BucketTaggingConfiguration arg1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setObjectRedirectLocation(final String arg0, final String arg1, final String arg2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRegion(final com.amazonaws.regions.Region arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setS3ClientOptions(final S3ClientOptions arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketLifecycleConfiguration(
      final SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBucketLifecycleConfiguration(
      final DeleteBucketLifecycleConfigurationRequest deleteBucketLifecycleConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketCrossOriginConfiguration(
      final SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(
      final DeleteBucketCrossOriginConfigurationRequest deleteBucketCrossOriginConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketTaggingConfiguration(
      final SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBucketTaggingConfiguration(
      final DeleteBucketTaggingConfigurationRequest deleteBucketTaggingConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBucketNotificationConfiguration(
      final SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest) {
    throw new UnsupportedOperationException();
  }

}
