/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_UPLOAD_TIMEOUT_IN_SEC;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.snowflake.ingest.utils.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RegisterServiceTest {
  @Parameterized.Parameters(name = "isIcebergMode: {0}")
  public static Object[] isIcebergMode() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public boolean isIcebergMode;

  @Test
  public void testRegisterService() throws ExecutionException, InterruptedException {
    RegisterService<StubChunkData> rs = new RegisterService<>(null, true);

    Pair<FlushService.BlobData<StubChunkData>, CompletableFuture<BlobMetadata>> blobFuture =
        new Pair<>(
            new FlushService.BlobData<>("test", null),
            CompletableFuture.completedFuture(new BlobMetadata("path", "md5", null, null)));
    rs.addBlobs(Collections.singletonList(blobFuture));
    Assert.assertEquals(1, rs.getBlobsList().size());
    Assert.assertEquals(false, blobFuture.getValue().get().getSpansMixedTables());
    List<FlushService.BlobData<StubChunkData>> errorBlobs = rs.registerBlobs(null);
    Assert.assertEquals(0, rs.getBlobsList().size());
    Assert.assertEquals(0, errorBlobs.size());
  }

  /**
   * Note that this exception will not perform retries since the completeExceptionally throws the
   * exception by wrapping inside ExecutionException. Check method {@link
   * CompletableFuture#get(long, TimeUnit)} javadocs
   *
   * <p>The check for retries checks the original exception instead of the exception.getCause()
   *
   * @throws Exception
   */
  @Test
  public void testRegisterServiceTimeoutException() throws Exception {
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("client", isIcebergMode);
    RegisterService<StubChunkData> rs = new RegisterService<>(client, true);

    Pair<FlushService.BlobData<StubChunkData>, CompletableFuture<BlobMetadata>> blobFuture1 =
        new Pair<>(
            new FlushService.BlobData<>("success", new ArrayList<>()),
            CompletableFuture.completedFuture(new BlobMetadata("path", "md5", null, null)));
    CompletableFuture future = new CompletableFuture();
    future.completeExceptionally(new TimeoutException());
    Pair<FlushService.BlobData<StubChunkData>, CompletableFuture<BlobMetadata>> blobFuture2 =
        new Pair<>(new FlushService.BlobData<StubChunkData>("fail", new ArrayList<>()), future);
    rs.addBlobs(Arrays.asList(blobFuture1, blobFuture2));
    Assert.assertEquals(2, rs.getBlobsList().size());
    try {
      List<FlushService.BlobData<StubChunkData>> errorBlobs = rs.registerBlobs(null);
      Assert.assertEquals(0, rs.getBlobsList().size());
      Assert.assertEquals(1, errorBlobs.size());
      Assert.assertEquals("fail", errorBlobs.get(0).getPath());
    } catch (Exception e) {
      Assert.fail("The timeout exception should be caught in registerBlobs");
    }
  }

  // Ignore since it runs for BLOB_UPLOAD_TIMEOUT_IN_SEC * BLOB_UPLOAD_MAX_RETRY_COUNT
  @Ignore
  @Test
  public void testRegisterServiceTimeoutException_testRetries() throws Exception {
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("client", isIcebergMode);
    RegisterService<StubChunkData> rs = new RegisterService<>(client, true);

    Pair<FlushService.BlobData<StubChunkData>, CompletableFuture<BlobMetadata>> blobFuture1 =
        new Pair<>(
            new FlushService.BlobData<>("success", new ArrayList<>()),
            CompletableFuture.completedFuture(new BlobMetadata("path", "md5", null, null)));
    CompletableFuture future = new CompletableFuture();
    future.thenRunAsync(
        () -> {
          try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(BLOB_UPLOAD_TIMEOUT_IN_SEC) + 5);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          return;
        });
    Pair<FlushService.BlobData<StubChunkData>, CompletableFuture<BlobMetadata>> blobFuture2 =
        new Pair<>(new FlushService.BlobData<StubChunkData>("fail", new ArrayList<>()), future);
    rs.addBlobs(Arrays.asList(blobFuture1, blobFuture2));
    Assert.assertEquals(2, rs.getBlobsList().size());
    try {
      List<FlushService.BlobData<StubChunkData>> errorBlobs = rs.registerBlobs(null);
      Assert.assertEquals(0, rs.getBlobsList().size());
      Assert.assertEquals(1, errorBlobs.size());
      Assert.assertEquals("fail", errorBlobs.get(0).getPath());
    } catch (Exception e) {
      Assert.fail("The timeout exception should be caught in registerBlobs");
    }
  }

  @Test
  public void testRegisterServiceNonTimeoutException() {
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("client", isIcebergMode);
    RegisterService<StubChunkData> rs = new RegisterService<>(client, true);

    CompletableFuture<BlobMetadata> future = new CompletableFuture<>();
    future.completeExceptionally(new IndexOutOfBoundsException());
    Pair<FlushService.BlobData<StubChunkData>, CompletableFuture<BlobMetadata>> blobFuture =
        new Pair<>(new FlushService.BlobData<>("fail", new ArrayList<>()), future);
    rs.addBlobs(Collections.singletonList(blobFuture));
    Assert.assertEquals(1, rs.getBlobsList().size());
    try {
      List<FlushService.BlobData<StubChunkData>> errorBlobs = rs.registerBlobs(null);
      Assert.assertEquals(0, rs.getBlobsList().size());
      Assert.assertEquals(1, errorBlobs.size());
      Assert.assertEquals("fail", errorBlobs.get(0).getPath());
    } catch (Exception e) {
      Assert.fail("The exception should be caught in registerBlobs");
    }
  }
}
