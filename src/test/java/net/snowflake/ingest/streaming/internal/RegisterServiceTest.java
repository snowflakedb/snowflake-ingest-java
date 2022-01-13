package net.snowflake.ingest.streaming.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import net.snowflake.ingest.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

public class RegisterServiceTest {

  @Test
  public void testRegisterService() throws Exception {
    RegisterService rs = new RegisterService(null, true);

    Pair<FlushService.BlobData, CompletableFuture<BlobMetadata>> blobFuture =
        new Pair<>(
            new FlushService.BlobData("test", null),
            CompletableFuture.completedFuture(new BlobMetadata("path", "md5", null)));
    rs.addBlobs(Collections.singletonList(blobFuture));
    Assert.assertEquals(1, rs.getBlobsList().size());
    List<FlushService.BlobData> errorBlobs = rs.registerBlobs(null);
    Assert.assertEquals(0, rs.getBlobsList().size());
    Assert.assertEquals(0, errorBlobs.size());
  }

  @Test
  public void testRegisterServiceTimeoutException() throws Exception {
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal("client");
    RegisterService rs = new RegisterService(client, true);

    Pair<FlushService.BlobData, CompletableFuture<BlobMetadata>> blobFuture1 =
        new Pair<>(
            new FlushService.BlobData("success", new ArrayList<>()),
            CompletableFuture.completedFuture(new BlobMetadata("path", "md5", null)));
    CompletableFuture future = new CompletableFuture();
    future.completeExceptionally(new TimeoutException());
    Pair<FlushService.BlobData, CompletableFuture<BlobMetadata>> blobFuture2 =
        new Pair<>(new FlushService.BlobData("fail", new ArrayList<>()), future);
    rs.addBlobs(Arrays.asList(blobFuture1, blobFuture2));
    Assert.assertEquals(2, rs.getBlobsList().size());
    try {
      List<FlushService.BlobData> errorBlobs = rs.registerBlobs(null);
      Assert.assertEquals(0, rs.getBlobsList().size());
      Assert.assertEquals(1, errorBlobs.size());
      Assert.assertEquals("fail", errorBlobs.get(0).getFilePath());
    } catch (Exception e) {
      Assert.fail("The timeout exception should be caught in registerBlobs");
    }
  }

  @Test
  public void testRegisterServiceNonTimeoutException() {
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal("client");
    RegisterService rs = new RegisterService(client, true);

    CompletableFuture future = new CompletableFuture();
    future.completeExceptionally(new IndexOutOfBoundsException());
    Pair<FlushService.BlobData, CompletableFuture<BlobMetadata>> blobFuture =
        new Pair<>(new FlushService.BlobData("fail", new ArrayList<>()), future);
    rs.addBlobs(Collections.singletonList(blobFuture));
    Assert.assertEquals(1, rs.getBlobsList().size());
    try {
      List<FlushService.BlobData> errorBlobs = rs.registerBlobs(null);
      Assert.assertEquals(0, rs.getBlobsList().size());
      Assert.assertEquals(1, errorBlobs.size());
      Assert.assertEquals("fail", errorBlobs.get(0).getFilePath());
    } catch (Exception e) {
      Assert.fail("The exception should be caught in registerBlobs");
    }
  }
}
