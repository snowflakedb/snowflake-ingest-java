package net.snowflake.ingest.streaming.internal;

import java.util.Arrays;
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

    Pair<String, CompletableFuture<BlobMetadata>> blobFuture =
        new Pair<>("test", CompletableFuture.completedFuture(new BlobMetadata("path", null)));
    rs.addBlobs(Arrays.asList(blobFuture));
    Assert.assertEquals(1, rs.getBlobsList().size());
    List<String> errorBlobs = rs.registerBlobs();
    Assert.assertEquals(0, rs.getBlobsList().size());
    Assert.assertEquals(0, errorBlobs.size());
  }

  @Test
  public void testRegisterServiceTimeoutException() throws Exception {
    RegisterService rs = new RegisterService(null, true);

    Pair<String, CompletableFuture<BlobMetadata>> blobFuture1 =
        new Pair<>("success", CompletableFuture.completedFuture(new BlobMetadata("path", null)));
    Pair<String, CompletableFuture<BlobMetadata>> blobFuture2 =
        new Pair<>("fail", CompletableFuture.failedFuture(new TimeoutException()));
    rs.addBlobs(Arrays.asList(blobFuture1, blobFuture2));
    Assert.assertEquals(2, rs.getBlobsList().size());
    try {
      List<String> errorBlobs = rs.registerBlobs();
      Assert.assertEquals(0, rs.getBlobsList().size());
      Assert.assertEquals(1, errorBlobs.size());
      Assert.assertEquals("fail", errorBlobs.get(0));
    } catch (Exception e) {
      Assert.fail("The timeout exception should be caught in registerBlobs");
    }
  }
}
