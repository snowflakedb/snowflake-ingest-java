/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import net.snowflake.ingest.utils.SFException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExternalVolumeManagerTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private ExternalVolumeManager manager;
  private FileLocationInfo fileLocationInfo;
  private ExecutorService executorService;

  @Before
  public void setup() throws JsonProcessingException {
    this.manager =
        new ExternalVolumeManager(
            false /* isTestMode */, "role", "clientName", MockSnowflakeServiceClient.create());

    Map<String, Object> fileLocationInfoMap = MockSnowflakeServiceClient.getStageLocationMap();
    fileLocationInfoMap.put("isClientSideEncrypted", false);
    String fileLocationInfoStr = objectMapper.writeValueAsString(fileLocationInfoMap);
    this.fileLocationInfo = objectMapper.readValue(fileLocationInfoStr, FileLocationInfo.class);
  }

  @After
  public void teardown() {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testRegister() {
    Exception ex = null;

    try {
      this.manager.registerTable(new TableRef("db", "schema", "table"), fileLocationInfo);
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
  }

  @Test
  public void testConcurrentRegisterTable() throws Exception {
    int numThreads = 50;
    int timeoutInSeconds = 30;
    List<Future<ExternalVolume>> allResults = doConcurrentTest(
        numThreads,
        timeoutInSeconds,
        () -> manager.registerTable(new TableRef("db", "schema", "table"), fileLocationInfo),
        () -> manager.getStorage("db.schema.table"));
    ExternalVolume extvol = manager.getStorage("db.schema.table");
    assertNotNull(extvol);
    for (int i = 0; i < numThreads; i++) {
      assertSame("" + i, extvol, allResults.get(i).get(timeoutInSeconds, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testGetStorage() {
    this.manager.registerTable(new TableRef("db", "schema", "table"), fileLocationInfo);
    ExternalVolume extvol = this.manager.getStorage("db.schema.table");
    assertNotNull(extvol);
  }

  @Test
  public void testGetStorageWithoutRegister() {
    SFException ex = null;
    try {
      manager.getStorage("db.schema.table");
    } catch (SFException e) {
      ex = e;
    }

    assertNotNull(ex);
    assertTrue(ex.getVendorCode().equals("0001"));
    assertTrue(ex.getMessage().contains("No external volume found for tableRef=db.schema.table"));
  }

  @Test
  public void testGenerateBlobPath() {
    manager.registerTable(new TableRef("db", "schema", "table"), fileLocationInfo);
    BlobPath blobPath = manager.generateBlobPath("db.schema.table");
    assertNotNull(blobPath);
    assertTrue(blobPath.hasToken);
    assertEquals(blobPath.fileName, "f1");
    assertEquals(blobPath.blobPath, "http://f1.com?token=t1");
  }

  @Test
  public void testConcurrentGenerateBlobPath() throws Exception {
    int numThreads = 50;
    int timeoutInSeconds = 60;
    manager.registerTable(new TableRef("db", "schema", "table"), fileLocationInfo);

    List<Future<BlobPath>> allResults = doConcurrentTest(
        numThreads,
        timeoutInSeconds,
        () -> {
          for (int i = 0; i < 1000; i++) {
            manager.generateBlobPath("db.schema.table");
          }
        },
        () -> manager.generateBlobPath("db.schema.table"));
    for (int i = 0; i < numThreads; i++) {
      BlobPath blobPath = allResults.get(0).get(timeoutInSeconds, TimeUnit.SECONDS);
      assertNotNull(blobPath);
      assertTrue(blobPath.hasToken);
      assertTrue(blobPath.blobPath, blobPath.blobPath.contains( "http://f1.com?token=t"));
    }
  }

  private <T> List<Future<T>> doConcurrentTest(int numThreads, int timeoutInSeconds, Runnable action, Supplier<T> getResult) throws Exception {
    assertNull(executorService);

    executorService =
        new ThreadPoolExecutor(
            numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    List<Callable<T>> tasks = new ArrayList<>();
    final CyclicBarrier startBarrier = new CyclicBarrier(numThreads);
    final CyclicBarrier endBarrier = new CyclicBarrier(numThreads);
    for (int i = 0; i < numThreads; i++) {
      tasks.add(
          () -> {
            startBarrier.await(timeoutInSeconds, TimeUnit.SECONDS);
            action.run();
            endBarrier.await();
            return getResult.get();
          });
    }

    List<Future<T>> allResults = executorService.invokeAll(tasks);
    allResults.get(0).get(timeoutInSeconds, TimeUnit.SECONDS);
    return allResults;
  }
  @Test
  public void testGetClientPrefix() {
    assertEquals(manager.getClientPrefix(), "test_prefix_123");
  }
}
