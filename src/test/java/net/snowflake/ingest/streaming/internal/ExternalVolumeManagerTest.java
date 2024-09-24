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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import net.snowflake.ingest.utils.SFException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExternalVolumeManagerTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private ExternalVolumeManager manager;
  private FileLocationInfo fileLocationInfo;

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
  public void teardown() {}

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
    ExecutorService executorService =
        new ThreadPoolExecutor(
            numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    List<Callable<ExternalVolume>> tasks = new ArrayList<>();
    final CyclicBarrier startBarrier = new CyclicBarrier(numThreads);
    final CyclicBarrier endBarrier = new CyclicBarrier(numThreads);
    for (int i = 0; i < numThreads; i++) {
      tasks.add(
          () -> {
            startBarrier.await(30, TimeUnit.SECONDS);
            manager.registerTable(new TableRef("db", "schema", "table"), fileLocationInfo);
            endBarrier.await();
            return manager.getStorage("db.schema.table");
          });
    }

    List<Future<ExternalVolume>> allResults = executorService.invokeAll(tasks);
    allResults.get(0).get(30, TimeUnit.SECONDS);

    ExternalVolume extvol = manager.getStorage("db.schema.table");
    assertNotNull(extvol);
    for (int i = 0; i < numThreads; i++) {
      assertSame("" + i, extvol, allResults.get(i).get(30, TimeUnit.SECONDS));
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
  public void testGetClientPrefix() {
    assertEquals(manager.getClientPrefix(), "test_prefix_123");
  }
}
